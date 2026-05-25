%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements a variation of 2-phase commit.
%%
%% Note: vote is rather heavy operation.
%% Do not use it when frequent coordination is needed.
%%
%% Each voting action uses the following following callbacks:
%%
%% <itemize>
%% <li>
%% <b>prepare</b>: Executed on the participant sites.
%% Classy prepends an additional arugment indicating whether the prepare action can have side effects.
%% Return value is a boolean indicating the participant's vote (`true' = `yes').
%% </li>
%%
%% <li>
%% <b>commit</b>: List of actions executed on the participant sites
%% when the coordinator decides to go ahead with the commit.
%% Classy doesn't add additional arguments.
%% Return value is ignored.
%%
%% Un-executed actions are retried in case of node restart.
%% </li>
%%
%% <li>
%% <b>rollback</b>: Executed on the participant sites
%% when the coordinator decides to abort the commit.
%% Classy doesn't add additional arguments.
%% Return value is ignored.
%% This callback can be retried on node restart.
%% </li>
%%
%% <li>
%% <b>post_vote</b>: Executed on the coordinator.
%% Classy prepends a boolean indicating result of the vote to the argument list.
%% Return value is ignored.
%% It's NOT guaranteed that all commit actions on the participants are finished by this time.
%% This callback can be retried on node restart.
%% </li>
%% </itemize>
-module(classy_vote).

-behavior(gen_statem).

%% API:
-export([ create/1
        ]).

%% Behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

%% internal exports:
-export([ create_table/0
        , start_link/1

        , participant_pre_vote/1
        , start_participant/1
        , receive_vote/1
        , receive_outcome/1
        ]).

-export_type([id/0]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy.hrl").
-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(ptab, classy_vote_table).

-define(coordinator(ID), {n, l, {classy_vote_coordinator, ID}}).
-define(participant(ID), {n, l, {classy_vote_participant, ID}}).
-define(via(NAME), {via, gproc, NAME}).

-define(state_timeout, state_timeout).

%% Lock tag associated with the operation.
%% It allows business logic to quickly enumerate ongoing votes of certain kind.
-type tag() :: term().
%% Unique ID of the vote.
-type id() :: classy_uid:cu_tuple().
%% Arbitrary lock information that business logic can use to detect conflicts.
-type lock() :: term().

%% Strategy used to decide when to commit
%%
%% `all': All participant must vote `true' within the timeout
-type strategy() :: {all, timeout()}.

-type actions() ::
        #{ prepare   := mfa()
         , commit    := [mfa()]
         , rollback  => mfa()
         }.

%% Warning: MFA's are persistently stored!
-type options() ::
        #{ tag       := tag()
         , prepare   := mfa()
         , commit    := [mfa()]
         , sites     := [classy:site()]
         , rollback  => mfa()
         , post_vote => mfa()
         , strategy  => strategy()
         , lock      => lock()
         }.

%% Protocol:
%%   Coordinator -> Participant
-type prepare() ::
        #{ id          := id()
         , tag         := tag()
         , lock        := lock()
         , prepare     := mfa()
         , commit      := [mfa()]
         , rollback    := mfa()
         , coordinator := classy:site()
         }.
%%   Coordinator <- Participant
-record(c_vote,
        { id            :: id()
        , vote          :: boolean()
        , from          :: classy:site()
        , reserved = [] :: term()
        }).
-type vote() :: #c_vote{}.
%%   Coordiantor -> Participant
-record(c_outcome,
        { id       :: id()
        , tag      :: tag()
        , result   :: boolean() % true = commit, false = rollback
        , reserved :: []
        }).
-type outcome() :: #c_outcome{}.

-define(s_vote, 0).
-define(s_commit, 10).
-define(s_rollback, 20).
-type commit_stage() :: ?s_vote | ?s_commit | ?s_rollback.

%% Persistent states:
%%   Keys:
%%     Common:
-record(pk,
        { is_coordinator :: boolean()
        , tag :: tag()
        , id :: id() | '_'
        }).
%%     Coordinator:
%%         Coordinator's own state:
-record(pk_c,
        { id :: id() | '_'
        }).
%%     Participant:
-record(pk_p,
        { tag :: tag()
        , id :: id() | '_'
        }).
%%   Values:
%%     Common:
-record(ps,
        { lock :: lock()
        , options :: map()
        , reserved = []
        }).
%%     Coordinator:
-record(ps_c,
        { stage :: commit_stage()
        , reserved = []
        }).
%%     Participant:
-record(ps_p_vote_yes,
        { reserved = []
        }).
-record(ps_p_rollback,
        { vote :: boolean()
        , reserved = []
        }).
-record(ps_p_comm,
        { remaining_actions :: [mfa()]
        , reserved = []
        }).

-type participant_state() :: #ps_p_vote_yes{} | #ps_p_rollback{} | #ps_p_comm{}.

%% FSM states:
%%  Coordinator:
-record(d_coord,
        { tag :: tag()
        , id :: id()
        , remaining_votes = [] :: [classy:site()]
        , opts :: options()
        }).
-type d_coord() :: #d_coord{}.
%%  Participant:
-record(d_part, {}).
-type d_part() :: #d_part{}.

%% Init args:
-record(init_coordinator,
        { id   :: classy_uid:cu_tuple()
        , tag  :: tag()
        , opts :: options()
        }).
-record(init_participant,
        { name :: #pk_p{}
        , opts :: prepare()
        }).
-type init_args() :: #init_coordinator{}
                   | #init_participant{}.

%%================================================================================
%% API functions
%%================================================================================

%% %% @doc List ongoing commit actions.
%% %%
%% %% Argument: match specification for the tag.
%% -spec ls_votes(_TagMatch) -> #{id() => vote_info()}.
%% ls_votes(TagMatch) ->
%%   fold_votes(
%%     TagMatch,
%%     fun(ID, Action, Acc) -> Acc#{ID => Action} end).

%% @doc Fold over ongoing commit actions.
%%
%% Argument: match specification for the tag.
%% -spec fold_votes(_TagMatch, fun((id(), vote_info(), Acc) -> Acc)) -> Acc.
%% fold_votes(TagMatch, Fun) ->
%%   BatchSize = 100,
%%   MS = { #classy_kv{k = #pk_p{tag = TagMatch, _ = '_'}, _ = '_'}
%%        , []
%%        , ['$_']
%%        },
%%   do_fold_votes(ets:select(?ptab, [MS], BatchSize), Fun, #{}).

%% @doc Initiate a new vote.
%%
%% Note: This function returns immediately.
-spec create(options()) -> ok | {error, _}.
create(UserOptions) ->
  maybe
    {ok, Tag, Options} ?= with_defaults(UserOptions),
    ID = classy_uid:cluster_unique_seq_tuple(classy_vote_sequence),
    {ok, _} ?= classy_sup:ensure_vote(
                 #init_coordinator{ id   = ID
                                  , tag  = Tag
                                  , opts = Options
                                  }),
    ok
  end.

%%================================================================================
%% Internal exports
%%================================================================================

%% @private
-spec create_table() -> ok.
create_table() ->
  classy_table:open(
    ?ptab,
    #{ ets_options => [ordered_set, {read_concurrency, true}]
     }).

%% @private
-spec start_link(init_args()) -> gen_statem:start_ret().
start_link(#init_coordinator{id = ID} = Arg) ->
  gen_statem:start_link(
    ?via(?coordinator(#pk_c{id = ID})),
    ?MODULE,
    Arg,
    []);
start_link(#init_participant{name = Name} = Arg) ->
  gen_statem:start_link(
    ?via(?coordinator(Name)),
    ?MODULE,
    Arg,
    []).

%% @private Coordinator -> Participant
-spec participant_pre_vote(prepare()) -> {ok, boolean()} | {error, _}.
participant_pre_vote(Prepare) ->
  do_prepare(Prepare, false).

%% @private Coordinator -> Participant
-spec start_participant(prepare()) -> {ok, pid()}.
start_participant(#{id := Id , tag := Tag} = Prepare) ->
  classy_sup:ensure_vote(
    #init_participant{ name = #pk_p{id = Id, tag = Tag}
                     , opts = Prepare
                     }).

%% @private Coordinator <- Participant
-spec receive_vote(vote()) -> ok | {error, _}.
receive_vote(#c_vote{id = Id} = Vote) ->
  gen_statem:call(
    ?via(?coordinator(#pk_c{id = Id})),
    Vote).

%% @private Coordinator -> Participant
-spec receive_outcome(outcome()) -> ok.
receive_outcome(#c_outcome{id = Id, tag = Tag} = Outcome) ->
  gen_statem:call(
    ?via(?participant(#pk_p{id = Id, tag = Tag})),
    Outcome).

%%================================================================================
%% behavior callbacks
%%================================================================================

%% @private
callback_mode() ->
  [handle_event_function, state_enter].

%% @private
init(InitOpts) ->
  process_flag(trap_exit, true),
  case InitOpts of
    #init_coordinator{} -> init_coordinator(InitOpts);
    #init_participant{} -> init_participant(InitOpts)
  end.

%% @private
%% Coodinator:
handle_event(enter, _OldState, #ps_c{} = S, D) ->
  coordinator_enter(S, D);
handle_event(call, #c_vote{from = From, vote = Vote}, #ps_c{} = S, D) ->
  coordinator_handle_vote(From, Vote, S, D);
handle_event(state_timeout, ?state_timeout, #ps_c{} = S, D) ->
  coordinator_state_timeout(S, D);
%% Common:
handle_event(info, {'EXIT', _, Reason}, _, _) ->
  case Reason of
    normal -> keep_state_and_data;
    _      -> {stop, shutdown}
  end;
handle_event(ET, Event, State, _Data) ->
  %% TODO: put ID and MFAs into error messages
  ?tp(warning, ?classy_unknown_event,
      #{ kind    => ET
       , content => Event
       , state   => State
       , server  => ?MODULE
       }),
  keep_state_and_data.

%% @private
terminate(Reason, State, _Data) ->
  %% TODO: put ID and MFAs into error messages
  classy_lib:is_normal_exit(Reason) orelse
    ?tp(warning, ?classy_abnormal_exit,
        #{ server => ?MODULE
         , reason => Reason
         , state  => State
         }),
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

%% do_fold_votes('$end_of_table', _Fun, Acc) ->
%%   Acc;
%% do_fold_votes({Matches, Continuation}, Fun, Acc0) ->
%%   Acc = lists:foldl(
%%           fun(#classy_kv{k = #pk_p{id = ID}, v = Status}, Acc1) ->
%%               Val = case Status of
%%                       #pv_p_vote{vote = Vote} ->
%%                         {voted, #{vote => Vote}};
%%                       #pv_p_comm{actions = Actions} ->
%%                         {committing, #{actions => Actions}};
%%                       #pv_p_rollback{} ->
%%                         {rollback, #{}}
%%                     end,
%%               Fun(ID, Val, Acc1)
%%           end,
%%           Acc0,
%%           Matches),
%%   do_fold_votes(ets:select(Continuation), Fun, Acc).

%%--------------------------------------------------------------------------------
%% Coordinator
%%--------------------------------------------------------------------------------

-spec init_coordinator(#init_coordinator{}) -> {next_state, #ps_c{}, d_coord()} | {error, _}.
init_coordinator(#init_coordinator{tag = Tag, id = Id, opts = Opts}) ->
  D = #d_coord{tag = Tag, id = Id, opts = Opts},
  case db_get_coord_stage(Id) of
    {ok, ?s_vote} ->
      %% If coordinator itself restarts during voting stage,
      %% it considers election failed, and votes are ignored.
      %% (something, something, social commentary)
      {next_state, #ps_c{stage = ?s_rollback}, D};
    {ok, Stage} ->
      {next_state, #ps_c{stage = Stage}, D};
    undefined ->
      %% This is a new election.
      %% Perform a pre-vote immediately,
      %% and return pre-vote result to the requester synchronously.
      %% This way the fail path doesn't need to write anything on disk,
      %% as the caller is blocked before it makes any changes that should be rolled back,
      %% should the coordinator fail or node restart during pre-vote.
      coordinator_pre_vote(D)
  end.

-spec coordinator_pre_vote(d_coord()) -> {ok, #ps_c{}, d_coord()} | {error, false}.
coordinator_pre_vote(#d_coord{id = Id, opts = Opts} = D) ->
  %% Perform a preliminary check:
  #{ sites    := Sites
   , strategy := {all, Timeout}
   } = Opts,
  NonOk = case classy_lib:sites_to_nodes(Sites) of
            {Nodes, []} ->
              Results = lists:zip(
                          Sites,
                          erpc:multicall(Nodes, ?MODULE, participant_pre_vote, [Opts], Timeout)),
              lists:filtermap(fun is_pre_vote_failure/1, Results);
            {_Nodes, BadSites} ->
              [{I, {error, site_unavailable}} || I <- BadSites]
          end,
  case NonOk of
    [] ->
      %% Now persist options:
      ok = db_set_options(true, Id, Opts),
      {ok, #ps_c{stage = ?s_vote}, D};
    _ ->
      ?tp(debug, classy_vote_pre_vote_no, Opts#{reason => NonOk}),
      {error, false}
  end.

-spec coordinator_enter(#ps_c{}, d_coord()) ->
        {keep_state_and_data, [gen_statem:action()]} |
        {keep_state, d_coord(), [gen_statem:action()]}.
coordinator_enter(
  State = #ps_c{stage = Stage},
  #d_coord{id = Id, opts = Opts} = D
 ) ->
  ok = db_set_coord_state(Id, State),
  case Stage of
    ?s_vote ->
      coordinator_perform_vote(D);
    ?s_commit ->
      coordinator_perform_commit(Id, Opts);
    ?s_rollback ->
      coordinator_perform_rollback(Id, Opts)
  end.

-spec coordinator_perform_vote(d_coord()) -> {keep_state, d_coord(), [gen_statem:action()]}.
coordinator_perform_vote(#d_coord{id = Id, opts = Options} = D0) ->
  #{ tag      := Tag
   , lock     := Lock
   , prepare  := Prepare
   , commit   := Commit
   , rollback := Rollback
   , sites    := Sites
   , strategy := {all, Timeout}
   } = Options,
  {ok, Self} = classy_node:the_site(),
  Prep = #{ id          => Id
          , tag         => Tag
          , lock        => Lock
          , prepare     => Prepare
          , commit      => Commit
          , rollback    => Rollback
          , coordinator => Self
          },
  %% We've just verified that participants are good, skipping further
  %% checks:
  {Nodes, _} = classy_lib:sites_to_nodes(Sites),
  erpc:multicast(Nodes, ?MODULE, start_participant, [Prep]),
  D = D0#d_coord{remaining_votes = Sites},
  {keep_state, D, [{state_timeout, ?state_timeout, Timeout}]}.

-spec coordinator_handle_vote(classy:site(), boolean(), #ps_c{}, d_coord()) ->
        {next_state, #ps_c{}, d_coord()} |
        {keep_state, d_coord()}.
coordinator_handle_vote(
  From,
  Vote,
  #ps_c{stage = ?s_vote},
  #d_coord{remaining_votes = Remaining0} = D0
 ) ->
  case Vote of
    true ->
      Remaining = Remaining0 -- [From],
      D = D0#d_coord{remaining_votes = Remaining},
      case Remaining of
        [] ->
          {next_state, #ps_c{stage = ?s_commit}, D};
        _ ->
          {keep_state, D}
      end;
    false ->
      D = D0#d_coord{remaining_votes = []},
      {next_state, #ps_c{stage = ?s_rollback}, D}
  end;
coordinator_handle_vote(_From, _Vote, #ps_c{}, D) ->
  %% Late vote; irrelevant
  {keep_state, D}.

coordinator_state_timeout(#ps_c{stage = ?s_vote}, D) ->
  {next_state, #ps_c{stage = ?s_rollback}, D#d_coord{remaining_votes = []}};
coordinator_state_timeout(#ps_c{stage = _}, _D) ->
  keep_state_and_data.

coordinator_perform_commit(_, _) ->
  {keep_state_and_data, []}.

coordinator_perform_rollback(_, _) ->
  {keep_state_and_data, []}.

is_pre_vote_failure({Site, Result}) ->
  case Result of
    {ok, {ok, true}} ->
      false;
    {ok, {ok, false}} ->
      {true, {Site, vote_no}};
    {ok, {error, _} = Err} ->
      {true, {Site, Err}};
    Err ->
      {true, {Site, Err}}
  end.

%%--------------------------------------------------------------------------------
%% Participant
%%--------------------------------------------------------------------------------

init_participant(#init_participant{name = Name, opts = Opts}) when is_record(Name, pk_p) ->
  #pk_p{tag = Tag, id = Id} = Name,
  case db_get_participant_state(Tag, Id) of
    {ok, State} ->
      {ok, State, #d_part{}};
    undefined ->
      ok = db_set_options(false, Id, Opts),
      {next_state, State, D} = participant_prepare(Opts, #d_part{}),
      {ok, State, D}
  end.

-spec participant_prepare(prepare(), d_part()) -> {next_state, participant_state(), d_part()}.
participant_prepare(
  #{ id          := Id
   , tag         := Tag
   , coordinator := Coordiantor
   } = Prepare,
   Data
 ) ->
  Outcome =
    case do_prepare(Prepare, true) of
      {ok, true} ->
        State = #ps_p_vote_yes{},
        ok = db_set_participant_state(Tag, Id, State),
        true;
      No ->
        case No of
          {ok, false} ->
            ok;
          _ ->
            ?tp(warning, classy_vote_error,
                Prepare#{ role => participant
                        , stage => vote
                        , reason => No
                        })
        end,
        State = #ps_p_rollback{vote = false},
        ok = db_set_participant_state(Id, Tag, State),
        false
    end,
  send_vote(Coordiantor, Id, Outcome),
  {next_state, State, Data}.

-spec send_vote(classy:site(), id(), boolean()) -> ok | {error, _}.
send_vote(Coordinator, Id, Vote) ->
  maybe
    {ok, Node} ?= classy_node:node_of_site(Coordinator, true),
    {ok, From} ?= classy_node:the_site(),
    Rec = #c_vote{ id = Id
                 , from = From
                 , vote = Vote
                 },
    erpc:call(Node, ?MODULE, receive_vote, [Rec])
  end.

-spec do_prepare(prepare(), boolean()) -> {ok, boolean()} | {error, _}.
do_prepare(
  #{ prepare     := Prep
   , commit      := Commit
   , rollback    := Rollback
   , coordinator := Coordinator
   },
  ForReal
 ) ->
  maybe
    ok ?= verify_prepare(Prep),
    ok ?= verify_commit(Commit),
    ok ?= verify_rollback(Rollback),
    ok ?= verify_coordinator(Coordinator),
    {M, F, Args} = Prep,
    Vote = apply(M, F, [ForReal | Args]),
    true ?= is_boolean(Vote) orelse {error, bad_result},
    {ok, Vote}
  end.

%%--------------------------------------------------------------------------------
%% Database access
%%--------------------------------------------------------------------------------

-spec db_set_coord_state(id(), #ps_c{}) -> ok.
db_set_coord_state(Id, State) when is_record(State, ps_c) ->
  classy_table:write(?ptab, #pk_c{id = Id}, State).

-spec db_get_coord_stage(id()) -> {ok, commit_stage()} | undefined.
db_get_coord_stage(Id) ->
  case classy_table:lookup(?ptab, #pk_c{id = Id}) of
    [#ps_c{stage = Stage}] ->
      {ok, Stage};
    [] ->
      undefined
  end.

-spec db_set_participant_state(tag(), id(), participant_state()) -> ok.
db_set_participant_state(Tag, Id, State) ->
  classy_table:write(
    ?ptab,
    #pk_p{tag = Tag, id = Id},
    State).

-spec db_get_participant_state(tag(), id()) -> {ok, participant_state()} | undefined.
db_get_participant_state(Tag, Id) ->
  case classy_table:lookup(?ptab, #pk_p{tag = Tag, id = Id}) of
    [State] ->
      {ok, State};
    [] ->
      undefined
  end.

-spec db_set_options(true, id(), options()) -> ok;
                    (false, id(), prepare()) -> ok.
db_set_options(IsCoordinator, VoteId, #{tag := Tag, lock := Lock} = Options) ->
  classy_table:write(
    ?ptab,
    #pk{is_coordinator = IsCoordinator, tag = Tag, id = VoteId},
    #ps{lock = Lock, options = Options}).

-spec db_get_options(boolean(), tag(), id()) -> {ok, options()} | undefined.
db_get_options(IsCoordinator, Tag, VoteId) ->
  case classy_table:lookup(
         ?ptab,
         #pk{is_coordinator = IsCoordinator, tag = Tag, id = VoteId}) of
    [#ps{options = Options}] ->
      {ok, Options};
    [] ->
      undefined
  end.

%%--------------------------------------------------------------------------------
%% Input validation
%%--------------------------------------------------------------------------------

-spec with_defaults(options()) -> {ok, tag(), options()} | {error, _}.
with_defaults(UserOpts) when is_map(UserOpts) ->
  Defaults = #{ rollback  => undefined
              , post_vote => undefined
              , strategy  => all
              , timeout   => 5_000
              , lock      => []
              },
  Merged = maps:merge(Defaults, UserOpts),
  case Merged of
    #{ tag       := Tag
     , prepare   := Prepare
     , commit    := Commit
     , rollback  := Rollback
     , post_vote := PostVote
     , sites     := Sites0
     , strategy  := Strategy0
     , lock      := _
     } ->
      maybe
        ok ?= verify_prepare(Prepare),
        ok ?= verify_commit(Commit),
        ok ?= verify_rollback(Rollback),
        ok ?= verify_post_vote(PostVote),
        {ok, Sites} ?= verify_sites(Sites0),
        {ok, Strategy} ?= verify_strategy(Strategy0),
        {ok, Tag, Merged#{ sites    := Sites
                         , strategy := Strategy
                         }}
      end;
    _ ->
      {error, badarg}
  end.

verify_prepare(Prepare) ->
  verify_mfa(bad_prepare, 0, Prepare).

verify_rollback(Rollback) ->
  verify_mfa(bad_rollback, 0, Rollback).

verify_coordinator(Coordinator) ->
  case classy_node:node_of_site(Coordinator, true) of
    {ok, _} ->
      ok;
    _ ->
      {errror, coordinator_unreachable}
  end.

-spec verify_mfa(atom(), non_neg_integer(), term()) -> ok | {error, {atom(), term()}}.
verify_mfa(Subject, NExtraArgs, {M, F, Args}) when is_atom(M),
                                                   is_atom(F),
                                                   is_list(Args) ->
  NArgs = length(Args) + NExtraArgs,
  Err = {error, {Subject, {M, F, NArgs}}},
  case erlang:function_exported(M, F, NArgs) of
    true  -> ok;
    false -> Err
  end;
verify_mfa(Subject, _, Other) ->
  {error, {Subject, Other}}.

verify_post_vote(undefined) ->
  ok;
verify_post_vote(MFA) ->
  verify_mfa(bad_post_vote, 1, MFA).

verify_commit(Commits) when is_list(Commits) ->
  try
    [case verify_mfa(bad_commit, 0, I) of
       ok ->
         ok;
       Err ->
         throw(Err)
     end || I <- Commits],
    ok
  catch
    Err -> Err
  end;
verify_commit(Other) ->
  {error, {bad_commit, Other}}.

verify_sites([]) ->
  {error, empty_participant_list};
verify_sites(Participants0) when is_list(Participants0) ->
  Participants = lists:uniq(Participants0),
  case classy:sites() of
    [] ->
      {error, site_is_not_running};
    Peers ->
      case Participants -- Peers of
        [] ->
          {ok, Participants};
        Bad ->
          {error, {bad_sites, Bad}}
      end
  end;
verify_sites(Bad) ->
  {error, {bad_sites, Bad}}.

verify_strategy(all) ->
  {ok, {all, classy_lib:rpc_timeout()}};
verify_strategy({all, Timeout}) when is_integer(Timeout), Timeout > 0 ->
  {ok, {all, Timeout}};
verify_strategy(Strategy) ->
  {error, {bad_strategy, Strategy}}.
