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
-module(classy_vote_coordinator).

-behavior(gen_statem).

%% API:
-export([ create/1
        ]).

%% Behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

%% internal exports:
-export([ create_table/0
        , start_link/1

        , receive_vote/1
        , receive_outcome/1

        , do_nothing/1
        , do_nothing/0
        ]).

-export_type([id/0]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy.hrl").
-include("classy_vote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-define(ptab, classy_vote_table).

-define(coordinator(ID), {n, l, {classy_vote_coordinator, ID}}).
-define(participant(ID), {n, l, {classy_vote_participant, ID}}).
-define(via(NAME), {via, gproc, NAME}).

-define(state_timeout, state_timeout).

-type mfargs() :: {module(), atom(), list()}.
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
        #{ prepare   := mfargs()
         , commit    := [mfargs()]
         , rollback  => mfargs()
         }.

-record(act,
        { site_bit :: non_neg_integer()
        , prepare  :: mfargs()
        , commit   :: [mfargs()]
        , rollback :: [mfargs()]
        , reserved :: []
        }).

%% Warning: MFA's are persistently stored!
-type options() ::
        #{ tag       := tag()
         , actions   := #{classy:site() => actions()}
         , post_vote => mfargs()
         , strategy  => strategy()
         , lock      => lock()
         }.

-type vote() :: #c_vote{}.
-type outcome() :: #c_outcome{}.

-define(s_vote, 0).
-define(s_commit, 10).
-define(s_rollback, 20).
-define(s_done, 30).
-type commit_stage() :: ?s_vote | ?s_commit | ?s_rollback | ?s_done.

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
%%   Values:
%%     Common:
-record(ps,
        { lock :: lock()
        , options :: options() | #prepare{}
        , reserved = []
        }).
%%     Coordinator:
-record(ps_c,
        { stage :: commit_stage()
          %% Bit field where 1s represent sites have to ack commit or rollback.
          %% Bit position is determined by `n' field of `action()'
          %% (set automatically).
        , remaining :: non_neg_integer()
        , reserved = []
        }).

%% FSM states:
%%  Coordinator:
-record(d_coord,
        { tag  :: tag()
        , lock :: lock()
        , id   :: id()
        , strategy :: strategy()
        , actions :: [#act{}]
        , post_vote :: [mfargs()]
        , reserved = []
        }).
-type d_coord() :: #d_coord{}.

%% Init args:
-record(init_coordinator,
        { id   :: classy_uid:cu_tuple()
        , tag  :: tag()
        , opts :: options()
        }).
-type init_args() :: #init_coordinator{}.

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
do_nothing() ->
  ok.

do_nothing(_) ->
  ok.

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
    []).

%% @private Coordinator <- Participant
-spec receive_vote(vote()) -> ok | {error, _}.
receive_vote(#c_vote{id = Id} = Vote) ->
  gen_statem:call(
    ?via(?coordinator(#pk_c{id = Id})),
    Vote).

%% @private Coordinator -> Participant
-spec receive_outcome(outcome()) -> ok.
receive_outcome(#c_outcome{id = Id, tag = Tag} = Outcome) ->
  case db_get_options(false, Tag, Id) of
    {ok, _} ->
      %% Table contains record of the vote. It means the participant
      %% flow is ongoing on the site.
      gen_statem:call(
        ?via(?participant(#pk_p{id = Id, tag = Tag})),
        Outcome);
    undefined ->
      %% Table doesn't contain any traces of the vote. Depending on the outcome, it means:
      %%
      %% Commit:
      %% All commit actions finished (`ps_p_vote_yes' record has been deleted).
      %%
      %% Rollback:
      %% 1. Rollback action finished.
      %% 2. Node never received vote request. There's nothing to rollback.
      ok
  end.

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
handle_event(enter, OldState, #ps_c{} = S, D) ->
  coordinator_enter(OldState, S, D);
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
  #{ lock := Lock
   , strategy := Strategy
   , actions := Actions
   , post_vote := PostVote
   } = Opts,
  D = #d_coord{ tag = Tag
              , lock = Lock
              , id = Id
              , strategy = Strategy
              , post_vote = PostVote
              ,
              },
  case db_get_coord_state(Id) of
    undefined ->
      %% This is a new election.
      %% Perform a pre-vote immediately,
      %% and return pre-vote result to the requester synchronously.
      %% This way the fail path doesn't need to write anything on disk,
      %% as the caller is blocked before it makes any changes that should be rolled back,
      %% should the coordinator fail or node restart during pre-vote.
      coordinator_pre_vote(D);
    {ok, Restored = #ps_c{stage = Stage0}} ->
      %% Coordinator restarted:
      Stage = case Stage0 of
                ?s_vote ->
                  %% If coordinator itself restarts during voting stage,
                  %% it considers election failed,
                  %% and all votes are ignored.
                  ?s_rollback;
                _ ->
                  Stage0
              end,
      {next_state, Restored#ps_c{stage = Stage}, D}
  end.

-spec coordinator_pre_vote(d_coord()) -> {ok, #ps_c{}, d_coord()} | {error, false}.
coordinator_pre_vote(#d_coord{id = Id, opts = Opts} = D) ->
  %% Perform a preliminary check:
  #{strategy := Strategy = {all, Timeout}} = Opts,
  Args = prepare_multi(participant_pre_vote, Id, Opts),
  Results = classy_lib:multicall(Args, Timeout),
  ?tp(debug, ?classy_vote_pre_results,
      Opts#{ id => Id
           , results => Results
           }),
  case decide_pre_vote_result(Strategy, maps:iterator(Results)) of
    true ->
      %% Now persist options (todo: do it atomically with creating the state?):
      ok = db_set_options(true, Id, Opts),
      { ok
      , #ps_c{ stage = ?s_vote
             , remaining = ones(maps:size(Results))
             }
      , D
      };
    false ->
      {error, false}
  end.

-spec coordinator_enter(#ps_c{}, #ps_c{}, d_coord()) ->
        {keep_state_and_data, gen_statem:action()} |
        {keep_state, d_coord(), gen_statem:action()}.
coordinator_enter(#ps_c{stage = Stage}, #ps_c{stage = Stage}, _D) ->
  keep_state_and_data;
coordinator_enter(#ps_c{stage = OldStage}, #ps_c{stage = Stage} = State, #d_coord{id = Id} = D) ->
  ok = db_set_coord_state(Id, State),
  ?tp(debug, ?classy_vote_coord_stage, #{id => Id, to => Stage, from => OldStage}),
  case Stage of
    ?s_vote ->
      coordinator_perform_vote(D);
    ?s_commit ->
      {keep_state_and_data, {state_timeout, 0, ?state_timeout}};
    ?s_rollback ->
      {keep_state_and_data, {state_timeout, 0, ?state_timeout}}
  end.

-spec coordinator_perform_vote(d_coord()) -> {keep_state, d_coord(), [gen_statem:action()]}.
coordinator_perform_vote(#d_coord{id = Id, opts = Options} = D0) ->
  #{ actions := Actions
   , strategy := {all, Timeout}
   } = Options,
  Args = prepare_multi(start_participant, Id, Options),
  _ = classy_lib:multicall(Args, Timeout),
  D = D0#d_coord{},
  {keep_state, D, [{state_timeout, ?state_timeout, Timeout}]}.

-spec coordinator_handle_vote(classy:site(), boolean(), #ps_c{}, d_coord()) ->
        {next_state, #ps_c{}, d_coord()} |
        {keep_state, d_coord()}.
coordinator_handle_vote(
  From,
  Vote,
  #ps_c{stage = Stage, remaining = Rem0} = S0,
  #d_coord{id = Id} = D
 ) ->
  ?tp(debug, ?classy_vote_coord_recv,
      #{ id => Id
       , from => From
       , vote => Vote
       , stage => Stage
       }),
  %% FIXME:
  SiteNr = 0,
  NParticipants = n_participants(D),
  Rem = unset_bit(SiteNr, Rem0),
  case Stage of
    ?s_vote ->
      case Vote of
        true ->
          case Rem of
            0 ->
              {next_state, S0#ps_c{stage = ?s_commit, remaining = ones(NParticipants)}, D};
            _ ->
              {next_state, S0#ps_c{remaining = Rem}, D}
          end;
        false ->
          {next_state, #ps_c{stage = ?s_rollback, remaining = ones(NParticipants)}, D}
      end;
    _ ->
      %% Late vote; irrelevant
      {keep_state, D}
  end.

-spec coordinator_state_timeout(#ps_c{}, d_coord()) ->
        {next_state, #ps_c{}, d_coord()} |
        {keep_state, d_coord(), gen_statem:action()}.
coordinator_state_timeout(#ps_c{stage = ?s_vote}, D) ->
  %% Vote timed out, move to rollback:
  { next_state
  , #ps_c{stage = ?s_rollback, remaining = ones(n_participants(D))}
  , D
  };
coordinator_state_timeout(#ps_c{stage = ?s_commit, remaining = Remaining}, D) ->
  coordinator_perform_commit(Remaining, D);
coordinator_state_timeout(#ps_c{stage = ?s_rollback}, D) ->
  coordinator_perform_rollback(D).

coordinator_perform_commit(Remaining, D = #d_coord{}) ->
  %% case coordinator_broadcast_outcome(D, true) of
  %%   ok ->
  {keep_state_and_data, []}.

coordinator_perform_rollback(_) ->
  {keep_state_and_data, []}.

-spec coordinator_broadcast_outcome(d_coord(), boolean()) -> ok | {error, [classy:site()]}.
coordinator_broadcast_outcome(#d_coord{tag = Tag, id = Id, opts = Opts}, IsCommit) ->
  #{actions := Actions} = Opts,
  Outcome = #c_outcome{ id = Id
                      , tag = Tag
                      , result = IsCommit
                      },
  Args = #{Site => {?MODULE, receive_outcome, [Outcome]} || Site := _ <- Actions},
  Results = classy_lib:multicall(Args, classy_lib:rpc_timeout()),
  BadSites = maps:fold(
               fun(Site, Result, Acc) ->
                   case Result of
                     {ok, ok} -> Acc;
                     _ -> [Site | Acc]
                   end
               end,
               [],
               Results),
  case BadSites of
    [] ->
      ok;
    _ ->
      {error, BadSites}
  end.

-spec prepare_multi(atom(), id(), options()) -> map().
prepare_multi(Function, Id, Options) ->
  #{ actions := Actions
   } = Options,
  #{Site => {?MODULE, Function, [prepare(Id, Options, Act)]} ||
    Site := Act <- Actions}.

-spec prepare(id(), options(), actions()) -> #prepare{}.
prepare(
  Id,
  #{tag := Tag, lock := Lock},
  #{prepare := Prep, commit := Commit, rollback := Rollback}
 ) ->
  {ok, Self} = classy_node:the_site(),
  #prepare{ id = Id
          , tag = Tag
          , lock = Lock
          , prepare = Prep
          , commit = Commit
          , rollback = Rollback
          , coordinator = Self
          }.

-spec decide_pre_vote_result(strategy(), maps:iterator()) -> boolean().
decide_pre_vote_result(Strategy, Iter0) ->
  {all, _} = Strategy,
  case maps:next(Iter0) of
    {_Site, Result, Iter} ->
      case Result of
        {ok, {ok, true}} ->
          decide_pre_vote_result(Strategy, Iter);
        _ ->
          false
      end;
    none ->
      true
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

-spec participant_prepare(#prepare{}, d_part()) -> {next_state, participant_state(), d_part()}.
participant_prepare(
  #prepare{ id = Id
          , tag = Tag
          , coordinator = Coordiantor
          , lock = Lock
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
                #{ id => Id
                 , tag => Tag
                 , lock => Lock
                 , role => participant
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

-spec do_prepare(#prepare{}, boolean()) -> {ok, boolean()} | {error, _}.
do_prepare(
  #prepare{ prepare     = Prep
          , commit      = Commit
          , rollback    = Rollback
          , coordinator = Coordinator
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

-spec db_get_coord_state(id()) -> {ok, #ps_c{}} | undefined.
db_get_coord_state(Id) ->
  case classy_table:lookup(?ptab, #pk_c{id = Id}) of
    [#ps_c{} = State] ->
      {ok, State};
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
                    (false, id(), #prepare{}) -> ok.
db_set_options(IsCoordinator, VoteId, Options) ->
  case IsCoordinator of
    true ->
      #{tag := Tag, lock := Lock} = Options;
    false ->
      #prepare{tag = Tag, lock = Lock} = Options
  end,
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
      %% Note this could race with table restoration
      undefined
  end.

%%--------------------------------------------------------------------------------
%% Input validation
%%--------------------------------------------------------------------------------

-spec with_defaults(options()) -> {ok, tag(), options()} | {error, _}.
with_defaults(UserOpts) when is_map(UserOpts) ->
  Defaults = #{ post_vote => {?MODULE, do_nothing, []}
              , strategy  => {all, classy_lib:rpc_timeout()}
              , lock      => []
              },
  Merged = maps:merge(Defaults, UserOpts),
  case Merged of
    #{ tag       := Tag
     , post_vote := PostVote
     , actions   := Actions0
     , strategy  := Strategy0
     , lock      := _
     } ->
      maybe
        ok ?= verify_post_vote(PostVote),
        {ok, Actions} ?= verify_actions(Actions0),
        {ok, Strategy} ?= verify_strategy(Strategy0),
        {ok, Tag, Merged#{ actions  := Actions
                         , strategy := Strategy
                         }}
      end;
    _ ->
      {error, badarg}
  end.

verify_post_vote(undefined) ->
  ok;
verify_post_vote(MFA) ->
  verify_mfa(bad_post_vote, 1, MFA).

verify_strategy(all) ->
  {ok, {all, classy_lib:rpc_timeout()}};
verify_strategy({all, Timeout}) when is_integer(Timeout), Timeout > 0 ->
  {ok, {all, Timeout}};
verify_strategy(Strategy) ->
  {error, {bad_strategy, Strategy}}.

verify_actions(Actions0) when is_map(Actions0) ->
  try
    maps:size(Actions0) =:= 0 andalso throw({error, no_actions}),
    {Actions, _} = maps:fold(fun enrich_action/3, {#{}, 0}, Actions0),
    {ok, Actions}
  catch
    Err -> Err
  end;
verify_actions(Bad) ->
  {error, {bad_actions, Bad}}.

enrich_action(Site, {Acc, SiteNr}, SiteActions) when is_binary(Site), is_map(SiteActions) ->
  ActionDefaults = #{rollback => []},
  case maps:merge(ActionDefaults, SiteActions) of
    #{prepare := Prep, commit := Commit, rollback := Rollback} = Result ->
      maybe
        ok ?= verify_prepare(Prep),
        ok ?= verify_commit(Commit),
        ok ?= verify_rollback(Rollback),
        { Acc#{Site => Result#{n => SiteNr}}
        , SiteNr + 1
        }
      else
        Err -> throw(Err)
      end;
    _ ->
      throw({error, {bad_action, Site, SiteActions}})
  end;
enrich_action(BadSite, _, BadAction) ->
  throw({error, {bad_action, BadSite, BadAction}}).

verify_prepare(Prepare) ->
  verify_mfa(bad_prepare, 0, Prepare).

verify_commit(Commit) ->
  verify_mfas(bad_commit, Commit).

verify_rollback(Rollback) ->
  verify_mfas(bad_rollback, Rollback).

verify_coordinator(Coordinator) ->
  case classy_node:node_of_site(Coordinator, true) of
    {ok, _} ->
      ok;
    _ ->
      {errror, coordinator_unreachable}
  end.

verify_mfas(Reason, Commits) when is_list(Commits) ->
  try
    [case verify_mfa(Reason, 0, I) of
       ok ->
         ok;
       Err ->
         throw(Err)
     end || I <- Commits],
    ok
  catch
    Err -> Err
  end;
verify_mfas(Reason, Other) ->
  {error, {Reason, Other}}.

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

-spec ones(pos_integer()) -> pos_integer().
ones(N) ->
  1 bsl N - 1.

-spec unset_bit(non_neg_integer(), non_neg_integer()) -> non_neg_integer().
unset_bit(Bit, Bitfield) ->
  Bitfield band bnot (1 bsl Bit).

-spec is_bit_set(non_neg_integer(), non_neg_integer()) -> boolean().
is_bit_set(Bit, Bitfield) ->
  (Bitfield band (1 bsl Bit)) > 0.

-spec n_participants(#d_coord{}) -> ok.
n_participants(D) ->
  %% FIXME:
  0.

-ifdef(TEST).

ones_test() ->
  ?assertEqual(2#1, ones(1)),
  ?assertEqual(2#11, ones(2)),
  ?assertEqual(2#111, ones(3)),
  ?assertEqual(2#1111, ones(4)),
  ?assertEqual(2#11111111111111111111, ones(20)).

unset_bit_test() ->
  N1 = ones(3),
  ?assertEqual(2#110, unset_bit(0, N1)),
  ?assertEqual(2#101, unset_bit(1, N1)),
  ?assertEqual(2#011, unset_bit(2, N1)),
  N2 = ones(5),
  ?assertEqual(2#11110, unset_bit(0, N2)),
  ?assertEqual(2#11101, unset_bit(1, N2)),
  ?assertEqual(2#11011, unset_bit(2, N2)),
  ?assertEqual(2#10111, unset_bit(3, N2)),
  ?assertEqual(2#01111, unset_bit(4, N2)).

-endif.
