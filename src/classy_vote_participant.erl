%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_vote_participant).

-behavior(gen_statem).

%% API:
-export([restore/0, start_link/1]).

%% Behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

%% Protocol:
-export([pre_vote/1, vote/1, receive_outcome/1]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy_vote.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(s_prepare, 0).
-define(s_wait_outcome, 1).
-define(s_commit, 2).
-define(s_rollback, 3).

-type stage() :: ?s_prepare | ?s_wait_outcome | ?s_commit | ?s_rollback.

%%             .----> rollback <-.
%%            /                   \
%%           /                     \
%%  o--- prepare          receive_outcome=false
%%          \                     /
%%           \                   /
%%            "-----> wait_outcome ---> receive_outcome=true --> commit

%% Persistent state (dynamically changing data):
%%   Key:
-record(pk_ps,
        { id :: classy_vote:id()
        }).
%%   Value:
-record(ps_ps,
        { stage :: stage()
        , vote :: 0..1
        , completed_actions :: non_neg_integer()
        , reserved = []
        }).
%% Persistent static data
%%   Key:
-record(pk_pd,
        { tag :: classy_vote:tag()
        , id :: classy_vote:id() | atom()
        }).
%%  Value: #prepare{}

-record(d,
        { prep                  :: #prepare{}
        , vote                  :: boolean() | undefined
        , completed_actions = 0 :: non_neg_integer()
        }).
-type d() :: #d{}.

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(#prepare{}) -> gen_statem:start_ret().
start_link(Prepare = #prepare{id = ID}) ->
  gen_statem:start_link(
    ?via(?participant(ID)),
    ?MODULE,
    [Prepare],
    []).

restore() ->
  %% Note: this call returns after the table is restored & safe to read:
  ok = classy_vote:create_table(),
  MS = { #classy_kv{k = #pk_pd{_ = '_'}, v = '$1', _ = '_'}
       , []
       , ['$1']
       },
  lists:foreach(
    fun(Prep) ->
        vote(Prep)
    end,
    ets:select(?ptab, [MS])).

%%================================================================================
%% Internal exports
%%================================================================================

%% @private Coordinator -> Participant
-spec pre_vote(#prepare{}) -> boolean() | {error, _}.
pre_vote(Prepare) ->
  case do_prepare(Prepare, false) of
    {ok, Bool} when is_boolean(Bool) ->
      Bool;
    {error, Err} ->
      error(Err)
  end.

%% @private Coordinator -> Participant
-spec vote(#prepare{}) -> ok | {error, _}.
vote(Prepare) ->
  case classy_sup:ensure_vote_participant([Prepare]) of
    {ok, _Pid} ->
      ok;
    Err ->
      Err
  end.

%% @private Coordinator -> Participant
-spec receive_outcome(classy_vote:outcome()) -> ack.
receive_outcome(#c_outcome{id = ID, tag = Tag} = Outcome) ->
  case get_prepare(Tag, ID) of
    {ok, _} ->
      %% Table contains record of the vote. It means the participant
      %% flow is ongoing on the site. Intermediate state when table is
      %% open but processes are not started yet are handled by the
      %% coordinator retry.
      gen_statem:call(
        ?via(?participant(ID)),
        Outcome);
    undefined ->
      %% Table doesn't contain any traces of the vote. Depending on the outcome, it means:
      %%
      %% Commit:
      %% All commit actions finished and state was deleted
      %%
      %% Rollback:
      %% 1. Rollback action finished.
      %% 2. Node never received vote request. There's nothing to rollback.
      ack
  end.

%%================================================================================
%% behavior callbacks
%%================================================================================

%% @private
callback_mode() ->
  [handle_event_function, state_enter].

%% @private
init([Prepare = #prepare{tag = Tag, id = ID}]) ->
  process_flag(trap_exit, true),
  case db_restore(Tag, ID) of
    {ok, Stage, D} ->
      %% An existing participant has been restored. Proceed where it
      %% ended:
      {ok, Stage, D};
    undefined ->
      %% This is a new participant:
      maybe
        {ok, D} ?= db_establish(?s_prepare, false, 0, Prepare),
        {ok, ?s_prepare, D}
      end
  end.

handle_event(enter, OldStage, Stage, D) ->
  enter(OldStage, Stage, D);
handle_event(state_timeout, ?state_timeout, ?s_prepare, D) ->
  %% Perform the actual vote:
  do_real_vote(D);
handle_event(info, {'EXIT', _, Reason}, _, _) ->
  case Reason of
    normal -> keep_state_and_data;
    _      -> {stop, shutdown}
  end;
handle_event({call, From}, #c_outcome{} = Outcome, ?s_wait_outcome, D) ->
  do_receive_outcome(From, Outcome, D);
handle_event(ET, Event, State, _Data) ->
  %% TODO: put ID and MFAs into error messages
  ?tp(warning, ?classy_unknown_event,
      #{ kind    => ET
       , content => Event
       , state   => State
       , server  => ?MODULE
       }),
  keep_state_and_data.

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

do_receive_outcome(From, #c_outcome{result = Result}, D0 = #d{vote = MyVote}) ->
  NextStage = case Result of
                true -> ?s_commit;
                false -> ?s_rollback
              end,
  {ok, D} = db_update(NextStage, MyVote, 0, D0),
  {next_state, NextStage, D, [{reply, From, ack}]}.

enter(OldStage, Stage, #d{prep = Prep} = D) ->
  #prepare{id = ID} = Prep,
  ?tp(debug, ?classy_vote_part_stage,
      #{ id => ID
       , to => Stage
       , from => OldStage
       }),
  case Stage of
    ?s_prepare ->
      {keep_state_and_data, mk_timer(0)};
    ?s_wait_outcome ->
      send_vote(D),
      keep_state_and_data;
    ?s_commit ->
      perform_commit(D);
    ?s_rollback ->
      perform_rollback(D)
  end.

-spec perform_commit(d()) -> {stop, normal, d()}.
perform_commit(D = #d{completed_actions = CA, prep = Prep}) ->
  #prepare{commit = CommitActions} = Prep,
  perform_actions(
    ?s_commit,
    nthtail(CA, CommitActions),
    D).

-spec perform_rollback(d()) -> {stop, normal, d()}.
perform_rollback(D = #d{completed_actions = CA, prep = Prep}) ->
  #prepare{rollback = RollbackActions} = Prep,
  perform_actions(
    ?s_rollback,
    nthtail(CA, RollbackActions),
    D).

-spec perform_actions(stage(), [classy_vote:mfargs()], d()) -> {stop, normal, d()}.
perform_actions(_, [], D) ->
  db_teardown(D),
  {stop, normal, D};
perform_actions(Stage, [MFA | Rest], D0 = #d{completed_actions = CA, vote = Vote}) ->
  _ = classy_lib:safe_apply(MFA),
  {ok, D} = db_update(Stage, Vote, CA + 1, D0),
  perform_actions(Stage, Rest, D).

-spec do_real_vote(d()) -> {next_state, ?s_wait_outcome, d()}.
do_real_vote(#d{prep = Prep} = D0) ->
  case do_prepare(Prep, true) of
    {ok, Vote} ->
      ok;
    Error ->
      ?tp(warning, classy_vote_participant_prep_fail,
          #{ id => Prep#prepare.id
           , tag => Prep#prepare.tag
           , reason => Error
           }),
      Vote = false
  end,
  {ok, D} = db_update(?s_wait_outcome, Vote, 0, D0),
  {next_state, ?s_wait_outcome, D}.

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
    ok ?= classy_vote:verify_prepare(Prep),
    ok ?= classy_vote:verify_commit(Commit),
    ok ?= classy_vote:verify_rollback(Rollback),
    ok ?= verify_coordinator(Coordinator),
    {M, F, Args} = Prep,
    {ok, Vote} ?= classy_lib:safe_apply(M, F, [ForReal | Args]),
    true ?= is_boolean(Vote) orelse {error, {bad_result, Vote}},
    {ok, Vote}
  end.

send_vote(#d{vote = Vote, prep = Prep = #prepare{id = ID}}) ->
  {ok, Self} = classy_node:the_site(),
  #prepare{id = Id, coordinator = Coordinator} = Prep,
  Arg = #c_vote{ id = Id
               , vote = Vote
               , from = Self
               },
  _ = ?tp_span(debug, ?classy_vote_part_send_vote, #{id => ID, vote => Vote, from => Self},
               classy_lib:multicall(
                 #{Coordinator => {classy_vote_coordinator, receive_vote, [Arg]}})),
  ok.

verify_coordinator(Coordinator) ->
  case classy_node:node_of_site(Coordinator, true) of
    {ok, _} ->
      ok;
    _ ->
      {errror, coordinator_unreachable}
  end.

-spec db_establish(stage(), boolean(), non_neg_integer(), #prepare{}) -> {ok, d()} | {error, _}.
db_establish(Stage, Vote, CompletedActions, Prep) ->
  #prepare{id = ID, tag = Tag} = Prep,
  DataKey = #pk_pd{tag = Tag, id = ID},
  StateKey = #pk_ps{id = ID},
  State = #ps_ps{stage = Stage, vote = b2i(Vote), completed_actions = CompletedActions},
  maybe
    {ok, _} ?= classy_table:atomically(
                 ?ptab,
                 [ {w, DataKey, Prep}
                 , {w, StateKey, State}
                 ]),
    {ok, #d{ prep = Prep
           , vote = Vote
           , completed_actions = CompletedActions
           }}
  end.

-spec db_teardown(d()) -> ok | {error, _}.
db_teardown(#d{prep = #prepare{id = ID, tag = Tag}}) ->
  DataKey = #pk_pd{tag = Tag, id = ID},
  StateKey = #pk_ps{id = ID},
  maybe
    {ok, _} ?= classy_table:atomically(
                 ?ptab,
                 [ {d, DataKey}
                 , {d, StateKey}
                 ]),
    ?tp(debug, ?classy_vote_part_flow_complete,
        #{ id => ID
         , tag => Tag
         }),
    ok
  end.

-spec db_update(stage(), boolean(), non_neg_integer(), #d{}) -> {ok, d()} | {error, _}.
db_update(Stage, Vote, CompletedActions, D0) ->
  #d{prep = #prepare{id = ID}} = D0,
  StateKey = #pk_ps{id = ID},
  State = #ps_ps{stage = Stage, vote = b2i(Vote), completed_actions = CompletedActions},
  maybe
    ok ?= classy_table:write(?ptab, StateKey, State),
    {ok, D0#d{ vote = Vote
             , completed_actions = CompletedActions
             }}
  end.

-spec db_restore(classy_vote:tag(), classy_vote:id()) -> {ok, stage(), d()} | undefined.
db_restore(Tag, Id) ->
  maybe
    {ok, Prepare} ?= get_prepare(Tag, Id),
    [ #ps_ps{ stage = Stage
            , vote = VoteI
            , completed_actions = CompletedActions
            }
    ] = classy_table:lookup(?ptab, #pk_ps{id = Id}),
    D = #d{ prep = Prepare
          , vote = i2b(VoteI)
          , completed_actions = CompletedActions
          },
    {ok, Stage, D}
  end.

-spec get_prepare(classy_vote:tag(), classy_vote:id()) -> {ok, #prepare{}} | undefined.
get_prepare(Tag, Id) ->
  case classy_table:lookup(?ptab, #pk_pd{tag = Tag, id = Id}) of
    [Prepare] when is_record(Prepare, prepare) ->
      {ok, Prepare};
    [] ->
      undefined
  end.

nthtail(NComplete, Actions) ->
  lists:nthtail(
    min(NComplete, length(Actions)),
    Actions).

mk_timer(After) ->
  {state_timeout, After, ?state_timeout}.

-spec b2i(boolean() | 0..1) -> 0..1.
b2i(false) -> 0;
b2i(true)  -> 1.

-spec i2b(boolean() | 0..1) -> boolean().
i2b(0) -> false;
i2b(1) -> true.
