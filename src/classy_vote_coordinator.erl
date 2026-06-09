%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @private
-module(classy_vote_coordinator).

-behavior(gen_statem).

%% API:
-export([ new/1
        , restore/0
        ]).

%% Behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

%% internal exports:
-export([ start_link/2
        , receive_vote/1
        ]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy.hrl").
-include("classy_vote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-record(act,
        { site_bit :: non_neg_integer()
        , prepare  :: classy_vote:mfargs()
        , commit   :: [classy_vote:mfargs()]
        , rollback :: [classy_vote:mfargs()]
        , reserved = []
        }).

-define(s_vote, 0).
-define(s_commit, 10).
-define(s_rollback, 20).
-define(s_done, 30).
-type commit_stage() :: ?s_vote | ?s_commit | ?s_rollback | ?s_done.

-type remaining() :: non_neg_integer().

%% Persistent state:
%%   Keys:
%%     Data key (opts):
-record(pk_cd,
        { tag :: classy_vote:tag()
        , id :: classy_vote:id() | atom()
        }).
%%     State key (stage + remaining replies):
-record(pk_cs,
        { id :: classy_vote:id() | '_'
        }).
%% Coordinator dynamic state:
-record(ps_c,
        { stage :: commit_stage()
          %% Bit field where 1s represent sites have to ack commit or rollback.
          %% Bit position is determined by `n' field of `action()'
          %% (set automatically).
        , remaining :: remaining()
        , reserved = []
        }).

%% This data is stored persistently:
-record(opts,
        { tag           :: classy_vote:tag() %% TODO: don't duplicate the tag?
        , lock          :: classy_vote:lock()
        , id            :: classy_vote:id()
        , strategy      :: classy_vote:strategy()
        , actions       :: #{classy:site() => #act{}}
        , post_vote     :: [classy_vote:mfargs()]
        , start_time    :: integer()
        , reserved = []
        }).
-record(d_coord,
        { opts :: #opts{}
        , remaining :: remaining()
        }).
-type d_coord() :: #d_coord{}.

%%================================================================================
%% API functions
%%================================================================================

-spec new(classy_vote:options()) -> {ok, pid()} | {error, _}.
new(Options) ->
  classy_sup:ensure_vote_coordinator([true, Options]).

%%================================================================================
%% Internal exports
%%================================================================================

%% @private
-spec start_link(true, map()) -> gen_statem:start_ret();
                (false, {classy_vote:tag(), classy_vote:id(), #opts{}}) -> gen_statem:start_ret().
start_link(true, Options) ->
  %% Create a new vote:
  ID = classy_uid:cluster_unique_seq_tuple(classy_vote_sequence),
  gen_statem:start_link(
    ?via(?coordinator(ID)),
    ?MODULE,
    [true, ID, Options],
    []);
start_link(false, {_Tag, ID, _Opts} = StartOpts) ->
  gen_statem:start_link(
    ?via(?coordinator(ID)),
    ?MODULE,
    [false, StartOpts],
    []).

%% @private Coordinator <- Participant
-spec receive_vote(classy_vote:vote()) -> ok | {error, _}.
receive_vote(#c_vote{id = Id} = Vote) ->
  gen_statem:call(
    ?via(?coordinator(#pk_cs{id = Id})),
    Vote).

%% @private Restore votes that were ongoing before the node shut down
restore() ->
  MS = {#classy_kv{k = #pk_cd{tag = '$1', id = '$1'}, v = '$3'}, [], [{{'$1', '$2', '$3'}}]},
  Ongoing = ets:select(?ptab, [MS]),
  lists:foreach(
    fun({_, _, _} = StartArgs) ->
        classy_sup:ensure_vote_coordinator([false, StartArgs])
    end,
    Ongoing).

%%================================================================================
%% behavior callbacks
%%================================================================================

%% @private
callback_mode() ->
  [handle_event_function, state_enter].

%% @private
init([true, ID, Options]) ->
  process_flag(trap_exit, true),
  init_new_coordinator(ID, Options);
init([false, {Tag, ID, Options}]) ->
  process_flag(trap_exit, true),
  restore_coordinator(Tag, ID, Options).

%% @private
%% Coodinator:
handle_event(enter, OldStage, Stage, D) ->
  enter(OldStage, Stage, D);
handle_event(call, #c_vote{from = From, vote = Vote}, Stage, D) ->
  handle_vote(From, Vote, Stage, D);
handle_event(state_timeout, ?state_timeout, Stage, D) ->
  state_timeout(Stage, D);
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

-spec init_new_coordinator(classy_vote:id(), map()) ->
        {ok, commit_stage(), d_coord()} | {error, _}.
init_new_coordinator(ID, Options) ->
  #{ tag := Tag
   , actions := Actions0
   , strategy := Strategy
   , post_vote := PostVote
   , lock := Lock
   } = Options,
  Actions = maps:fold(
              fun(Site, Action, {SiteBitAcc, Acc}) ->
                  #{prepare := Prep, commit := Comm, rollback := Rollback} = Action,
                  Rec = #act{ site_bit = SiteBitAcc
                            , prepare = Prep
                            , commit = Comm
                            , rollback = Rollback
                            },
                  {SiteBitAcc + 1, Acc#{Site => Rec}}
              end,
              {0, #{}},
              Actions0),
  Opts = #opts{ tag = Tag
              , lock = Lock
              , id = ID
              , strategy = Strategy
              , post_vote = PostVote
              , actions = Actions
              , start_time = os:system_time(millisecond)
              },
  D = #d_coord{opts = Opts, remaining = 0},
  %% This is a new election.
  %% Perform a pre-vote immediately,
  %% and return pre-vote result to the requester synchronously.
  %% This way the fail path doesn't need to write anything on disk,
  %% as the caller is blocked before it makes any changes that should be rolled back,
  %% should the coordinator fail or node restart during pre-vote.
  %% Perform a preliminary check:
  {all, Timeout} = Strategy,
  Args = prepare_multi(participant_pre_vote, D),
  PreVoteResults = classy_lib:multicall(Args, Timeout),
  ?tp(debug, ?classy_vote_pre_results,
      #{ id      => ID
       , tag     => Tag
       , lock    => Lock
       , results => PreVoteResults
       }),
  case decide_pre_vote_result(Strategy, maps:iterator(PreVoteResults)) of
    true ->
      %% Now persist options (todo: do it atomically with creating the state?):
      Remaining = ones(n_participants(D)),
      ok = db_establish(?s_vote, Remaining, D),
      {ok, ?s_vote, D#d_coord{remaining = Remaining}};
    false ->
      {error, false}
  end.

-spec restore_coordinator(classy_vote:tag(), classy_vote:id(), #opts{}) -> {ok, commit_stage(), d_coord()}.
restore_coordinator(_Tag, Id, Opts) ->
  [#ps_c{stage = Stage, remaining = Remaining}] = classy_table:lookup(?ptab, #pk_cs{id = Id}),
  D0 = #d_coord{opts = Opts, remaining = Remaining},
  case Stage of
    ?s_vote ->
      %% If voting was aborted, we just rollback:
      D = db_set_coord_state(?s_rollback, ones(n_participants(D0)), D0),
      {ok, ?s_rollback, D};
    Other ->
      {ok, Other, D0}
  end.

-spec enter(commit_stage(), commit_stage(), d_coord()) ->
        {keep_state_and_data, gen_statem:action()} |
        {keep_state, d_coord(), gen_statem:action()}.
enter(OldStage, Stage, D) ->
  #d_coord{opts = #opts{id = ID}} = D,
  ?tp(debug, ?classy_vote_coord_stage, #{id => ID, to => Stage, from => OldStage}),
  case Stage of
    ?s_vote ->
      perform_vote(D);
    _ when Stage =:= ?s_commit; Stage =:= ?s_rollback ->
      Remaining = ones(n_participants(D)),
      db_set_coord_state(Stage, Remaining, D),
      {keep_state, D#d_coord{remaining = Remaining}, mk_timeout(0)}
  end.

-spec perform_vote(d_coord()) -> {keep_state_and_data, gen_statem:action()}.
perform_vote(D = #d_coord{opts = Opts}) ->
  #opts{strategy = Strategy} = Opts,
  {all, Timeout} = Strategy,
  Args = prepare_multi(start_participant, D),
  _ = classy_lib:multicall(Args, Timeout),
  {keep_state_and_data, mk_timeout(Timeout)}.

-spec handle_vote(classy:site(), boolean(), #ps_c{}, d_coord()) ->
        {next_state, #ps_c{}, d_coord()} |
        {keep_state, d_coord()}.
handle_vote(
  From,
  Vote,
  Stage,
  #d_coord{opts = Opts, remaining = Rem0} = D0
 ) ->
  #opts{id = Id} = Opts,
  ?tp(debug, ?classy_vote_coord_recv,
      #{ id => Id
       , from => From
       , vote => Vote
       , stage => Stage
       }),
  %% FIXME:
  SiteNr = 0,
  NParticipants = n_participants(D0),
  Rem = unset_bit(SiteNr, Rem0),
  case Stage of
    ?s_vote ->
      case Vote of
        true ->
          case Rem of
            0 ->
              D = db_set_coord_state(?s_commit, ones(NParticipants), D0),
              {next_state, ?s_commit, D};
            _ ->
              D = db_set_coord_state(?s_vote, ones(NParticipants), D0),
              {keep_state, D}
          end;
        false ->
          D = db_set_coord_state(?s_rollback, ones(NParticipants), D0),
          {next_state, ?s_rollback, D}
      end;
    _ ->
      %% Late vote; irrelevant
      {keep_state, D0}
  end.

-spec state_timeout(commit_stage(), d_coord()) ->
        {next_state, commit_stage(), d_coord()} |
        {keep_state, d_coord(), gen_statem:action()}.
state_timeout(?s_vote, D0) ->
  %% Vote timed out, move to rollback:
  D = db_set_coord_state(?s_rollback, ones(n_participants(D0)), D0),
  {next_state, ?s_rollback, D};
state_timeout(Stage, D0) ->
  Outcome = case Stage of
              ?s_commit -> true;
              ?s_rollback -> false
            end,
  Remaining = broadcast_outcome(Outcome, D0),
  D = db_set_coord_state(?s_commit, Remaining, D0),
  case Remaining > 0 of
    true ->
      {keep_state, D, mk_timeout(classy_vote:retry_interval())};
    false ->
      perform_post_commit(Outcome, D)
  end.

-spec perform_post_commit(boolean(), d_coord()) -> {stop, normal}.
perform_post_commit(Outcome, D = #d_coord{opts = Opts}) ->
  #opts{post_vote = PostActions} = Opts,
  lists:foreach(
    fun({M, F, Args}) ->
        try apply(M, F, [Outcome | Args])
        catch
          EC:Err:Stack ->
            ?tp(error, classy_vote_post_vote_callback_error,
                #{ EC => Err
                 , mfa => {M, F, length(Args) + 1}
                 , stack => Stack
                 })
        end
    end,
    PostActions),
  ok = db_teardown(D),
  {stop, normal}.

broadcast_outcome(Result, #d_coord{remaining = Remaining0, opts = Options}) ->
  #opts{id = Id, tag = Tag, actions = Acts} = Options,
  Sites = classy:sites(),
  Call = {classy_vote_participant, receive_outcome,
          [ #c_outcome{ id = Id
                      , tag = Tag
                      , result = Result
                      }
          ]},
  %% Collect target sites:
  {Remaining, Multicall} =
    maps:fold(
      fun(Site, #act{site_bit = Bit}, {Remaining1, Acc}) ->
          case is_bit_set(Bit, Remaining1) andalso lists:member(Site, Sites) of
            false ->
              %% Either we've already collected reply from the site,
              %% or the site got kicked:
              {unset_bit(Bit, Remaining1), Acc};
            true ->
              %% Site needs reply:
              {Remaining1, Acc#{{Site, Bit} => Call}}
            end
      end,
      {Remaining0, #{}},
      Acts),
  %% Broadcast:
  Results = classy_lib:multicall(Multicall, classy_lib:rpc_timeout()),
  ?tp(debug, classy_vote_broadcast_outcome,
      #{ id => Id
       , tag => Tag
       , result => Results
       }),
  %% Remove sites that acked the result:
  maps:fold(
    fun({_Site, Bit}, Reply, Remaining3) ->
        case Reply of
          {ok, ack} ->
            unset_bit(Bit, Remaining3);
          _ ->
            Remaining3
        end
    end,
    Remaining,
    Results).

-spec prepare_multi(atom(), d_coord()) -> map().
prepare_multi(Function, D = #d_coord{opts = #opts{actions = Acts}}) ->
  #{Site => {classy_vote_participant, Function, [prepare(D, Act)]} ||
    Site := Act <- Acts}.

-spec prepare(d_coord(), #act{}) -> #prepare{}.
prepare(
  #d_coord{opts = #opts{id = Id, tag = Tag, lock = Lock}},
  #act{prepare = Prep, commit = Commit, rollback = Rollback}
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

-spec decide_pre_vote_result(classy_vote:strategy(), maps:iterator()) -> boolean().
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
%% Database access
%%--------------------------------------------------------------------------------

-spec db_set_coord_state(commit_stage(), remaining(), #d_coord{}) -> #d_coord{}.
db_set_coord_state(Stage, Remaining, D = #d_coord{opts = Opts}) ->
  #opts{id = Id} = Opts,
  ok = classy_table:write(
         ?ptab,
         #pk_cs{id = Id},
         #ps_c{stage = Stage, remaining = Remaining}),
  D#d_coord{remaining = Remaining}.

%% Write information about the vote to the DB atomically.
-spec db_establish(commit_stage(), remaining(), #d_coord{}) -> ok.
db_establish(Stage, Remaining, #d_coord{opts = Opts}) ->
  #opts{tag = Tag, id = Id} = Opts,
  StateKey = #pk_cs{id = Id},
  StaticDataKey = #pk_cd{tag = Tag, id = Id},
  {ok, _} = classy_table:atomically(
              ?ptab,
              [ {w, StateKey, #ps_c{stage = Stage, remaining = Remaining}}
              , {w, StaticDataKey, Opts}
              ]),
  ok.

%% Atomically delete information about the vote from the DB.
-spec db_teardown(d_coord()) -> ok.
db_teardown(#d_coord{opts = #opts{id = Id, tag = Tag}}) ->
  StateKey = #pk_cs{id = Id},
  StaticDataKey = #pk_cd{tag = Tag, id = Id},
  {ok, _} = classy_table:atomically(
              ?ptab,
              [ {d, StateKey}
              , {d, StaticDataKey}
              ]),
  ok.

-spec ones(pos_integer()) -> pos_integer().
ones(N) ->
  1 bsl N - 1.

-spec unset_bit(non_neg_integer(), non_neg_integer()) -> non_neg_integer().
unset_bit(Bit, Bitfield) ->
  Bitfield band bnot (1 bsl Bit).

-spec is_bit_set(non_neg_integer(), non_neg_integer()) -> boolean().
is_bit_set(Bit, Bitfield) ->
  (Bitfield band (1 bsl Bit)) > 0.

-spec n_participants(d_coord()) -> pos_integer().
n_participants(#d_coord{opts = Opts}) ->
  maps:size(Opts#opts.actions).

mk_timeout(N) ->
  {state_timeout, N, ?state_timeout}.

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

is_bit_set_test() ->
  N1 = ones(2),
  ?assert(is_bit_set(0, N1)),
  ?assert(is_bit_set(1, N1)),
  ?assertNot(is_bit_set(2, N1)).

-endif.
