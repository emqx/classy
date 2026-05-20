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
%% Classy doesn't add additional arguments.
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
%% Classy appends a boolean indicating result of the vote to the argument list.
%% Return value is ignored.
%% It's NOT guaranteed that all commit actions are finished by this time.
%% This callback can be retried on node restart.
%% </li>
%% </itemize>
-module(classy_vote).

-behavior(gen_statem).

%% API:
-export([ create/1
        , ls_votes/1
        , fold_votes/2
        ]).

%% Behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

%% internal exports:
-export([ create_table/0
        , start_link/1
        , do_nothing/1
        , restore/0
        ]).

-export_type([id/0]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy.hrl").
-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(ptab, classy_vote_table).

-type tag() :: term().
-type id() :: classy_uid:cu_tuple().

-define(coordinator(ID), {n, l, {classy_vote_coordinator, ID}}).
-define(participant(ID), {n, l, {classy_vote_participant, ID}}).
-define(via(NAME), {via, gproc, NAME}).

-type strategy() :: quorum.

%% Warning: MFA's are persistently stored!
-type options() ::
        #{ tag := tag()
         , prepare := mfa()
         , commit := [mfa()]
         , rollback => mfa()
         , post_vote => mfa()
         , sites := [classy:site()]
         , strategy => strategy()
         , timeout => pos_integer()
         }.

%% Persistent states:
%%    Keys:
-record(pk_c, {tag :: tag(), id :: id() | '_'}).          %% Coordinator key
-record(pk_p, {tag :: tag(), id :: id() | '_'}).          %% Participant key
%%    Coordinator:
-record(pv_c,
        { options :: options()
        , state :: _
        , votes :: [{classy:site(), boolean() | {error, _}}]
        }).
%%    Participant:
-record(pv_p_vote, {vote :: boolean(), reserved = []}).   %% Participant state; voted
-record(pv_p_comm, {actions :: [mfa()], reserved = []}).  %% Participant state; committing
-record(pv_p_rollback, {action :: mfa(), reserved = []}). %% Participant state; rolling back

%% FSM states:
-record(s_coord,
        { stage :: prep | rollback | commit
        }).
-record(s_part,
        { stage :: voted | reporting
        }).

%% Init args:
-record(init_coordinator,
        { id :: classy_uid:cu_tuple()
        , opts :: options()
        }).
-type init_args() :: #init_coordinator{}.

-type vote_info() :: {voted, #{vote => boolean()}}
                   | {committing, #{actions => [mfa()]}}
                   | {rollback, #{}}.

%%================================================================================
%% API functions
%%================================================================================

%% @doc List ongoing commit actions.
%%
%% Argument: match specification for the tag.
-spec ls_votes(_TagMatch) -> #{id() => vote_info()}.
ls_votes(TagMatch) ->
  fold_votes(
    TagMatch,
    fun(ID, Action, Acc) -> Acc#{ID => Action} end).

%% @doc Fold over ongoing commit actions.
%%
%% Argument: match specification for the tag.
-spec fold_votes(_TagMatch, fun((id(), vote_info(), Acc) -> Acc)) -> Acc.
fold_votes(TagMatch, Fun) ->
  BatchSize = 100,
  MS = { #classy_kv{k = #pk_p{tag = TagMatch, _ = '_'}, _ = '_'}
       , []
       , ['$_']
       },
  do_fold_votes(ets:select(?ptab, [MS], BatchSize), Fun, #{}).

%% @doc Initiate a new vote.
%%
%% Note: This function returns immediately.
-spec create(options()) -> ok | {error, _}.
create(UserOptions) ->
  maybe
    {ok, Options} ?= with_defaults(UserOptions),
    ID = classy_uid:cluster_unique_seq_tuple(classy_vote_sequence),
    {ok, _} ?= classy_sup:ensure_vote(
                 #init_coordinator{ id = ID
                                  , opts = Options
                                  }),
    ok
  end.

%% @doc Restore all previously scheduled actions
-spec restore() -> ok.
restore() ->
  %% TODO
  ok.

%%================================================================================
%% behavior callbacks
%%================================================================================

%% @private
callback_mode() ->
  [handle_event_function, state_enter].

%% @private
init(Args) ->
  process_flag(trap_exit, true),
  error(todo).

%% @private
handle_event(info, {'EXIT', _, Reason}, _, _) ->
  case Reason of
    normal -> keep_state_and_data;
    _      -> {stop, shutdown}
  end;
handle_event(ET, Event, State, Data) ->
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
%% Internal exports
%%================================================================================

-spec create_table() -> ok.
create_table() ->
  classy_table:open(
    ?ptab,
    #{ ets_options => [ordered_set, {read_concurrency, true}]
     }).

-spec start_link(init_args()) -> gen_statem:start_ret().
start_link(#init_coordinator{id = ID} = Args) ->
  gen_statem:start_link(?via(?coordinator(ID)), ?MODULE, Args, []).

-spec do_nothing(_) -> ok.
do_nothing(_Arg) ->
  ok.

%%================================================================================
%% Internal functions
%%================================================================================

do_fold_votes('$end_of_table', _Fun, Acc) ->
  Acc;
do_fold_votes({Matches, Continuation}, Fun, Acc0) ->
  Acc = lists:foldl(
          fun(#classy_kv{k = #pk_p{id = ID}, v = Status}, Acc1) ->
              Val = case Status of
                      #pv_p_vote{vote = Vote} ->
                        {voted, #{vote => Vote}};
                      #pv_p_comm{actions = Actions} ->
                        {committing, #{actions => Actions}};
                      #pv_p_rollback{} ->
                        {rollback, #{}}
                    end,
              Fun(ID, Val, Acc1)
          end,
          Acc0,
          Matches),
  do_fold_votes(ets:select(Continuation), Fun, Acc).

%%--------------------------------------------------------------------------------
%% Input validation
%%--------------------------------------------------------------------------------

-spec with_defaults(options()) -> {ok, options()} | {error, _}.
with_defaults(UserOpts) when is_map(UserOpts) ->
  Defaults = #{ rollback => undefined
              , post_vote => undefined
              , strategy => quorum
              , timeout => 5_000
              },
  Merged = maps:merge(Defaults, UserOpts),
  case Merged of
    #{ tag       := _
     , prepare   := Prepare
     , commit    := Commit
     , rollback  := Rollback
     , post_vote := PostVote
     , sites     := Sites0
     , strategy  := Strategy
     , timeout   := Timeout
     } ->
      maybe
        ok ?= verify_mfa(bad_prepare, 0, Prepare),
        ok ?= verify_commit(Commit),
        ok ?= verify_mfa(bad_rollback, 0, Rollback),
        ok ?= verify_post_vote(PostVote),
        {ok, Sites} ?= verify_sites(Sites0),
        true ?= Strategy =:= quorum orelse
          {error, {bad_strategy, Strategy}},
        true ?= (is_integer(Timeout) andalso Timeout > 0) orelse
          {error, {bad_timeout, Timeout}},
        {ok, Merged#{sites := Sites}}
      end;
    _ ->
      {error, badarg}
  end.

-spec verify_mfa(atom(), non_neg_integer(), term()) -> ok | {error, {atom(), term()}}.
verify_mfa(Subject, ExtraArgs, {M, F, Args}) when is_atom(M), is_atom(F), is_list(Args) ->
  NArgs = length(Args) + ExtraArgs,
  Err = {error, {Subject, {M, F, NArgs}}},
  try
    Exports = M:module_info(exports),
    case lists:member({F, NArgs}, Exports) of
      true  -> ok;
      false -> Err
    end
  catch
    _:_ ->
      Err
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
