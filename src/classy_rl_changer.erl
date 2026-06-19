%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_rl_changer).
-moduledoc false.

-behavior(gen_server).

%% API:
-export([to_int/1, to_atom/1, at_lower_level/2]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%% internal exports:
-export([start_link/0, set/1]).

-export_type([run_level_int/0]).

-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(SERVER, ?MODULE).

-type run_level_int() :: 0..3.

-record(cast_set, {rl :: run_level_int()}).

-record(call_at_run_level,
        { level :: classy:run_level()
        , function :: fun(() -> _)
        }).

%%================================================================================
%% API functions
%%================================================================================

-spec to_int(classy:run_level() | run_level_int()) -> run_level_int().
to_int(?stopped) -> 0;
to_int(?single)  -> 1;
to_int(?cluster) -> 2;
to_int(?quorum)  -> 3.

-spec to_atom(run_level_int()) -> classy:run_level().
to_atom(0) -> ?stopped;
to_atom(1) -> ?single;
to_atom(2) -> ?cluster;
to_atom(3) -> ?quorum.

-doc false.
-spec at_lower_level(classy:run_level(), fun(() -> Ret)) ->
        {ok, Ret} |
        {error | exit | throw, _Reason, _Stacktrace}.
at_lower_level(RunLevel, Fun) ->
  gen_server:call(
    ?SERVER,
    #call_at_run_level{level = RunLevel, function = Fun},
    infinity).

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec set(classy:run_level()) -> ok.
set(RunLevel) ->
  ?SERVER ! #cast_set{rl = to_int(RunLevel)},
  %% TODO: find a nicer way to correlate node and rl changer events:
  maybe_sleep(),
  ok.

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { set = 0 :: run_level_int()
        , current = 0 :: run_level_int()
        }).

init(_) ->
  S = #s{},
  {ok, S}.

handle_call(#call_at_run_level{level = RequestedRunLevel, function = Fun}, From, S0) ->
  #s{set = OldSet, current = OldCurrent} = S0,
  %% Retard the level:
  TmpLevel = min(OldSet, min(OldCurrent, to_int(RequestedRunLevel))),
  S1 = S0#s{set = TmpLevel},
  S2 = change_run_level(S1),
  %% Run function:
  Ret = try
          {ok, Fun()}
        catch
          EC:Err:Stack ->
            {EC, Err, Stack}
        end,
  gen_server:reply(From, Ret),
  %% Advance the level:
  S = change_run_level(S2#s{set = OldSet}),
  {noreply, S};
handle_call(_Call, _From, S) ->
  {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
  {noreply, S}.

handle_info(#cast_set{rl = NewRL0}, S0) ->
  NewRL = flush_sets(NewRL0),
  S = S0#s{set = NewRL},
  {noreply, change_run_level(S)};
handle_info(_Info, S) ->
  {noreply, S}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec change_run_level(#s{}) -> #s{}.
change_run_level(#s{current = Level, set = Level} = S) when is_integer(Level) ->
  S;
change_run_level(#s{current = From, set = To} = S) when To >= 0, To =< 3 ->
  Next = if To > From ->
             From + 1;
            To < From ->
             From - 1
         end,
  classy_hook:foreach(?on_change_run_level, [to_atom(From), to_atom(Next)]),
  change_run_level(S#s{current = Next}).

-spec flush_sets(run_level_int()) -> run_level_int().
flush_sets(RL0) ->
  receive
    #cast_set{rl = RL} ->
      flush_sets(RL)
  after 0 ->
      RL0
  end.

-ifndef(TEST).
maybe_sleep() ->
  ok.
-else.
maybe_sleep() ->
  %% Give RL changer time to emit events:
  timer:sleep(10).
-endif.
