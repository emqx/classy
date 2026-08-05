%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_rl_changer).
-moduledoc false.

-behavior(gen_server).

%% API:
-export([to_int/1, to_atom/1, at_lower_level/2, get/1, get_int/1]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/0, set/1, set_sync/2, enrich_site_info/1]).

-export_type([run_level_int/0]).

-include("classy_internal.hrl").

-compile({no_auto_import, [get/1]}).

%%================================================================================
%% Type declarations
%%================================================================================

-define(ctr_c, 1).
-define(ctr_s, 2).

-define(SERVER, ?MODULE).

-define(valid_level(LEVEL), ((LEVEL) =:= ?stopped orelse (LEVEL) =:= ?single orelse (LEVEL) =:= ?cluster orelse (LEVEL) =:= ?quorum)).

-type run_level_int() :: 0..3.

-record(call_set, {level :: classy:run_level()}).

-record(call_at_run_level,
        { level :: classy:run_level()
        , function :: fun(() -> _)
        }).

-record(call,
        { at :: run_level_int()
        , f :: fun(() -> _)
        }).

-record(running,
        { next :: run_level_int()
        , pid :: pid()
        }).

-define(pterm, classy_run_level_ctr).

%%================================================================================
%% API functions
%%================================================================================

-spec to_int(classy:run_level()) -> run_level_int().
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
-spec at_lower_level(classy:run_level(), fun(() -> any())) -> ok | {error, _}.
at_lower_level(RunLevel, Fun) ->
  gen_server:call(
    ?SERVER,
    #call_at_run_level{level = RunLevel, function = Fun},
    infinity).

-doc false.
-spec get(current | set) -> classy:run_level().
get(K) ->
  to_atom(get_int(K)).

-doc false.
-spec get_int(current | set) -> run_level_int().
get_int(K) ->
  try
    Cntr = persistent_term:get(?pterm),
    Idx = case K of
            set     -> ?ctr_s;
            current -> ?ctr_c
          end,
    atomics:get(Cntr, Idx)
  catch _:_ ->
      0
  end.

-doc false.
-spec enrich_site_info(classy:site_metadata()) -> classy:site_metadata().
enrich_site_info(Info) ->
  Info#{rl => get(current)}.

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [self()], []).

-spec set(classy:run_level()) -> ok.
set(RunLevel) ->
  gen_server:call(
    ?SERVER,
    #call_set{level = RunLevel},
    infinity).

%% Warning: this function only works for _lowering_ the layer.
-spec set_sync(classy:run_level(), timeout()) -> ok | {error, timeout}.
set_sync(RunLevel, Timeout) ->
  set(RunLevel),
  Parent = erlang:alias([reply]),
  Ref = make_ref(),
  at_lower_level(RunLevel, fun() -> Parent ! Ref end),
  receive
    Ref -> ok
  after Timeout ->
      unalias(Parent),
      {error, timeout}
  end.

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { set = 0 :: run_level_int()
        , current = 0 :: run_level_int()
        , running :: #running{} | undefined
        , actions = [] :: [#call{}]
        , counter :: atomics:atomics_ref()
        }).

init(_) ->
  process_flag(trap_exit, true),
  Ctr = atomics:new(2, []),
  persistent_term:put(?pterm, Ctr),
  {ok, #s{counter = Ctr}}.

handle_call(#call_set{level = Level}, _From, S0) ->
  if ?valid_level(Level) ->
      S = maybe_transition(S0#s{set = to_int(Level)}),
      {reply, ok, S};
     true ->
      {reply, {error, badarg}, S0}
  end;
handle_call(#call_at_run_level{level = Level, function = Fun}, _From, #s{actions = AA} = S0) ->
  if ?valid_level(Level), is_function(Fun, 0) ->
      New = #call{ at = to_int(Level)
                 , f  = Fun
                 },
      S = maybe_transition(S0#s{actions = [New | AA]}),
      {reply, ok, S};
     true ->
      {reply, {error, badarg}, S0}
  end;
handle_call(Call, From, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => call
       , from => From
       , content => Call
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

handle_info({'EXIT', Pid, Reason}, #s{running = #running{pid = Pid, next = Next}} = S0) ->
  S = S0#s{ running = undefined
          , current = Next
          },
  case Reason of
    normal ->
      ok;
    _ ->
      ?tp(error, ?classy_rl_changer_worker_crash, #{pid => Pid, reason => Reason, to => Next}),
      %% There is a chance that the worker crashed before updating the
      %% counter. Do it by ourselves:
      update_counter(?ctr_c, Next)
  end,
  {noreply, maybe_transition(S)};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

terminate(Reason, S) ->
  classy_lib:is_normal_exit(Reason) orelse
    ?tp(warning, ?classy_abnormal_exit,
        #{ server => ?MODULE
         , reason => Reason
         }),
  terminate_loop(maybe_transition(S#s{set = 0, actions = []})),
  persistent_term:erase(?pterm).

%%================================================================================
%% Internal functions
%%================================================================================

terminate_loop(#s{current = 0, running = undefined}) ->
  ok;
terminate_loop(#s{running = #running{next = Next, pid = Pid}} = S0) ->
  receive
    {'EXIT', Pid, _} ->
      S = S0#s{ running = undefined
              , current = Next
              },
      terminate_loop(maybe_transition(S))
  end.

-spec maybe_transition(#s{}) -> #s{}.
maybe_transition(#s{running = #running{}} = S) ->
  update_counter(?ctr_s, S#s.set),
  S;
maybe_transition(#s{actions = AA0, set = Set, current = From, running = undefined} = S0) ->
  update_counter(?ctr_s, Set),
  To = lists:foldl(
         fun(#call{at = At}, Acc) -> min(At, Acc) end,
         Set,
         AA0),
  Next = if To > From ->
             From + 1;
            To < From ->
             From - 1;
            To =:= From ->
             From
         end,
  {ExecNow, AA} =
    lists:partition(
      fun(#call{at = L}) -> L >= Next end,
      AA0),
  case ExecNow of
    [] when Next =:= From ->
      %% Nothing to do:
      S0;
    _ ->
      %% Start transition:
      Running = run_hooks(From, Next, ExecNow),
      S0#s{running = Running, actions = AA}
  end.

run_hooks(From, Next, Actions) ->
  FromA = to_atom(From),
  NextA = to_atom(Next),
  Worker = spawn_link(
             fun() ->
                 %% Run hooks:
                 if Next > From ->
                     classy_hook:foreach(?on_change_run_level, [FromA, NextA]);
                    From > Next ->
                     classy_hook:foreach_rev(?on_change_run_level, [FromA, NextA]);
                    true ->
                     ok
                 end,
                 %% All hooks RL changing have completed. Update the current run level:
                 update_counter(?ctr_c, Next),
                 %% Run actions scheduled by `at_lower_level':
                 lists:foreach(
                   fun(#call{f = Fun}) ->
                       try
                         Fun()
                       catch
                         EC:Err:Stack ->
                           ?tp(critical, ?classy_run_level_change_error,
                               #{ call => Fun
                                , EC => Err
                                , stack => Stack
                                })
                       end
                   end,
                   Actions)
             end),
  #running{ next = Next
          , pid = Worker
          }.

update_counter(Idx, Val) ->
  atomics:put(persistent_term:get(?pterm), Idx, Val).
