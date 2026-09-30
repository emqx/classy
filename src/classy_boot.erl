%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_boot).
-moduledoc """
This module controls the boot sequence of business applications managed by classy.
It is based on a concept of @emph{run levels} and @emph{barriers}.

@emph{Run level} is an integer in range @code{?classy_rl_stopped .. ?classy_rl_ready}
(where @code{classy_rl_stopped = 0} and @code{classy_rl_ready = 300}),
corresponding to the ``readiness state'' of the system.
Run levels 0..9 are reserved for classy,
which leaves business applications with 290 usable run levels.

Unless the whole BEAM VM or classy application is abruptly stopped,
the run level is stepped in an increasing or decreasing arithmetic progression.
When @erlfn{ref,erlref,classy,start_system,0} function is called for the first time,
the system starts at stopped run level (0) and eventually progresses to the ready run level (300).
Effect of @erlfn{ref,erlref,classy,stop_system,0} is the opposite.

@emph{Barriers} can be set to temporarily limit the progression of the run level.
When a barrier is set at run level @code{N},
the system drops level to N
(if it was at level @code{M > N}, then it will do so by going through @code{M, M - 1, ..., N} sequence)
and stays there until the barrier is removed.
If there are multiple barriers,
system waits for the one set at the lowest run level.

The general idea is that initialization of the business logic can be represented
as a directed acyclic dependency graph,
and each topological level of the DAG can be mapped to a certain run level.
Business applications can hook initialization and de-initialization logic to each level using @erlfn{ref,erlref,classy,on_run_level,2} function.
Initialization hooks can set up barriers that are removed asynchronously
(when some subsystem becomes ready).
This approach allows to avoid blocking calls that wait for the readiness condition,
which can be problematic when the system has to stop or restart before fully ready.

The run levels are split into several ranges:

@enumerate
  @item @b{stopped} @code{0..9}.
  These are reserved for classy's own initialization logic.
  Business applications must not use them.

  @item @b{single} @code{?classy_rl_single..99} where @code{?classy_rl_single = 10}.
  These run levels correspond to initialization of a singleton node.

  @item @b{cluster} @code{?classy_rl_cluster..199} where @code{?classy_rl_cluster = 100}.
  Boot sequence progresses to this stage when the number of known peers (up or down) is @code{>= @ref{n_sites}}.

  @item @b{quorum} @code{?classy_rl_quorum..299} where @code{?classy_rl_quorum = 200}.
  Boot sequence progresses to this stage when the number of known @emph{connected} peers is @code{>= @ref{quorum}}.

  @item @b{ready} @code{?classy_rl_ready = 300}.
  Final run level.
  The system is fully operational.
@end enumerate

WARNING: classy @b{doesn't check} that the boot dependency graph is acyclic and that mapping to run levels is valid,
leaving this responsibility to the system designer.
There are no safety checks for the barrier levels:
setting them improperly can lead to hung boot.
The developer can use @erlfn{ref,erlref,classy_boot,diagnostics,1} function to troubleshoot the boot state.
""".

-behavior(gen_server).

%% API:
-export([at_lower_level/2, run_level/1, set_barrier/3, rm_barrier/1, classify/1, diagnostics/1]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/0, ensure_started/0, stop_system/0, enrich_site_info/1, do_at_lower_level/3]).

-export_type([barrier_id/0, description/0]).

-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(barrier_key(LEVEL, ID), {LEVEL, {ID}}).

-define(ctr_c, 1).
-define(ctr_n, 2).
-define(ctr_t, 3).

-define(SERVER, ?MODULE).
-define(hook_runner, classy_boot_hook_worker).

-record(call_start, {}).
-record(call_stop, {}).

-record(call_set_barrier,
        { id :: barrier_id()
        , sync :: boolean()
        , level :: classy:run_level()
        , monitor :: pid() | undefined
        , description :: term() | undefined
        }).

-record(call_rm_barrier,
        { id :: barrier_id()
        }).

-define(diagnostic_timeout, diagnostic_timeout).

-record(running,
        { next :: classy:run_level()
        , pid :: pid()
        }).

-define(pterm, classy_run_level_ctr).


-doc """
Identifier of the run level barrier.
It should be legible,
since it can be logged and seen by the operator.
""".
-type barrier_id() :: term().

-doc """
Human-friendly description of the barrier that helps the operator to understand what blocks the boot sequence.
""".
-type description() :: binary() | undefined.

-doc """
Summary of the node's boot state.
""".
-type boot_info() ::
        #{ run_level := #{ current := classy:run_level()
                         , target  := classy:run_level()
                         }
         , blockers := [{classy:run_level(), barrier_id(), description()}]
         , running_hook := false | list()
         }.

-define(tab, classy_rl_barriers).

%%================================================================================
%% API functions
%%================================================================================

-doc """
This helper function can be used to lower the run level of the system to the given value and run the specified function.
It can be used to implement migrations that require business applications to be stopped.
It sets up a temporary barrier and removes it once @code{Fun} is complete.

WARNING: it guarantees that the run level will be @emph{at most} @code{RunLevel},
but it can be lower.
""".
-spec at_lower_level(classy:run_level(), fun(() -> Ret)) -> Ret.
at_lower_level(RunLevel, Fun) ->
  Result = proc_lib:start(
             ?MODULE, do_at_lower_level, [self(), RunLevel, Fun]),
  case Result of
    {ok, Ret} ->
      Ret;
    {error, EC, Err, Stack} ->
      erlang:raise(EC, Err, Stack)
  end.

-doc """
This function does the following:

@enumerate
@item Lowers the run level to the specified one (or less),
if the current run level was higher.
@item Return @code{ok} to the caller.
@item Prevents classy from advancing the run level until
@erlfn{ref,erlref,classy_boot,rm_barrier,1} is called with the same lock ID.
@end enumerate

If @code{async} option is present,
the function returns immediately without waiting for the level to be adjusted.

If @code{monitor} option is present,
the barrier is automatically removed when the process that set it terminates.

@code{@{hint, Hint@}} option allows to attach an arbitrary term
serving as a hint to the operator explaining what the boot is waiting for.

If the barrier with the same ID already existed,
its level and description are updated.

WARNING: with an exception of @code{monitor} option,
business logic is entirely responsible for removing the barriers.
""".
-spec set_barrier(barrier_id(), classy:run_level(), [Option]) -> ok | {error, deleted | badarg}
          when Option :: monitor | async | {hint, description()}.
set_barrier(LockId, RunLevel, Options) when ?valid_run_level(RunLevel) ->
  MaybePid = case lists:member(monitor, Options) of
               true  -> self();
               false -> undefined
             end,
  Sync = not lists:member(async, Options),
  case lists:keyfind(hint, 1, Options) of
    {hint, Hint} ->
      ok;
    false ->
      Hint = undefined
  end,
  gen_server:call(
    ?SERVER,
    #call_set_barrier{ id = LockId
                     , sync = Sync
                     , monitor = MaybePid
                     , level = RunLevel
                     , description = Hint
                     },
    infinity);
set_barrier(_, _, _) ->
  {error, badarg}.

-spec rm_barrier(barrier_id()) -> ok.
rm_barrier(LockId) ->
  gen_server:call(
    ?SERVER,
    #call_rm_barrier{id = LockId},
    infinity).

-doc """
Get run level.
Argument:

@itemize
@item @code{target} Target run level in accordance with all barriers.
If the system keeps running and new barriers are not set,
the system will eventually reach it.

@item @code{current} Current run level.
If the system is in the middle of transition
and @erlfn{ref,erlref,classy,on_run_level,2} hooks are running,
then this value is equal to the previous run level.

@item @code{next} Next run level.
If system is stable and no transition hooks are running,
its value is equal to the current one.

@end itemize
""".
-spec run_level(current | next | target) -> classy:run_level().
run_level(K) ->
  try
    Cntr = persistent_term:get(?pterm),
    Idx = case K of
            target  -> ?ctr_t;
            next    -> ?ctr_n;
            current -> ?ctr_c
          end,
    atomics:get(Cntr, Idx)
  catch _:_ ->
      0
  end.

-doc false.
-spec enrich_site_info(classy:site_metadata()) -> classy:site_metadata().
enrich_site_info(Info) ->
  Info#{rl => run_level(current)}.

-doc false.
-spec do_at_lower_level(pid(), classy:run_level(), fun(() -> any())) -> ok.
do_at_lower_level(Parent, Level, Fun) ->
  LockId = self(),
  %% FIXME: description should be present
  try
    ok = set_barrier(LockId, Level, [monitor]),
    Ret = Fun(),
    proc_lib:init_ack(Parent, {ok, Ret})
  catch
    EC:Err:Stack ->
      proc_lib:init_ack(Parent, {error, EC, Err, Stack})
  end.

-spec classify(classy:run_level()) -> stopped | single | cluster | quorum | ready.
classify(N) when is_integer(N), N >= 0 ->
  if N < ?classy_rl_single  -> stopped;
     N < ?classy_rl_cluster -> single;
     N < ?classy_rl_quorum  -> cluster;
     N < ?classy_rl_ready   -> quorum;
     true                   -> ready
  end.

-doc """
Return summary of information related to the node's boot state.
""".
-spec diagnostics(#{blocker_levels => pos_integer()}) -> boot_info().
diagnostics(Options) ->
  BlockerLevels = maps:get(blocker_levels, Options, 1),
  #{ run_level => #{ current => run_level(current)
                   , target  => run_level(target)
                   }
   , blockers => blockers(BlockerLevels)
   , running_hook => running_hook_info()
   }.

%%================================================================================
%% Internal exports
%%================================================================================

-doc false.
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [self()], []).

-doc false.
-spec stop_system() -> ok.
stop_system() ->
  gen_server:call(?SERVER, #call_stop{}, infinity).

-doc false.
-spec ensure_started() -> ok.
ensure_started() ->
  gen_server:call(?SERVER, #call_start{}, infinity).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(barrier,
        { k :: {classy:run_level() | atom(), {barrier_id()}}
        , mref :: reference() | atom()
        , description :: description() | atom()
        , reply_to :: gen_server:from() | atom()
        }).

-record(s,
        { started = false :: boolean()
          %% Maximum run level that the system naturally gravitates to.
        , max = 0 :: classy:run_level()
          %% Run leavel that has been currently reached:
        , current = 0 :: classy:run_level()
          %% Information about currently running transition hooks
        , running :: #running{} | undefined
        , counter :: atomics:atomics_ref()
        }).

-doc false.
init(_) ->
  process_flag(trap_exit, true),
  Ctr = atomics:new(3, []),
  persistent_term:put(?pterm, Ctr),
  ets:new(?tab, [protected, ordered_set, named_table, {keypos, #barrier.k}]),
  set_diag_timer(),
  {ok, #s{counter = Ctr}}.

-doc false.
handle_call(#call_start{}, _From, #s{started = Started} = S0) ->
  S = case Started of
        true  -> S0;
        false -> maybe_transition(S0#s{started = true, max = ?classy_rl_ready})
      end,
  {reply, ok, S};
handle_call(#call_stop{}, _From, S) ->
  {reply, ok, do_stop_system(S)};
handle_call(#call_set_barrier{} = Call, From, S) ->
  {noreply, handle_set_barrier(Call, From, S)};
handle_call(#call_rm_barrier{id = Id}, _From, S0) ->
  S = rm_barrier(by_id, Id, S0),
  {reply, ok, S};
handle_call(Call, From, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => call
       , from => From
       , content => Call
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

-doc false.
handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

-doc false.
handle_info({'EXIT', Pid, Reason}, #s{running = #running{pid = Pid, next = Next}} = S0) ->
  %% Finished running run level transition hooks:
  S = enter_level(Next, Reason, S0),
  {noreply, S};
handle_info({'DOWN', MRef, process, _, _}, S0) ->
  S = rm_barrier(by_mref, MRef, S0),
  {noreply, S};
handle_info(?diagnostic_timeout, #s{} = S0) ->
  maybe_format_diagnostics(),
  set_diag_timer(),
  S = S0#s{},
  {noreply, S};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

-doc false.
terminate(Reason, S = #s{}) ->
  classy_lib:is_normal_exit(Reason) orelse
    ?tp(warning, ?classy_abnormal_exit,
        #{ server => ?MODULE
         , reason => Reason
         }),
  do_stop_system(S),
  persistent_term:erase(?pterm).

%%================================================================================
%% Internal functions
%%================================================================================

-spec enter_level(classy:run_level(), term(), #s{}) -> #s{}.
enter_level(Level, Reason, S0) ->
  S = S0#s{ running = undefined
          , current = Level
          },
  update_counter(?ctr_c, Level),
  finish_set_barriers(Level),
  Reason =:= normal orelse
    ?tp(error, ?classy_boot_worker_crash, #{reason => Reason, to => Level}),
  maybe_transition(S).

finish_set_barriers(Level) ->
  MS = { #barrier{k = ?barrier_key(Level, '_'), reply_to = '$1', _ = '_'}
       , [{'=/=', '$1', undefined}]
       , ['$_']
       },
  finish_set_barriers(Level, ets:select(?tab, [MS], ?fold_batch_size)).

finish_set_barriers(_Level, '$end_of_table') ->
  ok;
finish_set_barriers(Level, {Batch, Cont}) ->
  _ = [maybe_reply_setter(I, ok) || I <- Batch],
  finish_set_barriers(Level, ets:select(Cont)).

-spec maybe_reply_setter(#barrier{}, term()) -> ok.
maybe_reply_setter(#barrier{reply_to = undefined}, _) ->
  ok;
maybe_reply_setter(#barrier{k = K, reply_to = ReplyTo}, Reply) ->
  gen_server:reply(ReplyTo, Reply),
  ets:update_element(?tab, K, {#barrier.reply_to, undefined}),
  ok.

-spec maybe_demonitor(#barrier{}) -> ok.
maybe_demonitor(#barrier{mref = Ref}) when is_reference(Ref) ->
  demonitor(Ref),
  ok;
maybe_demonitor(_) ->
  ok.

-spec handle_set_barrier(#call_set_barrier{}, gen_server:from(), #s{}) -> #s{}.
handle_set_barrier(Call, From, S0) ->
  #call_set_barrier{ id          = Id
                   , sync        = Sync
                   , level       = Level
                   , monitor     = MaybeMonitor
                   , description = MaybeDescription
                   } = Call,
  maybe
    true ?= Id =/= undefined,
    true ?= ?valid_run_level(Level),
    true ?= is_pid(MaybeMonitor) orelse MaybeMonitor =:= undefined,
    PrevTarget = target(S0),
    do_rm_barrier(by_id, Id),
    S = maybe_transition(do_add_barrier(Sync, From, Id, Level, MaybeMonitor, MaybeDescription, S0)),
    NewTarget = target(S),
    case NewTarget < PrevTarget of
      true ->
        logger:warning(
          #{ msg => target_run_level_dropped
           , new => NewTarget
           , id => Id
           , reason => MaybeDescription
           });
      false ->
        ok
    end,
    S
  else
    _ ->
      gen_server:reply(From, {error, badarg}),
      S0
  end.

-spec do_add_barrier(boolean(), gen_server:from(), barrier_id(), classy:run_level(), pid() | undefined, binary() | undefined, #s{}) -> #s{}.
do_add_barrier(Sync, From, Id, Level, MaybeMonitor, MaybeDescription, S) ->
  #s{current = Current0, running = Running} = S,
  %% Monitor the process that sets the barrior if needed:
  MaybeMRef = case is_pid(MaybeMonitor) of
                true  -> monitor(process, MaybeMonitor);
                false -> undefined
              end,
  %% Should we reply now?
  Current = case Running of
              undefined             -> Current0;
              #running{next = Next} -> max(Current0, Next)
            end,
  ReplyTo = if not Sync; Current =< Level ->
                %% Either an async call or already at a low enough
                %% level. Reply to the caller immediately:
                gen_server:reply(From, ok),
                undefined;
               true ->
                From
            end,
  Barrier = #barrier{ k           = ?barrier_key(Level, Id)
                    , mref        = MaybeMRef
                    , description = MaybeDescription
                    , reply_to    = ReplyTo
                    },
  ets:insert(?tab, Barrier),
  S.

-spec rm_barrier(by_mref, reference(), #s{}) -> #s{};
                (by_id, barrier_id(), #s{}) -> #s{}.
rm_barrier(How, Key, S) ->
  do_rm_barrier(How, Key),
  maybe_transition(S).

-spec do_rm_barrier(by_mref, reference()) -> ok;
                   (by_id, barrier_id()) -> ok.
do_rm_barrier(How, Del) ->
  %% TODO: this is inefficient, but we don't expect to have many
  %% barriers.
  ets:foldl(
    fun(#barrier{k = ?barrier_key(_Level, Id) = Key, mref = MRef} = I, Acc) ->
        Keep = if How =:= by_mref, MRef =:= Del ->
                   maybe_reply_setter(I, {error, deleted}),
                   false;
                  How =:= by_id, Id =:= Del ->
                   maybe_reply_setter(I, {error, deleted}),
                   maybe_demonitor(I),
                   false;
                  true ->
                   true
               end,
        Keep orelse ets:delete(?tab, Key),
        Acc
    end,
    ok,
    ?tab).

do_stop_system(#s{started = Started} = S0) ->
  S1 = S0#s{max = 0, started = false},
  S = case Started of
        true  -> terminate_loop(maybe_transition(S1));
        false -> S1
      end,
  ets:foldl(fun(I, ok) ->
                maybe_reply_setter(I, {error, deleted}),
                maybe_demonitor(I)
            end,
            ok,
            ?tab),
  ets:match_delete(?tab, '_'),
  S.

terminate_loop(#s{current = 0, running = undefined} = S) ->
  S;
terminate_loop(#s{running = #running{next = Next, pid = Pid}} = S) ->
  receive
    {'EXIT', Pid, Reason} ->
      terminate_loop(enter_level(Next, Reason, S))
  end.

-spec maybe_transition(#s{}) -> #s{}.
maybe_transition(#s{running = #running{}} = S) ->
  %% Update global counter even when there's nothing to do now:
  _ = target(S),
  S;
maybe_transition(#s{running = undefined, current = From} = S) ->
  To = target(S),
  Next = if To > From ->
             From + 1;
            To < From ->
             From - 1;
            To =:= From ->
             From
         end,
  update_counter(?ctr_n, Next),
  if Next =:= From ->
      S;
     true ->
      Running = run_hooks(From, Next),
      S#s{running = Running}
  end.

run_hooks(From, Next) ->
  Worker = spawn_link(
             fun() ->
                 erlang:register(?hook_runner, self()),
                 %% Run hooks:
                 if Next > From ->
                     classy_hook:foreach(?on_change_run_level, [enter, Next]);
                    From > Next ->
                     classy_hook:foreach_rev(?on_change_run_level, [leave, From]);
                    true ->
                     ok
                 end
             end),
  #running{ next = Next
          , pid = Worker
          }.

target(#s{max = Max}) ->
  Target = calc_target(Max),
  update_counter(?ctr_t, Target),
  Target.

calc_target(Max) ->
  max(0,
      case ets:first(?tab) of
        '$end_of_table' -> Max;
        {Level, _}      -> min(Max, Level)
      end).

update_counter(Idx, Val) ->
  atomics:put(persistent_term:get(?pterm), Idx, Val).

%% List barriers at the lowest `NLevels' run levels.
-spec blockers(pos_integer()) -> [{classy:run_level(), barrier_id(), term()}].
blockers(NLevels) ->
  MS = { #barrier{ k           = ?barrier_key('$1', '$2')
                 , description = '$3'
                 , _           = '_'
                 }
       , []
       , [{{'$1', '$2', '$3'}}]
       },
  case ets:select(?tab, [MS], ?fold_batch_size) of
    '$end_of_table' ->
      [];
    {[{Level, _, _} | _] = L, Cont} ->
      do_fold_blockers(Level, NLevels, L, Cont)
  end.

do_fold_blockers(Level, NLevels, [], Cont0) ->
  case ets:select(Cont0) of
    '$end_of_table' -> [];
    {L, Cont}       -> do_fold_blockers(Level, NLevels, L, Cont)
  end;
do_fold_blockers(Level, NLevels, [{Level, _, _} = Elem | L], Cont) ->
  [Elem | do_fold_blockers(Level, NLevels, L, Cont)];
do_fold_blockers(_Level0, NLevels, [{Level, _, _} = Elem | L], Cont) when NLevels > 1 ->
  [Elem | do_fold_blockers(Level, NLevels - 1, L, Cont)];
do_fold_blockers(_, _, _, _) ->
  [].

maybe_format_diagnostics() ->
  #{ run_level := RL
   , blockers := Blockers
   , running_hook := HookState
   } = Info = diagnostics(#{}),
  case RL of
    #{current := ?classy_rl_ready} when HookState =:= false,
                                        Blockers =:= [] ->
      %% System is running normally. Nothing to report.
      ok;
    _ ->
      logger:notice(Info#{msg => 'system_boot_state'})
  end.

set_diag_timer() ->
  erlang:send_after(
    application:get_env(classy, boot_diagnostic_interval, 15_000),
    self(),
    ?diagnostic_timeout).

-spec running_hook_info() -> list() | false.
running_hook_info() ->
  maybe
    Pid = whereis(?hook_runner),
    true ?= is_pid(Pid),
    [{current_stacktrace, Stack}] ?= process_info(Pid, [current_stacktrace]),
    Stack
  else
    _ ->
      false
  end.
