%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(classy_hook).
-moduledoc """
Module responsible for managing the hooks.
""".

%% API:
-export([ init/0
        , insert/3
        , unhook/1
        , foreach/2
        , foreach_rev/2
        , map/2
        , fold/3
        , all/2
        , first_match/2
        , default_timeout/0
        ]).

-export_type([ hookpoint/0
             , prio/0
             , hook/0
             , conf/0
             ]).

-include("classy_internal.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================
-define(tab, ?MODULE).

-doc """
Identifier of a hookpoint.
""".
-type hookpoint() :: atom().

-doc """
Functions registered into a hookpoint with higher priority are executed first.
""".
-type prio() :: integer().

-type conf() :: prio() | #{prio := prio(), timeout => timeout()}.

-doc """
A handle of a hook.
It can be used to unregister the hook.
""".
-opaque hook() :: tuple().

%%================================================================================
%% API functions
%%================================================================================

-doc """
Get the configured hook timeout value (with defaults).
""".
default_timeout() ->
  application:get_env(classy, hook_timeout, 30_000).

-doc false.
init() ->
  ets:new(?tab, [named_table, ordered_set, public, {keypos, 1}]),
  %% Default initialization:
  classy:on_node_init(fun classy_builtin_hooks:gen_random_site_id/0, ?min_hook_prio),
  classy:on_leave(fun classy_builtin_hooks:maybe_reinitialize_after_leave/3, ?min_hook_prio),
  %% Liveness tracking:
  classy:run_level(fun classy_liveness:on_run_level/2, ?max_hook_prio),
  classy:on_peer_connection_change(fun classy_liveness:on_peer_connection_change/3, ?max_hook_prio),
  %% Cluster-global variables:
  classy:enrich_site_info(fun classy_builtin_hooks:enrich_site_info/1, ?min_hook_prio),
  classy:enrich_site_info(fun classy_rl_changer:enrich_site_info/1, ?min_hook_prio),
  %% Info logging:
  classy:on_create_site(fun classy_builtin_hooks:log_create_site/1, ?max_hook_prio),
  classy:on_create_cluster(fun classy_builtin_hooks:log_create_cluster/2, ?max_hook_prio),
  classy:pre_join(fun classy_builtin_hooks:log_pre_join/4, ?max_hook_prio),
  classy:post_join(fun classy_builtin_hooks:log_post_join/4, ?min_hook_prio),
  classy:on_membership_change(fun classy_builtin_hooks:log_membership_change/4, ?max_hook_prio),
  classy:run_level(fun classy_builtin_hooks:log_run_level/2, ?min_hook_prio),
  classy:on_peer_connection_change(fun classy_builtin_hooks:log_peer_connection_change/3, ?max_hook_prio),
  classy:on_peer_liveness_change(fun classy_builtin_hooks:log_peer_liveness_change/2, ?max_hook_prio),
  classy:on_peer_restart(fun classy_builtin_hooks:log_peer_restart/2, ?max_hook_prio),
  classy:on_peer_node_change(fun classy_builtin_hooks:log_peer_node_change/3, ?max_hook_prio),
  classy:pre_autoclean(fun classy_builtin_hooks:log_autoclean/1, ?max_hook_prio),
  %% Discovery strategies:
  classy_discovery_static:install_dispatch_hook(),
  classy_discovery_dns:install_dispatch_hook(),
  classy_discovery_k8s:install_dispatch_hook(),
  classy_discovery_etcd:install_dispatch_hook(),
  %% Restore aborted votes at the end of transition to the `cluster' RL:
  classy:run_level(fun classy_vote:on_run_level/2, ?min_hook_prio + 1),
  %% User initialization:
  case application:get_env(classy, setup_hooks) of
    {ok, {Mod, Func, Args}} ->
      apply(Mod, Func, Args),
      ok;
    undefined ->
      ok
  end.

-spec insert(hookpoint(), fun(), conf()) -> hook().
insert(Hookpoint, Hook, Prio) when is_integer(Prio) ->
  insert(Hookpoint, Hook, #{prio => Prio});
insert(Hookpoint, Hook, #{prio := Prio} = Conf) when is_atom(Hookpoint),
                                                     is_integer(Prio),
                                                     is_function(Hook) ->
  Key = {Hookpoint, -Prio, Hook},
  Timeout = case Conf of
              #{timeout := TO} when TO =:= infinity;
                                    is_integer(TO) andalso TO > 0 ->
                TO;
              #{timeout := TO} ->
                error({invalid_hook_timeout, Hookpoint, Hook, TO});
              #{} ->
                undefined
            end,
  ets:insert(?tab, {Key, Timeout}),
  Key.

-doc """
Remove a previously inserted hook.
""".
-spec unhook(hook()) -> ok.
unhook(Key) ->
  ets:delete(?tab, Key),
  ok.

-doc """
Apply all functions hooked into @code{Hookpoint} to arguments @code{Args}.

Errors are ignored (logged).
""".
-spec foreach(hookpoint(), list()) -> ok.
foreach(Hookpoint, Args) ->
  lists:foreach(
    fun({Hook, Timeout}) ->
        safe_apply(Hookpoint, Hook, Args, Timeout)
    end,
    hooks(Hookpoint)).

-doc """
Similar to @code{foreach/2},
but hooks run in reverse priority order.
""".
-spec foreach_rev(hookpoint(), list()) -> ok.
foreach_rev(Hookpoint, Args) ->
  lists:foreach(
    fun({Hook, Timeout}) ->
        safe_apply(Hookpoint, Hook, Args, Timeout)
    end,
    lists:reverse(hooks(Hookpoint))).

-doc """
Fold over all functions registered in @code{Hookpoint}.
Accumulator argument is appended to the @code{Args} list.

Errors are ignored (logged).
""".
-spec fold(hookpoint(), list(), A) -> A.
fold(Hookpoint, Args, Acc0) ->
  try
    lists:foldl(
      fun({Hook, Timeout}, Acc1) ->
          case safe_apply(Hookpoint, Hook, Args ++ [Acc1], Timeout) of
            {ok, Acc} ->
              Acc;
            error ->
              Acc1
          end
      end,
      Acc0,
      hooks(Hookpoint))
  catch
    {stop, Result} ->
      Result
  end.

-doc """
Apply every hook to the arguments
and return the list of outputs for each hook.

Failures are ignored (logged).
""".
-spec map(hookpoint(), list()) -> list().
map(Hookpoint, Args) ->
  lists:filtermap(
    fun({Hook, Timeout}) ->
        case safe_apply(Hookpoint, Hook, Args, Timeout) of
          {ok, Result} ->
            {true, Result};
          _ ->
            false
        end
    end,
    hooks(Hookpoint)).

-doc """
Ensure that all functions hooked into @code{Hookpoint} return @code{ok}.

If any function returns other value or throws an exception,
this function returns @code{@{error, _@}}.
""".
-spec all(hookpoint(), list()) -> ok | {error, _}.
all(Hookpoint, Args) ->
  try
    lists:foreach(
      fun({Hook, Timeout}) ->
          case safe_apply(Hookpoint, Hook, Args, Timeout) of
            {ok, ok}           -> ok;
            {ok, {error, Err}} -> throw({found, Err});
            {ok, Res}          -> throw({found, {invalid_result, Res}});
            error              -> throw({found, callback_crashed})
          end
      end,
      hooks(Hookpoint)),
    ok
  catch
    {found, Err} -> {error, Err}
  end.

-doc """
Return result of the first hook that returned @code{@{ok, _@}} for
a given set of arguments.
""".
-spec first_match(hookpoint(), list()) -> {ok, _Val} | undefined.
first_match(Hookpoint, Args) ->
  try
    lists:foreach(
      fun({Hook, Timeout}) ->
          case safe_apply(Hookpoint, Hook, Args, Timeout) of
            {ok, {ok, Val}} -> throw({found, Val});
            _               -> ok
          end
      end,
      hooks(Hookpoint)),
    undefined
  catch
    {found, Err} -> {ok, Err}
  end.

%%================================================================================
%% Internal functions
%%================================================================================

hooks(Hookpoint) ->
  MS = { {{Hookpoint, '_', '$1'}, '$2'}
       , []
       , [{{'$1', '$2'}}]
       },
  ets:select(?tab, [MS]).

-spec safe_apply(hookpoint(), fun(), list(), timeout() | undefined) -> {ok, _Val} | error.
safe_apply(HookPoint, Fun, Args, MaybeTimeout) ->
  case MaybeTimeout of
    undefined -> Timeout = default_timeout();
    Timeout   -> ok
  end,
  case classy_lib:safe_apply_with_timeout({Fun, Args}, Timeout) of
    {ok, _} = Ok ->
      Ok;
    Err ->
      ?tp(critical, ?classy_hook_failure,
          #{ reason    => Err
           , hook      => Fun
           , hookpoint => HookPoint
           , args      => Args
           }),
      error
  end.
