%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Module responsible for managing the hooks.
-module(classy_hook).

%% API:
-export([ init/0
        , insert/3
        , unhook/1
        , foreach/2
        , fold/3
        , all/2
        , first_match/2
        ]).

-export_type([ hookpoint/0
             , prio/0
             , hook/0
             ]).

-include("classy_internal.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(tab, ?MODULE).

-type hookpoint() :: atom().

-type prio() :: integer().

-opaque hook() :: tuple().

%%================================================================================
%% API functions
%%================================================================================

%% @private
init() ->
  ets:new(?tab, [named_table, ordered_set, public, {keypos, 1}]),
  %% Default initialization:
  classy:on_node_init(fun classy_builtin_hooks:gen_random_site_id/0, -100),
  classy:post_kick(fun classy_builtin_hooks:maybe_reinitialize_after_kick/3, -100),
  %% Info logging:
  classy:on_create_site(fun classy_builtin_hooks:log_create_site/1, 100),
  classy:on_create_cluster(fun classy_builtin_hooks:log_create_cluster/2, 100),
  classy:pre_join(fun classy_builtin_hooks:log_pre_join/4, 100),
  classy:post_join(fun classy_builtin_hooks:log_post_join/3, -100),
  classy:on_membership_change(fun classy_builtin_hooks:log_membership_change/4, 100),
  classy:run_level(fun classy_builtin_hooks:log_run_level/2, -100),
  classy:on_peer_connection_status_change(fun classy_builtin_hooks:log_site_status_change/5, 100),
  %% User initialization:
  case application:get_env(classy, setup_hooks) of
    {ok, {Mod, Func, Args}} ->
      apply(Mod, Func, Args),
      ok;
    undefined ->
      ok
  end.

%% @private
-spec insert(hookpoint(), fun(), prio()) -> hook().
insert(Hookpoint, Hook, Prio) when is_atom(Hookpoint), is_integer(Prio), is_function(Hook) ->
  Key = {Hookpoint, -Prio, Hook},
  ets:insert(?tab, {Key}),
  Key.

%% @doc Remove a previously inserted hook
-spec unhook(hook()) -> ok.
unhook(Key) ->
  ets:delete(?tab, Key),
  ok.

%% @doc Execute all functions hooked into `Hookpoint'.
%%
%% Errors are ignored (logged).
-spec foreach(hookpoint(), list()) -> ok.
foreach(Hookpoint, Args) ->
  lists:foreach(
    fun(Hook) ->
        safe_apply(Hookpoint, Hook, Args)
    end,
    hooks(Hookpoint)).

%% @doc Fold over all functions registered in `Hookpoint'.
%% Accumulator argument is appended to the `Args' list.
%%
%% Errors are ignored (logged).
-spec fold(hookpoint(), list(), A) -> A.
fold(Hookpoint, Args, Acc0) ->
  try
    lists:foldl(
      fun(Hook, Acc1) ->
          case safe_apply(Hookpoint, Hook, Args ++ [Acc1]) of
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

%% @doc Ensure that all functions hooked into `Hookpoint' return `ok'.
%%
%% If any of the function returns `{error, _}' or throws an exception,
%% this function returns `{error, _}'.
-spec all(hookpoint(), list()) -> ok | {error, _}.
all(Hookpoint, Args) ->
  try
    lists:foreach(
      fun(Hook) ->
          case safe_apply(Hookpoint, Hook, Args) of
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

%% @doc Return result of the first hook that returned `{ok, _}' for
%% a given set of arguments.
-spec first_match(hookpoint(), list()) -> {ok, _Val} | undefined.
first_match(Hookpoint, Args) ->
  try
    lists:foreach(
      fun(Hook) ->
          case safe_apply(Hookpoint, Hook, Args) of
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
  MS = { {{Hookpoint, '_', '$1'}}
       , []
       , ['$1']
       },
  ets:select(?tab, [MS]).

-spec safe_apply(hookpoint(), fun(), list()) -> {ok, _Val} | error.
safe_apply(HookPoint, Fun, A) ->
  try
    {ok, apply(Fun, A)}
  catch
    EC:Err:Stack ->
      ?tp(warning, classy_hook_failure,
          #{ EC        => Err
           , stack     => Stack
           , hook      => Fun
           , hookpoint => HookPoint
           }),
      error
  end.
