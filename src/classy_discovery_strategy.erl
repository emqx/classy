%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(classy_discovery_strategy).
-moduledoc """
This module defines a behavior of discovery strategy.
""".

-export([hook/2]).
-export([get/0, discover/2, lock/2, unlock/2, register/2, unregister/2]).

-export_type([options/0, t/0]).

-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Callback and type declarations
%%================================================================================

-type t() :: {module(), options()}.

-type options() :: term().

-callback discover(options()) -> {ok, [node()]} | {error, term()}.

-callback lock(options()) -> ok | ignore | {error, term()}.

-callback unlock(options()) -> ok | ignore | {error, term()}.

-callback register(options()) -> ok | ignore | {error, term()}.

-callback unregister(options()) -> ok | ignore | {error, term()}.

-optional_callbacks([lock/1, unlock/1, register/1, unregister/1]).

-define(hook, ?MODULE).

%%================================================================================
%% API functions
%%================================================================================

-doc """
Register a discovery strategy.

Callbacks registered here match on the @ref{discovery_strategy} configuration
and return callback module implementing @code{classy_discovery_strategy} behavior.

The first hook that returns @code{@{ok, Module@}} wins
and handles all the callbacks for the next discovery cycle.
""".
-spec hook(fun(({atom(), options()}) -> {ok, module()} | undefined), classy_hook:prio()) -> classy_hook:hook().
hook(Fun, Prio) when is_function(Fun, 1), is_number(Prio) ->
  classy_hook:insert(?hook, Fun, Prio).

-doc """
Read @ref{discovery_strategy} environment variable and decide which strategy to use.
""".
-spec get() -> t() | undefined.
get() ->
  Conf = application:get_env(classy, discovery_strategy, {manual, []}),
  case classy_hook:first_match(?hook, [Conf]) of
    {ok, Module} ->
      {_Method, Options} = Conf,
      {Module, Options};
    undefined ->
      undefined
  end.

%%================================================================================
%% Internal exports
%%================================================================================

-doc false.
-spec discover(module(), options()) -> {ok, [node()]} | {error, term()}.
discover(Mod, Options) ->
  safe_call(Mod, ?FUNCTION_NAME, Options).

-doc false.
-spec lock(module(), options()) -> ok | ignore | {error, term()}.
lock(Mod, Options) ->
  case erlang:function_exported(Mod, ?FUNCTION_NAME, 1) of
    true ->
      safe_call(Mod, ?FUNCTION_NAME, Options);
    false ->
      ok
  end.

-doc false.
-spec unlock(module(), options()) -> ok | ignore | {error, term()}.
unlock(Mod, Options) ->
  case erlang:function_exported(Mod, ?FUNCTION_NAME, 1) of
    true ->
      safe_call(Mod, ?FUNCTION_NAME, Options);
    false ->
      ok
  end.

-doc false.
-spec register(module(), options()) -> ok | ignore | {error, term()}.
register(Mod, Options) ->
  case erlang:function_exported(Mod, ?FUNCTION_NAME, 1) of
    true ->
      safe_call(Mod, ?FUNCTION_NAME, Options);
    false ->
      ok
  end.

-doc false.
-spec unregister(module(), options()) -> ok | ignore | {error, term()}.
unregister(Mod, Options) ->
  case erlang:function_exported(Mod, ?FUNCTION_NAME, 1) of
    true ->
      safe_call(Mod, ?FUNCTION_NAME, Options);
    false ->
      ok
  end.

%%================================================================================
%% Internal functions
%%================================================================================

safe_call(Module, Function, Options) ->
  try apply(Module, Function, [Options])
  catch
    EC:Err:Stack ->
      ?tp(warning, classy_discovery_failure,
          #{ EC       => Err
           , stack    => Stack
           , module   => Module
           , function => Function
           , options  => Options
           }),
      {error, callback_crashed}
  end.
