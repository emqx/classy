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

-module(classy_discovery_static).

-behaviour(classy_discovery_strategy).

-export([ discover/1
        , hook/0
        ]).

hook() ->
  classy_discovery_strategy:hook(
    fun({static, _}) ->
        {ok, ?MODULE};
       (_) ->
        undefined
    end,
    0).

discover(Options) ->
  {ok, maps:get(seeds, Options, [])}.
