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

-module(classy_discovery_k8s_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(OPTIONS,
        #{ apiserver    => "http://10.110.111.204:8080"
         , namespace    => "default"
         , service_name => "classy"
         }).

all() ->
  classy_SUITE:all(?MODULE).

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_, _Config) ->
  meck:unload().

t_discover(_) ->
  {ok, LocalAppName, _} = classy_lib:split_node_name(node()),
  Host = <<"192.168.10.10">>,

  ok = meck:new(classy_httpc, [non_strict, no_history]),
  Json = <<"{\"subsets\": [{\"addresses\": [{\"ip\": \"", Host/binary, "\"}]}]}">>,
  ok = meck:expect(
         classy_httpc, get,
         fun(_Server, _Path, _Params, _Headers, _Opts) ->
             {ok, json:decode(Json)}
         end),
  %% Check the discovered node name when application name is set explicitly:
  ?assertEqual(
     {ok, <<"ekka">>, Host},
     discover_and_parse(maps:merge(?OPTIONS, #{app_name => "ekka"}))),
  %% Check the discovered node name when application name is inherited from the local node:
  ?assertEqual(
     {ok, LocalAppName, Host},
     discover_and_parse(?OPTIONS)).

discover_and_parse(Options) ->
  maybe
    {ok, [Node]} ?= classy_discovery_strategy:discover(classy_discovery_k8s, Options),
    classy_lib:split_node_name(Node)
  end.
