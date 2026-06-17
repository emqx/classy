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

t_discover(_) ->
  ok = meck:new(classy_httpc, [non_strict, no_history]),
  Json = <<"{\"subsets\": [{\"addresses\": [{\"ip\": \"192.168.10.10\"}]}]}">>,
  ok = meck:expect(
         classy_httpc, get,
         fun(_Server, _Path, _Params, _Headers, _Opts) ->
             {ok, jsone:decode(Json)}
         end),
  ?assertEqual(
     {ok, ['ekka@192.168.10.10']},
     classy_discovery_strategy:discover(
       classy_discovery_k8s,
       maps:merge(?OPTIONS, #{app_name => "ekka"}))),
  %% Below test relies on rebar3 ct is run with '--name ct@127.0.0.1'
  ?assertEqual(
     {ok, ['ct@192.168.10.10']},
     classy_discovery_strategy:discover(
       classy_discovery_k8s,
       ?OPTIONS)),
  ok = meck:unload(classy_httpc).
