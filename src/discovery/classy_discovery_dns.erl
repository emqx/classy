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

-module(classy_discovery_dns).

-behaviour(classy_discovery_strategy).

%% Cluster strategy callbacks
-export([ discover/1
        , lock/1
        , unlock/1
        , register/1
        , unregister/1
        ]).

-export_type([ opts/0
             ]).

-type opts() ::
    #{ name := string()
     , type => a | aaaa | srv
     , app  => string() | atom()
     }.

discover(Options) ->
  Defaults = #{ app  => undefined
              , type => a
              },
  #{ name := DomainName
   , app  := NodeName
   , type := Type
   } = maps:merge(Defaults, Options),
  {ok, [node_name(NodeName, Host) || Host <- resolve_hosts(DomainName, Type)]}.

resolve_hosts(DomainName, Type) when Type =:= a; Type =:= aaaa ->
  [inet:ntoa(IP) || IP <- inet_res:lookup(DomainName, in, Type)];
resolve_hosts(DomainName, srv) ->
  Records = inet_res:lookup(DomainName, in, srv),
  lists:usort(lists:map(fun({_, _, _, Host}) -> Host end, Records)).

node_name(undefined, Host) ->
  node_name(classy_autocluster:app_name(), Host);
node_name(NodeName, Host) ->
  list_to_atom(lists:concat([NodeName, "@", Host])).

lock(_Options) ->
  ok.

unlock(_Options) ->
  ok.

register(_Options) ->
  ok.

unregister(_Options) ->
  ok.
