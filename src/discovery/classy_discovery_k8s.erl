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

-module(classy_discovery_k8s).

-behaviour(classy_discovery_strategy).

%% Cluster strategy callbacks.
-export([ discover/1
        , hook/0
        ]).

-export_type([opts/0]).

-import(proplists, [get_value/2, get_value/3]).

-define(SERVICE_ACCOUNT_PATH, "/var/run/secrets/kubernetes.io/serviceaccount/").

-define(LOG(Level, Format, Args), logger:Level("Classy(k8s): " ++ Format, Args)).

-type opts() ::
        #{ apiserver    := string()
         , service_name := string()
         , app_name     => string()
         , address_type => ip | hostname | dns
         , namespace    => string()
         , suffix       => string()
         }.

hook() ->
  classy_discovery_strategy:hook(
    fun({k8s, _}) ->
        {ok, ?MODULE};
       (_) ->
        undefined
    end,
    0).

%%--------------------------------------------------------------------
%% classy_discovery_strategy callbacks
%%--------------------------------------------------------------------

discover(Options) ->
  Defaults = #{ app_name     => classy_autocluster:app_name()
              , address_type => ip
              , namespace    => "default"
              , suffix       => ""
              },
  #{ apiserver    := Server
   , service_name := Service
   , app_name     := App
   , address_type := AddrType
   , namespace    := Namespace
   , suffix       := Suffix
   } = maps:merge(Defaults, Options),
  case k8s_service_get(Server, Service, Namespace) of
    {ok, Response} ->
      Addresses = extract_addresses(AddrType, Response),
      {ok, [node_name(App, Addr, Service, AddrType, Namespace, Suffix) || Addr <- Addresses]};
    {error, Reason} ->
      {error, Reason}
  end.

node_name(App, Addr, Service, hostname, Namespace, Suffix) when length(Suffix) > 0 ->
  list_to_atom(lists:concat([App, "@", binary_to_list(Addr), ".", Service, ".", Namespace, ".", Suffix]));

node_name(App, Addr, _Service, dns, Namespace, Suffix) when length(Suffix) > 0 ->
  list_to_atom(lists:concat([App, "@", binary_to_list(Addr), ".", Namespace, ".", Suffix]));

node_name(App, Addr, _, _, _, _) ->
  list_to_atom(App ++ "@" ++ binary_to_list(Addr)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

k8s_service_get(Server, Service, Namespace) ->
  Headers = [{<<"Authorization">>, iolist_to_binary(["Bearer ", token()])}],
  HttpOpts = case filelib:is_file(cert_path()) of
                 true  -> [{ssl_options, [{cacertfile, cert_path()}]}];
                 false -> [{ssl_options, [{verify, verify_none}]}]
             end,
  classy_httpc:get(Server, service_path(Service, Namespace), [], Headers, HttpOpts).

service_path(Service, Namespace) ->
  lists:concat(["api/v1/namespaces/", Namespace, "/endpoints/", Service]).

% namespace() ->
%     binary_to_list(trim(read_file("namespace", <<"default">>))).

token() ->
  trim(read_file("token", <<"">>)).

cert_path() ->
  ?SERVICE_ACCOUNT_PATH ++ "/ca.crt".

read_file(Name, Default) ->
  case file:read_file(?SERVICE_ACCOUNT_PATH ++ Name) of
    {ok, Data} ->
      Data;
    {error, Error} ->
      ?LOG(error, "Cannot read ~s: ~p", [Name, Error]),
      Default
  end.

trim(S) ->
  binary:replace(S, <<"\n">>, <<>>).

extract_addresses(Type, Response) ->
  lists:flatten(
    [[extract_host(Type, Addr) || Addr <- maps:get(<<"addresses">>, Subset, [])]
     || Subset <- maps:get(<<"subsets">>, Response, [])]).

extract_host(ip, Addr) ->
  maps:get(<<"ip">>, Addr);

extract_host(hostname, Addr) ->
  maps:get(<<"hostname">>, Addr);

extract_host(dns, Addr) ->
  binary:replace(maps:get(<<"ip">>, Addr), <<".">>, <<"-">>, [global]).
