%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_site_metadata).
-moduledoc """
This module provides an API for setting persistent site properties.
These properties survive node restarts and cluster changes.

Each site property has either of two visibility types:

@enumerate
@item @code{site}
site properties are not tied to any particular cluster,
and are not visible to the peers.

Joining or leaving the cluster doesn't change visibility of the site-private properties.

@item @code{cluster}
cluster properties are tied to the cluster ID,
and they are replicated across the cluster via gossip protocol.
@end enumerate

Cluster and site properties are independent:
they exist in two different ``namespaces''.

@example erlang
classy_site_metadata:c_set(foo, bar).
classy_site_metadata:c_set(bar, baz).

...
classy:get_meta(Site) ->
  #@{ foo => bar
  , bar => baz
   @}.
@end example
""".

%% API:
-export([c_set/2, c_set/3, c_delete/1, c_delete/2, c_atomically/1, c_atomically/2, c_lookup/1, c_lookup/2, c_get_all/0, c_get_all/1]).
-export([s_set/2, s_delete/1, s_atomically/1, s_lookup/1, s_get_all/0]).

%% internal exports:
-export([ init/0
        , terminate/0
        ]).

-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(tab, classy_site_metadata).
%% Persistent data key wrappers:
%%   Site props:
-record(s, {k}).
%%   Cluster props:
-record(c, {c, k}).

%%================================================================================
%% API functions
%%================================================================================

-doc """
Update cluster metadata value associated with a key.
""".
-spec c_set(_Key, _Value) -> ok | {error, _}.
c_set(Key, Val) ->
  maybe
    {ok, Cluster} ?= classy:the_cluster_err(),
    c_set(Cluster, Key, Val)
  end.

-doc """
Version of @code{c_set} with explicit cluster.
""".
-spec c_set(classy:cluster_id(), _Key, _Value) -> ok | {error, _}.
c_set(Cluster, Key, Val) ->
  maybe
    ok ?= classy_table:write(
            ?tab,
            #c{c = Cluster, k = Key},
            Val),
    propagate(Cluster)
  end.

-doc """
Persistently set a site property.

These properties survive all cluster changes,
they don't get cleaned automatically.

WARNING: The purpose of site-private properties is to aid with node migration activities,
such as migrating to classy application or between major releases.

Do NOT use this feature for arbitrary application data,
use separate @code{classy_table}s instead.
""".
-spec s_set(_Key, _Value) -> ok | {error, _}.
s_set(Key, Val) ->
  classy_table:write(
    ?tab,
    #s{k = Key},
    Val).

-doc """
Delete a site metadata key.
""".
-spec c_delete(_Key) -> ok | {error, _}.
c_delete(Key) ->
  maybe
    {ok, Cluster} ?= classy:the_cluster_err(),
    c_delete(Cluster, Key)
  end.

-doc """
Version of @code{c_delete} with explicit cluster.
""".
-spec c_delete(classy:cluster_id(), _Key) -> ok | {error, _}.
c_delete(Cluster, Key) ->
  maybe
    ok ?= classy_table:delete(
            ?tab,
            #c{c = Cluster, k = Key}),
    propagate(Cluster)
  end.

-doc """
Delete a site-private key.
""".
-spec s_delete(_Key) -> ok | {error, _}.
s_delete(Key) ->
  classy_table:delete(
    ?tab,
    #s{k = Key}).

-doc """
Atomically update a number of cluster metadata key-values.
""".
-spec c_atomically([classy_table:atomic_op(Effect)]) -> {ok, [Effect]} | {error, _}.
c_atomically(Ops) ->
  maybe
    {ok, Cluster} ?= classy:the_cluster_err(),
    c_atomically(Cluster, Ops)
  end.

-spec c_atomically(classy:cluster_id(), [classy_table:atomic_op(Effect)]) -> {ok, [Effect]} | {error, _}.
c_atomically(Cluster, Ops0) ->
  maybe
    Ops = [case I of
             {w, K, V} ->
               {w, #c{c = Cluster, k = K}, V};
             {d, K} ->
               {d, #c{c = Cluster, k = K}};
             {then, _} = Eff ->
               Eff
           end || I <- Ops0],
    {ok, Effects} ?= classy_table:atomically(?tab, Ops),
    _ = propagate(Cluster),
    {ok, Effects}
  end.

-doc """
Atomically update a number of site-private key-values.
""".
-spec s_atomically([classy_table:atomic_op(Effect)]) -> {ok, [Effect]} | {error, _}.
s_atomically(Ops0) ->
  Ops = [case I of
           {w, K, V} ->
             {w, #s{k = K}, V};
           {d, K} ->
             {d, #s{k = K}};
           {then, _} = Eff ->
             Eff
         end || I <- Ops0],
  classy_table:atomically(?tab, Ops).

-doc """
Lookup a key from the metadata of the local site.

Raises an error when site is not in the cluster.
""".
-spec c_lookup(_Key) -> list().
c_lookup(Key) ->
  maybe
    {ok, Cluster} ?= classy:the_cluster(),
    c_lookup(Cluster, Key)
  else
    undefined ->
      error(not_in_cluster)
  end.

-doc """
Version of @code{c_lookup} with explicit cluster.
""".
-spec c_lookup(classy:cluster_id(), _Key) -> list().
c_lookup(Cluster, Key) ->
  classy_table:lookup(
    ?tab,
    #c{c = Cluster, k = Key}).

-doc """
Lookup a site-private key.
""".
-spec s_lookup(_Key) -> list().
s_lookup(Key) ->
  classy_table:lookup(
    ?tab,
    #s{k = Key}).

-doc """
Get all cluster metadata of the local site.
""".
-spec c_get_all() -> {ok, map()} | {error, not_in_cluster}.
c_get_all() ->
  maybe
    {ok, Cluster} ?= classy:the_cluster_err(),
    {ok, c_get_all(Cluster)}
  end.

-doc """
Version of @code{c_get_all} with explicit cluster.
""".
-spec c_get_all(classy:cluster_id()) -> map().
c_get_all(Cluster) ->
  MS = { #classy_kv{ k = #c{c = Cluster, k = '$1'}
                   , v = '$2'
                   , _ = '_'
                   }
       , []
       , [{{'$1', '$2'}}]
       },
  maps:from_list(classy_table:select(?tab, [MS])).

-doc """
Get all site-local metadata.
""".
-spec s_get_all() -> map().
s_get_all() ->
  MS = { #classy_kv{ k = #s{k = '$1'}
                   , v = '$2'
                   , _ = '_'
                   }
       , []
       , [{{'$1', '$2'}}]
       },
  maps:from_list(classy_table:select(?tab, [MS])).

%%================================================================================
%% Internal exports
%%================================================================================

-doc false.
init() ->
  classy_table:open(?tab, #{ets_options => [ordered_set, {read_concurrency, true}]}).

-doc false.
terminate() ->
  classy_table:stop(?tab, 5_000).

%%================================================================================
%% Internal functions
%%================================================================================

-spec propagate(classy:cluster_id()) -> ok | {error, _}.
propagate(Cluster) ->
  maybe
    {ok, Site} ?= classy:the_site_err(),
    {ok, _} ?= classy_membership:set_info(
                 Cluster,
                 Site,
                 c_get_all(Cluster)),
    ok
  end.
