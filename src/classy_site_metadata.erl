%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_site_metadata).
-moduledoc """
This module provides an API for setting site properties that are replicated across the cluster via gossip protocol.
The data is preserved between node restarts, but gets deleted when site leaves cluster.

@example erlang
classy_site_metadata:set(foo, bar).
classy_site_metadata:set(bar, baz).

...
classy:get_meta(Site) ->
  #@{ foo => bar
  , bar => baz
   @}.
@end example
""".

%% API:
-export([set/2, delete/1, lookup/1, get_all/0, get_all/1]).

%% internal exports:
-export([ on_leave/3
        ]).

-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-record(cluster_g, {k}).

%%================================================================================
%% API functions
%%================================================================================

-doc """
Update a site metadata value associated with key.
""".
-spec set(_Key, _Value) -> ok | {error, _}.
set(Key, Val) ->
  maybe
    ok ?= classy_table:write(
            ?globals,
            #cluster_g{k = Key},
            Val),
    propagate()
  end.

-doc """
Delete site metadata key.
""".
-spec delete(_Key) -> ok | {error, _}.
delete(Key) ->
  maybe
    ok ?= classy_table:delete(
            ?globals,
            #cluster_g{k = Key}),
    propagate()
  end.

-doc """
Lookup a key from the metadata of the local site.
""".
-spec lookup(_Key) -> list().
lookup(Key) ->
  classy_table:lookup(
    ?globals,
    #cluster_g{k = Key}).

-doc """
Get all local metadata keys.
""".
-spec get_all() -> map().
get_all() ->
  MS = { #classy_kv{ k = #cluster_g{k = '$1'}
                   , v = '$2'
                   , _ = '_'
                   }
       , []
       , [{{'$1', '$2'}}]
       },
  maps:from_list(classy_table:select(?globals, [MS])).

-spec get_all(classy:site()) -> {ok, map()} | undefined.
get_all(_Site) ->
  undefined.

%%================================================================================
%% Internal exports
%%================================================================================

-doc false.
-spec on_leave(classy:cluster_id(), classy:site(), classy:kick_intent()) -> ok.
on_leave(_Cluster, _Site, _Intent) ->
  maps:foreach(
    fun(K, _) ->
        delete(K)
    end,
    get_all()).

%%================================================================================
%% Internal functions
%%================================================================================

propagate() ->
  maybe
    {ok, Cluster} ?= classy:the_cluster_err(),
    {ok, Site} ?= classy:the_site_err(),
    classy_membership:set_info(Cluster, Site, get_all())
  end.
