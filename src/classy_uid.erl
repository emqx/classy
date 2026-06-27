%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(classy_uid).
-moduledoc """
This module contains utilities for constructing unique IDs
using various algorithms.
""".

%% API:
-export([ site_unique_tuple/0
        , cluster_unique_tuple/0
        ]).

%% internal exports:
-export([start_link/0]).

-export_type([su_tuple/0, cu_tuple/0]).

-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(volatile_sequences, classy_uid_volatile_sequences_tab).

-doc "Site-unique tuple".
-type su_tuple() :: {non_neg_integer(), pos_integer()}.

-doc "Cluster-unique tuple".
-type cu_tuple() :: {classy:site(), non_neg_integer(), pos_integer()}.

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-doc false.
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-doc """
Return a tuple that is unique within the site (across restarts),
but is NOT unique in a cluster.

SeqTuple returned by this function consists of a number of node
restarts followed by a @code{erlang:unique_integer}.

Site-unique tuples can be used to order events within the site.
""".
-spec site_unique_tuple() -> su_tuple().
site_unique_tuple() ->
  {ok, NRestarts} = classy_liveness:n_restarts(),
  {NRestarts, erlang:unique_integer([positive, monotonic])}.

-doc """
Return a tuple similar to @ref{classy_uid:site_unique_tuple/0},
but also including site id,
which makes it unique within the cluster.

Cluster-unique tuples can be used to order events on the originator site,
but not globally.
""".
-spec cluster_unique_tuple() -> cu_tuple().
cluster_unique_tuple() ->
  {ok, Site} = classy:the_site(),
  {ok, NRestarts} = classy_liveness:n_restarts(),
  {Site, NRestarts, erlang:unique_integer([positive, monotonic])}.

%%================================================================================
%% Internal functions
%%================================================================================
