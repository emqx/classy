%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(classy_uid).
-moduledoc """
This module contains utilities for constructing unique IDs
using various algorithms.
""".

-behavior(gen_server).

%% API:
-export([ site_unique_tuple/0
        , cluster_unique_tuple/0
        , site_unique_seq_tuple/1
        , cluster_unique_seq_tuple/1
        ]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/0]).

-export_type([su_tuple/0, cu_tuple/0, sequence/0]).

-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(volatile_sequences, classy_uid_volatile_sequences_tab).

-define(pterm_uid_gen, classy_uid_gen).

-type pterm_uid_gen() :: #{ site       := classy:site()
                          , n_restarts := non_neg_integer()
                          }.

-doc "Site-unique tuple".
-type su_tuple() :: {non_neg_integer(), pos_integer()}.

-doc "Cluster-unique tuple".
-type cu_tuple() :: {classy:site(), non_neg_integer(), pos_integer()}.

-doc "Identifier of a volatile sequence".
-type sequence() :: term().

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
  #{n_restarts := NRestarts} = get_pterm(),
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
  #{n_restarts := NRestarts, site := Site} = get_pterm(),
  {Site, NRestarts, erlang:unique_integer([positive, monotonic])}.

-doc """
Return a tuple that is guaranteed to be unique within sequence ID and site.

It consists of number of restarts
followed by a value of a volatile counter identified by sequence ID.
Sequence counter gets reset on node restart.

This function is expected to be slower than @code{classy_uid:site_unique_tuple/0},
but its values form a monotonic sequence.
""".
-spec site_unique_seq_tuple(sequence()) -> su_tuple().
site_unique_seq_tuple(Sequence) ->
  #{n_restarts := NRestarts} = get_pterm(),
  {NRestarts, volatile_counter(Sequence)}.

-doc """
Similar to @ref{classy_uid:site_unique_seq_tuple/1},
but includes site name.
""".
-spec cluster_unique_seq_tuple(sequence()) -> cu_tuple().
cluster_unique_seq_tuple(Sequence) ->
  #{n_restarts := NRestarts, site := Site} = get_pterm(),
  {Site, NRestarts, volatile_counter(Sequence)}.

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {}).

-doc false.
init(_) ->
  process_flag(trap_exit, true),
  {ok, NRestarts} = classy_node:n_restarts(),
  {ok, Site} = classy:the_site(),
  ets:new(?volatile_sequences, [set, named_table, public, {write_concurrency, true}]),
  set_pterm(#{ site               => Site
             , n_restarts         => NRestarts
             }),
  S = #s{},
  {ok, S}.

-doc false.
handle_call(Call, From, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => call
       , from => From
       , content => Call
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

-doc false.
handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

-doc false.
handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

-doc false.
terminate(_Reason, _S) ->
  persistent_term:erase(?pterm_uid_gen),
  ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

-spec set_pterm(pterm_uid_gen()) -> ok.
set_pterm(PT) ->
  persistent_term:put(?pterm_uid_gen, PT).

-spec get_pterm() -> pterm_uid_gen().
get_pterm() ->
  persistent_term:get(?pterm_uid_gen).

-spec volatile_counter(sequence()) -> pos_integer().
volatile_counter(Sequence) ->
  ets:update_counter(?volatile_sequences, Sequence, {2, 1}, {Sequence, 0}).
