%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(classy).
-moduledoc """
Main interface module of Classy application.

Note: business releases can install hooks by setting
@code{classy:setup_hooks} application environment variable to a tuple
@code{@{Module, Function, Args@}}.
This MFA can contain calls to various @code{classy:on_...} functions.

@xref{api/classy/classy_site_metadata,classy_site_metadata} module.
""".

%% API:
-export([ start_system/0
        , info/0
        , info/1
        , info/2
        , get_meta/1
        , n_restarts/0
        , n_restarts/1
        , node_to_site/0
        , node_of_site/2
        , join_node/2
        , kick_site/2
        , kick_node/2
        , sites/0
        , sites/1
        , nodes/1
        , quorum/1
        , fault_tolerance/1
        , at_lower_level/2
        , run_level/0
        , the_site/0
        , the_site_err/0
        , the_cluster/0
        , the_cluster_err/0
        , node_sets/0
        , prep_stop/0
        , prep_stop/1
        ]).

-export([ on_node_init/2
        , on_prep_stop/2
        , on_create_cluster/2
        , on_create_site/2
        , on_peer_connection_change/2
        , on_membership_change/2
        , on_peer_liveness_change/2
        , on_peer_node_change/2
        , on_peer_restart/2
        , on_node_classify/2
        , pre_join/2
        , post_join/2
        , pre_kick/2
        , on_kick_decided/2
        , on_leave/2
        , pre_autoclean/2
        , pre_autocluster/2
        , run_level/2
        , enrich_site_info/2
        , on_metadata_change/2

        , fallback_get_site/1
        , fallback_get_cluster/1
        , fallback_get_meta/2
        , fallback_get_peer_nodes/1
        ]).

-export_type([ cluster_id/0
             , site/0

             , join_intent/0
             , kick_intent/0

             , peer_info/0
             , info/0
             , site_metadata/0
             , cluster_info/0

             , run_level/0

             , node_set_name/0
             , node_set/0
             ]).

-include("classy_internal.hrl").
-compile({no_auto_import, [nodes/1]}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-doc """
Unique random persistent identifier of the cluster.
@xref{Cluster ID}.
""".
-type cluster_id() :: binary().

-doc """
Unique random persistent identifier of the site.
@xref{Site ID}.
""".
-type site() :: binary().

-type peer_info() ::
        #{ node        := node() | undefined
         , connected   := boolean()
         , last_update := classy_lib:unix_time_s()
         }.

-type info() ::
        #{ cluster     := cluster_id() | undefined
         , site        := site() | undefined
         , last_update := classy_lib:unix_time_s() | undefined
         , peers       := #{site() => peer_info()}
         , atom()      => _
         }.

-type site_metadata() :: map().

-type cluster_info() ::
        #{ infos     := #{node() => info()}
         , bad_nodes := #{node() => _}
         }.

-doc """
Join intent is an arbitrary term passed to @ref{classy:pre_join/2} and @ref{classy:post_join/2} hooks.
@code{pre_join} may match on intent to prevent join in certain cases,
while to @code{post_join} this value is merely informational.

Classy itself uses the following intents:
@itemize
@item @code{autocluster}:
When join is triggered by autocluster.
@end itemize
""".
-type join_intent() :: term().

-doc """
Kick intent is an arbitrary term passed to @link{classy:pre_kick/2}, @link{classy:on_kick_decided/2} and @link{classy:on_leave/2} hooks.
@code{pre_kick} may use intent to make a decision leaving the cluster in certain cases,
while to @code{on_leave} this value is merely informational.

Classy itself uses the following intents:
@itemize
@item @code{@{join, #@{node => node(), cluster => cluster_id(), ...@}@}}:
Site is leaving the cluster to immediately join a different one.

@item @code{kicked}:
Site detects that it got kicked from the cluster by a third party.

@item @code{autoclean}:
Site is kicked by the autoclean logic.

@end itemize
""".
-type kick_intent() :: {join, #{node := node(), cluster := cluster_id(), join_intent := join_intent()}}
                     | kicked
                     | autoclean
                     | term().

-doc """
@xref{Run level}
""".
-type run_level() :: ?stopped | ?single | ?cluster | ?quorum.

-doc """
An arbitrary ID of a node set.

Predefined sets are:
@table @code
@item all
names of all previously seen nodes that belong
(or belonged, if the site is currently down)
to the cluster members.
@item up

@item down
@item connected
there's an Erlang distribution connection to the node hosting the site.
@item disconnected
there's no Erlang distribution connection to the node hosting the site,
but site is not considered down.
@end table
""".
-type node_set_name() :: all | up | down | connected | disconnected | term().

-doc """
A set of nodes.
""".
-type node_set() :: ordsets:ordset(node()).

%%================================================================================
%% API functions
%%================================================================================

-doc """
Classy application starts in dormaint mode,
so other applications can safely declare it as a dependency,
without the risk that OTP application controller
triggers system startup sequence before all hooks are registered.

So simply launching classy application does nothing.
This function must be called in order for the system to come alive.
""".
-spec start_system() -> ok.
start_system() ->
  classy_sup:start_system().

%% RPC target
-doc """
Provide general information about the local node.
""".
-spec info() -> info().
info() ->
  %% Note: this is an RPC target.
  MaybeCluster = classy_node:maybe_cluster(),
  MaybeSite = classy_node:maybe_site(),
  PeerInfo0 = classy_node:peer_info(),
  case maps:take(MaybeSite, PeerInfo0) of
    {#{last_update := MyLU}, PeerInfo} ->
      ok;
    error ->
      PeerInfo = PeerInfo0,
      MyLU = undefined
  end,
  Acc = #{ cluster     => MaybeCluster
         , site        => MaybeSite
         , last_update => MyLU
         , peers       => PeerInfo
         , n_restarts  => n_restarts()
         },
  classy_hook:fold(?on_enrich_site_info, [], Acc).

-doc """
Gather @ref{classy:info/0} from a set of nodes.

The nodes don't have to be in the same cluster.

WARNING: as a side effect of calling this function,
the current node will establish Erlang distribution connection to all nodes in the list.
While this won't affect code using @code{@link{classy:nodes/1,classy:nodes}(connected)} API,
it may confuse code using plain @code{erlang:nodes()}.
""".
-spec info([node()]) -> cluster_info().
info(Nodes) ->
  info(1, Nodes).

-doc false.
-spec info(non_neg_integer(), [node()]) -> cluster_info().
info(_Hops, Nodes) ->
  RPCRet = erpc:multicall(
             Nodes,
             classy, info, [],
             classy_lib:rpc_timeout()),
  {Infos, BadNodes} =
    lists:foldl(
      fun({Node, NodeReply}, {AccInfos, AccBadNodes}) ->
          case NodeReply of
            {ok, Info} ->
              { AccInfos#{Node => Info}
              , AccBadNodes
              };
            Error ->
              { AccInfos
              , AccBadNodes#{Node => Error}
              }
          end
      end,
      {#{}, #{}},
      lists:zip(Nodes, RPCRet)),
  %% TODO: gather information about missing nodes, that is peers of the nodes
  %% TODO: make requests from a temporary, hidden Erlang node to avoid creating a mesh between different clusters?
  #{ infos     => Infos
   , bad_nodes => BadNodes
   }.

-doc """
Get cached site metadata for a remote site.

NOTE: This function works even if the site is down.

@xref{api/classy/classy_site_metadata,classy_site_metadata} module.
""".
-spec get_meta(classy:site()) -> {ok, site_metadata()} | undefined.
get_meta(Site) ->
  classy_node:get_meta(Site).

-doc """
Return the total number of times the site has been restarted.
A fresh node starts with this value = 0.

This value increases when the Erlang VM itself restarts,
or classy application goes from @code{stopped} to @code{single} run level.

NOTE: This counter doesn't get reset when node joins or leaves the cluster,
it increases instead.
""".
-spec n_restarts() -> non_neg_integer() | undefined.
n_restarts() ->
  case classy_node:n_restarts() of
    {ok, N} -> N;
    _       -> undefined
  end.

-doc """
Get cached value of the number of restarts of a remote site.

Note: for the local site,
please call @ref{classy:n_restarts/0},
as values returned by this function may be out-of-date.
""".
-spec n_restarts(site()) -> {ok, non_neg_integer()} | undefined.
n_restarts(Site) ->
  classy_node:n_restarts(Site).

-doc """
Return a mapping from known node names to site IDs.
""".
-spec node_to_site() -> {ok, #{node() => site()}} | {error, not_in_cluster | site_not_initialized}.
node_to_site() ->
  maybe
    {ok, _TheSite} ?= the_site_err(),
    {ok, _TheCluster} ?= the_cluster_err(),
    {ok, classy_node:node_to_site()}
  end.

-doc """
Locate a node that is currently hosting a site.

If @code{OnlyConnected} flag is set,
@code{undefined} is returned when the site is locally unreachable
(even if its node is otherwise known).
""".
-spec node_of_site(site(), boolean()) -> {ok, node()} | {error, {unknown_node | disconnected, site()}}.
node_of_site(Site, OnlyConnected) ->
  case classy_node:node_of_site(Site, OnlyConnected) of
    {ok, _} = Ok ->
      Ok;
    undefined when OnlyConnected ->
      {error, {disconnected, Site}};
    undefined ->
      {error, {unknown_node, Site}}
  end.

-doc """
Equivalent to @code{prep_stop(shutdown)}.
""".
-spec prep_stop() -> ok.
prep_stop() ->
  prep_stop(shutdown).

-doc """
This function can be called before the Erlang node shuts down.

It gracefully lowers the run level to stopped,
without blocking the application controller.

This is helpful if changing the run level involves stopping or starting OTP applications.
""".
-spec prep_stop(term()) -> ok.
prep_stop(Reason) ->
  classy_node:prep_stop(Reason),
  classy_sup:prep_stop().

%%--------------------------------------------------------------------------------
%% Cluster management
%%--------------------------------------------------------------------------------

-doc """
Join the local site to the cluster of a remote node.

This function allows a node to join a cluster by connecting to a known peer.

Arguments:

@enumerate
@item Name of a node to join.

Note: while the majority of classy APIs work with @ref{t:classy:site/0, site IDs},
joining a cluster is always done via regular Erlang node name.

@item Join intent, @pxref{t:classy:join_intent/0, join_intent()}
@end enumerate
""".
-spec join_node(node(), join_intent()) -> ok | {error, _}.
join_node(Node, Intent) ->
  classy_node:join_node(Node, Intent, any).

-doc """
Remove a site from the cluster.
Target site can be local or remote:
it is allowed for a site to kick itself from the cluster.

The kicked site creates an entirely new @ref{t:classy:cluster_id/0, cluster_id()},
and joins it as a singleton member.

Local site (one that initiates kick) runs the following hooks
with @code{Intent} equal to the value of the argument:
@enumerate
@item @ref{classy:pre_kick/2}.
It can decide that removing a site is unsafe and abort the command.

@item @ref{classy:on_leave/2}.
This hook is executed after the target is successfully kicked.
@end enumerate

NOTE: the intent is not propagated across different sites.
If the target site is not the same as the local site,
then the target runs @ref{classy:on_leave/2} with pre-defined intent @code{kicked}.
""".
-spec kick_site(site(), kick_intent()) -> ok | {error, _}.
kick_site(Site, Intent) ->
  classy_node:kick_site(Site, Intent).

-doc """
Translate node name to a site ID and kick it via @ref{classy:kick_site/2}.
""".
-spec kick_node(node(), kick_intent()) -> ok | {error, _}.
kick_node(Node, Intent) ->
  case {the_cluster(), the_site()} of
    {{ok, Cluster}, {ok, Local}} ->
      case classy_membership:site_of_node(Cluster, Local) of
        #{Node := Site} ->
          kick_site(Site, Intent);
        #{} ->
          {error, target_not_in_cluster}
      end;
    _ ->
      {error, local_not_in_cluster}
  end.

-doc """
List IDs of peer sites.
""".
-spec sites() -> [site()].
sites() ->
  maybe
    {ok, Cluster} ?= the_cluster(),
    {ok, Local} ?= the_site(),
    classy_membership:members(Cluster, Local)
  else
    _ ->
      []
  end.

-doc """
Get contents of a site set.

Note: argument is the same as for node sets.
""".
-spec sites(node_set_name()) -> [site()].
sites(SetName) ->
  case persistent_term:get(?pt_site_sets, #{}) of
    #{SetName := Set} -> Set;
    #{} -> []
  end.

-doc """
List peer nodes that belong to a node set.

Important to note: this function returns node names of @emph{peer sites}.
Random connected nodes,
such as shells or classy sites that are not member of the current cluster,
are excluded.
""".
-spec nodes(node_set_name()) -> [node()].
nodes(Name) ->
  case node_sets() of
    #{Name := V} -> V;
    #{}          -> []
  end.

-doc """
Return a map of all node sets.
""".
-spec node_sets() -> #{node_set_name() => node_set()}.
node_sets() ->
  persistent_term:get(?pt_node_sets, #{}).

-doc """
This function can be used to
lower the run level of the system to the given value
and run the specified function.

This function can be used to implement migrations that
require business applications to be stopped.

Note: this function returns immediately after scheduling the action,
but before the function is executed.
""".
-spec at_lower_level(run_level(), fun(() -> any())) -> ok | {error, _}.
at_lower_level(RunLevel, Fun) ->
  classy_rl_changer:at_lower_level(RunLevel, Fun).

-doc """
Get current run level.

NOTE: the value is updated after all @link{classy:run_level/2} hooks complete.
""".
-spec run_level() -> run_level().
run_level() ->
  classy_rl_changer:get(current).

-doc """
Get ID of the local site.
""".
-spec the_site() -> {ok, site()} | undefined.
the_site() ->
  case classy_node:maybe_site() of
    Site when is_binary(Site) ->
      {ok, Site};
    undefined ->
      undefined
  end.

-doc """
Similar to @code{the_site/0},
but returns an error tuple instead of @code{undefined}.

This wrapper is useful for @code{maybe} blocks.
""".
-spec the_site_err() -> {ok, site()} | {error, site_not_initialized}.
the_site_err() ->
  case classy_node:maybe_site() of
    Site when is_binary(Site) ->
      {ok, Site};
    undefined ->
      {error, site_not_initialized}
  end.

-doc """
Get ID of the cluster.
""".
-spec the_cluster() -> {ok, cluster_id()} | undefined.
the_cluster() ->
  case classy_node:maybe_cluster() of
    Cluster when is_binary(Cluster) ->
      {ok, Cluster};
    undefined ->
      undefined
  end.

-doc """
Similar to @code{the_cluster/0},
but returns an error tuple instead of @code{undefined}.

This wrapper is useful for @code{maybe} blocks.
""".
-spec the_cluster_err() -> {ok, cluster_id()} | {error, not_in_cluster}.
the_cluster_err() ->
  case classy_node:maybe_cluster() of
    Cluster when is_binary(Cluster) ->
      {ok, Cluster};
    undefined ->
      {error, not_in_cluster}
  end.

%%--------------------------------------------------------------------------------
%% Misc.
%%--------------------------------------------------------------------------------

-doc """
Calculate the number of nodes required for the quorum:

@itemize
@item @code{Integer}:
any integer value

@item @code{config}:
Return value of `classy.quorum' application environment variable

@item @code{running}:
Quorum among the running sites, not less than @code{quorum(config)}
@end itemize
""".
-spec quorum(config | running | non_neg_integer()) -> pos_integer().
quorum(N) when is_integer(N), N >= 0 ->
  N div 2 + 1;
quorum(config) ->
  max(1, application:get_env(classy, quorum, 1));
quorum(running) ->
  max(
    quorum(length(nodes(connected))),
    quorum(config)).

-doc """
Calculate how many nodes can be down while cluster still can maintain quorum.
""".
fault_tolerance(N) ->
  N - quorum(N).

%%--------------------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------------------

-doc """
Register a hook that is executed when the node (not the site) starts.

It is called before @ref{classy:the_site/0} and @code{classy:the_cluster/0}
are initialized,
and can be used to override the default cluster and site initialization logic.
""".
-spec on_node_init(fun(() -> _), classy_hook:prio()) -> classy_hook:hook().
on_node_init(Hook, Prio) ->
  classy_hook:insert(?on_node_init, Hook, Prio).

-doc """
Register a hook that is executed before shutting down business applications.

It is called before classy application starts the shutdown procedure,
as well when the site leaves a cluster or heals from a network partition.
""".
-spec on_prep_stop(fun((_Reason) -> _), classy_hook:prio()) -> classy_hook:hook().
on_prep_stop(Hook, Prio) ->
  classy_hook:insert(?on_prep_stop, Hook, Prio).

-doc """
This callback is executed once per cluster by the site that originally creates the cluster.
""".
-spec on_create_cluster(fun((cluster_id(), Local) -> _), classy_hook:prio()) ->
        classy_hook:hook()
  when Local :: site().
on_create_cluster(Hook, Prio) ->
  classy_hook:insert(?on_create_cluster, Hook, Prio).

-doc """
This callback is called once per site.
""".
-spec on_create_site(fun((site()) -> _), classy_hook:prio()) -> classy_hook:hook().
on_create_site(Hook, Prio) ->
  classy_hook:insert(?on_create_site, Hook, Prio).

-doc """
Register a hook that is executed when a site changes
status from connected (@code{true}) to disconnected (@code{false}) and vice versa.

Note: this hook runs in the classy main process.
Hence it should avoid blocking it.

WARNING: status change to @code{false} is not indicative of the remote site being actually down.
This can happen during a network partition.
""".
-spec on_peer_connection_change(Fun, classy_hook:prio()) -> classy_hook:hook()
   when Fun :: fun((Remote, node(), _IsConnected :: boolean()) -> _),
        Remote :: site().
on_peer_connection_change(Hook, Prio) ->
  classy_hook:insert(?on_peer_connection_status_change, Hook, Prio).

-doc """
Register a hook that is executed when a site joins or leaves a cluster.

@anchor {node_hook_execution}
Note: this hook can be executed multiple times if the local node is abruptly stopped while the hooks are running.
If the remote site re-joins the cluster while the local was down,
the hook may or may not run.
""".
-spec on_membership_change(
        fun((cluster_id(), _Local :: site(), _Remote :: site(), _IsMember :: boolean()) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook().
on_membership_change(Hook, Prio) ->
  classy_hook:insert(?on_membership_change, Hook, Prio).

-doc """
Register a hook that is executed when a site changes status for up to down or vice versa.

Note: this hook is different from @ref{classy:on_peer_connection_change/2},
as care is taken to avoid firing it during a network partition.

The decision to consider a peer down comes either from the peer itself when it shuts down gracefully
or from the quorum of other running peers.

@xref {node_hook_execution}.
""".
-spec on_peer_liveness_change(
        fun((_Remote :: site(), _IsAlive :: boolean()) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook().
on_peer_liveness_change(Hook, Prio) ->
  classy_hook:insert(?on_peer_liveness_change, Hook, Prio).

-doc """
Register a hook that is executed when a peer site changes the Erlang node name.

@xref {node_hook_execution}.
""".
-spec on_peer_node_change(
        fun((_Remote :: site(), _OldNode :: node(), _NewNode :: node()) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook().
on_peer_node_change(Hook, Prio) ->
  classy_hook:insert(?on_peer_node_change, Hook, Prio).

-doc """
Register a hook that is executed when a peer restarts.

@xref {node_hook_execution}.
""".
-spec on_peer_restart(
        fun((_Remote :: site(), _NRestarts :: pos_integer()) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook().
on_peer_restart(Hook, Prio) ->
  classy_hook:insert(?on_peer_restart, Hook, Prio).

-doc """
Register a hook that can place a site's node into an arbitrary number of custom node sets,
based on @ref{t:classy:info/0}.

@xref{classy:enrich_site_info/2}, @xref{classy:node_sets/0}.
""".
-spec on_node_classify(
        fun((site_metadata()) -> [node_set()]),
        classy_hook:prio()
       ) -> classy_hook:hook().
on_node_classify(Hook, Prio) ->
  classy_hook:insert(?on_node_classify, Hook, Prio).

-doc """
Register a hook that is executed before the local node joins a different cluster.

WARNING: this hook should not have side effects.
It should only check if it is ok to join.
""".
-spec pre_join(
        fun((cluster_id(), Remote, node(), join_intent()) -> ok | {error, _}),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Remote :: site().
pre_join(Hook, Prio) ->
  classy_hook:insert(?on_pre_join, Hook, Prio).

-doc """
Register a hook that is executed after the local site joins a cluster.

It is guaranteed to be called @emph{at least} once,
and must be idempotent.
""".
-spec post_join(
        fun((cluster_id(), Local, JoinedTo, join_intent()) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Local :: site(),
       JoinedTo :: node().
post_join(Hook, Prio) ->
  classy_hook:insert(?on_post_join, Hook, Prio).

-doc """
Register a hook that verifies whether or not a site can be kicked from the cluster.
This hook runs on the node that initiates the kick.

WARNING: this hook cannot have side effects.
""".
-spec pre_kick(
        fun((cluster_id(), Target, kick_intent()) -> ok | {error, _}),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Target :: site().
pre_kick(Hook, Prio) ->
  classy_hook:insert(?on_pre_kick, Hook, Prio).

-doc """
Register a hook that is executed after @ref{classy:pre_kick/2} hooks allow the kick to proceed,
but before the membership change is applied and before the site left the cluster.

This hook is executed only on the node that initiates the kick procedure.
It can be used to trigger some side effects while the site is still part of the cluster.

WARNING: theoretically, kick procedure can be aborted after this hook fires,
or in the middle of its execution.
In this case kick procedure @emph{won't} be retried.
Then side effects of this hook will be observed,
but the site will stay in the cluster.
As such, it's not recommended to perform any destructive actions here.

Normally, such actions should be performed in @link{classy:on_membership_change/2}.
""".
-spec on_kick_decided(
        fun((cluster_id(), Target, kick_intent()) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Target :: site().
on_kick_decided(Hook, Prio) ->
  classy_hook:insert(?on_kick_decided, Hook, Prio).

-doc """
Register a hook that is executed after the local site leaves a cluster.
This hook can perform destructive actions associated with cleanup.
""".
-spec on_leave(
        fun((OldCluster, Local, kick_intent()) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when OldCluster :: cluster_id(),
       Local :: site().
on_leave(Hook, Prio) ->
  classy_hook:insert(?on_leave, Hook, Prio).

-doc """
Register a hook that runs before autoclean finalizes the decision to kick a down site.

WARNING: this hook cannot have side effects.
""".
-spec pre_autoclean(
        fun((Remote) -> ok | {error, _}),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Remote :: site().
pre_autoclean(Hook, Prio) ->
  classy_hook:insert(?on_pre_autoclean, Hook, Prio).

-doc """
Register a hook that filters and ranks nodes for autocluster.
It allows the business code to pick the most appropriate cluster for automatic join.

WARNING: this hook cannot have side effects.
""".
-spec pre_autocluster(
        fun((cluster_info(), Discovered) -> Discovered),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Discovered :: [{cluster_id(), [node()]}].
pre_autocluster(Hook, Prio) ->
  classy_hook:insert(?on_pre_autocluster, Hook, Prio).

-doc """
Register a hook that is executed on change of the run level of the local site.

When run level increases, hooks with higher priority run earlier;
when it decreases, hooks run in the reverse order.

WARNING: if the callback interacts with the OTP application controller
(e.g. it starts or stops an OTP application),
then stopping classy application using @code{application:stop(classy)} will lead to a deadlock.
Use @code{classy:prep_stop()} function to safely lower the run level and shut down classy.
""".
-spec run_level(
        fun((run_level(), run_level()) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook().
run_level(Hook, Prio) ->
  classy_hook:insert(?on_change_run_level, Hook, Prio).

-doc """
Register a hook that can add entries to the map returned by @ref{classy:info/0}.
""".
-spec enrich_site_info(
        fun((info()) -> info()),
        classy_hook:prio()
       ) -> classy_hook:hook().
enrich_site_info(Hook, Prio) ->
  classy_hook:insert(?on_enrich_site_info, Hook, Prio).

-doc """
Register a hook that is called when metadata of a site (local or remote) changes.
""".
-spec on_metadata_change(
        fun((cluster_id(), site(), site_metadata()) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook().
on_metadata_change(Hook, Prio) ->
  classy_hook:insert(?on_metadata_change, Hook, Prio).

-doc """
Fallback: derive site ID of a remote node that doesn't run classy.

WARNING: Fallback mechanism is meant to support rolling cluster upgrade.
This functionality is inherently dangerous,
but it won't activate unless the user registers all fallback hooks.

The following limitations apply:

@enumerate
@item The hook should return a value that cannot change,
even after the node is upgraded.

@item At most one fallback hook should be registered for this hookpoint.
Registering more than one hook is dangerous,
since transient errors in the hook may lead to inconsistent results between the calls.

@end enumerate
""".
-spec fallback_get_site(fun((node()) -> {ok, site()} | undefined)) -> classy_hook:hook().
fallback_get_site(Hook) ->
  classy_hook:insert(?fallback_get_site, Hook, 0).

-doc """
Fallback: derive cluster ID of a remote node that doesn't run classy.

WARNING: Fallback mechanism is meant to support rolling cluster upgrade.
This functionality is inherently dangerous,
but it won't activate unless the user registers all fallback hooks.

The following limitations apply:

@enumerate
@item The hook should return a value that cannot change,
even after the node is upgraded.

@item All peer nodes of this one should return the same cluster ID.

@item At most one fallback hook should be registered for this hookpoint.
Registering more than one hook is dangerous,
since transient errors in the hook may lead to inconsistent results between the calls.

@end enumerate
""".
-spec fallback_get_cluster(fun((node()) -> {ok, cluster_id()} | undefined)) -> classy_hook:hook().
fallback_get_cluster(Hook) ->
  classy_hook:insert(?fallback_get_cluster, Hook, 0).

-doc """
Fallback: derive site metadata of a remote node that doesn't run classy.

This hook is optional.
""".
-spec fallback_get_meta(
        fun((node(), site_metadata()) -> site_metadata()),
        classy_hook:prio()
       ) -> classy_hook:hook().
fallback_get_meta(Hook, Prio) ->
  classy_hook:insert(?fallback_get_meta, Hook, Prio).

-doc """
Fallback: get list of peer nodes of a given node.

When a classy-aware node joins to another node via the fallback mechanism,
its peer information is seeded by running
@ref{classy:fallback_get_site/1}, @ref{classy:fallback_get_cluster/1} and @ref{classy:fallback_get_meta/2}
hooks for each node returned by this hook,
called with the node that is the target of @code{join_node} operation.

The following limitations apply:

@enumerate
@item All nodes must have the same cluster ID,
or join operation is aborted.

@item This hook runs @emph{once},
during the join operation.

If more nodes that are not classy-aware join or leave the cluster,
classy will ignore these changes.

Note that the fallback mechanism is meant for rolling upgrade.
It implies that all nodes subsequently update to classy-aware code.
@end enumerate
""".
-spec fallback_get_peer_nodes(fun((node()) -> {ok, [node()]} | undefined)) -> classy_hook:hook().
fallback_get_peer_nodes(Hook) ->
  classy_hook:insert(?fallback_get_peer_nodes, Hook, 0).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

%%================================================================================
%% Unit tests
%%================================================================================


-ifdef(TEST).

quorum_test() ->
  ?assertEqual(1, quorum(1)),
  ?assertEqual(2, quorum(2)),
  ?assertEqual(2, quorum(3)),
  ?assertEqual(3, quorum(4)),
  ?assertEqual(3, quorum(5)),
  ?assertEqual(4, quorum(6)),
  ?assertEqual(4, quorum(7)).

fault_tolerance_test() ->
  ?assertEqual(0, fault_tolerance(1)),
  ?assertEqual(0, fault_tolerance(2)),
  ?assertEqual(1, fault_tolerance(3)),
  ?assertEqual(1, fault_tolerance(4)),
  ?assertEqual(2, fault_tolerance(5)),
  ?assertEqual(2, fault_tolerance(6)),
  ?assertEqual(3, fault_tolerance(7)).

-endif.
