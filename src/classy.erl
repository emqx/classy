%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Main interface module of `classy'.
%%
%% Note: business releases can install hooks by setting
%% `classy:setup_hooks' application environment variable to a tuple
%% `{Module, Function, Args}'. This MFA can contain calls to various
%% `classy:on_...' functions.
-module(classy).

%% API:
-export([ info/0
        , info/1
        , info/2
        , node_of_site/2
        , join_node/2
        , kick_site/2
        , kick_node/2
        , sites/0
        , nodes/1
        , quorum/1
        , fault_tolerance/1
        , at_lower_level/2
        ]).

-export([ on_node_init/2
        , on_create_cluster/2
        , on_create_site/2
        , on_peer_connection_status_change/2
        , on_membership_change/2
        , pre_join/2
        , post_join/2
        , pre_kick/2
        , post_kick/2
        , pre_autoclean/2
        , pre_autocluster/2
        , run_level/2
        , enrich_site_info/2
        ]).

-export_type([ cluster_id/0
             , site/0

             , peer_info/0
             , info/0
             , cluster_info/0

             , run_level/0
             , membership_change_hook/0
             ]).

-include("classy_internal.hrl").
-compile({no_auto_import, [nodes/1]}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type cluster_id() :: binary().

-type site() :: binary().

-type peer_info() ::
        #{ node        := node() | undefined
         , up          := boolean()
         , last_update := classy_lib:unix_time_s()
         }.

-type info() ::
        #{ cluster     := cluster_id() | undefined
         , site        := site() | undefined
         , last_update := classy_lib:unix_time_s() | undefined
         , peers       := #{site() => peer_info()}
         , atom()      => _
         }.

-type cluster_info() ::
        #{ infos     := #{node() => info()}
         , bad_nodes := #{node() => _}
         }.

-type membership_change_hook() :: fun((cluster_id(), _Local :: site(), _Remote :: site(), _IsMember :: boolean()) -> _).

-type join_intent() :: join
                     | autocluster
                     | _.

-type kick_intent() :: join       %% Intent set by system when site leaves the cluster to join another one
                     | kicked     %% Intent set by system when site is kicked by a third party
                     | autoclean  %% Intent set by the system when the site is kicked by autoclean
                     | _.

-type run_level() :: stopped | single | cluster | quorum.

%%================================================================================
%% API functions
%%================================================================================

%% RPC target
%% @doc Provide general information about the local node.
-spec info() -> info().
info() ->
  %% Note: this is an RPC target.
  case classy_node:the_cluster() of
    {ok, MaybeCluster} -> ok;
    _                  -> MaybeCluster = undefined
  end,
  case classy_node:the_site() of
    {ok, MaybeSite} -> ok;
    _               -> MaybeSite = undefined
  end,
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
         },
  classy_hook:fold(?on_enrich_site_info, [], Acc).

%% @doc Gather `info/0' from a set of nodes.
%%
%% The nodes don't have to be in the same cluster.
%%
%% WARNING: as a side effect of calling this function,
%% the current node will establish Erlang distribution connection to all nodes in the list.
-spec info([node()]) -> cluster_info().
info(Nodes) ->
  info(1, Nodes).

%% @hidden
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

%% @doc Locate a node that is currently hosting a site.
%%
%% If `OnlyLive' flag is set, undefined is returned when the site is
%% down (even if its node is otherwise known).
-spec node_of_site(site(), boolean()) -> {ok, node()} | undefined.
node_of_site(Site, OnlyLive) ->
  classy_node:node_of_site(Site, OnlyLive).

%%--------------------------------------------------------------------------------
%% Cluster management
%%--------------------------------------------------------------------------------

%% @doc Join the local site to the cluster of a remote node.
%%
%% This function allows a node to join a cluster by connecting to a known peer.
%%
%% @param Node The node to join to
%% @param Intent The intent of the join operation.
%% Intent is an arbitrary term passed to `pre_join' callback.
%% The callback is free to interpret it according to the business
%% logic requirements.
-spec join_node(node(), join_intent()) -> ok | {error, _}.
join_node(Node, Intent) ->
  classy_node:join_node(Node, Intent, any).

-spec kick_site(site(), kick_intent()) -> ok | {error, _}.
kick_site(Site, Intent) ->
  classy_node:kick_site(Site, Intent).

-spec kick_node(node(), kick_intent()) -> ok | {error, _}.
kick_node(Node, Intent) ->
  case {classy_node:the_cluster(), classy_node:the_site()} of
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

%% @doc List all peers
-spec sites() -> [site()].
sites() ->
  maybe
    {ok, Cluster} ?= classy_node:the_cluster(),
    {ok, Local} ?= classy_node:the_site(),
    classy_membership:members(Cluster, Local)
  else
    _ ->
      []
  end.

-spec nodes(all | running | stopped) -> [node()].
nodes(Query) ->
  classy_node:nodes(Query).

%% @doc Lower the run level to the given value and run the specified function.
%%
%% This function can be used to implement migrations that require
%% business applications to be stopped.
-spec at_lower_level(classy_node:run_level_atom(), fun(() -> Ret)) ->
        {ok, Ret} |
        {error | exit | throw, _Reason, _Stacktrace}.
at_lower_level(RunLevel, Fun) ->
  classy_node:at_lower_level(RunLevel, Fun).

%%--------------------------------------------------------------------------------
%% Misc.
%%--------------------------------------------------------------------------------

%% @doc Calculate the number of nodes required for the quorum:
%%
%% <itemize>
%% <li>`Integer': any integer value</li>
%% <li>`config': Return value of `classy.quorum' application environment variable</li>
%% <li>`running': Quorum among the running sites, not less than `quorum(config)'</li>
%% </itemize>
-spec quorum(config | running | non_neg_integer()) -> pos_integer().
quorum(N) when is_integer(N), N >= 0 ->
  N div 2 + 1;
quorum(config) ->
  max(1, application:get_env(classy, quorum, 1));
quorum(running) ->
  max(
    quorum(length(nodes(running))),
    quorum(config)).

%% @doc Calculate how many nodes can be down, while cluster still maintains quorum.
fault_tolerance(N) ->
  N - quorum(N).

%%--------------------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------------------

%% @doc Register a hook that is executed when the node (not the site)
%% starts. It is called before `the_site' and `the_cluster' are
%% initialized and can be used to override the default cluster and
%% site initialization logic.
-spec on_node_init(fun(() -> _), classy_hook:prio()) -> classy_hook:hook().
on_node_init(Hook, Prio) ->
  classy_hook:insert(?on_node_init, Hook, Prio).

%% @doc This callback is called once per cluster by the site that
%% originally creates the cluster.
-spec on_create_cluster(fun((cluster_id(), Local) -> _), classy_hook:prio()) ->
        classy_hook:hook()
  when Local :: site().
on_create_cluster(Hook, Prio) ->
  classy_hook:insert(?on_create_cluster, Hook, Prio).

%% @doc This callback is called once per site.
-spec on_create_site(fun((site()) -> _), classy_hook:prio()) -> classy_hook:hook().
on_create_site(Hook, Prio) ->
  classy_hook:insert(?on_create_site, Hook, Prio).

%% @doc Register a hook that is executed when a site changes
%% status from connected (`true') to disconnected (`false') and vice versa.
%%
%% Note: this hook runs in the classy main process.
%% Hence it should avoid blocking it.
%%
%% Note: status change to `false' it not indicative of the remote node
%% being actually down. This can happen during a network partition.
-spec on_peer_connection_status_change(Fun, classy_hook:prio()) -> classy_hook:hook()
   when Fun :: fun((cluster_id(), Local, Remote, node(), _IsConnected :: boolean()) -> _),
        Local :: site(),
        Remote :: site().
on_peer_connection_status_change(Hook, Prio) ->
  classy_hook:insert(?on_peer_connection_status_change, Hook, Prio).

%% @doc Register a hook that is executed when a site joins or leaves a cluster.
-spec on_membership_change(membership_change_hook(), classy_hook:prio()) -> classy_hook:hook().
on_membership_change(Hook, Prio) ->
  classy_hook:insert(?on_membership_change, Hook, Prio).

%% @doc Register a hook that is executed before the local node joins a
%% remote site and/or cluster. WARNING: this hook should not have side
%% effects. It should only check if it is ok to join.
-spec pre_join(
        fun((cluster_id(), Remote, node(), join_intent()) -> ok | {error, _}),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Remote :: site().
pre_join(Hook, Prio) ->
  classy_hook:insert(?on_pre_join, Hook, Prio).

%% @doc Register a hook that is executed after the local site joins a
%% cluster.
-spec post_join(
        fun((cluster_id(), Local, JoinedTo) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Local :: site(),
       JoinedTo :: node().
post_join(Hook, Prio) ->
  classy_hook:insert(?on_post_join, Hook, Prio).

%% @doc Register a hook that verifies whether or not a site can be
%% kicked from the cluster. This hook runs on the node that initiates
%% the kick.
%%
%% WARNING: this hook cannot have side effects.
-spec pre_kick(
        fun((cluster_id(), Remote, kick_intent()) -> ok | {error, _}),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Remote :: site().
pre_kick(Hook, Prio) ->
  classy_hook:insert(?on_pre_kick, Hook, Prio).

%% @doc Register a hook that is executed after the local site leaves a
%% cluster. This hook can perform destructive actions associated with
%% cleanup.
-spec post_kick(
        fun((OldCluster, Local, kick_intent()) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when OldCluster :: cluster_id(),
       Local :: site().
post_kick(Hook, Prio) ->
  classy_hook:insert(?on_post_kick, Hook, Prio).

%% @doc Register a hook that runs before autoclean finalizes the
%% decision to kick a down site.
%%
%% WARNING: this hook cannot have side effects.
-spec pre_autoclean(
        fun((Remote) -> ok | {error, _}),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Remote :: site().
pre_autoclean(Hook, Prio) ->
  classy_hook:insert(?on_pre_autoclean, Hook, Prio).

%% @doc Register a hook that filters and ranks nodes for autocluster.
%% It allows the business code to pick the most appropriate cluster for automatic join.
%%
%% WARNING: this hook cannot have side effects.
-spec pre_autocluster(
        fun((cluster_info(), Discovered) -> Discovered),
        classy_hook:prio()
       ) -> classy_hook:hook()
  when Discovered :: [{cluster_id(), [node()]}].
pre_autocluster(Hook, Prio) ->
  classy_hook:insert(?on_pre_autocluster, Hook, Prio).

%% @doc Register a hook that is executed on change of the run level of
%% the local site.
-spec run_level(
        fun((run_level(), run_level()) -> _),
        classy_hook:prio()
       ) -> classy_hook:hook().
run_level(Hook, Prio) ->
  classy_hook:insert(?on_change_run_level, Hook, Prio).

%% @doc Register a hook that can add entries to the map returned by `info/0'.
-spec enrich_site_info(
        fun((info()) -> info()),
        classy_hook:prio()
       ) -> classy_hook:hook().
enrich_site_info(Hook, Prio) ->
  classy_hook:insert(?on_enrich_site_info, Hook, Prio).

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
