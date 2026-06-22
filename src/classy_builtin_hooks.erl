%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_builtin_hooks).

%% API:
-export([ gen_random_site_id/0
        , maybe_reinitialize_after_kick/3
        , log_create_site/1
        , log_create_cluster/2
        , log_pre_join/4
        , log_post_join/4
        , log_membership_change/4
        , log_run_level/2
        , log_peer_connection_change/3
        , log_peer_liveness_change/2
        , log_peer_restart/2
        , log_peer_node_change/3
        ]).

-include("classy_internal.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% API functions
%%================================================================================

%% @doc Initialize new site with a random site ID.
gen_random_site_id() ->
  ?tp(classy_on_node_init,
      #{ node => node()
       }),
  classy_node:maybe_init_the_site(undefined).

%% @doc Create a new cluster after getting kicked.
maybe_reinitialize_after_kick(OldCluster, Local, Intent) ->
  ?tp(info, classy_kicked_from_cluster,
      #{ old_cluster => OldCluster
       , local => Local
       , intent => Intent
       }),
  %% Re-initialize the local cluster upon getting kicked:
  Intent =/= join andalso
    classy_node:maybe_init_the_site(Local).

log_create_site(Site) ->
  ?tp(info, classy_create_new_site,
      #{ local => Site
       }).

log_create_cluster(Cluster, Site) ->
  ?tp(info, classy_create_new_cluster,
      #{ cluster => Cluster
       , local => Site
       }).

log_pre_join(Cluster, Remote, Node, UserArg) ->
  ?tp(debug, classy_pre_join_node,
      #{ cluster => Cluster
       , remote => Remote
       , remote_node => Node
       , user_arg => UserArg
       }).

log_post_join(Cluster, Local, JoinToNode, Intent) ->
  ?tp(notice, classy_joined_cluster,
      #{ cluster => Cluster
       , local => Local
       , joined_to_node => JoinToNode
       , intent => Intent
       }).

log_membership_change(Cluster, Local, Remote, Member) ->
  Kind = case Member of
           true -> classy_member_join;
           false -> classy_member_leave
         end,
  ?tp(info, Kind,
      #{ cluster => Cluster
       , local => Local
       , remote => Remote
       }).

log_run_level(From, To) ->
  ?tp(info, classy_change_run_level,
      #{ from => From
       , to => To
       , local => classy_node:maybe_site()
       }).

log_peer_connection_change(Site, Node, ConnStatus) ->
  Kind = case ConnStatus of
           true  -> classy_peer_connected;
           false -> classy_peer_disconnected
         end,
  Level = case classy_node:maybe_site() of
            Site -> debug;
            _    -> notice
          end,
  ?tp(Level, Kind,
      #{ site => Site
       , node => Node
       }).

log_peer_liveness_change(Peer, IsLive) ->
  case IsLive of
    true ->
      Level = info,
      Kind = classy_peer_up;
    false ->
      Level = warning,
      Kind = classy_peer_down
  end,
  ?tp(Level, Kind, #{site => Peer}).

log_peer_restart(Peer, NRestarts) ->
  ?tp(info, classy_peer_restarted, #{site => Peer, n_restarts => NRestarts}).

log_peer_node_change(Peer, From, To) ->
  ?tp(warning, classy_peer_node_change,
      #{ site => Peer
       , from => From
       , to => To
       }).

%%================================================================================
%% Internal functions
%%================================================================================
