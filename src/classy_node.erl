%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_node).
-moduledoc """
Management of the local site and node.
""".

-behavior(gen_server).

%% API:
-export([ start_link/0
        , maybe_init_the_site/1
        , join_node/3
        , kick_site/2
        , maybe_site/0
        , maybe_cluster/0
        , parent_site/0
        , nodes/1
        , peer_info/0
        , node_of_site/2
        , n_restarts/0
        ]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([hello/0, on_ptab_update/2, notify_mem_deltas/2]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy_internal.hrl").

-compile({no_auto_import, [nodes/1]}).

%%================================================================================
%% Type declarations
%%================================================================================

-define(pt_site, classy_node_the_site).
-define(pt_cluster, classy_node_the_cluster).

-define(SERVER, ?MODULE).

-define(globals, classy_node).
-define(the_site, the_site).
-define(the_cluster, the_cluster).
-define(parent_site, parent_site).

-record(call_join,
        { node :: node()
        , intent :: term()
        , cluster :: classy:cluster_id() | any
        }).
-record(call_kick, {site :: classy:site(), intent :: term()}).
-record(cast_mem_deltas,
        { cluster :: classy:cluster_id()
        , data :: [classy_membership:event()]
        }).

%%================================================================================
%% API functions
%%================================================================================

-doc """
Initialize local site and cluster.

Initialization of the site ID is done as following:

@enumerate
@item If the site ID is already stored in the DB,
then it is kept as is and nothing is done.

@item If the value not stored,
and is provided as a binary argument,
then the argument is used as the new site ID.

@item If the value is not stored,
and the argument is @code{undefined},
then site ID is initialized to a random value.
@end enumerate

When site ID changes,
@ref{classy:on_create_site/2} callback runs.

Cluster ID initialization logic is similar,
but there's no way to customize the initial value.
That has to do with the classy's requirement
that cluster IDs change to an entirely new value when site is kicked.
@code{classy:on_create_cluster/2} hook is called for the new clusters.
""".
-spec maybe_init_the_site(classy:site() | undefined) -> ok.
maybe_init_the_site(MaybeSite) ->
  {_IsNewSite, Site, Ops1} = ensure_the_id(?the_site, ?on_create_site, [], MaybeSite),
  {IsNewCluster, _Cluster, Ops2} = ensure_the_id(?the_cluster, ?on_create_cluster, [Site], undefined),
  Ops3 = case IsNewCluster of
           true  -> [{w, ?parent_site, Site}];
           false -> []
         end,
  {ok, Effects} = classy_table:atomically(?globals, Ops1 ++ Ops2 ++ Ops3),
  [Fun() || Fun <- Effects],
  ok.

-doc false.
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-doc """
Return ID of the cluster that the local site currently belongs to.
""".
-spec maybe_cluster() -> classy:cluster_id() | undefined.
maybe_cluster() ->
  persistent_term:get(?pt_cluster, undefined).

-doc """
Return ID of the local site.
""".
-spec maybe_site() -> classy:site() | undefined.
maybe_site() ->
  persistent_term:get(?pt_site, undefined).

-doc """
Return ID of the site that invited us to @code{the_cluster}.

The return value could be equal to @code{@{ok, the_site()@}} for the site that originally created the cluster.
@code{undefined} return value means the local site is not initialized.
""".
-spec parent_site() -> {ok, classy:site()} | undefined.
parent_site() ->
  case classy_table:lookup(?globals, ?parent_site) of
    [V] -> {ok, V};
    []  -> undefined
  end.

-doc false.
-spec join_node(node(), _Intent, classy:cluster_id() | any) -> ok | {error, _}.
join_node(Node, Intent, ExpectedCluster) ->
  case node() of
    Node ->
      ok;
    _ ->
      gen_server:call(
        ?SERVER,
        #call_join{node = Node, intent = Intent, cluster = ExpectedCluster},
        infinity)
  end.

-doc false.
-spec kick_site(classy:site(), _Intent) -> ok | {error, _}.
kick_site(Site, Intent) ->
  gen_server:call(
    ?SERVER,
    #call_kick{site = Site, intent = Intent},
    infinity).

-doc false.
-spec nodes(all | connected | disconnected) -> [node()].
nodes(Query) ->
  Filter = case Query of
             all          -> [];
             connected    -> [{'=:=', '$2', true}];
             disconnected -> [{'=:=', '$2', false}]
           end,
  MS = { #classy_kv{ v = #site_info{ node = '$1'
                                   , isconn = '$2'
                                   , _ = '_'
                                   }
                   , _ = '_'
                   }
       , [{'=/=', '$1', undefined} | Filter]
       , ['$1']
       },
  ets:select(?site_info, [MS]).

-spec peer_info() -> #{classy:site() => classy:peer_info()}.
peer_info() ->
  ets:foldl(
    fun(#classy_kv{k = Site, v = #site_info{node = Node, isconn = IsConn, last_update = LU}}, Acc) ->
        Info = #{ node        => Node
                , connected   => IsConn
                , last_update => LU
                },
        Acc#{Site => Info}
    end,
    #{},
    ?site_info).

-doc false.
-spec node_of_site(classy:site(), boolean()) -> {ok, node()} | undefined.
node_of_site(Site, OnlyConnected) ->
  case classy_table:lookup(?site_info, Site) of
    [#site_info{isconn = IsConnected, node = Node}] when IsConnected; not OnlyConnected ->
      {ok, Node};
    _ ->
      undefined
  end.

-doc """
Return number of node restarts since creation of the site.

This value is monotonically increasing.
""".
-spec n_restarts() -> {ok, non_neg_integer()} | {error, nodedown}.
n_restarts() ->
  case classy_table:lookup(?globals, ?n_restarts) of
    [N] ->
      {ok, N};
    _ ->
      {error, nodedown}
  end.

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { cluster :: classy:cluster_id() | undefined
        , site :: classy:site()
        }).

-doc false.
init(_) ->
  process_flag(trap_exit, true),
  net_kernel:monitor_nodes(
    true,
    #{ node_type => visible
     , nodedown_reason => true
     }),
  ok = classy_table:open(?globals, #{on_update => fun ?MODULE:on_ptab_update/2}),
  ok = classy_table:open(?site_info, #{ets_options => [{read_concurrency, true}]}),
  increase_n_restarts(),
  classy_hook:foreach(?on_node_init, []),
  case init_cluster() of
    {ok, _} = Ok ->
      Ok;
    {error, Reason} ->
      {stop, Reason, undefined}
  end.

-doc false.
handle_call(#call_join{} = Call, _From, S0) ->
  case handle_join(S0, Call) of
    {ok, S} ->
      {reply, ok, S};
    Err ->
      {reply, Err, S0}
  end;
handle_call(#call_kick{site = Target, intent = Intent}, _From, S) ->
  Ret =
    maybe
      {ok, Cluster} ?= the_cluster(),
      {ok, Local} ?= the_site(),
      handle_kick(Cluster, Local, Target, Intent)
    else
      _ -> {error, local_not_in_cluster}
    end,
  {reply, Ret, S};
handle_call(Call, From, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => call
       , from => From
       , content => Call
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

-doc false.
handle_cast(#cast_mem_deltas{} = Cast, S) ->
  handle_membership_change_event(Cast, S);
handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

-doc false.
handle_info({NodeUpOrDown, _Node, _}, S) when NodeUpOrDown =:= nodeup; NodeUpOrDown =:= nodedown ->
  {noreply, update_runtime(S)};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

-doc false.
terminate(Reason, S) ->
  classy_lib:is_normal_exit(Reason) orelse
    ?tp(warning, ?classy_abnormal_exit,
        #{ server => ?MODULE
         , reason => Reason
         }),
  update_liveness_info(S, false),
  classy_table:stop(?globals, 1_000),
  classy_table:stop(?site_info, 1_000),
  sync_set_run_level(?stopped),
  persistent_term:erase(?pt_site),
  persistent_term:erase(?pt_cluster).

%%================================================================================
%% Internal exports
%%================================================================================

%%  RPC target, called by remote node during `join'.
-doc false.
%% Returns information about the local site, used for bootstrapping the remote.
hello() ->
  maybe
    {ok, Cluster} ?= the_cluster(),
    {ok, Site} ?= the_site(),
    {ok, MemData} ?= classy_membership:get_data(Cluster, Site, 0, 0),
    #{ site => Site
     , cluster => Cluster
     , pid => whereis(?SERVER)
     , mem_data => MemData
     }
  else
    undefined ->
      {error, not_in_cluster};
    Err ->
      Err
  end.

-doc false.
on_ptab_update(_, Op) ->
  case Op of
    {w, ?the_cluster, Val} ->
      persistent_term:put(?pt_cluster, Val);
    {d, ?the_cluster} ->
      persistent_term:erase(?pt_cluster);
    {w, ?the_site, Val} ->
      persistent_term:put(?pt_site, Val);
    {d, ?the_site} ->
      persistent_term:erase(?pt_site);
    _ ->
      ok
  end.

-doc false.
-spec notify_mem_deltas(classy:cluster_id(), [classy_membership:event()]) -> ok.
notify_mem_deltas(Cluster, Deltas) ->
  gen_server:cast(
    ?SERVER,
    #cast_mem_deltas{ cluster = Cluster
                    , data = Deltas
                    }).

%%================================================================================
%% Internal functions
%%================================================================================

handle_membership_change_event(
  #cast_mem_deltas{ cluster = Cluster
                  , data = Deltas
                  },
  S0 = #s{cluster = ThisCluster, site = Local}
 ) ->
  ?tp(debug, membership_change,
      #{ cluster => Cluster
       , origin => Local
       , data => Deltas
       }),
  if Cluster =:= ThisCluster ->
      case apply_deltas_with_effects(Deltas, S0) of
        {ok, S}      -> {noreply, S};
        {error, Err} -> {stop, Err, undefined}
      end;
     true ->
      %% Update from the old cluster. Ignore it.
      {noreply, S0}
  end.

-spec update_runtime(#s{}) -> #s{}.
update_runtime(S) ->
  adjust_run_level(update_sites_status(S)).

handle_kick(Cluster, Local, Target, Intent) ->
  case classy_hook:all(?on_pre_kick, [Cluster, Target, Intent]) of
    ok ->
      Ret = classy_membership:set_member(Cluster, Local, Target, false),
      classy_membership:flush(Cluster, Local),
      Ret;
    Err ->
      Err
  end.

handle_join(S, Call) ->
  #call_join{ node    = Node
            , cluster = ExpectedCluster
            , intent  = Intent
            } = Call,
  case rpc:call(Node, ?MODULE, hello, [], classy_lib:rpc_timeout()) of
    #{ site := Remote
     , cluster := Cluster
     , pid := _RemotePid
     , mem_data := MemData
     } when Cluster =:= ExpectedCluster;
            ExpectedCluster =:= any ->
      case classy_hook:all(?on_pre_join, [Cluster, Remote, Node, Intent]) of
        ok ->
          Res =
            global:trans(
              {classy_node_join_lock, self()},
              fun() ->
                  do_join_node(Node, Cluster, Remote, MemData, Intent, S)
              end,
              [node(), Node],
              1),
          case Res of
            aborted ->
              {error, aborted};
            _ ->
              Res
          end;
        {error, _} = Err ->
          Err
      end;
    #{cluster := Cluster} ->
      {error, {cluster_changed, #{ExpectedCluster => Cluster}}};
    {error, _} = Err ->
      Err;
    Err ->
      {error, {bad_hello, Err}}
  end.

-spec do_join_node(
        node(),
        classy:cluster_id(),
        classy:site(),
        classy_membership:sync_data(),
        classy:join_intent(),
        #s{}
       ) ->
        {ok, #s{}} | {error, _}.
do_join_node(Node, Cluster, Remote, MemData, JoinIntent, S0) ->
  {ok, Local} = the_site(),
  case the_cluster() of
    {ok, Cluster} ->
      %% Already in the same cluster with `Node'. Set our membership
      %% status and trigger re-sync (do we need to re-run hooks?):
      classy_membership:call_sync(Cluster, Local, MemData),
      classy_membership:set_member(Cluster, Local, Local, true),
      classy_membership:flush(Cluster, Local),
      {ok, update_runtime(S0)};
    {ok, OldCluster} when OldCluster =/= Cluster ->
      %% Site is currently in a different cluster. Leave it first:
      LeaveIntent = join,
      case handle_kick(OldCluster, Local, Local, LeaveIntent) of
        ok ->
          {ok, S} = on_leave(S0, LeaveIntent),
          do_join_node(Node, Cluster, Remote, MemData, JoinIntent, S);
        Err ->
          Err
      end;
    undefined ->
      %% Site is not in any cluster:
      {ok, S} = join_cluster(Cluster, Node, Local, Remote, JoinIntent, S0),
      do_join_node(Node, Cluster, Remote, MemData, JoinIntent, S)
  end.

on_leave(S = #s{cluster = Cluster, site = Local}, Intent) ->
  sync_set_run_level(?stopped),
  %% Sync with the business apps:
  classy_table:delete(?globals, ?the_cluster),
  classy_hook:foreach(?on_post_kick, [Cluster, Local, Intent]),
  classy_table:clear(?site_info),
  case Intent of
    join ->
      {ok, S#s{cluster = undefined}};
    _ ->
      init_cluster()
  end.

-spec join_cluster(classy:cluster_id(), node(), classy:site(), classy:site(), classy:join_intent(), #s{}) -> {ok, #s{}}.
join_cluster(Cluster, JoinToNode, Local, Remote, Intent, S = #s{}) ->
  {ok, _} = classy_sup:ensure_membership(Cluster, Local),
  classy_hook:foreach(?on_post_join, [Cluster, Local, JoinToNode, Intent]),
  classy_table:dirty_write(?globals, ?the_cluster, Cluster),
  classy_table:dirty_write(?globals, ?parent_site, Remote),
  classy_table:flush(?globals),
  {ok, S#s{cluster = Cluster}}.

%% Update node tracking information
-spec update_sites_status(#s{}) -> #s{}.
update_sites_status(S) ->
  Batch =
    ets:foldl(
      fun(#classy_kv{k = Peer, v = SiteInfo}, Acc) ->
          update_site_info(Peer, SiteInfo, S) ++ Acc
      end,
      [],
      ?site_info),
  {ok, Effects} = classy_table:atomically(?site_info, Batch),
  run_site_info_effects(Effects),
  S.

init_cluster() ->
  maybe
    {ok, Cluster} ?= the_cluster(),
    {ok, Site} ?= the_site(),
    logger:update_process_metadata(#{local => Site}),
    {ok, _} = classy_sup:ensure_membership(Cluster, Site),
    start_old_clusters(Site),
    S = update_runtime(
          #s{ cluster = Cluster
            , site = Site
            }),
    update_liveness_info(S, true),
    ?tp(debug, classy_init_clustering, #{local => Site, cluster => Cluster}),
    {ok, S}
  else
    _ ->
      {error, default_site_not_initialized}
  end.

-spec ensure_the_id(?the_cluster | ?the_site, ?on_create_cluster | ?on_create_site, list(), binary() | undefined) ->
        { boolean()
        , binary()
        , [classy_table:atomic_op(fun(() -> _))]
        }.
ensure_the_id(Key, OnCreateHook, HookArgs, Default) ->
  case classy_table:lookup(?globals, Key) of
    [Bin] when is_binary(Bin) ->
      {false, Bin, []};
    [] ->
      case Default of
        undefined ->
          Val = binary:encode_hex(crypto:strong_rand_bytes(32), uppercase);
        Val when is_binary(Val) ->
          ok
      end,
      { true
      , Val
      , [ {w, Key, Val}
        , {then, fun() ->
                     classy_hook:foreach(OnCreateHook, [Val | HookArgs])
                 end}
        ]
      }
  end.

-spec adjust_run_level(#s{}) -> #s{}.
adjust_run_level(S = #s{cluster = Cluster, site = Site}) ->
  NKnown = length(classy_membership:members(Cluster, Site)),
  NConnected = length(nodes(connected)),
  RunLevel = case NKnown >= classy_lib:n_sites() of
               true  ->
                 case NConnected >= classy:quorum(config) of
                   true  -> ?quorum;
                   false -> ?cluster
                 end;
               false -> ?single
             end,
  set_run_level(RunLevel),
  %% Propagate info to peers:
  Info = classy_hook:fold(?on_enrich_site_info, [], #{rl => RunLevel, vsn => ?classy_proto_vsn}),
  classy_membership:set_info(Cluster, Site, Info),
  S.

%% Start membership processes for all known former clusters, in order
%% to relay information to former peers.
start_old_clusters(Site) ->
  maps:foreach(
    fun(Cluster, Peers) ->
        case Peers -- [Site] of
          []      -> ok;
          [_ | _] -> classy_sup:ensure_membership(Cluster, Site)
        end
    end,
    classy_membership:known_clusters(Site)).

update_liveness_info(#s{cluster = Cluster, site = Site}, Running) ->
  {ok, NR} = n_restarts(),
  classy_membership:set_liveness(Cluster, Site, Site, NR, Running).

-spec increase_n_restarts() -> ok.
increase_n_restarts() ->
  N = case classy_table:lookup(?globals, ?n_restarts) of
        [N0] when is_integer(N0) ->
          N0 + 1;
        [] ->
          0;
        Other ->
          ?tp(warning, ?classy_bad_data,
              #{ table => ?globals
               , key   => ?n_restarts
               , val   => Other
               }),
          0
      end,
  classy_table:write(?globals, ?n_restarts, N).

sync_set_run_level(Level) ->
  classy_rl_changer:set_sync(Level, infinity).

the_cluster() ->
  case classy_table:lookup(?globals, ?the_cluster) of
    [V] ->
      {ok, V};
    [] ->
      undefined
  end.

the_site() ->
  case classy_table:lookup(?globals, ?the_site) of
    [V] ->
      {ok, V};
    [] ->
      undefined
  end.

-spec apply_deltas_with_effects([classy_membership:event()], #s{}) -> {ok, #s{}} | {error, _}.
apply_deltas_with_effects(Deltas, S0 = #s{cluster = Cluster, site = Local}) ->
  {Updated, Kicked} = merge_deltas(Deltas),
  {ok, MyNR} = n_restarts(),
  case Kicked of
    #{Local := _} ->
      %% We got kicked remotely. In this case we don't bother
      %% importing the data and running the hooks, and go straight to
      %% `on_leave':
      ?tp(warning, classy_kicked_remotely,
          #{ cluster => Cluster
           , local   => Local
           }),
      case on_leave(S0, kicked) of
        {ok, S}      -> {ok, S};
        {error, Err} -> {stop, Err, undefined}
      end;
   #{} ->
      maybe
        {ok, S} ?= import_deltas(Updated, Kicked, S0),
        case Updated of
          #{Local := #site_info{isup = false, nrestarts = NR}} when NR >= MyNR ->
            %% Handle network partition; peers decided that we're down:
            ?tp(warning, classy_restarted_remotely,
                #{ cluster => Cluster
                 , local   => Local
                 }),
            on_remote_restart(S);
          _ ->
            %% Nothing happened:
            {ok, S}
        end
      end
  end.

-spec on_remote_restart(_) -> no_return().
on_remote_restart(S) ->
  %% TODO
  {ok, S}.

-spec import_deltas( #{classy:site() => #site_info{}}, #{classy:site() => true}, #s{}) ->
        {ok, #s{}} | {error, _}.
import_deltas(Updated, Kicked, S0 = #s{cluster = Cluster, site = Local}) ->
  %% 1. Process kicked nodes:
  Batch1 =
    maps:fold(
      fun(Peer, _, Acc) ->
          [ {d, Peer}
          , {then, {?on_membership_change, [Cluster, Local, Peer, false]}}
          | Acc
          ]
      end,
      [],
      Kicked),
  %% 2. Process updated nodes:
  Batch =
    maps:fold(
      fun(Peer, NewInfo, Acc) ->
          case Kicked of
            #{Peer := _} ->
              %% Ignore kicked sites:
              Acc;
            #{} ->
              update_site_info(Peer, NewInfo, S0) ++ Acc
          end
      end,
      Batch1,
      Updated),
  maybe
    {ok, Effects} ?= classy_table:atomically(?site_info, Batch),
    run_site_info_effects(Effects),
    %% TODO: update peer info or delete it
    {ok, adjust_run_level(S0)}
  end.

%% 1. Calculate connectivity to the node
%% 2. Diff the current information with the past
%% 3. Run the hooks if the site's status changes
%% 4. Schedule writing of the updated data to the DB
update_site_info(Peer, New0 = #site_info{isup = IsUp, nrestarts = NR}, #s{cluster = Cluster, site = Local}) ->
  Node = maps:get(Peer, classy_membership:node_of_site(Cluster, Local), undefined),
  IsConn = lists:member(Node, [node() | nodes()]),
  New = New0#site_info{isconn = IsConn, node = Node},
  case classy_table:lookup(?site_info, Peer) of
    [New] ->
      %% No change:
      [];
    Other  ->
      case Other of
        [#site_info{isup = IsUp0, nrestarts = NR0, node = Node0, isconn = IsConn0}] ->
          ok;
        [] ->
          IsUp0 = false,
          IsConn0 = false,
          %% If we haven't seen this peer before, do not report it as
          %% restarted:
          NR0 = NR,
          %% Do not report changed host as well:
          Node0 = Node
      end,
      %% Note: hooks are executed immediately, before data is written
      %% to the storage. It means that if the server is interrupted
      %% before the data is written, certain hooks will fire multiple
      %% times. This is what we want, in fact.
      case Other of
        [] -> classy_hook:foreach(?on_membership_change, [Cluster, Local, Peer, true]);
        _  -> ok
      end,
      if Peer =/= Local, IsUp0 =/= IsUp ->
          classy_hook:foreach(?on_peer_liveness_change, [Peer, IsUp]);
         true ->
          ok
      end,
      if Node =/= Node0 ->
          classy_hook:foreach(?on_peer_node_change, [Peer, Node0, Node]);
         true ->
            []
      end,
      if Peer =/= Local, NR > NR0, IsUp ->
          classy_hook:foreach(?on_peer_restart, [Peer, NR]);
         true ->
          []
      end,
      if Peer =/= Local, IsConn0 =/= IsConn ->
          classy_hook:foreach(?on_peer_connection_status_change, [Peer, Node, IsConn]);
         true ->
          []
      end,
      %% Schedule saving of the data:
      [{w, Peer, New#site_info{last_update = classy_lib:time_s()}}]
    end.

run_site_info_effects(Effects) ->
  [classy_hook:foreach(Hookpoint, Args) || {Hookpoint, Args} <:- Effects],
  ok.

-spec merge_deltas([classy_membership:event()]) -> {Updated, Kicked} when
    Updated :: #{classy:site() => #site_info{}},
    Kicked :: #{classy:site() => true}.
merge_deltas(Data) ->
  merge_deltas(Data, #{}, #{}).

-spec merge_deltas([classy_membership:event()], Updated, Kicked) -> {Updated, Kicked} when
    Updated :: #{classy:site() => #site_info{}},
    Kicked :: #{classy:site() => true}.
merge_deltas([], Updated, Kicked) ->
  {Updated, Kicked};
merge_deltas([Up | Rest], Updated0, Kicked0) ->
  Get = fun(Peer) ->
            case Updated0 of
              #{Peer := Val} ->
                Val;
              #{} ->
                case classy_table:lookup(?site_info, Peer) of
                  [Val] -> Val;
                  [] -> default_site_info()
                end
            end
        end,
  case Up of
    {mem, Peer, true} ->
      Updated = Updated0#{Peer => Get(Peer)},
      Kicked = maps:remove(Peer, Kicked0);
    {mem, Peer, false} ->
      Updated = Updated0,
      Kicked = Kicked0#{Peer => true};
    {host, Peer, Host} ->
      Info0 = Get(Peer),
      Info = Info0#site_info{node = Host},
      Updated = Updated0#{Peer => Info},
      Kicked = Kicked0;
    {meta, Peer, Meta} ->
      Info0 = Get(Peer),
      Info = Info0#site_info{meta = Meta},
      Updated = Updated0#{Peer => Info},
      Kicked = Kicked0;
    {liveness, Peer, IsUp, NRestarts} ->
      Info0 = Get(Peer),
      Info = Info0#site_info{ isup = IsUp
                            , nrestarts = NRestarts
                            },
      Updated = Updated0#{Peer => Info},
      Kicked = Kicked0
  end,
  merge_deltas(Rest, Updated, Kicked).

default_site_info() ->
  #site_info{ isconn = false
            , isup = false
            , last_update = classy_lib:time_s()
            }.

-ifndef(TEST).
%% In real live we change levels async-ly:
set_run_level(Level) ->
  classy_rl_changer:set(Level).
-else.
%% In the tests we want to sequence the events.
set_run_level(Level) ->
  ok = classy_rl_changer:set_sync(Level, 5_000),
  ok.
-endif.
