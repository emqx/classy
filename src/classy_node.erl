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
-export([hello/0, on_ptab_update/2]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy_internal.hrl").

-compile({no_auto_import, [nodes/1]}).

%%================================================================================
%% Type declarations
%%================================================================================

-define(pt_site, classy_node_the_site).
-define(pt_cluster, classy_node_the_cluster).

-define(SERVER, ?MODULE).

-define(ptab, classy_node).
-define(the_site, the_site).
-define(the_cluster, the_cluster).
-define(parent_site, parent_site).

-record(call_join,
        { node :: node()
        , intent :: term()
        , cluster :: classy:cluster_id() | any
        }).
-record(call_kick, {site :: classy:site(), intent :: term()}).
-record(cast_membership_change,
        { cluster :: classy:cluster_id()
        , local :: classy:site()
        , remote :: classy:site()
        , member :: boolean()
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
  {ok, Effects} = classy_table:atomically(?ptab, Ops1 ++ Ops2 ++ Ops3),
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
  case classy_table:lookup(?ptab, ?parent_site) of
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
  case classy_table:lookup(?ptab, ?n_restarts) of
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
        , peer_state = #{} :: #{classy:site() => {node(), boolean()}}
        }).

-doc false.
init(_) ->
  process_flag(trap_exit, true),
  net_kernel:monitor_nodes(
    true,
    #{ node_type => visible
     , nodedown_reason => true
     }),
  ok = classy_table:open(?ptab, #{on_update => fun ?MODULE:on_ptab_update/2}),
  ok = classy_table:open(?site_info, #{ets_options => [{read_concurrency, true}]}),
  classy:on_membership_change(fun on_membership_change/4, -100),
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
handle_cast(#cast_membership_change{} = Cast, S) ->
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
terminate(Reason, _S) ->
  classy_lib:is_normal_exit(Reason) orelse
    ?tp(warning, ?classy_abnormal_exit,
        #{ server => ?MODULE
         , reason => Reason
         }),
  classy_table:stop(?ptab, 1_000),
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

%%================================================================================
%% Internal functions
%%================================================================================

on_membership_change(Cluster, Local, Remote, Member) ->
  gen_server:cast(?SERVER,
                  #cast_membership_change{ cluster = Cluster
                                         , local = Local
                                         , remote = Remote
                                         , member = Member
                                         }).

handle_membership_change_event(
  #cast_membership_change{ cluster = Cluster
                         , local = Local
                         , remote = Remote
                         , member = Member
                         },
  S0 = #s{cluster = ThisCluster, site = ThisSite}
 ) ->
  ?tp(debug, membership_change,
      #{ cluster => Cluster
       , origin => Local
       , target => Remote
       , member => Member
       }),
  if Cluster =:= ThisCluster,
     Local =:= ThisSite,
     Remote =:= ThisSite,
     Member =:= false ->
      %% We got kicked:
      ?tp(warning, classy_kicked_remotely,
          #{ cluster => Cluster
           , local   => ThisSite
           }),
      case on_leave(S0, kicked) of
        {ok, S}      -> {noreply, S};
        {error, Err} -> {stop, Err, undefined}
      end;
     Cluster =:= ThisCluster ->
      {noreply, update_runtime(S0)};
     true ->
      {noreply, S0}
  end.

-spec update_runtime(#s{}) -> #s{}.
update_runtime(S0) ->
  S = update_sites_status(S0),
  adjust_run_level(S).

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
  set_run_level(?stopped),
  %% Sync with the business apps:
  _ = classy_rl_changer:at_lower_level(?stopped, fun() -> ok end),
  classy_table:delete(?ptab, ?the_cluster),
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
  classy_table:dirty_write(?ptab, ?the_cluster, Cluster),
  classy_table:dirty_write(?ptab, ?parent_site, Remote),
  classy_table:flush(?ptab),
  {ok, S#s{cluster = Cluster, peer_state = #{}}}.

%% Update node tracking information
-spec update_sites_status(#s{}) -> #s{}.
update_sites_status(S0 = #s{cluster = Cluster, site = Local}) ->
  %% Gather data:
  Nodes = [node() | erlang:nodes()],
  Members = classy_membership:members(Cluster, Local),
  NodesOfSite = classy_membership:node_of_site(Cluster, Local),
  %% Update members:
  S1 = lists:foldl(
         fun(Site, Acc) ->
             case NodesOfSite of
               #{Site := Node} ->
                 IsConn = lists:member(Node, Nodes);
               #{} ->
                 Node = undefined,
                 IsConn = false
             end,
             case classy_table:lookup(?site_info, Site) of
               [#site_info{isconn = IsConn, node = Node}] ->
                 %% No changes:
                 ok;
               _ ->
                 classy_table:dirty_write(
                   ?site_info,
                   Site,
                   #site_info{ isconn = IsConn
                             , node = Node
                             , last_update = classy_lib:time_s()
                             })
             end,
             maybe_on_peer_connection_status_change(Acc, Site, Node, IsConn, true)
        end,
        S0,
        Members),
  %% Delete info of gone members:
  S = ets:foldl(
        fun(#classy_kv{k = Site, v = #site_info{node = Node}}, Acc) ->
            case lists:member(Site, Members) of
              true ->
                Acc;
              false ->
                classy_table:dirty_delete(?site_info, Site),
                maybe_on_peer_connection_status_change(Acc, Site, Node, false, false)
            end
        end,
        S1,
        ?site_info),
  classy_table:flush(?site_info),
  S.

-spec maybe_on_peer_connection_status_change(#s{}, classy:site(), node() | undefined, boolean(), boolean()) -> #s{}.
maybe_on_peer_connection_status_change(S = #s{cluster = Cluster, site = Local, peer_state = PS0}, Site, Node, IsConn, Keep) ->
  Changed = case PS0 of
              #{Site := {Node, IsConn}} ->
                false;
              #{} ->
                classy_hook:foreach(?on_peer_connection_status_change, [Cluster, Local, Site, Node, IsConn]),
                true
            end,
  PS = if Changed andalso Keep ->
           PS0#{Site => {Node, IsConn}};
          not Keep ->
           maps:remove(Site, PS0);
          true ->
           PS0
       end,
  S#s{peer_state = PS}.

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
  case classy_table:lookup(?ptab, Key) of
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
  Info = classy_hook:fold(?on_enrich_site_info, [], #{rl => RunLevel}),
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

-spec increase_n_restarts() -> ok.
increase_n_restarts() ->
  N = case classy_table:lookup(?ptab, ?n_restarts) of
        [N0] when is_integer(N0) ->
          N0 + 1;
        [] ->
          0;
        Other ->
          ?tp(warning, ?classy_bad_data,
              #{ table => ?ptab
               , key   => ?n_restarts
               , val   => Other
               }),
          0
      end,
  classy_table:write(?ptab, ?n_restarts, N).

sync_set_run_level(Level) ->
  classy_rl_changer:set_sync(Level, infinity).

the_cluster() ->
  case classy_table:lookup(?ptab, ?the_cluster) of
    [V] ->
      {ok, V};
    [] ->
      undefined
  end.

the_site() ->
  case classy_table:lookup(?ptab, ?the_site) of
    [V] ->
      {ok, V};
    [] ->
      undefined
  end.

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
