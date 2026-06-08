%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_node).

-behavior(gen_server).

%% API:
-export([ start_link/0
        , maybe_init_the_site/1
        , join_node/3
        , kick_site/2
        , the_site/0
        , the_cluster/0
        , parent_site/0
        , nodes/1
        , peer_info/0
        , node_of_site/2
        , n_restarts/0

        , at_lower_level/2
        ]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([hello/0]).

-export_type([run_level_atom/0]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy_internal.hrl").

-compile({no_auto_import, [nodes/1]}).

%%================================================================================
%% Type declarations
%%================================================================================

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
-record(call_at_run_level,
        { level :: run_level_atom()
        , function :: fun(() -> _)
        }).

-type run_level_int() :: 0..3.
-type run_level_atom() :: ?stopped | ?single | ?cluster | ?quorum.

%%================================================================================
%% API functions
%%================================================================================

%% @doc Initialize the local site.
%%
%% Values that are not persistently stored are set to the given values.
%% Any `undefined' argument is replaced with a sufficiently unique random string.
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

%% @private
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Return ID of the cluster that the node currently belongs to.
-spec the_cluster() -> {ok, classy:cluster_id()} | undefined.
the_cluster() ->
  case classy_table:lookup(?ptab, ?the_cluster) of
    [V] -> {ok, V};
    []   -> undefined
  end.

%% @doc Return ID of the local site.
-spec the_site() -> {ok, classy:site()} | undefined.
the_site() ->
  case classy_table:lookup(?ptab, ?the_site) of
    [V] -> {ok, V};
    []  -> undefined
  end.

%% @doc Return ID of the site that invited us to `the_cluster'.
%%
%% The return value could be equal to `{ok, the_site()}' for the site that originally created the cluster.
%% `undefined' return value means the local site is not initialized.
-spec parent_site() -> {ok, classy:site()} | undefined.
parent_site() ->
  case classy_table:lookup(?ptab, ?parent_site) of
    [V] -> {ok, V};
    []  -> undefined
  end.

%% @doc Join to the cluster that `Node' belongs to.
%%
%% This function performs all necessary checks before making any changes.
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

%% @doc Kick a site from the cluster.
%%
%% This function performs all necessary checks before making any changes.
%% It can be used with the local site as well.
-spec kick_site(classy:site(), _Intent) -> ok | {error, _}.
kick_site(Site, Intent) ->
  gen_server:call(
    ?SERVER,
    #call_kick{site = Site, intent = Intent},
    infinity).

%% @doc Lower the run level to the given value and run the specified function.
-spec at_lower_level(run_level_atom(), fun(() -> Ret)) ->
        {ok, Ret} |
        {error | exit | throw, _Reason, _Stacktrace}.
at_lower_level(RunLevel, Fun) ->
  gen_server:call(
    ?SERVER,
    #call_at_run_level{level = RunLevel, function = Fun},
    infinity).

-spec nodes(all | running | stopped) -> [node()].
nodes(Query) ->
  Filter = case Query of
             all     -> [];
             running -> [{'=:=', '$2', true}];
             stopped -> [{'=:=', '$2', false}]
           end,
  MS = { #classy_kv{ v = #site_info{ node = '$1'
                                   , isup = '$2'
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
    fun(#classy_kv{k = Site, v = #site_info{node = Node, isup = IsUp, last_update = LU}}, Acc) ->
        Info = #{ node        => Node
                , up          => IsUp
                , last_update => LU
                },
        Acc#{Site => Info}
    end,
    #{},
    ?site_info).

-spec node_of_site(classy:site(), boolean()) -> {ok, node()} | undefined.
node_of_site(Site, OnlyConnected) ->
  case classy_table:lookup(?site_info, Site) of
    [#site_info{isup = IsConnected, node = Node}] when IsConnected; not OnlyConnected ->
      {ok, Node};
    _ ->
      undefined
  end.

%% @doc Return number of node restarts since creation of the site.
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
        , run_level = 0 :: run_level_int()
        , peer_state = #{} :: #{classy:site() => {node(), boolean()}}
        }).

%% @private
init(_) ->
  process_flag(trap_exit, true),
  net_kernel:monitor_nodes(
    true,
    #{ node_type => visible
     , nodedown_reason => true
     }),
  ok = classy_table:open(?ptab, #{}),
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

%% @private
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
handle_call(#call_at_run_level{level = RequestedRunLevel, function = Fun}, _From, S0) ->
  RunLevel = min(S0#s.run_level, run_level(RequestedRunLevel)),
  S = change_run_level(RunLevel, S0),
  Ret = try
          {ok, Fun()}
        catch
          EC:Err:Stack ->
            {EC, Err, Stack}
        end,
  {reply, Ret, update_runtime(S)};
handle_call(Call, From, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => call
       , from => From
       , content => Call
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

%% @private
handle_cast(#cast_membership_change{} = Cast, S) ->
  handle_membership_change_event(Cast, S);
handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

%% @private
handle_info({NodeUpOrDown, _Node, _}, S) when NodeUpOrDown =:= nodeup; NodeUpOrDown =:= nodedown ->
  {noreply, update_runtime(S)};
handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

%% @private
terminate(Reason, S) ->
  classy_lib:is_normal_exit(Reason) orelse
    ?tp(warning, ?classy_abnormal_exit,
        #{ server => ?MODULE
         , reason => Reason
         }),
  classy_table:stop(?ptab, 1_000),
  classy_table:stop(?site_info, 1_000),
  case S of
    #s{} -> change_run_level(run_level(?stopped), S);
    _    -> ok
  end.

%%================================================================================
%% Internal exports
%%================================================================================

%% @private RPC target, called by remote node during `join'.
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
          do_join_node(Node, Cluster, Remote, MemData, S);
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
        #s{}
       ) ->
        {ok, #s{}} | {error, _}.
do_join_node(Node, Cluster, Remote, MemData, S0) ->
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
      Intent = join,
      case handle_kick(OldCluster, Local, Local, Intent) of
        ok ->
          {ok, S} = on_leave(S0, Intent),
          do_join_node(Node, Cluster, Remote, MemData, S);
        Err ->
          Err
      end;
    undefined ->
      %% Site is not in any cluster:
      {ok, S} = join_cluster(Cluster, Node, Local, Remote, S0),
      do_join_node(Node, Cluster, Remote, MemData, S)
  end.

on_leave(S0 = #s{cluster = Cluster, site = Local}, Intent) ->
  S = change_run_level(run_level(?stopped), S0),
  classy_table:delete(?ptab, ?the_cluster),
  classy_hook:foreach(?on_post_kick, [Cluster, Local, Intent]),
  classy_table:clear(?site_info),
  case Intent of
    join ->
      {ok, S#s{cluster = undefined}};
    _ ->
      init_cluster()
  end.

-spec join_cluster(classy:cluster_id(), node(), classy:site(), classy:site(), #s{}) -> {ok, #s{}}.
join_cluster(Cluster, JoinToNode, Local, Remote, S = #s{run_level = 0}) ->
  {ok, _} = classy_sup:ensure_membership(Cluster, Local),
  classy_hook:foreach(?on_post_join, [Cluster, Local, JoinToNode]),
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
                 IsUp = lists:member(Node, Nodes);
               #{} ->
                 Node = undefined,
                 IsUp = false
             end,
             case classy_table:lookup(?site_info, Site) of
               [#site_info{isup = IsUp, node = Node}] ->
                 %% No changes:
                 ok;
               _ ->
                 classy_table:dirty_write(
                   ?site_info,
                   Site,
                   #site_info{ isup = IsUp
                             , node = Node
                             , last_update = classy_lib:time_s()
                             })
             end,
             maybe_on_peer_connection_status_change(Acc, Site, Node, IsUp, true)
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
maybe_on_peer_connection_status_change(S = #s{cluster = Cluster, site = Local, peer_state = PS0}, Site, Node, IsUp, Keep) ->
  Changed = case PS0 of
              #{Site := {Node, IsUp}} ->
                false;
              #{} ->
                classy_hook:foreach(?on_peer_connection_status_change, [Cluster, Local, Site, Node, IsUp]),
                true
            end,
  PS = if Changed andalso Keep ->
           PS0#{Site => {Node, IsUp}};
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
          Val = base64:encode(crypto:strong_rand_bytes(32));
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
  NRunning = length(nodes(running)),
  RunLevel = case NKnown >= classy_lib:n_sites() of
               true  ->
                 case NRunning >= classy:quorum(config) of
                   true  -> run_level(?quorum);
                   false -> run_level(?cluster)
                 end;
               false -> run_level(?single)
             end,
  change_run_level(RunLevel, S).

-spec change_run_level(run_level_int(), #s{}) -> #s{}.
change_run_level(Level, #s{run_level = Level} = S) when is_integer(Level) ->
  S;
change_run_level(To, #s{run_level = From} = S) when To >= 0, To =< 3 ->
  Next = if To > From ->
             From + 1;
            To < From ->
             From - 1
         end,
  classy_hook:foreach(?on_change_run_level, [run_level(From), run_level(Next)]),
  change_run_level(To, S#s{run_level = Next}).

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

-spec run_level(run_level_int()) -> run_level_atom();
               (run_level_atom()) -> run_level_int().
run_level(?stopped) -> 0;
run_level(?single)  -> 1;
run_level(?cluster) -> 2;
run_level(?quorum)  -> 3;
run_level(0) -> ?stopped;
run_level(1) -> ?single;
run_level(2) -> ?cluster;
run_level(3) -> ?quorum.
