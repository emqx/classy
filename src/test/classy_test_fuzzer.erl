%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc A helper module for implementing property-based tests on classy clusters.
-module(classy_test_fuzzer).

%% API:
-export([ format_cmds/1
        , cmds/2
        , is_running/2
        , running_sites/1
        , sites_of_cluster/2
        , trace_and_run/1
        , wrap_commands/1
        , cluster_of/2
        , real_cluster_of/1
        , clusters/1
        , diagnostic/1
        , diagnostic/2
        , call/5
        , call/4
        , call/3
        ]).

%% behavior callbacks:
-export([ command/1
        , initial_state/1
        , next_state/3
        , precondition/2
        , postcondition/3
        ]).

%% internal exports:
-export([ init_cluster/1
        , setup_hooks/1
        , join_node/4
        , kick_site/4
        , start_site/2
        , stop_site/1
        , familiar_cluster/0
        ]).

-export_type([test_conf/0]).

-include_lib("proper/include/proper.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(SNK_COLLECTOR, true).
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-dialyzer({nowarn_function,
           [ cmds/2
           , command/1
           , running_site_command_/2
           , stopped_site_command_/2
           , enrich_test_conf_/1
           , site_command_/2
           , maybe_generate/3
           ]}).

%%================================================================================
%% Type declarations
%%================================================================================

%% "Symbolic" id of the cluster
-type cluster() :: integer().

-type test_conf() ::
        #{ module := module()
         , sites => [{classy:site(), familiar:site_conf()}]
         , quorum => pos_integer()
         , n_sites => pos_integer()
         , _ => _
         }.

-type site_state() ::
        #{ %% Current symbolic cluster ID of the site:
           cluster := cluster()
         , running := boolean()
         , conf := familiar:site_conf()
         }.

-type s() ::
        #{ module := module()
         , sites := #{classy:site() => site_state()}
         , quorum := pos_integer()
         , n_sites := pos_integer()
           %% Symbolic ID of the cluster (it's not equal to the actual cluster ID, which is random)
         , cluster_id := pos_integer()
         , _ => _
         }.

-define(rpc_timeout, 5_000).
-define(sync_timeout, ?rpc_timeout * 5).

%%================================================================================
%% Internal exports
%%================================================================================

familiar_cluster() ->
  classy_fuzz_cluster.

-spec init_cluster(test_conf()) -> ok.
init_cluster(#{sites := Sites, quorum := Quorum, n_sites := NSites}) ->
  lists:foreach(
    fun({Site, Conf0}) ->
        Fixtures = maps:get(fixtures, Conf0, []),
        ClassyFixture = {familiar_app,
                         #{ app => classy
                          , env => #{ setup_hooks => {?MODULE, setup_hooks, [Site]}
                                    , quorum => Quorum
                                    , n_sites => NSites
                                    , sync_timeout => 1000
                                    }
                          }},
        Conf = Conf0#{fixtures => [ClassyFixture] ++ Fixtures},
        ?assertMatch(
           {ok, _},
           familiar:create_site(familiar_cluster(), Site, Conf))
    end,
    Sites).

setup_hooks(Site) ->
  classy:on_node_init(
    fun() ->
        classy_node:maybe_init_the_site(Site)
    end,
    0).

join_node(Origin, Target, Intent, S) ->
  TargetNode = familiar:which_node({familiar_cluster(), Target}),
  ?tp(info, classy_test_fuzzer_join_node,
      #{ site        => Origin
       , target      => Target
       , target_node => TargetNode
       , cluster     => real_cluster_of(Target)
       , intent      => Intent
       }),
  TargetCluster = cluster_of(Target, S),
  OriginCluster = cluster_of(Origin, S),
  case OriginCluster of
    TargetCluster ->
      %% If already in the same cluster, no join event expected
      Result = call(
                 Origin,
                 fun() ->
                     classy:join_node(TargetNode, Intent)
                 end,
                 ?rpc_timeout),
      {Result, {ok, []}};
    _ ->
      exec_and_wait_sync(
        [Origin | sites_of_cluster(TargetCluster, S)],
        fun() ->
            ?retry(100, 10,
                   ok = call(
                          Origin,
                          fun() ->
                              classy:join_node(TargetNode, Intent)
                          end,
                          ?rpc_timeout))
        end,
        ?match_event(#{?snk_kind := classy_member_join, remote := Origin}),
        S)
  end.

kick_site(Origin, Target, Intent, S) ->
  ?tp(info, classy_test_fuzzer_kick_site,
      #{ site => Origin
       , target => Target
       , intent => Intent
       }),
  exec_and_wait_sync(
    sites_of_cluster(cluster_of(Origin, S), S),
    fun() ->
        call(
          Origin,
          fun() ->
              classy:kick_site(Target, Intent)
          end,
          ?rpc_timeout)
    end,
    ?match_event(#{?snk_kind := classy_member_leave, remote := Target}),
    S).

stop_site(Site) ->
  familiar:stop_site(familiar_cluster(), Site).

start_site(Site, S) ->
  %% Note: since in non-singleton clusters we don't stop all sites,
  %% we can wait for a sync-in event to make sure the re-started site is synced:
  NEvents = case sites_of_cluster(cluster_of(Site, S), S) of
              [_] -> 2;
              _   -> 3
            end,
  {ok, Sub} = snabbkaffe:subscribe(
                fun(#{ ?snk_kind := classy_change_run_level
                     , to := single
                     , ?snk_meta := #{local := X}
                     }) ->
                    X =:= Site;
                   (#{ ?snk_kind := K
                     , ?snk_meta := #{local := X}
                     }) when X =:= Site ->
                    K =:= classy_membership_sync_out orelse K =:= classy_membership_sync_in;
                   (_) ->
                    false
                end,
                NEvents,
                ?sync_timeout),
  Ret = familiar:start_site({familiar_cluster(), Site}),
  {Ret, snabbkaffe:receive_events(Sub)}.

%%================================================================================
%% Utility functions
%%================================================================================

%% @doc Wrap every command in `trace_and_run' call:
wrap_commands(Cmds) ->
  [case I of
     {set, Var, {call, M, F, A}} ->
       {set, Var, {call, ?MODULE, trace_and_run, [{M, F, A}]}};
     _ ->
       I
   end || I <- Cmds].

trace_and_run(MFA = {M, F, A}) ->
  ?tp_span(info, classy_test_fuzzer_exec, #{mfa => MFA},
           apply(M, F, A)).

format_cmds(Cmds) ->
  lists:map(
    fun({init, {init, Cfg}}) ->
        io_lib:format("   %%% Test configuration: ~0p~n", [Cfg]);
       ({call, ?MODULE, init_cluster, [MS]}) ->
        io_lib:format("   %%% init(~0p)~n", [MS]);
       ({set, _, {call, M, F, Args}}) ->
        ArgsStr = [io_lib:format("~0p", [Arg]) || Arg <- Args],
        io_lib:format("   ~p:~p(~s),~n", [M, F, lists:join(", ", ArgsStr)]);
       (Other) ->
        io_lib:format(" %%% other(~0p)~n", [Other])
    end,
    Cmds).

cmds(NCommandsFactor, InitState) ->
  proper_statem:more_commands(
    NCommandsFactor,
    proper_statem:commands(
      ?MODULE,
      initial_state(InitState))).

-spec is_running(classy:site(), s()) -> boolean().
is_running(Site, #{sites := Sites}) ->
  #{Site := #{running := Running}} = Sites,
  Running.

-spec running_sites(s()) -> [classy:site()].
running_sites(#{sites := Sites}) ->
  maps:fold(
    fun(Site, #{running := Running}, Acc) ->
        case Running of
          true -> [Site | Acc];
          false -> Acc
        end
    end,
    [],
    Sites).

-spec sites_of_cluster(cluster(), s()) -> [classy:site()].
sites_of_cluster(Cluster, #{sites := Sites}) ->
  maps:fold(
    fun(Site, #{cluster := C}, Acc) ->
        if C =:= Cluster -> [Site | Acc];
           true          -> Acc
        end
    end,
    [],
    Sites).

-spec real_cluster_of(classy:site()) -> classy:cluster_id().
real_cluster_of(Site) ->
  ?retry(
     100,
     100,
     begin
       #{cluster := Cluster} =
         call(
           Site,
           classy_node, hello, [],
           ?rpc_timeout),
       Cluster
     end).

-spec cluster_of(classy:site(), s()) -> cluster().
cluster_of(Site, #{sites := Sites}) ->
  #{Site := #{cluster := C}} = Sites,
  C.

-spec clusters(s()) -> #{cluster() => [classy:site()]}.
clusters(#{sites := Sites}) ->
  maps:groups_from_list(
    fun({C, _}) -> C end,
    fun({_, S}) -> S end,
    [{C, S} || S := #{cluster := C} <- Sites]).

%% @doc Dump state of classy processes on the given sites.
-spec diagnostic(s()) -> #{classy:site() => _}.
diagnostic(#{sites := Sites} = S) ->
  diagnostic(maps:keys(Sites), S).

-spec diagnostic([classy:site()], s()) -> #{classy:site() => _}.
diagnostic(SelectedSites, #{sites := Sites}) ->
  maps:map(
    fun(Site, #{running := R}) ->
        case R of
          true ->
            catch call(
                    Site,
                    fun() ->
                        #{ members => classy_membership:dump()
                         , node => catch ets:tab2list(classy_node)
                         }
                    end,
                    5_000);
          false ->
            stopped
        end
    end,
    maps:with(SelectedSites, Sites)).

-spec call(classy:site(), module(), atom(), list(), timeout()) -> _.
call(Site, M, F, A, Timeout) ->
  familiar:call({familiar_cluster(), Site}, M, F, A, Timeout).

-spec call(classy:site(), module(), atom(), list()) -> _.
call(Site, M, F, A) ->
  familiar:call({familiar_cluster(), Site}, M, F, A).

-spec call(classy:site(), function(), timeout()) -> _.
call(Site, Fun, Timeout) ->
  familiar:call({familiar_cluster(), Site}, Fun, Timeout).

%%================================================================================
%% Proper generators
%%================================================================================

%% @private
enrich_test_conf_(Conf = #{module := _, sites := Sites}) ->
  ?LET(
     { Quorum
     , NSites
     },
     { maybe_generate(quorum, Conf, range(1, length(Sites)))
     , maybe_generate(n_sites, Conf, range(1, length(Sites)))
     },
     Conf#{ quorum => Quorum
          , n_sites => NSites
          }).

%% @private
running_site_command_(Site, S = #{sites := Sites}) ->
  #{Site := #{cluster := Cluster}} = Sites,
  OtherMembers = sites_of_cluster(Cluster, S) -- [Site],
  OtherRunning = running_sites(S),% -- [Site],
  frequency(
    [ {7, {call, ?MODULE, kick_site, [Site, oneof(OtherMembers), kick, S]}} || length(OtherMembers) > 0] ++
    [ {10, {call, ?MODULE, join_node, [Site, oneof(OtherRunning), join, S]}} || length(OtherRunning) > 0] ++
    [ {5, {call, ?MODULE, stop_site, [Site]}}
    | optcall(S, running_site_command, [Site, S], [])
    ]).

%% @private
stopped_site_command_(Site, S) ->
  frequency(
    [ {10, {call, ?MODULE, start_site, [Site, S]}}
    | optcall(S, stopped_site_command, [Site, S], [])
    ]).

%% @private
site_command_(Site, S) ->
  case is_running(Site, S) of
    true  -> running_site_command_(Site, S);
    false -> stopped_site_command_(Site, S)
  end.

%%================================================================================
%% behavior callbacks
%%================================================================================

%% @private
command({init, Conf0}) ->
  ?LET(Conf,
       enrich_test_conf_(Conf0),
       {call, ?MODULE, init_cluster, [Conf]});
command(S = #{sites := Sites}) ->
  SiteCmds = [{10, site_command_(Site, S)} || Site <- maps:keys(Sites)],
  CustomCmds = optcall(S, general_commands, [S], []),
  frequency(CustomCmds ++ SiteCmds).

%% @private
-spec initial_state(test_conf()) -> {init, test_conf()}.
initial_state(Conf) ->
  {init, Conf}.

%% @private
next_state(S, Ret, {call, ?MODULE, trace_and_run, [{M, F, A}]}) ->
  next_state(S, Ret, {call, M, F, A});
next_state(_, _Ret, {call, ?MODULE, init_cluster, [TestConf]}) ->
  #{sites := Sites0} = TestConf,
  {Sites, NextClusterId} =
    lists:mapfoldl(
      fun({Site, Conf}, Acc) ->
          Elem = { Site
                 , #{ cluster      => Acc
                    , running      => false
                    , conf         => Conf
                    }
                 },
          {Elem, Acc + 1}
      end,
      0,
      Sites0),
  TestConf#{ sites      := maps:from_list(Sites)
           , cluster_id => NextClusterId
           };
next_state(S, _Ret, {call, ?MODULE, start_site, [Site | _]}) ->
  update_site(
    Site,
    fun(SiteS) -> SiteS#{running := true} end,
    S);
next_state(S, _Ret, {call, ?MODULE, stop_site, [Site]}) ->
  update_site(
    Site,
    fun(SiteS) -> SiteS#{running := false} end,
    S);
next_state(S = #{sites := Sites}, _Ret, {call, ?MODULE, join_node, [Origin, Target | _]}) ->
  #{Target := #{cluster := Cluster}} = Sites,
  update_site(
    Origin,
    fun(SiteS) ->
        %% Joining to a live node syncs the origin:
        SiteS#{cluster := Cluster}
    end,
    S);
next_state(S, _Ret, {call, ?MODULE, kick_site, [_Origin, Target | _]}) ->
  #{cluster_id := NextClusterId} = S,
  update_site(
    Target,
    fun(SiteS) ->
        SiteS#{cluster := NextClusterId}
    end,
    S#{cluster_id := NextClusterId + 1});
next_state(S = #{module := Mod}, Ret, Command) ->
  Mod:next_state(S, Ret, Command).

%% @private
precondition(S, {call, ?MODULE, trace_and_run, [{M, F, A}]}) ->
  precondition(S, {call, M, F, A});
precondition({init, _}, {call, ?MODULE, init_cluster, _}) ->
  true;
precondition({init, _}, _) ->
  false;
precondition(S, {call, ?MODULE, kick_site, [Local, Target|_]}) ->
  is_running(Local, S) andalso
  cluster_of(Local, S) =:= cluster_of(Target, S);
precondition(S, {call, ?MODULE, join_node, [Local, Target|_]}) ->
  is_running(Local, S) andalso
  is_running(Target, S) andalso
  Local =/= Target;
precondition(S, {call, classy_test_site, stop, [Site]}) ->
  %% For simplicity, we avoid stopping all sites in clusters that have >1 sites.
  %% Stopping all sites at once leads to loss of synchronization and split views,
  %% since the site that recieved the last command may become unable to propagate data.
  %% Verifying such scenarios requires a more sophisticated model than we have now.
  Peers = sites_of_cluster(cluster_of(Site, S), S),
  case Peers of
    [_] ->
      %% Singleton clusters don't have this problem:
      true;
    _ ->
      case [I || I <- Peers -- [Site], is_running(I, S)] of
        [] -> false;
        _  -> true
      end
  end;
precondition(S, Call) ->
  optcall(S, precondition, [S, Call], true).

%% @private
postcondition(S, {call, ?MODULE, trace_and_run, [{M, F, A}]}, Result) ->
  postcondition(S, {call, M, F, A}, Result);
postcondition(PrevState, Call, Result) ->
  CurrentState = next_state(PrevState, Result, Call),
  case Call of
    {call, ?MODULE, join_node, Args} ->
      ?assertMatch(
         {ok, {ok, _Events}},
         Result,
         #{ msg => "Join failed"
          , args => Args
          , model_state => CurrentState
          });
    {call, ?MODULE, kick_site, Args} ->
      ?assertMatch(
         {ok, {ok, _Event}},
         Result,
         #{ msg => "Kick failed"
          , args => Args
          , model_state => CurrentState
          });
    {call, ?MODULE, start_site, Args} ->
      ?assertMatch(
         {{ok, _Node}, {ok, _Event}},
         Result,
         #{ msg => "Start failed"
          , args => Args
          , model_state => CurrentState
          });
    _ ->
      ok
  end,
  %% TODO: Ideally, system should be synchronized by waiting for
  %% events, but having this polling postcondition here helps to
  %% stabilize the tests.
  wait_clusters_converge(CurrentState),
  %% Call user's postcondition:
  optcall(CurrentState, postcondition, [CurrentState, Call, Result], true).

%%================================================================================
%% Internal functions
%%================================================================================

wait_clusters_converge(S) ->
  ?retry(100, 100,
         maps:foreach(
           fun(Cluster, Sites) ->
               verify_cluster_converged(Cluster, Sites, S)
           end,
           classy_test_fuzzer:clusters(S))).

verify_cluster_converged(Cluster, Sites, S) ->
  %% Verify that all running sites in the cluster have the same view of the cluster:
  Running = [I || I <- Sites, classy_test_fuzzer:is_running(I, S)],
  Views = [{ I
           , call(I, classy, sites, [])
           }
           || I <- Running],
  case Views of
    [] ->
      ok;
    [{_, Fst} | Rest] ->
      lists:foreach(
        fun({_Site, View}) ->
            ?assertEqual(
               lists:sort(Fst),
               lists:sort(View),
               #{ msg           => inconsistent_cluster
                , cluster       => Cluster
                , views         => Views
                , '~diagnostic' => diagnostic(Running, S)
                })
        end,
        Rest)
  end.

-spec exec_and_wait_sync([classy:site()], Action, snabbkaffe:predicate(), s()) ->
        {Result, {ok | timeout, [snabbkaffe:event()]}}
  when Action :: fun(() -> Result).
exec_and_wait_sync(Sites0, Action, Filter, S) ->
  Sites = lists:uniq([I || I <- Sites0, is_running(I, S)]),
  NEvents = length(Sites),
  WrappedFilter =
    fun(Event) ->
        case Filter(Event) of
          true ->
            #{?snk_meta := #{local := Local}} = Event,
            lists:member(Local, Sites);
          false ->
            false
        end
    end,
  {ok, Sub} = snabbkaffe:subscribe(WrappedFilter, NEvents, ?sync_timeout),
  Result = Action(),
  {Result, snabbkaffe:receive_events(Sub)}.

maybe_generate(Key, Conf, Generator) ->
  case Conf of
    #{Key := Val} -> exactly(Val);
    #{}           -> Generator
  end.

update_site(Site, Fun, S = #{sites := Sites}) ->
  S#{sites := maps:update_with(Site, Fun, Sites)}.

optcall(#{module := Mod}, Fun, Args, Default) ->
  case erlang:function_exported(Mod, Fun, length(Args)) of
    true  -> apply(Mod, Fun, Args);
    false -> Default
  end.
