%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-define(ON(SITE, BODY),
        familiar:call(
          {get_cluster(), SITE},
          fun() ->
              BODY
          end)).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("src/classy_internal.hrl").
-include("classy_test_macros.hrl").

%%================================================================================
%% Tests
%%================================================================================

t_010_cluster(_) ->
  ?check_trace(
     #{timetrap => 15_000},
     begin
       %% Create site and ensure that this operation is idempotent:
       {ok, S1} = familiar:create_site(get_cluster(), <<"foo">>),
       %% Check that error message is legible when calling a stopped site:
       ?assertError(
          {site_is_not_running, S1},
          familiar:call(S1,
                        fun() ->
                            ok
                        end)),
       %% Start site:
       ?assertEqual(
          {ok, 'foo@127.22.0.0'},
          familiar:start_site(S1)),
       ?assertMatch({error, already_started}, familiar:start_site(S1)),
       %% Test calls and log forwarding:
       ?assertEqual(
          'foo@127.22.0.0',
          familiar:call(S1, erlang, node, [])),
       ?assertMatch(
          ok,
          familiar:call(S1,
                        fun() ->
                            ?tp(test_msg_from_foo, #{})
                        end)),
       ?block_until(#{?snk_kind := test_msg_from_foo}),
       %% Test stopping idempotency:
       ?assertMatch(ok, familiar:stop_site(S1)),
       ?assertMatch(ok, familiar:stop_site(S1))
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

%% This testcase verifies happy case of joining one node to another:
t_020_join(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  ?check_trace(
     #{timetrap => 10_000},
     begin
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       #{ site := S1
        , cluster := Cluster1
        } = ?ON(S1, classy_node:hello()),
       #{ site := S2
        , cluster := Cluster2
        } = ?ON(S2, classy_node:hello()),
       RuntimeData = #{ nodes => [N1, N2]
                      , sites => [S1, S2]
                      , clusters => [Cluster1, Cluster2]
                      },
       %% Verify status of the nodes in the singleton mode. Both
       %% should belong to the cluster consisting of a single site,
       %% cluster ID should be equal to the site id:
       ?assertEqual(
          {ok, Cluster1},
          ?ON(S1, classy:the_cluster())),
       ?assertEqual(
          [S1],
          ?ON(S1, classy:sites())),
       ?assertEqual(
          {ok, Cluster2},
          ?ON(S2, classy:the_cluster())),
       ?assertEqual(
          [S2],
          ?ON(S2, classy:sites())),
       %% Join the nodes:
       ?tp(notice, test_join_n2, RuntimeData),
       ?assertMatch(
          ok,
          ?ON(S2, classy:join_node(N1, join))),
       wait_site_joined([S1, S2], Cluster1, S2),
       %% Verify state after join:
       ?assertEqual(
          {ok, Cluster1},
          ?ON(S1, classy:the_cluster())),
       ?assertEqual(
          {ok, Cluster1},
          ?ON(S2, classy:the_cluster())),
       ?assertSameSet(
          [S1, S2],
          ?ON(S1, classy:sites())),
       ?assertSameSet(
          [S1, S2],
          ?ON(S2, classy:sites())),
       RuntimeData
     end,
     [ fun initialization_hooks/2
     , {"join hooks",
        fun(Trace) ->
            ?assert(
               ?strict_causality(
                  #{?snk_kind := classy_pre_join_node, cluster := _C},
                  #{?snk_kind := classy_joined_cluster, cluster := _C},
                  Trace))
        end}
     , fun events_on_all_sites/1
     ]).

%% This testcase verifies happy case of kicking node from the cluster:
t_030_kick(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  S3 = <<"s3">>,
  Sites = [S1, S2, S3],
  ?check_trace(
     #{timetrap => 20_000},
     begin
       %% Prepare the system:
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       N3 = create_start_site(S3, #{}),
       #{ site := S1
        , cluster := Cluster1
        } = ?ON(S1, classy_node:hello()),
       ?assertMatch(ok, ?ON(S2, classy:join_node(N1, join))),
       ?assertMatch(ok, ?ON(S3, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster1, S2),
       wait_site_joined(Sites, Cluster1, S3),
       %% Verify state:
       [?assertSameSet(
           Sites,
           ?ON(I, classy:sites()))
        || I <- Sites],
       %% Try to kick non-existent nodes, it should fail:
       ?assertMatch(
          {error, target_not_in_cluster},
          ?ON(S1, classy:kick_node('fake@node.local', force))),
       %% Kick N1 from the cluster from N3:
       {ok, SubRef} = snabbkaffe:subscribe(?match_event(#{?snk_kind := classy_init_clustering})),
       ?assertMatch(ok, ?ON(S3, classy:kick_node(N1, force))),
       %% Wait for completion of the operation:
       {ok, _} = snabbkaffe:receive_events(SubRef),
       wait_site_kicked(Sites, Cluster1, S1),
       %% Verify state:
       [?assertSameSet(
           [S2, S3],
           ?ON(I, classy:sites()))
        || I <- [S2, S3]],
       ?assertEqual(
          [S1],
          ?ON(S1, classy:sites())),
       #{ nodes => [N1, N2, N3]
        , sites => Sites
        , clusters => [Cluster1]
        }
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

%% Verify that node can be kicked from the cluster while down:
t_040_kick_in_absentia(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  S3 = <<"s3">>,
  Sites = [S1, S2, S3],
  ?check_trace(
     #{timetrap => 20_000},
     begin
       %% Prepare the system:
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       N3 = create_start_site(S3, #{}),
       #{ site := S1
        , cluster := Cluster1
        } = ?ON(S1, classy_node:hello()),
       ?assertMatch(ok, ?ON(S2, classy:join_node(N1, join))),
       ?assertMatch(ok, ?ON(S3, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster1, S2),
       wait_site_joined(Sites, Cluster1, S3),
       %% Stop S1:
       stop_site(S1),
       %% Kick S1 from the cluster from S:
       ?assertMatch(ok, ?ON(S3, classy:kick_node(N1, kick))),
       wait_site_kicked([S2, S3], Cluster1, S1),
       %% Verify state:
       [?assertSameSet(
           [S2, S3],
           ?ON(I, classy:sites()))
        || I <- [S2, S3]],
       %% Bring S1 back up.
       %%   Upon realization that it got kicked, it should create a fresh cluster:
       {ok, SubRef} = snabbkaffe:subscribe(?match_event(#{ ?snk_kind := classy_init_clustering
                                                         , local := S1
                                                         , cluster := C
                                                         } when C =/= Cluster1)),
       ok = restart_site(S1),
       %% It should process the information about getting kicked:
       wait_site_kicked([S1], Cluster1, S1),
       {ok, _} = snabbkaffe:receive_events(SubRef),
       %% It should not rejoin the old cluster:
       [?assertSameSet(
           [S2, S3],
           ?ON(I, classy:sites()))
        || I <- [S2, S3]],
       %% It should forma a new singleton cluster instead:
       ?assertEqual(
          [S1],
          ?ON(S1, classy:sites())),
       #{ nodes => [N1, N2, N3]
        , sites => Sites
        , clusters => [Cluster1]
        }
     end,
     [ {"kicked_remotely_event",
        fun(#{nodes := [N1 | _]}, Trace) ->
            ?assertMatch(
               [_],
               [I || I = #{ ?snk_kind := classy_kicked_remotely
                          , ?snk_meta := #{node := N}
                          } <- Trace, N =:= N1])
        end}
     , fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

%% Verify that join and kick can be forbidden via hooks:
t_050_pre_checks(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  Sites = [S1, S2],
  ?check_trace(
     #{timetrap => 20_000},
     begin
       %% Prepare the system:
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       #{cluster := Cluster2} = ?ON(S2, classy_node:hello()),
       %% Inject hooks:
       ?ON(S1, classy:pre_join(
                 fun(_Cluster, _Remote, _Node, Intent) ->
                     case Intent of
                       force -> ok;
                       _ -> {error, forbidden}
                     end
                 end,
                 0)),
       ?ON(S2, classy:pre_kick(
                 fun(_Cluster, _Remote, Intent) ->
                     case Intent of
                       force -> ok;
                       _ -> {error, forbidden}
                     end
                 end,
                 0)),
       %% Join is forbidden:
       ?assertEqual(
          {error, forbidden},
          ?ON(S1, classy:join_node(N2, join))),
       %% Force join:
       ?assertEqual(
          ok,
          ?ON(S1, classy:join_node(N2, force))),
       wait_site_joined(Sites, Cluster2, S1),
       %% Kick is forbidden:
       ?assertEqual(
          {error, forbidden},
          ?ON(S2, classy:kick_node(N1, kick))),
       %% Force kick:
       ?assertEqual(
          ok,
          ?ON(S2, classy:kick_node(N1, force)))
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

%% This testcase verifies functionality of `at_lower_level' API.
t_060_at_lower_level(_) ->
  S1 = <<"s1">>,
  ?check_trace(
     #{timetrap => 20_000},
     begin
       %% Prepare the system:
       _N1 = create_start_site(S1, #{}),
       ct:sleep(1000),
       ?block_until(#{?snk_kind := classy_change_run_level, to := quorum}),
       ?assertMatch(
          ok,
          ?ON(S1,
              classy:at_lower_level(
                single,
                fun() ->
                    hello
                end))),
       ct:sleep(1000)
     end,
     [ {"run level transitions",
        fun(Trace) ->
            ?assertEqual(
               [ single, cluster, quorum
               , cluster, single
               , cluster, quorum
               ],
               ?projection(to, ?of_kind(classy_change_run_level, Trace)))
        end}
     , fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

%% Verify handling of timeouts during run level changes.
t_061_run_level_timeouts(_) ->
  S1 = <<"s1">>,
  ?check_trace(
     #{timetrap => 30_000},
     begin
       %% Setup:
       _N1 = create_start_site(S1, #{}),
       ?block_until(#{?snk_kind := classy_change_run_level, to := quorum}),
       Pred = ?match_event(#{?snk_kind := K} when K =:= rl_change;
                                                  K =:= ?classy_hook_failure;
                                                  K =:= ?classy_run_level_change_error),
       ?ON(S1,
           begin
             classy:run_level(
               fun(From, To) ->
                   ?tp(rl_change, #{f => From, t => To}),
                   timer:sleep(100)
               end,
               0)
           end),
       %% 1. First try setting run level multiple times, faster than
       %% the hooks can handle:
       ?ON(S1, application:set_env(classy, hook_timeout, 1000)),
       {ok, Sub1} = snabbkaffe:subscribe(Pred, 100, 3000, 0),
       %% Issue a few conflicting commands in rapid succession:
       ?ON(S1, classy_rl_changer:set(?stopped)),
       ?ON(S1, classy_rl_changer:set(?quorum)),
       ?ON(S1, classy_rl_changer:set(?stopped)),
       %% System should follow the last command:
       {_, Events1} = snabbkaffe:receive_events(Sub1),
       ?assertMatch(
          [ #{f := ?quorum, t := ?cluster}
          , #{f := ?cluster, t := ?single}
          , #{f := ?single, t := ?stopped}
          ],
          Events1),
       %% 2. Same logic applies when the system is stopped:
       %%    Prepare; go to the single state
       ?ON(S1,
           classy_rl_changer:set_sync(?single, 5_000)),
       %%    Request transition to quorum, and simultaneously stop
       %%    application (simulated by a supervisor request):
       {ok, Sub2} = snabbkaffe:subscribe(Pred, 100, 3000, 0),
       ?ON(S1,
           begin
             classy_rl_changer:set(?quorum),
             ok = supervisor:terminate_child(classy_sup, run_level_mgr)
           end),
       {_, Events2} = snabbkaffe:receive_events(Sub2),
       ?assertMatch(
          [ #{f := ?single, t := ?cluster}
          , #{f := ?cluster, t := ?single}
          , #{f := ?single, t := ?stopped}
          ],
          Events2),
       %%   3. Verify that timeouts are handled normally:
       ?ON(S1,
           begin
             {ok, _} = supervisor:restart_child(classy_sup, run_level_mgr),
             application:set_env(classy, hook_timeout, 10)
           end),
       {ok, Sub3} = snabbkaffe:subscribe(Pred, 100, 3000, 0),
       ?ON(S1,
           begin
             classy_rl_changer:set_sync(?quorum, 5_000),
             classy_rl_changer:set_sync(?stopped, 5_000)
           end),
       {_, Events3} = snabbkaffe:receive_events(Sub3),
       ?assertMatch(
          [ %% Advance:
            #{f := ?stopped, t := ?single}
          , #{?snk_kind := ?classy_hook_failure, reason := {error, timeout}}
          , #{f := ?single, t := ?cluster}
          , #{?snk_kind := ?classy_hook_failure, reason := {error, timeout}}
          , #{f := ?cluster, t := ?quorum}
          , #{?snk_kind := ?classy_hook_failure, reason := {error, timeout}}
            %% Retard:
          , #{f := ?quorum, t := ?cluster}
          , #{?snk_kind := ?classy_hook_failure, reason := {error, timeout}}
          , #{f := ?cluster, t := ?single}
          , #{?snk_kind := ?classy_hook_failure, reason := {error, timeout}}
          , #{f := ?single, t := ?stopped}
          , #{?snk_kind := ?classy_hook_failure, reason := {error, timeout}}
          ],
          Events3)
     end,
     [ fun events_on_all_sites/1
     ]).

%% This testcase verifies site autoclean functionality
t_070_cleanup(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  S3 = <<"s3">>,
  Sites = [S1, S2, S3],
  AppConf = {familiar_app,
             #{ app => classy
              , env => #{ quorum            => 2
                        , max_site_downtime => 1
                        , forget_after      => 0
                        , rpc_timeout       => 100
                        }
              }},
  Conf = #{fixtures => [AppConf]},
  ?check_trace(
     #{timetrap => 20_000},
     begin
       %% Prepare system:
       N1 = create_start_site(S1, Conf),
       _N2 = create_start_site(S2, Conf),
       _N3 = create_start_site(S3, Conf),
       {ok, Cluster} = ?ON(S1, classy:the_cluster()),
       ?assertMatch(ok, ?ON(S2, classy:join_node(N1, join))),
       ?assertMatch(ok, ?ON(S3, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster, S2),
       wait_site_joined(Sites, Cluster, S3),
       %% Stop two sites. Autoclean on S1 should not attempt to delete
       %% anything due to lack of quorum:
       stop_site(S2),
       stop_site(S3),
       ct:sleep(5_000),
       ?assertSameSet(Sites, ?ON(S1, classy:sites())),
       %% Bring up S2 and restore quorum, that should lead to deletion of S3:
       ?wait_async_action(
          restart_site(S2),
          #{?snk_kind := automatically_kick_down_site}),
       wait_site_kicked([S1, S2], Cluster, S3),
       ?assertSameSet([S1, S2], ?ON(S1, classy:sites())),
       ?assertSameSet([S1, S2], ?ON(S2, classy:sites()))
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

%% This testcase verifies membership cleanup.
t_071_membership_forget(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  S3 = <<"s3">>,
  S4 = <<"s4">>,
  Sites = [S1, S2, S3, S4],
  ForgetAfterS = 1,
  WaitForget = ForgetAfterS * 1000 + 10,
  AppConf = {familiar_app,
             #{ app => classy
              , env => #{forget_after => ForgetAfterS}
              }},
  Conf = #{fixtures => [AppConf]},
  ?check_trace(
     #{timetrap => 30_000},
     begin
       %% Prepare the system:
       N1 = create_start_site(S1, Conf),
       _N2 = create_start_site(S2, Conf),
       _N3 = create_start_site(S3, Conf),
       _N4 = create_start_site(S4, Conf),
       [ok = ?ON(I, classy:join_node(N1, join)) || I <- [S2, S3, S4]],
       #{cluster := Cluster} = ?ON(S1, classy_node:hello()),
       [wait_site_joined(Sites, Cluster, I) || I <- [S2, S3, S4]],
       %% Stop S3. Its absence should prevent cleanup from doing anything.
       stop_site(S3),
       %% Kick S2, pass time and trigger cleanup at S1:
       ?ON(S1, classy:kick_site(S2, kick)),
       ct:sleep(WaitForget),
       ?ON(S1, classy_membership:cleanup(Cluster, S1, ForgetAfterS)),
       %% It should remain in the cluster for now, as S3 is out-of-sync:
       [?assertMatch(
           #{{Cluster, I} := #{peers := #{S1 := _, S2 := _, S3 := _, S3 := _}}},
           ?ON(I, classy_membership:dump()),
           #{at => I})
        || I <- [S1, S4]],
       %% Kick S3 (while it's stopped and out of sync):
       ?ON(S1, classy:kick_site(S3, kick)),
       wait_site_kicked([S1, S4], Cluster, S3),
       %% After S3 expires, S1 should have no problem deleting both S2 and S3:
       ct:sleep(WaitForget * 3),
       ?ON(S1, classy_membership:cleanup(Cluster, S1, ForgetAfterS)),
       #{{Cluster, S1} := S1Info = #{peers := PeersOfS1}} = ?ON(S1, classy_membership:dump()),
       ?assertMatch(
          #{peers := #{S1 := _, S4 := _}},
          S1Info),
       ?assertNot(
          maps:is_key(S2, PeersOfS1),
          S1Info),
       ?assertNot(
          maps:is_key(S3, PeersOfS1),
          S1Info)
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).


%% This testcase verifies that sites that membership CRDT recovers
%% from lost packets. Packet loss is emulated by setting "acked_out"
%% counters to higher values.
t_080_desync(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  S3 = <<"s3">>,
  Sites = [S1, S2, S3],
  ?check_trace(
     #{timetrap => 20_000},
     begin
       %% Prepare system:
       N1 = create_start_site(S1, #{}),
       _N2 = create_start_site(S2, #{}),
       ?assertMatch(ok, ?ON(S2, classy:join_node(N1, join))),
       #{cluster := Cluster} = ?ON(S1, classy_node:hello()),
       %% Emulate de-sync by setting counters to very high values:
       ?force_ordering(
          #{?snk_kind := test_proceed},
          #{?snk_kind := classy_membership_sync_out}),
       ?ON(S1, classy_membership:reset_acked_out(Cluster, S1, S2, 1000)),
       ?ON(S2, classy_membership:reset_acked_out(Cluster, S2, S1, 1000)),
       ?tp(test_proceed, #{}),
       %% Wait until one of the sites detects the gap:
       ?block_until(#{?snk_kind := classy_membership_sync_gap}),
       %% Connect the third site to make sure the CRDT is healed:
       _N3 = create_start_site(S3, #{}),
       ?assertMatch(ok, ?ON(S3, classy:join_node(N1, join))),
       [?retry(
           1000,
           10,
           ?assertSameSet(
              Sites,
              ?ON(I, classy:sites())))
        || I <- Sites]
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

%% This testcase verifies `classy:info/0' and `classy:info/1'
t_090_info(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  Sites = [S1, S2],
  EnrichInfo = fun(Info) ->
                   Info#{hello => world}
               end,
  ?check_trace(
     #{timetrap => 20_000},
     begin
       %% Prepare system:
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       [?ON(I, classy:enrich_site_info(EnrichInfo, 0))
        || I <- Sites],
       %% Verify functions in singleton clusters:
       #{ cluster     := Cluster1
        , site        := S1
        , peers       := Peers1_0
        , last_update := _
        , hello       := world
        } = I1_0 = ?ON(S1, classy:info()),
       #{ cluster     := Cluster2_0
        , site        := S2
        , peers       := Peers2_0
        , last_update := _
        , hello       := world
        } = I2_0 = ?ON(S2, classy:info()),
       ?assert(is_binary(Cluster1)),
       ?assertNotEqual(Cluster1, Cluster2_0),
       ?assertEqual(0, maps:size(Peers1_0)),
       ?assertEqual(0, maps:size(Peers2_0)),
       %%
       ?assertMatch(
          #{ infos  :=
               #{ N1 := I1_0
                , N2 := I2_0
                }
           , bad_nodes :=
               #{'fake@node.local' := {error, {erpc, noconnection}}}
           },
          ?ON(S1, classy:info([N1, N2, 'fake@node.local']))),
       %% Form cluster:
       ?assertMatch(ok, ?ON(S2, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster1, S2),
       %% Verify `classy:info/1':
       [?assertMatch(
           #{ infos :=
                #{ N1 := #{ cluster := Cluster1
                          , site := S1
                          , peers := #{S2 := #{node := N2, connected := true, last_update := _}}
                          }
                 , N2 := #{ cluster := Cluster1
                          , site := S2
                          , peers := #{S1 := #{node := N1, connected := true, last_update := _}}
                          }
                 }
            },
           ?ON(I, classy:info([N1, N2])))
        || I <- [S1, S2]],
       ok
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

%% This testcase verifies behavior or `node_of_site' function when
%% peer goes down. `OnlyLive' flag should prevent this function from
%% returning an old node name.
t_091_node_of_site(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  Sites = [S1, S2],
  ?check_trace(
     #{timetrap => 20_000},
     begin
       %% Prepare system:
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       #{cluster := Cluster1} = ?ON(S1, classy_node:hello()),
       NodeMap = #{S1 => N1, S2 => N2},
       %% Verify function in singleton clusters:
       [?assertEqual(
           {ok, maps:get(I, NodeMap)},
           ?ON(I, classy:node_of_site(I, OnlyLive)))
        || I <- Sites, OnlyLive <- [true, false]],
       [?assertEqual(
           undefined,
           ?ON(I, classy:node_of_site(J, OnlyLive)))
        || I <- Sites, J <- Sites, I =/= J, OnlyLive <- [true, false]],
       %% Form cluster:
       ?assertMatch(ok, ?ON(S2, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster1, S2),
       %% Verify `classy:info':
       [?assertEqual(
           {ok, maps:get(J, NodeMap)},
           ?ON(I, classy:node_of_site(J, OnlyLive)))
        || I <- Sites, J <- Sites, OnlyLive <- [true, false]],
       %% Shut down S2 and verify that S1 reacted on changes:
       stop_site(S2),
       ?block_until(#{?snk_kind := classy_peer_disconnected, site := S2}),
       ct:sleep(100),
       ?assertEqual(
          undefined,
          ?ON(S1, classy:node_of_site(S2, true))),
       ?assertEqual(
          {ok, N2},
          ?ON(S1, classy:node_of_site(S2, false)))
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

t_092_link_detect(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  Sites = [S1, S2],
  ?check_trace(
     #{timetrap => 20_000},
     begin
       %% Prepare system:
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       #{cluster := Cluster} = ?ON(S1, classy_node:hello()),
       CInfo1 = classy:info([N1, N2]),
       %% Sites are disconnected:
       %%   Bidi:
       ?assertEqual(
          {ok, false},
          classy_partition:bidi_link(CInfo1, N1, N2),
          CInfo1),
       ?assertEqual(
          {ok, false},
          classy_partition:bidi_link(CInfo1, N2, N1),
          CInfo1),
       %%  Unid:
       ?assertEqual(
          {ok, false},
          classy_partition:unid_link(CInfo1, N1, N2),
          CInfo1),
       ?assertEqual(
          {ok, false},
          classy_partition:unid_link(CInfo1, N2, N1),
          CInfo1),
       %% Join sites:
       ?ON(S2, classy:join_node(N1, join)),
       wait_site_joined(Sites, Cluster, S2),
       %% Now joined sites should have a bidirectional link:
       CInfo2 = classy:info([N1, N2]),
       ?assertEqual(
          {ok, true},
          classy_partition:bidi_link(CInfo2, N1, N2),
          CInfo2),
       ?assertEqual(
          {ok, true},
          classy_partition:bidi_link(CInfo2, N2, N1),
          CInfo2),
       %% Uni-d as well:
       ?assertEqual(
          {ok, true},
          classy_partition:unid_link(CInfo2, N1, N2),
          CInfo2),
       ?assertEqual(
          {ok, true},
          classy_partition:unid_link(CInfo2, N2, N1),
          CInfo2),
       %% Stop one of the sites:
       stop_site(S2),
       ?block_until(#{?snk_kind := classy_peer_disconnected, site := S2}),
       ct:sleep(100),
       CInfo3 = classy:info([N1, N2]),
       %% We can still derive that there's no bidirectional link:
       ?assertEqual(
          {ok, false},
          classy_partition:bidi_link(CInfo3, N1, N2),
          CInfo3),
       ?assertEqual(
          {ok, false},
          classy_partition:bidi_link(CInfo3, N2, N1),
          CInfo3),
       %% Unid:
       ?assertEqual(
          {ok, false},
          classy_partition:unid_link(CInfo3, N1, N2),
          CInfo3),
       ?assertEqual(
          {error, insufficient_data},
          classy_partition:unid_link(CInfo3, N2, N1),
          CInfo3),
       %% Both sites are missing:
       ?assertEqual(
          {error, insufficient_data},
          classy_partition:bidi_link(CInfo3, 'missing1@badhost', 'missing2@badhost'),
          CInfo3)
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

%% This testcase verifies basic functionality of autocluster.
t_100_autocluster(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  Sites = [S1, S2],
  ?check_trace(
     #{timetrap => 20_000},
     begin
       %% Prepare system:
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       %% Update the discovery strategy in the runtime:
       Strategy = {static, #{seeds => [N1, N2]}},
       [?ON(I,
            application:set_env(classy, discovery_strategy, Strategy))
        || I <- Sites],
       %% Wait for the autocluster to do its job:
       ?block_until(#{?snk_kind := classy_member_join}),
       {ok, Cluster} = ?ON(S1, classy:the_cluster()),
       %% Verify candidates function:
       ?assertMatch(
          {ok, [{Cluster, N2}]},
          ?ON(S1, classy_autocluster:candidates()))
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

%% This testcase verifies that n_restarts counter increments every
%% time when the site is started. It also verifies ID generation
%% functions that depend on that counter.
t_200_n_restarts(_) ->
  S = <<"s1">>,
  ?check_trace(
     begin
       create_start_site(S, #{}),
       ?assertEqual(
          {ok, 0},
          ?ON(S, classy_node:n_restarts())),
       %% Verify serial UID tuples:
       ?assertEqual(
          {0, 1},
          ?ON(S, classy_uid:site_unique_seq_tuple(seq))),
       ?assertEqual(
          {0, 2},
          ?ON(S, classy_uid:site_unique_seq_tuple(seq))),
       ?assertEqual(
          {S, 0, 3},
          ?ON(S, classy_uid:cluster_unique_seq_tuple(seq))),
       %% Verify regular UID tuples:
       {0, UI1} = ?ON(S, classy_uid:site_unique_tuple()),
       {0, UI2} = ?ON(S, classy_uid:site_unique_tuple()),
       {S, 0, UI3} = ?ON(S, classy_uid:cluster_unique_tuple()),
       {S, 0, UI4} = ?ON(S, classy_uid:cluster_unique_tuple()),
       ?assertEqual(
          [UI1, UI2, UI3, UI4],
          lists:uniq([UI1, UI2, UI3, UI4])),
       [begin
          stop_site(S),
          restart_site(S),
          ?assertEqual(
             {ok, Nr},
             ?ON(S, classy_node:n_restarts())),
          %% Verify serial UID tuples:
          ?assertEqual(
             {Nr, 1},
             ?ON(S, classy_uid:site_unique_seq_tuple(seq))),
          ?assertEqual(
             {S, Nr, 2},
             ?ON(S, classy_uid:cluster_unique_seq_tuple(seq))),
            %% Verify regular UID tuples:
            ?assertMatch(
               {Nr, UI} when is_integer(UI),
               ?ON(S, classy_uid:site_unique_tuple())),
            ?assertMatch(
               {S, Nr, UI} when is_integer(UI),
               ?ON(S, classy_uid:cluster_unique_tuple()))
        end
        || Nr <- lists:seq(1, 5)]
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

%% This testcase verifies normal operation and error handling in `classy_lib:multicall' API.
t_300_rpc(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  SB = <<"sbad">>,
  ?check_trace(
     #{timetrap => 15_000},
     begin
       %% Prepare
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       {ok, Cluster} = ?ON(S1, classy:the_cluster()),
       ?assertMatch(ok, ?ON(S2, classy:join_node(N1, join))),
       wait_site_joined([S1, S2], Cluster, S2),
       %% Tests:
       ?assertEqual(
          #{ S1 => {ok, N1}
           , S2 => {ok, N2}
           , SB => {error, site_is_down}
           },
          ?ON(S1,
              classy_lib:multicall(
                #{S => {erlang, node, []} || S <- [S1, S2, SB]},
                5_000))),
       %% Handling of throw:
       ?assertEqual(
          #{ S1 => {error, {throw, {foo, S1}}}
           , S2 => {error, {throw, {foo, S2}}}
           , SB => {error, site_is_down}
           },
          ?ON(S1,
              classy_lib:multicall(
                #{S => {erlang, throw, [{foo, S}]} || S <- [S1, S2, SB]},
                5_000))),
       %% Handling of errors:
       ?assertMatch(
          #{ S1 := {error, {error, {foo, S1}, _}}
           , S2 := {error, {error, {foo, S2}, _}}
           , SB := {error, site_is_down}
           },
          ?ON(S1,
              classy_lib:multicall(
                #{S => {erlang, error, [{foo, S}]} || S <- [S1, S2, SB]},
                5_000))),
       ?assertEqual(
          #{ S1 => {error, {exit, {foo, S1}}}
           , S2 => {error, {exit, {foo, S2}}}
           , SB => {error, site_is_down}
           },
          ?ON(S1,
              classy_lib:multicall(
                #{S => {erlang, exit, [{foo, S}]} || S <- [S1, S2, SB]},
                5_000))),
       McallTimeout = 500,
       {Time, TimedOutVal} =
          ?ON(S1,
              timer:tc(
                fun() ->
                    classy_lib:multicall(
                      #{S => {timer, sleep, [1_000]} || S <- [S1, S2, SB]},
                      McallTimeout)
                end,
                [],
                millisecond)),
       ?assertEqual(
          #{ S1 => {error, timeout}
           , S2 => {error, timeout}
           , SB => {error, site_is_down}
           },
          TimedOutVal),
       ?give_or_take(McallTimeout, 50, Time),
       %% Multiple calls to the same site with tags:
       ?assertEqual(
          #{ {S2, 1} => {ok, N2}
           , {S2, 2} => {ok, N2}
           },
          ?ON(S1,
              classy_lib:multicall(
                #{ {S2, 1} => {erlang, node, []}
                 , {S2, 2} => {erlang, node, []}
                 },
                5_000))),
       ok
     end,
     []).

%% This testcase verifies behavior of RPC when a node gets stopped amidst a multicall:
t_310_rpc_to_failing_node(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  ?check_trace(
     #{timetrap => 15_000},
     begin
       %% Prepare
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       {ok, Cluster} = ?ON(S1, classy:the_cluster()),
       ?assertMatch(ok, ?ON(S2, classy:join_node(N1, join))),
       wait_site_joined([S1, S2], Cluster, S2),
       %% Test:
       FS2 = {get_cluster(), S2},
       spawn_link(
         fun() ->
             timer:sleep(1000),
             familiar:stop_site(FS2)
         end),
       ?assertEqual(
          #{S2 => {error, {erpc, noconnection}}},
          ?ON(S1,
              classy_lib:multicall(
                #{S2 => {timer, sleep, [30_000]}},
                30_000))),
       ok
     end,
     []).

%% This testcase verifies various scenarios related to 2PC that lead
%% to abort and rollback.
t_400_vote_smoke_abort(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  S3 = <<"s3">>,
  Sites = [S1, S2, S3],
  Ref1 = vote1,
  Ref2 = vote2,
  Ref3 = vote3,
  ?check_trace(
     #{timetrap => 15_000},
     begin
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       N3 = create_start_site(S3, #{}),
       Nodes = [N1, N2, N3],
       {ok, Cluster} = ?ON(S1, classy:the_cluster()),
       ?assertEqual(ok, ?ON(S2, classy:join_node(N1, join))),
       ?assertEqual(ok, ?ON(S3, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster, S2),
       wait_site_joined(Sites, Cluster, S3),
       %% Immediate failure, missing site:
       ?tp(test_stage, #{n => 1}),
       ?assertEqual(
          {error, #{<<"bad_site">> => {error, site_is_down}}},
          ?ON(S1,
              classy_vote:create(#{ tag     => Ref1
                                  , actions => #{<<"bad_site">> => make_vote(true, true, Ref1, 1)}
                                  , post_vote => make_post_vote(Ref1)
                                  }))),
       verify_no_votes(Nodes),
       %% Pre-vote fail:
       ?assertEqual(
          {error, #{S => {ok, false} || S <- [S1, S2, S3]}},
          ?ON(S1,
              classy_vote:create(#{ tag => Ref2
                                  , actions  => #{Site => make_vote(false, false, Ref2, 1) || Site <- [S1, S2, S3]}
                                  , post_vote => make_post_vote(Ref2)
                                  }))),
       verify_no_votes(Nodes),
       %% Vote stage fails:
       {ok, ID3} = ?ON(S1,
                       classy_vote:create(#{ tag => Ref3
                                           , actions => #{ S2 => make_vote(true, true, Ref3, 1)
                                                         , S3 => make_vote(true, false, Ref3, 1)
                                                         }
                                           , post_vote => make_post_vote(Ref3)
                                           })),
       ?assertNot(classy_vote:test_wait_conclude(ID3)),
       verify_no_votes(Nodes),
       Nodes
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     , {"rollback events",
        fun([_N1, N2, N3], Trace) ->
            ?assertMatch(
               [ #{ref := Ref3}
               ],
               ?of_node(N2, ?of_kind(classy_test_vote_rollback, Trace))),
            ?assertMatch(
               [ #{ref := Ref3}
               ],
               ?of_node(N3, ?of_kind(classy_test_vote_rollback, Trace))),
            ?assertMatch(
               [],
               ?of_kind(classy_test_vote_commit, Trace)),
            ?assertMatch(
               [#{ref := Ref3, result := false}],
               ?of_kind(classy_test_post_vote, Trace))
        end}
     | classy_vote:trace_props()
     ]).

%% Verify that timeout leads to aborted transaction:
t_401_vote_timeout(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  S3 = <<"s3">>,
  Sites = [S1, S2, S3],
  Ref1 = vote1,
  ?check_trace(
     #{timetrap => 15_000},
     begin
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       N3 = create_start_site(S3, #{}),
       Nodes = [N1, N2, N3],
       {ok, Cluster} = ?ON(S1, classy:the_cluster()),
       ?assertEqual(ok, ?ON(S2, classy:join_node(N1, join))),
       ?assertEqual(ok, ?ON(S3, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster, S2),
       wait_site_joined(Sites, Cluster, S3),
       ?force_ordering(
          #{?snk_kind := test_go},
          #{?snk_kind := classy_test_vote_prep, for_real := true}),
       {ok, ID} = ?ON(S1,
                      classy_vote:create(#{ tag => Ref1
                                          , actions => #{ S => make_vote(true, true, Ref1, 1) ||
                                                          S <- [S2, S3]
                                                        }
                                          , post_vote => make_post_vote(Ref1)
                                          , strategy => {all, 100}
                                          })),
       ct:sleep(200),
       ?tp(test_go, #{}),
       ?assertNot(classy_vote:test_wait_conclude(ID)),
       verify_no_votes(Nodes),
       Nodes
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     , {"rollback events",
        fun([_N1, N2, N3], Trace) ->
            ?assertMatch(
               [ #{ref := Ref1}
               ],
               ?of_node(N2, ?of_kind(classy_test_vote_rollback, Trace))),
            ?assertMatch(
               [ #{ref := Ref1}
               ],
               ?of_node(N3, ?of_kind(classy_test_vote_rollback, Trace))),
            ?assertMatch(
               [],
               ?of_kind(classy_test_vote_commit, Trace)),
            ?assertMatch(
               [#{ref := Ref1, result := false}],
               ?of_kind(classy_test_post_vote, Trace))
        end}
     | classy_vote:trace_props()
     ]).

%% Verify that restart of the coordinator during vote leads to abort
t_403_vote_coord_restart(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  S3 = <<"s3">>,
  Sites = [S1, S2, S3],
  Ref1 = vote1,
  ?check_trace(
     #{timetrap => 15_000},
     begin
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       N3 = create_start_site(S3, #{}),
       Nodes = [N1, N2, N3],
       {ok, Cluster} = ?ON(S1, classy:the_cluster()),
       ?assertEqual(ok, ?ON(S2, classy:join_node(N1, join))),
       ?assertEqual(ok, ?ON(S3, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster, S2),
       wait_site_joined(Sites, Cluster, S3),
       ?force_ordering(
          #{?snk_kind := test_go},
          #{?snk_kind := classy_test_vote_prep, for_real := true}),
       {ok, ID} = ?ON(S1,
                      classy_vote:create(#{ tag => Ref1
                                          , actions => #{ S => make_vote(true, true, Ref1, 1) ||
                                                          S <- [S2, S3]
                                                        }
                                          , post_vote => make_post_vote(Ref1)
                                          , strategy => {all, 10_000}
                                          })),
       stop_site(S1),
       ok = restart_site(S1),
       ?tp(notice, test_go, #{}),
       ?assertNot(classy_vote:test_wait_conclude(ID)),
       verify_no_votes(Nodes),
       Nodes
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     , {"rollback events",
        fun([_N1, N2, N3], Trace) ->
            ?assertMatch(
               [ #{ref := Ref1}
               ],
               ?of_node(N2, ?of_kind(classy_test_vote_rollback, Trace))),
            ?assertMatch(
               [ #{ref := Ref1}
               ],
               ?of_node(N3, ?of_kind(classy_test_vote_rollback, Trace))),
            ?assertMatch(
               [],
               ?of_kind(classy_test_vote_commit, Trace)),
            ?assertMatch(
               [#{ref := Ref1, result := false}],
               ?of_kind(classy_test_post_vote, Trace))
        end}
     | classy_vote:trace_props()
     ]).

%% Verify that restart of the participant during vote leads to abort
t_404_vote_part_restart(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  S3 = <<"s3">>,
  Sites = [S1, S2, S3],
  Ref1 = vote1,
  Ref2 = vote2,
  ?check_trace(
     #{timetrap => 30_000},
     begin
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       N3 = create_start_site(S3, #{}),
       Nodes = [N1, N2, N3],
       {ok, Cluster} = ?ON(S1, classy:the_cluster()),
       ?assertEqual(ok, ?ON(S2, classy:join_node(N1, join))),
       ?assertEqual(ok, ?ON(S3, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster, S2),
       wait_site_joined(Sites, Cluster, S3),
       %% Case 1: participant restarts *after* establishing vote
       %% request in the DB:
       ?force_ordering(
          #{?snk_kind := test_go1},
          #{ ?snk_kind := classy_test_vote_prep
           , for_real := true
           , ?snk_meta := #{node := N3}
           }),
       {ok, ID1} = ?ON(S1,
                       classy_vote:create(#{ tag => Ref1
                                           , actions => #{ S => make_vote(true, true, Ref1, 1) ||
                                                          S <- [S2, S3]
                                                         }
                                           , post_vote => make_post_vote(Ref1)
                                           , strategy => {all, 1_000}
                                           })),
       %% Make sure vote request is recorded in the DB:
       ?block_until(
          #{?snk_kind := ?classy_vote_part_established, id := ID1}),
       stop_site(S3),
       ?tp(notice, test_go1, #{}),
       ok = restart_site(S3),
       ?assertNot(classy_vote:test_wait_conclude(ID1)),
       verify_no_votes(Nodes),
       %% Case 2: participant restarts *before* recording the vote
       %% request into the DB. Flow should conclude without that
       %% participant.
       ?force_ordering(
          #{?snk_kind := test_go2},
          #{ ?snk_kind := ?classy_vote_part_recv
           , tag := Ref2
           , ?snk_meta := #{node := N3}
           }),
       {ok, ID2} = ?ON(S1,
                       classy_vote:create(#{ tag => Ref2
                                           , actions => #{ S => make_vote(true, true, Ref2, 1) ||
                                                          S <- [S2, S3]
                                                         }
                                           , post_vote => make_post_vote(Ref2)
                                           , strategy => {all, 1_000}
                                           })),
       stop_site(S3),
       ?tp(notice, test_go2, #{}),
       ok = restart_site(S3),
       ?assertNot(classy_vote:test_wait_conclude(ID2)),
       verify_no_votes(Nodes),
       Nodes
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     , {"rollback events",
        fun([_N1, N2, N3], Trace) ->
            ?assertMatch(
               [ #{ref := Ref1}
               , #{ref := Ref2}
               ],
               ?of_node(N2, ?of_kind(classy_test_vote_rollback, Trace))),
            ?assertMatch(
               [ #{ref := Ref1}
               ],
               ?of_node(N3, ?of_kind(classy_test_vote_rollback, Trace))),
            ?assertMatch(
               [],
               ?of_kind(classy_test_vote_commit, Trace)),
            ?assertMatch(
               [ #{ref := Ref1, result := false}
               , #{ref := Ref2, result := false}
               ],
               ?of_kind(classy_test_post_vote, Trace))
        end}
     | classy_vote:trace_props()
     ]).

%% Verify normal 2PC flow
t_410_vote_commit(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  S3 = <<"s3">>,
  Ref1 = vote1,
  Sites = [S1, S2, S3],
  ?check_trace(
     #{timetrap => 15_000},
     begin
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       N3 = create_start_site(S3, #{}),
       Nodes = [N1, N2, N3],
       {ok, Cluster} = ?ON(S1, classy:the_cluster()),
       ?assertEqual(ok, ?ON(S2, classy:join_node(N1, join))),
       ?assertEqual(ok, ?ON(S3, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster, S2),
       wait_site_joined(Sites, Cluster, S3),
       %% Vote stage fails:
       {ok, ID3} = ?ON(S1,
                       classy_vote:create(#{ tag => Ref1
                                           , actions => #{ S2 => make_vote(true, true, Ref1, 2)
                                                         , S3 => make_vote(true, true, Ref1, 1)
                                                         }
                                           , post_vote => make_post_vote(Ref1)
                                           })),
       ?assert(classy_vote:test_wait_conclude(ID3)),
       verify_no_votes(Nodes),
       Nodes
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     , {"commit events",
        fun([_N1, N2, N3], Trace) ->
            ?assertMatch(
               [ #{step := 1, ref := Ref1}
               , #{step := 2, ref := Ref1}
               ],
               ?of_node(N2, ?of_kind(classy_test_vote_commit, Trace))),
            ?assertMatch(
               [ #{step := 1, ref := Ref1}
               ],
               ?of_node(N3, ?of_kind(classy_test_vote_commit, Trace))),
            ?assertMatch(
               [#{ref := Ref1, result := true}],
               ?of_kind(classy_test_post_vote, Trace))
        end}
     | classy_vote:trace_props()
     ]).

%% Verify that commit flows finish after node restart:
t_411_commit_actions_after_restart(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  Ref1 = vote1,
  Sites = [S1, S2],
  ?check_trace(
     #{timetrap => 30_000},
     begin
       N1 = create_start_site(S1, #{peer => #{shutdown => halt}}),
       N2 = create_start_site(S2, #{peer => #{shutdown => halt}}),
       Nodes = [N1, N2],
       {ok, Cluster} = ?ON(S1, classy:the_cluster()),
       ?assertEqual(ok, ?ON(S2, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster, S2),
       %% Make sure post commit actions are delayed:
       ?force_ordering(
          #{?snk_kind := test_go},
          #{?snk_kind := K} when K =:= ?classy_vote_part_perform_action;
                                 K =:= ?classy_vote_coord_post_actions),
       {ok, ID} = ?ON(S1,
                      classy_vote:create(#{ tag => Ref1
                                          , actions => #{S2 => make_vote(true, true, Ref1, 1)}
                                          , post_vote => make_post_vote(Ref1)
                                          })),
       ?block_until(#{?snk_kind := ?classy_vote_coord_commit, id := ID}),
       ?block_until(#{?snk_kind := ?classy_vote_part_stage, to := 2, id := ID}),
       %% Restart sites:
       [familiar:kill_site({get_cluster(), S}) || S <- Sites],
       ?tp(notice, test_go, #{}),
       [ok = restart_site(S) || S <- Sites],
       ?assert(classy_vote:test_wait_conclude(ID)),
       verify_no_votes(Nodes),
       Nodes
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     , {"commit events",
        fun([_N1, N2], Trace) ->
            ?assertMatch(
               [ #{?snk_kind := test_go}
               , #{ ?snk_kind := classy_test_vote_commit
                  , step := 1
                  , ref := Ref1
                  , ?snk_meta := #{node := N2}
                  }
               ],
               ?of_kind([classy_test_vote_commit, test_go], Trace))
        end}
     , {"post vote events",
        fun(Trace) ->
            ?assertMatch(
               [ #{ref := Ref1, result := true}
               ],
               ?of_kind(classy_test_post_vote, Trace))
        end}
     , {"participant stages",
        fun([_N1, _N2], Trace) ->
            ?assertMatch(
               [ #{from := 0, to := 0} %% Enter prepare
               , #{from := 0, to := 1} %% Enter wait outcome
               , #{from := 1, to := 2} %% Received outcome; enter commit
               , #{?snk_kind := test_go} %% Restarted
               , #{from := 2, to := 2} %% Restored state into commit stage
               , #{?snk_kind := classy_test_vote_commit}
               ],
               ?of_kind([?classy_vote_part_stage, test_go, classy_test_vote_commit], Trace))
        end}
     | classy_vote:trace_props()
     ]).

%% Verify that failed commit flows are retried after node restart.
t_412_commit_action_crash(_) ->
  S1 = <<"s1">>,
  S2 = <<"s2">>,
  Ref1 = vote1,
  Sites = [S1, S2],
  ?check_trace(
     #{timetrap => 30_000},
     begin
       N1 = create_start_site(S1, #{}),
       N2 = create_start_site(S2, #{}),
       Nodes = [N1, N2],
       {ok, Cluster} = ?ON(S1, classy:the_cluster()),
       ?assertEqual(ok, ?ON(S2, classy:join_node(N1, join))),
       wait_site_joined(Sites, Cluster, S2),
       %% Inject failures into the commit flows:
       InjErr1 = ?inject_crash(
                    #{?snk_kind := classy_test_vote_commit, step := 2, ref := Ref1},
                    snabbkaffe_nemesis:always_crash()),
       InjErr2 = ?inject_crash(
                    #{?snk_kind := classy_test_post_vote, ref := Ref1},
                    snabbkaffe_nemesis:always_crash()),
       %% Start vote:
       {ok, ID} = ?ON(S1,
                      classy_vote:create(#{ tag => Ref1
                                          , actions => #{S2 => make_vote(true, true, Ref1, 3)}
                                          , post_vote => make_post_vote(Ref1)
                                          , on_fail => make_vote_on_fail(Ref1)
                                          })),
       %% Wait until failures are detected:
       ?block_until(#{?snk_kind := classy_test_vote_on_fail, id := ID, stage := coord_post_vote}),
       %% Participant:
       ?block_until(#{?snk_kind := classy_test_vote_on_fail, id := ID, stage := I} when is_integer(I)),
       %% Restart sites:
       [stop_site(S) || S <- Sites],
       snabbkaffe_nemesis:fix_crash(InjErr1),
       snabbkaffe_nemesis:fix_crash(InjErr2),
       ?tp(notice, test_restarted, #{}),
       [ok = restart_site(S) || S <- Sites],
       ?assert(classy_vote:test_wait_conclude(ID)),
       verify_no_votes(Nodes),
       Nodes
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     , {"coordinator history",
        fun([N1, _N2], Trace) ->
            ?assertMatch(
               [ #{from := 0, to := 0} %% Enter vote
               , #{from := 0, to := 10} %% Enter commit
               , #{?snk_kind := classy_test_vote_on_fail, tag := Ref1, id := _, reason :=_ }
                 %% Restarted
               , #{from := 10, to := 10} %% Re-enter commit
               , #{?snk_kind := classy_test_post_vote, ref := Ref1}
               ],
               ?of_node(N1,
                        ?of_kind(
                           [ ?classy_vote_coord_stage
                           , classy_test_post_vote
                           , classy_test_vote_on_fail
                           ],
                           Trace)))
        end}
     , {"participant history",
        fun([_N1, N2], Trace) ->
            ?assertMatch(
               [ #{from := 0, to := 0} %% Enter prepare
               , #{from := 0, to := 1} %% Enter wait outcome
               , #{from := 1, to := 2} %% Received outcome; enter commit
               , #{?snk_kind := classy_test_vote_commit, ref := Ref1, step := 1}
               , #{?snk_kind := classy_test_vote_on_fail, tag := Ref1, reason := _, id := _}
                 %% Restarted
               , #{from := 2, to := 2} %% Restored state into commit stage
               , #{?snk_kind := classy_test_vote_commit, ref := Ref1, step := 2}
               , #{?snk_kind := classy_test_vote_commit, ref := Ref1, step := 3}
               ],
               ?of_node(N2,
                        ?of_kind([ ?classy_vote_part_stage
                                 , classy_test_vote_commit
                                 , classy_test_vote_on_fail
                                 ],
                                 Trace)))
        end}
     | classy_vote:trace_props()
     ]).

%% Verify classy_vote:ls_votes functions (implicitly verify `classy_vote:fold_ongoing')
t_413_fold_votes(_) ->
  S1 = <<"s1">>,
  Ref1 = vote1,
  Ref2 = vote2,
  ?check_trace(
     #{timetrap => 30_000},
     begin
       N1 = create_start_site(S1, #{peer => #{shutdown => halt}}),
       %% Make sure votes hang long enough for us to inspect them:
       ?force_ordering(
          #{?snk_kind := test_go},
          #{?snk_kind := K} when K =:= classy_test_vote_commit;
                                 K =:= classy_test_post_vote),
       {ok, ID1} = ?ON(S1,
                       classy_vote:create(#{ tag => Ref1
                                           , actions => #{S1 => make_vote(true, true, Ref1, 1)}
                                           , post_vote => make_post_vote(Ref1)
                                           })),
       {ok, ID2} = ?ON(S1,
                       classy_vote:create(#{ tag => Ref2
                                           , actions => #{S1 => make_vote(true, true, Ref2, 1)}
                                           , post_vote => make_post_vote(Ref2)
                                           })),
       ct:sleep(100),
       ?assertMatch(
          [ #{ id := _
             , tag := Ref1
             , role := participant
             , coordinator := <<"s1">>
             }
          , #{ id := _
             , tag := Ref1
             , role := coordinator
             , start_time := _
             , participants := [<<"s1">>]
             }
          ],
          ?ON(S1, lists:sort(classy_vote:ls_votes(Ref1)))),
       ?assertMatch(
          [ #{ id := _
             , tag := Ref2
             , role := participant
             , coordinator := <<"s1">>
             }
          , #{ id := _
             , tag := Ref2
             , role := coordinator
             , start_time := _
             , participants := [<<"s1">>]
             }
          ],
          ?ON(S1, lists:sort(classy_vote:ls_votes(Ref2)))),
       ?assertMatch(
          [ #{ id := _
             , tag := Ref1
             , role := participant
             , coordinator := <<"s1">>
             }
          , #{ id := _
             , tag := Ref2
             , role := participant
             , coordinator := <<"s1">>
             }
          , #{ id := _
             , tag := Ref1
             , role := coordinator
             , start_time := _
             , participants := [<<"s1">>]
             }
          , #{ id := _
             , tag := Ref2
             , role := coordinator
             , start_time := _
             , participants := [<<"s1">>]
             }
          ],
          ?ON(S1, lists:sort(classy_vote:ls_votes()))),
       ok
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

%% This function fails if `Site' reports any site that must be stopped
%% according to the spec as running.
no_stopped_nodes_reported_as_running(Site, #{sites := Sites}) ->
  StoppedNodes = maps:fold(
                   fun(Peer, #{running := Running}, Acc) ->
                       case Running of
                         false ->
                           case fuzz_node_name(Peer) of
                             {ok, Node} -> [Node | Acc];
                             undefined  -> Acc
                           end;
                         true  -> Acc
                       end
                   end,
                   [],
                   Sites),
  Running = classy_test_fuzzer:call(Site, classy, nodes, [running]),
  ?assertMatch(
     Running,
     Running -- StoppedNodes,
     #{ msg => stopped_node_is_reported_as_running
      , on_site => Site
      , sites => Sites
      }).

%%================================================================================
%% Trace specs
%%================================================================================

no_unexpected_events(Trace) ->
  ?assertMatch(
     [],
     ?of_kind(
        [ ?classy_unknown_event
        , ?classy_abnormal_exit
        , ?classy_table_anomaly
        , ?classy_hook_failure
        , classy_discovery_failure
        , classy_table_on_update_callback_failure
        , ?classy_bad_data
        , ?classy_run_level_change_error
        ],
        Trace)).

events_on_all_sites(Trace) ->
  Sites = ?projection(local, ?of_kind(classy_create_new_site, Trace)),
  lists:foreach(
    fun(Site) ->
        ?assertMatch(
           {_, _},
           site_events(Site, Trace))
    end,
    Sites).

%% Verify sequence of site events, return the last event and the number of site events
site_events(Site, Trace) ->
  lists:foldl(
    fun(Event, {NEvents, PrevEvent}) ->
        case site_of_event(Event) of
          Site ->
            {NEvents + 1, validate_site_event(PrevEvent, Event)};
          _ ->
            {NEvents, PrevEvent}
        end
    end,
    {0, undefined},
    Trace).

%%    Ignore the following events:
validate_site_event(Prev, #{?snk_kind := Kind}) when
    Kind =:= classy_member_join;
    Kind =:= classy_member_leave;
    Kind =:= classy_init_clustering;
    Kind =:= classy_peer_up;
    Kind =:= classy_peer_down ->
  Prev;
%%    Site creation:
validate_site_event(undefined,
                    #{?snk_kind := classy_create_new_site} = E) ->
  E;
validate_site_event(#{?snk_kind := classy_create_new_site},
                    #{?snk_kind := classy_change_run_level, to := single} = E) ->
  E;
validate_site_event(#{?snk_kind := classy_create_new_site},
                    #{?snk_kind := classy_create_new_cluster} = E) ->
  E;
%%    Run level changes:
validate_site_event(#{?snk_kind := classy_change_run_level, to := stopped},
                    #{?snk_kind := classy_change_run_level, to := single} = E) ->
  E;
validate_site_event(#{?snk_kind := classy_change_run_level, to := single},
                    #{?snk_kind := classy_change_run_level, to := cluster} = E) ->
  E;
validate_site_event(#{?snk_kind := classy_change_run_level, to := cluster},
                    #{?snk_kind := classy_change_run_level, to := quorum} = E) ->
  E;
validate_site_event(#{?snk_kind := classy_change_run_level, to := quorum},
                    #{?snk_kind := classy_change_run_level, to := cluster} = E) ->
  E;
validate_site_event(#{?snk_kind := classy_change_run_level, to := cluster},
                    #{?snk_kind := classy_change_run_level, to := single} = E) ->
  E;
validate_site_event(#{?snk_kind := classy_change_run_level, to := single},
                    #{?snk_kind := classy_change_run_level, to := stopped} = E) ->
  E;
%%   Change of the cluster:
validate_site_event(#{?snk_kind := classy_change_run_level, to := stopped},
                    #{?snk_kind := classy_kicked_from_cluster} = E) ->
  E;
validate_site_event(#{?snk_kind := classy_kicked_from_cluster},
                    #{?snk_kind := classy_joined_cluster} = E) ->
  E;
validate_site_event(#{?snk_kind := classy_kicked_from_cluster},
                    #{?snk_kind := classy_create_new_cluster} = E) ->
  E;
validate_site_event(#{?snk_kind := classy_joined_cluster},
                    #{?snk_kind := classy_change_run_level, to := single} = E) ->
  E;
validate_site_event(#{?snk_kind := classy_create_new_cluster},
                    #{?snk_kind := classy_change_run_level, to := single} = E) ->
  E;
%%   Abrupt stop:
validate_site_event(_,
                    #{?snk_kind := familiar_peer_stop} = E) ->
  E;
validate_site_event(#{?snk_kind := familiar_peer_stop},
                    #{?snk_kind := classy_change_run_level, to := single} = E) ->
  E.

site_of_event(#{?snk_kind := Kind, local := Site}) when
    Kind =:= classy_create_new_site;
    Kind =:= classy_create_new_cluster;
    Kind =:= classy_member_join;
    Kind =:= classy_member_leave;
    Kind =:= classy_joined_cluster;
    Kind =:= classy_kicked_from_cluster;
    Kind =:= classy_change_run_level;
    Kind =:= classy_init_clustering ->
  Site;
site_of_event(#{?snk_kind := Kind, ?snk_meta := #{local := Site}}) when
    Kind =:= classy_peer_up;
    Kind =:= classy_peer_down ->
  Site;
site_of_event(#{?snk_kind := familiar_peer_stop, site := Site}) ->
  Site;
site_of_event(_) ->
  undefined.

%%================================================================================
%% Internal functions
%%================================================================================

init_per_suite(Cfg) ->
  Cfg.

end_per_suite(Cfg) ->
  Cfg.

next_state(_S, _Ret, Call) ->
  error({unknown_call, Call}).

init_per_testcase(TC, Cfg) ->
  Fixtures = [ {familiar_snabbkaffe, #{}}
             ],
  ok = familiar:start_link_cluster(
         #{ id => TC
          , fixtures => familiar:default_fixtures() ++ Fixtures
          , peer => #{args => ["-kernel", "prevent_overlapping_partitions", "false"]}
          }),
  put(classy_SUITE_cluster, {ok, TC}),
  Cfg.

get_cluster() ->
  {ok, Cluster} = get(classy_SUITE_cluster),
  Cluster.

create_start_site(Site, CustomConf) ->
  create_start_site(get_cluster(), Site, CustomConf).

create_start_site(Cluster, Site, CustomConf) ->
  Fixture = {familiar_app,
             #{ app => classy
              , env => #{ setup_hooks => {?MODULE, setup_hooks, [Site]}
                        , cleanup_check_interval => 100
                        , vote_retry_interval => 100
                        , rpc_timeout => 100
                        }
              }},
  Fixtures = maps:get(fixtures, CustomConf, []),
  Conf = CustomConf#{fixtures => [Fixture | Fixtures], start => true},
  case familiar:create_site(Cluster, Site, Conf) of
    {ok, _Site, Node} ->
      Node;
    Err ->
      error({failed_to_create_test_site, #{ cluster => Cluster
                                          , site => Site
                                          , conf => CustomConf
                                          , reason => Err
                                          }})
  end.

stop_site(Site) ->
  familiar:stop_site(get_cluster(), Site).

restart_site(Site) ->
  ?assertMatch(
     {ok, _},
     familiar:start_site({get_cluster(), Site})).

end_per_testcase(TC, Cfg) ->
  Success = case proplists:get_value(tc_status, Cfg) of
              ok -> true;
              _  -> false
            end,
  _ = familiar:stop_cluster(TC, Success),
  snabbkaffe:stop().

all() ->
  all(?MODULE).

all(Module) ->
  [I || {I, 1} <- Module:module_info(exports), I > 't_', I < 't`'].

wait_site_joined(WaitOnSites, Cluster, Site) ->
  lists:foreach(
    fun(Local) ->
        ?block_until(
           #{ ?snk_kind := classy_member_join
            , cluster := Cluster
            , local := Local
            , remote := Site
            })
    end,
    WaitOnSites),
  %% Account for possible race condition since the hook emitting the event is the first:
  ct:sleep(10).

sync_kick(ExecOn, Target, Intent, WaitOn) ->
  Pred = fun(#{?snk_kind := classy_member_leave, remote := Target, local := Local}) ->
             lists:member(Local, WaitOn);
            (#{?snk_kind := classy_kicked_from_cluster, local := Target}) ->
             true;
            (_) ->
             false
         end,
  {ok, Sub} = snabbkaffe:subscribe(Pred, length(WaitOn), infinity, 0),
  ?ON(ExecOn, classy:kick_site(Target, Intent)),
  {ok, _} = snabbkaffe:receive_events(Sub),
  ok.

wait_site_kicked(WaitOnSites, Cluster, Site) ->
  lists:foreach(
    fun(Local) ->
        ?block_until(
           #{ ?snk_kind := classy_member_leave
            , cluster := Cluster
            , local := Local
            , remote := Site
            })
    end,
    WaitOnSites -- [Site]),
  case lists:member(Site, WaitOnSites) of
    true ->
      ?block_until(#{?snk_kind := classy_kicked_from_cluster, local := Site});
    false ->
      ok
  end,
  %% Account for possible race condition since the hook emitting the event is the first:
  ct:sleep(10).

initialization_hooks(RuntimeData, Trace) ->
  #{ nodes := Nodes
   , sites := Sites
   , clusters := Clusters
   } = RuntimeData,
  ?assertSameSet(
     Nodes,
     ?projection(node, ?of_kind(classy_on_node_init, Trace))),
  ?assertSameSet(
     Sites,
     ?projection(local, ?of_kind(classy_create_new_site, Trace))),
  ?assertSameSet(
     Clusters,
     ?projection(cluster, ?of_kind(classy_create_new_cluster, Trace))).

setup_hooks(Site) ->
  classy:on_node_init(
    fun() ->
        classy_node:maybe_init_the_site(Site)
    end,
    0).

make_vote(HowToPreVote, HowToVote, Ref, NCommitSteps) ->
  #{ prepare  => {?MODULE, vote_prepare, [HowToPreVote, HowToVote, Ref]}
   , commit   => [{?MODULE, vote_commit, [Step, Ref]} ||
                   Step <- lists:seq(1, NCommitSteps)]
   , rollback => [{?MODULE, vote_rollback, [Ref]}]
   }.

make_post_vote(Ref) ->
  {?MODULE, post_vote, [Ref]}.

make_vote_on_fail(Ref) ->
  {?MODULE, vote_on_fail, [Ref]}.

vote_prepare(ForReal, Id, HowToPreVote, HowToVote, Ref) ->
  Result = case ForReal of
             true -> HowToVote;
             false -> HowToPreVote
           end,
  ?tp(classy_test_vote_prep,
      #{ ref => Ref
       , vote => Result
       , for_real => ForReal
       , id => Id
       }),
  Result.

vote_commit(Id, Step, Ref) ->
  ?tp(classy_test_vote_commit,
      #{ ref => Ref
       , step => Step
       , id => Id
       }).

vote_rollback(Id, Ref) ->
  ?tp(classy_test_vote_rollback, #{ref => Ref, id => Id}).

post_vote(Result, Id, Ref) ->
  ?tp(classy_test_post_vote, #{ref => Ref, result => Result, id => Id}).

vote_on_fail(FailInfo, Ref) ->
  ?tp(classy_test_vote_on_fail, FailInfo#{test_ref => Ref}).

verify_no_votes(Nodes) ->
  Results = erpc:multicall(Nodes, ets, tab2list, [classy_vote_table]),
  [?assertMatch({ok, []}, Result, Node) || {Node, Result} <- lists:zip(Nodes, Results)].

-spec proper_printout(string(), list()) -> _.
proper_printout(Char, []) when Char =:= ".";
                               Char =:= "x";
                               Char =:= "!" ->
  ct:print("~s", [[Char]]);
proper_printout(Fmt, Args) ->
  ct:pal(Fmt, Args).

fuzz_node_name(Site) ->
  familiar:last_node({classy_test_fuzzer:familiar_cluster(), Site}).
