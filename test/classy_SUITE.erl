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

-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("proper/include/proper.hrl").
-include("src/classy_internal.hrl").
-include_lib("familiar/include/familiar.hrl").

-define(assertSameSet(EXP, GOT), ?assertEqual(lists:sort(EXP), lists:sort(GOT))).
-define(assertSameSet(EXP, GOT, COMMENT), ?assertEqual(lists:sort(EXP), lists:sort(GOT), COMMENT)).

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
          ?ON(S1, classy_node:the_cluster())),
       ?assertEqual(
          [S1],
          ?ON(S1, classy:sites())),
       ?assertEqual(
          {ok, Cluster2},
          ?ON(S2, classy_node:the_cluster())),
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
          ?ON(S1, classy_node:the_cluster())),
       ?assertEqual(
          {ok, Cluster1},
          ?ON(S2, classy_node:the_cluster())),
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
       timer:sleep(1000),
       ?block_until(#{?snk_kind := classy_change_run_level, to := quorum}),
       ?assertMatch(
          {ok, hello},
          ?ON(S1,
              classy:at_lower_level(
                single,
                fun() ->
                    hello
                end)))
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
       {ok, Cluster} = ?ON(S1, classy_node:the_cluster()),
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

%% This testcase verifies `classy:info()' and
%% `classy_node:cluster_info/0' and `classy:clusters/1' functions.
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
       ?block_until(#{?snk_kind := classy_member_join})
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
       {ok, Cluster} = ?ON(S1, classy_node:the_cluster()),
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
                #{S => {erlang, node, []} || S <- [S1, S2, SB]}, 5_000))),
       %% Handling of throw:
       ?assertEqual(
          #{ S1 => {error, {throw, {foo, S1}}}
           , S2 => {error, {throw, {foo, S2}}}
           , SB => {error, site_is_down}
           },
          ?ON(S1,
              classy_lib:multicall(
                #{S => {erlang, throw, [{foo, S}]} || S <- [S1, S2, SB]}, 5_000))),
       %% Handling of errors:
       ?assertMatch(
          #{ S1 := {error, {error, {foo, S1}, _}}
           , S2 := {error, {error, {foo, S2}, _}}
           , SB := {error, site_is_down}
           },
          ?ON(S1,
              classy_lib:multicall(
                #{S => {erlang, error, [{foo, S}]} || S <- [S1, S2, SB]}, 5_000))),
       ?assertEqual(
          #{ S1 => {error, {exit, {foo, S1}}}
           , S2 => {error, {exit, {foo, S2}}}
           , SB => {error, site_is_down}
           },
          ?ON(S1,
              classy_lib:multicall(
                #{S => {erlang, exit, [{foo, S}]} || S <- [S1, S2, SB]}, 5_000))),
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

t_999_fuzz(_Config) ->
  %% NOTE: we set timeout at the lowest level to capture the trace
  %% and have a nicer error message.
  %%
  %% By default the number of tests and max_size are set to low
  %% values to avoid blowing up CI. Hence it's recommended to
  %% increase the max_size and numtests when doing local
  %% development using "apps/emqx/test/sessds.cfg"
  NTests = ct:get_config({fuzzer, n_tests}, 20),
  MaxSize = ct:get_config({fuzzer, max_size}, 100),
  NCommandsFactor = ct:get_config({fuzzer, command_multiplier}, 1),
  ?assertMatch(
     true,
     proper:quickcheck(
       ?FORALL(
          Cmds,
          classy_test_fuzzer:cmds(
            NCommandsFactor,
            #{ module => ?MODULE
             , sites => [ {<<"foo">>, #{}}
                        , {<<"bar">>, #{}}
                        , {<<"baz">>, #{}}
                        , {<<"quux">>, #{}}
                        ]
             }),
          try
            fuzz_prop(Cmds),
            true
          catch
            EC:Err:Stack ->
              ct:pal("!!!! Property failed ~p:~p:~p", [EC, Err, Stack]),
              false
          end)
      , [ {numtests, NTests}
        , {max_size, MaxSize}
        , {on_output, fun proper_printout/2}
          %% TODO: Shrinking is currently broken
        , {max_shrinks, 0}
        ]
      )).

fuzz_prop(Cmds) ->
  Cluster = classy_test_fuzzer:familiar_cluster(),
  ?check_trace(
     #{timetrap => 5_000 * length(Cmds) + 30_000},
     try
       %% Print information about the run:
       ct:pal("*** Commands:~n~s~n", [classy_test_fuzzer:format_cmds(Cmds)]),
       %% Initialize the system:
       ok = familiar:start_link_cluster(
              #{ id => Cluster
               , peer => #{args => ["-kernel", "prevent_overlapping_partitions", "false"]}
               , fixtures => familiar_fixture:defaults() ++ [{familiar_snabbkaffe, #{}}]
               }),
       %% Run test:
       {_History, State, Result} = proper_statem:run_commands(
                                     classy_test_fuzzer,
                                     classy_test_fuzzer:wrap_commands(Cmds)),
       ct:log(info, "*** Model state:~n  ~p~n", [State]),
       ct:log("*** Result:~n  ~p~n", [Result]),
       Result =:= ok orelse error({invalid_result, Result}),
       familiar:stop_cluster(Cluster, true)
     catch
       EC:Err:Stack ->
         ct:pal(error, "*** ~p:~p~n Stack:~p", [EC, Err, Stack])
     after
       familiar:stop_cluster(Cluster, false)
     end,
     [ fun no_unexpected_events/1
     , fun events_on_all_sites/1
     ]).

postcondition({init, _}, _Call, _Result) ->
  true;
postcondition(S, _Call, _Result) ->
  lists:foreach(
    fun(Site) ->
        ?retry(1000, 10, fuzz_verify_site(Site, S))
    end,
    classy_test_fuzzer:running_sites(S)),
  true.

fuzz_verify_site(Site, S = #{sites := Sites}) ->
  #{Site := #{cluster := Cluster}} = Sites,
  no_stopped_nodes_reported_as_running(Site, S),
  %% Verify list of peer sites:
  ExpectedSites = classy_test_fuzzer:sites_of_cluster(Cluster, S),
  ?assertSameSet(
     ExpectedSites,
     classy_test_fuzzer:call(Site, classy, sites, []),
     #{ on            => Site
      , msg           => "View of the cluster"
      , '~diagnostic' => classy_test_fuzzer:diagnostic(S)
      , model_state   => S
      }),
  %% Verify list of all nodes:
  ?assertSameSet(
     [Node || I <- ExpectedSites, {ok, Node} <- [fuzz_node_name(I)]],
     classy_test_fuzzer:call(Site, classy, nodes, [all]),
     #{ on            => Site
      , msg           => "View of all nodes"
      , '~diagnostic' => classy_test_fuzzer:diagnostic(S)
      , model_state   => S
      }),
  %% Check running nodes:
  ?assertSameSet(
     [Node
      || I <- ExpectedSites,
         {ok, Node} <- [fuzz_node_name(I)],
         classy_test_fuzzer:is_running(I, S)],
     classy_test_fuzzer:call(Site, classy, nodes, [running]),
     #{ on            => Site
      , msg           => "View of running nodes"
      , '~diagnostic' => classy_test_fuzzer:diagnostic(S)
      , model_state   => S
      }),
  ok.

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
        , classy_hook_failure
        , classy_discovery_failure
        , classy_table_on_update_callback_failure
        , ?classy_bad_data
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
                    #{?snk_kind := classy_test_site_stop} = E) ->
  E;
validate_site_event(#{?snk_kind := classy_test_site_stop},
                    #{?snk_kind := classy_change_run_level, to := single} = E) ->
  E.

site_of_event(#{?snk_kind := Kind, local := Site}) when
    Kind =:= classy_create_new_site;
    Kind =:= classy_create_new_cluster;
    Kind =:= classy_member_join;
    Kind =:= classy_member_leave;
    Kind =:= classy_joined_cluster;
    Kind =:= classy_kicked_from_cluster;
    Kind =:= classy_init_clustering ->
  Site;
site_of_event(#{?snk_kind := Kind, ?snk_meta := #{local := Site}}) when
    Kind =:= classy_change_run_level;
    Kind =:= classy_peer_up;
    Kind =:= classy_peer_down ->
  Site;
site_of_event(#{?snk_kind := classy_test_site_stop, site := Site}) ->
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

init_per_testcase(t_999_fuzz, Cfg) ->
  Cfg;
init_per_testcase(TC, Cfg) ->
  Fixtures = [ {familiar_snabbkaffe, #{}}
             ],
  ok = familiar:start_link_cluster(
         #{ id => TC
          , fixtures => familiar:default_fixtures() ++ Fixtures
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
    WaitOnSites),
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

-spec proper_printout(string(), list()) -> _.
proper_printout(Char, []) when Char =:= ".";
                               Char =:= "x";
                               Char =:= "!" ->
  ct:print("~s", [[Char]]);
proper_printout(Fmt, Args) ->
  ct:pal(Fmt, Args).

fuzz_node_name(Site) ->
  familiar:last_node({classy_test_fuzzer:familiar_cluster(), Site}).
