%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_ct).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("classy_internal.hrl").

create_cluster(ID) ->
  Fixtures = [ {familiar_snabbkaffe, #{}}
             ],
  familiar:start_link_cluster(
    #{ id => ID
     , fixtures => familiar:default_fixtures() ++ Fixtures
     , peer => #{args => ["-kernel", "prevent_overlapping_partitions", "false"]}
     }).

create_start_site(Site, CustomConf) ->
  create_start_site(get_cluster(), Site, CustomConf).

create_start_site(Cluster, Site, CustomConf) ->
  ClusterId = maps:get(cluster_id, CustomConf, undefined),
  AppFixture = {familiar_app,
                #{ app => classy
                 , env => #{ setup_hooks => {?MODULE, setup_hooks, [Site, ClusterId]}
                           , cleanup_check_interval => 100
                           , vote_retry_interval => 100
                           , rpc_timeout => 100
                           , discovery_interval => 100
                           }
                 }},
  StartSysFixture = {classy_start_system_fixture, #{}},
  Fixtures = maps:get(fixtures, CustomConf, []),
  Conf = CustomConf#{ fixtures => [AppFixture, StartSysFixture | Fixtures]
                    , start => true
                    },
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

get_cluster() ->
  {ok, Cluster} = get(classy_SUITE_cluster),
  Cluster.

setup_hooks(Site, MaybeClusterId) ->
  %% Not-so-elegant way to avoid setting `on_node_init' hook to a
  %% closure. When closure is used, it interacts badly with code
  %% load/unload, and makes `no_unexpected_events' property flaky,
  %% when hooks fail with badfun.
  persistent_term:put(classy_ct_site, {Site, MaybeClusterId}),
  classy:on_node_init(fun ?MODULE:on_node_init/0, 0).

on_node_init() ->
  {Site, MaybeClusterId} = persistent_term:get(classy_ct_site),
  case MaybeClusterId of
    undefined ->
      classy_node:maybe_init_the_site(Site);
    _ ->
      classy_node:maybe_init_the_site(Site, MaybeClusterId)
  end,
  classy:on_metadata_change(fun ?MODULE:on_metadata_change/3, 0),
  classy:on_node_classify(fun ?MODULE:on_node_classify/1, 0),
  classy:on_run_level(fun ?MODULE:on_run_level/2, 0).


on_run_level(Action, Level) when (Action =:= enter orelse Action =:= leave),
                                 ?valid_run_level(Level) ->
  %% Valid run level hook data
  if ?predefined_run_level(Level); Level =:= 0 ->
      ?tp(test_rl, #{Action => classy_rl_changer:classify(Level)});
     true ->
      ok
  end,
  %% Verify that run level observed by `classy:run_level' API doesn't
  %% change until all hooks are complete:
  case Action of
    enter ->
      ?defer_assert(?assertEqual(
                       max(0, Level - 1),
                       classy:run_level(),
                       "Current level when entering"));
    leave ->
      ?defer_assert(?assertEqual(
                       Level,
                       classy:run_level(),
                       "Current level when leaving"))
  end;
on_run_level(Action, Level) ->
  ?defer_assert(error({invalid_run_level, Action, Level})).

on_metadata_change(Cluster, Site, Meta) ->
  ?tp(notice, test_update_meta, Meta#{cluster => Cluster, site => Site}).

on_node_classify(Meta) ->
  maps:keys(Meta).

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
        , ?classy_rl_changer_worker_crash
        ],
        Trace)).
