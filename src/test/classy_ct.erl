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
  Fixture = {familiar_app,
             #{ app => classy
              , env => #{ setup_hooks => {?MODULE, setup_hooks, [Site]}
                        , cleanup_check_interval => 100
                        , vote_retry_interval => 100
                        , rpc_timeout => 100
                        , discovery_interval => 100
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

get_cluster() ->
  {ok, Cluster} = get(classy_SUITE_cluster),
  Cluster.

setup_hooks(Site) ->
  %% Not-so-elegant way to avoid setting `on_node_init' hook to a
  %% closure. When closure is used, it interacts badly with code
  %% load/unload, and makes `no_unexpected_events' property flaky,
  %% when hooks fail with badfun.
  persistent_term:put(classy_ct_site, Site),
  classy:on_node_init(fun ?MODULE:on_node_init/0, 0).

on_node_init() ->
  classy_node:maybe_init_the_site(persistent_term:get(classy_ct_site)),
  classy:on_metadata_change(fun ?MODULE:on_metadata_change/3, 0),
  classy:on_node_classify(fun ?MODULE:on_node_classify/1, 0),
  classy:run_level(fun ?MODULE:on_run_level/2, 0).


on_run_level(Prev, Next) ->
  ?defer_assert(?assertEqual(Next, classy:run_level())),
  ?defer_assert(case {Prev, Next} of
                  {stopped, single} -> ok;
                  {single, cluster} -> ok;
                  {cluster, quorum} -> ok;
                  {quorum, cluster} -> ok;
                  {cluster, single} -> ok;
                  {single, stopped} -> ok
                end).

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
        ],
        Trace)).
