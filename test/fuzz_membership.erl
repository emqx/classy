%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(fuzz_membership).

-export([prop_fuzz/0, postcondition/3]).

-include_lib("kernel/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("classy_test_macros.hrl").
-include("classy.hrl").

prop_fuzz() ->
  NCommandsFactor = 2,
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
       run_cmds(Cmds),
       true
     catch
       EC:Err:Stack ->
         ?LOG_CRITICAL("!!!! Property failed ~p:~p:~p", [EC, Err, Stack]),
         false
     end).

run_cmds(Cmds) ->
  Cluster = classy_test_fuzzer:familiar_cluster(),
  ?check_trace(
     #{timetrap => 5_000 * length(Cmds) + 30_000},
     try
       %% Print information about the run:
       ?LOG_NOTICE("*** Commands:~n~s~n", [classy_test_fuzzer:format_cmds(Cmds)]),
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
       ?LOG_INFO("*** Model state:~n  ~p~n", [State]),
       ?LOG_INFO("*** Result:~n  ~p~n", [Result]),
       Result =:= ok orelse error({invalid_result, Result}),
       familiar:stop_cluster(Cluster, true)
     catch
       EC:Err:Stack ->
         ?LOG_ERROR("*** ~p:~p~n Stack:~p", [EC, Err, Stack])
     after
       familiar:stop_cluster(Cluster, false)
     end,
     [ fun classy_SUITE:no_unexpected_events/1
     , fun classy_SUITE:events_on_all_sites/1
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
  classy_SUITE:no_stopped_nodes_reported_as_running(Site, S),
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
     [Node || I <- ExpectedSites, {ok, Node} <- [classy_SUITE:fuzz_node_name(I)]],
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
         {ok, Node} <- [classy_SUITE:fuzz_node_name(I)],
         classy_test_fuzzer:is_running(I, S)],
     classy_test_fuzzer:call(Site, classy, nodes, [running]),
     #{ on            => Site
      , msg           => "View of running nodes"
      , '~diagnostic' => classy_test_fuzzer:diagnostic(S)
      , model_state   => S
      }),
  ok.
