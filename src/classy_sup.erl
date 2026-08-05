%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(classy_sup).
-moduledoc false.

-behavior(supervisor).

%% API:
-export([ start_link/0
        , stop/1
        , start_table/2
        , ensure_membership/2
        , ensure_vote_coordinator/2
        , ensure_vote_participant/2
        , ensure_liveness_server/0
        , terminate_liveness_server/0
        , ensure_vote_sup/1
        , terminate_vote_sup/1
        , prep_stop/0
        ]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([ start_link_table_sup/0
        , start_link_membership_sup/0
        , start_link_vote_sup/0
        , start_link_vote_coordinator_sup/1
        , start_link_vote_participant_sup/1
        ]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-record(top, {}).
-record(table_sup, {}).
-record(membership_sup, {}).
-record(dynamic_sup, {}).
-record(vote_sup, {type :: coordinators | participants}).

-define(SUP, ?MODULE).
-define(TABLE_SUP, classy_table_sup).
-define(MEMBERSHIP_SUP, classy_membership_sup).
%% Supervisor for children that start and stop dynamically, e.g. when
%% run level changes:
-define(DYNAMIC_SUP, classy_rl_dependent_sup).
%% Vote supervisors:
%%   Single:
-define(VOTE_COORDINATOR_SUP_1, classy_vote_coordinator_sup1).
-define(VOTE_PARTICIPANT_SUP_1, classy_vote_participant_sup1).
%%   Cluster:
-define(VOTE_COORDINATOR_SUP_2, classy_vote_coordinator_sup2).
-define(VOTE_PARTICIPANT_SUP_2, classy_vote_participant_sup2).
%%   Quorum:
-define(VOTE_COORDINATOR_SUP_3, classy_vote_coordinator_sup3).
-define(VOTE_PARTICIPANT_SUP_3, classy_vote_participant_sup3).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
  supervisor:start_link({local, ?SUP}, ?MODULE, #top{}).

-spec stop(timeout()) -> ok.
stop(Timeout) ->
  classy_lib:sync_stop_proc(?SUP, shutdown, Timeout).

-spec start_table(classy_table:tab(), classy_table:options()) -> {ok, pid()} | {error, _}.
start_table(Tab, Options) ->
  supervisor:start_child(?TABLE_SUP, [Tab, Options]).

-spec ensure_membership(classy:cluster_id(), classy:site()) -> {ok, pid()} | {error, _}.
ensure_membership(Cluster, Site) ->
  case supervisor:start_child(?MEMBERSHIP_SUP, [Cluster, Site]) of
    {ok, _} = Ok ->
      Ok;
    {error, {already_started, Pid}} ->
      {ok, Pid};
    Err ->
      Err
  end.

-spec ensure_vote_coordinator(classy_rl_changer:run_level_int(), list()) -> {ok, pid()} | {error, _}.
ensure_vote_coordinator(RunLevel, Args) ->
  simple_one_for_one_ensure_child(vote_coord_sup(RunLevel), Args).

-spec ensure_vote_participant(classy_rl_changer:run_level_int(), list()) -> {ok, pid()} | {error, _}.
ensure_vote_participant(RunLevel, Args) ->
  simple_one_for_one_ensure_child(vote_participant_sup(RunLevel), Args).

-spec ensure_liveness_server() -> ok.
ensure_liveness_server() ->
  {ok, _} = ensure_child(
              ?DYNAMIC_SUP,
              #{ id       => liveness
               , start    => {classy_liveness, start_link, []}
               , shutdown => 10_000
               , restart  => permanent
               , type     => worker
               }),
  ok.

-spec terminate_liveness_server() -> ok.
terminate_liveness_server() ->
  terminate_child(?DYNAMIC_SUP, liveness).

-spec ensure_vote_sup(classy_rl_changer:run_level_int()) -> ok.
ensure_vote_sup(RunLevel) ->
  ensure_child(
    ?DYNAMIC_SUP,
    #{ id => vote_coord_sup(RunLevel)
     , start => {?MODULE, start_link_vote_coordinator_sup, [RunLevel]}
     , shutdown => infinity
     , restart => permanent
     , type => supervisor
     }),
  ensure_child(
    ?DYNAMIC_SUP,
    #{ id => vote_participant_sup(RunLevel)
     , start => {?MODULE, start_link_vote_participant_sup, [RunLevel]}
     , shutdown => infinity
     , restart => permanent
     , type => supervisor
     }),
  ok.

-spec terminate_vote_sup(classy_rl_changer:run_level_int()) -> ok.
terminate_vote_sup(RunLevel) ->
  terminate_child(?DYNAMIC_SUP, vote_coord_sup(RunLevel)),
  terminate_child(?DYNAMIC_SUP, vote_participant_sup(RunLevel)).

-spec prep_stop() -> ok.
prep_stop() ->
  gen_server:stop(?SUP, shutdown, infinity).

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link_table_sup() -> supervisor:startlink_ret().
start_link_table_sup() ->
  supervisor:start_link({local, ?TABLE_SUP}, ?MODULE, #table_sup{}).

-spec start_link_membership_sup() -> supervisor:startlink_ret().
start_link_membership_sup() ->
  supervisor:start_link({local, ?MEMBERSHIP_SUP}, ?MODULE, #membership_sup{}).

-spec start_link_vote_sup() -> supervisor:startlink_ret().
start_link_vote_sup() ->
  supervisor:start_link({local, ?DYNAMIC_SUP}, ?MODULE, #dynamic_sup{}).

-spec start_link_vote_coordinator_sup(classy_rl_changer:run_level_int()) -> supervisor:startlink_ret().
start_link_vote_coordinator_sup(RunLevel) ->
  Name = vote_coord_sup(RunLevel),
  maybe
    {ok, Pid} ?= supervisor:start_link({local, Name}, ?MODULE, #vote_sup{type = coordinators}),
    ok ?= classy_vote_coordinator:restore(RunLevel),
    {ok, Pid}
  end.

-spec start_link_vote_participant_sup(classy_rl_changer:run_level_int()) -> supervisor:startlink_ret().
start_link_vote_participant_sup(RunLevel) ->
  Name = vote_participant_sup(RunLevel),
  maybe
    {ok, Pid} ?= supervisor:start_link({local, Name}, ?MODULE, #vote_sup{type = participants}),
    ok ?= classy_vote_participant:restore(RunLevel),
    {ok, Pid}
  end.

%%================================================================================
%% behavior callbacks
%%================================================================================

init(#top{}) ->
  _ = classy_hook:init(),
  RLChanger = #{ id       => run_level_mgr
               , start    => {classy_rl_changer, start_link, []}
               , shutdown => infinity
               , restart  => permanent
               , type     => worker
               },
  Node = #{ id       => node
          , start    => {classy_node, start_link, []}
          , shutdown => infinity
          , restart  => permanent
          , type     => worker
          },
  Autocluster = #{ id       => autocluster
                 , start    => {classy_autocluster_sup, start_link, []}
                 , shutdown => infinity
                 , restart  => permanent
                 , type     => supervisor
                 },
  Children = [ sup_spec(#{id => ?TABLE_SUP, start => {?MODULE, start_link_table_sup, []}})
             , sup_spec(#{id => ?MEMBERSHIP_SUP, start => {?MODULE, start_link_membership_sup, []}})
             , sup_spec(#{id => ?DYNAMIC_SUP, start => {?MODULE, start_link_vote_sup, []}})
             , RLChanger
             , Node
             , Autocluster
             ],
  SupFlags = #{ strategy      => rest_for_one
              , intensity     => 10
              , period        => 10
              , auto_shutdown => never
              },
  {ok, {SupFlags, Children}};
init(#table_sup{}) ->
  Children = #{ id       => worker
              , start    => {classy_table, start_link, []}
              , shutdown => infinity
              , type     => worker
              , restart  => transient
              },
  SupFlags = #{ strategy      => simple_one_for_one
              , intensity     => 10
              , period        => 10
              , auto_shutdown => never
              },
  {ok, {SupFlags, [Children]}};
init(#membership_sup{}) ->
  Children = #{ id       => worker
              , start    => {classy_membership, start_link, []}
              , shutdown => 5_000
              , type     => worker
              , restart  => permanent
              },
  SupFlags = #{ strategy      => simple_one_for_one
              , intensity     => 10
              , period        => 10
              , auto_shutdown => never
              },
  {ok, {SupFlags, [Children]}};
init(#dynamic_sup{}) ->
  SupFlags = #{ strategy  => one_for_one
              , intensity => 10
              , period    => 10
              },
  {ok, {SupFlags, []}};
init(#vote_sup{type = coordinators}) ->
  %% Note: since both coordinator and participant workers deal with
  %% persistent data, recovery via restart by the supervisor is too
  %% risky. It can lead to the situation where workers restart from a
  %% corrupted state and immediately restart.
  %%
  %% We let operator or business logic restart the workers if they
  %% deem that safe. Additionally, the workers are automatically
  %% restarted on node restart.
  Children = #{ id       => worker
              , start    => {classy_vote_coordinator, start_link, []}
              , shutdown => 5_000
              , type     => worker
              , restart  => temporary
              },
  SupFlags = #{ strategy  => simple_one_for_one
              , intensity => 1_000_000
              , period    => 1
              },
  {ok, {SupFlags, [Children]}};
init(#vote_sup{type = participants}) ->
  Children = #{ id       => worker
              , start    => {classy_vote_participant, start_link, []}
              , shutdown => 5_000
              , type     => worker
              , restart  => temporary
              },
  SupFlags = #{ strategy  => simple_one_for_one
              , intensity => 1_000_000
              , period    => 1
              },
  {ok, {SupFlags, [Children]}}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec sup_spec(map()) -> supervisor:child_spec().
sup_spec(M) ->
  maps:merge(
    #{ shutdown    => infinity
     , restart     => permanent
     , type        => supervisor
     , significant => false
     },
    M).

simple_one_for_one_ensure_child(Sup, Args) ->
  case supervisor:start_child(Sup, Args) of
    {ok, _} = Ok ->
      Ok;
    {error, {already_started, Pid}} ->
      {ok, Pid};
    Err ->
      Err
  end.

ensure_child(Sup, Spec = #{id := Id}) ->
  case supervisor:start_child(Sup, Spec) of
    {ok, _} = Ok ->
      Ok;
    {error, {already_started, Pid}} ->
      {ok, Pid};
    {error, already_present} ->
      ok = supervisor:delete_child(Sup, Id),
      ensure_child(Sup, Spec);
    Err ->
      Err
  end.

terminate_child(Sup, Id) ->
  case supervisor:terminate_child(Sup, Id) of
    ok ->
      supervisor:delete_child(Sup, Id);
    {error, not_found} ->
      ok;
    Other ->
      Other
  end.

-spec vote_coord_sup(classy_rl_changer:run_level_int()) -> atom().
vote_coord_sup(1) ->
  ?VOTE_COORDINATOR_SUP_1;
vote_coord_sup(2) ->
  ?VOTE_COORDINATOR_SUP_2;
vote_coord_sup(3) ->
  ?VOTE_COORDINATOR_SUP_3.

-spec vote_participant_sup(classy_rl_changer:run_level_int()) -> atom().
vote_participant_sup(1) ->
  ?VOTE_PARTICIPANT_SUP_1;
vote_participant_sup(2) ->
  ?VOTE_PARTICIPANT_SUP_2;
vote_participant_sup(3) ->
  ?VOTE_PARTICIPANT_SUP_3.
