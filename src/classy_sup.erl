%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @private
-module(classy_sup).

-behavior(supervisor).

%% API:
-export([ start_link/0
        , stop/1
        , start_table/2
        , ensure_membership/2
        , ensure_vote_coordinator/1
        , ensure_vote_participant/1
        ]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([ start_link_table_sup/0
        , start_link_membership_sup/0
        , start_link_vote_coordinator_sup/0
        , start_link_vote_participant_sup/0
        ]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-record(top, {}).
-record(table_sup, {}).
-record(membership_sup, {}).

-define(SUP, ?MODULE).
-define(TABLE_SUP, classy_table_sup).
-define(MEMBERSHIP_SUP, classy_membership_sup).
-define(VOTE_COORDINATOR_SUP, classy_vote_coordinator_sup).
-define(VOTE_PARTICIPANT_SUP, classy_vote_participant_sup).

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

-spec ensure_vote_coordinator(_) -> {ok, pid()} | {error, _}.
ensure_vote_coordinator(Args) ->
  simple_one_for_one_ensure_child(?VOTE_COORDINATOR_SUP, Args).

-spec ensure_vote_participant(_) -> {ok, pid()} | {error, _}.
ensure_vote_participant(Args) ->
  simple_one_for_one_ensure_child(?VOTE_PARTICIPANT_SUP, Args).

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link_table_sup() -> supervisor:startlink_ret().
start_link_table_sup() ->
  supervisor:start_link({local, ?TABLE_SUP}, ?MODULE, #table_sup{}).

-spec start_link_membership_sup() -> supervisor:startlink_ret().
start_link_membership_sup() ->
  supervisor:start_link({local, ?MEMBERSHIP_SUP}, ?MODULE, #membership_sup{}).

-spec start_link_vote_coordinator_sup() -> supervisor:startlink_ret().
start_link_vote_coordinator_sup() ->
  case supervisor:start_link({local, ?VOTE_COORDINATOR_SUP}, ?MODULE, ?VOTE_COORDINATOR_SUP) of
    {ok, _} = Ok ->
      classy_vote_coordinator:restore(),
      Ok;
    Other ->
      Other
  end.

-spec start_link_vote_participant_sup() -> supervisor:startlink_ret().
start_link_vote_participant_sup() ->
  case supervisor:start_link({local, ?VOTE_PARTICIPANT_SUP}, ?MODULE, ?VOTE_PARTICIPANT_SUP) of
    {ok, _} = Ok ->
      classy_vote_participant:restore(),
      Ok;
    Other ->
      Other
  end.

%%================================================================================
%% behavior callbacks
%%================================================================================

init(#top{}) ->
  _ = classy_hook:init(),
  Node = #{ id       => node
          , start    => {classy_node, start_link, []}
          , shutdown => 10_000
          , restart  => permanent
          , type     => worker
          },
  UIDGen = #{ id       => uid
            , start    => {classy_uid, start_link, []}
            , shutdown => 5_000
            , restart  => permanent
            , type     => worker
            },
  Autoclean = #{ id       => autoclean
               , start    => {classy_autoclean, start_link, []}
               , shutdown => 10_000
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
             , Node
             , UIDGen
             , sup_spec(#{id => ?VOTE_COORDINATOR_SUP, start => {?MODULE, start_link_vote_coordinator_sup, []}})
             , sup_spec(#{id => ?VOTE_PARTICIPANT_SUP, start => {?MODULE, start_link_vote_participant_sup, []}})
             , Autoclean
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
init(?VOTE_COORDINATOR_SUP) ->
  Children = #{ id       => worker
              , start    => {classy_vote_coordinator, start_link, []}
              , shutdown => 5_000
              , type     => worker
              , restart  => transient
              },
  SupFlags = #{ strategy  => simple_one_for_one
              , intensity => 1_000_000
              , period    => 1
              },
  {ok, {SupFlags, [Children]}};
init(?VOTE_PARTICIPANT_SUP) ->
  Children = #{ id       => worker
              , start    => {classy_vote_participant, start_link, []}
              , shutdown => 5_000
              , type     => worker
              , restart  => transient
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
