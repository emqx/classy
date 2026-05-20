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
        , ensure_vote/1
        ]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([ start_link_table_sup/0
        , start_link_membership_sup/0
        , start_link_vote_sup/0
        ]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-record(top, {}).
-record(table_sup, {}).
-record(membership_sup, {}).
-record(vote_sup, {}).

-define(SUP, ?MODULE).
-define(TABLE_SUP, classy_table_sup).
-define(MEMBERSHIP_SUP, classy_membership_sup).
-define(VOTE_SUP, classy_vote_sup).

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

-spec ensure_vote(_) -> {ok, pid()} | {error, _}.
ensure_vote(Args) ->
  case supervisor:start_child(?VOTE_SUP, [Args]) of
    {ok, _} = Ok ->
      Ok;
    {error, {already_started, Pid}} ->
      {ok, Pid};
    Err ->
      Err
  end.

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
  case supervisor:start_link({local, ?VOTE_SUP}, ?MODULE, #vote_sup{}) of
    {ok, _} = Ok ->
      ok = classy_vote:create_table(),
      classy_vote:restore(),
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
             , sup_spec(#{id => ?VOTE_SUP, start => {?MODULE, start_link_vote_sup, []}})
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
init(#vote_sup{}) ->
  Children = #{ id       => worker
              , start    => {classy_vote, start_link, []}
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
