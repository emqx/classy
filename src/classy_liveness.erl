%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_liveness).
-moduledoc """
A process that is tasked with monitoring liveness status of peers.
Its responsibilities include:

@enumerate
@item detecting that remote sites are down,
making a coordinated decision that the site is down.

@item automatically kick sites that have been down from the cluster.
@end enumerate

Relevant configurations are @ref{max_site_downtime} and @ref{quorum}.

In a partitioned network there is a risk that sites try to kick each other.

Liveness requires quorum of running nodes before making the decision to kick.
Note: as @code{quorum(running)} is always >= @code{quorum(config)},
even in a partition containing single node,
liveness won't activate if @link{quorum} config is set to a value > 1.

""".

-behavior(gen_server).

%% API:
-export([n_restarts/0]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([ start_link/0

        , on_run_level/2
        , on_peer_connection_change/3

        , vote_down_prep/5
        , vote_down_commit/4
        , vote_kick_prep/4
        , vote_kick_commit/3
        ]).

-export_type([]).

-include("classy_internal.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-record(to_autoclean, {}).

-record(cast_check_site,
        { site :: classy:site()
        , node :: node()
        }).
-record(cast_quorum, {}).

-define(vote_tag(SITE), {classy_liveness, SITE}).

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-doc """
Return number of node restarts since creation of the site.

This value is monotonically increasing.
""".
-spec n_restarts() -> {ok, non_neg_integer()} | {error, nodedown}.
n_restarts() ->
  case classy_table:lookup(?globals, ?n_restarts) of
    [N] ->
      {ok, N};
    _ ->
      {error, nodedown}
  end.

%%================================================================================
%% Internal exports
%%================================================================================

-doc false.
-spec vote_down_prep(boolean(), classy_vote:id(), classy:cluster_id(), classy:site(), non_neg_integer()) -> boolean().
vote_down_prep(_ForReal, _Id, Cluster, Target, NRestarts) ->
  maybe
    {ok, Cluster} ?= classy:the_cluster(),
    {ok, NRestarts} ?= classy_node:n_restarts(Target),
    disconnected(Target)
  else
    _ -> false
  end.

-doc false.
-spec vote_down_commit(classy_vote:id(), classy:cluster_id(), classy:site(), non_neg_integer()) -> ok.
vote_down_commit(_Id, Cluster, Target, NRestarts) ->
  classy_membership:set_liveness(Cluster, classy_node:maybe_site(), Target, NRestarts, false, false).

-doc false.
-spec vote_kick_prep(boolean(), classy_vote:id(), classy:cluster_id(), classy:site()) -> boolean().
vote_kick_prep(_ForReal, _Id, Cluster, Target) ->
  can_be_kicked(Cluster, Target).

-doc false.
-spec vote_kick_commit(classy_vote:id(), classy:cluster_id(), classy:site()) -> ok.
vote_kick_commit(_Id, _Cluster, Target) ->
  classy:kick_site(Target, autoclean).

-doc false.
-spec on_peer_connection_change(classy:site(), node(), boolean()) -> ok.
on_peer_connection_change(_Site, _Node, true) ->
  ok;
on_peer_connection_change(Site, Node, false) ->
  gen_server:cast(?SERVER, #cast_check_site{site = Site, node = Node}).

-doc false.
-spec on_run_level(classy:run_level(), classy:run_level()) -> ok.
on_run_level(stopped, single) ->
  increase_n_restarts(),
  set_my_liveness_info(true);
on_run_level(single, stopped) ->
  set_my_liveness_info(false);
on_run_level(cluster, quorum) ->
  gen_server:cast(?SERVER, #cast_quorum{}),
  ok;
on_run_level(_, _) ->
  ok.

-doc false.
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { periodic_timer :: classy_lib:wakeup_timer()
        }).

-doc false.
init(_) ->
  process_flag(trap_exit, true),
  S = #s{},
  check_down(S),
  {ok, wakeup(S)}.

-doc false.
handle_call(Call, From, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => call
       , from => From
       , content => Call
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

-doc false.
handle_cast(#cast_quorum{}, S) ->
  check_down(S),
  {noreply, S};
handle_cast(#cast_check_site{site = Site, node = _Node}, S) ->
  check_down(Site, S),
  {noreply, S};
handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

-doc false.
handle_info(#to_autoclean{}, S0) ->
  S = S0#s{periodic_timer = undefined},
  check_down(S),
  kick_down_sites(S),
  {noreply, wakeup(S)};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

-doc false.
terminate(_Reason, _S) ->
  ok.

%%================================================================================
%% Internal functions
%%================================================================================

check_down(S) ->
  [check_down(I, S) || I <- classy:sites(all)].

check_down(Target, _S) ->
  maybe
    true ?= disconnected(Target),
    {ok, NRestarts} ?= classy_node:n_restarts(Target),
    {ok, Cluster} ?= classy:the_cluster(),
    {ok, Consilium} ?= consilium(),
    Actions = #{ prepare => {?MODULE, vote_down_prep, [Cluster, Target, NRestarts]}
               , commit  => [{?MODULE, vote_down_commit, [Cluster, Target, NRestarts]}]
               },
    _ = classy_vote:create(#{ tag => ?vote_tag(Target)
                            , actions => #{I => Actions || I <- Consilium}
                            }),
    ok
  else
    _ -> ok
  end.

set_my_liveness_info(Running) ->
  Cluster = classy_node:maybe_cluster(),
  Site = classy_node:maybe_site(),
  {ok, NR} = n_restarts(),
  classy_membership:set_liveness(Cluster, Site, Site, NR, true, Running).

-spec increase_n_restarts() -> non_neg_integer().
increase_n_restarts() ->
  %% TODO: run this in a critical section:
  do_increase_n_restarts().

do_increase_n_restarts() ->
  N = case classy_table:lookup(?globals, ?n_restarts) of
        [N0] when is_integer(N0) ->
          N0 + 1;
        [] ->
          1;
        Other ->
          ?tp(warning, ?classy_bad_data,
              #{ table => ?globals
               , key   => ?n_restarts
               , val   => Other
               }),
          1
      end,
  classy_table:write(?globals, ?n_restarts, N),
  N.

kick_down_sites(_S) ->
  maybe
    {ok, Cluster} ?= classy:the_cluster(),
    {ok, Local} ?= classy:the_site(),
    %% Calculate minimum wall time when site should be alive:
    MaxDownSecs = max_downtime(),
    true ?= is_integer(MaxDownSecs),
    lists:foreach(
      fun(Target) ->
          maybe
            true ?= can_be_kicked(Cluster, Target),
            ok ?= classy_hook:all(?on_pre_autoclean, [Target]),
            {ok, Consilium} ?= consilium(),
            Actions = #{ prepare => {?MODULE, vote_kick_prep, [Cluster, Target]}
                       , commit  => [{?MODULE, vote_kick_commit, [Cluster, Target]}]
                       },
            classy_vote:create(#{ tag => ?vote_tag(Target)
                                , actions => #{I => Actions || I <- Consilium}
                                })
          end
      end,
      classy_membership:members(Cluster, Local)),
    classy_membership:cleanup(Cluster, Local, forget_after())
  end,
  ok.

-spec wakeup(#s{}) -> #s{}.
wakeup(S = #s{periodic_timer = T0}) ->
  T = classy_lib:wakeup_after(#to_autoclean{}, check_interval(), T0),
  S#s{periodic_timer = T}.

-spec max_downtime() -> pos_integer() | infinity.
max_downtime() ->
  application:get_env(classy, max_site_downtime, infinity).

-spec check_interval() -> pos_integer().
check_interval() ->
  application:get_env(classy, cleanup_check_interval, 30_000).

-spec forget_after() -> pos_integer().
forget_after() ->
  application:get_env(classy, forget_after, 7 * 24 * 60 * 60).

-spec disconnected(classy:site()) -> boolean().
disconnected(Site) ->
  ordsets:is_element(Site, classy:sites(disconnected)) andalso
    not ordsets:is_element(Site , classy:sites(down)).

-spec site_disconn_since(classy:site()) -> {ok, classy_lib:unix_time_s()} | ignore.
site_disconn_since(Site) ->
  case classy_table:lookup(?site_info, Site) of
    [#site_info{isconn = false, isup = false, conn_change_time = DownSince}] ->
      {ok, DownSince};
    [] ->
      %% We have never seen the site alive:
      {ok, 0};
    [_] ->
      ignore
  end.

can_be_kicked(Cluster, Target) ->
  maybe
    %% Self-checks:
    {ok, Cluster} ?= classy:the_cluster(),
    {ok, Local} ?= classy:the_site(),
    true ?= Local =/= Target,
    MaxDownSecs = max_downtime(),
    true ?= is_integer(MaxDownSecs),
    %% Check target:
    true ?= ordsets:is_element(Target, classy:sites(down)),
    {ok, DisconnectedSince} ?= site_disconn_since(Target),
    classy_lib:time_s() - DisconnectedSince > MaxDownSecs
  else
    _ -> false
  end.

consilium() ->
  Sites =
    ordsets:intersection(
      [classy:sites(Set) || Set <- classy_lib:quorum_sets()]),
  case length(Sites) >= classy:quorum(config) of
    true ->
      {ok, Sites};
    false ->
      undefined
  end.
