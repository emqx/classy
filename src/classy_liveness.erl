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
-export([start_link/0, site_disconn_since/2, on_run_level/2]).

-export_type([]).

-include("classy_internal.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-record(to_check, {}).

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
%% behavior callbacks
%%================================================================================

-record(s,
        { t :: classy_lib:wakeup_timer()
        }).

-doc false.
init(_) ->
  process_flag(trap_exit, true),
  S = #s{},
  {ok, wakeup(S)}.

-doc false.
handle_call(_Call, _From, S) ->
  {reply, {error, unknown_call}, S}.

-doc false.
handle_cast(_Cast, S) ->
  {noreply, S}.

-doc false.
handle_info(#to_check{}, S0) ->
  S = S0#s{t = undefined},
  check_down_sites(),
  {noreply, wakeup(S)};
handle_info(_Info, S) ->
  {noreply, S}.

-doc false.
terminate(_Reason, _S) ->
  ok.

%%================================================================================
%% Internal exports
%%================================================================================

-doc false.
-spec on_run_level(classy:run_level(), classy:run_level()) -> ok.
on_run_level(stopped, single) ->
  increase_n_restarts(),
  set_my_liveness_info(true);
on_run_level(single, stopped) ->
  set_my_liveness_info(false);
on_run_level(_, _) ->
  ok.

-doc false.
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% RPC target.
-doc false.
-spec site_disconn_since(classy_lib:unix_time_s(), classy:site()) -> classy_lib:unix_time_s() | alive.
site_disconn_since(RemoteT, Site) ->
  case classy_table:lookup(?site_info, Site) of
    [#site_info{isconn = true}] ->
      alive;
    [#site_info{isconn = false, conn_change_time = DownSince}] ->
      classy_lib:adjust_time_s_skew(RemoteT, DownSince);
    [] ->
      %% We have never seen the site alive:
      0
  end.

%%================================================================================
%% Internal functions
%%================================================================================

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

check_down_sites() ->
  maybe
    {ok, Cluster} ?= classy:the_cluster(),
    {ok, Local} ?= classy:the_site(),
    %% Calculate minimum wall time when site should be alive:
    MaxDownSecs = max_downtime(),
    true ?= is_integer(MaxDownSecs),
    MinLastUpTime = classy_lib:time_s() - MaxDownSecs,
    lists:foreach(
      fun(Site) ->
          maybe
            true ?= Site =/= Local,
            %% Before asking the remote sites, check the local data first:
            [ #site_info{ node = Node
                        , isconn = false
                        , conn_change_time = LastUpdate
                        }
            ] ?= classy_table:lookup(?site_info, Site),
            true ?= LastUpdate < MinLastUpTime,
            %% Now check the quorum:
            {ok, DownSince} ?= last_alive_at(Site),
            true ?= is_integer(DownSince),
            true ?= DownSince < MinLastUpTime,
            %% Run hooks:
            ok ?= classy_hook:all(?on_pre_autoclean, [Site]),
            %% Now we're pretty certain that the site is really down:
            ?tp(notice, automatically_kick_down_site,
                #{ site          => Site
                 , node          => Node
                 , last_alive_at => LastUpdate
                 }),
            classy_node:kick_site(Site, autoclean)
          end
      end,
      classy_membership:members(Cluster, Local)),
    classy_membership:cleanup(Cluster, Local, forget_after())
  end,
  ok.

-spec last_alive_at(classy:site()) -> {ok, classy_lib:unix_time_s() | alive} | {error, no_quorum}.
last_alive_at(Site) ->
  Ret = erpc:multicall(
          classy:nodes(connected),
          ?MODULE, site_disconn_since, [classy_lib:time_s(), Site],
          classy_lib:rpc_timeout()),
  Results = [I || {ok, I} <- Ret],
  case length(Results) >= classy:quorum(running) of
    true ->
      {ok, lists:max(Results)};
    false ->
      {error, no_quorum}
  end.

-spec wakeup(#s{}) -> #s{}.
wakeup(S = #s{t = T0}) ->
  T = classy_lib:wakeup_after(#to_check{}, check_interval(), T0),
  S#s{t = T}.

-spec max_downtime() -> pos_integer() | infinity.
max_downtime() ->
  application:get_env(classy, max_site_downtime, infinity).

-spec check_interval() -> pos_integer().
check_interval() ->
  application:get_env(classy, cleanup_check_interval, 30_000).

-spec forget_after() -> pos_integer().
forget_after() ->
  application:get_env(classy, forget_after, 7 * 24 * 60 * 60).
