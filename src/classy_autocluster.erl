%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(classy_autocluster).
-moduledoc """
A server responsible for automatic peer discovery.
""".

-behavior(gen_server).

%% API:
-export([ start_link/0
        , enable/0
        , disable/0
        , app_name/0
        , candidates/0
        ]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-record(cast_enable, {enable :: boolean()}).
-record(to_discover, {}).

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-doc false.
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec enable() -> ok.
enable() ->
  gen_server:cast(?SERVER, #cast_enable{enable = true}).

-spec disable() -> ok.
disable() ->
  gen_server:cast(?SERVER, #cast_enable{enable = false}).

-doc """
Helper function that returns prefix of the local node name.
E.g. @code{'foo@@127.0.0.1'} -> @code{'foo'}.
""".
-spec app_name() -> string().
app_name() ->
  [Name | _] = string:tokens(atom_to_list(node()), "@"),
  Name.

-doc """
List candidates according to the selected strategy.
""".
-spec candidates() -> {ok, [{classy:cluster_id(), node()}]} | ignore.
candidates() ->
  with_strategy(
    fun(Mod, Options) ->
        fun() ->
            discover(Mod, Options)
        end
    end).

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
  {ok, wakeup_if_single(0, S)}.

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
handle_cast(#cast_enable{enable = Enable}, S0 = #s{t = T}) ->
  S = case Enable of
        true  -> wakeup(0, S0);
        false -> S0#s{t = classy_lib:cancel_wakeup(T)}
      end,
  {noreply, S};
handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

-doc false.
handle_info(#to_discover{}, S) ->
  {noreply, handle_discover(S)};
handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

-doc false.
terminate(Reason, _S) ->
  classy_lib:is_normal_exit(Reason) orelse
    ?tp(warning, ?classy_abnormal_exit,
        #{ server => ?MODULE
         , reason => Reason
         }),
  ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

handle_discover(S0) ->
  S = S0#s{t = undefined},
  discover_and_join(),
  wakeup_if_single(S).

-spec discover_and_join() -> ok | ignore | {error, _}.
discover_and_join() ->
  with_strategy(
    fun(Mod, Options) ->
        with_lock(
          Mod, Options,
          fun() ->
              maybe
                {ok, Candidates} ?= discover(Mod, Options),
                try_join(Candidates)
              else
                ignore ->
                  ignore;
                Other ->
                  logger:error("Discover and join error: ~p", [Other]),
                  ignore
              end
          end)
    end).

-spec with_lock(module(), list(), fun(() -> Ret)) -> Ret | ignore | {error, _}.
with_lock(Mod, Options, Fun) ->
  case classy_discovery_strategy:lock(Mod, Options) of
    ok ->
      try Fun()
      after
        log_error("Unlock", classy_discovery_strategy:unlock(Mod, Options))
      end;
    Other ->
      log_error("Lock", Other),
      Other
  end.

-spec discover(module(), list()) -> {ok, [{classy:cluster_id(), node()}]} | ignore.
discover(Mod, Options) ->
  Res = ?tp_span(debug, classy_autocluster_discover,
                 #{ mod => Mod
                  , options => Options
                  },
                 classy_discovery_strategy:discover(Mod, Options)),
  case Res of
    {ok, Candidates} when is_list(Candidates) ->
      ClusterInfo = #{bad_nodes := BadNodesWithReason} = classy:info(Candidates),
      BadNodes = maps:keys(BadNodesWithReason),
      BadNodes =/= [] andalso
        logger:info("classy_autocluster: discovered nodes are not responding: ~p", [BadNodes]),
      case rank_nodes(Candidates, ClusterInfo) of
        [] ->
          ignore;
        Ranked ->
          {ok, Ranked}
      end;
    Other ->
      log_error("Discover", Other),
      ignore
  end.

-spec rank_nodes([node()], classy:cluster_info()) -> [{classy:cluster_id(), node()}].
rank_nodes(Candidates, ClusterInfo = #{infos := SiteInfos}) ->
  L0 = lists:foldl(
         fun(Node, Acc) ->
             case SiteInfos of
               #{Node := #{site := S, cluster := C, peers := Peers}} when is_binary(S),
                                                                          is_binary(C),
                                                                          Node =/= node() ->
                 [{-classy_lib:n_connected_peers(Peers), S, C, Node} | Acc];
               _ ->
                 Acc
             end
         end,
         [],
         Candidates),
  L1 = lists:sort(L0),
  InitialRanking = [{Cluster, Node} || {_NPeers, _Site, Cluster, Node} <- L1],
  classy_hook:fold(
    ?on_pre_autocluster,
    [ClusterInfo],
    InitialRanking).

try_join([]) ->
  ignore;
try_join([{Cluster, Node} | Rest]) ->
  case classy_node:join_node(Node, autocluster, Cluster) of
    ok ->
      ok;
    _ ->
      try_join(Rest)
  end.

-spec wakeup_if_single(#s{}) -> #s{}.
wakeup_if_single(S) ->
  wakeup_if_single(discovery_interval(), S).

-spec wakeup_if_single(non_neg_integer(), #s{}) -> #s{}.
wakeup_if_single(Interval, S) ->
  case is_singleton() of
    false ->
      S#s{t = undefined};
    true ->
      wakeup(Interval, S)
  end.

-spec wakeup(non_neg_integer(), #s{}) -> #s{}.
wakeup(After, S = #s{t = T0}) ->
  T = classy_lib:wakeup_after(#to_discover{}, After, T0),
  S#s{t = T}.

with_strategy(Fun) ->
  case classy_discovery_strategy:get() of
    {Module, Options} ->
      Fun(Module, Options);
    undefined ->
      ignore
  end.

-spec discovery_interval() -> pos_integer().
discovery_interval() ->
  application:get_env(classy, discovery_interval, 5_000).

log_error(Format, {error, Reason}) ->
  logger:error(Format ++ " error: ~p", [Reason]);
log_error(_Format, _Ok) ->
  ok.

is_singleton() ->
  case classy:sites() of
    [_, _ | _] ->
      false;
    _ ->
      true
  end.
