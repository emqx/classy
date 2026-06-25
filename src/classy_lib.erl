%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(classy_lib).
-moduledoc """
Misc. utility functions.
""".

%% API:
-export([ fold_per_cluster/3
        , n_connected_peers/1
        , sites_to_nodes/1
        , safe_apply/1
        , safe_apply/3
        , safe_apply_with_timeout/2
        , multicast/1
        , multicall/1
        , multicall/2
        ]).

%% internal exports:
-export([ rpc_timeout/0
        , to_cluster_sets/0
        , quorum_sets/0
        , n_sites/0
        , time_s/0
        , adjust_time_s_skew/2

        , wakeup_after/3
        , cancel_wakeup/1

        , sync_stop_proc/3
        , ensure_list/1
        , is_normal_exit/1

        , map_deep_insert/3
        ]).

-export_type([ mfargs/0
             , unix_time_s/0
             , wakeup_timer/0
             , multicall_target/0
             , multicall_args/0
             , multicall_result/1
             , multicall_error/0
             , wrapped_exception/0
             ]).

%%================================================================================
%% Type declarations
%%================================================================================

-type mfargs() :: {module(), atom(), list()}.

-type callback() :: mfargs() | {function(), list()}.

-type multicall_target() :: classy:site() |
                            {classy:site(), _Token}.

-type multicall_args() :: #{multicall_target() => {module(), atom(), list()}}.

-type wrapped_exception() ::
        {error, {throw, _Reason}} |
        {error, {error, _Reason, _Stacktrace :: list()}} |
        {error, {exit, _Reason}}.

-type multicall_error() :: {error, site_is_down} | {error, {erpc, _}} | wrapped_exception().

-type multicall_result(Result) ::
        #{multicall_target() => {ok, Result} |
                                multicall_error()}.

-type unix_time_s() :: integer().

-type wakeup_timer() :: undefined | {integer(), reference()}.

%%================================================================================
%% API functions
%%================================================================================

-doc """
@xref{classy_lib:safe_apply/3}
""".
-spec safe_apply(callback()) -> {ok, term()} | wrapped_exception().
safe_apply({M, F, A}) ->
  safe_apply(M, F, A);
safe_apply({Fun, Args}) when is_function(Fun), is_list(Args) ->
  safe_apply({erlang, apply, [Fun, Args]}).

-doc """
Apply a function while catching all exceptions and returning them as a term.
""".
-spec safe_apply(module(), atom(), list()) -> {ok, term()} | wrapped_exception().
safe_apply(Module, Function, Args) ->
  try {ok, apply(Module, Function, Args)}
  catch
    throw:Reason ->
      {error, {throw, Reason}};
    error:Reason:Stack ->
      {error, {error, Reason, Stack}};
    exit:Reason ->
      {error, {exit, Reason}}
  end.

-doc """
Apply a function in a separate process with a timeout.
""".
-spec safe_apply_with_timeout(callback(), timeout()) -> {ok, term()} | wrapped_exception() | {error, timeout}.
safe_apply_with_timeout(Callback, Timeout) ->
  {Pid, MRef} = spawn_monitor(
                  fun() ->
                      exit(safe_apply(Callback))
                  end),
  receive
    {'DOWN', MRef, process, Pid, Reason} ->
      Reason
  after Timeout ->
      demonitor(MRef, [flush]),
      exit(Pid, kill),
      {error, timeout}
  end.

-doc """
Call functions on multiple sites similarly to @ref{classy_lib:multicall/2}
without waiting for the results.
""".
-spec multicast(multicall_args()) -> ok.
multicast(SitesWithArgs) ->
  maps:foreach(
    fun(SiteOrTuple, {Module, Function, Args}) when is_atom(Module),
                                                    is_atom(Function),
                                                    is_list(Args) ->

        case SiteOrTuple of
          Site when is_binary(Site) -> ok;
          {Site, _} -> ok
        end,
        case classy_node:node_of_site(Site, true) of
          {ok, Node} ->
            erpc:cast(Node, Module, Function, Args);
          undefined ->
            ok
        end
    end,
    SitesWithArgs).

-doc """
@xref{classy_lib:multicall/2}.
Uses the default timeout.
""".
-spec multicall(multicall_args()) -> multicall_result(term()).
multicall(SitesWithArgs) ->
  multicall(SitesWithArgs, rpc_timeout()).

-doc """
Call a function on multiple sites in parallel.

@b{Arguments:}
@enumerate
@item @code{SitesWithArgs}:
a map from RPC target to @code{@{Module, Function, Args@}} tuple.

Multicall target could be a site ID that is automatically translated to Erlang node,
or a tuple containing site ID and an arbitrary term.
The latter form allows to make multiple requests towards the same site.
Each multicall target can call a different function.

@item @code{Timeout}:
abandon waiting for replies after the specified timeout.
@end enumerate

@b{Return value:}

A map from RPC target to @code{@{ok, Return@}} tuple if the call was successful
or an error tuple @ref{t:classy_lib:multicall_error/0}.

@b{Example:}

@example
classy_lib:multicall(
  #@{S => @{erlang, node, []@} || S <- [S1, S2, SBad]@},
  5_000)
@end example

returns

@example
#@{ S1 => @{ok, 'node1@@localhost'@}
 , S2 => @{ok, 'node2@@localhost'@}
 , SBad => @{error, site_is_down@}
 @}
@end example
""".
-spec multicall(multicall_args(), timeout()) -> multicall_result(term()).
multicall(SitesWithArgs, Timeout) ->
  {ReqIdCollection, Sent, NotSent} =
    maps:fold(
      fun(SiteOrTuple, {Module, Function, Args}, {AccWait, AccSent, AccNotSent}) when
            is_atom(Module),
            is_atom(Function),
            is_list(Args) ->
          case SiteOrTuple of
            Site when is_binary(Site) -> ok;
            {Site, _}                 -> ok
          end,
          case classy_node:node_of_site(Site, true) of
            {ok, Node} ->
              ReqId = erpc:send_request(Node, Module, Function, Args),
              { erpc:reqids_add(ReqId, SiteOrTuple, AccWait)
              , [SiteOrTuple | AccSent]
              , AccNotSent
              };
            undefined ->
              { AccWait
              , AccSent
              , AccNotSent#{SiteOrTuple => {error, site_is_down}}
              }
          end
      end,
      {erpc:reqids_new(), [], #{}},
      SitesWithArgs),
  WaitTime = case Timeout of
               infinity ->
                 infinity;
               T when is_integer(T) ->
                 {abs, erlang:monotonic_time(millisecond) + Timeout}
             end,
  multicall_receive_replies(ReqIdCollection, WaitTime, NotSent, Sent).

-spec n_connected_peers(#{classy:site() => classy:peer_info()}) -> non_neg_integer().
n_connected_peers(Peers) ->
  maps:fold(
    fun(_, #{connected := Up}, Acc) ->
        case Up of
          true  -> Acc + 1;
          false -> Acc
        end
    end,
    0,
    Peers).

-doc """
Translate site IDs to node names of connected nodes.

Return list of unreachable nodes in the second element of the tuple.
""".
-spec sites_to_nodes([classy:site()]) -> {[node()], _BadSites :: [classy:site()]}.
sites_to_nodes(Sites) ->
  lists:foldl(
    fun(Site, {AccNodes, AccBad}) ->
        case classy_node:node_of_site(Site, true) of
          {ok, Node} ->
            {[Node | AccNodes], AccBad};
          undefined ->
            {AccNodes, [Site | AccBad]}
        end
    end,
    {[], []},
    Sites).

-doc """
Perform a fold over the result of @ref{classy:info/1}
with a separate accumulator per cluster.

@code{InitialAcc} parameter is used as the initial value of the accumulator for each cluster.

Sites that are not part of any cluster or don't have a site ID are ignored.
""".
-spec fold_per_cluster(Fun, Acc, classy:cluster_info()) -> #{classy:cluster_id() => Acc}
          when Fun :: fun((node(), classy:info(), Acc) -> Acc).
fold_per_cluster(Fun, InitialAcc, #{infos := Infos}) ->
  maps:fold(
    fun(Node, Info, Acc) ->
        case Info of
          #{cluster := Cluster, site := Site} when is_binary(Cluster), is_binary(Site) ->
            ClusterAcc0 = maps:get(Cluster, Acc, InitialAcc),
            ClusterAcc = Fun(Node, Info, ClusterAcc0),
            Acc#{Cluster => ClusterAcc};
          _ ->
            Acc
        end
    end,
    #{},
    Infos).

-doc "Return value @ref{rpc_timeout} environment variable (with default)".
rpc_timeout() ->
  application:get_env(classy, rpc_timeout, 5_000).

-doc "Return value of @ref{to_cluster_sets} (with default)".
-spec to_cluster_sets() -> [classy:node_set_name(), ...].
to_cluster_sets() ->
  [all | application:get_env(classy, to_cluster_sets, [])].

-doc "Return value of @ref{quorum_sets} (with default)".
-spec quorum_sets() -> [classy:node_set_name(), ...].
quorum_sets() ->
  [connected | application:get_env(classy, quorum_sets, [])].

-doc "Return value of @ref{n_sites} environment variable (with default)".
n_sites() ->
  application:get_env(classy, n_sites, 1).

-doc """
Adjust a local timestamp @code{Val} to the remote nodes's clock,
given the remote's ``current'' time @code{RemoteTimeS} at the time of the call.

Note: it only makes sense to use this function in a call with a finite timeout.
Then the error will be limited by the roundtrip timeout.
""".
adjust_time_s_skew(RemoteTimeS, Val) ->
  Skew = RemoteTimeS - time_s(),
  Val + Skew.

-ifndef(CONCUERROR).

-doc "Return Unix time in seconds.".
-spec time_s() -> unix_time_s().
time_s() ->
  os:system_time(second).

-endif.

-doc """
Set up a wakeup timer that sends message @code{Msg} to the calling process.

If the timer was previously set up to fire at a later time,
this function resets it to the earlier time.
""".
-spec wakeup_after(term(), integer(), wakeup_timer()) -> wakeup_timer().
wakeup_after(Msg, After, undefined) ->
  { erlang:monotonic_time(millisecond) + After
  , erlang:send_after(After, self(), Msg)
  };
wakeup_after(Msg, After, {OldDeadline, OldTRef} = Old) ->
  NewDeadline = erlang:monotonic_time(millisecond) + After,
  if OldDeadline > NewDeadline ->
      erlang:cancel_timer(OldTRef),
      { NewDeadline
      , erlang:send_after(After, self(), Msg)
      };
     true ->
      Old
  end.

-doc """
Cancel a timer created by @ref{classy_lib:wakeup_after/3}.
This function is idempotent.
""".
-spec cancel_wakeup(wakeup_timer()) -> undefined.
cancel_wakeup(undefined) ->
  undefined;
cancel_wakeup({_, TRef}) ->
  erlang:cancel_timer(TRef),
  undefined.

-doc "Send exit signal @code{Reason} to a process and wait for the shutdown".
-spec sync_stop_proc(pid() | atom(), _ExitReason, timeout()) -> ok | {error, timeout}.
sync_stop_proc(undefined, _, _) ->
  ok;
sync_stop_proc(Name, Reason, Timeout) when is_atom(Name) ->
  sync_stop_proc(whereis(Name), Reason, Timeout);
sync_stop_proc(Pid, Reason, Timeout) when is_pid(Pid) ->
  unlink(Pid),
  MRef = monitor(process, Pid),
  exit(Pid, Reason),
  receive
    {'DOWN', MRef, process, _, _} ->
      ok
  after Timeout ->
      {error, timeout}
  end.

-doc """
If input is a binary, convert it to a list.
Keep input list as is.
""".
-spec ensure_list(binary() | string()) -> string().
ensure_list(L) when is_list(L) ->
  L;
ensure_list(Bin) when is_binary(Bin) ->
  binary_to_list(Bin).

-spec is_normal_exit(_) -> boolean().
is_normal_exit(Reason) ->
  case Reason of
    normal   -> true;
    shutdown -> true;
    _        -> false
  end.

-spec map_deep_insert(list(), term(), map()) -> map().
map_deep_insert([], Val, _) ->
  Val;
map_deep_insert([K | Rest], Val, Outer) ->
  case Outer of
    #{K := Inner} ->
      Outer#{K := map_deep_insert(Rest, Val, Inner)};
    #{} ->
      Outer#{K => map_deep_insert(Rest, Val, #{})}
  end.

%%================================================================================
%% Internal functions
%%================================================================================

-spec multicall_receive_replies(
        erpc:request_id_collection(),
        erpc:timeout_time(),
        multicall_result(A),
        [multicall_target()]
       ) -> multicall_result(A).
multicall_receive_replies(Collection0, WaitTime, Acc, RemainingTargets) ->
  try erpc:receive_response(Collection0, WaitTime, true) of
    {Resp, Target, Collection} ->
      multicall_receive_replies(
        Collection,
        WaitTime,
        Acc#{Target => {ok, Resp}},
        RemainingTargets -- [Target]);
    no_request ->
      Acc
  catch
    error:{erpc, timeout} ->
      lists:foldl(
        fun(Target, Acc1) ->
            Acc1#{Target => {error, timeout}}
        end,
        Acc,
        RemainingTargets);
    throw:{Throw, Target, Collection} ->
      multicall_receive_replies(
        Collection,
        WaitTime,
        Acc#{Target => {error, {throw, Throw}}},
        RemainingTargets -- [Target]);
    exit:{{exception, Exit}, Target, Collection} ->
      multicall_receive_replies(
        Collection,
        WaitTime,
        Acc#{Target => {error, {exit, Exit}}},
        RemainingTargets -- [Target]);
    error:{Err, Target, Collection} ->
      case Err of
        {exception, Exc, Stack} ->
          Reason = {error, Exc, Stack};
        Reason ->
          ok
      end,
      multicall_receive_replies(
        Collection,
        WaitTime,
        Acc#{Target => {error, Reason}},
        RemainingTargets -- [Target])
  end.
