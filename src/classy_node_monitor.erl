%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_node_monitor).
-moduledoc """
This module implements a drop-in replacement for @code{net_kernel:monitor_nodes/1} feature,
that is aware of classy membership.

@code{nodeup} and @code{nodedown} are sent in the following cases:

@enumerate
@item
When node @code{N} that hosts a cluster member goes up,
other members of the cluster receive @code{@{nodeup, N@}} event.
@item
When a node @code{N} that hosts a cluster member goes down,
other members of the cluster receive @code{@{nodedown, N@}} event.
@item
When a node joins a cluster,
other members of the cluster receive @code{@{nodeup, N@}} event for the new node.
@item
When a site leaves a cluster,
the remaining members, that were previously connected to it, recieve @code{@{nodedown, N@}} event.
@end enumerate
""".

-behavior(gen_server).

%% API:
-export([monitor_nodes/1]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/0, on_peer_connection_change/3]).

-export_type([]).

-include("classy_internal.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(SERVER, ?MODULE).

-record(call_monitor,
        { pid :: pid()
        , enable :: boolean()
        }).

-record(cast_conn_change,
        { site :: classy:site()
        , node :: node()
        , conn :: boolean()
        }).

%%================================================================================
%% API functions
%%================================================================================

-doc """
If argument is @code{true} subscribe the caller to node connection change notifications.
If it is @code{false} then remove the subscription.
""".
-spec monitor_nodes(boolean()) -> ok.
monitor_nodes(Enable) when is_boolean(Enable) ->
  gen_server:call(?SERVER, #call_monitor{pid = self(), enable = Enable}).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { hook :: classy_hook:hook()
        , subs = #{} :: #{pid() => reference()}
        }).

init(_) ->
  process_flag(trap_exit, true),
  Hook = classy:on_peer_connection_change(fun ?MODULE:on_peer_connection_change/3, 0),
  S = #s{hook = Hook},
  {ok, S}.

handle_call(#call_monitor{pid = Pid, enable = Enable}, _From, S = #s{subs = Subs0}) ->
  case Subs0 of
    #{Pid := MRef} ->
      case Enable of
        true ->
          {reply, ok, S};
        false ->
          demonitor(MRef),
          Subs = maps:remove(Pid, Subs0),
          {reply, ok, S#s{subs = Subs}}
      end;
    #{} ->
      case Enable of
        true ->
          MRef = monitor(process, Pid),
          Subs = Subs0#{Pid => MRef},
          {reply, ok, S#s{subs = Subs}};
        false ->
          {reply, ok, S}
      end
  end;
handle_call(Call, From, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ call   => Call
       , from   => From
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

handle_cast(#cast_conn_change{node = Node, conn = Conn}, S = #s{subs = Subs}) ->
  Msg = case Conn of
          false -> {nodedown, Node};
          true  -> {nodeup, Node}
        end,
  case Node of
    undefined ->
      ok;
    _ ->
      maps:foreach(
        fun(Pid, _) ->
            Pid ! Msg
        end,
        Subs)
  end,
  {noreply, S};
handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ cast   => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

handle_info({'DOWN', _MRef, process, Pid, _}, S = #s{subs = Subs}) ->
  {noreply, S#s{subs = maps:remove(Pid, Subs)}};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ info   => Info
       , server => ?MODULE
       }),
  {noreply, S}.

terminate(_Reason, #s{hook = Hook}) ->
  classy_hook:unhook(Hook),
  ok.

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec on_peer_connection_change(classy:site(), node(), boolean()) -> ok.
on_peer_connection_change(Site, Node, IsConn) ->
  gen_server:cast(
    ?SERVER,
    #cast_conn_change{ site = Site
                     , node = Node
                     , conn = IsConn
                     }).

%%================================================================================
%% Internal functions
%%================================================================================
