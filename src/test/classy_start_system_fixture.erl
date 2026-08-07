%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_start_system_fixture).

-behavior(familiar_fixture).

%% behavior callbacks:
-export([init_per_node/4, cleanup_per_node/5]).

-include_lib("familiar/include/familiar.hrl").

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec init_per_node(familiar:site(), node(), _, familiar_fixture:state()) -> {ok, familiar_fixture:state()}.
init_per_node(Site, _Node, _, State) ->
  ok = ?ON(Site, classy:start_system()),
  {ok, State}.

-spec cleanup_per_node(familiar:site(), node(), _, familiar_fixture:state(), IsKill) -> ok | {error, _}
  when IsKill :: boolean().
cleanup_per_node(Site, _, _, _, false) ->
  ?ON(Site, classy:stop_system());
cleanup_per_node(_, _, _, _, true) ->
  ok.
