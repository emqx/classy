%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_start_system_fixture).

-behavior(familiar_fixture).

%% behavior callbacks:
-export([init_per_node/4]).

-include_lib("familiar/include/familiar.hrl").

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec init_per_node(familiar:site(), node(), _, familiar_fixture:state()) -> {ok, familiar_fixture:state()}.
init_per_node(Site, _Node, _, State) ->
  ok = ?ON(Site, classy:start_system()),
  {ok, State}.
