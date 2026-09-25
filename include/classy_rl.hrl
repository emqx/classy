%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(CLASSY_RT_HRL).
-define(CLASSY_RT_HRL, true).

-define(classy_rl_stride, 100).

-define(classy_rl_stopped, 0).
-define(classy_rl_single, 10).
-define(classy_rl_cluster, ?classy_rl_stride).
-define(classy_rl_quorum, (2 * ?classy_rl_stride)).
-define(classy_rl_ready, (3 * ?classy_rl_stride)).

-endif.
