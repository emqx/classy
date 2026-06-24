%%--------------------------------------------------------------------
%% Copyright (c) 2024, 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-ifndef(CLASSY_INTERNAL_HRL).
-define(CLASSY_INTERNAL_HRL, true).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy.hrl").

-define(classy_proto_vsn, 1).

-define(max_hook_prio, 100000).
-define(min_hook_prio, -?max_hook_prio).

-define(on_node_init, on_node_init).
-define(on_create_cluster, on_create_cluster).
-define(on_create_site, on_create_site).
-define(on_peer_connection_status_change, on_peer_connection_status_change).
-define(on_membership_change, on_membership_change).
-define(on_pre_join, on_pre_join).
-define(on_post_join, on_post_join).
-define(on_pre_kick, on_pre_kick).
-define(on_post_kick, on_post_kick).
-define(on_change_run_level, on_change_run_level).
-define(on_pre_autoclean, on_pre_autoclean).
-define(on_pre_autocluster, on_pre_autocluster).
-define(on_enrich_site_info, on_enrich_site_info).
-define(on_peer_liveness_change, on_peer_liveness_change).
-define(on_peer_restart, on_peer_restart).
-define(on_peer_node_change, on_peer_node_change).

%% Run levels:
-define(stopped, stopped).
-define(single, single).
-define(cluster, cluster).
-define(quorum, quorum).

%% Trace events
-define(classy_unknown_event, classy_unknown_event).
-define(classy_abnormal_exit, classy_abnormal_exit).
-define(classy_table_anomaly, classy_table_anomaly).
-define(classy_bad_data, classy_bad_data).
-define(classy_run_level_change_error, classy_run_level_change_error).
-define(classy_hook_failure, classy_hook_failure).

-define(classy_vote_pre_results, classy_vote_pre_results).
-define(classy_vote_coord_stage, classy_vote_coord_stage).
-define(classy_vote_coord_recv, classy_vote_coord_recv).
-define(classy_vote_coord_commit, classy_vote_coord_commit).
-define(classy_vote_coord_post_actions, classy_vote_coord_post_actions).
-define(classy_vote_part_stage, classy_vote_part_stage).
-define(classy_vote_part_perform_action, classy_vote_part_perform_action).
-define(classy_vote_flow_start, classy_vote_flow_start).
-define(classy_vote_part_established, classy_vote_part_established).
-define(classy_vote_part_recv_outcome, classy_vote_part_recv_outcome).
-define(classy_vote_alloc_id, classy_vote_alloc_id).
-define(classy_vote_part_recv, classy_vote_part_recv).
-define(classy_vote_part_send_vote, classy_vote_part_send_vote).
-define(classy_vote_part_flow_complete, classy_vote_part_flow_complete).
-define(classy_vote_coord_early_abort, classy_vote_coord_early_abort).
-define(classy_vote_coord_flow_complete, classy_vote_coord_flow_complete).

%% Site information:
-define(site_info, classy_site_status_tab).
-record(site_info,
        { isconn
        , isup
        , nrestarts
        , node
        , meta
          %% Time when the the value of isconn last changed:
        , conn_change_time
        , reserved = []
        }).
%%    Number of restarts since creation of the site
-define(n_restarts, n_restarts).
-record(liveness,
        { nr
        , isup
        , reserved = []
        }).

-endif.
