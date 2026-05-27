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

-define(on_node_init, on_node_init).
-define(on_create_cluster, on_create_cluster).
-define(on_create_site, on_create_site).
-define(on_site_status_change, on_site_status_change).
-define(on_membership_change, on_membership_change).
-define(on_pre_join, on_pre_join).
-define(on_post_join, on_post_join).
-define(on_pre_kick, on_pre_kick).
-define(on_post_kick, on_post_kick).
-define(on_change_run_level, on_change_run_level).
-define(on_pre_autoclean, on_pre_autoclean).
-define(on_pre_autocluster, on_pre_autocluster).
-define(on_enrich_site_info, on_enrich_site_info).

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

-define(classy_vote_pre_results, classy_vote_pre_results).
-define(classy_vote_coord_stage, classy_vote_coord_stage).
-define(classy_vote_coord_recv, classy_vote_coord_recv).

%% Site information:
-define(site_info, classy_site_status_tab).
-record(site_info, {isup, node, last_update}).
%%    Number of restarts since creation of the site
-define(n_restarts, n_restarts).

-endif.
