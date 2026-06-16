%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(CLASSY_VOTE_HRL).
-define(CLASSY_VOTE_HRL, true).

-include("classy_internal.hrl").

%% Protocol:
%%   Coordinator -> Participant
-record(prepare,
        { id            :: classy_vote:id()
        , tag           :: classy_vote:tag()
        , prepare       :: classy_lib:mfargs()
        , commit        :: [classy_lib:mfargs()]
        , rollback      :: [classy_lib:mfargs()]
        , coordinator   :: classy:site()
        , on_fail       :: [classy_lib:mfargs()]
        , reserved = [] :: term()
        }).
%%   Coordinator <- Participant
-record(c_vote,
        { id            :: classy_vote:id()
        , vote          :: boolean()
        , from          :: classy:site()
        , reserved = [] :: term()
        }).
%%   Coordinator -> Participant
-record(c_outcome,
        { id            :: classy_vote:id()
        , tag           :: classy_vote:tag()
        , result        :: boolean() % true = commit, false = rollback
        , reserved = [] :: term()
        }).

-define(ptab, classy_vote_table).

-define(coordinator(ID), {n, l, {classy_vote_coordinator, ID}}).
-define(participant(ID), {n, l, {classy_vote_participant, ID}}).
-define(via(NAME), {via, gproc, NAME}).

-define(state_timeout, state_timeout).

-ifndef(TEST).
-define(fold_batch_size, 100).
-else.
-define(fold_batch_size, 1).
-endif.

-endif.
