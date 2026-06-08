%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(CLASSY_VOTE_HRL).
-define(CLASSY_VOTE_HRL, true).

-include("classy_internal.hrl").

%% Protocol:
%%   Coordinator -> Participant
-record(prepare,
        { id            :: id()
        , tag           :: tag()
        , lock          :: lock()
        , prepare       :: mfargs()
        , commit        :: [mfargs()]
        , rollback      :: mfargs()
        , coordinator   :: classy:site()
        , reserved = [] :: term()
        }).
%%   Coordinator <- Participant
-record(c_vote,
        { id            :: id()
        , vote          :: boolean()
        , from          :: classy:site()
        , reserved = [] :: term()
        }).
%%   Coordiantor -> Participant
-record(c_outcome,
        { id       :: id()
        , tag      :: tag()
        , result   :: boolean() % true = commit, false = rollback
        , reserved :: []
        }).

-endif.
