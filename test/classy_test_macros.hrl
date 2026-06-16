%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(CLASSY_TEST_MACROS_HRL).
-define(CLASSY_TEST_MACROS_HRL, true).

-include_lib("proper/include/proper.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("familiar/include/familiar.hrl").

-define(assertSameSet(EXP, GOT), ?assertEqual(lists:sort(EXP), lists:sort(GOT))).
-define(assertSameSet(EXP, GOT, COMMENT), ?assertEqual(lists:sort(EXP), lists:sort(GOT), COMMENT)).

-endif.
