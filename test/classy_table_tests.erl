%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_table_tests).

-export([setup/1, cleanup/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("classy_internal.hrl").

%%================================================================================
%% Tests
%%================================================================================

%% This test verifies idempotency of `open' and `stop' functions.
smoke_open_test() ->
  Clean = setup(?FUNCTION_NAME),
  T = ?FUNCTION_NAME,
  ?check_trace(
     try
       ?assertEqual(ok, classy_table:open(T, #{})),
       ?assertEqual(ok, classy_table:open(T, #{})),
       ?assertEqual(ok, classy_table:stop(T, 1000)),
       ?assertEqual(ok, classy_table:stop(T, 1000)),
       ?assertEqual(ok, classy_table:open(T, #{})),
       ?assertEqual(ok, classy_table:open(T, #{}))
     after
       classy_table:drop(T),
       cleanup(Clean)
     end,
     fun classy_SUITE:no_unexpected_events/1).

%% This test verifies effects of write and delete operations.
smoke_write_delete_test() ->
  Clean = setup(?FUNCTION_NAME),
  T = ?FUNCTION_NAME,
  ?check_trace(
     try
       ?assertEqual(ok, classy_table:open(T, opts())),
       ?assertEqual(ok, classy_table:write(T, foo, foo)),
       ?assertEqual(ok, classy_table:write(T, foo, bar)),
       ?assertEqual([bar], classy_table:lookup(T, foo)),
       ?assertEqual(ok, classy_table:delete(T, foo)),
       ?assertEqual([], classy_table:lookup(T, foo)),
       ?assertEqual(ok, classy_table:stop(T, infinity)),
       ?assertEqual(
          {ok, [ {v, 1}
               , {w, foo, foo}
               , {w, foo, bar}
               , {d, foo}
               ]},
          classy_table:dump_wal(T))
     after
       cleanup(Clean)
     end,
     [ fun classy_SUITE:no_unexpected_events/1
     , {"events",
        fun(Trace) ->
            ?assertMatch(
               [ #{tab := T, op := open}
               , #{tab := T, op := {w, foo, foo}}
               , #{tab := T, op := {w, foo, bar}}
               , #{tab := T, op := {d, foo}}
               , #{tab := T, op := close}
               ],
               ?of_kind([classy_table_update], Trace))
        end}
     ]).

%% This test verifies that dirty writes don't overwrite normal writes during flush
smoke_write_vs_dirty_test() ->
  Clean = setup(?FUNCTION_NAME),
  T = ?FUNCTION_NAME,
  ?check_trace(
     try
       ?assertEqual(ok, classy_table:open(T, opts())),
       ?assertEqual(ok, classy_table:dirty_write(T, foo, bad)),
       ?assertEqual(ok, classy_table:dirty_write(T, bar, good)),
       ?assertEqual(ok, classy_table:write(T, foo, good)),
       ?assertEqual([good], classy_table:lookup(T, foo)),
       ?assertEqual([good], classy_table:lookup(T, bar)),
       ?assertEqual(ok, classy_table:flush(T)),
       ?assertEqual([good], classy_table:lookup(T, foo)),
       ?assertEqual([good], classy_table:lookup(T, bar)),
       %% Test dirty deletes:
       ?assertEqual(ok, classy_table:dirty_delete(T, foo)),
       ?assertEqual(ok, classy_table:dirty_write(T, bar, bad)),
       ?assertEqual(ok, classy_table:delete(T, bar)),
       ?assertEqual([], classy_table:lookup(T, foo)),
       ?assertEqual([], classy_table:lookup(T, bar)),
       ?assertEqual(ok, classy_table:flush(T)),
       ?assertEqual([], classy_table:lookup(T, foo)),
       ?assertEqual([], classy_table:lookup(T, bar)),
       ?assertEqual(ok, classy_table:stop(T, infinity)),
       ?assertMatch(
          {ok, [ {v, 1}
               , {f, 0, M1}
               , {w, foo, good}
               , {w, bar, good}
               , {f, 1, M1}
               , {f, 0, M2}
               , {d, bar}
               , {d, foo}
               , {f, 1, M2}
               ]},
          classy_table:dump_wal(T))
     after
       cleanup(Clean)
     end,
     [ fun classy_SUITE:no_unexpected_events/1
     ]).

%% This test verifies effects of dirty write and delete operations.
smoke_dirty_write_delete_test() ->
  Clean = setup(?FUNCTION_NAME),
  T = ?FUNCTION_NAME,
  Opts = opts(#{ets_options => [ordered_set]}),
  ?check_trace(
     try
       ?assertEqual(ok, classy_table:open(T, Opts)),
       ?assertEqual(ok, classy_table:dirty_write(T, foo, foo)),
       ?assertEqual(ok, classy_table:dirty_write(T, foo, bar)),
       ?assertEqual([bar], classy_table:lookup(T, foo)),
       ?assertEqual(ok, classy_table:dirty_delete(T, foo)),
       ?assertEqual([], classy_table:lookup(T, foo))
     after
       classy_table:drop(T),
       cleanup(Clean)
     end,
     [ fun classy_SUITE:no_unexpected_events/1
     , {"events",
        fun(Trace) ->
            ?assertMatch(
               [ open
               , {w, foo, foo}
               , {w, foo, bar}
               , {d, foo}
                 %% Effects of drop:
               , close
               ],
               ?projection(op, ?of_kind(classy_table_update, Trace)))
        end}
     ]).

%% This test verifies restoration of the log after combined dirty writes and deletes.
smoke_restore_test() ->
  Clean = setup(?FUNCTION_NAME),
  T = ?FUNCTION_NAME,
  ?check_trace(
     try
       ?assertEqual(ok, classy_table:open(T, #{})),
       %% Set `foo' to 100:
       [?assertEqual(ok, classy_table:dirty_write(T, foo, N)) || N <- lists:seq(1, 100)],
       %% Set `bar' to 100:
       [?assertEqual(ok, classy_table:write(T, bar, N)) || N <- lists:seq(1, 100)],
       %% Delete `baz' after setting it:
       ?assertEqual(ok, classy_table:dirty_write(T, baz, 1)),
       ?assertEqual(ok, classy_table:delete(T, baz)),
       %% Reopen table:
       ?assertEqual(ok, classy_table:stop(T, infinity)),
       ?assertEqual(ok, classy_table:open(T, #{})),
       %% Check values:
       ?assertEqual([100], classy_table:lookup(T, foo)),
       ?assertEqual([100], classy_table:lookup(T, bar)),
       ?assertEqual([], classy_table:lookup(T, baz))
     after
       classy_table:drop(T),
       cleanup(Clean)
     end,
     fun classy_SUITE:no_unexpected_events/1).

%% This test verifies snapshot restoration.
smoke_snapshot_test() ->
  Clean = setup(?FUNCTION_NAME),
  T = ?FUNCTION_NAME,
  ?check_trace(
     try
       %% Insert data:
       ?assertEqual(ok, classy_table:open(T, #{})),
       [?assertEqual(ok, classy_table:dirty_write(T, N, N)) || N <- lists:seq(1, 100)],
       %% Checkpoint and reopen table:
       ?assertEqual(ok, classy_table:force_compaction(T)),
       ?assertEqual(ok, classy_table:stop(T, infinity)),
       ?assertEqual(ok, classy_table:open(T, #{})),
       %% Verify data:
       [?assertEqual([N], classy_table:lookup(T, N)) || N <- lists:seq(1, 100)]
     after
       classy_table:drop(T),
       cleanup(Clean)
     end,
     fun classy_SUITE:no_unexpected_events/1).

%% This test verifies table clear operation
clear_test() ->
  Clean = setup(?FUNCTION_NAME),
  T = ?FUNCTION_NAME,
  Opts = opts(#{ets_options => [ordered_set]}),
  ?check_trace(
     try
       %% Insert data:
       ?assertEqual(ok, classy_table:open(T, Opts)),
       [?assertEqual(ok, classy_table:dirty_write(T, N, N)) || N <- lists:seq(1, 5)],
       [?assertEqual(ok, classy_table:write(T, N, N)) || N <- lists:seq(6, 10)],
       ?assertEqual(
          ok,
          classy_table:clear(T)),
       ?assertEqual(
          [],
          ets:match(T, '$1')),
       %% Checkpoint and reopen table:
       ?assertEqual(ok, classy_table:stop(T, infinity)),
       ?assertEqual(ok, classy_table:open(T, Opts)),
       %% Verify restored data:
       ?assertEqual(
          [],
          ets:match(T, '$1'))
     after
       classy_table:drop(T),
       cleanup(Clean)
     end,
     [ fun classy_SUITE:no_unexpected_events/1
     , {"events",
        fun(Trace) ->
            ?assertMatch(
               [ open
               , {w, 1, 1}
               , {w, 2, 2}
               , {w, 3, 3}
               , {w, 4, 4}
               , {w, 5, 5}
               , {w, 6, 6}
               , {w, 7, 7}
               , {w, 8, 8}
               , {w, 9, 9}
               , {w, 10, 10}
                 %% Clear effects:
               , {d, 1}
               , {d, 2}
               , {d, 3}
               , {d, 4}
               , {d, 5}
               , {d, 6}
               , {d, 7}
               , {d, 8}
               , {d, 9}
               , {d, 10}
               , close
                 %% Reopen
               , open
               , close
               ],
               ?projection(op, ?of_kind(classy_table_update, Trace)))
        end}
     ]).

%% This test verifies that during re-opening of the table all
%% pre-existing keys are treated as `{w, _, _}' effects.
reopen_effects_test() ->
  Clean = setup(?FUNCTION_NAME),
  T = ?FUNCTION_NAME,
  Opts = opts(#{ets_options => [ordered_set]}),
  ?check_trace(
     try
       %% Insert data:
       ?assertEqual(ok, classy_table:open(T, Opts)),
       %% Update each value twice to create more entries in WAL. Extra
       %% operations should not be visible during reopen:
       [?assertEqual(ok, classy_table:dirty_write(T, N, N)) || N <- lists:seq(1, 5),
                                                               _ <- lists:seq(1, 2)],
       %% Reopen table:
       ?assertEqual(ok, classy_table:stop(T, infinity)),
       ?tp(test_reopen, #{}),
       ?assertEqual(ok, classy_table:open(T, Opts))
     after
       classy_table:drop(T),
       cleanup(Clean)
     end,
     [ fun classy_SUITE:no_unexpected_events/1
     , {"events",
        fun(Trace) ->
            {_, After} = ?split_trace_at(#{?snk_kind := test_reopen}, Trace),
            ?assertMatch(
               [ open
               , {w, 1, 1}
               , {w, 2, 2}
               , {w, 3, 3}
               , {w, 4, 4}
               , {w, 5, 5}
               %% Effects of drop:
               , {d, 1}
               , {d, 2}
               , {d, 3}
               , {d, 4}
               , {d, 5}
               , close
               ],
               ?projection(op, ?of_kind(classy_table_update, After)))
        end}
     ]).

%% This test verifies that a flush that was aborted mid-way doesn't
%% get partially restored.
botched_flush_test() ->
  Clean = setup(?FUNCTION_NAME),
  T = ?FUNCTION_NAME,
  Opts = opts(#{ets_options => [ordered_set]}),
  ?check_trace(
     try
       %% Insert data:
       ?assertEqual(ok, classy_table:open(T, Opts)),
       %% Write some values:
       [?assertEqual(ok, classy_table:dirty_write(T, N, N)) || N <- lists:seq(1, 5)],
       %% Close table and reopen its WAL:
       ?assertEqual(ok, classy_table:stop(T, infinity)),
       {ok, Dir} = application:get_env(classy, table_dir),
       WAL = filename:join(Dir, atom_to_list(T)),
       ?assert(filelib:is_file(WAL)),
       {ok, Log} = disk_log:open([ {name, make_ref()}
                                 , {file, WAL}
                                 , {type, halt}
                                 , {format, internal}
                                 , {repair, true}
                                 ]),
       %% Write some data there to emulate series of aborted flushes:
       ok = disk_log:log_terms(Log,
                               [ {f, 0, 5}, {w, 1, bad}, {d, 2}, {w, 3, bad}
                               , {f, 0, 10}, {w, 4, bad}, {d, 5}
                               ]),
       ok = disk_log:close(Log),
       %% Reopen the table.
       ?assertEqual(ok, classy_table:open(T, Opts)),
       %% Its contents should be unchanged:
       ?assertEqual(
          [[I, I] || I <- lists:seq(1, 5)],
          ets:match(T, #classy_kv{k = '$1', v = '$2'}))
     after
       cleanup(Clean)
     end,
     fun(Trace) ->
         ?assertMatch(
            [_, _],
            ?of_kind(?classy_table_anomaly, Trace))
     end).

%%================================================================================
%% Helper functions
%%================================================================================

opts() ->
  opts(#{}).

opts(Opts) ->
  Opts#{on_update => fun log_update/2}.

log_update(Name, Op) ->
  ?tp(classy_table_update, #{tab => Name, op => Op}).

-record(cleanup,
        { dir
        , apps
        , sup
        }).

setup(TC) ->
  Dir = dir(TC),
  application:set_env(classy, table_dir, Dir),
  application:set_env(classy, table_batch_size, 2),
  ok = filelib:ensure_path(Dir),
  {ok, Apps1} = application:ensure_all_started(gproc),
  {ok, Apps2} = application:ensure_all_started(optvar),
  {ok, Sup} = classy_sup:start_link_table_sup(),
  #cleanup{ dir = Dir
          , apps = Apps2 ++ Apps1
          , sup = Sup
          }.

cleanup(#cleanup{dir = Dir, apps = Apps, sup = Sup}) ->
  ok = classy_lib:sync_stop_proc(Sup, shutdown, 5_000),
  [application:stop(A) || A <- lists:reverse(Apps)],
  ok = file:del_dir_r(Dir).

dir(TC) ->
  filename:join("_build/test_data", atom_to_list(TC)).
