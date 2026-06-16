%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(classy_table).

-moduledoc """
This module implements a standalone minimalist analogue of mnesia's @code{local_data} table with @code{disc_copies} storage.

It is used to store classy's own persistent data.
Other applications can also use it for data that doesn't require replication and is not written too frequently.

@section Implementation

Classy tables consist of a RAM cache backed by ETS and a durable write ahead log.
Durable operations (write, delete, atomically) are immediately appended to the WAL,
while dirty mutations (dirty_write, dirty_delete) only mark certain keys as dirty.
All dirtied keys are flushed to WAL only when table is flushed.

@anchor{table_badness}
The WAL is periodically compacted when its ``badness'' exceeds the configured threshold.
Badness is calculated as a difference between the number of table elements and length of the log.
WAL compaction is a blocking operation:
all table mutations get blocked while it goes on.
Compaction time is proportional to the number of elements stored in the table.

Because of this design,
frequent mutations of the same key, e.g.

@example
classy_table:write(Tab, foo, 1),
classy_table:write(Tab, foo, 2),
classy_table:write(Tab, foo, 3),
...
@end example

are rather inefficient.

@section Limitations

@itemize
@item
All dirty operations are volatile:
they update only RAM cache and do not get persisted on disk until @code{flush} is called or the table server terminates.
They are meant for the situations where some keys are frequently updated,
but these updates can be lost.

There is no automatic flushing of dirty operations,
the business code must call @ref{classy_table:flush/1} function explicitly.

If it fails to do so,
all work for persisting the data will be done on terminate or after a durable mutation,
which may be risky or lead to unexpected results.

@item
This module is meant for small volumes of data and infrequent updates.
It's optimized for simplicity, not storage efficiency or performance.

@item
While this module guarantees that durable mutations don't return until the data is committed to WAL,
it is currently possible to @emph{read} uncommitted writes via @ref{classy_table:lookup/2} or plain ets queries.

So, do not use classy tables as a synchronization mechanism between different processes.

@item
Likewise, @code{on_update} callback is executed @emph{before} operations are persisted to the WAL.
As such, it is @emph{not} a reliable way to mirror the state of a classy table to other storage.

@item
@code{on_update} operations block the table server.
They must not contain any sort of heavy or long-running tasks.

@end itemize
""".

-behavior(gen_server).

%% API:
-export([ open/2
        , stop/2
        , clear/1
        , drop/1
        , write/3
        , atomically/2
        , dirty_write/3
        , delete/2
        , dirty_delete/2
        , flush/1
        , force_compaction/1
        , lookup/2
          %% For debugging:
        , dump_wal/1
        , dump_wal/2
        ]).

%% behavior callbacks:
-export([ init/1
        , handle_continue/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        ]).

%% internal exports:
-export([start_link/2]).

-export_type([tab/0, rec/0, options/0, on_update_op/0, on_update_callback/0, atomic_op/1]).

-include("classy_internal.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(optvar(TAB), {classy_table_is_ready, TAB}).
-define(name(TAB), {n, l, {?MODULE, TAB}}).
-define(via(TAB), {via, gproc, ?name(TAB)}).

-doc """
Table name that is used as a table identifier for both classy and ETS.
All classy tables are named ETS tables.
""".
-type tab() :: atom().

%% Maximum supported WAL version:
-define(max_wal_version, 1).
%% This version will be used for new logs. Note: it can be less than
%% the max version to ensure backward-compatibility.
-define(default_wal_version, 1).

%% WAL data:
-define(vsn(VSN), {v, VSN}).
-define(w(K, V), {w, K, V}).
-define(d(K), {d, K}).
-define(clear, clear).

-doc """
A type of operation in a batch accepted by @link{classy_table:atomically/2,atomically}.

@itemize
@item @code{@{w, Key, Value@}}
equivalent to @code{classy_table:write(Tab, Key, Value)}

@item @code{@{d, Key@}}
equivalent to @code{classy_table:delete(Tab, Key)}

@item @code{@{then, Any@}}
ignored by classy.
If atomic batch was successfully committed,
then batch elements like this are returned as is.

@end itemize
""".
-type atomic_op(Effect) :: ?w(_, _) | ?d(_) | {then, Effect}.

-type on_update_op() :: open
                      | ?w(_Key, _Val)
                      | ?d(_Key)
                      | close.

-type on_update_callback() :: fun((tab(), on_update_op()) -> _).

-doc """
Table creation options.

@itemize
@item @b{ets_options}
List of options passed to ETS.

@item @b{badness_threshold}
@xref{table_badness}

@item @b{on_update}
Add a callback executed on every table mutation.

@end itemize
""".
-type options() ::
        #{ ets_options => list()
         , badness_threshold => pos_integer()
         , on_update => on_update_callback()
         }.

-doc """
All classy tables contain key-value data wrapped in @code{#classy_kv} record.
""".
-type rec() :: #classy_kv{ k :: term()
                         , v :: term()
                         }.

-record(call_ensure_open, {tab :: tab()}).
-record(call_atomically, {ops :: [?w(_, _) | ?d(_)]}).
-record(call_write, {k, v, wal = true :: boolean()}).
-record(call_delete, {k, wal = true :: boolean()}).
-record(call_flush, {}).
-record(call_force_compaction, {}).
-record(call_drop, {}).
-record(call_clear, {}).

%%   Markers inserted at beginning and end of flush, meant to prevent restoration of aborted flush:
-define(flush_begin(I), {f, 0, I}).
-define(flush_end(I), {f, 1, I}).
%%   Note: only `w' and `d' operations can appear inside flush_begin/end span.

-type op() :: ?vsn(pos_integer()) | ?w(_, _) | ?d(_) | ?clear | ?flush_begin(_) | ?flush_end(_).

-record(restore_state,
        { %% Currently pending atomicity marker:
          marker :: integer()
          %% Reversed list of operations pending restore:
        , ops :: [?w(_, _) | ?d(_)]
        }).

-type restore_state() :: expect_header
                       | none %% No atomicity marker is active
                       | #restore_state{}.

-define(to_auto_flush, to_auto_flush).

-define(call_timeout, infinity).

%%================================================================================
%% API functions
%%================================================================================

-doc """
Open a table named @code{Tab} and block the caller until all data is fully restored.

Note: this function is idempotent.
""".
-spec open(tab(), options()) -> ok | {error, _}.
open(Tab, Options) when is_atom(Tab), is_map(Options) ->
  case classy_sup:start_table(Tab, Options) of
    {ok, Pid} ->
      gen_server:call(Pid, #call_ensure_open{tab = Tab}, ?call_timeout);
    {error, {already_started, Pid}} ->
      gen_server:call(Pid, #call_ensure_open{tab = Tab}, ?call_timeout);
    Err = {error, _} ->
      Err
  end.

-doc """
Close the table.

Warning: any process reading the table will become blocked.
""".
-spec stop(tab(), timeout()) -> ok | {error, timeout}.
stop(Tab, Timeout) ->
  case gproc:where(?name(Tab)) of
    Pid when is_pid(Pid) ->
      MRef = monitor(process, Pid),
      erlang:exit(Pid, shutdown),
      receive
        {'DOWN', MRef, process, _, _} ->
          ok
      after Timeout ->
          demonitor(MRef, [flush]),
          {error, timeout}
      end;
    undefined ->
      ok
  end.

-doc """
Update the RAM representation of the record and mark it as dirty.
No writes to disk are made until any of the following calls complete:
@itemize
@item @ref{classy_table:flush/1}
@item @ref{classy_table:stop/2}
@item @ref{classy_table:write/3}
@item @ref{classy_table:delete/2}
@item  @ref{classy_table:atomically/2}
@end itemize
""".
-spec dirty_write(tab(), _Key, _Val) -> ok.
dirty_write(Tab, Key, Val) ->
  gen_server:call(
    ?via(Tab),
    #call_write{k = Key, v = Val, wal = false},
    ?call_timeout).

-doc """
Update RAM representation of a record,
write operation to WAL,
sync WAL and then return.

Warning: this is a heavy operation.
While this module batches writes,
writes or deletes coming from a single process are always interleaved with a datasync.

If some process needs to reliably update a large number of records at once,
it's better to use @ref{classy_table:atomically/2}.
""".
-spec write(tab(), _Key, _Val) -> ok.
write(Tab, Key, Val) ->
  gen_server:call(
    ?via(Tab),
    #call_write{k = Key, v = Val, wal = true},
    ?call_timeout).

-doc """
Dirty version of @ref{classy_table:delete/2}.
From durability perspective,
it has the same properties as @ref{classy_table:dirty_write/3}.
""".
-spec dirty_delete(tab(), _Key) -> ok.
dirty_delete(Tab, Key) ->
  gen_server:call(
    ?via(Tab),
    #call_delete{k = Key, wal = false},
    ?call_timeout).

-doc """
Delete a record from the table.
From durability perspective,
it has the same properties as @ref{classy_table:write/3}.
""".
-spec delete(tab(), _Key) -> ok.
delete(Tab, Key) ->
  gen_server:call(
    ?via(Tab),
    #call_delete{k = Key, wal = true},
    ?call_timeout).

-doc """
Commit a number of operations into a table atomically:
either all or none of operations are durably stored by the time this function returns.

Operations are denoted via @ref{t:classy_table:atomic_op/1} type.
""".
-spec atomically(tab(), [atomic_op(Effect)]) -> {ok, [Effect]} | {error, _}.
atomically(_Tab, []) ->
  {ok, []};
atomically(Tab, Ops) ->
  try
    {Writes, Effects} =
      lists:foldr(
        fun(?w(_, _) = W, {AccW, AccE}) -> {[W | AccW], AccE};
           (?d(_) = W,    {AccW, AccE}) -> {[W | AccW], AccE};
           ({then, E},    {AccW, AccE}) -> {AccW, [E | AccE]};
           (_, _)                       -> throw(badarg)
        end,
        {[], []},
        Ops),
      maybe
        ok ?= gen_server:call(
                ?via(Tab),
                #call_atomically{ops = Writes},
                ?call_timeout),
        {ok, Effects}
      end
  catch
    badarg ->
      {error, {badarg, Tab, Ops}}
  end.

-doc """
Persist all records that got dirtied prior to this call to WAL.

Flush is atomic, meaning either all or none dirty operations are restored.
However, if multiple processes perform unsynchronized dirty writes and flushes in parallel,
data can be restored partially.
""".
-spec flush(tab()) -> ok.
flush(Tab) ->
  gen_server:call(
    ?via(Tab),
    #call_flush{},
    ?call_timeout).

-doc """
Make a checkpoint and truncate the WAL.
""".
-spec force_compaction(tab()) -> ok.
force_compaction(Tab) ->
  gen_server:call(
    ?via(Tab),
    #call_force_compaction{},
    ?call_timeout).

-doc """
Drop the table (it must be open)
""".
-spec drop(tab()) -> ok.
drop(Tab) ->
  gen_server:call(
    ?via(Tab),
    #call_drop{},
    ?call_timeout).

-doc """
Lookup a value from the table.

WARNING: this function can block the caller until the table is fully restored.
""".
-spec lookup(tab(), _Key) -> [_Val].
lookup(Tab, Key) ->
  case ets:whereis(Tab) of
    undefined ->
      %% Protection against typos and deadlocks. If this happens, the
      %% user must fix application startup order.
      error({badtable, Tab});
    _ ->
      %% Avoid reads while table is not fully restored:
      optvar:read(?optvar(Tab)),
      [V || #classy_kv{v = V} <- ets:lookup(Tab, Key)]
  end.

-doc """
Delete all data in the table.
This is a durable operation.

@code{on_update} callback sees effects of this operation as series of regular deletes.
""".
-spec clear(tab()) -> ok.
clear(Tab) ->
  gen_server:call(
    ?via(Tab),
    #call_clear{},
    ?call_timeout).

-doc """
@xref{classy_table:dump_wal/2}, uses the default directory.
""".
-spec dump_wal(tab()) -> {ok, list()} | {error, _}.
dump_wal(Tab) when is_atom(Tab) ->
  dump_wal(application:get_env(classy, table_dir, "."), Tab).

-doc """
Dump WAL for debugging.

Warning: this function reads the entire WAL into memory.
""".
-spec dump_wal(file:filename(), tab()) -> {ok, list()} | {error, _}.
dump_wal(Dir, Tab) ->
  File = filename:join(Dir, atom_to_list(Tab)),
  maybe
    {ok, Log} ?= open_log(File, read_only),
    L = do_dump_wal(Log, start),
    close_log(Log),
    {ok, L}
  end.

%%================================================================================
%% Internal exports
%%================================================================================

-doc false.
-spec start_link(tab(), options()) -> {ok, pid()}.
start_link(Tab, Options) ->
  gen_server:start_link(?via(Tab), ?MODULE, [Tab, Options], []).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { name :: tab()
        , wal_version :: pos_integer()
        , ets :: ets:tid()
        , dir :: file:filename()
        , dirty :: #{_ => true}
        , log :: _
        , log_size = 0 :: non_neg_integer()
        , badness_threshold :: pos_integer()
        , buffer = queue:new() :: queue:queue(?w(_, _) | ?d(_))
        , pending_replies = [] :: [gen_server:from()]
        , auto_flush_timer :: classy_lib:wakeup_timer()
        , on_update :: on_update_callback() | undefined
        }).

-type s() :: #s{}.

-doc false.
init([TabName, Options]) ->
  process_flag(trap_exit, true),
  optvar:unset(?optvar(TabName)),
  ETSOpts = maps:get(ets_options, Options, [set]),
  BadnessThreshold = maps:get(badness_threshold, Options, 100),
  S = #s{ name = TabName
        , wal_version = ?default_wal_version
        , ets = ets:new(TabName, [named_table, protected, {keypos, #classy_kv.k} | ETSOpts])
        , dirty = #{}
        , dir = application:get_env(classy, table_dir, ".")
        , badness_threshold = BadnessThreshold
        , on_update = maps:get(on_update, Options, undefined)
        },
  exec_on_update(open, S),
  {ok, S, {continue, restore}}.

-doc false.
handle_continue(restore, S0 = #s{name = Name}) ->
  T0 = os:system_time(microsecond),
  S = restore(S0),
  Elapsed = (os:system_time(microsecond) - T0) / 1.0e6,
  LogLevel = if Elapsed > 0.1 -> warning;
                true          -> debug
             end,
  ?tp(LogLevel, classy_table_restore_time,
      #{ table => Name
       , time  => Elapsed
       }),
  optvar:set(?optvar(Name), true),
  {noreply, S}.

-doc false.
handle_call(#call_ensure_open{}, _From, S) ->
  {reply, ok, S};
handle_call(#call_atomically{ops = Ops}, From, S) ->
  handle_atomic(From, Ops, S);
handle_call(#call_write{} = C, From, S) ->
  handle_write(From, C, S);
handle_call(#call_delete{} = C, From, S) ->
  handle_delete(From, C, S);
handle_call(#call_flush{}, From, S) ->
  with_compaction(From, ok, handle_flush(S));
handle_call(#call_force_compaction{}, From, S0) ->
  case do_compaction(S0) of
    {ok, S} ->
      {reply, ok, S};
    {error, Reason, S} ->
      gen_server:reply(From, {error, Reason}),
      {stop, Reason, S}
  end;
handle_call(#call_clear{}, From, S) ->
  with_compaction(From, ok, handle_clear(S));
handle_call(#call_drop{}, From, S) ->
  {stop, normal, handle_drop(From, S)};
handle_call(Call, From, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => call
       , from => From
       , content => Call
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

-doc false.
handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

-doc false.
handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(?to_auto_flush, S0) ->
  case maybe_compact(handle_flush(S0)) of
    {ok, S} ->
      {noreply, S};
    {error, Reason, S} ->
      {stop, Reason, S}
  end;
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

-doc false.
terminate(Reason, S) ->
  classy_lib:is_normal_exit(Reason) orelse
    ?tp(warning, ?classy_abnormal_exit,
        #{ server => ?MODULE
         , reason => Reason
         }),
  case S of
    undefined ->
      ok;
    #s{name = Name, log = Log} ->
      optvar:unset(?optvar(Name)),
      handle_flush(S),
      Log =/= undefined andalso disk_log:close(Log),
      exec_on_update(close, S)
  end.

%%================================================================================
%% Internal functions
%%================================================================================

-spec restore(s()) -> s().
restore(S0 = #s{name = Name, ets = ETS, wal_version = Vsn}) ->
  RegularName = log_name(S0, ""),
  NewName = log_name(S0, ".NEW"),
  ets:match_delete(ETS, '_'),
  case {is_log(RegularName), is_log(NewName)} of
    {false, false} ->
      %% TODO: there's a theoretical unrecoverable condition when log
      %% file is created, but header is not written there.
      {ok, Log} = open_log(RegularName, read_write),
      do_write_log(Log, [?vsn(Vsn)]),
      S0#s{log = Log};
    {true, false} ->
      %% Normal case:
      {ok, Log} = open_log(RegularName, read_write),
      {ok, S} = replay_wal(S0#s{log = Log}),
      exec_on_update_open(S),
      S;
    {true, true} ->
      %% Server was stopped while compaction was ongoing:
      ?tp(warning, ?classy_table_anomaly,
         #{ type     => aborted_compaction
          , table    => Name
          , log_name => NewName
          }),
      file:delete(NewName),
      restore(S0);
    {false, true} ->
      %% Should not happen:
      exit({classy_unrecoverable_aborted_table_compaction, NewName})
  end.

-spec replay_wal(s()) -> {ok, s()} | {error, _}.
replay_wal(S) ->
  replay_wal(S, start, expect_header).

-spec replay_wal(s(), disk_log:continuation() | start, restore_state()) ->
        {ok, s()} | {error, _}.
replay_wal(S0 = #s{name = Name, log = WAL}, Cont0, RestoreState0) ->
  case read_log_chunk(WAL, Cont0, batch_size()) of
    {ok, Cont, Chunk} ->
      try
        {S = #s{}, RestoreState} = lists:foldl(
                                     fun replay_wal_chunk/2,
                                     {S0, RestoreState0},
                                     Chunk),
        replay_wal(S, Cont, RestoreState)
      catch
        {wal_error, Err} ->
          {error, Err}
      end;
    eof ->
      case RestoreState0 of
        none ->
          ok;
        #restore_state{marker = Marker} ->
          %% Flush was aborted mid-flight. Discard half-flushed batch:
          ?tp(error, ?classy_table_anomaly,
              #{ type   => aborted_flush
               , table  => Name
               , marker => Marker
               })
      end,
      {ok, S0}
  end.

-spec replay_wal_chunk(op(), {s(), restore_state()}) -> {s(), restore_state()}.
replay_wal_chunk(Op, {S, expect_header}) ->
  case Op of
    ?vsn(Version) when Version =< ?max_wal_version ->
      { S#s{wal_version = Version}
      , none
      };
    ?vsn(Version) ->
      ?tp(error, ?classy_table_anomaly,
          #{ type              => table_format_is_too_new
           , table             => S#s.name
           , version           => Version
           , supported_version => ?max_wal_version
           }),
      throw({wal_error, table_created_on_newer_version});
    _ ->
      ?tp(error, ?classy_table_anomaly,
          #{ type              => missing_header
           , table             => S#s.name
           , op                => Op
           }),
      throw({wal_error, missing_wal_header})
  end;
replay_wal_chunk(Op, {S, none}) ->
  case Op of
    ?flush_begin(Marker) ->
      {inc_log_size(S), #restore_state{marker = Marker, ops = []}};
    ?clear ->
      {inc_log_size(log_effects(replay, Op, S)), none};
    ?w(_, _) ->
      {inc_log_size(log_effects(replay, Op, S)), none};
    ?d(_) ->
      {inc_log_size(log_effects(replay, Op, S)), none};
    _ ->
      ?tp(error, ?classy_table_anomaly,
          #{ type  => unexpected_operation
           , table => S#s.name
           , op    => Op
           , state => none
           }),
      {inc_log_size(S), none}
  end;
replay_wal_chunk(Op, {S, RS0 = #restore_state{marker = Marker, ops = OpsAcc}}) ->
  %% There is an ongoing flush:
  case Op of
    ?flush_end(Marker) ->
      %% Flush was complete. Apply buffered operations:
      { lists:foldr(
          fun(I, Acc) -> log_effects(replay, I, Acc) end,
          inc_log_size(S),
          OpsAcc)
      , none
      };
    ?flush_begin(NewMarker) ->
      %% Flush was aborted mid-flight. Discard data:
      ?tp(error, ?classy_table_anomaly,
          #{ type   => aborted_flush
           , table  => S#s.name
           , marker => Marker
           }),
      {inc_log_size(S), #restore_state{marker = NewMarker, ops = []}};
    ?w(_, _) = Op ->
      {inc_log_size(S), RS0#restore_state{ops = [Op | OpsAcc]}};
    ?d(_) = Op ->
      {inc_log_size(S), RS0#restore_state{ops = [Op | OpsAcc]}};
    Other ->
      ?tp(error, ?classy_table_anomaly,
          #{ type  => aborted_flush
           , table => S#s.name
           , op    => Other
           , state => RS0
           }),
      {inc_log_size(S), none}
  end.

-spec log_name(s(), string()) -> file:filename().
log_name(#s{name = Name, dir = Dir}, Suffix) ->
  FN = atom_to_list(Name) ++ Suffix,
  filename:join(Dir, FN).

handle_atomic(From, Ops, S) ->
  {noreply, add_to_buffer(From, Ops, S)}.

handle_write(From, #call_write{k = K, v = V, wal = true}, S) ->
  {noreply, add_to_buffer(From, [?w(K, V)], S)};
handle_write(_From, #call_write{k = K, v = V, wal = false}, S) ->
  {reply, ok, log_effects(dirty, ?w(K, V), S)}.

handle_delete(From, #call_delete{k = K, wal = true}, S) ->
  {noreply, add_to_buffer(From, [?d(K)], S)};
handle_delete(_From, #call_delete{k = K, wal = false}, S) ->
  {reply, ok, log_effects(dirty, ?d(K), S)}.

-spec log_effects(normal | dirty | replay, op(), s()) -> s().
log_effects(_Context, ?flush_begin(_), S) ->
  S;
log_effects(_Context, ?flush_end(_), S) ->
  S;
log_effects(Context, ?clear, S = #s{ets = ETS}) ->
  Context =/= replay andalso exec_on_update_clear(S),
  ets:match_delete(ETS, '_'),
  S#s{ dirty = #{}
     , buffer = queue:new()
     };
log_effects(Context, Op = ?w(K, V), S = #s{ets = ETS, dirty = Dirty}) ->
  ets:insert(ETS, #classy_kv{k = K, v = V}),
  Context =/= replay andalso exec_on_update(Op, S),
  S#s{ dirty = case Context of
                 normal -> maps:remove(K, Dirty);
                 dirty  -> Dirty#{K => true};
                 replay -> Dirty
               end
     };
log_effects(Context, ?d(K), S = #s{ets = ETS, dirty = Dirty}) ->
  ets:delete(ETS, K),
  Context =/= replay andalso exec_on_update(?d(K), S),
  S#s{ dirty = case Context of
                 normal -> maps:remove(K, Dirty);
                 dirty  -> Dirty#{K => true};
                 replay -> Dirty
               end
     }.

handle_flush(S0 = #s{log = Log, dirty = Dirty, buffer = Buf, auto_flush_timer = AutoFlush}) ->
  S1 = S0#s{auto_flush_timer = classy_lib:cancel_wakeup(AutoFlush)},
  N = queue:len(Buf) + map_size(Dirty),
  case Log of
    undefined ->
      %% Log is closed due to previous error, do not flush:
      S = S1,
      Reply = {error, log_failed};
    _ when N =:= 0 ->
      %% No pending operations to flush:
      S = S1,
      Reply = ok;
    _ ->
      S = do_flush(N, S1),
      Reply = ok
  end,
  erlang:garbage_collect(),
  send_pending_replies(Reply, S).

do_flush(N, S = #s{ets = ETS, log = Log, buffer = Buf, dirty = DirtyKeys, log_size = LogSize0}) ->
  Marker = LogSize0,
  case N of
    1 ->
      %% Only one operation to flush. It's impossible to partially
      %% restore it, so skipping flush wrappers:
      BeginMarker = [],
      EndMarker = [],
      NMarkers = 0;
    _ ->
      BeginMarker = [?flush_begin(Marker)],
      EndMarker = [?flush_end(Marker)],
      NMarkers = 2
  end,
  %% 1. Dump buffered operations:
  Buffered = queue:to_list(Buf),
  ok = disk_log:log_terms(Log, BeginMarker ++ Buffered),
  %% 2. Dump dirty records. This has to be done after commiting the
  %% buffered ops, since adding operations to the buffer clears the
  %% dirty flags. So if the dirty flag is set, then any persistent
  %% operation with the key precedes the last dirty one.
  DirtyOps =
    maps:fold(
      fun(K, _, Acc) ->
          [case ets:lookup(ETS, K) of
             [#classy_kv{v = V}] -> ?w(K, V);
             []                  -> ?d(K)
           end | Acc]
      end,
      EndMarker,
      DirtyKeys),
  ok = disk_log:log_terms(Log, DirtyOps),
  ok = disk_log:sync(Log),
  LogSize = LogSize0 + N + NMarkers,
  S#s{ dirty = #{}
     , buffer = queue:new()
     , log_size = LogSize
     }.

-spec exec_on_update_open(s()) -> ok.
exec_on_update_open(#s{on_update = undefined}) ->
  ok;
exec_on_update_open(S = #s{ets = ETS}) ->
  ets:foldl(
    fun(#classy_kv{k = K, v = V}, Acc) ->
        exec_on_update(?w(K, V), S),
        Acc
    end,
    undefined,
    ETS),
  ok.

-spec exec_on_update_clear(s()) -> ok.
exec_on_update_clear(#s{on_update = undefined}) ->
  ok;
exec_on_update_clear(S = #s{ets = ETS}) ->
  ets:foldl(
    fun(#classy_kv{k = K}, Acc) ->
        exec_on_update(?d(K), S),
        Acc
    end,
    undefined,
    ETS),
  ok.

exec_on_update(_, #s{on_update = undefined}) ->
  ok;
exec_on_update(Op, #s{on_update = Fun, name = Name}) ->
  try Fun(Name, Op)
  catch
    EC:Err:Stack ->
      ?tp(error, classy_table_on_update_callback_failure,
          #{ EC         => Err
           , stacktrace => Stack
           , table      => Name
           , callback   => Fun
           })
  end.

-spec do_compaction(s()) -> {ok, s()} | {error, wal_compaction_failed, s()}.
do_compaction(S0 = #s{name = Name, ets = Ets}) ->
  S1 = #s{log = Old} = handle_flush(S0),
  ok = close_log(Old),
  S = S1#s{log = undefined},
  try
     NewName = log_name(S, ".NEW"),
     OldName = log_name(S, ""),
     NewWalVersion = ?default_wal_version,
     {ok, Log} = open_log(NewName, read_write),
     do_write_log(Log, [?vsn(NewWalVersion)]),
     LogSize =
       dump_ets(
         Log,
         0,
         ets:match(Ets, '$1', batch_size())),
     ok = rename_log(NewName, OldName),
     {ok, S#s{ log = Log
             , dirty = #{}
             , log_size = LogSize
             , wal_version = NewWalVersion
             }}
  catch
    EC:Err:Stack ->
      ?tp(error, ?classy_table_anomaly,
          #{ type  => failed_compaction
           , EC    => Err
           , stack => Stack
           , table => Name
           }),
      {error, wal_compaction_failed, S}
  end.

dump_ets(_Log, N, '$end_of_table') ->
  N;
dump_ets(Log, N, {Batch, Cont}) ->
  Recs = lists:map(
           fun([#classy_kv{k = K, v = V}]) ->
               ?w(K, V)
           end,
           Batch),
  ok = do_write_log(Log, Recs),
  dump_ets(
    Log,
    N + length(Recs),
    ets:match(Cont)).

-spec add_to_buffer(gen_server:from(), [?w(_, _) | ?d(_)], s()) -> s().
add_to_buffer(
  From,
  Ops,
  #s{ buffer = Buf0
    , pending_replies = PendingReplies
    , auto_flush_timer = Timer
    } = S0
 ) ->
  {Buf, S} =
    lists:foldl(
      fun(Op, {AccBuf, AccS}) ->
          { queue:in(Op, AccBuf)
          , log_effects(normal, Op, AccS)
          }
      end,
      {Buf0, S0},
      Ops),
  S#s{ buffer = Buf
     , pending_replies = [From | PendingReplies]
     , auto_flush_timer = classy_lib:wakeup_after(?to_auto_flush, 0, Timer)
     }.

send_pending_replies(Reply, S = #s{pending_replies = Pending}) ->
  [gen_server:reply(From, Reply) || From <- Pending],
  S#s{pending_replies = []}.

handle_clear(S0 = #s{log = Log, log_size = LogSize}) ->
  ok = do_write_log(Log, [?clear]),
  S = log_effects(normal, ?clear, S0#s{log_size = LogSize + 1}),
  send_pending_replies(ok, S).

handle_drop(From, S = #s{ets = Ets, log = Log}) ->
  exec_on_update_clear(S),
  exec_on_update(close, S),
  ets:delete(Ets),
  close_log(Log),
  file:delete(log_name(S, ".NEW")),
  file:delete(log_name(S, "")),
  gen_server:reply(From, ok),
  undefined.

with_compaction(From, Reply, S0) ->
  case maybe_compact(S0) of
    {ok, S} ->
      {reply, Reply, S};
    {error, Reason, S} ->
      gen_server:reply(From, {error, Reason}),
      {stop, Reason, S}
  end.

-spec maybe_compact(s()) -> {ok, s()} | {error, _Reason, s()}.
maybe_compact(S = #s{badness_threshold = Threshold}) ->
  case log_badness(S) >= Threshold of
    true ->
      do_compaction(S);
    false ->
      {ok, S}
  end.

log_badness(#s{ets = ETS, log_size = LogSize}) ->
  NItems = ets:info(ETS, size),
  max(0, LogSize - NItems).

batch_size() ->
  application:get_env(classy, table_batch_size, 100).

rename_log(From, To) ->
  file:rename(From, To).

-spec is_log(file:filename()) -> boolean().
is_log(Filename) ->
  filelib:is_regular(Filename).

open_log(Filename, Mode) ->
  Opts = [ {name, make_ref()}
         , {file, classy_lib:ensure_list(Filename)}
         , {mode, Mode}
         , {format, internal}
         , {type, halt}
         , {size, infinity}
         , {repair, true}
         , {notify, false}
         , {linkto, self()}
         ],
  case disk_log:open(Opts) of
    {ok, Log} ->
      {ok, Log};
    {repaired, Log, {recovered, Recovered}, {badbytes, BadBytes}} ->
      BadBytes > 0 andalso
        ?tp(error, ?classy_table_anomaly,
            #{ type      => wal_bad_bytes
             , file      => Filename
             , recovered => Recovered
             , bad_bytes => BadBytes
             }),
      {ok, Log};
    {error, Reason} ->
      {error, Reason}
  end.

close_log(undefined) ->
  ok;
close_log(Log) ->
  disk_log:close(Log).

do_write_log(Log, Terms) ->
  maybe
    ok ?= disk_log:log_terms(Log, Terms),
    disk_log:sync(Log)
  end.

do_dump_wal(Log, Cont0) ->
  case disk_log:chunk(Log, Cont0) of
    eof ->
      [];
    {Cont, Terms} ->
      Terms ++ do_dump_wal(Log, Cont);
    {Cont, Terms, BadBytes} ->
      Terms ++ [{'$bad_bytes', BadBytes} | do_dump_wal(Log, Cont)]
  end.

inc_log_size(S = #s{log_size = N}) ->
  S#s{log_size = N + 1}.

read_log_chunk(Log, Cont, Size) ->
  case disk_log:chunk(Log, Cont, Size) of
    {error, _} = Err ->
      Err;
    {NewCont, Terms} ->
      {ok, NewCont, Terms};
    {NewCont, Terms, _BadBytes} ->
      %% In case of corrupt data in read-only mode, we still return what we can
      {ok, NewCont, Terms};
    eof ->
      eof
  end.
