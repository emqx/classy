%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc
%% This module implements a standalone minimalist analogue of mnesia's `local_data' table with `disc_copies' storage.
%%
%% It is used to persistently save classy's own data.
%% Other applications can also use it for data that doesn't require replication and is not written too frequently.
%%
%% == Limitations ==
%% <itemize>
%% <li>
%% All "dirty" operation are volatile:
%% they update only RAM cache and do not get persisted on disk until `flush' is called or the table server terminates.
%% They are meant for the situations where some keys are frequently updated,
%% but these updates can be lost.
%%
%% There is no automatic flushing,
%% the business code must flush explicitly.
%%
%% If it fails to do so,
%% all work for persisting the data will be done on terminate,
%% which may be risky due to various timeouts.
%% </li>
%%
%% <li>
%% This module is meant for small volumes of data and infrequent updates.
%% It's optimized for simplicity, not storage efficiency or performance.
%% </li>
%% </itemize>
-module(classy_table).

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

-type tab() :: atom().

%% WAL data:
-define(w(K, V), {w, K, V}).
-define(d(K), {d, K}).
-define(clear, clear).

-type atomic_op(Effect) :: ?w(_, _) | ?d(_) | {then, Effect}.

-type on_update_op() :: open
                      | ?w(_Key, _Val)
                      | ?d(_Key)
                      | close.

-type on_update_callback() :: fun((tab(), on_update_op()) -> _).

-type options() ::
        #{ ets_options => list()
         , badness_threshold => pos_integer()
         , on_update => on_update_callback()
         }.

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

-type op() :: ?w(_, _) | ?d(_) | ?clear | ?flush_begin(_) | ?flush_end(_).

-record(restore_state,
        { %% Currently pending atomicity marker:
          marker :: integer()
          %% Reversed list of operations pending restore:
        , ops :: [?w(_, _) | ?d(_)]
        }).

-type restore_state() :: none %% No atomicity marker is active
                       | #restore_state{}.

-define(call_timeout, infinity).

%%================================================================================
%% API functions
%%================================================================================


%% @doc Open a table named `Tab'.
%%
%% Note: this function is idempotent.
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

%% @doc Close the table.
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

%% @doc Update the RAM representation of the record and mark it as dirty.
%% No writes to disk are made until `flush' is called explicitly or implicitly.
-spec dirty_write(tab(), _Key, _Val) -> ok.
dirty_write(Tab, Key, Val) ->
  gen_server:call(
    ?via(Tab),
    #call_write{k = Key, v = Val, wal = false},
    ?call_timeout).

%% @doc Write operation to WAL, sync WAL and then update RAM representation of the record.
%%
%% Note: this is a heavy operation.
-spec write(tab(), _Key, _Val) -> ok.
write(Tab, Key, Val) ->
  gen_server:call(
    ?via(Tab),
    #call_write{k = Key, v = Val, wal = true},
    ?call_timeout).

%% @doc Mark record as deleted and dirty.
-spec dirty_delete(tab(), _Key) -> ok.
dirty_delete(Tab, Key) ->
  gen_server:call(
    ?via(Tab),
    #call_delete{k = Key, wal = false},
    ?call_timeout).

%% @doc Write to the WAL that the record has been deleted and update the RAM representation.
-spec delete(tab(), _Key) -> ok.
delete(Tab, Key) ->
  gen_server:call(
    ?via(Tab),
    #call_delete{k = Key, wal = true},
    ?call_timeout).

%% @doc Write a number of operations atomically
-spec atomically(tab(), [atomic_op(Effect)]) -> {ok, [Effect]} | {error, _}.
atomically(_Tab, []) ->
  {ok, []};
atomically(Tab, Ops) ->
  try
    {Writes, Effects} =
      lists:foldl(
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
        {ok, lists:reverse(Effects)}
      end
  catch
    badarg ->
      {error, {badarg, Tab, Ops}}
  end.

%% @doc Persist all records that got dirtied prior to this call to WAL.
%%
%% Flush is atomic, meaning either all or none dirty operations are restored.
%% However, if multiple processes perform unsynchronized dirty writes and flushes in parallel,
%% data can be restored partially.
-spec flush(tab()) -> ok.
flush(Tab) ->
  gen_server:call(
    ?via(Tab),
    #call_flush{},
    ?call_timeout).

%% @doc Make a checkpoint and truncate the WAL.
-spec force_compaction(tab()) -> ok.
force_compaction(Tab) ->
  gen_server:call(
    ?via(Tab),
    #call_force_compaction{},
    ?call_timeout).

%% @doc Drop the table (it must be open)
-spec drop(tab()) -> ok.
drop(Tab) ->
  gen_server:call(
    ?via(Tab),
    #call_drop{},
    ?call_timeout).

%% @doc Lookup a value from the table.
%%
%% WARNING: this function can block the caller until the table is fully restored.
-spec lookup(tab(), _Key) -> [_Val].
lookup(Tab, Key) ->
  case ets:whereis(Tab) of
    undefined ->
      %% Protection against typos and such:
      error({badtable, Tab});
    _ ->
      %% Avoid reads while table is not fully restored:
      optvar:read(?optvar(Tab)),
      [V || #classy_kv{v = V} <- ets:lookup(Tab, Key)]
  end.

%% @doc Delete all data in the table.
-spec clear(tab()) -> ok.
clear(Tab) ->
  gen_server:call(
    ?via(Tab),
    #call_clear{},
    ?call_timeout).

%%================================================================================
%% Internal exports
%%================================================================================

%% @private
-spec start_link(tab(), options()) -> {ok, pid()}.
start_link(Tab, Options) ->
  gen_server:start_link(?via(Tab), ?MODULE, [Tab, Options], []).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s,
        { name :: tab()
        , ets :: ets:tid()
        , dir :: file:filename()
        , dirty :: #{_ => true}
        , log :: _
        , log_size = 0 :: non_neg_integer()
        , badness_threshold :: pos_integer()
        , on_update :: on_update_callback() | undefined
        }).

-type s() :: #s{}.

%% @private
init([TabName, Options]) ->
  process_flag(trap_exit, true),
  optvar:unset(?optvar(TabName)),
  ETSOpts = maps:get(ets_options, Options, [set]),
  BadnessThreshold = maps:get(badness_threshold, Options, 100),
  S = #s{ name = TabName
        , ets = ets:new(TabName, [named_table, protected, {keypos, #classy_kv.k} | ETSOpts])
        , dirty = #{}
        , dir = application:get_env(classy, table_dir, ".")
        , badness_threshold = BadnessThreshold
        , on_update = maps:get(on_update, Options, undefined)
        },
  exec_on_update(open, S),
  {ok, S, {continue, restore}}.

%% @private
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

%% @private
handle_call(#call_ensure_open{}, _From, S) ->
  {reply, ok, S};
handle_call(#call_atomically{ops = Ops}, From, S) ->
  maybe_compact(From, ok, handle_atomic(Ops, S));
handle_call(#call_write{} = C, _From, S) ->
  {reply, ok, handle_write(C, S)};
handle_call(#call_delete{} = C, _From, S) ->
  {reply, ok, handle_delete(C, S)};
handle_call(#call_flush{}, From, S) ->
  maybe_compact(From, ok, handle_flush(S));
handle_call(#call_force_compaction{}, From, S0) ->
  case do_compaction(S0) of
    {ok, S} ->
      {reply, ok, S};
    {error, Reason, S} ->
      gen_server:reply(From, {error, Reason}),
      {stop, compaction_failed, S}
  end;
handle_call(#call_clear{}, From, S0) ->
  S = handle_clear(S0),
  maybe_compact(From, ok, handle_flush(S));
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

%% @private
handle_cast(Cast, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => cast
       , content => Cast
       , server => ?MODULE
       }),
  {noreply, S}.

%% @private
handle_info({'EXIT', _, shutdown}, S) ->
  {stop, shutdown, S};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

%% @private
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
      disk_log:close(Log),
      exec_on_update(close, S)
  end.

%%================================================================================
%% Internal functions
%%================================================================================

-spec restore(s()) -> s().
restore(S = #s{name = Name, ets = ETS}) ->
  RegularName = log_name(S, ""),
  NewName = log_name(S, ".NEW"),
  ets:match_delete(ETS, '_'),
  case {is_log(RegularName), is_log(NewName)} of
    {false, false} ->
      {ok, Log} = open_log(RegularName, read_write),
      S#s{log = Log};
    {true, false} ->
      %% Normal case:
      {ok, Log} = open_log(RegularName, read_write),
      {ok, LogSize} = do_restore(Name, Log, start, none, ETS, 0),
      exec_on_update_open(S),
      S#s{ log = Log
         , log_size = LogSize
         };
    {true, true} ->
      %% Server was stopped while compaction was ongoing:
      ?tp(warning, ?classy_table_anomaly,
         #{ type     => aborted_compaction
          , table    => Name
          , log_name => NewName
          }),
      file:delete(NewName),
      restore(S);
    {false, true} ->
      %% Should not happen:
      exit({classy_unrecoverable_aborted_table_compaction, NewName})
  end.

do_restore(Name, Log, Cont0, RestoreState0, ETS, N) ->
  case read_log_chunk(Log, Cont0, batch_size()) of
    {ok, Cont, Chunk} ->
      RestoreState =
        lists:foldl(
          fun(Op, Acc) -> do_restore_op(Name, ETS, Op, Acc) end,
          RestoreState0,
          Chunk),
      do_restore(Name, Log, Cont, RestoreState, ETS, N + length(Chunk));
    eof ->
      case RestoreState0 of
        none ->
          ok;
        #restore_state{marker = Marker} ->
          %% Flush was aborted mid-flight. Discard data:
          ?tp(error, ?classy_table_anomaly,
              #{ type   => aborted_flush
               , table  => Name
               , marker => Marker
               })
      end,
      {ok, N}
  end.

-spec do_restore_op(tab(), ets:tid(), op(), restore_state()) -> restore_state().
do_restore_op(Name, ETS, Op, S0 = #restore_state{marker = Marker, ops = OpsAcc}) ->
  %% Flush is ongoing.
  %% To avoid restoring a partially flushed state,
  %% do not apply operations to the ETS, accumulate them in `OpsAcc'.
  case Op of
    ?flush_end(Marker) ->
      %% Note: order of operations is irrelevant:
      %% during flush we just iterate over map keys in random order.
      %% So, no `lists:reverse'.
      _ = [do_restore_op(Name, ETS, I, none) || I <- OpsAcc],
      none;
    ?flush_begin(NewMarker) ->
      %% Flush was aborted mid-flight. Discard data:
      ?tp(error, ?classy_table_anomaly,
          #{ type   => aborted_flush
           , table  => Name
           , marker => Marker
           }),
      #restore_state{marker = NewMarker, ops = []};
    ?w(_, _) = Op ->
      S0#restore_state{ops = [Op | OpsAcc]};
    ?d(_) = Op ->
      S0#restore_state{ops = [Op | OpsAcc]};
    Other ->
      ?tp(error, ?classy_table_anomaly,
          #{ type  => aborted_flush
           , table => Name
           , op    => Other
           , state => S0
           }),
      none
  end;
do_restore_op(Name, ETS, Op, none) ->
  case Op of
    ?w(K, V) ->
      ets:insert(ETS, #classy_kv{k = K, v = V}),
      none;
    ?d(K) ->
      ets:delete(ETS, K),
      none;
    ?clear ->
      ets:match_delete(ETS, '_'),
      none;
    ?flush_begin(Marker) ->
      #restore_state{marker = Marker, ops = []};
    Other ->
      ?tp(error, ?classy_table_anomaly,
          #{ type  => unexpected_operation
           , table => Name
           , op    => Other
           , state => none
           }),
      none
  end.

-spec log_name(s(), string()) -> file:filename().
log_name(#s{name = Name, dir = Dir}, Suffix) ->
  FN = atom_to_list(Name) ++ Suffix,
  filename:join(Dir, FN).

handle_atomic(Ops, S = #s{log_size = LogSize}) ->
  Marker = LogSize,
  Log = [?flush_begin(Marker) | Ops] ++ [?flush_end(Marker)],
  commit(Log, S).

handle_write(#call_write{k = K, v = V, wal = true}, S) ->
  commit([?w(K, V)], S);
handle_write(#call_write{k = K, v = V, wal = false}, S = #s{ets = ETS, dirty = D}) ->
  ets:insert(ETS, #classy_kv{k = K, v = V}),
  exec_on_update(?w(K, V), S),
  S#s{ dirty = D#{K => true}
     }.

handle_delete(#call_delete{k = K, wal = true}, S) ->
  commit([?d(K)], S);
handle_delete(#call_delete{k = K, wal = false}, S = #s{ets = ETS, dirty = D}) ->
  ets:delete(ETS, K),
  exec_on_update(?d(K), S),
  S#s{dirty = D#{K => true}}.

commit(Ops, S = #s{log = Log, log_size = LS}) ->
  ok = do_write_log(Log, Ops),
  lists:foldl(
    fun log_effects/2,
    S#s{log_size = LS + length(Ops)},
    Ops).

log_effects(?flush_begin(_), S) ->
  S;
log_effects(?flush_end(_), S) ->
  S;
log_effects(Op = ?w(K, V), S = #s{ets = ETS, dirty = Dirty}) ->
  ets:insert(ETS, #classy_kv{k = K, v = V}),
  exec_on_update(Op, S),
  S#s{ dirty = maps:remove(K, Dirty)
     };
log_effects(?d(K), S = #s{ets = ETS, dirty = Dirty}) ->
  ets:delete(ETS, K),
  exec_on_update(?d(K), S),
  S#s{ dirty = maps:remove(K, Dirty)
     }.

handle_flush(S = #s{log = Log, dirty = Dirty}) when Log =:= undefined;
                                                    map_size(Dirty) =:= 0 ->
  S;
handle_flush(S = #s{ets = ETS, log = Log, dirty = Dirty, log_size = LogSize0}) ->
  Marker = LogSize0,
  BeginMarker = ?flush_begin(Marker),
  EndMarker = ?flush_end(Marker),
  {LogSize, Ops} =
    maps:fold(
      fun(K, _, {AccNW, AccOps}) ->
          Op = case ets:lookup(ETS, K) of
                 [#classy_kv{v = V}] -> ?w(K, V);
                 []                  -> ?d(K)
               end,
          { AccNW + 1
          , [Op | AccOps]
          }
      end,
      {LogSize0, [EndMarker]},
      Dirty),
  ok = do_write_log(Log, [BeginMarker|Ops]),
  S#s{ dirty = #{}
     , log_size = LogSize + 2 %% account for the markers
     }.

-spec exec_on_update_open(#s{}) -> ok.
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

-spec exec_on_update_clear(#s{}) -> ok.
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

-spec do_compaction(s()) -> {ok, s()} | {error, _Reason, undefined}.
do_compaction(S0 = #s{name = Name, ets = Ets}) ->
  S1 = #s{log = Old} = handle_flush(S0),
  ok = close_log(Old),
  S = S1#s{log = undefined},
  try
     NewName = log_name(S, ".NEW"),
     OldName = log_name(S, ""),
     {ok, Log} = open_log(NewName, read_write),
     LogSize =
       dump_ets(
         Log,
         0,
         ets:match(Ets, '$1', batch_size())),
     ok = rename_log(NewName, OldName),
     {ok, S#s{log = Log, dirty = #{}, log_size = LogSize}}
  catch
    EC:Err:Stack ->
      ?tp(error, ?classy_table_anomaly,
          #{ type  => failed_compaction
           , EC    => Err
           , stack => Stack
           , table => Name
           }),
      {error, Err, undefined}
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

handle_clear(S = #s{ets = Ets, log = Log, log_size = LogSize}) ->
  ok = do_write_log(Log, [?clear]),
  exec_on_update_clear(S),
  ets:match_delete(Ets, '_'),
  S#s{dirty = #{}, log_size = LogSize + 1}.

handle_drop(From, S = #s{ets = Ets, log = Log}) ->
  exec_on_update_clear(S),
  exec_on_update(close, S),
  ets:delete(Ets),
  close_log(Log),
  file:delete(log_name(S, ".NEW")),
  file:delete(log_name(S, "")),
  gen_server:reply(From, ok),
  undefined.

maybe_compact(From, Reply, S0 = #s{badness_threshold = Threshold}) ->
  case log_badness(S0) >= Threshold of
    true ->
      case do_compaction(S0) of
        {ok, S} ->
          {reply, Reply, S};
        {error, Reason, S} ->
          gen_server:reply(From, {error, Reason}),
          {stop, Reason, S}
      end;
    false ->
      {reply, Reply, S0}
  end.

log_badness(#s{ets = ETS, log_size = LogSize}) ->
  NItems = ets:info(ETS, size),
  max(0, LogSize - NItems).

batch_size() ->
  application:get_env(classy, table_batch_size, 100).

-ifndef(CONCUERROR).
rename_log(From, To) ->
  file:rename(From, To).

is_log(Filename) ->
  filelib:is_file(Filename).

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

-endif.
