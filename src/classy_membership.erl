%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(classy_membership).
-moduledoc """
Cluster Membership CRDT.

This module provides a low-level API for maintaining and updating cluster membership information.
Business code should not use it directly.
""".

-behavior(gen_server).

%% API:
-export([ known_clusters/1
        , set_member/4
        , set_info/3
        , set_liveness/5
        , members/2
        , list_local_sites/1
        , get_data/4
        , cleanup/3
        , flush/2
        , node_of_site/2
        , site_of_node/2
        , to_liveness/3
        , dump/0
        ]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/2, cast_sync/3, call_sync/3]).

-ifdef(TEST).
-export([reset_acked_out/4]).
-endif.

-export_type([start_args/0, op/0, ord/0, clock/0, sync_data/0, pk_last/0, pv_last/0, event/0]).

-include("classy_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(call_timeout, infinity).

%%================================================================================
%% Type declarations
%%================================================================================

-define(default_sync_timeout, 1000).
-define(ptab, classy_membership).

-define(name(CLUSTER, SITE), {n, l, {?MODULE, CLUSTER, SITE}}).
-define(via(CLUSTER, SITE), {via, gproc, ?name(CLUSTER, SITE)}).

-type start_args() ::
        #{ cluster := classy:cluster_id()
         , site    := classy:site()
         }.

-type clock() :: non_neg_integer().

%% Types of keys stored in the cluster CRDT.
-record(mem, {s :: classy:site() | atom()}).
-record(host, {s :: classy:site() | atom()}).
-record(info, {s :: classy:site() | atom()}).
-record(live, {s :: classy:site() | atom()}).
-type key() :: #mem{} | #host{} | #info{} | #live{}.

-doc "Arbitrary term used to break ties between commands with the same logical timestamp.".
-type magic() :: term().

%% The following command is used to update status of `target' site
-record(op_set, {origin, k, c, m, val, owt, reserved}).

-type op() ::
        #op_set{ origin :: classy:site() %% Site that issued the command
               , k :: key()              %% Property of the target site that is being updated
               , c :: clock()            %% Logical time at `origin' when it issued the command:
               , m :: magic()            %% This term can be used to break ties. It's not super relevant.
               , val :: term()           %% Updated value
               , owt :: integer()        %% Origin's wall time when the update was made. This value
                                         %% isn't used by this module directly, but it gives hints to
                                         %% the autoclean
               , reserved :: term()
               }.

-doc "Projection of `op()` fields used to establish total order of logs.".
-type ord() :: {clock(), magic(), classy:site()}.

%% `site' sends a portion of its logs that is newer than `since':
-record(cast_sync,
        { cluster :: classy:cluster_id()
        , from :: classy:site()
        , since :: clock()
        , acked :: clock()
          %% c
        , c :: clock()
        , data :: [op()]
        , reserved
        }).

-type sync_data() :: #cast_sync{}.

%% Timeout message triggering syncing out state
-record(to_sync_out, {}).

-record(call_set, {k :: key(), v :: term()}).
-record(call_cleanup, {forget_after :: pos_integer()}).
-record(call_flush, {}).
-record(call_get_data, {since :: clock(), acked :: clock()}).

-record(s,
        { %% Cluster ID:
          cluster :: classy:cluster_id()
          %% Local site id:
        , site :: classy:site()
        , sync_timer :: classy_lib:wakeup_timer()
          %% Logical clock:
        , clock :: clock()
          %% Logical clock of the last update sent to `classy_node' worker
        , events_since = 0
        }).

%% Persistent table keys:

%% Lamport clock of the site:
-record(pk_clock,
        { c %% Cluster
        , s %% Site (local)
        }).

%% Clock of the last message received from the peer:
-record(pk_acked_in, {c :: classy:cluster_id(), l :: classy:site(), r :: classy:site()}).
%% Clock of the last sync message to the peer:
-record(pk_acked_out, {c :: classy:cluster_id(), l :: classy:site(), r :: classy:site()}).
%% Last set command for the site:
-record(pk_last, {c, l, k}).
-type pk_last() :: #pk_last{c :: classy:cluster_id(), l :: classy:site(), k :: key()}.

%% Composite table values:
-record(pv_last,
        { op    % Last update operation
        , toi   % Local logical time of importing the op
        }).
-type pv_last() :: #pv_last{op :: op(), toi :: clock()}.

-type event() :: {mem, classy:site(), boolean()} |
                 {host, classy:site(), node()} |
                 {meta, classy:site(), map()} |
                 {liveness, classy:site(), boolean(), non_neg_integer()}.

-type liveness() :: non_neg_integer().

%%================================================================================
%% API functions
%%================================================================================

-doc "Return collection of clusters where site is or was peer.".
-spec known_clusters(classy:site()) -> #{classy:cluster_id() => [classy:site()]}.
known_clusters(Site) ->
  ets:foldl(
    fun(#classy_kv{k = K}, Acc) ->
        case K of
          #pk_last{c = Cluster, l = Local, k = #mem{s = Remote}} when Local =:= Site ->
            maps:update_with(
              Cluster,
              fun(L) -> [Remote | L] end,
              [Remote],
              Acc);
          _ ->
            Acc
        end
    end,
    #{},
    ?ptab).

-doc """
Low-level call that sets @code{Target}'s membership state to @code{true}.

WARNING: this function does not check if target site exists and/or is part of cluster.
When called with invalid @code{Target},
it will create a new entry that will eventually make its way to the entire cluster.
This fictitious site will become a member until kicked,
and even then some records about it may be kept around.
""".
-spec set_member(classy:cluster_id(), classy:site(), classy:site(), boolean()) -> ok | {error, _}.
set_member(Cluster, Local, Target, IsMember) when is_boolean(IsMember) ->
  try
    gen_server:call(
      ?via(Cluster, Local),
      #call_set{k = #mem{s = Target}, v = IsMember},
      ?call_timeout)
  catch
    EC:Err -> {error, {EC, Err}}
  end.

-doc """
Set site information.

This function is called by the site itself.
""".
-spec set_info(classy:cluster_id(), classy:site(), _Info) -> ok | {error, _}.
set_info(Cluster, Local, Info) ->
  try
    gen_server:call(
      ?via(Cluster, Local),
      #call_set{k = #info{s = Local}, v = Info},
      ?call_timeout)
  catch
    EC:Err -> {error, {EC, Err}}
  end.

-doc """
Set site liveness info.

Normally, liveness is only updated by the local site.
The remotes can only update @code{IsUp} boolean on behalf of other nodes,
but they must always keep @code{NRestarts} as is.
""".
-spec set_liveness(classy:cluster_id(), classy:site(), classy:site(), integer(), boolean()) -> ok | {error, _}.
set_liveness(Cluster, Local, Target, NRestarts, IsUp) ->
  try
    gen_server:call(
      ?via(Cluster, Local),
      #call_set{k = #live{s = Target}, v = #liveness{nr = NRestarts, isup = IsUp}},
      ?call_timeout)
  catch
    EC:Err -> {error, {EC, Err}}
  end.

-doc """
Return active members of the @code{Cluster},
as perceived by @code{Local} site.

WARNING: if @code{Local} is not a member of the returned list,
then the local system may be permanently out of sync with the @code{Cluster} or @code{@{Cluster,Local@}} server may be inactive.
In both cases the result value can't be trusted.
""".
-spec members(classy:cluster_id(), classy:site()) -> [classy:site()].
members(Cluster, Local) ->
  MS = { #classy_kv{ k = #pk_last{c = Cluster, l = Local, k = #mem{s = '$1', _ = '_'}, _ = '_'}
                   , v = #pv_last{op = #op_set{val = true, _ = '_'}, _ = '_'}
                   , _ = '_'
                   }
       , []
       , ['$1']
       },
  ets:select(?ptab, [MS]).

-doc "List local sites and clusters.".
-spec list_local_sites(running | all) -> [{classy:cluster_id(), classy:site()}].
list_local_sites(running) ->
  MS = {{?name('$1', '$2'), '_', '_'}, [], [{{'$1', '$2'}}]},
  gproc:select({local, names}, [MS]);
list_local_sites(all) ->
  %% Every local site has a logical clock:
  MS = { #classy_kv{ k = #pk_clock{c = '$1', s = '$2'}
                   , _ = '_'
                   }
       , []
       , [{{'$1', '$2'}}]
       },
  ets:select(?ptab, [MS]).

-doc """
Return mapping of nodes to sites.

WARNING: it includes kicked members.
""".
-spec site_of_node(classy:cluster_id(), classy:site()) -> #{node() => classy:site()}.
site_of_node(Cluster, Local) ->
  maps:from_list(select_nodes(Cluster, Local, {{'$2', '$1'}})).

-doc """
Return mapping of sites to nodes.

WARNING: it includes kicked members.
""".
-spec node_of_site(classy:cluster_id(), classy:site()) -> #{classy:site() => node()}.
node_of_site(Cluster, Local) ->
  maps:from_list(select_nodes(Cluster, Local, {{'$1', '$2'}})).

-doc """
Delete sites that have been kicked for longer than
@code{ForgetAfter} seconds ago from the local state.
""".
-spec cleanup(classy:cluster_id(), classy:site(), pos_integer()) -> ok.
cleanup(Cluster, Local, ForgetAfter) ->
  gen_server:call(
    ?via(Cluster, Local),
    #call_cleanup{forget_after = ForgetAfter},
    ?call_timeout).

-doc "Force sending of events and execution of hooks.".
-spec flush(classy:cluster_id(), classy:site()) -> ok.
flush(Cluster, Local) ->
  gen_server:call(
    ?via(Cluster, Local),
    #call_flush{},
    ?call_timeout).

-doc "Format full state of the membership CRDT in human-readable form for tests and debugging.".
-spec dump() -> #{{classy:cluster_id(), classy:site()} => map()}.
dump() ->
  ets:foldl(
    fun(#classy_kv{k = K, v = V}, Acc) ->
        case K of
          #pk_last{c = Cluster, l = Local} ->
            #pv_last{ op = #op_set{ origin = Origin
                                  , k = Key
                                  , c = Clock
                                  , val = Val
                                  , owt = OWT
                                  }
                    , toi = TOI
                    } = V,
            Info = {Val, #{origin => Origin, ltime => Clock, wtime => OWT, ltime_imported => TOI}},
            Path = case Key of
                     #mem{s = Target}  -> [peers, Target, mem];
                     #host{s = Target} -> [peers, Target, host];
                     #info{s = Target} -> [peers, Target, info];
                     #live{s = Target} -> [peers, Target, liveness]
                   end,
            classy_lib:map_deep_insert(
              [{Cluster, Local} | Path],
              Info,
              Acc);
          #pk_acked_out{c = Cluster, l = Local, r = Remote} ->
            classy_lib:map_deep_insert(
              [{Cluster, Local}, acked_out, Remote],
              V,
              Acc);
          #pk_acked_in{c = Cluster, l = Local, r = Remote} ->
            classy_lib:map_deep_insert(
              [{Cluster, Local}, acked_in, Remote],
              V,
              Acc);
          #pk_clock{c = Cluster, s = Local} ->
            classy_lib:map_deep_insert(
              [{Cluster, Local}, clock],
              V,
              Acc)
        end
    end,
    #{},
    ?ptab).

-doc """
Convert information about liveness to a sortable non-negative integer replicated via CRDT.

Arguments:
@enumerate
  @item Number of restarts of the target site
  @item @code{true} of the update is made by the site itself,
        @code{false} if the target site is updated by the 3rd party.
  @item @code{true} if the site is up.
@end enumerate

Bits in the integer are organized such that:
@enumerate
  @item Liveness information for the higher number of restarts always wins.
  @item Liveness updates made by the 3rd party win.
        This way the target site knows that its peers decided that it went down.
@end enumerate
""".
-spec to_liveness(non_neg_integer(), boolean(), boolean()) -> liveness().
to_liveness(NRestarts, Self, IsUp) when is_integer(NRestarts),
                                        NRestarts >= 0,
                                        is_boolean(Self),
                                        is_boolean(IsUp) ->
  %% Liveness bits:
  %%
  %%           always 0       always 0
  %%              v               v
  %% n_restarts | 0  | not Self | 0 | IsUp |
  OnBehalf = case Self of
               false -> 2#100;
               true  -> 0
             end,
  UpBit = case IsUp of
            true -> 1;
            false -> 0
          end,
  (NRestarts bsl 4) bor OnBehalf bor UpBit.

-ifdef(TEST).

reset_acked_out(Cluster, Local, Remote, Clock) ->
  classy_table:dirty_write(
    ?ptab,
    #pk_acked_out{c = Cluster, l = Local, r = Remote},
    Clock).

-endif.

%%================================================================================
%% Internal exports
%%================================================================================

-doc false.
-spec start_link(classy:cluster_id(), classy:site()) -> {ok, pid()}.
start_link(Cluster, Local) ->
  Args = #{cluster => Cluster, site => Local},
  gen_server:start_link(?via(Cluster, Local), ?MODULE, Args, []).

%% @doc Send membership data to the process
-spec cast_sync(classy:cluster_id(), classy:site(), sync_data()) -> ok.
cast_sync(Cluster, Site, SyncData) ->
  gen_server:cast(?via(Cluster, Site), SyncData).

%% @doc Send membership data to the process
-spec call_sync(classy:cluster_id(), classy:site(), sync_data()) -> ok.
call_sync(Cluster, Site, SyncData) ->
  ?tp(classy_membership_call_sync,
      #{ clus => Cluster
       , local => Site
       , clock => #cast_sync.c
       }),
  gen_server:call(?via(Cluster, Site), SyncData, ?call_timeout).

-doc "Get membership data".
-spec get_data(classy:cluster_id(), classy:site(), clock(), clock()) -> {ok, sync_data()} | {error, _}.
get_data(Cluster, Local, Since, Acked) ->
  try
    gen_server:call(
      ?via(Cluster, Local),
      #call_get_data{since = Since, acked = Acked},
      5_000)
  catch
    exit:{noproc, _} ->
      {error, not_in_cluster}
  end.

%%================================================================================
%% behavior callbacks
%%================================================================================

-doc false.
-spec init(start_args()) -> {ok, #s{}}.
init(#{cluster := Cluster, site := Site}) when is_binary(Site), is_binary(Cluster) ->
  process_flag(trap_exit, true),
  logger:update_process_metadata(
    #{ cluster => Cluster
     , local => Site
     }),
  ok = classy_table:open(
         ?ptab,
         #{ets_options => [{read_concurrency, true}]}),
  case classy_table:lookup(?ptab, #pk_clock{c = Cluster, s = Site}) of
    [Clock] -> ok;
    [] -> Clock = 0
  end,
  S0 = #s{ cluster = Cluster
         , site = Site
         , clock = Clock
         },
  %% Establish own membership in the cluster. The below operation
  %% (with clock = 0) is used to set the default value.
  S1 = local_command(
         0,
         #call_set{ k = #mem{s = Site}
                  , v = true
                  },
         S0),
  S = local_command(
        #call_set{ k = #host{s = Site}
                 , v = node()
                 },
        S1),
  {ok, need_sync(0, S)}.

-doc false.
handle_call(#call_set{} = CMD, _From, S0) ->
  S = local_command(CMD, S0),
  {reply, ok, S};
handle_call(#call_get_data{since = Since, acked = Acked}, _From, S) ->
  Reply = {ok, get_sync_data(Since, Acked, S)},
  {reply, Reply, S};
handle_call(#call_cleanup{forget_after = FA}, _From, S) ->
  Reply = handle_cleanup(FA, S),
  {reply, Reply, S};
handle_call(#call_flush{}, _From, S0) ->
  S = handle_flush(S0),
  {reply, ok, S};
handle_call(#cast_sync{} = Req, _From, S0) ->
  S = handle_sync_in(Req, S0),
  {reply, ok, S};
handle_call(Call, From, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => call
       , from => From
       , content => Call
       , server => ?MODULE
       }),
  {reply, {error, unknown_call}, S}.

-doc false.
handle_cast(#cast_sync{} = Req, S0) ->
  S = handle_sync_in(Req, S0),
  {noreply, S};
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
handle_info(#to_sync_out{}, S0) ->
  S = handle_flush(S0#s{sync_timer = undefined}),
  {noreply, need_sync(S)};
handle_info(Info, S) ->
  ?tp(warning, ?classy_unknown_event,
      #{ kind => info
       , content => Info
       , server => ?MODULE
       }),
  {noreply, S}.

-doc false.
terminate(Reason, #s{cluster = Cluster, site = Site}) ->
  classy_lib:is_normal_exit(Reason) orelse
    ?tp(warning, ?classy_abnormal_exit,
        #{ server => ?MODULE
         , cluster => Cluster
         , site => Site
         , reason => Reason
         }),
  classy_table:flush(?ptab).

%%================================================================================
%% Internal functions
%%================================================================================

handle_flush(S0) ->
  %% Note: for performance, `handle_sync_in' and `local_command' functions update the Lamport clock and other states using `dirty_write'.
  %% It is correct, because losing these writes due to restart doesn't lead to any visible side effects.
  %% Even if the table server restarts and loses pending writes, the restored state will be equivalent to not receiving the messages.
  %% But *here* side effects are about to happen.
  %% As such, it is important to persist the changes:
  ok = classy_table:flush(?ptab),
  S = handle_sync_out(S0),
  ok = classy_table:flush(?ptab),
  %% This one updates hook cursor persistently, no need to flush again:
  notify(S).

-doc """
Total order of the operation.

Note that this total order is @b{not} strictly causal,
because Lamport clocks don't provide such guarantee.

Practically, lack of strict causality means the cluster will eventually converge to the same state,
but earlier join/leave commands may override later commands.

These adverse side effects can be observed when conflicting commands are issued on different nodes faster than the nodes sync with each other.
This is most likely to happen during a network partition.

Please see theories/classy.v file for more details and some intricate requirements for this function.
""".
-spec ord(op()) -> ord().
ord(#op_set{k = #live{}, val = #liveness{nr = NR, isup = IsUp}, c = C, origin = O}) ->
  %% Liveness information is merged according to special rules:
  %% here we use target's n_restarts for serialization (as it can only grow),
  %% also "down" events take priority over "up" events.
  %% This is to make sure that information about site getting down at a certain
  %% "incarnation" is eventually propagated everywhere.
  UpN = case IsUp of
          true -> 0;
          false -> 1
        end,
  {NR, UpN, C, O};
ord(#op_set{c = C, m = M, origin = O}) ->
  {C, M, O}.

-spec local_command(#call_set{}, #s{}) -> #s{}.
local_command(Cmd, S0) ->
  {C, S} = inc_get_clock(S0),
  local_command(C, Cmd, S).

-spec local_command(clock(), #call_set{}, #s{}) -> #s{}.
local_command(C, #call_set{k = K, v = V}, S = #s{site = Local}) ->
  ?tp(classy_local_command,
      #{ key => K
       , val => V
       , clock => C
       }),
  Op = #op_set{ origin = Local
              , c = C
              , k = K
              , val = V
              , owt = classy_lib:time_s()
              },
  ?tp(debug, classy_membership_local_command,
      #{ c => C
       , local => Local
       , k => K
       , clus => S#s.cluster
       , gcl => S#s.clock
       }),
  %% Note: here C is either `0' (when setting the default value) or `#s.c' (the last logical clock).
  %% In the former case we don't care about losing,
  %% and in the latter case we can't lose,
  %% so merge result can be ignored:
  _ = merge(C, Op, S),
  need_sync(S).

-spec handle_sync_in(#cast_sync{}, #s{}) -> #s{}.
handle_sync_in(Req, S0) ->
  #cast_sync{ from = From
            , since = Since
            , c = Cf
            , acked = AckedOut
            , data = Data
            } = Req,
  ?tp(classy_membership_sync_in,
      #{ from => From
       , since => Since
       , clock => Cf
       , acked => AckedOut
       , data => Data
       }),
  AckedIn = get_acked_in(From, S0),
  case AckedIn >= Since of
    true ->
      {Cl, S} = inc_get_clock(sync_clock(Cf, S0)),
      lists:foreach(
        fun(Op) -> merge(Cl, Op, S) end,
        Data),
      set_acked_in(From, Cf, S),
      set_acked_out(From, AckedOut, S),
      need_sync(S);
     false ->
      %% Gap in sequence. Ignore this message. Peer will be notified
      %% about the expected acked value during the next sync-out:
      ?tp(debug, classy_membership_sync_gap,
          #{ acked => {AckedIn, Since}
           , from => From
           , c => Cf
           , data => Data
           }),
      %% Always set remoted acked counter to heal the gap:
      set_acked_out(From, AckedOut, S0),
      need_sync(S0)
  end.

-spec handle_sync_out(#s{}) -> #s{}.
handle_sync_out(S = #s{cluster = Cluster}) ->
  SyncTargets = sync_targets(S),
  ?tp(classy_membership_sync_out, #{targets => SyncTargets}),
  maps:foreach(
    fun(Site, Node) ->
        Since = get_acked_out(Site, S),
        Acked = get_acked_in(Site, S),
        Data = get_sync_data(Since, Acked, S),
        ?tp(debug, classy_membership_sync_target,
            #{ remote => Site
             , node   => Node
             , since  => Since
             , acked  => Acked
             , data   => Data
             }),
        case node() of
          Node -> ?MODULE:cast_sync(Cluster, Site, Data);
          _    -> erpc:cast(Node, ?MODULE, cast_sync, [Cluster, Site, Data])
        end
    end,
    SyncTargets),
  S.

get_sync_data(Since, Acked, S = #s{cluster = Cluster, site = Local, clock = C}) ->
  #cast_sync{ cluster = Cluster
            , from = Local
            , since = Since
            , acked = Acked
            , c = C
            , data = memtab_since(Since, S)
            }.

-spec merge(clock(), op(), #s{}) -> boolean().
merge(LTime, Op, S) ->
  #op_set{k = K} = Op,
  case memtab_lookup(K, S) of
    {ok, Op0} ->
      case ord(Op) > ord(Op0) of
        true ->
          set_last(LTime, Op, S),
          true;
        false ->
          false
      end;
    undefined ->
      set_last(LTime, Op, S),
      true
  end.

notify(S = #s{clock = C, cluster = Cluster, events_since = EventsSince}) ->
  %% Find out what changed:
  UpdatedEntries = memtab_since(EventsSince + 1, S),
  Deltas = lists:flatmap(
             fun(#op_set{k = #mem{s = Peer}, val = IsMember}) ->
                 [{mem, Peer, IsMember}];
                (#op_set{k = #host{s = Peer}, val = Host}) ->
                 [{host, Peer, Host}];
                (#op_set{k = #info{s = Peer}, val = Meta}) ->
                 [{meta, Peer, Meta}];
                (#op_set{k = #live{s = Peer}, val = #liveness{isup = IsUp, nr = NR}}) ->
                 [{liveness, Peer, IsUp, NR}];
                (#op_set{}) ->
                 []
             end,
             UpdatedEntries),
  case Deltas of
    [] -> ok;
    _  -> classy_node:notify_mem_deltas(Cluster, Deltas)
  end,
  S#s{events_since = C}.

-spec handle_cleanup(pos_integer(), #s{}) -> ok.
handle_cleanup(ForgetAfter, S = #s{site = Self}) ->
  Sites = sites_for_cleanup(Self, ForgetAfter, S),
  ?tp(debug, classy_membership_forget,
      #{ interval => ForgetAfter
       , sites => Sites
       }),
  [forget_site(I, S) || I <- Sites],
  ok.

%% @doc Return list of peers that can be forgotten, according to the
%% following criteria:
%%
%% 1. Peer has been kicked from the cluster
%% 2. It happened at least SecsDown ago
%% 3. Other active peers have received all data about the peer
-spec sites_for_cleanup(classy:site(), integer(), #s{}) -> [classy:site()].
sites_for_cleanup(Self, SecsDown, S) ->
  MinTimeWhenKicked = classy_lib:time_s() - SecsDown,
  Peers = peers(S),
  LongGone = [I || I <- Peers, is_long_gone(MinTimeWhenKicked, I, S)],
  ActiveMembers = Peers -- LongGone,
  MinAcked = min_acked(ActiveMembers -- [Self], S),
  [I || I <- LongGone, max_toi(I, S) =< MinAcked].

forget_site(Site, #s{cluster = Cluster, site = Local}) when is_binary(Site) ->
  classy_table:dirty_delete(?ptab, #pk_last{c = Cluster, l = Local, k = #mem{s = Site}}),
  classy_table:dirty_delete(?ptab, #pk_last{c = Cluster, l = Local, k = #host{s = Site}}),
  classy_table:dirty_delete(?ptab, #pk_last{c = Cluster, l = Local, k = #info{s = Site}}),
  classy_table:dirty_delete(?ptab, #pk_last{c = Cluster, l = Local, k = #live{s = Site}}),
  classy_table:dirty_delete(?ptab, #pk_acked_in{c = Cluster, l = Local, r = Site}),
  classy_table:dirty_delete(?ptab, #pk_acked_out{c = Cluster, l = Local, r = Site}),
  ok.

%%--------------------------------------------------------------------------------
%% Interface for site state storage
%%--------------------------------------------------------------------------------

-spec set_last(clock(), op(), #s{}) -> ok.
set_last(LTime, Op, #s{cluster = Cluster, site = Local}) ->
  #op_set{k = K} = Op,
  classy_table:dirty_write(
    ?ptab,
    #pk_last{c = Cluster, l = Local, k = K},
    #pv_last{op = Op, toi = LTime}).

-spec memtab_lookup(key(), #s{}) -> {ok, op()} | undefined.
memtab_lookup(K, #s{cluster = Cluster, site = Local}) ->
  case classy_table:lookup(?ptab, #pk_last{c = Cluster, l = Local, k = K}) of
    [#pv_last{op = Op}] ->
      {ok, Op};
    [] ->
      undefined
  end.

-spec memtab_since(clock(), #s{}) -> [op()].
memtab_since(Since, #s{cluster = Cluster, site = Local}) ->
  MS = { #classy_kv{ k = #pk_last{c = Cluster, l = Local, _ = '_'}
                   , v = #pv_last{op = '$1', toi = '$2', _ = '_'}
                   , _ = '_'
                   }
       , [{'>=', '$2', Since}]
       , ['$1']
       },
  ets:select(?ptab, [MS]).

-spec peers(#s{}) -> [classy:site()].
peers(#s{cluster = Cluster, site = Local}) ->
  MS = { #classy_kv{ k = #pk_last{c = Cluster, l = Local, k = #mem{s = '$1'}}
                   , _ = '_'
                   }
       , []
       , ['$1']
       },
  ets:select(?ptab, [MS]).

-spec nodes_of_cluster(#s{}) -> #{classy:site() => node()}.
nodes_of_cluster(#s{cluster = Cluster, site = Local}) ->
  maps:from_list(select_nodes(Cluster, Local, {{'$1', '$2'}})).

select_nodes(Cluster, Local, Action) ->
  MS = { #classy_kv{ k = #pk_last{c = Cluster, l = Local,  k = #host{s = '$1'}}
                   , v = #pv_last{op = #op_set{val = '$2', _ = '_'}, _ = '_'}
                   , _ = '_'
                   }
       , []
       , [Action]
       },
  ets:select(?ptab, [MS]).

-doc """
Find minimal Lamport clock,
such that all sites from the list have acked it.
""".
-spec min_acked([classy:site()], #s{}) -> clock() | undefined.
min_acked(Sites, S = #s{}) ->
  lists:foldl(
    fun(Peer, Acc) ->
        min(get_acked_out(Peer, S), Acc)
    end,
    undefined,
    Sites).

-spec is_long_gone(pos_integer(), classy:site(), #s{}) -> boolean().
is_long_gone(MinTimeWhenKicked, Site, S) ->
  case memtab_lookup(#mem{s = Site}, S) of
    {ok, #op_set{val = false, owt = TimeWhenKicked}} when
        TimeWhenKicked < MinTimeWhenKicked ->
      true;
    _ ->
      false
  end.

%% @doc Return maximum `toi' for any property of the site.
-spec max_toi(classy:site(), #s{}) -> clock() | undefined.
max_toi(Site, #s{cluster = Cluster, site = Local}) ->
  MS = [{ #classy_kv{ k = #pk_last{c = Cluster, l = Local, k = Key, _ = '_'}
                    , v = #pv_last{toi = '$1', _ = '_'}
                    , _ = '_'
                    }
        , []
        , ['$1']
        } || Key <- [#mem{s = Site}, #host{s = Site}, #info{s = Site}, #live{s = Site}]],
  L = ets:select(?ptab, MS),
  case L of
    [] -> undefined;
    _  -> lists:max(L)
  end.

%%--------------------------------------------------------------------------------
%% Logical clocks
%%--------------------------------------------------------------------------------

-spec inc_get_clock(#s{}) -> {clock(), #s{}}.
inc_get_clock(S0 = #s{clock = C0}) ->
  C = C0 + 1,
  S = S0#s{clock = C},
  save_clock(S),
  {C, S}.

-spec sync_clock(clock(), #s{}) -> #s{}.
sync_clock(T1, S0 = #s{clock = T0}) ->
  S = S0#s{clock = max(T0, T1)},
  save_clock(S),
  S.

-spec save_clock(#s{}) -> ok.
save_clock(#s{cluster = Cluster, site = Local, clock = Clock}) ->
  classy_table:dirty_write(
    ?ptab,
    #pk_clock{c = Cluster, s = Local},
    Clock).

%%--------------------------------------------------------------------------------
%% Functions related to the p2p protocol
%%--------------------------------------------------------------------------------

%% Schedule sync with the peers after the default timeout.
-spec need_sync(#s{}) -> #s{}.
need_sync(S) ->
  need_sync(
    application:get_env(classy, sync_timeout, ?default_sync_timeout),
    S).

%% Schedule sync with the peers after a specified timeout.
%% This call has no effect if the sync is already scheduled.
-spec need_sync(non_neg_integer(), #s{}) -> #s{}.
need_sync(After, S = #s{sync_timer = T0}) ->
  T = classy_lib:wakeup_after(#to_sync_out{}, After, T0),
  S#s{sync_timer = T}.

-spec get_acked_in(classy:site(), #s{}) -> clock().
get_acked_in(Site, #s{cluster = C, site = Local}) ->
  case classy_table:lookup(?ptab, #pk_acked_in{c = C, l = Local, r = Site}) of
    [Clock] -> Clock;
    []      -> 0
  end.

-spec get_acked_out(classy:site(), #s{}) -> clock().
get_acked_out(Site, #s{cluster = C, site = Local}) ->
  case classy_table:lookup(?ptab, #pk_acked_out{c = C, l = Local, r = Site}) of
    [Clock] -> Clock;
    []      -> 0
  end.

-spec set_acked_in(classy:site(), clock(), #s{}) -> ok.
set_acked_in(Site, Clock, #s{cluster = Cluster, site = Local}) ->
  classy_table:dirty_write(
    ?ptab,
    #pk_acked_in{c = Cluster, l = Local, r = Site},
    Clock).

-spec set_acked_out(classy:site(), clock(), #s{}) -> ok.
set_acked_out(Site, Clock, #s{cluster = Cluster, site = Local}) ->
  classy_table:dirty_write(
    ?ptab,
    #pk_acked_out{c = Cluster, l = Local, r = Site},
    Clock).

sync_targets(S = #s{site = Local}) ->
  maps:remove(
    Local,
    nodes_of_cluster(S)).

%%--------------------------------------------------------------------------------
%% Unit tests
%%--------------------------------------------------------------------------------

-ifdef(TEST).

table_scans_test() ->
  Cleanup = classy_table_tests:setup(?FUNCTION_NAME),
  S1 = #s{cluster = <<"c1">>, site = <<"s1">>},
  S2 = #s{cluster = <<"c2">>, site = <<"s2">>},
  try
    classy_table:open(?ptab, #{}),
    [begin
       true = merge(
                0,
                #op_set{ origin = <<"s1">>
                       , k = #mem{s = <<"s1">>}
                       , val = true
                       },
                S),
       true = merge(
                0,
                #op_set{ origin = <<"s1">>
                       , k = #host{s = <<"s1">>}
                       , val = 'n1@localhost'
                       },
                S),
       true = merge(
                0,
                #op_set{ origin = <<"s1">>
                       , k = #mem{s = <<"s2">>}
                       , val = false
                       },
                S),
       true = merge(
                0,
                #op_set{ origin = <<"s1">>
                       , k = #host{s = <<"s2">>}
                       , val = 'n2@localhost'
                       },
                S)
     end || S <- [S1, S2]],
    %% Check `peers' function:
    [?assertEqual(
        [<<"s1">>, <<"s2">>],
        lists:sort(peers(S)))
     || S <- [S1, S2]],
    %% Check `members' function:
    ?assertEqual(
       [<<"s1">>],
       members(<<"c1">>, <<"s1">>)),
    ?assertEqual(
       [<<"s1">>],
       members(<<"c2">>, <<"s2">>)
      ),
    ?assertEqual(
       [],
       members(<<"c1">>, <<"s2">>)),
    %% Check `nodes_of_cluster' function:
    [?assertEqual(
        #{<<"s1">> => 'n1@localhost', <<"s2">> => 'n2@localhost'},
        nodes_of_cluster(S))
     || S <- [S1, S2]]
  after
    classy_table:drop(?ptab),
    classy_table_tests:cleanup(Cleanup)
  end.

is_long_gone_test() ->
  Cleanup = classy_table_tests:setup(?FUNCTION_NAME),
  S1 = #s{cluster = <<"c1">>, site = <<"s1">>},
  S2 = #s{cluster = <<"c2">>, site = <<"s2">>},
  try
    classy_table:open(?ptab, #{}),
    [begin
       true = merge(
                0,
                #op_set{ origin = <<"s1">>
                       , k = #mem{s = <<"s1">>}
                       , val = true
                       , owt = 0
                       },
                S),
       true = merge(
                0,
                #op_set{ origin = <<"s1">>
                       , k = #mem{s = <<"s2">>}
                       , val = false
                       , owt = 1
                       },
                S),
       true = merge(
                0,
                #op_set{ origin = <<"s1">>
                       , k = #mem{s = <<"s3">>}
                       , val = true
                       , owt = 1
                       },
                S),
       true = merge(
                0,
                #op_set{ origin = <<"s1">>
                       , k = #mem{s = <<"s4">>}
                       , val = false
                       , owt = 2
                       },
                S),
       %% T = 3:
       ?assertNot(is_long_gone(3, <<"s1">>, S)),
       ?assert(is_long_gone(3, <<"s2">>, S)),
       ?assertNot(is_long_gone(3, <<"s3">>, S)),
       ?assert(is_long_gone(3, <<"s4">>, S)),
       %% T = 2:
       ?assertNot(is_long_gone(2, <<"s1">>, S)),
       ?assert(is_long_gone(2, <<"s2">>, S)),
       ?assertNot(is_long_gone(2, <<"s3">>, S)),
       ?assertNot(is_long_gone(2, <<"s4">>, S)),
       %% T = 1:
       ?assertNot(is_long_gone(0, <<"s1">>, S)),
       ?assertNot(is_long_gone(0, <<"s2">>, S)),
       ?assertNot(is_long_gone(0, <<"s3">>, S)),
       ?assertNot(is_long_gone(0, <<"s4">>, S))
     end || S <- [S1, S2]]
  after
    classy_table:drop(?ptab),
    classy_table_tests:cleanup(Cleanup)
  end.

-endif.
