%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(classy_vote_participant).


-behavior(gen_statem).

%% API:
-export([]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([ participant_pre_vote/1
        , start_participant/1
        ]).

-export_type([]).

-include("classy_vote.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%% Participant's persistent state key:
-record(pk_p,
        { tag :: tag()
        , id :: id() | '_'
        }).

-record(init_participant,
        { name :: #pk_p{}
        , opts :: #prepare{}
        }).

-record(ps_p_vote_yes,
        { reserved = []
        }).
-record(ps_p_rollback,
        { vote :: boolean()
        , reserved = []
        }).
-record(ps_p_comm,
        { completed_actions :: non_neg_integer()
        , reserved = []
        }).

-type participant_state() :: #ps_p_vote_yes{} | #ps_p_rollback{} | #ps_p_comm{}.

%%  Participant:
-record(d_part, {}).
-type d_part() :: #d_part{}.

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal exports
%%================================================================================

%% @private Coordinator -> Participant
-spec participant_pre_vote(#prepare{}) -> {ok, boolean()} | {error, _}.
participant_pre_vote(Prepare) ->
  do_prepare(Prepare, false).

%%================================================================================
%% Internal functions
%%================================================================================

-spec do_prepare(#prepare{}, boolean()) -> {ok, boolean()} | {error, _}.
do_prepare(
  #prepare{ prepare     = Prep
          , commit      = Commit
          , rollback    = Rollback
          , coordinator = Coordinator
          },
  ForReal
 ) ->
  maybe
    ok ?= classy_vote:verify_prepare(Prep),
    ok ?= classy_vote:verify_commit(Commit),
    ok ?= classy_vote:verify_rollback(Rollback),
    ok ?= verify_coordinator(Coordinator),
    {M, F, Args} = Prep,
    Vote = apply(M, F, [ForReal | Args]),
    true ?= is_boolean(Vote) orelse {error, bad_result},
    {ok, Vote}
  end.


-spec send_vote(classy:site(), id(), boolean()) -> ok | {error, _}.
send_vote(Coordinator, Id, Vote) ->
  maybe
    {ok, Node} ?= classy_node:node_of_site(Coordinator, true),
    {ok, From} ?= classy_node:the_site(),
    Rec = #c_vote{ id = Id
                 , from = From
                 , vote = Vote
                 },
    erpc:call(Node, ?MODULE, receive_vote, [Rec])
  end.

verify_coordinator(Coordinator) ->
  case classy_node:node_of_site(Coordinator, true) of
    {ok, _} ->
      ok;
    _ ->
      {errror, coordinator_unreachable}
  end.
