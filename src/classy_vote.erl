%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements a variation of 2-phase commit.
%%
%% Note: vote is rather heavy operation.
%% Do not use it when frequent coordination is needed.
%%
%% Each voting action uses the following following callbacks:
%%
%% <itemize>
%% <li>
%% <b>prepare</b>: Executed on the participant sites.
%% Classy prepends an additional argument indicating whether the prepare action can have side effects.
%% Return value is a boolean indicating the participant's vote (`true' = `yes').
%% </li>
%%
%% <li>
%% <b>commit</b>: List of actions executed on the participant sites
%% when the coordinator decides to go ahead with the commit.
%% Classy doesn't add additional arguments.
%% Return value is ignored.
%%
%% Un-executed or failed actions are retried after node restart.
%% </li>
%%
%% <li>
%% <b>rollback</b>: Executed on the participant sites
%% when the coordinator decides to abort the commit.
%% Classy doesn't add additional arguments.
%% Return value is ignored.
%% This callback can be retried on node restart.
%% </li>
%%
%% <li>
%% <b>post_vote</b>: Executed on the coordinator.
%% Classy prepends a boolean indicating result of the vote to the argument list.
%% Return value is ignored.
%% It's NOT guaranteed that all commit actions on the participants are finished by this time.
%% This callback can be retried on node restart.
%% </li>
%%
%% <li>
%% <b>on_fail</b>: Executed on both coordinator and participant if commit / rollback / post_commit actions fail.
%% This callback may be used signal failures to the business logic.
%% Classy prepends an argument of type `fail_info'.
%% </li>
%% </itemize>
%%
%% WARNING: when `commit', `rollback' or `post_vote' actions fail,
%% coordinator or participant processes stop, but pending commit action will linger in the DB.
%% Classy will <b>not</b> attempt to recover the action until the next node restart.
%% Recovery of failed commit or rollback actions is left to the API consumers.
-module(classy_vote).

%% API:
-export([ create/1
        , ls_votes/0
        , ls_votes/1
        , fold_ongoing/3
        ]).

%% internal exports:
-export([ create_table/0
        , verify_prepare/1
        , verify_commit/1
        , verify_rollback/1
        , verify_mfas/3
        , verify_mfa/3
        , retry_interval/0
        , on_fail/2
        ]).

-export_type([ id/0
             , tag/0
             , strategy/0
             , actions/0
             , options/0
             , vote/0
             , outcome/0
             , fail_info/0
             , vote_info/0
             ]).

-include("classy.hrl").
-include("classy_vote.hrl").

-ifdef(TEST).
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("eunit/include/eunit.hrl").
-export([ test_wait_conclude/1
        , trace_props/0
        , prop_every_vote_concludes/1
        , prop_coord_receives_votes/1
        ]).
-endif.

%%================================================================================
%% Type declarations
%%================================================================================


%% Lock tag associated with the operation.
%% It allows business logic to quickly enumerate ongoing votes of certain kind.
-type tag() :: term().
%% Unique ID of the vote.
-type id() :: classy_uid:cu_tuple().

%% Strategy used to decide when to commit
%%
%% `all': All participant must vote `true' within the timeout
-type strategy() :: {all, timeout()}.

-type actions() ::
        #{ prepare   := classy_lib:mfargs()
         , commit    := [classy_lib:mfargs()]
         , rollback  => classy_lib:mfargs()
         }.

%% Warning: MFA's are persistently stored!
-type options() ::
        #{ tag       := tag()
         , actions   := #{classy:site() => actions()}
         , post_vote => classy_lib:mfargs()
         , strategy  => strategy()
         , on_fail   => classy_lib:mfargs()
         }.

-type vote() :: #c_vote{}.
-type outcome() :: #c_outcome{}.

-type fail_info() ::
        #{ tag    := tag()
         , id     := id()
         , reason := _
         , _      => _
         }.

-type vote_info() ::
        #{ tag  := tag()
         , id   := id()
         , role := coordinator | participant
         , _    => _
         }.

%%================================================================================
%% API functions
%%================================================================================

%% @doc List all ongoing commit actions.
-spec ls_votes() -> [vote_info()].
ls_votes() ->
  ls_votes('_').

%% @doc List ongoing commit actions.
%%
%% Argument: match specification for the tag.
-spec ls_votes(_TagMatch) -> [vote_info()].
ls_votes(TagMatch) ->
  fold_ongoing(
    fun(VoteInfo, Acc) -> [VoteInfo | Acc] end,
    [],
    TagMatch).

%% @doc Fold over ongoing commit actions.
%%
%% Argument: match specification for the tag.
-spec fold_ongoing(fun((vote_info(), Acc) -> Acc), Acc, _TagMatchPattern) -> Acc.
fold_ongoing(Fun, Acc0, TagMatch) ->
  classy_vote_participant:fold_ongoing(
    Fun,
    classy_vote_coordinator:fold_ongoing(Fun, Acc0, TagMatch),
    TagMatch).

%% @doc Initiate a new vote.
%%
%% Note: This function returns immediately.
-spec create(options()) -> {ok, classy_vote:id()} | {error, _}.
create(UserOptions) ->
  maybe
    %% Create a new vote:
    ID = classy_uid:cluster_unique_seq_tuple(classy_vote_sequence),
    {ok, Options} ?= with_defaults(UserOptions),
    {ok, _} ?= classy_vote_coordinator:new(ID, Options),
    {ok, ID}
  end.

%%================================================================================
%% Internal exports
%%================================================================================

-doc false.
-spec create_table() -> ok.
create_table() ->
  classy_table:open(
    ?ptab,
    #{ ets_options => [ordered_set, {read_concurrency, true}]
     }).

-doc false.
verify_prepare(Prepare) ->
  verify_mfa(bad_prepare, 2, Prepare).

-doc false.
verify_commit(Commit) ->
  verify_mfas(bad_commit, 1, Commit).

-doc false.
verify_rollback(Rollback) ->
  verify_mfas(bad_rollback, 1, Rollback).

-doc false.
verify_mfas(Reason, NExtraArgs, L) when is_list(L) ->
  try
    [case verify_mfa(Reason, NExtraArgs, I) of
       ok ->
         ok;
       Err ->
         throw(Err)
     end || I <- L],
    ok
  catch
    Err -> Err
  end;
verify_mfas(Reason, _, Other) ->
  {error, {Reason, Other}}.

-doc false.
-spec verify_mfa(atom(), non_neg_integer(), term()) -> ok | {error, {atom(), term()}}.
verify_mfa(Subject, NExtraArgs, {M, F, Args}) when is_atom(M),
                                                   is_atom(F),
                                                   is_list(Args) ->
  NArgs = length(Args) + NExtraArgs,
  Err = {error, {Subject, {M, F, NArgs}}},
  case erlang:function_exported(M, F, NArgs) of
    true  -> ok;
    false -> Err
  end;
verify_mfa(Subject, _, Other) ->
  {error, {Subject, Other}}.

-doc false.
retry_interval() ->
  application:get_env(classy, vote_retry_interval, 5_000).

-doc false.
-spec on_fail(fail_info(), [classy_lib:mfargs()]) -> ok.
on_fail(FailInfo, Funs) ->
  lists:foreach(
    fun({M, F, Args}) ->
        _ = classy_lib:safe_apply(M, F, [FailInfo | Args])
    end,
    Funs).

%%================================================================================
%% Internal functions
%%================================================================================

%%--------------------------------------------------------------------------------
%% Input validation
%%--------------------------------------------------------------------------------

-spec with_defaults(options()) -> {ok, options()} | {error, _}.
with_defaults(UserOpts) when is_map(UserOpts) ->
  Defaults = #{ strategy  => {all, classy_lib:rpc_timeout()}
              },
  Merged = maps:merge(Defaults, UserOpts),
  case Merged of
    #{ tag       := _
     , actions   := Actions0
     , strategy  := Strategy0
     } ->
      maybe
        {ok, Actions} ?= verify_actions(Actions0),
        {ok, Strategy} ?= verify_strategy(Strategy0),
        {ok, PostVote} ?= verify_post_vote(Merged),
        {ok, OnFail} ?= verify_on_fail(Merged),
        {ok, Merged#{ actions   := Actions
                    , strategy  := Strategy
                    , post_vote => PostVote
                    , on_fail   => OnFail
                    }}
      end;
    _ ->
      {error, badarg}
  end.

verify_post_vote(#{post_vote := PostVote}) ->
  maybe
    ok = verify_mfa(bad_post_vote, 2, PostVote),
    {ok, [PostVote]}
  end;
verify_post_vote(#{}) ->
  {ok, []}.

verify_on_fail(#{on_fail := OnFail}) ->
  maybe
    ok = verify_mfa(bad_on_fail, 1, OnFail),
    {ok, [OnFail]}
  end;
verify_on_fail(#{}) ->
  {ok, []}.

verify_strategy(all) ->
  {ok, {all, classy_lib:rpc_timeout()}};
verify_strategy({all, Timeout}) when is_integer(Timeout), Timeout > 0 ->
  {ok, {all, Timeout}};
verify_strategy(Strategy) ->
  {error, {bad_strategy, Strategy}}.

verify_actions(Actions0) when is_map(Actions0) ->
  try
    maps:size(Actions0) =:= 0 andalso throw({error, no_actions}),
    Actions = maps:map(fun enrich_action/2, Actions0),
    {ok, Actions}
  catch
    Err -> Err
  end;
verify_actions(Bad) ->
  {error, {bad_actions, Bad}}.

enrich_action(Site, SiteActions) when is_binary(Site), is_map(SiteActions) ->
  ActionDefaults = #{rollback => []},
  case maps:merge(ActionDefaults, SiteActions) of
    #{prepare := Prep, commit := Commit, rollback := Rollback} = Result ->
      maybe
        ok ?= verify_prepare(Prep),
        ok ?= verify_commit(Commit),
        ok ?= verify_rollback(Rollback),
        Result
      else
        Err -> throw(Err)
      end;
    _ ->
      throw({error, {bad_action, Site, SiteActions}})
  end;
enrich_action(BadSite, BadAction) ->
  throw({error, {bad_action, BadSite, BadAction}}).

-ifdef(TEST).

-spec test_wait_conclude(classy_vote:id()) -> boolean().
test_wait_conclude(ID) ->
  %% 1. Wait coordinator:
  {ok, CoordEvt} = ?block_until(#{?snk_kind := K, id := ID} when K =:= ?classy_vote_coord_early_abort;
                                                                 K =:= ?classy_vote_coord_flow_complete),
  case CoordEvt of
    #{?snk_kind := ?classy_vote_coord_early_abort} ->
      false;
    #{?snk_kind := ?classy_vote_coord_flow_complete, outcome := Outcome} ->
      %% Receive all vote initiation events from the participants
      %% (assuming the number of participants is < 100):
      {ok, SubRef} = snabbkaffe:subscribe(
                       ?match_event(
                          #{ ?snk_kind := ?classy_vote_part_established
                           , id := ID
                           }),
                       100,
                       0,
                       infinity),
      {timeout, PartVoteInitEvents} = snabbkaffe:receive_events(SubRef),
      %% Wait for conclusion:
      lists:foreach(
        fun(#{?snk_meta := #{node := Node}}) ->
            ?block_until(#{ ?snk_kind := ?classy_vote_part_flow_complete
                          , id := ID
                          , ?snk_meta := #{node := Node}
                          })
        end,
        PartVoteInitEvents),
      Outcome
  end.

trace_props() ->
  [ fun ?MODULE:prop_every_vote_concludes/1
  , fun ?MODULE:prop_coord_receives_votes/1
  ].

prop_every_vote_concludes(Trace) ->
  ?strict_causality(
     #{?snk_kind := ?classy_vote_flow_start, id := _Id},
     #{?snk_kind := K, id := _Id} when K =:= ?classy_vote_coord_flow_complete;
                                       K =:= ?classy_vote_coord_early_abort,
     Trace).

prop_coord_receives_votes(Trace) ->
  ?strict_causality(
     #{?snk_kind := ?classy_vote_part_send_vote, id := _Id, vote := _Vote, from := _From, ?snk_span := start},
     #{?snk_kind := ?classy_vote_coord_recv, id := _Id, vote := _Vote, from := _From},
     Trace).

-endif.
