%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(classy_vote).
-moduledoc """
This module implements a variation of 2-phase commit.

Important to note:

@enumerate
@item All callbacks involved in the operations are persistently stored.
  Certain callbacks may be retried after a node restart.
  Hence, user must make sure that functions involved in the commit are not removed during upgrade.

@item Vote is rather heavy operation.
  Do not use it when frequent coordination is needed.

@item Commit flows may hang for an unlimited time if the coordinator node fails
during the decision stage.
@end enumerate

@section Error Handling

This API uses both synchronous and asynchronous methods of status and error reporting.
Both methods must be handled in all cases.
Note: when @link{classy_vote:create/1,create/1} API returns @code{@{ok, _@}},
it doesn't mean the commit has been completed.

@enumerate
@item
If this function returns an error tuple,
it means the commit followed a "fast abort" path.
"Fast abort" path is synchronous,
and it implies that no persistent changes have been made to the involved sites
(participant and coordinator).

@item
When the function returns @code{@{ok, _@}},
it means commit flow entered "persistent" path.
Persistent path continues even after restart of any involved site.
Because of that, it uses asynchronous method of status reporting
via callbacks passed in the options.

The coordinator is notified of the outcome via @code{post_vote} callback.

The participants are notified via their respective @code{commit} or @code{rollback} callbacks.

@item
Additionally,
if @code{post_vote}, @code{commit} or @code{rollback} callbacks throw an exception,
@code{on_fail} callback gets involved.

Such mechanism is used because classy doesn't automatically retry any actions that failed on the persistent path:
there is a high risk that these actions will just keep failing repeatedly.
Instead, they are abandoned until the next node restart.
@code{on_fail} callback provides a mechanism to signal such failures back to the business logic.

@end enumerate
""".

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
        , prop_every_participant_receives_outcome/1
        ]).
-endif.

%%================================================================================
%% Type declarations
%%================================================================================


-doc """
Arbitrary tag associated with the operation.
It allows business logic to quickly enumerate ongoing votes of certain kind.
""".
-type tag() :: term().

-doc "Unique ID of the vote.".
-type id() :: classy_uid:cu_tuple().

-doc """
Strategy used to decide whether to commit.

@code{all}: All participant must vote yes within the timeout.
""".
-type strategy() :: {all, timeout()}.

-doc """
Per-participant set of commit actions.

@table @code
@item prepare
  Callback that lets the participant decide whether to commit.

  Classy prepends two additional values to the user-specified argument list:
  @enumerate
    @item A boolean indicating whether the prepare action can have side effects.
    It is set to @code{false} during pre-commit fast abort check
    and to @code{true} during the persistent flow.

    @item ID of the vote.
  @end enumerate

  The return value is a boolean indicating the participant's vote (@code{true} means ``yes'').

@item commit
  List of actions executed on the sites if the coordinator decides to go ahead with the commit.
  Classy prepends vote ID to the user-specified argument list.
  Return value is ignored.

@item rollback
  Action executed on the participant when the coordinator decides to abort the commit.
  Classy prepends vote ID to the user-specified argument list.
  Return value is ignored.
@end table
""".
-type actions() ::
        #{ prepare   := classy_lib:mfargs()
         , commit    := [classy_lib:mfargs()]
         , rollback  => classy_lib:mfargs()
         }.

-doc """
Common vote options.

@table @code
@item tag
  An arbitrary tag identifying the commit action.
  Ongoing commit actions can be efficiently filtered by the tag.

@item actions
  A map from @link{t:classy:site/0,site ID} to per-site @ref{t:classy_vote:actions/0, commit actions}.
  Each site in the map becomes a vote participant.
  Participants' actions may be non-uniform.

@item post_vote
  Callback that is executed on the coordinator after the decision is made.
  Classy prepends two arguments to the user-specified argument list:

  @enumerate
  @item A boolean indicating the decision
  @item Vote ID
  @end enumerate

  The return value is ignored.

  Note: it's NOT guaranteed that all commit actions on the participants are finished
  by the time when @code{post_vote} is called.
  This callback can be retried on node restart.

@item strategy
  @xref{t:classy_vote:strategy/0,strategy()}.

@item on_fail
  Executed on both coordinator and participant if commit / rollback / post_commit actions fail.
  This callback may be used signal failures to the business logic.
  Classy prepends an argument of type @ref{t:classy_vote:fail_info/0} to the user-specified argument list.
@end table
""".
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

-doc """
@xref{classy_vote:ls_votes/1}. No filtering.
""".
-spec ls_votes() -> [vote_info()].
ls_votes() ->
  ls_votes('_').

-doc """
List all ongoing 2PC flows where the local site is either a coordinator or a participant.

The argument is an ETS match expression that allows to filter on the tag.
""".
-spec ls_votes(_TagMatch) -> [vote_info()].
ls_votes(TagMatch) ->
  fold_ongoing(
    fun(VoteInfo, Acc) -> [VoteInfo | Acc] end,
    [],
    TagMatch).

-doc """
Fold over ongoing 2PC flows where the local site is either a coordinator or a participant.

Arguments:

@enumerate
@item Fold function
@item Initial accumulator
@item ETS match expression for filtering the tag
@end enumerate
""".
-spec fold_ongoing(fun((vote_info(), Acc) -> Acc), Acc, _TagMatchPattern) -> Acc.
fold_ongoing(Fun, Acc0, TagMatch) ->
  classy_vote_participant:fold_ongoing(
    Fun,
    classy_vote_coordinator:fold_ongoing(Fun, Acc0, TagMatch),
    TagMatch).

-doc """
Initiate a new vote, @pxref{t:classy_vote:options/0,options/0}.
""".
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
  , fun ?MODULE:prop_every_participant_receives_outcome/1
  ].

prop_every_vote_concludes(Trace) ->
  ?strict_causality(
     #{?snk_kind := ?classy_vote_flow_start, id := _Id},
     #{?snk_kind := K, id := _Id} when K =:= ?classy_vote_coord_flow_complete;
                                       K =:= ?classy_vote_coord_early_abort,
     Trace).

%% This property should always hold, unless the coordinator is removed
%% from the cluster and the participants auto-abort.
prop_every_participant_receives_outcome(Trace) ->
  ?strict_causality(
     #{?snk_kind := ?classy_vote_part_established, id := _Id, site := _Site},
     #{?snk_kind := ?classy_vote_part_recv_outcome, id := _Id, site := _Site},
     Trace).

-endif.
