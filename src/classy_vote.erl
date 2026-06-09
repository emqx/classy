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
%% Classy prepends an additional arugment indicating whether the prepare action can have side effects.
%% Return value is a boolean indicating the participant's vote (`true' = `yes').
%% </li>
%%
%% <li>
%% <b>commit</b>: List of actions executed on the participant sites
%% when the coordinator decides to go ahead with the commit.
%% Classy doesn't add additional arguments.
%% Return value is ignored.
%%
%% Un-executed actions are retried in case of node restart.
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
%% </itemize>
-module(classy_vote).

%% API:
-export([ create/1
        ]).

%% internal exports:
-export([ create_table/0
        , verify_prepare/1
        , verify_commit/1
        , verify_rollback/1
        , verify_mfas/2
        , verify_mfa/3
        , retry_interval/0
        ]).

-export_type([id/0, tag/0, lock/0, mfargs/0, strategy/0, actions/0, options/0, vote/0, outcome/0]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("classy.hrl").
-include("classy_vote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type mfargs() :: {module(), atom(), list()}.
%% Lock tag associated with the operation.
%% It allows business logic to quickly enumerate ongoing votes of certain kind.
-type tag() :: term().
%% Unique ID of the vote.
-type id() :: classy_uid:cu_tuple().
%% Arbitrary lock information that business logic can use to detect conflicts.
-type lock() :: term().

%% Strategy used to decide when to commit
%%
%% `all': All participant must vote `true' within the timeout
-type strategy() :: {all, timeout()}.

-type actions() ::
        #{ prepare   := mfargs()
         , commit    := [mfargs()]
         , rollback  => mfargs()
         }.

-record(act,
        { site_bit :: non_neg_integer()
        , prepare  :: mfargs()
        , commit   :: [mfargs()]
        , rollback :: [mfargs()]
        , reserved :: []
        }).

%% Warning: MFA's are persistently stored!
-type options() ::
        #{ tag       := tag()
         , actions   := #{classy:site() => actions()}
         , post_vote => mfargs()
         , strategy  => strategy()
         , lock      => lock()
         }.

-type vote() :: #c_vote{}.
-type outcome() :: #c_outcome{}.

%%================================================================================
%% API functions
%%================================================================================

%% %% @doc List ongoing commit actions.
%% %%
%% %% Argument: match specification for the tag.
%% -spec ls_votes(_TagMatch) -> #{id() => vote_info()}.
%% ls_votes(TagMatch) ->
%%   fold_votes(
%%     TagMatch,
%%     fun(ID, Action, Acc) -> Acc#{ID => Action} end).

%% @doc Fold over ongoing commit actions.
%%
%% Argument: match specification for the tag.
%% -spec fold_votes(_TagMatch, fun((id(), vote_info(), Acc) -> Acc)) -> Acc.
%% fold_votes(TagMatch, Fun) ->
%%   BatchSize = 100,
%%   MS = { #classy_kv{k = #pk_p{tag = TagMatch, _ = '_'}, _ = '_'}
%%        , []
%%        , ['$_']
%%        },
%%   do_fold_votes(ets:select(?ptab, [MS], BatchSize), Fun, #{}).

%% @doc Initiate a new vote.
%%
%% Note: This function returns immediately.
-spec create(options()) -> ok | {error, _}.
create(UserOptions) ->
  maybe
    {ok, Options} ?= with_defaults(UserOptions),
    {ok, _} ?= classy_vote_coordinator:new(Options),
    ok
  end.

%%================================================================================
%% Internal exports
%%================================================================================

%% @private
-spec create_table() -> ok.
create_table() ->
  classy_table:open(
    ?ptab,
    #{ ets_options => [ordered_set, {read_concurrency, true}]
     }).

verify_prepare(Prepare) ->
  verify_mfa(bad_prepare, 0, Prepare).

verify_commit(Commit) ->
  verify_mfas(bad_commit, Commit).

verify_rollback(Rollback) ->
  verify_mfas(bad_rollback, Rollback).

verify_mfas(Reason, Commits) when is_list(Commits) ->
  try
    [case verify_mfa(Reason, 0, I) of
       ok ->
         ok;
       Err ->
         throw(Err)
     end || I <- Commits],
    ok
  catch
    Err -> Err
  end;
verify_mfas(Reason, Other) ->
  {error, {Reason, Other}}.

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

retry_interval() ->
  application:get_env(classy, vote_retry_interval, 5_000).

%%================================================================================
%% Internal functions
%%================================================================================

%%--------------------------------------------------------------------------------
%% Database access
%%--------------------------------------------------------------------------------

%% -spec db_set_options(true, id(), options()) -> ok;
%%                     (false, id(), #prepare{}) -> ok.
%% db_set_options(IsCoordinator, VoteId, Options) ->
%%   case IsCoordinator of
%%     true ->
%%       #{tag := Tag, lock := Lock} = Options;
%%     false ->
%%       #prepare{tag = Tag, lock = Lock} = Options
%%   end,
%%   classy_table:write(
%%     ?ptab,
%%     #pk{is_coordinator = IsCoordinator, tag = Tag, id = VoteId},
%%     #ps{lock = Lock, options = Options}).

%% -spec db_get_options(boolean(), tag(), id()) -> {ok, options()} | undefined.
%% db_get_options(IsCoordinator, Tag, VoteId) ->
%%   case classy_table:lookup(
%%          ?ptab,
%%          #pk{is_coordinator = IsCoordinator, tag = Tag, id = VoteId}) of
%%     [#ps{options = Options}] ->
%%       {ok, Options};
%%     [] ->
%%       %% Note this could race with table restoration
%%       undefined
%%   end.

%%--------------------------------------------------------------------------------
%% Input validation
%%--------------------------------------------------------------------------------

-spec with_defaults(options()) -> {ok, options()} | {error, _}.
with_defaults(UserOpts) when is_map(UserOpts) ->
  Defaults = #{ post_vote => {?MODULE, do_nothing, []}
              , strategy  => {all, classy_lib:rpc_timeout()}
              , lock      => []
              },
  Merged = maps:merge(Defaults, UserOpts),
  case Merged of
    #{ tag       := _
     , post_vote := PostVote
     , actions   := Actions0
     , strategy  := Strategy0
     , lock      := _
     } ->
      maybe
        ok ?= verify_post_vote(PostVote),
        {ok, Actions} ?= verify_actions(Actions0),
        {ok, Strategy} ?= verify_strategy(Strategy0),
        {ok, Merged#{ actions  := Actions
                    , strategy := Strategy
                    }}
      end;
    _ ->
      {error, badarg}
  end.

verify_post_vote(undefined) ->
  ok;
verify_post_vote(MFA) ->
  verify_mfa(bad_post_vote, 1, MFA).

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
