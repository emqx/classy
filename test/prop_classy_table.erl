%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(prop_classy_table).

-export([prop_table/0]).
-export([initial_state/0, command/1, next_state/3, precondition/2, postcondition/3]).

-include_lib("kernel/include/logger.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("classy.hrl").

prop_table() ->
  ?FORALL(Cmds, commands(?MODULE),
          ?TRAPEXIT(
             begin
               Cleanup = classy_table_tests:setup(?FUNCTION_NAME),
               {History, State, Result} = run_commands(?MODULE, Cmds),
               classy_table_tests:cleanup(Cleanup),
               ?WHENFAIL(io:format("History: ~w~nState: ~w\nResult: ~w~n",
                                   [History, State, Result]),
                         Result =:= ok)
             end)).

key() ->
  oneof([foo, bar, 1, {1}]).

value() ->
  oneof([number(), binary()]).

atomically() ->
  list(oneof([ {w, key(), value()}
             , {d, key()}
             ])).

-type s() :: #{open := boolean(), data := #{}, tab := classy_table:tab()}.

initial_state() ->
  #{ open => false
   , data => #{}
   , tab => classy_prop_test_tab
   }.

tab_opts() ->
  #{ ets_options => [ordered_set]
   }.

command(#{open := false, tab := Tab}) ->
  oneof([ {call, classy_table, open, [Tab, tab_opts()]}
        ]);
command(#{open := true, tab := Tab}) ->
  oneof([ {call, classy_table, stop, [Tab, infinity]}
        , {call, classy_table, open, [Tab, tab_opts()]}
        , {call, classy_table, flush, [Tab]}
        , {call, classy_table, drop, [Tab]}
          %% Writes
        , {call, classy_table, write, [Tab, key(), value()]}
        , {call, classy_table, dirty_write, [Tab, key(), value()]}
          %% Deletes
        , {call, classy_table, delete, [Tab, key()]}
        , {call, classy_table, dirty_delete, [Tab, key()]}
          %% Atomic batches
        , {call, classy_table, atomically, [Tab, atomically()]}
        ]).

next_state(S, _V, {call, _, open, _}) ->
  S#{open := true};
next_state(S, _V, {call, _, stop, _}) ->
  S#{open := false};
next_state(S, _V, {call, _, flush, _}) ->
  S;
next_state(S, _V, {call, _, drop, _}) ->
  S#{open := false, data := #{}};
next_state(S, _V, {call, _, Write, [_, Key, Val]}) when Write =:= write;
                                                        Write =:= dirty_write ->
  #{data := D} = S,
  S#{data := D#{Key => Val}};
next_state(S, _V, {call, _, Write, [_, Key]}) when Write =:= delete;
                                                   Write =:= dirty_delete ->
  #{data := D} = S,
  S#{data := maps:remove(Key, D)};
next_state(S, _V, {call, _, atomically, [_, Batch]}) ->
  #{data := D0} = S,
  D = lists:foldl(
        fun({w, K, V}, D1) ->
            D1#{K => V};
           ({d, K}, D1) ->
            maps:remove(K, D1)
        end,
        D0,
        Batch),
  S#{data := D}.

precondition(_, _) ->
  true.

postcondition(S0, Call, Result) ->
  S = next_state(S0, Result, Call),
  check_result(S, Call, Result) and check_content(S).

check_content(#{open := false}) ->
  true;
check_content(#{open := true, tab := Tab, data := Data}) ->
  {Missing, Success} =
    ets:foldl(
      fun(#classy_kv{k = K, v = V}, {D0, Acc}) ->
          case maps:take(K, D0) of
            error ->
              ?LOG_ERROR(#{ unexpected_key => K
                          , unexpected_value => V
                          }),
              {D0, false};
            {Expected, D} ->
              if V =:= Expected ->
                  {D, Acc};
                 true ->
                  ?LOG_ERROR(#{ unexpected_value => V
                              , key => K
                              , expected_value => Expected
                              }),
                  {D, false}
              end
          end
      end,
      {Data, true},
      Tab),
  if map_size(Missing) =:= 0 ->
      Success;
     true ->
      ?LOG_ERROR("Table is missing elements: ~p", [Missing]),
      false
  end.

check_result(_, {call, _, open, _}, ok) ->
  true;
check_result(_, {call, _, stop, _}, ok) ->
  true;
check_result(_, {call, _, flush, _}, ok) ->
  true;
check_result(_, {call, _, drop, _}, ok) ->
  true;
check_result(_, {call, _, write, _}, ok) ->
  true;
check_result(_, {call, _, dirty_write, _}, ok) ->
  true;
check_result(_, {call, _, delete, _}, ok) ->
  true;
check_result(_, {call, _, dirty_delete, _}, ok) ->
  true;
check_result(_, {call, _, atomically, _}, {ok, []}) ->
  true;
check_result(S, Call, Result) ->
  ?LOG_ERROR(#{ call => Call
              , result => Result
              , state => S
              }),
  false.
