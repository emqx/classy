-module(prop_misc).

-export([prop_liveness_convert/0, prop_liveness_compare/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("classy.hrl").

prop_liveness_convert() ->
  ?FORALL(
     {NRes, Self, IsUp},
     {non_neg_integer(), boolean(), boolean()},
     ?TRAPEXIT(
        begin
          Liveness = classy_membership:to_liveness(NRes, Self, IsUp),
          Result = classy_membership:from_liveness(Liveness),
          ?WHENFAIL(io:format("NR=~p, S=~p, U=~p~nLiveness=~p result=~p~n",
                              [NRes, Self, IsUp, Liveness, Result]),
                    Result =:= {NRes, Self, IsUp})
        end)).

prop_liveness_compare() ->
  ?FORALL(
     {NRes1, NRes2, Self1, Self2, IsUp1, IsUp2},
     {non_neg_integer(), non_neg_integer(), boolean(), boolean(), boolean(), boolean()},
     ?TRAPEXIT(
        begin
          Liveness1 = classy_membership:to_liveness(NRes1, Self1, IsUp1),
          Liveness2 = classy_membership:to_liveness(NRes2, Self2, IsUp2),
          ?WHENFAIL(io:format("l1 = ~p l2 = ~p", [Liveness1, Liveness2]),
                    if NRes1 =:= NRes2, Self1 =:= Self2 ->
                        congruent(IsUp1, IsUp2, Liveness1, Liveness2);
                       NRes1 =:= NRes2 ->
                        %% Note: swapping here is intentional:
                        congruent(Self2, Self1, Liveness1, Liveness2);
                       true ->
                        congruent(NRes1, NRes2, Liveness1, Liveness2)
                    end)
        end)).

congruent(A1, A2, B1, B2) ->
  if A1 > A2   -> B1 > B2;
     A1 < A2   -> B1 < B2;
     A1 =:= A2 -> B1 =:= B2
  end.
