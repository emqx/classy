%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(classy_partition).
-moduledoc """
This module contains various algorithms for calculating network partitions.
""".

%% API:
-export([ bidi_link/3
        , full_meshes/1
        ]).

-export_type([partition/0]).

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type partition() :: ordsets:ordset({classy:site(), node()}).

%%================================================================================
%% API functions
%%================================================================================

-doc """
Return @code{@{ok, true@}} if nodes are mutually connected to each other,
or @code{@{ok, false@}} when either node considers the other disconnected.

If either node is absent in the @code{ClusterInfo},
then an error tuple is returned.
""".
-spec bidi_link(classy:cluster_info(), node(), node()) -> {ok, boolean()} | {error, _}.
bidi_link(ClusterInfo, NodeA, NodeB) ->
  case ClusterInfo of
    #{infos := #{NodeA := A, NodeB := B}} ->
      #{site := SiteA, peers := PeersA} = A,
      #{site := SiteB, peers := PeersB} = B,
      maybe
        #{SiteB := #{connected := true, node := NodeB}} ?= PeersA,
        #{SiteA := #{connected := true, node := NodeA}} ?= PeersB,
        {ok, true}
      else
        _ ->
          {ok, false}
      end;
    _ ->
       {error, insufficient_data}
  end.

-doc """
This greedy algorithm finds full meshes in the network.
Each site appears in exactly one full mesh.

Note: because of that property,
this function returns ambiguous results when network partitions are overlapping.
More specifically, it will be overly eager in detecting partitions,
and will ignore some existing links.
""".
-spec full_meshes(classy:cluster_info()) -> #{classy:cluster_id() => [partition()]}.
full_meshes(ClusterInfo) ->
  Res = classy_lib:fold_per_cluster(
          fun(Node, #{site := Site}, Meshes) ->
              add_to_full_mesh(ClusterInfo, Node, Site, Meshes)
          end,
          [],
          ClusterInfo),
  #{K => sort_partitions(V) || K := V <- Res}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec add_to_full_mesh(
        classy:cluster_info(),
        node(),
        classy:site(),
        [partition()]
       ) ->
        [partition()].
add_to_full_mesh(_ClusterInfo, Node, Site, []) ->
  [[{Site, Node}]];
add_to_full_mesh(ClusterInfo, Node, Site, [Mesh | OtherMeshes]) ->
  case is_in_full_mesh(ClusterInfo, Node, Mesh) of
    true ->
      [ [{Site, Node} | Mesh]
      | OtherMeshes
      ];
    false ->
      [ Mesh
      | add_to_full_mesh(ClusterInfo, Node, Site, OtherMeshes)
      ]
  end.

is_in_full_mesh(_ClusterInfo, _Node, []) ->
  true;
is_in_full_mesh(ClusterInfo, Node, [{_RSite, RNode} | Rest]) ->
  case bidi_link(ClusterInfo, Node, RNode) of
    {ok, true} ->
      is_in_full_mesh(ClusterInfo, Node, Rest);
    _ ->
      false
  end.

%% Sort partitions by size.
%% Break ties by comparing minimum site ID in each partition.
-spec sort_partitions([partition()]) -> [partition()].
sort_partitions(Partitions0) ->
  Partitions = [ordsets:from_list(I) || I <- Partitions0],
  L = [{ -length(I)
       , case I of
           [{MinSiteId, _} | _] -> MinSiteId;
           _                    -> undefined
         end
       , I
       } || I <- Partitions],
  [I || {_, _, I} <- lists:sort(L)].

-ifdef(TEST).

full_mesh_test() ->
  Cluster = <<"cluster1">>,
  N = 5,
  FullMesh = test_cluster_info_full_mesh(Cluster, N),
  %% No partitions:
  ?assertEqual(
     #{Cluster => [test_ids(1, N)]},
     full_meshes(FullMesh)),
  %% Bidi-partition: [1, 2, 3] [4, 5]
  BidiMeshes = test_disconnects(
                 [{K, L} || I <- [1, 2, 3],
                            J <- [4, 5],
                            {K, L} <- [{I, J}, {J, I}]],
                 FullMesh),
  ?assertEqual(
     #{Cluster => [test_ids(1, 3), test_ids(4, 5)]},
     full_meshes(BidiMeshes)),
  %% Uni-directional partition: [1, 2, 3] [4, 5]
  UnidiMeshes = test_disconnects(
                 [{I, J} || I <- [1, 2, 3],
                            J <- [4, 5]],
                 FullMesh),
  ?assertEqual(
     #{Cluster => [test_ids(1, 3), test_ids(4, 5)]},
     full_meshes(UnidiMeshes)),
  %% Overlapping partitions, 5 is disconnected from 1:
  Overlapping1 = test_disconnect(5, 1, FullMesh),
  ?assertEqual(
     #{Cluster => [test_ids(1, 4), test_ids(5, 5)]},
     full_meshes(Overlapping1)),
  %% Overlapping partitions, 4 and 5 are disconnected from 1.
  Overlapping2 = test_disconnects(
                   [{4, 1}, {5, 1}],
                   FullMesh),
  ?assertEqual(
     #{Cluster => [test_ids(1, 3), test_ids(4, 5)]},
     full_meshes(Overlapping2)),
  %% Overlapping partitions, 4 and 5 are disconnected from 1 and from each other:
  Overlapping3 = test_disconnects(
                   [{4, 1}, {5, 1}, {4, 5}, {5, 4}],
                   FullMesh),
  ?assertEqual(
     #{Cluster => [test_ids(1, 3), test_ids(4, 4), test_ids(5, 5)]},
     full_meshes(Overlapping3)).

test_cluster_info_full_mesh(Cluster, N) ->
  Peers = maps:from_list(
            [{Site, #{ node        => Node
                     , connected   => true
                     , last_update => 0
                     }} ||
              {Site, Node} <- test_ids(1, N)]),
  Infos = maps:from_list(
            [{ Node
             , #{ cluster     => Cluster
                , site        => Site
                , last_update => 0
                , peers       => maps:remove(Site, Peers)
                }
             } ||
              {Site, Node} <- test_ids(1, N)]),
  #{ infos     => Infos
   , bad_nodes => #{}
   }.

test_disconnects([], Mesh) ->
  Mesh;
test_disconnects([{Node, Site} | Rest], Mesh) ->
  test_disconnects(
    Rest,
    test_disconnect(Node, Site, Mesh)).

%% Disconnect site with id `B' from site with id `A'.
test_disconnect(A, B, ClusterInfo = #{infos := Infos}) ->
  {_, NodeA} = test_ids(A),
  {SiteB, _} = test_ids(B),
  #{NodeA := AInfo = #{peers := Peers0}} = Infos,
  #{SiteB := SiteBInfo} = Peers0,
  Peers = Peers0#{SiteB := SiteBInfo#{connected => false}},
  ClusterInfo#{infos := Infos#{NodeA := AInfo#{peers := Peers}}}.

test_ids(N) ->
  B = integer_to_binary(N),
  { <<"s", B/binary>>
  , binary_to_atom(<<"n", B/binary, "@h">>)
  }.

test_ids(N, M) ->
  [test_ids(I) || I <- lists:seq(N, M)].

-endif.
