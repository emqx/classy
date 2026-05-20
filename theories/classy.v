(** * CRDTs
 *)

Require Import
  List
  Orders
  Permutation
  Program.

Import ListNotations.

From Hammer Require Import Tactics.

(** ** Network Emulation

Let's define an auxiliary property that,
given two lists [l] and [l'],
states that [l'] contains all elements from [l] in the original order,
but with 0 or more repetitions.

It will be used to model delivery retries.
*)
Section repeat_delivery.
  Section defn.
    Context {T : Type}.

    Inductive RepeatDelivery_ : option T -> list T -> list T -> Prop :=
    | rep_delivery_nil :
      forall prev,
        RepeatDelivery_ prev [] []
    | rep_delivery_next :
      forall prev a l l',
        RepeatDelivery_ (Some a) l l' ->
        RepeatDelivery_ prev (a :: l) (a :: l')
    | rep_delivery_repeat :
      forall prev l l',
        RepeatDelivery_ (Some prev) l l' ->
        RepeatDelivery_ (Some prev) l (prev :: l').

    Definition RepeatDelivery l l' : Prop :=
      RepeatDelivery_ None l l'.
  End defn.

  Section tests.
    (** Allowed pairs of lists: *)
    Goal forall T, @RepeatDelivery T [] [].
      sauto.
    Qed.

    Goal RepeatDelivery [1; 2; 3] [1; 2; 3].
      sauto.
    Qed.

    Goal RepeatDelivery [1] [1; 1; 1].
      sauto.
    Qed.

    Goal RepeatDelivery [1; 2; 3; 4] [1; 1; 1; 2; 3; 3; 3; 4; 4].
      sauto.
    Qed.

    (** Losing deliveries is not allowed: *)
    Goal ~RepeatDelivery [1] [].
      sauto.
    Qed.

    Goal ~RepeatDelivery [1; 2; 3] [1; 1; 3; 3].
      sauto.
    Qed.

    (** Creating deliveries out of thin air is not allowed: *)
    Goal ~RepeatDelivery [] [1].
      sauto.
    Qed.

    Goal ~RepeatDelivery [1] [1; 2].
     sauto.
    Qed.

    Goal ~RepeatDelivery [1; 2; 3] [1; 1; 2; 2; 2; 4; 3; 3].
      sauto.
    Qed.

    (** Permutations are not allowed: *)
    Goal ~RepeatDelivery [1; 2] [2; 1].
      sauto.
    Qed.
  End tests.
End repeat_delivery.

(** Here [l''] is a network packet delivery order,
where eventual delivery of every packet from [l] is guaranteed,
but packets may be deliveried more than once and in random order.

It's defined in two steps: first we multiply the original packets,
then shuffle them.
*)
Definition eventual_delivery {T} (l l'' : list T) : Prop :=
  exists l',
    RepeatDelivery l l' /\ Permutation l' l''.

(** ** CRDT Definition and Properties *)
Section CRDT.
  Class CRDT (S OP : Set) :=
    { crdt_apply : OP -> S -> S;
      crdt_commut s a b : crdt_apply b (crdt_apply a s) = crdt_apply a (crdt_apply b s);
      crdt_idempotent s a : crdt_apply a s = crdt_apply a (crdt_apply a s);
    }.

  Section props.
    Context `{Hcrdt : CRDT}.

    Let apply_ops l s := fold_left (flip crdt_apply) l s.

    (** Let's prove that CRDTs tolerate repeats: *)
    Fixpoint crdt_dedup0 prev l l' s
      (H : RepeatDelivery_ (Some prev) l l') {struct H} :
      apply_ops l (crdt_apply prev s) = apply_ops l' (crdt_apply prev s).
    Proof.
      inversion H as [|? next l1 l1' Hl1'|? ? l1 Hl1]; subst; clear H.
      - sauto.
      - simpl. unfold flip.
        apply (crdt_dedup0 next l1 l1' (crdt_apply prev s) Hl1').
      - simpl. unfold flip.
        rewrite <-crdt_idempotent.
        apply (crdt_dedup0 prev l l1 s Hl1).
    Qed.

    Lemma crdt_dedup l l' s :
      RepeatDelivery l l' ->
      apply_ops l s = apply_ops l' s.
    Proof.
      intros H.
      destruct l as [|prev l].
      - sauto.
      - inversion_clear H.
        now apply crdt_dedup0.
    Qed.

    (** ..And permutations: *)
    Lemma crdt_permutation l l' s :
        Permutation l l' ->
        apply_ops l s = apply_ops l' s.
    Proof.
      intros Hperm.
      generalize dependent s.
      induction Hperm; sauto.
    Qed.

    (** Finally, let's prove that CRDT converges under eventual delivery rules: *)
    Theorem crdt_convergence l l'' s :
      eventual_delivery l l'' ->
      apply_ops l s = apply_ops l'' s.
    Proof.
      intros [l' [Hrep Hperm]].
      apply crdt_dedup with (s := s) in Hrep.
      apply crdt_permutation with (s := s) in Hperm.
      now rewrite Hrep, Hperm.
    Qed.
  End props.
End CRDT.

(** ** State-based CRDTs

In state-based CRDTs [S] parameter is also [OP],
and [crdt_apply] method is a merge function that combines the two states.
Therefore, this merge function should be idempotent and commutative.
*)

(** *** Ordering Relation

In our case, merge operation is defined via the total order of state update operations:
state with a higher order wins.

The below section describes a class of total orders that "works as
Erlang term comparison".

Formally, it expands stdlib's [StrictOrder] class to be decidable,
and so that [~ a<b /\ ~ b<a] implies [a = b].
*)
Section compares.
  Context `{StrictOrder}.

  Inductive compares a b : Set :=
  | comp_eq : a = b -> compares a b
  | comp_lt : R a b -> compares a b
  | comp_gt : R b a -> compares a b.
End compares.

Class StrictOrderDec `{Hord : StrictOrder} :=
  { compare_dec : forall a b, @compares _ R a b
  }.

Global Arguments StrictOrderDec (A _).

Infix "<?>" := compare_dec (at level 50).

(** *** Merge Function

Now let's prove some very general properties about convergence of classy's state-based CRDT.

Note: the following section reasons about state of a single site.
*)
Section merge.
  Context {Operation Ord : Set} `{Hdecord : StrictOrderDec Ord}.

  (** Eventual consistency is assured by commutativity, idempotence and associativity of state merge function.


      [ord] is some function that returns order of the operation: *)
  Context (ord : Operation -> Ord).

  (** When we merge two operations, one with the greater order wins: *)
  Definition merge0 a b :=
    match ord a <?> ord b with
    | comp_lt _ _ _ => b
    | _ => a
    end.

  (* begin details *)
  Lemma Rab_to_ord a b:
    R a b ->
    exists H, a <?> b = comp_lt a b H.
  Proof.
    intros H.
    destruct Hord as [Hirr Htran].
    destruct (a <?> b).
    - exfalso. subst. now apply Hirr in H.
    - now exists r.
    - exfalso. specialize (Htran _ _ _ H r) as H1. now apply Hirr in H1.
  Qed.

  Lemma Rba_to_ord a b:
    R b a ->
    exists H, a <?> b = comp_gt a b H.
  Proof.
    intros H.
    destruct Hord as [Hirr Htran].
    destruct (a <?> b).
    - exfalso. subst. now apply Hirr in H.
    - exfalso. specialize (Htran _ _ _ H r) as H1. now apply Hirr in H1.
    - now exists r.
  Qed.
  (* end details *)

  (** It's trivial to prove that this operation is idempotent. *)
  Lemma merge0_idemp a : merge0 a a = a.
  Proof.
    sauto unfold:merge0.
  Qed.

  (** Things get more complicated when it comes to merging two or more operations.

      Notice that in general different operations may have the same order.
      This is bad news, because it can break many properties of the merge operation (associativity, idempotency and commutativity).
      In practical terms,
      it makes the result of merge dependent on the order of network interations between the peers.

      It's also not practical to demand [ord] function to be bijective.
      Instead, we require that any two different operations have different [ord] *in the context of the merge operation*:
   *)
  Definition proper_pair a b :=
    ord a = ord b ->
    a = b.
  (** ...Going back to the Erlang code,
      let's informally prove that this weaker property ([proper_pair]) is satisfied for every pair of operations created by the running system.

      By definition, [ord] is a triple of [{Clock, Magic, Origin}] fields from the original record.
      Observe that pair of values [Origin] and [Clock] is always unique:
      every time [Origin] site issues an operation about any other site,
      it increments the clock.
   *)

  (* begin details *)
  Lemma merge0_lt a b :
    proper_pair a b ->
    R (ord a) (ord b) ->
    merge0 a b = b.
  Proof.
    intros Hp H.
    apply Rab_to_ord in H.
    destruct H as [H Hl].
    unfold merge0. now rewrite Hl.
  Qed.

  Lemma merge0_gt a b :
    proper_pair a b ->
    R (ord b) (ord a) ->
    merge0 a b = a.
  Proof.
    intros Hp H.
    apply Rba_to_ord in H.
    destruct H as [H Hl].
    unfold merge0. now rewrite Hl.
  Qed.
  (* end details *)

  (** For every proper pair of operations [merge0] is commutative: *)
  Lemma merge0_commut a b :
    proper_pair a b ->
    merge0 a b = merge0 b a.
  Proof.
    intros Hproper.
    unfold merge0.
    destruct Hord as [Hirr Htran].
    destruct (ord a <?> ord b) as [ab|ab|ab];
    destruct (ord b <?> ord a) as [ba|ba|ba];
      subst; try apply Hproper; try easy;
      specialize (Htran _ _ _ ab ba) as Hcontradiction;
      now apply Hirr in Hcontradiction.
  Qed.

  (* begin details *)
  Ltac rev_eq :=
    let H1 := fresh in
    let H2 := fresh in
    progress match goal with
    | [ H1 : ord ?a = ord ?b, H2 : proper_pair ?a ?b |- _ ] =>
        apply H2 in H1;
        subst;
        repeat rewrite merge0_idemp
    end.

  Ltac rev_lt :=
    let H1 := fresh in
    let H2 := fresh in
    progress match goal with
    | [ H1 : R (ord ?a) (ord ?b), H2 : proper_pair ?a ?b |- _ ] =>
        rewrite (merge0_lt a b H2 H1)
    end.

  Ltac rev_gt :=
    let H1 := fresh in
    let H2 := fresh in
    progress match goal with
    | [ H1 : R (ord ?b) (ord ?a), H2 : proper_pair ?a ?b |- _ ] =>
        rewrite (merge0_gt a b H2 H1)
    end.
  (* end details *)

  (** For any mutually proper pair of operations [merge0] is associative: *)
  Lemma merge0_assoc a b c :
    proper_pair a b ->
    proper_pair b c ->
    proper_pair a c ->
    merge0 (merge0 a b) c = merge0 a (merge0 b c).
  Proof with repeat (rev_eq || rev_lt || rev_gt); try rewrite merge0_idemp; try easy.
    intros Hproper_ab Hproper_bc Hproper_ac.
    unfold max.
    destruct (ord a <?> ord b) as [ab|ab|ab].
    - rev_eq.
      destruct (ord b <?> ord c) as [bc|bc|bc]...
    - rev_lt.
      destruct (ord b <?> ord c) as [bc|bc|bc]...
      + rewrite (merge0_lt a c); sauto.
    - rev_gt.
      destruct (ord b <?> ord c) as [bc|bc|bc]...
      + rewrite (merge0_gt a c); sauto.
  Qed.

  (** Now let's expand our definition of merge to the case where previous operation is unknown:
   *)
  Definition Last := option Operation.

  Definition merge (a b : Last) : Last :=
    match a, b with
    | None, None => None
    | Some a, None => Some a
    | None, Some b => Some b
    | Some a, Some b => Some (merge0 a b)
    end.

  Definition proper_pair_l a b :=
    match a, b with
    | Some a, Some b => proper_pair a b
    | _, _ => True
    end.

  (** This expanded definition is also commutative: *)
  Lemma merge_commut a b :
    proper_pair_l a b ->
    merge a b = merge b a.
  Proof.
    sauto unfold: merge, proper_pair_l use:merge0_commut.
  Qed.

  (** ...idempotent: *)
  Lemma merge_idemp a :
    merge a a = a.
  Proof.
    sauto unfold: merge use:merge0_idemp.
  Qed.

  (** ...and associative: *)
  Lemma merge_assoc a b c :
    proper_pair_l a b ->
    proper_pair_l b c ->
    proper_pair_l a c ->
    merge (merge a b) c = merge a (merge b c).
  Proof.
    sauto unfold: merge, proper_pair_l use:merge0_assoc.
  Qed.

  Definition merge_l (l : list Operation) : Last :=
    let f acc elem := merge acc (Some elem) in
    fold_left f l None.

  Definition proper_log (l : list Operation) : Prop := ForallPairs proper_pair l.
End merge.
