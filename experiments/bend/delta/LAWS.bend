# The delta algebra's laws (docs/key-index-format.md, K44). Stated by the
# human; PROOF.bend proves them.

import Base
import ./main.bend as M

# The empty delta merges to nothing, on either side.
law merge_nil_l:
  for -G: Data
  for +a: M.Delta<G>
  {M.merge(G, M.DNil{}, a) == a : M.Delta<G>}

law merge_nil_r:
  for -G: Data
  for +a: M.Delta<G>
  {M.merge(G, a, M.DNil{}) == a : M.Delta<G>}

# (a) Merging is associative: deltas (and range summaries) can be grouped
# any way.
law merge_assoc:
  for -G: Data
  for +a: M.Delta<G>
  for +b: M.Delta<G>
  for +c: M.Delta<G>
  {M.merge(G, M.merge(G, a, b), c) == M.merge(G, a, M.merge(G, b, c)) : M.Delta<G>}

# (b) Commits merged one by one, and the same commits merged as range
# summaries (each group summarised, then the summaries merged), give the
# same classes.
law grouped_classes:
  for -G: Data
  for gs: List<&2, List<&2, M.Delta<G>>>
  {M.classes(G, M.summary(G, M.concat(G, gs))) == M.classes(G, M.summary(G, M.summaries(G, gs))) : List<&2, M.Change>}

# (d) A delta that agrees with its state changes the live count by exactly
# added minus removed: after + removed == before + added.
law live_count:
  for -G: Data
  for +s: M.State<G>
  for +d: M.Delta<G>
  for h: M.Consistent(G, s, d)
  {Nat.add(M.size(G, M.apply(G, s, d)), M.removed(G, d)) == Nat.add(M.size(G, s), M.added(G, d)) : Nat}

# A key's entry in a merge is its newer entry over its older one.
law lookup_merge:
  for -G: Data
  for +k: Nat
  for +a: M.Delta<G>
  for +b: M.Delta<G>
  {M.lookup(G, k, M.merge(G, a, b)) == M.over(G, M.lookup(G, k, b), M.lookup(G, k, a)) : Maybe<&2, M.Info<G>>}

# (c) For commits P+1..N that each agree with the state before them, at
# every key: the range summary's entry was live before exactly when the key
# was live at P, and is live exactly when the key is live at N; a key
# without an entry has the same presence at P and N. p is the key's
# presence at P.
law range_agrees:
  for -G: Data
  for +cs: List<&2, M.Delta<G>>
  for +p: Bool
  for +k: Nat
  for h: M.Ok(G, cs, p, k)
  M.Agrees(G, p, M.pres(G, cs, p, k), M.lookup(G, k, M.summary(G, cs)))

# (c) As classes: the summary's class of each key it holds is the change of
# the key's presence from P to N (added, updated, removed, nothing).
law range_changes:
  for -G: Data
  for +cs: List<&2, M.Delta<G>>
  for +p: Bool
  for +k: Nat
  for h: M.Ok(G, cs, p, k)
  M.ClassAgrees(G, p, M.pres(G, cs, p, k), M.lookup(G, k, M.summary(G, cs)))

# The parallel balanced reduction of a range is its one-by-one summary,
# whatever the fuel.
law tree_summary:
  for -G: Data
  for +fuel: Nat
  for +ds: List<&2, M.Delta<G>>
  {M.tree(G, fuel, ds) == M.summary(G, ds) : M.Delta<G>}
