# Lean proofs for the key index's span design

Four open questions from `docs/key-index-design.md` (the span design,
thr_xvgqnrw2kr), settled in Lean 4.34 with no Mathlib. Every theorem below
checks with only Lean's standard axioms (`propext`, `Classical.choice`,
`Quot.sound`) and no `sorry`. `lake build` in this directory rebuilds all
of it in about 5 seconds.

| # | Question | Answer | File |
|---|---|---|---|
| 1 | Does the balance guard bound writes when merged spans share keys, or drop superseded versions? | Yes: at most `(1 + 43R + 20R log₂ K)` entries written per committed entry (R attempts per merge), for any adversarial order | `KeyIndex/WriteBound.lean` |
| 2 | Do merges that respect boundaries keep every catch-up range exactly tiled? | Yes, and a catch-up's spans merge to the per-commit fold | `KeyIndex/Tiling.lean` |
| 3 | Is the read-ahead rule right with increasing generations? | Yes; and dropping added-then-removed entries breaks it (counterexample) | `KeyIndex/Keys.lean` |
| 4 | Does a newest-first lookup over spans equal the fold at the head? | Yes | `KeyIndex/Keys.lean` |

`KeyIndex/Delta.lean` ties the per-key model to the actual sorted-list
merge. The compiled model matched the native `stream::Merge` on 2,000
random histories.

## 1. The write bound with dedup

**The worry.** The design's doubling argument says a merge under the guard
(its largest input holds at most 4× the others) costs at most 5× its
smaller inputs, and every entry charged sits in a span that at least
doubles. That holds when spans have disjoint keys. With shared keys, a
merge's output can stay near its largest input's size, so nothing doubles.
In the revised policy ("versions", `bench/keys/spans.py` at 9e8183d), a
merge can even end up smaller than its largest input. When an endpoint
retires, coalescing drops the superseded versions: a span holding the same
N keys in two segments becomes N.

**Why the bound survives.** Take a merge whose inputs hold S entries in
all, m in the largest, and whose output holds u ≤ S. Either:

- at least a quarter of the inputs were dropped (u ≤ 3S/4). The dropped
  entries pay, and each is a copy of one committed entry, dropped once.
  Example: a span of 1,000 keys merges with 300 updates of those keys. The
  output has 1,000 entries, and 300 were dropped.
- or the output kept more than 3/4 of them. Then every input but the
  largest grows by more than half (u > 1.5 x), since x ≤ S/2. A logarithmic
  weight per entry drops for each of those entries, and the guard says they
  hold at least a fifth of S ≥ u.

The weight is a harmonic sum, `ℓ(s) = Σ_{j=s+1..K} ⌊4K/j⌋` ≈ 4K ln(K/s).
It drops by at least K when a span grows by half. When a span shrinks it
rises smoothly, by at most 4K per entry removed, and the dropped entries pay
for that too. A floored log₂ weight would jump at powers of two, and a span
shrinking by one entry across such a jump would be unaffordable. That's why
the first version of this proof, which assumed outputs never shrink, used
log₂ and this one doesn't.

A merge into the base is guarded on the base, so it costs at most 5× the
newer entries it absorbs, and those leave the newer spans for good. It may
also drop tombstones and versions.

**The model.** Spans are only their sizes, so the theorem covers every set
of keys and versions. Steps:

- `commit n`: a span of `n ≤ K` entries at the head, written once;
- `merge`: any run of adjacent spans `l1 ++ m :: l2`, with `m` the largest
  and `5m ≤ 4·S` (the guard), and an output of any `u ≤ S`, `u ≤ K`,
  attempted `a ≤ R` times, each attempt writing `u`;
- `base`: the base and the oldest spans, with `5·base ≤ 4·(base + Σ)` and
  any output up to `base + Σ`, attempted `a ≤ R` times.

Boundaries, size classes and the cap decide which guarded merges happen,
never what one costs. So the adversary may choose any sequence. (In the
revised policy the cap never merges; it only drops a position.)

**Theorems.**

```lean
theorem merge_pays : M·u + 5·(u·ℓ u) + 23·(M·u) ≤ 23·(M·S) + 5·pot inputs
theorem write_bound (h : Reach w R s) :
    M · written ≤ (1 + 23 R) · (M · committed) + 5 R · (committed · ℓ 1)
theorem write_bound_log2 (hK : 0 < K) (h : Reach (harmonic K) R s) :
    written ≤ (1 + 43 R + 20 R log₂ K) · committed
```

`written` counts every entry written: commits, and every attempt of every
merge and base merge. `write_bound` holds for any weight with the three
properties: it never rises with size, it drops by `M` when a span grows
by half, and a span's total weight rises by at most `4M` per entry removed.
`harmonic K` is one such weight.

**What K is.** K bounds a span's entries. With one version per segment, a
span can hold several entries per key, so the distinct keys no longer bound
it. Each entry is a copy of a distinct committed entry, though, so the total
committed over the horizon always works. So does the distinct keys times
(1 + the endpoints live inside a span).

**What the numbers mean.** With R = 1 and K = 10⁹: at most 44 + 20·29 = 624
written per committed entry. This is a worst-case guarantee and the
constants are loose. The design's replays measure 10–17×, and its
adversarial release orders 1.0–3.9×. The log factor is unavoidable: guarded
pairwise merges of equal disjoint spans rewrite each entry log₂ K times.
Without the guard there is no bound. That was the reviewer's 265× case, a
big span rewritten once per released boundary.

**Hypotheses to keep true.**

- Every merge obeys the guard, on the inputs' actual entries before dedup.
  The design thread confirmed this: `Sim.allowed` checks it before every
  merge (oldest-absorbs, the window of 4, the base). There are no forced or
  cap-driven merges.
- Attempts per published merge are bounded by R. Work on merges that never
  publish (an upload that is abandoned, not retried) isn't covered. The
  engine has to cap it, or count it against a merge that does publish.

## 2. The tiling invariant

**The model** (`Tiling.lean`) is generic in what a span holds: any
associative merge `mul`, plus a base normalisation `norm` with
`norm (mul (norm x) y) = norm (mul x y)`. Commit `i`'s delta is fixed in
advance (`c i`). Each span records `(start, length, contents)`. Steps:

- commit: span `[head+1, head+1]`;
- birth: a boundary at `head + 1`. This covers a position, a pass's landing
  point, and an attempt's reservation at its claim, selections included;
- release: any subset of the boundaries survives;
- merge: two or more adjacent spans, none but the first starting at a
  boundary;
- base merge: the oldest spans, none starting at a boundary.

**Theorems.**

```lean
theorem reach_inv (h : Reach A c s) : Inv A c s
-- Inv: base = norm (fold of commits 0 .. baseN);
--      the spans tile (baseN, head] exactly, each = the fold of its own commits;
--      every live boundary is a span start or head + 1.

theorem catch_up (h : Reach A c s) (hP : P ∈ s.bnds)
    (hQ : Q ∈ s.bnds ∨ Q = s.head + 1) (hPQ : P < Q) :
    ∃ pre mid post, s.spans = pre ++ mid ++ post ∧
      mergeAll A mid = some (seg A c P (Q - P - 1))
```

In words: between two boundaries, a contiguous run of spans tiles
`[P, Q − 1]` exactly and merges to what the per-commit deltas would. "A
boundary born at head + 1 never cuts a span" is the birth case of the
invariant.

## 3. The read-ahead rule

**The model** (`Keys.lean`), per key. A span's entry is `{gen, del, pred}`.
Merging keeps the newer state and the older predecessor. That is
`Keys.alg`, an instance of the tiling model, and `Delta.sem_merge` shows the
sorted-list merge has exactly this meaning. For one key: commit `j`
writes entry `e j` (none if it doesn't touch the key), and `pr j` is the
key's presence after commit `j`.

**Hypotheses.** Generations strictly increase with commits
(`i < j → g i < g j`). Each entry carries its commit's generation. Presence
follows the entries: a touched key is live iff its entry isn't deleted, and
an untouched key keeps its presence. A keys= run read the key at commit `r`,
`P ≤ r ≤ N`, and delivered `pr r`, which the committed attempt's sealed
result records.

**The rule.** Merge the key's entries over `[P, N]`. No entry, or one with
`gen ≤ g r`: skip. Otherwise deliver `classify (delivered) (live now)`.

```lean
theorem read_ahead … (hPr : P ≤ r) (hrN : r ≤ N) :
    rule g e pr P r N =
      if (∃ j, r < j ∧ j ≤ N ∧ (e j).isSome)
      then some (classify (pr r) (pr N)) else none

theorem read_ahead_spans … -- the same, on the actual run of spans between
                           -- boundaries P and N + 1 of any reachable state
```

So the rule skips the key exactly when nothing touched it after the read,
and otherwise delivers its true presence change from `r` to `N`.

**Counterexample** (`drop_breaks_read_ahead`, proved by computation, no
axioms). This is the design's own example. Consumer Y sits at position 3; a
keys= run reads `d` at commit 4, live (added at generation 40); commit 5
removes `d` (generation 50). A merge that drops "added then removed" entries
leaves span `[3, 5]` with no entry for `d`, so Y skips it and keeps `d`
forever. The kept tombstone (generation 50 > 40, deleted) gives "removed",
which is the truth.

## 4. Lookups at the head

```lean
def lookupHead s k := (s.spans.reverse.findSome? (·.x k)).or (s.base k)

theorem lookup_head (h : Reach (alg G) c s) (k : Nat) :
    live (lookupHead s k) = live (seg (alg G) c 0 s.head k)
```

The newest-first lookup scans the spans from newest to oldest, takes the
first that holds the key, and else the base. In every reachable state it
sees what the merge of all commits says: the key's generation if live,
nothing otherwise. A tombstone found in a span correctly hides an older
live entry. The base's dropped tombstones and predecessors change nothing a
reader sees.

## Differential test

`lake build run` compiles the Lean list merge (`Main.lean`).
`../bend/harness`'s `bend-harness .lake/build/bin/run diff 2000` feeds it
2,000 random consistent histories (1 to 12 commits, keys over 48 bits,
generations over all 64). For every summary entry it checked:

- (key, generation, deleted) against the native `stream::Merge` over the
  commits' `.kx` files. `stream::Merge` takes each key's newest entry, which
  is exactly result 4's newest-first lookup over runs;
- the predecessor bit against a Rust transcription (main has no merge that
  keeps predecessors yet);
- the tree reduction against the one-by-one fold;
- presence before and after against the history.

All 222,066 summary entries were equal.

## What the Rust implementation must be checked against

These are the theorems' hypotheses. Each one the Rust breaks voids a
result.

1. **The span merge, per key.** The output has each key's newest entry
   (generation, deleted, payload) and the oldest input's predecessor. It
   never drops an entry outside the base, including tombstones that name no
   predecessor. Check it differentially against the Lean model, as above.
   Results 2–4 depend on it.
2. **The guard on every merge.** Largest input ≤ 4 × the others, on actual
   entries before dedup, for every kind of merge. Assert it when planning,
   and again when installing. Bound the attempts per merge, and don't let
   abandoned merges write without limit. Result 1 depends on it.
3. **Boundaries.** Born only at head + 1, and an attempt's reservation is
   taken at its claim. Installing a merge asserts that no live boundary
   starts any input but the first. A base merge asserts that none starts any
   of its inputs. Results 2–4 depend on it.
4. **Generations.** Strictly increasing with commit number, and every entry
   at its commit's generation. Result 3 depends on it, and so does the
   K47 "versions from the commit" rule.
5. **The delivered state.** For a keys= run, the read-ahead entry's
   attempt records the presence it delivered at `r`, as committed. Result 3
   depends on it.
6. **Exact writes.** A predecessor iff the key was live before the commit.
   The classes of `changes(P → N)` need this (the Bend-era law (c),
   `../bend/lean/Delta.lean`), but the read-ahead rule does not.

## Effort

About an hour of agent time, 01:31 to 02:35 CEST, after the Bend
experiment (`../bend/REPORT.md`). Failing check rounds, per file:

| File | Rounds | What failed |
|---|---|---|
| WriteBound, first version | 3 | missing core lemmas (`List.le_sum_of_mem`, `Nat.log2_one`); `simp [Nat.log2]` unfolds the definition; one coefficient rewrite |
| WriteBound, shrinking merges and retries (02:15 to 02:35) | 5 | a muddled case split, rethought before it ran; `Nat.succ_mul` rewrote `4 * K`; a structure field `M` that `omega` saw as an unknown; an implicit size in a `rw`; ring identities, which `grind` proves where `omega` can't |
| Tiling | 3 | `seg_append`'s index arithmetic; `cover` of an append; a misplaced doc comment |
| Keys, lookup | 3 | the base's normalisation: `st` can tell a tombstone from no entry, so the last step had to go through `live`; `mergeAll`'s equation lemma |
| Keys, read-ahead | 2 | `if` on a `Prop` needs `Classical`; one `le_refl` |
| Delta, bridge, `read_ahead_spans` | 0 | |

The arithmetic went to `omega`, and the three-way key comparisons to one
`rcases … <;> simp_all` line. The time went into choosing models whose
statements are honest: sizes only for result 1, so it covers every key
set; a generic algebra for result 2; per key for results 3 and 4.

## Not covered

- Read cost. The guard limits writes, but nothing here bounds how many spans
  accumulate. Bounding that is a separate question.
- The base merge streamed in key slices (an open question in the doc).
- Pins and file deletion (the "no dangling read" invariant). These belong
  in the TLA+ spec the doc plans.
- Payloads are modelled as part of the newer entry's state. They ride with
  `gen` and `del`, unexamined.
