# Attempt log

Wall-clock times (CEST, 2026-10-04), each check is one `bend PROOF.bend` run
(about 0.1 s each). "Iteration" = one edit-check round that failed.

## Step 1: install, trivial law, C (00:41 to 00:50)

- `curl -fsSL https://bend-lang.com/install.sh | sh`: Bend 2.0.35 into
  `~/.bend`, sha256-checked. Fine.
- `add_zero` law from the guide: checks first try, `bend hello.bend -o
  hello.c` writes 3,057 lines of C (the runtime is in every file), `-o
  hello` builds with clang in 0.5 s, runs.
- `--verdict` needs Lean 4.34.0 (elan); installed it (~2 min). First
  `--verdict` run builds the kernel: 18 s. ALL PROOFS CHECK.
- Nat at runtime is a 64-bit word holding up to 2^48-1; past it the program
  stops with "a Nat past the largest immediate 2^48-1". Nat.cmp is native.

## Step 2: model (00:50 to 00:52)

- First draft of the merge (`t1.bend`) checked and ran first try, including
  the termination check: the comparison of the heads comes in as an
  argument `c`, so `mrg(a, ys, heads(a, ys))` passes `a` unchanged and `ys`
  smaller. (Base's own `List.merge` uses a fuel argument instead.)
- Iterations:
  1. `model.bend` empty (my `sed -n` mistake); Bend said "unknown:
     model.Ent", not "empty module". Cost one round.
  2. `Nat & Class` in a `List<&2, _>`: a pair is `Type`, not `Data`.
     Replaced by a `Change` datatype.
  3. `SNil` already a constructor in Base (constructors are global).
     Renamed `StNil`/`StCon`.

## Step 3: laws

### merge_nil_l, merge_nil_r

- Iteration 1: `{==}` failed for `merge(DNil, a)`. `mrg`'s first case is
  `case DNil{} _: b`, yet `mrg(DNil, a, EQ)` does not reduce: a multi-value
  match splits on every scrutinee, so the checker needs `a`'s constructor.
  Matching `a` in the proof fixed it.

### (d) live_count (00:52 to 00:54, 6 iterations)

- Strategy: `Cons(s, d, c)` (the "before" bits agree with the state) is
  defined by the same walk as `app(s, d, c)`, so a proof generalised over
  any `c` goes through by structure alone, no key comparisons and no
  sortedness. Brute force in Python first (`/tmp/bendx/model.py`) showed
  this law holds on unsorted lists, while (c) does not.
- Iterations, all on the match discipline:
  1. matched `dl` after a destructuring let: refused.
  2. matched `b dl`: refused, `b` is bound after `dl` (scrutinees in
     binder order).
  3. a `+x = Con{..}` let needs an annotation: `{M.DCon{..} : M.Delta<G>}`.
  4. matched the entry `id` after the comparison `c`: refused, `c` is a
     later binder than `d`'s fields. Fix: a nested pattern
     `M.DCon{kd, M.Info{g, dl, b}, rd}` and `match dl b c` at once.
  5. a let before that match: refused again; moved the lets into the
     cases.
- Arithmetic: `add_succ` (copied from the par-sort demo), `succ_eq`,
  `succ_shift`. Rewrites `%e : P` need the whole goal `P` spelled with `_`
  where `e`'s right side is replaced by its left side; `+x = ...` lets
  shorten it.

### (a) merge_assoc (00:55 to 00:59, 5 failing rounds)

- Plan, before writing anything: the goal is stuck on `Nat.cmp(kx, ky)`
  (a match cannot inspect a call), so:
  - step lemmas `lt_r`, `gt_r`, `eq_r` (and `_l` flips): one merge step
    given `{LT == Nat.cmp(kx, ky)}`, each a single rewrite;
  - `tri(x, y, z)`: the three comparisons are consistent, stated through a
    type-level table `Imp(c1, c2, c3)` (Unit for the 13 orderings, Empty
    for the 14 others). Checked first try;
  - `assoc3`: takes the three comparisons as parameters (so they can be
    matched) with `{c == Nat.cmp(..)}` evidence, `tri`'s fact, and the 7
    induction hypotheses for smaller triples as arguments (no mutual
    recursion, so the IHs are passed in). The 14 impossible orderings close
    with an empty `match t:`; each of the 13 real ones (11 distinct proofs)
    is 4 to 7 rewrites plus `{==}`.
- The case texts were stamped out with a short Python script from a table
  of (ordering, rewrite chain), as an agent writing 13 similar cases would.
- Rounds that failed: lets before the match (again); evidence `-e` (erased)
  "consumed more than once" (a rewrite uses its proof live), `+e` works;
  all 13 cases then failed once on `-zs` (same reason), made the data
  parameters `+`; the `DNil, DCon, DCon` corner needed `merge_nil_l` flipped
  with `Equal.sym`. Every rewrite chain was right the first time it ran.
- `--verdict`: ALL PROOFS CHECK, 0.2 s (the kernel binary is cached).
  Sanity: an `@unsafe` shortcut in `comb_assoc` makes `--verdict` refuse
  and name the 6 defs that lean on it.

### (b) grouped_classes (00:59 to 01:00, 1 failing round)

- `fold_acc`: folding onto acc = merging acc with the fold from nothing
  (uses (a)); `fold_app`: folding a concatenation = folding twice (no
  rewrite at all, definitional); `grp`: by induction on the groups.
- The one failure: list parameters used twice need `+`.
- Orientation is the recurring cost: `%e` replaces e's right side by its
  left, so lemmas are stated "backwards" (`{merge(acc, fold(ds)) ==
  fold(ds, acc)}`) to rewrite the term the goal holds.

### (c) range_agrees, range_changes (01:00 to 01:04)

- The walk-based form (apply(apply(s, d1), d2) == apply(s, merge(d1, d2)))
  needs sortedness: brute force showed it false on unsorted input, and a
  deleted entry hides the next key's order. Restated per key instead:
  `lookup(k, d)` is the ordered lookup of a sorted run (stops at the first
  key past k), and `lookup(k, merge(a, b)) == over(lookup(k, b), lookup(k,
  a))` holds for any lists (brute-forced first). Law (c) is then about one
  key's history: presence `p` at P, commits that each agree with the
  presence before them (`Ok`), and the summary's entry: before == presence
  at P, live == presence at N; no entry, same presence.
- Iterations: 3 on definitions (binder order in `over`, so its newer
  argument comes first; termination order of `pres`/`Ok`, the shrinking
  list first). `lookup_merge` reused `tri` and the assoc template: checked
  first run. `lk_fold`, `agree_step`, `core`, `class_of`: checked first
  run.
- Mutation tests: three false variants of laws (swapped p, swapped
  newer/older, swapped added/removed) are refused; three planted code bugs
  (comb keeps the newer "before", heads compared in the wrong order,
  `removed` counting the wrong class) are refused by the proofs.

### (d) live_count: see above.

### tree_summary (01:13 to 01:15, 2 failing rounds)

- Added after the first benchmark: a balanced tree of merges, both halves
  in parallel (`l r = tree(..) tree(..)`), with fuel for termination
  (falls back to the one-by-one summary). Law: equals the summary whatever
  the fuel. `take_drop` plus the (b) lemmas; failures: `fuel` used twice
  needs `+`; one orientation slip.

## Step 4: C, differential test, throughput (01:05 to 01:19)

- `run.bend` + `io.c`: five foreign effects (`Bx.load`, `Bx.word`,
  `Bx.put`, `Bx.save`, clocks) hand u32 words in and out; everything else is
  the proven model. Bend lists every def that relies on foreign code; none
  of `main.bend`'s is among them.
- Iterations: a tuple destructure in a `do` block (refused: "destructure in
  its body"); `match` inside `do` (refused: move it to a def); `+i` for a
  reused index; `String.eq`, not `String.is_eq`; a failed rebuild left the
  old binary in place (I ran it once by mistake).
- Harness (Rust, `../harness`): first diff run passed. Timing: the machine
  was at load 49 on 18 cores (other agents), so wall times were 2x the CPU
  times (1,285 involuntary context switches in one run). Switched to CPU
  time (`clock()` on both sides), best of 5, Bend `--threads 1` (it
  defaults to every core).

## Lean comparison (01:20 to 01:30)

Same definitions and laws in Lean 4.34 (core, no Mathlib), `lean/Delta.lean`,
273 lines with the definitions.

- merge_assoc: 3 rounds. 1: `by_cases` on six comparisons + `simp_all` +
  `omega` left goals where `if ky < kz` was not decided from `kz < ky`.
  2: `rcases Nat.lt_trichotomy` three times, `subst_vars`, `simp_all [merge,
  comb, Nat.lt_irrefl, Nat.lt_asymm]`: checks. The 27 cases are one tactic
  line; Bend needed the `Imp` table, `tri`, 5 step lemmas and 27 written
  cases.
- Everything else in one file, then 3 rounds: `rfl` does not unfold `merge`
  (well-founded recursion, not definitional: the mirror image of Bend's
  stuck multi-scrutinee match); `subst` needs a variable side; `Consistent
  snil d` does not reduce for a variable `d` (same pattern-compilation
  effect as in Bend), fixed with `cases d`.
- Compiled with `lake build` (Lean also emits C), driven on the harness's
  inputs (`Bench.lean`); checksums equal Bend's on all three shapes.

## The design thread's answer (thr_xvgqnrw2kr, 01:22)

Asked at Erwin's request whether this algebra survives the span rework
(docs/key-index-design.md on main, cc54fcc). Yes: a span is this delta with
"before" widened to the span, its merge is this merge, changes(P->N) is (c),
the base merge and count are (d); (c)'s precondition is the design's "exact
writes". Predecessor generation instead of a bit: faithful as Maybe<G>, older
kept. Next laws it wants, in order: tiling bookkeeping; read-ahead with
increasing generations (and the counterexample when "neither" entries are
dropped); a write-amplification bound for the balance-guarded merge policy
that survives dedup (open); newest-first lookup equals the fold.
