# Bend 2 for the key index's delta algebra

Question: can Bend 2 be useful for the pure, correctness-critical core of
Solera's key index? The loop under test: state the constraints as laws, let
an agent iterate until the proofs check, and get a fast program out.

Short answer. The loop works. I stated nine laws about the delta algebra,
Bend checked every proof, the Lean-backed `--verdict` kernel agreed, planted
bugs were caught, and the compiled C matched Rust on 2,000 random histories.
But for Solera I would not adopt Bend. The same laws took under a quarter
of the lines in Lean, where tactics did in one line the case analysis Bend made me
write out 27 times, and Lean's compiled code ran this merge 2 to 4 times
faster than Bend's. Keep the LAWS/PROOF workflow. Do it in Lean.

Everything is on the branch under `experiments/bend/`. `LOG.md` is the
attempt-by-attempt record.

## What was built

`delta/main.bend` is the model. A delta is a key-sorted list of entries.
An entry is a key, a generation, a deleted flag and a live-before bit:

```
key 7: gen 12, deleted, live before     → removed at gen 12
```

Merging an older delta with a newer one walks both in key order. A key held
by one side passes through. A key held by both keeps the older entry's
live-before and the newer entry's generation and deleted flag. An example
across two commits:

```
commit 1: key 7 added    (gen 1, live,    not live before)
commit 2: key 7 removed  (gen 2, deleted, live before)
merged:   key 7          (gen 2, deleted, not live before)  → nothing
```

The class reads off the two bits: absent before and live after is added,
live at both is updated, live before and absent after is removed, absent at
both is nothing. The merged entry stays even when its class is nothing.

Keys are `Nat`. At runtime Bend stores a Nat as a 64-bit word that holds up
to 2^48 − 1 and stops the program with an error past that. That covers keys
encoded as 6 big-endian bytes, which is what the differential test does.
Real keys are byte strings, so a full model would compare byte lists.
Generations are a type parameter `G`. The merge never compares generations,
since the newer entry wins by position, so the proofs don't care what `G`
is. At runtime `G` is a pair of U32 halves, which carries the full 64 bits.
Nothing in this algebra needed 64-bit arithmetic, and that's luck. See the
limits below.

`delta/run.bend` is the program that runs the merge, and `delta/io.c` is a
small C module of five effects that hand u32 words in and out of Bend.
`harness/` is the Rust side.

## What was proven

All in `delta/LAWS.bend`, proven in `delta/PROOF.bend`. `bend PROOF.bend
--verdict` prints ALL PROOFS CHECK.

| Law | Says |
|---|---|
| `merge_nil_l`, `merge_nil_r` | the empty delta is an identity on both sides |
| (a) `merge_assoc` | `merge(merge(a, b), c) == merge(a, merge(b, c))`, for any lists |
| (b) `grouped_classes` | commits merged one by one and the same commits merged as range summaries give the same classes, for any split into consecutive groups |
| `tree_summary` | a balanced tree of merges, both halves in parallel, equals the one-by-one summary |
| `lookup_merge` | a key's entry in a merge is its newer entry over its older one |
| (c) `range_agrees` | per key: if each commit's live-before bit agrees with the key's presence before that commit, then the range summary's entry has live-before = presence at P and live-after = presence at N, and a key with no entry has the same presence at P and N |
| (c) `range_changes` | the same in classes: the summary's class of each key is exactly the change of its presence from P to N |
| (d) `live_count` | a delta whose live-before bits agree with the state changes the live count by exactly added − removed |

Two modeling choices matter for reading (c) and (d).

Law (c) is per key. I first tried the whole-state version, applying `d1`
and then `d2` to a state versus applying their merge. A Python brute force
showed it is false on unsorted lists, and a proof would have needed
sortedness lemmas everywhere. The per-key version uses the lookup of a
sorted run, which stops at the first key past the one sought, and
`lookup_merge` then holds for any lists. So (c) says what changes(P → N)
means for every key without any sortedness hypothesis. The price: it's a
statement about `lookup`, which only means "the key's entry" when the run is
sorted. The runs are sorted, because the Rust writer refuses unsorted keys.

Law (d)'s hypothesis `Consistent(s, d)` is defined by the same walk as
`apply`. I checked that it isn't vacuous: a concrete state and delta inhabit
it.

The laws are not trivially true. Three false variants of the laws were
refused: swapping the presence at P, swapping newer and older in
`lookup_merge`, and swapping added and removed in (d). So were three bugs
planted in the code: `comb` keeping the newer entry's live-before, the merge
comparing its heads in the wrong order, and `removed` counting the wrong
class. Each one broke a proof.

### It is the algebra the span design relies on

Erwin asked whether this survives the rework to a key-ordered span list
(`docs/key-index-design.md`, cc54fcc), and I asked the design thread
(thr_xvgqnrw2kr). It is. A span is this delta with "before" widened to
"before the span". The span merge is this merge, changes(P → N) is (c), and
the base merge and live count are (d). The precondition of (c) is the
design's "exact writes" assumption. The design keeps a predecessor
generation rather than a bit. That is faithful as `Maybe<G>` with the older
side's value kept, and the proofs don't depend on `G`.

The design thread's list of what to prove next, in its order:

1. Tiling bookkeeping. After any merges that never cross a boundary, the
   spans between P and N + 1 tile [P, N] exactly and merge to the
   per-commit fold, and a boundary born at head + 1 never cuts a span.
2. Read-ahead. With generations strictly increasing, a key read at commit
   `r` gets class (state delivered at r, state at N) when its newest
   generation is above gen(r). Prove that equals the presence change from r
   to N, and show the counterexample when "nothing" entries are dropped.
3. The open one: a bound on entries written per entry committed for the
   balance-guarded merge policy that survives dedup, or a counterexample.
   The doubling argument fails when merged spans dedup to near the size of
   their largest input. Simulated adversarial orders give 1.0 to 3.9×.
4. A newest-first lookup over the spans equals the fold's state at the
   head.

Number 2 needs ordered 64-bit generations, which is where Bend's numbers
hurt. Number 3 is real combinatorics. Both point to Lean.

## Effort

Wall clock, one agent, 2026-10-04: install at 00:41, all nine laws proven
by 01:15, C and the differential test by 01:19, the Lean port by 01:30.
Agent minutes say little about human effort, so here are the counts too.

| | Failing check rounds | Notes |
|---|---|---|
| model definitions | 3 | an empty module reported as "unknown type", a pair is `Type` not `Data`, a constructor name taken by Base |
| identities | 1 | `merge(DNil, a)` does not reduce for a variable `a` |
| (d) live count | 6 | all on match discipline, none on the math |
| (a) associativity | 5 | the plan took longer than the rounds, see below |
| (b) grouped | 1 | a list used twice must be `+` |
| (c) per key | 3 | definitions only; the proofs checked on their first run |
| tree | 2 | |
| runtime program | 5 | `do` blocks refuse matches and tuple destructuring |
| Lean, everything | 6 | |

Every check takes about 0.1 to 0.2 s, so a round costs nothing in machine
time. Bend's error messages are good: the expected and observed goal with
the full context, at the right line. Only one misled me.

Most failures were about Bend's rules, not about mathematics. The math
effort sat in one place, associativity. A merge step depends on
`Nat.cmp(kx, ky)`, a computed value, and Bend can't match on a computed
value. So:

- the merge takes the comparison as an argument, `mrg(a, b, c)`;
- five step lemmas unfold one merge step given evidence
  `{LT == Nat.cmp(kx, ky)}`;
- a type-level table `Imp(c1, c2, c3)` lists the 13 consistent orderings of
  three keys, and a lemma `tri` proves the comparisons always fit it;
- a helper takes the three comparisons as parameters so it can match them,
  closes the 14 impossible orderings, and proves each of the 13 real ones
  with 4 to 7 hand-spelled rewrites. Bend has no mutual recursion, so its
  seven induction hypotheses come in as arguments.

That is 27 written cases. A short Python script stamped them out from a
table, as an agent writing many similar cases would. Every rewrite chain
was right the first time it ran. `lookup_merge` reused the same shape and
checked on its first run. Lean did the same 27 cases in one line:

```lean
rcases Nat.lt_trichotomy kx ky with a | a | a <;> rcases ... <;> (try subst_vars) <;>
  first | omega | simp_all [merge, comb, Nat.lt_irrefl, Nat.lt_asymm]
```

| | Bend | Lean 4.34 |
|---|---|---|
| model, laws, proofs | 385 + 81 + 805 lines | 273 lines in all |
| full check | 0.14 s, `--verdict` 0.2 s (18 s the first time, to build the kernel) | 2.4 s |
| laws | 9 | the same 9, plus helpers stated as theorems |

## Bend's limits, as met

- No tactics. Every rewrite `%e : P` spells out the whole goal `P` with `_`
  where `e`'s right side stands, and replaces it with `e`'s left side. So
  lemmas get stated backwards to rewrite whatever the goal holds. A
  `+x = ...` let shortens the spelling.
- A match can't inspect a computed value. That shaped the code itself: the
  merge takes its comparison as a parameter so the proof can match on it.
  The prover dictates the program's signature.
- A multi-value match reduces only once every scrutinee is a constructor.
  `merge(DNil, a)` is stuck until `a` is matched, even though the first case
  ignores it.
- Matches follow binder order. A pattern's fields rank where their parent
  parameter does, so an entry's fields must be matched before a later
  parameter, and no let may precede a match. These cost most of the
  failing rounds.
- Affinity reaches the proofs. A proof or a value used twice must be `+`.
  Erased `-` evidence counts as consumed when a rewrite uses it.
- No mutual recursion, and termination reads arguments left to right. The
  shrinking argument goes first, and helpers get their induction hypotheses
  as arguments.
- Numbers. A Nat past 2^48 − 1 stops the program at runtime. U32 is a word
  of 32 Bools in proofs, so proving anything about 64-bit generations as
  U32 pairs means proving things about bit vectors. This algebra escaped
  because it never compares generations. The read-ahead law can't.
- Constructor names are global, and a pair is `Type`, not `Data`, so it
  can't sit in a `List<&2, _>`.
- Calling Bend from C is not supported. Foreign code goes the other way, as
  effects, and their C internals have "no ABI promise". I fed the merge one
  u32 word per effect call.

What Bend gets right: the checker is fast and precise; it names every def
that relies on `@unsafe` or foreign code, and the I/O shell was flagged
while none of the proven model was; `--verdict` rechecks with a kernel that
has a Lean proof; and parallelism came free. The tree reduction is one
`l r = tree(..) tree(..)` line, and its correctness proof took two rounds.

The trust story has two gaps that Bend's README states. The translation
from Bend to the kernel's input is not proven: read the emitted `.bendtt`
to confirm what a law says. The C compiler is unaudited, so the compiled
program is only as trustworthy as the differential test.

## The generated C, and its speed

`bend run.bend -o run.c` writes one 6,000-line file: the runtime, with the
program inside it as segments of a state machine. A merge step allocates
5-word heap nodes, bumps reference counts and pushes a continuation frame,
since `DCon{k, i, mrg(...)}` is not a tail call. In the LT and GT branches
the untouched list's head node is rebuilt twice, once for the recursive
call and once for the inlined `heads(...)` comparison. It reads well enough
for an audit, but nobody would maintain it, and the runtime can't be split
from the program.

Differential test (`harness`, `bend-harness RUN diff 2000`): 2,000 random
consistent histories, 1 to 12 commits each, 222,066 summary entries. Bend's
fold, Bend's parallel tree, a Rust transcription of the merge,
`solera_native::stream::Merge` over real `.kx` files (key, generation and
deleted only, since Rust has no live-before merge yet), and law (c) checked
on the data. All equal.

Throughput, in CPU time, best of 5. The machine was shared and at load 49
on 18 cores during the first runs, which doubled wall times, so both sides
read process CPU time through `clock()`. Bend ran on one thread except where
noted. Lean's times are wall clock, at load 8.

| Merge | 16 commits, 633K entries | 64 commits, 528K | 2 commits, 1.4M |
|---|---|---|---|
| Bend, one by one | 215 ms | 1,055 ms | 82 ms |
| Bend, tree | 137 ms | 186 ms | 80 ms |
| Bend, tree, 2 threads (merge wall time) | 103 ms | 124 ms | 71 ms |
| Lean, one by one | 74 ms | 270 ms | 44 ms |
| Lean, tree | 59 ms | 92 ms | 42 ms |
| Rust transcription, one by one | 12.7 ms | 37.4 ms | 9.2 ms |
| Rust transcription, tree | 14.5 ms | 16.1 ms | 9.3 ms |
| Rust `stream::Merge`, k-way over `.kx` with decoding | 87 ms | 119 ms | 104 ms |

On the same algorithm Bend is 9 to 28 times slower than Rust, and 2 to 4
times slower than Lean's compiled C. Lean's checksums match Bend's on all
three inputs. `stream::Merge` isn't a like-for-like baseline: it decodes
varint, prefix-compressed blocks and runs a heap over k runs, while the
others merge decoded lists. Bend's parallel tree gave 1.15 to 1.5× from a
second thread. The tree itself is the bigger win: 5.7× over the one-by-one
fold at 64 commits, because the fold copies its growing accumulator once
per commit. That one is an algorithm choice, available in any language, and
law (a) is what licenses it.

## Verdict

For Solera's pure cores (the merge algebra, the tiling and read-ahead rules,
the staleness predicates, pattern matching), the law-driven loop is worth
using. Writing the laws found the real question in (c), whole state or per
key, and the proofs caught every planted bug. An agent can grind out these
proofs.

Bend is the wrong tool for it today:

- Proofs cost about 4.6× the lines of Lean (1,271 against 273), and the cost grows with case analysis.
  Bend has no `omega`, `simp` or `decide`, so every ordering, every rewrite
  and every refutation is written by hand. Staleness predicates are Boolean
  case analysis, where Lean's `decide` and `simp` do the work. Pattern
  matching means strings, which are lists of characters in Bend, with a
  small library.
- The read-ahead law and the amplification bound need 64-bit generation
  order and arithmetic. Bend has neither in proofs, and Lean has `omega`.
- Bend's output doesn't replace the Rust. It's a separate program, about 10×
  slower, that C can't call. Lean's compiled code doesn't replace the Rust
  either, so either way the proven model is a reference that the Rust gets
  tested against. Lean is the better reference: shorter proofs, faster
  code, and a mature ecosystem.
- Verus would prove properties of the Rust merge itself, which removes the
  transcription gap. A streaming k-way merge with a heap and block decoding
  is far harder to verify than this algebra, though. I'd prove the algebra
  in Lean and hold the Rust to it with differential tests, as `harness/`
  does here.

What to keep from Bend: the convention. A human-owned `LAWS` file, an
agent-owned `PROOF` file, and one command as the gate. That works the same
way in Lean.

A suggested next step, if wanted: port the design thread's four laws to
Lean in `experiments/bend/lean/`, starting with read-ahead and the open
amplification bound.

## Reproduce

```
curl -fsSL https://bend-lang.com/install.sh | sh           # Bend 2.0.35
cd experiments/bend/delta
bend PROOF.bend --verdict                                  # needs Lean 4.34 (elan)
bend run.bend -o /tmp/bendx/build/run
cd ../harness && cargo build --release
target/release/bend-harness /tmp/bendx/build/run diff 2000
target/release/bend-harness /tmp/bendx/build/run bench 16 100000 1000000
cd ../lean && lake build && lean Delta.lean               # the Lean port
```
