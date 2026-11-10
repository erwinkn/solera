# Tracking what flows along edges: the target design

Status: **target design (T43), decided.** It is the design the new core is
built to. It starts from Fable's brief (*Incremental data flow tracking:
architecture brief*, Oct 5 2026) and applies Erwin's decisions on it
(D180, D183, D184). Where it departs from the brief, it says so and why.
It was revised after a first review (W65, A40), which fixed the owed-count
rule under conditions (§6) and the cut against the cleanup cursor (§13),
and again after an independent review (SOLE-4, findings F1 to F8), which
replaced the rules for late and dead writes on tables (§8) and offered a
simpler form of two mechanisms (§6, §10).

On 2026-11-10 Erwin answered or delegated every open question. His
mandate for the rest: the orchestrator's shape and API are agreed, and
every remaining choice is settled by experiment (performance, storage,
request counts, or a model check), not by argument. Three statements of
his frame the design:

- **Fresh start.** Nothing runs on Solera yet. The new core is switched on
  with a fresh start: no engine state, progress, run history or index file
  is converted, and nothing is kept for compatibility. User data stays in
  its stores (§15).
- **Scale target.** 100M keys per output, about 1% of them changing per
  day, 10k-key batches, one 4-vCPU engine. Every measurement that settles
  a remaining choice is taken at that scale.
- **Tables stay exact.** A Postgres table never holds a row the log does
  not have (§8).

Three kinds of statement appear below, always marked:

- **Settled.** Decided, with its decision number: D180, D183, D184 from
  before, and D187 to D204 from 2026-11-10. §17 lists each with its
  evidence.
- **Pending the model's verdict.** Two choices, each with two forms
  described side by side: the form of a full run (§10) and the form of
  staleness (§6). A model check decides each; the simpler form that passes
  wins.
- **P1 to P16** are the names this design gave its proposals before they
  were decided. They are kept as names, so that §17 can map each to its
  decision.

Fable's brief numbers its own decisions D1 to D18. To keep them apart from
the initiative's decision numbers, this document writes them **Fable D2**,
**Fable D14**, and so on.

## 1. Summary

The system stores two logs of facts and derives everything else.

- **The output log**, per output partition: every commit, as one entry per
  changed key: `(key, commit, new version, replaced version)`.
- **The input log**, per consumer input: one small **batch record** per
  committed batch: "took key range (lo, hi] at upstream commit H, starting
  from this slice of progress, under these conditions". It points into the
  output log and does not copy the keys.

Three structures are derived from them:

| Structure | What it is | Size |
|---|---|---|
| Change index | The output log sorted by key, in a few immutable layers | ~16 bytes per change |
| Progress | Per edge, "key range → upstream commit", plus a few per-key exceptions | 1 to 10 ranges per edge, usually |
| Key outcomes | Per per-key partition, the keys whose latest outcome is not ok (D179) | One entry per failing key |

**The idea that keeps progress small** (Fable's). A batch reads one
upstream commit H and walks keys in order. Once it commits, every key in
its range is in sync with H, including keys it did not touch. They were
not owed, so they already matched H. So a consumer's position is a handful
of key ranges, each tagged with a commit, not one version per key.

**One query does most of the work.** `diff(output, C1, C2, range)` returns
the keys whose state differs between two commits. Planning calls it with
C2 at the head; lineage calls it with a past batch's two commits. Same
function, same files: history reproduces exactly what a batch received.

**What Erwin changed in Fable's design** (all settled):

| Fable's brief | This design | Decision |
|---|---|---|
| Workers plan batches; the engine validates | The engine plans, from its warm on-disk index cache | D183 |
| Staleness from counters that may over-count; a false alarm costs an empty batch | Answers are always exact, kept current by propagating each commit to the edges that read it; no false-alarm runs. Whether each edge keeps exact counts or one known owed key is pending (§6) | D183 |
| A grace period T: replaced files live T hours; attempts older than T are refused | Exact cleanup: only attempts in flight read old data files, so a file goes once every live attempt planned at or after the commit that replaced it | D183 |
| Layers are Parquet | Our own block format, one entry per changed key, chosen by benchmark (T44, SOLE-3; §4) | D183, D199 |
| Postgres outputs stage, then publish after the journal decides | One simple store contract: a write returns the keys it wrote; a load returns the keys and versions it actually loaded; deviations are recorded as facts. No staging, publish, two-phase commit, `acquire`, intents or repair clock. A store that writes in place keeps one version check per partition inside each write transaction, so a table never holds a row the log does not have (§8) | D184, D196 |
| Batch records carry their replaced slice and a digest | Kept | D184 (Fable D10) |
| Context read per batch | Kept | D184 (Fable D11) |
| History exact while a run is retained | Kept | D180 |

**Where it runs** (D203). The new core (the index, `diff`, the planner,
progress, staleness and commit building) is written in Rust from the
start, behind Python bindings (PyO3). The server, the worker and the
user-facing SDK stay Python. Python and Rust cross once per call (a plan,
a commit), never once per key. Whether the core does its own I/O or works
on bytes Python fetched is pending a measurement (§15).

## 2. Words

Each term is used in one sense only.

| Term | Meaning |
|---|---|
| Output | One output partition: a versioned map from key to version. |
| Version | An opaque token compared only for equality. For a produced output, the engine assigns it: the writing attempt's id (today's *generation*). For a source, the source's own version string. Never reused for a produced output. |
| Commit | A numbered, atomic change to one output. Numbers start at 1, rise by 1, and are never reused. |
| State at c, `S(c)` | The output's map after commit c. |
| Head | The output's latest commit. |
| Entry | One key's change in one commit: `(key, commit, new, replaced)`. |
| Clear | A commit flag: "everything present before is removed, then this commit's entries apply". Used by a full run of a single-value or append-only output, and of a keyed output only in form (a) of §10. |
| Edge | One keyed input of one consumer partition, reading one upstream output. |
| Conditions | What a key was processed under: the input's patterns, the context (the heads of the consumer's whole and dep inputs), the asset's definition, and the input's rebuild counter. |
| Rebuild counter | A number per input, part of the conditions. Raising it makes every key owed: a full run asked for by hand of a per-key asset (§10). |
| Progress | Per edge, what the consumer holds for every key. Stored as **pieces** (key ranges with one value each) and **points** (single keys with an explicit value). Replaces today's *observation record*. |
| Owed | A key whose upstream state now, under the conditions now, differs from what progress says the consumer holds. |
| Due | Owed, and a default run would take it now. A failing key waiting for its retry is owed but not due (§11). |
| Batch record | The input-log entry for one committed batch on one edge. |
| Cut | The oldest commit the change index must still answer at. |
| Live attempt | An attempt whose spec the engine issued and which it has not yet settled (committed, failed, canceled or given up on). It holds the head of every output it reads. |
| Anomaly | A version loaded from a produced output, or an absence, that neither its log nor a live attempt explains (§8). |
| Version check | On a store that writes in place: one row per output partition holding the highest attempt version that has written; every write transaction raises it to its own version first, or is refused (§8). |

Notation: `k1@4` is key k1 at version 4. `c4` is commit 4 of `items`, and
`f2` commit 2 of `feed`; other outputs' commits are written `copy#2`. A range `(lo, hi]` excludes
lo and includes hi; `−∞` and `+∞` are the ends of the key space. `−` in a
version column means absent.

## 3. The cast

One example runs through the whole document. It is `behaviors.md`'s cast,
plus one consumer of `copy`:

```
feed     a keyed source (keys k1, k2, …)
  └─▶ items     keyed, incremental over feed
        ├─▶ tally     plain incremental over items; keeps a count: before + len(added) − len(removed)
        ├─▶ copy      plain incremental over items; a keyed copy of it
        │     └─▶ archive   plain incremental over copy
        └─▶ checks    per-key (each=True) over items; reads factor whole
factor   an unkeyed source
```

`items` receives four commits. To make examples easy to follow, a version
here is named after the commit that wrote it: `k2@2` was written by c2.

| Commit | Entries, as key: replaced → new |
|---|---|
| c1 | k1: − → 1 · k2: − → 1 · k3: − → 1 |
| c2 | k2: 1 → 2 · k4: − → 2 |
| c3 | k3: 1 → − · k5: − → 3 |
| c4 | k1: 1 → 4 |

| State | k1 | k2 | k3 | k4 | k5 |
|---|---|---|---|---|---|
| S(c1) | 1 | 1 | 1 | − | − |
| S(c2) | 1 | 2 | 1 | 2 | − |
| S(c3) | 1 | 2 | − | 2 | 3 |
| S(c4) | 4 | 2 | − | 2 | 3 |

`tally` reads `items` with `batch_size=2`. Its story, used in §5 to §7:

| Step | items' head | What happens |
|---|---|---|
| Run 1, batch 1 | c1 | Takes k1, k2 (added) |
| c2 lands | c2 | |
| Run 1, batch 2 | c2 | Takes k3, k4 (added); nothing lies beyond |
| c3, c4 land | c4 | |
| Run 2, batch 1 | c4 | Takes k1, k2 (updated) |
| Run 2 is canceled | | |

## 4. The output log and the change index

### Entries

| Field | Meaning |
|---|---|
| `key` | The key that changed |
| `commit` | The commit that changed it |
| `new` | Its version after the commit; absent if the commit removed it |
| `replaced` | Its version just before; absent if the commit added it |

The kind of change follows from the two versions: added (nothing
replaced), updated (both present), removed (nothing new).

The commit header holds the commit number, a timestamp, who produced it
(run, batch, attempt; or a source report), counts by kind, the live key
count afterwards, the smallest and largest key touched, and the `clear`
flag.

### Four rules

1. **Numbers never restart.** An output deleted and declared again is a new
   output with a new log (its new **life**, as today).
2. **Entries chain.** For one key, each entry's `replaced` equals the
   previous entry's `new`. Every merge checks it.
3. **The engine is the only writer.** It builds every commit's entries
   itself, at commit time, from the keys the write returned and its own
   index (D184). *Departs from Fable's rule 3* (a writer whose parent moved
   rebuilds and retries): with the engine building entries in commit
   order, the parent is always the head.
4. **A commit may be a clear.** Only a full run of an ordinary asset uses
   one: always for a single-value or append-only output, and for a keyed
   output only if form (a) of §10 is chosen. Its own entries still carry
   the true replaced version. It is one flag, not 100 million removal
   entries.

**Why entries carry the replaced version** (**D187**, P1, Fable D2). A diff needs
only the entries inside its window; cleanup knows which file each commit
replaced; and a key that went v1 → v2 → v1 is recognised as unchanged.
Fable's cost was that the writer must know what it replaces. That cost is
gone here, because the engine computes it from its index when it commits (D184), as today's
resolver already does for writes.

### Layers

The change index is the output log itself, sorted by key and stored as a
few immutable layers on object storage. Nothing is stored twice: the
planner and history read the same files (**D188**, P2, Fable D3).

- A layer covers a contiguous run of commits and holds their entries,
  sorted by key, then by commit, newest first.
- The newest layers are single commits, written when the commit is made and
  never rewritten.
- Background merges combine adjacent layers. The oldest layer is the
  **base**: one entry per key present at the cut.
- The layer list lives in the engine's journaled state. Nothing is found by
  LIST.

```
commits   1 ............ 900 | 901 ..... 990 | 991 .. 998 | 999 | 1000
layers    base at the cut     | merged        | merged     | one | one    <- head
```

Today's layers keep each key's newest state plus the commits that added or
removed it (*flips*), split into *main* and *side* parts, with removed keys
in the base's *graveyard*. All of that goes: an entry carries both
versions, so a reader needs neither flips nor side parts.

### Queries

| Query | Returns | Used by |
|---|---|---|
| `diff(C1, C2, range)` | Keys whose state differs between C1 and C2, with both versions | Planning, staleness, lineage |
| `scan(C, range)` | Every key present at C, with its version | First runs, full runs, condition changes |
| `get(C, keys)` | The listed keys' state at C | Points, key-list runs |
| `history(key)` | Every retained entry for one key | Key trace |

All take an optional "first N after key k" and a pattern filter. C1 and C2
may be any commits at or above the cut, not only the head (D180; today's
index answers only at a head a batch pinned).

### How diff works

1. Pick the layers whose commit run overlaps (C1, C2].
2. In each, read only the blocks overlapping the key range.
3. Merge by key, keeping the entries with C1 < commit ≤ C2.
4. The oldest kept entry's `replaced` is the state at C1; the newest's
   `new` is the state at C2. Emit the key if they differ.

**Example: `diff(c1, c4)`.**

| Key | Entries in (c1, c4] | At c1 | At c4 | Result |
|---|---|---|---|---|
| k1 | c4 | 1 | 4 | updated |
| k2 | c2 | 1 | 2 | updated |
| k3 | c3 | 1 | − | removed |
| k4 | c2 | − | 2 | added |
| k5 | c3 | − | 3 | added |

**Example: a revert.** `feed`'s k1 goes `v1` → `v2` → `v1` (feed commits f2
and f3). `items` last read feed at f1. `diff(f1, f3)` finds two entries for
k1: the oldest replaced `v1`, the newest left `v1`. Equal ends: k1 is not
emitted. Today the same history delivers k1 as updated (`behaviors.md`
INC-4); that behaviour changes (§16).

**A window that contains a clear.** Entries before a clear do not say what
it removed, so the diff becomes a merge of two scans, `scan(C1)` and
`scan(C2)`. That reads every key in the range, which is unavoidable:
everything changed. The engine keeps each output's list of clear commits,
so the planner always knows. (Under form (b) of §10 a keyed output never
has a clear, and this case does not arise for it.)

### Merging

**The size rule** (**D189**, P3, Fable D5; today's rule, carried over). A merged
layer reaching above the cut holds at most 4 × the bytes of all newer
layers, plus 1 MiB. So a reader starting at any commit reads at most about
5 × the bytes that changed since, plus 1 MiB. Today's tiers, two merge
lanes, back-off (D181) and writer backpressure stay.

**What a merge keeps.** Above the cut, every entry. At or below it, entries
fold into the base: one entry per key present at the cut. The cut never
passes the cleanup cursor (§13). With preserved
commits (§13, later), merges below the cut keep the states at those commits
too.

### File format

**Settled (D199): our own block format, one entry per changed key**, read
and written by the Rust extension, as today's index is
(`key-index-format.md`). D183 said the format would be decided by
benchmark; two benchmarks decided it. *Departs from Fable D4*, which chose
Parquet so DuckDB could read layers directly. A layer is a key-sorted run
of entries in blocks, with a small index of block bounds.

**Against Parquet (T44, 100M keys, 1,264 measurements).** Both formats
were read through the same I/O code, so the benchmark compares formats,
not readers (D185). They are within about 10% of each other on the
index's own reads.

| | Our format | Parquet |
|---|---|---|
| Storage requests per query | 1× | 2 to 3× |
| Scattered gets | 1× | 1.7 to 2× slower |
| Metadata per 100M keys | 1.8 MB | 10 MB |
| File size | 1× | 10 to 12% smaller |
| History query from DuckDB | 0.35 to 0.56 s, through our reader and an Arrow export | 2 to 160 ms, read directly |
| Dependency | None new | 5.7 MB more native module, 34 s more clean build |

The one place the formats differ by a factor is scattered lookups, and
this design makes many of them: points, key-list runs, and staleness
lookups (§6). The format is also still ours to change (preserved commits
change what merges keep, §13), and a public format would make each change
a compatibility question. History through our reader is fast enough for a
person asking a question. Before SQL over history is promised, a DuckDB
table function over our reader is to be measured.

**Range records and run-length versions (SOLE-3, branch
`exp/layer-format-ranges`).** Both were measured as extensions of our
format and rejected. One entry per changed key stays.

- **A range record** says "every key present in [lo, hi] just before
  commit c was rewritten at c to version g". It helps a full rewrite: 100M
  keys become 101 records, stored bytes fall to 0.57× (2,336 → 1,324 MB),
  and a scan at head goes from 36 to 24 s. But the saving is temporary:
  once the history is folded into the base, the base holds one entry per
  key anyway (612 MB in every variant). A record does not carry the
  versions it replaced, so it breaks the read bound of the size rule: the
  last-commit diff on the rewrite at 100M keys goes from 0.47 s · 7 GETs ·
  10 MB to 0.67 s · 13 GETs · 13 MB, and a paused consumer's diff over
  daily-clustered changes at 10M keys from 28 to 354 GETs. On daily
  scattered changes, the scale target's case, no record is ever found.
  And every reader, merge and cleanup would have to resolve records
  against older layers. To revisit only if large range-partitioned
  rewrites come to dominate real change logs.
- **Run-length versions** make every workload's layers 2 to 7% larger;
  zstd already finds the repeats. The one gain (−31% on the hot
  workload's change layers) is outweighed by its base.

## 5. Progress: pieces and points

Progress says, for every key of an edge, what the consumer holds. It
replaces today's observation record (base, ranges, points).

### Pieces

A piece is a key range with one value. Pieces never overlap and cover the
whole key space. A new edge has one piece: everything `empty`.

| Value | What it says about every key in the range |
|---|---|
| `at(H, conditions)` | The consumer holds the key as upstream had it at H, if the conditions' patterns take it; otherwise nothing |
| `empty` | The consumer holds nothing |
| `held` | The consumer holds whatever its own output holds for the key, at no known upstream version (§10) |

Today's *base* is simply a piece covering everything.

### Points

A point is one key with an explicit value that overrides its piece: absent;
a version, with the context and definition it was processed under; or
`held`. Points come from three places only:

- a failing key, which keeps what the consumer held before (§11);
- a key processed through an explicit key list (§10);
- a key loaded at another version than planned (§8).

**Points carry explicit versions** (**D190 (2)**, P5, Fable D7), never "a version
since replaced" as today's fold writes. A stuck key never forces the index
to keep old history.

### Decoding, and what owed means

To decode a key, use its point if it has one, otherwise its piece. A key is
owed when what it decodes to differs from upstream now, under the
conditions now:

| Consumer holds | Upstream now | Owed |
|---|---|---|
| Nothing | Present and taken by the patterns | an add |
| Version v | Same version, same context and definition | nothing |
| Version v | Another version, or another context or definition | an update |
| `held` | Present and taken | an update |
| Version v, or `held` | Absent, or no longer taken | a removal |
| Nothing | Absent or not taken | nothing |

### What a batch commit writes

A batch that planned at head H and covered (lo, hi] writes one piece:
`(lo, hi] → at(H, the conditions it was planned under)`. Those may differ
from the conditions at commit: a pattern change between the two is an
input change, which the commit check lets through, and the piece's
conditions then make its keys owed. Inside that range the batch leaves a
point for exactly the keys whose holding differs from what the new piece
says: failed keys, keys held back from the batch, and keys loaded at
another version. Each point written carries its reason (failed, held back,
deviation, key list), which lineage needs (§12). Every other point in the
range is dropped.

**Why it is correct.** A key in the range that the batch did not take was
not owed at H, which means it already held what `at(H)` says.

### Worked example: tally

| Step | Plan | Progress afterwards |
|---|---|---|
| Start | | (−∞, +∞) empty |
| Run 1, batch 1 at c1 | `scan(c1)` finds k1, k2, k3; takes k1, k2 (added); the range ends at k2 | (−∞, k2] at c1 · (k2, +∞) empty |
| Run 1, batch 2 at c2 | `scan(c2)` over (k2, +∞) finds k3, k4 (added); nothing beyond, so the range runs to +∞ | (−∞, k2] at c1 · (k2, +∞) at c2 |
| Run 2, batch 1 at c4 | `diff(c1, c4)` over (−∞, k2] gives k1 (1 → 4) and k2 (1 → 2), updated | (−∞, k2] at c4 · (k2, +∞) at c2 |
| Run 2 canceled | | |

What is owed now: nothing in (−∞, k2]; in (k2, +∞), `diff(c2, c4)` gives
k3 removed and k5 added. No piece is `empty`, so `tally` is **complete,
only stale** (D177). Its count: 4 after run 1; still 4 after run 2's
updates; the next run removes k3 and adds k5, and it stays 4, the number
of keys items holds.

Note what nothing had to record: k2 changed at c2 under a range run 1 had
already passed. The piece `(−∞, k2] at c1` says so by itself.

### No fold (D190 (1), P4, Fable D6)

Progress changes in two ways only: a batch commit, and a reset (§10).
Nothing relabels it. Today, every batch commit **folds**: older ranges
relabel to the newest head, and their keys that changed meanwhile become
points "present at a version since replaced". That fold exists because
today's index answers only at a pinned head and keeps no old versions. With
MVCC entries, a piece can keep its own commit for as long as it lives, so
the fold and its points go.

Pieces stay few anyway: a run sweeps the key space, so its batches
overwrite older pieces; adjacent equal pieces merge; and when little is
owed, one batch covers a wide range. In the worst case, a 10,000-batch run
racing upstream commits leaves up to 10,000 pieces (~1 MB) until the next
run collapses them; an empty batch still writes its range.

Progress lives in the engine's memory and is rebuilt from batch commits
since the last checkpoint. Past a few thousand points an edge's points
spill to a sorted file, as today.

### Other inputs

The other input kinds are the keyed design without keys, as today.

| Input | Progress | Owed when |
|---|---|---|
| Unkeyed incremental | "Consumed through commit c" | The head is past c. A clear after c makes the next run a full run (INC-13) |
| Whole or dep | None of its own: its head is part of the context | The context now differs from a piece's |

## 6. Staleness kept exact by propagation

**Settled (D183):** answers are always exact. Nothing is checked every
second; the engine knows where changes happen and propagates them along
the graph. *Departs from Fable §12*, whose per-edge counters could
over-count and whose false alarms cost an empty batch.

### The form: pending the model's verdict

Two forms meet D183. Both are exact, both are updated by each upstream
commit rather than by polling, and neither keeps a counter that can
over-count.

- **Form (a): exact owed counts** (P14, this design's first proposal).
  Each edge keeps the exact number of keys each piece owes, adjusted entry
  by entry at every upstream commit.
- **Form (b): one witness per input** (review F3). Each edge keeps one key
  known to be due, or the fact that nothing is due. "How many keys?" is
  counted by the planner when someone asks, and cached.

| | Today | Form (a): exact counts | Form (b): one witness |
|---|---|---|---|
| "Is `tally` stale?" | The planner's comparison, run when asked, cached | Any piece count above zero | The edge has a witness |
| Work per upstream commit | None until asked | Every entry: find its piece, compare, adjust; a lookup for each key that changed again since its piece | Entries up to the first that matters while fresh; while stale, only "does this commit touch the witness?" |
| "How many keys?" | Counted when asked | Always known, except "upstream incomplete" | Counted by the planner when asked, then cached |
| Code that decides what is owed | The planner | The planner, and the count rules, which must agree with it | The planner |
| After a restart | Nothing to rebuild | Replay the commits since the checkpoint through the count rules | Compare the stored commit with the head |
| A revert (k1: v1 → v2 → v1) | Reads as changed (INC-4) | Count goes to 1, then back to 0; no run | Witness set, then dropped; no run |

**How it is decided.** A model check of both for correctness, then a
measurement of the cost of counting on request at the scale target (a
consumer far behind on a 100M-key output). The simpler form that passes
wins. The Captain's lean is (b): it keeps today's property that the
planner is the only code that decides what is owed (STA-3), and it does no
work per entry for edges nobody looks at. Form (a)'s one advantage is a
number that is always there, which only the console uses. Until the
verdict, §16 and §17 mark what depends on it.

### Form (a): exact owed counts (P14)

Each edge keeps, for each piece, the **exact number of keys it owes**.
Keys with a point are counted separately, one by one. Four events change
the numbers.

**1. An upstream commit.** For each entry `k: r → n` in the commit,
compare whole observations, as §5's table does:

- **Held**: what the consumer holds for k under its piece. For
  `at(P, conditions)`, that is k's state at P with the piece's context and
  definition, or nothing if the piece's patterns do not take k. For
  `empty`, nothing. For `held`, "held" if the consumer's own output has k.
- **Wanted before**: `r` under the conditions now, or nothing if the
  patterns now do not take k. **Wanted after**: `n`, the same way.
- k was owed before if held differs from wanted before (nothing and
  nothing are equal); it is owed after if held differs from wanted after.
  The piece's count moves by the difference: +1, 0 or −1.

What the rule gives in the three usual cases:

| Piece | Entry | Count |
|---|---|---|
| Current conditions | k unchanged since P, now updated | +1 |
| Current conditions | k reverts to what the consumer holds | −1 |
| Any | k is not taken by the patterns now | 0: both wanted sides are nothing (CHG-9) |
| Context or definition differs from now | k present before and after | 0: it was already owed, as updated |
| Context or definition differs from now | k added, and the consumer holds nothing | +1 |

**Finding what is held.** Usually k had not changed since the piece's
commit P, so its state at P is `r`, known for free. When the engine
computes `r` from its index, it also learns **since**: the commit that
wrote `r`, or the upstream's latest clear if that is later. (A clear
removes keys without naming them, so a key it removed and a later commit
re-adds has `r` = nothing, while a piece older than the clear holds it.)
If since ≤ P, held is `r`. Otherwise the engine looks it up: `get(P, k)`
in its warm index. A point compares its own value the same way.

**The lookup budget.** One warm-index lookup per entry whose key changed
again since the piece's commit, once per distinct piece commit (edges whose
pieces sit at the same commit share it). For an edge near the head, that
is reverts only. For a consumer a month behind, it is every change of a
hot key.

**2. A clear** (where one is used, §10). A piece whose commit precedes the
upstream's latest clear is not propagated entry by entry. During the upstream's full run every
rebuilt key would need a lookup, for counts nobody can act on: the gate
holds those edges (§10). Its reason is **upstream incomplete**, exact but
not a number. It is counted once, by a scan merging S(P) and S(head), when
the upstream is complete again. `empty` and `held` pieces are counted the
same way.

**3. A batch commit.** A batch's range always starts at a piece boundary:
a run's first batch starts at −∞, and the task's progress key stays a
boundary until the run ends (equal neighbours merge only then). So a batch
replaces the pieces its range covers and cuts at most one, the piece
holding its last key.

- The **new piece**, if the conditions at commit are still those it was
  planned under, owes `diff(H, head)` over its range, minus its points:
  usually nothing, or the few keys commits since H changed. If the
  patterns or a whole input moved between plan and commit, any key of the
  range may be owed without having changed upstream, so a diff is not
  enough: the new piece is counted by a scan, as in event 4 (this is §5's
  rule, which writes the piece under the conditions it was planned
  under).
- The **cut piece's remainder** keeps its count minus the owed keys inside
  the range. Those are the keys the plan found owed there at H (taken or
  held back), plus the net changes propagation applied inside the range
  since H. The engine keeps that sum per live attempt as it propagates, so
  the split costs no lookup.

**Example: a cut.** Suppose run 3 of `tally` uses `batch_size=1`. It
starts from run 2's end: `(−∞, k2] at c4` owes 0, `(k2, +∞) at c2` owes 2
(k3, k5). Batch 1 plans at c4: k3, to remove; the range is (−∞, k3].
Before it commits, c5 re-adds k3 at version 5. k3 changed since c2 (it was
removed at c3), so held is looked up: k3@1. Owed before (1 against
nothing) and after (1 against 5): the count moves by 0, and the sum inside
the range since c4 is 0. At commit:

| Piece | Owes | Why |
|---|---|---|
| (−∞, k3] at c4 | 1 | `diff(c4, c5)` names k3: the consumer now holds nothing, upstream has k3@5 |
| (k3, +∞) at c2 | 1 | 2 − (1 planned + 0 since c4): k5 |

**4. A change of conditions, a reset, or a new edge.** The affected pieces
are counted by a scan when the change happens. A status asked meanwhile
waits for it; it is never guessed (`behaviors.md` STA-12).

**After a restart.** Counts are checkpointed with progress. Replay
re-propagates the commits since the checkpoint, reading their entries from
the layer files in the warm cache.

#### Examples of form (a)

**tally's counts through §3's story** (current conditions, every key
taken).

| Event | (−∞, k2] | (k2, +∞) |
|---|---|---|
| After run 1, batch 1 | at c1, owes 0 | empty, owes 1 (k3) |
| c2: k2 1 → 2, k4 − → 2 | owes 1 (k2 unchanged since c1: +1) | owes 2 (k4 added to an empty range: +1) |
| After run 1, batch 2 | owes 1 | at c2, owes 0 |
| c3: k3 1 → −, k5 − → 3 | owes 1 | owes 2 |
| c4: k1 1 → 4 | owes 2 | owes 2 |
| After run 2, batch 1 | at c4, owes 0 | owes 2 (k3, k5) |

**A revert raises no alarm.** `items` holds `(−∞, +∞) at f1` on `feed`,
owing 0. f2 changes k1 `v1 → v2`: k1 unchanged since f1, so +1. f3 changes
k1 back, `v2 → v1`: k1 changed since f1, so the engine looks up held =
`v1`. Owed before (`v1` against `v2`), not owed after (`v1` against `v1`):
−1. The count is 0 again. `items` was truly stale between f2 and f3, and
is fresh after f3, with no run spent finding that out.

**The context moves.** `checks` holds `(−∞, +∞) at c1` under `factor` at
w1, with k1@1 and k2@1. `factor` moves to w2: the scan counts 2, both
present keys owed as updated (CHG-10). Then items updates k1 to 2: held
k1@1 under w1, wanted k1@1 then k1@2 under w2, owed both times: 0, still
2. Then items adds k3: +1, now 3.

**An excluded key.** `copy` takes `k*` only. items commits nothing but
`x1`: both wanted sides are nothing, the count does not move, and `copy`
stays fresh (CHG-9).

**Cost of form (a).** Each upstream commit costs work proportional to its
entries times the distinct piece commits reading it, plus the lookups of
the budget above. For a large commit that is not a clear (a 100M-key
update), the counts are computed as a stream; "stale or not" is known at
the first entry of a key unchanged since the piece, long before the exact
count is in. Neither this loop nor the lookups have been measured.

### Form (b): one witness per input (review F3)

Each edge stores one of two states. Each carries X, the upstream commit it
was last brought up to.

- **fresh(X)**: nothing is due as of X.
- **stale(w, reason, since, X)**: key w, the **witness**, is due as of X,
  for this reason, and the edge has been stale since that moment.

A **probe** is the planner asked for one key: it stops at the first due
key it finds, or finds none.

1. **An upstream commit, the edge fresh.** Read the commit's entries in
   key order and stop at the first that matters. A key the patterns take,
   with no point, is the witness: no lookup is needed, because nothing was
   due, so the consumer held what upstream had before this commit, and an
   entry is a change. A key with a point is compared with its new state,
   and is the witness if they differ and it is due. A key the patterns
   leave out is skipped. If nothing matters, the state becomes fresh(this
   commit).
2. **An upstream commit, the edge stale with witness w.** No entry for w:
   nothing to do. An entry for w: recheck w, one lookup. Still due: keep
   it. Otherwise probe.
3. **A batch commit.** If stale, recheck the witness; if the batch
   processed it, probe. If fresh, probe the batch's own range, since
   upstream may have moved since the plan. Inside a run the next plan does
   the probe's work: any key it takes is the witness. A cancel changes
   nothing the consumer holds.
4. **A change of conditions** (patterns, context, definition, rebuild
   counter). Probe. When every present key is owed, it stops at the first
   key it meets.
5. **A retry comes due** by time, or is forced: that key is the witness if
   the edge was fresh. This reads the index of due keys (§11).
6. **Asking.** Compare X with the upstream's head; if the head is newer,
   run the commits in between through rules 1 and 2. A lost notification
   can delay an answer by one comparison, never change it. A restart needs
   nothing more.
7. **A gate** (where one applies, §10): the edge reads "held: upstream
   incomplete". When the upstream completes, one probe.

To keep STA-2 ("a stale partition says every reason that holds"), the
edge keeps one witness per kind of reason: input changed, conditions
changed, never processed, retry due.

**`tally` through §3's story**, against form (a)'s counts:

| Event | Form (a): (−∞, k2] · (k2, +∞) | Form (b) |
|---|---|---|
| After run 1, batch 1 (k1, k2 at c1) | 0 · 1 | stale, witness k3: never processed |
| c2: k2 1 → 2, k4 added | 1 · 2 | c2 does not touch k3: no work |
| After run 1, batch 2 (k3, k4 at c2) | 1 · 0 | k3 processed; the probe finds k2, changed at c2: witness k2 |
| c3: k3 removed, k5 added | 1 · 2 | no work |
| c4: k1 1 → 4 | 2 · 2 | no work |
| After run 2, batch 1 (k1, k2 at c4) | 0 · 2 | k2 processed; the probe finds k3, removed at c3: witness k3 |
| Someone asks "how many?" | 2, already known | the planner counts k3 and k5: 2 |

**A revert raises no alarm.** `items` is fresh on `feed` at f1. f2 changes
k1 from v1 to v2: rule 1 makes k1 the witness, with no lookup. f3 changes
it back: rule 2 rechecks k1, finds that `items` holds v1 and upstream has
v1, and probes. The probe finds nothing: `items` is fresh, and no run
started. **An excluded key**: `copy` takes `k*` only, `items` commits only
`x1`; rule 1 skips it, and `copy` stays fresh (CHG-9).

**Cost of form (b).** An upstream commit costs entries up to the first
that matters while fresh, and one check while stale; a 100M-entry commit
usually stops at its first entry. The expensive cases are a commit whose
entries the patterns all leave out (read in full, as in form (a)), and a
probe that finds nothing, which walks every piece once against the head.
The worst is a consumer far behind whose few owed keys revert one after
another, so that each probe walks its lag; a probe that resumes after the
old witness spreads one walk over all of them. A count on request reads
everything that changed since the oldest piece: T44 measured that read for
a consumer paused 30 days on a 100M-key output at about 2 to 23 s from
cold storage, about 8 s for scattered daily changes. That is the
measurement the verdict still needs, on the new index.

### What follows, under either form

| Question | Form (a) | Form (b) |
|---|---|---|
| Is anything due on this edge? | Any piece count or due point above zero | The edge has a witness |
| Why? | The reason of each owing piece: input changed (behind the head), patterns, context, definition or rebuild counter changed (its conditions differ from now), never processed (`empty`), to reconcile (`held`), upstream incomplete (held by a gate, §10) | The reason of each witness kept, one per kind |
| Is the partition stale? | Any of its edges owes something due (STA-3: a default run would load something) | The same |
| Upstream stale | A partition whose stale status flips tells its consumers, which tell theirs: work only where the status changes | The same |
| Complete? (D177) | No `empty` piece owes anything: an `empty` piece's count is exactly its unprocessed upstream keys | For each `empty` piece, whether it has an upstream key: one key or none |
| How many keys owed? | The sum of the counts: exact, except "upstream incomplete" where a gate holds the edge, which has no number until the upstream is complete (D197) | Counted when asked: every owed key, that is the keys an unlimited plan would take plus the owed keys not yet due (failing keys waiting for their retry, and keys held back while their producer repairs an anomaly, §8), each counted once; cached until the upstream or the progress changes |
| How many commits behind, since when? | The oldest owing piece's commit, against the head and the commit headers | The same, from progress |
| Which keys? | The planner without a limit, on demand; costs a read proportional to the answer | The same |

### As built (B7, `native/tracking/src/stale/`)

Form (b) was decided (P14 in the Captain's decisions) and is built as
follows. Per keyed edge the core keeps its conditions now (a watch event,
journaled when the edge opens and whenever a deploy changes them), a
generation, and what is known: fresh, or stale with a witness, its reason
and since when, exact for the edge's progress and conditions at one
generation, as of one upstream commit.

- **Applying an event never reads the index.** A batch commit, a reset, a
  rename or new conditions raise the edge's generation and record which key
  ranges changed (a reset or new conditions: all of them). What is known
  then stops being current, and the status reads "unknown" until a refresh.
- **At an upstream commit** (rules 1 and 2), the commit builder hands the
  commit's entries to staleness, which adds a witness part to the commit's
  own event for every current edge reading the output: fresh, the first
  entry the patterns take with no point makes it stale; stale, a commit
  that does not touch the witness changes nothing. An edge the entries
  cannot decide (the commit touches its witness, or a point differs) is
  left for a refresh.
- **A refresh** (rules 3, 4, 6, 7) brings an edge current from the index: a
  stale witness is rechecked at the head, and a probe looks for another
  only if it is no longer due; fresh as of commit x, rule 1 runs over the
  diff from x to the head outside the changed ranges (there the edge still
  holds what upstream had at x), then one probe from the first changed
  range; otherwise one probe from the first key. A refresh uses what changed
  since the last exact answer instead of replaying commits in order, so the
  model's ordering fixes (catch up before a batch commit, a pattern change
  or a reset to `held`) hold by construction.
- **A witness part applies only to the generation it was computed for** and
  never moves x back: a part computed against progress that moved is
  dropped and recomputed. A lost or late notice delays an answer, never
  changes it.
- **Retries** (rule 5): a failed key whose retry came due makes a fresh edge
  stale, and starts a new generation, since what is due changed with no
  change to progress; at startup the engine applies it to every retry
  already due. Generations come from one counter and are never reused, so
  a part for an edge since closed never applies to one reopened under its
  name.
- **The gate** (rule 8): one event per output marks its full run started or
  completed; meanwhile readers that keep the gate read "held: upstream
  incomplete", and when it lifts they are probed again.
- **Counts** are the planner's owed count, cached until the edge's
  generation or its upstream's head moves; a held edge has none (D197). One reason per edge (the
  witness's); one witness per kind of reason is not built.

At 100M keys (`bench/core/stale/`): a notice costs 0.08 to 0.18 µs per
edge, and about 100 ns per entry per edge when no entry matters; a count
costs 1.1 s cold for an edge a day behind and 16 s for 30 days behind,
within 6% of the index's own diff for a day.

## 7. A batch from start to finish

**Settled (D183): the engine plans**, because its warm on-disk index cache
makes planning cheap there. *Departs from Fable D8* (workers plan from
immutable files, the engine never handles key lists).

1. **The engine plans at the head.** From the task's progress key, it walks
   the pieces and points after it, collects candidates until it has N
   (`batch_size`), and classes them. The range ends at the last key taken,
   or at +∞ if the candidates ran out.
2. **It issues the spec**: the head H, the range, each key with its class
   and its version at H (D178: the keys travel in the spec), the conditions,
   and the version this attempt will write under. The attempt is now live,
   holding H and the heads of every other output it reads.
3. **The worker loads** the keys at their versions. The load returns the
   keys and versions it actually loaded (D184, §8).
4. **The worker runs the producer** with the classes as loaded.
5. **The worker writes** through the store, which returns the keys it wrote
   and removed (D184).
6. **The worker reports**: what it loaded, what it wrote, per-key outcomes.
7. **The engine commits, in one journal event**: the output commit (entries
   built from the written keys, the attempt's version and the replaced
   versions from its index), the edge's new piece and points, the batch
   record, the key outcomes that changed, and, under P13, any load
   reports on sources.
8. **It propagates** the output commit to the edges that read the output
   (§6).

A batch whose output does not change makes no output commit: writing
nothing wakes nothing (INC-14). It still commits its progress. A full
run's first batch always commits, even when it writes no key (SEL-11): a
clear is a change, and under form (b) of §10 a first batch over an empty
upstream also completes the run and removes every leftover.

### Where candidates come from

| Under the walk | Candidates | Class |
|---|---|---|
| A piece `at(P)`, same conditions | `diff(P, H, range)` | From the two states |
| A piece `at(P)`, other conditions | `scan(H)` merged with `diff(P, H)` over the range | §9 |
| An `empty` piece | `scan(H, range)` | added |
| A `held` piece | `scan(H)` merged with a scan of the consumer's own output | both: updated; upstream only: added; own only: removed |
| A point | `get(H, key)` against the point | From the two states |

### Commit checks

The engine refuses a batch's commit, writing nothing, when the asset's
definition changed since it was planned, or an upstream it read was
deleted and declared again (a new life). This is today's commit check, kept. The
batch's keys stay owed.

### Failure, retry, cancel

- **A failed attempt appended nothing.** Its keys stay owed.
- **Each attempt plans at its own head** (**D190 (3)**, P6, Fable D9; today's
  behaviour). A retry is a new spec at the new head and may hold other keys
  than the failed attempt.
- **A cancel stops new specs.** Committed batches stay committed.

### The batch record

**Settled (D184, Fable D10):** each record carries the slice of progress
it replaced, a digest, and the planner's version. One per committed batch
per edge.

| Field | tally, run 2, batch 1 |
|---|---|
| Run, batch, attempt, mode | Run 2, batch 1, attempt 1, incremental |
| Edge | items → tally |
| Range | (−∞, k2] |
| Head | c4 |
| Conditions | patterns p1, no context, definition d1 |
| Replaced slice | (−∞, k2] at c1, no points |
| Points written | none (each would carry its reason) |
| Counts | updated 2 |
| Digest | hash of [(k1, 4, updated), (k2, 2, updated)] |
| Planner version | 1 |
| Own head at plan | tally#2 (read only when a replaced part is `held`, §12) |
| Output commit | tally#3 |

About 200 bytes plus the replaced slice, usually one piece.

- **The replaced slice makes the record stand alone.** The batch's exact
  input is the diff from that slice to its head, over its range. No earlier
  record is replayed, so retention can drop any run without breaking
  another.
- **The digest makes recomputation honest.** Lineage recomputes the key
  list; if its hash differs, the answer is an error naming the batch, never
  a wrong list.
- **Points written** carry the deviations (§8): keys loaded at another
  version than planned, with the version loaded. Each point names its
  reason: failed, held back, deviation, or key list.
- **A key-list run records its key list**, the one case where planned keys
  are stored: the user's input cannot be derived.

### Several keyed inputs

A batch covers the same key range on every keyed input; candidates merge
in key order and N counts distinct keys. Each edge gets its own piece at
its own head and its own record.

## 8. The store contract

**Settled (D184, amended by D196 for stores that write in place).**

| Call | Takes | Returns |
|---|---|---|
| write | a batch of rows (and removals), the version the engine assigned to the attempt | the keys it wrote and removed; on a store that writes in place, refused whole if a newer attempt has written the partition (D196) |
| load | the planned keys, each with its planned version | the keys and versions it actually loaded, absent included |
| cleanup (kept from today) | a key and version, an attempt's version, or a whole output | nothing; idempotent |

- **The engine assigns versions** and computes replaced versions from its
  index. A store keeps each key's version beside its data: in the object's
  name (FileStore, S3Store, as today), or in a version column (a table).
- **A load says what it read**, as sources already do (D146, D147). It is
  robust to a key that moved or vanished since planning: it loads what is
  there and says so.
- **Deviations are facts.** A key loaded at another version than planned is
  processed as loaded and recorded as a point in the batch record. The
  producer's output commit is recorded as usual. A version of a produced
  output that neither its log nor a live attempt explains is an anomaly
  instead (D196, below).
- **No staging, publish or two-phase commit, and no store *kinds*.** A
  store that writes in place keeps one version check per partition inside
  each write transaction (D196, below).

**Example: a store that moved on.** `items` is on Postgres. `tally`'s batch
plans at c4 to load k1@4 and k2@2. Before it loads, items commits c5
(k2: 2 → 5). The load returns k1@4 and k2@5. The batch processes k2 as
updated to 5, and commits `(−∞, k2] at c4` plus a point `k2 → 5`. After
the commit, the new piece's diff(c4, c5) names k2, but k2's point equals
upstream now: nothing is owed in (−∞, k2]. Today this batch would replan
at c5 (STO-3).

**What this deletes.** The fenced store kind, `acquire`, `keys()`, repair
by presence, the repair clock, the write gate's intents, `reads()`
snapshots, the replan on a newer generation, and the waits it needs
(held: `moved`, `writing`, `repair`). The fence table's row per partition
stays, as the version check of D196.

### Late and dead writes on stores that write in place

A store that writes in place (a table, a key-value store) can receive
writes the engine never committed: an attempt that wrote and then died
before its commit, or a late write from an attempt the engine gave up on.
`lifecycle.md` §9.1 lists how a write lands late: a worker that lost
contact but runs on, a paused process, a request still queued at the
database, a client timeout that hides a write that completed. A store
that names objects by version (FileStore, S3Store) is safe: such a write
only creates an object nothing references.

**Settled (D196): tables stay exact.** A table never holds a row its log
does not have, and a version no commit recorded never enters the log.
Five rules (review F1, recommendations 1 to 5):

1. **One version check per partition, inside every write transaction.**
   The store keeps one row per output partition holding the highest
   attempt version that has written. A write transaction first raises it
   to its own version, or is refused whole if it is already higher. The
   row lock makes a newer writer wait for an older writer's open
   transaction; after that, the older one can never write again. This is
   today's fence row, now taken only inside write transactions.
2. **Every row carries its version**, in a version column, which D184
   needs anyway so that a load can report versions. A dead attempt's rows
   can then be listed (`WHERE version = 6`), and the next attempt repairs
   exactly, without waiting for a consumer to find them: it deletes the
   rows whose key the log does not have, and the producer re-owes the
   others (the key, for a per-key asset; for an ordinary asset, the dead
   attempt's range, which is in its spec, in `all` mode on the retry).
3. **The first attempt after one that ended without committing always
   opens a write transaction**, even an empty one. That raises the check
   and locks the late writer out. Today's store has the same rule.
4. **An anomaly is any loaded version, or absence, that neither the log
   nor a live attempt explains**: a dead attempt's version, a version that
   went back after a restore from backup, a row that vanished. The engine
   raises an alarm naming the output, the key and, where it knows it, the
   attempt. The consumer's key is not due until the producer has redone
   it, so no run re-reads the same dead row; where the batch's spec can
   already name the dead versions, those keys are held back instead of
   processed. The anomaly never enters the producer's log. A version of an
   attempt still live is not an anomaly: the point stands, and the key
   settles when that attempt commits.
5. **An asset never builds on a dead write to its own output.** An
   attempt whose `ctx.load()` returns a version other than the asset's
   own last commit owes a full run of the asset.

To say it plainly: this keeps **one check per partition per write
transaction**. That is a fence in the textbook sense, and D184 said "no
fencing", so D196 partly reverses D184. What D184 was after, deleting the
protocol around the fence, still holds. **D184 still deletes:** `acquire`
before reads, the write gate's intents, repair by presence, the repair
clock, readers waiting for a repair (held `moved`, `writing`, `repair`),
`reads()` snapshots and replans on a newer version. `lifecycle.md` §9's
"every store is exact" stands.

A key may stay a group of rows (STO-6): the check is per partition, so it
needs no unique key per row, and it covers a write that replaces a whole
partition and a SQL write materialised inside the database.

**Why not a guard per row.** This design first proposed one (P15:
`WHERE excluded.version > t.version` on the upsert). It compares against
the existing row, so a late write that inserts has nothing to compare
with:

> `items` is on Postgres and holds k2@2. Attempt A (version 6) plans k2 as
> updated, then stalls. The engine gives up on A. `feed` removes k2, and
> the retry B (version 7) deletes the row and commits c5 (`k2: 2 → −`).
> A wakes up; there is no row, so its upsert inserts k2@6.

The review reproduced this on PostgreSQL 16 with that exact guard. Under
D196, B's write transaction raised the partition's check to 7 before it
deleted the row, so A's transaction is refused whole. The repair the
design first paired with the guard (P16: re-owe once a consumer trips
over the row) is replaced by rules 2 and 4: the repair starts at once,
and consumers do not act on rows known to be dead.

**Example: a dead write to `tally`** (rule 5). `tally` is on Postgres and
holds 4. `items` adds k6. tally's attempt A reads 4, writes 5 and dies
before its commit. The log still says 4, and k6 is still owed. The retry
B's `ctx.load()` returns 5, a version that is not tally's last commit:
`tally` owes a full run, instead of adding k6 again and keeping 6 for
good.

**Cleanup never deletes an in-place row by version alone.** A dead row of
a key the log has is that key's only row: deleting it would leave the key
absent while the log says present. Rule 2 deletes only the rows whose key
the log lacks; the others wait for the producer's rewrite. CLN-5's late
cleanup by version applies to stores that name objects by version.

**P13: loads report what they find, for sources only.** A load that finds
a source at another version than its log reports it to the source's log,
as Fable D16 says. The report becomes an entry, unless the log changed
that key since the batch's head (the log then knows better). A source's
log is the engine's record of what the outside said, and a load is one
more observation of the outside. **Settled (D193): sources only.** A side
effect to know: a load can now move a source's head, so two consumers of
a source that changes on every read take turns triggering each other.
The engine's rate limit (ENG-10) bounds that, no longer the sensor's
interval.

**Rejected: P13 on produced outputs** (this design's first draft).
Counterexample, from the W65 review: items on a table with no version
check. Attempt A
(version 6) writes k2@6 and is given up on; its retry B writes k2@7 and
commits c2. A's queued write lands later, and the row regresses to k2@6.
tally plans k2@7, loads k2@6, and the report writes c3 `k2: 7 → 6` into
items' log. Downstream processes an update to older data, nothing re-owes
items, and the regression stands until k2's input changes again. The log
would record a defect as a change.

## 9. Conditions: patterns, context, definition

A change of conditions writes nothing to progress. Each piece remembers
the conditions it was processed under; a difference from now makes keys
owed (Fable D11; today's behaviour).

| Part | When it differs from now |
|---|---|
| Patterns | Keys taken by exactly one of the two pattern sets are owed |
| Context | Every present key is owed, as updated |
| Definition | Every present key is owed. An ordinary asset rebuilds with a full run (§10); a per-key asset keeps its progress and reprocesses every key (D194, §10) |
| Rebuild counter | Every present key is owed: a full run of a per-key asset asked for by hand (D194, §10) |

**Store config is not part of the definition** (D202). Rotating a
Postgres password, or pointing a DSN at a new host holding the same data,
makes nothing owed. The definition includes the store's version, so a
change that really alters the data bumps that version. Moving an output to
another store is a new life (§10), as today.

**Settled (D184, Fable D11): context is read per batch.** Each batch reads
the heads of the consumer's whole and dep inputs when it starts and stamps
them on its piece. If `factor` moves during a run of `checks`, later
batches stamp the new head and the earlier ranges are owed again
afterwards. A run's consistency across a moving whole input is the
pipeline author's responsibility.

**Planning over a piece with other conditions.** A diff is not enough, since
a key can be owed without changing upstream. The planner merges
`scan(H)` with `diff(P, H)` over the range and, per key, compares what the
consumer holds (the state at P, if the old patterns took it) with what it
should hold (the state at H, if the new patterns take it). Only prefixes
where the two pattern sets can disagree are scanned, as today.

**Example: patterns.** `checks` processed `a.csv@1`, `b.csv@1` under
`*.csv`; the patterns become `b.csv` and `*.xlsx`; upstream also has
`c.xlsx@1`. Owed: `a.csv` removed (no longer taken), `c.xlsx` added;
`b.csv` nothing.

## 10. Run modes and full runs

| Mode | Keys taken | What a commit writes | After a cancel |
|---|---|---|---|
| Incremental | Due keys, in key order | A piece for the range, plus points | The rest stays owed |
| All | Every owed key, and every other present key as `unchanged` | The same | The rest is as it was |
| Key list | Exactly the listed keys | Points only; as a full run's first batch, also that batch's full-run writes (below; SEL-12) | The listed keys are done |
| Full | Everything. An ordinary asset resets its inputs' progress at its first batch; a per-key asset resets nothing (D194) | The reset, if any, then a piece per batch | An ordinary asset's unreached part stays `empty`; a per-key asset's unreached keys stay owed |

### Key list

`tally`'s progress is `(−∞, +∞) at c2`; the head is c4. Owed: k1 (1 → 4),
k3 (removed), k5 (added). The user runs `keys=(k1, k5)`: the commit writes
points `k1 → 4` and `k5 → 3`, and the piece stays at c2. Owed afterwards:
k3 only. `diff(c2, c4)` still names k1 and k5, but their points equal
upstream now. The next incremental run covers the range and drops both
points.

### Full run of an ordinary asset

**Settled, whichever form wins: no new life** (the first half of P7,
Fable D12). A full run keeps the output's log and its numbering, and
downstream keeps its progress. A new life is only for an output deleted
and declared again (a removal, a move to another store). Today, resets of
unkeyed outputs start a new life (D178), so a full run cascades down the
graph as full runs, and a keyed full run writes a replacement delta of
every removed key. For keyed outputs that cascade goes. An append-only
upstream's clear still makes its consumer run full (INC-13 stays).

**Common to both forms.** The first batch's commit resets the asset's
inputs' progress to `empty`, in the same journal event, and the producer's
first batch is told `batch.reset`, as today. If the first batch fails,
nothing happened. Versions are never reused, so a rebuilt key can never
look unchanged by accident.

**Single-value and append-only outputs clear at the start in both forms,**
and their readers are held until the run completes: a half-built count or
log is wrong, not merely stale. Only keyed outputs have a choice.

**For keyed outputs, the form is pending the model's verdict.**

- **Form (a): clear at the start, plus the gate** (P7, P12, this design's
  first proposal). The first batch's commit is a clear. Downstream runs
  no batch while the upstream is incomplete.
- **Form (b): rebuild in place, remove leftovers at the end** (review F2).
  Each batch overwrites its keys, and the commit that completes the run
  removes every key the run did not write again.

The running case. `copy` holds `copy#1`: k1@1, k2@1, k3@1, k4@1 (copied
from c2). Its definition changes, so it runs full at c4, where `items`
holds k1, k2, k4, k5, with `batch_size=2`. `archive` sits at `copy#1`.

| Step | (a): log entry | (a): copy holds | (b): log entry | (b): copy holds |
|---|---|---|---|---|
| Before | | k1@1 k2@1 k3@1 k4@1 | | k1@1 k2@1 k3@1 k4@1 |
| Batch 1, `copy#2` | **clear**; k1: 1 → 2; k2: 1 → 2 | k1@2 k2@2 | k1: 1 → 2; k2: 1 → 2; rebuild floor set to 2 | k1@2 k2@2 **k3@1 k4@1** |
| Batch 2, `copy#3` | k4: − → 3; k5: − → 3 | k1@2 k2@2 k4@3 k5@3 | k4: 1 → 3; k5: − → 3; **k3: 1 → −** (a leftover); floor cleared | k1@2 k2@2 k4@3 k5@3 |

What `archive` is given:

| | Today (no gate) | (a): with the gate | (b): no gate needed |
|---|---|---|---|
| At `copy#2` | k1, k2 updated; **k3, k4 removed** | nothing: held | k1, k2 updated |
| At `copy#3` | **k4 added**; k5 added | k1, k2, k4 updated; k3 removed; k5 added | k4 updated; k5 added; k3 removed |

Without a gate today, `archive` removes k4 and adds it back, and its
output lacks k4 in between. Both forms avoid that: (a) by holding
`archive`, (b) because the rebuild never makes a key disappear and come
back.

#### Form (a): clear at the start, plus the gate

- The first batch's commit is a **clear** on the asset's own output. On
  Postgres it deletes the partition's rows. Later batches are ordinary.
- **Downstream keeps its progress** and sees one diff across the clear
  (two scans, §4).
- **The gate (P12):** while an upstream is incomplete (mid full run, with
  an `empty` piece still owing), downstream runs no batch over it, by
  default. The engine checks this before every spec, so a downstream run
  already in flight pauses too ("held: upstream incomplete"). Each input
  can opt out if it wants partial data.
- **The one way the gate holds for good.** A full run canceled midway, or
  a first run that never finishes, leaves the upstream incomplete, and
  nothing resumes a run by itself (RUN-10). Downstream then waits until
  someone runs the upstream again. The status says so: "held: upstream
  incomplete (copy)".

What the review found against it: every deploy that changes a
definition freezes the whole subtree below the asset for the length of
its full run (10,000 batches at the scale target); the table is half empty
during the run; a cancel holds downstream for good; and on a table the
gate does not protect a downstream batch already in flight, which loads
the cleared rows as absent. If (a) is chosen, the review asks that the
gate never hold a keyed reader's first run, and that a cancel get an alarm
and one command that lets downstream proceed.

#### Form (b): rebuild in place, remove leftovers at the end

One more number per asset partition, the **rebuild floor**: a version, or
nothing. While it is set, the asset is rebuilding.

1. **The first batch** resets progress (above), sets the floor to its own
   attempt's version V, and records its entries, in one journal event.
2. **Every batch** writes its keys as a normal keyed write. Its entries
   carry the true replaced version (`k1: 1 → 2`). Rows written by the
   rebuild have a version at or above V; rows from before are below V.
3. **`ctx.load()` during a rebuild returns only rows at or above the
   floor**, so a producer that builds on its own output still starts from
   nothing. For stores that put the version in the object's name, that is
   a filter on the engine's key list; for a table, `WHERE version >= V`
   on the version column (§8). In the first batch the floor is not set
   yet, so `ctx.load()` returns the old output, as D166 says today.
4. **The completing commit** (no `empty` range still has an upstream
   key, D177) also removes every **leftover**: each key of the asset's own
   index with a version below V gets an entry `key: old → −`, found by one
   scan of that index. A table removes them with one `DELETE … WHERE
   version < V`; a store that names objects by version needs no call, and
   cleanup deletes the files later. The floor is then cleared. If the run
   completes with no batch of its own (the remaining upstream keys were
   deleted), the engine commits the removal itself.

A canceled rebuild keeps its floor and leaves old rows where it did not
get to: stale, but present. The next run continues it. A second
definition change during a rebuild moves the floor up; rows of the
abandoned rebuild become leftovers like any others. The floor never goes
down. It relies on versions that only grow per partition, which D196's
version check guarantees on tables.

A gate is still needed for three kinds of reader: readers that take the
output whole (whole and dep inputs, fan-ins), which should not read a mix
of old and new rows; readers of a keyed output built up over several
batches, such as a count per group, whose rows are wrong mid-rebuild; and
keyed readers that ask to wait for one diff. How the engine learns that a
keyed output accumulates (a declaration, or noticing `ctx.load()`) is part
of what the verdict settles.

#### Side by side

| | Today | (a): clear and gate | (b): rebuild in place |
|---|---|---|---|
| First batch | Replaces the output with its keys | A clear, then its keys | Writes its keys; deletes nothing |
| Keys the run does not write again | Gone at the first batch | Gone at the first batch | Removed by the completing commit |
| The table during the run | Partly empty | Partly empty | Full: a mix of old and new rows |
| Keyed downstream during the run | Runs; sees removals, then re-adds | Held until the run completes | Runs; sees updates and adds |
| Downstream after a cancel | Has processed the removals | Held until someone reruns | Unaffected |
| Index cost | One removal entry per key | One flag; diffs across it by two scans | One entry per key that really disappeared; one scan of the own index per full run |
| Extra state | | The list of clear commits | The rebuild floor until the run completes |

**How it is decided.** The model thread checks both forms; the simpler
one that passes wins. The Captain's lean is (b). The review's small model
of (b) (§18) found no key removed and then re-added within a rebuild,
where (a) produced 398 such pairs over the same histories (with upstream
removals switched off, so that every such pair is churn).

**"Upstream incomplete" without a number** (D197). Wherever a gate holds
an edge, its reason is "upstream incomplete": exact, but not a count. A
count would cost one index lookup per rebuilt key for an edge that cannot
run anyway. When the upstream completes, the edge is counted once (§6).

### Full run of a per-key asset

**Settled (D194, the amended P8).** A per-key asset (one per-key input,
outputs keyed by its key) resets nothing on a definition change or a full
run. With success-only progress (§11), its progress already describes its
output exactly, key by key.

- **A definition change.** The pieces keep the old definition, which
  differs from the new one, so every key they cover is owed (§9). The
  classes come from progress and the upstream's index alone. `checks`
  holds k1, k2, x9; `items` holds k1, k2, k3: k1 and k2 updated, k3 added,
  x9 removed. The design's first version reset progress to `held` and
  scanned `checks`' own output to find the same classes; that scan goes.
- **A full run asked for by hand**, with no definition change, has no
  condition that differs. Each input has a **rebuild counter**, part of
  the conditions. Asking for a full run raises it, which makes every piece
  differ; each batch stamps the new value on its piece, so a canceled run
  resumes where it stopped.
- The output never empties, so the asset stays complete and needs no gate.

**`held` stays only for progress that cannot be trusted:** an upstream
deleted and declared again, state brought in from elsewhere, or a store
repaired by hand. Then a per-key asset resets to `held`, and each batch
compares the upstream with the asset's own output (§7); an ordinary asset
owes a full run.

## 11. Per-key assets: key outcomes

**Kept (D179):** per-key assets expose each key's latest outcome; only
outcomes other than ok are stored, sparse; ok, removed and unmatched are
derived.

**Settled (D191, P9, Fable D14): progress records success only.** A failing key keeps what
the consumer held before, as a point, so it stays owed; its outcome entry
holds the failure, the tries, what it failed on, and its retry schedule.
*This changes today's rule*, where a failing key counts as processed at
the version it failed on.

**Example.** `checks`' run 2, batch 1 at c4 takes k1 (1 → 4) and k2
(1 → 2); k2 fails.

| | After the commit |
|---|---|
| Piece | (−∞, k2] at c4 |
| Point | k2 → 1, with the context and definition it was processed under |
| Key outcome | k2 failed, 1 try, failed on k2@2 |

k1 is not held up: the piece moved on. k2 is **owed** (holds 1, upstream
2) but not **due**: it waits for its retry: new input, its backoff, a new
deploy, or a forced retry (KEY-3). `checks` is stale for k3 and k5, and
shows one failing key.

**Why success only.** A retry is classed right (a key that never succeeded
comes as added); a key that never succeeded and then vanishes upstream owes
nothing; and progress always describes the output. That last property is
what lets a per-key asset rebuild without a reset (D194, §10).

**When the point and the outcome entry go.** Both go when the key
succeeds, when it is processed as removed or unmatched, and at a reset.
When upstream goes back to what the consumer holds, the failure is moot
and only the **outcome entry** goes: the key is no longer owed, so nothing
is retried. The **point stays**: without it, the piece would say the
consumer holds the version it failed on. The next batch over its range
drops it, because it then equals what the new piece says.

**The index of due keys.** Retries were planning's slowest case: T45
measured 7,000 due keys a second, and asked for an index of what is due.
D191 builds it: per per-key partition, the failing keys ordered by the
time their retry comes due, so a plan reads only the keys due now and
the engine wakes at the next due time.

## 12. History

**Settled (D180):** while a run is in history, everything about it is
answerable exactly, per key.

| Table | One row per | Holds |
|---|---|---|
| `runs`, `attempts` | Run, attempt | As today |
| `batch_inputs` | Committed batch and edge | The batch record (§7): the input log |
| `commits` | Output commit | The commit header (§4) |
| `key_outcomes` | Key with a non-ok outcome in a batch | Key, version, outcome, error, duration |
| Change index layers | Entry | The output log, in the files the planner reads |

**Batch lineage.** Recompute the batch's planned input from its record,
one part of its replaced slice at a time, the way the planner built it
(§7):

- a replaced piece `at(P)` under the batch's conditions:
  `diff(P, head)` over its part of the range;
- a replaced piece under other conditions: `scan(head)` merged with the
  state at P over its part, as the planner does (§9);
- a replaced `empty` piece: `scan(head)` over its part;
- a replaced `held` piece: `scan(head)` merged with the consumer's own
  output at the commit the batch's spec recorded as its own head when it
  was planned. (The batch may make no output commit, so "its output
  commit minus one" does not always exist.) While the record is retained,
  that commit counts toward the cut of the consumer's own index (§13);
- each replaced point: `get(head, key)` against the point;
- an `all` run: every other present key of `scan(head)` in the range, as
  `unchanged`.

The **digest covers the planned key list**, with each key's class and
version at the head: check it on the list just recomputed. Then apply the
points written (dropping held-back keys, replacing deviations by the
versions loaded) to get what the batch actually received. The output side
is the entries of its output commit.

> tally's run 2, batch 1: range (−∞, k2], head c4, replaced (−∞, k2] at c1.
> `diff(c1, c4)` over (−∞, k2] gives k1 1 → 4 and k2 1 → 2, both updated.
> The digest matches. Its output is tally#3.

**Key trace.** Alternate two lookups: `history(key)` on an output gives the
commits that changed the key; for each consumer, the batch records whose
range holds the key say which received it, and at what version.

| Step | Lookup | Finds |
|---|---|---|
| 1 | `history(k2)` on items | c1: added @1. c2: 1 → 2 |
| 2 | tally's records whose range holds k2 | Run 1 batch 1 (head c1, from `empty`); run 2 batch 1 (head c4, from c1) |
| 3 | What each received | k2@1 added; k2@2 updated |
| 4 | Their output commits | tally#1 and tally#3. tally re-keys (a count), so the trace continues at commit level |

**Flow metrics** come from counts stored at commit: keys per hour by class
from `batch_inputs`, full-run frequency from `runs`, reuse from a batch's
count over the upstream's live key count.

**What is stored per key** (**Settled, D195**, P10, Fable D17): store
failures, a duration histogram per batch, and each batch's 10 slowest
keys with their durations. Each batch's ok keys are recomputed by lineage.
Duration per ok key is not stored (100M rows per full run); the histogram
says a batch was slow, and its slowest keys say why, at 0.1% of the rows
for 10k-key batches. Today's `key_outcomes` table logs every call, ok
included, with its duration, so this changes HIS-6.

**Honest answers.** "k1 was at version 1 then; that file has since been
deleted." "Run not retained." A digest mismatch is an error naming the
batch.

## 13. Retention and cleanup

### The cut (D192, P11, Fable D18, staged)

**Settled (D192): a single cut now, preserved commits later, with an alarm
and a way out.** The cut is the oldest of:

- the commits named by any retained run's batch records (replaced slices
  and heads);
- the commits of live pieces;
- the cleanup cursor (below).

Every entry above the cut is kept; below it, entries fold into the base.

**The cut never passes the cleanup cursor.** Cleanup reads each entry's
replaced version, and a clear's replaced keys with `scan(r − 1)`. If the
cut passed a lagging cursor (cleanup is background and can be stuck,
CLN-7, CLN-8), merges would fold away what it still has to delete, and
those files would leak for good. Today's rule has the same guard.

**A paused consumer never loses its place** (CLN-11): the cut never passes
a live piece, so the consumer catches up with one diff, never a full run.
Its cost is index size: every entry since it stopped. The cost lasts
longer than the pause: the batches that catch it up record the old piece
commit in their replaced slices, so every entry since stays until those
runs leave history.

More than a paused consumer holds the cut: a consumer with no automation,
or one run monthly, and a consumer's run retention, since its retained
batch records name upstream commits. `Retention(runs=90)` on a monthly
asset keeps seven and a half years of its upstream's entries. A setting on
`tally` decides the size of `items`' index.

> `copy` is run by hand once a quarter. `items` is refreshed daily, every
> one of 100M keys rewritten. After 90 days `items`' index holds about 9
> billion entries (T44: one such rewrite is about 1.1 GB), and every scan
> reads about 90 entries per key instead of one.

So the single cut comes with two things (review F5):

- **An alarm** when an output's entries above the cut exceed a set
  multiple of its live keys. It names what pins the cut: the consumer and
  its piece, or the run whose records hold it.
- **An operator command that releases a consumer's place.** The released
  consumer owes a full run, and the cut may pass the commits it held. The
  same command can drop lineage for runs older than a date, releasing what
  their records hold.

Until preserved commits are built, the history a paused consumer holds is
unbounded, and the alarm is what watches its cost (`behaviors.md` open
question 12). Fable's refinement bounds that cost to one entry per changed
key: a **horizon** plus **preserved commits**, where merges below the
horizon keep only the states at commits someone still refers to. It is
the same file format with a different merge rule, so it comes later in the
build. Its merges respect the cleanup cursor too: they never combine
entries the cursor has not passed.

### Cleanup of replaced data (exact)

**Settled (D183).** Lagging consumers never read old data files: every
batch plans at the head and loads versions at that head; only the index
needs old commits. So only live attempts hold old data.

**What a live attempt holds.** The head of every output it reads, as of
its spec: its keyed inputs, its whole and dep inputs (the context), and
its own output, which `ctx.load()` reads.

**The rule.** A version replaced at commit r is deleted once r is at or
below the oldest head any live attempt holds on that output, or the head
when none is live. Each entry names what it replaced; a cleanup cursor per
output walks commits in order, as today (D168).

**Example.** items is on FileStore. c4 replaced k1@1 with k1@4. A live
attempt of `checks` holds c3 (it may load k1@1); one of `tally` holds c4.
The oldest held head is c3 < c4: k1@1's file stays. When `checks`' attempt
settles, the oldest is c4: the file goes. `copy`, paused with its progress
at c1, holds nothing. When it runs, it plans at the head and loads k1@4.

*Departs from Fable D15* (the grace period T). Erwin's reason: all the
information is at the engine's disposal, exact cleanup is more efficient,
and sizing T is its own problem. It also deletes today's reader pins (the
claim's pin of index state, durable multi-attempt reads) and the oldest
observation's hold on the cursor.

**Other files.** Index layers replaced by a merge go once the merge
publishes; history queries that lose a layer retry with the current list.
What an abandoned attempt wrote to a store that names objects by version
goes by its version, late, as today (CLN-5). On an in-place store, only
the repair of §8 (D196, rule 2) deletes a dead attempt's rows.

**A removed dynamic partition** (D201). When `sites` drops `west`, the
data of `site_files[west]` follows the rule that already exists for a
removed output: it stays readable for the `cleanup_after` window (a week
by default, which an output may override; CLN-6), then goes. A partition
that comes back within the window picks up where it left off, since its
last head is kept (PAR-6).

### Append-only outputs (D198)

An append-only output keeps everything by default. An output may opt into
a retention that drops its oldest batches. Run retention never deletes
data. A consumer that falls behind the retention edge (its progress points
at data already dropped) owes a full run over what remains: the engine
warns before that happens, marks the consumer "behind retention", and
tells a producer running full over a shortened log that the log is
shortened.

> `events` appends with each commit and keeps 30 days. `digest` reads it
> incrementally and was paused 40 days. Its next run is a full run over
> the 30 days `events` still holds, and its status said so before.

## 14. Today → target

| Today | Target |
|---|---|
| Observation record: base, ranges, points | Progress: pieces and points |
| The fold: ranges relabel to the newest head; changed keys become "version since replaced" points | Deleted (D190) |
| Base before-image (decided, not built) | Dropped: the cut never passes a live piece |
| Stamped layers: newest state, flips, main and side parts, graveyard | MVCC layers: entries with both versions |
| Δ(P, H) at a pinned head; `CutError`; `NotHeld` | `diff(C1, C2)` between any retained commits |
| Deltas' replaced generation, for cleanup only (D168) | In every entry; used by diff, cleanup and staleness |
| Staleness computed on demand, cached per revision | Exact staleness kept current at each commit: exact owed counts, or one witness per input with counts on request (pending, §6) |
| Reader pins; cleanup held by the oldest observation | Cleanup held by the oldest live attempt's head |
| A new life for a reset (unkeyed); a replacement delta (keyed) | No new life: lives only for deleted-and-recreated outputs. A clear for single-value and append-only outputs; for keyed outputs a clear plus the gate, or a rebuild in place (pending, §10) |
| A per-key full run resets to `held` and scans the asset's own output | Nothing resets; the definition or a rebuild counter makes every key owed; `held` only for untrusted progress (D194) |
| Fenced and immutable store kinds; `acquire`, `reads()`, replans and holds | One contract: write returns keys, load returns versions; in-place stores keep one version check per partition inside each write transaction (D196) |
| Repair by presence after a dead writer; the repair clock; readers wait | The next attempt lists the dead attempt's rows by version and repairs them at once; anomalies are alarmed, and the consumer's key is not due until the producer redoes it (D196) |
| Failing keys processed at the version they failed on | Success only; failing keys keep a point; an index of due keys (D191) |
| Every per-key call logged with its duration | Failures, a duration histogram per batch, each batch's 10 slowest keys (D195) |
| A planner and engine in Python, over a Rust index | A new core in Rust behind PyO3; server, worker and SDK stay Python (D203) |
| Workers resolve large deltas | The engine builds every commit's entries |
| **Stay:** key outcomes (D179), `behaviors.md`, a task's progress key (D154, D155), keys in the spec (D178), the commit check, the size rule (P3, Fable D5), merge lanes and back-off (D181), the cleanup cursor (D168), completeness (D177), our block format (D199) | |

## 15. Building and switching

**Settled (D204): build the new core once, beside today's engine, and
switch at a fresh start.** The new core (D203: Rust, behind PyO3) is
written once, next to today's engine, which keeps running with its tests
until the switch. Today's Python planner, progress and staleness are not
reworked toward the target on the way. This design first proposed a
nine-step migration of today's engine; under D203 several of its steps
would have been written twice, once in Python on today's index and again
in Rust on the new one (review F7).

**The switch is a fresh start.** Nothing runs on Solera yet, so nothing is
converted: no engine state, progress, run history or index file survives
the switch, and no compatibility shim or conversion code is written. User
data stays in its stores. The new core starts in a fresh namespace and
refuses one from before the switch (ENG-16, as D169 did for the last index
switch). Every consumer's first run under the new core is a first run.

**Pending a measurement: the core's I/O boundary.** Whether the Rust core
reads and writes object storage itself, or works on bytes that Python
fetched for it. The thread building the index core measures both at the
scale target and decides.

**The order of the work is in a separate build plan** (ERW-9, being
written now as `plans/build-plan.md` in the crew's shared drive). It also
names what the model must check before each part is built (review
F6): the store contract with late and dead writers, the full-run form, and
cleanup against the cut.

## 16. Changes to `behaviors.md`

These are the entries the new core changes. `behaviors.md` is updated, with
executable contract tests, as part of the build (T41). "Pending" marks a
target that depends on a form still waiting for the model's verdict (§6,
§10).

### Entries whose expected behaviour changes

| Entry | Today | Target | From |
|---|---|---|---|
| INC-4 | An update reverted to the processed version is delivered as updated | Not delivered: diff sees equal ends. Stale only between the change and the revert, fresh once the versions match again, with no run spent | D187 |
| INC-8 | Nothing goes back in time | Unchanged rule; on in-place stores it rests on the version check per partition (D196) instead of today's fence protocol | D196 |
| KEY-3 | A Rejected or Failed key is retried when its input changes | Also: if its input reverts to the version the consumer holds, the failure is moot and goes without a retry. Same output, another outcome shown | D191 |
| KEY-5 | A failing key counts as processed at the version it failed on | A failing key is owed (its point keeps what it held) and shows `owed: true`; a key that never succeeded and is removed upstream owes nothing, and its outcome goes | D191 |
| KEY-10 | A full run starts the failing keys over | Same effect without a reset: the new definition or rebuild counter makes every key due, failing ones included, and a key that then succeeds drops its failure | D194 |
| STA-3 | Stale means a default run would load something | Unchanged rule; now explicit that owed failing keys not yet due do not make a partition stale | D191 |
| STA-12 | Computed exactly when asked | Pending (§6). Form (a): kept exact at every commit, condition changes counted at the change, a status asked meanwhile waits. Form (b): stale or fresh kept exact at every commit, the number computed when asked. Either way, "upstream incomplete" has no number where a gate holds the edge | D183, D197 |
| STO-1 | A store is immutable or fenced | One contract, no kinds; stores that write in place keep one version check per partition per write transaction | D184, D196 |
| STO-2 | An immutable store returns exactly the pinned version | A load returns the versions it read; a store naming objects by version returns exactly the planned ones | D184 |
| STO-3 | A batch on a fenced store is classed at the write its read saw; a moved store replans | No replan: keys loaded at another version are processed as loaded and recorded as points | D184 |
| STO-4 | A stale writer on a fenced store changes nothing | Unchanged rule: a write transaction of an attempt older than the partition's version check is refused whole; no `acquire` before it | D196 |
| STO-5 | A fenced partition at rest holds exactly its keys | Unchanged rule for every table: a table never holds a row its log does not have. A dead attempt's rows are repaired by the next attempt | D196 |
| ENG-7 | A worker the engine gave up on cannot change what a newer one committed | Unchanged: by version in the name on object stores, by the version check on tables | D196 |
| ENG-8 | A dead writer on a fenced store is repaired; readers wait | No repair clock and no waiting: the next attempt lists the dead attempt's rows by version and repairs them; a consumer that loads one meanwhile raises an anomaly and its key is not due until the producer redoes it | D196 |
| CLN-3 | A deleted run's late worker writes nothing | On in-place stores it rests on the version check | D196 |
| CLN-5 | What an abandoned attempt wrote is cleaned up, late | Only on stores that name objects by version; on an in-place store the next attempt's repair deletes the rows the log lacks, and the producer rewrites the others | D196 |
| SRC-10 | A tick that saw a source another client moved since is refused whole | With D193, consumer loads move source heads too, so ticks on busy sources would collide more. The check becomes per key: a tick is refused only if a key it reports changed since it observed | D193 |
| HIS-3 | Postgres lineage is the version the read saw, `uncommitted` if no commit made it | Lineage is the batch record: planned versions plus deviations | D184 |
| HIS-5 | Not built | Built: exact per-key lineage while the run is retained | D180 |
| HIS-6 | Every call's outcome is logged, ok included, with its duration | Failures, a duration histogram per batch, and each batch's 10 slowest keys | D195 |
| RST-8 | A removed consumer stops holding back its upstream's history | At once for its pieces; its retained batch records hold the cut until its runs leave history | D180 |
| CLN-4 | A lagging consumer can hold back cleanup | Only attempts in flight hold it back | D183 |
| CLN-6 | A reader pinned before a removal holds that output's cleanup | An attempt in flight from before the removal holds it. A removed dynamic partition's data follows the same `cleanup_after` window | D183, D201 |
| CLN-11 | Not built; today a consumer below the cut gets a full run | Built: the cut never passes a live piece. Open question 12 answered: unbounded index history for a paused consumer until preserved commits are built, watched by an alarm, with a command that releases its place | D192 |
| SEL-11 | A full run over an empty upstream reaches its producer and empties the output | Unchanged. Pending only in how: a clear that writes no key (form (a)), or a first batch that completes the run and removes every leftover (form (b)) | §10 |
| SEL-12 | A due full run may be carried out by several runs, `keys=` ones included | Unchanged rule. Pending: after `keys=(k1)` as the first batch, `copy` holds k1 alone (form (a)), or the new k1 and the old k2, k3 until the run completes (form (b)) | §10 |
| RUN-4 | Waiting reasons: claim, concurrency, merges, engine, executor, invalid | Adds `upstream incomplete` where a gate holds the input (§10) | D197 |
| AUT-4, CHG-4 | Docs contradict whether a pattern change fires `OnChange` (open question 6) | A deploy that changes patterns fires `OnChange` when the change makes something owed; one that owes nothing fires nothing (CHG-7) | D200 |
| CHG-1 | Whether store config is in the definition is open (open question 11) | It is not: the store's version is, its config is not | D202 |
| PAR-6 | A removed partition's head stays readable; its cleanup open (open question 10) | Readable for `cleanup_after`, then cleaned up (CLN-6) | D201 |

### New entries

| Proposed id | Rule | Example | From |
|---|---|---|---|
| INC-15 | An upstream full run never makes a keyed downstream remove a key and add it back; downstream keeps its progress | §10: archive gets k1, k2, k4 updated, k3 removed, k5 added (pending: as one diff after the gate, or as it goes) | P7, §10 |
| STA-13 | Downstream held by a gate runs no batch while its upstream rebuilds. Pending: every reader of an incomplete upstream (form (a)), or only whole-input readers, readers of outputs built up over several batches, and readers that ask to wait (form (b)) | §10: archive and copy's full run | P12, §10 |
| STA-14 | No false alarms: a partition is stale only if something is due; changes that cancel out start nothing | §6: feed's k1 v1 → v2 → v1 | D183 |
| STA-15 | Failing keys not yet due are shown, not stale | §11: checks shows k2 failing, stale only for k3 and k5 | D191 |
| KEY-12 | A key that never succeeded and is removed upstream owes nothing | items adds k6; checks fails on it at its first try; items removes k6: no removal is delivered, and the outcome goes | D191 |
| KEY-13 | A per-key asset's full run resets nothing and never empties its output; a full run asked for by hand raises the rebuild counter, and a canceled one resumes where it stopped | §10: checks holds k1, k2, x9 | D194 |
| CHG-13 | A whole input moving mid-run makes earlier ranges owed again | §9: factor moves during checks' run | D184 |
| HIS-7 | A batch's input is recomputed from its record and checked against its digest, which covers the planned key list; a mismatch is an error naming the batch | §12 | D184 |
| HIS-8 | Lineage lists keys loaded at another version than planned, with the version loaded | §8: tally's point k2 → 5 | D184 |
| CLN-12 | A replaced data file goes once no live attempt planned before its replacement | §13: k1@1 goes when checks' attempt settles | D183 |
| CLN-13 | An output whose index history exceeds a multiple of its live keys raises an alarm naming what pins its cut; an operator can release a consumer's place, which then owes a full run | §13: copy run quarterly over a daily-rewritten items | D192 |
| CLN-14 | An append-only output keeps everything unless it opts into a retention; a consumer behind the retention edge is warned, then owes a full run over what remains | §13: digest paused 40 days over a 30-day events | D198 |
| SRC-12 | A load that finds a source at another version than its log records it in the source's log, unless the log changed that key since the batch's head. Two consumers of a source that changes on every read may trigger each other, bounded by ENG-10 | A source's file changed before its sensor saw it | D193 |
| ENG-17 | A loaded version, or absence, of a produced output that neither its log nor a live attempt explains is alarmed; the consumer's key is not due until the producer redoes it; it never enters the log | §8: tally loads k2@6 from dead attempt A | D196 |
| ENG-18 | An asset whose `ctx.load()` returns a version other than its own last commit owes a full run | §8: tally after a dead write of 5 | D196 |

## 17. Decisions

### The frame (Erwin, 2026-11-10)

Not decisions of the design itself, but the terms every decision below
was taken under:

- **Settle by experiment.** The shape and API are agreed; every remaining
  choice is settled by performance, storage, request counts or a model
  check, and recorded with its evidence.
- **Fresh start.** Nothing runs on Solera yet; no engine state, progress,
  run history or index file is converted, and no compatibility shim is
  written (D204).
- **Scale target.** 100M keys per output, about 1% changing per day,
  10k-key batches, one 4-vCPU engine. Every measurement that settles a
  remaining choice is taken at it; some results quoted here (SOLE-3's
  clustered workloads) were measured at 10M keys.
- **Tables stay exact.** A Postgres table never holds a row the log does
  not have (D196).

### Settled before this round

| Decision | What it fixes here |
|---|---|
| D180 | History exact while a run is retained; diffs and scans between any two retained commits (§4, §12) |
| D183 | The engine plans (§7); exact answers kept current by propagation, no counters that over-count, no false-alarm runs (§6); exact cleanup by the oldest live attempt's head (§13); the file format decided by benchmark, which D199 did (§4) |
| D184 | The store contract (§8); batch records with replaced slice, digest, planner version (§7, Fable D10); context per batch (§9, Fable D11). Its "no fencing" is partly reversed by D196 |
| D179, D177, D178, D154, D155 | Kept as they are: key outcomes, completeness, keys in the spec, a task's progress key, the vocabulary |

### Settled on 2026-11-10

Erwin answered some of these himself. He delegated the rest to the
Captain, to be settled by experiment and recorded with their evidence.
The "Was" column gives the proposal or question each one settles.

| # | Decision | Was | Where | Evidence |
|---|---|---|---|---|
| D187 | Each entry stores the version it replaced | P1 | §4 | Erwin. The engine already computes the replaced version at commit; reverts become exact |
| D188 | One index serves planning and history | P2 | §4 | Erwin. No second copy that can drift; the planner and history read the same files |
| D189 | Keep the 4× merge rule (+1 MiB) | P3 | §4 | Erwin. Today's rule; it bounds any read to about 5× what changed |
| D190 | (1) No fold; (2) points carry explicit versions; (3) each attempt plans at its own head | P4, P5, P6 | §5, §7 | Delegated. Both reviews (W65, SOLE-4) agree; SOLE-4 re-derived every worked example by hand |
| D191 | Progress records successes only; the index of due keys T45 asked for is built | P9 | §11 | Delegated. T45 measured retries, its slowest case, at 7,000 due keys a second |
| D192 | A single cut now, preserved commits later, with an alarm naming what pins the cut and a command that releases a consumer's place | P11 | §13 | Delegated. SOLE-4 F5: a quarterly consumer over a daily 100M-key rewrite holds about 9 billion entries |
| D193 | Loads report what they find into the log, for sources only | P13 | §8 | Delegated. W65's counterexample: on produced outputs it would log a dead write as a change |
| D194 | A per-key asset's definition change or full run resets nothing; a rebuild counter per input, part of the conditions, serves a full run by hand; `held` only for untrusted progress | P8 (Q2) | §9, §10 | Delegated. With D191, progress and the upstream index give the same classes as the reset, without scanning the output (SOLE-4, Q2) |
| D195 | Store failures, a duration histogram per batch, and each batch's 10 slowest keys | P10 (Q3) | §12 | Delegated. 0.1% of the rows at 10k-key batches, instead of 100M rows per full run |
| D196 | Tables stay exact: one version check per partition in each write transaction, a version on every row, an opening write transaction after a dead attempt, anomalies widened and not due, no building on a dead write to one's own output. Replaces P15 and P16; partly reverses D184's "no fencing" | P15, P16 (Q4, Q5) | §8 | Erwin: "Exact tables yes". SOLE-4 F1 reproduced on PostgreSQL 16.15 a late insert that P15's row guard lets through |
| D197 | "Upstream incomplete" without a number is acceptable where a gate holds the edge | Q6 | §6, §10 | Delegated. A number would cost one lookup per rebuilt key for an edge that cannot run |
| D198 | Append-only outputs keep everything by default and may opt into a retention; a consumer behind the retention edge is warned, then owes a full run over what remains | Q7 | §13 | Delegated. Losing data is the user's choice; run retention never deletes data |
| D199 | Our own block format, one entry per changed key | Q8 | §4 | T44: a half to a third of Parquet's requests, scattered gets 1.7 to 2× faster, a fifth of the metadata. SOLE-3: range records and run-length versions do not pay |
| D200 | A deploy that changes patterns fires `OnChange` when the change makes something owed | Q9 | §16 | Delegated. The deploy is the change an `OnChange` author expects a reaction to; a change that owes nothing fires nothing (CHG-7) |
| D201 | A removed dynamic partition's data follows the `cleanup_after` window (CLN-6) | Q10 | §13 | Delegated. One existing knob for data that is no longer declared |
| D202 | Store config is not part of an asset's definition | Q11 | §9 | Delegated. Today's code already leaves it out (`model.py`, `declaration`); a password rotation must not make every asset owe a full run |
| D203 | The new core is written in Rust from the start, behind PyO3; server, worker and SDK stay Python; one crossing per call, never per key | Q13 | §1, §15 | Delegated. T45: the engine's event loop is 82 to 90% busy, mostly with about 5 µs per key of Python classification |
| D204 | Build the new core once, beside today's engine, and switch at a fresh start, with no conversion | The migration plan | §15 | Erwin: a fresh start is fine. SOLE-4 F7: the nine-step plan wrote several steps twice |

**Approved as they stand** (Q12), decisions taken earlier without Erwin:

- **D178:** a batch's keys are read from its attempt spec at commit, not
  copied into the journal; a head's list of dynamic partitions is derived
  from the index.
- **D182:** refines D178: the list stays on the head, computed by the
  engine at each commit (the previous list plus the commit's entries).
- **D181:** a merge that keeps failing backs off (after 3 failures, 10
  minutes, doubling up to 4 hours) and raises an alarm, instead of
  stopping for good.
- **D185:** the format benchmark read both formats through the same I/O
  code, so it compares formats, not readers (§4).
- **D186:** the planner streams: a batch costs its own keys, not the key
  space's (a 10k-key plan at 100M keys takes 1.9 to 3.4 s at the median,
  where a batch took 706 s at 10M keys before).

**Superseded.** D156 (a reverted update is delivered as updated) gives way
to D187 (INC-4). The contradiction between D34 and D140 (2) on pattern
changes and `OnChange` is resolved by D200. `lifecycle.md` §9's "every
store is exact" stands: D196 keeps it.

### Pending

| Choice | Forms | How it is decided | Lean |
|---|---|---|---|
| The form of a full run (P7's clear, P12; review F2) | (a) clear at the start, plus the downstream gate; (b) rebuild in place, remove leftovers at the commit that completes the run (§10) | The model thread checks both; the simpler one that passes wins. Settled either way: no new life for a full run | (b) |
| The form of staleness (P14; review F3) | (a) exact owed counts per piece, updated at every commit; (b) one witness due key per input, counts on request (§6) | A model check for correctness, then a measurement of counting on request at the scale target | (b) |
| The Rust core's I/O boundary | I/O inside Rust, or bytes fetched by Python (§15) | The index-core thread measures and decides | None |

### Open questions from Fable's brief, answered

| Question | Answer |
|---|---|
| Do key-preserving assets have exactly one keyed input? | Yes: a per-key asset has exactly one incremental input and keys every output by it (`sdk.py`, `_check_each`) |
| Context per batch or per run? | Per batch (D184) |
| Stage, then publish, for non-object stores? | No (D184). One version check per partition instead (D196) |
| The default grace period T? | No T (D183) |
| Incomplete upstream: block or warn? | Block, by default, where a gate applies; which readers it applies to depends on the full-run form (§10, pending) |
| Rejected keys stay owed and held back? | Yes (D191): owed, not due |
| Duration per ok key? | Not stored; a histogram and the 10 slowest keys per batch (D195) |
| Circuit breaker for mass failure? | Not now: points and outcomes spill to files. Revisit if a mass failure shows up in practice |
| Unkeyed outputs: how long do they keep appended data? | Everything by default, with an opt-in retention per output (D198) |
| Build aside and swap for full runs? | No. Neither form builds aside: (a) clears and gates, (b) rebuilds in place and removes leftovers at the end (§10) |

## 18. How the examples were checked

A model outside the repo, `~/.solera-design/tracking-model/model.py` on
Erwin's machine, checks the rules and examples of this design's first
forms: form (a) of §6 and form (a) of §10. It implements:

- the output log: entries with both versions and `since`, clears, and
  `diff` and `scan` computed from entries alone;
- progress: pieces (`at` with patterns and context, `empty`, `held`),
  points, decoding, and the batch commit rule with failed keys and keys
  loaded at another version;
- §6's counts: event 1 with whole observations and the `since` shortcut;
  event 2 (a clear: older pieces wait, then one scan when the upstream
  completes); event 3 (the batch-cut split, computed by the rule, not by
  recounting); event 4 (condition changes and `held` resets, by scan).

A random test (3,000 histories, about 67,000 steps; 3 to 6 keys plus an
`x1` some patterns exclude; versions that revert; clears; pattern and
context changes; batches planned a commit or two before they commit)
checks after every step that progress decodes to a brute-force record of
what the consumer holds, that every counted piece's count equals a
brute-force count, and that every diff and scan equals the true states.
All pass. The model also prints the examples of §5, §6 and §10, which
match the tables.

What it does not check: the definition as a condition (it behaves as the
context), `all` and key-list runs, several keyed inputs, the gate itself
(it only holds batches while the upstream is incomplete), cleanup and the
cut, P13 and the store rules of §8, lineage and digests. The checks in §3,
§4, §7, §8, §11, §12 and §13 that the model does not print were done by
hand. The independent review (SOLE-4) re-derived every worked example of
§5 to §13 by hand and found them consistent.

The review also wrote two small random-history models of the forms (b),
kept with its report:

- **Form (b) of §10** (`fullrun.py`): one input, a table as one map,
  three definitions (one drops keys, one renames every key), cancels and
  definition changes mid-rebuild, and an accumulating second output. Over
  3,000 histories (about 74,000 steps, 2,746 completed rebuilds) the
  output always equalled the log, `ctx.load()` never returned a row below
  the floor, any interleaving ended in the right state, and `archive`
  always ended equal to `copy`. Removing the `ctx.load()` filter or the
  final removal makes it fail.
- **Form (b) of §6** (`witness.py`): one input of six keys, reverting
  versions, batches planned several commits before they commit, cancels,
  failing keys, forced retries, pattern and context changes. Over 3,000
  histories (about 55,000 steps) the edge was stale exactly when a key was
  due, and the witness was always due. Disabling any of rules 1 to 5 makes
  it fail.

Neither covers several inputs, key-list runs, the gate, restarts, cleanup
or lineage. The verdicts on the two pending forms come from a model that
checks both forms of each. The review asks that the model move into the
repository and be extended to the store contract with late and dead
writers and to cleanup against the cut, and that the executable contract
tests (T41) come before the parts they cover; the build plan (§15) says
when.
