# Tracking what flows along edges: the target design

Status: **target design (T43), decided.** It is the design the new core is
built to. It starts from Fable's brief (*Incremental data flow tracking:
architecture brief*, Oct 5 2026) and applies Erwin's decisions on it
(D180, D183, D184). Where it departs from the brief, it says so and why.
It was revised after a first review (W65, A40), which fixed the owed-count
rule under conditions (§6) and the cut against the cleanup cursor (§13),
and again after an independent review (SOLE-4, findings F1 to F8), which
replaced the rules for late and dead writes on tables (§8) and offered a
simpler form of two mechanisms (§6, §10). A model in the repository
(`tests/tracking`, §18) then chose between the forms of those two
mechanisms.

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

Two kinds of label appear below:

- **Settled.** Decided, with its decision number: D180, D183, D184 from
  before, D187 to D204 from Erwin's answers of 2026-11-10, and D205 to
  D209 from the experiments and the build that followed. §17 lists each
  with its evidence. Nothing in this design is pending.
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
| Staleness from counters that may over-count; a false alarm costs an empty batch | Answers are always exact, kept current by propagating each commit to the edges that read it; no false-alarm runs. Each edge keeps one key known to be due; counts are computed when asked (§6) | D183, D206 |
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
a commit), never once per key. The core does its own I/O (D208), and the
engine builds every commit itself, never a worker (D207).

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
| Clear | A commit flag: "everything present before is removed, then this commit's entries apply". The first commit of every full run of an ordinary asset carries it (§10). |
| Gate | The rule that a consumer runs no batch over an input whose upstream is incomplete: mid full run or mid first run (§10). |
| Edge | One keyed input of one consumer partition, reading one upstream output. |
| Conditions | What a key was processed under: the input's patterns, the context (the heads of the consumer's whole and dep inputs), the asset's definition, and the input's rebuild counter. |
| Rebuild counter | A number per input, part of the conditions. Raising it makes every key owed: a full run asked for by hand of a per-key asset (§10). |
| Progress | Per edge, what the consumer holds for every key. Stored as **pieces** (key ranges with one value each) and **points** (single keys with an explicit value). Replaces today's *observation record*. |
| Owed | A key whose upstream state now, under the conditions now, differs from what progress says the consumer holds. |
| Due | Owed, and a default run would take it now. A failing key waiting for its retry is owed but not due (§11). |
| Witness | One key an edge knows to be due, kept exact at every upstream commit; an edge with none is fresh (§6). |
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
   index (D184), on a thread off its event loop. No commit is handed to a
   worker, however large (D207, §7). *Departs from Fable's rule 3* (a
   writer whose parent moved rebuilds and retries): with the engine
   building entries in commit order, the parent is always the head.
4. **A commit may be a clear.** Only the first commit of a full run of an
   ordinary asset is one (§10). Its own entries still carry the true
   replaced version. It is one flag, not 100 million removal entries.

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
so the planner always knows.

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

A batch's range always starts at a piece boundary: a run's first batch
starts at −∞, and the run's progress key stays a boundary until the run
ends (equal neighbours merge only then). A batch that planned at head H
and covered (lo, hi] writes one piece:
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

**Settled (D206): one witness due key per input** (review F3), checked in
the model (§18). Each edge keeps one key known to be due, or the fact that
nothing is due, and keeps that exact at every upstream commit. "How many
keys?" is counted by the planner when someone asks, and cached. The exact
owed counts per piece this design first proposed (P14) are not built.

| | Today | The witness |
|---|---|---|
| "Is `tally` stale?" | The planner's comparison, run when asked, cached | The edge has a witness |
| Work per upstream commit | None until asked | Entries up to the first that matters while fresh; while stale, only "does this commit touch the witness?" |
| "How many keys?" | Counted when asked | Counted by the planner when asked, then cached |
| Code that decides what is owed | The planner | The planner, still the only one (STA-3) |
| After a restart | Nothing to rebuild | Compare the stored commit with the head, and re-arm the retries already due |
| A revert (k1: v1 → v2 → v1) | Reads as changed (INC-4) | Witness set, then dropped; no run |

**Why not the counts.** The model checked both forms over 5,000 random
histories of 40 steps each. Both answer exactly once fixed, but P14 as
written miscounted in three cases (a piece committed under changed
conditions, the split of a recounted piece, key-list runs), and needed a
rule for each. The witness keeps one key and one commit number per input,
where P14 keeps a count per piece plus a running sum per live attempt; it
takes 124 lines of the model against 150; and over 3,000 histories it read
4,079 upstream entries where P14 adjusted 43,860. P14's one advantage, a
number that is always there, only the console would use. Still to be
measured, by the build (unit B7): the cost of a count on request at the
scale target, for a consumer far behind on a 100M-key output.

### What is stored

Each edge stores one of two states. Each carries X, the upstream commit it
was last brought up to.

- **fresh(X)**: nothing is due as of X.
- **stale(w, reason, since, X)**: key w, the **witness**, is due as of X,
  for this reason, and the edge has been stale since that moment.

To keep STA-2 ("a stale partition says every reason that holds"), the
edge keeps one witness per kind of reason: input changed, conditions
changed, never processed, retry due.

### The rules

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
   the probe's work: any key it takes is the witness. **A batch that
   resets progress** (a full run's first batch, §10) **probes the whole
   input**, not only its range: it is the one batch that changes keys
   outside its range. A cancel changes nothing the consumer holds.
4. **A change of conditions** (patterns, context, definition, rebuild
   counter), **or a reset of progress to `held`**: probe. When every
   present key is owed, it stops at the first key it meets.
5. **A retry comes due** by time, or is forced: that key is the witness if
   the edge was fresh. This reads the index of due keys (§11). **At
   startup**, the engine applies this rule to every retry already due in
   that index, since a timer that fired before a restart may not have been
   recorded.
6. **The probe** returns a witness, and the edge is stale with it; or it
   returns nothing, and the edge is fresh as of the head.
7. **Asking, and catching up.** Compare X with the upstream's head; if the
   head is newer, run the commits in between through rules 1 and 2, in
   order. A lost notification can delay an answer by one comparison, never
   change it. **Every other rule starts with this catch-up:** rules 3, 4
   and 5, every change of conditions, and every reset of progress first
   bring the edge up to the head, and only then apply the batch, the
   change or the reset. In the other order, after a lost notification,
   rule 1 would apply an old commit to newer progress or newer conditions
   and name a key that is not due.
8. **A gate** (§10): while the upstream is mid full run, the edge reads
   "held: upstream incomplete", exact but with no key and no number
   (D197). When the upstream completes, one probe.

A batch in flight when its upstream clears also follows §10's hold-back
rule: it holds back any key it loaded at another commit than it planned,
and that key keeps its old point and stays owed.

### Examples

**`tally` through §3's story.**

| Event | The witness |
|---|---|
| After run 1, batch 1 (k1, k2 at c1) | stale, witness k3: never processed |
| c2: k2 1 → 2, k4 added | c2 does not touch k3: no work |
| After run 1, batch 2 (k3, k4 at c2) | k3 processed; the probe finds k2, changed at c2: witness k2 |
| c3: k3 removed, k5 added | no work |
| c4: k1 1 → 4 | no work |
| After run 2, batch 1 (k1, k2 at c4) | k2 processed; the probe finds k3, removed at c3: witness k3 |
| Someone asks "how many?" | the planner counts k3 and k5: 2 |

**A revert raises no alarm.** `items` is fresh on `feed` at f1. f2 changes
k1 from v1 to v2: rule 1 makes k1 the witness, with no lookup. f3 changes
it back: rule 2 rechecks k1, finds that `items` holds v1 and upstream has
v1, and probes. The probe finds nothing: `items` is fresh, and no run
started. `items` was truly stale between f2 and f3.

**An excluded key.** `copy` takes `k*` only and is fresh. `items` commits
only `x1`; rule 1 skips it, and `copy` stays fresh (CHG-9).

**The context moves.** `checks` is fresh, holding k1 and k2 processed under
`factor` at w1. `factor` moves to w2. Rule 4 catches up, then probes; the
first key it meets, k1, is owed as updated: witness k1, reason "conditions
changed". The count, if asked, is 2 (CHG-10).

**A failing key.** `checks` fails on k2 and keeps the point "k2 at version
1". k2 is owed but not due, so it does not make `checks` stale by itself.
When `items` changes k2 again, rule 1 compares the new state with the point
and makes k2 the witness if it is due; when k2's backoff ends, rule 5 does.

**A lost notification.** `items` c1 adds k1, k3, k4, and the engine
restarts before `checks` hears of it. `checks` plans and commits k1 and k3.
Rule 3 first catches up: it replays c1 against the progress from before
the batch, which names k1; then it applies the batch, which processed k1,
so it probes and finds k4. In the other order, rule 1 would replay c1
against the new progress and name k3 without a lookup; k3 was already
processed, so the witness would not be due, and if no later commit
touched k3, `checks` would read stale for good.

### Cost

An upstream commit costs entries up to the first that matters while
fresh, and one check while stale; a 100M-entry commit usually stops at its
first entry. A consumer a month behind costs nothing at a commit that does
not touch its witness. The expensive cases are a commit whose entries the
patterns all leave out (read in full), and a probe that finds nothing,
which walks every piece once against the head. The worst is a consumer far
behind whose few owed keys revert one after another, so that each probe
walks its lag; a probe that resumes after the old witness spreads one walk
over all of them. A count on request reads everything that changed since
the oldest piece: T44 measured that read for a consumer paused 30 days on
a 100M-key output at about 2 to 23 s from cold storage, about 8 s for
scattered daily changes.

### What follows

| Question | Answer |
|---|---|
| Is anything due on this edge? | The edge has a witness |
| Why? | The reason of each witness kept, one per kind: input changed (behind the head), conditions changed (patterns, context, definition or rebuild counter), never processed (`empty`), retry due; or "upstream incomplete" while a gate holds the edge |
| Is the partition stale? | Any of its edges has a witness (STA-3: a default run would load something) |
| Upstream stale | A partition whose stale status flips tells its consumers, which tell theirs: work only where the status changes |
| Complete? (D177) | For each `empty` piece, whether it has an upstream key: one key or none |
| How many keys owed? | Counted when asked: the keys an unlimited plan would take plus the owed keys not yet due (failing keys waiting for their retry, and keys held back while their producer repairs an anomaly, §8), each counted once; cached until the upstream, the progress or the conditions (patterns, context, definition, rebuild counter) change. No number while a gate holds the edge (D197) |
| How many commits behind, since when? | The oldest owing piece's commit, against the head and the commit headers; "since" from the witness |
| Which keys? | The planner without a limit, on demand; costs a read proportional to the answer |

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
nothing wakes nothing (INC-14). It still commits its progress. A clear is
always a change, so a full run's first batch commits even when it writes
no key (SEL-11).

### Who builds a commit

**Settled (D207): the engine builds every commit, and nothing goes to a
worker** (review F4 asked). The engine reads the replaced versions from
its index and writes the commit's layer on a thread off its event loop.
Measured at the scale target (100M keys, one 4-vCPU machine):

| Commit | CPU | Storage requests | Memory |
|---|---|---|---|
| A 10k-key batch in one key range, cold | about 10 ms | 11 to 15 GETs, one 62 KB PUT | |
| A 100M-key commit (a full rewrite), streamed | about 30 s | | 579 MB peak |
| A 10k-key batch scattered over the whole output, cold | 2.6 to 2.9 s | about 2,850 GETs | |

The one miss is the scattered batch, and all of its cost is the index's
`get` walking whole blocks it was not asked about; a `get` that skips
them should bring it to 0.5 to 1 s. Sending such commits to a worker would
not make them cheaper, only move them, and it would add a second commit
builder, a copy of the layer list on workers, and a rebuild whenever the
head moved in between. The fix belongs in `get`.

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
   processed. This hold-back and the one of §10 (a key loaded at another
   version than planned while its upstream rebuilds) are one rule, built
   once. The anomaly never enters the producer's log. A version of an
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

**What a pattern is.** An input's `include` and `exclude` patterns pick
the upstream keys it takes: a key is taken when it matches an include (any
key, without one) and no exclude. A pattern is a glob over the key string
(`**` crosses `/`, `*` and `?` do not) or a `Regex("…")`.

**Settled (D209): a `Regex` uses the Rust `regex` dialect, matched against
the whole key.** The SDK validates it when the input is declared, through
the same native matcher the core uses, and refuses lookaround and
backreferences with an error that names what the dialect lacks. Today's
Python path matches through that matcher too, never through Python's
`re`. Python's
`re` and Rust's `regex` differ in syntax and in Unicode classes, so two
dialects would let the SDK and the core disagree on which keys are owed;
matching in Python instead would cross into Python once per key (D203).
The Rust dialect also matches in linear time. Erwin should know this one:
it is visible to users, though nothing breaks, since Solera has none yet.

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
| Key list | Exactly the listed keys | Points only; as a full run's first batch, also the clear and the reset (SEL-12) | The listed keys are done |
| Full | Everything. An ordinary asset resets its inputs' progress at its first batch; a per-key asset resets nothing (D194) | The reset and the clear, if any, then a piece per batch | An ordinary asset's unreached part stays `empty`; a per-key asset's unreached keys stay owed |

### Key list

`tally`'s progress is `(−∞, +∞) at c2`; the head is c4. Owed: k1 (1 → 4),
k3 (removed), k5 (added). The user runs `keys=(k1, k5)`: the commit writes
points `k1 → 4` and `k5 → 3`, and the piece stays at c2. Owed afterwards:
k3 only. `diff(c2, c4)` still names k1 and k5, but their points equal
upstream now. The next incremental run covers the range and drops both
points.

A key list writes a point for every listed key, even one that ends absent
and even on an `empty` piece, so completeness (D177) counts it as
processed. An empty key list, `keys=()`, processes nothing; it is not a
range run.

### Full run of an ordinary asset

**Settled (D205): the clear at the first batch, plus the gate** (P7, P12),
chosen by the model (§18). It holds for every ordinary output: keyed,
single-value and append-only alike.

**No new life.** A full run keeps the output's log and its numbering, and
downstream keeps its progress. A new life is only for an output deleted
and declared again (a removal, a move to another store). Today, resets of
unkeyed outputs start a new life (D178), so a full run cascades down the
graph as full runs, and a keyed full run writes a replacement delta of
every removed key. For keyed outputs that cascade goes. An append-only
upstream's clear still makes its consumer run full (INC-13 stays).

1. **The first batch plans on reset progress.** Every key counts as added,
   and the producer is told `batch.reset`, as today. The run's progress
   key resets with progress. If the first batch fails, nothing happened.
2. **The first batch's commit is a clear.** In one journal event it
   carries the clear flag and resets the asset's inputs' progress to
   `empty`. On Postgres it deletes the partition's rows. Later batches are
   ordinary. Downstream sees one diff across the clear (two scans, §4).
   Versions are never reused, so a rebuilt key can never look unchanged by
   accident.
3. **The gate.** While an upstream is incomplete (mid full run, or a first
   run not yet complete: an `empty` piece still owes a key), every reader
   of it runs no batch over it, by default: keyed readers, readers of an
   output that accumulates (a count kept through `ctx.load()`), and readers
   that take it whole. The engine checks this before every spec, so a
   downstream run already in flight pauses too ("held: upstream
   incomplete"). Each input can opt out if it wants partial data.
4. **Hold back what a rebuild changed under a batch.** When a reader's
   batch commits, it holds back every key it loaded at another version
   than it planned, if its upstream is incomplete, **or if a clear has
   come since the batch planned** (in the commits after its head H), even
   when that rebuild has completed by the time the batch commits. The key
   keeps its old point and stays owed; a later batch retries it. On stores
   that name objects by version a load is exact, so this never fires. On a
   table it is the same rule as §8's rule 4, and is built as one rule with
   it. To tell which rebuild, if any, a key loaded as absent belongs to,
   the engine looks at the commit that last removed it (by a clear or by
   an ordinary removal): the last removal at or before H when the batch
   planned the key absent, and, only on a table for a key planned present
   that loads absent, the last removal at load time. It records this when
   the batch loads, not when it commits.
5. **A canceled or failed full run** leaves the upstream incomplete, and
   nothing resumes a run by itself (RUN-10). It raises an alarm, and one
   operator command lets downstream proceed over the partial output.

**Example: the gate.** `copy` holds `copy#1`: k1@1, k2@1, k3@1, k4@1
(copied from c2). Its definition changes, so it runs full at c4, where
`items` holds k1, k2, k4, k5, with `batch_size=2`. Batch 1 commits
`copy#2`: a clear, re-adding k1@2 and k2@2. Batch 2 commits `copy#3`: k4@3
and k5@3. `archive` sits at `copy#1`.

| | archive's input |
|---|---|
| Without the gate, at copy#2 | k1, k2 updated; **k3, k4 removed** |
| … then at copy#3 | **k4 added**, k5 added |
| With the gate, one diff(copy#1, copy#3) | k1, k2, k4 updated; k3 removed; k5 added |

Without the gate, `archive` removes k4 and adds it back, and its output
lacks k4 in between. During the full run, `archive`'s reason is "upstream
incomplete", with no number (D197), and one probe follows when copy#3
completes the run (§6).

**Example: the hold-back, on a table.** Before copy's definition changes,
`archive` has planned a batch at copy#1 that takes k3 and k4 as updated.
`copy`'s full run then commits copy#2, which deletes every row and writes
k1 and k2. `archive`'s batch loads k3 and k4 and finds them absent. The
gate does not stop a batch already in flight, but rule 4 does: k3 and k4
were loaded at another version than planned while copy was incomplete, so
both are held back, keep their points, and stay owed. After copy#3,
`archive` gets k4 updated and k3 removed, with nothing removed and added
back. Without the rule, `archive` would remove k4 and `copy#3` would bring
it back: the churn the gate exists to prevent. The same rule keeps a
reader of `tally` from loading a count the rebuild has half built.

**What the other form would have bought, and why it lost.** The review
proposed rebuilding in place instead: each batch overwrites its keys, the
old rows stay visible behind a "rebuild floor", and the commit that
completes the run removes every key the run did not write again. The model
checked both over 5,000 histories of 60 steps, on file stores and on
tables, and both pass every invariant. What the rebuild in place buys is
availability: a plain keyed reader is never held (over 2,000 histories the
model held `archive` 5,499 times under the clear, never under the floor),
a table stays full during a rebuild, and a canceled rebuild freezes
nothing downstream. It lost on Erwin's rule, the simpler form that passes.
The clear needs 6 rules against 7; the floor still needs the clear and its
gate for single-value and append-only outputs, so it adds a second
mechanism; and under the floor a reader of an accumulating output loaded
a half-built count in 325 of 600 histories unless the output was declared
as accumulating, so correctness would rest on a declaration by the user
and the agreed API would grow. The clear also writes 17% fewer log
entries.

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
| Key outcome | k2 failed, 1 try, failed on k2@2 (the version the batch loaded, not the head's) |

k1 is not held up: the piece moved on. k2 is **owed** (holds 1, upstream
2) but not **due**: it waits for its retry: new input, its backoff, a new
deploy, or a forced retry (KEY-3). `checks` is stale for k3 and k5, and
shows one failing key.

**Why success only.** A retry is classed right (a key that never succeeded
comes as added); a key that never succeeded and then vanishes upstream owes
nothing; and progress always describes the output. That last property is
what lets a per-key asset rebuild without a reset (D194, §10).

**When the point and the outcome entry go.** Both go when the key
succeeds, when it is processed as removed or unmatched, and at a reset. An
outcome never outlives its point: the witness (§6) relies on every failing
key having one. The outcome alone also goes once the key is no longer
owed: upstream went back to what the consumer holds, or the conditions no
longer take the key. The failure is then moot, and nothing is retried. The
engine checks this at every upstream commit, at every batch commit, and
after every change of patterns, context or definition. The **point
stays**: without it, the piece would say the consumer holds the version it
failed on. The next batch over its range drops it, because it then equals
what the new piece says.

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
| Staleness computed on demand, cached per revision | One witness due key per input, kept exact at each commit; counts on request (D206) |
| Reader pins; cleanup held by the oldest observation | Cleanup held by the oldest live attempt's head |
| A new life for a reset (unkeyed); a replacement delta (keyed) | No new life: lives only for deleted-and-recreated outputs. A clear at the first batch, the gate on every reader, and the hold-back rule (D205) |
| A per-key full run resets to `held` and scans the asset's own output | Nothing resets; the definition or a rebuild counter makes every key owed; `held` only for untrusted progress (D194) |
| Fenced and immutable store kinds; `acquire`, `reads()`, replans and holds | One contract: write returns keys, load returns versions; in-place stores keep one version check per partition inside each write transaction (D196) |
| Repair by presence after a dead writer; the repair clock; readers wait | The next attempt lists the dead attempt's rows by version and repairs them at once; anomalies are alarmed, and the consumer's key is not due until the producer redoes it (D196) |
| Failing keys processed at the version they failed on | Success only; failing keys keep a point; an index of due keys (D191) |
| Every per-key call logged with its duration | Failures, a duration histogram per batch, each batch's 10 slowest keys (D195) |
| A planner and engine in Python, over a Rust index | A new core in Rust behind PyO3, doing its own I/O; server, worker and SDK stay Python (D203, D208) |
| `Regex` key patterns in Python's `re` dialect | The Rust `regex` dialect, checked at declaration (D209) |
| Workers resolve large deltas | The engine builds every commit's entries, never a worker (D207) |
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

**Settled (D208): the core does its own I/O.** It reads and writes object
storage itself (the `object_store` crate), with one shared memory cache,
fetches of the same block made once however many queries ask for it, and
an on-disk cache of whole layer files. Its calls are synchronous and
release Python's global lock; the engine caps how many heavy queries run
at once. Measured with 50 concurrent queries on local files, on S3, and on
S3 with 30 ms added: the two choices were equal on small queries. On 1,000
scattered gets, having Python fetch the bytes cost 100 to 120 µs more CPU
per request, stalled the engine's event loop for up to 0.76 s (under
60 ms with Rust doing the I/O), and was 3 to 93% slower. The cost is 60 s
more release build and 8 MB more extension.

**The order of the work is in a separate build plan** (ERW-9,
`plans/build-plan.md` in the crew's shared drive). It also names what the
model must check before each part is built (review F6). The full-run and
staleness forms are checked (§18); stores that write in place with late
and dead writers, cleanup against the cut, and lineage are next.

## 16. Changes to `behaviors.md`

These are the entries the new core changes. `behaviors.md` is updated, with
executable contract tests, as part of the build (T41).

### Entries whose expected behaviour changes

| Entry | Today | Target | From |
|---|---|---|---|
| INC-4 | An update reverted to the processed version is delivered as updated | Not delivered: diff sees equal ends. Stale only between the change and the revert, fresh once the versions match again, with no run spent | D187 |
| INC-8 | Nothing goes back in time | Unchanged rule; on in-place stores it rests on the version check per partition (D196) instead of today's fence protocol | D196 |
| KEY-3 | A Rejected or Failed key is retried when its input changes | Also: if its input reverts to the version the consumer holds, the failure is moot and goes without a retry. Same output, another outcome shown | D191 |
| KEY-5 | A failing key counts as processed at the version it failed on | A failing key is owed (its point keeps what it held) and shows `owed: true`; a key that never succeeded and is removed upstream owes nothing, and its outcome goes | D191 |
| KEY-10 | A full run starts the failing keys over | Same effect without a reset: the new definition or rebuild counter makes every key due, failing ones included, and a key that then succeeds drops its failure | D194 |
| STA-3 | Stale means a default run would load something | Unchanged rule; now explicit that owed failing keys not yet due do not make a partition stale | D191 |
| STA-12 | Computed exactly when asked | Stale or fresh kept exact at every commit by one witness per input; the number of keys owed is still computed exactly when asked; "upstream incomplete" has no number while a gate holds the edge | D183, D197, D206 |
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
| SEL-11 | A full run over an empty upstream reaches its producer and empties the output | Unchanged; the clear is a commit even when it writes no key | D205 |
| SEL-12 | A due full run may be carried out by several runs, `keys=` ones included | Unchanged; a `keys=` run as a full run's first batch writes the clear, the reset and its points, so `copy` holds k1 alone | D205 |
| RUN-4 | Waiting reasons: claim, concurrency, merges, engine, executor, invalid | Adds `upstream incomplete` where a gate holds the input (§10) | D197 |
| AUT-4, CHG-4 | Docs contradict whether a pattern change fires `OnChange` (open question 6) | A deploy that changes patterns fires `OnChange` when the change makes something owed; one that owes nothing fires nothing (CHG-7) | D200 |
| CHG-1 | Whether store config is in the definition is open (open question 11) | It is not: the store's version is, its config is not | D202 |
| PAR-6 | A removed partition's head stays readable; its cleanup open (open question 10) | Readable for `cleanup_after`, then cleaned up (CLN-6) | D201 |

### New entries

| Proposed id | Rule | Example | From |
|---|---|---|---|
| INC-15 | An upstream full run reaches downstream as one change; downstream keeps its progress, and the rebuild alone never makes it remove a key and add it back (a key upstream really removed and re-added still is) | §10: archive gets k1, k2, k4 updated, k3 removed, k5 added in one diff | D205 |
| INC-16 | A reader's batch holds back every key it loaded at another version than it planned, while its upstream is incomplete or if a clear came since it planned; the key stays owed | §10: archive's batch in flight loads k3, k4 absent after copy's clear | D205 |
| STA-13 | Every reader of an incomplete upstream (keyed, accumulating, whole) runs no batch over it, by default; an input may opt out. A canceled or failed full run raises an alarm, and one command lets downstream proceed | §10: archive is held while copy's full run is between batches | D205 |
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

### Settled by experiment, and during the build (2026-11-10)

| # | Decision | Was | Where | Evidence |
|---|---|---|---|---|
| D205 | Full runs: the clear at the first batch, plus the gate on every reader of an incomplete upstream (keyed, accumulating, whole), with an opt-out per input; a reader's batch holds back what it loaded at another version than planned while its upstream is incomplete or if a clear came since it planned; a canceled or failed full run raises an alarm and has one command that lets downstream proceed. No new life | P7, P12 (review F2) | §4, §7, §10 | Model (`tests/tracking`): both forms pass every invariant over 5,000 histories × 60 steps on file and table stores; the clear needs 6 rules where the floor needs 7 plus an SDK declaration, writes 17% fewer entries, and needs no second mechanism for single-value outputs. The rebuild in place would have kept readers available (archive held 5,499 times in 2,000 histories under the clear, 0 under the floor) |
| D206 | Staleness: one witness due key per input, kept exact at every upstream commit, counts on request; with the model's fixes (catch up first, a reset probes the whole input, a reset to `held` probes, a restart re-arms due retries, an outcome never outlives its point). P14's eager counts are not built | P14 (review F3) | §6, §11 | Model: both forms exact over 5,000 histories × 40 steps once fixed (P14 as written miscounted in three cases); one key and one commit number per input; 124 model lines against 150; 4,079 entries read against 43,860 adjusted. Count-on-request cost at scale: still to be measured, by unit B7 |
| D207 | The engine builds every commit, on a thread off its event loop; nothing goes to a worker | Review F4 | §4, §7 | SOLE-12 at 100M keys: a 10k-key batch in one range about 10 ms CPU, 11–15 GETs cold, one 62 KB PUT; a 100M-key commit streamed about 30 s CPU, 579 MB peak. The scattered 10k-key batch (2.6–2.9 s) is the index's `get`, to be fixed there |
| D208 | The core does its own I/O in Rust (`object_store`), with one shared memory cache, single-flight fetches and an on-disk cache of layer files; synchronous calls that release the GIL; heavy queries capped by the engine | The I/O boundary | §1, §15 | SOLE-6, 50 concurrent queries on local files, S3 and S3 + 30 ms: equal on small queries; on 1K scattered gets Python fetching cost 100–120 µs more CPU per GET, stalled the event loop up to 0.76 s (Rust under 60 ms) and was 3–93% slower. Cost: +60 s release build, +8 MB |
| D209 | A `Regex(...)` key pattern uses the Rust `regex` dialect, matched against the whole key, validated at declaration by the native matcher, which today's Python path also matches through; lookaround and backreferences are refused | A contract choice in the build (SOLE-11) | §9 | Two dialects would make the SDK and the core disagree on which keys are owed; matching in Python would cross once per key; the Rust dialect is linear-time. Pre-usage, so no pattern breaks. Visible to users |

Nothing in this design is pending. What remains to measure (the cost of a
count on request at the scale target, among others) is listed with the
unit that measures it in the build plan (§15).

### Open questions from Fable's brief, answered

| Question | Answer |
|---|---|
| Do key-preserving assets have exactly one keyed input? | Yes: a per-key asset has exactly one incremental input and keys every output by it (`sdk.py`, `_check_each`) |
| Context per batch or per run? | Per batch (D184) |
| Stage, then publish, for non-object stores? | No (D184). One version check per partition instead (D196) |
| The default grace period T? | No T (D183) |
| Incomplete upstream: block or warn? | Block every reader by default, with an opt-out per input (D205) |
| Rejected keys stay owed and held back? | Yes (D191): owed, not due |
| Duration per ok key? | Not stored; a histogram and the 10 slowest keys per batch (D195) |
| Circuit breaker for mass failure? | Not now: points and outcomes spill to files. Revisit if a mass failure shows up in practice |
| Unkeyed outputs: how long do they keep appended data? | Everything by default, with an opt-in retention per output (D198) |
| Build aside and swap for full runs? | No: the clear plus the gate (D205) gives the same consistency downstream without double storage |

## 18. How the design is checked

**The model is in the repository: `tests/tracking/`** (its README, and
`docs/verification.md`, "The tracking model"). It is a brute-force model
in Python of this design, not of the engine. It chose the two forms D205
and D206 by model check, and the new core's units test against it as an
oracle (`tests.tracking.oracle`: replay a history, then ask what a
consumer holds, what it owes, what is due and what the next plan takes).
It keeps only the decided forms; the rejected ones (P14's counts and the
rebuild in place) are in the history of solera#15. It replaces the
design's first model, which lived outside the repository and checked the
earlier forms.

```bash
uv run pytest tests/tracking -q            # CI: 200 random histories per store, every short staleness history, every calibration (~20 s)
uv run pytest tests/tracking -q --slow     # 5,000 random histories per store
uv run python -m tests.tracking fullrun --kind table --off hold_back --shrink   # the shortest history that breaks without a rule
```

**What it models.** The output log: entries with both versions (D187),
clears, and `diff`, `scan` and `get` computed from entries alone, checked
against the true states at every pair of commits. Progress: pieces
(`at(H, conditions)`, `empty`, `held`) and points, decoding, a planner
that reads candidates piece by piece (§7), and the batch commit rule (§5)
with failing keys (D191), keys loaded at another version, key-list runs,
and batches that start at a piece boundary (asserted).

- **Staleness** (`staleness.py`): `checks` over `items`, through upstream
  commits that revert, an upstream full run, batches planned and
  committed later, failing keys owed but not due, retries by timer and by
  force, pattern, context and definition changes, key-list runs, a full
  run's resetting first batch, a reset to `held`, lost notifications and
  restarts. After every step: progress decodes to what `checks` really
  holds; the planner's candidates equal the brute-force owed set; the
  answer is "held" exactly while `items` is mid full run, else stale
  exactly when some key is due; the witness is due; completeness is
  exact; rule 1 never takes a key that is not due. CI also checks every
  history of up to four events (about 280,000).
- **Full runs** (`fullrun.py`): `items` and `feed` drive `copy` (an
  ordinary keyed copy whose definitions keep, drop or rename keys) and
  `tally` (a count per group kept through `ctx.load()`); `archive` reads
  copy, `report` reads tally, `summary` reads copy whole, and `checks` is
  per-key under the amended P8 (D194). Histories add definition changes,
  full runs by hand, `keys=` runs (also as a first batch), cancels,
  restarts that kill every attempt in flight, and a reader's load as its
  own step, on file stores and on tables. After every step: each log
  equals its store; every consumer's progress decodes to what it
  processed; a plan takes exactly the first due keys; completeness is
  exact; no reader processes a row of an unfinished rebuild; no keyed
  reader is given a key removed and then added back within one rebuild,
  unless `items` really removed and re-added it meanwhile;
  `summary` never reads a copy that is incomplete or mixes definitions.
  Once quiet, every output equals what its definition gives from its
  inputs.

**Every rule is needed.** Each rule is named, and `histories.py` holds one
shrunk history per rule that passes with every rule on and fails with
that rule off; CI runs them all. The rules the model had to add to the
design are written into §6 and §10: for the witness, catching up first, a
reset that probes the whole input, a reset to `held` that probes, re-arming
due retries at a restart, and the hold-back; for full runs, the
hold-back.

**What it does not check yet:** stores that write in place with late and
dead writers (§8), cleanup against the cut (§13), lineage and digests
(§12), several upstream levels rebuilding at once, one witness per kind of
reason (STA-2), the propagation of "upstream stale" down the graph, and the
cost of any of it at the scale target. The first three are the model's
next phase, before the units that build them (§15). The checks in §3, §4,
§7, §8, §12 and §13 that the model does not cover were done by hand; the
independent review (SOLE-4) re-derived every worked example of §5 to §13
and found them consistent.
