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

### As built (B7, `native/tracking/src/stale/`)

The witness (D206) is built as follows. Per keyed edge the core keeps its
conditions now (a watch event, journaled when the edge opens and whenever
a deploy changes them), a generation, and what is known: fresh, or stale
with a witness, its reason and since when, exact for the edge's progress
and conditions at one generation, as of one upstream commit.

- **Applying an event never reads the index.** A batch commit, a reset, a
  rename or new conditions start a new generation and record which key
  ranges changed (a reset, a batch that resets progress or new conditions:
  all of them). What is known then stops being current, and the status
  reads "unknown" until a refresh. Generations come from one counter and
  are never reused, so a part for an edge since closed never applies to one
  reopened under its name.
- **At an upstream commit** (rules 1 and 2), the commit builder hands the
  commit's entries to staleness, which adds a witness part to the commit's
  own event for every current edge reading the output: fresh, the first
  entry the patterns take with no point makes it stale; stale, a commit
  that does not touch the witness changes nothing. An edge the entries
  cannot decide (the commit touches its witness, or a point differs) is
  left for a refresh.
- **A refresh** (rules 3, 4, 6, 7) brings an edge current from the index: a
  stale witness is rechecked at the head, and a probe looks for another
  only if it is no longer due; fresh as of commit X, rule 1 runs over the
  diff from X to the head outside the changed ranges (there the edge still
  holds what upstream had at X), then one probe from the first changed
  range; otherwise one probe from the first key. A refresh reads what
  changed since the last exact answer instead of replaying commits in
  order, so rule 7's catch-up never applies an old commit to newer progress
  or conditions.
- **A witness part applies only to the generation it was computed for** and
  never moves X back: a part computed against progress that moved is
  dropped and recomputed. A lost or late notice delays an answer, never
  changes it.
- **Retries** (rule 5): a failed key whose retry came due makes a fresh edge
  stale. The part always starts a new generation, since what is due
  changed with no change to progress: a notice or refresh computed before
  it is dropped, and if one wins the race the edge reads unknown until a
  refresh. At startup the engine applies the rule to every retry already
  due.
- **The gate** (rule 8): one event per output marks its full run started or
  completed; meanwhile readers that keep the gate read "held: upstream
  incomplete", with no count, and when it lifts they are probed again.
- **Counts** are the planner's owed count, cached until the edge's
  generation (its progress or conditions) or its upstream's head moves.
- **One reason per edge**, the witness's. One witness per kind of reason
  (STA-2) is not built yet.

At 100M keys (`bench/core/stale/`): a notice costs 0.08 to 0.18 µs per
edge, and about 100 ns per entry per edge when no entry matters; a count
costs 1.1 s cold for an edge a day behind and 16 s for 30 days behind,
within 6% of the index's own diff for a day.

## 7. A batch from start to finish

**Settled (D183): the engine plans**, because its warm on-disk index cache
makes planning cheap there. *Departs from Fable D8* (workers plan from