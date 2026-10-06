# Tracking what flows along edges: the target design

Status: **target design (T43), for Erwin's review.** It is the plan the
rebuild follows after the observed-set work. It starts from Fable's brief
(*Incremental data flow tracking: architecture brief*, Oct 5 2026) and
applies Erwin's decisions on it (D180, D183, D184). Where it departs from
the brief, it says so and why.

Two kinds of decision appear below, always marked:

- **Settled.** Decided by Erwin, with its decision number (D180, D183,
  D184). Stated as decided.
- **Proposed.** This design's recommendation, waiting for Erwin's
  confirmation. Numbered P1 to P15 and listed with a recommendation in
  §17.

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
| Staleness from counters that may over-count; a false alarm costs an empty batch | Answers are always exact, kept current by propagating each commit to the edges that read it; no false-alarm runs | D183 |
| A grace period T: replaced files live T hours; attempts older than T are refused | Exact cleanup: only attempts in flight read old data files, so a file goes once every live attempt planned at or after the commit that replaced it | D183 |
| Layers are Parquet | Parquet or our block format, decided by benchmark (T44); this design is format-agnostic | D183 |
| Postgres outputs stage, then publish after the journal decides | One simple store contract: a write returns the keys it wrote; a load returns the keys and versions it actually loaded; deviations are recorded as facts. No staging, publish, two-phase commit or fencing | D184 |
| Batch records carry their replaced slice and a digest | Kept | D184 (Fable D10) |
| Context read per batch | Kept | D184 (Fable D11) |
| History exact while a run is retained | Kept | D180 |

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
| Clear | A commit flag: "everything present before is removed, then this commit's entries apply". |
| Edge | One keyed input of one consumer partition, reading one upstream output. |
| Conditions | What a key was processed under: the input's patterns, the context (the heads of the consumer's whole and dep inputs), and the asset's definition. |
| Progress | Per edge, what the consumer holds for every key. Stored as **pieces** (key ranges with one value each) and **points** (single keys with an explicit value). Replaces today's *observation record*. |
| Owed | A key whose upstream state now, under the conditions now, differs from what progress says the consumer holds. |
| Due | Owed, and a default run would take it now. A failing key waiting for its retry is owed but not due (§11). |
| Batch record | The input-log entry for one committed batch on one edge. |
| Cut | The oldest commit the change index must still answer at. |
| Live attempt | An attempt whose spec the engine issued and which it has not yet settled (committed, failed, canceled or given up on). |

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
   one (§10). Its own entries still carry the true replaced version. It is
   one flag, not 100 million removal entries.

**Why entries carry the replaced version** (**P1**, Fable D2). A diff needs
only the entries inside its window; cleanup knows which file each commit
replaced; and a key that went v1 → v2 → v1 is recognised as unchanged.
Fable's cost was that the writer must know what it replaces. That cost is
gone here, because the engine computes it from its index when it commits (D184), as today's
resolver already does for writes.

### Layers

The change index is the output log itself, sorted by key and stored as a
few immutable layers on object storage. Nothing is stored twice: the
planner and history read the same files (**P2**, Fable D3).

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

**The size rule** (**P3**, Fable D5; today's rule, carried over). A merged
layer reaching above the cut holds at most 4 × the bytes of all newer
layers, plus 1 MiB. So a reader starting at any commit reads at most about
5 × the bytes that changed since, plus 1 MiB. Today's tiers, two merge
lanes, back-off (D181) and writer backpressure stay.

**What a merge keeps.** Above the cut, every entry. At or below it, entries
fold into the base: one entry per key present at the cut. With preserved
commits (§13, later), merges below the cut keep the states at those commits
too.

### File format

**Settled (D183): decided by benchmark (T44).** *Departs from Fable D4*,
which chose Parquet so DuckDB could read layers directly. Nothing here
depends on the choice: a layer is a key-sorted run of entries in blocks,
with a small index of block bounds. If our block format wins, history
queries read layers through the same native reader the engine uses; if
Parquet wins, DuckDB reads them directly.

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

**Points carry explicit versions** (**P5**, Fable D7), never "a version
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
`(lo, hi] → at(H, conditions now)`. Inside that range it leaves a point for
exactly the keys whose holding differs from what the new piece says: failed
keys, keys held back from the batch, and keys loaded at another version.
Every other point in the range is dropped.

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

### No fold (P4, Fable D6)

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

### Owed counts (P14)

Each edge keeps, for each piece, the **exact number of keys it owes**
(keys with a point are counted separately, one by one). Three events change
the numbers.

**1. An upstream commit.** For each entry `k: r → n` that falls in a piece
`at(P)`, let `h` be what the consumer holds for k, its state at P. Before
the commit k was owed if `h ≠ r`; after, if `h ≠ n`. The count moves by the
difference.

- Usually k had not changed since P. Then `h = r`, known for free: when the
  engine computes `r` from its index, it learns which commit wrote `r`, and
  that commit is at or before P. The count goes up by one.
- Only a key that changed again since P needs `h` looked up, with `get(P, k)`
  in the engine's warm index. That is exactly the case of a revert.

For an `empty` piece, `h` is "nothing"; for `held`, it is "held" when the
consumer's own output has the key. A point compares its own value with `r`
and `n` the same way. A clear commit is counted by scan.

**2. A batch commit.** The new piece owes `diff(H, head)` over its range:
usually nothing, or a few keys from commits since H. A piece the batch cuts
keeps the owed keys outside the batch's range; the engine knows the ones
inside, since it planned them and has propagated every commit since H.

**3. A change of conditions** (patterns, context, definition), a reset, or
a new edge. The affected pieces are counted by a scan, when the change
happens. A status asked meanwhile waits for it; it is never guessed
(`behaviors.md` STA-12).

**Example: tally's counts through §3's story.**

| Event | (−∞, k2] | (k2, +∞) |
|---|---|---|
| After run 1, batch 1 | at c1, owes 0 | empty, owes 1 (k3) |
| c2: k2 1 → 2, k4 − → 2 | owes 1 (k2 unchanged since c1: +1) | owes 2 (k4 added to an empty range: +1) |
| After run 1, batch 2 | owes 1 | at c2, owes 0 |
| c3: k3 1 → −, k5 − → 3 | owes 1 | owes 2 |
| c4: k1 1 → 4 | owes 2 | owes 2 |
| After run 2, batch 1 | at c4, owes 0 | owes 2 (k3, k5) |

**Example: a revert raises no alarm.** `items` holds `(−∞, +∞) at f1` on
`feed`, owing 0. f2 changes k1 `v1 → v2`: k1 unchanged since f1, so +1.
f3 changes k1 back, `v2 → v1`: k1 changed since f1, so the engine looks up
`h = S(f1)[k1] = v1`; before, `v1 ≠ v2` (owed), after, `v1 = v1` (not owed):
−1. The count is 0 again. `items` was truly stale between f2 and f3,
and is fresh after f3, with no run spent finding that out.

### What follows from the counts

| Question | Answer |
|---|---|
| Is anything owed on this edge? | Any piece count or due point above zero |
| Why? | The reason of each owing piece: input changed (behind the head), patterns, context or definition changed (its conditions differ from now), never processed (`empty`), to reconcile (`held`) |
| Is the partition stale? | Any of its edges owes something due (STA-3: a default run would load something) |
| Upstream stale | A partition whose stale status flips tells its consumers, which tell theirs: work only where the status changes |
| Complete? (D177) | No `empty` piece owes anything: an `empty` piece's count is exactly its unprocessed upstream keys |
| How many keys owed? | The sum of the counts: exact |
| How many commits behind, since when? | The oldest owing piece's commit, against the head and the commit headers |
| Which keys? | The planner without a limit, on demand; costs a read proportional to the answer |

### Cost

Each upstream commit costs work proportional to its entries times the edges
reading it, plus one warm-index lookup per entry that changed again since
a piece's commit. Edges whose pieces sit at the same commit share the work.
For a large commit (a 100M-key rewrite), the counts are computed as a
stream; "stale or not" is known at the first entry of a key unchanged since
the piece, long before the exact count is in.

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
   holding H.
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
   reports.
8. **It propagates** the output commit to the edges that read the output
   (§6).

A batch whose output does not change makes no output commit: writing
nothing wakes nothing (INC-14). It still commits its progress.

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
- **Each attempt plans at its own head** (**P6**, Fable D9; today's
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
| Points written | none |
| Counts | updated 2 |
| Digest | hash of [(k1, 4, updated), (k2, 2, updated)] |
| Planner version | 1 |
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
  version than planned, with the version loaded.
- **A key-list run records its key list**, the one case where planned keys
  are stored: the user's input cannot be derived.

### Several keyed inputs

A batch covers the same key range on every keyed input; candidates merge
in key order and N counts distinct keys. Each edge gets its own piece at
its own head and its own record.

## 8. The store contract

**Settled (D184).**

| Call | Takes | Returns |
|---|---|---|
| write | a batch of rows (and removals), the version the engine assigned to the attempt | the keys it wrote and removed |
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
  producer's output commit is recorded as usual.
- **No staging, publish, two-phase commit or fencing.** There are no store
  *kinds*.

**Example: a store that moved on.** `items` is on Postgres. `tally`'s batch
plans at c4 to load k1@4 and k2@2. Before it loads, items commits c5
(k2: 2 → 5). The load returns k1@4 and k2@5. The batch processes k2 as
updated to 5, and commits `(−∞, k2] at c4` plus a point `k2 → 5`. After
the commit, the new piece's diff(c4, c5) names k2, but k2's point equals
upstream now: nothing is owed in (−∞, k2]. Today this batch would replan
at c5 (STO-3).

**What this deletes.** The fenced store kind, `acquire`, `keys()`, the
fence table, repair by presence, the repair clock, the write gate's
intents, `reads()` snapshots, the replan on a newer generation, and the
waits it needs (held: `moved`, `writing`, `repair`).

### What it gives up, and how it heals

A store that writes in place (a table) can now hold rows no commit
recorded: an attempt that wrote and then died, or a late write from an
attempt the engine gave up on.

**Example.** items' attempt A plans to update k2 and writes k2@6 to the
table, then dies before it commits. The log still says k2@2. `tally` plans
k2@2 and loads k2@6: a point `k2 → 6`. k2 is owed again (holds 6, log says
2). The next run plans k2@2 and loads k2@6 again: the key stays owed for as
long as nobody rewrites it.

Two proposals close this:

- **P13. Loads report what they find.** A key loaded at another version is
  also reported to the upstream's log, as Fable D16 does for sources: the
  report becomes an entry `k2: 2 → 6`, unless the log changed k2 since the
  batch's head (the log then knows better). `tally` is then not owed, and
  items' retry, which still owes its input, writes k2@7 over it. The same
  rule corrects a source's log when a load finds the outside moved.
- **P15. "Newer version wins" writes, as a recipe.** A table store writes
  `ON CONFLICT … WHERE excluded.version > t.version`. A late write of an
  older attempt then changes nothing. Recommended for SQL stores; not part
  of the contract.

## 9. Conditions: patterns, context, definition

A change of conditions writes nothing to progress. Each piece remembers
the conditions it was processed under; a difference from now makes keys
owed (Fable D11; today's behaviour).

| Part | When it differs from now |
|---|---|
| Patterns | Keys taken by exactly one of the two pattern sets are owed |
| Context | Every present key is owed, as updated |
| Definition | Every present key is owed; an ordinary asset rebuilds with a full run |

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
| Key list | Exactly the listed keys | Points only | The listed keys are done |
| Full | Everything, after a reset written by the first batch | The reset, then a piece per batch | The unreached part is `empty` or `held` |

### Key list

`tally`'s progress is `(−∞, +∞) at c2`; the head is c4. Owed: k1 (1 → 4),
k3 (removed), k5 (added). The user runs `keys=(k1, k5)`: the commit writes
points `k1 → 4` and `k5 → 3`, and the piece stays at c2. Owed afterwards:
k3 only. `diff(c2, c4)` still names k1 and k5, but their points equal
upstream now. The next incremental run covers the range and drops both
points.

### Full run of an ordinary asset: a clear

**P7 (Fable D12):** the output's reset is a **clear** commit in the same
log, with the same numbering. A new life is only for an output deleted and
declared again (a removal, a move to another store). Today, resets of
unkeyed outputs start a new life (D178) and a keyed full run writes a
replacement delta of every removed key.

- The first batch's commit is a clear on the consumer's own output, and the
  same event resets its inputs' progress to `empty`. Later batches are
  ordinary.
- If the first batch fails, nothing happened.
- **Downstream keeps its progress.** It sees one diff across the clear
  (two scans). Versions are never reused, so a rebuilt key can never look
  unchanged by accident.

### The upstream-incomplete gate

**P12:** while an upstream is incomplete (mid full run, with an `empty`
piece still owing), downstream runs no batch over it, by default. The
engine checks this before every spec, so a downstream run already in
flight pauses too ("held: upstream incomplete").

**Example.** `copy` holds `copy#1`: k1@1, k2@1, k3@1, k4@1 (copied from c2).
Its definition changes, so it runs full at c4 with `batch_size=2`. Batch 1
commits `copy#2`: a clear, re-adding k1@2 and k2@2. Batch 2 commits
`copy#3`: k4@3 and k5@3. `archive` sits at `copy#1`.

| | archive's input |
|---|---|
| Without the gate, at copy#2 | k1, k2 updated; **k3, k4 removed** |
| … then at copy#3 | **k4 added**, k5 added |
| With the gate, one diff(copy#1, copy#3) | k1, k2, k4 updated; k3 removed; k5 added |

Without the gate, `archive` removes k4 and adds it back, and its output
lacks k4 in between.

### Full run of a per-key asset: held

**P8 (Fable D13):** a per-key asset (one per-key input, outputs keyed by
its key) resets its progress to `held` and clears nothing (today's
behaviour). `checks` holds k1, k2, x9; upstream holds k1, k2, k3: k1 and k2
updated, k3 added, x9 removed. Its output never empties, so it stays
complete and needs no gate.

The same fallback applies when progress can no longer be trusted (an
upstream deleted and declared again): a per-key asset resets to `held`; an
ordinary asset owes a full run.

## 11. Per-key assets: key outcomes

**Kept (D179):** per-key assets expose each key's latest outcome; only
outcomes other than ok are stored, sparse; ok, removed and unmatched are
derived.

**P9 (Fable D14): progress records success only.** A failing key keeps what
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
nothing; and progress always describes the output.

The point and the outcome entry go together: when the key succeeds, when
it is processed as removed or unmatched, when what it failed on is no
longer what it should be and it is not owed (upstream went back to what
the consumer holds), or at a reset.

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

**Batch lineage.** Recompute the batch's input from its record: for each
replaced piece, `diff(piece's commit, record's head)` over the range; apply
the points written; check the digest. The output side is the entries of
its output commit.

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

**What is stored per key** (**P10**, Fable D17): non-ok outcomes only.
Each batch's ok keys are recomputed by lineage. Duration per ok key is not
stored (100M rows per full run); a duration histogram per batch is.

**Honest answers.** "k1 was at version 1 then; that file has since been
deleted." "Run not retained." A digest mismatch is an error naming the
batch.

## 13. Retention and cleanup

### The cut (P11, Fable D18, staged)

**Start with a single cut**, as D180 states: the oldest commit any retained
run or live piece refers to. Every entry above it is kept; below it,
entries fold into the base. A retained batch record names its replaced
slice's commits and its head, so all of them stay above the cut.

**A paused consumer never loses its place** (CLN-11): the cut never passes
a live piece, so it catches up with one diff, never a full run. Its cost is
index size: every entry since it stopped. Fable's refinement bounds that to
one entry per changed key: a **horizon** plus **preserved commits**,
where merges below the horizon keep only the states at commits someone
still refers to. It is the same file format with a different
merge rule, so it can come later (step 9 of §15).

### Cleanup of replaced data (exact)

**Settled (D183).** Lagging consumers never read old data files: every
batch plans at the head and loads versions at that head; only the index
needs old commits. So only live attempts hold old data.

**The rule.** A version replaced at commit r is deleted once r is at or
below the oldest head any live attempt holds on that output, or the head
when none is live. Each entry names what it replaced; a cleanup cursor per
output walks commits in order, as today (D168). A clear replaces every key
present before it, found with `scan(r − 1)`.

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
What an abandoned attempt wrote goes by its version, late, as today
(CLN-5).

## 14. Today → target

| Today | Target |
|---|---|
| Observation record: base, ranges, points | Progress: pieces and points |
| The fold: ranges relabel to the newest head; changed keys become "version since replaced" points | Deleted (P4) |
| Base before-image (decided, not built) | Dropped: the cut never passes a live piece |
| Stamped layers: newest state, flips, main and side parts, graveyard | MVCC layers: entries with both versions |
| Δ(P, H) at a pinned head; `CutError`; `NotHeld` | `diff(C1, C2)` between any retained commits |
| Deltas' replaced generation, for cleanup only (D168) | In every entry; used by diff, cleanup and staleness |
| Staleness computed on demand, cached per revision | Exact owed counts, updated on each commit |
| Reader pins; cleanup held by the oldest observation | Cleanup held by the oldest live attempt's head |
| A new life for a reset (unkeyed); a replacement delta (keyed) | A clear commit; lives only for deleted-and-recreated outputs |
| Fenced and immutable store kinds; `acquire`, `reads()`, repair, replans and holds | One contract: write returns keys, load returns versions |
| Failing keys processed at the version they failed on | Success only; failing keys keep a point (P9) |
| Workers resolve large deltas | The engine builds every commit's entries |
| **Stay:** key outcomes (D179), `behaviors.md`, a task's progress key (D154, D155), keys in the spec (D178), the commit check, the size rule (D5), merge lanes and back-off (D181), the cleanup cursor (D168), completeness (D177) | |

## 15. Migration plan

Each step lands on its own, with tests, and leaves the system working.

| # | Step | What it adds | What it deletes |
|---|---|---|---|
| 1 | **The input log.** No index change. | A batch record per committed batch per edge in `batch_inputs`: range, head, conditions, replaced slice, points written, counts, digest, planner version, output commit | Nothing. Lineage cannot be recomputed until step 5; records written from now on will be |
| 2 | **Drop the fold** (P4). | Pieces keep their own heads; equal neighbours merge; the base is one piece; Δ is read from each piece's own commit (today's index allows it: the cut is the oldest live commit) | The relabel fold, fold-made points, the "range becomes the base" rule, the before-image decision |
| 3 | **Success-only progress** (P9) | Failing keys keep a point; due keys come from points plus key outcomes | "A failing key is processed at the version it failed on" |
| 4 | **The store contract** (D184) | write returns keys; load returns versions; deviations become points in the record; the engine builds every commit's entries; tables keep a version column | Store kinds, `acquire`, `keys()`, the fence table, repair by presence, the repair clock, gate intents, `reads()`, replans on a newer generation, held `moved`/`writing`/`repair`, lineage's `uncommitted` |
| 5 | **MVCC entries and `diff(C1, C2)`** | Entries with both versions; merges keep every entry above the cut; `diff`, `scan`, `get` at any retained commit; `history(key)`; lineage and key trace from batch records; the single cut counts retained records | Flips, main and side parts, the graveyard, `NotHeld`, `CutError`'s full-run fallback for live readers, the per-batch pinned index state, the deltas' cleanup-only replaced generation |
| 6 | **Exact owed counts** (D183) | Per-piece counts updated on each commit; stale status propagated down the graph | The on-demand staleness compare and its cache |
| 7 | **Clears and the upstream-incomplete gate** (P7, P12) | A full run's first commit is a clear; diff across a clear by two scans; the gate before each spec | Lives for resets of unkeyed outputs (D178's life on unkeyed records); replacement deltas listing every removed key |
| 8 | **Exact cleanup** (D183) | Cleanup held by the oldest live attempt's head | Reader pins of index state, durable multi-attempt reads, the oldest observation's hold on the cleanup cursor |
| 9 | **Preserved commits** (P11, later) | A horizon; merges below it keep states at preserved commits | The single cut's "keep every entry" below the horizon |

Order. Steps 1 and 2 come first and touch no format. Step 8 is
independent of the others: since the observed-set rebuild removed passes,
only attempts read data files at an old head. Step 6 needs step 5's replaced versions to be
exact on reverts. Step 4 is independent of the index work.

## 16. Changes to `behaviors.md`

### Entries whose expected behaviour changes

| Entry | Today | Target | Step |
|---|---|---|---|
| INC-4 | An update reverted to the processed version is delivered as updated | Not delivered, and never stale: diff sees equal ends | 5, 6 |
| KEY-5 | A failing key counts as processed at the version it failed on | A failing key is owed (its point keeps what it held) and shows `owed: true`; a key that never succeeded and is removed upstream owes nothing, and its outcome goes | 3 |
| STA-3 | Stale means a default run would load something | Unchanged rule; now explicit that owed failing keys not yet due do not make a partition stale | 3 |
| STA-12 | Computed exactly when asked | Kept exact at every commit; condition changes counted at the change; a status asked meanwhile waits | 6 |
| STO-1 | A store is immutable or fenced | One contract, no kinds | 4 |
| STO-2 | An immutable store returns exactly the pinned version | A load returns the versions it read; a store naming objects by version returns exactly the planned ones | 4 |
| STO-3 | A batch on a fenced store is classed at the write its read saw; a moved store replans | No replan: keys loaded at another version are processed as loaded and recorded as points | 4 |
| STO-4 | A stale writer on a fenced store changes nothing | Deleted. A late write can land on a store that writes in place; P15 makes it harmless where the store can | 4 |
| STO-5 | A fenced partition at rest holds exactly its keys | A table at rest holds its log's keys unless a write landed outside a commit; such keys show as deviations at the next load (and, with P13, enter the log) | 4 |
| ENG-7 | A worker the engine gave up on cannot change what a newer one committed | Holds for stores that name objects by version (and P15 tables); otherwise detected at the next load | 4 |
| ENG-8 | A dead writer on a fenced store is repaired; readers wait | Deleted: no repair, no repair clock, no waiting | 4 |
| HIS-3 | Postgres lineage is the version the read saw, `uncommitted` if no commit made it | Lineage is the batch record: planned versions plus deviations | 4 |
| HIS-5 | Not built | Built: exact per-key lineage while the run is retained | 5 |
| CLN-4 | A lagging consumer can hold back cleanup | Only attempts in flight hold it back | 8 |
| CLN-6 | A reader pinned before a removal holds that output's cleanup | An attempt in flight from before the removal holds it | 8 |
| CLN-11 | Not built; today a consumer below the cut gets a full run | Built: the cut never passes a live piece (open question 12 answered: unbounded index history for a paused consumer until step 9) | 5 |
| RUN-4 | Waiting reasons: claim, concurrency, merges, engine, executor, invalid | Adds `upstream incomplete` (P12) | 7 |

### New entries

| Proposed id | Rule | Example | From |
|---|---|---|---|
| INC-15 | An upstream full run reaches downstream as one change; downstream keeps its progress | §10: archive gets k1, k2, k4 updated, k3 removed, k5 added | P7 |
| STA-13 | Downstream of an incomplete upstream runs no batch, by default | §10: archive is held while copy's full run is between batches | P12 |
| STA-14 | No false alarms: a partition is stale only if something is due; changes that cancel out start nothing | §6: feed's k1 v1 → v2 → v1 | D183 |
| STA-15 | Failing keys not yet due are shown, not stale | §11: checks shows k2 failing, stale only for k3 and k5 | P9 |
| KEY-12 | A key that never succeeded and is removed upstream owes nothing | items adds k6; checks fails on it at its first try; items removes k6: no removal is delivered, and the outcome goes | P9 |
| CHG-13 | A whole input moving mid-run makes earlier ranges owed again | §9: factor moves during checks' run | D184 |
| HIS-7 | A batch's input is recomputed from its record and checked against its digest; a mismatch is an error naming the batch | §12 | D184 |
| HIS-8 | Lineage lists keys loaded at another version than planned, with the version loaded | §8: tally's point k2 → 5 | D184 |
| CLN-12 | A replaced data file goes once no live attempt planned before its replacement | §13: k1@1 goes when checks' attempt settles | D183 |
| SRC-12 | A load that finds another version than the log records it in the upstream's log, unless the log changed that key since the batch's head | §8: k2: 2 → 6 | P13 |

## 17. Decisions

### Settled

| Decision | What it fixes here |
|---|---|
| D180 | History exact while a run is retained; diffs and scans between any two retained commits (§4, §12) |
| D183 | The engine plans (§7); exact answers kept current by propagation, no counters, no false-alarm runs (§6); exact cleanup by the oldest live attempt's head (§13); file format by benchmark (§4) |
| D184 | The store contract (§8); batch records with replaced slice, digest, planner version (§7, Fable D10); context per batch (§9, Fable D11) |
| D179, D177, D178, D154, D155 | Kept as they are: key outcomes, completeness, keys in the spec, a task's progress key, the vocabulary |

### Proposed, for Erwin to confirm

| # | Proposal | Recommendation |
|---|---|---|
| P1 | Entries carry the replaced version (Fable D2) | **Yes.** It makes diffs local to their window, reverts exact, cleanup a walk, and §6's propagation cheap; the engine already computes it |
| P2 | One index for planning and history (Fable D3) | **Yes.** No second copy; planner and history cannot disagree. Cost: a reader far behind reads every version of a key that changed often |
| P3 | The 4 × size rule carried over (Fable D5) | **Yes.** It is today's rule and bounds any reader's read to ~5 × what changed |
| P4 | Pieces keep their own heads; no relabel fold (Fable D6) | **Yes.** Deletes the fold and its points; up to one piece per batch until the next run |
| P5 | Points carry explicit versions (Fable D7) | **Yes.** A stuck key holds no history |
| P6 | Each attempt plans at its own head (Fable D9) | **Yes.** Today's behaviour; it is what lets cleanup look only at live attempts |
| P7 | A full run's reset is a clear in the same numbering; lives only for deleted-and-recreated outputs (Fable D12) | **Yes.** Downstream keeps its progress; no cascade of full runs |
| P8 | `held` for per-key full runs and untrusted progress (Fable D13) | **Yes.** Today's behaviour, renamed |
| P9 | Progress records success only; failures in the sparse key outcomes (Fable D14) | **Yes,** with the owed/due split of §11, so failing keys never keep a partition stale |
| P10 | Per-key history computed; only non-ok outcomes stored (Fable D17) | **Yes,** with a duration histogram per batch instead of per-key durations |
| P11 | Horizon plus preserved commits, starting with a single cut (Fable D18) | **Yes, staged:** single cut now (step 5), preserved commits when a paused consumer's index cost shows up |
| P12 | An incomplete upstream blocks downstream batches by default | **Yes.** Without it a downstream partition churns removals and re-adds during every upstream full run. Opt-out per input for consumers that want partial data |
| P13 | Loads report what they find into the upstream's log, guarded (Fable D16, extended to produced outputs) | **Yes.** Without it, a row no commit recorded keeps a key owed forever (§8) |
| P14 | Staleness as exact per-piece owed counts, updated from each commit's entries (how §6 implements D183) | **Yes.** The alternative, re-diffing every reading edge on each commit, costs the whole lag at every commit |
| P15 | "Newer version wins" writes for SQL stores, as a recipe, not a contract | **Yes.** One line of SQL removes the late-writer case where a store can do it |

### Open questions from Fable's brief, answered

| Question | Answer |
|---|---|
| Do key-preserving assets have exactly one keyed input? | Yes: a per-key asset has exactly one incremental input and keys every output by it (`sdk.py`, `_check_each`) |
| Context per batch or per run? | Per batch (D184) |
| Stage, then publish, for non-object stores? | No (D184) |
| The default grace period T? | No T (D183) |
| Incomplete upstream: block or warn? | Block by default (P12) |
| Rejected keys stay owed and held back? | Yes (P9): owed, not due |
| Duration per ok key? | Not stored; a histogram per batch (P10) |
| Circuit breaker for mass failure? | Not now: points and outcomes spill to files. Revisit if a mass failure shows up in practice |
| Unkeyed outputs: how long do they keep appended data? | Open: a consumer's full run re-reads only what is still there. Needs Erwin's call with retention |
| Build aside and swap for full runs? | No: the clear plus the gate (P7, P12) gives the same consistency downstream without double storage |

## 18. How the examples were checked

A small model outside the repo
(`~/.solera-design/tracking-model/model.py`) implements entries, diff and
scan from entries alone (clears included), pieces and points, the batch
commit rule with failures and loads at another version, `held` resets,
and §6's owed-count propagation. A random test over tiny universes (3 to
6 keys, versions that revert, occasional clears) checks after every step
that progress decodes to a brute-force record of what the consumer holds,
that the propagated counts equal brute-force counts, and that every diff
and scan equals the true states: about 68,000 steps, all passing. The
model also prints §3 to §11's examples (tally's progress and counts, the
revert, the key list, the failure, the clear and the gate, the deviation),
which match the tables above.
