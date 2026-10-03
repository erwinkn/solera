# Presence at a position (design study)

Status: **proposal**, for Erwin. Measurements from `bench/keys/presence.py`
on this branch; the prototype is `_native.presence` (`native/src/format.rs`,
`Merge::existed` in `stream.rs`).

## The question

A consumer at position N reads what changed upstream in commits N..H. For
each changed key it must know whether the key was live at N. With the
answer, K44's batch fields follow (added: absent at N, live at H; updated:
live at both; removed: live at N, gone at H), and so does K39/K46's net
staleness (a key absent at both ends changed nothing). Without it,
`ctx.load() + len(added) - len(removed)` drifts.

## What already exists

Two things make most of the answer free.

1. **The delta log.** For every output some asset reads incrementally,
   the index keeps each commit's original delta file until no position or
   claim needs it (`IndexState.log`, `Upkeep.truncate`). Compaction never
   rewrites these files. So the record of N..H is already there, the
   very files the pass reads.
2. **Predecessors.** In a delta file, a tombstone is written only over a
   live key, and an upsert names its predecessor generation when the
   writer read the key live. So "the key was live before this entry" is
   `deleted or predecessor is not None`. For a key changed in N..H, its
   *oldest* entry in the range answers for N, and its newest answers for H.

This is exact as long as every writer knows. Most do: the engine's warm
resolver (point lookups in its local copies), streaming patches and full
replacements (merge-joins), the failed-keys index (`exact=True`). One path
guesses: the sparse reader on a cold worker, used when the engine declines
a resolve. There, a bare upsert that some key filter flags and no
tombstone filter flags is taken as live without reading its block
(`Known::Other` in `sparse.rs`) and written **with no predecessor**.

The gap is wider than the Bloom false-positive rate. Every update that
goes through that path reads as an *add* in the log. Measured at 100M keys,
cold: of 1,000 updated keys, the log says 1,000 were added. The false
positive is the smaller, opposite error. A brand-new key the filters flag
(1.2% of new keys at 100M in steady state, since a key meets three or four
files' filters) is counted as an update in the index's key count. Its
delta entry still has no predecessor, so the log gets it right by luck.

## Options

| | Exact? | Write cost (cold, 100M, 1K updates) | Plan cost | Extra storage | Concepts |
|---|---|---|---|---|---|
| Today | no: cold-path updates read as adds; FPs miscount | 53 GETs, 0.5 s | the pass's log scan | none | inexact count, recount |
| **A. Exact writes, always** | yes | 1,851 GETs, 1.8 s; 10K updates stream, 258 GETs, 19 s. Warm engine: unchanged | the pass's log scan, one more bit per key | none | removes the two above |
| A'. Exact only for consumed outputs | yes | same, only where consumed | same | none | keeps both, adds a deploy race |
| B. Pinned snapshot at each position, horizon | yes | none | log scan **plus** lookups: 1,758 GETs (1 behind) to 1.4 GB (100+ behind) at 100M | ~1× the index per position 100 commits back (1M–10M), 45% at 100M after 10K | pinned positions, horizon |
| C. Resolve at compaction | eventually | none | wait for the resolution, or guess | resolution records | unresolved entries, a new file |
| D. Exact sets instead of Bloom | almost | tails 3× larger on every cold write | log scan | +3–7 B per key | none new, a format change |
| E. Lifespans (born/died per entry), versioned bitmaps | yes | as A, plus a field | as A | +1 varint per entry | lifespan |

Why the others lose:

- **A'** saves cold-path reads on outputs nobody reads incrementally, but
  keeps the inexact count and its recount, and opens a race. An upstream
  attempt launched before the deploy that adds a consumer writes inferred
  entries, then commits after the consumer's first pass has pinned its
  head. Its delta lands in the log with guesses in it.
- **B** is exact but never cheaper. The consumer still reads the log to
  learn which keys changed, then pays lookups on top: at 100M, a 1K-key
  lookup costs what an exact cold write costs (1,758 GETs), and from 100
  commits behind it reads the whole snapshot (1.4 GB). Each position also
  pins files compaction replaced. The retention sim (amplification.py's
  planner, 20 pins per size) says a pin 100 commits old keeps 90% of a 1M
  index alive, and 10,000 commits old ~1× at 1M–10M and 45% at 100M. Per
  distinct position.
- **C** knows the truth only when a merge brings an inferred entry
  together with the key's older entry. Level-0 merges among themselves
  never do; at 100M an entry may reach the level holding its predecessor
  thousands of commits later. A consumer planning before that can't. The
  result also needs a place to live (the log files are immutable), so: a
  new file kind and an "unresolved" state.
- **D** runs into information theory. Exact membership of arbitrary keys
  needs the keys, which are the blocks. Elias-Fano over 64-bit key hashes
  costs 2 + 64 − log2(n) bits: 5.8 B per key at 1M, 4.9 B at 100M,
  against Bloom's 1.75 B, and is still wrong once per n/2^64 probes
  (5e-12 at 100M). Pushing that to 2^-64 costs ~8.3 B per key, as much as
  an entry. A cold writer fetches every file's tail, so that is ~3× the
  350 MB it reads today. Or it fetches parts per key, one request each,
  which is A's block read again.
- **E**: lifespans need the same exact read at write time to fill `born`,
  and the log already answers for every position it covers. Versioned
  bitmaps need dense key ids, which a sorted LSM has none of.
- **A cheaper A, flagging inferred entries**, keeps the guess. 99% of
  flagged keys are updates; the rest are adds, and only a read tells them
  apart.

## Measurements

Local object store, every request delayed 30 ms and each connection
limited to 80 MB/s, 64 in parallel, cold readers (as `bench.py`). Request
counts and bytes are exact; wall times are as good as that model. Index in
steady state: at 1M one 8.9 MB level plus 7 deltas; at 100M, levels of 72
MB, 640 MB and 704 MB plus 7 deltas (format v3, no payloads). M5 Max.

**Writes, cold** (the path the engine's resolver takes when it declines;
a warm engine answers exactly either way, measured at 113 ms per 1K keys
at 100M in `resolved-commits.md`):

| 100M keys | Today | Exact | Today's errors |
|---|---|---|---|
| 1K updates | 53 GETs, 350 MB, 0.47 s | 1,851 GETs, 425 MB, 1.75 s | 1,000 of 1,000 read as adds |
| 10K updates | 53 GETs, 350 MB, 0.27 s | streamed: 258 GETs, 1.4 GB, 19 s | 9,993 of 10,000 |
| 1K inserts | 53 GETs, 0.25 s | 65 GETs, 0.29 s | 12 counted as updates |
| 10K inserts | 53 GETs, 0.26 s | 140 GETs, 0.38 s | 87 counted as updates |

At 1M, every row is 8 GETs, 8.9 MB and ~0.28 s both ways with no errors:
levels under 32 MB are read whole, so nothing is ever inferred. Exactness
costs something only for update-heavy writes into an index past ~30 MB
that the engine declined. The worst case is bounded by the existing
switch to streaming, which is about what an engine cache fill costs.
Inserts barely pay, since the filters clear them.

**Planning**, a consumer behind a log of 1K-key commits (90% updates, 5%
removes, 5% adds or re-adds), classes checked against a model of the keys:

| Behind | Presence through the log (prototype) | The pass's own scan today | B's extra lookups, 1M · 100M |
|---|---|---|---|
| 1 | 1 GET, 14 KB, 34 ms | same | 8 GETs, 0.3 s · 1,758 GETs, 2.9 s |
| 100 | 100 GETs, 1.4 MB, 0.11 s | same | 8 GETs, 0.3 s · 161 GETs, 1.4 GB, 9 s |
| 10,000 | 10,000 GETs, 135 MB, 18–20 s CPU | 10,000 GETs, 100 s (1M) · 308 s (100M) | 8 GETs, 1.9 s · 145 GETs, 1.4 GB, 13 s |

The presence merge reads the same bytes as the pass and costs no more
CPU. The 10,000-behind row is slow for a reason unrelated to presence: a
10,000-way merge on one core, and `pending` restarts it for every 100K-key
page. At 1M keys that log is 10× the index (1.27M distinct keys changed).
A full pass would read 9 MB.

## Recommendation

**Every delta entry says whether its key was live before it.** Writers
always resolve exactly (A). The consumer classes each changed key from the
log it already reads: the oldest entry in the range gives N, the newest
gives H. Nothing new is stored.

What changes:

- `Sparse::classify` loses the shortcut: a flagged bare upsert gets the
  block read, like anything else the filters can't clear.
- The pass's merge reports, per key, `existed` beside the newest entry
  (`Merge::existed` in the prototype, a few lines). K44's added, updated
  and removed come from it; "neither" keys are not delivered; staleness is
  "some key under the patterns is not neither", with an early exit.
- For a key in K45's read-ahead, the base is the generation the entry
  read, not N: the oldest entry newer than it decides. Generations rise
  with commit numbers, so the merge filters shadowed entries by
  generation.
- Deleted: the inexact count (`IndexState.inexact`, `count_exact`,
  `DeltaFiles.exact`, `Delta.exact`), the recount job and
  `recount_interval`, `Known::Other`/`Old::Other`/`Sparse.inferred`,
  `resolve(exact=)`, and the "within the filters' false-positive rate"
  caveats in `Batch.count` and `key-index-costs.md`.

**Optional: Erwin's horizon, applied to the log.** If the log since a
position holds more entries than the index has live keys, the next pass is
full. It reuses an existing rule (a log that no longer holds the delta
means a full pass). It caps the log at about the index's size in entries,
and at 1M keys it replaces the 18 s merge above with a 9 MB read. A full
pass restarts K44's count from 0, which stays correct.

Correctness under the usual suspects:

- **Compaction** never touches log files. It drops predecessors only in
  the compacted levels, which presence never reads.
- **Retries** rewrite commit c against the same index, since the claim
  serializes writers; a dead attempt's delta never enters the log.
- **Resets**: an upstream full run is a streamed replacement, already
  exact. An output reset, moved or removed at a deploy drops the positions
  on it, so the consumer does a full pass and asks nothing.
- **Renames** keep the index and its log (F12's rule covers a name
  declared away and back).
- **Log truncation** past a position already forces a full pass.
- **Pattern changes** go through the diff pass, which decides membership
  by match. Presence applies to the keys whose match did not change.

## Worked example

`items` holds a and b (generation 1). A plain incremental consumer keeps
`tally = ctx.load() + len(added) - len(removed)`.

| Commit | Change | Delta entry, exact | Today, cold worker |
|---|---|---|---|
| 1 | add c | c g10 | same |
| 2 | remove a | a tombstone g20, pred 1 | same |
| 3 | re-add a, update b | a g30 (no pred: was a tombstone); b g30, pred 1 | b g30, **no pred** |
| 4 | add d | d g40 | same |
| 5 | remove d | d tombstone g50, pred 40 | same |

Consumer at position 1, tally 2. Oldest and newest entry per key: a
(tombstone, then live) updated; b (pred, live) updated; c (no pred, live)
added; d (no pred, then tombstone) neither, so not delivered. Tally 2 + 1 −
0 = 3, and `items` holds a, b, c. A consumer at position 3 (it read 1–2,
tally 2 for b and c) sees a added (its oldest entry in 3..5 names no
predecessor), b updated, d neither: 2 + 1 = 3.

Today, b's update at commit 3 names no predecessor, so both consumers
class b as added and reach 4. `presence.py example` runs this through
`KeyIndex.resolve` with the filters forced on, both ways, and prints the
classes above.

## What the tests and the sim must check

- **Exact deltas, as a property.** For every committed delta entry of key
  k at commit c: a tombstone means k was live after c − 1; an upsert names
  a predecessor iff k was live after c − 1. Over random histories, through
  every writer: warm engine, cold sparse, streamed patch, replacement,
  source commit, repair, retry.
- **The count.** After every batch, the sum of added − removed over the
  pass equals live(H) − live(N) under the patterns. With K45 read-ahead
  entries, batches split anywhere, and compaction and log truncation
  interleaved. `test_staleness.py`'s `tally` is this check; it needs the
  cold path to run.
- **Reaching the cold path.** The sim runs with the engine cache on and
  small indexes, which are read whole, so it never infers anything today.
  It needs a mode with the resolver off, `bits_per_item=1` (false
  positives on most keys), `whole_threshold=0`, `small_file=0` and
  streaming off. Under that mode today's code fails the count at once,
  which is the regression test.
- **TLA+**: little. `Execution.tla` keeps each output's whole history
  (`log`, `UpTo`), so presence at N is already expressible there, and a
  K44 count invariant on `DeltaStep` is cheap to add. The bug this note
  fixes sits below that abstraction, in what a writer records, so the
  property test is what catches it.
