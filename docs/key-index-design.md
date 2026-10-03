# Key index design: one tiling of spans

Status: **proposal**, for Erwin. It follows `presence-at-position.md` and
`delta-log-ranges.md` (branch `bb/design-study-exact-presence-at-a-position`)
and keeps their baseline: every delta entry records exactly whether its key
was live before it. The numbers come from `bench/keys/tiling.py` (a replay
of the compaction policy on file metadata) and `bench/keys/tiling_reads.py`
(real files, today's reader, cold). Everything else is reused from the
earlier studies and says where it comes from.

## The answer in brief

Today the index is three structures. A leveled LSM serves lookups and full
reads. The delta log, one file per commit kept for consumers, serves
catch-up. The proposed range tree would make far catch-ups cheap. Each one
has its own merge rule and its own deletion rule.

The working hypothesis was that two orderings are needed: key order (lookup,
scan) and time order (changes). That holds, but it doesn't take two
structures. **One list of key-sorted file sets, each covering a stretch of
commits, gives both**: key order inside each set, time order across them.
Lookups read the list newest first, the way an LSM reads its levels. A
consumer reads the part of the list after its position, the way it reads
the delta log today. One merge rule and one compaction policy maintain it.
One constraint keeps it correct: a merge never joins two sets across a
commit some reader starts from.

Measured against today, at 100M keys:

- compaction writes 10–17× the committed bytes, against 30× for today's
  leveled planner on the same workload;
- an exact cold lookup of 1K keys takes 1,117 GETs and 285 MB, against
  1,851 GETs and 425 MB for the previous study's steady state;
- a catch-up of 10,000 commits reads ~5 spans (about 10 GETs), holding
  only the keys that changed;
- the separate log, the range tree, log truncation, the leveled planner,
  the inexact count and recount, the tombstone filter and the `.kg` garbage
  files are all deleted.

## The structure

**Span.** A span covers commits `[a, b]` and is a key-sorted set of `.kx`
files with non-overlapping key ranges, like a level today. For each key that
commits `a..b` changed, it holds one entry:

- the key's **newest** entry in the range: its generation, the deleted
  flag, its payload;
- its **predecessor**: the key's generation just before `a`, or none if
  the key was not live then.

That is a delta entry, with "before this commit" widened to "before this
span". A commit's delta is the span `[c, c]`, exactly today's delta file.

**Tiling.** The spans tile commit time with no gap and no overlap, from 0 to
the head. The first span is the **base**. Nothing reads from inside it or
before it, so it keeps live keys only, with no tombstones and no
predecessors: it is today's bottom level. `IndexState` becomes the live
count plus a list of spans `(a, b, files)`.

**Boundaries.** A boundary is a commit that some reader needs to be the
start of a span. Each **observer** contributes one:

| Observer | Its boundary | Lives |
|---|---|---|
| a consumer's position | `next` | until the position moves or is dropped |
| a pass under way (delta, full or diff; one attempt or several) over commits up to `to`, the head when it started | its landing point `to + 1`, where the position will move | until the pass ends |
| a pattern-change drain | its split commit | until the drain ends |
| an attempt in flight | none of its own: it reads the state it was handed, under its pin, and belongs to its pass, if any | |
| a read-ahead entry | none (see changes, below) | |

**Every boundary is born at the head + 1**, past every span that exists, so
adding one never cuts a span, and no alignment scheme is needed: that one
fact replaces the range tree's alignment rules. A position is born at a
pass's landing point. A keyed pass pages by key over commits fixed when it
starts (`positions.py`, kind `keys`), so it lands at the head + 1 of that
moment. Commit-by-commit batches exist only for unkeyed upstreams, which
have no key index.

A picture: 100M live keys, a daily consumer, an hourly consumer, and a
third consumer's pass under way.

```
commits  0 … 1,047,551   1,047,552 … 1,055,831              1,055,832 … 1,056,191 (head)
spans    [ base: 100M ]  [6.3M][1.1M][300K][64K][16K][4K]   [290K][48K][16K][4K][1K][1K]
                         ^ daily consumer's position         ^ hourly consumer's position  ^ 1,056,192: a pass's landing point
```

(Entries per span. Within each group of spans, sizes fall toward the head,
and the newer spans hold less than a quarter of the oldest: the policy
below.)

A lookup reads every span, newest first. The daily consumer reads every span
after its marker; the hourly one, the spans after its own.

**One merge.** Merging adjacent spans keeps, per key, the newest entry of
the newer span that has the key, and the predecessor of the older one.
Both are "last" and "first" over a sequence, so the merge is associative
and any grouping gives the same span (`delta-log-ranges.md` checked this
key for key). A key added and then removed inside the span stays, as a
tombstone naming no predecessor: a read-ahead key may have been delivered
in between (the worked example shows the case). Merging into the base also
drops tombstones and predecessors.

**One policy**, run by upkeep like compaction today. The spans between two
consecutive boundaries form a group (the first group starts with the base),
and merges stay within a group:

1. **Into the oldest.** Once the other spans of a group hold at least a
   quarter of its oldest span's entries, the whole group merges into one
   span. In the first group, that is the base merge: today's compaction
   into the bottom level and today's log truncation, at once.
2. **By size.** Otherwise, four adjacent spans of the same size class
   (`⌊log₄(entries / 1,000)⌋`) merge into one. A span older than a bigger
   neighbour merges with it first. That keeps sizes falling toward the
   head, which a vanished boundary can break: without this rule, an hourly
   consumer at 100M left 176 spans on average, up to 354.

The engine derives the boundaries from its model when it plans a merge, as
`Upkeep.truncate` derives the commits still needed today. A merge planned
before a boundary was born is still valid, since that boundary lies past
every input.

## How each operation runs

Costs are cold, from the object store (30 ms per request, 80 MB/s per
connection, 64 in parallel, as `bench.py`). "Today" is the previous study's
steady state at 100M: levels of 72, 640 and 704 MB plus 7 deltas, 2.0
entries per live key, 53 files. "Tiling" is 22 spans: a base, then 4-way
size classes of 1K to 4M entries, three each (1.16 entries per live key).
That is near the most the policy leaves with a daily and an hourly
consumer (15 spans on average, 24 at most, in the replay). The
43-span variant doubles the classes, standing for two such groups.

### 1. append(changes)

Resolve every written key against the spans, newest first: key filters,
then a block read for every key a filter matches, as `KeyIndex._find`
already does across levels. That gives each key's current entry exactly:
its predecessor (the replaced version, which is the object to clean up)
and whether it was live (the batch class, the live count). Write the span
`[c, c]`, one PUT, and add `added − removed` to the count.

| 1K random updates, cold, exact | 1M keys | 100M keys |
|---|---|---|
| Today | 8 GETs, 9 MB, 0.19 s | 1,851 GETs, 425 MB, 2.7 s |
| Tiling, 22 spans | 13 GETs, 11 MB, 0.20 s | **1,117 GETs, 285 MB, 1.5 s** |
| Tiling, 43 spans | 25 GETs, 14 MB, 0.21 s | 1,163 GETs, 343 MB, 1.7 s |
| Engine cache warm (`resolved-commits.md`) | 0 GETs | 0 GETs, ~113 ms |

At 1M every span is read whole, so the cost is one GET per span. At 100M,
the block reads dominate: about one per key in the base, plus one per key
that a newer span also holds. The tiling holds fewer duplicate entries
(1.16 against 2.0 per live key), so it reads fewer blocks and smaller
filters. In fairness: the planner's own replay ends near 1.24 entries per
live key (`amplification.py`), so a better-tuned leveled layout would
narrow the gap. The span count costs little: 43 spans cost 4% more GETs
than 22 (each span adds one 0.35% false positive per key). Inserts are
cleared by the filters, as today (65 GETs at 100M in the previous study).

### 2. lookup(keys, at = c)

The same read without the write. At the head it reads every span. A
lookup at an older `c` comes from an attempt reading its pinned head: it
reads the spans it was handed, which ended at `c` then, kept by its pin.
(A pass reading at `c` over several attempts holds `c + 1` as a boundary,
so the current spans serve it too.) Same costs as append, less the PUT.

### 3. scan(at = c, range or pattern)

Merge the spans up to `c` in key order, page by page with a key cursor
(`KeyIndex._scan`, unchanged: it already merges "levels newest first").
Prefix globs are key-range seeks; other patterns filter the merged stream.
A count is a scan. A pattern change's difference is one scan at the head,
testing each key against both patterns.

**Stability while commits arrive.** `c + 1` is a boundary, so no merge ever
mixes commits after `c` into the spans before it. A merge inside `[0, c]`
leaves the view at `c` unchanged. So a pass that pages over hours can read
each page from whatever spans exist when that page runs, and needs no pin
on index files for its duration. Each attempt still pins the files of the
state it was handed, for its own lifetime, as today.

| 100K-key page, mid-index | 1M keys | 100M keys |
|---|---|---|
| Today | 9 GETs, 1 MB, 0.09 s | 19 GETs, 4 MB, 0.15 s |
| Tiling, 22 spans | 14 GETs, 3 MB, 0.10 s | 37 GETs, 13 MB, 0.13 s |
| Tiling, 43 spans | 26 GETs, 6 MB, 0.12 s | 70 GETs, 25 MB, 0.16 s |

Scans pay for the extra spans in requests, at the same wall time. Small
spans are read whole: files under 2 MB, today's `small_file` rule. A full
scan reads 1.16 entries per live key, against 1.24–2.0 today.

### 4. changes(P → N, keys or range, per-key lower bounds)

`P` is a boundary (the position) and `N + 1` is one too (the pass's
landing point), so the spans between them tile `[P, N]` exactly. Merge
them. For each key, the newest entry and the oldest predecessor give its
class:

| Live before P (predecessor) | Live at N (newest entry) | Class |
|---|---|---|
| no | yes | added |
| yes | yes | updated |
| yes | no | removed |
| no | no | neither: not delivered |

- **keys=**: point lookups in those spans (filters, then blocks). **Range
  or prefix**: a seek in each span.
- **Read-ahead** (per-key lower bounds). An entry names an upstream commit
  `r`, and the attempt's spec lists its keys and how each was delivered
  (upserted or removed). For such a key, the bound is commit `r`'s
  generation (K47: "versions from the commit"): generations rise with
  commit numbers, so the key changed after `r` exactly when its newest
  entry is newer. If it didn't, skip it. If it did, its class is (its
  state delivered at `r`, its state at `N`). No span needs to start at `r`,
  which is why read-ahead entries are not boundaries.
- **Staleness** runs the same merge and stops at the first key that is not
  neither and not covered by the read-ahead. Transitive staleness asks the
  same of the upstream.
- **Paged and resumable** by key cursor. `P` and `N + 1` stay boundaries
  while the pass lives, so every page reads a tiling of `[P, N]`, even if
  its spans were merged in between.

| Behind | Today: per-commit deltas | Tiling (spans: the replay; bytes: the range tree's measurements) |
|---|---|---|
| 1 commit | 1 GET, 14 KB, 34 ms | same: the span `[c, c]` |
| 100 | 100 GETs, 1.0 MB, 0.1 s | ~6 spans, ~1 MB, ~0.1 s |
| 360 (hourly) | 360 GETs | 6 (1M) to 8 (100M) spans: 312K–360K entries |
| 10,000 (daily), 1M keys | 10,000 GETs, 101 MB, 11 s (one merge; 100 s paged today) | 5 spans, 1.24M entries: ~37 MB, ~1.2 s |
| 10,000 (daily), 100M keys | 10,000 GETs, 97 MB, 11 s (308 s paged today) | 5 spans, 8.3M entries: ~84 MB, ~2.4 s |
| keys= of 100 keys, 10,000 behind | 10,000 GETs, 6.5 s | ~10 GETs (1M) to ~110 GETs, 23 MB (100M), 0.2 s |

The merge is the range tree's, over fewer files: that study measured 10–14
files, 37–84 MB and 1.2–2.4 s for 10,000 behind, and checked every class
against the per-commit merge. CPU is one streaming k-way merge over a
handful of spans. Today's `pending` restarts a 10,000-way merge for every
page; that cost goes away.

## Write amplification and storage

From `tiling.py` (the second half of a long run: 40,000 commits of 1K keys
at 1M, 200,000 at 100M, mixed 90% updates, 5% removes, 5% adds). "Today" is
`amplification.py` replaying the real leveled planner on the same commits
(16.3× and 29.8×). Consumers read every period, in commits; at one commit
every 10 s, 360 is hourly, 8,640 daily and 60,480 weekly.

| Consumers | Written per entry committed, 1M · 100M | Spans, mean (max), 1M · 100M | Entries per live key, mean (max), 1M · 100M |
|---|---|---|---|
| Today, any | 16 · 30 (range files would add ~3) | 5 · 7–10 sorted levels (each level-0 file counts one) | 1.03 · 1.24–2.0, plus the retained log |
| one, every commit | 7.1 · 10.3 | 6 (11) · 11 (21) | 1.13 (1.25) · 1.12 (1.25) |
| + hourly | 8.9 · 15.9 | 5 (9) · 13 (22) | 1.16 (1.31) · 1.13 (1.25) |
| + hourly + daily | 9.0 · 17.2 | 10 (15) · 15 (24) | 2.15 (2.54) · 1.17 (1.33) |
| ten, periods 1 to 8,640 | 10.0 · 16.8 | 12 (16) · 15 (20) | 3.43 (4.31) · 1.16 (1.29) |
| + weekly | 7.2 · 10.4 | 8 (14) · 10 (20) | 2.87 (4.11) · 1.24 (1.49) |

At 100M with 10K-key commits: 8.6×, 10.5× and 13.1× for the first, third
and fourth rows.

- **Writes.** Compaction writes 35–60% of what today's planner writes on
  the same commits, before counting the range files today would add.
  The base absorbs the newer spans once they reach a quarter of it. At 100M
  that is a streaming rewrite of ~1 GB, ~27 s on 8 cores (today's measured
  full-level compaction), every ~25,000 commits of 1K keys (about 3 days at
  one commit per 10 s). Estimated rather than measured: small merges come
  about once per 3 commits, 4 GETs and 1 PUT each, so ~1.3 GETs and ~0.3
  PUTs per commit, against 1.2 and 0.14 today.
- **Storage** is the base plus the keys changed since the oldest boundary,
  once per group of spans. At 100M, nothing a day or a week behind saturates:
  1.12–1.24 entries per live key. A small, hot index is different. At 1M
  keys with 10M changes a day, a daily consumer's group holds nearly every
  key again (2.15). Ten consumers spread over a day keep several such groups
  (3.43): each boundary keeps its own record of what changed after it. The
  storage cap bounds it. Per-boundary bits (rejected alternatives, below)
  would shrink that record to a bit per key, at the price of reading more
  per catch-up.
- **Today's** steady state also keeps every per-commit delta any consumer
  still needs, on top of its levels. The tiling keeps no copy: its recent
  spans are those deltas.

## Observers, trimming and compaction

**The one rule** ("no file is deleted while some observer needs it") holds
in two halves:

1. **Compaction keeps every boundary a span start.** The current spans then
   serve every long-lived observer: a consumer, a pass, a drain. Nothing
   older needs keeping for them.
2. **A file a merge replaced is deleted once no pin predates the merge.**
   Pins are short: an attempt holds the state it was handed for its own
   lifetime, and the engine pins its own reads, as today.

**Trimming** is not a separate operation: it is rule 1 of the policy on
the first group. When the oldest boundary moves on, the spans before its
new place may join the base.

**The storage cap.** When the spans after the oldest position hold more
than C × the live keys in entries, the engine drops that position: its next
run does a full pass, as after a reset. The default is C = 2. Past that,
the catch-up reads more than twice what a full read does, and the
consumer's work is no longer much smaller than a full pass. A position
with a pass under way is never dropped, since that pass is catching up. The cap measures entries, not commits, so it costs nothing at 100M (a
consumer a week behind holds 0.24 extra entries per live key). At 1M with
ten consumers it drops the daily ones (9 drops over 20,000 commits), and
storage stays under 3.0 entries per live key. With C = 1, a weekly consumer
at 1M holds 2.05 at most.

**How compaction meets observers.**

- A merge is planned against the boundaries that exist when it is planned.
  A boundary born later is past every input.
- A boundary that disappears (a position moves, a pass ends) only allows
  more merging. The next plan uses it.
- A position never moves backwards: it advances, or it is dropped and its
  full pass lands at the head + 1. A reset, move or removal of the output
  drops the whole index.
- Outputs are written under names unique to their merge, as compaction's
  are today, so a retried or duplicated merge is harmless. Installing one is
  one event (spans in, span out), and applying it can assert that no
  current boundary lies strictly inside the new span.
- Cleanups need nothing from compaction. A commit's delta lists the version
  it replaced for every key (exact predecessors). The cleanup reads those
  from the delta file, which `cleanup_reads` keeps alive until the cleanup
  is done, whether or not a merge has absorbed the span. Store objects stay
  protected by pins, as today.

## What changes, and what is deleted

Deleted, each because none of the four operations needs it once writes are
exact and the spans exist:

| Today | Why it goes |
|---|---|
| Levels and the leveled planner: `FileInfo.level`, `l0_max_files`, `level_base`, `fanout`, the "push the least-overlapping file" rule | spans, and the policy above |
| The delta log as a second list: `IndexState.log`, `keep_log`, `_consumed`, `covers`, `slice`, `truncated`, the `IndexTruncated` event and the truncation in `Upkeep.truncate` | the spans are the log; trimming is the base merge |
| The range tree (proposed, never built) | spans give catch-up its few files |
| The inexact count and the recount: `inexact`, `count_exact`, `Delta.exact`, `DeltaFiles.exact`, the recount job and `recount_interval`, `Known::Other`, `Old::Other`, `Sparse.inferred`, `resolve(exact=)` | writes are exact (the first study's option A) |
| The tombstone Bloom filter (format v4 keeps one filter) | it only served the inferred shortcut: under exact writes, a key-filter hit reads the block anyway, for the predecessor |
| `.kg` garbage files: `Merge.compact(garbage=)`, `GarbageFile`, `decode_garbage`, the `sidecar` cleanup kind, `native/src/garbage.rs` | they listed the objects inexact deltas missed; exact deltas list every replaced version |
| Compaction dropping predecessors outside the bottom level | one merge rule: the oldest predecessor survives, and only the base drops them |
| `pending`'s per-page restart of a merge over thousands of deltas | a catch-up merges a handful of spans |
| Index-file pins held for the whole of a multi-batch pass or a pattern-change drain (`pass.pin`) | their boundary keeps their view readable from current files; pins remain for attempts and store objects |
| Per-key consumer payloads in the index (K47) | consumer records are a position plus read-ahead entries |

Changed:

- `IndexState`: `count`, `spans: [(a, b, files)]`, `prefix`. A file
  belongs to one span.
- One native merge with a `base` flag replaces the compaction merge and the
  range merge (`Merge.compact`, `merge_ranges`).
- Readers: `_find`, `_scan` and `pending` already take a list of sorted
  file sets, newest first (today's levels), so they take spans.
  `pending(P, N)` reads the spans of `[P, N]` and reports each key's oldest
  predecessor beside its newest entry (the first study's `Merge::existed`).
- Upkeep plans with the policy above, against boundaries derived from
  positions, passes and drains: what `Upkeep.truncate` computes today as
  `needed`, from positions and claims.
- The read-ahead's per-key bound is the generation of the upstream commit it
  read at. The state it delivered comes from the attempt's spec.

Kept: the `.kx` format (blocks, key filter, payloads for source versions
and failure records), the engine cache and engine-served reads, `DeltaKeys`,
cleanups driven by delta predecessors, pins for attempts.

New words for the glossary: **span** (replacing level, delta log and range
file; "run" is taken), **boundary**, **base**. "Observer" names the four
kinds of reader in the table above. "Delta" stays: a commit's span.

## Rejected alternatives

- **Today plus the range tree** (two orderings, three structures). It works
  (the range tree study measured it), but it keeps two merge rules, two
  deletion rules, a leveled planner writing 16–31× and range files writing
  3× more. The tiling gives the same catch-up and both orderings with one
  structure, and writes less.
- **A key-sorted multiversion LSM**: each key keeps its versions across
  levels, and compaction keeps the versions observers need. Catch-up loses
  time locality. With random writes, every bottom-level block holds some
  key changed after `P`. So `changes(P → N)` from a day back reads about the
  whole index: ~0.7 GB at 100M, against 84 MB from the spans. The tiling is
  this structure restricted to time-contiguous files, and that restriction
  is what buys the locality. One idea survives as an escape hatch: a span
  merged across a boundary could keep one "live before" bit per key per
  inner boundary. That would bound the span count with very many observers.
  It isn't needed at the counts measured.
- **A persistent or prolly tree** (Dolt): one structure for "live now",
  "live at N" and the N..H diff. On random keys, each changed key rewrites
  its leaf. 1K random updates at 100M rewrite ~1K leaves, 4–64 MB per
  commit, against a ~15 KB delta plus 10–17× compaction. A consumer a day
  behind diffs two trees in which nearly every leaf differs: with 400-key
  leaves and 8.6% of keys changed, a leaf stays untouched with probability
  0.914^400 ≈ e^−36. That reads ~2× the index, against 84 MB. A consumer
  one commit behind reads ~1K leaves, against one delta. Retained roots need chunk
  garbage collection. It wins on clustered keys and many cheap snapshots,
  neither of which is this workload.
- **Pinned snapshots at each position** (the first study's option B). With
  boundaries, the state at `P − 1` is readable for free. But the consumer
  would still look up every changed key there: 1,758 GETs per 1K keys at
  100M, on every pass, where the writer paid once.
- **Resolution deferred to compaction or to readers** (blind writes). The
  live count and the cleanups wait for resolution. In the tiling, a blind
  entry resolves only at its group's merge into the oldest span, or by
  each consumer's lookup at `P − 1` (option B's cost). Exact writes cost
  only cold writers (an engine with its cache answers in ~113 ms with no
  GETs), so they stay. The structure doesn't rule out adding blind writes
  later for cold, write-heavy outputs.
- **Aligned spans** (the range tree's blocks applied to the tiling). The
  tiling would then be a pure function of the head and the boundaries, but
  a decomposition costs up to 2(f − 1) spans per level per boundary: the
  range tree study counted 25 files (43 worst) at fanout 8 for one
  catch-up, against ~5 here. Alignment is needed only when a boundary can
  fall inside an existing span, and boundaries are born at the head + 1.
- **Capping spans per group by merging the smallest neighbours.** Measured:
  the newest span absorbs every commit, 540–1,300× written at 100M.

## A worked example

`items` holds `a` and `b`, written by commit 0 at generation 1: the base.
Commit `c` writes at generation `10c` after that.

| Commit | Change | Delta (span `[c, c]`) |
|---|---|---|
| 1 | add c | c g10 |
| 2 | remove a | a tombstone g20, pred 1 |
| 3 | re-add a, update b | a g30 (no pred: a was a tombstone); b g30, pred 1 |
| 4 | add d, update c | d g40; c g40, pred 10 |
| 5 | remove d | d tombstone g50, pred 40 |

Two consumers keep `tally = ctx.load() + len(added) − len(removed)`.

- **X** sits at position 1, with tally 2 (`a`, `b`).
- **Y** read through commit 2 and sits at position 3, with tally 2 (`b`,
  `c`). Then a keys= run named `d` and read it at head 4. Y's record is now
  position 3 plus the read-ahead entry `[4, run, attempt]`: `d` was
  delivered upserted, and its bound is commit 4's generation, 40. Y's tally
  is 3.

The boundaries are 1 (X) and 3 (Y). Compaction may merge `[1,1]` with
`[2,2]` (no boundary at 2), and `[3,3]`, `[4,4]`, `[5,5]` together (none at
4 or 5), but never across 3:

```
base  [0, 0]  a g1 · b g1
span  [1, 2]  a tombstone g20 pred 1 · c g10
span  [3, 5]  a g30 · b g30 pred 1 · c g40 pred 10 · d tombstone g50
```

In `[3, 5]`, `d` keeps its oldest predecessor (none, from commit 4) and its
newest entry (the tombstone), so it is a tombstone naming no predecessor.

- **Lookup at the head:** `a` live (g30, found in `[3, 5]`), `b` and `c`
  live, `d` absent. The count went 2 → 3 → 2 → 3 → 4 → 3, and it is 3.
- **X's catch-up, `changes(1 → 5)`,** merges both spans. `a`: newest g30,
  live; oldest predecessor 1, from `[1, 2]`; so updated. `b`: updated. `c`:
  newest g40; oldest entry in `[1, 2]` names no predecessor; so added.
  `d`: a tombstone naming none, so neither, not delivered. Tally 2 + 1 − 0
  = 3: `a`, `b`, `c`.
- **Y's catch-up, `changes(3 → 5)`,** reads `[3, 5]`. `a` names no
  predecessor and is live: added. `b` and `c`: updated. `d` is in the
  read-ahead: its newest generation, 50, is past the bound 40, so it
  changed after Y read it. It was delivered upserted and is now gone:
  removed. Tally 3 + 1 − 1 = 3: `a`, `b`, `c`.

Two counterfactuals show why the rules exist:

- **Merging across Y's boundary** would make `[1, 5]`. There `a`'s oldest
  predecessor is 1 and `c`'s oldest entry names none, so Y would be told
  `a` was updated (it never had `a`) and `c` was added (it already had
  `c`). Its tally happens to land on 3, but an `each=True` consumer would
  never build `a` and would build `c` twice.
- **Dropping `d`** as "added and removed inside `[3, 5]`" would hide its
  removal from Y, and Y would keep `d` forever.

Once Y's next pass lands at 6, boundary 3 goes and the two spans may merge.
Once X moves past 5 too, the first group merges into the base: `a g30 · b
g30 · c g40`. `d`'s tombstone and every predecessor are dropped. The
versions that dropped out (`a` g1, `b` g1, `c` g10, `d` g40) were already
cleaned up, from the predecessors in deltas 2–5. No `.kg` file is
involved.

## What the TLA+ spec and the sim must check

**TLA+, a new `KeyIndex.tla`** (Positions.tla keeps the records, and per
K47 an `each=True` consumer uses the same record). Model: two or three keys,
about six commits, the spans as `(a, b, key → [newest, pred])`, observers
(positions, passes with landing points, a drain, the cap), pins and
file deletion. Actions: `Commit` (exact: a predecessor iff live),
`Merge(i)` and `MergeIntoBase`, guarded by the boundary rule, the observer
actions, and `Delete` after pins. Invariants:

- **Tiling**: the spans cover `0..head` contiguously, and every live
  boundary starts a span.
- **Reads agree with history**, from `Execution.tla`'s full log: for every
  observer, `changes(P → N)` from the spans equals the classes computed
  from the log, `scan(c)` equals the live set at `c`, and a lookup at the
  head equals the live set.
- **Read-ahead**: for a key read at `r`, the class from (delivered state,
  newest generation against `r`'s generation) equals the truth relative to
  `r`.
- **Count**: over a pass, added − removed = live(N) − live(P − 1).
- **No dangling read**: every file an in-flight reader holds exists.
- **The cap never drops a position with a pass under way.**
- **Calibration**, each fix switched off must fail: merging across a
  boundary (reads disagree); dropping neither-entries outside the base
  (read-ahead fails); a boundary born before the head (a span gets cut);
  deleting without pins (dangling read).

**The sim** (`tests/sim`):

- **Exact deltas as a property.** Every committed entry of key k at commit
  c names a predecessor iff k was live after c − 1, through every writer:
  warm engine, cold sparse, streamed patch, replacement, source commit,
  repair, retry. That needs the cold mode the first study asked for:
  resolver off, `bits_per_item=1`, `whole_threshold=0`, `small_file=0`,
  streaming off.
- **Compaction anywhere.** Random merges interleaved between pages, passes
  and commits, with boundaries derived from the model, checking after every
  step that each boundary starts a span and each reader's answer equals the
  oracle's.
- **The tally.** Each consumer's sum of added − removed tracks the live
  count under its patterns, with read-ahead entries, batches split
  anywhere, and positions dropped by a small cap. A dropped consumer's full
  pass rebuilds to the oracle.
- **Cleanups.** Every replaced object is cleaned up once, never while a pin
  could read it, and with no `.kg` files.
- **Bounds**, reported rather than asserted: spans, and entries per live
  key, against the replay's numbers.

## Open questions

- **C = 2** for the cap, and **4** for both the size classes and the
  merge-into-oldest ratio, are the replay's choices. Size classes of 8
  write 0–13% less at 1M and about the same at 100M (−18% to +6%), for up
  to ~50% more spans.
- **A 100M base merge takes ~27 s.** It can stream in key slices over
  several upkeep steps, since nothing observes the commits it absorbs. That
  isn't needed now.
- **Very many observers on one partition.** Spans grow with distinct
  boundaries (one per consumer position, at least). Dozens cost one more
  GET per span on cold reads, in the same parallel round. Hundreds would
  call for the per-boundary bits above.
