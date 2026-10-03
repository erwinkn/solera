# Range files over the delta log (design study)

Status: **proposal**, for Erwin. Follows `presence-at-position.md` and
assumes its option A (every delta entry names its predecessor exactly).
Measured by `bench/keys/ranges.py`; the prototype merge is
`_native.merge_ranges` (`native/src/format.rs`).

## The problem

A consumer at position N catches up by reading one delta file per commit
from N to the head and merging them by key. At 10,000 commits behind that
is 10,000 GETs and a 10,000-way merge. `KeyIndex.pending` also restarts
that merge for every 100K-key page: 100 s at 1M keys, 308 s at 100M. A
daily consumer of a source that commits every 10 s is 8,640 behind each
time it runs, so this is a normal case.

## Range files

A **range file** is a delta over several commits. It covers commits
[a, b] and holds, per key changed in them:

- the key's newest entry in the range (generation, deleted, payload);
- as predecessor, the key's generation before a, or none if it was not
  live.

That is today's `.kx` format, with the predecessor meaning what it means in
a per-commit delta. A per-commit delta is the range [c, c]. A consumer
classes keys from range files exactly as from deltas: predecessor present
means the key existed at N, and the newest entry says what it is at H.

**The merge is associative.** Merging adjacent ranges keeps, per key, the
newest entry of the newest range and the predecessor of the oldest. Both
are "first" and "last" over a sequence, so any grouping gives the same
file. Tombstones and re-adds need no special case. A re-added key's oldest
entry in the range is the tombstone, which names the live generation
before it, so the key reads as updated. The bench checks this key for key:
for 1, 100 and 10,000 commits behind, at 1M and 100M keys, the range tree
gives exactly the per-commit merge's classes.

**Nothing is dropped.** A key added and removed inside a range stays, as a
tombstone naming no predecessor. Dropping it would cap a range at the keys
live at its two ends, but it breaks readers whose base is inside the range:

- A K45 `keys=` run reads k at commit c, where k was added after N; k is
  removed after c. In the range [N, H], k is absent at both ends. Dropped,
  the next pass never delivers k's removal, and the consumer keeps it
  forever. Kept, its newest entry is a tombstone newer than c: delivered as
  removed.
- The same holds for an `each=True` output key built by a `keys=` run
  inside the range.

So a range holds every key changed in it. That is at most the keys live at
either end plus the churn: with no churn, about the index. The brief's
bound ("never more than the live keys") holds only without churn.

**K45 read-ahead.** For a key an entry read at commit c, the class is
relative to c, not N. The newest entry says whether it changed after c
(generations rise with commits) and its state at H. The state at c comes
from the read-ahead's own batch (delivered as upserted or removed). The map
planning already builds (key to generation read) needs that one bit
more. A key missing from the range was not changed past N, so the
read-ahead never delivered it.

## The tree

Block (l, i) covers commits [i·f^l, (i+1)·f^l − 1]. Level 0 is the
per-commit deltas; a block of level l merges its f children in one f-way
merge. A consumer at N reads the greedy decomposition of [N, H]: from N,
the largest complete block starting there, then again from its end. That
is at most 2(f − 1) blocks per level.

**Built by upkeep, after the commit that completes a block**, like
compaction: same scheduler, same native `Merge` and `Writer`, same journal
event shape (files in, files out), and written through the engine cache.
Readers never wait for a build: until a parent exists they read its
children. A block that starts before the oldest boundary (below) is never
built. When every consumer reads after every commit, nothing is built at
all. The cost falls only on laggards.

**Deleted by two rules**, over boundaries: each position's `next`, a
pattern change's split commit, and each claim's first commit and the one
after its last (where its position will land).

1. A block that starts before the oldest boundary goes. No reader can use
   it whole. This is today's log truncation, applied to blocks.
2. Once a block is built, its children go, unless a boundary lies strictly
   inside it.

The bench asserts the kept set holds every block each decomposition needs.
Deletions go through the existing pinned garbage order, so a pass pinned
before a deletion still reads its files. Per-commit deltas also stay while
level 0 of the index or a pending cleanup references them, as today.

**Races.** A pass in flight is covered by its claim, which protects its
first commit and its landing point, and by its pin. Without the landing
point, a parent built during the pass would delete the children the next
decomposition starts at. A new consumer's full pass lands at its pinned
head + 1, also protected by its claim. Builds are deterministic and only
add files, so a retried or duplicated build is harmless (named per
attempt, like compaction outputs).

**Checklist.** Retries: only committed deltas enter the log, and blocks
are built after commit. Upstream full runs are streamed replacements,
already exact. Output resets and removals drop the index, the log and the
positions on it, so the consumer does a full pass. Renames keep the index
and its tree. A pattern change's diff pass reads the head; its split
commit is a boundary, so [next, split] decomposes like any range.

## Measurements

Log of 12,000 commits of 1K keys (90% updates, 5% removes, 5% adds or
re-adds) over 1M and 100M keys; the consumer 10,000 behind sits at commit
2,000, not on an aligned boundary. Local object store, 30 ms per request,
80 MB/s per connection, 64 in parallel; fanout 2 measured.

**Catch-up** (classes and counts computed; "today" from the first study's
10,000-commit log):

| Behind | Today, paged | One merge (pending fixed) | Packed object | **Range tree** |
|---|---|---|---|---|
| 1 | 1 GET, 34 ms | same | same | same |
| 100 | 100 GETs, 0.11 s | 100 GETs, 1.0 MB, 0.1 s | 1 GET, 0.07 s | 3 GETs, 0.9 MB, 0.12 s |
| 10,000 at 1M | 10,000 GETs, 100 s | 10,000 GETs, 101 MB, 11.2 s | 7 GETs, 101 MB, 6.3 s | **10 GETs, 37 MB, 1.2 s** |
| 10,000 at 100M | 10,000 GETs, 308 s | 10,000 GETs, 97 MB, 11.3 s | 6 GETs, 97 MB, 6.6 s | **14 GETs, 84 MB, 2.4 s** |

A `keys=` run of 100 keys, 10,000 behind: 10,000 GETs and 6.5 s through the
per-commit deltas; through the range files, 10 GETs (1M) or 109 GETs and
23 MB (100M, filters then blocks), in 0.2 s.

At 1M, 10,000 commits touch every key ~12 times, so the range files
deduplicate 101 MB down to 37 MB. At 100M nearly every change is a distinct
key, and the gain is in requests and CPU, not bytes.

**Writes and storage**, per level sizes measured at fanout 2; other
fanouts keep a subset of the same blocks:

| Fanout | Bytes written, × the deltas (1M · 100M) | PUTs per commit | Files read, 10,000 behind (worst) |
|---|---|---|---|
| 2 (measured) | 9.3 · 11.2 | 1 | 10 (19) |
| 4 | 4.4 · 5.2 | 0.33 | 16 (29) |
| 8 | 2.7 · 3.4 | 0.14 | 25 (43) |
| 16 | 1.9 · 2.4 | 0.07 | 40 (74) |

That is for a consumer lagging over the whole log; one that keeps up costs
nothing. Building all 13 levels of 12,000 commits took 24–28 s of one core,
about 2 ms per commit. Stored, for the consumer 10,000 behind: 37 MB at 1M
and 84 MB at 100M, against 101 MB and 97 MB of per-commit deltas.

**Packing** (Erwin's original idea: deltas back to back in one object,
with an offset index) cuts the requests to a handful but keeps the bytes
and the 10,000-way merge (6.3–6.6 s of CPU). Nothing needs per-commit
detail once a commit's cleanups are done, so ranges win on every axis.

## Concepts

- **New:** the range file (a delta over several commits, same format, same
  meaning of the predecessor) and the tree's alignment (block (l, i),
  fanout f). `IndexState.log` becomes a list of blocks.
- **Deleted:** the horizon rule from the first study. A catch-up reads at
  most ~2(f − 1) files per level, holding the keys changed, so a full pass
  is never cheaper for that reason. The per-page restart stops costing
  anything: a page merges a few runs, not thousands. Log truncation is now
  rule 1, not a separate rule.
- **Shared with compaction:** format, merge, writer, upkeep scheduling,
  garbage order, engine cache. **Not shared: the files.** A level file
  holds a key range over whatever time its merges spanned, so it has no
  commit boundary at N. Compaction also drops predecessors and, at the
  bottom, tombstones. The output rule differs by a few lines (newest entry
  plus oldest predecessor, nothing dropped).

## Recommendation

Build range files, fanout 8: about 3× the delta bytes written by upkeep,
and only for laggards; one PUT per 7 commits; a catch-up of 10,000 commits
in ~25 files, one parallel round (fanout 2 measured 1.2–2.4 s, against
100–308 s today).
Fix `pending`'s per-page restart in the same change, since it shares the
merge. Drop the horizon. Don't build packing.

Tests: the presence property from the first note, now over a tree with
random fanout and random positions, claims and read-ahead entries (classes
equal the per-commit merge's); and the deletion rule, as an invariant that
every boundary's decomposition is buildable from kept blocks at every step
of a random history with upkeep and passes interleaved.
