# Key index: two views, designed from scratch

Status: **phase 1 design** (W53, T26; D105), for Erwin and the coordinator.
No product code. Phase 2 prototypes both views and measures them against
spans (`key-index-design.md`) on the same harness; the plan is at the end.
Written against main at dff5077; spans' later fixes (d880955) are noted
where they change a comparison.

The brief: design a key-ordered view and a time-ordered view, each for its
own workload alone, and reuse existing code or formats only where the
from-scratch answer lands on them independently. Spans being built is not a
reason for anything below. Neither is the earlier study
(`docs/key-index-two-views.md` on `study/key-index-two-views`): its
conclusions are re-derived or dropped here, and where this design differs it
says so.

Every number says where it comes from:

- **measured**: micro-measurements on this branch, one core, no I/O
  (`bench/keys/views/codec/`, `globs.py`; outputs committed beside them);
- **replayed**: the structure run on metadata with the uniform-key density
  model that the span replays use (`bench/keys/views/model.py`, output
  `model.tsv`), so it compares with the span design's *replayed* tables;
- **cited**: the span design's own numbers, with their label there.

Nothing here is measured on object storage yet. That is phase 2.

## The answer in brief

- **One delta per commit stays**, one PUT, written by the attempt as today.
  It is level 0 of both views. Nothing else happens on the commit path.
- **The time view (T)** is an aligned tree over commit numbers with fanout
  b = 4. A level-j **node** covers commits `[i·4^j, (i+1)·4^j − 1]` and holds
  each key's net change over that range: its state before and its state
  after. Level 1 is a **pack**: four deltas copied into one object, unmerged.
  `changes(P, N)` merges the files that tile `[P, N]` (its **cover**, up to
  25 of them) a page at a time.
- **The key view (K)** is a **base** (every live key once, as of some commit
  `w`) plus the T nodes that tile `w + 1` to the head (its **chain**, 8–19
  runs). The base absorbs the chain when one level-8 node (100M keys) or
  level-5 node (1M keys) is complete. K has no merge pipeline of its own:
  K's ideal upper runs are geometric tiers of recent changes, and T's aligned
  nodes are exactly that.
- **Retention is one number, the floor.** The floor is the oldest commit any
  reader may still start from. T keeps every node at or above it and deletes
  the rest. Any commit at or above the floor is a valid starting point, so
  landing points, retries and selections need no reservation in T. Snapshot
  reads pin a K manifest (base files, `w`, and the commit), nothing else.
- **No versions, anywhere.** Every file holds a key at most once. No
  endpoints inside files, no generation bounds, no read rule, no span cap, no
  forced merges, no stale-version rewrites.
- **The format is v4's container and row entries**, with zstd-1 instead of
  zlib-1, 16 KiB blocks instead of 64 KiB, and no Bloom filter on the base.
  Measured: 13% (ids) to 27% (UUIDs) smaller and 1.4–3× faster to decode;
  a cold point lookup decodes ~8× less per key. These are format
  levers that spans could adopt too; phase 2 measures spans both ways so the
  structural comparison stays fair.
- **Globs use a skip-scan over the block index.** It skips a block when no
  key between its first and last can match. Zero bytes stored; on
  path-shaped keys it reads 1.0–1.6× the blocks that hold a match. Per-block
  trigram filters never did better (measured).
- **Replayed costs.** Background writes ~9.2 per entry committed at 100M
  keys (spans: 12.4–14.4) and ~5.8 at 1M (spans: 6.6–9.4); K holds 1.31
  entries per live key at 100M (spans 1.13–1.24) and 1.46 at 1M (spans
  1.65–9.9); a day-behind catch-up reads 1.03× what changed at 100M and
  3.9× at 1M (spans: 1.03–1.8× and 1.25–7.1×). T stores ~0.5 entries per
  live key per day of reader lag at 100M and ~40 at 1M. Storage is where
  this design pays.
- **Six of A17's ten findings cannot arise; two get simpler; two stay.** The
  A19 fixes get what they need: a pinned snapshot (K), the state at a
  reader's end (K at N), the net rule and an early-stopping merge (T).

## Shared ground

### What a commit writes

A commit must stay one PUT, and both views must cover the head the moment
the commit lands, or every read would need a second path for "recent
commits". So the one object a commit writes has to be the newest piece of
both views. It must carry what each needs:

- K needs each written key's new state: generation, deleted flag, payload;
- T needs the same plus the key's state before the commit: was it live, and
  its payload (for the net rule).

That is exactly today's delta: a key-sorted `.kx` file, each entry with its
new state and its predecessor (generation, and payload on a
payload-bearing index). The writer resolves it exactly against K's head, as
today. So the delta is not reused out of convenience; it is what the
constraint forces. Its predecessor generation also drives store cleanup, as
today.

### What is read, by whom

| Read | View | Who issues it | When it is cold |
|---|---|---|---|
| exact head lookups of written keys (predecessor, live count) | K at head | the writer: the engine's resolver (warm) or a worker | engine restart; workers without engine service |
| point lookups at the head (keys=, immutable-store reads, failed keys) | K at head | workers, the engine | as above |
| a full pass: every key at a pinned commit, a page at a time | K pinned | a consumer's attempts | pages too large for the engine's record |
| prefix and glob pages | K (head or pinned) | consumers with patterns | as above |
| `changes(P, N)`, paged and resumable, keys= and range forms | T | consumers, staleness | catch-ups past the record budget |
| the state at a reader's end N (read-ahead, removals owed) | K at N | consumers | as above |

## The time view

### Structure

A level-j node covers the aligned range `[i·b^j, (i+1)·b^j − 1]`. For every
key changed in that range it holds one entry, in the delta's shape:

- the key's state **after** the range: generation, deleted flag, payload;
- its state **before** the range, as the predecessor: present or not, its
  generation and (payload-bearing indexes) its payload.

A commit's delta is the net change of one commit; a node is the net change
of 4^j commits. Same shape, same format, same reader. The node is built from
its four children by a k-way merge by key: **after** from the newest child
holding the key, **before** from the oldest. The merge is associative, so
any tiling of `[P, N]` by nodes gives the same answer.

**Dropped when building**: a key absent before and absent after (added then
removed inside the range). Nothing needs it (see read-ahead below).

**Kept**: a key live at both ends with equal payloads (a source that went
v1 → v2 → v1). Its class is "neither", decided at read, but its after
generation must survive: if an older node in a cover updated the key and
this one reverted it, the cover's after state must name this node's
generation, since the store has already cleaned up the older one. (The
earlier study's checker found the same; this design keeps the rule.)

**Level 1 is a pack, not a merged node.** A pack is the four deltas copied
byte for byte into one object, each still a complete `.kx` section, plus a
small directory (commit, offset, length per section). Readers treat a whole
pack as a level-1 node by merging its four sections on read, which costs the
same bytes as a merged node would hold (four 1K-key commits barely overlap).
The pack exists for one reason: it gives every old commit's delta a name the
reader can compute (below), so T's state in the journal stays a handful of
numbers instead of one entry per commit.

### Reading `changes(P, N)`

The **cover** of `[P, N]` is computed left to right: from `c = P`, take the
largest node that starts at `c` and ends at or before `N`, then continue
after it. Example, b = 4, `P = 1,237`, `N = 1,300`:

```
1,237  1,238  1,239                         three sections of pack [1,236 – 1,239]
[1,240 – 1,243]  [1,244 – 1,247]            two packs (level 1)
[1,248 – 1,263]  [1,264 – 1,279]  [1,280 – 1,295]   three level-2 nodes
[1,296 – 1,299]                             a pack
1,300                                       a section of pack [1,300 – 1,303]
```

Ten reads, each a node, a pack or a pack section (the three sections of one
pack are one range read). At most b − 1 nodes per level
on each side, so a reader 10,000 commits behind opens 18 on average, 25 at
most (replayed, b = 4).

Pages go by key, as today: each page reads, from every file in the cover,
the blocks below the next key bound, merges them, classes each key by its
state before `P` (the oldest file holding it) and after `N` (the newest), and
returns the page and a cursor. Classes are today's: added, updated, removed,
and "neither" for a payload-bearing key live at both ends with equal
payloads. Resuming a page recomputes the cover; nodes built in between
change which files it opens, not the answer.

- **keys=** reads, per file in the cover, the blocks that may hold the keys
  (with the filter where it pays, below).
- **Staleness** runs the same merge and stops at the first key it delivers.
- **Round trips**: 2 for the first page (file tails, then blocks), 1 per page
  after, as for spans. Small files (packs of 1K-key commits are ~40 KB) are
  read whole in the first round trip.

### Read-ahead: classed from K at N, not from T

A read-ahead entry says a keys= run delivered key `k` as of commit `r`, live
or removed. The next delta must class `k` against what was delivered. All it
needs is `k`'s state at `N`:

| Delivered at r | At N | Class |
|---|---|---|
| live | live, generation ≤ g(r) | skip: unchanged since it was read |
| live | live, generation > g(r) | updated (neither if a payload-bearing key's payload is equal) |
| live | absent | removed |
| removed | live | added |
| removed | absent | skip |

So read-ahead keys are looked up in K at N, in one batch, in parallel with
the first page (no extra round trip), and left out of T's own classing. This
is why T may drop absent-to-absent keys: the one reader who would need them
(a key delivered live, then removed and its whole history inside the range)
gets "absent at N" from K and is classed removed. Spans keep those
tombstones in every span outside the base for this case (A12-1); T does not
need to.

*Example.* A consumer sits at `P = 3` and a keys= run delivered `d` live at
`r = 4`. Commit 4 added `d` (it was absent before 3), commit 5 removed it.
T's node for `[4, 5]` drops `d` (absent before 4, absent after 5). K at 5
has no `d`: delivered live, absent now: removed. Without the K lookup the
consumer would keep `d` forever.

### Retention: the floor

The **floor** is the oldest commit any reader may still start a T read from:
the minimum of every consumer position (including a pass's start and a
pattern change's split), K's base watermark `w + 1`, and every pinned K
manifest's `w + 1`. T keeps every merged node that starts at or after the
floor and every pack that ends at or after it (a reader positioned inside a
pack reads its later sections); it deletes the rest.

Why this is enough:

- A cover for `[P, N]` with `P ≥ floor` only uses files starting at or after
  `P`, or sections of the pack holding `P`. All exist (covers fall back to
  smaller nodes where a level is not built yet).
- New readers are born at the head + 1, above everything, so **the floor
  never decreases** in the durable state. A file below the floor is garbage
  forever.
- **No reservation for landing points.** A keys= selection, a covering retry
  or a pass's batch lands at the head + 1 at its claim. In T that commit is
  above the floor by construction: nothing needs to remember it. (Spans need
  every landing point reserved exactly, since merges coalesce across any
  commit that is not an endpoint: A17's R5.)

What it costs: storage. With a reader a day behind, T holds every level's
nodes for a day of commits: ~0.52 entries per live key at 100M (52M entries,
~0.6 GB at ~11.5 B per node entry), ~40 at 1M (40M entries, ~0.5 GB)
(replayed). A reader a week behind holds seven times that. Keeping only the
nodes on each reader's path (the earlier study's "chains") cuts this, but
brings back exact reservations for every reader. Storage is last in
Erwin's priorities, so the floor wins. The physical budget still applies: a
reader whose catch-up would cost more than a full pass is dropped to one.

If no consumer reads the index, the floor is `w + 1`: T holds only K's chain.

### T's state, names and builds

T's state in the journal, per index: the floor, and per level the commit up
to which nodes are built. Nothing per node. File names are computed:
`t{j}-{start:012d}-e{epoch}.kx`, the epoch being the engine's fencing epoch
(D86). The state records, per level, the commit from which each epoch built
(a new entry only at a takeover). Readers get a per-level tail-size hint in
the same state, so the first round trip is one suffix-range GET per file
(`bytes=-hint`), which also returns the object's size.

Unpacked deltas (the commits since the last complete pack, normally ≤ 3)
stay listed in the state, as today's newest spans are.

**Builds.** When a pack's four commits are in, upkeep copies the four deltas
into the pack and publishes it (one journal record, "level 1 built through
c"). When four nodes of level j are built, it merges them into one level
j + 1 node. A level above K's base level is built only while the floor is at
or below its start (some reader is far enough behind to use it). Builds are
deterministic: the same children give the same bytes, so a build retried or
duplicated by a zombie is harmless, and each output still has a unique name
by epoch.

**Indexes that skip commits** (a failure index, written only when keys
fail): the state records skipped commit ranges, and a node whose range lies
inside one is empty and never written.

### Alternatives for T, compared

| Option | What it costs | Verdict |
|---|---|---|
| Raw deltas only | a reader 10,000 behind opens 10,000 files: 157 waves of 64 GETs, ≥ 4.7 s of round trips | no |
| One packed log, merged at read | 6–7 GETs at 10,000 behind, but every page merges every delta: measured 5.5–6.6 s for the first page, ~1.5 GB peak (cited, span design) | no: the first page waits for the whole range |
| Nodes merged between live endpoints (blocking spans) | one read per gap between readers: 543–546 files with 100 daily readers (cited) | no |
| Spans (versions kept per endpoint) | one structure for both views; at 1M keys with lagging readers, reads 3–7× what changed and lookups carry the versions | the incumbent; phase 2 measures it |
| **Aligned tree, b = 4, floor retention** | 18 files at 10,000 behind; reads 1.03× what changed at 100M | **chosen** |

Fanout, replayed (writes per entry committed for T and the base, without the
delta itself; files a reader opens, mean (max); read relative to what
changed):

| b | 100M: writes | 100M: 360 behind | 100M: 10,000 behind | 1M: writes | 1M: 10,000 behind | T stored per live key, floor a day back (100M · 1M) |
|---|---|---|---|---|---|---|
| 2 | 18.5 | 7.4 (11), 1.00× | 12.2 (17), 1.03× | 12.0 | 12.2 (17), 3.6× | 1.1 · 86 |
| **4** | **9.2** | **11.4 (18), 1.00×** | **18.4 (25), 1.04×** | **5.8** | **18.4 (25), 4.3×** | **0.5 · 41** |
| 8 | 8.2 | 18.6 (24), 1.00× | 29.4 (39), 1.04× | 5.0 | 29.4 (39), 4.7× | 0.3 · 26 |
| 16 | 5.2 | 29.5 (45), 1.00× | 45.4 (55), 1.04× | 2.4 | 45.4 (55), 5.1× | 0.3 · 18 |

(r = 4 for 100M and 1M; `model.tsv` has r = 2 and 8.) Files a reader opens
cost requests, not round trips: they are fetched in parallel. b = 4 halves
b = 2's writes for 50% more files; b = 8 saves little more.

**The weak spot: small indexes, far readers.** At 1M keys a day of 1K-key
commits changes nearly every key, and the cover of a day holds several
high-level nodes that each touch most keys: the reader reads ~4× what
changed, ~4M entries (~45 MB), where a full read is 1M. Spans read 1.25–7.1×
(cited, replayed) on the same patterns. It is cheap in absolute terms (one
or two seconds of transfer at 1M), but it is not the 1.0× T gives at 100M.

## The key view

### Structure

K at commit `S` = the **base** (every key live at `w`, once, no tombstones,
in key-range files of ≤ 64 MB) ⊕ the **chain**: T's cover of `[w + 1, S]`,
read newest first. The newest entry of a key wins; a tombstone in the chain
hides the base's entry.

Why the chain is T's nodes. Designed for lookups and pages alone, K wants a
small number of sorted runs whose sizes grow geometrically, so that each
entry is rewritten once per tier and a read opens few runs (a size-tiered
LSM with a base). With commits of similar size, T's aligned levels are those
tiers: level j holds 4^j commits. So K reads T's nodes rather than writing
its own copies of the same entries. Where commit sizes are skewed (a 1M-key
commit), size-tiering would rewrite the big commit less often than aligned
levels do; T must build the aligned levels anyway for catch-up, so K takes
them for free.

**The base merge.** The base absorbs the chain once a node at the base level
`jb` is complete: the smallest level whose node holds at least a quarter of
the base (r = 4): `jb = 8` (65,536 commits) at 100M keys, `jb = 5` (1,024
commits) at 1M (replayed). The merge reads two inputs, the base and that one
node, and writes a new base: newest wins, tombstones dropped. With clustered
keys, base files whose key range the node does not touch are kept as they
are.

Replayed (b = 4, r = 4): K opens 10.5 runs on average, 19 at most, at 100M
(8.5 and 16 at 1M), and holds 1.31 entries per live key at 100M (1.46 at
1M). Spans hold 1.13–1.24 at 100M and 1.65–9.9 at 1M with lagging readers
(cited, replayed), since every reader's position keeps versions.

### Reads

- **Head lookups** (writers, keys=): every run's tail in round trip 1, then
  one block per key per run that may hold it, newest first, all in round
  trip 2. Small chain files are read whole in round trip 1.
- **Pages** (full passes, prefixes): today's windowed merge of sorted runs,
  newest first, a key cursor. One entry per key per run, so a page never
  splits a key's versions (A17's R2 cannot occur).
- **Prefixes**: a seek in every run's block index.
- **Globs**: below.
- **At a pinned commit**: the same reads over the pinned manifest.

### Snapshots: pins

A full pass at head `S` (D93: it reads a pinned snapshot) **pins K at S**:
the base files it was handed, `w`, and `S`. The chain is recomputed from T's
state at each page: T's nodes over `[w + 1, S]` start at or after `w + 1`,
which the pin holds in the floor. The pinned base files stay until the pin is
released, through the existing pin mechanism. A delta pass reading `[P, N]`
over several attempts pins K at N the same way, so its read-ahead lookups see
N.

What a pin costs: if a base merge runs during the pass, the old base stays
until the pass ends, one more copy of the base (~0.8 GB at 100M). Spans pay
the same for a slow attempt pinned across a base merge.

**The alternative**: K at S = K at the head with T's "before" states over
`[S + 1, head]` applied on top (an undo). No base pin, but every page also
reads that part of T, and the reads grow while the pass runs. Pins cost
storage only, and storage comes last, so the design uses pins.

### Filters: where a Bloom filter pays

A Bloom filter saves a block read for a key a run does not hold, and costs
reading the filter. A cold lookup of 1K keys (97.5% existing) at 100M:

| Run | Filter | Without it |
|---|---|---|
| the base (100M keys) | 175 MB read (14 bits per key), then ~975 block GETs | ~1,000 block GETs of 16 KiB: 16 MB |
| a chain node of 64K keys (level 3) | 112 KB, then the few blocks that hold one of the 1K keys | up to 1,000 block GETs |

The base holds almost every key a writer touches, so its filter never saves
much: **the base is written without a filter**. Chain nodes and other T
files hold few of any given key set, so **they carry one**, and the reader
decides per run: read the filter if it is smaller than the blocks it would
save (`filter bytes < expected absent keys × block bytes`), else go straight
to the blocks. The engine's warm cache never reads filters (its local copies
are binary-searched, `local.rs`).

**Prefix Bloom filters** (RocksDB's prefix extractor) need a fixed prefix
length, which arbitrary byte-string keys do not have; file key ranges and
block indexes already prune prefix seeks. Not used.

### Globs: a skip-scan over the block index

Every key in a block lies between its first and last keys (or the next
block's first key, all format v4 records). A block can be skipped if no
string in that interval matches the glob. The test walks the glob's
automaton down both interval bounds and stops as soon as it is free of both
(`globs.py`, `intersects`). It needs no stored bytes and runs on the block
index the reader fetches anyway.

Measured on 1M path-shaped keys (`tenant/site/date/file.ext`) and 1M
12-digit ids: the share of blocks each method reads, at 64 KiB / 16 KiB
blocks:

| Glob | Blocks holding a match | Interval skip | Common-prefix skip | Trigram filter per block |
|---|---|---|---|---|
| `tenant-03/**/*` | 12.6% / 12.6% | 12.6% / 12.6% | 13.4% / 12.8% | 100% / 100% |
| `tenant-*/site-0042/**/*` | 1.9% / 0.8% | 3.0% / 1.0% | 17.9% / 6.8% | 4.8% / 2.2% |
| `tenant-03/site-*/2024-03-*/*` | 12.6% / 10.2% | 12.6% / 12.6% | 13.4% / 12.8% | 12.6% / 12.4% |
| `*/*/2024-03-*/*` | 100% / 81.6% | 100% / 100% | 100% / 100% | 100% / 98% |
| `**/report-0004?.*` | 96% / 54% | 100% / 100% | 100% / 100% | 100% / 100% |
| `**/*.pdf` | 100% / 100% | 100% / 100% | 100% / 100% | 100% / 100% |
| ids `0042*` | 0.3% / 0.1% | 0.3% / 0.1% | 5.7% / 1.9% | 100% / 98% |
| ids `*4242*` | 83% / 36% | 100% / 100% | 100% / 100% | 100% / 98% |

The trigram filters would store 0.5–2.5 B per key (2–15% of the raw keys) and
never beat the interval test. Nothing but a full scan serves a true infix or
suffix pattern at these block sizes: matches are spread across every block.
Patterns in Solera are declared on assets, and their full passes are rare,
so this is acceptable. A pattern that must be fast can get its own derived
index later.

### Alternatives for K, compared

| Option | Verdict |
|---|---|
| Spans' files (versions per reader) | lookups and pages read the versions every lagging reader keeps: 1.65–9.9 entries per live key at 1M (cited) |
| A dedicated size-tiered LSM beside T | same tiers as T's levels, written twice: ~+7 writes per entry at 100M for nothing |
| Leveled LSM (few runs, every merge into the next level) | fewer runs (~4) but every 1K-key delta rewrites level-1 files: today's leveled planner measured 51× writes at 100M (cited) |
| Copy-on-write snapshot pages (A20) | random commits copy 62–226 rows per update (cited) |
| **Base + T's chain** | **chosen** |

## File format

The v4 container (blocks, block index with first keys, offsets, sizes and
CRCs, a Bloom filter, a fixed footer) is the textbook sorted-file layout; a
from-scratch design lands on it too, and K and T files use it unchanged.
Four choices inside it were re-made with measurements (`codec/results.tsv`:
400K entries per dataset, generations spread over 12,000 commits, payloads
16 random bytes, one core, decoding = decompress and parse into arrays).

**Codec: zstd level 1, replacing zlib level 1.** Bytes per entry and decode
speed (millions of entries per second), row entries:

| Data | zlib-1, 64 KiB | zstd-1, 64 KiB | zstd-1, 16 KiB | lz4, 16 KiB | none, 16 KiB |
|---|---|---|---|---|---|
| ids at 100M, base | 9.32 B · 11.2 | 7.90 B · 23.9 | 8.11 B · 23.3 | 10.31 B · 32.3 | 11.70 B · 40.4 |
| ids at 100M, node | 13.47 B · 9.6 | 11.45 B · 19.9 | 11.64 B · 20.4 | 14.29 B · 30.2 | 15.50 B · 36.2 |
| UUIDs, base | 32.27 B · 4.5 | 23.48 B · 16.1 | 23.69 B · 13.7 | 35.98 B · 21.7 | 37.00 B · 40.3 |
| paths, base | 12.71 B · 9.4 | 9.41 B · 15.2 | 10.09 B · 12.9 | 13.84 B · 21.9 | 26.28 B · 27.7 |
| ids at 100M, base, 16 B payloads | 28.24 B · 5.6 | 26.38 B · 15.2 | 26.46 B · 14.9 | 28.77 B · 32.0 | 28.71 B · 37.3 |

zstd-1 is the smallest of the fast codecs on every dataset (zstd-3 saves
another 0–9% at half the encode speed) and decodes 1.4–3.6× faster than
zlib-1. On the harness's link (64 connections of 80 MB/s, 4 cores), a full
read of 100M ids is bound by decoding for every codec: ~2.2 s with zlib-1,
~1.1 s with zstd-1, ~0.8 s with lz4. lz4 is that much faster there but
stores 27–52% more bytes (ids, paths, UUIDs), and on a slower link (a
worker's 10 Gb/s NIC, ~1.2 GB/s) the two even out; bytes also cost on
Railway, where uploads are billed (below). zstd-1 is chosen; lz4 stays a
phase-2 configuration. zstd-1 also encodes twice as fast as zlib-1 (15–18
against 8 M/s for ids).

**Blocks: 16 KiB.** A cold lookup decodes one block per key: 0.06 ms at
16 KiB with zstd-1 against 0.50 ms at 64 KiB with zlib-1 (ids at 100M). For
1K keys that is 0.06 s of one core instead of 0.5 s (v4's real reader decodes
at 4.6 M/s, so its cost today is higher still). Smaller blocks cost 2–3% in
bytes and a 4× larger block index: ~1.8 MB for a 100M-key base, split over
its 64 MB files so each tail stays ~140 KB. Globs read 2–3× fewer bytes at
16 KiB (above).

**Rows, not columns.** Columnar blocks (one section per field, generations
as offsets from the block's smallest) with zstd-1 are 20% smaller for
numeric ids, 10% for UUIDs, and 2–5% *larger* for paths; they decode no
faster (ids: 19 against 23 M/s). Not worth a second entry layout.

**Filters optional.** A filter with `nbits = 0` means "no filter: every key
may be present" (the base).

**One entry per key per file.** v4 readers already accept it; nothing in K
or T needs repeated keys.

So the format is v4 with codec 2 (zstd), `nbits = 0` allowed, and a default
block size of 16 KiB: version 5. The pack adds one container: concatenated
v5 sections, then a directory (per section: commit, offset, length) and a
footer pointing at it.

## Cleanup under zombies

Engines are fenced by the journal's compare-and-swap; a fenced engine may
still run for a while, paused and resumed, with a stale model. The rule: **an
engine deletes only what its durable state, as of a successful journal write
made after it listed the files, declares dead.** The write is the barrier
(A17's R1): a fenced engine's write fails and it deletes nothing.

| File | Dead when | Why a zombie cannot get it wrong |
|---|---|---|
| a T node (a pack) | it starts (ends) below the floor | the floor never decreases in the durable state, so a floor that passed the barrier is at most every later floor |
| a base file | replaced by a published base merge, and no pin taken before the replacement remains | as today's pin floors; a replaced file is never referenced again |
| a delta | packed, its cleanup acknowledged, and no attempt pin names it | as today |
| an unpublished output (node, pack or base) | its epoch is at most this engine's and the durable state does not name it | a later engine's outputs carry a later epoch (D86) |

T builds are deterministic, so two engines building the same node write the
same content under different names; only the one the journal publishes is
read.

## Costs, before measurement

On S3, Standard list prices: PUT $5 per million, GET $0.40 per million,
$0.023 per GB-month, in-region transfer free. On Railway, verified today on
Railway's billing page: **$0.015 per GB-month, S3 operations free, bucket
egress free; buckets are reachable only over public networking, and uploads
from Railway services to a bucket are billed as service egress, $0.05 per
GB.** So the brief's "on Railway requests and transfer are free" holds for
reads, not writes: on Railway, written bytes cost money.

What this design adds per commit, at one commit of 1K keys every 10 s
(259,200 a month), replayed: about one third of a PUT for packs and nodes
($0.43 a month on S3, against the delta's own $1.30), and ~9 entries written
per entry committed at 100M, ~10 with the delta (~21 GB a month at ~8 B per
entry: $1.04 a month of Railway upload egress). T's storage with a
day-behind reader: ~0.6 GB at 100M ($0.014 on S3, $0.009 on Railway). These
are the numbers phase 2 replaces.

## A worked example

The span design's example, so they can be read side by side, with b = 2 to
keep it small. `items` holds `a` and `b`, written by commit 0 at generation
1; commit `c` writes at generation `10c`.

| Commit | Change | Delta (before → after) |
|---|---|---|
| 1 | add c | c: absent → g10 |
| 2 | remove a | a: g1 → absent |
| 3 | re-add a, update b | a: absent → g30; b: g1 → g30 |
| 4 | add d, update c | d: absent → g40; c: g10 → g40 |
| 5 | remove d | d: g40 → absent |

T at head 5: packs `[0, 1]`, `[2, 3]`, `[4, 5]`; one level-2 node `[0, 3]`
(`a: absent → g30 · b: absent → g30 · c: absent → g10`). The base level is 2
here: once `[0, 3]` exists, the base becomes the state at 3 (`a g30 · b g30 ·
c g10`, `w = 3`) and K's chain is the pack `[4, 5]`.

- **Lookup at the head**: chain first: `c` g40, `d` removed; then the base:
  `a` g30, `b` g30. Live count 3.
- **X at position 1**, `changes(1, 5)`: cover = section 1 of pack `[0, 1]`,
  pack `[2, 3]`, pack `[4, 5]`. `a`: before 1 live (pack `[2, 3]` names its
  predecessor g1), after g30: updated. `b`: updated. `c`: absent before
  (section 1), g40 after: added. `d`: absent → absent: neither, not
  delivered. Tally 2 + 1 = 3.
- **Y at position 3**, read-ahead `d` delivered live at 4: cover = section 3
  of pack `[2, 3]`, pack `[4, 5]`. `a`: absent before 3, live after: added.
  `b`, `c`: updated. `d` is a read-ahead key, looked up in K at 5: absent,
  delivered live: removed. Tally 3 + 1 − 1 = 3.
- **Z, a new consumer**, starts a full pass at 5 and pins K at 5 (the base at
  3, `w = 3`, `S = 5`). Commits 6 and 7 land and the base merges; Z's pages
  still read the old base plus T's `[4, 5]`, and every key is added. When the
  pass ends, Z's position is 6 and its next delta reads T from 6.
- **The floor** is min(X's 1, Y's 3, K's `w + 1 = 4`, Z's pin `4`) = 1. Once
  X moves to 6, it is 3: the pack `[0, 1]` ends below it and the node
  `[0, 3]` starts below it, so both are deleted; the pack `[2, 3]` stays,
  since Y reads its section 3. Nothing about Y's landing point was ever
  reserved.

## Reuse audit

Reused because the from-scratch design lands on the same thing, replaced
where it does not:

| Existing | Verdict | Why |
|---|---|---|
| the delta (`.kx` per commit, entries with predecessor and prior payload; `delta.rs`, `entries.rs`, `sort.rs`, `rows.rs`, `arrow.rs`) | **reuse** | one PUT carrying old and new state is what the commit constraint forces |
| exact writes: `sparse.rs`, `KeyIndex._find`, `_stream`, `jobs::Join` | **reuse**, with a run list of base files + chain + unpacked deltas, and the per-run filter rule | resolving written keys against sorted runs newest first is K's lookup |
| v4 container: blocks, block index, footer, CRCs, blocked Bloom filter with XXH3 (`format.rs`, `key-index-format.md`) | **reuse with changes** (v5): codec 2 = zstd-1, 16 KiB default, `nbits = 0` | the measurements above |
| the v4 entry encoding (row, prefix-compressed keys, flags, varints) | **reuse** | columnar did not pay |
| repeated keys in a file, "newest version first" across blocks and files | **not used** | no versions |
| `stream.rs` newest-wins merge of runs, `Writer`, decoding ahead | **reuse** for the base merge and pages | |
| `spans.rs`: `Groups` collecting a key's versions, `retain`, `change` with generation bounds `g_p`, `g_n1`, `older`, `at` | **replaced** by a two-ended net merge (before from the oldest run, after from the newest; no bounds) | no versions, no clipping |
| `jobs::SpanMerge`, `Merge.spans` | **replaced** by `NetMerge` (T builds) and a pack copier | |
| `local.rs` (engine cache copies, `Snapshot`) | **reuse** | its file selection fix (R3) becomes unnecessary: one entry per key |
| cold pages: `Merge.read` (d880955, D108), a native job fed segment by segment from the block holding the cursor; `_key_blocks` | **reuse the streaming job**, without its version folds (`Changed`, `At`): with one entry per key per run it is a plain newest-wins merge for K and a two-ended net merge for T | a page fed by segments is what a from-scratch reader wants too: fewer GETs per page than a window per file |
| `KeyIndex.changes_page`, `_range`, `_lowered` | **replaced**: the cover, the net merge, read-ahead from K at N | |
| `IndexState`, `Span`, `committed`, `merged`, `holds`, `covers`, `generation` | **replaced** by `{count, base: {w, files}, t: {floor, built per level, epochs, hints, skips}, unpacked deltas}` | |
| `_Policy` (guard, read rule, fan-in cap, triggers), `plan_merge`, stale rewrites, `MERGE_ATTEMPTS` per input set | **deleted**; T builds are scheduled by alignment, the base merge by one trigger | |
| `upkeep.py`: two lanes, orphan collection by epoch, publication through the journal, pins | **reuse the mechanisms**: a build lane and a base lane; floor deletion added | |
| `positions.reads` (endpoint transfer per plan) and the model's endpoint set | **mostly deleted**: T needs the floor, K needs pins | |
| `ObjectIO` with injected latency (`io.py`), `EngineCache`, `Reads` | **reuse** | |

## A17, finding by finding

Spans fixed R2, R7, R8, R9 and R10 on main after this design began
(d880955: streamed version folds, a native page job `Merge.read`, durable
merge accounting; D108, D109). So "cannot arise" below no longer means
"spans are broken there". It means the mechanism, and the code that now
guards it, does not exist in two views.

| Finding | Here |
|---|---|
| R1 (P1) a zombie deletes a published output | **same obligation, smaller surface**: the barrier before deletion; T's deletions are by the floor, which a stale engine can only underestimate |
| R2 (P1) a key's versions cross a page window | **cannot arise**: one entry per key per file. A page that returns no key still moves its cursor |
| R3 (P1) cached lookup picks the wrong file | **cannot arise**: a key appears once per run, files are disjoint |
| R4 (P2) a selection with no position | **simpler**: nothing in the index to transfer; the plan only pins what it reads |
| R5 (P2) a covering retry's landing point lost | **cannot arise in T**: every commit above the floor is a valid start |
| R6 (P2) admission for every writer | **same**: one rule for every index writer (outputs, sources, failure indexes) on the build backlog: unpacked deltas and unbuilt levels |
| R7 (P2) rejected rewrites re-uploaded | **cannot arise**: no rewrite that may publish nothing; every build and base merge publishes |
| R8 (P2) attempt budget lost on restart | **same, if a write bound is promised**: a failing build or base merge needs a durable attempt count and an alarm. T's writes are bounded by its levels without it |
| R9 (P2) `u64::MAX` as "no bound" | **cannot arise**: no generation bounds in any read |
| R10 (P2) a hot key's versions in memory | **cannot arise**: a merge holds one entry per input per key |

## A19 (D93, D100), what each fix needs

| Finding | Needs | Here |
|---|---|---|
| R1, R2 a full pass over a moving head | a pinned snapshot | K pinned at S |
| R3 completion forgetting a removal owed | the state of delivered keys at N | K at N |
| R4 pattern-time selections | recorded as read-ahead, classed at N | K at N; the membership diff reads K pinned at the split |
| R5 early removal on a current-only store (D100: classes follow the index at the pass's version) | the index at the pass's version | K pinned at S for a full pass; T's state after N for a delta |
| R6, R7 | nothing from the index | n/a |
| R8 a forced retry after a revert | `changes(keys=)` with the net rule | T keeps the after generation of equal-payload reverts; "neither" decided at read |
| R9, R10 staleness roll-ups | a merge that stops at the first delivered key | T's paged merge |

## What this design gives up

- **Storage, and a lifetime gated by the slowest reader.** T keeps all
  levels back to the floor: ~41 entries per live key per day of reader lag at
  1M, ~0.5 at 100M. Spans keep at most one version per key per lagging
  reader (D85), which is bounded by the key count. T's floor is not: a
  stalled reader makes T grow by a day's worth of every level each day. D110
  rejected per-commit deltas kept back to the oldest reader for exactly this
  reason. The bound T needs is a compaction anchored on positions: below the
  second-oldest reader's position, the oldest reader is the only one left,
  so everything between its position and the next can be merged into one
  node starting at its position. That caps T at about one node per reader
  (≤ one entry per key each), as spans are capped. It reserves positions,
  which are durable records, but never landing points. Phase 2 measures the
  stalled pass with and without it.
- **Far readers of small indexes read ~4× what changed.** Above.
- **Two kinds of background work** (T builds, base merges) and a pack format.
  But no merge policy: builds follow alignment, the base one trigger.
- **The format levers are not two-views wins.** zstd, 16 KiB blocks and a
  filterless base apply to spans too. Phase 2 measures spans with and
  without them.

## Phase 2: prototype and measurement plan

**Prototype** (bench code, no product code, as the v4 prototype was):
`native` additions behind feature flags or in a bench crate: codec 2, `nbits
= 0`, `NetMerge`, the pack copier. Python in `bench/keys/views/`: T's state,
cover, chain, floor, builds and base merge; lookup, page, glob page,
`changes_page` and read-ahead over `ObjectIO` with injected latency.

**Harness**: spans' own (`bench/keys/spanbench.py`, D88), from W42 through
the coordinator, so spans are measured as built, on the same traces and
reader processes. Two views run the same traces through the same reader
API.

**Traces** (seeded, uniform and path-shaped keys):

1. 1M and 100M keys; 12,000 commits of 1K keys (90% updates, 5% adds, 5%
   removes);
2. readers 1, 100, 360, 8,640 and 10,000 behind;
3. 100 daily readers spread over the day;
4. a full pass stalled from commit 2,000;
5. temporary-key churn: half of each commit's keys removed 100 commits later;
6. a 1M-key commit in the 100M trace.

**Readers**: cold, each in its own process, 30 ms per request, 80 MB/s per
connection, 64 in parallel; 4 cores, under `nice` and memory limits on the
Mac (at most 4 parallel jobs).

**Configurations**: spans as built (v4); spans with codec 2 and 16 KiB blocks
(the format levers, if W42 agrees the codec switch is a one-line change);
two views on v4 settings; two views on v5. The first and third compare
structures; the second and fourth, formats.

**Measured per scenario**: first page; full catch-up; sequential round
trips; GETs; bytes read and written; peak memory per reader process; writes
per entry committed (deltas, packs and nodes, base merges, separately);
storage mean and peak, with pins; dollars a month on S3 (Standard list) and
Railway ($0.015 per GB-month, upload egress $0.05 per GB, reads free). For
K: 1K cold exact lookups, a 100K-key page and a glob page, at the head and
pinned.

**Correctness**: every result checked key by key against the per-commit fold
(classes, generations, payloads, presence at N with read-ahead), including
after builds and base merges between pages of one catch-up.

**Decision rule**, proposed: switch if two views are at least as fast on
first pages and lookups at both sizes, faster on full catch-ups or cold
lookups by ≥ 1.5× somewhere that matters, with writes and dollars within 1.5×
of spans; keep spans otherwise. To be confirmed by the coordinator before
the runs.

## Open questions

1. **Railway writes.** Uploads from Railway services are billed at $0.05 per
   GB (public networking only). Should write amplification weigh more than
   the brief assumed?
2. **The floor under a stalled reader.** Without the position-anchored
   compaction above, a reader a week behind makes T keep a week of every
   level. Is the compaction worth its extra rule, or is a byte budget on the
   floor (past it, the oldest reader does a full pass) enough?
3. **Spans with the format levers.** Ask W42 whether measuring spans with
   zstd and 16 KiB blocks is in scope for phase 2.
