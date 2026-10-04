# Key index from first principles: stamped layers, tiered by recency

Status: **phase 1 design accepted; phase 2 prototype built** (W57, T31;
the go is D151). The prototype is bench code on this branch:
`native/src/layers.rs` and `bench/keys/fp/layers.py`, plugged into W53's
harness as `viewbench.py --index layers`; W53 measures it against spans and
two views. Naming: the index's sorted runs are **layers** (Solera's "run" is
an asset's run).

Evidence labels: **measured** (micro-measurements on this branch, one core,
no I/O: `bench/keys/fp/codec/`, output in `results.txt`), **replayed** (the
structure run on metadata with the density model of the span replays:
`bench/keys/fp/model.py`, output in `bench/keys/fp/results.md`), **checked**
(`bench/keys/fp/check.py`: random histories against the per-commit fold),
**cited** (the prior designs' own numbers, with their labels there). Nothing
here ran on object storage. Every server run was capped with `systemd-run
--user --scope -p MemoryMax=8G -p CPUQuota=400%` or less.

## The answer in brief

- **One fact shapes the design.** With change kinds in the log, a key's
  presence at any past commit P follows from its presence now and how many
  times it was added or removed since: **presence at P = presence at H, flipped
  once per add or remove after P.** So the index never needs a key's state
  at P. It needs, per key, its newest state, the generation of its last
  change, and the commits where it was added or removed (its **flips**), for
  as far back as readers may start.
- **One structure: a few key-sorted layers, tiered by recency.** A **layer**
  covers a stretch of commits `[a, b]` and holds one **stamped entry** per key
  changed in it: presence at `b`, the generation of its last change (its
  **stamp**), its flips, and a source's payload. A commit's delta is the
  newest layer. Layers merge by size (four alike become one), newest wins, flips
  unite. The oldest layer is the **base**.
- **Δ(P, H, keys) reads only the layers that end after P.** A key differs iff it
  has an entry stamped after P and is present at one end at least; its state
  at H is its newest entry, its presence at P is that state flipped by its
  flips after P. Any P is exact,
  inside a merged layer too: **no endpoints, no reservations, no versions per
  reader, no clipping at H** (a batch reads the layer list it pinned at H).
- **One reader-agnostic rule bounds what a reader reads:** a layer may hold at
  most **k × the bytes of all newer layers + Z** (k = 4, Z = 1 MB). A reader
  then reads at most (1 + k) × what it must, plus Z, wherever its P falls.
  Replayed: a reader 100 commits behind reads 1.7–1.8× the bytes of what
  changed, a day behind 1.4× (1M keys) and 1.6× (100M).
- **Retention is one number, the cut:** the newer of the window's edge and
  the oldest live P. Merges drop flips at or below it, and the base drops its
  tombstones. Readers behind the window keep a before-image (the observed
  set's mechanism), built by an ordinary Δ(P, head).
- **Costs, replayed at one commit of 1K keys every 10 s:** 8.6 layers on
  average at 1M and 14.6 at 100M (14 and 21 at most); 7.1× (1M) and 11.4×
  (100M) entries written per entry committed; 69–98 KB uploaded and 1.4 PUTs
  per commit; ~$2.0 a month on S3 and $0.9–1.3 on Railway with a warm
  writer, within ~10% of spans' and two views' (cited). Storage 15 MB at 1M
  and 1.0 GB at 100M. One round trip for every query once the engine holds
  the layer indexes; two cold.
- **What it gives up:** a key's version or payload at P (a source key that
  reverts reads as a redundant update), reads at an interior commit that no
  batch pinned, and fast non-prefix pattern scans (they read the whole index,
  only at full-scan moments).

## The problem, restated

The log is given: each commit writes one **delta**, entries sorted by key,
each `(key, kind, payload)` with kind added, updated or removed, payload for
sources only. The journal's commit record names the delta and its one
generation; a key's data object is named by key and generation.

One query, **Δ(P, H, keys)**: every key whose state differs between P and H,
with its presence at P and at H and its version at H.

- **P** is −∞ (everything, as added) or a commit a reader observed at.
- **H** is the head or a recently pinned head (a batch pins it). Never older.
- **keys** is a sorted list, or "after c, the first N that differ"; either may
  carry a pattern filter (globs or regexes, include and exclude).

Who asks, and how often:

| Caller | Query | Shape | Hot? |
|---|---|---|---|
| a writer, before each commit | Δ(−∞, head, its sorted keys) | 1K–100K point lookups | every commit; the engine's resolver serves it warm |
| a run's batch (the cursor walk) | Δ(P, H, after c, first N) | a page by key | every batch of every run |
| a `keys=` run, a point's re-check | Δ(P or −∞, H, sorted keys) | point lookups | per run |
| a first run, a start-over, a membership query after a pattern change | Δ(−∞, H, after c, first N, pattern) | a full scan, paged | rare |
| retention, folding a reader behind the cut | Δ(P, head, everything) | a catch-up | once per lagging reader |

Priorities: sequential round trips first, then requests and cost, then
storage. Object storage: immutable objects, compare-and-swap, range GETs at
6–30 ms, high parallelism. 1M–100M keys per partition. One writer per
partition; fenced engines may run on as zombies.

## What the narrowed problem makes unnecessary

Each fact below is new since spans and two views were designed. Each
removes a mechanism one of them needed.

| Fact | What it removes |
|---|---|
| Change kinds give presence at P from the first change after P | before-states and predecessors in files; a key's version at P (spans kept one per endpoint, two views kept a "before" per node) |
| H is the head or a pin a batch holds | reads clipped at an interior N (spans' `g(N + 1)`); snapshots of old commits; keeping anything for "N" beyond the batch's own pin |
| P is at or after the cut, and the cut is known | history older than the cut; any need to name P inside the structure |
| A run walks "after c, first N" | none (it adds a need): results come in key order, a page at a time, in bounded memory. A time-ordered log alone cannot serve it |
| Storage is cheap; requests and round trips are not | the pressure to share bytes across readers: a reader may read some extra bytes, if it saves requests and rules |

The first fact is the pivot. Spans kept, inside merged files, the version
each reader's endpoint sees; two views kept, per tree node, each key's state
before and after. Both exist to answer "what was k at P". Δ only asks
whether k was *present* at P, and the change kinds make that a parity:

```
k's history:   commit 3: added   commit 7: updated   commit 9: removed   commit 12: added
flips:         3, 9, 12                                       (adds and removes only)
now (H = 15):  present
at P = 10:     flips after 10: {12}        one flip   → absent at 10
at P = 8:      flips after 8:  {9, 12}     two flips  → present at 8
at P = 1:      flips after 1:  {3, 9, 12}  three      → absent at 1
```

## Five candidates

Each is built from the facts above. Numbers are for one commit of 1K random
keys every 10 s (90% updates, 5% adds, 5% removes), a reader 100 commits
behind (~100K changed keys) and one 10,000 behind (~1.25M at 1M keys, ~9.5M
at 100M).

**1. One stamped head map.** A single key-sorted map of the current state;
each key carries its stamp and its flips. Δ(P, H) is a filtered scan: every
entry stamped after P. No time structure at all, nothing about readers.

- At 1M keys the map is ~9 MB (measured: 9.3 B per entry): any Δ is 1–2 GETs
  and ~0.1 s. It is the right answer at that size.
- At 100M the map is ~810 MB, and every catch-up scans it: a reader 100
  commits behind reads 810 MB to find 0.8 MB of changes (1,000×). Keeping a
  tail of recent deltas beside the map helps only readers newer than the
  map's last rewrite, and the tail needs tiering to stay cheap to look up:
  candidate 2.

**2. Stamped layers, tiered by recency** (chosen). The same entries, split
into a few key-sorted layers by when they changed. A reader at P reads the
layers that end after P. The rule "a layer holds at most k × all newer layers + Z"
bounds the bytes it reads before P. At 1M the rule lets nearly everything
merge into the base, and candidate 1 is what remains.

**3. Fan-out on write.** P comes from a finite, known set, so give each
distinct live P its own pending change set, appended at each commit. Δ reads
exactly its set: 1.0× what changed, one round trip.

- Writes grow with distinct P's: 100 daily readers spread over a day means
  ~100 sets, so each commit writes ~100 × 5.8 KB and 100 PUTs, before the sets'
  own compaction. On S3, ~26M PUTs a month ($130); on Railway, ~150 GB of
  uploads before compaction ($7.5) and several times that after.
- Nested sets (set(P₁) ⊇ set(P₂) for P₁ < P₂) can be stored as differences
  between consecutive P's: that is blocking spans, 543–546 files with 100
  daily readers (cited, spans' replay).

**4. A time-ordered log, sorted at read.** H is always the head, and
storage is cheap: keep the deltas in commit order (packed into chunks), and
let the reader read (P, H], dedup and sort.

- Near readers do well: 100 behind reads ~0.6 MB and sorts 100K keys.
- The cursor walk fails for far readers. Each batch asks "after c, first N"
  at its own head, so either each batch re-reads the whole suffix (10,000
  behind at 100M: ~58 MB × ~950 batches of 10K keys ≈ 55 GB per run) or the
  reader holds ~9.5M sorted keys (~300 MB) in memory for the run's lifetime.
  Head lookups and first runs also need a separate key-ordered map: two
  structures. Its useful core survives in candidate 2: the newest layers are
  tiny and read whole.

**5. Key-sharded pages with change stamps.** Partition the key space into
pages; each carries its newest stamp, so a reader skips pages unchanged
since P (copy-on-write pages, as in the snapshot-page study).

- Random keys touch every page within a few commits: at 100M with
  256-key pages, a reader 100 commits behind finds ~23% of pages changed and
  reads ~370 MB for 0.8 MB of changes.
- Writes copy whole pages: 62–256 rows per update (cited, A20), so a 1K-key
  commit uploads 0.5–2 MB against candidate 2's ~0.1 MB. Pages pay off only
  on clustered keys; candidate 2 gets that case from block stamps (below).

| Candidate | 100 behind, 100M | 10,000 behind, 100M | Head lookups | Writes per commit | Fails on |
|---|---|---|---|---|---|
| 1. stamped head map | ~810 MB | ~810 MB | 1 layer | rewrite the map: ~810 MB per R commits | near readers at 100M |
| **2. stamped layers, tiered** | **1.4 MB, 1 RT** | **~130 MB, 1 RT per page** | **9–15 layers** | **~0.1 MB, 1.4 PUTs** | (chosen) |
| 3. fan-out on write | 0.8 MB | 77 MB | a separate map | ~100 PUTs, ~0.6 MB+ | writes, PUTs |
| 4. log, sorted at read | 0.6 MB | 58 MB per batch, or 300 MB of memory | a separate map | 1 PUT | the cursor walk |
| 5. sharded pages | ~370 MB | ~all of it | 1 tree | 0.5–2 MB | random keys |

## The design

### What a layer holds

A **layer** covers commits `[a, b]` and is one key-sorted file (split into
files of ≤ 64 MB at key boundaries when large), cut into self-delimiting
16 KiB compressed blocks. For every key changed in `[a, b]` it holds one
**stamped entry**:

| Field | Meaning | Bytes (measured, ids, zstd-1) |
|---|---|---|
| key | prefix-compressed against the previous key | most of it |
| present | whether the key exists after commit `b` | one flag byte: present, has flips, has payload |
| stamp | the generation of its last change in `[a, b]`: its version at `b` | ~2 B (varint from the block's smallest) |
| flips | the generations in `[a, b]`, newer than the cut, where it was added or removed | 0 B for most keys; ~2 B each |
| payload | a source's own version at `b`, if present | as given |
| start | whether the key existed just before `a` (phase 2) | a bit of the flag byte |

Measured (`bench/keys/fp/codec`, 12-digit ids, each file at its own key
density): a stamped entry is **8.1 B** at 100M (9.3 B at 1M, 9.3 B with half the
keys churning), against the minimal delta's 5.8 B (1K-key delta); with
16-byte source payloads, 25.6 against 24.9 B. Decoding runs at 25–37M
entries per second per core.

**A delta is a layer.** The layer `[c, c]` is commit c's delta, read as stamped
entries with the commit's generation (from the commit record) as every
stamp: added → present, flip c; updated → present; removed → absent, flip c.
Nothing is rewritten at commit.

**Main and side parts.** A layer's entries live in two parts. The **main**
part holds what head reads and readers after the layer need. The **side**
part holds what only a reader whose P falls inside the layer needs:

- in **the base** (the oldest layer, `[0, w]`), every absent key: nothing is
  older, so its tombstones shadow nothing (the base's side part is its
  **graveyard**);
- in every other layer, keys **absent at both ends**: added and removed
  inside it (under churn, most temporary keys). Phase 2 added this: an
  entry's start bit says whether the key existed before the layer.

Why a reader whose P is before a layer can skip that layer's side part: all
of the layer's flips are then after P and after the cut (none was dropped), so
a key absent at both ends flipped an even number of times there and changes
nothing at P or at H; and it shadows nothing, since it did not exist before
the layer. Head reads (P = −∞) skip every side part.

**The layer index** is a small object written beside each layer larger than one
block: per block, its first key, offset, length, and its newest stamp; and,
for layers other than the base, a Bloom filter of its keys (10 bits per key).
Measured: 0.012–0.017 B per entry without the filter (~1.5 MB for a 100M-key
base). Deltas need none up to one block (~2K keys); a larger delta's block
boundaries, handed over by the writer at commit, become its layer index,
written by the engine with the commit record.

### The layout

Layers tile commit time from 0 to the head, without gaps or overlaps, newest
smallest. Replayed, 100M keys, a moment in the base trace (illustrative; a
layer of the sweep):

```
commit time →                                                                      head
├──────────────── base [0, 64,512] 817 MB ────────────────┼── 203 MB ──┼56┼56┼56┼14┼14┼4┼4┼4┼1┼1┼1┼·┼·┼·┼·┤
                                                     (layers above the base, newest smallest; · = a few KB)
```

At 1M keys the same rule keeps the base close to the head: base ~9 MB of
live keys, then layers of 3.3, 0.6, 0.1 MB… down to single deltas.

The **manifest** is the layer list at a head: for each layer, `[a, b]`, its
files, sizes, entry count, tier and layer index. It lives in the index's state
in the journal. A batch **pins** the manifest at its H for its duration (the
claim's existing reader pin); merges publish new manifests meanwhile, and
the batch keeps reading its own.

### Reading Δ(P, H, keys)

Over the manifest pinned at H, take the layers that end after P (for
P = −∞: all of them, main parts only; otherwise main parts, plus the side
part of the one layer P falls inside). Merge them by key. For each key:

```
entries  = its entries in those layers, newest layer first, keeping only stamps > g(P)
if none:   unchanged since P: skip            (an entry stamped ≤ g(P) is older news)
at_H     = entries[0].present;  version = entries[0].stamp;  payload = entries[0].payload
at_P     = at_H XOR (count of flips > g(P) across entries is odd)      # P = −∞: absent
if not at_P and not at_H: skip                 # added and removed inside (P, H]: "neither"
emit (key, at_P, at_H, version if at_H, payload if at_H)
```

`g(P)` is P's generation; stamps and flips are generations, which rise with
commits. Only the oldest layer read (the one straddling P) can hold entries
stamped at or before g(P); every newer layer lies wholly after P.

Why it is exact (checked: 3,000 random histories, 532,206 reads at every
P from the cut to the head, key by key against the fold, `check.py`): a merge
keeps, per key, its newest state and every flip newer than the cut; the layers
that end after P cover (P, H] with nothing missing; so the newest entry
stamped after P is the key's state at H, and the flips after P are exactly its
adds and removes in (P, H].

### A worked example

The history the span and two-view designs used, so the three can be read
side by side. `items` holds `a` and `b`, written by commit 0 at generation 1;
commit c writes at generation 10c. Flips are shown as commits.

| Commit | Change | Delta |
|---|---|---|
| 1 | add c | c added |
| 2 | remove a | a removed |
| 3 | re-add a, update b | a added · b updated |
| 4 | add d, update c | c updated · d added |
| 5 | remove d | d removed |

After upkeep merges commits 1–5 into one layer (the window holds them all):

```
base [0, 0]   a ● g1        b ● g1                            (● present, ○ absent)
layer  [1, 5]   a ● g30 {2,3}  b ● g30 {}  c ● g40 {1}  d ○ g50 {4,5}
```

- **X observed at 0** (holds a, b). Δ(0, 5) reads the layer `[1, 5]` only (the
  base ends at 0). `a`: stamped after 0; flips after 0 are {2, 3}, even, so
  present at 0 as now: **updated**. `b`: **updated**. `c`: flips {1}, odd:
  absent at 0, present now: **added**. `d`: flips {4, 5}, even: absent at 0
  and now: skipped. X's tally 2 + 1 = 3.
- **Y observed at 2** (holds b, c; `a` was removed at 2). Δ(2, 5), the same
  layer, now straddling P: `a`: flips after 2 are {3}: absent at 2, present
  now: **added**. `b`: **updated**. `c`: flips after 2: none; present at 2:
  **updated**. `d`: {4, 5}: skipped. Y's tally 2 + 1 = 3.
- **A writer** about to commit 6 (update c, add d) asks Δ(−∞, 5, [c, d]):
  the newest entries are `c ● g40` and `d ○`: c is present at g40 (updated,
  and g40's object is replaced), d is absent (added).
- **A first run** pages Δ(−∞, 5, after "", first 2): `a ● g30`, `b ● g30`
  (the layer's entries beat the base's); its next batch pins head 6 and asks
  for the first 2 after `b`: `c`, `d` (added at 6).
- **The cut reaches 3.** The next merge of the base with `[1, 5]` drops flips
  at or below 3: `a ● g30`, `b ● g30`, `c ● g40`, and `d ○ g50 {4, 5}` kept in
  the base's side part: a reader observed at 4 must still learn "removed". Once the
  cut passes 5, `d` goes.

Had the merge kept only presence at its ends (two views' before/after), Y,
whose P lies inside the merged layer, could not be answered from it.

### Query forms

**1. A writer's sorted keys at the head, Δ(−∞, head, keys).** Every layer is
read in the same round trip, in parallel; for each key, the newest layer
holding it decides. Per layer, the reader picks the faster of two plans and,
within 10% of the same time, the one with fewer requests:

- **seek**: one block per distinct block the keys fall in (the layer index
  says which); adjacent blocks become one range GET;
- **stream**: the whole layer in 16 MB range GETs, decoded.

A plan's time is its request waves (64 in flight, ~30 ms each), its bytes
over the NIC (1.25 GB/s) and its entries decoded (~120M/s on 4 cores). The
choice is in bytes and requests, never in blocks: the failure W53 found (a
block-count rule streamed all 100M keys for each 1K-key commit, 23 s) cannot
recur, since streaming 810 MB costs ~0.6 s of NIC and ~0.8 s of decoding
against ~0.5 s for 1K seeks. Replayed choices at 100M: the base is sought
(~1K GETs, one per key), layers of tens of MB are streamed, tiny layers read
whole.

**Filters.** A layer's Bloom filter lets a lookup skip the layer for keys it
does not hold. The base gets none: it holds nearly every key a writer
touches (updates are 90%), so its filter would cost ~125 MB to save ~5% of
its block reads. The other layers carry one in their layer index; it pays only
when it is already in hand (the engine holds layer indexes), since fetching it
cold costs a round trip and bytes proportional to the layer. Replayed at 100M:
1,012 GETs, 205 MB and ~0.85 s per 1K cold lookups without filters; 1,287
GETs, 23 MB and ~0.66 s with them held (they turn streamed layers into a few
seeks). Either way the base's ~1K seeks, one per key, dominate. Phase 2
measures both; the default is filters in the layer index, used when held.

**The warm path.** The engine's cache holds layer files on local disk and layer
indexes in memory; a writer's lookup is then local reads (~10 ms for 1K keys,
cited, spans' engine cache). Cold lookups happen after an engine restart or
for partitions the cache evicted.

**2. The cursor walk, Δ(P, H, after c, first N).** A merge over the layers that
end after P, from key c, stopping at the N-th key that differs:

```
layers ending after P (pinned at H):   ┌──── key space ───────────────────────────────────┐
  straddler [.., P .. ]   ░░░░░░░░░░░▓▓▓░░░░░░░░░░░░░░░░░░░░   (read from c to c'; entries ≤ g(P) skipped)
  layer                      ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓                 (one range GET each, c to c')
  layer                      ▓▓▓▓▓▓▓▓▓▓
  deltas                   ▓ ▓ ▓                              (tiny: read whole)
                           c ─────────── c'  → returns the differing keys in (c, c'] and c'
```

- The engine picks c' from the layer indexes so that the layers newer than P
  hold about N entries between c and c' (entry counts per block are known),
  and fetches every layer's blocks over (c, c'] in **one round trip**, one range
  GET per layer. If fewer than N keys differ up to c', it may continue (another
  round trip) or return what it has: the answer is always "every differing
  key in (c, c'], and c'", so a short page is still a correct range for the
  observed set's `(c, c'] @ H`.
- The straddler's **block stamps** (newest stamp per block) let the reader
  skip its blocks whose keys all changed at or before P. With random keys
  that rarely helps; with keys that correlate with time (sequential ids,
  dates), a straddler costs nearly nothing.
- A page's bytes are bounded by a byte budget, not only by N, so a pattern
  that matches few keys cannot make one batch read the whole index into
  memory: it returns early with its c'.
- Memory: one block window per layer plus the page.

**3. A sorted key list with a P, Δ(P, H, keys)** (`keys=` runs, points):
point lookups as in form 1, restricted to the layers that end after P. A key
with no entry stamped after P is unchanged and omitted.

**4. Full scans, Δ(−∞, H, after c, first N, pattern)** (first runs,
start-overs, the membership query after a pattern change): the cursor walk
over every layer, the base's live file included, keeping present keys. The
pattern decides which blocks are fetched (below).

**5. Folding a reader behind the cut.** Δ(P, head) for the whole key range,
paged: the reader's before-image is its answer (each key's presence at P),
and its base then relabels to that head (not to the cut: this keeps the
query at the head, the only H the index serves).

### Patterns

Patterns only cut work at full scans: in steady state the candidates are the
changed keys, which a reader filters as it merges, at no extra cost. At a
full scan:

- **A literal prefix** (every glob before its first wildcard; a regex's
  proved common prefix) is a key range: each layer is sought to it through its
  layer index.
- **Any glob or regex** is compiled to an automaton, and the reader **seeks
  with it** over sorted keys, as Lucene's `AutomatonQuery` intersects an
  automaton with its terms: at a key that cannot lead to a match, compute the
  smallest key above it that can, and jump there through the layer index. At
  block level this skips every block whose interval `[first key, next
  block's first key)` contains no match. It costs no stored bytes. Cited
  (W53's measurement on 1M path keys, 16 KiB blocks): `tenant-*/site-0042/**/*`
  reads 1.0% of blocks (0.8% hold a match); `tenant-03/**/*` 12.6%; a suffix
  or infix (`**/*.pdf`, `*4242*`) reads 100%. A25's two faults in that
  prototype's interval test (a short match between longer bounds; `**` at
  the end) apply here: the automaton walk is checked against Solera's matcher
  on random short keys, as W53's corrected version was.
- **Excludes** never cut reads (a block may hold an excluded key and others);
  they filter keys.
- **Suffix and infix patterns read the whole index.** A reversed-key layer
  (every key reversed, so a suffix becomes a prefix) would make suffix scans
  seek, at the cost of a second full copy of the keys, written by every merge
  (~1× the index more in storage and writes) for a scan that happens at
  first runs and pattern changes. An n-gram index (trigram filters per
  block) never beat the interval skip in W53's measurement (cited: 0.5–2.5 B
  per key, and 98–100% of blocks for `*4242*`). Neither is built. A full scan
  at 100M reads ~1 GB, about 1–2 s of transfer and decoding on 4 cores, once
  per first run, which then loads and processes the matching rows anyway.

### Sources' payload equality

A layer keeps each key's payload at its stamp: the version at H. It keeps no
payload at P. So a source key that went v1 → v2 → v1 inside (P, H] is
present at both ends with a stamp after P, and Δ returns it **updated**,
although its payload is unchanged: the redundant-update fallback the brief
allows. The writer is unaffected: its lookup returns the head payload, which
is all it compares. Detecting the revert would need the payload at P, for
any P in the window: a payload per update, not per flip, kept for the whole
window (a week of 1K-key commits at 16 B payloads is ~60M payloads, ~1.5 GB
per partition, and every merge carrying them). That is what a per-reader
version bought in spans. Not kept: the fallback costs one redundant update
per reverted key, which a consumer handles as any other update.

### Stale entries

When a key changes, its new entry lands in the newest layer; its older
entries in older layers stay until a merge joins them. They are **stale**, and
the read rule makes them harmless: newest wins.

- **At the head** (lookups, scans), a key is decided by the newest layer that
  holds it; a lookup stops there, a scan merges layers and drops the older
  entries. Each stale entry costs its bytes once per scan. Replayed: the layers
  hold 1.5 (1M) and 1.2 (100M) entries per live key on average in the base
  trace; under churn 3.3 (1M) and 1.3 (100M).
- **In Δ(P, H)**, a stale entry in a layer newer than P carries flips the
  reader needs (the key was added in one layer, removed in the next): it is
  read for them, not wasted. A stale entry in the straddler stamped at or
  before P is skipped.
- **A merge drops them**: it keeps one entry per key, the newest, with the
  inputs' flips united. A key's stale entries therefore live until the merge
  that joins its layers: a few commits for the newest tiers, until the next
  base merge for entries in the base (at 100M, every 2–4 days).

### Flip lists: the bound, and what the cut keeps

A key gains a flip each commit that adds or removes it, and loses it when a
merge sees it at or below the cut. So a layer's entry holds at most one flip
per commit that toggled the key since the cut: **≤ the commits in the window,
and in practice one or two.** The pathological key toggled by every commit
for a week holds 60,480 flips (~120 KB): a long entry, read and merged in a
stream like a large payload, never a memory problem of many versions (A17's
R10). The cut keeps every flip newer than it, and nothing older:

- a key **updated** keeps no flips at all;
- a key **added** in the window keeps one flip until the cut passes it;
- a key **removed** in the window keeps its tombstone and one flip until the
  cut passes the removal, then disappears at the next base merge;
- a **temporary key** (added and removed inside the window) keeps two flips
  and an absent entry until the cut passes the removal; a reader whose P falls
  between the add and the remove needs it ("removed").

## Compaction

Upkeep merges adjacent layers. A merge keeps, per key, the newest state and
the union of flips newer than the cut; with the base among its inputs, it
drops absent keys with no flips left.

**The rules, in order:**

1. **Tiers.** A layer's tier is ⌊log₄(bytes / 8 KB)⌋. When f = 4 adjacent layers
   share a tier, they merge. Upkeep takes the newest such group first.
2. **The base.** The base absorbs the layers just above it once they hold a
   quarter of its bytes, reaching as far as rule 3 allows.
3. **The reader bound** (applies to both): a merge's output that reaches past
   the cut must hold at most **k × the bytes of every newer layer + Z**
   (k = 4, Z = 1 MB). Output wholly at or below the cut is free.

Why the bound bounds reads: a reader whose P lies inside layer i reads all
newer layers (it must: they hold only changes after P, at most duplicated by
stale entries) plus layer i, and layer i is at most k × those + Z. So it reads at
most (1 + k) × what the newer layers hold, plus Z, whatever P is and without
knowing P. The bound holds when the layer is written; newer layers can later
shrink by deduplication (at 1M keys especially), so it is a design rule,
not an invariant, and the replay measures what readers actually read: 1.4–2.0×
what changed on average in the base trace, 5.5× at the worst sampled moment
for a reader 100 commits behind (a fresh layer of ~1 MB next to it). Without
the bound, a reader 100 behind at 100M reads 7.6× on average and up to
1,000× (replayed, below).

**Write amplification**, replayed: 7.1× (1M) and 11.4× (100M) entries
written per entry committed, background only; 11× and 16× in bytes (layers'
entries are larger than deltas'). Each entry is rewritten once per tier it
climbs (~6 tiers from a 6 KB delta to a 50–200 MB layer) and, at 100M, ~4×
more by base merges (each rewrites the 810 MB base to absorb a quarter of
it). That is ~70–100 KB uploaded per commit.

Upkeep runs two lanes, each one merge at a time: the base, and the tiers.
Their inputs never overlap. **Backpressure**: when queued merge input
exceeds twice the index's bytes (upkeep is failing or far behind), commits
to the partition wait, for every writer of the index, sources and failure
indexes included (A17's R6).

### Parameters, and what moves them

Replayed on the base trace at 100M keys, the cut at the oldest live P
(below), one parameter moved at a time from k = 4, Z = 4 MB, f = 4, r = 4
(the sweep's starting point; the chosen Z = 1 MB is the second row). Reads
are MB and × the bytes of what changed (worst sampled moment).

| Setting | Layers, mean (max) | Entries written per entry committed | Stored | 100 behind | 10,000 behind | Cold 1K lookup |
|---|---|---|---|---|---|---|
| k = 4, Z = 4 MB, f = 4, r = 4 | 13.9 (21) | 11.4 | 999 MB | 1.86 MB, 2.30× (5.5×) | 129 MB, 1.67× (3.7×) | 1,011 GETs, 0.85 s |
| **Z = 1 MB (chosen)** | **14.6 (21)** | **11.4** | **999 MB** | **1.37 MB, 1.69× (5.5×)** | **129 MB, 1.67× (3.7×)** | **1,012 GETs, 0.85 s** |
| Z = 16 MB | 13.1 (21) | 11.4 | 998 MB | 2.46 MB, 3.04× (18.8×) | 131 MB, 1.70× (3.7×) | 1,010 GETs, 0.85 s |
| k = 2 | 15.5 (24) | 10.5 | 1,002 MB | 1.86 MB, 2.30× (5.5×) | 108 MB, 1.40× (1.8×) | 1,012 GETs, 0.86 s |
| k = 8 | 13.0 (21) | 11.4 | 996 MB | 1.86 MB, 2.30× (5.5×) | 144 MB, 1.87× (3.7×) | 1,010 GETs, 0.85 s |
| no reader bound | 11.4 (20) | 10.4 | 912 MB | 6.12 MB, 7.56× (1,004×) | 404 MB, 5.23× (11.5×) | 1,004 GETs, 0.69 s |
| f = 8 | 20.5 (33) | 7.8 | 1,009 MB | 1.28 MB, 1.58× (2.2×) | 110 MB, 1.42× (1.8×) | 1,017 GETs, 0.88 s |
| r = 2 | 14.5 (22) | 9.4 | 1,115 MB | 1.86 MB, 2.30× (5.5×) | 129 MB, 1.67× (3.7×) | 1,018 GETs, 1.08 s |
| one merge per lane at 2M entries per commit | 15.1 (22) | 11.4 | 999 MB | 1.83 MB, 2.27× (5.5×) | 129 MB, 1.67× (3.7×) | 1,012 GETs, 0.86 s |

- **The bound is what keeps near readers near**: without it, a reader 100
  behind can land in a 400 MB layer.
- **Z = 1 MB** cuts a near reader's bytes by a quarter for 0.7 more layers on
  average; 16 MB lets the newest layer grow to 16 MB with nothing newer.
- **k** trades layers against far readers' bytes, mildly either way.
- **f = 8 writes 30% less** and reads a little less, at ~6 more layers on
  average (33 at most): more requests per page and lookup. Writes are cheap
  in dollars here (below), so f = 4 keeps the layer count down; phase 2 can
  measure both.
- **r = 2** (the base absorbs at half its size) writes 18% less but keeps
  larger layers above the base, which cold lookups stream (1.08 against
  0.85 s).
- **Budgeted upkeep** changes nothing but half a layer.

## Retention

**The cut** is the oldest P the index answers. It is the newer of:

- the **window**'s edge, head − W, W tied to run-history retention: a
  reader whose P would fall behind it is folded first (the observed set
  keeps a before-image, built by Δ(P, head) as above, and relabels its base
  to that head); and
- the **oldest live P**: the oldest commit any observation record or
  in-flight claim names (bases, ranges' heads, claims' heads). The engine
  knows this set; the index takes one number from it.

The index's state records the cut, and it only rises. A merge reads it
before planning and drops flips at or below it; the base merge drops
tombstones with no flips left. Δ refuses any P below the recorded cut,
loudly, and the reader does a full compare: a missed reader costs a full
run, never a silent wrong answer.

**The window alone** is reader-agnostic: the cut moves on a schedule.
**The oldest live P** is one number from records the engine already scans
to fold readers before moving the cut; it is monotone (every new P is a
recent head) and safe when stale (a stale engine's floor is lower, so it
keeps more). It matters at small sizes and long windows, where a window's
worth of tombstones outweighs the live keys (below).

What retention never needs: per-reader boundaries inside layers, reservations
before merges, or a reader set inside merge decisions.

### The live-P set: what using it buys, with numbers

Three ways to use what the engine knows about readers, replayed with 100
daily readers spread over the day plus readers 1, 100, 360, 8,640 and
10,000 commits behind:

| What the index uses | 1M, 7-day window | 1M, 30-day window | 1M, churn | 100M, 7-day window | 100M, churn |
|---|---|---|---|---|---|
| nothing: the cut is the window's edge | 31 MB · 7.1× · 31 MB, 2.8× | 90 MB · 6.1× · 90 MB, 8.0× | 382 MB · 8.1× · 250 MB, 23× | 1,148 MB · 9.9× · 122 MB, 1.6× | 1,413 MB · 9.7× · 194 MB, 4.8× |
| **the oldest live P, as the cut** | **15 MB · 7.1× · 15 MB, 1.4×** | **15 MB · 7.1× · 15 MB, 1.4×** | **72 MB · 7.7× · 72 MB, 6.6×** | **999 MB · 11.4× · 129 MB, 1.7×** | **1,054 MB · 10.3× · 183 MB, 4.5×** |
| the whole set, in merges too | 15 MB · 7.1× · 15 MB, 1.4× | 15 MB · 7.1× · 15 MB, 1.4× | 72 MB · 7.7× · 72 MB, 6.6× | 999 MB · 11.4× · 129 MB, 1.7× | (not run) |

Each cell: stored · entries written per entry committed · what a reader
10,000 commits behind reads, and × the bytes of what changed.

- **The oldest live P** pays at 1M keys: it halves storage (31 → 15 MB on a
  7-day window, 90 → 15 MB on a 30-day one, 382 → 72 MB under churn), and a
  reader 10,000 commits behind reads 1.4× what changed instead of 2.8–8.0×
  (23× → 6.6× under churn): the tombstones of a week or a month of removals,
  1.5M–6.5M of them against 1M live keys, are what it no longer keeps. At
  100M it changes little: storage −13 to −25%, writes +6 to +15% (the base
  absorbs more often), reads about the same.
- **The whole set** (the reader bound waived for a merge whose output holds
  no live P, as spans' read rule was per endpoint) changes nothing
  measurable: with 100 daily readers plus near ones, a P falls every few
  dozen commits, so nearly every output holds one. It would add, for no gain,
  what spans needed: a P set inside merge planning, and the reservations
  A17's R4 and R5 were about.
- **Where the set would pay, and why it is not taken.** Under churn, a
  temporary key (added, then removed 100 commits later) keeps its tombstone
  and two flips until the cut passes its removal, because a reader whose P
  falls between the add and the remove must learn "removed". Knowing every
  live P, a merge could drop the key when no P falls in that interval. With
  100 daily readers a P falls in most 100-commit intervals, so the saving is
  small; with one hourly and one daily reader it would be most of the 6.6×
  above (estimated, not replayed; 72 MB at 1M, ~0.1 s of transfer). Taking it means reserving every P
  before every merge: the cost the design exists to avoid.

So the design uses one number, the oldest live P, and never the set.

## Files, publication and deletion

### Names

Every object the index writes is named under the index's prefix with its
**life** (the output home and reset count), the writing engine's **epoch**,
and a unique id: `{life}/l{a}-{b}-e{epoch}-{id}.lay` and `.lix` for its
layer index. No name is ever reused; a reset starts a new life, so an old
life's collector can never target a new life's file (A25's R4).

### Publication

1. **Reserve the attempt.** Before uploading, the merge's attempt is
   recorded durably in the journal, keyed by life and input layer names
   (D109): attempts survive takeovers, and after R = 3 failures of one input
   set the index stops merging it and raises an alarm (A17's R8, A25's R6).
2. **Upload** the output files and layer index.
3. **Publish** `IndexMerged(life, inputs, output, cut used)` through the
   journal. It applies only if the life matches and the inputs are still
   exactly layers of the current manifest; otherwise it is refused and the
   output is an orphan.
4. **Inputs become garbage** when the publication is durable.

### Deletion under zombies

An engine deletes only what its durable state, as of a successful journal
write made after it listed the files, declares dead. The write is the
barrier (A17's R1, F40): a fenced engine's write fails, and it deletes
nothing.

| Object | Dead when | Why a zombie cannot get it wrong |
|---|---|---|
| a replaced layer (or delta) | its replacement's publication is durable, and no pin older than that publication remains | the barrier; pins are durable; a replaced name is never referenced again |
| an orphan output | no manifest, pin or running merge names it, judged after the listing, and its epoch is at most the collector's | a later engine's outputs carry a later epoch (F40's rule, `Spans.tla`'s `FixEpoch`) |
| a layer index | with its layer | same |

Raising the cut deletes nothing by itself: it only lets later merges drop
flips and tombstones, which become garbage through rule 1 of the table.

### Pins for H

A batch pins the manifest at its H for its duration: the claim's reader pin,
acquired atomically with the manifest. While it holds, the layers it names
stay, whatever upkeep publishes. Pins are short (one batch); a slow batch
pinned across a base merge keeps the old base one batch longer (~0.8 GB at
100M; cited, spans pay the same).

### Store data

The minimal delta records no replaced generation, and layers drop stale
entries when they merge. So store cleanup (the objects of replaced
versions, on immutable stores) is fed by the writer: its lookup, Δ(−∞, head,
keys), returns each key's version at the head, which is the version its
commit replaces. That list goes with the commit to the cleanup task (an
interface for the coordinator to settle; the index's part is that the
version is in the writer's hand at commit).

## Costs

All replayed (`model.py`, base trace, the cut at the oldest live P,
instant upkeep), at one commit of 1K keys every 10 s (259,200 a month).
Round trips assume the engine holds the manifest and layer indexes; a fully
cold reader adds one round trip to fetch the layer indexes (~0.015 B per entry:
~30 KB at 1M, ~1.5 MB for the 100M base). Requests: 30 ms each, 64 in flight;
NIC 1.25 GB/s; decoding 120M entries/s on 4 cores. Catch-ups are paged in
batches of 10K keys (the observed set's batch); the engine reads each layer in
windows of ≥ 1 MB and serves consecutive batches from them, so a catch-up's
GETs are about one per layer plus one per MB.

| Query | 1M keys: round trips · GETs · MB read | 100M keys: round trips · GETs · MB read |
|---|---|---|
| writer, 1K sorted keys, engine warm | local · 0 · 0 (~10 ms, cited) | local · 0 · 0 |
| writer, 1K sorted keys, cold | 1 · 9 · 13 (~0.05 s) | 1 · 1,012 · 205 (~0.85 s); filters held: 1,287 · 23 (~0.66 s) |
| near reader, 100 behind (~100K keys, 10 batches) | 1 per batch · 8 · 1.6 (1.8×) | 1 per batch · 7 · 1.4 (1.7×) |
| hourly reader, 360 behind | 1 per batch · 13 · 5.4 (2.0×) | 1 per batch · 12 · 4.6 (1.6×) |
| far reader, 10,000 behind (1.25M / 9.5M keys) | 1 per batch · 24 · 15 (1.4×) | 1 per batch · 142 · 129 (1.7×) |
| first run or start-over (all keys) | 1 per batch · ~22 · 13 | 1 per batch · ~1,015 · 999 (~1.7 s of transfer and decoding in all) |
| first run, prefix pattern matching 1/8 of keys | ~1/8 of the above | ~140 · ~125 |
| first run, suffix or infix pattern | as a full scan | as a full scan |
| fold of a reader behind the window | as a far reader | as a far reader |
| 100 daily readers | each its own far-ish catch-up; the index's structure is unchanged by their number | same |

Dollars a month. S3: PUT $5 per million, GET $0.40 per million,
$0.023 per GB-month, in-region transfer free. Railway: requests and
downloads free, uploads $0.05 per GB, $0.015 per GB-month. Readers: one
hourly, one daily, and 100 daily readers.

| | 1M, S3 | 1M, Railway | 100M, S3 | 100M, Railway |
|---|---|---|---|---|
| deltas: 1 PUT and 5.8 KB per commit | $1.30 | $0.08 | $1.30 | $0.08 |
| merges: 0.38–0.42 PUTs and 63–92 KB per commit | $0.54 | $0.82 | $0.49 | $1.19 |
| merges' input reads (~1.5 GETs per commit) | $0.16 | free | $0.16 | free |
| catch-ups (~85K / ~450K GETs) | $0.03 | free | $0.18 | free |
| storage (15 MB / 1.0 GB) | $0.0003 | $0.0002 | $0.023 | $0.015 |
| **total, warm writer** | **$2.03** | **$0.90** | **$2.15** | **$1.29** |
| a cold writer adds (9 / 1,012 GETs per commit) | $0.93 | free | $105 | free |

Against the baselines on the same prices and rate (cited, `viewbench`):
spans $1.99 and $1.88 on S3 with a warm writer (1M, 100M), two views $1.85
and $1.85; on Railway spans $1.15–1.36 and $1.29, two views $1.28 and $0.94;
a cold writer at 100M costs spans $91 and two views $114 a month.

Read on this:

- **Every query is one round trip** with metadata in hand. A near reader's
  page: one range GET per layer (~6 layers); a far reader's: ~12.
- **Writes are in the prior designs' range.** Spans replayed 7–10× (1M) and
  12–14× (100M) compaction entry writes, and measured 6–7× over 12,000
  commits (too few to reach a base merge at 100M); two views measured
  6.5–8.5× and replayed 5.8–9.2×, but stored 55–293 MB at 1M against 15 MB
  here (cited).
- **Storage stays near the index's own size**: 1.5 entries per live key at
  the head at 1M, 1.2 at 100M, plus side parts.
- **Cold writers at 100M are the expensive case**, as in every design
  (cited: spans $91, two views $114 a month on S3 with a cold writer at
  100M): ~1K GETs per commit, one per key, since random keys share no block
  in a 100M-key base. The engine's warm cache is what makes writes cheap.

## How it differs from spans and from two views

| | Spans | Two views | Stamped layers |
|---|---|---|---|
| structures | one: spans tiling time | two: a key view (base + chain) and a time tree | one: layers tiling time |
| per key per file | one version per reader segment, plus a predecessor | one entry (before and after state) | one entry: newest state, stamp, flips |
| what answers "present at P" | the version kept for P's endpoint | a cover of aligned nodes from P | the flips after P |
| P inside a merged file | only at a kept endpoint | never (the tree keeps every level) | anywhere |
| readers in merge decisions | every endpoint, a read rule, a span cap | none (but every level kept back to the floor) | none; one number (the cut) bounds flips |
| H | clipped at g(N + 1) in every read | the cover ends at N | a pinned manifest |
| reads bounded by | the read rule (soft) and the cap | exact covers (1.03× at 100M, ~4× at 1M) | k × newer + Z (hard, reader-agnostic) |
| storage, 1M, daily readers | 42–79 MB (cited) | 55–293 MB (cited) | 15 MB |

Why each difference follows from the problem:

- **No versions per reader.** Spans kept the version each endpoint sees
  because its reads asked for a key's state at P (lookups at endpoints, the
  net rule's payload). Δ asks only for presence at P, and the change kinds
  make presence a parity of flips. One entry per key per layer follows, and so
  do lookups that do not slow down with readers (spans measured 1.8 s for
  1K cold lookups at 1M with 100 daily readers, two views 0.17 s; cited).
- **No interior N.** Spans clipped every read at g(N + 1) because passes
  ended at old commits; two views kept nodes ending anywhere. H is now the
  head or a pin, so a pinned manifest suffices: nothing in a file refers to H.
- **One tiling, not every level.** Two views kept every aligned level so
  that any [P, N] had an exact cover; that cost ~41 entries per live key per
  day of reader lag at 1M (cited). Flips make a straddled layer exact, so only
  one tiling is needed, and a straddled layer's cost is bounded by the rule
  instead: a few bytes more per reader, against several copies of the index.
- **No reservations.** Both prior designs needed every reader's start (and
  spans every end and landing point) recorded before the next merge or
  collection, or the reader lost its answer. Here a reader needs only P ≥ cut,
  a single monotone number checked loudly.

What it keeps from them: layers tiling time (spans), a base absorbing old layers
on a size trigger (both), the reader-bound idea (spans' read rule, made
reader-agnostic), zstd-1 and 16 KiB blocks with a filterless base (two
views' measurements), the epoch rule and durable attempt accounting
(spans' fixes), and the automaton seek for globs (two views' interval test,
fixed by A25).

## What the design does not support

| Not supported | Who would need it | Why no caller does |
|---|---|---|
| a key's version or payload at P | the net rule "neither" for a source key that reverted; a before-image's payload | the brief allows the redundant update; the observed set classes by presence plus "differs", and a key with a stamp after P differs for derived outputs by construction |
| Δ at an interior H (no batch pinned it) | the old passes' paused ends; a before-image at the cut | runs walk at their own heads; a fold relabels to the head (query form 5) |
| lookups or scans at an old commit | spans' `lookup(at=c)`, a full pass's frozen snapshot | there is no pass; each batch reads at its own pinned head |
| P below the cut | a reader older than the window | it holds a before-image; a stale P fails loudly and becomes a full compare |
| the replaced generation in the delta | store cleanup | the writer's lookup returns it at commit (above) |
| fast suffix and infix scans | a first run under `**/*.pdf` | full scans are rare and read ~1 GB at 100M; a derived index can come later |
| version-ordered or time-ordered listings ("keys changed in commit 7") | none in the brief | the journal names each commit's delta, which lists them |

## Prior designs, walked as a checklist

A17's findings against spans as built, and A25's against two views:

| Finding | Here |
|---|---|
| A17 R1, F40: a zombie deletes a published output | the barrier before deletion and the epoch rule, as fixed for spans |
| A17 R2: a key's versions cross a page window | cannot arise: one entry per key per layer; a page always moves its cursor (it returns c') |
| A17 R3: cached lookup picks the wrong file | cannot arise: a layer's files have disjoint key ranges and one entry per key |
| A17 R4, R5: selections and covering retries lose their endpoints | cannot arise: nothing is reserved; P ≥ cut is the only condition |
| A17 R6: admission for every writer | kept: one backlog rule (queued merge input ≤ 2× the index's bytes) gates every index writer, sources and failure indexes included |
| A17 R7: rejected rewrites re-uploaded | cannot arise: no rewrite may publish nothing; every merge's output replaces its inputs |
| A17 R8, A25 R6: attempt budget lost on restart | kept: attempts reserved durably before upload |
| A17 R9: `u64::MAX` as "no bound" | cannot arise: reads compare stamps with g(P) only; P = −∞ is a separate case, not a sentinel |
| A17 R10: a hot key's versions in memory | a hot key holds one entry per layer; only its flips grow (≤ the window's commits that toggled it), streamed |
| A25 R1, R2: glob pruning holes | the automaton seek is checked against Solera's matcher (phase 2), with A25's two counterexamples as named cases |
| A25 R3: read-ahead newer than a pass's end | cannot arise: no pass; a batch reads at its own head |
| A25 R4: names reused across lives | names carry the life, the epoch and a unique id |
| A25 R5: unbounded retention under a stalled reader | the window bounds the cut's lag; a stalled reader is folded |
| A25 R7, R8: layer counts and storage sampled with bias | replayed at every 97th head over the second half of each trace, all tiers counted; storage includes side parts |

## Settled, and open

Settled (D151):

1. **Cleanup's replaced generation** comes from the writer's head lookup at
   commit, as the cleanup-task design on main does (276a26e).
2. **A reader behind the window** builds its before-image as Δ(P, head) and
   its base moves to that head: the index never serves an older end.
3. **The cut's oldest live P** includes in-flight batches' heads; a P below
   the cut fails loudly and falls back to a full compare.

Open:

1. **The window W.** Run-history retention sets it. At 1M keys with this
   trace a week of removals is 1.5× the live keys; the oldest-live-P cut makes
   W matter only for stalled readers.
2. **Filters in the layer index**: kept by default in the design; the
   prototype has none (its cold readers would not use them); phase 2's
   numbers say whether a held filter is worth adding.

## Phase 2 plan, for the go

- **Prototype** on W53's harness (`viewbench.py`, branch
  `exp/key-index-two-views`), as bench code: the minimal delta writer
  (W53's codec), layers and layer indexes, the merge (native, a k-way merge with
  flip union, reusing `stream.rs`'s newest-wins merge), the policy, the
  cut, and Δ in its forms over `ObjectIO` with injected latency.
- **Traces**: the baseline's (1M and 100M, 12,000 commits of 1K keys; readers
  1, 100, 360, 8,640, 10,000 behind; 100 daily readers; churn; a 1M-key
  commit), plus a long trace at 1M (≥ 70,000 commits) so a 7-day window and
  base merges are reached.
- **Checked** key by key against the fold for every read, including pages
  resumed across merges and reads with P inside a merged layer.
- **Measured against** spans and two views where the baseline has them: cold
  lookups, write resolution, near and far catch-ups (first page and full),
  full-scan pages with prefix and non-prefix patterns, writes per entry,
  PUTs, storage, dollars.
