# Key index design: stamped layers, tiered by recency

Status: **built** (T33, D156), replacing spans. The design and its
alternatives are argued in `docs/key-index-from-first-principles.md` on
branch `design/key-index-fp` (df9ac17), with the prototype and its
replays; this document is what runs. Bytes are in `key-index-format.md`;
the code is `native/src/layers.rs` (blocks, merges, scans, the delta
writer) and `python/solera/keys/layers.py` (the state, reads, writes,
merges), with the engine's cache in `layer_cache.py`.

Evidence labels: **measured** (W53's 1M campaign on real files, cold
readers in isolated processes, 30 ms per request, 80 MB/s per connection,
64 in parallel: `bench/keys/views/campaign.md` on `exp/key-index-campaign`,
43aa22c; 236 reads, every one checked key by key against the per-commit
fold, no mismatch), **replayed** (the structure on metadata:
`bench/keys/fp/model.py` on the design branch), **checked**
(`tests/sdk/test_keys_layers.py`: random histories against the fold).

## The answer in brief

- **One fact shapes the design.** A commit's delta says, per key, whether
  it was added, updated or removed. So a key's presence at any past commit P
  follows from its presence now and how often it was added or removed since:
  **presence at P = presence at H, flipped once per add or remove after P.**
  The index never stores a key's state at P. Per key it keeps its newest
  state, the commit and generation of its last change, and the commits that
  added or removed it (its **flips**), as far back as readers may start.
- **One structure: a few key-sorted layers, tiered by recency.** A **layer**
  covers commits `[a, b]` and holds one entry per key changed in them. A
  commit's delta is the layer `[c, c]`, written by the attempt and never
  rewritten at commit. Upkeep merges adjacent layers: newest state wins,
  flips unite. The oldest layer is the **base**.
- **Δ(P, H) reads only the layers that end after P**, and is exact for any P
  at or after the cut, inside a merged layer too: no endpoints, no versions
  kept per reader, no reservations before merges.
- **One reader-agnostic rule bounds what a reader reads:** a merge's output
  reaching past the cut holds at most 4 × the bytes of every newer layer, plus
  1 MiB. A reader then reads at most 5 × what changed, plus 1 MiB, wherever
  its P falls.
- **Retention is one number, the cut**: the oldest commit any reader may still
  start from. Merges drop flips at or below it.
- **Measured at 1M keys** (layers against spans, zlib64): a reader 100
  commits behind in 0.11 s and 7 GETs (spans 0.52 s), 10,000 behind in 1.8 s
  and 33 GETs (spans 7.6 s, 141 GETs); 16 MB stored (57 MB); 6.2 background
  entry writes per entry committed (6.3); $1.91 a month on S3 with a warm
  writer (cited from the campaign's cost model).
- **What it gives up:** a source key's payload at P (a key that reverts reads
  as a redundant update), reads at an H no batch pinned, and fast suffix or
  infix pattern scans (they read the whole index, at full-scan moments only).

## What a layer holds

A layer covers commits `[a, b]`. Per key changed in them, one entry:

| Field | Meaning |
|---|---|
| key | |
| present | whether the key exists after `b` |
| start | whether it existed just before `a` |
| commit, generation | of its last change in `[a, b]`: its version at `b` |
| flips | the commits in `[a, b]` that added or removed it, newest first, those at or below the cut dropped |
| payload | a source key's own version at `b` (`versions.md` §2), if present |

**A delta is a layer.** Commit c's delta holds each key once, with its kind
relative to the head before c, and every entry takes the commit's number
and generation from the commit record. Read as a layer: added → present,
absent at the start, one flip at c; updated → present at both ends, no flip;
removed → absent, present at the start, one flip at c.

**Main and side parts.** A layer's entries live in two parts:

- the **main** part holds what the head and readers starting after the layer
  need: present keys, and keys present at the start (their removal shadows
  older entries);
- the **side** part holds what only a reader starting inside the layer
  needs: keys absent at both ends (added and removed inside it, under churn
  most temporary keys), and in the base every absent key with flips left
  (the base's **graveyard**: nothing is older, so its removals shadow
  nothing).

A reader whose P is before a layer skips its side part: every flip of the
layer is then after P, so a key absent at both ends flipped an even number
of times and changes nothing between P and H. Head reads skip every side
part.

**The index object.** Parts larger than 256 KiB have one beside them: per
block its file, offset, length, first key, entry count and newest commit
(`key-index-format.md` § Indexes). Smaller parts are read whole.

**The state.** The layer list lives in the journal, in the partition's index
state (`LayerState`): per layer `[a, b]`, its parts' files (name, size,
entries, first and last key) and index objects, and for a delta the
commit's generation; plus the index's prefix, its **life**, its key count
and its cut. A batch pins the state at its H (the claim's pin): merges
publish new lists meanwhile, and the batch keeps reading its own.

## Reading Δ(P, H)

Over the state pinned at H, take the layers that end after P: their main
parts, plus the side part of the layer P falls inside. Merge them by key.
For each key:

```
entries = its entries in those layers, newest first, keeping only commits > P
if none:   unchanged since P: skip
at_H    = entries[0].present      (its generation and payload too)
at_P    = at_H XOR (number of flips > P across entries is odd)
if not at_P and not at_H: skip    (added and removed inside (P, H])
emit (key, at_P, at_H, generation, payload)
```

Only the layer P falls inside can hold entries changed at or before P; every
newer layer lies wholly after P. Why it is exact: a merge keeps, per key,
its newest state and every flip after the cut; the layers ending after P
cover (P, H] with nothing missing; so the newest entry changed after P is
the key's state at H, and the flips after P are exactly its adds and
removes in (P, H].

**Errors, loud by design.** A P below the cut raises `CutError`: flips the
answer needs may be gone, and the caller falls back to a full compare (a
full run, never a silent wrong answer). An H inside a merged layer raises
`NotHeld`: a batch reads the state it pinned at its H.

### A worked example

`items` holds `a` and `b`, written by commit 0. Commit c writes at
generation 10c.

| Commit | Change | Delta |
|---|---|---|
| 1 | add c | c added |
| 2 | remove a | a removed |
| 3 | re-add a, update b | a added · b updated |
| 4 | add d, update c | c updated · d added |
| 5 | remove d | d removed |

Upkeep merges commits 1–5 (● present, ○ absent; flips as commits):

```
base  [0, 0]   main: a ● c0   b ● c0
layer [1, 5]   main: a ● c3 {3, 2}   b ● c3 {}   c ● c4 {1}
               side: d ○ c5 {5, 4}                 (absent at both ends)
```

- **X started at 0** (holds a, b). Δ(0, 5) reads the main part of `[1, 5]`
  only: the base ends at 0, and `[1, 5]` lies wholly after P, so its side
  part changes nothing for X. `a`: flips after 0 are {3, 2}, even: present
  at 0 as now, **updated**. `b`: **updated**. `c`: {1}, odd: absent at 0,
  **added**.
- **Y started at 2** (holds b, c; `a` was removed at 2). Δ(2, 5): P falls
  inside `[1, 5]`, so its side part is read too. `a`: flips after 2 are {3}:
  absent at 2, present now, **added**. `b`: **updated**. `c`: no flips after
  2, present at both ends, changed at 4: **updated**. `d`: {5, 4}, even,
  absent now: absent at both ends, skipped.
- **A writer** committing 6 (update c, add d) looks c and d up at the head:
  c ● at generation 40 (updated; on an immutable store, 40's object is what
  it replaces), d absent (added).
- **The cut reaches 3.** The next merge of the base with `[1, 5]` drops flips
  at or below 3: `a ● c3`, `b ● c3`, `c ● c4`, and `d ○ {5, 4}` in the base's
  graveyard: a reader started at 4 must still learn "removed". Once the cut
  passes 5, `d` goes.

## Query forms

All forms are one native scan (`layers_scan`) over the layers' blocks, so a
key list, a page and a lookup cannot disagree (A31 R1).

1. **A page**, Δ(P, H, after c, first N): the merge walk from key c. The
   engine picks a bound c' from the index objects so the layers after P hold
   about N entries in (c, c'], fetches every layer's blocks there in one
   round trip, and returns every differing key in (c, c'] and c'. A short
   page is still a correct range. Pages also stop at `upto`, at a byte
   budget (64 MiB read: a sparse pattern cannot pull the whole index into
   memory), and keep a bounded block cache (64 MiB) so consecutive pages
   share blocks. The layer P falls inside skips blocks whose newest commit
   is at or before P: with keys that follow time (sequential ids, dates) it
   costs almost nothing.
2. **A sorted key list**, Δ(P, H, keys): the same scan restricted to the
   keys, side parts included. A key unchanged since P is omitted.
3. **Head lookups**, Δ(−∞, H, keys) (writers, the resolver): every layer's
   main part in one round trip, in parallel; per key the newest layer
   holding it decides. Allocation-free walk (`lookup`).
4. **Full scans with a pattern** (first runs, pattern changes): a page with
   P = −∞ and a glob. Solera's grammar (`**/`, `**`, `*`, `?`) compiles to a
   bitmask automaton with a literal prefilter; a block is skipped when no
   match can lie between its first key and the next block's
   (`may_match_between`, checked exhaustively against the matcher on short
   keys: A25's R1 and R2). A prefix pattern reads its range; a suffix or
   infix pattern reads every block. Measured: `cust-00042*` 0.22 s and 0.2 MB,
   `*4242*` 0.54 s and 17 MB (the whole 1M index).

### Stream or seek, in bytes

For a key list, each part is either **sought** (one range GET per run of
adjacent blocks the keys fall in) or **streamed** (16 MiB range GETs over
the whole part), whichever a time model says is faster: request waves (64
in flight, 30 ms each), a connection's bytes (80 MB/s), and entries decoded
(120M/s). Within 10% the plan with fewer requests wins. The choice is made
in bytes and requests, never in block counts: a 100M-key base is sought for
1K keys (~1K GETs, ~0.5 s), never streamed (~0.6 s of transfer and ~0.8 s
of decoding). Small parts (no index object) are read whole.

## Writing a commit's delta

The attempt resolves its writes **exactly** at the head it claimed: per
written key, in key order, against the index's head lookup.

| Write | At the head | Delta entry |
|---|---|---|
| upsert | absent | added |
| upsert | present | updated (a source key with an equal payload: nothing) |
| remove | present | removed |
| remove | absent | nothing |

So the key count is exact (added − removed), and the store is told exactly
what to write and delete. On an immutable store's outputs each updated or
removed entry also records the generation it replaced, for the store's
cleanup task (`lifecycle.md` §9.8); fenced stores and sources record none.
The index never reads it.

Two paths, by size:

- **Sparse** (`resolve`): the blocks the keys fall in (or whole parts,
  where streaming is faster), held in memory while it resolves. Taken when
  that holds at most 64 MiB.
- **Streamed** (`write_patch`, `write_replace`): a join of the sorted writes
  against every layer's main part, read 8 MiB at a time per input. A
  **replacement** (a full key map: a source commit, a reconcile) removes
  every present key it omits; a reconcile streams the store's listing with
  the run's writes as an overlay.

The delta's files are named by commit and attempt (`{commit}-{attempt}-{n}.lay`),
so a retry never overwrites a dead attempt's. The commit record carries
them (`DeltaFiles`: part, added, removed, generation); `committed` refuses
a generation that does not rise (A31 R4).

The engine's resolver (`solera/keys/resolver.py`) computes the same delta
for workers from the engine's warm cache, and declines deltas over 256 KiB
(the worker then resolves itself).

## Compaction

Upkeep merges adjacent layers. A merge keeps per key the newest state and
the union of flips after the cut; with the base among its inputs it drops
absent keys with no flips left.

**The rule** (`LayerState.plan`):

1. **Tiers.** A layer's tier is ⌊log₄(bytes / 8 KiB)⌋. Four adjacent layers
   of one tier merge, the newest group first.
2. **The base** absorbs the layers above it once they hold a quarter of its
   bytes, reaching as far as rule 3 allows.
3. **The reader bound** (both): an output reaching past the cut holds at most
   4 × the bytes of every newer layer + 1 MiB (a delta counted at 1.45 × its
   bytes: a layer entry is larger). An output wholly at or below the cut is
   free.

Why the bound bounds reads: a reader whose P lies inside layer i reads every
newer layer (they hold only changes after P) plus layer i, which is at most
4 × those + 1 MiB. Without the bound, a reader 100 commits behind at 100M
keys reads up to 1,000 × what changed (replayed).

**Two lanes**, each one merge at a time: the base, and the tiers. Their
inputs never overlap (a lane plans around layers a running merge holds).

**Self-check (A31 R4).** When no input lost a flip to the cut (every input
is complete), a merge checks every key: presence = start XOR (odd number of
flips). It also requires rising generations across commits. A failure
aborts the merge loudly: a corrupt layer is never published.

**Attempts.** A merge is identified by `{life}|{input ids}`. Upkeep records
`MergeAttempted` durably before uploading; once one input set has had 3
attempts, none published, the index merges no more in that life, alarmed
(A17's R8, A25's R6): backpressure then holds its writers until an operator
acts or a new life starts.

**Backpressure.** When an index holds more than 64 layers (upkeep failing
or behind), commits to the partition wait, from every writer of the index:
outputs, sources and outcome indexes (A17's R6).

Measured at 1M: 6.2 background entry writes per entry committed, 1.39 PUTs
per commit, 16 MB stored on average (22 MB at peak) for ~9 MB of live keys.

## Retention: the cut

**The cut** is the oldest commit any reader may still start from: every
position, pass and in-flight claim (`model.oldest_observed`). The window
of run history does not move it: a reader behind the window is folded
first (the observed set rebuilds its before-image with a Δ(P, head)), and
the fold then lets the oldest P rise (A31 R3). The state records the cut
and it only rises (`with_cut`).

A merge reads the cut when planned and drops flips at or below it. A key:

- updated keeps no flips;
- added keeps one flip until the cut passes it;
- removed keeps its entry and one flip until the cut passes the removal, then
  goes at the next base merge;
- added and removed in the window keeps two flips and an absent entry (a
  side part) until the cut passes the removal.

The worst case is a key toggled by every commit: one flip per commit since
the cut, a long entry read and merged as a stream, like a large payload.

Raising the cut deletes nothing by itself: it lets later merges drop flips,
whose inputs then become garbage as below.

## Lifecycles

**Names.** A merge output is
`{life}/l{a}-{b}-e{epoch}-{ulid}-{m|s}{n}.lay`, under the index's prefix
(`keys/{output}/{partition}/`). No name is reused, and a reset (or move or
removal) starts a new life, so one life's collector never targets
another's files (A25's R4).

**Publication.** Reserve the attempt (durable `MergeAttempted`), upload,
then publish `IndexMerged {life, ids, layer}`. It applies only if the life
matches and the inputs are still adjacent layers of the current list
(`holds`); otherwise its output is an orphan.

**Deletion.** Only the engine deletes index files, never a store's cleanup
task. A file is dead when the state stops referencing it (a merge published,
a life ended), no pin older than that remains, and, for an immutable store's
delta, the partition's cleanup cursor has passed it (cleanup reads its
replaced generations: `lifecycle.md` §9.8). Deletion
follows a successful journal write made after the decision: a fenced
engine's write fails and it deletes nothing (A17's R1).

**Orphans.** Uploads no state, pin or running merge names: an abandoned
attempt's delta, a refused merge's output. The collector judges a merge
output only if its epoch is at most its own (a newer engine's outputs carry
a newer epoch); an attempt's delta goes once its attempt is settled.

**Pins.** A batch pins the state at its H for its duration; the layers it
names stay, whatever upkeep publishes meanwhile.

## The engine's cache

The engine keeps (`LayerCache`):

- **in memory**, index objects and small parts (LRU by bytes);
- **on its disk**, layer files: installed from its own uploads (a merge's
  outputs, a resolved delta), filled per index it serves, evicted by bytes.

A warm index answers lookups, resolves and pages from local reads, with no
object GET (checked). This fixes the campaign's one losing row: a cold 1K
lookup took 0.50 s and 17 GETs (two views: 0.17 s, 7 GETs), two round trips
for the index objects then small layers streamed whole.

## Measured

W53's campaign, 1M keys, base trace, cold readers (first page · full, GETs,
MB, peak memory):

| | layers | spans (zlib64) |
|---|---|---|
| 100 behind | 0.09 · 0.11 s · 7 · 0.9 MB · 39 MB | 0.51 · 0.52 s · 9 · 6.2 MB · 107 MB |
| 360 behind | 0.26 · 0.46 s · 15 · 5.2 MB · 73 MB | 0.38 · 0.60 s · 10 · 6.2 MB · 156 MB |
| 10,000 behind | 0.41 · 1.80 s · 33 · 21 MB · 159 MB | 1.04 · 7.55 s · 141 · 75 MB · 489 MB |
| 1K cold lookups | 0.50 s · 17 · 17 MB · 47 MB | 0.54 s · 15 · 98 MB · 206 MB |
| 100K-key page | 0.35 s · 16 · 4.5 MB · 61 MB | 1.23 s · 26 · 14.9 MB · 250 MB |
| stored, mean · peak | 16 · 22 MB | 57 · 98 MB |
| background writes per entry committed | 6.18 | 6.34 |
| $ a month, S3 warm · cold writer | $1.91 · $3.67 | $1.99 · $3.55 |

Under churn (half the keys temporary) a reader 10,000 behind reads 35 MB in
3.1 s (spans 75 MB, 12.7 s), and storage is 25 MB. The cold rows predate the
engine cache above. The 360-behind row is the one where layers take more
requests than the others (15 GETs against 10–11).

## What is checked

- **Against the fold** (`test_keys_layers.py`): random histories (adds,
  updates, removes, source payloads, replacements) with merges under a
  moving cut; at every P from the cut to the head, every form (pages with
  `after`, `upto`, globs and budgets, key lists, lookups, pinned heads)
  against the per-commit fold, key by key.
- **Lifecycle**: CutError below the cut, NotHeld inside a merged layer,
  generations that must rise, publication refused on a changed life or
  inputs, disjoint lanes and attempt keys, epochs and backlog, the warm
  cache reading no object.
- **Native** (`layers.rs`): round trips, merges keeping flips after the cut
  and splitting parts, the parity self-check, replacements, the glob interval
  test exhaustively against the matcher.

## What it gives up

- **A source key's payload at P.** A layer keeps the payload at its last
  change only, so a key that goes v1 → v2 → v1 inside (P, H] reads as
  updated. A consumer handles it as any update. Keeping it would mean a
  payload per update for the whole window.
- **Reads at an H no batch pinned.** Merges reach past it.
- **Fast suffix and infix scans.** They read the whole index (17 MB at 1M,
  ~1 GB at 100M), at first runs and pattern changes only. A reversed-key copy
  would make suffixes seek at the cost of a second copy of every key; not
  built.
