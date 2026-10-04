# Key index design: spans with endpoint versions

Status: **approved for building** (coordinator, D53), being built in steps on
`main`: step 1, exact writes and format v4 (one filter, bounded blocks), is
in; spans, the version-keeping merge and the read API follow. Until then the
leveled index and its delta log described in `object-store-state.md` §6 run.
The measurements and prototypes it cites live on branch
`bb/key-index-design-first-principles-thr_xvgqnrw2kr`.

Revised after review, for Erwin. The first version
(cc54fcc) was reviewed: build with changes. Its span representation and
exact writes stay. Its boundary lifecycle and merge policy change here,
because endpoints that block merges make the span count grow with the
number of observers, and an observer retiring forced a large rewrite. The
review's findings and where each is answered are listed at the end. Every
number says where it comes from: **measured** (real files, today's
reader: `bench/keys/catchup.py`, `layouts.py`, `tiling_reads.py`),
**replayed** (the merge policy on metadata, with the density model of
`amplification.py`: `spans.py`, `retention.py`), or **proved** (Lean,
`experiments/lean/KeyIndex/WriteBound.lean` and `Segments.lean` at 5a34e6e on branch
`bb/experiment-bend-2-for-the-key-index-s-delta-alge-thr_dqc6iaviun`).
Replayed numbers are preliminary until the implementation replaces them.

**Measured on format v4 files** (`bench/keys/v4bench.py`, prototype
`native/src/v4.rs`): the `capped` policy running real version-keeping
merges over 12,000 commits of 1K keys, with consumers holding live
endpoints, then cold readers in isolated processes (30 ms per request,
80 MB/s per connection, 64 in parallel). Every read was compared key by key
with the per-commit fold: no mismatch in any scenario. Peak memory is the
reader process's own, above its baseline.

| Scenario | Spans, max | Compaction / total entry writes | Compressed bytes, compaction / total | Stored |
|---|---|---|---|---|
| 1M, daily readers, a 100K-key commit every 3,000 | 16 | 6.5× / 7.5× | 6.0× / 7.0× | 33 MB |
| 1M, daily readers, hot set (half the updates to 1% of keys) | 15 | 6.1× / 7.1× | 5.5× / 6.5× | 42 MB |
| 1M, 100 daily readers, spread | 24 | 6.4× / 7.4× | 5.8× / 6.8× | 80 MB |
| 1M, a daily reader stalled from commit 2,000 | 13 | 6.5× / 7.5× | 6.0× / 7.0× | 29 MB |
| 1M, 60 staggered hourly readers | 14 | 7.0× / 8.0× | 6.3× / 7.3× | 32 MB |
| 100M, daily readers, a 1M-key commit every 3,000 | 18 | 6.7× / 7.7× | 6.6× / 7.6× | 340 MB |

Catch-up, `changes(P, H)` paged 100K entries at a time, against the
control (the same deltas in one packed object, merged at once; its first
key needs every delta's first block, so its first page is its full read):

| Behind | v4 spans: first page · full · GETs · MB · peak | Packed: full · GETs · MB · peak |
|---|---|---|
| 100, 1M keys | 0.10 s · 0.10–0.16 s · 10–16 · 0.8–1.7 · 22–35 MB | 0.06 s · 1 · 1.0 · 15 MB |
| 360, 1M | 0.09–0.10 s · 0.29–0.72 s · 25–55 · 2.9–7.7 · 17–46 MB | 0.16 s · 1 · 3.6 · 52 MB |
| 10,000, 1M (1.3–1.4M keys changed) | 0.10–0.11 s · 1.4–5.6 s · 105–1,502 · 16–61 · 54–137 MB | 5.5–5.8 s · 6–7 · 100–102 · ~1.5 GB |
| 100, 100M | 0.09 s · 0.51 s · 24 · 5.5 · 11 MB | 0.06 s · 1 · 1.0 · 14 MB |
| 360, 100M | 0.10 s · 0.53 s · 24 · 5.5 · 46 MB | 0.16 s · 1 · 3.5 · 52 MB |
| 10,000, 100M (12M keys changed) | 0.11 s · 8.5 s · 633 · 79 · 554 MB | 6.6 s · 7 · 116 · 1,553 MB |

At the head: 1K exact lookups take 18–42 GETs, 21–61 MB and 0.48–0.88 s at
1M (today's leveled layout on a matched trace: 3 GETs, 10 MB, 0.21 s), and
1,166 GETs, 242 MB and 1.8 s at 100M (leveled, matched trace with other
keys: 907 GETs, 200 MB, 1.13 s). A 100K-key scan page takes 0.08–0.10 s.

What this says:

- **Writes and span counts hold** on real files: 6–7× compaction entry
  writes in every scenario, at most 24 spans with 100 readers, the cap
  never reached.
- **Far catch-ups win where it matters for a consumer**: the first page in
  0.1 s instead of 5.5–6.6 s, and a fraction of the memory. A full 12M-key
  read at 100M is slower than one packed merge (8.5 s against 6.6 s).
- **Near catch-ups lose to one packed object**: 0.1–0.7 s against
  0.06–0.16 s, since spans of 5–7 files and the read rule's 10 MB slack
  cost more requests and bytes than one object holding exactly the deltas.
- **Lookups pay for retained versions**: at 1M with lagging readers the
  index holds 3–8× the live keys and a cold lookup reads them; at 100M the
  versions are a small share and lookups cost about what they cost today.
  The engine's warm cache answers lookups locally either way.

Limits: keys in the v4 runs are evenly spaced (they compress to ~3.4 B per
entry at 100M); merges ran in-process with instant upkeep; upload and
publication failures are not injected here (`retention.py` models them).

Write ratios count entries: entries written (merge outputs, plus the
initial delta where marked "total") per entry committed. Compressed bytes
are reported separately where measured. "RSS" figures are increases in the
benchmark process's high-water mark, not a reader's peak memory.

It follows `presence-at-position.md` and `delta-log-ranges.md` (branch
`bb/design-study-exact-presence-at-a-position`), and keeps their baseline:
every delta entry records exactly whether its key was live before it.

## The answer in brief

Today the index is three structures: a leveled LSM for lookups and full
reads, the delta log (one file per commit, kept for consumers) for
catch-up, and a proposed range tree to make far catch-ups cheap. Two
orderings are needed (key order for lookups and scans, commit order for
catch-up), but one structure gives both: **a list of key-sorted file sets
("spans"), each covering a stretch of commits**. Lookups read the spans
newest first, like LSM levels. A consumer reads the spans from its
position on, like the delta log.

The commits observers read from are **endpoints**. A merge may cross an
endpoint: it then keeps, per key, the version that endpoint sees (the key's
state just before it), as an LSM keeps the versions its snapshots see. So
observers usually cost retained versions rather than extra spans, and an
observer that goes away costs nothing until a merge would rewrite its span
anyway. Not always: large commits each pinned by its own endpoint cannot
share a span without making some reader read far more than it needs (the
second review's A12-4). There a hard cap on spans decides, and the readers
inside a forced merge pay with extra reads.

- **Entry writes are bounded**, whatever order observers come and go in:
  entries written (deltas included) ≤ (1 + 43R + 20R ⌊log₂ K⌋) × entries
  committed, K the most entries a span holds and R the attempts per merge
  (proved in Lean for every merge the policy makes, including forced ones
  and single-span rewrites; R = 3 and K = 10⁹ give 1,870×: loose, but a
  guarantee). Replayed under the reviews' churn scenarios: 7–10× compaction
  writes at 1M keys, 12–14× at 100M. On real files, one trace through the
  real compactions: 14.0× total entry writes against 51.3× for today's
  leveled planner at 100M keys (9.0× against 18.2× at 1M).
- **Spans are capped at 32.** Under the replayed patterns (1 to 1,000
  distinct endpoints, stalled passes, churn) they stay at 9–22 on average
  and 30 at most at 100M keys (7–19 and 24 at 1M); large pinned commits hit
  the cap, and upkeep then forces guarded merges
  across endpoints. Blocking endpoints instead gives 546 spans at 100.
- **Catch-up reads what changed in its range**: measured over real spans,
  paged, 10,000 commits behind at 100M keys takes 16 spans, 259 GETs, 65 MB
  and 7.7 s, against 10,000 deltas today, or one packed object merged at
  once (6 GETs, 97 MB, 12.9 s). No reader ever reads more than a full read.
- **Lookups and appends cost what they cost today**, measured on layouts
  built from the same trace: 901 against 907 GETs for 1K cold exact lookups
  at 100M, 10 ms warm. The first version's 40% saving compared against a
  leveled layout holding twice the entries; it is withdrawn.
- **Deleted**: the separate log, the range tree, log truncation, the leveled
  planner, the inexact count and recount, the tombstone filter and the
  `.kg` garbage files.

## The structure

**Span.** A span covers commits `[a, b]` and is a key-sorted set of `.kx`
files with non-overlapping key ranges. A commit's delta is the span
`[c, c]`. A span is split into **segments** by the endpoints that were live
when it was written. For each key it holds:

- one **version** (generation, deleted flag, payload) per segment the key
  changed in: the newest one in that segment. The last is the key's newest
  version in the span; each earlier one is what the next endpoint sees;
- its **predecessor**, on its oldest version: the key's generation before
  `a`, or none if it was not live.

So a key's entries in a file run newest first, and a lookup takes the
first. The format lets a key appear several times in a file, in that order
(format v4). The span from commit 0, the **base**, holds no predecessors.
Its **initial segment** (the part before the oldest live endpoint inside
it) keeps live keys only: a tombstone there means absent, as no entry does.
Every later segment keeps added-then-removed tombstones, even inside the
base. Otherwise (the second review's A12-1): a consumer at 1, commit 1 adds
`d` and a selection delivers it live, commit 2 removes `d`; a base merge
crossing endpoint 1 keeps only `d`'s tombstone at 2, and dropping it would
leave the consumer holding `d` forever.

**Tiling.** The spans tile commit time from 0 to the head, without gaps or
overlaps. `IndexState` holds the live count and a list of spans
`(a, b, segment starts, files)`.

**Endpoints**, and who holds them. An endpoint is a commit some reader will
start from or read at. Each is born at the head + 1, past every span, so
adding one never cuts a span.

| Holder | Endpoint | Reserved | Released |
|---|---|---|---|
| a consumer's position | `next` | when the position is set | when it moves or is dropped |
| an attempt that may advance a position: a pass's batch, a keys= selection (a covering one collapses the record to its head + 1), a pattern-change drain | the head + 1 at its claim: its landing point | at the claim or preparation, before its record is installed | when its result is durably part of the position or pass; on failure |
| a pass under way (delta, full or diff) over commits up to `to` | `to + 1` | when the pass starts | when it ends |
| a pattern change | its split commit + 1 | at the change | when its drain ends |

Read-ahead entries hold no endpoint (see changes, below).

**One merge.** Merging adjacent spans concatenates their segments, then
coalesces the segments whose dividing endpoint is no longer live: per key,
the newer version survives. A version is kept iff it is the key's newest,
or some live endpoint sees it. The oldest predecessor survives. Nothing
else is dropped: a key added and removed inside a span stays as a
tombstone naming no predecessor, since a read-ahead may have delivered it
in between. For a fixed set of live endpoints the merge is associative
(proved for one segment in Bend, `LAWS.bend`), and later merges see fewer
endpoints inside old spans, never more.

## The merge policy

Three rules, in order of precedence:

1. **The guard** (hard; bounds writes). The largest input holds at most 4×
   the other inputs combined, in entries, counted before dedup; or the merge
   drops at least a quarter of its inputs (output ≤ ¾ of the inputs). Under
   it, entries written, the initial deltas included, are at most
   (1 + 43R + 20R ⌊log₂ K⌋) × entries committed, for any sequence of
   commits, endpoint births and retirements (Lean, `WriteBound.lean` at
   5a34e6e). Two conditions come with it:
   - K bounds a span's entries. With one version per segment, distinct
     keys no longer do; keys × (1 + endpoints live inside the span) does,
     and so does the total committed over the horizon.
   - R bounds attempts. Every attempt of a merge counts as written, whether
     it publishes, fails, or is abandoned after upload. The engine allows
     R = 3 attempts per input set; after the third failure it stops merging
     that index and raises an alarm rather than retrying forever. Abandoned
     outputs are reclaimed by the orphan collector (lifecycles, below).
   The single-span rewrite (a span alone, once a quarter of its entries are
   versions no live endpoint sees) is the "drops a quarter" case.
2. **The fan-in cap** (hard; bounds readers' fan-in). At most F = 32
   spans per index. Past it, upkeep merges the guarded window of 2 to 4
   adjacent spans with the fewest entries, even across a live endpoint and
   against the read rule.
3. **The read rule** (soft; keeps catch-up local). Below the cap, a merge
   may put a live endpoint `e` inside its output only if, in the output,
   the bytes before `e` are at most max(λ × the bytes from `e` on, Z), and
   the bytes from `e` on at most max(λ × the bytes before `e`, Z): λ = 1,
   Z = 10 MB. Both ends matter: a pass pinned at `[P, N]` must not face much
   older data before `P`, nor much newer data after `N` (the second review's
   A12-3: one old entry merged with 2M newer ones made a pass read 2M
   entries for 1). The rule is a preference, not a bound: under the cap it
   gives way, and a reader's read is then bounded only by a full read.

Upkeep runs these triggers, in order:

- **Into the base.** Once the spans after the base, as far as the read rule
  lets it reach, hold a quarter of it, they merge into it.
- **Four alike.** The newest window of 4 adjacent spans whose largest holds
  at most the other three combined.
- **Stragglers.** A span smaller than its newer neighbour (left behind when
  an endpoint went away) joins the shortest window around it that satisfies
  the guard and the read rule. Without this rule, a stalled pass plus hourly readers at
  100M left 282 spans on average, 714 at most (replayed).
- **Stale versions.** A span whose dead versions reach a quarter of it is
  rewritten alone.

Upkeep has two lanes, each one merge at a time: merges into the base, and
the rest. Their inputs never overlap. With one lane, a 100M base merge
(~60 commits of upkeep at 2M entries per commit) let 70–117 spans pile up
behind it (replayed); with two, at most 18–24.

## How each operation runs

### 1. append(changes)

Resolve each written key against the spans newest first: key filters, then
a block read for every key a filter matches (`KeyIndex._find`, as today
across levels); within a span, a key's first entry is its newest. That
gives each key's current version exactly: its predecessor (the replaced
version: the object to clean up) and whether it was live (the batch class,
the live count). Write the span `[c, c]`, one PUT, and add
`added − removed` to the count.

Measured (`layouts.py`): one trace of 30,000 commits of 1K keys (20,000 at
1M) applied to both layouts, today's leveled index by its real planner and
compaction, the spans by the policy (real `merge_ranges` merges, the base
merge through `KeyIndex.compact`), one consumer reading every commit; then
1K keys drawn from the whole key space; cold is 30 ms per request, 80 MB/s
per connection, 64 in parallel; warm is the engine cache filled
(`EngineCache`):

| | 1M: leveled | 1M: spans | 100M: leveled | 100M: spans |
|---|---|---|---|---|
| written per entry committed | 17.2 | 8.0 | 50.3 | 13.0 |
| sorted runs · files · entries per live key (at the end) | 3 · 3 · 1.03 | 3 · 3 · 1.10 | 5 · 11 · 1.01 | 9 · 14 · 1.02 |
| lookup 1K, exact, cold | 3 GETs, 10 MB, 0.21 s | 3 GETs, 11 MB, 0.29 s | 907 GETs, 200 MB, 1.13 s | 901 GETs, 207 MB, 1.20 s |
| append 1K updates, exact, cold | 3 GETs, 1 PUT, 0.24 s | 3 GETs, 1 PUT, 0.31 s | 58 GETs, 409 MB, 5.9 s (streamed) | 65 GETs, 417 MB, 7.7 s (streamed) |
| page 100K, mid-index, cold | 4 GETs, 0.9 MB, 0.10 s | 4 GETs, 2.0 MB, 0.14 s | 9 GETs, 1.6 MB, 0.09 s | 13 GETs, 4.8 MB, 0.11 s |
| lookup 1K, warm | 0 GETs, 0.01 s | 0 GETs, 0.01 s | 0 GETs, 0.01 s | 0 GETs, 0.01 s |
| page 100K, warm | 0 GETs, 0.01 s | 0 GETs, 0.02 s | 0 GETs, 0.01 s | 0 GETs, 0.01 s |

The 100M snapshot came just after the spans' base merge (writes 9.8× before
it, 13.0× after); on average the replay holds 1.12–1.16 entries per live
key for spans, so filters and blocks cost ~10–15% more between base
merges. The warm append is the engine's resolver (`resolved-commits.md`):
the same local reads plus a PUT. Today's leveled planner writes 50× on real
v3 entries at 100M, above `amplification.py`'s 30× (which assumed 29-byte
entries): byte-sized level targets hold 3× more entries than it modelled.
`tiling_reads.py`'s hand-built layouts, quoted by the first version, are
superseded by these.

### 2. lookup(keys, at = c)

At the head: the first entry of each key, newest span first. At an older
`c`, only for a reserved endpoint (`c + 1` reserved) or for an attempt
reading the manifest it was handed while its pin keeps those files: per
key, the newest version older than commit `c + 1`'s generation.

### 3. scan(at = c, range or pattern)

Merge the spans in key order, page by page with a key cursor
(`KeyIndex._scan`, which merges sorted runs newest first and fetches only
the blocks a page needs). At the head, each key's newest version; at a
reserved `c`, its newest version older than commit `c + 1`'s generation.
Prefix globs are key-range seeks; other patterns filter the stream. A count
is a scan.

**Stable while commits arrive.** Every merge keeps the versions `c + 1`
sees, so the view at `c` is the same from any spans that exist later. A
pass that pages over hours reads each page from the spans current when the
page runs. Its index files need no pin for the whole pass; each attempt
pins the manifest it was handed, for its own lifetime. The pass's data
versions are another matter: it keeps its original data pin (see
lifecycles).

### 4. changes(P → N, keys or range, per-key lower bounds)

`P` and `N + 1` are reserved endpoints, with generations `g(P)` and
`g(N + 1)` (the first generation of each commit; generations rise with
commit numbers within an output partition). Read the spans overlapping
`[P, N]`, and clip every key to that range:

- its versions **in the range** are those with `g(P) ≤ generation <
  g(N + 1)`. A key with none did not change in `[P, N]` and is skipped,
  including a key first touched after `N`;
- its state **at `N`** is its newest version older than `g(N + 1)`: a later
  span can extend past `N`, and its newer versions are ignored;
- its state **before `P`** is its newest version older than `g(P)`, or, if
  it has none in these spans, the predecessor on its oldest version.

The same clipping applies to the "did it change" test, to staleness, and
to every resumed page. Example (A12-2): `k` added at 1, removed at 2, and a
span `[1, 2]` holding both versions. `changes(1, 1)` sees only the version
at 1 and returns added; taking the span's newest version would return
neither.

| Live before P | Live at N | Class |
|---|---|---|
| no | yes | added |
| yes | yes | updated |
| yes | no | removed |
| no | no | neither: not delivered |

- **keys=**: point lookups in those spans. **Range or prefix**: a seek in
  each span.
- **Staleness** runs the same merge and stops at the first key that is
  delivered and not covered by the read-ahead. Transitive staleness asks the
  same of the upstream.
- **Paged and resumable** by key cursor: every page reads a tiling of
  `[P, N]` that keeps the versions `P` and `N + 1` see, whatever merges ran
  in between.

**Read-ahead.** An entry `[r, run, attempt]` says a keys= selection read
some keys as of upstream commit `r`. Classing such a key needs its state at
`r`, which neither the key, `r`, nor its newest version can tell once the
history in between is merged. If `k` is live now at a newer generation, it
is added if the consumer was given `k` as removed at `r`, and updated if it
was given `k` live. So:

- the **delivered state comes from the attempt's sealed result**
  (`state.attempt_result`), read with its spec (`engine._read_ahead` reads
  only the spec today): the keys it delivered as upserted, and as removed. A
  key the selection named but did not deliver (unchanged past `next`) is not
  part of the entry;
- per key, the **latest read wins**: the entry with the newest `r` that
  delivered it;
- the key is skipped if its newest version is no newer than commit `r`'s
  generation (generations rise with commit numbers within an output
  partition); otherwise its class is (its delivered state, its state at
  `N`);
- entries before `next` collapse into the snapshot, as today
  (`positions.collapse`). A covering selection collapses the record to its
  head + 1, its reserved landing point.

Spans keep added-then-removed tombstones outside the base for this rule.

**Measured catch-up** (`catchup.py`: 12,000 commits of 1K keys, four
consumers 1, 100, 360 and 10,000 behind; spans built by the policy on real
files with `merge_ranges`, blocking at the four positions; paged 100K keys
at a time through `KeyIndex._scan`; every layout's classes checked against
the per-commit merge; 30 ms per request, 80 MB/s per connection, 64 in
parallel):

| Behind | Spans | Aligned blocks (range tree, fanout 8) | Packed deltas, one merge |
|---|---|---|---|
| 1 | 1 GET, 10 KB, 30 ms | same | same |
| 100 | 4 GETs, 0.9 MB, 0.06 s | 9 GETs, 0.9 MB, 0.07 s | 1 GET, 1.0 MB, 0.08 s |
| 360 | 9 GETs, 3.3 MB, 0.18 s | 10 GETs, 3.4 MB, 0.17 s | 1 GET, 3.5 MB, 0.24 s |
| 10,000, 1M keys | 13 spans: 27 GETs, 14 MB, 1.2 s | 25 blocks: 159 GETs, 41 MB, 2.2 s | 7 GETs, 101 MB, 11.7 s |
| 10,000, 100M keys | 16 spans: 259 GETs, 65 MB, 7.7 s | 25 blocks: 965 GETs, 68 MB, 8.1 s | 6 GETs, 97 MB, 12.9 s |

The 100M catch-up is dominated by paging: ~85 pages of 100K keys, each
fetching a block range from each span. Building the spans took 6.2× (1M)
and 8.7× (100M) compaction entry writes per entry committed past the oldest
consumer; the aligned blocks 3.5× and 4.4×, on top of whatever the LSM
writes. Limits of this measurement (the second review): the spans do not
cross the consumers' positions; the classes were checked as four aggregate
counts, not key by key; the packed column merges the whole object at once
rather than paging a `changes()`; the increases in the process's high-water
RSS it printed (+20 MB for spans, +419–494 MB for packed) are not
per-reader peak memory and are not compared here. The v4 prototype measures
the same API key by key, in isolated reader processes.

## Resource limits

Merging across endpoints trades two costs. Spans that share one sorted file
cannot skip by time inside it, so an old pass may read data outside its
range (A12-3). Spans kept apart per endpoint read exactly, but their number
grows with endpoints, and large pinned commits leave nothing a guarded,
read-respecting merge may join (A12-4). No layout avoids both, so the
design states limits and what happens at each (decision D64, for Erwin):

| Resource | Limit | At the limit |
|---|---|---|
| Entries written | (1 + 43R + 20R ⌊log₂ K⌋) × committed, R = 3 | proved for every merge the policy makes; after 3 failed attempts on an input set, merging stops for that index and an alarm is raised |
| Spans per index (scan fan-in, filters probed, cold tail GETs) | 32 | forced guarded merges across endpoints; readers inside pay extra reads |
| A reader's read | a full read of the index | structural: a catch-up reads a subset of the spans a full read reads |
| Decoded bytes per reader | one block range per span per page, plus a page of at most 16 MB | 32 spans × 2 × 64 KB + the page: about 20 MB |
| Pending maintenance | queued merge input ≤ 2× the index's bytes | commits to the partition wait (writer backpressure) until upkeep catches up |
| Endpoints | no count limit; measured up to 1,000 distinct | their cost is retained versions (the physical budget) and, under the span cap, reads |
| Retained bytes | the physical budget over five lifetimes (below) | backpressure, then cancel-restart of the oldest protected reader |

Nothing here relaxes the guard to regain compaction progress.

Replayed (`spans.py`; commits of 1K keys, 90% updates, 5% removes, 5%
adds; the second half of 40,000–200,000 commits measured; consumers read
every `period` commits, at one commit per 10 s 360 is hourly and 8,640
daily). Each cell: compaction entry writes per entry committed · spans,
mean (max) · entries per live key. `versions` is the first revision
(merges cross endpoints under a one-ended read rule, no span cap);
`blocked` the reviewed original, with the same triggers and guard. The
final policy (`capped`: two-ended read rule, span cap 32) follows the
table:

| Observers | 1M keys, versions | 1M, blocked | 100M keys, versions | 100M, blocked |
|---|---|---|---|---|
| 1 daily (+ one every commit) | 8.4 · 5.7 (10) · 2.28 | 6.3 · 7.2 (12) · 2.48 | 12.6 · 8.9 (18) · 1.16 | 13.1 · 16 (32) · 1.12 |
| 10 daily, spread over the day | 7.4 · 12 (17) · 7.42 | 6.8 · 33 (38) · 7.20 | 13.8 · 17 (28) · 1.22 | 15.5 · 41 (52) · 1.22 |
| 100 daily, spread | 7.6 · 18 (23) · 9.56 | 7.4 · 543 (548) · 9.59 | 14.4 · 21 (29) · 1.24 | 15.4 · 546 (558) · 1.23 |
| 1,000 daily, spread | 7.9 · 19 (24) · 9.92 | not run (≥ 1,000 spans) | 13.6 · 22 (30) · 1.24 | not run |
| an endpoint at every commit (1,000 readers, period 1,000) | 9.4 · 7.3 (12) · 2.37 | ≥ 1,000 spans | 13.9 · 10 (17) · 1.13 | ≥ 1,000 spans |
| a full pass stalled 20,000 commits, + hourly | 8.0 · 6.0 (10) · 2.59 | 7.2 · 12 (17) · 3.06 | 12.5 · 8.9 (17) · 1.16 | 13.6 · 16 (28) · 1.17 |
| 60 hourly readers, staggered | 7.4 · 6.5 (11) · 1.65 | 9.2 · 183 (188) · 1.49 | 13.0 · 9.4 (18) · 1.13 | 14.8 · 187 (196) · 1.12 |
| temporary keys (half of each commit, removed 100 commits later), hourly + daily | 9.5 · 6.4 (12) · 7.56 | 9.5 · 13 (19) · 10.5 | 12.6 · 8.6 (15) · 1.13 | 13.1 · 13 (21) · 1.13 |
| a 1M-key commit after 1K ones, hourly + daily | 7.3 · 6.0 (14) · 2.35 | 6.9 · 11 (18) · 2.55 | 12.4 · 8.8 (18) · 1.15 | 13.2 · 16 (28) · 1.15 |

Each cell is the worse of instant upkeep and budgeted upkeep (one merge per
lane at 2M entries per commit, about one core); they differ by at most 4
spans at the peak.

The final policy, `capped`, on the same scenarios (replayed; worse of
instant and budgeted upkeep; each cell: compaction entry writes · spans,
mean (max) · entries per live key · the slowest reader's read per read, ×
what changed in its range):

| Observers | 1M keys | 100M keys |
|---|---|---|
| 1 daily | 7.0 · 6.6 (12) · 2.84 · 2.97 | 12.6 · 9.9 (18) · 1.16 · 1.08 |
| 10 daily, spread | 7.4 · 12 (17) · 7.42 · 5.89 | 13.6 · 18 (28) · 1.22 · 1.11 |
| 100 daily, spread | 7.6 · 18 (23) · 9.56 · 6.98 | 14.4 · 21 (29) · 1.24 · 1.07 |
| 1,000 daily, spread | 7.9 · 19 (24) · 9.92 · 7.09 | 13.6 · 22 (30) · 1.24 · 1.08 |
| an endpoint at every commit | 9.4 · 7.3 (12) · 2.37 · 2.13 | 13.9 · 10 (17) · 1.13 · 1.40 |
| stalled full pass, + hourly | 6.6 · 7.4 (13) · 3.80 · (stalled) | 13.1 · 9.8 (21) · 1.17 · 1.06 |
| 60 staggered hourly | 7.4 · 6.5 (11) · 1.65 · 2.02 | 13.0 · 9.4 (18) · 1.13 · 1.82 |
| temporary keys, hourly + daily | 7.7 · 7.5 (14) · 6.49 · 1.25 | 12.9 · 10 (18) · 1.13 · 1.03 |
| a 1M-key commit after 1K ones | 6.6 · 7.1 (16) · 3.04 · 2.75 | 12.9 · 9.7 (21) · 1.15 · 1.04 |

And the second review's two constructions (`adversarial.py`, replayed):

| Case | `versions` | `capped` |
|---|---|---|
| A12-3: base, then spans of 1, 1M and 1M entries; a pass at `[0, 0]` | merged: the pass reads 1,990,001 entries for its 1 | refused: it reads 1 |
| A12-4: base, then forty 2M-entry commits, an endpoint at each | 41 spans, no merge planned | 32 spans (36M entries written); a reader of one commit reads up to 2× its 2M |

- **Compaction entry writes stay at 7–10× (1M) and 12–14× (100M)** under
  every pattern (add 1 for total entry writes), far inside the proved bound.
- **Spans stay under the cap without forcing** in every replayed pattern;
  only A12-4's construction reaches it.
- **What observers cost is storage**, and at small sizes reads. Each
  endpoint keeps the keys changed between it and the next: free at 100M
  (1.13–1.24 entries per live key), 7–10× at 1M keys with a reader every 15
  minutes of a 10-second source or heavy temporary-key churn. There a daily
  reader reads 6–7× what changed, still less than a full read; the
  per-position budget turns such readers into full passes. The tables run
  without budgets, to show the cost.
- **The two-ended read rule costs little at 100M** and some at 1M: the 1M
  daily reader reads 3.0× what changed against 2.0× under `versions`.

## Retention: each lifetime on its own

| Lifetime | Keeps | Ends |
|---|---|---|
| endpoint reservations | versions in spans | its holder releases it |
| index pins | an attempt's manifest: files a merge has since replaced | the attempt settles |
| data pins | replaced store objects (immutable stores) | the oldest reader pinned before their replacement settles; a pass keeps its original data pin across attempts |
| cleanup-delta retention | a commit's delta file, which lists the objects it replaced | its cleanup is acknowledged (`cleanup_reads`) |
| conditional publication | a merge's uploaded output | published through the journal, or reclaimed if abandoned |

Replayed (`retention.py`, 100M keys, the versions policy with budgeted
upkeep, hourly and daily readers; per commit an attempt pinning its
manifest for 6 commits, one in 1,000 for 600; cleanups done 6 commits
later, one in 1,000 stuck for 2,000; one merge in 20 uploaded but never
published, reclaimed 360 commits later; 10 B per entry):

| Lifetime | Mean | Peak |
|---|---|---|
| current spans | 1,141 MB | 1,278 MB |
| replaced files kept by attempt pins | 4 MB | 1,279 MB |
| deltas kept for cleanups | 0.1 MB | 0.1 MB |
| unpublished or abandoned merge outputs | 6 MB | 1,000 MB |
| replaced data objects kept by pins | 170K | 585K |

At 1M keys: 24 MB of spans; 5 MB (peak 32) pinned; 1 MB (peak 6)
unpublished. The means are small. The peaks are single events that each
cost about one more copy of the index: a slow attempt pinned across a base
merge keeps the old base, and an abandoned base merge holds its output
until it is reclaimed. The physical budget must leave room for one of
each, or reclaim abandoned outputs sooner.

**Budgets**, in bytes and requests rather than entries:

- **Per position: incremental or full.** When a position's catch-up would
  read more bytes than a full read of the index, or its retained versions
  pass the per-position budget, the position is dropped and its next run
  does a full pass. The consumer pays a rebuild, which the engine weighs
  from the asset's last full run where it has one. A position with a pass
  under way is not dropped by this rule.
- **The physical budget** covers all five lifetimes. Past it, the engine
  applies backpressure (new attempts on the partition wait), then cancels
  and restarts the oldest protected reader (a stalled pass), releasing its
  endpoint and pins. It never deletes what a holder still needs.

## Lifecycles the implementation must honour

- **Passes.** A pass between attempts keeps its endpoints and its original
  data-version pin; each attempt pins the index manifest it was handed,
  separately. Index-file retention and data-object retention are distinct.
- **Merges.** A merge records its input spans (identities, digests) and the
  index's life. Publishing re-checks both against the current state; a
  retried or duplicate merge whose inputs are gone, or which belongs to an
  earlier life, is discarded. Outputs have names unique to the merge.
- **Crash after upload, before publication.** Readers keep using the old
  state. Abandoned outputs (files under the index prefix that no state and
  no merge in progress references) are found and reclaimed.
- **Publication and deletion.** A merge is published through the journal.
  Its inputs are deleted only once the publication is durable and no pin
  older than it remains.
- **Reset, store move, remove and recreate.** A new index life fences old
  claims and merge jobs: their results are refused. Nothing merges across a
  reset by commit numbers.
- **Rename** follows the index's identity (its prefix), as today.
- **Pattern change.** Reserve split + 1. The membership diff (which keys
  changed match) stays separate from ordinary classification.
- **Empty commits, empty base.** An empty delta is still a span, so
  coverage has no holes. A read that finds a hole fails loudly.
- **Read-ahead results outlive their entries.** A sealed attempt result
  referenced by a read-ahead entry is kept until the entry collapses, as
  run retention keeps named runs today (`upkeep.py:307-309`); manual
  pruning refuses it, or drops the entry and makes the next pass deliver
  its keys again. A missing result is never reconstructed silently.
- **Orphans and pins.** The orphan collector's roots are every file of the
  current state, of every manifest a pin holds, of pending cleanups, and
  of merges in progress. A reader acquires a manifest and its pin
  atomically.
- **No inferred history.** Every index must have been written exactly from
  its first commit. There are no deployments, so a fresh index is a
  precondition, not a migration.
- **The count invariant** compares against the consumer's effective
  baseline: live(P − 1) adjusted by what its read-ahead entries delivered.

## What changes, and what is deleted

Deleted:

| Today | Why it goes |
|---|---|
| Levels and the leveled planner (`FileInfo.level`, `l0_max_files`, `level_base`, `fanout`) | spans and the policy above |
| The delta log as a second list (`IndexState.log`, `keep_log`, `_consumed`, `covers`, `slice`, `truncated`, `IndexTruncated`, the truncation in `Upkeep.truncate`) | the spans are the log; trimming is the base merge |
| The range tree (proposed) | spans give catch-up its few files, with fewer GETs (measured) |
| The inexact count and the recount (`inexact`, `count_exact`, `Delta.exact`, `DeltaFiles.exact`, recount, `recount_interval`, `Known::Other`, `Old::Other`, `Sparse.inferred`, `resolve(exact=)`) | writes are exact |
| The tombstone Bloom filter | under exact writes, a key-filter hit reads the block anyway |
| `.kg` garbage files (`Merge.compact(garbage=)`, `GarbageFile`, `decode_garbage`, the `sidecar` cleanup kind, `native/src/garbage.rs`) | exact deltas list every replaced version |
| `pending`'s per-page restart of a merge over thousands of deltas | a catch-up merges a handful of spans |
| Per-key consumer payloads in the index (K47) | consumer records are a position plus read-ahead entries |

Changed:

- `.kx` format v4: one filter; a key may repeat in a file, newest first,
  one entry per segment, the predecessor on the oldest.
- `IndexState`: `count`, `spans: [(a, b, segment starts, files)]`,
  `prefix`.
- One native merge (versions kept per live endpoint, the oldest
  predecessor kept, the base dropping what means absent) replaces the
  compaction merge and the range merge.
- Readers (`_find`, `_scan`, `pending`) take spans and an optional endpoint
  generation; `pending(P, N)` reports each key's state before `P` beside its
  newest entry.
- Upkeep: the policy and two lanes above, planned against endpoints derived
  from positions, claims, passes and drains.
- The read-ahead reads attempt results as well as specs.

Kept: blocks, the key filter, payloads for source versions and failure
records, the engine cache, engine-served reads, `DeltaKeys`, cleanups driven
by delta predecessors, pins.

New words for the glossary: **span**, **endpoint**, **base** ("run" was
taken). "Delta" stays: a commit's span.

## Alternatives, compared

- **Blocking endpoints** (the reviewed version: a merge never crosses an
  endpoint). One version per key per span, a simpler merge. But the span
  count grows with distinct endpoints (543–546 spans at 100 daily readers,
replayed), and each
  retirement forced a rewrite of the spans after it (the review's 265×
  case). Versions cost a format change and a version-aware reader.
- **Today's LSM plus a packed, resumable log.** Lookups keep the leveled
  layout; catch-up reads one packed object in a few range GETs and merges
  every delta once, holding a cursor per delta. Measured 10,000 behind: 6–7
  GETs, but 97–101 MB and 11.7–12.9 s for the merge, against 13–16 spans,
  14–65 MB and 1.2–7.7 s. It keeps today's leveled compaction (51.3× total
  entry writes at 100M, measured) and two structures. It stays the control
  for the v4 measurements, with a paged, resumable merge.
- **Aligned tiers** (the range tree). Measured on the same log: 25 blocks
  and 159–965 GETs for 10,000 behind, against 27–259 for spans, at similar
  bytes, and 3.5–4.4× written on top of the LSM's compaction.
- **Presence bits instead of versions** at inner endpoints: enough for a
  consumer's classes, not for a pass pinned at a middle endpoint, which
  reads the versions there.
- **A key-sorted multiversion LSM without time order.** A catch-up from a
  day back reads about the whole index (every bottom block holds a key
  changed since), ~0.7 GB at 100M against 65 MB. The read bound is what
  keeps the spans' time locality.
- **A persistent or prolly tree** (Dolt). Random writes rewrite one leaf
  per changed key (4–64 MB per 1K-key commit at 100M); a day-behind diff
  reads both trees, since nearly every leaf differs (0.914^400 ≈ e^−36
  untouched).
- **Pinned snapshots per position, and blind writes resolved later.** Each
  moves the exact lookup from the writer, who pays once, to every consumer
  pass, or leaves the count and the cleanups unresolved.

## A worked example

`items` holds `a` and `b`, written by commit 0 at generation 1: the base.
Commit `c` writes at generation `10c` after that.

| Commit | Change | Delta (span `[c, c]`) |
|---|---|---|
| 1 | add c | c g10 |
| 2 | remove a | a tombstone g20, pred 1 |
| 3 | re-add a, update b | a g30 (no pred); b g30, pred 1 |
| 4 | add d, update c | d g40; c g40, pred 10 |
| 5 | remove d | d tombstone g50, pred 40 |

Two consumers keep `tally = ctx.load() + len(added) − len(removed)`.

- **X** sits at position 1, with tally 2 (`a`, `b`).
- **Y** read through commit 2 and sits at position 3, with tally 2 (`b`,
  `c`). A keys= selection then read `d` at head 4; its sealed result says `d`
  was delivered upserted. Y's record is position 3 plus `[4, run,
  attempt]`; its bound for `d` is commit 4's generation, 40. Its tally is 3.

The endpoints are 1 (X) and 3 (Y). Suppose upkeep merges all five deltas
into one span `[1, 5]` (the read bound allows it: everything is tiny).
Endpoint 3 is live inside it, so the span has two segments, `[1, 2]` and
`[3, 5]`, and keeps the version endpoint 3 sees for each key that changed
on both sides:

```
base  [0, 0]   a g1 · b g1
span  [1, 5]   a: g30 | tombstone g20, pred 1     (newest | seen by 3)
               b: g30, pred 1
               c: g40 | g10                        (newest | seen by 3, no pred)
               d: tombstone g50, no pred
```

- **Lookup at the head:** each key's first entry: `a` g30, `b` g30, `c` g40
  live, `d` gone. The count is 3.
- **X, `changes(1 → 5)`:** the state before 1 is each key's oldest
  predecessor. `a` was live (pred 1) and is live: updated. `b`: updated. `c`
  names none: added. `d` names none and is gone: neither. Tally 2 + 1 = 3.
- **Y, `changes(3 → 5)`:** the state before 3 is each key's newest version
  older than commit 3. `a`: the tombstone g20, absent; live now: added. `b`
  has no version before 3, so its predecessor: live; updated. `c`: g10,
  live: updated. `d` is in the read-ahead: its newest generation, 50, is
  past the bound 40, so it changed after Y read it; it was delivered
  upserted and is gone: removed. Tally 3 + 1 − 1 = 3: `a`, `b`, `c`.

Had the merge dropped the version endpoint 3 sees for `a`, Y would read
`a`'s oldest predecessor (live) and class it updated, though Y never had it.
Had it dropped `d` as added-and-removed, Y would keep `d` forever. Once Y
moves past 5, endpoint 3 retires: the next merge touching `[1, 5]`
coalesces its segments (`a: g30, pred 1`, `c: g40`), or the span is
rewritten alone once such dead versions reach a quarter of it. When X moves
on too, the span joins the base: `a g30 · b g30 · c g40`, with `d`'s
tombstone and the predecessors gone. The replaced versions (`a` g1, `b` g1,
`c` g10, `d` g40) were cleaned up from the deltas of commits 2–5.

## What must be checked

**Lean** (proposed by the coordinator):

- done (5a34e6e): the write bound, every merge guarded or dropping a
  quarter of its inputs (the single-span rewrite), for any order of
  commits, endpoint births and retirements, outputs of any size up to the
  inputs' total, R attempts per merge;
- done (5a34e6e, `Segments.decode_encode`): a key's kept versions in a
  span, decoded into per-segment (newest, predecessor) pairs, equal the
  model's segments; so the tiling, catch-up and read-ahead results hold at
  segment level. The implementation's check: a reader at `P` gets each
  key's state before `P` as its newest kept version older than `P`'s
  generation, else the span's predecessor;
- done (`Tiling.lean`, `catch_up`, `lookup_head`): merges that respect
  boundaries keep every catch-up range exactly tiled, and its spans merge
  to the per-commit fold; a lookup at the head equals the live state. With
  the round trip above, this holds for spans with versions, read segment by
  segment;
- done (`Keys.lean`, `read_ahead`, `read_ahead_spans`): with generations
  increasing in commit order and the delivered state known, the class
  equals the presence change from `r` to `N`; and dropping
  added-then-removed tombstones breaks it (`drop_breaks_read_ahead`, the
  worked example's key `d`).

All on branch `bb/experiment-bend-2-for-the-key-index-s-delta-alge-thr_dqc6iaviun`,
standard axioms only (`experiments/lean/REPORT.md`). What they leave to
TLA+ and the sim: where the delivered state comes from (the sealed
result), endpoint reservation lifetimes, pins, publication and crashes.

**TLA+** (`KeyIndex.tla`): spans with segments; endpoint holders with the
reservation lifetimes above, a selection's landing point included; pins;
two upkeep lanes; publication through the journal; a crash after upload; a
reset fencing old merges. Invariants: reads agree with the full history
(`Execution.tla`'s log) at every reserved endpoint; no file deleted while a
pin or a pending cleanup needs it; no published merge from an old life or
with changed inputs; the count against the effective baseline. Calibrations,
each must fail with its fix off: a selection without a reserved landing
point (its position lands inside a merged span); a merge dropping a version
a live endpoint sees; tombstones dropped outside the base; deletion before
durable publication.

**The sim** (`tests/sim`): exact deltas through every writer, in the cold
mode (resolver off, `bits_per_item=1`, `whole_threshold=0`,
`small_file=0`); compaction interleaved anywhere, with endpoints derived
from the model and every reader checked against the oracle; the tally with
read-ahead entries read from results, batches split anywhere, positions
dropped; cleanups exactly once and never under a pin; injected upload and
publication failures; spans and written bytes reported against the bounds.

## The review, finding by finding

| Finding | Answer |
|---|---|
| P1-1 landing points | every attempt that may advance a position reserves the head + 1 at its claim (selections, a pass's first attempt, drains), kept until durable transfer, released on failure; a TLA+ calibration |
| P1-2 read-ahead state | delivered state from the sealed attempt result; latest read wins; tombstones kept |
| P1-3 retirement rewrites | merges cross endpoints, so retirement forces nothing; the guard bounds writes in any order (Lean, 5a34e6e); replayed under the review's churn scenarios |
| P1-4 span count, memory | spans set by the policy, not by observers; a bound per resource; versions, not bits |
| P2-1 retention | five lifetimes accounted separately; byte budgets with backpressure and cancel-restart |
| P2-2 mixed models | numbers labelled; catch-up measured on real spans against packed deltas and aligned blocks; both layouts built from one trace by real compactions |
| P2-3 cap exemption at 0 | removed (`tiling.py`); only positions with a pass under way are exempt, and the physical budget covers them |

The second review (A12), on a7fc05f:

| Finding | Answer |
|---|---|
| A12-1 base normalization | only the base's initial segment, before the oldest live endpoint inside it, keeps live keys only; later segments keep tombstones |
| A12-2 clip changes at `N` | every key clipped to `[g(P), g(N + 1))`: the "changed" test, the state at `N`, staleness and resumed pages |
| A12-3 an old pass facing newer data | the read rule checks both ends, in bytes, and is a preference; the universal 2×/10 MB bound is withdrawn; a reader's read is bounded by a full read (replayed: the merge is refused, the pass reads 1 entry, not 1,990,001) |
| A12-4 many large pinned commits | a hard cap of 32 spans, by forced guarded merges; readers inside pay up to 2× (replayed: 41 spans become 32); explicit limits and backpressure in the resource table |
| A12-5 the theorem | entries, not bytes: (1 + 43R + 20R ⌊log₂ K⌋), deltas included; single-span rewrites covered (5a34e6e); every attempt charged, R = 3 |
| A12-6 amplification labels | compaction entry writes and total entry writes labelled; 14.0× against 51.3× total at 100M; bytes with the v4 measurements |
| A12-7 RSS | relabelled as process high-water increases; the memory comparison is dropped; isolated reader processes in the v4 measurements |
| key-by-key checks; sealed result retention | the v4 prototype compares every key and class; results kept until their entries collapse |

## Open questions

- λ = 1 and Z = 10 MB for the read rule, 32 for the span cap, 4 for the
  guard and the window, R = 3, the backlog limit at 2× the index: chosen on
  the replay; the v4 measurements re-check them.
- The physical budget's default, and whether cancel-restart of a stalled
  pass needs the user's consent.
- Format v4 lets a key repeat within a file; the reference implementation
  (`tests/sdk/keys_reference.py`) changes with it.
