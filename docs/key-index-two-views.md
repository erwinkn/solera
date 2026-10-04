# Key index: two views, key-ordered and time-ordered

Status: **design study** (A21, T23), for Erwin. No product code. The question:
instead of one structure serving every read (spans,
[`key-index-design.md`](key-index-design.md)), keep two, each specialised for one
kind of read, both cheap to write. Does that beat spans for Solera's priorities
(latency first, then requests and cost, then storage), and is it worth proper
experiments?

**Verdict: no-go for experiments on replacing spans.** The two views are sound,
and simpler where spans are subtle, but they don't buy latency where it
matters. Three ideas from the study are worth taking anyway (below).

Every number says where it comes from: **measured** (cited from the span
design's v4 prototype runs, or the micro-measurement here), **replayed** (the
merge policies run on metadata with `spans.py`'s density model, in
[`bench/keys/twoviews/model.py`](../bench/keys/twoviews/model.py)) or
**checked** (an exhaustive-ish algebra check against the per-commit fold,
[`bench/keys/twoviews/check.py`](../bench/keys/twoviews/check.py)). Request,
latency and dollar figures are **estimates** on a stated model, calibrated
against the measured spans and leveled numbers (§ Numbers).

## The answer in brief

- **Round trips are the same in every design.** A cold read is "fetch each
  file's tail (block index, filters), then fetch the blocks": 2 sequential
  round trips, or 1 when the files are small enough to read whole. Spans, spans
  with the cheap levers, and two views all sit there. One metadata object
  saves requests, not round trips: it is still fetched before the blocks.
- **The key view wins cold head reads only where spans hold many versions**:
  small indexes with many lagging readers. At 1M keys with 10 daily readers,
  spans hold ~7.6 entries per live key; 1K cold exact lookups take ~0.55 s
  against ~0.11 s (5×), a 100K-key page 0.13 s against 0.08 s. At 100M keys
  spans hold ~1.2 entries per live key and the two are within ~15% (1.4 s
  against 1.2 s for 1K cold lookups, the same 0.09 s page).
- **Catch-up is a tie on latency.** The first page takes 2 round trips
  (~0.07–0.1 s) everywhere. The time view reads only what lies in the range
  (1.00–1.03× what changed at 100M, 1.1–3.4× at 1M) against spans' 1.1–2.0×
  and 2.0–7.2×, which shortens full catch-ups at 1M a little and changes
  nothing at 100M.
- **Two views write more and store more**: ~1.5–1.8× spans' background entry
  writes (K's merges plus T's levels), and the time view's retained nodes plus a
  day of raw deltas. In dollars it's a wash: on S3 the bill is the commit PUT
  and, for a cold writer, its lookups; on Railway it's storage, under 3 cents a
  month at 100M in every design.
- **The engine's warm cache answers lookups and reads ahead in ~10 ms in every
  design.** The cold path these views improve is the one Solera already avoids
  in steady state.
- **It is simpler where spans are subtle, but there are two of it.** No merge
  ever looks at an endpoint; no key has several versions in a file; no read
  rule, span cap or forced merge. Five of A17's ten findings cannot happen. But
  there are two background pipelines, two retention rules over shared files,
  and two backlogs to admit writers against, and spans are built, measured,
  proved in Lean and being fixed.

Worth taking anyway:

1. **The digest check, for spans.** The writer already knows every key's old
   and new state; it can keep one more number per commit, the sum of a hash of
   every live key's (key, generation). Any merged file covering commits
   `[a, b]` then satisfies Σ h(after) − Σ h(before) = D(b) − D(a − 1). Checked
   before publication, a merge bug (a key's wrong version kept, or a key dropped)
   becomes a refused merge, not a corrupted index. Measured: 1.75 ns per entry
   natively, ~0.2 s for a 100M-entry merge, against ~220 ns per entry to decode.
2. **Eager merging hurts spans' catch-up.** Forcing spans down to 6 makes
   merges cross endpoints, so spans keep more versions and readers read out of
   range: the hourly reader at 100M reads 8.0× what changed instead of 2.0×,
   and background writes go from 14× to 20× (replayed). T22 should report the
   read ratio beside the span count.
3. **Keep raw deltas back to the oldest endpoint** (storage is cheap: ~90 MB a
   day at 1K keys every 10 s). Then a missed reservation (A17's R5) costs a
   slower catch-up, not a forced full pass. This works for spans as is.

What would flip the verdict: production indexes that are small to medium
(≤ ~10M keys) with tens of lagging readers *and* cold writers, or spans'
version machinery producing another round of P1s after A17's fixes. The time
view below is then the design to prototype, and § Go/no-go has the plan.

## The design

### One log, two views derived from it

Today a commit writes one **delta**: a key-sorted `.kx` file holding, per key it
wrote, the new state and the state it replaced (its predecessor generation,
whether it was live, its payload on a source). That stays exactly as is.

- **The key view (K)** answers "what is each key's state now, or at a pinned
  commit?": a single-version LSM. Its newest runs *are* the deltas, referenced
  in place. Merges keep each key's newest entry and nothing else; a tombstone
  disappears once nothing older is under it. Runs tile commit time, as spans
  do (the same policy, with the head as the only endpoint), so each merge
  output covers a commit range and can be checked by the digest.
- **The time view (T)** answers "what changed between P and N?": **nodes** over
  aligned commit ranges. A node at level `j` covers `[i·bʲ, (i+1)·bʲ − 1]` and
  holds, for each key touched in it, its state before the range and its state
  after. Level 0 is the deltas themselves. When a range completes, its node is
  built by merging its `b` children: before from the oldest, after from the
  newest. A key absent before and absent after (added then removed inside) is
  dropped.

A commit is still one PUT. The journal record that commits a delta also makes
it the newest run of K and the newest level-0 node of T, so **both views
always cover the head**: the brief's "watermark" is always the head, and the
background work (K's merges, T's node builds) only cuts how many files a read
opens. Nothing is ever written twice by the writer; there is no dual write to
fall out of step.

The merge rule for T is associative (proved for spans' one-segment merge in
`LAWS.bend`; checked here over 539,495 ranges), so any cover of `[P, N]` by
adjacent nodes gives the same answer. Fanout `b` trades writes for fan-in
(replayed, § Numbers): `b = 4` writes each entry ~5× at 1M keys and ~8× at
100M up to 4⁹ = 262,144 commits per node (a month at one commit per 10 s),
and a daily reader's cover is ~19 nodes.

### The example

The span design's example, so the two can be read side by side. `items` holds
`a` and `b`, written by commit 0 at generation 1. Commit `c` writes at
generation `10c`.

| Commit | Change | Delta (before → after) |
|---|---|---|
| 1 | add c | c: absent → g10 |
| 2 | remove a | a: g1 → absent (g20) |
| 3 | re-add a, update b | a: absent → g30; b: g1 → g30 |
| 4 | add d, update c | d: absent → g40; c: g10 → g40 |
| 5 | remove d | d: g40 → absent (g50) |

With `b = 2`, the complete nodes at head 5:

```
level 0   D0 D1 D2 D3 D4 D5              (the deltas)
[0,1]     a: absent → g1 · b: absent → g1 · c: absent → g10
[2,3]     a: g1 → g30 · b: g1 → g30      (a was live before 2, live after 3)
[4,5]     c: g10 → g40                   (d: absent → absent, dropped)
[0,3]     a: absent → g30 · b: absent → g30 · c: absent → g10
```

K, once compacted to the head: `a g30 · b g30 · c g40`. Before compaction, the
base `a g1 · b g1` under the runs D1 … D5.

Two consumers keep `tally = ctx.load() + len(added) − len(removed)`. **X** sits
at position 1 with tally 2 (`a`, `b`). **Y** sits at position 3 with tally 2
(`b`, `c`), plus a read-ahead entry: a keys= run delivered `d` live as of
commit 4. Its tally is 3.

### 1. Appending a delta

Unchanged: resolve each written key against K newest first (filters, then a
block per filter hit), write the delta with each key's predecessor and
whether it was live, one PUT, and add `added − removed` to the count. One more
field rides the journal record: the live-state digest `D(c)` (§ The sync risk).

*Example.* Commit 6 updates `b`. K's newest entry for `b` is g30, live: the
delta says `b: g30 → g60`, the count stays 3, and `b`'s object g30 is cleaned
up later from this delta.

Sequential round trips: as today. A cold writer pays 2 for its lookups (1 at
small sizes), 1 for the PUT, then the journal's write; a warm writer (the
engine's resolver) only the PUT and the journal.

### 2. changes(P, N)

Cover `[P, N]` with the largest complete nodes, left to right (the canonical
decomposition: at most `b − 1` nodes per level on each side), and merge them
page by page by key: before from the oldest node holding the key, after from
the newest. Then class as today: added, updated, removed; a payload-bearing
key live at both ends with equal payloads is neither.

*X, changes(1, 5)*: `D1 + [2,3] + [4,5]`. `a`: live before (from `[2,3]`), g30
after: updated. `b`: updated. `c`: absent before (D1), g40 after: added. `d`
appears nowhere: nothing. Tally 2 + 1 = 3.

*Y, changes(3, 5)*: `D3 + [4,5]`. `a`: absent before (D3), g30: added. `b`: g1
→ g30: updated. `c`: g10 → g40: updated. Tally 3 + 1, and the read-ahead (§ 5)
removes `d`: 3.

**P inside a merged range.** Y's position 3 falls inside `[2,3]` and `[0,3]`.
That's fine: those nodes are never the only copy. The retention rule (§ 6)
keeps D3 because endpoint 3 needs it, so the cover uses D3 instead of `[2,3]`.
No node carries versions for an endpoint inside it; the finer nodes are kept
instead. That's the whole trade against spans: **storage instead of versions**.

And if 3 had never been reserved (A17's R5: a covering retry's landing point)?
With raw deltas kept back to the oldest endpoint, D3 is still there: the cover
works, one extra GET. Without them, the read finds a hole and fails loudly;
the consumer does a full pass, as with spans today.

Round trips: 2 for the first page (node tails, then a block range per node), 1
per further page. A daily reader's cover is ~19 nodes at `b = 4` against ~16–18
spans; the nodes are fetched in parallel, so that's requests, not latency.

### 3. Prefix and pattern pages, at the head and at a pinned position

At the head: merge K's runs by key, page by page with a key cursor, a prefix a
key-range seek, as `KeyIndex._scan` does over spans. K holds ~1.1 entries per
live key against spans' 1.2–9.6, so a page reads fewer bytes; the round trips
are the same 2.

**At a pinned position** (D93: a full pass reads a pinned snapshot). The pass
reserves its start endpoint `S + 1` and pins K's manifest as of `S`: the files
K had then, newest runs included. Pages read that manifest, which is a
complete single-version snapshot: every key once, so every delivery is truly
"added". Later merges write new files; the pinned ones stay until the pass
ends.

*Example.* A new consumer Z starts a full pass at head 5 and pins K as of 5.
Commit 6 updates `b`, commit 7 removes `c`, and K compacts. Z still reads `a`,
`b`, `c` from its pinned files: all added, count 3. When the pass completes,
Z's next delta is `changes(6, head)` from T: `b` updated, `c` removed.

Spans do this with endpoint versions in shared files; K does it with whole
files kept alive. A pin costs at most one copy of K's replaced files: ~1 GB at
100M keys, $0.02 a month on S3 for a pass that runs a month.

**Non-prefix patterns** (`*alpha*`, `site/*/2024-*`) filter the stream: a page
reads the blocks it would read anyway and keeps the matching keys. The brief
suggested per-block n-gram filters, to skip blocks that cannot match. They
help less than they seem:

- A block holds ~2,400 keys. A pattern matching a share `s` of keys, spread
  out, leaves a block skippable with probability (1 − s)^2400: 9% at s = 0.1%,
  79% at s = 0.01%. Only rare or clustered matches skip much.
- Ids drawn from a small alphabet fill a block with every trigram (hex has
  4,096; one block of hex ids holds nearly all), so the filter never says no.
  A Bloom over a block's trigrams of general text costs ~25 KB per 64 KB
  block (~40% more storage) at 10 bits per trigram.
- Patterns in Solera are declared on assets (`include=`), not ad hoc. A
  pattern that matters can get its own derived run set (a K filtered by the
  pattern, built the same way and checked by the same digest), which is exact.

None of this depends on two views: block filters are a format lever and apply
to spans as well. The one slow case is a full pass under a rare non-prefix
pattern at 100M keys: a full scan decodes ~100M entries, ~5 s on four cores,
whichever design.

### 4. Writers' head lookups

K newest first, as today over spans. With one entry per key, K's filters and
blocks are those of the live keys alone: at 1M keys with 10 daily readers, 11
MB read for 1K cold lookups against 76 MB from spans (replayed). At 100M keys
both read the bottom run's filters (~175 MB) and one block per key; the
difference is the versions spans carry: ~25% more bytes, 1.37 s against 1.21 s.

### 5. Exact presence at a reader's position (read-ahead)

A read-ahead entry `[r, run, attempt]` says a keys= run delivered some keys as
of upstream commit `r`, and D93 adds current-only reads that saw newer state.
Classing such a key needs what was delivered (from the attempt's sealed
result, as today) and the key's state at `N`: **K at `N`** (the head, or the
pinned manifest of the pass that reads up to `N`). A key live at `N` whose
generation is no newer than commit `r`'s didn't change: skipped. Otherwise its
class is (delivered state, state at `N`). K needs no tombstones for this: a key
delivered live and absent at `N` changed.

*Y's `d`*: delivered live at 4. K at 5 has no `d`: removed. Tally 3 + 1 − 1 = 3,
`a`, `b`, `c`. Checked against the fold over 3.2M (key, r, N) cases.

T doesn't serve read-ahead: a read-ahead commit `r` holds no endpoint, and
nodes are cut only at endpoints. That's also why T may drop absent-to-absent
keys (spans must keep them, A12-1): read-ahead keys are classed from K, so no
reader needs T to remember them.

### 6. Many lagging readers: what T keeps

An endpoint `e` (a position, a pass's ends, a landing point, as reserved today)
needs two chains of nodes:

- its **ascending chain**, the nodes a reader at `e` climbs through: from `e`,
  the largest aligned node starting there, then the next, up to the top level;
- its **descending chain**, the nodes a range ending at `e − 1` descends
  through (a pass pinned at `N` reads `[P, N]`, `e = N + 1`).

T keeps those chains for every live endpoint, the head's descending chain (the
frontier), and every top-level node from the oldest endpoint on. A node no
chain needs is deleted once its parent exists. Since endpoints are born at
head + 1, past every existing node, a deleted node is never needed again.
`check.py` verifies that the kept nodes answer every query between endpoints,
at every head, and that no dropped node is needed later, over 357,446 queries
with endpoints coming and going (and that dropping the descending chains
fails).

*Example*, endpoints 1 and 3 at head 5: kept are D1, D2, D3, `[2,3]`, `[4,5]`.
`[0,1]` and `[0,3]` cross endpoint 1 and nobody's chain uses them: deleted.

Each endpoint costs at most `2(b − 1)` nodes per level. The top-level nodes in
between are shared by all readers. So at 100M keys T holds roughly the raw
changes since the oldest endpoint, about once; at 1M keys it holds a few
copies of the index (each high node touches most keys). Spans hold at most
one version per key per lagging position (D85). Neither grows without bound
once the per-position budget turns a reader that would read more than a full
index into a full pass.

### 7. Pins, cleanup and zombie engines

Files and who holds them:

| File | Held by | Freed when |
|---|---|---|
| a delta | K (until merged into a run), T (while an endpoint's chain needs it), raw retention (back to the oldest endpoint), its cleanup (as today) | all four let go |
| a K run | K's current manifest; any pinned manifest (an attempt, a pass's snapshot) | replaced and unpinned |
| a T node | the retention rule above; an attempt's pinned cover | no chain needs it and no pin holds it |

The orphan collector's roots are the union, as today's are the union of the
current state, pinned manifests, pending cleanups and merges in progress. A
zombie engine is the same problem as with spans and has the same answer:
D86's epochs in every output name, and A17-R1's barrier between listing and
deleting. One thing gets easier: a T node is a pure function of its range, so
a node built twice (a retry, a zombie) has the same content and either copy
serves. Names must still be unique, so the collector never deletes a file a
live owner wrote under the same name.

## The sync risk, weighed

Erwin's worry: duplicated state drifting apart. Here is everything that can
diverge, and what stops it.

| What could diverge | Example | Ruled out by | Detected by |
|---|---|---|---|
| A view against the log | a K merge keeps `b` g1 instead of g30; a T node takes `a`'s before state from the wrong child | both views are pure functions of the deltas; nothing writes a view but its own merge, published through the journal with its input identities checked (as spans' `holds`) | the digest identity on every merge output, before publication |
| One view against the other at the same N | Y's changes(3, 5) says `c` updated (from T) while the read-ahead finds `c` absent at 5 (from K) | each reader's N names one commit; both reads are of that commit by construction (K's manifest as of N, T's cover ending at N) | follows from the row above: both are checked against the same D(N) |
| A view running ahead of or behind the log | K's merge publishes over a delta that a reset since fenced | the delta enters both views in the commit's own journal record; merges check the index's life | the journal refuses it, as for spans |
| A file freed while a view still needs it | the collector deletes D3 while endpoint 3's chain needs it | one collector, roots the union of both views' holds | a read that finds a hole fails loudly |
| The log itself | a writer resolves `b` against a wrong head and writes the wrong predecessor | the same as spans: exact writes against one view | the next digest check of any merge covering that commit |

**The digest.** The writer keeps `D(c) = Σ h(key, generation)` over the live
keys after commit `c`, updated from the exact delta it already computes (add
the new versions' hashes, subtract the replaced ones'), one 8-byte field per
commit in the journal. For any file covering `[a, b]` holding, per key, its
state before `a` and after `b` (a T node; a K run, if it keeps its oldest entry's predecessor,
a few bytes per entry), the sum over its keys of h(after) − h(before) must
equal `D(b) − D(a − 1)`: the change telescopes. `check.py` verified the
identity on 89,374 nodes and caught all 78,746 injected corruptions (an entry
dropped, a before or after state altered), none missed. A 64-bit sum misses an
independent corruption with probability 2⁻⁶⁴. Natively it costs 1.75 ns per
entry (measured, both hashes), noise beside decoding.

The same identity holds for spans: a span's newest version and its oldest
predecessor per key are its after and before. So the drift check is not a
reason to choose two views; it's a cheap guard spans should adopt.

Weighed: with derivation instead of dual writes, the remaining sync risk is
bugs in background merges and in retention, which spans have too. Two views
have twice the merge code and two retention rules, each simpler. The digest
makes the first class detectable before publication in either design; the
second is the collector's union of roots, which both need.

## Numbers

All replayed and modelled; the full output, both sizes and both reader
counts, is [`bench/keys/twoviews/results.md`](../bench/keys/twoviews/results.md).

**The workload.** 1K random keys per commit (97.5% existing keys, 2.5% new),
one commit every 10 s: 259,200 commits a month. Readers: one at the head
(the writer), 10 or 100 daily readers spread over the day, one hourly. A full
pass once a month (a new consumer). Spans' replays run 40,000 commits at 1M
keys and 120,000 at 100M, measuring the second half.

**The read model.** 10 B per entry with its filter (1.75 B), 2,400 entries per
64 KiB block, 64 MiB files, 16 MiB range reads; files under 2 MiB and runs
under 32 MiB read whole; a run streamed past `Options.stream_density` and
`stream_reads`; 30 ms per request, 64 in parallel, 500 MB/s aggregate and
80 MB/s per connection; decoding at 18M entries/s (~4 cores at the measured
4.6M/s each). One metadata object makes all tails one GET. Prices: S3
Standard list (PUT $5 per million, GET $0.40 per million, $0.023 per
GB-month; DELETE and in-region transfer free); Railway: requests and transfer
free, $0.015 per GB-month (assumed from Railway's bucket pricing; check before relying on it).

**Calibration** against the span design's measurements (different traces, so
only the order of magnitude is the test):

| 1K cold exact lookups | Model | Measured |
|---|---|---|
| spans, 100M keys | 1,100 GETs · 318 MB · 1.4 s | 1,166 GETs · 242 MB · 1.8 s (v4 prototype) |
| single-version (K; leveled measured), 100M | 1,091 GETs · 242 MB · 1.2 s | 907 GETs · 200 MB · 1.13 s (`layouts.py`) |
| spans with lagging readers, 1M | 13–18 GETs · 76–96 MB · 0.55–0.71 s | 18–42 GETs · 21–61 MB · 0.48–0.88 s |
| single-version, 1M | 5 GETs · 11 MB · 0.11 s | 3 GETs · 10 MB · 0.21 s |

The model counts requests well and under-counts wall time at 1M (it has no
Python). Spans' replayed compaction writes (7.5× at 1M, 14× at 100M) match the
span design's replays (7.0–7.6×, 12.6–14.4×); its v4 measurements were lower
(6–7×) on a different trace.

### Per workload, 100M keys, 10 daily readers + 1 hourly

Each cell: sequential round trips · requests · bytes read · seconds, cold.

| Workload | Spans, as built | Spans + levers | Two views |
|---|---|---|---|
| append a delta | 1 PUT, plus the lookups below | same | same |
| writers' head lookups (1K keys) | 2 RT · 1,100 GETs · 318 MB · 1.37 s | 2 RT · 1,178 GETs · 249 MB · 1.26 s | 2 RT · 1,091 GETs · 242 MB · 1.21 s |
| changes(P, head), hourly reader | first page 0.09 s; full 7 GETs · 8 MB · 0.2 s; reads 2.0× what changed | first page 0.12 s; full 21 GETs · 30 MB · 0.4 s; 8.0× | first page 0.06 s; full 13 GETs · 4 MB · 0.1 s; 1.00× |
| changes(P, head), daily reader | first page 0.07 s; full 1,344 GETs · 90 MB · 3.8 s; 1.10× | first page 0.09 s; full 499 GETs · 261 MB · 4.7 s; 3.2× | first page 0.07 s; full 1,596 GETs · 84 MB · 3.9 s; 1.03× |
| 100K-key page, head or pinned snapshot | 2 RT · 46 GETs · 4.5 MB · 0.09 s | 2 RT · 7 GETs · 3.6 MB · 0.09 s | 2 RT · 27 GETs · 4.4 MB · 0.09 s |
| read-ahead presence | a lookup per key at N: as the lookups row, per key | same | same, against K at N |
| runs a head read opens, mean (max) | 17.8 (22) | 6.0 (7) | 8.4 (14) in K |
| background entry writes per entry committed | 14.1 | 19.8 | 13.3 (K) + 8.0 (T) = 21.3 |
| bytes stored, mean | 1.20 GB | 1.22 GB | 1.13 GB K + 0.20 GB T + 0.09 GB raw deltas = 1.42 GB, plus up to one K copy (~1.1 GB) per pass pinned across a base merge |

Dollars a month:

| Workload | Spans, as built | Spans + levers | Two views |
|---|---|---|---|
| S3: append (the delta PUT) | $1.30 | $1.30 | $1.30 |
| S3: writers' head lookups, cold every commit | $114.04 | $122.14 | $113.06 |
| S3: background merges and node builds | $1.02 | $2.66 | $1.58 |
| S3: every catch-up | $0.16 | $0.07 | $0.20 |
| S3: one full pass | $0.005 | $0.002 | $0.001 |
| S3: storage | $0.028 | $0.028 | $0.032 |
| **S3, total, warm writer** | **$2.51** | **$4.05** | **$3.11** |
| **S3, total, cold writer** | **$116.55** | **$126.19** | **$116.17** |
| **Railway, total** (storage only) | **$0.018** | **$0.018** | **$0.021** |

With 100 daily readers at 100M the picture holds: catch-ups cost $1.82,
$0.60 and $1.92 a month, warm totals $4.18, $4.59 and $4.83, and T keeps
0.33 GB.

### Per workload, 1M keys, 100 daily readers + 1 hourly (many lagging readers)

| Workload | Spans, as built | Spans + levers | Two views |
|---|---|---|---|
| entries stored per live key | 9.58 | 12.15 | 1.12 (K) |
| writers' head lookups (1K keys) | 1 RT · 18 GETs · 96 MB · 0.71 s | 1 RT · 12 GETs · 121 MB · 0.50 s | 1 RT · 5 GETs · 11 MB · 0.11 s |
| changes(P, head), hourly reader | first page 0.08 s; full 6 GETs · 6 MB · 0.2 s; 1.96× | first page 0.12 s; full 21 GETs · 22 MB · 0.3 s; 7.3× | first page 0.06 s; full 12 GETs · 3 MB · 0.1 s; 1.12× |
| changes(P, head), daily reader | first page 0.12 s; full 252 GETs · 89 MB · 1.2 s; 7.2× | first page 0.13 s; full 79 GETs · 111 MB · 1.3 s; 9.0× | first page 0.09 s; full 266 GETs · 41 MB · 0.8 s; 3.4× |
| 100K-key page, head or pinned snapshot | 2 RT · 31 GETs · 11.2 MB · 0.14 s | 2 RT · 7 GETs · 13.2 MB · 0.16 s | 2 RT · 6 GETs · 2.3 MB · 0.08 s |
| background entry writes per entry committed | 7.7 | 17.7 | 8.0 (K) + 4.9 (T) = 12.9 |
| bytes stored, mean | 0.10 GB | 0.12 GB | 0.01 GB K + 0.26 GB T + 0.09 GB raw = 0.36 GB |
| **S3, total, warm writer** | **$2.63** | **$4.06** | **$3.19** |
| **S3, total, cold writer** | **$4.49** | **$5.33** | **$3.72** |
| **Railway, total** | **$0.001** | **$0.002** | **$0.005** |

What the tables say:

- **Round trips never differ.** 2 for a cold read of a large run, 1 when the
  runs are small enough to read whole (every 1M lookup here). The metadata
  object cuts GETs (46 → 7 per page at 100M) but not round trips.
- **The key view's win is the versions spans don't hold.** At 1M with 100
  readers spans hold 9.6 entries per live key, so cold lookups read 9× the
  bytes (0.71 s against 0.11 s). At 100M spans hold 1.2 and the gap is 13%.
- **The time view reads exactly its range** (1.00–1.03× what changed at 100M)
  but opens more runs (12–19 nodes against 6–18 spans), so it makes about as
  many requests. Catch-up latency is a tie at 100M; at 1M the daily reader's
  full catch-up drops from 1.2 s to 0.8 s.
- **Eager merging is the worst of the three for catch-up**: forced merges
  across endpoints make readers read 3–9× what changed, and writes go up
  1.4–2.3×. It does cut the runs a head read opens to 6, which is what it's
  for.
- **On S3, the bill is the commit PUT, and cold writers' lookups if the engine
  cache is cold.** $114 a month at 100M for a cold writer every 10 s in every
  design, against $1–3 for everything else. The engine cache, not the layout,
  is the lever. On Railway the whole index costs 2 cents a month at 100M.

### The time view's fanout

| 100M keys | writes per entry | hourly reader: nodes, read × changed | daily | a week | a month |
|---|---|---|---|---|---|
| b = 2, top 2¹⁸ | 16.4 | 8.4, 1.00× | 13.2, 1.03× | 15.9, 1.20× | 17.7, 1.86× |
| b = 4, top 4⁹ | 8.0 | 12.3, 1.00× | 19.0, 1.03× | 23.7, 1.24× | 27.2, 2.09× |
| b = 8, top 8⁶ | 5.2 | 18.7, 1.00× | 28.8, 1.03× | 35.9, 1.22× | 42.2, 2.31× |
| b = 4, top 4⁶ (fixed at 4,096 commits) | 6.0 | 12.3, 1.00× | 19.1, 1.03× | 32.2, 1.29× | 80.1, 2.58× |

At 1M keys `b = 4, top 4⁹` writes 4.9× and a daily reader reads 3.4× what
changed (high nodes touch most keys, and a reader's cover repeats them); a
fixed top at 4⁶ makes a month-old reader read 9.4×. The top level must reach
the oldest reader the per-position budget allows.

## A17 and A19, as a checklist

**A17** (the key index review, at eef4f33). "Gone" means the mechanism cannot
arise in two views; "same" means the obligation stays.

| Finding | Spans | Two views |
|---|---|---|
| R1 (P1) zombie orphan deletion | fix: barrier after listing, D86 epochs | **same**, over more file kinds (deltas, K runs, T nodes). Deterministic node contents make double builds harmless; names stay unique |
| R2 (P1) a key's versions crossing a page window | fix: finish the key before the cursor | **gone**: one entry per key per file. A page that returns nothing must still advance its cursor |
| R3 (P1) cached lookup picks the wrong file of a repeated key | fix: first file holding the key | **gone**: a run's files have disjoint key ranges, a key once |
| R4 (P2) a selection with no position | engine fix | **same** (the engine's) |
| R5 (P2) a covering retry's landing endpoint | fix: reserve every landing point | **same** obligation (T's retention needs reserved endpoints), **softened**: raw deltas back to the oldest endpoint make a miss a slower read, not a full pass (also available to spans) |
| R6 (P2) admission for every writer | fix: one admission rule | **same**, over two backlogs: K's runs and T's unbuilt nodes |
| R7 (P2) rejected stale-version rewrites | fix: remember failed input sets | **gone**: no stale-version rewrite; a node or run is a function of its inputs |
| R8 (P2) attempt budget lost on restart | fix: durable attempt counts | **same** if a write bound is promised |
| R9 (P2) `u64::MAX` as "no bound" | fix: `Option` | **gone** for reads: K is pinned by manifest, T by commit range; no generation bound. Generation overflow must still be refused |
| R10 (P2) a hot key's versions in RAM | fix: stream versions | **gone**: a merge holds one entry per input file per key |

**A19** (incremental semantics, at 7c81f84) sits above the index; D93 fixes it.
What matters here is that each fix gets what it needs from the index:

| Finding | What the fix needs from the index | Two views |
|---|---|---|
| R1, R2 a full pass over a moving head | D93 (1): a pinned snapshot at the pass's start | K's pinned manifest (§ 3) |
| R3 completion forgetting a removal | removals of delivered keys, classed at N | K at N (§ 5) |
| R4 pattern-time selections | selections recorded as read-ahead, classed via `lower=` | K at N, per key |
| R5 F38's early removal | recorded as read-ahead, classed against it | K at N, per key |
| R6 selections and `batch_size` | nothing (engine) | n/a |
| R7 input bindings | nothing (engine) | n/a |
| R8 a forced retry after a revert | the net rule on payloads | T's net rule, the same as spans' |
| R9, R10 staleness roll-ups | `changes` that stops at the first delivered key | T's merge stops early, as spans' does |

Neither review argues for or against two views on correctness grounds alone:
five A17 mechanisms disappear, and the A19 fixes are served equally.

## Go/no-go

**No-go** for experiments on two views as a replacement for spans:

- Latency, the first priority, doesn't move at scale: the same round trips,
  the same first-page times, cold lookups within ~15% at 100M. The clear wins
  (5× on cold lookups, smaller pages, ~2× less read on catch-ups) are at 1M
  keys with many lagging readers, where every operation already takes well
  under a second and the engine's warm cache answers anyway.
- Requests and cost: a wash. The S3 bill is the commit PUT and cold writers'
  lookups in every design; Railway's is storage, cents.
- Storage, last: two views keep more (T's nodes, raw deltas, pinned K files),
  as the brief allowed.
- The simplification is real but bought with a second pipeline, and spans are
  built, measured, proved and in their fix round.

**Take now**, whatever T22 finds: the digest check for spans; raw deltas kept
back to the oldest endpoint; and in T22, report the read ratio beside the span
count when testing eager merging.

**Flip to go if** T22's re-measurement or production shows either (a) cold
head lookups or head pages dominated by spans' retained versions at the index
sizes users run (more than ~2 entries per live key), with the engine cache
often cold; or (b) A17's fixes for R2, R3, R9 and R10 failing re-review, or a
new P1 in endpoint-version handling. Then the plan:

1. **Build what the trigger names, beside spans.** For (a), K alone: a
   single-version run set over the deltas (T22's head snapshot is the same
   thing). For (b), both views, with K as spans' policy run with the head as
   its only endpoint. On the shipped `.kx` v4 format: a T node is a `.kx` file
   whose entries carry a before state, the delta format with its predecessor
   bits. Native node merge, the canonical cover, the retention rule, the
   digest.
2. **Same traces as `v4bench.py`**: 1M and 100M keys, 12,000 commits of 1K
   keys; readers 1, 100, 360, 8,640 and 10,000 behind; 100 daily readers
   spread; a stalled pass; temporary-key churn; a 1M-key commit. Cold readers
   in isolated processes, 30 ms per request, 80 MB/s per connection, 64 in
   parallel.
3. **Measure, key by key against the per-commit fold**: first page and full
   catch-up time, GETs, bytes, peak memory; entries written per entry
   committed, T's and spans' separately; bytes stored, mean and peak, with
   pins; the same for K against spans on cold 1K lookups and 100K-key pages
   at the head and at a pinned snapshot.
4. **Fanout**: `b` = 2, 4, 8 with the top level at a month of commits.
5. **Decide** on the gap: go further only if T's catch-up or K's cold head
   reads are ≥ 2× faster at 100M keys or at the sizes users run, at ≤ 2× spans'
   writes.

Run on Opus 5.5 High (D97), under `systemd-run` caps, after T18's
re-measurement lands.

## What was checked, and the limits

- `check.py` (5,000 random histories of 6 keys and up to 24 commits,
  payload-bearing or derived, fanout 2–4, top level 1–4; 10 s): T's
  `changes(P, N)` equals the fold's classes and delivered generations over
  539,495 ranges; the retention rule answers 357,446 endpoint queries with
  endpoints coming and going, and 234,771 dropped nodes are never needed
  later; read-ahead against K matches over 3,236,970 cases; the digest
  identity holds on 89,374 nodes and caught all 78,746 injected corruptions.
  Three calibrations each fail as they should: retention without the
  descending chains; pruning equal-payload entries (it loses the delivered
  generation); read-ahead without the generation test.
- `model.py`: spans, spans with eager merging and K are `spans.py`'s policy,
  replayed; T is analytic over the same density model; requests and seconds
  come from a stated read model, calibrated in § Numbers. Uniform random keys,
  1K per commit; no injected failures; upkeep instant.
- The digest's cost: a C loop over 10M entries of 16-byte keys, 5 passes,
  under the caps.
- Not done: no prototype of either view on real files, no S3 run, no
  pinned-snapshot timing for K, no measurement of the n-gram filters.
