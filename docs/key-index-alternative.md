# A simpler key index: immutable snapshot pages

**Verdict: keep spans.** A tree of immutable, single-version snapshot pages makes
historical lookup and change classification easier to reason about. It does not
meet Solera's workloads at comparable cost. Scattered commits copy whole pages;
packing those pages creates substantial dead space or requires another
compaction system. This study found a useful alternative for bulk, clustered
writes, but no reason to replace spans for Solera's small, scattered commits.

At 1M keys, 1,000 random updates copy **226,029 rows per commit**, measured with
256-row leaves. Spans' published prototype measurements are **7.4–8.0 total entry
writes per committed entry** on its mixed traces. Even 64-row leaves copy
**62.0 rows per update**. At 100M, the corresponding estimate is **255.7** and
**64.0**. These ratios count leaf rows only; directory writes make snapshots
more expensive.

Read performance is a better story. In the 1M fixture, a packed snapshot diff
10,000 commits behind has an estimated first-page time of **0.108 s** at 30 ms
per request, close to spans' historically measured **0.10–0.11 s**. That estimate
includes aggressive range coalescing and excludes encoding, decoding and actual
store scheduling. It is not an object-store measurement. The recommendation
rests on measured copying and retention, not on claiming that trees always
read more slowly.

## Scope and evidence

This is A20, based on Solera `7c81f845344e23bae935b9471f34f3ef2b221556`.
Only this document and `bench/keys/alternative/` are added. No product code,
existing benchmark or dependency configuration changes.

Read first:

- [The span design](key-index-design.md), [the leveled cost study](key-index-costs.md)
  and [positions derived from reads](positions-from-reads.md).
- A17's ten findings, `review/a17/handoff.md` on branch `review/a17-artifacts`,
  reviewed source `eef4f335f64ec0565c47f05d3159c917cac9cda0`.
- A9's review is recorded as A9 in Initiative;
  its actual local artifact is
  `/home/exedev/.bb/thread-storage/thr_kybinuxkzk/reports/A9-presence-review.md`,
  with its `A9-plain-deltas.md` follow-up. A9 already identified persistent trees
  as the strongest architectural alternative. This study measures that option;
  it does not claim to have invented persistent snapshots.
- D85 removes the lagging-consumer cap. D86 describes durable ownership epochs.
  D88 distinguishes the packed-delta control from the shipped leveled index.

Evidence labels throughout:

- **Measured** means a measurement or deterministic counter from the named
  executable run. Historical measurements are explicitly attributed. Counting
  bytes that a simulated layout would write is not an actual storage write.
- **Estimated** means a stated model or extrapolation. No new 100M run or S3 run
  was performed. No unlabeled timing is a production claim.
- **Proved** means the short argument in this document or an identified existing
  proof. The new tree has no Lean or TLA proof.

The [prototype README](../bench/keys/alternative/README.md) gives exact commands,
limitations and file meanings. [analysis.json](../bench/keys/alternative/analysis.json)
contains all derived values. [The handoff](../bench/keys/alternative/handoff.md)
records decisions and remaining work.

## The alternative

A position names a tree root. Each root describes the entire live keyspace at
one commit. Every leaf contains a sorted set of distinct keys, each with its
current generation and optional source payload. A deleted key is absent.
Different roots share unchanged leaves and directory nodes.

A leaf is addressed directly, by immutable object identity, offset and length.
Directory entries associate disjoint key intervals with child references. An
update copies the affected leaves and their directory paths, then atomically
publishes the new root. It never edits an existing page. The exact previous
value is already available while rewriting a leaf, so the writer can produce
exact predecessor information for data cleanup and adjust the live count.

Use ordinary key-range splits when a leaf exceeds its byte budget. Do not
rebalance pages merely to make two snapshots look alike. Different tree shapes
are legal; the diff aligns key intervals, descending on either side until their
ranges match. Equality of immutable child references permits skipping a shared
interval. Unequal references mean "compare", not "different values". This
avoids depending on canonical chunk boundaries or hash equality for correctness.

The proposed implementation would use byte-bounded pages, with large payloads
stored separately and compared exactly when source equality matters. The
measurement fixture instead uses fixed ranges of 16, 64, 256 or 1,024 slots and
a simple two-level directory. The byte-key model exercises splits, but uses a
flat directory. Neither prototype implements the complete proposed tree.

There are two physical choices:

1. **One object per page.** This is the simpler version. Removing a root permits
   unreachable pages to be collected individually. At 1M, a uniform 1K commit
   creates about **883 leaf objects, measured**, plus directory objects with
   256-row leaves. At 100M it creates about **999, estimated**. Parallelism can
   hide some time; it cannot remove the PUTs or copied rows.
2. **Pack the copied pages for a commit into one immutable object.** This is the
   version measured here. A reference names a pack and slice; the root is
   published after all its slices exist. Most small commits need one pack PUT,
   plus journal publication. Reads coalesce nearby slices. A pack cannot be
   deleted while even one leaf in it remains reachable. Reclaiming its dead
   slices requires relocation, new references and publication.

There is one version of a key per leaf and per snapshot, not one version of a
key in the whole store. Pinned roots necessarily retain history. Large payload
objects, directory pages and optional packs also mean this is not literally a
one-record-kind replacement for `.kx`.

### The invariants

The read path needs these properties:

1. Each published root is a complete ordered map with disjoint child intervals.
2. Each reachable leaf contains at most one value for a key.
3. Every referenced page exists before root publication and never changes.
4. A reader owns a durable pin before its root may be retired.
5. A new root derives only from pinned inputs; an unreachable page cannot become
   reachable again. Orphan names are never reused.

The first three make ordinary reads much simpler than span reads. The last two
are still concurrency obligations. They become especially important when
physical pack relocation is added.

### Each workload

| Workload | Snapshot operation | Cost and obligation |
|---|---|---|
| Append a delta | Apply sorted changes to the head root; copy affected leaves and paths; upload and CAS-publish | Exact old values and live count come from the leaves. Commit latency includes this copying. |
| Catch-up from position P through commit N | Diff `root(P−1)` against `root(N)` | Skip identical children; stream unequal key ranges. The size of the changed *pages*, not just changed keys, determines work. |
| Exact old-position presence/version | Look up the key in its retained root | One search path, one leaf, no version group and no generation-bound sentinel. |
| Head lookups for writers | Look up the head root, batching keys by leaf | Cached leaves answer locally. Source equality remains an exact comparison. |
| Range or prefix page | Ordered tree walk at the chosen root | Byte-order comparisons; prefix tests or a correctly computed optional upper bound. Empty and all-`ff` prefixes must work. |
| Many consumers and cleanup | Share a root when positions match; otherwise pin separate roots and their reachable pages | No global oldest-consumer replay requirement. Copying unchanged neighbors can make each pin expensive. Packs amplify physical retention. |

Only roots currently owned by the head, a position, a pass or an attempt need
retention. The design does not promise access to every historical commit.
Pinning an unretained old commit later is an error, not a reason to resurrect
unreachable storage.

### Worked example: cancellation and re-adds

Start at commit 0 with `a@g1` and `b@g1`. Position 1 means the first unread
commit is 1, so it holds root 0.

| Commit | Write | New root's live entries |
|---|---|---|
| 1 | add `c@g10` | a, b, c |
| 2 | remove a | b, c |
| 3 | re-add `a@g30`, update `b@g30` | a, b, c |
| 4 | add `d@g40` | a, b, c, d |
| 5 | remove d | a, b, c |

Diffing root 0 and root 5 yields a updated, b updated, c added, and d neither.
The old count was 2; adding 1 and removing 0 gives 3. A consumer at position 3
holds root 2, where a was absent, so the same root 5 makes a **added**.

No predecessor-chain merge is needed. Missing at the old root means absent
there; missing at the new root means absent now.

### Worked example: read-ahead cannot disappear in cancellation

Suppose a `keys=(d,)` run delivered d at commit 4. A later whole-position diff
still sees d absent at both ends. That is insufficient: the consumer has d and
needs a removal.

Keep the attempt's sealed delivered state, including explicit absence, as
Solera already requires for read-ahead. Query candidates are the union of root
diff candidates and read-ahead keys. For each read-ahead key, compare the
state actually delivered with its state at root N. Thus d is removed even
though it appears in neither endpoint tree. This also avoids pinning a whole
snapshot for every read-ahead key.

A source with payload `v1 → v2 → v1` is neither when its endpoint payloads are
equal, despite different generations. A derived output has no source payload;
a changed live generation is updated. Equal roots skip safely in both cases,
but a read-ahead exception still needs its own comparison. Empty payload is a
valid value, distinct from absence.

The public engine could continue exposing the four classes. This is not a
byte-for-byte replacement for internal `changes()` rows: the tree deliberately
forgets the time of an absent-to-absent event. An adapter must use delivered
state and root identity instead of relying on a tombstone's generation. The
old predecessor generation needed for physical data cleanup comes from the
write, before its leaf is replaced. No engine adapter was built here.

### Worked example: split and resume

A leaf holds a, b, c, d. Insert bb; it splits into `[a,b]` and `[bb,c,d]`.
The old root still names the original leaf. The diff aligns the old leaf's
range with both new leaves, compares entries in key order and returns only bb
as added. It does not treat an unfamiliar child reference as a new key range.

A continuation token contains the index life, both pinned root identities,
query bounds and the last fully examined key. It reopens both trees strictly
after that key. `None` means the beginning; the empty byte string is a valid
key. A page stopped by a byte/work budget can return no changes but must move
its examined-key cursor. End-of-stream is explicit. A key occurs once in each
root, so its versions cannot straddle a page boundary, but long values and
empty-result pages still need a bounded streaming implementation.

## Publication, takeover and collection

Root publication is one conditional journal update for the same index life and
expected parent root. A writer that loses CAS re-resolves against the new head;
it cannot publish a root based on a stale predecessor. Output rename retains
its life and storage prefix; reset creates a new life. A pass pins both its
baseline and target roots. Any attempt that can advance a position transfers
its target-root pin atomically to that position, including covering retries.
A selection with no position owns its ordinary read pin but transfers nothing.

For per-page objects, a conservative collector can use a durable collection
cut. It protects the transitive closure of every root and input pin at that cut,
and any upload reservation eligible for publication. It considers only objects
created before the cut. New names include the fenced engine epoch and a unique
allocation identity. A successor cannot publish from unpinned inputs or reuse
an orphan object. Reservation and pin registration serialize with the cut.

Under those rules, a post-cut root can only reference old pages already
protected by the cut or newly allocated pages excluded from deletion. By
induction, the cut's dead objects cannot become live later. **Proved conditional
safety argument**, not a verified implementation. A zombie can then finish
that fixed deletion set without deleting successor objects. Merely attaching
an epoch to a fresh listing is insufficient: the mark, allocation cutoff,
publication reservations and no-resurrection rule all matter.

At a crash after upload but before publication, the reservation protects the
upload until the publisher is durably abandoned. A takeover cannot reclaim
unfinished reservations solely because the old process paused. It must make
publication by that reservation impossible first. Persist retry exhaustion
before work if a lifetime upload bound is promised. These are the same broad
obligations that A17 exposed; changing the data structure does not remove them.

Packs add a separate problem. The collector marks *packs* containing reachable
pages, so tiny live slices hold dead bytes. Relocating live pages requires
copying them, rebuilding references up to every retained root being relocated,
and CAS-publishing replacement physical roots with identical logical content.
Old physical roots remain pinned by existing readers. An indirection map is an
alternative, but then the map is another persistent index and pinning problem.
Neither relocation scheme is implemented or included in the write ratios here.

## Measurements at 1M keys

Runs used the shared Linux server, AMD EPYC 9554P, under
`systemd-run --user --scope -p MemoryMax=8G -p CPUQuota=400%`.
The scope reported `MemoryMax=8589934592` and `CPUQuotaPerSecUSec=4s`.
The fixture is single-threaded; the quota is an upper bound, not four busy CPUs.
Final process resource reports are checked in. This is an in-memory page and
pack-layout prototype, not a storage-service benchmark.

The main trace starts with 1M keys, applies 12,000 commits of 1K distinct keys,
and pins 101 roots 100 commits apart. Ten percent of writes make a key absent;
other writes update or re-add it. It does not grow past the original keyspace.
The sweep uses uniform updates, a dispersed hot set and different batch sizes.
Historical spans/leveled runs used different mixes, codecs, hardware and
sometimes different CPU caps. The following are comparisons of the same
*workloads*, not a controlled three-engine benchmark on one identical trace.

### Writes: the rejection criterion

| 1M, 1K uniform updates/commit | Leaf rows written/update, measured | Total raw bytes/input byte, estimated including directory | Local apply p50/p95, measured |
|---|---:|---:|---:|
| 16-row leaves | 15.88× | 180.73× | 0.94 / 1.59 ms |
| 64-row leaves | 62.02× | 109.24× | 0.57 / 0.97 ms |
| 256-row leaves | 226.03× | 237.84× | 0.55 / 1.01 ms |
| 1,024-row leaves | 641.41× | 644.36× | 0.90 / 1.59 ms |

The byte estimate assumes 16-byte input rows and 48-byte directory references,
with fanout 128. It is deliberately separate from the measured leaf counter;
compression changes it. The small-page rows are not an argument that a literal
16-byte key/version must cost 181 times as much on the wire. They show where the
cost moves when leaves shrink.

Additional measured leaf ratios with 256-row leaves:

| Scenario | Leaf writes/update, measured | Interpretation |
|---|---:|---|
| 12K commits with absence/re-adds | 224.17× | Still rewrites mostly unchanged neighbors. |
| Half of updates in a dispersed 1% hot set | 228.18× | Hot keys spread across pages; repeated *keys* do not imply repeated *pages within one commit*. |
| 16-key commits | 255.44× | Nearly one full leaf per changed key. Directory estimate raises raw total to 559.75×. |
| 100K-key commits | 10.00× | Copies the whole 1M-slot map each time. |

A contiguous 1K-key update touches at most five 256-row leaves, so writes at
most 1,280 leaf slots, **proved** for the fixed layout. A whole-map replacement
writes each row once, **proved**. These are the candidate's good workloads.
They do not cover D80's small batches and arbitrary string-key distributions.

Compression was sampled separately on 128 head leaves. Measured bytes per
original slot were 4.60 for evenly spaced IDs, 7.70 for random IDs, 20.96/24.09
with a 16-byte source payload, and 21.46 for UUID-like keys. The latter is only a
within-page sample. At the 7.70 B/slot sample, the uniform 256-row layout would
write about **1.74 MB of compressed leaves per 1K commit, estimated**, before
directory and cleanup work. Codec savings do not erase copying 226 neighbors
per update.

### The comparison on the requested workloads

`M` below means **measured**; `E` means **estimated**. Historical `M` numbers
come from the documents named above, not new reruns. A dash means no comparable
measurement, not zero cost. This table uses the 256-row packed candidate.

| Scenario | Snapshot pages, 1M | Spans, 1M | Old leveled index, 1M |
|---|---|---|---|
| Sustained 1K commits | M 226.0× leaf writes; E 237.8× raw bytes incl. directories | M 7.4–8.0× total entries on mixed traces; M 9.0× on the older matched planner trace | M 18.2× total entries on that matched trace |
| 10 commits behind, first/full page | E 0.302 / 0.302 s; M 9,868 changed keys | No 10-commit measurement; M 0.10 / 0.10–0.16 s at **100**, supplied as nearby context only | E ≥1 request wave, 0.03 s network floor for 10 small deltas; full CPU not measured |
| 1,000 commits behind, first/full | E 0.124 / 0.409 s; M 625,950 changed keys | No exact 1,000-commit measurement; M first page 0.09–0.11 s across measured 360 and 10K traces | M 3.1 s full with repeated page merges, 1.3 s with one retained merge; 1,000 GETs |
| 10,000 commits behind, first/full | E 0.108 / 0.409 s; M 991,352 changed keys | M 0.10–0.11 / 1.4–5.6 s; 1.3–1.4M changed keys | M 101.0 s full, or 21.5 s retaining the merge; 10,000 GETs. First-page time unreported. |
| Cold 1K head lookups | E 0.193 s with coalescing | M 0.48–0.88 s, 18–42 GETs, 21–61 MB | M 0.21 s, 3 GETs, 10 MB on span study's matched-layout trace |
| Warm 1K head lookups | M sub-ms RAM fixture, excluding decode/cache I/O | Warm immutable cache; no isolated 1M timing in the cited matched trace | Warm immutable cache; historical end-to-end writes are a different metric |
| 1K point reads at old positions | E 0.183 / 0.184 / 0.188 s at 10 / 1K / 10K behind | Supported by endpoint lookup; no isolated comparable timings published | Not directly supported by the old head index; raw history needs an additional historical-state strategy |
| Head page of 100K keys | E 0.098 s | M 0.08–0.10 s | M 0.08 s for the older **10K** batch benchmark; exact 100K counterpart not reported |
| Many lagging consumers | M 101 roots: 1.616 GB reachable raw leaves, 10.721 GB whole raw packs; E directories extra | M 100 spread daily readers: 80 MB compressed stored; at most 24 spans | E 10M retained events at historical 6.84 B/event ≈68.4 MB log plus current index; does not itself provide old snapshots |

The absence of matched small-window and old-position measurements is real. No
interpolated span timing has been disguised as a result. Completing W42's
shipped-API remeasurement would improve those cells; it cannot make this
candidate's measured page copying disappear.

**The packed-delta control is not the old leveled index.** Its 5.5–6.6 s far
catch-up figures in the span design belong to one packed, one-shot merge.
The old index retained per-commit files and restarted page merges. This study
keeps those baselines separate. The old index also had inferred cold writes;
its early cold-write timings are not evidence for today's exact-write contract.

### What the read estimates include

Each native query records actual referenced leaf slices and contiguous pack
ranges. The estimator tries gap limits of 0, 64 KiB, 1 MiB and 16 MiB, then uses

`2 × latency + ceil(data_GETs / 64) × latency + transferred_bytes / 500 MB/s`.

This assumes the directory can be resolved in two metadata rounds, batches all
selected data requests, and adds no codec CPU. It is a transparent planning
estimate, not a lower bound or a tail-latency guarantee. At 30 ms:

| 1M operation | Selected data requests, measured layout | Raw transfer incl. gaps, measured layout | Remote time, estimated |
|---|---:|---:|---:|
| 10-behind full | 42 | 106.0 MB | 0.302 s |
| 1K-behind first 100K | 46 | 16.8 MB | 0.124 s |
| 10K-behind first 100K | 45 | 9.1 MB | 0.108 s |
| 10K-behind full | 72 | 144.4 MB | 0.409 s |
| 1K head lookups | 29 | 51.4 MB | 0.193 s |

At 6 ms the model chooses less overfetch; estimates are 0.169 s for the near
catch-up, 0.036 s for the far first page, and 0.070 s for head lookups. Without
gap coalescing the far first page would use 349 data requests and the near full
query 3,123. A respectable planner is essential to making packs competitive.

Local queries take sub-ms to several ms, but use predecoded numeric rows.
These numbers cannot be compared with a Python/native API that decodes string
keys, materializes results or reads a disk cache. The complete history process
peaks around **1.71 GB RSS, measured**, mainly because it retains all pinned
roots in RAM. This is not a cold-reader RSS measurement. Production could keep
roots/pages on disk and use a byte-limited cache. The prototype does not verify
that proposed production memory bound.

### Pins and physical retention

| Retained roots, spaced 100 commits | Reachable raw leaf bytes, measured | Whole raw leaf packs held, measured |
|---|---:|---:|
| 1 | 16.0 MB | 114.7 MB |
| 2 | 32.0 MB | 218.6 MB |
| 11 | 176.0 MB | 1.159 GB |
| 101 | 1.616 GB | 10.721 GB |

These are layout counters, not files allocated on disk. They exclude directory
bytes. They do include dead slices in any referenced pack. Even the single-head
case holds roughly seven times its reachable leaf bytes before pack cleanup.
The 64-row run retains 1.613 GB of live leaf slots and 9.940 GB of packs for 101
roots. Smaller pages do not solve packing fragmentation.

An indefinitely stalled consumer holds one immutable snapshot, not an ever-growing
history by itself. That property is good. The expense comes from many distinct
snapshots and packing granularity. At 1M, about 100K random updates between
pins touch virtually every 256-row page, even though most individual keys did
not change. Spans retain versions of changed keys; snapshot pages retain their
unchanged neighbors as well.

## Scaling to 100M: estimates only

For N uniformly distributed keys, B changes and L rows per leaf, an independent
sampling approximation gives

`dirty_leaves = (N/L) × [1 − (1 − L/N)^B]`.

Multiply by L for copied leaf rows. The 1M, L=256 formula predicts 225.88 copied
rows/update; the distinct-key experiment measured 226.03. The small discrepancy
is expected because the formula samples with replacement. `analyze.py` applies
the same occupancy calculation to each directory level; it adds levels rather
than copying a giant flat root.

| 100M, 1K uniform changes | Leaf writes/update, estimated | Raw leaf bytes/commit, estimated | Raw directory bytes/commit, estimated |
|---|---:|---:|---:|
| 16-row leaves | 16.00× | 0.256 MB | 8.28 MB |
| 64-row leaves | 63.98× | 1.024 MB | 6.49 MB |
| 256-row leaves | 255.67× | 4.091 MB | 5.39 MB |
| 1,024-row leaves | 1,018.78× | 16.300 MB | 3.46 MB |

The directory estimates assume 48-byte uncompressed references and fanout 128;
compressed references, adaptive fanout or smaller internal nodes can change
them substantially. The leaf-copy result is independent of that assumption.
For context, spans measured 7.7× total entry writes on its 100M daily-reader
trace and 14.0× on the older matched trace; the old leveled layout measured
51.3× on that matched trace. Those are historical measurements, not 100M results
from this study.

With L=256, comparing two roots touches these leaf populations:

| Behind | Changed keys, estimated | Fraction of leaves changed, estimated | Two-root raw leaf bytes, estimated | Ideal transfer floor at 500 MB/s, estimated |
|---|---:|---:|---:|---:|
| 10 | 10.0K | 2.53% | 80.9 MB | 0.162 s |
| 1,000 | 995K | 92.27% | 2.953 GB | 5.905 s |
| 10,000 | 9.52M | ~100% | 3.200 GB | 6.400 s |

These byte estimates exclude directory reads, gaps and request latency, and
assume the raw 16-byte fixture format and a streaming leaf comparison.
They are not compressed bandwidth claims. Changing that width scales the
bytes; it does not reduce how many neighbors must be decoded and compared.
At 1K and 10K behind, the first 100K changes require about 297 MB and 33.6 MB of
raw leaf comparisons respectively, estimated from uniformly distributed changes.
A near catch-up can be much less efficient than a far one per delivered key.

| Requested 100M scenario | Snapshot pages | Spans, historical measurement | Old leveled index, historical measurement |
|---|---|---|---|
| Cold 1K head/old-position points | E ~999 leaves plus ~851 bottom directory nodes; ≈4.09 MB raw leaves + 5.4 MB directory; uncached old root has the same shape | M head 1.8 s, 1,166 GETs, 242 MB; old-position batch timing not published | M exact head lookup 1.13 s, 907 GETs, 200 MB in the matched-layout comparison; old-position API absent |
| Warm points | E no object GETs if paths/leaves cached; CPU is not extrapolated from numeric IDs | M matched warm lookup about 10 ms in the earlier layout study | M matched warm lookup about 10 ms; separate exact commit measurement 113 ms includes upload |
| Far catch-up, first/full | E 33.6 MB / 3.2 GB raw leaf comparison at 10K behind; remote time not defensibly fixed without a 100M layout | M 0.11 / 8.5 s at 10K behind, 79 MB | No matched exact-head, exact-history run. The packed control's M 6.6 s is a different design. |
| Many roots at 100-commit spacing | E 1.6 GB initial leaves + 100 × 0.361 GB ≈37.7 GB reachable raw leaves at 101 roots | Per-endpoint version retention; 100-reader 100M measurement not published | Retained log scales with events; historical point queries still need an extension |

The candidate may win individual cold point reads. It may also win large
clustered writes. Neither rescues the small scattered-write workload or the
near catch-up's page amplification.

## A17 correctness checklist

"Avoided" below means the specific failure mechanism is absent. "Handled by
rule" means the design has an answer which is not an implemented lifecycle
proof. Engine wiring is still work; this document does not declare A17 fixed.

| Finding | Span obligation | Snapshot alternative | Old leveled obligation |
|---|---|---|---|
| R1, P1: zombie orphan deletion | A stale collector must not delete successor merge output. D86's epoch design addresses ownership. | **Handled by rule, still a lifecycle risk.** Durable mark/cut, protected input pins/reservations, unique allocation names and no resurrection. Epochs alone do not prove tracing GC safe. | Same publication/collector ownership problem for rewritten files. |
| R2, P1: versions of one key cross a cold page's block/file window | Finish the key before setting the exclusive cursor; do so with bounded memory. | **Specific mechanism avoided.** One record/key/root. Still must align split ranges, stream an oversized value and advance empty-result cursors. | No endpoint-version group within one level file; cross-level merge cursor still needs progress. |
| R3, P1: cached lookup selects the last file starting with a repeated key | Find the newest occurrence across equal file minima. | **Avoided.** A direct child reference chooses the unique leaf at that root. Cache identity is the immutable page/slice, never just a key range. | Read newest levels first; the old single-version file invariant avoids this particular equal-minimum chain. |
| R4, P2: selection has `position=None` | Do not dereference or transfer a nonexistent position. | **Still present in the engine.** Own read root pin, no position transfer. A new index cannot fix this control-flow bug. | Same engine obligation. |
| R5, P2: covering retry loses its landing endpoint | Reserve every potential landing endpoint before a span can merge across it. | **Reservation mechanism avoided, lifetime rule remains.** The retry already pins target root N. Transfer that pin to position N+1 in the same journal change; never release first. | Keep the retry's history/pin until transfer. |
| R6, P2: sources/failure indexes bypass admission | Bound queued spans and merge backlog for every writer. | **Span backlog avoided.** Synchronous root construction has no merge frontier. All writer paths still share in-flight-byte, upload and storage admission, especially if pack relocation is enabled. | All paths must honor L0/compaction backlog admission. |
| R7, P2: rejected stale-version rewrite uploads repeatedly | Charge uploads and identify unchanged input/endpoint sets durably. | **Specific rewrite avoided.** No endpoint-version rewrite. Pack relocation can recreate the general bug; identify the physical input set and reserve attempts before upload. | Rejected/retried compactions likewise need accounting. |
| R8, P2: retry budget resets on engine restart | Persist attempt counts and exhaustion across ownership epochs. | **Still present if claiming an upload bound.** Root builds and pack relocations can be abandoned. Persist operation/input identity and reserved attempt count; no bound is claimed by this prototype. | Same durable retry-budget obligation. |
| R9, P2: `u64::MAX` doubles as no bound | Separate a valid exclusive generation bound from unbounded head. | **Avoided for snapshot selection.** Roots choose history; all generation values remain data. Cursor/root absence is explicit, never an integer sentinel. Generation allocation still must reject overflow. | Any bound API still needs explicit optionality. |
| R10, P2: hot-key versions accumulate in RAM | Stream a key's endpoint versions and payloads rather than collecting the group. | **Specific group avoided.** At most the old and new live value are compared. Large values still require byte limits and streaming/out-of-line payloads; total retained storage can be large. | A merge buffers one version per participating run, not every endpoint, but oversized values still need limits. |
| F40, zombie replay of R1 | Protect published **and still-publishing** successor output after the old collector's listing resumes. | **Handled by the same unimplemented cut/reservation rules.** A newly uploaded successor pack is outside the old cut; an older in-flight upload stays reserved until it cannot publish. Test both interleavings. | Same race class. F40 is R1's lifecycle replay, not an eleventh independent defect. |

Other cases every design must cover are empty commits, empty trees, add/remove
cancellation, absent read-ahead, re-adds, source payload equality and reversion,
renames, resets, failed publication, pin acquisition racing retirement, byte
prefixes with no finite successor, and a root split while an older root remains
pinned. The snapshot model checks the read semantics and split cases. It does
not exercise a journal, collection, restart or pack relocation.

## Alternatives and the final choice

A9 already evaluated a coarse time tree over packed deltas. It retains raw slices
for interior cut points and still needs a head index. It can reduce repeated
catch-up work, but is not a simpler single replacement for all six workloads.
An MVCC key/version LSM retains arbitrary historical lookup but recreates
multi-version groups and needs time-oriented discovery for cheap catch-up.
Full periodic snapshots plus replay simplify files at the cost of read latency
between checkpoints. None supplies a measured way around the tradeoff here.

Three snapshot variations were considered:

- Tiny leaves reduce neighbor copying but increase directory traffic and request
  count. The page-size sweep measures that tradeoff directly.
- One object per page removes pack relocation, but creates hundreds of PUTs per
  ordinary commit. Packing is necessary for a serious object-store candidate.
- A mutable delta overlay or buffered tree batches small writes before page
  copying. To serve every commit position while that buffer is live, it needs
  retained deltas, historical overlays, merge policy and cleanup. It might be a
  good index, but it has given up the tested design's main simplification.

**Keep spans**, with A17's fixes and the shipped-API remeasurement. Do not add a
second snapshot index as a default hybrid; it pays both write paths and adds a
second publication/retention lifecycle. A future immutable snapshot export for
rare bulk scans could be useful if requested, but this study supplies no reason
to build it now.

The positive result is specific: single-version snapshot roots avoid several
reader bugs, and root diff plus explicit read-ahead exceptions is an attractive
semantic model. The negative result is also specific: practical page sizes
amplify small scattered writes, many pins keep unchanged neighbors, and packing
needs cleanup machinery. There is no universal impossibility proof here. A
production tree, a different page representation or a different workload could
change the outcome. The measured candidate is not a substantially simpler,
comparable-cost replacement for Solera's current workloads.
