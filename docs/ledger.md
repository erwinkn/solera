# The ledger: incremental reads from state, not history (draft)

Status: **draft** for Erwin's state-based model. Docs only; nothing is
built. If the model is not confirmed, this note is dropped.

## The model in one paragraph

Each consumer partition keeps, per keyed incremental input, a **ledger**:
key → the upstream version it actually processed — the version it was
**served**, which on a current-only store may be newer than the one the
index named when the batch was planned. Every run records what it
processed, whatever started it: a default run, a `keys=` run, a retry.
What a partition **owes** is a comparison, made on demand, of the
upstream's keys and versions under the input's current patterns with its
ledger:

| Upstream (under the patterns) | Ledger | Owed |
|---|---|---|
| has `k` | lacks `k` | an **add** |
| has `k` at `v2` | has `k` at `v1` | an **update** |
| lacks `k`, or `k` no longer matches the patterns | has `k` | a **removal** |
| has `k` at `v` | has `k` at `v` | nothing |

A default run processes exactly what is owed, and a batch's `added`,
`updated` and `removed` are those three sets. `keys=` only narrows a run
to the named keys of what is owed. A partition is stale for its input
exactly when something is owed. Patterns are applied when the comparison
is made; nothing is kept about how they changed.

## Worked examples

`feed` → `items` (keyed) → `tally`, a count kept as `before + len(added) −
len(removed)`, and `checks`, a per-key asset over `items`. Versions are
written `k1@3` (key `k1`, generation 3).

**`keys=` before a default run.** `items` holds `k1@1 k2@1`, `tally`'s
ledger is empty. `keys=(k1)`: `k1` is owed an add; the run processes it
(`added=[k1]`, count 1) and its commit writes `k1@1` to the ledger. `k1`
is then updated to `k1@4`. The default run compares: `k1` is owed an
update (`@4` against `@1`), `k2` an add. It processes both (`added=[k2]`,
`updated=[k1]`, count 2) and records `k1@4 k2@1`. Nothing is delivered
twice and nothing is missed, with no read-ahead and no cap: the ledger is
the record of what the `keys=` run did.

**A pattern widening.** `tally` reads `include=k1`; its ledger is `k1@1`.
The deploy widens to `k*`. The next comparison sees `k2@1` upstream,
matching now and missing from the ledger: an add. One run delivers
`added=[k2]`. A `keys=(k2)` run first instead does the same, and the
default run after it finds nothing owed. A `keys=(k9)` for a key that
never existed finds nothing owed and delivers nothing (A26 N2's case).

**A pattern narrowing.** `include=k*` narrowed to `k1`; the ledger is
`k1@1 k2@1`. `k2` no longer matches and is in the ledger: a removal. The
run delivers `removed=[k2]` and drops it from the ledger.

**A definition change.** `tally`'s version is bumped. Its ledger is set
aside: the partition reconciles against an empty one, so everything
upstream is owed an add, and the first batch is `full` and `first`, so the
consumer starts over. A per-key asset keeps its outputs through the start
over, as now, and the keys its output still holds that the new pass does
not name are removed after it — read from its output index, since the
ledger no longer says what it held.

**A current-only revert.** `items` lives in a current-only store. A batch
planned at `k2@2` loads `k2`'s row, and the store is already at `@3`. The
worker records what it was served, `k2@3`. The source then restores
`k2@2`. The next comparison sees `@2` upstream against `@3` in the
ledger: an update, and `k2` is processed again at `@2`. Under positions
this case nets out to nothing and leaves `v3` data behind (A26 N4).

**A same-version re-arrival.** A per-key batch finds no row for `k2`: it
was removed since the batch was planned, so its outputs go and the ledger
drops `k2`. `k2` comes back at the same version before the next run. The
comparison sees `k2` upstream and not in the ledger: an add, and `checks`
rebuilds it. Under positions the delta nets this to nothing and `k2`
stays missing (A26 N4).

**A takeover mid-run.** A run has committed batches 1 and 2 when its
engine is replaced; batch 3's attempt dies uncommitted. Batches 1 and 2's
ledger entries were committed with their outputs; batch 3's were not. The
next engine compares again and finds batch 3's keys still owed. Where a
run stopped is only a hint (its cursor); the ledger decides.

**Per-key versus plain.** For `checks` (per-key) the ledger is, per key,
the upstream version its output for that key was built from: a key with
a failure record counts as processed at the version it failed at, so a
key is retried by its failure record, not re-owed. For `tally` (plain,
unkeyed output) the ledger is the only record of which upstream keys the
count holds, and that is what makes the count exact: `added` is never a
key it holds, `removed` always one.

## Where the ledger lives, and how it commits

A ledger is a key index of its own, in the same format (key → payload,
here the served upstream generation), one per consumer partition and
keyed incremental input, under the consumer's index prefix
(`@ledger/{asset}/{input}`). A batch writes its ledger delta beside its
output deltas before its result is sealed; the engine installs the
ledger's new state in the same `AttemptFinished` that installs the
outputs' heads and indexes. So the ledger and the outputs commit together
or not at all, and an abandoned attempt's ledger delta is cleaned up like
its other delta files.

For a per-key asset the ledger could instead be the payload of its own
output index. I recommend a ledger of its own for both kinds: a per-key
call may write no output for a key (it returned nothing, or removed its
own key) and still have processed it; a multi-output asset has no single
index to carry it; and one shape for both keeps staleness and planning to
one code path. Its cost is one more delta file per commit.

Whole and dep inputs are not per key: the partition keeps their versions
beside the ledger (`seen`, as today). When one moves, every ledger entry
is owed an update, not an add: the consumer does not start over (semantic
change (d)'s rule, unchanged).

## Keeping staleness cheap

The ledger also keeps a **checked point** `C`: the upstream commit up to
which it has been compared in full. Only a key changed after `C` can be
owed, so the usual comparison reads `changes(C, head)` — the same span
read as today's delta — and looks those keys up in the ledger, and the
next comparison starts from the head the last one reached. A `keys=` run
records entries but leaves `C` as it is.

A **full compare** — a merge of the upstream index, under the patterns,
with the ledger, both in key order — is due only when the candidates
cannot tell: after a pattern change (a key's membership can change with
no upstream commit), after a definition change (against an empty ledger,
which is just the upstream's keys) and when the log no longer holds
`(C, head]`. It is two index reads and a merge. From the measured full
replacement compare (`key-index-costs.md`, one index read whole and
compared), two of them:

| Keys | Full compare | Usual comparison (1K changed) |
|---|---|---|
| 1M | ~10 GETs, ~0.2 s, ~55 MB | ~9 GETs, <0.1 s |
| 100M | ~300 GETs, ~10–15 s, ~5.4 GB | ~50 GETs, ~0.8 s (cold) |

At 100M a full compare is a run's work, not a status page's: it runs when
the next run plans, and until then the partition reports `definition
changed`, without listing its keys.

## What it replaces

| Today | With the ledger |
|---|---|
| Position: `next`, a `pass` (`from`, `at`, `batch`, `pin`), `fingerprint` | The ledger and its checked point `C`; a run's cursor is a hint. `fingerprint` stays: a change resets the ledger |
| K45 read-ahead (`ahead`), its 10,000-run cap, paged selections' shared entries | Gone: a `keys=` run writes ledger entries like any run |
| D93: a full pass reads a pinned snapshot; selections record entries, are classed via `lower=`; a covering selection collapses the record | Gone: a first run processes keys at their current version and records what it was served; what changes meanwhile is owed by the next comparison |
| D100: classes follow the index, rows the store; rowless deliveries | The class is the comparison with the ledger; the ledger records the served version, so no row is ever delivered for a key the store no longer has |
| The pattern change: old patterns' delta, a membership diff over a pinned snapshot, the old/new split | Gone: patterns apply when the comparison is made |
| `caught_up`, `caught_up_at` | Derived: nothing owed. `seen` stays for whole and dep inputs |
| Staleness: three predicates over position, read-ahead and pass state; K38's filter-then-confirm for `each` | One comparison, for both kinds |
| F35, F37, F38's early removal, A19 R1–R5, A26 N2–N5 | Cannot arise: they are disagreements between a history (position, read-ahead, pass) and the state it summarises |

Deleted from the code: `positions.py` but for the checked point;
`_selection` and its branches; `_read_ahead`, `_read_ahead_of` and
`changes(lower=)`; the full-pass snapshot and its pin; `walked`,
`read_from`, `held_at`, `pattern_change`; `_dep_restart`'s pass
bookkeeping; most of `staleness.py`. Kept: the key index, `changes()`,
the failure index, per-key reconcile against the output index, and
positions by commit for unkeyed upstreams (no keys, no ledger).

## What gets harder

- **Storage and writes.** A ledger is as large as the keys its consumer
  processed: ~27 B a key, so ~2.7 GB at 100M keys, for each consumer of
  each big upstream, and one more delta per commit. Today a position
  stores nothing per key.
- **Served versions on a current-only store** are read from the head
  index when the row is loaded. A store behind its index while still
  holding a row (an older row, not a missing one, which F33 already
  catches) records a version newer than the row: the one gap left.
- **A full compare at 100M** takes seconds and reads both indexes whole,
  so staleness after a pattern or definition change is reported without
  keys until a run makes it.
- **A per-key definition change** needs the output index to find what to
  remove, since the ledger starts empty.
- **Moving off positions** is a reset for every consumer: its ledger
  starts empty, and its first run is a full one.
