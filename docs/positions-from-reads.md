# Positions from what was read (design note, K43, K45)

Status: **approved** by Erwin (K43, amended by K45 and its read-ahead
entries). The plain incremental read-ahead is built; per-key records and
staleness are being built. It supersedes K36–K42 and K40's wording.

## Units

- An **output unit** is the grain at which an asset stores and versions
  data: a **key** within a partition if the output is keyed, else the
  **partition**. An unpartitioned asset is one partition (`""` in code,
  as today); the console and the CLI never show it or ask for it.
- An **input unit**, per input, is what one output unit can be traced
  to: **one upstream key** if the asset is `each=True`, else the
  **upstream partition(s)** the input reads.

**Dependency.** With `each=True`, output key `k` depends on input key `k`
and on nothing else of that input. Otherwise every output unit of a
partition depends on the whole input unit: a keyed output that is not
`each` has all its keys depend on the input together, since nothing says
which upstream keys made which output key.

*Example.* `file_index` (`each=True`) reads `site_files`: its key
`alpha-file-2` depends on `site_files`' key `alpha-file-2`. `tally`
(keyed, not `each`) reads `site_files/alpha`: every key of `tally/alpha`
depends on all of `site_files/alpha`.

## The rule

The engine keeps a **record of what was seen** at input-unit grain, and
every successful attempt updates the records of the units it committed
from what it **actually read**. The run spec does not matter (default,
`partitions=`, `keys=`, a retry pass).

An output unit is **stale** iff an input unit it depends on changed
after the unit was written (K39: a change under the input's patterns; a
key the patterns exclude never counts), or its asset changed since.
Roll-ups use "any": a partition is stale if any of its keys is, an asset
if any of its partitions is.

The **position is derived** from the records, as the index planning uses
for incremental reads: not a second source of truth.

## Where each record lives

| Input unit | Record | Where | Exists today? |
|---|---|---|---|
| upstream key (`each=True`) | the generation of the input key the output key was built from | the output key index entry's payload (v3 has one) | no: the one new datum |
| upstream partition, incremental input | the position: `next`, the pass under way, the patterns | partition record | yes, but moved by the plan (below) |
| upstream partition, incremental input, keys read ahead | the read-ahead: `[commit, run, attempt]` per `keys=` run since the last pass (K45) | the position, capped at 10,000 entries | yes |
| upstream partition, whole or dep input | the head generation read (a digest of the refs, for a fan-in) | partition record | folded into the fingerprint's digest today |
| the declaration | the asset change it last caught up to | partition record | yes: `caught_up_at`, against `changed_at` |

**The read-ahead** (K45 and its amendment). A `keys=` run of a plain
incremental input is delivered the delta past `next`, filtered to the keys
it names and to what the read-ahead lacks, as of one upstream commit, the
head its attempt pinned; its attempt's immutable spec already lists the
keys. So the position records only `[commit, run, attempt]` (the run
locates the spec), and stores no per-key version. The record is snapshot
`next` plus that list:

- The next pass reads the delta past `next`. A changed key is skipped if
  an entry read it at or after its last change; one that changed again
  since is delivered. Planning reads the listed specs only when the list
  is not empty, together, into one map of key to the latest upstream
  generation an entry read it at (an output partition's generations rise
  with its commit numbers).
- Once a pass moves `next`, the entries read before it collapse into the
  snapshot: after a default run the list is empty. A `keys=` run that
  leaves nothing past `next` uncovered collapses it at once: `next`
  moves to the head it read, and the list empties.
- The cap counts entries, `keys=` runs since the last pass, not keys:
  10,000 per partition, past which a `keys=` run is refused with "run the
  partition first". One run may name any number of keys.
- Retention keeps a run whose attempt an entry names, until it collapses.
  A run deleted by hand loses its entry: its keys are delivered again.
- **A full pass due runs across runs** (Erwin's correction). After an
  asset change or a reset (no position, a fingerprint change, a log that
  no longer holds the delta), a full pass is due, and any mix of runs may
  complete it. Its first delivery starts over, whether a `keys=` run or a
  default run: its first batch is full and first, so the consumer
  rebuilds (a count restarts from 0). Later `keys=` runs continue the same
  pass with their named keys, recorded like any read-ahead entry, with the
  pass as their base instead of a snapshot. A default run delivers only
  the keys the pass has not delivered and finishes it, never starting over
  again, so nothing earlier `keys=` runs wrote is dropped. Once every key
  under the patterns has been delivered within the pass at its current
  version, the asset is fresh, whichever run delivered the last piece, and
  the record collapses to a snapshot. Nothing is delivered twice within a
  pass, and K44's added, updated and removed are relative to its start:
  everything is added. *Example:* `copy` holds k1, k2, k3, and its version
  is bumped. keys=(k1) starts over with k1; keys=(k2, k3) continues, and
  `copy` is fresh. Or, after keys=(k1), a default run delivers k2 and k3
  and finishes.
- **A pattern change under way** decides membership first: a `keys=` run
  meanwhile merges the keys it names and records nothing.

`each=True` keeps per-key records in its output's key index instead, with
no cap.

An `each=True` key's shared whole or dep inputs need no payload: they
are pinned at the claim, and the key's own generation `g` is the event
counter at its claim, so the key read every shared head committed before
`g`, and is stale exactly for a head committed after it.

Why a per-key payload, and not the output key's own generation: an
upstream key can change between a run's read and its commit. Its version
is then older than the output key's generation, yet the run never read
it, so comparing the two generations would miss the change.

## Runs target only what input units allow

- `partitions=` always: a partition is an output unit or holds them.
- `keys=` on any keyed incremental input: per key for `each=True`; for a
  plain input, through the read-ahead above (K45 replaced K43's refusal).

## How runs update records, and positions follow

Position at 56, upstream head at 60. Commits 56–60 touched `k1`, `k2`,
`k3`.

- **`each=True`, `keys=("k1","k2")`.** The run reads both at 60. `k1`'s
  and `k2`'s payloads now hold the generations read, so they are not
  stale; `k3` still is. The position is the first commit past which some
  key under the patterns is newer than its record, so it stays at 56. A
  later `keys=("k3",)` covers the rest: at its commit, every key the
  delta log holds past 56 is current, and the position moves to 61. The
  check reads only the delta past the position. After a reset there is
  no position, and the check reads the whole upstream index once, so it
  is bounded: tried only when the selection is as large as the index's
  key count.
- **Not `each`, `partitions=("P1",)`.** A partition rerun reads its input
  unit whole, so P1's records update as any default run's do, and P1 is
  current. Today already works this way.
- **Not `each`, `keys=("k1",)`.** The run is delivered `k1` as of 60 and
  commits; the position becomes snapshot 56 plus `[60, run, attempt]`. The
  next default run delivers `k2` and `k3`, skipping `k1` (unless it changed
  again after 60), and collapses the list. Had the run named `k1`, `k2` and
  `k3`, nothing would be left past 56, and the record would collapse at
  once to snapshot 61. A named key that did not change past 56 is not
  delivered at all.
- **A default run** reads `from..to` in full, and the position moves to
  `to + 1`, as today.

## Staleness at every level

Staleness is **transitive and says why** (K46). A stale status carries
one or more reasons:

- **input changed**: an input unit it depends on changed since it read
  it; rerunning it helps;
- **upstream stale**: an upstream it depends on is itself stale, to any
  depth; rerun the upstream first, or run with `upstream=True`;
- **definition changed**: its asset changed (version, configuration,
  patterns, rename, added or reset) since it was written.

It is computed on demand, never stored as a flag: a project's statuses are
one walk up the lineage, memoized, each partition's direct checks or any
partition it reads being stale (`python/solera_server/staleness.py`).
*Example:* feed → items → copy. feed commits and items has not rerun:
items is stale (input changed), and so is copy (upstream stale), though
items has not moved. Automations are unchanged: `OnChange` fires on real
commits, and copy's input changes once items reruns.

As built, an incremental input is behind when a key its patterns take
changed past `next` and is not read ahead at or after its change, or when
there is no position or a pass under way; a whole or dep input, when its
version differs from the one recorded at the last catch-up (`seen`). One
case over-reports until the range scan of K44 lands: a key added and
removed past `next` counts, though it nets out.

- **key** (`each=True`): its input key's current generation differs from
  its payload (or the key is missing, or removed upstream); or a shared
  whole or dep head was committed after the key's generation; or its
  asset changed after it was written.
- **partition**: one of its keys is stale (`each=True`). Otherwise a key
  under the patterns changed, appeared or disappeared upstream past the
  position (K39); or a whole or dep head differs from its record; or
  `caught_up_at` is before `changed_at`. A keyed output that is not
  `each` has its keys share the partition's answer: all its keys are
  stale, or none. An unkeyed output has no keys (`tracked: false`).
  `GET /assets/{name}/stale-keys?partition=&after=`, `solera stale ASSET
  [PARTITION]`.
- **asset**: one of its partitions is stale.

No other state is needed. A default run's dry plan is the same
comparison: the work it would do is exactly the stale part.

## What it replaces or deletes

- **Gone:** the partition record's `reset` flag, and the clause that
  promoted a `keys=` run (both on the parked `held/keys-rule` commit,
  which this replaces).
- **Gone:** the `selection` plan kind's special case in
  `positions.advance`, and K36's "a keys= run never moves a position".
- **Changed:** today a position is moved by the spec's plan (`engine.py`,
  `commit_attempt`: `advance(plan, after)`). It becomes what the attempt
  reports it read (`delivered`: the keys, and the commits through which
  it read).
- **Gone:** the fingerprint's `refs` part. Each whole or dep input's
  version becomes its own field in the partition record, so a change is
  a comparison, not a digest mismatch forcing a full pass: semantic
  change (d), falling out.
- **Derived:** `caught_up` (no unit stale) and `caught_up_at` (the
  declaration seen), from the records instead of set by the commit path.
- **Kept, but no longer consulted:** the history's lineage table. It
  stays the durable "what was each version built from", for users.
- **Tests:** those that run `keys=` on assets that are not `each` (F17's,
  among others) move to `each=True` assets or to partition reruns.

## Risks

- **Reset rule.** No record means everything is stale, which is correct.
  A selection then covers the partition only by naming every key: the
  bounded check above.
- **Pattern changes.** A pattern change is an asset change, so every unit
  is stale until it reads again under the new patterns. The position's
  diff pass keeps its old/new split.
- **Retry passes.** An `each` key that failed was read, so its payload
  holds what it read. Its failure record keeps it due: failing, not
  stale.
- **Repairs after a writer died.** A dead attempt committed nothing, so
  its reads update no record. A repair moves no record. Unchanged.
