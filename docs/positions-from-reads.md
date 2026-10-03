# Positions from what was read (design note, K43)

Status: **approved** by Erwin (K43). Not built yet: it follows the
attempt control file. It supersedes K36–K42's read-ahead and K40's
wording.

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
| upstream partition, whole or dep input | the head generation read (a digest of the refs, for a fan-in) | partition record | folded into the fingerprint's digest today |
| the declaration | the asset change it last caught up to | partition record | yes: `caught_up_at`, against `changed_at` |

There is **no read-ahead** anywhere: no capped set, no new engine state
beyond the per-key payload for `each=True`. None is needed, because
`keys=` only exists where the input unit is a key (below).

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
- `keys=` only on `each=True` assets, the only ones whose input unit is a
  key. Elsewhere it is refused at submission: "keys= needs an each=True
  input; rerun the partition".

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
- **Not `each`, `keys=…`.** Refused at submission.
- **A default run** reads `from..to` in full, and the position moves to
  `to + 1`, as today.

## Staleness at every level

- **key** (`each=True`): its input key's current generation differs from
  its payload (or the key is missing, or removed upstream); or a shared
  whole or dep head was committed after the key's generation; or its
  asset changed after it was written.
- **partition**: one of its keys is stale (`each=True`). Otherwise a key
  under the patterns changed, appeared or disappeared upstream past the
  position (K39); or a whole or dep head differs from its record; or
  `caught_up_at` is before `changed_at`. A keyed output that is not
  `each` has its keys share the partition's answer: its stale-keys
  listing says so ("not tracked per key").
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
