# One record of what each output unit saw (design note, K41/K42)

Status: **proposal**, for Erwin. Not built.

## The rule

For every output **unit**, the engine keeps which version of each upstream
input the unit last saw. The unit is the finest grain we can know: a
**key** where the per-key mapping is known (`each=True`: an output key
saw exactly its input key), else the **partition**, or the asset when it
is unpartitioned.

**One record shape, two grains.** Against a keyed upstream, what a unit
saw is a set of upstream `(key, version)` pairs. That set has a compact
name. An upstream's key index as of commit `N` is immutable, so "read as of
`N`, under these patterns" names the whole set in one number, which is
what the position already is. A partial read (a `keys=` run) adds the keys
read ahead of `N`, at their versions. So every record is **a snapshot
commit `N`, plus the keys read ahead of it**. Each and non-Each assets
differ only in grain: per key for `each=True` (a key saw its one input
key), and per partition otherwise. A non-Each partition's own keys,
whatever they are, inherit its record: the lineage runs from the partition
to a set of upstream keys with versions, not key to key.

A successful attempt updates the records of the units it committed, from
what it was actually given. The run spec does not matter (default,
`keys=`, `partitions=`, a retry pass). A unit is **stale** iff an input it
depends on now has another version than its record says, or its asset
changed since. Positions are not a second source of truth. They are the
index planning uses for incremental reads, derived from the records.

## Where each record lives (the engine stays bounded)

| Grain | Record | Where | Exists today? |
|---|---|---|---|
| partition, incremental input | the snapshot: the upstream commit through which every key it takes was seen, **the position** (`next`, and the pass under way) | partition record | yes, but moved from the plan (below) |
| partition (not `each=True`), keys read ahead | `{key: version}` read past the position by `keys=` runs; it empties when the position passes them | partition record, capped (say 10,000 keys): past the cap the partition simply stays stale until a default run | no: new, small, and only after a selection |
| partition, whole or dep input | the head generation read (a digest of the refs, for a fan-in) | partition record | folded into the position's fingerprint digest; the history's lineage has each one |
| partition, the declaration | the asset change it last caught up to | partition record | yes: `caught_up_at`, against `changed_at` |
| key (`each=True` only) | the upstream key's generation it was built from: its read-ahead entry, per key | the output key index entry's payload (v3 has one; sources use it for their version) | no: new, the per-key datum |
| key (`each=True`), shared whole or dep inputs | nothing extra: the key's own generation, the claim's event counter, against the shared input's head commit position | — | yes |

The last row needs no payload. Whole inputs are pinned at the claim, and
a key's generation `g` is the event counter at its claim. So the key read
every shared head committed before `g`, and it is stale exactly for a head
committed after `g`.

The read-ahead is the one new datum: the per-key payload for
`each=True`, the capped set in the partition record otherwise. Two edge
cases force it:

- A selection reads some keys past the snapshot, not all of them. One
  number cannot say which.
- For the per-key payload, an upstream key can change between a run's
  read and its commit. Its version is then older than the output key's
  own generation, yet the run never read it, so comparing the two
  generations would miss the change.

## How runs update records, and positions follow

Position at 56, upstream head at 60. Commits 56–60 touched `k1`, `k2`,
`k3`. A run with `keys=("k1","k2")` reads both at 60 and commits.

- **`each=True`.** `k1`'s and `k2`'s payloads now hold the generations
  read at 60, so they are not stale. `k3` still is. The position is
  derived as the first commit past which some key the patterns take is
  newer than its record, so it stays at 56. A later `keys=("k3",)`
  covers the rest: at its commit, every key the delta log holds past 56
  is current, and the position moves to 61. The check reads only the
  delta past the position. After a reset there is no position, and the
  check reads the whole upstream index once, so it is worth bounding (only
  tried when the selection is as large as the index's count).
- **Any other asset** (partition grain). The partition's record becomes
  snapshot 56 plus read-ahead `{k1: v60, k2: v60}`. `k3`'s change past 56
  is not covered, so the partition is still stale, and so are all its own
  keys, which inherit its record. A later `keys=("k3",)` adds `k3`. Now
  every key past 56 under the patterns is in the read-ahead at its
  current version, so the record collapses back to snapshot 61 with
  nothing ahead. Its stale-keys listing answers "not tracked per key"
  (K40): its keys share the partition's answer.
- **`partitions=("P1","P2")`.** Their records update as any run's do, so
  P1 and P2 are current. Today already works this way.
- **A default run** reads `from..to` in full, and the position moves to
  `to + 1`, as today.

## Staleness: a comparison against the records, rolled up with "any"

- **key** (`each=True`): its upstream key's current generation differs
  from its payload (or the key is missing, or removed upstream). It is
  also stale when a shared input's head was committed after the key's
  generation, or its asset changed after it was written.
- **partition**: one of its keys is stale (`each=True`). Otherwise a key
  under its patterns changed, appeared or disappeared upstream since the
  snapshot (K39: commits past the position holding a key the patterns
  take), and is not in the read-ahead at its current version. It is also
  stale when a whole or dep head differs from its record, or
  `caught_up_at` is before `changed_at`. Its keys inherit the answer.
- **asset**: one of its partitions is stale.

No other state is needed. A default run's dry plan is the same
comparison: the work it would do is exactly the stale part.

## What it replaces or deletes

- **Gone:** the partition record's `reset` flag, and the clause that
  promoted a `keys=` run (both on the parked `held/keys-rule` commit).
- **Gone:** the `selection` plan kind's special case in
  `positions.advance`, and K36's "a keys= run never moves a position".
- **Changed:** today a position is moved by the spec's plan
  (`engine.py`, `commit_attempt`: `positions.advance(plan, after)`). The
  plan's range and batch become what the attempt reports it read
  (`delivered`: the keys, and the commits through which it read).
- **Gone:** the fingerprint's `refs` part. Each whole or dep input's
  version becomes its own field in the partition record, so a change is a
  comparison, not a digest mismatch forcing a full pass. This is
  semantic change (d), falling out.
- **Derived:** `caught_up` (no unit stale) and `caught_up_at` (the
  declaration seen), from the records instead of set by the commit path.
- **Kept, but no longer consulted:** the history's lineage table. It stays
  the durable "what was each version built from", for users.

## Risks

- **Reset rule.** No record means everything is stale, which is correct. A
  selection then covers the partition only by naming every key: the
  bounded check above.
- **Pattern changes.** A pattern change is an asset change, so every unit
  is stale until it re-reads under the new patterns. The position's diff
  pass keeps its old/new split.
- **Retry passes.** An Each key that failed was read, so its payload
  holds what it read. Its failure record keeps it due: failing, not stale.
- **Repairs after a writer died.** A dead attempt committed nothing, so
  its reads update no record. A repair moves no record. Unchanged.
