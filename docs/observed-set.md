# The observed set: incremental reads from one observation record (draft)

Status: **draft**, revised after review A27 ("build with listed
changes") and W36's model of it (`ObservedSet.tla`: three
counterexamples, each fix checked in the model). Docs only; nothing is
built. Names (D133): the **observed set** is what a consumer partition
has processed, key → upstream version; its stored form is the
**observation record** — a **base**, explicit **points** and compressed
**ranges**, with disjoint overwrites and prefix membership queries —
which decodes to it. The two earlier encodings it
replaces are on this branch's history (`fb56cbe`, `bdf8962`).

## The principle

A consumer partition's **observed set** `S`, per keyed incremental input, is
key → the **observation** it processed: whether the key was present, its
version and payload as served, the whole and dep versions it was
processed under (its **context**), and the upstream's **life**. `S` is
independent of what upstream holds now.

What the partition owes is decided **key by key, once**: for each
**candidate** key, from its old effective state, `decode(R)(k)`, to its
new one, the upstream now under the current patterns and context:

| Old (`decode`) | New (upstream now, current patterns, current context) | Owed |
|---|---|---|
| absent | present | an add |
| present | absent, or no longer taken | a removal |
| present | present, another version, or another context | an update |
| same | same | nothing |

Net changes and membership queries never class anything themselves: they
only collect candidates, deduplicated, and the table above classes each
one (A27 R4). The owed set is the staleness, and exactly what a default
run loads, as `added`, `updated` and `removed`. A `keys=` run is a filter
on it. A pattern change stores nothing: it only changes the "new" side.

**The invariant.** After every commit, `decode(R) = S`. Every override is
written by a committed batch, and folded only when removing it decodes
to the same value.

## The observation record

The observation record `R` is three layers, the first that holds `k`
deciding:

1. **Points**: `k → observation`, explicit — presence, version, payload,
   the patterns it was read under, context, life, and the head it was
   read at (`observed_at`). Written where a batch's observation of `k` is
   not what a range or the base would decode: a `keys=` run's keys, and a
   current-only store serving a row other than the batch's version named,
   or none. A point pins nothing (A27 R11).
2. **Ranges**: disjoint key ranges `[lo, hi] → (T, patterns, context,
   life)`: every key in it as upstream had it at endpoint `T`, if those
   patterns take it, else absent. A pass's committed prefix. Compressed:
   one range stands for every key of it.
3. **The base**: `(P, patterns, context, life)`: every key as at endpoint
   `P`, if those patterns take it, else absent. One per partition, in one
   of two forms (below): a **commit** `P`, or a commit `C` with a
   **before-image** of what changed since `P`.

Patterns are stored normalised: no `include` is the universal include,
never an empty list (A27 R3). Contexts are stored once per partition, in
a small table that layers refer to by id.

**Overwrite.** A batch's commit overwrites exactly the keys it committed:
a range write `[a, c] @ T` splits every range it overlaps — their parts
outside `[a, c]` stay, so a replanned pass never discards an old tail —
and drops the points inside observed at or before `T`, which it
supersedes; then the batch's own points go on top. A point observed
after `T` stays over the range: the pass leaves its key alone — it is
neither delivered nor reclassified — so a pass at an older `T` never sends
a key back to a version older than one a `keys=` run already delivered.
One `AttemptFinished` carries the batch's outputs and its overwrite,
atomically (A27 R8).

**The commit check.** A batch commits only if, at its commit, its
input's life is still current, no start-over is owed (an upstream reset's
rebuild, or a definition change that reset `R`), and — for a pass's
batch — the pass it was planned under is still the partition's active
scan plan. Otherwise it is refused and writes nothing: its keys stay
owed. A `keys=` batch that starts no pass has no plan to check: its
points are exact at the head they were read at. The engine's commit has
the life check today; the observed set adds the other two.

**Outcomes.** A failed or abandoned attempt writes nothing: its keys stay
owed. A per-key batch's failed keys are processed — seen at the version
they failed at, and retried by their failure record (A19 R8), not owed. A
drained, canceled per-key batch commits its finished keys as points; its
interrupted keys are not seen, so they stay owed.

**Fold.** An override folds when the value decoded *with it removed*
equals its own. So `P = k1@1`, a range `k1@2`, and a newer selection
`k1@1`: removing the point would reveal the range's `@2`, so it stays.

**Rebase.** A default run that leaves nothing owed moves the base to the
endpoint `T` it read at, with the current patterns and context, and drops
every override that the new base, with that override removed, decodes
to. Decided by decode equality at that fixed `T`, so updates arriving
meanwhile do not hold an old base forever; they are owed next.

## Two forms of base: a commit, or a commit with a before-image

A base is normally a **commit**: `P`, decoded against the upstream's
history — so the key index keeps `P` as an endpoint. When retention is
about to cut past `P`, at a cut `C`, the base takes its second form: the
commit `C` with a **before-image** — a file of only the keys changed in
`(P, C]` (under `P`'s patterns), each with its presence and version or
payload at `P`. A key added in `(P, C]` is in it as absent; one removed,
as present at its old version. Upkeep writes it once, from the index
while `P` is still readable (index metadata, no store code), and one
journal event installs it before the cut: `P` is released, `C` becomes
the base's endpoint. Points stay as they are; a range older than the cut
is folded in the same way.

Decode reads the before-image for its keys and the key view at `C` for
everything else — the same answer `P` gave. Classify is unchanged.
Candidates: the before-image's keys, `changes(C, now)`, and a membership
query if the patterns differ from the base's. If retention later cuts
past `C` too, the next before-image adds the keys changed in `(C, C′]`,
keeping each existing key's entry at `P`. After a completed default run
the base is a plain commit again, and the file is deleted.

Its size is proportional to the keys changed in `(P, C]`, not to the
index: a reader that lags while a few keys churn holds a few entries
(~27 B each). A full file — the whole observed set — is only the degenerate
case where nearly every key changed.

The index stays reader-agnostic: retention asks for every base older
than its cut to be given a before-image first, and keeps no per-reader
snapshot. The reader owns its before-image through its observation record.

**A cut past an active pass's `T`.** The pass's committed ranges get
before-images like any range; its scan plan cannot, since its next
batches need the key view at `T` itself. So the cut **replans** the pass
at the head: a new scan plan, `T` the head, the cursor at the start (the
overwrite keeps the old ranges' tails, as in any replan). A batch in
flight on the old plan is refused at its commit by the commit check, and
its keys stay owed. A pass therefore never holds retention: one paused
for weeks is replanned, not kept.

## Observations: what was served (A27 R2)

An observation is what the store served, taken from the row itself: a
current-only store returns, with each row, its version and payload, and
says which named keys it has none for. Never a separate lookup of the
head index — a source can revert between the load and the lookup. A store
that cannot say what it served cannot back an incremental input on
current rows; its reads fail instead of guessing.

The batch's classes are then decided from the observations, before the
producer is called: planned as an update, served absent — a removal if
the key was held, nothing if not; served a version equal to what was
seen — nothing. So an aggregate's count follows what it was actually
given (a tally of 2 cannot be left holding one member). A served row the
batch's version did not name becomes a point.

## Context and lives (A27 R1, R7)

**Context.** Every layer carries the whole and dep versions it was
processed under. A key's old context is its layer's; one differing from
now owes an update. So a `keys=(k1)` run after `factor` moved to `w2`
writes `k1` under `w2` — fresh — while `k2`, decoded from a base under
`w1`, is owed; if `factor` returns to `w1`, it reverses. When the base's
context differs from now, every key the base decodes present is a
candidate, enumerated by a scan of the base even with no keyed change. A
pass records the context it reads under, and its range carries it.

**Lives.** Each layer names the upstream index's **life**: its output
home and reset count, never a name or a commit number, which repeat
across lives; a rename keeps it, with the spill and pins. An upstream
reset (removed and declared again, moved to another store) starts a new
life and deletes the old index, so the old layers cannot be decoded. The
partition then records a **rebuild**: its observed set is "the old life",
and it owes a start-over — staleness says so, as an input change — which
its next run makes: the first batch is `full` and `first`, a plain
consumer starts over, and a per-key one reconciles against its output
index. That batch's commit resets `R` to an empty base in the new life.

## Candidates, queries and their cost

For a decode-equal state with unchanged patterns and context, the
candidates are complete:

- `changes(P, now)`'s keys, outside the ranges and points;
- for each range, `changes(T, now)` within it — which also finds a key
  absent at `T` and restored since, absent at `P` too (A27 R9);
- every point's key.

Outside them a key's old and new states are the base's, unchanged. Each
is classed once. A base with a before-image adds the before-image's keys
to `changes(C, now)`'s.

**When the patterns changed**, the keys whose membership may differ are
added: those a changed pattern can match. After normalisation, each glob
has a literal prefix (the characters before its first wildcard; a regex
only a proved common prefix), which bounds it to one key range: scanned
in each layer's index state and at now, clipped to that layer's range,
in the index's byte order with the exact prefix key included (`after` and
`until` are exclusive). A pattern with no prefix — the universal include
appearing or going, `**/archive/**`, `*template*`, an unanchored regex —
bounds nothing: its query is the whole range, a full compare.

**When the context changed**, the base (and any range under the old
context) is scanned for its present keys, as above.

**Costs.** The usual comparison is today's delta read plus point lookups.
A changed prefix pattern costs the keys under the prefix in each view. A
full compare reads the upstream at `P` and now, merged with the
overrides: two key views, streamed (bounded memory, not bounded reads);
at 100M keys several GB, run when a run plans. Endpoints retained: `P`
and each range's `T` — one per range, and ranges are few (a pass, its
replans until a rebase); points add none. Each range is one `changes`
call over its key range; points are one key-list call.

**Freshness is exact or pending** (A27 R10). Staleness is the comparison,
computed and cached per observation record revision and upstream head; where a
full compare is due and not yet made, the partition reports **pending**,
never `stale` or fresh on a guess: widening `include` to a key that
never existed changes no debt. Shared, definition, retry, reconcile and
rebuild debt are explicit, an empty first pass included. An `each` chain
intersects the upstream's owed keys with the consumer's patterns, key by
key, and rolls up with "any".

## Bounds and spill (A27 R5, R11)

Points and ranges live in the partition's record up to a bound (1,000
overrides, say). Past it they **spill** to an overrides index under the
consumer's index prefix, committed with the batch: each entry the whole
override — kind, presence, version, payload, the patterns and context it
was observed under (by id), life. Absence is a live, tagged payload, not
an index tombstone, which lookups skip and merges drop; an index deletion
only removes an override. Nothing is refused. Folding streams, and a
rebase rewrites the spill once, not once per batch. The worst case is
explicit: `M` `keys=` keys left behind by a moving source are `M`
points, `M` values, and no retained endpoint.

## The active pass and its pin (A27 R6)

A pass under way has a **scan plan** apart from its committed prefix: its
identity, its `T`, its cursor, and a durable reader pin on the row objects
it reads (a range kept only for decoding needs `T`'s metadata, not its
rows). The pin is taken when the pass starts — a `keys=` run that starts
one included, before its first prefix commits — and held through claim
ends, takeovers and renames until the pass ends, is reset, or is
replanned by a retention cut past its `T` (above). The pin protects the
rows the plan reads; it never holds the index's retention.

## Worked examples

`feed` → `items` (keyed) → `tally`, a count `before + len(added) −
len(removed)`, and `checks`, per-key over `items`. `k1@3` is key `k1` at
generation 3; `⊥` an empty base.

**`keys=` before a default run.** `R = ⊥`. `keys=(k1)` at head `H1`:
point `k1@1`. `k1` updated to `@4`. Candidates: everything since `⊥`, and
`k1`: `k1` is owed an update (`@4` against `@1`), `k2` an add. The run
rebases at its `T`: `R = (T)`, the point folded.

**Widening.** Base `(P, include=k1)`; the deploy widens to `k*`. The
membership query over prefix `k` finds `k2`: decoded absent, present and
taken now: an add. After `keys=(k2)`, a point `k2@1` under `k*`: decoded
present at the same version — nothing, not added twice. `keys=(k9)`, a
key that never existed: decoded absent, absent now — nothing, and no
point.

**Narrowing.** Base `(P, include=k*)`, narrowed to `k1`: the query over
`k` finds `k2`, decoded present, no longer taken: a removal.

**A first include (A27 R3).** No `include` (universal) over `keep/a`,
`drop/b`; the deploy adds `include=keep/*`. The universal include going is
a prefixless change: the whole range is queried, and `drop/b` is owed a
removal. Querying only `keep/` would have missed it.

**An add during a narrowing (A27 R4).** The base's patterns took `drop/*`,
which had no key; `drop/b` is added upstream while the current patterns
exclude `drop/*`. `drop/b` is a candidate (a change since `P`): decoded
absent, excluded now: nothing. Classing the change under the old patterns
would have owed an add of an excluded key.

**A definition change.** `R` resets to an empty base: everything upstream
is owed an add, and the first batch starts the consumer over; a per-key
asset's leftover outputs are found in its output index.

**A full pass, part done.** From `⊥`, a pass at `T` commits `k1`: a range
`[−∞, k1] @ T` over the base. `k2` changes meanwhile; the pass reads it
at `T`, held by its pin. Done: the range spans everything and rebases to
`(T)`. The change after `T` is in `changes(T, now)`: owed once.

**A replan.** A pass committed `[−∞, k2] @ T1`, then is replanned at `T2`
and commits `[−∞, k1] @ T2`: the overwrite splits the old range, keeping
`(k1, k2] @ T1`. The tail is never lost.

**A cut during a pass.** A pass at `T = 1` has committed `k1`: a range
`[−∞, k1] @ 1`. `k2` changes at commit 2, and retention cuts at 2. The
range gets its before-image (empty: nothing in it changed), and the pass
is replanned at the head, 2. A batch for `k2` planned at 1 and still in
flight is refused at its commit; the replanned pass delivers `k2` at 2.
Kept at 1, its next batch would have needed the key view at 1, gone.

**A selection ahead of a pass.** A pass is at `T = 1`; `k1` is updated to
`@2`, and `keys=(k1)` delivers `@2`: a point `k1@2`, observed at 2. The
pass then reaches `k1`. The point was observed after its `T`, so the pass
leaves `k1` alone, and its range write keeps the point. Without the
`observed_at` test it would deliver `k1@1`, sending the consumer back.

**A shared input moves (A27 R1).** Base under `factor=w1`; `factor`
becomes `w2`. Every key the base decodes present is owed an update.
`keys=(k1)` writes `k1` under `w2`: fresh, `k2` still owed. `factor`
back to `w1`: `k1` owed, `k2` fresh.

**The current-only revert (A27 R2).** A batch names `k2@2`; the store
serves the row `@3`, saying so: the class is decided from `@3`, and a
point `k2@3` is written. Restored to `@2`: `@3` against `@2`, an update.
A row served ahead of every commit is a point all the same: no commit has
to describe it.

**The same-version re-arrival.** The store has no row for a planned `k2`:
for a plain batch, a removal if `k2` was held; for a per-key one, its
outputs go. Point `k2 absent`. `k2` returns at the same version: decoded
absent, present now: an add.

**A reader paused 40 days, a 30-day window.** `tally`'s base is commit
`P`, day 0, holding `k1@1 k2@1 k3@1`; its automation is disabled, and
the index keeps 30 days of history. Upstream: `k2` updated on day 10,
`k3` removed on day 20, `k4` added on day 35. On day 29, retention's next
cut `C` would pass `P`: upkeep writes the before-image of what changed in
`(P, C]` — `k2@1`, `k3@1` (present), two entries, not the whole set —
installs it, and the base is `C` with it; `P` is released. On day 40
`tally` runs. Candidates: the before-image's `k2` and `k3`, and
`changes(C, now)`'s `k4`. Classed as always: `k2` `@1` against `@7`, an
update; `k3` present against absent, a removal; `k4` absent at `C`
against present, an add; `k1`, in neither, unchanged. The run completes:
the base is the commit it read at, and the file is deleted. Without it,
day 40 would find `P` gone and owe a start-over.

**An upstream reset (A27 R7).** `items` is moved to another store; its
old index goes. `tally` records a rebuild: owed a start-over, not a
decode it can no longer make.

**A takeover mid-run.** Batches 1 and 2 committed their overwrites;
batch 3's attempt dies, writing none. The next engine decodes `R` to `S`,
and batch 3's keys are owed; the pass's pin and `T` held throughout.

**Per-key versus plain.** One observation record for both. For `checks`, failed keys are
seen, retried by their failure records. For `tally`, `S` is the only
record of what the count holds: `added` is never a key it holds,
`removed` always one.

## Each past finding is a decode mismatch

| Finding | Why it cannot recur |
|---|---|
| A19 R1, R2: a key added twice | it decodes present: an update or nothing |
| A19 R3: a delivered removal forgotten | the removed key decodes present until a run removes it |
| A19 R4, A26 N2, N3: pattern-time selections | a selection writes points under the patterns it read under; folds are decode-equal; candidates are classed once |
| A19 R5, A26 N4, F38: served rows | observations come from the row; classes follow them before the callback |
| A19 R7, F37: shared inputs | context per layer; scanned when it moves |
| A19 R9, R10, F35: staleness | one comparison, exact or pending; `each` chains intersect key by key |
| F41: a position past a removal it never delivered (`items` over a current-only `feed` going `{…, k11}` → `{k11}` → `{k1, k3}`: read through commit 2, still holding `k11`, fresh) | nothing moves past an observation: `k11` stays in `S` as served until a batch delivers its removal, and the next comparison classes it from that decoded state against upstream now — an owed removal |
| A26 N1: the pass's rows | the scan plan holds its pin from its start |
| A26 N5: the cap | there is none: overrides spill |
| A27 R3, R4, R8, R9 | normalised patterns; candidates classed once; disjoint overwrite and decode-equal fold; per-range changes |
| `ObservedSet.tla` 1 (P1, W36): a retention cut past an active pass's `T` (a pass at `T = 1` commits `k1`; `k2` changes at 2; the cut at 2 gives the base and ranges their before-images, and the pass's next batch, `k2`, needs the key view at 1, gone) | the cut replans the pass at the head; a batch in flight on the old plan is refused by the commit check, its keys owed; no pass holds retention |
| `ObservedSet.tla` 2 (P2, W36): a pass sends a key back (the pass is at `T = 1`; `k1` is updated to `@2`; `keys=(k1)` delivers `@2`; the pass reaches `k1` and would deliver `@1`) | points record `observed_at`; a pass leaves alone a key whose point was observed after its `T`, and its range write keeps that point |
| `ObservedSet.tla` 3 (P1, W36): a batch committing across a change it was not planned under (its pass replanned or ended, an upstream reset, a definition change) | the commit check: the active scan plan, the current life, no start-over owed |

A19 R6 (`keys=` bounded by `batch_size` and `concurrency`) and R8 (a
forced retry) are kept as they are, outside the observation record.

## What it replaces

| Today | With the observed set |
|---|---|
| Position: `next`, `pass` (`from`, `at`, `batch`, `pin`), `fingerprint`, `began`, `seen` | The base; a pass's committed prefix as ranges, its scan plan apart; `fingerprint` stays: a change resets `R` |
| K45 read-ahead (`ahead`), its cap, paged selections' shared entries | Points, spilling past a bound |
| D93: snapshot passes; selections classed via `lower=` | A pass reads at its `T`, held by its pin; selections write points; `decode` replaces `lower=` |
| D100: classes from the index, rows from the store; rowless deliveries | Classes from what was served, decided before the callback |
| The pattern change: old patterns' delta, membership diff, old/new split | Normalised patterns per layer; prefix membership queries; one classification |
| `caught_up`, `caught_up_at`, `_dep_restart` | Derived: nothing owed; context per layer |
| Staleness: three predicates; K38's filter-then-confirm | One comparison, exact or pending |

Deleted: `_selection` and its branches, `held_at`, `walked`, `read_from`,
`pattern_change` and the `diff` mode, `_read_ahead`, `_read_ahead_of`,
`changes(lower=)`, `READ_AHEAD_FULL` and the cap, `_dep_restart`,
`caught_up`, most of `staleness.py` and `positions.py`, D100's rowless
deliveries and `gone_since`'s early removal. Kept: the key index and
`changes()`, endpoint reservation and reader pins, the failure index and
retries, per-key reconcile, D111's bounds, positions by commit for
unkeyed upstreams.

## Testing

The staleness reference keeps `S` literally: a dict, key → observation,
updated by each step as the model says. A property test runs random
histories — upstream commits and resets, current-only rows served ahead,
behind or not at all, `keys=` and default runs, cancels and failures,
pattern changes (first include, prefixless globs) and definition and
shared-input changes, replans, takeovers, renames — and after **every
commit** checks that `R` decodes to the dict, and that staleness and a
default run's load equal the dict's comparison with the upstream. The
A19, A26 and A27 histories are named examples, and so are
`ObservedSet.tla`'s three counterexamples and F41 (W38's
replay in `tests/sim/test_replays.py`, a strict xfail in the old model). Before relying on the
bounds: measure a large spill and a pattern change at 100M.

## What gets harder

- **Stores of current rows must say what they served**, per row; one that
  cannot fails its reads for incremental consumers instead of guessing.
- **A full compare at 100M** — a prefixless pattern change, a moved shared
  input, a truncated log — reads two whole key views; until a run makes
  it, the partition is pending.
- **An upstream reset** is a start-over for its consumers.
- **Moving to it** resets every consumer once: an empty base, and a full
  first run.

## What we build

**Records** (in the consumer partition's record, per keyed input):

```text
Context   = {id, whole_and_dep_versions}
Layer     = {endpoint, patterns (normalised), context: id, life}
Base      = Layer                                   # endpoint P: a commit
          | Layer + {before_image}                  # endpoint C; the keys changed in (P, C], as at P
Range     = Layer + {lo, hi}                        # endpoint T; disjoint, sorted
Point     = {key, present, version, payload, patterns, context: id, life, observed_at}
ScanPlan  = {id, T, cursor, patterns, context: id, pin}  # the active pass only
ObservationRecord = {base, ranges[], points{}, contexts[], spill: index state | None,
                     scan_plan: ScanPlan | None, rebuild: life | None}
```

**Decode** — the observed set, from an observation record (the old
effective state of `k`):

```text
decode(R, k):
    if R.rebuild:                      return UNKNOWN          # a start-over is owed
    if k in R.points (or its spill):   return R.points[k] as an observation
    layer = the range holding k, else R.base
    if not layer.patterns.take(k):     return ABSENT
    if layer.before_image and k in layer.before_image:
        entry = layer.before_image[k]                      # as at P
        entry = entry if entry.present else None           # a key added since P: absent then
    else:
        entry = index(layer.life).lookup(k, at=layer.endpoint)
    return ABSENT if entry is None else (entry.version, entry.payload, layer.context)
```

**Classify** (one candidate, once):

```text
classify(R, k, now):                  # now: head, current patterns, current context
    old = decode(R, k)
    if old is UNKNOWN:                          return REBUILD   # the whole partition starts over
    new = ABSENT if not now.patterns.take(k) else index.lookup(k, at=now.head)
    if old is ABSENT and new is ABSENT:         return NOTHING
    if old is ABSENT:                           return ADDED
    if new is ABSENT:                           return REMOVED
    if differs(old.version, old.payload, new):  return UPDATED   # the net rule
    if old.context != now.context:              return UPDATED
    return NOTHING

owed(R, now) = {k: c for k in candidates(R, now) if (c := classify(R, k, now)) != NOTHING}
# candidates: changes(endpoint, now), plus a before-image's keys; plus per-range changes,
# point keys, membership and context scans
```

A batch then loads its keys — less any whose point was observed after
its plan's `T` — turns each served row into an observation, re-runs
`classify` with the observation as `new`, calls the producer with those
classes, and commits its overwrite — ranges and points — with its
outputs, if the commit check passes:

```text
may_commit(batch, R, now):
    if batch.life != now.life:                        return False   # the input's life is current
    if R.rebuild or now.start_over_owed:              return False   # no start-over owed
    if batch.plan is None:                            return True    # a selection's points
    return R.scan_plan is not None and R.scan_plan.id == batch.plan  # still the active pass
```
