# Versions

Status: **built.** Decided by Erwin. It replaced content hashing (the
row digest): nothing hashes user data, and nothing reads data back from a
store to version it.

Words (`glossary.md`): a **version** tells whether something changed,
read by its subject. An asset's or a store's version is a code version,
bumped by hand. A key's version is the **generation** that last wrote it
— unless the key is a source's and the source says its own (an etag, an
`updated_at`), which is only ever compared. An unkeyed source's version
is one string.

Every concept below carries, next to it, the edge case or user
convenience that justifies it. A concept without one does not belong
here.

## 1. The model

A key is identified by `(output, partition, key)`. **Its version is the
generation of the write that last wrote it.** The generation is what
`lifecycle.md` §9.7 already defines: the event counter of the writing
attempt's claim, carried in its spec, larger for every later attempt on a
partition. The index already stores it as each entry's locator, the name an
immutable store gives the key's object, so the version adds nothing new.

**Change detection is "was it written".** Any write of a key is a change
of that key. An asset runs only on changed inputs, so what it writes is
new by construction.

```
index:  a@g100  b@g100
attempt g140 returns Patch([a, c], remove=[b])
delta:  a@g140 (changed)  c@g140 (added)  b deleted
```

A producer that rewrites everything every run makes its consumers
reprocess everything. That is accepted: to avoid it, the producer returns
a `Patch` of the keys that changed, or leaves an output out of its
`Result`, which writes nothing.

The same holds for a whole output partition: its version is the generation of
the last commit that changed it. A ref carries its `generation` (one
concept, not two); a store builds refs without it and the worker stamps
the attempt's.

## 2. Sources own their change detection

A source is fed from outside, so no attempt wrote its keys and no
generation says when they changed. A source commit takes the engine's
event counter as its generation, like an attempt's claim, and then:

- **A version per key** (an etag, a ctag, an `updated_at`) may come with
  the commit: `observe()` returning `{key: version}`, `solera commit
  --upsert '{"a.csv": "c7"}'`. The engine stores it as the key's index
  entry payload. A key whose version equals the stored one is unchanged;
  any other key the commit names is changed, at the commit's generation.
  This holds wherever versions come, a full map or a patch: the same
  version means the same content. The key stays the key (`a.csv`).
  *Edge case:* a sensor that lists a 1M-file folder every five minutes
  must not mark 1M files changed every five minutes when two moved.
- **No version:** every key the commit names is changed. A full
  tick without versions therefore marks every key changed, by
  convention.
- **A source that already knows its changes** (`Observed(upsert, remove)`,
  a delta feed, `--upsert '["a.csv"]'`) needs no versions: the keys it
  names are its changes.
- **An unkeyed source** is one version (`--version`, `observe() ->
  str`), kept on its head: equal to the head's, no commit; else a commit
  at a new generation.
  *Edge case:* a sensor polling `max(updated_at)` every minute.

```
stored:   a.csv version "c7"   b.csv version "c3"
observe → {a.csv: "c7", b.csv: "c4", d.csv: "c1"}            a full map
delta:    b.csv@g210 (changed)  d.csv@g210 (added)            a.csv unchanged
```

**Dynamic partitions** reuse the version: an element's version is empty,
since membership is all an element holds, so listing it again changes
nothing.
*Edge case:* the demo's `sites` re-lists every site on every cron run;
without this, each run would wake every consumer of the set.

## 3. The index entry

The `.kx` entry collapses (format version 3; no deployment, so no
migration):

```
before   (key, version, deleted, locator)        + predecessor (version, locator)
v3       (key, generation, deleted, payload?)    + predecessor generation
```

- `version` and `locator` merge into `generation`. *Why:* they were two
  fields for one fact once versions stopped being digests.
- `payload` is one optional opaque field the index's kind interprets: a
  source key's version, or a failed keys's failure record
  (`per-key-processing.md` §9). A flag bit, then length and bytes; an
  upsert carrying a payload equal to the live entry's is unchanged.
  *Edge case:* §2's full ticks; and one field, not two, for the
  failed keys, which needed one of its own.
- The predecessor is the generation the change superseded. *Edge case:*
  an immutable store cleanups the superseded object by name,
  `{key}/{generation}`.
- **Filters: keys and tombstones; the pair filter went.** It answered "is
  `(key, version)` live?", which no write asks any more: a written key is
  a change whatever the index holds, and the only comparison left, a
  source's versions, reads the entry or streams the index. The sparse
  reader keeps its two answers: no key filter matched means new; a key
  filter match with no tombstone match, for an upsert without a payload,
  counts as an update (inexact on a false positive, as before); anything
  else gets an exact lookup.

The resolver, the engine cache, compaction and `.kg` garbage files keep
their roles with the smaller entry. A delta needs the index only to tell
added from updated (the count), to find predecessors, and, for a
replacement, which live keys it leaves out.

## 4. Writes and stores

`Store.prepare` reads a keyed write once, as today, but only its key
column: natively from Python rows, DataFrames and Arrow, sorted and
grouped by key (`Rows`). Nothing else of a row reaches native code.

A store still:

1. writes each upsert's rows, deletes each remove, clears the partition for a
   whole write;
2. is `immutable` or `fenced` (`stores.md`);
3. if immutable, names a key's object `{output}/{partition}/{key}/{generation}`
   and cleanups by `("key", key, generation)`;
4. if fenced, keeps the fence row's `written` generation and offers
   `reads()`;
5. if fenced, answers **`keys(ref, among)`**: the keys present in the
   partition, sorted, among the given ones or all of them, never a value.
   *Edge case:* repair, §5;
6. for an opaque write (`Opaque`, which it reads itself: Postgres's
   `Sql`), reports the keys it wrote, sorted.

A store no longer computes a version, declares `stamped` columns, or
refuses values its database would round: Postgres may coerce a data
column, and declared columns are the user's contract. **A key is the one
exception**: a stored key's canonical text must equal the indexed key,
or the write fails (`key '1.0' would be stored as another key in column
id`). PostgresStore checks it after each chunk of a keyed write, by counting
the chunk's distinct `key::text`; nothing is hashed. *Edge case:* an index
listing `1.0` over a row stored as `1` would hand every reader a key the
store cannot find. Kind inference for a table a write creates stays: it
types columns, it does not version them.

**`revision=` is deleted** from `Output`, with everything that renders,
checks or reads back a revision column. *Why:* its one use was to say
"unchanged" for rewritten keys, which "was it written" no longer asks.

**One write per key per generation.** An immutable store names a key's
object by generation, so an attempt writes a key at most once (or the
same bytes again, a retried call). *Why:* it is what makes a name
immutable; the conformance kit holds stores to it.

## 5. Repair after a crash

Only fenced stores repair: an immutable store's dead writer leaves only
names no entry points at. An attempt that took its gate and died leaves
its intents, the keys it meant to change. The next attempt on the partition
acquires the fence, so the dead writer can write nothing more, then:

- a key it writes or removes itself ends as it says;
- every other intended key: the store says whether it is present
  (`keys(ref, among)`). Present: the key takes the new attempt's
  generation, so it counts as changed. Absent and live in the index: a
  tombstone. Absent and not in the index: nothing.

```
index         k absent (a new key)
attempt g12   inserts k, dies after its gate; the insert did or did not land
attempt g15   k present → k@g15         k absent → nothing
```

*Edge case for the presence check:* without it, g15 would mark `k` live
whether or not the insert landed, and the index would list a key with no
rows, counted, and handed to every full pass. It reads keys, never
values.

**Unknown writes.** An opaque write's gate names no keys. If it dies, its
next write must cover every key: a replacement or opaque write rewrites
the partition, and every key it writes is at its generation anyway; a patch
first reads every present key (`keys(ref, None)`), gives each the new
generation and tombstones live keys the store lacks. Either way consumers
take everything again.

**A repair always writes.** The repairing attempt runs its store write
transaction even when its own write is empty, so the fence row's
`written` becomes its generation. *Edge case:* §7, break 1.

## 6. Lineage

Lineage records **the generation read**, per input partition:

- an immutable store's read is the pinned generation, exactly;
- a fenced store's read is the fence row's `written` at the reader's
  snapshot (`reads()`);
- `uncommitted` when no commit of the partition has that generation;
  *Edge case:* a reader saw a dead attempt's write;
- a fixed pass over several batches (a delta pass, a pattern change) records the
  generation the pass was cut at, persisted with the pass, not
  the head's when a later batch is read. *Edge case:* a delta pass cut at g2
  whose second batch is read after g3 committed read g2's content;
- an external source, read current with no fence, records the generation
  of the tick the attempt was pinned to. No new marker: external
  sources are read current by definition. A key its index names that the
  source no longer holds fails the load, retryably, with the key and its
  version (F33): nothing is delivered until the source's next commit
  removes or restores it.

`ctx.load` reads are not lineage edges: they read no input of the
attempt's. One moment reads a partition once, so the first read of a partition is
what lineage records; there is no "mixed" marker. The per-key versions a
current read used to report, computed by hashing what it loaded, are
gone. The pin stays internal: the engine uses it to deliver, and lineage
shows it only as debugging detail.

```
B pins A@g10 and reads A's partition in Postgres while A's attempt g12 writes it
lineage:  B ← A, generation 12                    (g12 committed)
          B ← A, generation 12, uncommitted        (g12 died; the repair is g15)
```

## 7. Edge cases checked

| Case | What happens | Holds |
|---|---|---|
| A rewrites `k` while B reads it | Immutable: B reads the pinned object, then `k` again with A's delta. Fenced: B may read A's new rows and records generation 12; A's commit, or the repair of its dead attempt, puts `k` in a delta B receives later, and B rereads | yes |
| Identical rewrites | Every key rewritten is a change; consumers reprocess. A whole input rewritten identically changes its ref's generation, so it resets its consumers' incremental inputs (the fingerprint holds input refs): a full redelivery | accepted |
| `version=` bump | The fingerprint changes, the asset's inputs reset, every key is reprocessed and written at a new generation, so consumers reprocess too. (Revision outputs used to hide this; they are gone.) A cursor producer with no inputs reprocesses nothing, as today | yes |
| Deploys | The deploy number moves; only failed `Each` keys get their one try, and those that succeed are written at a new generation | yes |
| Retries | A new attempt has a new generation; an uncommitted attempt's delta files and objects are cleaned up. A store call retried inside one attempt rewrites the same names with the same bytes | yes |
| `Each` full redelivery (truncated log, reset) | Every key is processed and written again; its consumers reprocess everything | accepted |
| Pattern change | Newly matched keys are delivered at their generation; unmatched ones removed | yes |
| Unknown opaque writes | §5: a rewrite, or a key scan before a patch | yes |
| Store move | A reset: the moved output is a new one (object-store-state.md §2) — no head, a fresh index, a whole first write, every key at a new generation; its consumers and its own inputs start over | yes |
| Rename | Index entries, generations and object names stay | yes |
| Failed keys retry, upstream changed | The failure record's upstream generation differs from the key's in the pinned input, so the key comes with the delta pass instead (`per-key-processing.md` §9) | yes |

**Breaks with the model as Erwin stated it.** One:

1. **A repair that writes no rows leaves lineage saying `uncommitted`
   forever.**

   ```
   attempt g12   writes k to Postgres (written = 12), dies after its gate
   attempt g15   patches nothing of its own; repairs k → k@g15 in the index, writes no rows
   reader C      reads the partition later: written = 12 → "generation 12, uncommitted"
   ```

   C read content that g15 committed, and lineage says nobody did, until
   the next write. Fix, already in §5: the repair always runs its write
   transaction, stamping `written = 15`. It adds no state: `fence(...,
   write=True)` exists.

## 8. What is deleted

| What | Where |
|---|---|
| The row-digest grammar and doc | `docs/row-digest.md`, `native/src/digest.rs`, `tests/sdk/test_row_digest.py` |
| Value encoders | `native/src/pyvalue.rs` and `arrow.rs`, all but reading a key (`str` or `int`) |
| Group and content digests | `rows.rs` (`content`, digest `Versions`), `group_digest`, `Rows.digest`, `Rows.values`' hashing |
| `revision=` | `Output`, `Rows`, `TableRef`'s handle, the `Sql` revision path, `Engine._rendered`, demo's `site_files` |
| Store-computed versions | `Prepared.version`, `KeyedWrite.version`, `stores._digest`, FileStore's value hash and batch chain, PostgresStore `_rows_version` and its `Sql` statement digest; `Ref.version` |
| `exclude` and `stamped` | `prepare`, `prepare_for`, `Rows`, PostgresStore |
| Postgres exactness checks | `_check_types`, `_kept`, `_digested`, their tests |
| Read-backs | `_sorted_rows`' every-column branch; `worker._repair`'s load and hash; `_reconcile`'s row scan |
| Read-time hashing | `observed.py`'s per-key versions; lineage `keys` and `key_count` |
| The pair filter and the version field | `.kx` format, `.kxl` local form, `_python.py`, the sparse reader's pair step |
| Lineage `mixed`, per-read pinning | `observed.py` |
| Benchmarks of digesting | `bench/keys/digest.py`, `bench/keys/pyrows.py` |

Hashing that stays, none of it over user data: key and tombstone
filters, file and payload checksums, the fingerprint, the project
revision.

## 9. Tests

- **Unit.** Entry format v3 round trips in Rust and `_python.py`; every
  write is a change; a retried store call in one attempt gives the same
  delta bytes; source versions: equal unchanged, different changed, absent
  changed; set elements re-listed unchanged; immutable names and
  cleanups by generation; `keys(ref, among)` in the store conformance
  kit; the repair example of §5 both ways; a dead opaque writer then a
  replacement, then a patch.
- **The simulation** (`tests/sim`, `verification.md`). Its invariant
  "Reads say what they read" drops the per-key part: a read reports the
  generation that wrote the partition. Add one invariant and one check:
  - after every step, for a fenced partition with nothing owing a repair, the
    index's live keys are exactly the store's present keys;
  - at convergence, every lineage generation that some commit settled is
    not reported `uncommitted` (break 1).

  The simulation's own oracle already checks convergence of every
  output; it must keep passing with every producer rewriting whole.

## 10. How it was built

One implementation worker, in this order, merged when `tests/` and
`tests/sim` passed. Interfaces first: the v3 entry, `Rows` (keys only),
`Store.keys`, `Ref.generation`.

1. **Key index and sources:** format v3 and the pair filter's removal
   (Rust, `_python.py`, `.kxl`); the delta rule (written means changed,
   versions compared); source commits' generation and versions; sets' empty
   version; rendering a version as its generation.
2. **Stores and native:** `Rows` keys-only; delete digests and encoders;
   FileStore and S3Store names by generation; PostgresStore without
   exactness checks or `stamped`, `scan` becoming `keys`, `Sql` reporting
   keys; `revision=` out of `Output`; the conformance kit.
3. **Lifecycle and lineage:** `_store_outputs` and `Ref.generation`;
   repair by presence and the always-write rule; unknown opaque writes;
   `observed.py` and lineage `{generation, uncommitted?}`; `Each`
   (`ctx.revision` and the failure entry's `revision` become the
   upstream `generation`); the simulation's invariants.

Tests for the review's four sequences, `tests/server/test_versions.py`
and `test_lineage_reads.py`: a delta pass's lineage across batches and a
later commit; a failure record across an engine restart with a skewed
clock; repair keeping or dropping a dead writer's key; an external
table's lineage at its tick. Postgres key identity:
`tests/sdk/test_postgres.py`.

Then the docs: delete `row-digest.md`; update `architecture.md` §3,
`object-store-state.md` §5–§7 and §9, `stores.md`, `per-key-processing.md`
§6–§7 and §9, `resolved-commits.md` §3 and §6, `lifecycle.md` §9.6 and
§9.8, `key-index-format.md`, `verification.md`, and the README's "an
unchanged commit wakes nothing" (true now only for sources with versions,
sets, and producers that write nothing).

## 11. Notes

1. **Source versions in patches.** §2 compares a version wherever one
   comes, full map or patch: one rule, confirmed by Erwin.
2. **`--full` on a source-fed asset** rewrites everything at new
   generations and wakes all consumers. Accepted by the model; noted
   because users may run it more than they expect.
