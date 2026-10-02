# Versions

Status: **target design, not built.** Decided by Erwin. It replaces
content hashing (`row-digest.md`, deleted by this design): nothing hashes
user data, and nothing reads data back from a store to version it.

Every concept below carries, next to it, the edge case or user
convenience that justifies it. A concept without one does not belong
here.

## 1. The model

A key is identified by `(output, partition, key)`. **Its version is the
generation of the write that last wrote it.** The generation is what
`lifecycle.md` §9.7 already defines: the event position of the writing
attempt's claim, carried in its spec, larger for every later attempt on a
scope. The index already stores it as each entry's locator, the name an
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

The same holds for a whole output scope: its version is the generation of
the last commit that changed it. `Ref.version` goes; a ref carries its
`generation` (one version concept, not two).

## 2. Sources own their change detection

A source is fed from outside, so no attempt wrote its keys and no
generation says when they changed. A source commit takes the engine's
event position as its generation, like an attempt's claim, and then:

- **A token per key** (an etag, a ctag, an `updated_at`) may come with the
  commit: `observe()` returning `{key: token}`, `solera commit --keys`
  with a map. The engine stores it on the key's index entry. A key whose
  token equals the stored one is unchanged; any other key the commit
  names is changed, at the commit's generation. The key stays the key
  (`a.csv`); the token is only compared, never shown as a version.
  *Edge case:* a sensor that lists a 1M-file folder every five minutes
  must not mark 1M files changed every five minutes when two moved.
- **No token:** every key the commit names is changed. A full observation
  without tokens therefore marks every key changed, by convention.
- **A source that already knows its changes** (`Observed(upsert, remove)`,
  a delta feed, `--upsert`) needs no tokens: the keys it names are its
  changes.
- **An unkeyed source** is one token (`--version`, `observe() -> str`):
  equal to the head's, no commit; else a commit at a new generation.
  *Edge case:* a sensor polling `max(updated_at)` every minute.

```
stored:   a.csv token "c7"   b.csv token "c3"
observe → {a.csv: "c7", b.csv: "c4", d.csv: "c1"}            a full map
delta:    b.csv@g210 (changed)  d.csv@g210 (added)            a.csv unchanged
```

**Partition sets** reuse the token: an element's token is empty, since
membership is all an element holds, so listing it again changes nothing.
*Edge case:* the demo's `sites` re-lists every site on every cron run;
without this, each run would wake every consumer of the set.

## 3. The index entry

The `.kx` entry collapses (format version 3; no deployment, so no
migration):

```
today    (key, version, deleted, locator)        + predecessor (version, locator)
target   (key, generation, deleted, token?)      + predecessor generation
```

- `version` and `locator` merge into `generation`. *Why:* they were two
  fields for one fact once versions stopped being digests.
- `token` is present only on source and partition-set entries (a flag
  bit, then length and bytes). *Edge case:* §2's full observations.
- The predecessor is the generation the change superseded. *Edge case:*
  an immutable store discards the superseded object by name,
  `{key}/{generation}`.
- **Filters: keys and tombstones; the pair filter goes.** It answered "is
  `(key, version)` live?", which no write asks any more: a written key is
  a change whatever the index holds, and the only comparison left, a
  source's tokens, reads the entry or streams the index. The sparse
  reader keeps its two answers: no key filter matched means new; a key
  filter match with no tombstone match counts as an update (inexact on a
  false positive, as today); anything else gets an exact lookup.

The resolver, the engine cache, compaction and `.kg` garbage files keep
their roles with the smaller entry. A delta needs the index only to tell
added from updated (the count), to find predecessors, and, for a
replacement, which live keys it leaves out.

## 4. Writes and stores

`Store.prepare` reads a keyed write once, as today, but only its key
column: natively from Python rows, DataFrames and Arrow, sorted and
grouped by key (`Rows`). Nothing else of a row reaches native code.

A store still:

1. writes each upsert's rows, deletes each remove, clears the scope for a
   whole write;
2. is `immutable` or `fenced` (`stores.md`);
3. if immutable, names a key's object `{output}/{partition}/{key}/{generation}`
   and discards by `("key", key, generation)`;
4. if fenced, keeps the fence row's `written` generation and offers
   `reads()`;
5. if fenced, answers **`keys(ref, among)`**: the keys present in the
   scope, sorted, among the given ones or all of them, never a value.
   *Edge case:* repair, §5;
6. for a `Sql` write, reports the keys it wrote, sorted.

A store no longer computes a version, declares `stamped` columns, or
refuses values its database would round. PostgresStore's exactness
checks (`_check_types`, `_kept`, `_digested`) go without warnings:
Postgres still rejects what it cannot cast, and declared columns are the
user's contract. Kind inference for a table a write creates stays: it
types columns, it does not version them.

**`revision=` is deleted** from `Output`, with everything that renders,
checks or reads back a revision column. *Why:* its one use was to say
"unchanged" for rewritten keys, which "was it written" no longer asks.

## 5. Repair after a crash

Only fenced stores repair: an immutable store's dead writer leaves only
names no entry points at. An attempt that took its gate and died leaves
its intents, the keys it meant to change. The next attempt on the scope
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
rows, counted, and handed to every full delivery. It reads keys, never
values.

**Unknown writes.** A `Sql` write's gate names no keys. If it dies, its
next write must cover every key: a replacement or `Sql` write rewrites
the scope, and every key it writes is at its generation anyway; a patch
first reads every present key (`keys(ref, None)`), gives each the new
generation and tombstones live keys the store lacks. Either way consumers
take everything again.

**A repair always writes.** The repairing attempt runs its store write
transaction even when its own write is empty, so the fence row's
`written` becomes its generation. *Edge case:* §7, break 1.

## 6. Lineage

Lineage records **the generation read**, per input slice:

- an immutable store's read is the pinned generation, exactly;
- a fenced store's read is the fence row's `written` at the reader's
  snapshot (`reads()`);
- `uncommitted` when no commit of the slice has that generation;
  *Edge case:* a reader saw a dead attempt's write;
- `mixed` when two loads of one slice saw two generations. *Edge case:*
  two pages of one input read at two moments.

The per-key versions a current read reports today, computed by hashing
what it loaded (`observed.py`), go. The pin stays internal: the engine
uses it to deliver, and lineage shows it only as debugging detail.

```
B pins A@g10 and reads A's slice in Postgres while A's attempt g12 writes it
lineage:  B ← A, generation 12                    (g12 committed)
          B ← A, generation 12, uncommitted        (g12 died; the repair is g15)
```

## 7. Edge cases checked

| Case | What happens | Holds |
|---|---|---|
| A rewrites `k` while B reads it | Immutable: B reads the pinned object, then `k` again with A's delta. Fenced: B may read A's new rows and records generation 12; A's commit, or the repair of its dead attempt, puts `k` in a delta B receives later, and B rereads | yes |
| Identical rewrites | Every key rewritten is a change; consumers reprocess. A whole input rewritten identically changes its ref's generation, so it resets its consumers' incremental edges (the fingerprint holds input refs): a full redelivery | accepted |
| `version=` bump | The fingerprint changes, the asset's edges reset, every key is reprocessed and written at a new generation, so consumers reprocess too. (Revision outputs used to hide this; they are gone.) A cursor producer with no inputs reprocesses nothing, as today | yes |
| Deploys | The epoch moves; only failed `Each` keys get their one try, and those that succeed are written at a new generation | yes |
| Retries | A new attempt has a new generation; an uncommitted attempt's delta files and objects are discarded. A store call retried inside one attempt rewrites the same names with the same bytes | yes |
| `Each` full redelivery (truncated log, reset) | Every key is processed and written again; its consumers reprocess everything | accepted |
| Rescope | Newly matched keys are delivered at their generation; unmatched ones removed | yes |
| Unknown `Sql` writes | §5: a rewrite, or a key scan before a patch | yes |
| Store move | The moved output starts over (`59812c4`): a fresh index, a whole first write, every key at a new generation, consumers take everything | yes |
| Rename | Index entries, generations and object names stay | yes |
| Failure index retry, upstream changed | The entry's upstream generation differs from the pinned one, so the key comes with the change window instead (`per-key-processing.md` §9) | yes |

**Breaks with the model as Erwin stated it.** One:

1. **A repair that writes no rows leaves lineage saying `uncommitted`
   forever.**

   ```
   attempt g12   writes k to Postgres (written = 12), dies after its gate
   attempt g15   patches nothing of its own; repairs k → k@g15 in the index, writes no rows
   reader C      reads the slice later: written = 12 → "generation 12, uncommitted"
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

Hashing that stays, none of it over user data: key and tombstone
filters, file and payload checksums, the fingerprint, the project
revision.

## 9. Tests

- **Unit.** Entry format v3 round trips in Rust and `_python.py`; every
  write is a change; a retried store call in one attempt gives the same
  delta bytes; source tokens: equal unchanged, different changed, absent
  changed; set elements re-listed unchanged; immutable names and
  discards by generation; `keys(ref, among)` in the store conformance
  kit; the repair example of §5 both ways; a dead `Sql` writer then a
  replacement, then a patch.
- **The simulation** (`tests/sim`, `verification.md`). Its invariant
  "Reads say what they read" drops the per-key part: a read reports the
  generation that wrote the slice. Add one invariant and one check:
  - after every step, for a fenced scope with nothing unsettled, the
    index's live keys are exactly the store's present keys;
  - at convergence, every lineage generation that some commit settled is
    not reported `uncommitted` (break 1).

  The simulation's own oracle already checks convergence of every
  output; it must keep passing with every producer rewriting whole.

## 10. Migration

One implementation worker, in this order, merged when `tests/` and
`tests/sim` pass. Interfaces first: the v3 entry, `Rows` (keys only),
`Store.keys`, `Ref.generation`.

1. **Key index and sources:** format v3 and the pair filter's removal
   (Rust, `_python.py`, `.kxl`); the delta rule (written means changed,
   tokens compared); source commits' generation and tokens; sets' empty
   token; rendering a version as its generation.
2. **Stores and native:** `Rows` keys-only; delete digests and encoders;
   FileStore and S3Store names by generation; PostgresStore without
   exactness checks or `stamped`, `scan` becoming `keys`, `Sql` reporting
   keys; `revision=` out of `Output`; the conformance kit.
3. **Lifecycle and lineage:** `_store_outputs` and `Ref.generation`;
   repair by presence and the always-write rule; unknown `Sql` writes;
   `observed.py` and lineage `{generation, uncommitted?, mixed?}`; `Each`
   (`ctx.revision` and the failure entry's `revision` become the
   upstream `generation`); the simulation's invariants.

Then the docs: delete `row-digest.md`; update `architecture.md` §3,
`object-store-state.md` §5–§7 and §9, `stores.md`, `per-key-processing.md`
§6–§7 and §9, `resolved-commits.md` §3 and §6, `lifecycle.md` §9.6 and
§9.8, `key-index-format.md`, `verification.md`, and the README's "an
unchanged commit wakes nothing" (true now only for sources with tokens,
sets, and producers that write nothing).

## 11. Open questions

1. **Source tokens in patches.** §2 compares a token wherever one comes,
   full map or patch, because one rule is simpler than two. A source
   sending `upsert={k: same token}` then gets "unchanged". Erwin to
   confirm that reading.
2. **`--full` on a source-fed asset** rewrites everything at new
   generations and wakes all consumers. Accepted by the model; noted
   because users may run it more than they expect.
