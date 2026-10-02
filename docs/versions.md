# Versions from provenance

Status: **target design, not built.** It replaces content hashing
(`row-digest.md`, which this design deletes) with version tokens the
system already has. Decided by Erwin: key versions come from provenance,
never from content. This doc says what each version is, what changes in
the key index, the stores, the worker and lineage, what is deleted, how
we test it, and how three implementers split the work.

## 0. The design in one paragraph

A key's version is a token that says who or what produced the key's
current content: a source's revision (an etag, a ctag, an `updated_at`),
the value of a declared `revision=` column, for an `Each` output the
version of the upstream key it was computed from under a given code
epoch, and otherwise the **generation** of the attempt that wrote it.
Change detection becomes "was it written", not "does the content differ":
an asset runs only on changed inputs, so what it writes is new by
construction. Nothing ever hashes user data, and nothing reads data back
from a store to version it. After a dead attempt, its intended keys take
the repairing attempt's generation and count as changed. A store that
reads current rows reports which generation it read, and lineage records
that.

## 1. Why

Today every keyed write is hashed (`row-digest.md`): each row is encoded
in a canonical byte grammar, digested with XXH3-128, grouped per key. For
that to work, the same logical rows must hash the same however they
arrive: Python values, pandas columns, Arrow arrays, or rows Postgres
returns after a `Sql` write. Keeping those four paths in agreement costs:

- a 450-line byte grammar in Rust (`digest.rs`), two value encoders
  (`pyvalue.rs`, `arrow.rs`) and golden vectors on both sides;
- Postgres refusing writes it would store with any loss a hash could see:
  `Decimal("1.234")` into `numeric(6,2)`, a float a `real` narrows,
  nanoseconds into a timestamp, `char(n)` padding, 42 into a text column;
- reading data back: a `Sql` write without `revision=` streams every
  written row out of Postgres to hash it, repair reads intended keys' rows
  back and hashes them, and a current read re-hashes the page it loaded so
  lineage can say which versions it saw.

What hashing bought is one property: a producer that rewrites identical
content wakes nothing downstream. Provenance keeps that property wherever
a token already says "same content" (a revision, a source etag, the same
upstream key under the same code), and gives it up elsewhere. A
full-refresh producer without `revision=` now makes its consumers reread
everything it wrote. That is the user's to optimize, by declaring a
revision.

## 2. Version tokens

### 2.1 Per write kind

| What writes the key | Token | Equal to the index's (so "unchanged") when |
|---|---|---|
| a source commit with a token per key (`{key: etag}`, a sensor's map, `Observed`) | `s` + the committer's text | the committer sends the same text again |
| a source commit listing keys without tokens (`--upsert '["u-1"]'`) | `g` + the commit's generation | never: listing a key says it was written now |
| a partition set's element | `p` | the element is already present |
| rows of an output declaring `revision="col"` | `r` + the producer's fingerprint + the column's text | same revision text, same fingerprint |
| an `Each` output for upstream key `k` | `e` + XXH3-128(epoch, fingerprint, `k`'s upstream token) | `k` is reprocessed at the same upstream version, under the same code epoch and fingerprint |
| anything else: rows without `revision=`, `keyed=True` values, `Sql` without `revision=`, a repair | `g` + the writing attempt's generation | never, except the same attempt writing again (a retried store call) |

**The generation** is what `lifecycle.md` §9.7 already defines: the event
position of the attempt's claim, carried in the spec. Every later attempt
on a scope gets a larger one, so two committed writes never share a `g`
token. A source commit's generation is the engine's event position when
it prepares the commit, under the same rule: only one commit per head can
land, so no two committed source writes on one scope share it.

**The fingerprint** is the per-attempt digest `Engine._fingerprint`
already computes for watermarks: the asset's `version=`, its stores'
versions, its migrations, the run's config, and the refs of its
non-incremental inputs and deps. A change to any of these resets the
asset's incremental edges today, and every key is processed again. The
token has to change with it, or a reprocessed key would come out
"unchanged" and its consumers would never hear about it:

```
file_rows:  revision="ctag", inputs: Incremental(files) + whole crosswalk
crosswalk changes  →  edge reset, every file reprocessed, rows now joined differently
token without the fingerprint:  r"ctag-7"  = index      →  unchanged, consumers keep stale rows
token with it:                  r<fp2>"ctag-7" ≠ r<fp1>"ctag-7"  →  changed, consumers reread
```

So a `version=` bump now reaches downstream through revision outputs too.
Today it does not: `file_index` reprocesses every key under `version="3"`,
but a revision-keyed output would write the same revisions and wake
nothing.

**The epoch** is the engine's number for the project revision it serves
(`per-key-processing.md` §13): it grows with every deploy. Only `Each`
tokens carry it, because only `Each` relies on the code being a function
of its input (`per-key-processing.md` §14: one file, its rows). A
`revision=` is the user's claim about content, and a deploy does not
change content the user has versioned; putting the epoch in `r` tokens
would make every deploy a full change for every revision output.

### 2.2 Encoding in the index

A token is the `version` byte string of a `.kx` entry
(`key-index-format.md`). The file format does not change: versions were
opaque bytes and stay opaque bytes. Only what produces them changes.

```
token  := tag u8 · body
g      0x67  varint(generation)
s      0x73  committer's text, UTF-8 (bytes as they are)
p      0x70  (empty)
r      0x72  fp8 · revision text          fp8: the first 8 bytes of the producer's fingerprint
e      0x65  16 bytes: XXH3-128(varint(epoch) · varint(len(fp)) · fp · upstream token)
```

- **Comparison is byte equality, nothing else.** No token kind is
  ordered: an etag has no order, an `updated_at` can go back after a
  restore, and generations are compared only by the store fence, never as
  versions. The native delta rule (`native/src/delta.rs`) is unchanged.
- **Tags keep kinds apart.** An output that starts declaring `revision=`
  goes from `g` tokens to `r` tokens; the tag guarantees the first
  revision write differs from every generation, so it counts as a change
  once, not as a false "unchanged".
- **Sizes.** `g` is 4 to 6 bytes, `e` 17, `r` 9 plus the revision, where
  today every version is a 16-byte digest or the revision's text. A `g`
  entry's version repeats its locator; block compression absorbs it, and
  dropping it would make "changed" depend on the locator, which the delta
  rule ignores today. Not worth a format change.
- **XXH3 stays for our own bytes**: Bloom filters, file and payload
  digests, `e` tokens over tokens. Never over user data.

**Rendering**, for `key_outcomes.revision`, `/keys` listings and the
console, by tag, with no lookup in the manifest (`Engine._rendered` goes):

| Tag | Shows | Example |
|---|---|---|
| `g` | `g` and the decimal generation | `g184467` |
| `s` | the text | `"0x8DC4A1F2"` |
| `r` | the revision text (the fingerprint is not shown) | `ctag-7` |
| `e` | `e:` and the first 12 hex digits | `e:9c41e0d2a7f3` |
| `p` | `present` | |

### 2.3 Revision text

The rendering rules of `row-digest.md` § Revisions move here, trimmed to
the types a revision is in practice. The native code renders Python
values and Arrow arrays alike:

| Value | Text |
|---|---|
| string | as it is |
| bytes | as they are |
| integer, any width | decimal |
| decimal | plain decimal, normalized (`1.2`, `1000`) |
| date | `YYYY-MM-DD` |
| timestamp, naive | `YYYY-MM-DDTHH:MM:SS[.fffffffff]` |
| timestamp with a zone | the same in UTC, then `Z` |

A null, a boolean, a float, a time, a duration, an interval or a nested
value is a write error asking for one of the above; so are two rows of
one key with different revisions.

Agreement between Python and Arrow is now a cost property, not a
correctness one. If a pandas write renders `2026-01-01T00:00:00Z` and a
later `Sql` write of the same instant rendered something else, the key
would count as changed once: a spurious reread. The correctness property
is the other direction: two different values of a type never render
equally, which the table guarantees.

### 2.4 Output versions

`Ref.version`, the version of a whole output scope, is no longer
computed by the store. The harness sets it to the commit's rendered `g`
token; an unkeyed source keeps the committer's `version=` string.

| Output | `Ref.version` after a commit | No new head when |
|---|---|---|
| unkeyed value | `g{generation}` | the producer omits it (`Result(outputs={...})` without it) |
| unkeyed incremental batch | `g{generation}` of the commit that appended | the `Patch` is empty |
| keyed output | `g{generation}` of the commit that changed the index | the delta is empty and nothing is unsettled (`resolved-commits.md` §3) |
| unkeyed source | the committer's `version=` | it equals the head's |
| keyed source | `g{generation}` of the commit | the delta is empty |

FileStore's sha256 of each value's bytes, its digest chain over batches,
PostgresStore's `_rows_version` and the digest of a `Sql` statement all
go. A store returns a ref with `version` empty, and the harness fills it.
An unkeyed value rewritten with the same bytes is now a new version: its
consumers run again. That is the cost named in §1.

## 3. What a write becomes

### 3.1 `Rows` and `prepare`: keys and revisions only

`Store.prepare` still reads a keyed write once, and `Prepared` still
carries it through resolution, repair and storage. What it reads is
smaller:

| | Today | Target |
|---|---|---|
| columns read natively | every column (to hash rows) | the key column, and the revision column if declared |
| per key | a 16-byte group digest | the revision text, or nothing |
| `keyed=True` values | `Rows.values` hashes each value | keys only |
| `exclude` / `Store.stamped` | leaves stamped columns out of the hash | gone: nothing hashes rows |
| `Prepared.version`, `KeyedWrite.version(prior)` | a digest of every key and version | gone (§2.4) |
| `take(indices)` | rows "as hashed", so stored = hashed | rows as the store persists them; no constraint beyond the store's own |

`Rows` keeps its job of sorting keys natively, grouping rows by key
(`find`) and paging, so a 100M-key write still makes no per-key Python
object. The harness turns `Rows` into a sorted run by a **rule** chosen
per output:

```python
Rule.generation(g)          # every key: g token
Rule.revision(fingerprint)  # every key: r token from its revision text
Rule.each(epoch, fingerprint, upstream)   # per key, from the page's upstream tokens
Rule.present()              # partition sets
Rule.tokens({key: token})   # source commits
```

A DataFrame now reaches native code as one or two columns, not all of
them. `frames` still normalizes missing values to None, because Postgres
stores None as NULL; the reason in its docstring changes, the code does
not.

### 3.2 What a store must still do

1. Write each upsert's group, delete each remove, and clear the scope for
   a `whole` write. (Unchanged.)
2. Be `immutable` or `fenced` (`stores.md`). (Unchanged.)
3. Immutable stores name objects by generation. A key's object becomes
   `{output}/{partition}/{key}/{generation}.json`: the generation alone is
   unique per key, since an attempt writes a key once. The version leaves
   the name, and `version_name`'s hashing of long versions with it.
   `discard` items become `("key", key, locator)`.
4. Fenced stores keep the fence's `written` generation and implement
   `reads()` (`stores.md`, "What a read sees"). (Unchanged.)
5. **New: fenced stores answer `keys`.**

   ```python
   async def keys(self, ref, among: Sequence[str] | None) -> AsyncIterator[list[str]]
   ```

   The keys present in the scope, sorted by their UTF-8 bytes, a chunk at
   a time: those of `among`, or every one. Keys only, never a value.
   PostgresStore's `scan` becomes this, with its "every column" branch
   gone. Repair (§5) is its only caller.
6. A `Sql` write reports its keys, sorted, plus each key's revision value
   when the output declares one (`Written.keys`). The "every column" path
   goes: without `revision=`, every key takes the attempt's `g` token.

What a store no longer does: compute `Ref.version`, declare `stamped`,
store "what digests as hashed", refuse lossy values.

### 3.3 What remains of Postgres' type checks

`_check_types` and `_kept` existed so a value would read back exactly as
it was hashed. With no hash there is nothing to agree with, and both go.
Postgres itself still rejects what it cannot cast (`"abc"` into
`bigint`), and declared columns stay the user's contract. Two kinds of
coercion become silent: rounding to a column's precision, and a number
written into a text column as its text. Both are ordinary database
behaviour.

No warnings replace them. A warning per write costs the same Python loop
over every value as the check did, and lands in logs nobody reads. If a
user gets bitten, the place to add one is `_ensure`'s first page, sampled.

Kind inference for a table a write creates (`_column_types`,
`frames.kind_of`, `frame_kinds`) stays: it decides column types, not
versions.

## 4. Deltas and the resolver

The key index, its delta rule, the resolver protocol, the engine cache,
the sparse reader and the streaming merge-join are unchanged. A write is
a sorted run of `(key, token, deleted)`, compared with the pinned index by
byte equality, exactly as before. What changes is which tokens come in,
and so how often "unchanged" happens.

**Patches.** Each upsert's token against the index's entry: absent →
added; different → changed; equal → no entry. A remove of a live key → a
tombstone.

```
index:  a=g100  b=r<fp>"v3"  c=g100
patch from attempt g140 (no revision):  a, d                 → delta: a=g140, d=g140 (added)
patch from attempt g141 (revision):     b="v3", e="v1"       → delta: e (added); b unchanged
```

With `g` tokens every upsert is a change, so the cold sparse reader's
pair filter almost never matches (0.35% false positives, which then take
an exact lookup) and the key filter alone decides new against existing.
No code needs to know this.

**Replacements.** Every written key against the whole index, and every
live key not written becomes a tombstone. A replacement without
`revision=` changes every key it writes, so its delta is as large as the
write: 1M keys rewritten every hour is a 1M-entry delta every hour, and on
an immutable store 1M new objects plus 1M discards. The engine resolves
replacements up to `resolve_max_entries`; past it, the streaming
merge-join, as today. Declaring a revision brings it back to the changed
keys only.

**`Each` pages.** The worker knows each page key's upstream token from
the window it read (`ctx.revision`), and the spec carries the epoch and
the fingerprint. A call that returns rows for `k` writes `k` at
`e(epoch, fp, upstream(k))`.

```
epoch 12, fp F.  sharepoint_files: run-17.csv = s"ctag-7"
page 1:  icp(run-17.csv) ok            → icp_samples: run-17.csv = e(12, F, s"ctag-7")     changed
log truncated, full redelivery, same epoch:
page 9:  icp(run-17.csv) ok, same rows → e(12, F, s"ctag-7") = index                       unchanged
deploy (epoch 13), forced retry of the failed keys only: run-17.csv not called          no change
ctag 8 arrives:  icp(run-17.csv)       → e(13, F, s"ctag-8")                                changed
```

A full redelivery of an `Each` edge (a truncated log, a rescope, a
`--keys` rerun) therefore wakes nothing downstream for keys whose
upstream did not move. That is where provenance is strictly better than
generations. `Rejected` and failed keys write nothing and keep their
token. An `Each` output declaring `revision=` is refused at registration:
its `e` token already carries the upstream revision.

**Retries.** A failed attempt that never took its gate committed nothing
and its delta files are discarded; the retry has a new generation, so its
`g` tokens are new and its `e` and `r` tokens are the same as the dead
attempt's would have been. A retried store call inside one attempt
produces the same tokens and the same delta bytes (store invariant 2). An
attempt that took its gate and died is §5.

**Epochs and deploys.** A deploy bumps the epoch and nothing else: no
key is reprocessed because of it, except failed `Each` keys, which get
their one try per deploy (`per-key-processing.md` §9) and, if they
succeed, a token under the new epoch. A `version=` bump changes the
fingerprint, resets the asset's edges and reprocesses every key; every
token it writes is new (`g` by generation, `r` and `e` by fingerprint),
so its consumers reprocess too.

**`full` runs.** A `full` run rewrites a scope whole. Its `g` tokens are
new; its `r` and `e` tokens are the same when nothing upstream or in the
fingerprint moved, so consumers of a revision or `Each` output are not
woken by `--full` alone. To force them, bump `version=` (§9, question 3).

**Sources and sensors.** An API commit or a sensor tick builds its run
with `Rule.tokens`: the committer's tokens as `s`, listed keys as `g` of
the commit. A sensor that returns a full map of etags every five minutes
changes only the keys whose etag moved, as today. A sensor that returns a
list of keys every five minutes now changes every key every five minutes;
it should return tokens. The demo's `uploads` source, committed with
lists, gets new `g` tokens on each commit; that is the intent of a commit.

**Counts, predecessors, compaction, `.kg` garbage files** are unchanged.

## 5. Dead attempts: repair without reading content

Only fenced stores repair (`lifecycle.md` §9.6). An immutable store's
dead writer leaves only names no index entry points at.

### 5.1 Named intents

An attempt takes its gate, writes part of its keys, and dies. Its gate
lists its delta files: the keys it meant to change. The next attempt on
the scope acquires the fence (so the dead writer can write nothing more),
then settles every intended key in its own commit:

| Intended key `k` | Today | Target |
|---|---|---|
| the new attempt writes or removes `k` itself | its own write decides `k`, and the store rewrites `k` even if unchanged | the same |
| the new attempt does not mention `k` | load `k`'s rows, hash them, put that version in the delta | ask the store whether `k` is present (`keys(ref, among)`). Present: `k` = the new attempt's `g` token. Absent and live in the index: a tombstone. Absent and not in the index: nothing |

Every intended key ends up in the repair commit's delta (or provably did
not change), so every consumer that read the dead attempt's writes hears
about them later (§6).

Why a `g` token and not the index's old token, for a revision output:

```
index         k = r<fp>"v5"
attempt g12   writes k at "v6" to Postgres, dies after the gate      store may hold v5's or v6's rows
attempt g15   patches other keys; repairs k as r<fp>"v5" (the index's)   ← wrong
later         producer writes k at "v5": equal to the index, so the store is not written
result        the store holds v6's rows forever, the index says v5
with g15      k = g15: the later "v5" write differs, the store rewrites k, consumers reread
```

Why ask for presence instead of reading nothing at all. Erwin's decision
was "no read-back", and this is the one place it does not hold literally.
A presence query reads keys, never values, and hashes nothing. Without it,
the repair cannot tell these apart:

```
index         k absent (a new key)
attempt g12   upserts k, dies; its insert did or did not commit
no read       k = g15, live: if the insert never landed, the index lists a key with no rows,
              counts it, and every full delivery hands consumers an empty key
presence      absent → nothing; present → k = g15
```

The same holds for an intended remove of a live key. The alternative that
reads nothing is to finish the dead attempt's removes (delete `k` again,
record a tombstone) and assume its upserts landed. That is wrong for the
new-key case above, so this design asks the store. It is one indexed
`SELECT key … WHERE key = ANY(…)` per 100K intended keys, after
acquisition.

### 5.2 Unknown writes (`Sql`)

A `Sql` write's gate names no keys: the query could change any key, and
its key list is reported only after it commits. If it dies in between,
the scope is marked for a rewrite. Its next commit must cover every key:

| The next write is | It does |
|---|---|
| a replacement, or a `Sql` write (the usual case: the retry of the same `Sql` asset) | clears and rewrites the whole scope, no read. Every key it writes takes its `g` token, even on a revision output, so consumers take everything again |
| a patch | streams the store's whole key list (`keys(ref, None)`): every present key takes the new `g` token unless the patch writes it, every live index key the store lacks becomes a tombstone |

Either way the commit is effectively a full delivery to every consumer.
That is the honest answer to "anything may have changed", and it costs one
key scan only in the rare patch-after-dead-`Sql` case.

### 5.3 Settling promptly

A consumer may have read a dead attempt's writes, and only the repair
commit tells it so. Repair happens on the scope's next attempt, usually
the failed attempt's own retry. A scope whose run was canceled stays
unsettled until something runs it again, visible in `GET /holds`. Repair
needs no user code (acquire, presence, delta), so the engine could launch
a settle-only attempt; §9, question 2.

## 6. Reads, and why they stay correct

The case: consumer B pins producer A at head `g10` and reads key `k`
while A's attempt `g12` rewrites `k`. B must never keep a stale or
uncommitted view of `k` forever. Four rules cover it:

1. **Every write after B's pin produces a delta B receives later.** A
   committed write's delta holds every key it changed (§4). A dead write's
   keys are in the repair commit's delta (§5). B's watermark is behind
   both, so both reach it.
2. **Immutable stores read exactly the pin.** B names `k`'s object by the
   pinned `(version, locator)`. `g12` wrote a different name. B reads
   `g10`'s content, then `g12`'s with the delta.
3. **Current-read stores report what they read.** PostgresStore reads in
   one REPEATABLE READ snapshot and returns the fence row's `written`
   generation per slice. B's lineage records "pinned `g10`, read `g12`".
   If `g12` commits, the lineage API shows what `g12` committed; if it
   died, that B read an uncommitted generation. B reread `k` with `g12`'s
   delta or the repair's either way.
4. **Optional watermark skip.** When B read a whole slice of A in one
   snapshot and saw generation `g12`, and `g12` then commits that slice,
   B has already consumed it. Its input can count as `A@g12` and
   `OnChange` need not run B again. Without the skip, B runs once more
   and rereads the same content: harmless. Not in v1 (§9, question 4).

```
B pins A@g10 (Postgres)
A g12 writes k=v2, commits its transaction        fence row: written = 12
B reads k                                          gets v2, reports written = 12
A g12 commits: delta {k}                          B's next window holds k: rereads v2 (a repeat)
-- or --
A g12 dies after the gate                          unsettled {k}
A g15 repairs: k present → k = g15                 B's next window holds k: rereads
```

**Lineage.** A `lineage` row keeps `input_version` (the pinned head's
rendered token) and `input_generation`. Its `read` JSON shrinks to
`{generation, mixed?}`: the generation a current read saw, and whether
two loads of one slice saw two generations. The `keys` and `key_count`
fields go, with the read-time re-hashing in `observed.py` (`_saw` calling
`prepare_for(...).entries()` on what it loaded). The API answer is
`{exact: true}`, or `{exact: false, pinned_generation, generation,
version}`, where `version` is what that generation committed, if it did:
`history._read` does this already, minus the per-key part.

What lineage loses: for a page read at a generation other than the pin,
it no longer lists each key's version as read. It says which generation
was read, and that generation's commit (or its absence) says the rest.

## 7. Store-reported changes: later, not v1

A store could tell the harness which keys really changed, computed in its
own type system:

```sql
INSERT INTO t AS cur … ON CONFLICT (part, key) DO UPDATE SET …
  WHERE (cur.*) IS DISTINCT FROM (EXCLUDED.*)
RETURNING key;
```

Keys the store proves unchanged would leave the delta. Not in v1:

- Correctness never needs it. It only narrows deltas.
- It reverses the write order. Today the delta is resolved and uploaded
  before the gate, because the gate's intents are the delta's files.
  Narrowing after the write needs a second delta file (the narrowed one
  committed, the wide one kept as the intent). That is doable, but it is a
  new phase in `_store_outputs`, the resolver and the conformance kit.
- It fits one row per key. Groups (`per-key-processing.md` §6) are
  delete-then-insert, where "distinct" needs a multiset comparison per
  key in SQL.
- Users already have the escape hatch: declare `revision=`, computed in
  the store if they like (`md5(t::text) AS rev` in a `Sql` query). That
  is the same comparison, in Postgres' types, chosen by the user.

The place it would pay off is a big `Sql` replacement without
`revision=`. If one shows up, add `Written.unchanged` (a sorted key list,
only narrowing, only from fenced stores) with the two-file commit.

## 8. What is deleted

| What | Where |
|---|---|
| The row-digest grammar and its doc | `docs/row-digest.md` (the revision table moves to §2.3 here), `native/src/digest.rs` |
| The Python value encoder | `native/src/pyvalue.rs`, all but key text (`str`/`int`) and revision text |
| The Arrow value encoder | `native/src/arrow.rs`, all but reading a key column and a revision column |
| Group digests, content digests | `rows.rs` (`content`, the digest `Versions`, xxh3 over rows), `group_digest`, `Rows.digest` |
| `exclude` and `Store.stamped` | `prepare`, `prepare_for`, `Rows.records/columns/arrow`, PostgresStore |
| Store-computed versions | `Prepared.version`, `KeyedWrite.version`, `stores._digest`, FileStore's `_put` hash and batch chain, PostgresStore `_rows_version` and the `Sql` statement digest |
| Version in FileStore names | `version_name`, `key_name`'s version part, `("key", key, version_hex, locator)` |
| Postgres fidelity checks | `_check_types`, `_kept`, `_digested` |
| `Sql`'s every-column read-back | `_sorted_rows`'s `SELECT *` branch |
| Repair by reading rows | `worker._repair`'s `load` + `prepare_for` + `entries`; `_reconcile`'s row scan and its "read whole" fallback |
| Read-time digesting | `observed.py` `_saw`'s per-key versions; lineage `keys` and `key_count` |
| Manifest-based rendering | `Engine._rendered` |
| Tests of the above | `tests/sdk/test_row_digest.py`; Python/Arrow digest agreement; both-`Sql`-paths agreement (`per-key-processing.md` §18); Postgres fidelity refusals in `tests/sdk/test_postgres.py`; per-key versions in `tests/server/test_lineage_reads.py` |

What stays hashed, none of it user data: Bloom filters, `.kx`/`.kxl`
checksums and digests, the resolve payload digest, the fingerprint, the
project revision, `e` tokens.

## 9. Open questions

1. **Presence queries in repair** (§5.1). They break "no read-back" in
   letter, reading keys, never values. The alternative that reads nothing
   (finish removes, assume upserts landed) is wrong for new keys. Erwin to
   confirm.
2. **A settle-only attempt.** Should the engine launch one for an
   unsettled scope instead of waiting for the next run (§5.3)? It needs no
   producer, so it is cheap. Proposed: yes, after v1.
3. **`--full` and `Each`/revision outputs.** A full run reproduces equal
   `e` and `r` tokens and wakes nothing downstream (§4). Probably right,
   since full rebuilds the asset, not its consumers, but a user running
   `--full` after an outside fix may expect consumers to follow. Option:
   a `full` run adds its generation to the fingerprint.
4. **The watermark skip** (§6, rule 4). Worth it for big whole reads of
   Postgres outputs; it needs the engine to match a read generation to a
   later commit of the same slice. Proposed: after v1, measured on a
   whole-read consumer.
5. **Run config in `e` tokens.** The fingerprint holds the run's config,
   so two runs with different config give different tokens: right if
   config changes outputs, churn if it is operational (a timeout). Same as
   watermarks today; left as is.
6. **Lists to keyed sources.** `--upsert '["u-1"]'` now always changes
   `u-1`. A full-map list (`--keys '["a","b"]'`) changes every key on every
   commit. The alternative is `p` tokens for lists (presence only), which
   would let a re-listed key stay "unchanged" though it may have changed.
   Proposed: `g`, as above.

## 10. Tests and the simulator

Unit tests, per piece:

- **Tokens.** Golden bytes for each tag; rendering; `e` over nested
  tokens; a tag change is a change (`g` → `r`); revision text over the
  §2.3 types from Python and Arrow, equal values equal and distinct values
  distinct (property-based); refused types refused.
- **Rules.** `Rule.generation` makes every write a change; a repeated
  store call in one attempt gives identical tokens and identical delta
  bytes; `Rule.revision` is unchanged under the same fingerprint and
  changed under another; `Each` tokens do not depend on page size,
  concurrency, key order or retries.
- **Stores.** The conformance kit drives tokens instead of digests; a new
  scenario for `keys(ref, among)` on fenced stores (present, absent,
  removed, a dead writer's insert); FileStore names by generation and
  discards by `(key, locator)`; a `Sql` write without `revision=` reports
  keys only.
- **Repair.** The revision example of §5.1 ends with the store rewritten;
  the new-key example leaves the index without `k` when the insert did not
  land, and with `k = g15` when it did; a dead `Sql` writer followed by a
  replacement versions every key `g`, followed by a patch reconciles by
  key scan.
- **No read-back.** An instrumented store fails the test if the harness
  calls `load` while writing, or `keys` without unsettled intents.
- **Lineage.** A current read at the pinned generation is exact; at
  another, `read` holds exactly `{generation}`; two loads at two
  generations are `mixed`.

**The simulator** is new: `tests/sim/test_versions.py`, seeded random
schedules through the real engine, worker code, key index and stores
(FileStore on `file://`, PostgresStore when `SOLERA_TEST_DATABASE_URL` is
set), with a model of the truth beside them. A schedule mixes producers
(patch, replacement, `Each` pages, `Sql`, revision and not), consumers
(incremental and whole, snapshot and current-read), source commits, and
faults: a worker killed before the gate, after part of its writes, after
its writes and before its result; a store call retried; compaction and
log truncation between steps; deploys and `version=` bumps. The model
records, per key, the id of the last write that landed in the store.

Properties, checked after every step or at quiescence:

1. **The index matches the store.** For a scope with nothing unsettled,
   the live keys of the index are exactly the keys present in the store.
2. **No missed change.** At quiescence (every scope settled, every
   consumer caught up), every consumer's last read of every key saw the
   store's last landed write of it. This is the property §6 argues for.
3. **Generations never collide.** No two committed writes on a scope
   share a `g` token.
4. **"Unchanged" is honest.** An empty delta with nothing unsettled means
   no store write (migrations aside).
5. **Determinism.** Replaying a schedule with the same seed gives the
   same delta files, entry for entry.
6. **Lineage says what was read.** Every recorded `read.generation` equals
   the fence row's `written` at the reader's snapshot.

The simulator must also show it can fail. Its own tests remove each rule
of §5 in turn (repair with the index's old token, repair without the
presence query, no unknown-`Sql` handling) and expect a counterexample to
property 1 or 2 within a fixed budget of schedules.

## 11. Migration plan

No deployment exists and nothing is kept compatible: no `.kx` version
bump, no data migration. The work splits in three, on one integration
branch `versions` merged to `main` when the suite is green; the three
interfaces below are fixed first so the parts proceed in parallel.

### Interfaces, fixed on day one

```python
# solera.versions (Python reference) and native/src/versions.rs (owner: key index)
generation(g: int) -> bytes
source(text: str | bytes) -> bytes
revision(fingerprint: str, text: bytes) -> bytes
each(epoch: int, fingerprint: str, upstream: bytes) -> bytes
PRESENT: bytes
render(token: bytes) -> str
class Rule: generation(g) | revision(fp) | each(epoch, fp, upstream) | present() | tokens(map)

# solera.keys.Rows (owner: stores/native)
Rows.records(rows, key, revision=None); Rows.columns(names, columns, key, revision=None)
Rows.arrow(data, key, revision=None); Rows.values(items); Rows.keys(elements)
rows.find(keys); rows.pages(size); rows.entries() -> (keys, revisions | None)

# Store (owner: stores/native)
async def keys(self, ref, among) -> AsyncIterator[list[str]]   # fenced stores
Written(ref, keys)    # ref.version empty; keys: sorted [(key, revision?)] chunks for Sql
```

### Implementer 1: stores and native

1. `Rows` reads keys and revisions only; delete `digest.rs`, the value
   encoders but for keys and revisions, group and content digests,
   `exclude`.
2. `prepare` and `frames`: no version, no `stamped`; `Prepared` and
   `KeyedWrite` lose `version`.
3. FileStore and S3Store: names by generation, `discard` by `(key,
   locator)`, no value hashing, empty `Ref.version`.
4. PostgresStore: delete `_check_types`, `_kept`, `_digested`,
   `_rows_version`, `stamped`; `scan` becomes `keys`; `Sql` reports keys
   and revisions only.
5. Conformance kit (`solera.testing.stores`) and `examples/json_table_store.py`:
   tokens, the `keys` scenario.
6. Delete `tests/sdk/test_row_digest.py` and the fidelity tests; add the
   revision-text and store tests of §10.

### Implementer 2: lifecycle, repair, lineage

1. `_store_outputs`: choose each output's `Rule` (spec: generation, epoch,
   fingerprint, the `Each` page's upstream tokens), and fill `Ref.version`.
   Make sure the spec carries the fingerprint and the epoch for every
   attempt, not only `Each` pages.
2. `_repair`: presence by `keys(ref, among)`, `g` tokens for present
   keys, tombstones for absent live ones. `_reconcile`: the key scan of
   §5.2; the "next write is whole" branch with forced `g` tokens.
3. `Each`: tokens from `Rule.each`; refuse `revision=` on `Each` outputs
   at registration.
4. `observed.py`: drop per-key digesting; lineage `read` to
   `{generation, mixed?}`; `history._read` without `keys`/`key_count`.
5. The simulator (§10), shared with implementer 3 for its index faults.

### Implementer 3: key index, resolver, sources

1. `solera.versions` and `versions.rs` first (day one), with golden
   vectors; `SortedRun.from_rows(rows, removes, rule)` and
   `Job.replace(..., rule)` in place of the digest-reading paths.
2. Source commits and sensors: `Rule.tokens` (`s` for tokens, `g` of the
   commit for lists, `p` for sets); the source commit's generation; drop
   the digest-based `ref["version"]` for keyed sources.
3. `Engine._rendered` → `render`; `key_outcomes` and `/keys` show
   rendered tokens.
4. Resolver and cache: no protocol change; check that nothing assumes a
   16-byte version (`_python.py`, the `.kxl` local form, test fixtures).

### Then, together

- **Docs.** Delete `row-digest.md`. Update `architecture.md` §3
  (the Version table), `object-store-state.md` §6 (Compute a delta, Full
  replacement, the `Sql` paragraph), §7 (lineage `read`), §9 (names),
  `stores.md` (Values a store takes, `keys`, invariants), `per-key-processing.md`
  §6 and §7 (a key's version), `resolved-commits.md` §3 (repair table) and §11,
  `lifecycle.md` §9.6 and §9.8 (unknown writes, names),
  `key-index-format.md` (what a version is), and the README ("an unchanged
  commit wakes nothing": true for `site_files`, which declares
  `revision=`; `site_events` writes no batch when the feed returns none).
- **The demo.** `sites`, `site_files` and the cursor keep the demo quiet
  between feed ticks. Check that `fleet_index`, `site_digest` and
  `site_status` rerunning on each upstream commit is what the walkthrough
  says.
- **Measure** a 1M-key replacement without `revision=` on FileStore and on
  Postgres, before and after, so §4's cost statement has numbers.
