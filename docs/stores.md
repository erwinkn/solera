# Writing a store

A store is where an asset's outputs live: files, a bucket, a database. The
engine never touches its data; workers call the store, and the engine
records what they committed. This page is the contract a store implements,
the invariants it must hold, recipes for common backends, and the
scenarios — runnable, in `solera.testing.stores` — that check them.

If you are implementing a store: read **The contract**, pick a **kind**,
follow its **recipe**, then make the **conformance kit** pass.

## The problem a store solves

An attempt W1 can still write after the engine gave up on it: it lost its
connection but runs on, it paused (GC, a VM migration), a request it sent
is still queued at the backend, or it was started twice. Meanwhile its
retry W2 commits. If W1's late write lands, the store holds something the
engine never committed — a key regressed to an old version, a deleted key
back, a batch replaced.

Waiting longer only makes that less likely. So every store guarantees it
cannot happen, in one of two ways — its **kind**:

| Kind | How a late writer is made harmless | Implement | A read sees |
|---|---|---|---|
| `immutable` | It writes only names no other attempt uses. A late write creates an object nothing references; the engine has it deleted later. | `discard` | exactly the pinned version |
| `fenced` | Every write checks, atomically, that its attempt still holds the slice. A newer attempt takes it first (`acquire`), so the older one is refused. | `acquire` | the current rows |

There is no third kind: registration refuses a store that declares
anything else, or lacks the method its kind needs.

## The contract

```python
class Store(Protocol):
    writes: str                    # "immutable" or "fenced"
    version: str = "1"             # bump when the stored format changes
    ref_type: type[Ref] = Ref

    def can_load(self, t, selection) -> bool: ...           # at registration
    def can_store(self, t, output) -> bool: ...             # at registration
    async def store(self, write, prior, scope) -> Written: ...
    async def load(self, ref, t, selection) -> Any: ...

    async def discard(self, scope, prior, items) -> None: ...   # immutable
    async def acquire(self, scope, prior) -> None: ...          # fenced
    async def migrate(self, output, migrations, scope=None, prior=None) -> list[str]: ...  # optional
    def prepare(self, write, output) -> Prepared: ...          # optional: types of its own
    def reads(self) -> AsyncContextManager[Reader]: ...       # optional: a store of current rows

class Reader(Protocol):                                        # what `reads()` yields
    async def load(self, ref, t, selection) -> tuple[Any, int | None]: ...  # value, generation written
```

**`Scope`** — what a write belongs to:

| Field | Meaning |
|---|---|
| `output` | the `Output` declaration: `name`, `key`, `revision`, `incremental`, `config` |
| `partition` | the partition key, `""` for an unpartitioned output |
| `batch` | for an incremental output, the batch number the engine assigned |
| `reset` | the write starts the content over (a `full` run): keep nothing of `prior` |
| `attempt` | the writing attempt's id |
| `generation` | a number the engine assigns each attempt on a scope, larger for every later attempt; `None` outside an attempt |
| `invocation` | which process runs the attempt: an attempt started twice has one generation and two invocations, and only the first to claim it may write |

**`store(write, prior, scope) -> Written(ref)`** applies a write and
returns a ref to the new content, with a `version` that changes when the
content does. `prior` is the committed head: where the content is — a
renamed output's objects, or table, stay where they were, so every ref to
them stays readable; the declaration names the place only of a first
write — and what the write builds on, unless `scope.reset`, when nothing
of it is kept. `None` is a first write. `acquire` and `migrate` get it
too, to find the same place.

- An unkeyed, non-incremental output's write is a value: replace it.
- An unkeyed incremental output's write is a `Patch` of rows: append it as
  batch `scope.batch` — or, reset, start the batches over at it.
- A keyed output's write arrives as a `KeyedWrite`, already resolved
  against the engine's key index. A store reads it four ways: `whole` —
  the write is the scope's entire content, so clear the scope first;
  `pages()` — the keys to write, a page at a time, each `(key, version,
  rows)`, only that page taken from the write (`iter_pages()` for a store
  writing on a thread of its own); `removes` — the keys to delete; and
  `version(prior)`. Every other key stays as it is. A key with zero rows
  does not exist. Plain values (a list of rows, a `Patch`) may also arrive
  outside the engine: `KeyedWrite.of(store, write, output, prior)` turns
  them into one. A store reading values of its own type defines
  `prepare(write, output)`.

**`prepare(write, output)`** (optional) reads a keyed write of the types
the store takes — see **Values a store takes** below. Without it, the
store takes plain Python.

**`load(ref, t, selection)`** materializes `t` (`list[dict]`, a DataFrame,
…). `selection` is `None` (everything), `Keys` (key → `(version,
locator)`: only those keys) or `Batches(lo, hi)`. An immutable store needs
`Keys` to find a keyed output's objects — the locator is the generation
that wrote each key. With `Keys`, `t` may be `dict[str, T]`
(`solera.stores.by_key_type(t)` is `T`): each key's rows on their own, as
`T` — how `Each` reads a page — and a key with no rows absent, since it
does not exist. `can_load` says which `t` a store loads, by key or not;
say only what `load` does.

**`discard(scope, prior, items)`** (immutable) deletes objects nothing reads
any more. `items` name them: `("key", key, version_hex, locator)`,
`("path", path)`, `("value", generation)`, `("batch", n, generation)`, or
`("batches", lo, hi)`. The engine names only objects no reader pins;
deleting a name twice, or one never written, must be harmless.

**`acquire(scope, prior)`** (fenced) takes the scope's slice for
`scope.generation` and `scope.invocation`, in a transaction of its own,
before the attempt reads anything from the store. It must wait for an older writer's open
transaction on the slice, and refuse (`StoreError`) when a newer
generation, or another invocation of this generation, holds it.

**Errors.** Raise `WriteError` for a malformed write (duplicate keys, a
wrong shape), `StoreError` for anything else the store refuses. A store
call that raises after the attempt began writing leaves its writes
*uncertain*: the next attempt repairs them (fenced stores keep the
attempt's intents for that).

## Values a store takes

The framework knows plain Python only: a keyed write is a list of
mappings, a by-key `{key: rows}`, a `keyed=True` dict or a partition
set's elements, and `solera.stores.prepare` reads it. DataFrames, Arrow
tables or a type of your own are a store's business: a store that takes
them reads them in `prepare` — once, for the key index and for its own
write — and says so in `can_store`, which registration checks against the
producer's return annotation.

- **Plain Python only:** define no `prepare`; `can_store` returns
  `solera.stores.takes(t, output)` — the forms the framework defines, per
  kind of output, once.
- **DataFrames and Arrow:** `solera.stores.frames` reads them, importing
  pandas or pyarrow only for a value of their type. FileStore, S3Store
  and PostgresStore use it:

  ```python
  from solera.stores import frames, takes

  class MyStore:
      def prepare(self, write, output):
          return frames.prepare(write, output)

      def can_store(self, t, output):
          return takes(t, output, frames=True)
  ```

- **A type of your own:** build the `Prepared` yourself — native `Rows`
  of its keys and versions, and `take(indices)`, the rows it stores:

  ```python
  from solera.keys import Rows
  from solera.stores import Prepared, prepare

  def prepare(self, write, output):
      if not isinstance(write, Sheet):
          return prepare(write, output)            # plain Python, as the default
      rows = [{"id": k, "amount": a} for k, a in write.lines]
      return Prepared(output, Rows.records(rows, output.key), lambda at: rows if at is None else [rows[i] for i in at])
  ```

  Rows built from a columnar form keep it: `Rows.columns(names, columns,
  key)` takes column lists, `Rows.arrow(data, key)` anything with
  `__arrow_c_stream__`, without making a dict per row.

What a store stores must digest as what `prepare` hashed
(docs/row-digest.md): take the stored rows from the same reading, and
refuse a value your backend would read back as another value — another
type, or rounded. PostgresStore refuses 42 in a text column, and, in the
columns versions digest (every one, or a keyed output's key and
`revision`), `Decimal("1.234")` in a `numeric(6,2)`, a float a `real`
would narrow, nanoseconds in a timestamp, and padding a `char(n)` adds.

## The invariants

A store of either kind:

1. **A write is the content the engine will record.** After `store`
   returns, a load of its ref (with the keys the index will hold) returns
   exactly the write applied to `prior`: a replacement drops the keys it
   does not name, a patch changes only its keys and removes, and a key
   written with zero rows is gone.
2. **An attempt's repeated write is one write.** The same attempt —
   generation and invocation — writing the same content again (a retried
   call) leaves the same content and version.

An `immutable` store:

3. **Names are never reused.** Every object's name includes the writing
   attempt's generation (or another value no other attempt uses), and
   writes are create-only. So nothing an attempt writes can replace what
   another wrote.
4. **A pinned read returns its version.** A ref and selection read before a
   newer commit return the same content after it.
5. **Discard deletes only what it names.** Discarding superseded names, an
   abandoned attempt's names, or names never written leaves every object a
   current or pinned reader needs.

A `fenced` store:

6. **A stale writer changes nothing.** Once generation g acquired a slice,
   every write of an older generation to it is refused, before it changes
   anything — including writes already queued at the backend.
7. **One generation, one invocation.** A second invocation of the
   generation holding the slice can neither acquire nor write; the holding
   invocation acquiring again succeeds (its own retry).
8. **A newer writer waits for an open older one.** `acquire` waits until an
   older writer's open transaction ends, so the newer attempt's repair
   reads see everything the older one committed; from then on the older
   one is refused.
9. **Fencing covers the whole write domain.** If one write can affect
   several partitions' data (a table-wide migration, a shared table
   rewrite), it must fence all of them, or serialize against every
   writer of the table (PostgresStore holds a table-wide lock for its
   migrations).

## What a read sees

An immutable store keeps every version until no reader pins it, so a
run reads exactly the versions it pinned. A fenced store keeps one copy:
a load returns the rows as they are now. So a run can read two outputs at
different moments, and a row changed after the run pinned it is read in
its newer form — and delivered again with the change that made it, a
harmless repeat. Fencing makes writes safe; it does not make reads
repeatable — so such a store says what it read, with `reads()`:

- **One moment.** `async with store.reads() as reader:` — every
  `reader.load(ref, t, selection)` runs in one snapshot (PostgresStore: a
  REPEATABLE READ, READ ONLY transaction), so an attempt's inputs from the
  store are read together. The harness opens it for the inputs and closes
  it before the producer runs: a long producer holds no snapshot.
- **The generation it saw.** Each load returns `(value, generation)`: the
  generation whose write transaction last changed the slice, read in the
  same snapshot. Keep it beside the fence: set it in every write
  transaction, as it commits — never in `acquire`, which writes nothing
  (an acquisition is no version: reporting it would claim one never
  read). `solera.fencing` does both: `fence(cur, scope, domain,
  write=True)` in a write, `written(cur, domain, partition)` in a read.
  None if no fenced write changed the slice.

The engine records it as lineage, which names what was read: a snapshot
store's read is the pinned version; a current read is the version its
generation committed — the pinned one, or a newer one, with each key's
version as read for a page of keys (`Keys`) — or flagged `uncommitted`
when no attempt committed what it read, so lineage never claims a
version that was not read. The conformance kit's
`READS` scenario checks a store that defines `reads()`.

## Recipes

### A SQL table: `fence()` in every write transaction

`solera.fencing.fence(cur, scope, domain)` makes a SQL store fenced. Call
it first in every write transaction, and as `acquire`:

```python
from solera.fencing import fence, fence_table

class EventsStore:
    writes = "fenced"

    async def acquire(self, scope, prior):
        with connect(self.dsn) as conn, conn.cursor() as cur:
            fence(cur, scope, "public.events")

    async def store(self, write, prior, scope):
        with connect(self.dsn) as conn, conn.cursor() as cur:   # one transaction
            fence(cur, scope, "public.events")                   # before any change
            ...                                                   # DELETE / INSERT / MERGE
```

`fence` upserts `(domain, partition) → (generation, invocation)` in the
`solera_fences` table (`fence_table(cur)` creates it, once), keeps the row
locked until the transaction ends, and raises `StoreError` when a newer
generation or another invocation holds it. The row lock is what makes a
newer `acquire` wait for an older open transaction. The SQL is
PostgreSQL's; `param=` adapts placeholders for another driver, and any
database with `INSERT … ON CONFLICT … DO UPDATE … WHERE … RETURNING` and
row locks works the same way.

Declare the table's columns (`Output(..., columns={"n": "bigint"})`, and
migrations to change them): a table a write creates from inferred types
holds what that first write happened to show. PostgresStore infers them
when undeclared — from a DataFrame's schema, else from every value not
null — refuses a column it cannot type, and logs what it inferred. A
migration's payload is the store's own business (PostgresStore: SQL
text, or a callable taking a cursor): `migrate` refuses one it cannot run.

SQL a user hands the store for a write must not reach past the scope the
engine fenced and records. PostgresStore's `Sql` is a query, never a
statement: it is embedded in the store's own `INSERT … SELECT … FROM
(<query>) _src` and prepared, so DML, DDL and a second statement do not
parse; only a function the query calls could still write, and
`sql_read_only=True` reads the query in a READ ONLY transaction, which no
function can turn back (a `SET ROLE` can: a function may `RESET ROLE`).

Write a keyed output page by page: for `whole`, clear the slice first;
then for each of `write.pages()`, delete its keys and insert their rows
(or `MERGE`); then delete `removes`. A complete
example, which passes the conformance kit, is
[`examples/json_table_store.py`](../examples/json_table_store.py).

### An object store: immutable names

Name each object with the writing attempt's generation and write it
create-only:

```
{output}/{partition}/{key}/{version}.{generation}.json   a key at a version
{output}/{partition}@{generation}.json                  a value
{output}/{partition}/{batch:012d}/{generation}.json      a batch
```

A keyed load computes names from `Keys` (`(version, locator)` → the
object), never by listing. Batches: several attempts may write batch n
(a retry reuses its number); the committed one is the highest generation.
`discard` deletes the names it is given. FileStore and S3Store
(`solera.stores`) are this recipe.

### A key-value store: conditional puts

Either kind works. Immutable: keys like `{key}@{version}.{generation}`,
written with put-if-absent, read through `Keys`. Fenced: keep a fence
record per partition and make every write conditional on it — a
transaction or compare-and-set that checks the fence record holds
`(generation, invocation)` in the same atomic operation as the write. A
check followed by a separate write is not a fence: the write can land
after a newer attempt took over.

### Append-only sinks

A sink that can only append (a log, a stream) is immutable when each
record carries `(batch, generation)` and readers keep, per batch, the
highest generation: an abandoned attempt's records are then never read.

## Scenarios

The conformance kit runs these sequences against your store. Each states
the exact outcome. (`gN` is generation N; content is `{key: version}`.)

| Kind | Scenario | Sequence | Expected |
|---|---|---|---|
| all | a replacement is the scope's whole content | g1 writes {a:1, b:1}; g2 replaces with {b:1, c:1} | the scope holds b:1, c:1 |
| all | a patch changes only its keys | g1 writes {a:1, b:1, d:1}; g2 patches b:2, c:1, removes a | b:2, c:1, d:1 |
| all | an empty replacement holds no key | g1 writes {a:1}; g2 replaces with zero rows | nothing |
| all | a write repeated by its attempt lands once | g4 writes {a:1, b:1}; g4 (same invocation) writes it again | same version; a:1, b:1 |
| all | batches append and load by range | g1 appends batch 3 {a}; g2 batch 4 {b} | whole: a, b; `Batches(4, 4)`: b |
| immutable | a pinned read returns its version | g5 writes {a:1}, pin; g9 writes {a:2} | the pin reads a:1; the new ref a:2 |
| immutable | discarding never takes what is read | g5 writes {a:1}; g7 writes b (never committed); g9 writes {a:2}; discard a@g5, b@g7 and a name never written, twice | the new ref reads a:2 |
| fenced | a stale writer is refused | g5 writes {a:1}; g9 acquires, writes {a:2}; g5 writes {a:0} | g5 refused (`StoreError`); a:2 |
| fenced | one generation admits one invocation | g5 writes; g9 acquires as x; g9 as y acquires, then writes | y refused both times; x acquiring again succeeds; x's write stands |
| fenced | a first write acquires | g3 acquires an empty slice, writes {a:1}; g2 writes | g2 refused; a:1 |
| fenced | the next attempt replaces what a dead writer left | g1 writes {a:1}; g5's patch of c lands, then g5 dies; g9 acquires, replaces with {a:2, b:1}; g5 patches again | a:2, b:1 (c gone); g5 refused |
| fenced | a newer writer waits for an open older one | g5's write transaction is open; g9 acquires | g9 waits until g5 commits, then takes the slice; g5's next write refused |

The last scenario needs a hook only you can write: `Harness.hold(scope)`,
an async context manager that opens a write transaction of `scope`
holding its fence until the block ends, then commits.

## The conformance kit

```python
import uuid
import pytest
from solera.sdk import Output
from solera.testing.stores import Harness, scenarios

@pytest.fixture
def harness():
    return Harness(
        store=MyStore(DSN),
        output=lambda **decl: Output(f"t_{uuid.uuid4().hex[:12]}", store="mine", **decl),
        hold=None,  # or your open-transaction hook, for a fenced store
    )

@pytest.mark.parametrize("scenario", scenarios(MyStore), ids=lambda s: s.__name__)
async def test_my_store_conforms(harness, scenario):
    await scenario(harness)
```

`output(**decl)` must return a fresh output on your store each call
(scenarios never share data), accepting the declarations the scenarios use:
`key="id", revision="v"` (rows `{"id", "v"}` keyed by `id`),
`incremental=True` (batches of rows), or none. The scenarios drive the
store as the engine does — keyed writes as `KeyedWrite`s resolved against a
key index the kit keeps, keyed loads through `Keys` — and raise
`AssertionError` with the scenario's expectation when the store answers
otherwise. FileStore, S3Store, PostgresStore and the example SQL store
pass it in Solera's own tests (`tests/sdk/test_store_conformance.py`),
where the example store with its fence left out fails it.

## What the engine guarantees a store

- One attempt at a time works on a scope (an attempt holds the scope's
  lock while it runs); a second invocation of an attempt, or a writer the
  engine gave up on, is exactly what your kind makes harmless.
- A fenced store's `acquire` is called after the producer computed and
  before the attempt reads the store, every time.
- Generations only grow for a scope: every later attempt's is larger.
- The engine records a commit only after `store` returned; a ref it
  records is one your store returned.
- `discard` is called only with names no reader pins, in the scope's next
  attempt or right after a commit (`lifecycle.md` §9.8).
