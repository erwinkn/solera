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
engine never committed — a key regressed to older rows, a deleted key
back, a commit replaced.

Waiting longer only makes that less likely. So every store guarantees it
cannot happen, in one of two ways — its **kind**:

| Kind | How a late writer is made harmless | Implement | A read sees |
|---|---|---|---|
| `immutable` | It writes only names no other attempt uses. A late write creates an object nothing references; the engine has it deleted later. | — | exactly the pinned generation |
| `fenced` | Every write checks, atomically, that its attempt still holds the partition. A newer attempt takes it first (`acquire`), so the older one is refused. | `acquire`, `keys` | the current rows |

Both kinds implement `cleanup`: an immutable store for what commits let
go of and what late writers left, both for an output removed or moved
away.

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
    async def store(self, write, prior, context) -> Written: ...
    async def load(self, ref, t, selection) -> Any: ...

    async def cleanup(self, output, *, home=None, partition=None, key=None,
                      generation=None, before=None) -> None: ...
    async def acquire(self, context, prior) -> None: ...          # fenced
    def keys(self, ref, among) -> Iterable[list[str]]: ...      # fenced
    async def migrate(self, output, migrations, context=None, prior=None) -> list[str]: ...  # optional
    def prepare(self, write, output) -> Prepared: ...          # optional: types of its own
    def reads(self) -> AsyncContextManager[Reader]: ...       # optional: a store of current rows

class Reader(Protocol):                                        # what `reads()` yields
    async def load(self, ref, t, selection) -> tuple[Any, int | None]: ...  # value, generation written
```

**`WriteContext`** — what a write belongs to:

| Field | Meaning |
|---|---|
| `output` | the `Output` declaration: `name`, `key`, `incremental`, `config` |
| `partition` | the partition key, `""` for an unpartitioned output |
| `commit_number` | for an incremental output, the commit number the engine assigned |
| `home` | the name the output's life began under: where a first write puts every partition of it, renamed or not (K25); `None` is the output's name |
| `reset` | the write starts the content over (a `full` run): keep nothing of `prior` |
| `attempt` | the writing attempt's id |
| `generation` | a number the engine assigns each attempt on a partition, larger for every later attempt; `None` outside an attempt |
| `worker_id` | which process runs the attempt: an attempt started twice has one generation and two workers, and only the one that owns it may write |

**`store(write, prior, context) -> Written(ref)`** applies a write and
returns a ref to the new content; the worker stamps the ref with the
attempt's generation, its version (`versions.md`). A store that wrote
nothing returns `prior`. `prior` is the committed head: where the content is — a
renamed output's objects, or table, stay where they were, so every ref to
them stays readable; the declaration names the place only of a first
write — and what the write builds on, unless `context.reset`, when nothing
of it is kept. `None` is a first write. `acquire` and `migrate` get it
too, to find the same place. A first write's place comes from
`context.home`, the name the output's life began under, not its name
now: one life keeps every partition in one place, so `copy` renamed
`mirror` writes a new partition under `copy/` too, and its whole-output
cleanup is one place (K25).

- An unkeyed, non-incremental output's write is a value: replace it.
- An unkeyed incremental output's write is a `Patch` of rows: append it as
  commit `context.commit_number` — or, reset, start the commits over at it.
- A keyed output's write arrives as a `KeyedWrite`, already resolved
  against the engine's key index. A store reads it three ways: `reset` —
  the write is the partition's entire content, so clear the partition first;
  `chunks()` — the keys to write, a chunk at a time, each `(key, rows)`,
  only that chunk taken from the write (`iter_chunks()` for a store
  writing on a thread of its own); and `removes` — the keys to delete.
  Every other key stays as it is. An immutable store writes a key at
  most once per generation (a retried call, the same bytes): the
  generation names its object. A key with zero rows
  does not exist. Plain values (a list of rows, a `Patch`) may also arrive
  outside the engine: `KeyedWrite.of(store, write, output, prior)` turns
  them into one. A store reading values of its own type defines
  `prepare(write, output)`.

**`prepare(write, output)`** (optional) reads a keyed write of the types
the store takes — see **Values a store takes** below. Without it, the
store takes plain Python.

**`load(ref, t, selection)`** materializes `t` (`list[dict]`, a DataFrame,
…). `selection` is `None` (everything), `Keys` (key → the generation that
last wrote it: only those keys) or `Commits(lo, hi)`. An immutable store
needs `Keys` to find a keyed output's objects, which it names by those
generations. With `Keys`, `t` may be `dict[str, T]`
(`solera.stores.by_key_type(t)` is `T`): each key's rows on their own, as
`T` — how per-key incremental reads a batch — and a key with no rows absent, since it
does not exist. `can_load` says which `t` a store loads, by key or not;
say only what `load` does.

### Cleanup

**`cleanup(output, *, home, partition, key, generation, before)`** deletes
every object of the output the pattern matches. A stored object is named
by identity: its output life (`home`, the name it began under, else the
output's own; see `WriteContext.home`), its `partition`, its `key` (an
unkeyed incremental output's commit number; a value has none) and the
`generation` that wrote it, which is also a key's version. Any field left
out matches anything, not only trailing ones, so it is a pattern, not a
prefix; `before=G` matches every generation older than `G`:

| Call | Deletes | When the engine asks |
|---|---|---|
| `cleanup(o, home=h, before=G)` | the whole output life | it was removed, or moved to another store; `G` is the first generation after, so a later life of the same name in the same store is never touched |
| `cleanup(o, partition=p)` | a partition | a removed dynamic partition |
| `cleanup(o, partition=p, key=k, generation=g)` | one superseded version | a commit replaced it |
| `cleanup(o, partition=p, generation=g)` | all an attempt wrote | it ended without committing, or wrote after it ended |

A store declares `cleanup_after`, how long a removed or moved output's
data stays before a cleanup task deletes it (an output may override it).
A built-in store's class and config enter the manifest, so that task can
rebuild it once the project no longer declares it; registration refuses
a secret written in them — a DSN with a password, S3 keys — naming the
field: pass `env:NAME` instead, resolved in the worker. A store of your
own carries nothing: its cleanup uses the project's store of that name,
and is stuck, saying so, once that is gone.

The engine asks only for what no reader pins; the store just deletes,
idempotently — twice, or what was never written, is harmless. Each
store satisfies a pattern as it can: FileStore and S3Store by name (the
exact name when the pattern gives it, else a listing filtered by the
name's last two components), PostgresStore by `DELETE … WHERE`, or `DROP
TABLE` for the whole output. Its rows carry no generation, so a
partition's is its fence row's last write: its rows go only if that
matches, and the table is dropped only when no partition of it is
newer.

**`acquire(context, prior)`** (fenced) takes the partition for
`context.generation` and `context.worker_id`, in a transaction of its own,
before the attempt reads anything from the store. It must wait for an older writer's open
transaction on the partition, and refuse (`StoreError`) when a newer
generation, or another worker of this generation, holds it.

**`keys(ref, among)`** (fenced) yields the keys `ref`'s partition holds now —
among `among`, or all of them for None — in sorted chunks, by their
bytes; never a value. A repair asks it after a dead writer (which of the
keys it meant to change landed), and so does the reconciliation of an
opaque write that died (every key the partition holds), `versions.md` §5.

**Errors.** Raise `WriteError` for a malformed write (duplicate keys, a
wrong shape), `StoreError` for anything else the store refuses. A store
call that raises after the attempt began writing leaves its write
`writing`: the next attempt repairs them (fenced stores keep the
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
  of its keys, and `take(indices)`, the rows it stores:

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

Nothing reads a row's values but the store: a backend may coerce a data
column as its types say (declared columns are the user's contract). A key
is the exception — the index lists it, so the store must hold it as
itself: refuse a key your backend would read back as another (PostgresStore
fails the write when a key's canonical text, `key::text`, is not the key
it was given, e.g. `1.0` in a `numeric` key column).

## The invariants

A store of either kind:

1. **A write is the content the engine will record.** After `store`
   returns, a load of its ref (with the keys the index will hold) returns
   exactly the write applied to `prior`: a replacement drops the keys it
   does not name, a patch changes only its keys and removes, and a key
   written with zero rows is gone.
2. **An attempt's repeated write is one write.** The same attempt —
   generation and worker — writing the same content again (a retried
   call) leaves the same content and objects.

An `immutable` store:

3. **Names are never reused.** Every object's name includes the writing
   attempt's generation (or another value no other attempt uses), and
   writes are create-only. So nothing an attempt writes can replace what
   another wrote.
4. **A pinned read returns its generation's content.** A ref and selection read before a
   newer commit return the same content after it.
5. **Clean up deletes only what it names.** Cleaning up superseded names, an
   abandoned attempt's names, or names never written leaves every object a
   current or pinned reader needs.

A `fenced` store:

6. **A stale writer changes nothing.** Once generation g acquired a partition,
   every write of an older generation to it is refused, before it changes
   anything — including writes already queued at the backend.
7. **One generation, one worker.** A second worker of the
   generation holding the partition can neither acquire nor write; the holding
   worker acquiring again succeeds (its own retry).
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

An immutable store keeps every generation until no reader pins it, so a
run reads exactly the generations it pinned. A fenced store keeps one copy:
a load returns the rows as they are now. So a run can read two outputs at
different moments, and a row changed after the run pinned it is read in
its newer form — and delivered again with the change that made it, a
harmless repeat. Fencing makes writes safe; it does not make reads
repeatable — so such a store says what it read, with `reads()`:

- **One moment.** `async with store.reads() as reader:` — every
  `reader.load(ref, t, selection)` runs in one snapshot (PostgresStore: a
  REPEATABLE READ, READ ONLY transaction), so an attempt's inputs from the
  store are read together. The worker opens it for the inputs and closes
  it before the producer runs: a long producer holds no snapshot.
- **The generation it saw.** Each load returns `(value, generation)`: the
  generation whose write transaction last changed the partition, read in the
  same snapshot. Keep it beside the fence: set it in every write
  transaction, as it commits — never in `acquire`, which writes nothing
  (an acquisition writes no content: reporting it would claim content
  never read). A write that changes no row still sets it: the worker
  gives a fenced store an empty write only to repair a dead writer's,
  and the partition must then read as the repair's (`versions.md` §5).
  `solera.fencing` does both: `fence(cur, context, domain, write=True)` in
  a write, `written(cur, domain, partition)` in a read. None if no fenced
  write changed the partition.

The engine records it as lineage, which names what was read: a snapshot
store's read is the pinned generation; a current read is the generation
it saw — the pinned one, or a newer one — flagged `uncommitted` when no
attempt committed it, so lineage never claims content that was not read
(`versions.md` §6). The conformance kit's
`READS` scenario checks a store that defines `reads()`.

## Recipes

### A SQL table: `fence()` in every write transaction

`solera.fencing.fence(cur, context, domain)` makes a SQL store fenced. Call
it first in every write transaction, and as `acquire`:

```python
from solera.fencing import fence, fence_table

class EventsStore:
    writes = "fenced"

    async def acquire(self, context, prior):
        with connect(self.dsn) as conn, conn.cursor() as cur:
            fence(cur, context, "public.events")

    async def store(self, write, prior, context):
        with connect(self.dsn) as conn, conn.cursor() as cur:   # one transaction
            fence(cur, context, "public.events")                   # before any change
            ...                                                   # DELETE / INSERT / MERGE
```

`fence` upserts `(domain, partition) → (generation, worker_id)` in the
`solera_fences` table (`fence_table(cur)` creates it, once), keeps the row
locked until the transaction ends, and raises `StoreError` when a newer
generation or another worker holds it. The row lock is what makes a
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

SQL a user hands the store for a write must not reach past the partition the
engine fenced and records. PostgresStore's `Sql` is a query, never a
statement: it is embedded in the store's own `INSERT … SELECT … FROM
(<query>) _src` and prepared, so DML, DDL and a second statement do not
parse; only a function the query calls could still write, and
`sql_read_only=True` reads the query in a READ ONLY transaction, which no
function can turn back (a `SET ROLE` can: a function may `RESET ROLE`).

Write a keyed output chunk by chunk: for `reset`, clear the partition first;
then for each of `write.chunks()`, delete its keys and insert their rows
(or `MERGE`); then delete `removes`. `keys(ref, among)` is a `SELECT
DISTINCT` of the key column over the partition, through a server-side cursor. A complete
example, which passes the conformance kit, is
[`examples/json_table_store.py`](../examples/json_table_store.py).

### An object store: immutable names

Name each object with the writing attempt's generation and write it
create-only:

```
{output}/{partition}/{key}/{generation}.json             a key, as a generation wrote it
{output}/{partition}@{generation}.json                  a value
{output}/{partition}/{commit_number:012d}/{generation}.json      a commit
```

A keyed load computes names from `Keys` (each key's generation → the
object), never by listing. Commits: several attempts may write commit n
(a retry reuses its number); the committed one is the highest generation.
`cleanup` matches names: a generation is a name's last component, a key
or commit the one before it. FileStore and S3Store
(`solera.stores`) are this recipe.

### A key-value store: conditional puts

Either kind works. Immutable: keys like `{key}@{generation}`,
written with put-if-absent, read through `Keys`. Fenced: keep a fence
record per partition and make every write conditional on it — a
transaction or compare-and-set that checks the fence record holds
`(generation, worker_id)` in the same atomic operation as the write. A
check followed by a separate write is not a fence: the write can land
after a newer attempt took over.

### Append-only sinks

A sink that can only append (a log, a stream) is immutable when each
record carries `(commit, generation)` and readers keep, per commit, the
highest generation: an abandoned attempt's records are then never read.

## Scenarios

The conformance kit runs these sequences against your store. Each states
the exact outcome. (`gN` is generation N; content is `{key: v}`, a row
`{"id": key, "v": v}`.)

| Kind | Scenario | Sequence | Expected |
|---|---|---|---|
| all | a replacement is the partition's whole content | g1 writes {a:1, b:1}; g2 replaces with {b:1, c:1} | the partition holds b:1, c:1 |
| all | a patch changes only its keys | g1 writes {a:1, b:1, d:1}; g2 patches b:2, c:1, removes a | b:2, c:1, d:1 |
| all | an empty replacement holds no key | g1 writes {a:1}; g2 replaces with zero rows | nothing |
| all | a write repeated by its attempt lands once | g4 writes {a:1, b:1}; g4 (same worker) writes it again | same ref; a:1, b:1 |
| all | commits append and load by range | g1 appends commit 3 {a}; g2 commit 4 {b} | whole: a, b; `Commits(4, 4)`: b |
| all | a replacement resolved writes its keys and removes the rest | g1 writes {a:1, b:1, c:1}; g2 replaces with {a:1, b:2}, resolved against the index | a and b at g2; c removed; a:1, b:2 |
| immutable | a pinned read returns its version | g5 writes {a:1}, pin; g9 writes {a:2} | the pin reads a:1; the new ref a:2 |
| immutable | cleaning up never takes what is read | g5 writes {a:1}; g7 writes b (never committed); g9 writes {a:2}; clean up a@g5, b@g7 and a name never written, twice | the new ref reads a:2 |
| fenced | a stale writer is refused | g5 writes {a:1}; g9 acquires, writes {a:2}; g5 writes {a:0} | g5 refused (`StoreError`); a:2 |
| fenced | one generation admits one worker | g5 writes; g9 acquires as x; g9 as y acquires, then writes | y refused both times; x acquiring again succeeds; x's write stands |
| fenced | a first write acquires | g3 acquires an empty partition, writes {a:1}; g2 writes | g2 refused; a:1 |
| fenced | the next attempt replaces what a dead writer left | g1 writes {a:1}; g5's patch of c lands, then g5 dies; g9 acquires, replaces with {a:2, b:1}; g5 patches again | a:2, b:1 (c gone); g5 refused |
| fenced | a partition says which keys it holds | g1 writes {a, b, é, B}; g5's patch of c lands, then g5 dies | `keys(ref, None)`: B, a, b, c, é (by bytes); among given keys, only those held |
| fenced | a newer writer waits for an open older one | g5's write transaction is open; g9 acquires | g9 waits until g5 commits, then takes the partition; g5's next write refused |

The last scenario needs a hook only you can write: `Harness.hold(context)`,
an async context manager that opens a write transaction of `partition`
holding its fence until the block ends, then commits.

## The conformance kit

```python
import uuid
import pytest
from solera.sdk import Output
from solera.testing.stores import Harness, scenarios

@pytest.fixture
def worker():
    return Harness(
        store=MyStore(DSN),
        output=lambda **decl: Output(f"t_{uuid.uuid4().hex[:12]}", store="mine", **decl),
        hold=None,  # or your open-transaction hook, for a fenced store
    )

@pytest.mark.parametrize("scenario", scenarios(MyStore), ids=lambda s: s.__name__)
async def test_my_store_conforms(worker, scenario):
    await scenario(worker)
```

`output(**decl)` must return a fresh output on your store each call
(scenarios never share data), accepting the declarations the scenarios use:
`key="id"` (rows `{"id", "v"}` keyed by `id`),
`incremental=True` (commits of rows), or none. The scenarios drive the
store as the engine does — keyed writes as `KeyedWrite`s resolved against a
key index the kit keeps, keyed loads through `Keys` — and raise
`AssertionError` with the scenario's expectation when the store answers
otherwise. FileStore, S3Store, PostgresStore and the example SQL store
pass it in Solera's own tests (`tests/sdk/test_store_conformance.py`),
where the example store with its fence left out fails it.

## What the engine guarantees a store

- One attempt at a time works on a partition (an attempt holds the partition's
  lock while it runs); a second worker of an attempt, or a writer the
  engine gave up on, is exactly what your kind makes harmless.
- A fenced store's `acquire` is called after the producer computed and
  before the attempt reads the store, every time.
- Generations only grow for a partition: every later attempt's is larger.
- The engine records a commit only after `store` returned; a ref it
  records is one your store returned.
- `cleanup` is called only for what no reader pins: in the partition's next
  attempt, right after a commit, or by a cleanup task once the output is
  removed or moved away (`lifecycle.md` §9.8).
