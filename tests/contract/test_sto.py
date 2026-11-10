"""STO: fenced and immutable stores (docs/behaviors.md)."""

import asyncio
import os
import uuid

import pytest
from solera.sdk import Incremental, Output, Project, RegistrationError, Retry, asset
from solera.stores import FileStore, Patch, StoreError, WriteContext

from tests.contract.harness import rebuild

DSN = os.environ.get("SOLERA_TEST_DATABASE_URL")


@pytest.fixture(params=["file", pytest.param("postgres", marks=pytest.mark.postgres)])
def store(request, tmp_path):
    """A shipped store, and what an output on it declares."""

    if request.param == "file":
        yield FileStore(tmp_path / "data"), {}
        return
    if not DSN:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    import psycopg
    from solera_postgres import PostgresStore

    schema = f"contract_{uuid.uuid4().hex[:10]}"
    yield (
        PostgresStore("env:SOLERA_TEST_DATABASE_URL"),
        {"schema": schema, "columns": {"id": "text", "v": "text"}},
    )
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def written(store, decl, value: dict) -> Project:
    """`items`, keyed by `id` on `store`, writes `value["v"]` as it stands."""

    @asset(outputs=Output("items", key="id", store="s", **decl))
    def items():
        return value["v"]

    return Project(assets=[items], stores={"s": store})


async def content(solera) -> dict:
    """Each key's rows' values, as a reader of `items` gets them."""

    out = {}
    for row in await solera.read("items"):
        out.setdefault(str(row["id"]), []).append(row["v"])
    assert set(out) == set(await solera.keys("items"))
    return {k: sorted(v) for k, v in out.items()}


async def test_sto_6_writes_mean_the_same_on_every_store(solera, store):
    store, decl = store
    value = {"v": [{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]}
    await solera.deploy(written(store, decl, value))
    await solera.run("items")
    assert await content(solera) == {"a": ["1"], "b": ["1"]}
    value["v"] = [{"id": "c", "v": "1"}]  # a replacement is the whole content
    await solera.run("items")
    assert await content(solera) == {"c": ["1"]}
    value["v"] = Patch([{"id": "d", "v": "1"}])  # a patch changes only its keys
    await solera.run("items")
    assert await content(solera) == {"c": ["1"], "d": ["1"]}
    value["v"] = Patch([{"id": "c", "v": "2"}], remove=["d"])
    await solera.run("items")
    assert await content(solera) == {"c": ["2"]}
    value["v"] = Patch([{"id": "e", "v": "1"}, {"id": "e", "v": "2"}])  # many rows of a key: its group
    await solera.run("items")
    assert await content(solera) == {"c": ["2"], "e": ["1", "2"]}
    value["v"] = Patch({"e": [], "f": [{"v": "1"}]})  # a key with no rows does not exist
    await solera.run("items")
    assert await content(solera) == {"c": ["2"], "f": ["1"]}


async def test_sto_6_a_key_given_no_rows_does_not_exist(solera):
    """docs/per-key-processing.md §6: a key with zero rows does not exist. A
    by-key write gives `b` no rows: a replacement leaves it out, a patch
    removes it — from the index, so from every consumer and store."""

    value = {"v": {"a": [{"n": 1}], "b": []}}

    @asset(outputs=Output("items", key="id"))
    def items():
        return value["v"]

    await solera.deploy(Project(assets=[items]))
    await solera.run("items")
    assert sorted(await solera.keys("items")) == ["a"]
    value["v"] = Patch({"b": [{"n": 2}]})
    await solera.run("items")
    assert sorted(await solera.keys("items")) == ["a", "b"]
    value["v"] = Patch({"b": []})
    await solera.run("items")
    assert sorted(await solera.keys("items")) == ["a"]


def files_project(content: dict, fn) -> Project:
    """`files`, a producer that writes what changed in `content` since it
    last ran, and `parse`, `fn` over each of its keys."""

    written = {}

    @asset(outputs=Output("files", keyed=True))
    def files():
        changed = {k: v for k, v in content.items() if written.get(k) != v}
        gone = [k for k in written if k not in content]
        written.clear()
        written.update(content)
        return Patch(changed, remove=gone)

    parse = asset(fn, inputs={"file": Incremental("files", each=True)}, outputs=Output("samples", key="path"))
    return Project(assets=[files, parse])


async def test_sto_6_one_call_per_key_many_rows_one_write(solera):
    """Each key's call returns its rows; the store takes every key's group
    in one write. A file that parses to nothing has no key in the output —
    a key with no rows does not exist — while its outcome says it was
    processed."""

    content = {"a.csv": "1,2", "b.csv": "3", "c.csv": ""}
    calls = []

    def parse(ctx, file: dict):
        calls.append(ctx.key)
        return [{"value": int(v)} for v in file["text"].split(",") if v]

    await solera.deploy(files_project({k: {"text": v} for k, v in content.items()}, parse))
    detail = await solera.run("parse", upstream=True)
    assert sorted(calls) == ["a.csv", "b.csv", "c.csv"]
    got = await solera.read("samples", typ=dict[str, list])
    assert {k: sorted(r["value"] for r in v) for k, v in got.items()} == {"a.csv": [1, 2], "b.csv": [3]}
    assert all(r["path"] == k for k, v in got.items() for r in v)  # the store stamps the key
    attempt = detail["attempts"][next(t["id"] for t in detail["tasks"] if t["asset"] == "parse")][-1]
    assert attempt["keys"] == {"ok": 3}


async def test_sto_7_a_byte_valued_key_fails_the_write_instead_of_vanishing(solera):
    """A key is a `str` or an `int`, by one rule for the index and the store.
    A `bytes` key once committed into the index while the store, naming it
    differently, wrote nothing; now the write fails and nothing changes."""

    pending = {"rows": [{"id": "a", "n": 1}]}

    @asset(outputs=Output("items", key="id"))
    def items():
        return Patch(pending["rows"])

    await solera.deploy(Project(assets=[items]))
    await solera.run("items")
    pending["rows"] = [{"id": b"b", "n": 2}]
    detail = await solera.run("items", expect="failed")
    assert "a key must be a str or an int" in detail["tasks"][0]["error"]
    assert sorted(await solera.keys("items")) == ["a"]


@pytest.mark.postgres
async def test_sto_7_a_key_its_column_would_change_fails_the_write(solera, tmp_path):
    """A Postgres `numeric` key column reads `01` back as `1`: that write
    fails, and the output keeps what it had."""

    if not DSN:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    import psycopg
    from solera_postgres import PostgresStore

    schema = f"contract_{uuid.uuid4().hex[:10]}"
    value = {"v": [{"id": "1", "v": "x"}]}
    decl = {"schema": schema, "columns": {"id": "numeric", "v": "text"}}
    try:
        await solera.deploy(written(PostgresStore("env:SOLERA_TEST_DATABASE_URL"), decl, value))
        await solera.run("items")
        value["v"] = Patch([{"id": "01", "v": "y"}])
        detail = await solera.run("items", expect="failed")
        assert "stored as another key" in detail["tasks"][0]["error"]
        assert sorted(await solera.keys("items")) == ["1"]
        assert [r["v"] for r in await solera.read("items")] == ["x"]
    finally:
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


# -- target (PR 2) --


class Faults:
    """A user's store, a shipped one beneath, that a test makes misbehave.
    `hang_after_write()`: the next write lands, then its worker hangs there,
    for the test to kill before the commit. `late`, an event: the next write
    is sent and lands only once it is set, after the worker that sent it is
    gone (a request still queued at the database); `outcomes` says how it
    landed. `before_load`, a coroutine function: awaited once, as the next
    load begins. `calls` lists what the engine and its workers asked of it,
    `versions` the version of each write that landed."""

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.before_load = self.late = self.after_write = self.landing = None
        self.landed, self.sent = asyncio.Event(), asyncio.Event()
        self.calls, self.outcomes, self.versions = [], [], []

    def hang_after_write(self) -> None:
        self.landed.clear()
        self.after_write = asyncio.Event().wait

    async def store(self, write, prior, context):
        self.calls.append("store")
        base = super().store
        if self.late is not None:
            late, self.late = self.late, None

            async def land():
                await late.wait()
                try:
                    await base(write, prior, context)
                    self.outcomes.append("written")
                except Exception as error:
                    self.outcomes.append(error)

            self.landing = asyncio.create_task(land())
            self.sent.set()
            await asyncio.Event().wait()  # its worker dies here
        written = await base(write, prior, context)
        self.versions.append(context.generation)
        self.landed.set()
        if self.after_write is not None:
            hook, self.after_write = self.after_write, None
            await hook()
        return written

    async def load(self, *args, **kw):
        self.calls.append("load")
        await self.loading()
        return await super().load(*args, **kw)

    async def loading(self) -> None:
        if self.before_load is not None:
            hook, self.before_load = self.before_load, None
            await hook()


class Files(Faults, FileStore):
    """A FileStore a test makes misbehave."""


COLUMNS = {"id": "text", "v": "text"}


@pytest.fixture
def table():
    """A Postgres store a test makes misbehave (`Faults`), with `schema`, a
    schema of its own for the outputs it holds; dropped afterwards."""

    if not DSN:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    import contextlib

    import psycopg
    from solera_postgres import PostgresStore

    class Table(Faults, PostgresStore):
        async def acquire(self, *args, **kw):
            self.calls.append("acquire")
            return await super().acquire(*args, **kw)

        @contextlib.asynccontextmanager
        async def reads(self):
            self.calls.append("load")
            await self.loading()
            async with super().reads() as reader:
                yield reader

    store = Table("env:SOLERA_TEST_DATABASE_URL")
    store.schema = f"contract_{uuid.uuid4().hex[:10]}"
    yield store
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{store.schema}" CASCADE')


def on(store, **columns) -> dict:
    """What an output on `store` declares, `Output(…, **on(store))`: a table's
    schema and columns, `id` and `v` unless `columns` says."""

    if isinstance(store, FileStore):
        return {"store": "s"}
    return {"store": "s", "schema": store.schema, "columns": columns or COLUMNS}


def in_table(store, table: str = "items") -> dict[str, list]:
    """Each key's values as the table holds them, read in the database
    itself: what anyone reading the table sees, whatever the log lists."""

    import psycopg

    out = {}
    with psycopg.connect(DSN) as conn:
        for key, v in conn.execute(f'SELECT id, v FROM "{store.schema}"."{table}"').fetchall():
            out.setdefault(key, []).append(v)
    return {k: sorted(v) for k, v in out.items()}


def rows(*pairs: str) -> list[dict]:
    """`rows("a=1", "b=2")`: key a at value 1, key b at value 2."""

    return [{"id": k, "v": v} for k, v in (p.split("=") for p in pairs)]


def items_on(store, writes: list, received: list | None = None, **decl) -> Project:
    """`items`, keyed by `id` on `store`, writes the next of `writes` each
    time it runs, the last one from then on; `copy`, on the default store,
    reads it incrementally and records what each batch received:
    `(classes, {key: value})`; `marker`, read by nothing, is `cleanup_pass`'s."""

    received = [] if received is None else received

    @asset(outputs=Output("items", key="id", **on(store)), **decl)
    def items():
        return writes.pop(0) if len(writes) > 1 else writes[0]

    @asset(inputs={"items": Incremental()}, outputs=Output("copy", key="id"))
    def copy(ctx, items: list):
        batch = ctx.batch["items"]
        classes = (sorted(batch.added), sorted(batch.updated), sorted(batch.removed))
        received.append((classes, {str(r["id"]): r["v"] for r in items}))
        return rebuild(batch, [{"id": r["id"], "v": r["v"]} for r in items])

    return Project(assets=[items, copy, marker()], stores={"s": store})


def marker():
    """`marker`, an output nothing reads, rewritten each time it runs."""

    n = [0]

    @asset(outputs=Output("marker", key="id"))
    def marker():
        n[0] += 1
        return rows(f"m={n[0]}")

    return marker


async def cleanup_pass(solera) -> None:
    """A barrier: the cleanups due by now have had their pass. `marker` is
    written twice; once the version its second write superseded is gone,
    and no cleanup run is queued or running, the engine has cleaned up
    after every commit before it. The clock moves a minute at each look,
    so that cleanups spaced out in time come due."""

    await solera.run("marker")
    superseded = await solera.head("marker"), await solera.keys("marker")
    await solera.run("marker")

    async def passed():
        solera.advance(60)
        if await solera.at("marker", *superseded) is not None:
            return False
        return not await solera.runs(status=["queued", "running"], origin=["cleanup"])

    await solera.until(passed)


async def served(solera, project) -> None:
    """`project`, on workers the test can kill, beating every 0.2 s."""

    await solera.deploy(project, placements=solera.workers(), heartbeat_seconds=0.2)


async def killed_after_writing(solera, store) -> None:
    """The attempt writing to `store` lands its write, and its worker is
    killed before the commit: what it wrote stays."""

    await solera.until(store.landed.is_set)
    assert await solera.kill_workers() == 1


def attempts_of(detail: dict, name: str) -> list[dict]:
    [task] = [t for t in detail["tasks"] if t["asset"] == name]
    return detail["attempts"].get(task["id"], [])


# -- STO-1 ------------------------------------------------------------------------


class Plain:
    """A user's store that declares no kind: FileStore's methods, but `hidden`."""

    def __init__(self, path, hidden=()):
        self._files, self._hidden = FileStore(path), {"writes", *hidden}

    def __getattr__(self, name):
        if name in self._hidden:
            raise AttributeError(name)
        return getattr(self._files, name)


async def test_sto_1_a_store_declares_no_kind(solera):
    """No store kinds: a store with the contract's methods registers and
    serves, saying nothing of how it writes."""

    @asset(outputs=Output("items", key="id", store="plain"))
    def items():
        return rows("a=1", "b=1")

    await solera.deploy(Project(assets=[items], stores={"plain": Plain(solera.root / "plain")}))
    await solera.run("items")
    assert await solera.rows("items") == {"a": "1", "b": "1"}


@pytest.mark.parametrize("method", ["store", "load", "cleanup"])
async def test_sto_1_registration_refuses_a_store_missing_a_method(solera, method):
    @asset(outputs=Output("items", key="id", store="plain"))
    def items():
        return rows("a=1")

    with pytest.raises(RegistrationError, match=rf"{method}\(\)"):
        await solera.deploy(
            Project(assets=[items], stores={"plain": Plain(solera.root / "plain", hidden=[method])})
        )


# -- STO-2 ------------------------------------------------------------------------


async def test_sto_2_a_store_that_names_objects_by_version_loads_exactly_the_planned_ones(solera):
    """g5 writes {a:1}, and `copy` plans it; g9 writes {a:2} before `copy`
    loads: `copy` still loads a:1."""

    files, received = Files(solera.root / "files"), []
    await solera.deploy(items_on(files, [rows("a=1"), rows("a=2")], received))
    await solera.run("items")

    async def meanwhile():  # once copy planned a at g5, before it loads
        await solera.run("items")

    files.before_load = meanwhile
    await solera.run("copy")
    assert received == [((["a"], [], []), {"a": "1"})]
    assert await solera.rows("items") == {"a": "2"}


# -- STO-3 ------------------------------------------------------------------------


@pytest.mark.postgres
async def test_sto_3_a_key_loaded_at_another_version_is_processed_as_loaded_and_nothing_replans(
    solera, table
):
    """`copy`'s batch plans k1@4 and k2@2; `items` commits k2 → 5 before the
    load. The batch gets k2 as updated at 5, in its one attempt, and once
    it commits k2 is not owed again."""

    received = []
    writes = [rows("k1=1", "k2=1"), rows("k1=4", "k2=2"), Patch(rows("k2=5"))]
    await solera.deploy(items_on(table, writes, received))
    await solera.run("items")
    await solera.run("copy")
    await solera.run("items")

    async def meanwhile():  # once copy planned k1@4 and k2@2, before it loads
        await solera.run("items")

    table.before_load = meanwhile
    detail = await solera.run("copy")
    assert received[1:] == [(([], ["k1", "k2"], []), {"k1": "4", "k2": "5"})]
    assert [a["outcome"] for a in attempts_of(detail, "copy")] == ["succeeded"]  # nothing replans
    assert await solera.stale("copy") == []
    await solera.run("copy")
    assert len(received) == 2 and await solera.rows("copy") == {"k1": "4", "k2": "5"}


# -- STO-4 ------------------------------------------------------------------------


@pytest.mark.postgres
async def test_sto_4_a_stale_writer_changes_nothing(table):
    """g5 writes; g9 writes; g5 writes again: refused, whole."""

    output = Output("items", key="id", **on(table))

    def by(generation: int) -> WriteContext:
        return WriteContext(
            output, "", attempt=f"a{generation}", generation=generation, worker_id=f"w{generation}"
        )

    first = await table.store(rows("a=1"), None, by(5))
    second = await table.store(rows("a=2", "b=2"), first.ref, by(9))
    with pytest.raises(StoreError):
        await table.store(Patch(rows("a=3", "c=3")), second.ref, by(5))
    assert in_table(table) == {"a": ["2"], "b": ["2"]}


@pytest.mark.postgres
async def test_sto_4_nothing_is_acquired_before_reads(solera, table):
    """`copy` reads `items` and writes a table: the engine asks the table
    for nothing before the write, whose transaction checks the version."""

    @asset(outputs=Output("items", key="id"))
    def items():
        return rows("a=1")

    @asset(inputs={"items": Incremental()}, outputs=Output("copy", key="id", **on(table)))
    def copy(ctx, items: list):
        return rebuild(ctx.batch["items"], items)

    await solera.deploy(Project(assets=[items, copy], stores={"s": table}))
    await solera.run("items")
    await solera.run("copy")
    assert table.calls == ["store"]
    assert in_table(table, "copy") == {"a": ["1"]}


# -- STO-5 ------------------------------------------------------------------------


@pytest.mark.postgres
async def test_sto_5_a_table_never_holds_a_row_its_log_does_not_have(solera, table):
    """`items` holds a and b. An attempt writes c and a → 2, and dies before
    its commit; its retry writes a → 2. At rest the table holds exactly the
    keys `items` lists, as its log has them: no c."""

    writes = [rows("a=1", "b=1"), Patch(rows("c=1", "a=2")), Patch(rows("a=2"))]
    await served(solera, items_on(table, writes, retries=Retry(1, delay=0)))
    await solera.run("items")
    table.hang_after_write()
    run = await solera.submit("items")
    await killed_after_writing(solera, table)
    assert (await solera.wait(run))["request"]["status"] == "succeeded"
    assert await solera.rows("items") == {"a": "2", "b": "1"}
    assert in_table(table) == {"a": ["2"], "b": ["1"]}
