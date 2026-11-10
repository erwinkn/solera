"""STO: fenced and immutable stores (docs/behaviors.md)."""

import os
import uuid
from types import SimpleNamespace

import pytest
from solera.sdk import Incremental, Output, Project, asset
from solera.stores import FileStore, Patch
from solera.testing.stores import Ledger, context, now, rows, write

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


async def test_sto_6_the_same_attempt_writing_again_is_one_write(store):
    """A retried call: the same attempt — one generation, one worker — gives
    the store the same write twice, as the worker does
    (`solera.testing.stores`). A replacement, a patch and an incremental
    output's commit each land once: the same ref, their rows once."""

    store, decl = store
    h = SimpleNamespace(store=store)
    out, ledger = Output(f"t_{uuid.uuid4().hex[:12]}", key="id", store="s", **decl), Ledger()
    content = [{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]
    first = await write(h, out, content, 1, ledger)
    assert await write(h, out, content, 1, ledger) == first
    assert await now(h, first, ledger) == [("a", "1"), ("b", "1")]
    patch = [{"id": "b", "v": "2"}, {"id": "c", "v": "1"}]
    second = await write(h, out, patch, 2, ledger, first, remove=["a"], patch=True)
    assert await write(h, out, patch, 2, ledger, first, remove=["a"], patch=True) == second
    assert await now(h, second, ledger) == [("b", "2"), ("c", "1")]

    out = Output(f"t_{uuid.uuid4().hex[:12]}", incremental=True, store="s", **decl)
    commit = [{"id": "a", "v": "1"}]
    first = await store.store(Patch(commit), None, context(out, 1, commit_number=3))
    again = await store.store(Patch(commit), None, context(out, 1, commit_number=3))
    assert again.ref == first.ref
    assert await rows(h, again.ref, None) == [("a", "1")]


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
