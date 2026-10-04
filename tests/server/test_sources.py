"""D147: a source's data is loaded through a function (`@source`) or its
store's `serve`, which says the version each key was served at; the batch
carries it as `served` (docs/stores.md, "Sources: how data is loaded")."""

import pytest
from solera.sdk import In, Incremental, Loaded, Output, Project, Ref, RegistrationError, Source, asset, source
from solera.stores import FileStore
from solera_server.state import State

from .engines import drive, make_engine, status_of


async def _engine(tmp_path, project):
    state = await State.open((tmp_path / "state").as_uri(), "test", flush_interval=0.001)
    engine = make_engine(state, project)
    await engine.initialize()
    return state, engine


async def test_a_function_source_serves_rows_with_their_versions(tmp_path):
    """Keyed: called with the batch's keys. The consumer gets the rows, and
    `served` each key's version."""

    outside = {"a": "1", "b": "2"}
    seen = []

    @source(key="id")
    async def files(keys, ctx):
        return {
            k: Loaded({"id": k, "v": outside[k]}, version=f"etag-{outside[k]}") for k in keys if k in outside
        }

    @asset(inputs={"files": Incremental()}, outputs=Output("copy", key="id"))
    def copy(ctx, files: list):
        seen.append((sorted(r["id"] for r in files), dict(ctx.batch["files"].served)))
        return files

    state, engine = await _engine(
        tmp_path, Project(assets=[copy], sources=[files], default_store=FileStore(tmp_path / "out"))
    )
    await engine.commit_source("files", upsert={"a": "1", "b": "2"})
    assert status_of(await drive(engine, await engine.submit(["copy"]))) == "succeeded"
    assert seen == [(["a", "b"], {"a": "etag-1", "b": "etag-2"})]
    await engine.stop()
    await state.close()


async def test_a_key_the_loader_leaves_out_was_observed_absent():
    from solera.stores import Keys
    from solera_worker.sources import SourceLoader

    @source(key="id")
    async def files(keys, ctx):
        return {"a": Loaded({"id": "a"}, version="e1")}

    reader = SourceLoader(files)
    rows = await reader.load(Ref("files", "default", {}, ""), list, Keys({"a": 1, "b": 2}))
    assert rows == [{"id": "a"}] and reader.served == {"a": "e1", "b": None}


async def test_a_keyed_loader_must_say_what_it_served(tmp_path):
    @source(key="id")
    async def files(keys, ctx):
        return {k: {"id": k} for k in keys}  # rows, but no versions

    @asset(inputs={"files": Incremental()})
    def count(files: list):
        return len(files)

    state, engine = await _engine(tmp_path, Project(assets=[count], sources=[files]))
    await engine.commit_source("files", upsert={"a": "1"})
    detail = await drive(engine, await engine.submit(["count"]))
    assert status_of(detail) == "failed"
    [[attempt]] = detail["attempts"].values()
    assert "Loaded(row, version=" in attempt["error"]
    await engine.stop()
    await state.close()


async def test_an_unkeyed_function_source_serves_its_value(tmp_path):
    @source
    async def rates(keys, ctx):
        assert keys is None
        return Loaded({"eur": 1.1}, version="2026-10-04")

    @asset(inputs={"rates": In()})
    def priced(rates: dict):
        return rates["eur"] * 2

    state, engine = await _engine(tmp_path, Project(assets=[priced], sources=[rates]))
    await engine.commit_source("rates", version="v1")
    assert status_of(await drive(engine, await engine.submit(["priced"]))) == "succeeded"
    await engine.stop()
    await state.close()


def test_registration_refuses_a_keyed_source_loaded_without_versions(tmp_path):
    """A keyed source loaded as data needs a loader that says what it served;
    one read as a `Ref` loads nothing, so needs none."""

    @asset(inputs={"uploads": Incremental()})
    def ingest(uploads: list):
        return []

    with pytest.raises(RegistrationError, match="which version"):
        Project(assets=[ingest], sources=[Source("uploads", key="id")])

    @asset(inputs={"uploads": In()})
    def refs(uploads: Ref):
        return []

    Project(assets=[refs], sources=[Source("uploads", key="id")])


async def test_a_file_source_serves_objects_at_the_stores_version(tmp_path):
    """A store-backed source: objects under `path`, each served as `{key: name,
    "content": bytes}` at the store's word for it — on local disk its stamp
    (inode, mtime, size); with `hash=True` a hash of its content."""

    data = tmp_path / "data"
    (data / "incoming").mkdir(parents=True)
    (data / "incoming" / "a.txt").write_text("one")
    seen = {}

    @asset(inputs={"docs": Incremental()})
    def sizes(ctx, docs: list):
        seen["docs"] = ({r["name"]: r["content"] for r in docs}, dict(ctx.batch["docs"].served))
        return {r["name"]: len(r["content"]) for r in docs}

    @asset(inputs={"hashed": Incremental()})
    def hashes(ctx, hashed: list):
        seen["hashed"] = dict(ctx.batch["hashed"].served)
        return len(hashed)

    project = Project(
        assets=[sizes, hashes],
        sources=[
            Source("docs", key="name", path="incoming"),
            Source("hashed", key="name", path="incoming", hash=True),
        ],
        default_store=FileStore(data),
    )
    state, engine = await _engine(tmp_path, project)
    for name in ("docs", "hashed"):
        await engine.commit_source(name, upsert={"a.txt": "v1"})
    assert status_of(await drive(engine, await engine.submit(["sizes", "hashes"]))) == "succeeded"
    (content, served), hashed = seen["docs"], seen["hashed"]
    assert content == {"a.txt": b"one"} and served["a.txt"]  # the file's stamp
    assert hashed == {"a.txt": "sha256:7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"}
    await engine.stop()
    await state.close()


async def test_an_unkeyed_file_source_serves_the_objects_bytes(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    (data / "rates.json").write_text('{"eur": 1.1}')
    got = []

    @asset(inputs={"rates": In()})
    def priced(rates: bytes):
        got.append(rates)
        return 1

    project = Project(
        assets=[priced], sources=[Source("rates", path="rates.json")], default_store=FileStore(data)
    )
    state, engine = await _engine(tmp_path, project)
    await engine.commit_source("rates", version="v1")
    assert status_of(await drive(engine, await engine.submit(["priced"]))) == "succeeded"
    assert got == [b'{"eur": 1.1}']
    await engine.stop()
    await state.close()


async def test_a_table_source_serves_rows_at_their_version_column(tmp_path):
    import os
    import uuid

    dsn = os.environ.get("SOLERA_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    import psycopg
    from solera_postgres import PostgresStore

    table = f"orders_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"CREATE TABLE public.{table} (id text PRIMARY KEY, v int, updated_at bigint)")
        conn.execute(f"INSERT INTO public.{table} VALUES ('a', 1, 100), ('b', 2, 200)")
    seen = []

    @asset(inputs={"orders": Incremental()})
    def total(ctx, orders: list):
        seen.append(dict(ctx.batch["orders"].served))
        return sum(r["v"] for r in orders)

    pg = PostgresStore("env:SOLERA_TEST_DATABASE_URL")
    with pytest.raises(RegistrationError, match="which version"):  # keyed, and nothing names the version
        Project(
            assets=[total],
            sources=[Source("orders", store="pg", key="id", table=f"public.{table}")],
            stores={"pg": pg},
        )
    source = Source("orders", store="pg", key="id", table=f"public.{table}", version_column="updated_at")
    project = Project(
        assets=[total], sources=[source], stores={"pg": pg}, default_store=FileStore(tmp_path / "out")
    )
    state, engine = await _engine(tmp_path, project)
    await engine.commit_source("orders", upsert={"a": "100", "b": "200"})
    assert status_of(await drive(engine, await engine.submit(["total"]))) == "succeeded"
    assert seen == [{"a": "100", "b": "200"}]
    await engine.stop()
    await state.close()
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"DROP TABLE public.{table}")
