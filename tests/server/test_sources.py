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
