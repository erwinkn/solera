"""Regressions the deterministic simulation found (tests/sim), each reduced
to its smallest engine-level sequence."""

import asyncio

import pytest
from solera.sdk import Incremental, Output, Project, asset
from solera.stores import FileStore

from ..conftest import whole
from .test_engine import drive, make_engine, state, status_of  # noqa: F401


@pytest.mark.xfail(strict=True, reason="sim finding: a keyed output's index survives a change of store")
async def test_a_keyed_output_moved_to_another_store_stays_readable(state, tmp_path):  # noqa: F811
    """An output moved to another store keeps its key index; the next write
    resolved against it stores only the keys that changed, so a key it did
    not change must still read back — from the store the head now names."""

    content = {"rows": [{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]}

    def project(store):
        @asset(outputs=Output("items", key="id", revision="v", store=store))
        def items():
            return content["rows"]

        return Project(assets=[items], stores={"other": FileStore(tmp_path / "other")})

    first = project(None)
    engine = make_engine(state, first)
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    await engine.stop()

    second = project("other")
    engine = make_engine(state, second)
    await engine.initialize()
    content["rows"] = [{"id": "a", "v": "2"}, {"id": "b", "v": "1"}]  # only a changes
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    head = state.model.heads[("items", "")]
    assert head["ref"]["store"] == "other"
    rows = await second.stores["other"].load(_ref(head), list[dict], await whole(state, "items"))
    assert sorted((r["id"], r["v"]) for r in rows) == [("a", "2"), ("b", "1")]


def _ref(head):
    from solera.sdk import Ref

    return Ref(**{k: head["ref"][k] for k in ("output", "store", "handle", "version", "partition", "meta")})


@pytest.mark.xfail(strict=True, reason="sim finding: a removed asset's launched attempt re-queues its task")
async def test_a_removed_assets_last_attempt_ends_its_run(state, monkeypatch):  # noqa: F811
    """§11: an attempt launched before its asset was removed settles under
    the contract it was launched with. When it commits a page and asks for
    more, the asset is gone: its task cannot run again, and its run must
    end rather than hold the task forever."""

    from solera_server import attempts

    monkeypatch.setattr(attempts, "AFTER_COMMIT_WAIT", 0.2)  # the stopped engine's answer to `finished`
    entered, release = asyncio.Event(), asyncio.Event()

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a"}, {"id": "b"}]

    @asset(inputs={"files": Incremental(page_size=1)})
    async def pages(files: list):
        entered.set()
        await release.wait()
        return []

    first = Project(assets=[files, pages])
    engine = make_engine(state, first)
    await engine.initialize()
    await drive(engine, await engine.submit(["files"]))
    run = await engine.submit(["pages"])
    while not entered.is_set():
        await engine.tick()
        await asyncio.sleep(0.01)
    await engine.stop()  # the attempt runs on; the next engine adopts it

    second = Project(assets=[files])  # `pages` is gone
    engine = make_engine(state, second)
    await engine.initialize()
    release.set()
    try:
        detail = await drive(engine, run, timeout=10)
    except TimeoutError:
        tasks = state.model.runs[run["id"]]["tasks"].values()
        raise AssertionError(f"the run never ends: {[(t['status'], t.get('held')) for t in tasks]}") from None
    assert status_of(detail) in {"succeeded", "failed", "canceled"}


@pytest.mark.xfail(strict=True, reason="sim finding: an attempt launched before a rename never settles")
async def test_an_attempt_launched_before_a_rename_settles(state, monkeypatch):  # noqa: F811
    """docs/object-store-state.md §2: a rename moves heads, indexes and
    outcomes to the new name; an attempt launched under the old name still
    settles — its run ends and the scope is free for the next one."""

    from solera_server import attempts

    monkeypatch.setattr(attempts, "AFTER_COMMIT_WAIT", 0.2)  # the stopped engine's answer to `finished`
    entered, release = asyncio.Event(), asyncio.Event()

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a"}]

    calls = []

    @asset(inputs={"files": Incremental()})
    async def old(files: list):
        calls.append(1)
        if len(calls) > 1:  # the second run's attempt is in flight across the rename
            entered.set()
            await release.wait()
        return [{"n": len(files)}]

    engine = make_engine(state, Project(assets=[files, old]))
    await engine.initialize()
    await drive(engine, await engine.submit(["old"], upstream=True))
    run = await engine.submit(["old"], mode="full")
    while not entered.is_set():
        await engine.tick()
        await asyncio.sleep(0.01)
    await engine.stop()

    @asset(inputs={"files": Incremental()}, aliases=["old"])
    async def new(files: list):
        return [{"n": len(files)}]

    engine = make_engine(state, Project(assets=[files, new]))
    await engine.initialize()
    release.set()
    try:
        await drive(engine, run, timeout=10)
    except TimeoutError:
        raise AssertionError(f"the run never ends: claims {list(state.model.claims)}") from None
    assert status_of(await drive(engine, await engine.submit(["new"], mode="full"))) == "succeeded"
