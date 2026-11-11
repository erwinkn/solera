"""Output lives across deploys on the new tracking core: a gate entry names the life it
read, a kind change ends a core life, proceed locks the lives of now, and a rebind away
from a removed upstream closes its edge (Sol's probes of PR 2's redesign, round 1)."""

import asyncio

import pytest
from solera.sdk import In, Incremental, Output, Project, Retry, asset
from solera.stores import FileStore
from solera_server.tracking import full

from tests.contract.harness import Solera, rebuild


@pytest.fixture
async def solera(tmp_path):
    s = Solera(tmp_path, "core")
    yield s
    await s.close()


class World:
    def __init__(self, s):
        self.s = s
        self.hold = asyncio.Event()
        self.hold.set()
        self.entered = asyncio.Event()
        self.report_hold = asyncio.Event()
        self.report_hold.set()
        self.report_entered = asyncio.Event()
        self.seen = []

    def project(self, store=None, keyed=True):
        w = self

        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": 1} for k in "abc"]

        @asset(
            inputs={"files": Incremental(batch_size=1)},
            outputs=Output("copy", key="id" if keyed else None, store=store),
            retries=Retry(n=0),
        )
        async def copy(ctx, files: list):
            if ctx.batch["files"].index > 0:
                w.entered.set()
                await w.hold.wait()
            if keyed:
                return rebuild(ctx.batch["files"], files)
            before = [] if ctx.batch["files"].reset else (await ctx.load() or [])
            return before + files

        @asset(inputs={"copy": In()}, retries=Retry(n=0))
        async def report(copy: list):
            w.seen.append([r["id"] for r in copy])
            w.report_entered.set()
            await w.report_hold.wait()
            return len(copy)

        return Project(
            assets=[files, copy, report],
            default_store=FileStore(w.s.root / "data"),
            stores={"other": FileStore(w.s.root / "other")},
        )

    async def cancel_midway(self, **kw):
        self.entered.clear()
        self.hold.clear()
        request = await self.s.submit("copy", **kw)
        await self.s.until(self.entered.is_set)
        await self.s.cancel(request["id"])
        await self.s.wait(request)
        self.hold.set()


@pytest.mark.xfail(strict=True, reason="ERW-42: proceed is refused on the new tracking core")
async def test_proceed_rechecks_lives_after_awaiting_locks(solera):
    w = World(solera)
    await solera.deploy(w.project(), cancel_grace=0.1, heartbeat_seconds=0.1)
    await solera.run("files")
    await solera.run("copy")
    await w.cancel_midway(mode="full")
    eng = solera.engine
    old_id = eng.m.core_outputs["copy", ""]
    lock = eng.core.lock(old_id)
    await lock.acquire()
    proceeding = asyncio.create_task(eng.proceed("copy"))
    await asyncio.sleep(0.05)
    new = w.project(store="other")
    eng.redeploy(new.manifest)
    solera.project = new
    from solera_server.executors.inline import InlinePlacement

    # A new InlinePlacement is selected from the current project for the new run.
    original = InlinePlacement._work

    def work(self, stage):
        self.project = new
        return original(self, stage)

    from unittest.mock import patch

    with patch.object(InlinePlacement, "_work", work):
        await w.cancel_midway(mode="full")
    new_id = eng.m.core_outputs["copy", ""]
    assert new_id != old_id and eng.core.tracking.full_run(new_id.encode()) is not None
    lock.release()
    await proceeding
    assert full.waiting(eng) == set()
    assert eng.core.tracking.full_run(new_id.encode()) is None, "proceed left the new life full-run marked"


async def test_old_whole_pin_is_not_reinterpreted_on_new_lifes_axis(solera):
    w = World(solera)
    await solera.deploy(w.project(), cancel_grace=0.1, heartbeat_seconds=0.1)
    await solera.run("files")
    await solera.run("copy", keys={"files": ["a", "b", "c"]})
    await solera.settle(0.2)
    assert not full.waiting(solera.engine)
    w.report_hold.clear()
    reader = await solera.submit("report")
    await solera.until(w.report_entered.is_set)
    await solera.deploy(w.project(store="other"), cancel_grace=0.1, heartbeat_seconds=0.1)
    await w.cancel_midway(mode="full")
    old_at = solera.engine.m.task(
        next(t["id"] for t in (await solera.get(f"/runs/{reader['id']}"))["tasks"])
    )["launched"]["prepared"]["gate"][0][2]
    w.report_hold.set()
    await solera.settle(0.5)
    detail = await solera.get(f"/runs/{reader['id']}")
    assert detail["request"]["status"] == "succeeded", (old_at, detail)


async def test_keyed_to_value_output_uses_new_commits_axis(solera):
    w = World(solera)
    await solera.deploy(w.project(), cancel_grace=0.1, heartbeat_seconds=0.1)
    await solera.run("files")
    await solera.run("copy")
    await solera.deploy(w.project(keyed=False), cancel_grace=0.1, heartbeat_seconds=0.1)
    await w.cancel_midway(mode="full")
    reader = await solera.submit("report")
    await solera.settle(0.4)
    assert not w.seen, f"report read a partial first batch: {w.seen}"
    await solera.cancel(reader["id"])


async def test_rebinding_away_from_removed_output_closes_the_old_edge(tmp_path):
    s = Solera(tmp_path, "core")

    def project(old):
        @asset(outputs=Output("left", key="id"))
        def left():
            return [{"id": "a", "v": 1}]

        @asset(outputs=Output("right", key="id"))
        def right():
            return [{"id": "b", "v": 1}]

        @asset(inputs={"data": Incremental("left" if old else "right")}, outputs=Output("copy", key="id"))
        def copy(data: list):
            return data

        return Project(
            assets=[left, right, copy] if old else [right, copy], default_store=FileStore(tmp_path / "data")
        )

    try:
        await s.deploy(project(True))
        await s.run("left")
        await s.run("copy")
        old = s.engine.m.core_edges["copy", "", "data"]
        await s.deploy(project(False))
        assert ("copy", "", "data") not in s.engine.m.core_edges, old
        assert ("copy", "", "data") not in s.engine.m.core_read
    finally:
        await s.close()
