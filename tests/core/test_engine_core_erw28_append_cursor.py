"""Round 4 probe (older code, the unkeyed analog of Sol's round-3 H3): a task's cursor over an
append-only input is a commit number without the upstream's life. After the upstream is reset
(a store move) and another run of the reader commits on the new life, a paused run goes on from
its old commit number on the new life."""

import asyncio
import os

import pytest
from solera.sdk import Incremental, Output, Project, Retry, asset
from solera.stores import FileStore, Patch
from solera_server.executors.inline import InlinePlacement

from tests.contract.harness import Solera

PATH = os.environ.get("TRACKING_PATH", "core")


@pytest.mark.xfail(
    strict=True, reason="ERW-28: the commit cursor of an append-only input does not name the upstream life"
)
async def test_a_paused_runs_commit_cursor_survives_its_upstreams_reset(tmp_path, monkeypatch):
    s = Solera(tmp_path, PATH)
    n, pages, projects = [0], [], {}

    def project(store):
        @asset(outputs=Output("events", incremental=True, store=store))
        def events():
            n[0] += 1
            return Patch([{"n": n[0]}])

        @asset(inputs={"events": Incremental(batch_size=1)}, outputs=Output("tail"), retries=Retry(n=1))
        async def tail(ctx, events: list):
            b = ctx.batch["events"]
            held = [] if b.reset else (await ctx.load() or [])
            pages.append((b.index, b.reset, [e["n"] for e in events]))
            return held + [e["n"] for e in events]

        made = Project(
            assets=[events, tail],
            stores={"other": FileStore(tmp_path / "other")},
            default_store=FileStore(tmp_path / "data"),
        )
        projects[made.manifest["deploy"]] = made
        return made

    try:
        await s.deploy(project("default"))
        work = InlinePlacement._work

        def current_worker(placement, stage):
            placement.project = projects.get(s.engine.manifest["deploy"], placement.project)
            return work(placement, stage)

        monkeypatch.setattr(InlinePlacement, "_work", current_worker)
        for _ in range(4):
            await s.run("events")
        old = await s.submit("tail")
        entered, seen, record = asyncio.Event(), [0], s.state.record

        def hooked(*events, **kw):
            result = record(*events, **kw)
            for e in events:
                if e.get("type") == "AttemptFinished" and e.get("run") == old["id"] and e.get("commit"):
                    seen[0] += 1
                    if seen[0] == 2 and not entered.is_set():
                        s.engine._control(old["id"], "pause", "test")
                        entered.set()
            return result

        monkeypatch.setattr(s.state, "record", hooked)
        await s.until(entered.is_set)
        await s.settle(0.2)
        moved = project("other")
        s.engine.redeploy(moved.manifest)
        s.project = moved
        for _ in range(4):
            await s.run("events")  # 5, 6, 7, 8 on the new life
        await s.run("tail", expect=None)  # another run: reads the new life from its start
        after_other = await s.read("tail")
        await s.engine.pause(old["id"], paused=False)
        detail = await s.wait(old)
        await s.run("tail", expect=None)
        state = {
            "pages": pages,
            "after the other run": after_other,
            "tail": await s.read("tail"),
            "old run": detail["request"]["status"],
        }
        assert state["tail"] == [5, 6, 7, 8], state
    finally:
        await s.close()
