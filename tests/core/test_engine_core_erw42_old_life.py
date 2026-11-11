"""Review round 4 (Fable F2): a whole reader in flight over a proceeded head across a store move. Strict xfails until ERW-42 lets an operator proceed on the new tracking core
with that fact owned; they are its tests."""

import asyncio

import pytest
from solera.sdk import In, Incremental, Output, Project, Retry, asset
from solera.stores import FileStore
from solera_server.executors.inline import InlinePlacement
from solera_server.tracking import full

from tests.contract.harness import Solera, rebuild

ERW42 = "ERW-42: an operator's proceed is refused on the new tracking core until 'read at a proceeded head' has an owner"
pytestmark = pytest.mark.xfail(strict=True, reason=ERW42)


@pytest.mark.parametrize("meanwhile", ["store move", "copy completes", "nothing"])
async def test_pcA_a_whole_reader_in_flight_over_a_proceeded_head(tmp_path, monkeypatch, meanwhile):
    s = Solera(tmp_path, "core")
    hold, waiting, seen, projects = asyncio.Event(), [False], [], {}
    hold.set()
    hold2, waiting2 = asyncio.Event(), [False]
    hold2.set()

    def project(store):
        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k} for k in "abcd"]

        @asset(
            inputs={"files": Incremental(batch_size=1)},
            outputs=Output("copy", key="id", store=store),
            retries=Retry(n=0),
        )
        async def copy(ctx, files: list):
            if not hold.is_set() and ctx.batch["files"].index >= 1:
                waiting[0] = True
                await hold.wait()
            return rebuild(ctx.batch["files"], files)

        @asset(inputs={"copy": In()}, outputs=Output("second", key="id"), retries=Retry(n=0))
        async def second(copy: list):
            if not hold2.is_set():
                waiting2[0] = True
                await hold2.wait()
            return [{"id": r["id"]} for r in copy]

        @asset(inputs={"second": In()}, retries=Retry(n=0))
        def report(second: list):
            seen.append(sorted(r["id"] for r in second))
            return len(second)

        made = Project(
            assets=[files, copy, second, report],
            stores={"other": FileStore(tmp_path / "other")},
            default_store=FileStore(tmp_path / "data"),
        )
        projects[made.manifest["deploy"]] = made
        return made

    try:
        await s.deploy(project("default"), cancel_grace=0.2, heartbeat_seconds=0.1)
        work = InlinePlacement._work

        def current_worker(placement, stage):
            placement.project = projects.get(s.engine.manifest["deploy"], placement.project)
            return work(placement, stage)

        monkeypatch.setattr(InlinePlacement, "_work", current_worker)
        monkeypatch.setattr(full, "settles", lambda *a, **k: False)  # pc1's cause, kept out of this probe
        await s.run("files")
        await s.run("copy")
        await s.run("second")
        await s.run("report")
        hold.clear()
        run = await s.submit("copy", mode="full")
        await s.until(lambda: waiting[0])
        await s.cancel(run["id"])
        await s.wait(run)
        hold.set()
        await s.engine.proceed("copy", "", "test")
        hold2.clear()
        reading = await s.submit("second")  # pins copy's proceeded head {a}
        await s.until(lambda: waiting2[0])
        if meanwhile == "store move":
            moved = project("other")
            s.engine.redeploy(moved.manifest)
            s.project = moved
        elif meanwhile == "copy completes":
            await s.run("copy")
        hold2.set()
        detail = await s.wait(reading)
        tries = [(a["outcome"], (a.get("error") or "")[:80]) for t in detail["attempts"].values() for a in t]
        state = {
            "second's tries": tries,
            "second holds": sorted(await s.rows("second")),
            "spans": {str(k): v for k, v in s.engine.m.partial_spans.items()},
            "waiting": sorted(full.waiting(s.engine)),
        }
        n = len(seen)
        reader = await s.submit("report")
        try:
            await s.wait(reader, 3)
        except TimeoutError:
            await s.cancel(reader["id"])
        state["report read"] = seen[n:]
        assert not seen[n:] or seen[-1] == list("abcd"), (
            f"report read second, built from copy's partial head, with no proceed on second: {state}"
        )
    finally:
        hold.set()
        hold2.set()
        await s.close()
