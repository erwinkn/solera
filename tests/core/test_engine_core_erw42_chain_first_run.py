"""Review round 4 (Fable F1): the repo's chain test continued. Strict xfails until ERW-42 lets an operator proceed on the new tracking core
with that fact owned; they are its tests."""

import asyncio

import pytest
from solera.sdk import In, Incremental, Output, Project, Retry, asset
from solera.stores import FileStore

from tests.contract.harness import Solera, rebuild

ERW42 = "ERW-42: an operator's proceed is refused on the new tracking core until 'read at a proceeded head' has an owner"
pytestmark = pytest.mark.xfail(strict=True, reason=ERW42)


async def test_pc2b_first_run_flavour(tmp_path):
    solera = Solera(tmp_path, "core")
    hold, waiting, seen = asyncio.Event(), [False], []
    hold.set()

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k} for k in "abcd"]

    @asset(inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"), retries=Retry(n=0))
    async def copy(ctx, files: list):
        if ctx.batch["files"].index > 0 and not hold.is_set():
            waiting[0] = True
            await hold.wait()
        return rebuild(ctx.batch["files"], files)

    @asset(inputs={"copy": Incremental(batch_size=1)}, outputs=Output("second", key="id"), retries=Retry(n=0))
    def second(ctx, copy: list):
        return rebuild(ctx.batch["copy"], copy)

    @asset(inputs={"second": In()}, retries=Retry(n=0))
    def report(second: list):
        seen.append(sorted(r["id"] for r in second))
        return len(second)

    events, record = [], None
    try:
        await solera.deploy(
            Project(assets=[files, copy, second, report], default_store=FileStore(solera.root / "data")),
            cancel_grace=0.2,
            heartbeat_seconds=0.1,
        )
        eng = solera.engine
        record = solera.state.record

        def hooked(*es, **kw):
            for e in es:
                if e.get("type") in ("Settled", "Proceeded"):
                    events.append((e["type"], e["asset"]))
                if e.get("type") == "AttemptFinished" and e.get("commit"):
                    t = eng.m.task(e["task"]) or {}
                    events.append(("commit", t.get("asset"), bool(e["commit"].get("over_proceeded"))))
            return record(*es, **kw)

        solera.state.record = hooked
        await solera.run("files")
        hold.clear()
        run = await solera.submit("copy")
        await solera.until(lambda: waiting[0])
        await solera.cancel(run["id"])
        await solera.wait(run)
        hold.set()
        await eng.proceed("copy", "", "test")
        await solera.run("second")
        reader = await solera.submit("report")  # held
        await solera.settle(0.5)
        assert not seen
        await solera.run("copy")  # the walk is taken to its end: abcd, complete
        await solera.settle(0.5)
        state = {
            "events": events,
            "spans": {str(k): v for k, v in eng.m.partial_spans.items()},
            "second": sorted(await solera.rows("second")),
            "copy": sorted(await solera.rows("copy")),
            "second stale": await solera.stale("second"),
        }
        try:
            await solera.wait(reader, 3)
        except TimeoutError:
            pass
        state["seen"] = seen
        assert not seen or seen[-1] == list("abcd"), (
            f"report read second, built from copy's partial head and not rebuilt since, with no proceed on second: {state}"
        )
    finally:
        hold.set()
        await solera.close()
