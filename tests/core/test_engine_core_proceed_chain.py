"""A reader whose walk ends over a proceeded upstream head is not complete (round 4, chaos
seeds 8505 and 7827): an operator's proceed lets readers of that output go on, not readers of
their outputs, so the reader's own readers stay held, as `settle`'s rule (iii) already says."""

import asyncio

import pytest
from solera.sdk import In, Incremental, Output, Project, Retry, asset
from solera.stores import FileStore

from tests.contract.harness import Solera, rebuild


@pytest.fixture
async def solera(tmp_path):
    s = Solera(tmp_path, "core")
    yield s
    await s.close()


@pytest.mark.xfail(strict=True, reason="ERW-42: proceed is refused on the new tracking core")
async def test_a_walk_over_a_proceeded_head_does_not_complete_its_reader(solera):
    hold, waiting, seen = asyncio.Event(), [False], []
    hold.set()

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k} for k in "abcd"]

    @asset(inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"), retries=Retry(n=0))
    async def copy(ctx, files: list):
        if ctx.batch["files"].index > 0:
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

    await solera.deploy(
        Project(assets=[files, copy, second, report], default_store=FileStore(solera.root / "data")),
        cancel_grace=0.2,
        heartbeat_seconds=0.1,
    )
    eng = solera.engine
    await solera.run("files")
    hold.clear()
    run = await solera.submit("copy")
    await solera.until(lambda: waiting[0])
    await solera.cancel(run["id"])
    await solera.wait(run)
    hold.set()
    await eng.proceed("copy", "", "test")  # readers of copy may read {"a"}
    await solera.run("second")  # its walk ends at copy's proceeded head
    state = {
        "spans": {str(k): v for k, v in eng.m.partial_spans.items()},
        "second": sorted(await solera.rows("second")),
    }
    reader = await solera.submit("report")
    try:
        await solera.wait(reader, 3)
    except TimeoutError:
        pass
    state["seen"] = seen
    await solera.cancel(reader["id"])
    assert not seen, f"report read second, built from copy's partial head, with no proceed on second: {state}"
