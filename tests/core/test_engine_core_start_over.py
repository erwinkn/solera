"""A start-over batch that does not commit an output whose span is open closes that span
just above its head and opens none: the head stays in a closed span. If the walk then ends
without committing that output, completion, settle and proceed (which act on open spans
only) never free it (round 2, Fable). Here the asset returns `Result(outputs={})` for a
batch with no row to write."""

import asyncio

import pytest
from solera.sdk import In, Incremental, Output, Project, Result, Retry, asset
from solera.stores import FileStore
from solera_server.tracking import full

from tests.contract.harness import Solera, rebuild


@pytest.fixture
async def solera(tmp_path):
    s = Solera(tmp_path, "core")
    yield s
    await s.close()


@pytest.mark.parametrize(
    "then",
    [
        "to the end",
        pytest.param(
            "stopped again, then proceed",
            marks=pytest.mark.xfail(
                strict=True, reason="ERW-42: proceed is refused on the new tracking core"
            ),
        ),
    ],
)
async def test_a_start_over_that_writes_nothing_frees_its_output_at_the_end(solera, then):
    rows = {k: 1 for k in "abcd"}
    hold, waiting, seen = asyncio.Event(), [False], []
    hold.set()

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    @asset(inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"), retries=Retry(n=0))
    async def copy(ctx, files: list):
        b = ctx.batch["files"]
        if b.index > 0:
            waiting[0] = True
            await hold.wait()
            waiting[0] = False
        out = [{"id": r["id"], "v": r["v"]} for r in files if r["v"] > 0]
        if not out and not b.removed:
            return Result(outputs={})  # nothing to write for this batch
        return rebuild(b, out)

    @asset(inputs={"copy": In()}, retries=Retry(n=0))
    def report(copy: list):
        seen.append(sorted(r["id"] for r in copy))
        return len(copy)

    await solera.deploy(
        Project(assets=[files, copy, report], default_store=FileStore(solera.root / "data")),
        cancel_grace=0.2,
        heartbeat_seconds=0.1,
    )
    eng = solera.engine

    async def stop_midway(**fields):
        hold.clear()
        waiting[0] = False
        run = await solera.submit("copy", **fields)
        await solera.until(lambda: waiting[0])
        await solera.cancel(run["id"])
        await solera.wait(run)
        hold.set()

    await solera.run("files")
    await stop_midway()  # copy holds "a": partial
    spans1 = [list(x) for x in eng.m.partial_spans[eng.m.span_key("copy", "")]]
    for k in rows:
        rows[k] = 0  # no row qualifies any more
    await solera.run("files")
    answered = None
    if then == "to the end":
        await solera.run("copy", mode="full")
    else:
        await stop_midway(mode="full")
        answered = await eng.proceed("copy", "", "test")
    await solera.settle(0.5)
    reader = await solera.submit("report")
    try:
        await solera.wait(reader, 3)
    except TimeoutError:
        pass
    [task] = [t for t in (await solera.get(f"/runs/{reader['id']}"))["tasks"] if t["asset"] == "report"]
    state = {
        "report": (task["status"], task.get("held")),
        "spans before": spans1,
        "spans": dict(eng.m.partial_spans),
        "axis head": eng.m.axis_head("copy", ""),
        "core_empty": dict(eng.m.core_empty),
        "stopped": full.stopped(eng),
        "proceed answered": answered,
    }
    await solera.cancel(reader["id"])
    assert task["status"] == "succeeded", state
