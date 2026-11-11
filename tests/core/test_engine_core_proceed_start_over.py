"""A start-over is a new epoch for every output of its partition (review round 3, Sol and
Fable): after an operator's proceed, a start-over whose first batch commits no output closes
the proceeded span below the head and opens a new span that is not proceeded, so readers wait
for the new walk again and the alarm sees it stopped."""

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


@pytest.mark.xfail(strict=True, reason="ERW-42: proceed is refused on the new tracking core")
async def test_a_start_over_after_proceed_holds_readers_again(solera):
    rows = {k: 1 for k in "abcd"}
    hold, waiting, seen, stop_at = asyncio.Event(), [False], [], [0]
    hold.set()

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    @asset(inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"), retries=Retry(n=0))
    async def copy(ctx, files: list):
        b = ctx.batch["files"]
        if b.index > stop_at[0]:
            waiting[0] = True
            await hold.wait()
            waiting[0] = False
        out = [{"id": r["id"], "v": r["v"]} for r in files if r["v"] > 0]
        if not out and not b.removed:
            return Result(outputs={})  # nothing to write for this batch
        return rebuild(b, out)

    @asset(inputs={"copy": In()}, retries=Retry(n=0))
    def report(copy: list):
        seen.append(sorted((r["id"], r["v"]) for r in copy))
        return len(copy)

    await solera.deploy(
        Project(assets=[files, copy, report], default_store=FileStore(solera.root / "data")),
        cancel_grace=0.2,
        heartbeat_seconds=0.1,
    )
    eng = solera.engine

    async def stop_midway(after, **fields):
        stop_at[0] = after
        hold.clear()
        waiting[0] = False
        run = await solera.submit("copy", **fields)
        await solera.until(lambda: waiting[0])
        await solera.cancel(run["id"])
        await solera.wait(run)
        hold.set()

    await solera.run("files")
    await stop_midway(0)  # copy holds "a": partial
    await eng.proceed("copy", "", "test")  # the operator lets readers go on over {"a"}
    await solera.run("report")
    assert seen[-1] == [("a", 1)]
    rows.update({"a": 0, "b": 2, "c": 2, "d": 2})  # the start-over's first batch ("a") writes nothing
    await solera.run("files")
    await stop_midway(1, mode="full")  # it starts over, commits "b" in its second batch, and stops
    await solera.settle(0.5)
    spans = {str(k): v for k, v in eng.m.partial_spans.items()}
    reader = await solera.submit("report")
    try:
        await solera.wait(reader, 3)
    except TimeoutError:
        pass
    [task] = [t for t in (await solera.get(f"/runs/{reader['id']}"))["tasks"] if t["asset"] == "report"]
    state = {
        "report": (task["status"], task.get("held")),
        "report read": seen[-1],
        "copy holds": sorted(await solera.rows("copy")),
        "spans": spans,
        "core_empty": dict(eng.m.core_empty),
        "stopped": full.stopped(eng),
    }
    await solera.cancel(reader["id"])
    assert task["status"] != "succeeded", (
        f"a reader read the restarted walk's partial output with no new proceed: {state}"
    )


@pytest.mark.xfail(strict=True, reason="ERW-42: proceed is refused on the new tracking core")
async def test_new_commits_after_an_omitted_startover_do_not_inherit_proceed(tmp_path):
    s = Solera(tmp_path, "core")
    release, entered = asyncio.Event(), asyncio.Event()
    round = 0
    seen = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k} for k in "abcd"]

    @asset(inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"), retries=Retry(n=0))
    async def copy(ctx, files: list):
        b = ctx.batch["files"]
        if (round == 0 and b.index == 1) or (round == 1 and b.index == 2):
            entered.set()
            await release.wait()
        if round == 1 and b.index == 0:
            return Result(outputs={})
        return rebuild(b, files)

    @asset(inputs={"copy": In()})
    def report(copy: list):
        seen.append(sorted(r["id"] for r in copy))
        return seen[-1]

    try:
        await s.deploy(
            Project(assets=[files, copy, report], default_store=FileStore(tmp_path / "data")),
            cancel_grace=0.1,
            heartbeat_seconds=0.1,
        )
        await s.run("files")
        first = await s.submit("copy")
        await s.until(entered.is_set)
        await s.cancel(first["id"])
        await s.wait(first)
        await s.engine.proceed("copy", "", "test")
        assert ("copy", "") not in full.waiting(s.engine)
        round = 1
        entered.clear()
        second = await s.submit("copy", mode="full")
        await s.until(entered.is_set)
        state = {
            "spans": dict(s.engine.m.partial_spans),
            "head": s.engine.m.axis_head("copy", ""),
            "empty": dict(s.engine.m.core_empty),
            "rows": sorted(await s.rows("copy")),
        }
        reader = await s.submit("report")
        await s.settle(0.3)
        state["reader"] = (await s.get(f"/runs/{reader['id']}"))["request"]["status"]
        state["seen"] = seen
        print(state)
        assert not seen and ("copy", "") in full.waiting(s.engine), state
        release.set()
        await s.wait(second)
    finally:
        release.set()
        await s.close()
