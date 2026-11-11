"""Review round 4 (Sol H1): readers of a proceeded head across output axes, incremental, whole and fan-in. Strict xfails until ERW-42 lets an operator proceed on the new tracking core
with that fact owned; they are its tests."""

import asyncio
import os

import pytest
from solera.sdk import In, Incremental, Output, Project, Retry, StaticPartitions, asset, canonical_partition
from solera.stores import FileStore, Patch
from solera_server.tracking import full

from tests.contract.harness import Solera, rebuild
from tests.core.test_engine_core_redeploy_random import assert_replay

ERW42 = "ERW-42: an operator's proceed is refused on the new tracking core until 'read at a proceeded head' has an owner"
pytestmark = pytest.mark.xfail(strict=True, reason=ERW42)


@pytest.mark.parametrize("kind", ["incremental", "whole", "fanin"])
@pytest.mark.parametrize("axis", ["keyed", "value", "append"])
async def test_chain_holds_reader_until_own_proceed_or_upstream_whole(tmp_path, monkeypatch, kind, axis):
    if os.environ.get("CHAIN_FIX_OFF"):
        monkeypatch.setattr(full, "over_proceeded", lambda *args: False)
    s = Solera(tmp_path, os.environ.get("REVIEW_TRACKING", "core"))
    hold, waiting, seen, commits = asyncio.Event(), asyncio.Event(), [], []
    hold.set()
    dims = {"site": StaticPartitions(["west"])} if kind == "fanin" else None
    part = canonical_partition(["site"], {"site": "west"}) if dims else ""

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k} for k in "abcd"]

    @asset(
        partitions=dims,
        inputs={"files": Incremental(batch_size=1)},
        outputs=Output("mid", key="id"),
        retries=Retry(n=0),
    )
    async def mid(ctx, files: list):
        if ctx.batch["files"].index > 0:
            waiting.set()
            await hold.wait()
        return rebuild(ctx.batch["files"], files)

    inp = (
        Incremental("mid", batch_size=10)
        if kind == "incremental"
        else In("mid", all_partitions=kind == "fanin")
    )

    async def body(ctx, rows):
        if kind == "fanin":
            rows = [r for rs in rows.values() for r in rs]
        if axis == "keyed":
            return rebuild(ctx.batch["rows"], rows) if kind == "incremental" else rows
        if axis == "append":
            return Patch(rows)
        return rows

    body.__annotations__["rows"] = dict[str, list] if kind == "fanin" else list
    body.__name__ = body.__qualname__ = "second"
    second = asset(
        inputs={"rows": inp},
        outputs=Output("second", key="id" if axis == "keyed" else None, incremental=axis != "value"),
        retries=Retry(n=0),
    )(body)

    @asset(inputs={"second": In()}, retries=Retry(n=0))
    def tail(second: list):
        seen.append(sorted(r["id"] for r in second))
        return seen[-1]

    try:
        await s.deploy(
            Project(assets=[files, mid, second, tail], default_store=FileStore(tmp_path / "data")),
            cancel_grace=0.1,
            heartbeat_seconds=0.1,
        )
        apply = s.engine.m.apply

        def spy(e):
            result = apply(e)
            if e.get("type") == "AttemptFinished" and e.get("commit"):
                commits.append((e["commit"].get("over_proceeded"), dict(s.engine.m.partial_spans)))
            return result

        s.engine.m.apply = spy
        await s.run("files")
        hold.clear()
        run = await s.submit("mid", partitions=[part])
        await s.until(waiting.is_set)
        await s.cancel(run["id"])
        await s.wait(run)
        hold.set()
        await s.engine.proceed("mid", part, "test")
        await s.run("second")
        await s.settle(0.3)
        reader = await s.submit("tail")
        await s.settle(0.3)
        state = {
            "seen": seen,
            "spans": dict(s.engine.m.partial_spans),
            "stopped": full.stopped(s.engine),
            "commits": commits,
        }
        print(kind, axis, state)
        assert not seen, state
        assert any(x["asset"] == "second" for x in full.stopped(s.engine)), state
        await assert_replay(s)
        await s.restart()
        assert ("second", "") in full.waiting(s.engine)
        await s.engine.proceed("second", "", "test")
        await s.wait(reader)
        assert seen == [["a"]]
        await s.run("mid", partitions=[part])
        await s.run("second", expect=None)
        await s.settle(0.3)
        assert ("second", "") not in full.waiting(s.engine), s.engine.m.partial_spans
        await assert_replay(s)
    finally:
        hold.set()
        await s.close()
