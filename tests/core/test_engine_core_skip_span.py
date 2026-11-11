"""A walk that ends in a batch with nothing to load (a skip) ends the walk from empty, and the
skip's event visits the asset's declared outputs (round 2, Fable). Here the skip reads a head
an operator proceeded over, so the walk's end leaves copy partial (a proceed lets readers of
mid go on, not readers of theirs: round 4, the proceed chain) until mid is whole again; then
copy's next run completes it."""

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


class W:
    """files -> mid (batch 1, holds past its first batch) -> copy (batch 10) -> report."""

    def __init__(self, s):
        self.s = s
        self.rows = {k: 1 for k in "abcd"}
        self.hold, self.waiting, self.seen = asyncio.Event(), [False], []
        self.hold.set()

    def project(self):
        w = self

        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in w.rows.items()]

        @asset(
            inputs={"files": Incremental(batch_size=1)}, outputs=Output("mid", key="id"), retries=Retry(n=0)
        )
        async def mid(ctx, files: list):
            b = ctx.batch["files"]
            if b.index > 0:
                w.waiting[0] = True
                await w.hold.wait()
                w.waiting[0] = False
            return rebuild(b, [{"id": r["id"], "v": r["v"]} for r in files])

        @asset(
            inputs={"mid": Incremental(batch_size=10)}, outputs=Output("copy", key="id"), retries=Retry(n=0)
        )
        async def copy(ctx, mid: list):
            return rebuild(ctx.batch["mid"], [{"id": r["id"], "v": r["v"]} for r in mid])

        @asset(inputs={"copy": In()}, retries=Retry(n=0))
        def report(copy: list):
            w.seen.append(sorted(r["id"] for r in copy))
            return len(copy)

        return Project(assets=[files, mid, copy, report], default_store=FileStore(w.s.root / "data"))


async def test_a_walk_that_ends_in_a_skip_completes_its_outputs(solera):
    w = W(solera)
    await solera.deploy(w.project(), cancel_grace=0.2, heartbeat_seconds=0.1)
    eng = solera.engine
    await solera.run("files")
    # mid stops after its first batch (holds "a"); the operator lets its readers go on.
    w.hold.clear()
    stopped = await solera.submit("mid")
    await solera.until(lambda: w.waiting[0])
    await solera.cancel(stopped["id"])
    await solera.wait(stopped)
    w.hold.set()
    await eng.proceed("mid", "", "test")
    # copy's first run is a key list: it takes "a", the only key mid holds, and its walk from empty stays pending.
    await solera.run("copy", keys={"mid": ["a"]})
    await solera.settle(0.3)
    before = (
        sorted(full.waiting(eng)),
        {k: [list(x) for x in v] for k, v in eng.m.partial_spans.items()},
        dict(eng.m.core_empty),
    )
    events, apply = [], eng.m.apply

    def spy(e):
        out = apply(e)
        events.append(
            (
                e["type"],
                e.get("outcome"),
                {k: [list(x) for x in v] for k, v in eng.m.partial_spans.items()},
                dict(eng.m.core_empty),
            )
        )
        return out

    eng.m.apply = spy
    detail = await solera.run("copy", expect=None)  # nothing owed: its batch is a skip, and the walk ends
    await solera.settle(0.5)
    ended = [e for e in events if e[0] == "AttemptFinished"]
    copy_spans = eng.m.partial_spans.get(eng.m.span_key("copy", ""))
    assert ("copy", "", "mid") not in eng.m.core_empty, (before, events)
    assert copy_spans and copy_spans[-1][1] is None and not copy_spans[-1][2], (
        f"copy's walk ended over mid's proceeded head (run {detail['request']['status']}, attempts {ended}), "
        f"so copy stays partial: {copy_spans}; before the run: {before}"
    )
    eng.m.apply = apply
    await solera.run("mid", mode="full")  # mid is whole again
    await solera.run("copy")
    await solera.settle(0.5)
    copy_spans = eng.m.partial_spans.get(eng.m.span_key("copy", ""))
    assert not copy_spans or copy_spans[-1][1] is not None, (copy_spans, full.stopped(eng))
    await solera.run("report")
    assert w.seen[-1] == sorted(w.rows), w.seen
