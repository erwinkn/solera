"""An attempt launched just after a deploy that made its keyed output unkeyed names a core
id for it (AttemptLaunched mints from the contract it prepared under). The output then has
a core id and no core commits, so its axis head is 0 for good and a later full run opens no
span: its readers read partial content (round 2, Fable; cause L, the axis read from "has a
core id")."""

import asyncio
from unittest.mock import patch

import pytest
from solera.sdk import In, Incremental, Output, Project, Retry, asset
from solera.stores import FileStore
from solera_server.executors.inline import InlinePlacement
from solera_server.tracking import full

from tests.contract.harness import Solera, rebuild


@pytest.fixture
async def solera(tmp_path):
    s = Solera(tmp_path)
    yield s
    await s.close()


class W:
    def __init__(self, s):
        self.s = s
        self.rows = {k: 1 for k in "abcd"}
        self.hold, self.waiting, self.seen = asyncio.Event(), [False], []
        self.hold.set()

    def project(self, kind):
        w = self

        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in w.rows.items()]

        @asset(
            inputs={"files": Incremental(batch_size=1)},
            outputs=Output("copy", key="id") if kind == "keyed" else Output("copy"),
            retries=Retry(n=0),
        )
        async def copy(ctx, files: list):
            b = ctx.batch["files"]
            if b.index > 0:
                w.waiting[0] = True
                await w.hold.wait()
                w.waiting[0] = False
            out = [{"id": r["id"], "v": r["v"]} for r in files]
            if kind == "keyed":
                return rebuild(b, out)
            held = None if b.reset else await ctx.load()
            return [*(held or []), *out]

        @asset(inputs={"copy": In()}, retries=Retry(n=0))
        def report(copy: list):
            w.seen.append(sorted(r["id"] for r in copy))
            return len(copy)

        return Project(assets=[files, copy, report], default_store=FileStore(w.s.root / "data"))


async def test_a_launch_that_races_an_axis_change_leaves_no_core_id_on_the_unkeyed_output(solera):
    w = W(solera)
    await solera.deploy(w.project("keyed"), cancel_grace=0.2, heartbeat_seconds=0.1)
    await solera.run("files")
    await solera.run("copy")
    eng = solera.engine
    w.rows["e"] = 1
    await solera.run("files")

    # The launch of copy's next attempt stops while its spec is written.
    gate, blocked = asyncio.Event(), asyncio.Event()
    create = eng.state.create_object

    async def slow(path, data, *a, **kw):
        if not gate.is_set() and path.endswith(".spec"):
            blocked.set()
            await gate.wait()
        return await create(path, data, *a, **kw)

    eng.state.create_object = slow
    first = await solera.submit("copy")
    await solera.until(blocked.is_set)
    new = w.project("value")
    eng.redeploy(new.manifest)  # copy is a single value now: its axis changed, its life is reset
    solera.project = new
    assert ("copy", "") not in eng.m.core_outputs
    gate.set()
    original = InlinePlacement._work

    def work(self, stage):
        self.project = new
        return original(self, stage)

    with patch.object(InlinePlacement, "_work", work):
        try:
            await solera.wait(first, 10)
        except TimeoutError:
            pass
        stale_id = eng.m.core_outputs.get(("copy", ""))
        # copy under its new definition, to the end; then a full run of it stopped midway.
        await solera.run("copy", expect=None)
        await solera.run("copy", expect=None)
        await solera.settle(0.3)
        whole = sorted(r["id"] for r in await solera.read("copy"))
        assert whole == ["a", "b", "c", "d", "e"], whole
        w.hold.clear()
        w.waiting[0] = False
        stopped = await solera.submit("copy", mode="full")
        await solera.until(lambda: w.waiting[0])
        partial = sorted(r["id"] for r in await solera.read("copy"))
        reader = await solera.submit("report")
        await solera.settle(0.6)
        state = (
            f"core id of the unkeyed output: {stale_id}; copy holds {partial}; waiting {sorted(full.waiting(eng))}; "
            f"spans {dict(eng.m.partial_spans)}; axis head {eng.m.axis_head('copy', '')}"
        )
        w.hold.set()
        await solera.wait(stopped)
        await solera.wait(reader)
    assert w.seen == [["a", "b", "c", "d", "e"]], f"report read {w.seen} while copy was mid full run. {state}"
