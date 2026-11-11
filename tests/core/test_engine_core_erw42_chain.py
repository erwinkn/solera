"""Review round 4 (Fable F1), the proceed chain: a reader of a proceeded head, and its readers. Strict xfails until ERW-42 lets an operator proceed on the new tracking core
with that fact owned; they are its tests."""

import asyncio

import pytest
from solera.sdk import In, Incremental, Output, Project, Retry, asset
from solera.stores import FileStore
from solera_server.tracking import full

from tests.contract.harness import Solera, rebuild

ERW42 = "ERW-42: an operator's proceed is refused on the new tracking core until 'read at a proceeded head' has an owner"
pytestmark = pytest.mark.xfail(strict=True, reason=ERW42)


class Chain:
    def __init__(self, tmp_path, reader="incremental", second_batch=10):
        self.s = Solera(tmp_path, "core")
        self.hold, self.waiting, self.seen = asyncio.Event(), [False], []
        self.hold.set()
        self.stop_at = [1]
        self.hold2, self.waiting2, self.stop2 = asyncio.Event(), [False], [None]
        self.hold2.set()
        c = self

        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k} for k in "abcd"]

        @asset(
            inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"), retries=Retry(n=0)
        )
        async def copy(ctx, files: list):
            if not c.hold.is_set() and ctx.batch["files"].index >= c.stop_at[0]:
                c.waiting[0] = True
                await c.hold.wait()
            return rebuild(ctx.batch["files"], files)

        if reader == "incremental":

            @asset(
                inputs={"copy": Incremental(batch_size=second_batch)},
                outputs=Output("second", key="id"),
                retries=Retry(n=0),
            )
            async def second(ctx, copy: list):
                if c.stop2[0] is not None and ctx.batch["copy"].index >= c.stop2[0]:
                    c.waiting2[0] = True
                    await c.hold2.wait()
                return rebuild(ctx.batch["copy"], copy)

        else:

            @asset(inputs={"copy": In()}, outputs=Output("second", key="id"), retries=Retry(n=0))
            def second(copy: list):
                return [{"id": r["id"]} for r in copy]

        @asset(inputs={"second": In()}, retries=Retry(n=0))
        def report(second: list):
            c.seen.append(sorted(r["id"] for r in second))
            return len(second)

        self.project = Project(
            assets=[files, copy, second, report], default_store=FileStore(self.s.root / "data")
        )

    async def start(self):
        await self.s.deploy(self.project, cancel_grace=0.2, heartbeat_seconds=0.1)
        return self.s

    async def stop_copy_midway(self, **fields):
        s = self.s
        self.hold.clear()
        self.waiting[0] = False
        run = await s.submit("copy", **fields)
        await s.until(lambda: self.waiting[0])
        await s.cancel(run["id"])
        await s.wait(run)
        self.hold.set()

    async def reads(self, seconds=3):
        """Whether a run of report gets to read second within `seconds`: what it read, or None."""
        s, n = self.s, len(self.seen)
        reader = await s.submit("report")
        try:
            await s.wait(reader, seconds)
        except TimeoutError:
            pass
        out = self.seen[n:] or None
        if out is None:
            await s.cancel(reader["id"])
            await s.wait(reader)
        return out

    def spans(self):
        return {str(k): v for k, v in self.s.engine.m.partial_spans.items()}


async def complete_then_partial(c):
    """Everything complete over abcd; then copy's full run stops after its first batch (the clear)
    and the operator proceeds: copy holds {a}."""
    s = await c.start()
    await s.run("files")
    await s.run("copy")
    await s.run("second")
    await s.run("report")
    assert c.seen[-1] == list("abcd")
    await c.stop_copy_midway(mode="full")
    assert sorted(await s.rows("copy")) == ["a"]
    await s.engine.proceed("copy", "", "test")
    return s


async def test_pc1_a_whole_reader_of_a_proceeded_head_stays_partial(tmp_path):
    c = Chain(tmp_path, reader="whole")
    try:
        s = await complete_then_partial(c)
        await s.run("second")  # whole read of copy's proceeded head {a}
        at_commit = c.spans()
        await s.settle(0.5)
        state = {
            "spans at second's commit": at_commit,
            "spans after ticks": c.spans(),
            "second": sorted(await s.rows("second")),
        }
        read = await c.reads()
        state["report read"] = read
        assert read is None, (
            f"report read second, a whole copy of copy's partial head, with no proceed on second: {state}"
        )
    finally:
        c.hold.set()
        await c.s.close()


async def test_pc2_the_upstream_completing_does_not_complete_a_reader_that_has_not_reread(tmp_path):
    c = Chain(tmp_path)
    try:
        s = await complete_then_partial(c)
        await s.run("second")  # removes b, c, d: second holds {a}
        assert sorted(await s.rows("second")) == ["a"]
        assert await c.reads(2) is None  # the round-4 fix: held
        before = c.spans()
        await s.run("copy")  # the stopped walk is taken to its end: copy holds abcd, complete
        assert sorted(await s.rows("copy")) == list("abcd")
        await s.settle(0.5)
        state = {
            "spans before copy completed": before,
            "spans after": c.spans(),
            "second holds": sorted(await s.rows("second")),
            "second stale": await s.stale("second"),
        }
        read = await c.reads()
        state["report read"] = read
        assert read is None or read[-1] == list("abcd"), (
            f"report read second's content built from copy's partial head, missing keys no complete commit "
            f"of copy ever lacked, with no proceed on second and before second read copy again: {state}"
        )
    finally:
        c.hold.set()
        await c.s.close()


async def test_pc3_the_first_batch_on_a_whole_head_does_not_complete_the_reader(tmp_path, monkeypatch):
    c = Chain(tmp_path, second_batch=1)
    try:
        s = await complete_then_partial(c)
        await s.run("second")  # holds {a}
        monkeypatch.setattr(full, "settles", lambda *a, **k: False)  # isolate the commit path from settle
        await s.run("copy")  # copy complete: abcd
        c.stop2[0] = 1  # second's catch-up: its first batch (b) commits, its second waits
        c.hold2.clear()
        run = await s.submit("second")
        await s.until(lambda: c.waiting2[0])
        state = {"spans": c.spans(), "second holds": sorted(await s.rows("second"))}
        read = await c.reads()
        state["report read"] = read
        c.hold2.set()
        await s.wait(run)
        assert read is None, (
            f"report read second mid catch-up: keys c and d are still missing as second read them at copy's "
            f"partial head, with no proceed on second: {state}"
        )
    finally:
        c.hold.set()
        c.hold2.set()
        await c.s.close()


async def test_pc5_a_skip_over_the_proceeded_head_keeps_the_reader_partial(tmp_path):
    c = Chain(tmp_path)
    try:
        s = await complete_then_partial(c)
        await s.run("second")
        first = c.spans()
        detail = await s.run("second", expect=None)  # nothing owed at the same proceeded head: a skip
        state = {"first": first, "after skip": c.spans(), "run": detail["request"]["status"]}
        assert ("second", "") in full.waiting(s.engine), state
        assert await c.reads(2) is None, state
    finally:
        c.hold.set()
        await c.s.close()


async def test_pc6_a_replay_rebuilds_the_spans(tmp_path):
    c = Chain(tmp_path)
    try:
        s = await complete_then_partial(c)
        await s.run("second")
        before = c.spans()
        await s.restart()
        assert c.spans() == before
        assert await c.reads(2) is None
    finally:
        c.hold.set()
        await c.s.close()


async def test_pc7_the_alarm_names_the_reader_and_proceed_on_it_lets_report_read(tmp_path):
    c = Chain(tmp_path)
    try:
        s = await complete_then_partial(c)
        await s.run("second")
        stopped = [(x["asset"], x["output"]) for x in full.stopped(s.engine)]
        assert ("second", "second") in stopped and ("copy", "copy") not in stopped, stopped
        await s.engine.proceed("second", "", "test")
        assert (await c.reads())[-1] == ["a"]
        await s.run("second", expect=None)  # a skip at the same head: stays proceeded
        assert (await c.reads())[-1] == ["a"]
        await s.restart()
        assert (await c.reads())[-1] == ["a"]
    finally:
        c.hold.set()
        await c.s.close()


async def test_pc8_the_reader_completes_once_it_read_the_whole_head(tmp_path):
    c = Chain(tmp_path)
    try:
        s = await complete_then_partial(c)
        await s.run("second")
        await s.run("copy")
        await s.run("second")
        await s.settle(0.3)
        assert not full.waiting(s.engine) and not full.stopped(s.engine), (c.spans(), full.stopped(s.engine))
        assert (await c.reads())[-1] == list("abcd")
        assert not [k for k, v in s.engine.m.partial_spans.items() if v[-1][1] is None], c.spans()
    finally:
        c.hold.set()
        await c.s.close()


async def test_pc9_a_start_over_of_the_reader_over_the_proceeded_head(tmp_path):
    """second is proceeded too, then starts over while copy is still proceeded-partial: a new epoch,
    so report is held again (D205) and the alarm names second again."""
    c = Chain(tmp_path)
    try:
        s = await complete_then_partial(c)
        await s.run("second")
        await s.engine.proceed("second", "", "test")
        assert (await c.reads())[-1] == ["a"]
        await s.run("second", mode="full")
        state = {"spans": c.spans(), "stopped": full.stopped(s.engine)}
        assert await c.reads(2) is None, state
        assert ("second", "") in full.waiting(s.engine), state
    finally:
        c.hold.set()
        await c.s.close()
