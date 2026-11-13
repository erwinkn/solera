"""An output whose commit axis changes in place (keyed, append-only, single value) is
reset on the new tracking core, so no partial span is read on the wrong axis (Fable's
probes of PR 2's redesign, round 1)."""

import asyncio

import pytest
from solera.sdk import In, Incremental, Output, Project, Retry, asset
from solera.stores import FileStore, Patch
from solera_server.tracking import full

from tests.contract.harness import Solera, rebuild


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path)
    yield namespace
    await namespace.close()


class Keyed:
    def __init__(self, solera):
        self.s = solera
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

        @asset(inputs={"copy": In()})
        def report(copy: list):
            w.seen.append(sorted(r["id"] for r in copy))
            return len(copy)

        return Project(assets=[files, copy, report], default_store=FileStore(w.s.root / "data"))

    async def deploy(self, kind):
        await self.s.deploy(self.project(kind), cancel_grace=0.2, heartbeat_seconds=0.1)


async def test_a_reader_waits_for_a_full_run_of_an_output_that_was_keyed(solera):
    w = Keyed(solera)
    await w.deploy("keyed")
    await solera.run("files")
    await solera.run("copy")
    await w.deploy("value")  # the same output on the same store, no longer keyed: its definition changed
    w.hold.clear()
    rebuild_run = await solera.submit("copy")  # a full run, of 4 batches: held after the first
    await solera.until(lambda: w.waiting[0])
    eng = solera.engine
    assert [r["id"] for r in await solera.read("copy")] == ["a"]  # partial: one key of four
    reader = await solera.submit("report")
    await solera.settle(0.5)
    [task] = [t for t in (await solera.get(f"/runs/{reader['id']}"))["tasks"] if t["asset"] == "report"]
    state = (task["status"], task.get("held"), w.seen, sorted(full.waiting(eng)), dict(eng.m.partial_spans))
    w.hold.set()
    await solera.wait(rebuild_run)
    await solera.wait(reader)
    assert w.seen == [["a", "b", "c", "d"]], f"report read {w.seen} while copy was mid full run: {state}"


async def test_readers_of_an_output_that_was_keyed_and_stopped_midway_go_on_once_it_is_complete(solera):
    w = Keyed(solera)
    await w.deploy("keyed")
    await solera.run("files")
    await solera.run("copy")
    w.hold.clear()
    stopped = await solera.submit("copy", mode="full")
    await solera.until(lambda: w.waiting[0])
    await solera.cancel(stopped["id"])
    await solera.wait(stopped)
    w.hold.set()
    await w.deploy("value")
    eng = solera.engine
    await solera.run("copy")  # a whole run under the new definition: complete
    await solera.settle(0.5)
    assert sorted(r["id"] for r in await solera.read("copy")) == ["a", "b", "c", "d"]
    reader = await solera.submit("report")
    try:
        await solera.wait(reader, 5)
    except TimeoutError:
        [task] = [t for t in (await solera.get(f"/runs/{reader['id']}"))["tasks"] if t["asset"] == "report"]
        raise AssertionError(
            f"copy is complete and report still waits: {task.get('held')}, "
            f"waiting {sorted(full.waiting(eng))}, spans {dict(eng.m.partial_spans)}, axis head {eng.m.axis_head('copy', '')}"
        ) from None
    assert w.seen == [["a", "b", "c", "d"]], w.seen


class W:
    def __init__(self, solera):
        self.s = solera
        self.rows = {k: 1 for k in "abcd"}
        self.hold, self.waiting, self.seen = asyncio.Event(), [False], []
        self.first = asyncio.Event()
        self.first.set()
        self.in_first = [False]
        self.hold.set()

    def project(self, kind):
        w = self

        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in w.rows.items()]

        output = {
            "keyed": Output("copy", key="id"),
            "value": Output("copy"),
            "append": Output("copy", incremental=True),
        }[kind]

        @asset(inputs={"files": Incremental(batch_size=1)}, outputs=output, retries=Retry(n=0))
        async def copy(ctx, files: list):
            b = ctx.batch["files"]
            if b.index == 0:
                w.in_first[0] = True
                await w.first.wait()
                w.in_first[0] = False
            else:
                w.waiting[0] = True
                await w.hold.wait()
                w.waiting[0] = False
            out = [{"id": r["id"], "v": r["v"]} for r in files]
            if kind == "keyed":
                return rebuild(b, out)
            if kind == "append":
                return Patch(out)
            held = None if b.reset else await ctx.load()
            return [*(held or []), *out]

        @asset(inputs={"copy": In()})
        def report(copy: list):
            w.seen.append(sorted(r["id"] for r in copy))
            return len(copy)

        return Project(assets=[files, copy, report], default_store=FileStore(w.s.root / "data"))

    async def deploy(self, kind):
        await self.s.deploy(self.project(kind), cancel_grace=0.2, heartbeat_seconds=0.1)

    async def stop_midway(self, **fields):
        self.hold.clear()
        stopped = await self.s.submit("copy", **fields)
        await self.s.until(lambda: self.waiting[0])
        await self.s.cancel(stopped["id"])
        await self.s.wait(stopped)
        self.hold.set()


async def test_a_reader_of_an_unkeyed_output_stopped_midway_that_becomes_keyed_reads_nothing_partial(solera):
    w = W(solera)
    await w.deploy("value")
    await solera.run("files")
    await w.stop_midway()  # its first run, stopped after one batch of four: partial
    assert [r["id"] for r in await solera.read("copy")] == ["a"]
    await w.deploy("keyed")
    eng = solera.engine
    assert eng.m.heads.get(("copy", "")) is None  # the axis change reset it
    w.first.clear()
    rebuild_run = await solera.submit("copy")
    await solera.until(lambda: w.in_first[0])  # launched: its first batch has not committed
    reader = await solera.submit("report")
    await solera.settle(0.5)
    [task] = [t for t in (await solera.get(f"/runs/{reader['id']}"))["tasks"] if t["asset"] == "report"]
    state = (
        task["status"],
        task.get("held"),
        task.get("error"),
        sorted(full.waiting(eng)),
        dict(eng.m.partial_spans),
        eng.m.axis_head("copy", ""),
    )
    assert w.seen == [] and task["status"] != "succeeded", (
        f"copy's head is its partial one and report is not held: {state}, read {w.seen}"
    )
    w.first.set()
    await solera.wait(rebuild_run)
    await solera.wait(reader)
    kinds, apply = [], eng.m.apply

    def spy(e):
        kinds.append(e["type"])
        return apply(e)

    eng.m.apply = spy
    await solera.settle(1.0)
    assert kinds.count("Settled") <= 1, (
        f"{kinds.count('Settled')} Settled events in a second of idle ticks; spans {dict(eng.m.partial_spans)}"
    )


async def test_settle_journals_at_most_once_after_an_unkeyed_output_became_keyed(solera):
    w = W(solera)
    await w.deploy("value")
    await solera.run("files")
    await w.stop_midway()
    await w.deploy("keyed")
    eng = solera.engine
    await solera.run("copy")
    kinds, apply = [], eng.m.apply

    def spy(e):
        kinds.append(e["type"])
        return apply(e)

    eng.m.apply = spy
    await solera.settle(1.0)
    assert kinds.count("Settled") <= 1, (
        f"{kinds.count('Settled')} Settled events in a second of idle ticks; spans {dict(eng.m.partial_spans)}"
    )


async def test_a_value_output_that_becomes_append_only_is_complete_after_a_full_run(solera):
    w = W(solera)
    await w.deploy("value")
    await solera.run("files")
    await solera.run("copy")  # 4 batches: its first 3 commits are partial, on the generation axis
    eng = solera.engine
    spans = [list(s) for s in eng.m.partial_spans[("copy", "")]]
    await w.deploy("append")
    eng = solera.engine
    await solera.run("copy")  # a full run under the new definition: commits 0..3
    n = 0
    while not any(lo <= eng.m.axis_head("copy", "") < hi for lo, hi in spans if hi is not None):
        n += 1
        w.rows[f"k{n:03d}"] = 1
        await solera.run("files")
        await solera.run("copy")
        assert n < 60, (spans, eng.m.axis_head("copy", ""))
    await solera.settle(0.5)
    assert full.stopped(eng) == [] and sorted(full.waiting(eng)) == [], (
        f"copy is complete at commit {eng.m.axis_head('copy', '')} and counts as partial: spans "
        f"{dict(eng.m.partial_spans)}, stopped {full.stopped(eng)}"
    )
