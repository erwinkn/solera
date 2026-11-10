"""Partial content on the new tracking core (D205): the gate on the heads a batch reads,
partial spans, start-overs and the readers in flight (Fable's probes of PR 2's redesign)."""

import asyncio

import pytest
from solera.sdk import (
    In,
    Incremental,
    Output,
    Project,
    Result,
    Retry,
    Source,
    StaticPartitions,
    asset,
    canonical_partition,
)
from solera.stores import FileStore, Patch
from solera_server.tracking import full

from tests.contract.harness import Outside, Solera, SourceStore, rebuild


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path, "core")
    yield namespace
    await namespace.close()


class World:
    """files (keyed, rows) -> copy (keyed incremental, batch 1) -> report (whole).
    copy's batches past the first wait on hold while it is clear."""

    def __init__(self, solera, rows, batch_size=1):
        self.solera, self.rows, self.batch_size = solera, rows, batch_size
        self.hold = asyncio.Event()
        self.hold.set()
        self.waiting = False
        self.seen = []  # what report read
        self.copied = []  # copy's batches

    async def held(self):
        self.waiting = True
        await self.hold.wait()
        self.waiting = False

    def project(self, variant="inc", extra=()):
        w, rows = self, self.rows

        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in rows.items()]

        if variant == "inc":

            @asset(inputs={"files": Incremental(batch_size=w.batch_size)}, outputs=Output("copy", key="id"))
            async def copy(ctx, files: list):
                w.copied.append(sorted(r["id"] for r in files))
                if ctx.batch["files"].index > 0:
                    await w.held()
                return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

        elif variant == "renamed":  # the same input under another parameter name

            @asset(
                inputs={"src": Incremental("files", batch_size=w.batch_size)},
                outputs=Output("copy", key="id"),
            )
            async def copy(ctx, src: list):
                w.copied.append(sorted(r["id"] for r in src))
                if ctx.batch["src"].index > 0:
                    await w.held()
                return rebuild(ctx.batch["src"], [{"id": r["id"], "v": r["v"]} for r in src])

        elif variant == "whole":  # reads files whole now

            @asset(inputs={"files": In()}, outputs=Output("copy", key="id"))
            async def copy(ctx, files: list):
                await w.held()
                return [{"id": r["id"], "v": r["v"]} for r in files]

        @asset(inputs={"copy": In()})
        def report(copy: list):
            w.seen.append(sorted(r["id"] for r in copy))
            return len(copy)

        return Project(assets=[files, copy, report, *extra], default_store=FileStore(w.solera.root / "data"))

    async def deploy(self, variant="inc", **kw):
        await self.solera.deploy(self.project(variant), cancel_grace=0.2, heartbeat_seconds=0.1, **kw)
        return self

    async def cancel_midway(self, **fields):
        self.hold.clear()
        run = await self.solera.submit("copy", **fields)
        await self.solera.until(lambda: self.waiting)
        await self.solera.cancel(run["id"])
        detail = await self.solera.wait(run)
        self.hold.set()
        assert detail["request"]["status"] == "canceled", detail
        return detail


async def task_of(solera, run, name):
    [task] = [t for t in (await solera.get(f"/runs/{run['id']}"))["tasks"] if t["asset"] == name]
    return task


# test_window2.py: Window 2 of the note, on 1e9ee39: a reader planned at a complete head H whose worker is still


@pytest.mark.parametrize("reader", ["arch", "acc", "snap"])
async def test_a_reader_in_flight_commits_what_it_planned_when_its_upstream_clears(solera, reader, capsys):
    rows = {k: 1 for k in "abcd"}
    hold = {"copy": asyncio.Event(), "reader": asyncio.Event()}
    waiting = {"copy": False, "reader": False}
    for e in hold.values():
        e.set()
    told = []

    async def held(name):
        waiting[name] = True
        await hold[name].wait()
        waiting[name] = False

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    @asset(inputs={"files": Incremental(batch_size=2)}, outputs=Output("copy", key="id"))
    async def copy(ctx, files: list):
        if ctx.batch["files"].index > 0:
            await held("copy")
        return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

    @asset(inputs={"copy": Incremental(batch_size=10)}, outputs=Output("arch", key="id"))
    async def arch(ctx, copy: list):
        b = ctx.batch["copy"]
        told.append(("arch", sorted(r["id"] for r in copy), sorted(b.removed)))
        await held("reader")
        return rebuild(b, [{"id": r["id"], "v": r["v"]} for r in copy])

    @asset(inputs={"copy": Incremental(batch_size=10)}, outputs=Output("acc"))
    async def acc(ctx, copy: list):
        b = ctx.batch["copy"]
        prior = None if b.reset else await ctx.load()
        told.append(("acc", sorted(r["id"] for r in copy), sorted(b.removed)))
        await held("reader")
        out = dict((prior or {}).get("rows") or {})
        for r in copy:
            out[r["id"]] = r["v"]
        for k in b.removed:
            out.pop(k, None)
        return {"rows": out}

    @asset(inputs={"copy": In()}, outputs=Output("snap"))
    async def snap(copy: list):
        told.append(("snap", sorted(r["id"] for r in copy), []))
        await held("reader")
        return {"rows": {r["id"]: r["v"] for r in copy}}

    project = Project(assets=[files, copy, arch, acc, snap], default_store=FileStore(solera.root / "data"))
    await solera.deploy(project, cancel_grace=0.2, heartbeat_seconds=0.1)
    await solera.run("files")
    await solera.run("copy")
    await solera.run(reader)
    for k in rows:
        rows[k] = 2
    await solera.run("files")
    await solera.run("copy")  # copy at a complete head H: four keys at v=2
    told.clear()

    hold["reader"].clear()
    r = await solera.submit(reader)
    await solera.until(lambda: waiting["reader"])  # planned at H, its worker holds
    hold["copy"].clear()
    c = await solera.submit("copy", mode="full")
    await solera.until(lambda: waiting["copy"])  # copy cleared: {a, b}
    assert await solera.rows("copy") == {"a": 2, "b": 2}
    hold["reader"].set()
    detail = await solera.wait(r)
    got = await solera.rows("arch") if reader == "arch" else (await solera.read(reader))["rows"]
    [task] = [t for t in detail["tasks"] if t["asset"] == reader]
    with capsys.disabled():
        print(
            f"\n{reader}: status={detail['request']['status']} outcomes={task.get('outcomes')} got={got} told={told}"
        )
    assert detail["request"]["status"] == "succeeded", (detail["request"], task)
    assert got == {k: 2 for k in "abcd"}, got

    told.clear()
    again = await solera.submit(reader)
    await solera.settle(0.4)
    [t2] = [t for t in (await solera.get(f"/runs/{again['id']}"))["tasks"] if t["asset"] == reader]
    assert t2.get("held") == ["upstream incomplete", "copy"], t2
    hold["copy"].set()
    await solera.wait(c)
    await solera.wait(again)
    got = await solera.rows("arch") if reader == "arch" else (await solera.read(reader))["rows"]
    with capsys.disabled():
        print(f"{reader}: after copy completed: got={got} told={told}")
    assert got == {k: 2 for k in "abcd"}, got


# test_window2_join.py: Window 2 for a reader with two keyed inputs, on 1e9ee39 (FileStore). The batch takes a key on


async def test_a_two_input_reader_in_flight_when_one_input_clears(solera, capsys):
    left = {k: 1 for k in "abcd"}
    right = {k: 1 for k in "abcd"}
    hold = {"copy": asyncio.Event(), "join": asyncio.Event()}
    waiting = {"copy": False, "join": False}
    for e in hold.values():
        e.set()
    ran = []

    async def held(name):
        waiting[name] = True
        await hold[name].wait()
        waiting[name] = False

    @asset(outputs=Output("left", key="id"))
    def left_():
        return [{"id": k, "v": v} for k, v in left.items()]

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in right.items()]

    @asset(inputs={"files": Incremental(batch_size=2)}, outputs=Output("copy", key="id"))
    async def copy(ctx, files: list):
        if ctx.batch["files"].index > 0:
            await held("copy")
        return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

    @asset(
        inputs={"x": Incremental("left", batch_size=10), "y": Incremental("copy", batch_size=10)},
        outputs=Output("join"),
    )
    async def join(ctx, x: list, y: list):
        ran.append((sorted(r["id"] for r in x), sorted(r["id"] for r in y)))
        await held("join")
        prior = None if ctx.batch["x"].reset or ctx.batch["y"].reset else await ctx.load()
        out = {"x": dict((prior or {}).get("x") or {}), "y": dict((prior or {}).get("y") or {})}
        for side, rows in (("x", x), ("y", y)):
            for r in rows:
                out[side][r["id"]] = r["v"]
        return out

    left_.__name__ = "left"
    project = Project(assets=[left_, files, copy, join], default_store=FileStore(solera.root / "data"))
    await solera.deploy(project, cancel_grace=0.2, heartbeat_seconds=0.1)
    await solera.run("left")
    await solera.run("files")
    await solera.run("copy")
    await solera.run("join")
    left["a"] = 2
    await solera.run("left")
    ran.clear()
    hold["join"].clear()
    j = await solera.submit("join")
    await solera.until(
        lambda: waiting["join"]
    )  # planned: x takes a, y takes nothing; copy at a complete head
    hold["copy"].clear()
    c = await solera.submit("copy", mode="full")
    await solera.until(lambda: waiting["copy"])  # copy cleared
    hold["join"].set()
    await solera.settle(1.0)
    [task] = [t for t in (await solera.get(f"/runs/{j['id']}"))["tasks"] if t["asset"] == "join"]
    with capsys.disabled():
        print(
            f"\njoin after its upstream cleared: status={task['status']} outcomes={task.get('outcomes')} held={task.get('held')} batches={ran}"
        )
    hold["copy"].set()
    await solera.wait(c)
    detail = await solera.wait(j)
    [task] = [t for t in detail["tasks"] if t["asset"] == "join"]
    got = await solera.read("join")
    with capsys.disabled():
        print(
            f"join at the end: status={task['status']} outcomes={task.get('outcomes')} batches={ran} x.a={got['x'].get('a')}"
        )
    assert got["x"]["a"] == 2


# test_nested_rebuild.py: settle (and the gate at 1e9ee39) judge a producer complete against its upstream as it is now.


class NestedWorld:
    def __init__(self, solera, rows):
        self.solera, self.rows = solera, rows
        self.hold = {"mid": asyncio.Event(), "leaf": asyncio.Event()}
        for e in self.hold.values():
            e.set()
        self.waiting = {"mid": False, "leaf": False}
        self.seen = []

    async def held(self, name):
        self.waiting[name] = True
        await self.hold[name].wait()
        self.waiting[name] = False

    def project(self):
        w, rows = self, self.rows

        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in rows.items()]

        @asset(inputs={"files": Incremental(batch_size=1)}, outputs=Output("mid", key="id"))
        async def mid(ctx, files: list):
            if ctx.batch["files"].index > 0:
                await w.held("mid")
            return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

        @asset(inputs={"mid": Incremental(batch_size=1)}, outputs=Output("leaf", key="id"))
        async def leaf(ctx, mid: list):
            if ctx.batch["mid"].index > 0:
                await w.held("leaf")
            return rebuild(ctx.batch["mid"], [{"id": r["id"], "v": r["v"]} for r in mid])

        @asset(inputs={"leaf": In()})
        def report(leaf: list):
            w.seen.append(sorted(r["id"] for r in leaf))
            return len(leaf)

        return Project(assets=[files, mid, leaf, report], default_store=FileStore(w.solera.root / "data"))


async def nested_task_of(solera, run, name):
    [task] = [t for t in (await solera.get(f"/runs/{run['id']}"))["tasks"] if t["asset"] == name]
    return task


async def test_a_producer_stopped_midway_is_not_complete_because_its_upstream_is_mid_rebuild(solera):
    w = NestedWorld(solera, {"a": 1, "b": 1, "c": 1})
    await solera.deploy(w.project(), cancel_grace=0.2, heartbeat_seconds=0.1)
    await solera.run("files")
    await solera.run("mid")
    await solera.run("leaf")
    await solera.run("report")
    assert w.seen == [["a", "b", "c"]]
    eng = solera.engine

    # leaf's full run stops after one key of three: leaf holds {a}, mid full run, its readers wait.
    w.hold["leaf"].clear()
    stopped = await solera.submit("leaf", mode="full")
    await solera.until(lambda: w.waiting["leaf"])
    await solera.cancel(stopped["id"])
    await solera.wait(stopped)
    w.hold["leaf"].set()
    assert await solera.rows("leaf") == {"a": 1}
    reader = await solera.submit("report")
    await solera.settle(0.4)
    task = await nested_task_of(solera, reader, "report")
    assert task.get("held") == ["upstream incomplete", "leaf"], task
    assert ("leaf", "") in full.waiting(eng)
    assert w.seen == [["a", "b", "c"]]

    # mid's full run now starts: its first batch clears mid and writes {a}; its second waits.
    w.hold["mid"].clear()
    rebuilding = await solera.submit("mid", mode="full")
    await solera.until(lambda: w.waiting["mid"])
    assert await solera.rows("mid") == {"a": 1}
    await solera.settle(0.8)

    task = await nested_task_of(solera, reader, "report")
    state = {
        "leaf rows": await solera.rows("leaf"),
        "report saw": w.seen[1:],
        "report task": (task.get("status"), task.get("held")),
        "incomplete": sorted(full.waiting(eng)),
        "running": sorted(full.running(eng.core)),
        "stopped": full.stopped(eng),
        "leaf complete_known": eng._complete_known("leaf", ""),
        "core_empty": {str(k): v for k, v in eng.m.core_empty.items()},
    }
    w.hold["mid"].set()
    await solera.wait(rebuilding)
    await solera.settle(0.5)
    after = {
        "incomplete": sorted(full.waiting(eng)),
        "running": sorted(full.running(eng.core)),
        "report saw": w.seen[1:],
    }
    await solera.cancel(reader["id"])
    assert w.seen[1:] == [], (
        f"report read a partial leaf while mid was mid rebuild: {state}; after mid completed: {after}"
    )


# test_fan_in_owner.py: Which partitions a whole fan-in reads is decided by Planner.head_materialized ->


async def test_after_proceed_a_fan_in_reads_without_the_partial_partition(solera):
    rows = {"a": 1, "b": 1, "c": 1}
    hold, waiting, seen = asyncio.Event(), [False], []
    hold.set()
    site = {"site": StaticPartitions(["east", "west"])}

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    @asset(partitions=site, inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"))
    async def copy(ctx, files: list):
        if ctx.batch["files"].index > 0:
            waiting[0] = True
            await hold.wait()
            waiting[0] = False
        return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

    @asset(inputs={"copy": In(all_partitions=True)})
    def total(copy: dict[str, list]):
        seen.append({k: sorted(r["id"] for r in v) for k, v in copy.items()})
        return sum(len(v) for v in copy.values())

    project = Project(assets=[files, copy, total], default_store=FileStore(solera.root / "data"))
    await solera.deploy(project, cancel_grace=0.2, heartbeat_seconds=0.1)
    east = canonical_partition(["site"], {"site": "east"})
    west = canonical_partition(["site"], {"site": "west"})
    await solera.run("files")
    await solera.run("copy", partitions=[east, west])
    await solera.run("total")
    assert seen == [{east: ["a", "b", "c"], west: ["a", "b", "c"]}], seen

    hold.clear()
    stopped = await solera.submit("copy", partitions=[west], mode="full")
    await solera.until(lambda: waiting[0])
    await solera.cancel(stopped["id"])
    await solera.wait(stopped)
    hold.set()
    eng = solera.engine
    assert await solera.rows("copy", west) == {"a": 1}
    reader = await solera.submit("total")
    await solera.settle(0.4)
    [task] = [t for t in (await solera.get(f"/runs/{reader['id']}"))["tasks"] if t["asset"] == "total"]
    assert task.get("held") == ["upstream incomplete", "copy"], task
    assert len(seen) == 1

    await eng.proceed("copy", west, "test")
    await solera.wait(reader)
    print(
        "\nafter proceed, total read:", seen[1:], "complete_known(west)=", eng._complete_known("copy", west)
    )
    assert seen[1:] == [{east: ["a", "b", "c"], west: ["a"]}], (
        f"after proceed the fan-in read {seen[1:]}: the partial partition was left out, not read "
        f"(complete_known={eng._complete_known('copy', west)}, incomplete={sorted(full.waiting(eng))})"
    )


# test_fan_in_after_move.py: After an upstream's store move, a partition that has not yet started over is 'not complete'


async def test_a_fan_in_after_its_upstreams_upstream_moved(solera, capsys):
    rows = {"a": 1, "b": 1, "c": 1}
    seen = []
    site = {"site": StaticPartitions(["east", "west"])}

    def project(store=None):
        @asset(outputs=Output("files", key="id", store=store))
        def files():
            return [{"id": k, "v": v} for k, v in rows.items()]

        @asset(
            partitions=site, inputs={"files": Incremental(batch_size=10)}, outputs=Output("copy", key="id")
        )
        async def copy(ctx, files: list):
            return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

        @asset(inputs={"copy": In(all_partitions=True)})
        def total(copy: dict[str, list]):
            seen.append({k: sorted(r["id"] for r in v) for k, v in copy.items()})
            return sum(len(v) for v in copy.values())

        stores = {"other": FileStore(solera.root / "other")}
        return Project(
            assets=[files, copy, total], stores=stores, default_store=FileStore(solera.root / "data")
        )

    await solera.deploy(project())
    east = canonical_partition(["site"], {"site": "east"})
    west = canonical_partition(["site"], {"site": "west"})
    await solera.run("files")
    await solera.run("copy", partitions=[east, west])
    await solera.run("total")
    assert seen == [{east: ["a", "b", "c"], west: ["a", "b", "c"]}], seen
    await solera.deploy(project("other"))  # files starts a new life
    await solera.run("files")
    await solera.run("copy", partitions=[east])  # east starts over; west has not yet
    seen.clear()
    try:
        detail = await solera.run("total", expect=None)
        status = detail["request"]["status"]
    except Exception as error:
        status = f"refused: {str(error)[:120]}"
    with capsys.disabled():
        print(f"\n[{solera.path}] total: {status}; read {seen}")
    assert seen == [{east: ["a", "b", "c"], west: ["a", "b", "c"]}] or not seen, (
        f"[{solera.path}] the fan-in ran without a partition whose content is whole: {seen}"
    )


# test_append_reader.py: What an unkeyed incremental reader of an append-only output reads after its producer's full run:


async def test_what_the_reader_of_an_append_only_output_reads_after_a_full_run(solera, capsys):
    rows = {"a": 1, "b": 1, "c": 1}
    pages, commits = [], []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    @asset(inputs={"files": Incremental(batch_size=1)}, outputs=Output("log", incremental=True))
    def log(ctx, files: list):
        return Patch([{"id": r["id"]} for r in files])

    @asset(inputs={"log": Incremental(batch_size=10)}, outputs=Output("tail"))
    def tail(ctx, log: list):
        b = ctx.batch["log"]
        pages.append({"reset": b.reset, "commits": b.upstream.commits, "rows": sorted(r["id"] for r in log)})
        return {"n": len(log)}

    await solera.deploy(Project(assets=[files, log, tail], default_store=FileStore(solera.root / "data")))
    eng = solera.engine

    def spy():
        record = solera.state.record

        def spied(*events, **kw):
            out = record(*events, **kw)
            for e in events:
                if e.get("type") == "AttemptFinished" and e.get("commit"):
                    head = eng.m.heads.get(("log", "")) or {}
                    commits.append(
                        {
                            "task": eng.m.task(e["task"])["asset"] if eng.m.task(e["task"]) else None,
                            "log head": (head.get("commit_number"), head.get("base")),
                            "empty": {k[0] + "." + k[2]: v for k, v in eng.m.core_empty.items()},
                        }
                    )
            return out

        solera.state.record = spied

    spy()
    await solera.run("files")
    await solera.run("log")
    await solera.run("tail")
    first = list(commits), list(pages)
    commits.clear(), pages.clear()
    await solera.run("log", mode="full")
    after_full = list(commits)
    commits.clear()
    plan = eng._commits(
        next(i for i in eng.planner().inputs("tail", "") if i.param == "log"),
        (eng.m.partition("tail", "").get("observed") or {}).get("log"),
        None,
        True,
    )
    await solera.run("tail")
    with capsys.disabled():
        print("\nfirst runs:", first)
        print("log full run commits:", after_full)
        print("tail's next plan (full, since the base moved):", plan)
        print("tail read:", pages)


# test_deploy_round_trip.py: A deploy closes an input's edge (the input becomes In(), or its parameter is renamed) and a


def round_trip_project(root, rows, state, variant="inc"):
    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    if variant == "inc":

        @asset(inputs={"files": Incremental(batch_size=10)}, outputs=Output("copy", key="id"))
        async def copy(ctx, files: list):
            b = ctx.batch["files"]
            state["batches"].append(
                {"reset": b.reset, "rows": sorted(r["id"] for r in files), "removed": list(b.removed)}
            )
            return rebuild(b, [{"id": r["id"], "v": r["v"]} for r in files])

    elif variant == "renamed":

        @asset(inputs={"src": Incremental("files", batch_size=10)}, outputs=Output("copy", key="id"))
        async def copy(ctx, src: list):
            b = ctx.batch["src"]
            return rebuild(b, [{"id": r["id"], "v": r["v"]} for r in src])

    else:

        @asset(inputs={"files": In()}, outputs=Output("copy", key="id"))
        async def copy(ctx, files: list):
            return [{"id": r["id"], "v": r["v"]} for r in files]

    return Project(assets=[files, copy], default_store=FileStore(root / "data"))


@pytest.mark.parametrize("variant", ["whole", "renamed"])
async def test_a_rollback_before_the_asset_ran_keeps_keys_removed_upstream_meanwhile(solera, variant):
    rows = {"a": 1, "b": 1, "c": 1}
    state = {"batches": []}
    await solera.deploy(round_trip_project(solera.root, rows, state))
    await solera.run("files")
    await solera.run("copy")
    assert await solera.rows("copy") == rows
    await solera.deploy(round_trip_project(solera.root, rows, state, variant))  # copy does not run under it
    del rows["c"]
    await solera.run("files")
    await solera.deploy(round_trip_project(solera.root, rows, state))  # rolled back
    state["batches"].clear()
    status_before = await solera.status("copy")
    await solera.run("copy")
    await solera.settle(0.2)
    got = await solera.rows("copy")
    assert got == rows, (
        f"copy keeps a key removed upstream: {got}; status before the run {status_before!r}, "
        f"after {await solera.status('copy')!r}; batches {state['batches']}"
    )


# test_new_life_retry.py: An upstream's new life (its output moved to another store) makes its reader start over:


def new_life_project(root, rows, state, store=None):
    @asset(outputs=Output("files", key="id", store=store))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    @asset(inputs={"files": Incremental(batch_size=10)}, outputs=Output("copy", key="id"), retries=Retry(n=0))
    async def copy(ctx, files: list):
        b = ctx.batch["files"]
        state["batches"].append({"reset": b.reset, "rows": sorted(r["id"] for r in files)})
        if state["fail"]:
            state["fail"] = False
            raise RuntimeError("a transient failure")
        return rebuild(b, [{"id": r["id"], "v": r["v"]} for r in files])

    stores = {"other": FileStore(root / "other")}
    return Project(assets=[files, copy], stores=stores, default_store=FileStore(root / "data"))


@pytest.mark.parametrize("fails", [False, True])
async def test_a_reader_starts_over_on_its_upstreams_new_life_even_if_its_first_batch_fails(solera, fails):
    rows = {"a": 1, "b": 1, "c": 1}
    state = {"batches": [], "fail": False}
    await solera.deploy(new_life_project(solera.root, rows, state))
    await solera.run("files")
    await solera.run("copy")
    assert await solera.rows("copy") == rows
    await solera.deploy(new_life_project(solera.root, rows, state, store="other"))  # files starts a new life
    del rows["c"]
    await solera.run("files")
    state["batches"].clear()
    if fails:
        state["fail"] = True
        await solera.run("copy", expect="failed")
    await solera.run("copy")
    await solera.settle(0.2)
    got = await solera.rows("copy")
    status = await solera.status("copy")
    assert got == rows, (
        f"copy still holds a key of files' dead life: {got}; status {status!r}; "
        f"what copy's batches were told: {state['batches']}"
    )


# test_gate_deploy.py: A deploy while a producer is incomplete: does the gate still hold its readers?


INCOMPLETE = "upstream incomplete"


async def held_reader(solera, w):
    reader = await solera.submit("report")
    await solera.settle(0.3)
    task = await task_of(solera, reader, "report")
    assert task.get("held") == [INCOMPLETE, "copy"], task
    assert w.seen == []
    return reader


@pytest.mark.parametrize("variant", ["renamed", "whole"])
async def test_first_run_stopped_midway_then_a_deploy_changes_the_input(solera, variant):
    """copy's first run stops after one of three keys: report waits (STA-13).
    A deploy then renames copy's input parameter, or makes it a whole input.
    copy has not run again: its output is still one key of three."""

    w = await World(solera, {"a": 1, "b": 1, "c": 1}).deploy()
    await solera.run("files")
    await w.cancel_midway()
    assert await solera.rows("copy") == {"a": 1}
    reader = await held_reader(solera, w)
    assert ("copy", "") in full.waiting(solera.engine)
    w.hold.clear()  # copy does not get to run again
    await w.deploy(variant)
    await solera.settle(0.5)
    assert await solera.rows("copy") == {"a": 1}, "copy has not run since"
    task = await task_of(solera, reader, "report")
    assert w.seen == [], f"report read a partial copy: {w.seen}; task {task.get('status')}"
    assert ("copy", "") in full.waiting(solera.engine)


@pytest.mark.parametrize("variant", ["renamed", "whole"])
async def test_full_run_stopped_midway_then_a_deploy_changes_the_input(solera, variant):
    """The same with a full run of a keyed output (cleared, mid full run)."""

    w = await World(solera, {"a": 1, "b": 1, "c": 1}).deploy()
    await solera.run("files")
    await solera.run("copy")
    await w.cancel_midway(mode="full")
    assert await solera.rows("copy") == {"a": 1}
    reader = await held_reader(solera, w)
    assert full.running(solera.engine.core) == {("copy", "")}
    w.hold.clear()
    await w.deploy(variant)
    await solera.settle(0.5)
    assert await solera.rows("copy") == {"a": 1}, "copy has not run since"
    task = await task_of(solera, reader, "report")
    assert w.seen == [], f"report read a partial copy: {w.seen}; task {task.get('status')}"
    assert ("copy", "") in full.waiting(solera.engine)


# test_dispatch_gate_error.py: The gate runs planner.inputs for every due task inside the dispatch pass. One queued task


def dims_project(root, rows, state, dims):
    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    @asset(inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"))
    async def copy(ctx, files: list):
        if ctx.batch["files"].index > 0:
            state["waiting"] = True
            await state["hold"].wait()
        return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

    @asset(partitions=dims, inputs={"files": In()})
    def sited(files: list):
        return {"n": len(files)}

    @asset
    def other():
        state["other"] += 1
        return {"n": 1}

    return Project(assets=[files, copy, sited, other], default_store=FileStore(root / "data"))


async def test_one_stale_queued_task_does_not_stop_the_dispatch_pass(solera):
    rows = {"a": 1, "b": 1, "c": 1}
    state = {"hold": asyncio.Event(), "waiting": False, "other": 0}
    site = {"site": StaticPartitions(["east", "west"])}
    await solera.deploy(dims_project(solera.root, rows, state, site), cancel_grace=0.2, heartbeat_seconds=0.1)
    await solera.run("files")
    stopped = await solera.submit("copy")  # copy's first run stops midway: copy is incomplete
    await solera.until(lambda: state["waiting"])
    await solera.cancel(stopped["id"])
    state["hold"].set()
    await solera.wait(stopped)
    if solera.path == "core":
        assert full.waiting(solera.engine) == {("copy", "")}
    east = canonical_partition(["site"], {"site": "east"})
    queued = await solera.submit(
        "sited", partitions=[east]
    )  # queued; the deploy comes before it is dispatched
    both = {"site": StaticPartitions(["east", "west"]), "day": StaticPartitions(["one"])}
    await solera.deploy(dims_project(solera.root, rows, state, both), cancel_grace=0.2, heartbeat_seconds=0.1)
    asked = await solera.submit("other")
    errors = []
    for _ in range(40):
        try:
            await solera.engine.tick()
        except Exception as error:  # the serving loop logs it and ticks again
            errors.append(f"{type(error).__name__}: {error}")
        await asyncio.sleep(0.05)
    detail = await solera.get(f"/runs/{asked['id']}")
    stale = await solera.get(f"/runs/{queued['id']}")
    assert state["other"] == 1 and detail["request"]["status"] == "succeeded", (
        f"'other' never ran: status {detail['request']['status']}; the stale task's run is "
        f"{stale['request']['status']}; {len(errors)} of 40 ticks raised: {errors[:1]}"
    )


async def test_proceed_api_answers(solera, capsys):
    await World(solera, {"a": 1, "b": 1}).deploy()
    await solera.run("files")
    await solera.run("copy")
    out = {}
    for name in ("nope", "copy", "files"):
        try:
            response = await solera.request("POST", f"/assets/{name}/proceed", ok=False, json={})
            out[name] = (response.status_code, response.text[:200])
        except Exception as error:
            out[name] = ("raised", type(error).__name__, str(error)[:200])
    with capsys.disabled():
        print("\nproceed:", out)
    assert out["nope"][0] in (404, 400, 409), out
    assert out["copy"][0] in (409, 400), out


async def test_after_a_premature_end_the_gate_still_holds(solera, tmp_path):
    rows = {"a": 1, "b": 1, "c": 1}
    seen = []

    def project(version):
        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in rows.items()]

        @asset(outputs=Output("events", incremental=True))
        def events():
            return Patch([{"n": 1}])

        @asset(
            inputs={"files": Incremental(batch_size=2), "events": Incremental(batch_size=1)},
            outputs=Output("copy", key="id"),
            version=version,
        )
        async def copy(ctx, files: list, events: list):
            return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

        @asset(inputs={"copy": In()})
        def report(copy: list):
            seen.append(sorted(r["id"] for r in copy))
            return len(copy)

        return Project(assets=[files, events, copy, report], default_store=FileStore(tmp_path / "data"))

    await solera.deploy(project("1"))
    await solera.run("files")
    for _ in range(3):
        await solera.run("events")
    await solera.run("copy")
    await solera.deploy(project("2"))
    await solera.run("copy", keys={"files": ["a"]})
    eng = solera.engine
    reader = await solera.submit("report")
    await solera.settle(0.5)
    assert ("copy", "") in full.waiting(eng), full.waiting(eng)
    assert seen == [], seen
    assert [s["output"] for s in full.stopped(eng)] == ["copy"]
    await solera.cancel(reader["id"])


# test_regress_complete.py: A producer stopped midway is complete once the rest is deleted upstream (STA-10). A key


async def test_a_new_upstream_key_closes_the_gate_on_an_unchanged_output(solera):
    w = await World(solera, {"a": 1, "b": 1, "c": 1}).deploy()
    await solera.run("files")
    await w.cancel_midway()
    del w.rows["b"], w.rows["c"]
    await solera.run("files")
    await solera.settle(0.3)
    assert full.waiting(solera.engine) == set()
    await solera.run("report")
    assert w.seen == [["a"]]
    w.rows["d"] = 1
    await solera.run("files")
    await solera.settle(0.3)
    reader = await solera.submit("report")
    await solera.settle(0.5)
    task = await task_of(solera, reader, "report")
    stopped = full.stopped(solera.engine)
    await solera.cancel(reader["id"])
    assert task["status"] == "succeeded", (
        f"copy's content did not change, yet its reader now waits: held={task.get('held')} alarm={stopped}"
    )


async def test_a_batch_planned_at_a_partial_head_is_refused_at_commit(solera, tmp_path, monkeypatch):
    # prepare's check and the dispatch gate are bypassed: the commit's own check refuses it.
    from solera_server.tracking import prepare as prepare_module

    rebuilding, entered, release, seen = [False], asyncio.Event(), asyncio.Event(), []

    @asset(inputs={"feed": Incremental(batch_size=1)}, outputs=Output("copy", key="id"))
    async def copy(ctx, feed: list):
        if rebuilding[0] and ctx.batch["feed"].index == 1:
            entered.set()
            await release.wait()
        return rebuild(ctx.batch["feed"], feed)

    @asset(inputs={"copy": In()})
    def report(copy: list):
        seen.append(len(copy))
        return len(copy)

    outside = Outside()
    await solera.deploy(
        Project(
            assets=[copy, report],
            sources=[Source("feed", key="id", store="ext")],
            stores={"ext": SourceStore(tmp_path / "ext", outside)},
            default_store=FileStore(tmp_path / "data"),
        )
    )
    outside.feed.update(a="1", b="1", c="1")
    await solera.commit_source("feed", upsert=outside.feed)
    await solera.run("copy")
    rebuilding[0] = True
    producer = await solera.submit("copy", mode="full")
    await solera.until(entered.is_set)
    assert ("copy", "") in full.waiting(solera.engine)
    monkeypatch.setattr(full, "gate", lambda *a: None)
    monkeypatch.setattr(prepare_module, "held", lambda m, heads: None)
    reader = await solera.submit("report")
    await solera.until(lambda: seen)
    await solera.settle(0.3)
    monkeypatch.undo()
    release.set()
    await solera.wait(producer)
    detail = await solera.wait(reader)
    outcomes = [a["outcome"] for a in sum(detail["attempts"].values(), [])]
    assert seen[0] == 1 and "replanned" in outcomes and outcomes[-1] == "succeeded", (seen, outcomes)
    assert await solera.read("report") == 3


async def test_every_taker_of_several_output_locks_takes_them_in_life_id_order(solera, tmp_path):
    def project(both):
        @asset(outputs=[Output("z_out", key="id"), *([Output("a_out", key="id")] if both else [])])
        def pair():
            rows = [{"id": "k", "v": "1"}]
            return Result(outputs={"z_out": rows, "a_out": rows} if both else {"z_out": rows})

        return Project(assets=[pair], default_store=FileStore(tmp_path / "data"))

    await solera.deploy(project(False))
    await solera.run("pair")
    await solera.deploy(project(True))
    eng, taken = solera.engine, []
    lock = eng.core.lock

    def recording(id_):
        taken.append(id_)
        return lock(id_)

    eng.core.lock = recording
    await solera.run("pair", mode="full")
    ids = {name: eng.m.core_outputs[(name, "")] for name in ("a_out", "z_out")}
    assert sorted(ids.values()) != [ids["a_out"], ids["z_out"]], "the names sort otherwise than the ids"
    assert taken == sorted(ids.values()), taken
