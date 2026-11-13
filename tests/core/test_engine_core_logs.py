"""Logs on the engine: an unkeyed incremental output, whose rows the core
keeps in the output's layers of the change index, and the readers that
consume it commit by commit."""

import datetime as dt

import pytest
from solera.sdk import In, Incremental, Output, Project, asset
from solera.stores import FileStore, Patch
from solera_server.model import edge_id, output_id
from solera_worker import batch

from tests.contract.harness import Solera


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path)
    yield namespace
    await namespace.close()


def deploy(solera, *assets):
    return solera.deploy(Project(assets=list(assets), default_store=FileStore(solera.root / "data")))


async def test_a_log_s_rows_come_back_as_written_in_order_across_parts(solera, monkeypatch):
    """A commit of many rows is cut into parts, and rows JSON cannot hold
    exactly are pickled: a whole read, the producer's own `ctx.load()` and an
    incremental read all get the same rows, in the order they were appended."""

    monkeypatch.setattr(batch, "PART_BYTES", 200)  # a few rows to a part
    commits = [
        [{"n": n, "text": "x" * 40} for n in range(50)],
        [{"at": dt.datetime(2026, 11, 13, 9, 30), "pair": (1, 2)}],
        [{"n": n} for n in range(50, 60)],
    ]
    turn, held, whole, tail = [0], [], [], []

    @asset(outputs=Output("events", incremental=True))
    async def events(ctx):
        held.append(await ctx.load())
        return Patch(commits[turn[0]])

    @asset(inputs={"events": In()})
    def everything(events: list):
        whole.append(events)
        return len(events)

    @asset(inputs={"events": Incremental()})
    def digest(ctx, events: list):
        tail.append(events)
        return len(events)

    await deploy(solera, events, everything, digest)
    for turn[0] in range(3):
        await solera.run("events")
    await solera.run("everything")
    await solera.run("digest")
    rows = [row for commit in commits for row in commit]
    assert whole == [rows] and tail == [rows]
    assert held == [None, commits[0], commits[0] + commits[1]]
    assert await solera.read("events") == rows
    layers = solera.engine.m.core.output(output_id("events", ""))["layers"]
    assert [layer["keys"] > 1 for layer in layers] == [True, False, False], (
        "the first commit is several parts"
    )


async def test_an_empty_patch_appends_no_commit(solera):
    """INC-14 for a log: a write of no rows is no commit, and its reader owes
    nothing."""

    rows, seen = [[{"n": 1}]], []

    @asset(outputs=Output("events", incremental=True))
    def events():
        return Patch(rows[0])

    @asset(inputs={"events": Incremental()})
    def digest(ctx, events: list):
        seen.append(len(events))
        return len(events)

    await deploy(solera, events, digest)
    await solera.run("events")
    await solera.run("digest")
    rows[0] = []
    await solera.run("events")
    assert solera.engine.m.core.output(output_id("events", ""))["head"] == 1
    assert await solera.stale("digest") == []
    await solera.run("digest")
    assert seen == [1]


async def test_a_log_rebuilt_in_batches_starts_over_once_and_its_reader_with_it(solera):
    """`events` appends one commit per batch of `files`. Its full run takes
    two batches: the first starts the log over, the second appends to what
    the first wrote. `digest` then starts over and reads both commits."""

    content, seen = {"v": [{"id": f"k{i}", "v": 1} for i in range(4)]}, []

    @asset(outputs=Output("files", key="id"))
    def files():
        return content["v"]

    @asset(inputs={"files": Incremental(batch_size=2)}, outputs=Output("events", incremental=True))
    def events(ctx, files: list):
        return Patch([{"id": row["id"], "v": row["v"]} for row in files])

    @asset(inputs={"events": Incremental()})
    def digest(ctx, events: list):
        seen.append((ctx.batch["events"].reset, sorted(e["id"] for e in events)))
        return len(events)

    await deploy(solera, files, events, digest)
    await solera.run("files")
    await solera.run("events")
    await solera.run("digest")
    keys = ["k0", "k1", "k2", "k3"]
    assert seen == [(False, keys)]
    await solera.run("events", mode="full")
    assert sorted(e["id"] for e in await solera.read("events")) == keys, "the first life's rows are gone"
    assert solera.engine.m.core.full_run(output_id("events", "")) is None
    await solera.run("digest")
    assert seen[-1] == (True, keys)
    assert await solera.stale("digest") == []


async def test_a_reader_of_keys_and_of_a_log_walks_both_to_the_end(solera):
    """`report` reads `files` three keys at a time and `events` one commit at
    a time. Its walk ends when both do: each key and each commit is delivered
    once, and the partition is complete only then."""

    seen = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": f"k{i}"} for i in range(3)]

    @asset(outputs=Output("events", incremental=True))
    def events():
        return Patch([{"n": 1}])

    @asset(inputs={"files": Incremental(batch_size=3), "events": Incremental(batch_size=1)})
    def report(ctx, files: list, events: list):
        keys, log = ctx.batch["files"], ctx.batch["events"]
        seen.append((sorted(r["id"] for r in files), list(log.upstream.commits), keys.final, log.final))
        return len(seen)

    await deploy(solera, files, events, report)
    await solera.run("files")
    for _ in range(3):
        await solera.run("events")
    await solera.run("report")
    assert seen == [
        (["k0", "k1", "k2"], [0], True, False),
        ([], [1], True, False),
        ([], [2], True, True),
    ]
    assert await solera.engine.complete("report", "")
    edge = solera.engine.m.core.edge(edge_id("report", "", "events"))
    assert edge["log"] == {"through": 3, "walking": False, "restarted": False}
    assert await solera.stale("report") == []
    await solera.run("events")
    await solera.run("report")
    assert seen[-1] == ([], [3], True, True)
