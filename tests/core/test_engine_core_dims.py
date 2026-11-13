"""Dimensions on the new tracking core: a whole fan-in across a dynamic dimension is
gated on the dimension's set output, a dimension added under a partition stopped midway
stops no tick, and a fan-in reader in flight commits what it planned (Fable's probes of
PR 2's redesign, round 1)."""

import asyncio

import pytest
from solera.sdk import (
    DynamicPartitions,
    In,
    Incremental,
    Output,
    Project,
    StaticPartitions,
    asset,
    canonical_partition,
)
from solera.stores import FileStore, Patch
from solera_server.tracking import full

from tests.contract.harness import Solera, rebuild


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path)
    yield namespace
    await namespace.close()


async def test_a_fan_in_across_a_dimension_whose_set_is_mid_full_run_waits(solera):
    rows = {k: 1 for k in "abcd"}
    hold, waiting, seen = asyncio.Event(), [False], []
    hold.set()

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    @asset(inputs={"files": Incremental(batch_size=1)}, outputs=DynamicPartitions("sites"))
    async def sites(ctx, files: list):
        b = ctx.batch["files"]
        if b.index > 0:
            waiting[0] = True
            await hold.wait()
            waiting[0] = False
        keys = [r["id"] for r in files]
        return keys if b.reset else Patch(keys, remove=list(b.removed))

    @asset(partitions="sites")
    def per(ctx):
        return [{"site": ctx.partition}]

    @asset(inputs={"per": In()})
    def rollup(per: dict[str, list]):
        seen.append(sorted(per))
        return len(per)

    project = Project(assets=[files, sites, per, rollup], default_store=FileStore(solera.root / "data"))
    await solera.deploy(project, cancel_grace=0.2, heartbeat_seconds=0.1)
    await solera.run("files")
    await solera.run("sites")
    await solera.run("per", partitions="all")
    await solera.run("rollup")
    assert seen == [["a", "b", "c", "d"]], seen

    hold.clear()
    rebuild_run = await solera.submit("sites", mode="full")
    await solera.until(lambda: waiting[0])
    m = solera.engine.m
    print("sites head partitions mid full run:", m.heads[("sites", "")].get("partitions"))
    reader = await solera.submit("rollup")
    await solera.settle(0.6)
    task = [t for t in (await solera.get(f"/runs/{reader['id']}"))["tasks"] if t["asset"] == "rollup"][0]
    print("rollup task:", task["status"], task.get("held"), "seen:", seen)
    hold.set()
    await solera.wait(rebuild_run)
    await solera.wait(reader)
    assert seen[1:] == [["a", "b", "c", "d"]], (
        f"rollup read {seen[1:]} while the dimension's set was mid full run: a fold over a partial set"
    )


async def test_a_dimension_added_to_an_asset_with_a_partition_stopped_midway_stops_no_tick(solera):
    rows = {k: 1 for k in "abc"}
    hold, waiting = asyncio.Event(), [False]

    def project(dims):
        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in rows.items()]

        @asset(partitions=dims, inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"))
        async def copy(ctx, files: list):
            if ctx.batch["files"].index > 0:
                waiting[0] = True
                await hold.wait()
            return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

        return Project(assets=[files, copy], default_store=FileStore(solera.root / "data"))

    await solera.deploy(
        project({"site": StaticPartitions(["east", "west"])}), cancel_grace=0.2, heartbeat_seconds=0.1
    )
    west = canonical_partition(["site"], {"site": "west"})
    await solera.run("files")
    stopped = await solera.submit("copy", partitions=[west])
    await solera.until(lambda: waiting[0])
    await solera.cancel(stopped["id"])
    await solera.wait(stopped)
    hold.set()

    two = {"site": StaticPartitions(["east", "west"]), "kind": StaticPartitions(["x"])}
    await solera.deploy(project(two), cancel_grace=0.2, heartbeat_seconds=0.1)
    for _ in range(5):
        await (
            solera.engine.tick()
        )  # ValueError: Partition key 'west' does not match dimensions ['kind', 'site']
        await asyncio.sleep(0.05)
    await solera.run("files")


@pytest.mark.parametrize("load", ["data"])
async def test_a_fan_in_reader_in_flight_commits_what_it_planned_when_a_partition_clears(solera, load):
    rows = {k: 1 for k in "abcd"}
    hold = {"copy": asyncio.Event(), "total": asyncio.Event()}
    waiting = {"copy": False, "total": False}
    for e in hold.values():
        e.set()
    seen = []
    site = {"site": StaticPartitions(["east", "west"])}

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    @asset(partitions=site, inputs={"files": Incremental(batch_size=2)}, outputs=Output("copy", key="id"))
    async def copy(ctx, files: list):
        if ctx.batch["files"].index > 0:
            waiting["copy"] = True
            await hold["copy"].wait()
            waiting["copy"] = False
        return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

    if load == "data":

        @asset(inputs={"copy": In(all_partitions=True)}, outputs=Output("total"))
        async def total(ctx, copy: dict[str, list]):
            waiting["total"] = True
            await hold["total"].wait()
            waiting["total"] = False
            got = {k: sorted(r["id"] for r in v) for k, v in copy.items()}
            seen.append(got)
            return got

    else:
        from solera.sdk import Ref

        @asset(inputs={"copy": In(all_partitions=True)}, outputs=Output("total"))
        async def total(ctx, copy: dict[str, Ref]):
            waiting["total"] = True
            await hold["total"].wait()
            waiting["total"] = False
            got = {k: sorted(r["id"] for r in await ctx.load(ref, list)) for k, ref in copy.items()}
            seen.append(got)
            return got

    project = Project(assets=[files, copy, total], default_store=FileStore(solera.root / "data"))
    await solera.deploy(project, cancel_grace=0.2, heartbeat_seconds=0.1)
    east = canonical_partition(["site"], {"site": "east"})
    west = canonical_partition(["site"], {"site": "west"})
    await solera.run("files")
    await solera.run("copy", partitions=[east, west])
    hold["total"].clear()
    reader = await solera.submit("total")
    await solera.until(lambda: waiting["total"])  # planned at complete heads; its worker has not loaded
    hold["copy"].clear()
    clearing = await solera.submit("copy", partitions=[west], mode="full")
    await solera.until(lambda: waiting["copy"])  # west is cleared, and holds 2 keys of 4
    assert sorted(full.waiting(solera.engine)) == [("copy", west)]
    assert await solera.rows("copy", west) == {"a": 1, "b": 1}
    hold["total"].set()
    detail = await solera.wait(reader, 10)
    [task] = [t for t in detail["tasks"] if t["asset"] == "total"]
    outcomes = [a["outcome"] for a in detail["attempts"][task["id"]]]
    print("reader outcomes:", outcomes, "seen:", seen)
    # every commit of total holds whole partitions only
    for got in seen:
        if detail["request"]["status"] == "succeeded" and got is seen[-1]:
            assert got == {east: list("abcd"), west: list("abcd")}, (outcomes, seen)
    hold["copy"].set()
    await solera.wait(clearing)
    assert detail["request"]["status"] == "succeeded", (detail["request"]["status"], outcomes)
