"""AUT: automations (docs/behaviors.md)."""

import asyncio
import datetime as dt
import random

from croniter import croniter
from solera.sdk import (
    Cron,
    DynamicPartitions,
    Every,
    In,
    Incremental,
    Loaded,
    OnChange,
    OnDeploy,
    Output,
    Project,
    Retry,
    Source,
    StaticPartitions,
    asset,
    job,
    source,
)
from solera.stores import FileStore

from tests.contract.harness import Outside, SourceStore, rebuild


async def automated(solera, name: str | None = None) -> list[dict]:
    """The runs automations started, newest first; `name`'s alone if given."""

    return await solera.runs(origin="automation", **({"automation": name} if name else {}))


async def test_aut_1_a_change_fires_one_run_over_its_targets_in_order(solera):
    order = []

    @asset(partitions=StaticPartitions(["a", "b"]), deps=["feed"])
    def root(ctx):
        order.append(("root", ctx.partition))
        return [{"p": ctx.partition}]

    @asset(partitions=StaticPartitions(["a", "b"]), inputs={"root": "root"})
    def downstream(ctx, root: list):
        order.append(("downstream", ctx.partition))
        return root

    both = OnChange("feed", name="both", targets=["root", "downstream"])
    only_a = OnChange("feed", name="only_a", targets=["root"], partitions=["a"], enabled=False)
    await solera.deploy(
        Project(assets=[root, downstream], sources=[Source("feed")], automations=[both, only_a])
    )
    await solera.commit_source("feed", version="v1")
    await solera.until(lambda: len(order) == 4)
    await solera.quiet()
    [run] = await automated(solera)
    run = await solera.get(f"/runs/{run['id']}")
    assert {t["asset"] for t in run["tasks"]} == {"root", "downstream"}  # one run
    assert order.index(("root", "a")) < order.index(("downstream", "a"))
    assert order.index(("root", "b")) < order.index(("downstream", "b"))
    assert run["request"]["partitions"] == {"downstream": ["a", "b"], "root": ["a", "b"]}

    await solera.post("/automations/both/disable")  # one that names its partitions runs those
    await solera.post("/automations/only_a/enable")
    await solera.commit_source("feed", version="v2")
    await solera.until(lambda: len(order) == 5)
    await solera.quiet()
    [run] = await automated(solera, "only_a")
    run = await solera.get(f"/runs/{run['id']}")
    assert [(t["asset"], t["partition"]) for t in run["tasks"]] == [("root", "a")]


async def test_aut_1_a_firing_projects_the_changed_partitions(solera):
    seen = []

    @asset(outputs=DynamicPartitions("sites"))
    def sites():
        return ["s1", "s2"]

    @asset(partitions="sites")
    def site_data(ctx):
        return [{"site": ctx.partition}]

    @asset(partitions="sites", inputs={"data": "site_data"}, automations=OnChange())
    def per_site(ctx, data: list):
        seen.append(ctx.partition)
        return data

    await solera.deploy(Project(assets=[sites, site_data, per_site]))
    await solera.run("sites")
    await solera.run("site_data", partitions=["s2"])
    await solera.until(lambda: seen)
    await solera.quiet()
    assert seen == ["s2"]
    [run] = await automated(solera)
    assert run["partitions"] == {"per_site": ["s2"]}


async def test_aut_2_a_change_during_a_run_waits_for_it(solera):
    """`report` is running when `prices` commits again: the firing waits and
    runs after, so the change is not consumed by work that could not see it."""

    release, outside, calls = asyncio.Event(), {"v": "v1"}, []

    @source
    async def prices(keys, ctx):
        return Loaded(outside["v"], version=outside["v"])

    @asset(inputs={"prices": In()}, automations=OnChange())
    async def report(ctx, prices: str):
        calls.append(prices)
        if len(calls) == 1:
            await release.wait()
        return [prices]

    await solera.deploy(Project(assets=[report], sources=[prices]))
    await solera.commit_source("prices", version="v1")
    await solera.until(lambda: calls)
    outside["v"] = "v2"  # read live, the running attempt still has v1
    await solera.commit_source("prices", version="v2")
    await solera.settle(0.3)
    assert len(await automated(solera)) == 1  # the change waits
    release.set()
    await solera.until(lambda: len(calls) == 2)
    await solera.quiet()
    assert calls[-1] == "v2" and await solera.read("report") == ["v2"]
    assert await solera.stale("report") == []


async def test_aut_2_a_change_waits_for_work_already_queued(solera):
    """The firing would leave out a partition another run has queued, and run
    what reads it against old data: it waits, then fires and orders after it."""

    value, reads = {"v": 1}, []

    @asset(deps=["feed"])
    def root():
        return [value["v"]]

    @asset(inputs={"root": "root"})
    def downstream(root: list):
        reads.append(list(root))
        return root

    both = OnChange("feed", name="both", targets=["root", "downstream"])
    await solera.deploy(Project(assets=[root, downstream], sources=[Source("feed")], automations=[both]))
    await solera.post("/automations/both/disable")
    await solera.run("downstream", upstream=True)
    await solera.post("/automations/both/enable")
    queued = await solera.submit("root")
    await solera.post(f"/runs/{queued['id']}/pause")
    value["v"] = 2
    await solera.commit_source("feed", version="v2")
    await solera.settle(0.3)
    assert await automated(solera) == []  # owed `root` is queued elsewhere
    await solera.post(f"/runs/{queued['id']}/resume")
    await solera.wait(queued)
    await solera.until(lambda: reads[-1] == [2])
    await solera.quiet()
    [run] = await automated(solera)
    assert run["created_at"] >= (await solera.get(f"/runs/{queued['id']}"))["request"]["updated_at"]


async def test_aut_3_every_waits_for_its_first_interval(solera):
    calls = []

    @asset(automations=Every(60))
    def polled():
        calls.append(1)
        return [len(calls)]

    await solera.deploy(Project(assets=[polled]))
    await solera.settle(0.2)
    assert calls == [] and await automated(solera) == []  # nothing at declaration
    solera.advance(61)
    await solera.until(lambda: calls)
    await solera.quiet()
    assert len(await automated(solera)) == 1


async def test_aut_3_a_cron_first_fires_at_its_next_time(solera):
    """`Cron("0 6 * * *")` declared at 14:00 first fires tomorrow at 06:00."""

    calls = []

    @asset(automations=Cron("0 6 * * *"))
    def morning():
        calls.append(1)
        return [len(calls)]

    now = solera.clock()
    next_six = croniter("0 6 * * *", dt.datetime.fromtimestamp(now, dt.UTC)).get_next(float)
    await solera.deploy(Project(assets=[morning]))
    solera.advance(max(0, next_six - now - 60))  # a minute before
    await solera.settle(0.2)
    assert calls == [] and await automated(solera) == []
    solera.advance(120)
    await solera.until(lambda: calls)
    await solera.quiet()
    assert len(await automated(solera)) == 1


async def test_aut_3_a_tick_is_skipped_for_a_partition_queued(solera):
    calls = []

    @asset(automations=Every(60))
    def polled():
        calls.append(1)
        return []

    await solera.deploy(Project(assets=[polled]))
    run = await solera.submit("polled")
    await solera.post(f"/runs/{run['id']}/pause")  # its task stays queued
    solera.advance(61)
    await solera.settle(0.3)
    assert await automated(solera) == []
    await solera.post(f"/runs/{run['id']}/resume")
    await solera.wait(run)
    solera.advance(61)
    await solera.until(lambda: len(calls) == 2)
    await solera.quiet()
    assert len(await automated(solera)) == 1


async def test_aut_3_a_tick_is_skipped_for_a_partition_running(solera):
    release, calls = asyncio.Event(), []

    @asset(automations=Every(60))
    async def polled():
        calls.append(1)
        if len(calls) == 1:
            await release.wait()
        return []

    await solera.deploy(Project(assets=[polled]))
    run = await solera.submit("polled")
    await solera.until(lambda: calls)
    solera.advance(61)
    await solera.settle(0.3)
    assert await automated(solera) == [] and len(calls) == 1
    release.set()
    await solera.wait(run)
    solera.advance(61)
    await solera.until(lambda: len(calls) == 2)
    await solera.quiet()
    assert len(await automated(solera)) == 1


def deployed(calls: list, label: str, version: str = "1") -> Project:
    @job(automations=OnDeploy(), version=version)
    def announce():
        calls.append(label)

    return Project(assets=[announce])


async def test_aut_5_on_deploy_fires_once_per_new_deploy(solera):
    calls = []
    await solera.deploy(deployed(calls, "first"))
    await solera.until(lambda: calls)
    await solera.quiet()
    await solera.settle(0.2)
    assert calls == ["first"]
    await solera.restart()  # the same deploy: silent
    await solera.settle(0.3)
    assert calls == ["first"]
    await solera.deploy(deployed(calls, "second", version="2"))
    await solera.until(lambda: len(calls) == 2)
    await solera.quiet()
    assert calls == ["first", "second"]


async def test_aut_5_two_deploys_before_a_tick_fire_once_for_the_latest(solera):
    calls = []
    await solera.deploy(deployed(calls, "a"))
    await solera.deploy(deployed(calls, "b", version="2"))
    await solera.until(lambda: calls)
    await solera.quiet()
    await solera.settle(0.2)
    assert calls == ["b"]
    assert len(await automated(solera)) == 1


async def test_aut_6_a_failing_partitions_changes_back_off(solera):
    """`consumer` fails the same way on every change: its firings wait 60 s,
    doubling with each failure in a row, and a success ends the wait. It
    stays visibly failed meanwhile."""

    calls, broken = [], {"on": True}

    @asset(outputs=Output("feed", key="id"))
    def feed():
        return [{"id": "a", "v": len(calls)}]

    @asset(inputs={"feed": Incremental()}, automations=OnChange(), retries=Retry(n=0))
    def consumer(feed: list):
        calls.append(True)
        if broken["on"]:
            raise RuntimeError("the input is broken")
        return []

    await solera.deploy(Project(assets=[feed, consumer]))
    await solera.run("feed")
    await solera.until(lambda: calls)
    await solera.quiet()
    assert await solera.status("consumer") == "failed"
    for _ in range(2):  # changes while it backs off: no runs
        await solera.run("feed")
        await solera.settle(0.05)
    assert len(calls) == 1 and await solera.status("consumer") == "failed"
    solera.advance(61)  # its wait is over: one run takes every change
    await solera.until(lambda: len(calls) == 2)
    await solera.quiet()
    broken["on"] = False
    await solera.run("feed")
    solera.advance(61)
    await solera.settle(0.1)
    assert len(calls) == 2  # still waiting: the second wait is twice the first
    solera.advance(61)
    await solera.until(lambda: len(calls) == 3)
    await solera.quiet()
    assert await solera.status("consumer") != "failed"
    await solera.run("feed")  # the success ended the wait
    await solera.until(lambda: len(calls) == 4)


async def test_aut_7_automated_outputs_converge_on_their_sources(solera):
    """A burst of source commits under automations alone: after quiet, every
    output equals what its sources imply, with nothing owed."""

    outside = Outside()

    @asset(
        inputs={"feed": Incremental(batch_size=3)}, outputs=Output("items", key="id"), automations=OnChange()
    )
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(
        inputs={"items": Incremental(batch_size=2)}, outputs=Output("copy", key="id"), automations=OnChange()
    )
    def copy(ctx, items: list):
        return rebuild(ctx.batch["items"], [{"id": r["id"], "v": r["v"]} for r in items])

    @asset(inputs={"items": Incremental()}, automations=OnChange())
    async def tally(ctx, items: list):
        batch = ctx.batch["items"]
        before = 0 if batch.reset else ((await ctx.load()) or {"rows": 0})["rows"]
        return {"rows": before + len(batch.added) - len(batch.removed)}

    @asset(
        inputs={"row": Incremental("items", each=True)},
        outputs=Output("checks", key="id"),
        automations=OnChange(),
    )
    async def checks(ctx, row: list):
        return [{"id": ctx.key, "v": row[0]["v"]}]

    await solera.deploy(
        Project(
            assets=[items, copy, tally, checks],
            sources=[Source("feed", key="id", store="ext")],
            stores={"ext": SourceStore(solera.root / "ext", outside)},
            default_store=FileStore(solera.root / "data"),
        )
    )
    rng = random.Random(7)
    for n in range(12):
        upserts = {f"k{rng.randrange(10)}": str(n) for _ in range(rng.randrange(1, 4))}
        removes = [k for k in rng.sample(sorted(outside.feed), min(len(outside.feed), rng.randrange(2)))]
        removes = [k for k in removes if k not in upserts]
        outside.feed.update(upserts)
        for k in removes:
            del outside.feed[k]
        await solera.commit_source("feed", upsert=upserts, remove=removes)
        await solera.settle(rng.choice([0.0, 0.02, 0.05]))

    async def converged():
        return (
            await solera.rows("items") == outside.feed
            and await solera.rows("copy") == outside.feed
            and await solera.rows("checks") == outside.feed
            and (await solera.read("tally") or {"rows": 0})["rows"] == len(outside.feed)
        )

    await solera.until(converged)
    await solera.quiet()
    assert await converged()
    for name in ("items", "copy", "tally", "checks"):
        assert await solera.stale(name) == [], name


# -- target (PR 2) --


# -- AUT-4 (target) ---------------------------------------------------------------------


def patterned(root, calls: list, include, version: str = "1", automations=None) -> Project:
    """`items` (keyed: k1, k2, k3, run by hand) -> `tally`, incremental over
    the keys `include` takes, automated by `OnChange` unless told otherwise;
    `calls` gets each batch's (added, removed, reset)."""

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": k, "v": 1} for k in ("k1", "k2", "k3")]

    @asset(
        inputs={"items": Incremental(include=include)},
        outputs=Output("tally"),
        version=version,
        automations=OnChange() if automations is None else automations,
    )
    def tally(ctx, items: list):
        batch = ctx.batch["items"]
        calls.append((sorted(batch.added), sorted(batch.removed), batch.reset))
        return {"n": len(items)}

    return Project(assets=[items, tally], default_store=FileStore(root / "data"))


async def built_patterned(solera, calls: list, **decl) -> None:
    """`patterned` served, and `tally` built by its automation."""

    await solera.deploy(patterned(solera.root, calls, **decl))
    await solera.run("items")
    await solera.until(lambda: calls)
    await solera.quiet()


async def test_aut_4_a_pattern_change_that_owes_nothing_fires_nothing(solera):
    """`tally` takes k1, then k1 and k9, a key that never existed: nothing is
    owed, so its `OnChange` does not fire."""

    calls = []
    await built_patterned(solera, calls, include=["k1"])
    fired = len(await automated(solera))
    await solera.deploy(patterned(solera.root, calls, include=["k1", "k9"]))
    await solera.settle(0.5)
    await solera.quiet()
    assert len(await automated(solera)) == fired and calls == [(["k1"], [], False)]
    assert await solera.stale("tally") == []


async def test_aut_4_a_definition_change_fires_onchange_once(solera):
    calls = []
    await built_patterned(solera, calls, include=["k*"])
    fired = len(await automated(solera))
    await solera.deploy(patterned(solera.root, calls, include=["k*"], version="2"))
    await solera.until(lambda: len(calls) == 2)
    await solera.quiet()
    await solera.settle(0.3)
    assert len(await automated(solera)) == fired + 1
    assert calls[-1] == (["k1", "k2", "k3"], [], True)  # its next run is a full run
    assert await solera.stale("tally") == []


async def test_aut_4_with_no_automation_a_changed_asset_shows_stale(solera):
    calls = []
    await solera.deploy(patterned(solera.root, calls, include=["k*"], automations=()))
    await solera.run(["items", "tally"])
    await solera.deploy(patterned(solera.root, calls, include=["k*"], version="2", automations=()))
    await solera.settle(0.3)
    assert len(calls) == 1 and await automated(solera) == []
    assert await solera.stale("tally") == ["definition changed"]
