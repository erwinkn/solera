"""Planning (engine review thr_e77mqir977 #2, #4, #5; system review
thr_t6wbrkikak #3, #4; review round 3): which partitions a request or a change
selects, what each reads, and the order a run's tasks take."""

import asyncio
import datetime as dt
import time

import httpx
import pytest
from solera.sdk import (
    Automation,
    DynamicPartitions,
    In,
    OnChange,
    Output,
    Project,
    Source,
    StaticPartitions,
    TimePartitions,
    asset,
)
from solera_server.api import create_app
from solera_server.planning import (
    MAX_PARTITIONS,
    Planner,
    enumerate_partitions,
    membership,
    select_partitions,
    size,
)

from .engines import drive, make_engine

UTC = dt.UTC


def test_the_latest_window_is_not_frozen_at_five_thousand():
    hourly = TimePartitions(start="2026-01-01", every="1h")
    august, october = dt.datetime(2026, 8, 2, tzinfo=UTC), dt.datetime(2026, 10, 2, tzinfo=UTC)
    assert hourly.latest(october) == "2026-10-01T23:00" and hourly.latest(august) == "2026-08-01T23:00"
    assert hourly.contains("2026-10-01T23:00", october) and not hourly.contains("2026-10-02T00:00", october)
    assert not hourly.contains("2026-10-01T23:30", october) and not hourly.contains("tomorrow", october)
    assert len(hourly.keys(october)) == 274 * 24  # past the old cap, enumerated in full
    with pytest.raises(ValueError, match="more than 100 windows"):
        hourly.keys(october, limit=100)  # an explicit limit, never a silent truncation
    offset = TimePartitions(start="2026-01-01", every="1d", end_offset="2d", end="2026-09-15")
    assert (
        offset.latest(october) == "2026-09-14" and offset.latest(dt.datetime(2026, 1, 2, tzinfo=UTC)) is None
    )
    weekly = TimePartitions(start="2026-09-01", every="0 0 * * 1", format="%Y-%m-%d")
    assert weekly.latest(october) == "2026-09-21" == weekly.keys(october)[-1]
    paris = TimePartitions(start="2026-03-28", every="1d", timezone="Europe/Paris")  # across DST
    now = dt.datetime(2026, 4, 2, 12, tzinfo=UTC)
    assert paris.keys(now)[-1] == paris.latest(now) == "2026-04-01"


def test_one_explicit_scope_never_enumerates_the_domain():
    dims = {
        "a": {"kind": "static", "keys": [f"a{i}" for i in range(1000)]},
        "b": {"kind": "static", "keys": [f"b{i}" for i in range(1000)]},
    }
    now = dt.datetime(2026, 10, 2, tzinfo=UTC)
    pick = select_partitions(dims, ["a=a7,b=b9"], now=now, partitions=lambda o: None, missing=lambda s: True)
    start = time.perf_counter()
    for _ in range(100):
        select_partitions(
            dims, ["a=a7,b=b9", "a=nope,b=b9"], now=now, partitions=lambda o: None, missing=lambda s: True
        )
    assert time.perf_counter() - start < 1.0  # 1,000,000 possible partitions, none listed
    assert len(pick) == 1
    with pytest.raises(ValueError, match=f"more than {MAX_PARTITIONS}"):
        select_partitions(dims, "all", now=now, partitions=lambda o: None, missing=lambda s: True)
    hourly = {"t": {"kind": "time", "start": "2020-01-01", "every": "1h", "format": "%Y-%m-%dT%H:00"}}
    assert select_partitions(
        hourly, "latest", now=now, partitions=lambda o: None, missing=lambda s: True
    ) == ["2026-10-01T23:00"]
    assert select_partitions(
        hourly, ["2026-10-01T22:00"], now=now, partitions=lambda o: None, missing=lambda s: True
    )


def test_membership_and_size_agree_with_the_enumeration():
    dims = {
        "day": {"kind": "time", "start": "2026-09-28", "every": "1d", "format": "%Y-%m-%d"},
        "site": {"kind": "set", "output": "sites"},
        "tier": {"kind": "static", "keys": ["a,b", "c"]},
    }
    now, partitions = dt.datetime(2026, 10, 2, tzinfo=UTC), {"sites": ["Richmond", "Oslo"]}.get
    listed = enumerate_partitions(dims, now, partitions)
    member = membership(dims, now, partitions)
    assert size(dims, now, partitions) == len(listed) == 4 * 2 * 2 and all(member(s) for s in listed)
    assert member("day=2026-09-28,site=Oslo,tier=a%2Cb")
    assert not member("day=2026-10-02,site=Oslo,tier=c")  # its window is still open
    assert not member("day=2026-09-28,site=Paris,tier=c")
    assert not member("site=Oslo,day=2026-09-28,tier=c")  # not canonical: never listed
    assert not member("day=2026-09-28,site=Oslo,tier=a,b") and not member("day=2026-09-28")
    assert membership({}, now, partitions)("") and not membership({}, now, partitions)("x")
    assert size({}, now, partitions) == 1
    big = {d: {"kind": "static", "keys": [f"{d}{i}" for i in range(1000)]} for d in "ab"}
    assert size(big, now, partitions) == 1_000_000 > MAX_PARTITIONS  # counted, not listed


async def test_a_fan_in_reads_the_heads_that_exist(state):
    """Engine review round 2 #7: a whole fan-in over a million-partition
    domain pins the heads that exist and agree with the consumer's shared keys
    — it never lists the domain. Building upstream does, and is refused."""
    seen = {}
    a = StaticPartitions([f"a{i}" for i in range(1000)])
    b = StaticPartitions([f"b{i}" for i in range(1000)])

    @asset(partitions={"a": a, "b": b})
    def grid(ctx):
        return [{"at": ctx.partition}]

    @asset(inputs={"grid": In()})
    def rollup(grid: dict[str, list]):
        seen["rollup"] = sorted(grid)
        return [{"n": len(grid)}]

    @asset(partitions={"a": a}, inputs={"grid": In()})
    def by_a(ctx, grid: dict[str, list]):
        seen[ctx.partition] = sorted(grid)
        return [{"n": len(grid)}]

    project = Project(assets=[grid, rollup, by_a])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["grid"], partitions=["a=a7,b=b9", "a=a8,b=b1"]))
    start = time.perf_counter()
    assert (await drive(engine, await engine.submit(["rollup"])))["request"]["status"] == "succeeded"
    assert (await drive(engine, await engine.submit(["by_a"], partitions=["a7"])))["request"]["status"] == (
        "succeeded"
    )
    assert time.perf_counter() - start < 5.0
    assert seen == {"rollup": ["a=a7,b=b9", "a=a8,b=b1"], "a7": ["b9"]}  # keyed by the collapsed dims
    with pytest.raises(ValueError, match=f"more than {MAX_PARTITIONS}"):
        await engine.submit(["rollup"], upstream=True)  # a build lists the domain: bounded


def test_the_planner_takes_its_view_as_arguments():
    """Engine review round 2 #9: planning is a synchronous function of the
    manifest, the heads, the time and the heads a sensor's commits will
    install — no engine, no context variable."""

    @asset(outputs=Output("raw", key="id"))
    def raw():
        return []

    @asset(partitions=TimePartitions(start="2026-09-01", every="1d"), inputs={"raw": "raw"})
    def daily(ctx, raw: list):
        return []

    manifest = Project(assets=[raw, daily]).manifest
    heads: dict = {}
    now = dt.datetime(2026, 10, 2, 12, tzinfo=UTC).timestamp()

    def planner(projected=None):
        return Planner(manifest, lambda o, s: heads.get((o, s)), lambda o: [], now, projected)

    assert planner().plan_run(["daily"], skip_missing_inputs=True) is None  # `raw` was never written
    projected = {("raw", ""): {"ref": {"version": "v1"}}}
    run = planner(projected).plan_run(["daily"], skip_missing_inputs=True, sensor="watch")
    assert [t["partition"] for t in run["tasks"].values()] == ["2026-10-01"]  # `latest` at `now`
    assert run["sensor"] == "watch" and run["created_at"] == now
    assert not heads  # a projection is read, never installed


def test_an_empty_fan_in_is_missing():
    """A fan-in — a whole input, or a dep across a dimension the consumer
    lacks — with no upstream head at all counts as missing: under
    `skip_missing_inputs` its partition waits for the first one. Otherwise it
    runs over nothing, as before."""
    day, site = StaticPartitions(["d1", "d2"]), StaticPartitions(["east", "west"])

    @asset(partitions={"day": day, "site": site})
    def readings(ctx):
        return []

    @asset(partitions={"day": day}, deps=["readings"])
    def report(ctx):
        return []

    @asset(partitions={"day": day}, inputs={"readings": In()})
    def rollup(ctx, readings: dict[str, list]):
        return []

    manifest = Project(assets=[readings, report, rollup]).manifest
    heads, drained = {}, {}
    now = dt.datetime(2026, 10, 2, tzinfo=UTC).timestamp()

    def planned(target, **kw):
        planner = Planner(  # one per operation: it reads the view once
            manifest,
            lambda o, s: heads.get((o, s)),
            lambda o: [(s, h) for (out, s), h in heads.items() if out == o],
            now,
            caught_up=lambda a, s: drained.get((a, s), False),
        )
        run = planner.plan_run([target], partitions="all", **kw)
        return (
            None
            if run is None
            else sorted(t["partition"] for t in run["tasks"].values() if t["asset"] == target)
        )

    for target in ("report", "rollup"):
        assert planned(target, skip_missing_inputs=True) is None
        assert planned(target) == ["d1", "d2"]  # unchanged without the flag
        assert planned(target, skip_missing_inputs=True, upstream=True) == ["d1", "d2"]  # the run builds them
    heads[("readings", "day=d1,site=west")] = {"asset": "readings"}
    drained[("readings", "day=d1,site=west")] = False  # a pass under way
    assert planned("report", skip_missing_inputs=True) == ["d1"]  # one head of its day is enough
    assert planned("rollup", skip_missing_inputs=True) is None  # a whole fan-in reads complete heads
    drained[("readings", "day=d1,site=west")] = True
    assert planned("rollup", skip_missing_inputs=True) == ["d1"]


async def test_a_fan_in_reads_only_current_partitions(state):
    """Review round 3 (engine B2, system B2): a retired partition's head is
    kept for inspection but no fan-in reads it — not a whole input, not a
    dep across the dimension, not a missing-input check."""
    members, seen = {"keys": ["east", "west"]}, {}

    @asset(outputs=DynamicPartitions("sites"))
    def sites():
        return members["keys"]

    @asset(partitions="sites")
    def per_site(ctx):
        return [{"site": ctx.partition}]

    @asset(inputs={"per_site": In()})
    def rollup(per_site: dict[str, list]):
        seen["rollup"] = sorted(per_site)
        return [{"n": len(per_site)}]

    project = Project(assets=[sites, per_site, rollup])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["sites"]))
    await drive(engine, await engine.submit(["rollup"], upstream=True, partitions="all"))
    assert seen["rollup"] == ["east", "west"]
    members["keys"] = ["east"]  # west retires
    await drive(engine, await engine.submit(["sites"]))
    await drive(engine, await engine.submit(["rollup"]))
    assert seen["rollup"] == ["east"]
    assert state.model.heads.get(("per_site", "west")) is not None  # kept, not read
    members["keys"] = ["north"]  # only retired partitions have heads now
    await drive(engine, await engine.submit(["sites"]))
    assert await engine.submit(["rollup"], skip_missing_inputs=True) is None


def test_latest_and_changes_are_counted_before_they_are_listed():
    """Review round 3 (system B3, engine P2): `latest` holds time dimensions
    at their latest window but lists the others in full, and a change
    reaches every partition it does not pin — both refused past `MAX_SCOPES`, as
    `all` is, before a partition is built."""
    big = StaticPartitions([f"k{i}" for i in range(400)])

    @asset(partitions={"a": big, "b": big})
    def grid(ctx):
        return []

    @asset(partitions={"day": TimePartitions(start="2026-09-01", every="1d"), "a": big})
    def daily(ctx):
        return []

    manifest = Project(assets=[grid, daily], sources=[Source("feed")]).manifest
    now = dt.datetime(2026, 10, 2, tzinfo=UTC).timestamp()
    planner = Planner(manifest, lambda o, s: None, lambda o: [], now)
    start = time.perf_counter()
    for selection in ("all", "latest"):
        with pytest.raises(ValueError, match=f"160000 partitions, more than {MAX_PARTITIONS}"):
            planner.plan_run(["grid"], partitions=selection)
    with pytest.raises(ValueError, match=f"more than {MAX_PARTITIONS}"):
        planner.reach(None, "", "grid")  # a source change reaches every partition
    assert time.perf_counter() - start < 1.0  # refused before listing
    assert len(planner.reach("grid", "a=k1,b=k2", "daily")) == 31  # `a` pinned, every day listed
    assert len(planner.plan_run(["daily"])["tasks"]) == 400  # the latest day, every `a`


async def test_an_onchange_firing_is_one_run_in_order(state):
    """Review round 3 (engine B1): a firing is one run over every target, so
    a target that reads another waits for it; an automation that names
    `partitions` runs those, not the change's projection."""
    order = []

    @asset(partitions=StaticPartitions(["a", "b"]), deps=["feed"])
    def root(ctx):
        order.append(("root", ctx.partition))
        return [{"p": ctx.partition}]

    @asset(partitions=StaticPartitions(["a", "b"]), inputs={"root": "root"})
    def downstream(ctx, root: list):
        order.append(("downstream", ctx.partition))
        return root

    automation = Automation("both", targets=["root", "downstream"], trigger=OnChange("feed"))
    only_a = Automation("only_a", targets=["root"], trigger=OnChange("feed"), partitions=["a"], enabled=False)
    project = Project(assets=[root, downstream], sources=[Source("feed")], automations=[automation, only_a])
    engine = make_engine(state, project)
    await engine.initialize()
    await engine.commit_source("feed", version="v1")
    await engine.tick()
    fired = state.model.automations["both"]
    assert not fired["pending"] and fired["last_run"]
    run = await drive(engine, {"id": fired["last_run"]})
    assert run["request"]["status"] == "succeeded"
    assert {t["asset"] for t in run["tasks"]} == {"root", "downstream"}  # one run
    assert order.index(("root", "a")) < order.index(("downstream", "a"))
    assert run["request"]["partitions"] == {"downstream": ["a", "b"], "root": ["a", "b"]}
    await engine.set_automation("both", False)
    await engine.set_automation("only_a", True)
    await engine.commit_source("feed", version="v2")
    await engine.tick()
    run = state.model.runs[state.model.automations["only_a"]["last_run"]]
    assert sorted(t["partition"] for t in run["tasks"].values()) == ["a"]


def test_linking_a_run_is_linear():
    """Review round 3 (P1): a one-to-one input links each task by lookup, a
    fan-in through one grouping per input shape — not by scanning every
    upstream partition for every task."""
    keys = StaticPartitions([f"k{i:05d}" for i in range(4000)])
    sites = StaticPartitions(["east", "west"])

    @asset(partitions={"k": keys, "site": sites})
    def raw(ctx):
        return []

    @asset(partitions={"k": keys}, inputs={"raw": In()})
    def cooked(ctx, raw: dict[str, list]):
        return []

    @asset(partitions={"k": keys}, inputs={"cooked": "cooked"})
    def served(ctx, cooked: list):
        return []

    manifest = Project(assets=[raw, cooked, served]).manifest
    planner = Planner(
        manifest, lambda o, s: None, lambda o: [], dt.datetime(2026, 10, 2, tzinfo=UTC).timestamp()
    )
    start = time.perf_counter()
    run = planner.plan_run(["served"], partitions="all", upstream=True)
    assert time.perf_counter() - start < 3.0  # 12,000 tasks; ~10 s when quadratic
    tasks = run["tasks"]
    assert len(tasks) == 16000
    served_7 = tasks[f"{run['id']}/served:k00007"]
    cooked_7 = tasks[f"{run['id']}/cooked:k00007"]
    assert served_7["deps"] == [cooked_7["id"]]
    assert cooked_7["deps"] == [f"{run['id']}/raw:k=k00007,site=east", f"{run['id']}/raw:k=k00007,site=west"]


async def test_a_source_change_fans_out_over_a_partitioned_consumer(state):
    @asset(
        partitions={"site": StaticPartitions(["a", "b"])},
        deps=["prices"],
        automations=Automation(trigger=OnChange()),
    )
    def report(ctx):
        return [{"site": ctx.partition}]

    project = Project(assets=[report], sources=[Source("prices")])
    engine = make_engine(state, project)
    await engine.initialize()
    await engine.commit_source("prices", version="v2")
    await engine.tick()
    partitions = sorted(
        t["partition"] for t in state.model.runs[next(iter(state.model.runs))]["tasks"].values()
    )
    assert partitions == ["a", "b"] and not state.model.automations["report.onchange.0"]["pending"]


async def test_a_change_during_a_run_is_kept_for_after_it(state):
    from solera_server.executors.inline import InlinePlacement

    release, calls = asyncio.Event(), []

    @asset(deps=["prices"], automations=Automation(trigger=OnChange()))
    async def report(ctx):
        calls.append(ctx.run_id)
        if len(calls) == 1:
            await release.wait()
        return [1]

    project = Project(assets=[report], sources=[Source("prices")])
    engine = make_engine(state, project, placements={"Local": lambda s, c: InlinePlacement(c, project)})
    await engine.initialize()
    await engine.commit_source("prices", version="v1")
    while not calls:
        await engine.tick()
        await asyncio.sleep(0.02)
    await engine.commit_source("prices", version="v2")  # the running attempt pinned v1
    for _ in range(5):
        await engine.tick()
        await asyncio.sleep(0.02)
    assert state.model.automations["report.onchange.0"]["pending"]  # not consumed by a no-op
    release.set()
    for _ in range(3000):
        await engine.tick()
        if len(calls) == 2 and not state.model.automations["report.onchange.0"]["pending"]:
            break
        await asyncio.sleep(0.02)
    assert len(calls) == 2  # v2 was processed


async def test_unkeyed_source_commits_over_http(state):
    @asset(deps=["matrix"])
    def report():
        return [1]

    project = Project(assets=[report], sources=[Source("matrix")], name="p")
    engine = make_engine(state, project)
    await engine.initialize()
    app = create_app(engine=engine, insecure=True)
    app.state.engine = engine
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/projects/p/sources/matrix/commit", json={"version": "v2"})
    assert response.status_code == 200, response.text
    assert state.model.heads[("matrix", "")]["version"] == "v2"


def test_the_cli_says_no_removals_as_none():
    import argparse

    from solera_server.cli import _commit_payload

    args = argparse.Namespace(version="v2", keys=None, upsert=None, remove=None, by=None)
    assert _commit_payload(args)["remove"] is None
