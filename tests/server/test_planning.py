"""Planning (engine review thr_e77mqir977 #2, #4, #5; system review
thr_t6wbrkikak #3, #4): which scopes a request or a change selects."""

import asyncio
import datetime as dt
import time

import httpx
import pytest
from solera.sdk import (
    AllPartitions,
    Automation,
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
    MAX_SCOPES,
    Planner,
    enumerate_scopes,
    membership,
    select_scopes,
    size,
)

from .test_engine import drive, make_engine, state  # noqa: F401

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
    pick = select_scopes(dims, ["a=a7,b=b9"], now=now, elements=lambda o: None, missing=lambda s: True)
    start = time.perf_counter()
    for _ in range(100):
        select_scopes(
            dims, ["a=a7,b=b9", "a=nope,b=b9"], now=now, elements=lambda o: None, missing=lambda s: True
        )
    assert time.perf_counter() - start < 1.0  # 1,000,000 possible scopes, none listed
    assert len(pick) == 1
    with pytest.raises(ValueError, match=f"more than {MAX_SCOPES}"):
        select_scopes(dims, "all", now=now, elements=lambda o: None, missing=lambda s: True)
    hourly = {"t": {"kind": "time", "start": "2020-01-01", "every": "1h", "format": "%Y-%m-%dT%H:00"}}
    assert select_scopes(hourly, "latest", now=now, elements=lambda o: None, missing=lambda s: True) == [
        "2026-10-01T23:00"
    ]
    assert select_scopes(
        hourly, ["2026-10-01T22:00"], now=now, elements=lambda o: None, missing=lambda s: True
    )


def test_membership_and_size_agree_with_the_enumeration():
    dims = {
        "day": {"kind": "time", "start": "2026-09-28", "every": "1d", "format": "%Y-%m-%d"},
        "site": {"kind": "set", "output": "sites"},
        "tier": {"kind": "static", "keys": ["a,b", "c"]},
    }
    now, elements = dt.datetime(2026, 10, 2, tzinfo=UTC), {"sites": ["Richmond", "Oslo"]}.get
    listed = enumerate_scopes(dims, now, elements)
    member = membership(dims, now, elements)
    assert size(dims, now, elements) == len(listed) == 4 * 2 * 2 and all(member(s) for s in listed)
    assert member("day=2026-09-28,site=Oslo,tier=a%2Cb")
    assert not member("day=2026-10-02,site=Oslo,tier=c")  # its window is still open
    assert not member("day=2026-09-28,site=Paris,tier=c")
    assert not member("site=Oslo,day=2026-09-28,tier=c")  # not canonical: never listed
    assert not member("day=2026-09-28,site=Oslo,tier=a,b") and not member("day=2026-09-28")
    assert membership({}, now, elements)("") and not membership({}, now, elements)("x")
    assert size({}, now, elements) == 1
    big = {d: {"kind": "static", "keys": [f"{d}{i}" for i in range(1000)]} for d in "ab"}
    assert size(big, now, elements) == 1_000_000 > MAX_SCOPES  # counted, not listed


async def test_a_fan_in_reads_the_heads_that_exist(state):  # noqa: F811
    """Engine review round 2 #7: an AllPartitions read over a million-scope
    domain pins the heads that exist and agree with the consumer's shared keys
    — it never lists the domain. Building upstream does, and is refused."""
    seen = {}
    a = StaticPartitions([f"a{i}" for i in range(1000)])
    b = StaticPartitions([f"b{i}" for i in range(1000)])

    @asset(partitions={"a": a, "b": b})
    def grid(ctx):
        return [{"at": ctx.partition}]

    @asset(inputs={"grid": AllPartitions()})
    def rollup(grid: dict[str, list]):
        seen["rollup"] = sorted(grid)
        return [{"n": len(grid)}]

    @asset(partitions={"a": a}, inputs={"grid": AllPartitions()})
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
    with pytest.raises(ValueError, match=f"more than {MAX_SCOPES}"):
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
    projected = {("raw", ""): {"complete": True, "ref": {"version": "v1"}}}
    run = planner(projected).plan_run(["daily"], skip_missing_inputs=True, sensor="watch")
    assert [t["scope"] for t in run["tasks"].values()] == ["2026-10-01"]  # `latest` at `now`
    assert run["sensor"] == "watch" and run["created_at"] == now
    assert not heads  # a projection is read, never installed


async def test_a_source_change_fans_out_over_a_partitioned_consumer(state):  # noqa: F811
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
    scopes = sorted(t["scope"] for t in state.model.runs[next(iter(state.model.runs))]["tasks"].values())
    assert scopes == ["a", "b"] and not state.model.automations["report.onchange.0"]["pending"]


async def test_a_change_during_a_run_is_kept_for_after_it(state):  # noqa: F811
    from solera_server.placements.inline import InlinePlacement

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
    for _ in range(200):
        await engine.tick()
        if len(calls) == 2 and not state.model.automations["report.onchange.0"]["pending"]:
            break
        await asyncio.sleep(0.02)
    assert len(calls) == 2  # v2 was processed


async def test_unkeyed_source_commits_over_http(state):  # noqa: F811
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
    assert state.model.heads[("matrix", "")]["ref"]["version"] == "v2"


def test_the_cli_says_no_removals_as_none():
    import argparse

    from solera_server.cli import _commit_payload

    args = argparse.Namespace(version="v2", keys=None, upsert=None, remove=None, by=None)
    assert _commit_payload(args)["remove"] is None
