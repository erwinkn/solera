"""PAR: partitions, fan-in, broadcast, dynamic partitions (docs/behaviors.md)."""

import datetime as dt
import time

import pytest
from solera.sdk import (
    Commit,
    DynamicPartitions,
    Every,
    In,
    Incremental,
    OnChange,
    Output,
    Project,
    RegistrationError,
    Result,
    Retry,
    RunRequest,
    Source,
    StaticPartitions,
    Tick,
    TimePartitions,
    asset,
    canonical_partition,
    sensor,
    split_partition,
)

from tests.contract.harness import Refused

AS_OF = dt.datetime(2026, 9, 19, 12, 0, 0, tzinfo=dt.UTC)


def test_par_1_keys_join_dimensions_sorted_by_name():
    """Multi-dimension keys are `dim=key` sorted by dimension name."""

    dims = {"site": {"kind": "static", "keys": ["Richmond"]}, "day": {"kind": "time"}}
    key = canonical_partition(dims, {"site": "Richmond", "day": "2026-09-19"})
    assert key == "day=2026-09-19,site=Richmond"
    assert split_partition(dims, key) == {"site": "Richmond", "day": "2026-09-19"}
    single = {"site": {"kind": "static", "keys": ["Richmond"]}}
    assert canonical_partition(single, {"site": "Richmond"}) == "Richmond"
    assert split_partition(single, "Richmond") == {"site": "Richmond"}


def test_par_1_time_windows_are_half_open():
    """every='1d' yields half-open daily windows keyed YYYY-MM-DD."""

    tp = TimePartitions(start="2026-09-17", every="1d")
    assert tp.keys(AS_OF) == ["2026-09-17", "2026-09-18"]
    assert tp.latest(AS_OF) == "2026-09-18"
    assert tp.window("2026-09-18") == ("2026-09-18T00:00:00+00:00", "2026-09-19T00:00:00+00:00")


def test_par_1_time_windows_align_in_their_timezone():
    """Windows align to the declared timezone, not UTC."""

    tp = TimePartitions(start="2026-09-17", every="1d", timezone="America/New_York")
    start, end = tp.window("2026-09-18")
    assert start == "2026-09-18T00:00:00-04:00"
    assert end == "2026-09-19T00:00:00-04:00"


async def test_par_1_runs_name_partitions_by_their_canonical_key(solera):
    @asset(partitions={"site": StaticPartitions(["Richmond"]), "day": StaticPartitions(["2024-01-01"])})
    def readings(ctx):
        return [ctx.partition]

    @asset
    def total():
        return [1]

    await solera.deploy(Project(assets=[readings, total]))
    detail = await solera.run("readings", partitions="all")
    assert [t["partition"] for t in detail["tasks"]] == ["day=2024-01-01,site=Richmond"]
    assert await solera.read("readings", "day=2024-01-01,site=Richmond") == ["day=2024-01-01,site=Richmond"]
    detail = await solera.run("total")
    assert [t["partition"] for t in detail["tasks"]] == [""]  # its one partition
    assert [h["partition"] for h in (await solera.get("/outputs/total/heads"))["heads"]] == [""]
    with pytest.raises(Refused, match="unpartitioned"):
        await solera.get("/partitions/total")


async def test_par_2_a_consumer_only_dimension_broadcasts_an_upstream_only_one_collapses(solera):
    """A consumer-only dimension broadcasts; an upstream-only dimension fans
    in as a whole input."""

    seen = []

    @asset(outputs=DynamicPartitions("sites"))
    def sites():
        return ["s1", "s2"]

    @asset(partitions={"site": "sites"})
    def site_data(ctx):
        return [{"site": ctx.partition, "v": 1}]

    @asset
    def config():
        return {"threshold": 5}

    @asset(partitions={"site": "sites"}, inputs={"cfg": "config"})
    def broadcast(ctx, cfg: list):
        seen.append((ctx.partition, cfg["threshold"]))
        return [{"site": ctx.partition}]

    @asset(inputs={"site_data": In()})
    def collapsed(site_data: dict[str, list]):
        return [{"n": len(site_data), "sites": sorted(site_data)}]

    await solera.deploy(Project(assets=[sites, config, site_data, broadcast, collapsed]))
    await solera.run("sites")  # commit the dynamic partitions
    await solera.run("broadcast", upstream=True)
    assert sorted(seen) == [("s1", 5), ("s2", 5)]
    await solera.run("collapsed", upstream=True)
    assert await solera.read("collapsed") == [{"n": 2, "sites": ["s1", "s2"]}]


def readings_and_report(seen: dict, **report_kw):
    """`readings` by day × site; `report` by day, fanning in over sites."""

    days, sites = (
        StaticPartitions(["2026-10-01", "2026-10-02"]),
        StaticPartitions(["Oslo", "Paris", "Richmond"]),
    )

    @asset(partitions={"day": days, "site": sites})
    def readings(ctx):
        return [ctx.partition]

    @asset(partitions={"day": days}, inputs={"readings": In()}, **report_kw)
    def report(ctx, readings: dict[str, list]):
        seen[ctx.partition] = readings
        return [len(readings)]

    return Project(assets=[readings, report])


async def test_par_2_a_shared_dimension_projects_and_an_upstream_only_one_fans_in(solera):
    seen = {}
    await solera.deploy(readings_and_report(seen))
    await solera.run("report", partitions=["2026-10-01"], upstream=True)
    assert seen == {
        "2026-10-01": {
            "Oslo": ["day=2026-10-01,site=Oslo"],
            "Paris": ["day=2026-10-01,site=Paris"],
            "Richmond": ["day=2026-10-01,site=Richmond"],
        }
    }


def test_par_2_an_incremental_input_may_not_fan_in():
    days, sites = StaticPartitions(["d1"]), StaticPartitions(["east", "west"])

    @asset(partitions={"day": days, "site": sites}, outputs=Output("readings", key="id"))
    def readings(ctx):
        return []

    @asset(partitions={"day": days}, inputs={"readings": Incremental()})
    def report(ctx, readings: list):
        return []

    with pytest.raises(RegistrationError):
        Project(assets=[readings, report])


async def test_par_3_a_fan_in_reads_the_heads_that_exist(solera):
    """A whole fan-in over a million-partition domain reads the heads that
    exist and agree with the consumer's shared keys: it never lists the
    domain. Building upstream does, and is refused."""

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

    await solera.deploy(Project(assets=[grid, rollup, by_a]))
    await solera.run("grid", partitions=["a=a7,b=b9", "a=a8,b=b1"])
    start = time.perf_counter()
    await solera.run("rollup")
    await solera.run("by_a", partitions=["a7"])
    assert time.perf_counter() - start < 5.0
    assert seen == {"rollup": ["a=a7,b=b9", "a=a8,b=b1"], "a7": ["b9"]}  # keyed by the collapsed dims
    with pytest.raises(Refused, match="more than 100000"):
        await solera.submit("rollup", upstream=True)  # a build lists the domain: bounded


async def test_par_3_a_fan_in_never_waits_for_a_partition_with_no_head(solera):
    seen = {}
    await solera.deploy(readings_and_report(seen))
    await solera.run("readings", partitions=["day=2026-10-01,site=Oslo", "day=2026-10-01,site=Richmond"])
    await solera.run("report", partitions=["2026-10-01"])  # Paris has no head
    assert sorted(seen["2026-10-01"]) == ["Oslo", "Richmond"]
    await solera.run("report", partitions=["2026-10-01"], upstream=True)  # to wait for all
    assert sorted(seen["2026-10-01"]) == ["Oslo", "Paris", "Richmond"]


async def test_par_3_with_skip_missing_inputs_a_fan_in_with_no_head_is_skipped(solera):
    seen = {}
    await solera.deploy(
        readings_and_report(seen, automations=Every(60, skip_missing_inputs=True, partitions="all"))
    )
    [auto] = (await solera.get("/automations"))["automations"]
    await solera.post(f"/automations/{auto['name']}/run-now")
    await solera.settle(0.2)
    assert seen == {} and await solera.runs() == []  # no readings at all: nothing to read yet
    await solera.run("readings", partitions=["day=2026-10-02,site=Paris"])
    await solera.post(f"/automations/{auto['name']}/run-now")
    [run] = await solera.runs(automation=auto["name"])
    await solera.wait(run)
    assert seen == {"2026-10-02": {"Paris": ["day=2026-10-02,site=Paris"]}}


async def test_par_3_a_fan_in_reads_complete_passes_only(solera):
    """A partition whose pass is under way is not read: a change its first
    batch made waits until its last batch drains it — even one that writes
    nothing — and the fan-in reads complete data."""

    observed = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a"}, {"id": "b"}]

    @asset(inputs={"files": Incremental(batch_size=1)})
    def mid(ctx, files: list):
        return [{"n": 1}] if ctx.batch["files"].first else Result(outputs={})

    @asset(inputs={"mid": In(all_partitions=True)}, automations=OnChange("mid"))
    def agg(mid: dict[str, list]):
        observed.append(sorted(mid))
        return [len(mid)]

    await solera.deploy(Project(assets=[files, mid, agg]))
    await solera.run("mid", upstream=True)
    await solera.until(lambda: observed)
    await solera.quiet()
    assert observed and all(seen == [""] for seen in observed)  # never an empty, partial read


async def test_par_4_all_partitions_reads_every_partition_the_shared_dimensions_too(solera):
    """`all_partitions=True` reads every partition of the upstream as
    `dict[partition, value]`, the shared dimensions too, with no projection:
    `report` for east compares east's value with every site's. Unpartitioned,
    it is the one head under `""`. Without it, a shared dimension is
    projected and the value comes plain."""

    seen = {}
    sites = StaticPartitions(["east", "west"])

    @asset
    def raw():
        return [1]

    @asset(inputs={"raw": In(all_partitions=True)}, retries=Retry(n=0))
    def summary(raw: dict[str, list]):
        seen["summary"] = raw
        return [len(raw)]

    @asset(partitions={"site": sites})
    def per_site(ctx):
        return [ctx.partition]

    @asset(
        partitions={"site": sites},
        inputs={"every": In("per_site", all_partitions=True), "own": In("per_site")},
        retries=Retry(n=0),
    )
    def report(ctx, every: dict[str, list], own: list):
        seen[ctx.partition] = (every, own)
        return [len(every)]

    await solera.deploy(Project(assets=[raw, summary, per_site, report]))
    await solera.run("summary", upstream=True)
    assert seen["summary"] == {"": [1]}
    await solera.run("report", partitions="all", upstream=True)
    every = {"east": ["east"], "west": ["west"]}
    assert seen["east"] == (every, ["east"]) and seen["west"] == (every, ["west"])


async def test_par_5_an_external_partition_set_is_patched_by_commits(solera):
    """An external DynamicPartitions is patched through the commit API; new
    keys surface through partitions='missing'; listing the same again is no change."""

    ran = []

    @asset(partitions="uploads")
    def per_upload(ctx):
        ran.append(ctx.partition)
        return [{"u": ctx.partition}]

    await solera.deploy(Project(assets=[per_upload], sources=[DynamicPartitions("uploads")]))
    await solera.commit_source("uploads", upsert=["u-1", "u-2"])
    await solera.run("per_upload", partitions="missing")
    assert sorted(ran) == ["u-1", "u-2"]
    ran.clear()
    await solera.commit_source("uploads", upsert=["u-3"])
    await solera.run("per_upload", partitions="missing")
    assert ran == ["u-3"]
    assert (await solera.commit_source("uploads", keys=["u-1", "u-2", "u-3"]))["changed"] is False


async def test_par_5_missing_on_a_schedule_picks_up_new_partitions(solera):
    ran = []
    members = {"keys": ["a"]}

    @asset(outputs=DynamicPartitions("things"))
    def things():
        return members["keys"]

    @asset(partitions="things", automations=Every(60, partitions="missing"))
    def per_thing(ctx):
        ran.append(ctx.partition)
        return [{"t": ctx.partition}]

    await solera.deploy(Project(assets=[things, per_thing]))
    await solera.run("things")
    solera.advance(61)
    await solera.until(lambda: ran)
    await solera.quiet()
    assert ran == ["a"]
    members["keys"] = ["a", "b"]
    await solera.run("things")
    solera.advance(61)
    await solera.until(lambda: len(ran) > 1)
    await solera.quiet()
    assert ran == ["a", "b"]  # only the new partition ran


async def test_par_5_re_listing_the_same_partitions_wakes_no_consumer(solera):
    ran = []

    @asset(outputs=DynamicPartitions("sites"))
    def sites():
        return ["s1", "s2"]

    @asset(partitions="sites", deps=["sites"], automations=OnChange())
    def per_site(ctx):
        ran.append(ctx.partition)
        return [{"site": ctx.partition}]

    await solera.deploy(Project(assets=[sites, per_site]))
    await solera.run("sites")
    await solera.until(lambda: len(ran) == 2)
    await solera.quiet()
    assert sorted(ran) == ["s1", "s2"]
    automated = len(await solera.runs(origin="automation"))
    await solera.run("sites")  # every site again
    await solera.settle(0.5)
    assert sorted(ran) == ["s1", "s2"] and len(await solera.runs(origin="automation")) == automated


async def test_par_7_latest_missing_all_and_a_list_select_partitions(solera):
    """'latest', 'missing', 'all' and explicit lists select from the partitions."""

    ran = []

    @asset(partitions=StaticPartitions(["a", "b", "c"]))
    def stat(ctx):
        ran.append(ctx.partition)
        return [{"p": ctx.partition}]

    await solera.deploy(Project(assets=[stat]))
    await solera.run("stat", partitions=["a", "c"])
    assert sorted(ran) == ["a", "c"]
    ran.clear()
    await solera.run("stat", partitions="missing")
    assert ran == ["b"]  # a and c have complete heads
    ran.clear()
    await solera.run("stat", partitions="all")
    assert sorted(ran) == ["a", "b", "c"]


async def test_par_7_latest_is_the_last_complete_window(solera):
    ran = []
    today = dt.datetime.fromtimestamp(solera.clock(), dt.UTC).date()

    @asset(partitions=TimePartitions(start=str(today - dt.timedelta(days=5)), every="1d"))
    def daily(ctx):
        ran.append(ctx.partition)
        return [ctx.partition]

    await solera.deploy(Project(assets=[daily]))
    await solera.run("daily")  # "latest", the default
    assert ran == [str(today - dt.timedelta(days=1))]
    solera.advance(86400)
    await solera.run("daily", partitions="latest")
    assert ran[-1] == str(today)


async def test_par_7_a_selection_is_counted_before_it_is_listed(solera):
    """Past 100,000 partitions a selection is refused; an explicit list never
    enumerates the domain."""

    a = StaticPartitions([f"a{i}" for i in range(1000)])
    b = StaticPartitions([f"b{i}" for i in range(1000)])
    ran = []

    @asset(partitions={"a": a, "b": b})
    def grid(ctx):
        ran.append(ctx.partition)
        return [ctx.partition]

    await solera.deploy(Project(assets=[grid]))
    for selection in ("all", "missing"):
        with pytest.raises(Refused, match="more than 100000"):
            await solera.submit("grid", partitions=selection)
    start = time.perf_counter()
    await solera.run("grid", partitions=["a=a7,b=b9"])
    assert time.perf_counter() - start < 2.0  # 1,000,000 possible partitions, none listed
    assert ran == ["a=a7,b=b9"]


async def test_par_8_a_source_change_reaches_every_partition_of_its_consumer(solera):
    ran = []

    @asset(partitions={"site": StaticPartitions(["a", "b"])}, deps=["rates"], automations=OnChange())
    def site_report(ctx):
        ran.append(ctx.partition)
        return [{"site": ctx.partition}]

    await solera.deploy(Project(assets=[site_report], sources=[Source("rates")]))
    await solera.commit_source("rates", version="v2")
    await solera.until(lambda: len(ran) == 2)
    await solera.quiet()
    [run] = await solera.runs(origin="automation")
    detail = await solera.get(f"/runs/{run['id']}")
    assert sorted(t["partition"] for t in detail["tasks"]) == ["a", "b"]
    assert sorted(ran) == ["a", "b"]
    for site in ("a", "b"):
        assert await solera.stale("site_report", site) == []


async def test_par_9_a_sensors_requested_runs_see_its_own_commits(solera):
    """A tick replaces `sites` `[old]` with `[new]` and asks for every
    partition: the run targets `new`."""

    sites, ran = DynamicPartitions("sites"), []

    @asset(partitions=sites)
    def per_site(ctx) -> int:
        ran.append(ctx.partition)
        return 1

    @sensor(every=60, commits=["sites"])
    def discover(ctx):
        return Tick(commits=[Commit("sites", keys=["new"])], runs=[RunRequest("per_site", partitions="all")])

    await solera.deploy(Project(assets=[per_site], sources=[sites], sensors=[discover]))
    await solera.commit_source("sites", keys=["old"])
    await solera.sensors(1)
    [run] = await solera.runs(origin="sensor")
    detail = await solera.wait(run)
    assert [t["partition"] for t in detail["tasks"]] == ["new"]
    assert ran == ["new"]
    assert sorted(await solera.keys("sites")) == ["new"]
