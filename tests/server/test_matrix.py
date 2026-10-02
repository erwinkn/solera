"""Engine-level scenarios across boundaries (review round 4): each row
crosses two features that are tested apart elsewhere — a write's reset and
its payload, a fan-in and its edge shape, registration and queued work, a
firing and work already queued — through the real engine and stores."""

import asyncio
import time

from solera.sdk import (
    AllPartitions,
    Automation,
    Incremental,
    OnChange,
    Output,
    Project,
    Result,
    Retry,
    Source,
    StaticPartitions,
    asset,
)
from solera.stores import Patch
from solera_server.planning import select_scopes

from .test_engine import drive, make_engine, state, status_of, task_statuses  # noqa: F401

# -- full rewrite × equal payload -------------------------------------------------------


async def test_an_identical_full_rewrite_stays_readable(state):  # noqa: F811
    seen = []

    @asset(outputs=Output("log", incremental=True))
    def log():
        return Patch([{"n": 1}])

    @asset(inputs={"log": Incremental()})
    def reader(log: list):
        seen.append(list(log))
        return []

    project = Project(assets=[log, reader])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["log"]))
    await drive(engine, await engine.submit(["log"], mode="full"))  # the same payload, written again
    await drive(engine, await engine.submit(["reader"]))
    assert seen == [[{"n": 1}]]


async def test_a_full_run_after_a_rename_stays_readable(state):  # noqa: F811
    """Worker/stores review round 4 #1: `old` wrote {a, b}; renamed `new`,
    a full run writes {a: 2, b: 1}. Only `a` is written again: `b` stays
    where `old` put it, so the new head must still say its content is
    there, and a reader sees both."""
    seen = []

    @asset(outputs=Output(key="k"))
    def old():
        return [{"k": "a", "v": 1}, {"k": "b", "v": 1}]

    engine = make_engine(state, Project(assets=[old]))
    await engine.initialize()
    await drive(engine, await engine.submit(["old"]))
    await engine.stop()

    @asset(outputs=Output(key="k"), aliases=["old"])
    def new():
        return [{"k": "a", "v": 2}, {"k": "b", "v": 1}]

    @asset(inputs={"new": "new"})
    def reader(new: list):
        seen.append(sorted((r["k"], r["v"]) for r in new))
        return []

    engine = make_engine(state, Project(assets=[new, reader]))
    await engine.initialize()
    await drive(engine, await engine.submit(["new"], mode="full"))
    assert status_of(await drive(engine, await engine.submit(["reader"]))) == "succeeded"
    assert seen == [[("a", 2), ("b", 1)]]


# -- AllPartitions × zero free dimensions -----------------------------------------------


async def test_all_partitions_with_nothing_to_collapse(state):  # noqa: F811
    """Unpartitioned on both sides, or every dimension shared: one complete
    head, read under the key `""`."""
    seen = {}
    sites = StaticPartitions(["east", "west"])

    @asset
    def raw():
        return [1]

    @asset(inputs={"raw": AllPartitions()}, retries=Retry(n=0))
    def summary(raw: dict[str, list]):
        seen["summary"] = raw
        return [len(raw)]

    @asset(partitions={"site": sites})
    def per_site(ctx):
        return [ctx.partition]

    @asset(partitions={"site": sites}, inputs={"per_site": AllPartitions()}, retries=Retry(n=0))
    def mirrored(ctx, per_site: dict[str, list]):
        seen[ctx.partition] = per_site
        return [len(per_site)]

    project = Project(assets=[raw, summary, per_site, mirrored])
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["summary"], upstream=True))
    assert status_of(detail) == "succeeded" and seen["summary"] == {"": [1]}
    detail = await drive(engine, await engine.submit(["mirrored"], partitions="all", upstream=True))
    assert status_of(detail) == "succeeded"
    assert seen["east"] == {"": ["east"]} and seen["west"] == {"": ["west"]}


# -- registration × queued work ---------------------------------------------------------


async def test_a_removed_asset_takes_its_queued_work_with_it(state):  # noqa: F811
    """A queued or waiting task of an asset the new project drops is
    canceled, saying why, and its run rolls up; one of a renamed asset runs
    under the new name; unrelated work dispatches."""

    @asset
    def doomed():
        return [1]

    @asset(inputs={"doomed": "doomed"})
    def after(doomed: list):
        return doomed

    @asset
    def old_name():
        return [2]

    first = Project(assets=[doomed, after, old_name])
    engine = make_engine(state, first)
    await engine.initialize()
    stale = await engine.submit(["after"], upstream=True)
    renamed = await engine.submit(["old_name"])
    for run in (stale, renamed):
        await engine.pause(run["id"])
    await engine.stop()

    @asset(aliases=["old_name"])
    def new_name():
        return [2]

    @asset
    def other():
        return [3]

    second = Project(assets=[new_name, other])
    engine = make_engine(state, second)
    await engine.initialize()
    gone = state.model.runs[stale["id"]]
    statuses = {t["asset"]: (t["status"], t.get("error")) for t in gone["tasks"].values()}
    assert statuses == {
        "doomed": ("canceled", "asset 'doomed' is no longer in the project"),
        "after": ("canceled", "asset 'after' is no longer in the project"),
    }
    assert gone["status"] == "failed"
    detail = await drive(engine, await engine.submit(["other"]))
    assert status_of(detail) == "succeeded"
    await engine.pause(renamed["id"], False)
    detail = await drive(engine, renamed)
    assert task_statuses(detail) == {"new_name": "succeeded"}


# -- OnChange × work queued in another run ----------------------------------------------


async def test_a_change_waits_for_work_already_queued(state):  # noqa: F811
    """The firing would drop a scope another run has queued, and run what
    reads it against old data: the change waits while any owed scope is
    claimed or queued, then fires and orders after it."""
    value, reads = {"v": 1}, []

    @asset(deps=["feed"])
    def root():
        return [value["v"]]

    @asset(inputs={"root": "root"})
    def downstream(root: list):
        reads.append(list(root))
        return root

    automation = Automation("both", targets=["root", "downstream"], trigger=OnChange("feed"))
    project = Project(assets=[root, downstream], sources=[Source("feed")], automations=[automation])
    engine = make_engine(state, project)
    await engine.initialize()
    await engine.set_automation("both", False)
    await drive(engine, await engine.submit(["downstream"], upstream=True))
    await engine.set_automation("both", True)
    queued = await engine.submit(["root"])
    await engine.pause(queued["id"])
    value["v"] = 2
    await engine.commit_source("feed", version="v2")
    engine._automation_tick()
    assert state.model.automations["both"]["pending"]  # owed `root` is queued elsewhere
    await engine.pause(queued["id"], False)
    await drive(engine, queued)
    for _ in range(100):
        await engine.tick()
        if not state.model.automations["both"]["pending"] and reads[-1] == [2]:
            break
        await asyncio.sleep(0.01)
    assert reads[-1] == [2]


# -- OnChange × a delivery under way ----------------------------------------------------


async def test_a_change_is_kept_until_its_delivery_completes(state):  # noqa: F811
    """An AllPartitions read excludes a scope whose delivery is under way: a
    change made by its first page waits until the last page drains it — even
    when that page writes nothing — then fires once, over complete data."""
    observed = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a"}, {"id": "b"}]

    @asset(inputs={"files": Incremental(page_size=1)})
    def mid(ctx, files: list):
        return [{"n": 1}] if ctx.changes["files"].first else Result(outputs={})

    @asset(inputs={"mid": AllPartitions()}, automations=Automation(trigger=OnChange("mid")))
    def agg(mid: dict[str, list]):
        observed.append(sorted(mid))
        return [len(mid)]

    project = Project(assets=[files, mid, agg])
    engine = make_engine(state, project)
    await engine.initialize()
    run = await engine.submit(["mid"], upstream=True)
    await drive(engine, run)
    for _ in range(100):
        await engine.tick()
        if observed and not state.model.automations["agg.onchange.0"]["pending"]:
            break
        await asyncio.sleep(0.01)
    assert observed and all(seen == [""] for seen in observed)  # never an empty, partial read


# -- explicit selection at scale --------------------------------------------------------


def test_an_explicit_selection_is_linear():
    """A request may name up to `MAX_SCOPES` scopes: each is checked once."""
    import datetime as dt

    keys = [f"k{i}" for i in range(80_000)]
    dims = {"k": {"kind": "static", "keys": keys}}
    start = time.perf_counter()
    picked = select_scopes(
        dims,
        keys + keys[:10],
        now=dt.datetime(2026, 10, 2, tzinfo=dt.UTC),
        elements=lambda o: None,
        missing=bool,
    )
    assert len(picked) == 80_000 and time.perf_counter() - start < 3.0  # ~25 s when quadratic


# -- registration × delivery obligations ------------------------------------------------


async def test_a_removed_consumer_lets_go_of_its_upstreams_log(state):  # noqa: F811
    """Review round 5, engine #3 and system #2: `gone` and `keep` read
    `feed` incrementally. `gone` is removed: its watermark goes with it, so
    the log of `feed`'s later batches is kept only as long as `keep` needs
    it."""

    rows = [{"id": "a"}]

    @asset(outputs=Output("feed", key="id"))
    def feed():
        return list(rows)

    @asset(inputs={"feed": Incremental()})
    def gone(feed: list):
        return []

    @asset(inputs={"feed": Incremental()})
    def keep(feed: list):
        return []

    engine = make_engine(state, Project(assets=[feed, gone, keep]))
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["gone", "keep"], upstream=True))) == "succeeded"
    await engine.stop()

    engine = make_engine(state, Project(assets=[feed, keep]))
    await engine.initialize()
    assert sorted(k[0] for k in state.model.watermarks) == ["keep"]
    for key in ("b", "c", "d"):
        rows.append({"id": key})
        await drive(engine, await engine.submit(["keep"], upstream=True))
        engine.upkeep.truncate()
    assert state.model.indexes[("feed", "")].log == ()


# -- an unchanged rewrite × interpretation ----------------------------------------------


async def test_an_unchanged_value_rewritten_keeps_its_readers_deliveries(state):  # noqa: F811
    """Review round 5, system #4: `settings` is written again with the same
    content, at a new object (FileStore names a value by its generation).
    Its version is the same, so a reader of `feed` that also reads it keeps
    its watermark: nothing changed in `feed`, nothing is delivered again."""

    calls = []

    @asset(outputs=Output("feed", key="id"))
    def feed():
        return [{"id": "a"}]

    @asset
    def settings():
        return {"rate": 2}

    @asset(inputs={"feed": Incremental(), "settings": "settings"})
    def reader(feed: list, settings: dict):
        calls.append(len(feed))
        return []

    engine = make_engine(state, Project(assets=[feed, settings, reader]))
    await engine.initialize()
    refs = []
    for _ in range(3):
        assert status_of(await drive(engine, await engine.submit(["reader"], upstream=True))) == "succeeded"
        ref = state.model.heads[("settings", "")]["ref"]
        refs.append((ref["handle"]["path"], ref["version"]))
    assert len({path for path, _ in refs}) == 3 and len({version for _, version in refs}) == 1
    assert calls == [1]


# -- OnChange × an interrupted delivery, for Each -----------------------------------------


async def test_an_each_delivery_resumed_by_a_firing_takes_its_change(state):  # noqa: F811
    """As test_sim_found's keyed case, for an Each edge: a full delivery cut
    short after its first key, the upstream changing, the firing resuming it
    — the change is delivered, and only then is the scope drained."""
    from solera.sdk import AutoRefresh, Each

    content, calls = {"a": "1", "b": "1"}, []

    @asset(outputs=Output("items", key="id", revision="v"))
    def items():
        return [{"id": k, "v": v} for k, v in content.items()]

    @asset(
        inputs={"item": Each("items", page_size=1)},
        outputs=Output("out", key="id"),
        automations=AutoRefresh(),
    )
    def out(ctx, item: list):
        calls.append((ctx.key, item[0]["v"]))
        return [{"v": item[0]["v"]}]

    project = Project(assets=[items, out])
    engine = make_engine(state, project)
    await engine.initialize()
    await engine.set_automation("out.onchange.0", False)
    await drive(engine, await engine.submit(["items"]))
    run = await engine.submit(["out"])
    while (state.model.watermarks.get(("out", "item", "")) or {}).get("delivery", {}).get("page") != 1:
        await engine.tick()
        await asyncio.sleep(0.01)
    await engine.cancel(run["id"])
    await drive(engine, run)
    await engine.set_automation("out.onchange.0", True)
    content.update({"a": "2", "b": "2"})
    await drive(engine, await engine.submit(["items"]))
    for _ in range(200):
        await engine.tick()
        busy = any(r["status"] not in ("succeeded", "failed", "canceled") for r in state.model.runs.values())
        if not busy and not state.model.automations["out.onchange.0"]["pending"]:
            break
        await asyncio.sleep(0.01)
    assert ("a", "2") in calls and ("b", "2") in calls
    assert state.model.progress[("out", "")] == {"drained": True}
