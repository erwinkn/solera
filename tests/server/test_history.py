"""The run history (docs/object-store-state.md §7): rows born in the model,
flushed to Parquet, merged, hidden when deleted, and queried with DuckDB."""

import copy

import pytest
from solera.sdk import In, Output, Project, Result, Retry, Source, asset
from solera_server.engine import Conflict, Engine
from solera_server.history import History, RunFilter, bucket_for, execution
from solera_server.placements.inline import InlinePlacement
from solera_server.state import State


class Clock:
    def __init__(self):
        self.now = 1_800_000_000.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
async def state(tmp_path, clock):
    opened = await State.open(tmp_path.as_uri(), "test", clock=clock, flush_interval=0.001)
    yield opened
    await opened.close()


@asset(tags={"team": "growth"})
def orders(ctx):
    n = ctx.config.get("n", 3)
    ctx.mark("counted")
    ctx.metadata(rows=n, source="shop")
    return [{"id": i} for i in range(n)]


@asset(inputs={"orders": In()}, retries=Retry(n=0))
def revenue(orders: list, ctx):
    if ctx.config.get("fail"):
        raise RuntimeError("upstream timeout")
    return Result({"revenue": len(orders)}, metadata={"revenue": {"total": len(orders) * 10}})


@asset(outputs=Output("ranked", key="id"))
def ranked():
    return [{"id": "a"}, {"id": "b"}]


PROJECT = Project(assets=[orders, revenue, ranked], sources=[Source("uploads", key="id")])


def engine_for(state, clock, **history):
    return Engine(
        state,
        PROJECT.manifest,
        placements={"Local": lambda s, c: InlinePlacement(c, PROJECT)},
        clock=clock,
        eval_interval=0.01,
        history=History(state, clock=clock, **history) if history else None,
    )


async def run(engine, clock, targets, **kw):
    clock.now += 60
    detail = await engine.run_until((await engine.submit(targets, **kw))["id"], 60)
    return detail["request"]


async def test_versions_carry_metadata_and_lineage(state, clock):
    engine = engine_for(state, clock)
    await engine.initialize()
    await run(engine, clock, ["revenue"], upstream=True, config={"n": 4})
    await run(engine, clock, ["ranked"])
    made = (await engine.history.materializations(outputs=["orders", "revenue", "ranked"]))[
        "materializations"
    ]
    by = {m["output"]: m for m in made}
    assert by["orders"]["metadata"] == {"rows": 4, "source": "shop"}
    assert by["orders"]["rows"] == 4
    assert by["revenue"]["metadata"] == {"total": 40}
    assert by["ranked"]["rows"] == 2  # a keyed output counts its keys

    # Paging one version at a time walks them all, even those made at the same moment.
    paged, cursor = [], None
    while True:
        page = await engine.history.materializations(
            outputs=["orders", "revenue", "ranked"], before=cursor, limit=1
        )
        paged += page["materializations"]
        if (cursor := page["next"]) is None:
            break
    assert paged == made

    head = state.model.heads[("revenue", "")]["ref"]["generation"]
    up = await engine.history.lineage("revenue", "", head)
    orders = state.model.heads[("orders", "")]["ref"]["generation"]
    assert [
        (e["from"]["output"], e["from"]["generation"], e["to"]["output"], e["param"]) for e in up["edges"]
    ] == [("orders", orders, "revenue", "orders")]
    assert all(n["current"] for n in up["nodes"])
    down = await engine.history.lineage("orders", "", orders, downstream=True)
    assert {n["output"] for n in down["nodes"]} == {"orders", "revenue"}


async def test_runs_filter_facets_and_pages(state, clock):
    engine = engine_for(state, clock)
    await engine.initialize()
    ok = await run(engine, clock, ["orders"], tags={"env": "prod"})
    bad = await run(engine, clock, ["revenue"], config={"fail": True}, tags={"env": "dev"})
    assert bad["status"] == "failed"
    third = await run(engine, clock, ["orders"], tags={"env": "prod", "who": "ci"})

    async def ids(**kw):
        return [r["id"] for r in (await engine.list_runs(RunFilter(**kw)))["runs"]]

    assert await ids() == [third["id"], bad["id"], ok["id"]]
    assert await ids(status=["failed"]) == [bad["id"]]
    assert await ids(tag=["env=prod"]) == [third["id"], ok["id"]]
    assert await ids(tag=["env=prod", "who"]) == [third["id"]]
    assert await ids(q="timeout") == [bad["id"]]
    assert await ids(asset_tag=["team=growth"]) == [third["id"], ok["id"]]
    assert await ids(since=ok["created_at"] + 1) == [third["id"], bad["id"]]

    page = await engine.list_runs(limit=2)
    assert [r["id"] for r in page["runs"]] == [third["id"], bad["id"]] and page["next"] == bad["id"]
    rest = await engine.list_runs(before=page["next"], limit=2)
    assert [r["id"] for r in rest["runs"]] == [ok["id"]] and rest["next"] is None
    assert page["total"] == 3 and rest["total"] == 1
    row = page["runs"][1]
    assert row["error"] and "upstream timeout" in row["error"] and row["failed_count"] == 1

    facets = await engine.history.facets(RunFilter(status=["failed"]))
    # A facet ignores its own field: every status is still counted.
    assert {f["value"]: f["count"] for f in facets["status"]} == {"succeeded": 2, "failed": 1}
    assert {f["value"]: f["count"] for f in facets["asset"]} == {"revenue": 1}
    assert {f["value"]: f["count"] for f in facets["tag"]} == {"env=dev": 1}

    histogram = await engine.history.histogram(RunFilter(since=ok["created_at"] - 1))
    assert sum(sum(b["counts"].values()) for b in histogram["bars"]) == 3
    assert bucket_for(3600) == 60 and bucket_for(30 * 86400) == 43200

    # Numbered pages pin the newest run of the first page: a run arriving
    # meanwhile doesn't shift the second page.
    await run(engine, clock, ["orders"])
    second = await engine.list_runs(anchor=third["id"], offset=2, limit=2)
    assert [r["id"] for r in second["runs"]] == [ok["id"]] and second["total"] == 3


async def test_live_runs_are_listed(state, clock):
    engine = engine_for(state, clock)
    await engine.initialize()
    submitted = await engine.submit(["orders"], tags={"env": "prod"})
    await engine.pause(submitted["id"])
    listed = (await engine.list_runs(RunFilter(tag=["env=prod"])))["runs"]
    assert [(r["id"], r["status"]) for r in listed] == [(submitted["id"], "paused")]


async def test_a_query_builds_only_the_live_rows_it_reads(state, clock, monkeypatch):
    """Review round 4, engine #7: listing runs builds no task rows, and the
    tasks of one run build only that run's."""

    from solera_server import history

    built = []

    def task_rows(run):
        built.append(run["id"])
        return rows(run)

    rows = history.task_rows
    monkeypatch.setattr(history, "task_rows", task_rows)
    engine = engine_for(state, clock)
    await engine.initialize()
    paused = [await engine.submit(["orders"]) for _ in range(2)]
    for submitted in paused:
        await engine.pause(submitted["id"])
    listed = (await engine.list_runs(RunFilter()))["runs"]
    assert sorted(r["id"] for r in listed) == sorted(r["id"] for r in paused) and built == []
    found = await engine.history.tasks(run=paused[0]["id"])
    assert [t["asset"] for t in found["tasks"]] == ["orders"] and built == [paused[0]["id"]]


async def test_a_run_reads_the_same_once_archived(state, clock):
    engine = engine_for(state, clock)
    await engine.initialize()
    details = {}
    record = state.record

    def spy(*events, **kw):  # the run just before it moves into the history
        for e in events:
            if e["type"] == "RunArchived":
                details[e["run"]] = copy.deepcopy(state.model.runs[e["run"]])
        record(*events, **kw)

    state.record = spy
    failed = await run(engine, clock, ["revenue"], upstream=True, config={"fail": True}, tags={"env": "prod"})
    commit = await engine.commit_source("uploads", upsert=["a", "b"], by="api")
    state.record = record
    # Its attempts were in the history all along: the same rows, live or archived.
    before = engine._detail(details[failed["id"]], True, await engine.history.attempts(failed["id"]))
    events = await engine.history.events(failed["id"])
    assert [e["n"] for e in events] == list(range(1, len(events) + 1))
    assert [e["type"] for e in events if e["task"] is None] == ["submitted", "failed"]
    assert {a.get("executor") for t in before["attempts"].values() for a in t} == {"local"}
    assert await engine.run_detail(failed["id"]) == before
    record = await engine.history.run(commit["run"])
    assert record == {
        "id": commit["run"],
        "source": "uploads",
        "by": "api",
        "batch": 0,
        "upserted": ["a", "b"],
        "deleted": [],
    }


async def test_flush_merge_delete_and_purge(state, clock):
    engine = engine_for(state, clock, flush_rows=1, merge_width=2, purge_seconds=100)
    await engine.initialize()
    runs = [(await run(engine, clock, ["orders"], config={"n": i}))["id"] for i in range(4)]
    await engine.history.lake.tick()
    files = state.model.history.files
    assert not any(state.model.history.rows.values())
    # Each run's rows went out in its own flush; pairs of them were merged.
    assert sum(f["rows"] for f in files["runs"]) == 4
    lake = engine.history.lake
    await lake.stop()
    while lake.plan():
        lake.maintain()
        await lake.job
    assert len(files["runs"]) == 1
    assert [r["id"] for r in (await engine.list_runs())["runs"]] == runs[::-1]

    await engine.delete_run(runs[1])
    hidden = files["runs"][0]
    assert hidden["hidden"] == [runs[1]]
    assert runs[1] not in [r["id"] for r in (await engine.list_runs())["runs"]]
    assert await engine.history.run(runs[1]) is None
    clock.now += 101  # the file is rewritten without the run
    lake.maintain()
    await lake.job
    assert "hidden" not in files["runs"][0] and files["runs"][0]["rows"] == 3
    assert hidden["path"] in {path for path, _ in state.model.garbage}
    made = (await engine.history.materializations(outputs=["orders"]))["materializations"]
    assert runs[1] not in {m["run"] for m in made}


async def test_stale_flush_is_discarded(state, clock):
    engine = engine_for(state, clock)
    await engine.initialize()
    doomed = (await run(engine, clock, ["orders"]))["id"]
    history, lake = engine.history, engine.history.lake
    write = lake._write

    async def racing(table, rows):
        written = await write(table, rows)
        if table == "runs":
            history.delete([doomed])  # deleted while the flush was writing
        return written

    lake._write = racing
    await lake.flush(force=True)
    lake._write = write
    assert not state.model.history.files  # the stale files were not installed
    assert await state.list_objects("history/") == []
    await lake.flush(force=True)
    assert await engine.history.run(doomed) is None


def line(e):
    """An event as `who type [name]`: `run`, a task's asset, or `asset#` for its attempt."""

    who = "run" if e["task"] is None else e["task"].split("/")[1].split(":")[0] + "#" * bool(e["attempt"])
    named = e["type"] in ("launched", "loaded", "mark", "stored")
    return " ".join(
        [who, e["type"], *([e["name"]] if named else []), *([str(e["rows"])] if e["rows"] else [])]
    )


async def test_a_run_has_one_timeline(state, clock):
    """The engine's events and the worker's, in the order they happened.
    The worker's clock is not the engine's: its events are kept within the
    attempt's launch and end — here, one engine moment."""

    engine = engine_for(state, clock)
    await engine.initialize()
    done = await run(engine, clock, ["revenue"], upstream=True)
    events = await engine.history.events(done["id"])
    assert [line(e) for e in events] == [
        "run submitted",
        "orders ready",
        "orders# claimed",
        "orders# launched local",
        "orders# booted",
        "orders# imported",
        "orders# computing",
        "orders# mark counted",
        "orders# computed",
        "orders# writing",
        "orders# stored orders 3",
        "orders# finished",
        "orders# committed",
        "orders succeeded",
        "revenue ready",
        "revenue# claimed",
        "revenue# launched local",
        "revenue# booted",
        "revenue# imported",
        "revenue# loaded orders 3",
        "revenue# computing",
        "revenue# computed",
        "revenue# writing",
        "revenue# stored revenue",
        "revenue# finished",
        "revenue# committed",
        "revenue succeeded",
        "run succeeded",
    ]
    assert {e["at"] for e in events} == {clock.now}
    assert {e["by"] for e in events if e["type"] in ("booted", "mark")} == {"worker"}

    await engine.tick()  # archived
    rows = await engine.history.query(
        lambda con: con.execute(
            "SELECT preparing, provisioning, importing, loading, computing, writing, settling, "
            "peak_memory, cpu_seconds FROM attempts WHERE run = ?",
            [done["id"]],
        ).fetchall(),
        ("attempts",),
    )
    for *phases, peak, cpu in rows:
        assert phases == [0.0] * 7  # every phase reached, at one engine moment
        assert peak is None and cpu >= 0  # an attempt in the engine's process has no peak of its own


async def test_a_task_waits_only_while_it_could_run(state, clock):
    """A task's wait leaves out its run being paused, and the engine being down."""

    engine = engine_for(state, clock)
    await engine.initialize()
    start = clock.now
    submitted = await engine.submit(["orders"])
    clock.now += 10
    await engine.pause(submitted["id"])
    clock.now += 20
    await engine.pause(submitted["id"], False)
    clock.now += 10
    await engine.stop()  # down since `start + 40`
    clock.now += 60
    engine = engine_for(state, clock)
    await engine.initialize()
    clock.now += 10
    await engine.run_until(submitted["id"], 60)
    events = await engine.history.events(submitted["id"])
    outage = next(e for e in events if e["type"] == "outage")
    assert (outage["at"], outage["until"]) == (start + 40, start + 100)
    assert [e["type"] for e in events][:6] == ["submitted", "ready", "paused", "resumed", "outage", "claimed"]
    await engine.tick()
    [(wait,)] = await engine.history.query(
        lambda con: con.execute("SELECT wait FROM tasks WHERE run = ?", [submitted["id"]]).fetchall(),
        ("tasks",),
    )
    assert wait == 10 + 10 + 10


async def test_a_task_held_back_says_why(state, clock):
    engine = Engine(
        state,
        PROJECT.manifest,
        placements={"Local": lambda s, c: InlinePlacement(c, PROJECT)},
        clock=clock,
        concurrency=1,
    )
    await engine.initialize()
    done = await run(engine, clock, ["orders", "ranked"])
    held = [e for e in await engine.history.events(done["id"]) if e["type"] == "held"]
    assert [(line(e), e["reason"]) for e in held] == [("ranked held", "engine")]  # once, not every tick


def test_an_attempt_records_where_it_ran():
    """The executor and what was asked of it: numbers as columns, the rest
    verbatim; a named GPU type counts one."""

    spec = {"executor": "gpu", "kind": "Modal", "environment": {"app": "a"}, "placement": {"gpu": "A10G"}}
    assert execution(spec) == {"executor": "gpu", "gpu": 1, "options": {"gpu": "A10G"}}
    spec = {"executor": "etl", "placement": {"cpu": 4, "memory": 30 * 10**9, "image": "etl:3"}}
    assert execution(spec) == {
        "executor": "etl",
        "cpu": 4,
        "memory": 30 * 10**9,
        "options": {"image": "etl:3"},
    }


async def test_stats(state, clock):
    engine = engine_for(state, clock)
    await engine.initialize()
    for _ in range(3):
        await run(engine, clock, ["orders"])
    await run(engine, clock, ["revenue"], config={"fail": True})
    stats = await engine.history.stats()
    by = {row["asset"]: row for row in stats["assets"]}
    assert by["orders"]["tasks"] == 3 and by["orders"]["failed"] == 0
    assert by["revenue"]["failed"] == 1
    assert by["orders"]["p50"] is not None and by["orders"]["hours"] >= 0
    assert [row["executor"] for row in stats["executors"]] == ["local"]
    # Unpartitioned tasks have the empty scope; a partition narrows to its own.
    assert (await engine.history.stats(asset="orders", scope=""))["assets"] == [by["orders"]]
    assert (await engine.history.stats(asset="orders", scope="2026-01-01"))["assets"] == []


async def test_a_retry_asks_for_the_work_it_selects(state, clock):
    """Review round 3 (system B4): a retry's request is built from the tasks
    it reruns — a key override for an edge none of them reads is left out,
    and each asset's scopes are kept as a map, live and in the history."""
    from solera.sdk import Incremental, StaticPartitions

    @asset(outputs=Output("log", key="id"))
    def log():
        return [{"id": "a"}]

    @asset(inputs={"log": Incremental()})
    def digest(log: list):
        return [{"n": len(log)}]

    @asset(partitions=StaticPartitions(["east", "west"]), inputs={"digest": In()}, retries=Retry(n=0))
    def site(ctx, digest: list):
        if ctx.partition == "west" and ctx.config.get("fail"):
            raise RuntimeError("west is down")
        return [{"site": ctx.partition}]

    project = Project(assets=[log, digest, site])
    engine = Engine(
        state,
        project.manifest,
        placements={"Local": lambda s, c: InlinePlacement(c, project)},
        clock=clock,
        eval_interval=0.01,
    )
    await engine.initialize()
    failed = await run(
        engine, clock, ["site"], partitions="all", upstream=True, config={"fail": True}, keys={"log": "full"}
    )
    assert failed["status"] == "failed"
    retried = await engine.retry(failed["id"], by="ops")
    request = state.model.runs[retried["id"]]
    assert request["keys"] is None  # `digest` succeeded: no retried task reads `log`
    assert request["partitions"] == {"site": ["west"]}
    clock.now += 60
    await engine.run_until(retried["id"], 60)
    assert (await engine.run_detail(retried["id"]))["request"]["partitions"] == {"site": ["west"]}
    [row] = (await engine.list_runs(RunFilter(), limit=1))["runs"]
    assert row["id"] == retried["id"] and row["partitions"] == {"site": ["west"]}


async def test_a_retry_is_a_new_run_and_the_old_one_stays_as_it_ended(state, clock):
    """D1: retrying a finished run submits its failed, canceled and blocked
    work as a new run with `retry_of`; the old run, archived or not, reads
    as it did. A run still in progress, or with nothing failed, refuses."""

    engine = engine_for(state, clock)
    await engine.initialize()
    failed = await run(engine, clock, ["revenue"], upstream=True, config={"fail": True}, tags={"env": "prod"})
    before = await engine.run_detail(failed["id"])
    assert failed["id"] not in state.model.runs  # archived
    owed = {(t["asset"], t["scope"]) for t in before["tasks"] if t["status"] in ("failed", "blocked")}
    assert owed
    clock.now += 60
    retried = await engine.retry(failed["id"], by="ops")
    assert retried["id"] != failed["id"]
    again = state.model.runs[retried["id"]]
    assert (
        again["retry_of"] == failed["id"]
        and again["config"] == {"fail": True}
        and again["tags"] == {"env": "prod"}
    )
    assert {(t["asset"], t["scope"]) for t in again["tasks"].values()} == owed
    with pytest.raises(Conflict, match="not finished"):
        await engine.retry(retried["id"])
    await engine.run_until(retried["id"], 60)
    assert await engine.run_detail(failed["id"]) == before
    [row] = (await engine.list_runs(RunFilter(), limit=1))["runs"]
    assert row["id"] == retried["id"] and row["retry_of"] == failed["id"]
    ok = await run(engine, clock, ["orders"])
    with pytest.raises(Conflict, match="nothing to retry"):
        await engine.retry(ok["id"])
