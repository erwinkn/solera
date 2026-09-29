"""The run history (docs/object-store-state.md §7): rows born in the model,
flushed to Parquet, merged, hidden when deleted, and queried with DuckDB."""

import pytest
from solera.sdk import In, Output, Project, Result, Retry, Source, asset
from solera_server.engine import Engine
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

    head = state.model.heads[("revenue", "")]["ref"]["version"]
    up = await engine.history.lineage("revenue", "", head)
    assert [(e["from"]["output"], e["to"]["output"], e["param"]) for e in up["edges"]] == [
        ("orders", "revenue", "orders")
    ]
    assert all(n["current"] for n in up["nodes"])
    orders_version = state.model.heads[("orders", "")]["ref"]["version"]
    down = await engine.history.lineage("orders", "", orders_version, downstream=True)
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


async def test_a_run_reads_the_same_once_archived(state, clock):
    engine = engine_for(state, clock)
    await engine.initialize()
    details = {}
    emit = state.emit

    async def spy(e):  # the run's detail just before it moves into the history
        if e["type"] == "RunArchived":
            details[e["run"]] = await engine.run_detail(e["run"])
        await emit(e)

    state.emit = spy
    failed = await run(engine, clock, ["revenue"], upstream=True, config={"fail": True}, tags={"env": "prod"})
    await engine.retry(failed["id"])
    await engine.run_until(failed["id"], 60)
    commit = await engine.commit_source("uploads", upsert=["a", "b"], by="api")
    state.emit = emit
    before = details[failed["id"]]
    assert [t["retried"] for t in before["tasks"] if t["asset"] == "revenue"] == [1]
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
    await engine.tick()
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
            await history.delete([doomed])  # deleted while the flush was writing
        return written

    lake._write = racing
    await lake.flush(force=True)
    lake._write = write
    assert not state.model.history.files  # the stale files were not installed
    assert await state.list_objects("history/") == []
    await lake.flush(force=True)
    assert await engine.history.run(doomed) is None


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
