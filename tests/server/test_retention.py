"""Retention (docs/object-store-state.md §11): finished runs go once every
asset they ran lets go of them; current state and the data in stores never
do. Runs in which every task was skipped are recorded as skipped, and
listed only when asked for."""

import pytest
from solera.sdk import Incremental, Output, Project, Retention, asset
from solera_server.engine import Engine
from solera_server.history import RunFilter
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


def engine_for(state, project, clock):
    return Engine(
        state,
        project.manifest,
        placements={"Local": lambda s, c: InlinePlacement(c, project)},
        clock=clock,
        eval_interval=0.01,
        retention_interval=0,
    )


async def history_ids(engine) -> list[str]:
    """Every finished run in the history, oldest first."""

    def work(con):
        return [r[0] for r in con.execute("SELECT id FROM runs ORDER BY id").fetchall()]

    return await engine.history.query(work, ("runs",), live=False)


async def run_dirs(state) -> set[str]:
    return {p.split("/")[1] for p in await state.list_objects("runs/")}


async def run(engine, targets, **kw):
    detail = await engine.run_until((await engine.submit(targets, **kw))["id"], 60)
    assert detail["request"]["status"] == "succeeded", [
        a.get("error") for x in detail["attempts"].values() for a in x
    ]
    return detail["request"]["id"]


async def test_keep_the_newest_runs(state, clock):
    counter = {"n": 0}

    @asset(retention=Retention(runs=2))
    def kept():
        counter["n"] += 1
        return {"n": counter["n"]}

    @asset(retention=Retention(forever=True))
    def forever():
        return {"n": 1}

    project = Project(assets=[kept, forever], retention=Retention(days=1))
    engine = engine_for(state, project, clock)
    await engine.initialize()
    ids = []
    for _ in range(4):
        clock.now += 60
        ids.append(await run(engine, ["kept"]))
    clock.now += 60
    kept_forever = await run(engine, ["forever"], mode="full")
    clock.now += 2 * 86400  # past the project default too
    await engine.tick()
    assert await history_ids(engine) == [*ids[-2:], kept_forever]
    assert await run_dirs(state) == {*ids[-2:], kept_forever}
    # The head of a run long gone still loads, and still names that run.
    head = state.model.heads[("kept", "")]
    assert head["run"] == ids[-1]


async def test_quiet_runs_are_recorded_as_skipped(state, clock):
    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a"}]

    @asset(inputs={"items": Incremental()})
    def reader(items: list):
        return []

    project = Project(assets=[items, reader])
    engine = engine_for(state, project, clock)
    await engine.initialize()
    await run(engine, ["reader"], upstream=True)
    quiet = await run(engine, ["reader"])  # nothing pending: skipped
    assert quiet not in await run_dirs(state)
    assert (await engine.run_detail(quiet))["tasks"][0]["status"] == "skipped"
    assert quiet not in {r["id"] for r in (await engine.list_runs())["runs"]}
    skipped = (await engine.list_runs(RunFilter(status=["skipped"])))["runs"]
    assert [r["id"] for r in skipped] == [quiet]


async def test_source_commits_are_recorded_as_runs(state, clock):
    """§7: each source commit that changes something is a run with no tasks,
    saying who committed and what changed; the default policy expires it."""

    from solera.sdk import Source

    @asset(inputs={"uploads": Incremental()})
    def ingest(uploads: list):
        return []

    project = Project(assets=[ingest], sources=[Source("uploads", key="id")], retention=Retention(days=1))
    engine = engine_for(state, project, clock)
    await engine.initialize()
    first = await engine.commit_source("uploads", keys={"u-6": "1", "u-7": "1"}, by="sharepoint-webhook")
    assert not (await engine.commit_source("uploads", upsert={"u-7": "1"}))["changed"]  # no change, no run
    clock.now += 60
    second = await engine.commit_source("uploads", keys={"u-7": "2"}, by="api")
    record = await engine.history.run(second["run"])
    assert record == {
        "id": second["run"],
        "source": "uploads",
        "by": "api",
        "batch": 1,
        "upserted": ["u-7"],
        "deleted": ["u-6"],
    }
    assert state.model.heads[("uploads", "")]["run"] == second["run"]
    view = (await engine.run_detail(first["run"]))["request"]
    assert view["by"] == "sharepoint-webhook" and view["status"] == "succeeded" and view["tasks"] == []
    assert view["created_at"] == pytest.approx(clock.now - 60, abs=0.01)
    listed = [r["id"] for r in (await engine.list_runs())["runs"]]
    assert listed[:2] == [second["run"], first["run"]]
    clock.now += 2 * 86400
    await engine.tick()
    assert await history_ids(engine) == []  # expired under the project default
