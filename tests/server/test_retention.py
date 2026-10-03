"""Retention (docs/object-store-state.md §11): finished runs go once every
asset they ran lets go of them; current state and the data in stores never
do. Runs in which every task was skipped are recorded as skipped, and
listed only when asked for."""

import pytest
from solera.sdk import Incremental, Output, Project, Retention, asset
from solera_server.engine import Engine
from solera_server.executors.inline import InlinePlacement
from solera_server.history import RunFilter
from solera_server.state import State, Unavailable


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
    """Runs whose objects are there: a deleted run leaves none (docs/lifecycle.md §2.4)."""

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
    await engine.upkeep.tick()
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
        "commit_number": 1,
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
    await engine.upkeep.tick()
    assert await history_ids(engine) == []  # expired under the project default


@asset
def plain():
    return {"n": 1}


PLAIN = Project(assets=[plain])


async def test_a_run_retires_for_good_before_its_files_go(tmp_path, state, clock):
    """Deletion is retirement first: `RunsDeleted` is durable before any file
    goes. A replaced engine cannot make it durable, so it deletes nothing;
    and a deletion cut short resumes after a restart."""

    engine = engine_for(state, PLAIN, clock)
    await engine.initialize()
    gone, kept = await run(engine, ["plain"]), await run(engine, ["plain"])
    await engine.history.lake.flush(force=True)

    successor = await State.open(tmp_path.as_uri(), "test", clock=clock, flush_interval=0.001)
    with pytest.raises(Unavailable):
        await engine.delete_run(gone)  # replaced: retirement never becomes durable
    assert await run_dirs(state) == {gone, kept}
    await engine.stop()

    later = engine_for(successor, PLAIN, clock)
    await later.initialize()
    delete = successor.delete_run

    async def crash(run_id):
        raise OSError("the engine went down")

    successor.delete_run = crash
    with pytest.raises(OSError):
        await later.delete_run(gone)
    assert successor.model.deleted == [gone] and await later.history.run(gone) is None
    await later.stop()
    await successor.close()

    again = await State.open(tmp_path.as_uri(), "test", clock=clock, flush_interval=0.001)
    assert again.model.deleted == [gone]
    restarted = engine_for(again, PLAIN, clock)
    await restarted.upkeep.tick()
    assert again.model.deleted == [] and await run_dirs(again) == {kept}
    with pytest.raises(KeyError):
        await restarted.retry(gone)  # a retired run never comes back
    successor.delete_run = delete
    await again.close()


async def test_a_deleted_run_takes_its_control_files_and_a_late_worker_writes_nothing(state, clock):
    """docs/lifecycle.md §2.4 ("a worker paused for a week"): deleting a run
    deletes everything of it, control files included, and nothing is kept
    for later. A worker that resumes then swaps on the version it last
    read; the file is gone, so the swap is refused, and it writes nothing
    (a worker never creates the file)."""

    from solera import lifecycle
    from solera.objects import Conflict, swap

    from tests.server.test_fence import Gated

    engine = engine_for(state, Project(assets=[plain], default_store=Gated()), clock)  # gated
    await engine.initialize()
    gone = await run(engine, ["plain"])
    [attempt] = {p.rsplit("/", 1)[-1].split(".")[0] for p in await state.list_objects(f"runs/{gone}/")}
    _, version = await lifecycle.read_control(state.objects, gone, attempt)
    await engine.history.lake.flush(force=True)
    await engine.delete_run(gone)
    assert await state.list_objects(f"runs/{gone}/") == []
    body = lifecycle.control(lifecycle.WRITING, worker_id="late", intents={})
    with pytest.raises(Conflict):  # what a resumed worker's gate meets
        await swap(state.objects, f"runs/{gone}/{attempt}{lifecycle.CONTROL}", body, version)
    assert await state.list_objects(f"runs/{gone}/") == []


async def test_runs_kept_forever_do_not_crowd_out_expired_ones(state, clock):
    """Review round 2, engine #5: the candidates are counted after each
    asset's horizon applies, so older runs of an asset kept forever never
    fill the page that the expired run of another asset is due on."""

    @asset(retention=Retention(forever=True))
    def keep() -> int:
        return 1

    @asset(retention=Retention(days=1))
    def daily() -> int:
        return 1

    engine = engine_for(state, Project(assets=[keep, daily]), clock)
    await engine.initialize()
    kept = []
    for _ in range(3):
        clock.now += 60
        kept.append(await run(engine, ["keep"]))
    clock.now += 60
    gone = await run(engine, ["daily"])
    clock.now += 2 * 86400
    assert await engine.history.expired({"daily": clock.now - 86400}, None, limit=2) == [(gone, "succeeded")]
    await engine.upkeep.sweep()
    assert await history_ids(engine) == kept


async def test_pruning_a_skipped_run_deletes_what_its_attempt_wrote(state, clock):
    """Review round 2, engine #6: a consumer whose patterns take none of
    the keys launches, then reports skipped. Pruning it removes its spec,
    claim and result with its history."""

    @asset(outputs=Output("items", keyed=True))
    def items():
        return {"x": 1}

    @asset(inputs={"items": Incremental(include=["z*"])})
    def picky(items: dict) -> int:
        return len(items)

    engine = engine_for(state, Project(assets=[items, picky]), clock)
    await engine.initialize()
    await run(engine, ["items"])
    skipped = await run(engine, ["picky"])
    assert (await engine.run_detail(skipped))["request"]["status"] in ("skipped", "succeeded")
    assert skipped in await run_dirs(state)
    await engine.prune(asset="picky")
    assert skipped not in await run_dirs(state)
