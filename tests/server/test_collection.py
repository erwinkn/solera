"""Collection of immutable stores' data (docs/lifecycle.md §9.8): what a
commit or an abandoned attempt let go of is cleaned up by a cleanup task,
once no reader can still need it. Every wait is on a cleanup run settling,
which acknowledges what it cleaned only once it is done — never on time."""

import asyncio
import json
import math
import random

from solera.sdk import Incremental, Output, Project, Retry, asset
from solera.stores import FileStore, Patch
from solera_server.engine import Engine
from solera_server.executors.inline import InlinePlacement
from solera_server.model import TERMINAL_RUN
from solera_server.state import State

from tests.conftest import whole, worker_finished


def engine_for(state, project, **kw):
    placements = {"Local": lambda s, c: InlinePlacement(c, project)}
    kw.setdefault("cleanup_interval", 0.0)  # the cleanup job looks at every tick
    return Engine(state, project.manifest, placements=placements, clock=state.clock, eval_interval=0.02, **kw)


GUARD = 300.0  # seconds: a deadlock guard on a run settling, never a timing assumption


async def cleaned(engine) -> list[dict]:
    """Every cleanup due, run until none is: each wait is on a cleanup run
    settling, whose result acknowledges its entries and its step only once
    their deletes are done. Workers still finishing after their runs settled
    are waited for first: what is due is decided once they are done. Their
    runs' details."""

    done = []
    await worker_finished()
    for _ in range(50):
        engine._submit_cleanups()
        pending = [
            r["id"]
            for r in engine.m.runs.values()
            if r["kind"] == "cleanup" and r["status"] not in TERMINAL_RUN
        ]
        if not pending:
            return done
        for run_id in pending:
            done.append(await engine.run_until(run_id, GUARD))
        await worker_finished()
    raise AssertionError(f"cleanup never settled: {engine.m.cleanups}, {engine.m.cleaning}")


async def run(engine, targets):
    """A run settled, its workers done, and the cleanup tasks it made due run."""

    detail = await engine.run_until((await engine.submit(targets))["id"], GUARD)
    assert detail["request"]["status"] == "succeeded", [t.get("error") for t in detail["tasks"]]
    await worker_finished()
    await cleaned(engine)
    return detail


def objects(data, output) -> set[str]:
    return {str(p.relative_to(data)).rsplit(".", 1)[0] for p in (data / output).rglob("*") if p.is_file()}


async def named(state, output) -> set[str]:
    """The objects the key index names: exactly what a keyed output should hold."""

    keys = await whole(state, output)
    return {FileStore.key_name(output, k, g) for k, g in keys.generations.items()}


async def no_cleanups_after_commit(*args):
    return {}


async def test_superseded_versions_go_right_after_the_commit(tmp_path, data):
    """A commit lets go of each changed key's predecessor and of a value's
    previous object; its own worker deletes them once the commit is durable
    (D8) — a partition that never runs again keeps no garbage."""

    values = iter([{"a": 1, "b": 1}, {"a": 2, "b": 1}, {"a": 2, "b": 1}])
    totals = iter([1, 2, 2])

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return next(values)

    @asset
    def total():
        return next(totals)

    project = Project(assets=[scores, total])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    for _ in range(2):
        await run(engine, ["scores", "total"])
    assert objects(data, "scores") == await named(state, "scores")  # the old `a` already gone
    assert len(list(data.glob("total@*"))) == 1
    assert ("scores", "") not in state.model.cleanups
    await engine.stop()
    await state.close()


async def test_an_abandoned_attempts_objects_go(tmp_path, data, monkeypatch):
    """An attempt dies after its first object landed: everything it wrote
    carries its generation. Its cleanup runs late (the sweep, §9.8): due its
    asset's timeout and the cancel grace after its end, by when a worker
    that outlived it has written its last — so the retry, right after,
    leaves the object, and the partition's next attempt past the window
    cleans it up, and its delta file."""

    values = [{"a": 1, "b": 1}, {"a": 2, "c": 1}, {"a": 2, "c": 1}, {"a": 2, "c": 1}]

    @asset(outputs=Output("scores", keyed=True), retries=Retry(1, delay=0), timeout=1)
    def scores():
        return values.pop(0)

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, cancel_grace=0)
    await engine.initialize()
    await run(engine, ["scores"])
    put, puts = FileStore._put, []

    async def dying(self, base, value):
        await put(self, base, value)
        puts.append(base)
        if len(puts) == 1:
            raise OSError("the worker died")  # once one object landed

    monkeypatch.setattr(FileStore, "_put", dying)
    detail = await run(engine, ["scores"])
    first, _ = detail["attempts"][detail["tasks"][0]["id"]]
    assert first["outcome"] == "failed"
    assert (data / f"{puts[0]}.json").exists()  # a late writer might still add to it: not yet
    late = data / "scores" / "zz" / f"{puts[0].rsplit('/', 1)[1]}.json"  # and one does, after its end
    late.parent.mkdir(parents=True, exist_ok=True)
    late.write_text("{}")
    await asyncio.sleep(1.1)  # the window passes
    await run(engine, ["scores"])  # the next attempt cleans it up, and what the retry superseded
    assert not (data / f"{puts[0]}.json").exists() and not late.exists()  # one cleanup takes both
    assert objects(data, "scores") == await named(state, "scores")
    prefix = state.model.indexes[("scores", "")].prefix
    engine.upkeep._orphans_at = -math.inf  # its delta: the engine's orphan rule, no cleanup task's
    await engine.upkeep.collect_orphans()
    await engine.upkeep.collect()
    left = {p.rsplit("/", 1)[-1] for p in await state.list_objects(prefix)}
    assert not any(first["id"] in name for name in left)  # its delta file went too
    await engine.stop()
    await state.close()


async def test_merges_leave_nothing_uncollected(tmp_path, data):
    """Merges drop superseded entries; writes are exact, so every version a
    merge drops was named as replaced by the delta that superseded it, and
    merges list nothing of their own: with everything collected, the store
    holds exactly what the index names. A consumer's observations hold the
    cleanup cursor back meanwhile."""

    rng = random.Random(11)
    pending = {"rows": {}}

    @asset(outputs=Output("items", keyed=True))
    def items():
        return Patch(pending["rows"])

    @asset(inputs={"items": Incremental(batch_size=5)})
    def mirror(items: list):
        return []

    project = Project(assets=[items, mirror])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    compacted = 0
    for step in range(24):
        pending["rows"] = {f"k{rng.randrange(12)}": rng.randrange(4) for _ in range(4)}
        await run(engine, ["items"])
        if step % 6 == 5:
            await run(engine, ["mirror"])
        engine.upkeep.maintain()
        for job in list(engine.upkeep.jobs.values()):
            await job
        compacted += any(not x.delta for x in state.model.indexes[("items", "")].layers)  # merge outputs
    assert compacted, "no merge ran"
    pending["rows"] = {}
    for _ in range(4):  # unchanged runs: each collects what is due
        await run(engine, ["items"])
    assert objects(data, "items") == await named(state, "items")
    await engine.stop()
    await state.close()


async def test_a_reader_pin_holds_collection_back(tmp_path):
    """Cleanup is due only once every reader pin has passed it: the
    claims of attempts in flight."""

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return {"a": 1}

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    m = state.model
    m.cleanups[("scores", "")] = [{"n": 10, "id": "10.0", "kind": "version", "generation": 1}]
    m.claims["reader"] = {"attempt": "r", "generation": 9, "started_at": 0, "status": "running"}
    assert engine._due_cleanups("scores", "", "me") == []
    m.claims["reader"]["generation"] = 10
    assert [d["n"] for d in engine._due_cleanups("scores", "", "me")] == [10]
    assert [d["n"] for d in engine._due_cleanups("scores", "", "r")] == [10]  # its own claim reads none of it
    await state.close()


async def test_entries_of_one_event_are_acknowledged_one_by_one(tmp_path):
    """Astra review 2, P2-5: one commit lets go of several entries at one
    event counter, and the per-task limit splits them. Acknowledging the
    delivered ones leaves the others queued."""

    from solera_server import engine as engine_module

    writes = iter([{"a": 1}])

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return next(writes, Patch({}))  # then nothing: no commit lets go of anything more

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["scores"])
    m = state.model
    m.cleanups.pop(("scores", ""), None)  # the first commit's own
    m.event_counter += 1
    m._collect("scores", "", {"kind": "version", "generation": 1})
    m._collect("scores", "", {"kind": "version", "generation": 2})
    m._collect("scores", "", {"kind": "version", "generation": 3})
    entries = m.cleanups[("scores", "")]
    assert len({d["n"] for d in entries}) == 1 and len({d["id"] for d in entries}) == 3
    submit = engine._submit_cleanups
    engine._submit_cleanups = lambda partitions=None: None  # this test submits each task itself
    engine_module.CLEANUPS, limit = 1, engine_module.CLEANUPS
    try:
        submit()
        [first] = [
            r for r in engine.m.runs.values() if r["kind"] == "cleanup" and r["status"] not in TERMINAL_RUN
        ]
        await engine.run_until(first["id"], GUARD)  # the first alone
        assert [d["generation"] for d in m.cleanups[("scores", "")]] == [2, 3]
    finally:
        engine_module.CLEANUPS, engine._submit_cleanups = limit, submit
    await cleaned(engine)  # the others, each acknowledged
    assert ("scores", "") not in m.cleanups
    await engine.stop()
    await state.close()


def scores_project():
    values = iter([{"a": 1, "b": 1}, {"a": 2, "b": 1}, {"a": 3, "b": 1}, {"a": 3, "b": 1}])

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return next(values)

    return Project(assets=[scores])


async def test_a_cleanup_task_that_keeps_failing_is_stuck_and_shown(tmp_path, data, monkeypatch):
    """A cleanup task whose store refuses retries within its budget; then the
    cursor step it was handed is stuck — kept, shown, and handed to no
    further task, holding the deltas after it — until an operator clears it."""

    from solera_server import engine as engine_module

    async def refused(self, *args, **kw):
        raise OSError("the bucket refuses deletes")

    monkeypatch.setattr(engine_module, "CLEANUP_TRIES", 2)
    monkeypatch.setattr(engine_module, "CLEANUP_RETRY_DELAY", 0.0)
    project = scores_project()
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["scores"])
    monkeypatch.setattr(FileStore, "cleanup", refused)
    await run(engine, ["scores"])  # supersedes `a`: its cleanup step's task fails, twice
    [step] = state.model.cleaning[("scores", "")]
    assert step["stuck"] and objects(data, "scores") != await named(state, "scores")
    await engine.tick()
    assert await cleaned(engine) == []  # no more tasks: it waits for an operator
    view = engine.partition_cleanups("scores", "")
    assert view["pending"] == 0 and [e["id"] for e in view["stuck"]] == [f"delta:{step['n']}"]
    assert engine.clear_cleanups("scores", "", "ops")["cleared"] == [f"delta:{step['n']}"]
    assert ("scores", "") not in state.model.cleaning  # given up on: its objects stay
    await engine.stop()
    await state.close()


async def test_a_reader_pin_at_commit_keeps_the_garbage_queued(tmp_path, data):
    """D8: a reader still pinned before the commit may read what it let go
    of: nothing is cleaned up after the commit; its delta stays queued."""

    project = scores_project()
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["scores"])
    with state.model.reading():  # a reader of the index as it is before the commit
        await run(engine, ["scores"])
        assert objects(data, "scores") != await named(state, "scores")
        assert [d["commit"] for d in state.model.cleaning[("scores", "")]] == [1]  # queued
    await run(engine, ["scores"])
    assert objects(data, "scores") == await named(state, "scores")
    await engine.stop()
    await state.close()


async def test_a_run_is_settled_before_its_cleanup_has_run(tmp_path, data, monkeypatch):
    """Review round 3, S2: a run reads settled while the cleanup its commit
    made due (a cleanup task's step, D168) is still under way; tests wait
    for that step's acknowledgement, never for a while, nor for workers."""

    cleanup, go = FileStore.cleanup, asyncio.Event()

    async def held(self, *args, **kw):
        await go.wait()
        await cleanup(self, *args, **kw)

    monkeypatch.setattr(FileStore, "cleanup", held)
    project = scores_project()
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["scores"])  # nothing superseded yet
    settled = await engine.run_until((await engine.submit(["scores"]))["id"], GUARD)  # supersedes `a`
    assert settled["request"]["status"] == "succeeded"
    assert objects(data, "scores") != await named(state, "scores")  # its cleanup is still to come
    assert state.model.cleaning[("scores", "")]  # queued, not acknowledged
    go.set()
    await cleaned(engine)
    assert objects(data, "scores") == await named(state, "scores")
    await engine.stop()
    await state.close()


async def test_a_slow_reader_holds_back_only_what_it_reads(tmp_path):
    """Review round 2, system #6 and engine #8: pins are per output partition.
    An attempt reading `scores` holds back `scores`' garbage only, not
    another output's nor the history's; a claim still preparing holds
    back everything; a history query holds back only history files."""

    project = Project(assets=[scores_project().assets["scores"]])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    m = state.model
    mine, other = m.index("scores", "").prefix, m.index("elsewhere", "").prefix
    garbage = [f"{mine}a.kx", f"{other}b.kx", "history/runs/c.parquet"]
    for path in garbage:
        await state.create_object(path, b"x")
    m.event_counter += 1
    m.garbage += [[path, m.event_counter] for path in garbage]
    m.claims["t"] = {
        "attempt": "r",
        "generation": m.event_counter - 1,
        "prefixes": (mine,),
    }  # an attempt reading scores
    await engine.upkeep.collect()
    assert [g[0] for g in m.garbage] == [f"{mine}a.kx"]
    m.claims["t"]["prefixes"] = None  # still preparing: it may read anything
    m.garbage += [[f"{other}d.kx", m.event_counter]]
    await state.create_object(f"{other}d.kx", b"x")
    await engine.upkeep.collect()
    assert sorted(g[0] for g in m.garbage) == sorted([f"{mine}a.kx", f"{other}d.kx"])
    del m.claims["t"]
    with m.reading("history/"):  # a history query: index files are not its
        await engine.upkeep.collect()
    assert m.garbage == []
    await state.close()


async def test_collection_reduces_the_pins_once_per_pass(tmp_path, monkeypatch):
    """Review round 4, engine #6: a pass costs garbage + pins, not garbage ×
    pins. 40 output partitions, each with a garbage file and a reader; the even
    ones' readers pinned before their file was let go of. The pins are
    walked once, and each odd file goes."""

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, scores_project())
    await engine.initialize()
    m = state.model
    prefixes = [m.index(f"out{i}", "").prefix for i in range(40)]
    for prefix in prefixes:
        await state.create_object(f"{prefix}a.kx", b"x")
    m.event_counter += 1
    m.garbage += [[f"{prefix}a.kx", m.event_counter] for prefix in prefixes]
    for i, prefix in enumerate(prefixes):
        m.claims[f"t{i}"] = {
            "attempt": f"a{i}",
            "generation": m.event_counter - (i % 2 == 0),
            "prefixes": (prefix,),
        }

    walks = []

    class Pins(list):
        def __iter__(self):
            walks.append(1)
            return super().__iter__()

    pins = m.pins
    monkeypatch.setattr(m, "pins", lambda but=None: Pins(pins(but)))
    await engine.upkeep.collect()
    assert len(walks) == 1
    assert [g[0] for g in m.garbage] == [f"{p}a.kx" for p in prefixes[::2]]
    await state.close()


async def test_a_pool_job_with_no_inputs_pins_only_its_output(tmp_path):
    """Engine #8: a launched attempt's pin names what it reads and writes;
    one with no inputs, nothing else of the namespace."""

    from solera.executors import Pool

    @asset(executor=Pool("jobs")(), outputs=Output("made", keyed=True))
    def lonely():
        return {"a": 1}

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, Project(assets=[lonely]))
    await engine.initialize()
    await engine.submit(["lonely"])
    for _ in range(3000):
        await engine.tick()
        if state.model.pool:
            break
        await asyncio.sleep(0.02)
    [claim] = state.model.claims.values()
    assert claim["prefixes"] == (state.model.index("made", "").prefix,)
    await state.close()


async def test_a_delta_a_launching_attempt_was_handed_outlives_its_acknowledgement(
    tmp_path, data, monkeypatch
):
    """F36. The engine hands a cleanup task a cursor step in its spec; before
    `AttemptLaunched` lands (the spec and the control file are being
    written), the step's deltas leave the queue (another task acknowledged
    them). Were the attempt's claim to name the step's files only from
    `AttemptLaunched` on, nothing would hold the delta file meanwhile:
    collection would delete it, and the worker read a file that is gone
    (simulation, seed 11: a `checks` delta file deleted 6 s before its
    reader came)."""

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return {"a": 1}

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, Project(assets=[scores]))
    await engine.initialize()
    await run(engine, ["scores"])
    m = state.model
    index = m.indexes[("scores", "")]
    [name] = [n for n in index.referenced() if n.endswith(".lay")]
    real = await state.get_object(index.path(name))  # a real delta file's bytes
    path = f"{index.prefix}held.lay"
    await state.create_object(path, real)
    m.cleaning[("scores", "")] = [
        {
            "n": 1,
            "life": index.life,
            "commit": 0,
            "generation": index.generation,
            "prefix": index.prefix,
            "files": ["held.lay"],
        }
    ]
    m.garbage.append([path, 1])  # the index let go of it: the queued step alone holds it

    create, at_control, go, specs = state.create_object, asyncio.Event(), asyncio.Event(), []

    async def held(name, body, *args, **kw):
        if name.endswith(".spec"):
            specs.append(json.loads(body))
        if name.endswith(".control"):  # the spec is written; `AttemptLaunched` is not recorded yet
            at_control.set()
            await go.wait()
        return await create(name, body, *args, **kw)

    monkeypatch.setattr(state, "create_object", held)
    engine._submit_cleanups()
    running = asyncio.create_task(cleaned(engine))
    await asyncio.wait_for(at_control.wait(), GUARD)
    assert specs[-1]["cleanup"]["outputs"]["scores"]["deltas"]["deltas"][0]["files"] == ["held.lay"]
    del m.cleaning[("scores", "")]  # acknowledged by another meanwhile
    await engine.upkeep.collect()
    survived = await state.get_object(path) is not None
    go.set()
    await running
    await engine.stop()
    await state.close()
    assert survived, "collection deleted the delta file a launching cleanup task's spec hands it"
