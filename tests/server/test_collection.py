"""Collection of immutable stores' data (docs/lifecycle.md §9.8): what a
commit, a compaction or an abandoned attempt let go of is cleaned up by a
cleanup task, once no reader can still need it."""

import asyncio
import json
import random

from solera.keys.index import Options
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


async def cleaned(engine) -> list[dict]:
    """Every cleanup task submitted, run to its end, until nothing is due (a
    task skipped for a reader pinned at its prepare leaves its entries to
    the job); their runs' details."""

    done = []
    for _ in range(500):
        pending = [
            r for r in engine.m.runs.values() if r["kind"] == "cleanup" and r["status"] not in TERMINAL_RUN
        ]
        for r in pending:
            done.append(await engine.run_until(r["id"], 20))
        if not pending:
            if not any(engine._due_cleanups(o, p, None) for o, p in list(engine.m.cleanups)):
                return done
            await engine.tick()
            await asyncio.sleep(0.01)
    raise AssertionError(f"cleanup never settled: {engine.m.cleanups}")


async def run(engine, targets):
    """A run settled, its workers done, and the cleanup tasks it made due run."""

    detail = await engine.run_until((await engine.submit(targets))["id"], 20)
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
    await engine.upkeep.collect()
    left = {p.rsplit("/", 1)[-1] for p in await state.list_objects(prefix)}
    assert not any(first["id"] in name for name in left)  # its delta file went too
    await engine.stop()
    await state.close()


async def test_compaction_leaves_nothing_uncollected(tmp_path, data):
    """Compaction drops shadowed entries; writes are exact, so every one was
    named as a predecessor by the delta that superseded it, and compaction
    lists nothing of its own: with everything collected, the store holds
    exactly what the index names."""

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
    engine = engine_for(state, project, key_options=Options(window=2))
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
        files = state.model.indexes[("items", "")].files
        compacted += any(f.name.startswith("m") for f in files)  # merge outputs
    assert compacted, "no merge ran"
    pending["rows"] = {}
    for _ in range(4):  # unchanged runs: each collects what is due
        await run(engine, ["items"])
    assert objects(data, "items") == await named(state, "items")
    await engine.stop()
    await state.close()


async def test_a_reader_pin_holds_collection_back(tmp_path):
    """Cleanup is due only once every reader pin has passed it: the
    claims of attempts in flight, and a delta pass in batches over attempts."""

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
    m._partition("c", "")["positions"] = {
        "e": {
            "kind": "keys",
            "output": "scores",
            "upstream_partition": "",
            "next": 0,
            "pass": {"mode": "delta", "from": 0, "to": 1, "at": "k", "batch": 1, "batches": 2, "pin": 8},
        }
    }
    assert engine._due_cleanups("scores", "", "me") == []
    del m.partitions[("c", "")]
    assert [d["n"] for d in engine._due_cleanups("scores", "", "me")] == [10]
    assert [d["n"] for d in engine._due_cleanups("scores", "", "r")] == [10]  # its own claim reads none of it
    await state.close()


async def test_a_delta_a_pending_cleanup_reads_outlives_its_index(tmp_path, data):
    """Review P2-7: compaction lets go of a delta file whose entry is still
    queued: index cleanup keeps the file until the entry is done. And an
    entry whose file is missing is not acknowledged as done."""

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return {"a": 1}

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["scores"])
    m = state.model
    prefix = m.indexes[("scores", "")].prefix
    path = f"{prefix}held.kx"
    await state.create_object(path, b"delta")
    m.cleanups[("scores", "")] = [{"n": 1, "id": "1.0", "kind": "delta", "prefix": prefix, "files": ["held"]}]
    m.garbage.append([path, 1])  # the index let go of it
    await engine.upkeep.collect()
    assert await state.get_object(path) == b"delta" and [path, 1] in m.garbage
    m.cleanups[("scores", "")][0]["files"] = ["gone"]  # its names cannot be read
    await run(engine, ["scores"])
    assert [d["files"] for d in m.cleanups[("scores", "")]] == [["gone"]]  # still pending
    del m.cleanups[("scores", "")]
    await engine.upkeep.collect()
    assert await state.get_object(path) is None
    await engine.stop()
    await state.close()


async def test_an_entry_whose_names_cannot_be_read_gets_stuck_and_is_shown(tmp_path):
    """A cleanup task that cannot read an entry's names counts it a miss, and
    the next one is submitted as it ends; after three, the entry is stuck: no
    longer handed out, so no more tasks; shown in diagnostics and on the
    partition's head, until an operator clears it."""

    import httpx
    from solera_server.api import create_app

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
    prefix = m.indexes[("scores", "")].prefix
    m.cleanups[("scores", "")] = [{"n": 1, "id": "1.0", "kind": "delta", "prefix": prefix, "files": ["gone"]}]
    engine._submit_cleanups()
    assert len(await cleaned(engine)) == 3  # a miss each, then stuck: no fourth
    [entry] = m.cleanups[("scores", "")]
    assert entry["misses"] == 3 and entry["stuck"]
    assert engine._due_cleanups("scores", "", None) == []
    app = create_app(engine=engine, insecure=True)
    app.state.engine = engine
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/api/diagnostics")).json()["stuck_cleanups"] == [
            {"output": "scores", "partition": "", "id": "1.0"}
        ]
        base = f"/api/projects/{project.manifest['name']}"
        [head] = (await client.get(f"{base}/outputs/scores/heads")).json()["heads"]
        assert head["cleanups"]["pending"] == 0 and [e["id"] for e in head["cleanups"]["stuck"]] == ["1.0"]
        cleared = await client.post(f"{base}/cleanups:clear", json={"output": "scores", "by": "ops"})
        assert cleared.json()["cleared"] == ["1.0"]
    assert ("scores", "") not in m.cleanups
    await engine.stop()
    await state.close()


async def test_entries_of_one_event_are_acknowledged_one_by_one(tmp_path):
    """Astra review 2, P2-5: one commit lets go of several entries at one
    event counter, and the per-task limit splits them. Acknowledging the
    delivered ones leaves the others queued, and a resolved sibling's
    acknowledgment does not erase an unresolved one's misses."""

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
    prefix = m.indexes[("scores", "")].prefix
    m.cleanups.pop(("scores", ""), None)  # the first commit's own
    m.event_counter += 1
    m._collect("scores", "", {"kind": "version", "generation": 1})
    m._collect("scores", "", {"kind": "delta", "prefix": prefix, "files": ["gone"]})
    m._collect("scores", "", {"kind": "version", "generation": 2})
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
        await engine.run_until(first["id"], 20)  # the first alone
        assert [d["kind"] for d in m.cleanups[("scores", "")]] == ["delta", "version"]
    finally:
        engine_module.CLEANUPS, engine._submit_cleanups = limit, submit
    submit()
    await cleaned(engine)  # the unresolved delta, a miss a task, and its resolved sibling
    [left] = m.cleanups[("scores", "")]
    assert left["kind"] == "delta" and left["misses"] == 3 and left["stuck"]
    await engine.stop()
    await state.close()


def scores_project():
    values = iter([{"a": 1, "b": 1}, {"a": 2, "b": 1}, {"a": 3, "b": 1}, {"a": 3, "b": 1}])

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return next(values)

    return Project(assets=[scores])


async def test_a_cleanup_task_that_keeps_failing_is_stuck_and_shown(tmp_path, data, monkeypatch):
    """A cleanup task whose store refuses retries within its budget; then its
    entries are stuck — kept, shown, and handed to no further task — until
    an operator clears them."""

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
    await run(engine, ["scores"])  # supersedes `a`: its cleanup task fails, twice
    [entry] = state.model.cleanups[("scores", "")]
    assert entry["stuck"] and objects(data, "scores") != await named(state, "scores")
    await engine.tick()
    assert await cleaned(engine) == []  # no more tasks: it waits for an operator
    await engine.stop()
    await state.close()


async def test_a_reader_pin_at_commit_keeps_the_garbage_queued(tmp_path, data):
    """D8: a reader still pinned before the commit may read what it let go
    of: nothing is cleaned up after the commit; the entry stays queued."""

    project = scores_project()
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["scores"])
    with state.model.reading():  # a reader of the index as it is before the commit
        await run(engine, ["scores"])
        assert objects(data, "scores") != await named(state, "scores")
        assert [d["kind"] for d in state.model.cleanups[("scores", "")]] == ["delta"]
    await run(engine, ["scores"])
    assert objects(data, "scores") == await named(state, "scores")
    await engine.stop()
    await state.close()


async def test_a_run_is_settled_before_its_worker_has_cleaned_up(tmp_path, data, monkeypatch):
    """Review round 3, S2: a run reads settled while the cleanup task its
    commit made due is still under way; tests wait for that task, never for
    a while."""

    cleanup, go = FileStore.cleanup, asyncio.Event()

    async def held(self, *args, **kw):
        await go.wait()
        await cleanup(self, *args, **kw)

    monkeypatch.setattr(FileStore, "cleanup", held)
    project = scores_project()
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    await engine.run_until((await engine.submit(["scores"]))["id"], 20)
    go.set()
    await worker_finished()
    go.clear()
    settled = await engine.run_until((await engine.submit(["scores"]))["id"], 20)
    assert settled["request"]["status"] == "succeeded"
    assert objects(data, "scores") != await named(state, "scores")  # the worker is still cleaning up
    go.set()
    await worker_finished()
    assert objects(data, "scores") == await named(state, "scores")


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
    """F36. The engine hands a cleanup task a pending delta entry in its spec;
    before `AttemptLaunched` lands (the spec and the control file are being
    written), the entry goes (an operator clears it). Were the attempt's
    claim to name the entry's files only from `AttemptLaunched` on, nothing
    would hold the delta file meanwhile: collection would delete it, and the
    worker read a file that is gone (simulation, seed 11, when the next
    attempt cleaned up: a `checks` delta file deleted 6 s before its reader
    came)."""

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return {"a": 1}

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, Project(assets=[scores]))
    await engine.initialize()
    await run(engine, ["scores"])
    m = state.model
    index = m.indexes[("scores", "")]
    real = await state.get_object(index.path(sorted(index.referenced())[0]))  # a real delta file's bytes
    path = f"{index.prefix}held.kx"
    await state.create_object(path, real)
    m.cleanups[("scores", "")] = [
        {"n": 1, "id": "1.0", "kind": "delta", "prefix": index.prefix, "files": ["held"]}
    ]
    m.garbage.append([path, 1])  # the index let go of it: the pending entry alone holds it

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
    await asyncio.wait_for(at_control.wait(), 20)
    assert specs[-1]["cleanup"]["outputs"]["scores"]["cleanup"][0]["files"] == ["held"]  # handed the entry
    del m.cleanups[("scores", "")]  # an operator cleared the entry meanwhile
    await engine.upkeep.collect()
    survived = await state.get_object(path) is not None
    go.set()
    await running
    await engine.stop()
    await state.close()
    assert survived, "collection deleted the delta file a launching cleanup task's spec hands it"
