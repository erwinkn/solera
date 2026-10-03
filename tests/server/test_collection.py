"""Collection of immutable stores' data (docs/lifecycle.md §9.8): what a
commit, a compaction or an abandoned attempt let go of is cleaned up by the
partition's next attempt, once no reader can still need it."""

import asyncio
import random

from solera.keys.index import Options
from solera.sdk import Incremental, Output, Project, Retry, asset
from solera.stores import FileStore, Patch
from solera_server.engine import Engine
from solera_server.executors.inline import InlinePlacement
from solera_server.state import State

from tests.conftest import whole, worker_finished


def engine_for(state, project, **kw):
    placements = {"Local": lambda s, c: InlinePlacement(c, project)}
    return Engine(state, project.manifest, placements=placements, clock=state.clock, eval_interval=0.02, **kw)


async def run(engine, targets):
    """A run settled, and its workers done: their cleanups after the commit too."""

    detail = await engine.run_until((await engine.submit(targets))["id"], 20)
    assert detail["request"]["status"] == "succeeded", [t.get("error") for t in detail["tasks"]]
    await worker_finished()
    return detail


def objects(data, output) -> set[str]:
    return {str(p.relative_to(data)).rsplit(".", 1)[0] for p in (data / output).rglob("*") if p.is_file()}


async def named(state, output) -> set[str]:
    """The objects the key index names: exactly what a keyed output should hold."""

    keys = await whole(state, output)
    return {FileStore.key_name(output, k, g) for k, g in keys.generations.items()}


async def no_discards_after_commit(*args):
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
    carries its generation, and its delta file names it all. The retry, the
    partition's next attempt, cleanups it and its delta file."""

    values = [{"a": 1, "b": 1}, {"a": 2, "c": 1}, {"a": 2, "c": 1}, {"a": 2, "c": 1}]

    @asset(outputs=Output("scores", keyed=True), retries=Retry(1, delay=0))
    def scores():
        return values.pop(0)

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
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
    assert not (data / f"{puts[0]}.json").exists()  # the retry collected it
    await run(engine, ["scores"])  # and what the retry's commit superseded goes next
    assert objects(data, "scores") == await named(state, "scores")
    prefix = state.model.indexes[("scores", "")].prefix
    await engine.upkeep.collect()
    left = {p.rsplit("/", 1)[-1] for p in await state.list_objects(prefix)}
    assert not any(first["id"] in name for name in left)  # its delta file went too
    await engine.stop()
    await state.close()


async def test_compaction_garbage_is_collected(tmp_path, data):
    """Compaction drops shadowed entries — some never named as predecessors
    (a key the filters cleared) — and lists them; with everything collected,
    the store holds exactly what the index names."""

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
    engine = engine_for(state, project, key_options=Options(l0_max_files=3))
    await engine.initialize()
    compacted = 0
    for step in range(24):
        pending["rows"] = {f"k{rng.randrange(12)}": rng.randrange(4) for _ in range(4)}
        await run(engine, ["items"])
        if step % 6 == 5:
            await run(engine, ["mirror"])
        engine.upkeep.truncate()
        engine.upkeep.maintain()
        for job in list(engine.upkeep.jobs.values()):
            await job
        compacted += sum(1 for d in state.model.cleanups.get(("items", ""), []) if d["kind"] == "sidecar")
    assert compacted, "no compaction listed garbage"
    pending["rows"] = {}
    for _ in range(4):  # unchanged runs: each collects what is due
        await run(engine, ["items"])
    assert objects(data, "items") == await named(state, "items")
    await engine.upkeep.collect()
    sidecars = [
        p for p in await state.list_objects(state.model.indexes[("items", "")].prefix) if p.endswith(".kg")
    ]
    assert sidecars == []  # each went once its entries were cleaned up
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
    m.cleanups[("scores", "")] = [{"n": 10, "id": "10.0", "kind": "items", "items": [["path", "x"]]}]
    m.claims["reader"] = {"attempt": "r", "generation": 9, "started_at": 0, "status": "running"}
    assert engine._due_cleanups("scores", "", "me") == []
    m.claims["reader"]["generation"] = 10
    m._partition("c", "")["bookmarks"] = {
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


async def test_a_delta_a_pending_discard_reads_outlives_its_index(tmp_path, data):
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
    """After three attempts that could not read an entry's names, it is
    stuck: no longer handed out, so it takes no attempt's slot; shown in
    diagnostics and on the partition's head, until an operator clears it."""

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
    engine._due_after = no_discards_after_commit  # the next attempts' path alone
    await run(engine, ["scores"])
    m = state.model
    prefix = m.indexes[("scores", "")].prefix
    m.cleanups[("scores", "")] = [{"n": 1, "id": "1.0", "kind": "delta", "prefix": prefix, "files": ["gone"]}]
    for misses in (1, 2, 3):
        await run(engine, ["scores"])
        [entry] = m.cleanups[("scores", "")]
        assert entry["misses"] == misses and entry.get("stuck", False) == (misses == 3)
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
    event counter, and the pass limit can split them. Acknowledging
    the delivered ones leaves the others queued; and a resolved sibling's
    acknowledgment does not erase an unresolved one's miss."""

    from solera_server import engine as engine_module

    writes = iter([{"a": 1}])

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return next(writes, Patch({}))  # then nothing: no commit lets go of anything more

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    engine._due_after = no_discards_after_commit  # the next attempts' path alone
    await run(engine, ["scores"])
    m = state.model
    prefix = m.indexes[("scores", "")].prefix
    m.cleanups.pop(("scores", ""), None)  # the first commit's own
    m.event_counter += 1
    m._collect("scores", "", {"kind": "items", "items": [["path", "x"]]})
    m._collect("scores", "", {"kind": "delta", "prefix": prefix, "files": ["gone"]})
    m._collect("scores", "", {"kind": "items", "items": [["path", "y"]]})
    entries = m.cleanups[("scores", "")]
    assert len({d["n"] for d in entries}) == 1 and len({d["id"] for d in entries}) == 3
    engine_module.DISCARDS, limit = 1, engine_module.DISCARDS
    try:
        await run(engine, ["scores"])  # delivers the first alone
    finally:
        engine_module.DISCARDS = limit
    assert [d["kind"] for d in m.cleanups[("scores", "")]] == ["delta", "items"]
    await run(engine, ["scores"])  # the unresolved delta and its resolved sibling
    [left] = m.cleanups[("scores", "")]
    assert left["kind"] == "delta" and left["misses"] == 1
    await engine.stop()
    await state.close()


def scores_project():
    values = iter([{"a": 1, "b": 1}, {"a": 2, "b": 1}, {"a": 3, "b": 1}, {"a": 3, "b": 1}])

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return next(values)

    return Project(assets=[scores])


async def test_without_the_channel_the_next_attempt_discards(tmp_path, data, monkeypatch):
    """D8's fallbacks: the engine unreachable when the worker says it
    finished, or the worker gone before it acknowledges, leave the entries
    queued; the partition's next attempt cleanups them (a second delete of the
    same names is no harm)."""

    from solera_worker.channel import LocalChannel

    project = scores_project()
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["scores"])
    finished = LocalChannel.finished

    async def unreachable(self, body):
        await finished(self, body)
        raise OSError("connection reset")  # its answer is lost

    monkeypatch.setattr(LocalChannel, "finished", unreachable)
    await run(engine, ["scores"])  # supersedes `a`, cannot clean up
    assert objects(data, "scores") != await named(state, "scores") and state.model.cleanups
    monkeypatch.setattr(LocalChannel, "finished", finished)

    async def gone(self, body):
        raise OSError("the worker died before it acknowledged")

    monkeypatch.setattr(LocalChannel, "cleaned_up", gone)
    await run(engine, ["scores"])  # supersedes `a` again: cleanups it, cannot acknowledge
    assert objects(data, "scores") == await named(state, "scores") and state.model.cleanups
    monkeypatch.undo()
    await run(engine, ["scores"])  # deletes them again, harmlessly, and acknowledges
    assert ("scores", "") not in state.model.cleanups
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
    """Review round 3, S2: the worker cleanups after its commit, so a run
    reads settled while its cleanups are still under way; tests wait for
    the worker, never for a while."""

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
    for _ in range(100):
        await engine.tick()
        if state.model.pool:
            break
        await asyncio.sleep(0.02)
    [claim] = state.model.claims.values()
    assert claim["prefixes"] == (state.model.index("made", "").prefix,)
    await state.close()
