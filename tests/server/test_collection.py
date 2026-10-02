"""Collection of immutable stores' data (docs/lifecycle.md §9.8): what a
commit, a compaction or an abandoned attempt let go of is discarded by the
scope's next attempt, once no reader can still need it."""

import asyncio
import random

from solera.keys.index import Options
from solera.sdk import Incremental, Output, Project, Retry, asset
from solera.stores import FileStore, Patch
from solera_server.engine import Engine
from solera_server.placements.inline import InlinePlacement
from solera_server.state import State

from tests.conftest import whole, worker_finished


def engine_for(state, project, **kw):
    placements = {"Local": lambda s, c: InlinePlacement(c, project)}
    return Engine(state, project.manifest, placements=placements, clock=state.clock, eval_interval=0.02, **kw)


async def run(engine, targets):
    """A run settled, and its workers done: their discards after the commit too."""

    detail = await engine.run_until((await engine.submit(targets))["id"], 20)
    assert detail["request"]["status"] == "succeeded", [t.get("error") for t in detail["tasks"]]
    await worker_finished()
    return detail


def objects(data, output) -> set[str]:
    return {str(p.relative_to(data)).rsplit(".", 1)[0] for p in (data / output).rglob("*") if p.is_file()}


async def named(state, output) -> set[str]:
    """The objects the key index names: exactly what a keyed output should hold."""

    keys = await whole(state, output)
    return {FileStore.key_name(output, k, v, loc) for k, (v, loc) in keys.revisions.items()}


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
    assert ("scores", "") not in state.model.discards
    await engine.stop()
    await state.close()


async def test_an_abandoned_attempts_objects_go(tmp_path, data, monkeypatch):
    """An attempt dies after its first object landed: everything it wrote
    carries its generation, and its delta file names it all. The retry, the
    scope's next attempt, discards it and its delta file."""

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
    assert first["status"] == "failed"
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

    @asset(inputs={"items": Incremental(page_size=5)})
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
        compacted += sum(1 for d in state.model.discards.get(("items", ""), []) if d["kind"] == "sidecar")
    assert compacted, "no compaction listed garbage"
    pending["rows"] = {}
    for _ in range(4):  # unchanged runs: each collects what is due
        await run(engine, ["items"])
    assert objects(data, "items") == await named(state, "items")
    await engine.upkeep.collect()
    sidecars = [
        p for p in await state.list_objects(state.model.indexes[("items", "")].prefix) if p.endswith(".kg")
    ]
    assert sidecars == []  # each went once its entries were discarded
    await engine.stop()
    await state.close()


async def test_a_reader_pin_holds_collection_back(tmp_path):
    """Data garbage is due only once every reader pin has passed it: the
    claims of attempts in flight, and a delta window paged over attempts."""

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return {"a": 1}

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    m = state.model
    m.discards[("scores", "")] = [{"n": 10, "id": "10.0", "kind": "items", "items": [["path", "x"]]}]
    m.claims["reader"] = {"attempt": "r", "pin": 9, "started_at": 0, "status": "running"}
    assert engine._due_discards("scores", "", "me") == []
    m.claims["reader"]["pin"] = 10
    m.watermarks[("c", "e", "")] = {
        "kind": "keys",
        "output": "scores",
        "up": "",
        "next": 0,
        "delivery": {"mode": "delta", "from": 0, "to": 1, "at": "k", "page": 1, "pages": 2, "pin": 8},
    }
    assert engine._due_discards("scores", "", "me") == []
    del m.watermarks[("c", "e", "")]
    assert [d["n"] for d in engine._due_discards("scores", "", "me")] == [10]
    assert [d["n"] for d in engine._due_discards("scores", "", "r")] == [10]  # its own claim reads none of it
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
    m.discards[("scores", "")] = [{"n": 1, "id": "1.0", "kind": "delta", "prefix": prefix, "files": ["held"]}]
    m.garbage.append([path, 1])  # the index let go of it
    await engine.upkeep.collect()
    assert await state.get_object(path) == b"delta" and [path, 1] in m.garbage
    m.discards[("scores", "")][0]["files"] = ["gone"]  # its names cannot be read
    await run(engine, ["scores"])
    assert [d["files"] for d in m.discards[("scores", "")]] == [["gone"]]  # still pending
    del m.discards[("scores", "")]
    await engine.upkeep.collect()
    assert await state.get_object(path) is None
    await engine.stop()
    await state.close()


async def test_an_entry_whose_names_cannot_be_read_gets_stuck_and_is_shown(tmp_path):
    """After three attempts that could not read an entry's names, it is
    stuck: no longer handed out, so it takes no attempt's slot; shown in
    diagnostics and on the scope's head, until an operator clears it."""

    import httpx
    from solera_server.api import create_app

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return {"a": 1}

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    engine._due_after = no_discards_after_commit  # the next attempts' path alone
    await run(engine, ["scores"])
    m = state.model
    prefix = m.indexes[("scores", "")].prefix
    m.discards[("scores", "")] = [{"n": 1, "id": "1.0", "kind": "delta", "prefix": prefix, "files": ["gone"]}]
    for misses in (1, 2, 3):
        await run(engine, ["scores"])
        [entry] = m.discards[("scores", "")]
        assert entry["misses"] == misses and entry.get("stuck", False) == (misses == 3)
    assert engine._due_discards("scores", "", None) == []
    app = create_app(engine=engine, insecure=True)
    app.state.engine = engine
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/api/diagnostics")).json()["stuck_discards"] == [
            {"output": "scores", "scope": "", "id": "1.0"}
        ]
        base = f"/api/projects/{project.manifest['name']}"
        [head] = (await client.get(f"{base}/outputs/scores/heads")).json()["heads"]
        assert head["discards"]["pending"] == 0 and [e["id"] for e in head["discards"]["stuck"]] == ["1.0"]
        cleared = await client.post(f"{base}/scopes:clear-discards", json={"output": "scores", "by": "ops"})
        assert cleared.json()["cleared"] == ["1.0"]
    assert ("scores", "") not in m.discards
    await engine.stop()
    await state.close()


async def test_entries_of_one_event_are_acknowledged_one_by_one(tmp_path):
    """Astra review 2, P2-5: one commit lets go of several entries at one
    event position, and the delivery limit can split them. Acknowledging
    the delivered ones leaves the others queued; and a resolved sibling's
    acknowledgment does not erase an unresolved one's miss."""

    from solera_server import engine as engine_module

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return {"a": 1}

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    engine._due_after = no_discards_after_commit  # the next attempts' path alone
    await run(engine, ["scores"])
    m = state.model
    prefix = m.indexes[("scores", "")].prefix
    m.discards.pop(("scores", ""), None)  # the first commit's own
    m.applied += 1
    m._collect("scores", "", {"kind": "items", "items": [["path", "x"]]})
    m._collect("scores", "", {"kind": "delta", "prefix": prefix, "files": ["gone"]})
    m._collect("scores", "", {"kind": "items", "items": [["path", "y"]]})
    entries = m.discards[("scores", "")]
    assert len({d["n"] for d in entries}) == 1 and len({d["id"] for d in entries}) == 3
    engine_module.DISCARDS, limit = 1, engine_module.DISCARDS
    try:
        await run(engine, ["scores"])  # delivers the first alone
    finally:
        engine_module.DISCARDS = limit
    assert [d["kind"] for d in m.discards[("scores", "")]] == ["delta", "items"]
    await run(engine, ["scores"])  # the unresolved delta and its resolved sibling
    [left] = m.discards[("scores", "")]
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
    queued; the scope's next attempt discards them (a second delete of the
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
    await run(engine, ["scores"])  # supersedes `a`, cannot discard
    assert objects(data, "scores") != await named(state, "scores") and state.model.discards
    monkeypatch.setattr(LocalChannel, "finished", finished)

    async def gone(self, body):
        raise OSError("the worker died before it acknowledged")

    monkeypatch.setattr(LocalChannel, "discarded", gone)
    await run(engine, ["scores"])  # supersedes `a` again: discards it, cannot acknowledge
    assert objects(data, "scores") == await named(state, "scores") and state.model.discards
    monkeypatch.undo()
    await run(engine, ["scores"])  # deletes them again, harmlessly, and acknowledges
    assert ("scores", "") not in state.model.discards
    await engine.stop()
    await state.close()


async def test_a_reader_pin_at_commit_keeps_the_garbage_queued(tmp_path, data):
    """D8: a reader still pinned before the commit may read what it let go
    of: nothing is discarded after the commit; the entry stays queued."""

    project = scores_project()
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["scores"])
    with state.model.reading():  # a reader of the index as it is before the commit
        await run(engine, ["scores"])
        assert objects(data, "scores") != await named(state, "scores")
        assert [d["kind"] for d in state.model.discards[("scores", "")]] == ["delta"]
    await run(engine, ["scores"])
    assert objects(data, "scores") == await named(state, "scores")
    await engine.stop()
    await state.close()


async def test_a_run_is_settled_before_its_worker_has_discarded(tmp_path, data, monkeypatch):
    """Review round 3, S2: the worker discards after its commit, so a run
    reads settled while its discards are still under way; tests wait for
    the worker, never for a while."""

    discard, go = FileStore.discard, asyncio.Event()

    async def held(self, *args, **kw):
        await go.wait()
        await discard(self, *args, **kw)

    monkeypatch.setattr(FileStore, "discard", held)
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
    assert objects(data, "scores") != await named(state, "scores")  # the worker is still discarding
    go.set()
    await worker_finished()
    assert objects(data, "scores") == await named(state, "scores")


async def test_a_slow_reader_holds_back_only_what_it_reads(tmp_path):
    """Review round 2, system #6 and engine #8: pins are per output scope.
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
    m.applied += 1
    m.garbage += [[path, m.applied] for path in garbage]
    m.claims["t"] = {"attempt": "r", "pin": m.applied - 1, "domains": (mine,)}  # an attempt reading scores
    await engine.upkeep.collect()
    assert [g[0] for g in m.garbage] == [f"{mine}a.kx"]
    m.claims["t"]["domains"] = None  # still preparing: it may read anything
    m.garbage += [[f"{other}d.kx", m.applied]]
    await state.create_object(f"{other}d.kx", b"x")
    await engine.upkeep.collect()
    assert sorted(g[0] for g in m.garbage) == sorted([f"{mine}a.kx", f"{other}d.kx"])
    del m.claims["t"]
    with m.reading("history/"):  # a history query: index files are not its
        await engine.upkeep.collect()
    assert m.garbage == []
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
    assert claim["domains"] == (state.model.index("made", "").prefix,)
    await state.close()
