"""Key indexes in the engine (docs/object-store-state.md §6): the harness works
out each commit's delta against the output's index, consumers read the delta
log, the engine truncates the log behind consumers, compacts, recounts, and
deletes files nothing references — and renames carry all of it along (§2)."""

import asyncio
import dataclasses
import random
import threading

import pytest
from solera._native import group_digest
from solera.keys import resolver
from solera.keys.index import DeltaFiles, IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO
from solera.sdk import Incremental, Output, PartitionSet, Project, Ref, Source, asset
from solera.stores import FileStore, Patch
from solera_server.engine import Engine
from solera_server.keyservice import KeyService
from solera_server.placements.inline import InlinePlacement
from solera_server.state import State

from tests.conftest import whole


@pytest.fixture
async def state(tmp_path):
    opened = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    yield opened
    await opened.close()


def engine_for(state, project, **kw):
    return Engine(
        state,
        project.manifest,
        placements={"Local": lambda s, c: InlinePlacement(c, project)},
        clock=state.clock,
        eval_interval=0.01,
        **kw,
    )


async def run(engine, targets, **kw):
    detail = await engine.run_until((await engine.submit(targets, **kw))["id"], 60)
    assert detail["request"]["status"] == "succeeded", str(detail)[-3000:]
    return detail


async def settle(engine):
    """Let background compactions finish, then tick so their results land."""

    upkeep = engine.upkeep
    for _ in range(20):
        await upkeep.tick()
        if not upkeep.jobs:
            await upkeep.tick()
            if not upkeep.jobs:
                return
        await asyncio.gather(*upkeep.jobs.values(), return_exceptions=True)


def on_disk(state, index) -> set[str]:
    root = state.objects_url.removeprefix("file://")
    from pathlib import Path

    return {str(p.relative_to(root)) for p in (Path(root) / index.prefix).glob("*.kx")}


class CountingStore(FileStore):
    def __init__(self):
        super().__init__()
        self.stored = 0

    async def store(self, write, prior, scope):
        self.stored += 1
        return await super().store(write, prior, scope)


async def test_unchanged_writes_skip_the_store(state):
    """§6: a write the index shows changes nothing is never stored; the head,
    its batch and its count stay as they are."""

    store = CountingStore()
    rows = {"v": [{"id": "a", "v": 1}, {"id": "b", "v": 1}]}

    @asset(outputs=Output("items", key="id", revision="v", store="counting"))
    def items():
        return rows["v"]

    project = Project(assets=[items], stores={"counting": store})
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["items"])
    first = dict(state.model.heads[("items", "")])
    assert store.stored == 1 and first["batch"] == 0 and first["count"] == 2
    await run(engine, ["items"])
    await run(engine, ["items"], mode="full")  # a full run of identical content too
    assert store.stored == 1
    assert state.model.heads[("items", "")]["ref"] == first["ref"]
    assert state.model.heads[("items", "")]["batch"] == 0
    rows["v"] = [{"id": "a", "v": 2}]
    await run(engine, ["items"])
    head = state.model.heads[("items", "")]
    assert store.stored == 2 and head["batch"] == 1 and head["count"] == 1


async def test_a_keyed_write_reaches_the_store_as_its_delta(state, data):
    """§6: the store is told which keys changed, and writes only those: a
    dict output whose `b` changed and `a` went touches two objects."""

    values = {"v": {"a": 1, "b": 1, "c": 1}}

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return values["v"]

    project = Project(assets=[scores])
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["scores"])
    before = {str(p.relative_to(data)) for p in (data / "scores").rglob("*.json")}
    values["v"] = {"b": 2, "c": 1}
    await run(engine, ["scores"])
    after = {str(p.relative_to(data)) for p in (data / "scores").rglob("*.json")}
    [new] = after - before  # one object written: `b`'s new version; `c` untouched, `a` dropped by the index
    assert new.startswith("scores/b/") and (data / new).read_text() == "2"
    ref = Ref.from_json(state.model.heads[("scores", "")]["ref"])
    assert await project.stores["default"].load(ref, None, await whole(state, "scores")) == {"b": 2, "c": 1}


async def test_row_digests_are_16_bytes_end_to_end(state):
    """§6: without a declared revision, a key's version is the 16-byte group
    digest of its rows (docs/row-digest.md): what the index stores, and what
    key listings show in hex."""

    rows = [{"id": "a", "n": 1}, {"id": "b", "n": 2}]

    @asset(outputs=Output("items", key="id"))
    def items():
        return rows

    engine = engine_for(state, Project(assets=[items]))
    await engine.initialize()
    await run(engine, ["items"])
    index = KeyIndex(ObjectIO(state.objects), None, state.model.indexes[("items", "")])
    keys, versions, _, _ = await index.page(None, 10)
    digests = [group_digest([r], "id") for r in rows]
    assert keys == [b"a", b"b"] and versions == digests
    assert all(len(v) == 16 for v in versions)
    listed = await engine.list_keys("items")
    assert listed["keys"] == {"a": digests[0].hex(), "b": digests[1].hex()}


@pytest.mark.parametrize("arrow", [False, True], ids=["rows", "arrow"])
async def test_a_patch_reconciles_what_a_dead_sql_writer_left(state, arrow):
    """docs/resolved-commits.md §3: a dead `Sql` writer's intent names no keys.
    It deleted `a` and inserted `b` and died before reporting; the next patch,
    of `c`, cannot read back "the intended keys" — it reconciles the whole
    store against the index, and the commit settles the intent. (An
    overwrite store: an immutable one never has such an intent.)"""

    from tests.server.test_fence import LiveStore

    live = LiveStore()
    pending = {"rows": [{"id": "a", "v": 1}]}

    @asset(outputs=Output("items", key="id", revision="v", store="live"))
    def items():
        if arrow:  # the same patch as an Arrow table: repair takes what `key_rows` takes
            import pyarrow as pa

            return Patch(pa.Table.from_pylist(pending["rows"]))
        return Patch(pending["rows"])

    engine = engine_for(state, Project(assets=[items], stores={"live": live}))
    await engine.initialize()
    await run(engine, ["items"])
    # What the dead writer did to the store, and the intent its gate left.
    del live.rows["a"]
    live.rows["b"] = {"id": "b", "v": 1}
    key = ("items", "")
    state.model.unsettled[key] = [
        {"added": 0, "removed": 0, "exact": True, "files": [], "unknown": True, "run": "r", "attempt": "dead"}
    ]
    pending["rows"] = [{"id": "c", "v": 1}]
    await run(engine, ["items"])
    assert key not in state.model.unsettled
    index = state.model.indexes[key]
    assert index.count == 2 and index.count_exact
    assert sorted((await engine.list_keys("items"))["keys"]) == ["b", "c"]
    assert sorted(live.rows) == ["b", "c"]


async def test_compaction_truncation_and_garbage(state):
    """§6: level 0 is compacted once it holds `l0_max_files` files; the delta
    log keeps only what the consumer's watermark still needs; files nothing
    references are deleted — and every delivery stays exact throughout."""

    rng = random.Random(7)
    truth: dict[str, int] = {}
    pending = {"rows": [], "remove": []}
    seen: dict[str, int] = {}

    @asset(outputs=Output("items", key="id", revision="v"))
    def items():
        return Patch(pending["rows"], remove=pending["remove"])

    @asset(inputs={"items": Incremental(batch_size=7)})
    def mirror(ctx, items: list):
        changes = ctx.changes["items"]
        if changes.full:
            seen.clear()
        for row in items:
            seen[row["id"]] = row["v"]
        for key in changes.deleted:
            seen.pop(key, None)
        return [{"n": len(items)}]

    project = Project(assets=[items, mirror])
    engine = engine_for(state, project, key_options=Options(l0_max_files=3))
    await engine.initialize()
    for step in range(40):
        rows, remove = {}, set()
        for _ in range(rng.randint(1, 6)):
            key = f"k{rng.randint(0, 30):02d}"
            if truth and rng.random() < 0.25:
                gone = rng.choice(sorted(truth))
                remove.add(gone)
                rows.pop(gone, None)
            elif key not in remove:
                rows[key] = rng.randint(0, 3)
        pending["rows"] = [{"id": k, "v": v} for k, v in rows.items()]
        pending["remove"] = sorted(remove)
        await run(engine, ["items"])
        for key in remove:
            truth.pop(key, None)
        truth.update(rows)
        if step % 5 == 4:
            await run(engine, ["mirror"])
            assert seen == truth
        await settle(engine)

    index = state.model.indexes[("items", "")]
    assert index.count == len(truth) and index.count_exact
    assert len(index.level(0)) < 3 and index.depth >= 1  # compacted
    head_batch = state.model.heads[("items", "")]["batch"]
    watermark = state.model.watermarks[("mirror", "items", "")]
    assert watermark["batch"] == head_batch + 1
    assert all(batch >= watermark["batch"] for batch, _ in index.log)  # truncated behind it
    read = state.model.discard_reads()  # kept for the discards still pending (docs/lifecycle.md §9.8)
    assert {path for path, _ in state.model.garbage} <= read
    assert on_disk(state, index) == {index.path(n) for n in index.referenced()} | read
    listed = await engine.list_keys("items")
    assert listed["keys"] == {k: str(v) for k, v in sorted(truth.items())}


async def test_recount_makes_an_approximate_count_exact(state):
    """§6: an index whose count drifted is recounted with a full scan."""

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": str(i)} for i in range(5)]

    project = Project(assets=[items])
    engine = engine_for(state, project, recount_interval=0)
    await engine.initialize()
    await run(engine, ["items"])
    key = ("items", "")
    state.model.indexes[key] = dataclasses.replace(state.model.indexes[key], count=3, inexact=1)
    await settle(engine)
    assert state.model.indexes[key].count == 5 and state.model.indexes[key].count_exact
    assert state.model.heads[key]["count"] == 5


async def test_a_commit_during_the_recount_keeps_it(state, monkeypatch):
    """§6: a recount is exact for the index it pinned; a commit landing while
    it runs adds its own `added - removed` on top, so a busy index still gets
    its exact count back."""

    rows = [{"id": str(i)} for i in range(5)]

    @asset(outputs=Output("items", key="id"))
    def items():
        return rows

    engine = engine_for(state, Project(assets=[items]), recount_interval=0)
    await engine.initialize()
    await run(engine, ["items"])
    key = ("items", "")
    state.model.indexes[key] = dataclasses.replace(state.model.indexes[key], count=3, inexact=1)

    # A recount still running when the next commit lands, as a large index's is.
    started, release = threading.Event(), threading.Event()
    recount, counted = KeyIndex.recount, []

    async def slow_recount(self):
        started.set()
        await asyncio.to_thread(release.wait, 10)
        counted.append(await recount(self))
        return counted[-1]

    monkeypatch.setattr(KeyIndex, "recount", slow_recount)
    await engine.upkeep.tick()
    assert engine.upkeep.jobs, "a recount should be running"
    await asyncio.to_thread(started.wait, 10)
    rows.append({"id": "new"})
    await run(engine, ["items"])  # an exact commit: +1
    assert state.model.indexes[key].count == 4 and not state.model.indexes[key].count_exact
    release.set()
    await asyncio.gather(*engine.upkeep.jobs.values())
    index = state.model.indexes[key]
    assert counted == [5] and engine.upkeep.last_error is None  # one recount, of the pinned 5 keys
    assert index.count == 6 == len(rows) and index.count_exact
    assert state.model.heads[key]["count"] == 6


def test_a_recount_stays_inexact_if_a_later_commit_was():
    pinned = IndexState(count=10, inexact=2)
    later = pinned.committed(0, DeltaFiles([], 3, 0, True), keep_log=False)
    later = later.committed(1, DeltaFiles([], 1, 0, False), keep_log=False)
    assert (later.count, later.inexact) == (14, 3)
    recounted = later.recounted(9, pinned.count, pinned.inexact)
    assert (recounted.count, recounted.inexact, recounted.count_exact) == (13, 1, False)
    assert later.recounted(9, later.count, later.inexact).count_exact


async def test_a_consumer_without_a_log_starts_over(state):
    """§6: a watermark whose window the log no longer holds gets a full delivery."""

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a"}, {"id": "b"}]

    deliveries = []

    @asset(inputs={"items": Incremental()})
    def mirror(ctx, items: list):
        deliveries.append((ctx.changes["items"].full, sorted(r["id"] for r in items)))
        return []

    project = Project(assets=[items, mirror])
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["mirror"], upstream=True)
    engine.upkeep.truncate()
    wm = state.model.watermarks[("mirror", "items", "")]
    state.model.watermarks[("mirror", "items", "")] = {**wm, "batch": 0}  # behind the (empty) log
    await run(engine, ["mirror"])
    assert deliveries == [(True, ["a", "b"]), (True, ["a", "b"])]


async def test_keyed_source_commits_go_through_the_index(state):
    """§6: a source commit becomes a delta file; a full map replaces the
    content, deleting what it omits; identical content is not a change."""

    got = []

    @asset(inputs={"uploads": Incremental()})
    def ingest(ctx, uploads: list):
        got.append((sorted(ctx.changes["uploads"].upserted), sorted(ctx.changes["uploads"].deleted)))
        return []

    class External(FileStore):
        """The source's data lives elsewhere; loads answer from the selection."""

        async def load(self, ref, t, selection):
            return [{"id": k} for k in selection.revisions]

    project = Project(
        assets=[ingest], sources=[Source("uploads", key="id", store="ext")], stores={"ext": External()}
    )
    engine = engine_for(state, project)
    await engine.initialize()
    assert (await engine.commit_source("uploads", keys={"a": "1", "b": "1"}))["changed"]
    await run(engine, ["ingest"])
    assert (await engine.commit_source("uploads", keys={"b": "2", "c": "1"}))["changed"]
    assert not (await engine.commit_source("uploads", upsert={"c": "1"}))["changed"]
    await run(engine, ["ingest"])
    assert got == [(["a", "b"], []), (["b", "c"], ["a"])]
    head = state.model.heads[("uploads", "")]
    assert head["batch"] == 1 and head["count"] == 2


async def test_partition_set_elements_ride_on_the_head(state):
    @asset(outputs=PartitionSet("sites"))
    def sites():
        return Patch(["east"])

    project = Project(assets=[sites])
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["sites"])
    await run(engine, ["sites"])  # the same element again: nothing changes
    head = state.model.heads[("sites", "")]
    assert head["elements"] == ["east"] and head["count"] == 1 and head["batch"] == 0


async def test_key_cache_holds_index_files(state, tmp_path):
    """§6: attempts and the engine share a disk cache of index files."""

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a"}]

    project = Project(assets=[items])
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["items"])
    cached = list((tmp_path / ".key-cache" / "test").iterdir())
    assert any(p.name.endswith(".kx") for p in cached)


async def test_renamed_asset_keeps_its_state(state):
    """§2: `aliases=` moves heads, key indexes, cursors, watermarks and
    automation state to the new name; a consumer continues incrementally."""

    delivered = []
    rows = {"v": [{"id": "a", "v": 1}, {"id": "b", "v": 1}]}

    def mirror(ctx, feed: list):
        delivered.append((ctx.changes["feed"].full, sorted(r["id"] for r in feed)))
        return []

    def feed():
        return rows["v"]

    def source_feed():
        return rows["v"]

    before_project = Project(
        assets=[
            asset(outputs=Output(key="id", revision="v"))(feed),
            asset(inputs={"feed": Incremental()})(mirror),
        ]
    )
    engine = engine_for(state, before_project)
    await engine.initialize()
    await run(engine, ["mirror"], upstream=True)
    before = state.model.heads[("feed", "")]

    project = Project(
        assets=[
            asset(outputs=Output(key="id", revision="v"), aliases=["feed"])(source_feed),
            asset(inputs={"feed": Incremental("source_feed")})(mirror),
        ]
    )
    engine = engine_for(state, project)
    await engine.initialize()
    m = state.model
    assert ("feed", "") not in m.heads and m.heads[("source_feed", "")]["ref"] == before["ref"]
    assert m.heads[("source_feed", "")]["asset"] == "source_feed"
    assert m.indexes[("source_feed", "")].prefix == "keys/feed/_/"  # files stay where they are
    assert m.watermarks[("mirror", "feed", "")]["output"] == "source_feed"
    rows["v"] = [{"id": "a", "v": 1}, {"id": "b", "v": 2}]
    await run(engine, ["mirror"], upstream=True)
    assert m.heads[("source_feed", "")]["batch"] == 1
    assert delivered == [(True, ["a", "b"]), (False, ["b"])]  # only the change, not everything
    ref = Ref.from_json(m.heads[("source_feed", "")]["ref"])
    loaded = await project.stores["default"].load(ref, None, await whole(state, "source_feed"))
    assert {r["id"]: r["v"] for r in loaded} == {"a": 1, "b": 2}


async def test_small_writes_resolve_in_the_engine_and_pages_come_inline(state, monkeypatch):
    """docs/resolved-commits.md §4, §7: once the engine's cache holds an index,
    a small patch's delta comes from the engine — the worker reads no index
    file — and a consumer's pending page comes inline in its spec."""

    from solera.keys.io import ObjectIO as IO

    pending = {"rows": [{"id": f"k{i}", "v": 1} for i in range(50)]}
    seen: dict[str, int] = {}

    @asset(outputs=Output("items", key="id", revision="v"))
    def items():
        return Patch(pending["rows"])

    @asset(inputs={"items": Incremental(batch_size=100)})
    def mirror(ctx, items: list):
        for row in items:
            seen[row["id"]] = row["v"]
        for key in ctx.changes["items"].deleted:
            seen.pop(key, None)
        return [{"n": len(items)}]

    answers, reads = [], []
    real_resolve = KeyService.resolve

    async def resolve(self, attempt, body, prepared, live, position):
        out = await real_resolve(self, attempt, body, prepared, live, position)
        answers.append(resolver.answers(out)["items"][0]["result"] if out else None)
        return out

    monkeypatch.setattr(KeyService, "resolve", resolve)
    inlined, real_inline = [], KeyService.inline

    def inline(self, *args):
        out = real_inline(self, *args)
        inlined.append(out is not None)
        return out

    monkeypatch.setattr(KeyService, "inline", inline)
    engine = engine_for(state, Project(assets=[items, mirror]))
    await engine.initialize()
    await run(engine, ["mirror"], upstream=True)
    for n in range(4):
        pending["rows"] = [{"id": f"k{i}", "v": n + 2} for i in range(n, 50, 7)]
        await settle(engine)
        real_read = IO.read

        async def read(self, path, start, end, size, real_read=real_read):
            reads.append(path)
            return await real_read(self, path, start, end, size)

        monkeypatch.setattr(IO, "read", read)
        reads.clear()  # the last round's: once warm, nothing reads an index file
        await run(engine, ["items"])
        await run(engine, ["mirror"])
        monkeypatch.setattr(IO, "read", real_read)
        truth = {k: v for k, (v, _) in (await _listed(engine)).items()}
        assert seen == truth
    assert set(answers[-3:]) == {"delta"}  # warm: the engine answers
    assert inlined[-3:] == [True] * 3  # and the consumer's pages come inline
    assert not [p for p in reads if p.endswith(".kx")]  # so nothing reads an index file
    await engine.keys.stop()


async def _listed(engine):
    listed = await engine.list_keys("items")
    return {k: (int(v), 0) for k, v in listed["keys"].items()}


@pytest.mark.parametrize("patch", [False, True], ids=["replace", "patch"])
async def test_too_many_changes_to_list_still_write_only_what_the_delta_names(
    state, data, monkeypatch, patch
):
    """docs/lifecycle.md §9.8: an immutable store's every object must be named
    by an index entry, or nothing collects it. Past what the worker lists
    (`LISTED`, here 0), the store pages the changed keys from the delta files:
    an unchanged key gets no new object under a generation no entry names."""

    from solera_worker import worker

    monkeypatch.setattr(worker, "LISTED", 0)
    rows = {"a": 1, "b": 1}

    @asset(outputs=Output("items", key="id", revision="v"))
    def items():
        content = [{"id": k, "v": v} for k, v in rows.items()]
        return Patch(content) if patch else content

    engine = engine_for(state, Project(assets=[items]))
    await engine.initialize()
    await run(engine, ["items"])
    rows["b"] = 2
    await run(engine, ["items"])
    objects = {p.parent.name: [] for p in data.rglob("*.json") if "items" in p.parts}
    for p in data.rglob("*.json"):
        if "items" in p.parts:
            objects[p.parent.name].append(p.name)
    assert len(objects["a"]) == 1, objects  # unchanged: its first object only
    assert len(objects["b"]) == 2, objects  # changed: a new one, the old one until collected


async def test_a_byte_valued_key_fails_the_write_instead_of_vanishing(state):
    """A key is a `str` or an `int`, by one rule for the index and the store.
    A `bytes` key once committed into the index while the store, naming it
    differently, wrote nothing; now the write fails and nothing changes."""

    pending = {"rows": [{"id": "a", "n": 1}]}

    @asset(outputs=Output("items", key="id"))
    def items():
        return Patch(pending["rows"])

    engine = engine_for(state, Project(assets=[items]))
    await engine.initialize()
    await run(engine, ["items"])
    pending["rows"] = [{"id": b"b", "n": 2}]
    detail = await engine.run_until((await engine.submit(["items"]))["id"], 60)
    assert detail["request"]["status"] == "failed"
    assert "a key must be a str or an int" in detail["tasks"][0]["error"]
    assert sorted((await engine.list_keys("items"))["keys"]) == ["a"]


@pytest.mark.parametrize("cache", [True, False], ids=["engine", "no-cache"])
async def test_a_key_removed_twice_is_removed_once(state, cache):
    """A patch naming a removal twice is normalized once, before either the
    engine's request or the worker's own resolve is built."""

    pending = {"value": [{"id": "a", "v": 1}, {"id": "b", "v": 1}]}

    @asset(outputs=Output("items", key="id", revision="v"))
    def items():
        return pending["value"]

    engine = engine_for(state, Project(assets=[items]), resolve_cache=cache)
    await engine.initialize()
    await run(engine, ["items"])
    pending["value"] = Patch([], remove=["a", "a"])
    await run(engine, ["items"])
    assert sorted((await engine.list_keys("items"))["keys"]) == ["b"]
    assert state.model.indexes[("items", "")].count == 1
    if engine.keys is not None:
        await engine.keys.stop()


async def test_collection_waits_for_the_engines_own_readers(state):
    """Review 8: a fill of the engine's cache is a reader pin like an
    attempt's — collection deletes nothing it may still read."""

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a"}]

    engine = engine_for(state, Project(assets=[items]))
    await engine.initialize()
    await run(engine, ["items"])
    path = "keys/items/_/gone.kx"
    await state.put_object(path, b"x")
    state.model.garbage.append([path, state.model.applied])

    class Reading:
        def floor(self):
            return state.model.applied - 1  # a fill that read the index before the file was let go of

    engine.upkeep.keys, keys = Reading(), engine.upkeep.keys
    await engine.upkeep.collect()
    assert await state.get_object(path) == b"x"
    engine.upkeep.keys = keys
    await engine.upkeep.collect()
    assert await state.get_object(path) is None
    if engine.keys is not None:
        await engine.keys.stop()


async def test_an_empty_replacement_of_a_big_index_clears_it(state, monkeypatch):
    """Clearing a keyed output whose index is past what the engine resolves
    (`RESOLVE_ENTRIES`, here 1) streams an empty replacement: every key goes."""

    from solera_worker import worker

    monkeypatch.setattr(worker, "RESOLVE_ENTRIES", 1)
    rows = [{"id": "a", "n": 1}, {"id": "b", "n": 2}]

    @asset(outputs=Output("items", key="id"))
    def items():
        return list(rows)

    engine = engine_for(state, Project(assets=[items]))
    await engine.initialize()
    await run(engine, ["items"])
    rows.clear()
    await run(engine, ["items"])
    assert (await engine.list_keys("items"))["keys"] == {}


async def test_a_whole_keyed_read_is_loaded_a_page_at_a_time(state, monkeypatch):
    """A whole read of an immutable store's keyed output names its objects
    from the pinned index, a page at a time (`REPAIR_PAGE`, here 2), and puts
    the pages together: never one selection of every key."""

    from solera_worker import worker

    monkeypatch.setattr(worker, "REPAIR_PAGE", 2)
    seen = {}

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": f"k{i}", "n": i} for i in range(5)]

    @asset(outputs=Output("by_value", keyed=True))
    def by_value():
        return {f"v{i}": i for i in range(5)}

    @asset
    def reader(items: list[dict], by_value: dict):
        seen["items"], seen["by_value"] = items, by_value

    engine = engine_for(state, Project(assets=[items, by_value, reader]))
    await engine.initialize()
    await run(engine, ["items", "by_value", "reader"])
    assert sorted(r["n"] for r in seen["items"]) == list(range(5))
    assert seen["by_value"] == {f"v{i}": i for i in range(5)}
