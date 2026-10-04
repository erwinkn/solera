"""Key indexes in the engine (docs/object-store-state.md §6): the worker works
out each commit's delta against the output's index, consumers read the delta
log, the engine truncates the log behind consumers, compacts, recounts, and
deletes files nothing references — and renames carry all of it along (§2)."""

import asyncio
import dataclasses
import random
import threading

import pytest
from solera.keys import resolver
from solera.keys.index import DeltaFiles, IndexState, KeyIndex, Options
from solera.sdk import DynamicPartitions, Incremental, Output, Project, Ref, Source, asset
from solera.stores import FileStore, Patch
from solera_server.engine import Engine
from solera_server.executors.inline import InlinePlacement
from solera_server.keyservice import KeyService
from solera_server.state import State

from tests.conftest import whole


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

    async def store(self, write, prior, context):
        self.stored += 1
        return await super().store(write, prior, context)


async def test_every_write_is_a_change_and_an_empty_patch_none(state):
    """docs/versions.md §1: writing a key changes it — the same rows written
    again are a new version, at the attempt's generation — while a patch
    of nothing is never stored, and the head, its commit number and its count stay."""

    store = CountingStore()
    rows = {"v": [{"id": "a", "v": 1}, {"id": "b", "v": 1}]}

    @asset(outputs=Output("items", key="id", store="counting"))
    def items():
        return rows["v"]

    project = Project(assets=[items], stores={"counting": store})
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["items"])
    first = dict(state.model.heads[("items", "")])
    assert store.stored == 1 and first["commit_number"] == 0 and first["count"] == 2
    await run(engine, ["items"])  # identical rows: written again
    second = state.model.heads[("items", "")]
    assert store.stored == 2 and second["commit_number"] == 1 and second["count"] == 2
    assert second["ref"]["generation"] > first["ref"]["generation"]
    rows["v"] = Patch([])
    await run(engine, ["items"])
    assert store.stored == 2 and state.model.heads[("items", "")]["commit_number"] == 1


async def test_a_keyed_write_reaches_the_store_as_its_delta(state, data):
    """§6: the store is told which keys the delta writes, and writes only
    those: a dict output that drops `a` writes `b` and `c` again — every
    written key is a change — and `a` goes with the index."""

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
    new = sorted(after - before)  # `b` and `c` written again; `a` dropped by the index
    assert [n.split("/")[1] for n in new] == ["b", "c"] and (data / new[0]).read_text() == "2"
    ref = Ref.from_json(state.model.heads[("scores", "")]["ref"])
    assert await project.stores["default"].load(ref, None, await whole(state, "scores")) == {"b": 2, "c": 1}


async def test_a_keys_version_is_the_generation_that_wrote_it(state):
    """docs/versions.md §1: a key's version is the generation of the write
    that last wrote it: what the index holds, what the head's ref carries,
    and what key listings show."""

    rows = {"v": [{"id": "a", "n": 1}, {"id": "b", "n": 2}]}

    @asset(outputs=Output("items", key="id"))
    def items():
        return Patch(rows["v"])

    engine = engine_for(state, Project(assets=[items]))
    await engine.initialize()
    await run(engine, ["items"])
    first = state.model.heads[("items", "")]["ref"]["generation"]
    assert first > 0
    assert (await engine.list_keys("items"))["keys"] == {"a": first, "b": first}
    rows["v"] = [{"id": "b", "n": 3}]
    await run(engine, ["items"])
    second = state.model.heads[("items", "")]["ref"]["generation"]
    assert second > first
    assert (await engine.list_keys("items"))["keys"] == {"a": first, "b": second}


@pytest.mark.parametrize("arrow", [False, True], ids=["rows", "arrow"])
async def test_a_patch_reconciles_what_a_dead_sql_writer_left(state, arrow):
    """docs/versions.md §5: a dead `Sql` writer's intent names no keys. It
    deleted `a` and inserted `b` and died before reporting; the next patch,
    of `c`, cannot know "the intended keys" — it takes every key the store
    holds, and the commit settles the intent. (A fenced store: an immutable
    one never has such an intent.)"""

    from tests.server.test_fence import LiveStore

    live = LiveStore()
    pending = {"rows": [{"id": "a", "v": 1}]}

    @asset(outputs=Output("items", key="id", store="live"))
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
    state.model.repairs[key] = [
        {"added": 0, "removed": 0, "exact": True, "files": [], "unknown": True, "run": "r", "attempt": "dead"}
    ]
    pending["rows"] = [{"id": "c", "v": 1}]
    await run(engine, ["items"])
    assert key not in state.model.repairs
    index = state.model.indexes[key]
    assert index.count == 2 and index.count_exact
    assert sorted((await engine.list_keys("items"))["keys"]) == ["b", "c"]
    assert sorted(live.rows) == ["b", "c"]


async def test_compaction_truncation_and_garbage(state):
    """§6: level 0 is compacted once it holds `l0_max_files` files; the delta
    log keeps only what the consumer's position still needs; files nothing
    references are deleted — and every pass stays exact throughout."""

    rng = random.Random(7)
    truth: dict[str, int] = {}
    pending = {"rows": [], "remove": []}
    seen: dict[str, int] = {}

    @asset(outputs=Output("items", key="id"))
    def items():
        return Patch(pending["rows"], remove=pending["remove"])

    @asset(inputs={"items": Incremental(batch_size=7)})
    def mirror(ctx, items: list):
        changes = ctx.batch["items"]
        if changes.full and changes.first:
            seen.clear()
        for row in items:
            seen[row["id"]] = row["v"]
        for key in changes.removed:
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
    head_commit = state.model.heads[("items", "")]["commit_number"]
    position = state.model.position("mirror", "items", "")
    assert position["next"] == head_commit + 1
    assert all(commit_number >= position["next"] for commit_number, _ in index.log)  # truncated behind it
    read = state.model.cleanup_reads()  # kept for the cleanups still pending (docs/lifecycle.md §9.8)
    assert {path for path, _ in state.model.garbage} <= read
    assert on_disk(state, index) == {index.path(n) for n in index.referenced()} | read
    assert sorted((await engine.list_keys("items"))["keys"]) == sorted(truth)
    assert await _stored(engine, state) == truth


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
    assert counted == [5] and not engine.failing  # one recount, of the pinned 5 keys
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
    """§6: a position whose delta the log no longer holds gets a full pass."""

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a"}, {"id": "b"}]

    deliveries = []

    @asset(inputs={"items": Incremental()})
    def mirror(ctx, items: list):
        deliveries.append((ctx.batch["items"].full, sorted(r["id"] for r in items)))
        return []

    project = Project(assets=[items, mirror])
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["mirror"], upstream=True)
    engine.upkeep.truncate()
    position = state.model.position("mirror", "items", "")
    state.model.partition("mirror", "")["positions"]["items"] = {
        **position,
        "next": 0,
    }  # behind the (empty) log
    await run(engine, ["mirror"])
    assert deliveries == [(True, ["a", "b"]), (True, ["a", "b"])]


async def test_keyed_source_commits_go_through_the_index(state):
    """§6: a source commit becomes a delta file; a full map replaces the
    content, deleting what it omits; a key at the version it holds is not
    a change (docs/versions.md §2)."""

    got = []

    @asset(inputs={"uploads": Incremental()})
    def ingest(ctx, uploads: list):
        got.append((sorted(ctx.batch["uploads"].upserted), sorted(ctx.batch["uploads"].removed)))
        return []

    class External(FileStore):
        """The source's data lives elsewhere; loads answer from the selection."""

        async def load(self, ref, t, selection):
            return [{"id": k} for k in selection.generations]

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
    assert head["commit_number"] == 1 and head["count"] == 2


async def test_partition_set_elements_ride_on_the_head(state):
    @asset(outputs=DynamicPartitions("sites"))
    def sites():
        return Patch(["east"])

    project = Project(assets=[sites])
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["sites"])
    await run(engine, ["sites"])  # the same element again: nothing changes
    head = state.model.heads[("sites", "")]
    assert head["partitions"] == ["east"] and head["count"] == 1 and head["commit_number"] == 0


async def test_only_the_engine_caches_index_files(state, tmp_path):
    """D6: workers read index files from the store; the one cache of them is
    the engine's, in local form, beside `file://` state."""

    from solera_server.keyservice import cache_root

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a"}]

    project = Project(assets=[items])
    assert "key_cache" not in project.manifest
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["items"])
    root = tmp_path / ".key-cache" / "test"
    assert cache_root(state.objects_url) == str(root)
    assert not [p for p in root.rglob("*") if p.name.endswith(".kx")]  # no worker copies


async def test_renamed_asset_keeps_its_state(state):
    """§2: `aliases=` moves heads, key indexes, cursors, positions and
    automation state to the new name; a consumer continues incrementally."""

    delivered = []
    rows = {"v": [{"id": "a", "v": 1}, {"id": "b", "v": 1}]}

    def mirror(ctx, feed: list):
        delivered.append((ctx.batch["feed"].full, sorted(r["id"] for r in feed)))
        return []

    def feed():
        return rows["v"]

    def source_feed():
        return rows["v"]

    before_project = Project(
        assets=[
            asset(outputs=Output(key="id"))(feed),
            asset(inputs={"feed": Incremental()})(mirror),
        ]
    )
    engine = engine_for(state, before_project)
    await engine.initialize()
    await run(engine, ["mirror"], upstream=True)
    before = state.model.heads[("feed", "")]

    project = Project(
        assets=[
            asset(outputs=Output(key="id"), aliases=["feed"])(source_feed),
            asset(inputs={"feed": Incremental("source_feed")})(mirror),
        ]
    )
    engine = engine_for(state, project)
    await engine.initialize()
    m = state.model
    assert ("feed", "") not in m.heads and m.heads[("source_feed", "")]["ref"] == before["ref"]
    assert m.heads[("source_feed", "")]["asset"] == "source_feed"
    assert m.indexes[("source_feed", "")].prefix == "keys/feed/_/"  # files stay where they are
    assert m.position("mirror", "feed", "")["output"] == "source_feed"
    rows["v"] = Patch([{"id": "b", "v": 2}])
    await run(engine, ["mirror"], upstream=True)
    assert m.heads[("source_feed", "")]["commit_number"] == 1
    assert delivered == [(True, ["a", "b"]), (False, ["b"])]  # only the change, not everything
    ref = Ref.from_json(m.heads[("source_feed", "")]["ref"])
    loaded = await project.stores["default"].load(ref, None, await whole(state, "source_feed"))
    assert {r["id"]: r["v"] for r in loaded} == {"a": 1, "b": 2}


async def test_small_writes_resolve_in_the_engine_and_batches_come_with_start(state, monkeypatch):
    """docs/resolved-commits.md §4, §7: once the engine's cache holds an index,
    a small patch's delta comes from the engine — the worker reads no index
    file — and a consumer's pending batch comes with its start reply."""

    from solera.keys.io import ObjectIO as IO

    pending = {"rows": [{"id": f"k{i}", "v": 1} for i in range(50)]}
    seen: dict[str, int] = {}

    @asset(outputs=Output("items", key="id"))
    def items():
        return Patch(pending["rows"])

    @asset(inputs={"items": Incremental(batch_size=100)})
    def mirror(ctx, items: list):
        for row in items:
            seen[row["id"]] = row["v"]
        for key in ctx.batch["items"].removed:
            seen.pop(key, None)
        return [{"n": len(items)}]

    answers, reads = [], []
    real_resolve = KeyService.resolve

    async def resolve(self, attempt, body, prepared, live, at):
        out = await real_resolve(self, attempt, body, prepared, live, at)
        answers.append(resolver.answers(out)["items"][0]["result"] if out else None)
        return out

    monkeypatch.setattr(KeyService, "resolve", resolve)
    served, real_reads = [], KeyService.reads

    async def reads_(self, spec, *args):
        out = await real_reads(self, spec, *args)
        if any("batch" in pin for pin in spec["inputs"].values()):  # a consumer's start
            served.append(out is not None)
        return out

    monkeypatch.setattr(KeyService, "reads", reads_)
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
        await _warm(engine, ("items", ""))  # its delta installed: what the next start reads
        await run(engine, ["mirror"])
        monkeypatch.setattr(IO, "read", real_read)
        assert seen == await _stored(engine, state)
    assert set(answers[-3:]) == {"delta"}  # warm: the engine answers
    assert served[-3:] == [True] * 3  # and the consumer's batches come with its start
    assert not [p for p in reads if p.endswith(".kx")]  # so nothing reads an index file
    await engine.keys.stop()


async def test_input_reads_come_from_the_engine_once_warm(state, monkeypatch):
    """docs/resolved-commits.md §7: once the engine's cache holds an index, a
    consumer's batches — a full pass, delta passes — come with its start
    reply, and its worker reads no index file to find
    them; what it delivers is what the store's batches would have."""

    from solera.keys.io import ObjectIO as IO

    rows = {"v": [{"id": f"k{i:03d}", "v": 1} for i in range(300)]}
    seen: dict[str, dict[str, int]] = {"mirror": {}, "copy": {}}

    @asset(outputs=Output("items", key="id"))
    def items():
        return Patch(rows["v"])

    def consumer(name):
        def fn(ctx, items: list):
            for row in items:
                seen[name][row["id"]] = row["v"]
            for key in ctx.batch["items"].removed:
                seen[name].pop(key, None)
            return [{"n": len(items)}]

        fn.__name__ = name
        return asset(inputs={"items": Incremental(batch_size=100)})(fn)

    served, real_reads = [], KeyService.reads

    async def reads(self, spec, at):
        out = await real_reads(self, spec, at)
        served.append(out is not None)
        return out

    monkeypatch.setattr(KeyService, "reads", reads)
    engine = engine_for(state, Project(assets=[items, consumer("mirror"), consumer("copy")]))
    await engine.initialize()
    await run(engine, ["mirror"], upstream=True)

    async def warm():
        await _warm(engine, ("items", ""))

    index_reads, real_read = [], IO.read

    async def read(self, path, start, end, size):
        index_reads.append(path)
        return await real_read(self, path, start, end, size)

    for n in range(3):
        rows["v"] = [{"id": f"k{i:03d}", "v": n + 2} for i in range(n, 300, 5)]
        await run(engine, ["items"])
        await warm()
        monkeypatch.setattr(IO, "read", read)
        served.clear()
        await run(engine, ["mirror"] if n < 2 else ["mirror", "copy"])  # copy: a full pass
        monkeypatch.setattr(IO, "read", real_read)
        assert served and all(served)
        truth = await _stored(engine, state)
        assert seen["mirror"] == truth
    assert seen["copy"] == truth
    assert not [p for p in index_reads if p.endswith(".kx")]  # every batch came with its start
    await engine.keys.stop()


async def _warm(engine, key):
    """The fill a cold read queued, and each commit's delta, installed."""

    for _ in range(3000):
        files = {f"{engine.m.indexes[key].prefix}{n}.kx" for n in engine.m.indexes[key].referenced()}
        if files <= set(engine.keys.cache.files):
            return
        await asyncio.wrap_future(
            engine.keys._submit(engine.keys.cache.fill(engine.keys.io, engine.m.indexes[key]))
        )
        await asyncio.sleep(0.02)
    raise AssertionError("the index never warmed")


async def _stored(engine, state):
    """`items` as its store holds it: each key's `v`."""

    ref = Ref.from_json(state.model.heads[("items", "")]["ref"])
    loaded = await FileStore().load(ref, None, await whole(state, "items"))
    return {r["id"]: r["v"] for r in loaded}


@pytest.mark.parametrize("patch", [False, True], ids=["replace", "patch"])
async def test_too_many_changes_to_list_still_write_only_what_the_delta_names(
    state, data, monkeypatch, patch
):
    """docs/lifecycle.md §9.8: an immutable store's every object must be named
    by an index entry, or nothing collects it. Past what the worker lists
    (`LISTED`, here 0), the store pages the written keys from the delta
    files: a key the delta does not write — one a patch leaves alone —
    gets no new object under a generation no entry names."""

    from solera_worker import worker

    monkeypatch.setattr(worker, "LISTED", 0)
    rows = {"a": 1, "b": 1}

    @asset(outputs=Output("items", key="id"))
    def items():
        content = [{"id": k, "v": v} for k, v in rows.items()]
        return Patch(content) if patch else content

    engine = engine_for(state, Project(assets=[items]))
    await engine.initialize()
    await run(engine, ["items"])
    rows["b"] = 2
    if patch:
        del rows["a"]  # the patch leaves `a` alone
    await run(engine, ["items"])
    if patch:
        rows["a"] = 1
    for _ in range(6000):  # the worker deletes what the commit superseded after it (§9.8), and says so
        if not engine.m.cleanups.get(("items", "")):
            break
        await asyncio.sleep(0.01)
    objects = {p.parent.name: [] for p in data.rglob("*.json") if "items" in p.parts}
    for p in data.rglob("*.json"):
        if "items" in p.parts:
            objects[p.parent.name].append(p.name)
    assert len(objects["a"]) == 1, objects  # its newest object only: written again, or left alone
    assert len(objects["b"]) == 1, objects  # written again; the old one went after the commit (D8)


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

    @asset(outputs=Output("items", key="id"))
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
    state.model.garbage.append([path, state.model.event_counter])

    class Reading:
        def floor(self):
            return state.model.event_counter - 1  # a fill that read the index before the file was let go of

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


async def test_load_intent_is_decided_once(state, monkeypatch):
    """Review round 4: registration decides what an input receives; its pin
    carries that, the worker loads by it, and the engine reads ahead for it
    alone — a `Ref` input pins no index and is read for by nobody."""

    seen, specs = {}, []

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": f"k{i}", "n": i} for i in range(5)]

    @asset
    def by_ref(items: Ref):
        seen["ref"] = items

    @asset
    def by_data(items: list[dict]):
        seen["data"] = items

    real = KeyService.reads

    async def reads(self, spec, at):
        specs.append(spec)
        return await real(self, spec, at)

    monkeypatch.setattr(KeyService, "reads", reads)
    project = Project(assets=[items, by_ref, by_data])
    inputs = project.manifest["assets"]
    assert inputs["by_ref"]["inputs"]["items"]["load"] == "ref"
    assert inputs["by_data"]["inputs"]["items"]["load"] == "data"
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["items", "by_ref", "by_data"])
    assert isinstance(seen["ref"], Ref) and sorted(r["n"] for r in seen["data"]) == list(range(5))
    pins = [s["inputs"]["items"] for s in specs if "items" in s["inputs"]]
    ref_pin = next(p for p in pins if p["load"] == "ref")
    data_pin = next(p for p in pins if p["load"] == "data")
    assert "index" not in ref_pin and "index" in data_pin  # an immutable store's whole read
    await engine.keys.stop()


async def test_a_listing_holds_its_index_files_through_collection(tmp_path, monkeypatch):
    """Engine review #3: a key listing captured the index, then compaction
    and collection run before it reads: its files must still be there."""

    import asyncio

    from solera.keys.index import KeyIndex
    from solera.sdk import Source
    from solera_server.engine import Engine

    project = Project(sources=[Source("uploads", key="id")])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = Engine(
        state, project.manifest, clock=state.clock, key_options=Options(l0_max_files=2), resolve_cache=False
    )
    await engine.initialize()
    for n in range(3):
        await engine.commit_source("uploads", upsert={f"k{n}": "1"})
    page, paused, go = KeyIndex.page, asyncio.Event(), asyncio.Event()

    async def held(self, *args, **kw):
        paused.set()
        await go.wait()
        return await page(self, *args, **kw)

    monkeypatch.setattr(KeyIndex, "page", held)
    listing = asyncio.create_task(engine.list_keys("uploads"))
    await paused.wait()
    engine.upkeep.maintain()
    for job in list(engine.upkeep.jobs.values()):
        await job
    assert state.model.garbage  # compaction let the listed files go
    await engine.upkeep.collect()
    go.set()
    assert set((await listing)["keys"]) == {"k0", "k1", "k2"}
    await engine.upkeep.collect()  # done reading: now they go
    assert not state.model.garbage
    await state.close()


async def test_a_key_given_no_rows_does_not_exist(state):
    """docs/per-key-processing.md §6: a key with zero rows does not exist. A
    by-key write gives `b` no rows: a replacement leaves it out, a patch
    removes it — from the index, so from every consumer and store."""

    value = {"v": {"a": [{"n": 1}], "b": []}}

    @asset(outputs=Output("items", key="id"))
    def items():
        return value["v"]

    engine = engine_for(state, Project(assets=[items]))
    await engine.initialize()
    await run(engine, ["items"])
    assert sorted((await engine.list_keys("items"))["keys"]) == ["a"]
    value["v"] = Patch({"b": [{"n": 2}]})
    await run(engine, ["items"])
    assert sorted((await engine.list_keys("items"))["keys"]) == ["a", "b"]
    value["v"] = Patch({"b": []})
    await run(engine, ["items"])
    assert sorted((await engine.list_keys("items"))["keys"]) == ["a"]
