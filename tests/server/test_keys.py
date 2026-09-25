"""Key indexes in the engine (docs/object-store-state.md §6): the harness works
out each commit's delta against the output's index, consumers read the delta
log, the engine truncates the log behind consumers, compacts, recounts, and
deletes files nothing references — and renames carry all of it along (§2)."""

import asyncio
import dataclasses
import random

import pytest
from solera.keys.index import Options
from solera.sdk import Incremental, Output, PartitionSet, Project, Ref, Source, asset
from solera.stores import JsonStore, Patch
from solera_server.engine import Engine
from solera_server.placements.inline import InlinePlacement
from solera_server.state import State


@pytest.fixture
async def state(tmp_path):
    opened = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    yield opened
    await opened.close()


def engine_for(state, project, **kw):
    return Engine(
        state,
        project.manifest,
        placements={"Local": lambda e, o, c: InlinePlacement(c, project)},
        clock=state.clock,
        eval_interval=0.01,
        **kw,
    )


async def run(engine, targets, **kw):
    detail = await engine.run_until((await engine.submit(targets, **kw))["id"], 60)
    assert detail["request"]["status"] == "succeeded", detail
    return detail


async def settle(engine):
    """Let background compactions finish, then tick so their results land."""

    for _ in range(20):
        await engine.tick()
        if not engine.maintaining:
            await engine.tick()
            if not engine.maintaining:
                return
        await asyncio.gather(*engine.maintaining.values(), return_exceptions=True)


def on_disk(state, index) -> set[str]:
    root = state.objects_url.removeprefix("file://")
    from pathlib import Path

    return {str(p.relative_to(root)) for p in (Path(root) / index.prefix).glob("*.kx")}


class CountingStore(JsonStore):
    def __init__(self):
        super().__init__()
        self.writes = 0

    async def store(self, write, prior, scope):
        self.writes += 1
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
    assert store.writes == 1 and first["batch"] == 0 and first["count"] == 2
    await run(engine, ["items"])
    await run(engine, ["items"], mode="full")  # a full run of identical content too
    assert store.writes == 1
    assert state.model.heads[("items", "")]["ref"] == first["ref"]
    assert state.model.heads[("items", "")]["batch"] == 0
    rows["v"] = [{"id": "a", "v": 2}]
    await run(engine, ["items"])
    head = state.model.heads[("items", "")]
    assert store.writes == 2 and head["batch"] == 1 and head["count"] == 1


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
    assert not state.model.garbage
    assert on_disk(state, index) == {index.path(n) for n in index.referenced()}
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
    state.model.indexes[key] = dataclasses.replace(state.model.indexes[key], count=3, count_exact=False)
    await settle(engine)
    assert state.model.indexes[key].count == 5 and state.model.indexes[key].count_exact
    assert state.model.heads[key]["count"] == 5


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

    class External(JsonStore):
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
    loaded = await project.stores["json"].load(ref, None, None)
    assert {r["id"]: r["v"] for r in loaded} == {"a": 1, "b": 2}
