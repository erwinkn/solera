"""Key indexes in the engine (docs/object-store-state.md §6): the worker works
out each commit's delta against the output's index as its span, consumers
read `changes` from their endpoints, the engine merges spans keeping those
endpoints and deletes files nothing references — and renames carry all of
it along (§2)."""

import asyncio
import contextlib
import inspect
import random
from collections import Counter
from dataclasses import replace

import pytest
from solera.keys import resolver
from solera.keys.index import KeyIndex, Options
from solera.sdk import DynamicPartitions, Incremental, Loaded, Output, Project, Ref, Source, asset, source
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
    """Let background merges finish, then tick so their results land."""

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

    from tests.server.remote import LiveStore

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
        {"added": 0, "removed": 0, "files": [], "unknown": True, "run": "r", "attempt": "dead"}
    ]
    pending["rows"] = [{"id": "c", "v": 1}]
    await run(engine, ["items"])
    assert key not in state.model.repairs
    index = state.model.indexes[key]
    assert index.count == 2
    assert sorted((await engine.list_keys("items"))["keys"]) == ["b", "c"]
    assert sorted(live.rows) == ["b", "c"]


async def test_merges_and_garbage(state):
    """§6: spans merge as the policy plans them, keeping the boundaries the
    consumer's position still reads from; files nothing references are
    deleted — and every pass stays exact throughout."""

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
    engine = engine_for(state, project, key_options=Options(window=2))
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
    assert index.count == len(truth)
    assert len(index.spans) < 10 and index.spans[0].b > 0  # merged, into the base too
    head_commit = state.model.heads[("items", "")]["commit_number"]
    rec = state.model.partition("mirror", "")["observed"]["items"]  # observed at the head
    assert rec["base"]["endpoint"] == head_commit and state.model.oldest_observed("items", "") == head_commit
    read = state.model.cleanup_reads()  # kept for the cleanups still pending (docs/lifecycle.md §9.8)
    assert {path for path, _ in state.model.garbage} <= read
    assert on_disk(state, index) == {index.path(n) for n in index.referenced()} | read
    assert sorted((await engine.list_keys("items"))["keys"]) == sorted(truth)
    assert await _stored(engine, state) == truth


def items_project(rows):
    @asset(outputs=Output("items", key="id"))
    def items():
        return list(rows)

    return Project(assets=[items])


async def test_a_merge_planned_against_another_life_or_files_publishes_nothing(state):
    """docs/key-index-design.md § Lifecycles: a merge publishes only if the
    index is still the life it was planned against and holds its inputs'
    files; otherwise its output is deleted, and the index is untouched."""

    rows = [{"id": "a"}]
    engine = engine_for(state, items_project(rows), key_options=Options(window=2))
    await engine.initialize()
    for n in range(4):
        rows.append({"id": f"k{n}"})
        await run(engine, ["items"])
    key = ("items", "")
    planned = state.model.indexes[key]
    endpoints = state.model.endpoints(*key)
    plan = KeyIndex(None, None, planned, engine.key_options).plan_merge(endpoints, lane="base")
    assert plan is not None
    first = planned.spans[0]
    for current in (
        replace(planned, life="another"),  # reset since: another life
        replace(planned, spans=(replace(first, files=()), *planned.spans[1:])),  # its inputs' files changed
    ):
        state.model.indexes[key] = current
        await engine.upkeep._merge(key, "base", planned, plan, endpoints)
        assert state.model.indexes[key] is current  # nothing published
        assert not any(
            p.rpartition("/")[2].startswith("m") for p in on_disk(state, planned)
        )  # its output went
    state.model.indexes[key] = planned


async def test_orphaned_merge_outputs_are_collected(state):
    """A merge output nothing names — its merge failed after uploading, or
    its engine stopped before publishing — is deleted by the orphan
    collector; a referenced file, and a running merge's output, are not."""

    rows = [{"id": "a"}, {"id": "b"}]
    engine = engine_for(state, items_project(rows))
    await engine.initialize()
    await run(engine, ["items"])
    index = state.model.indexes[("items", "")]
    epoch = state.journal.epoch
    orphan = index.path(f"m000000000000-000000000003-{epoch:06d}-01ORPHAN.0000")
    running = index.path(f"m000000000004-000000000006-{epoch:06d}-01RUNNING.0000")
    for path in (orphan, running):
        await state.put_object(path, b"not read")
    engine.upkeep._busy[(("items", ""), "tail")] = frozenset({(4, 4), (5, 6)})
    await engine.upkeep.collect_orphans()
    present = on_disk(state, index)
    assert orphan not in present and running in present
    assert {index.path(n) for n in index.referenced()} <= present
    engine.upkeep._busy.clear()


async def test_a_fenced_engine_never_collects_its_successors_merge_outputs(tmp_path):
    """A17: engine A lists `keys/`, pauses; B takes the namespace over and
    publishes a merge; A resumes and must not delete B's output, which its
    own model never heard of. A merge output carries its engine's epoch, one
    past its predecessor's: A deletes only its own epoch's or earlier ones'.
    B, later, reclaims what A left unpublished."""

    url = tmp_path.as_uri()
    a = await State.open(url, "test", flush_interval=0.001)
    engine_a = engine_for(a, items_project([{"id": "a"}]))
    await engine_a.initialize()
    await run(engine_a, ["items"])
    index = a.model.indexes[("items", "")]
    left = index.path(f"m000000000000-000000000000-{a.journal.epoch:06d}-01LEFT.0000")  # A's, unpublished
    await a.put_object(left, b"x")
    await engine_a.upkeep.tasks.close()  # A's tick stops here: it pauses

    b = await State.open(url, "test", flush_interval=0.001)  # takes over: A is fenced
    assert b.journal.epoch == a.journal.epoch + 1
    published = index.path(f"m000000000000-000000000000-{b.journal.epoch:06d}-01LIVE.0000")
    await b.put_object(published, b"x")  # as if B published it: A's model never hears of it

    engine_a.upkeep._orphans_at = float("-inf")
    await engine_a.upkeep.collect_orphans()  # A resumes
    assert published in on_disk(b, index)  # a later epoch's: never A's to judge
    assert left not in on_disk(b, index)  # its own, unpublished

    await b.put_object(left, b"x")
    engine_b = engine_for(b, items_project([{"id": "a"}]))
    await engine_b.initialize()
    await engine_b.upkeep.collect_orphans()
    assert left not in on_disk(b, index) and published not in on_disk(b, index)  # B's model names neither
    await engine_b.stop()
    with contextlib.suppress(Exception):
        await engine_a.stop()
    await b.close()


async def test_f40_a_zombies_orphan_collector_spares_the_serving_engines_spans(tmp_path):
    """F40 (spec/tla/Spans.tla, calibration orphans-state): engine A commits
    once, then B takes over and merges. A has not written since, so it does
    not know it is fenced; its next orphan collection lists the merge output
    B published, which A's model does not name. It must not delete it: the
    output is of B's epoch, later than A's."""

    url, rows = tmp_path.as_uri(), [{"id": "a"}]
    old_state = await State.open(url, "test", flush_interval=0.001)
    old = engine_for(old_state, items_project(rows))
    await old.initialize()
    await run(old, ["items"])
    state = await State.open(url, "test", flush_interval=0.001)  # B fences A's journal
    engine = engine_for(state, items_project(rows), key_options=Options(window=2))
    await engine.initialize()
    for n in range(4):
        rows.append({"id": f"k{n}"})
        await run(engine, ["items"])
    await settle(engine)
    index = state.model.indexes[("items", "")]
    assert any(n.startswith("m") for n in index.referenced())  # B merged
    old.upkeep._orphans_at = float("-inf")
    await old.upkeep.collect_orphans()  # A, by the state it last knew
    try:
        assert {index.path(n) for n in index.referenced()} <= on_disk(state, index)
    finally:
        for e, s in ((old, old_state), (engine, state)):
            with contextlib.suppress(Exception):
                await e.stop()
            with contextlib.suppress(Exception):
                await s.close()


async def test_a_fenced_engine_never_deletes_what_only_its_unflushed_merge_let_go_of(tmp_path):
    """Coordinator, on R1: engine A publishes merge M over output X in its
    model, but IndexMerged(M) is not durable yet; then B takes over, and
    B's state still names X. A must delete X neither as an orphan nor as
    garbage. Named is never judged from non-durable state alone: M put X on
    A's garbage (`Model._replace_index`), which the orphan collector counts
    as named, and garbage goes only after `State.durable()`, which a fenced
    journal refuses (F26)."""

    from solera_server.state import Unavailable

    url = tmp_path.as_uri()
    a = await State.open(url, "test", flush_interval=3600)  # flushes only when asked
    rows = [{"id": "a"}]
    engine_a = engine_for(a, items_project(rows), key_options=Options(window=2))
    await engine_a.initialize()
    for n in range(4):
        rows.append({"id": f"k{n}"})
        await run(engine_a, ["items"])
    await engine_a.upkeep.tasks.close()  # merges below are this test's own
    key = ("items", "")

    async def merge_all():
        index = a.model.indexes[key]
        endpoints = a.model.endpoints(*key)
        await engine_a.upkeep._merge(key, "base", index, (0, len(index.spans)), endpoints)

    await merge_all()
    await a.durable()
    x = {a.model.indexes[key].path(f.name) for f in a.model.indexes[key].files}  # X: merge outputs
    assert all(p.rpartition("/")[2].startswith("m") for p in x)
    rows.append({"id": "late"})
    await run(engine_a, ["items"])
    for cleanup in [r for r in engine_a.m.runs.values() if r["kind"] == "cleanup"]:
        await engine_a.run_until(cleanup["id"], 20)  # its launch would flush M below: done first
    await a.durable()
    await merge_all()  # M, over X: recorded, not flushed
    assert x <= {path for path, _ in a.model.garbage}

    b = await State.open(url, "test", flush_interval=0.001)  # takes over; A is fenced
    names = {b.model.indexes[key].path(f.name) for f in b.model.indexes[key].files}
    assert x <= names  # B's live index references X

    engine_a.upkeep._orphans_at = float("-inf")
    await engine_a.upkeep.collect_orphans()
    with contextlib.suppress(Unavailable):
        await engine_a.upkeep.collect()
    assert x <= on_disk(b, b.model.indexes[key])
    with contextlib.suppress(Exception):
        await engine_a.stop()
    await b.close()


async def test_a_merge_that_keeps_failing_stops_and_alarms_across_restarts(tmp_path, monkeypatch):
    """docs/key-index-design.md § The write bound: R = 3 uploads per input
    set; after the third, none published, the index stops merging, alarmed.
    A17 R8: the count is durable, recorded before each upload — a restart
    or a takeover does not give the same inputs three more — and keyed by
    the index's life, so a new life merges afresh."""

    url = tmp_path.as_uri()
    a = await State.open(url, "test", flush_interval=0.001)
    rows = [{"id": "a"}]
    engine = engine_for(a, items_project(rows), key_options=Options(window=2))
    await engine.initialize()
    for n in range(4):
        rows.append({"id": f"k{n}"})
        await run(engine, ["items"])
    await engine.upkeep.tasks.close()  # the rounds below are this test's own
    calls = []

    async def broken(self, plan, endpoints, **_):
        calls.append(plan)
        raise RuntimeError("the store is down")

    monkeypatch.setattr(KeyIndex, "merge", broken)
    key = ("items", "")

    async def rounds(upkeep, n=6):
        for _ in range(n):
            upkeep._checked.clear()
            upkeep.maintain()
            await asyncio.gather(*upkeep.jobs.values(), return_exceptions=True)

    await rounds(engine.upkeep)
    assert max(Counter(calls).values()) == 3  # an input set, three times; then the index stopped
    assert "merges no more" in engine.upkeep.failing["key index items/ merges"]
    tried = len(calls)
    await engine.stop()
    await a.close()

    b = await State.open(url, "test", flush_interval=0.001)  # a restart: another engine
    again = engine_for(b, items_project(rows), key_options=Options(window=2))
    await again.initialize()
    await again.upkeep.tasks.close()
    await rounds(again.upkeep)
    assert len(calls) == tried  # nothing more: the budget is spent in this life
    assert "merges no more" in again.upkeep.failing["key index items/ merges"]
    index = b.model.indexes[key]
    b.model.indexes[key] = replace(index, life="another")  # a reset since: a new life
    await rounds(again.upkeep, 1)
    assert len(calls) > tried  # it merges afresh
    await again.stop()
    await b.close()


async def test_a_span_rewrite_that_drops_too_little_is_remembered(state, monkeypatch):
    """A17 R7: a rewrite found to drop too little is counted without
    uploading, remembered durably (`MergeRejected`), and not tried again
    over unrelated commits, which leave its files and endpoints as they are."""

    rows = [{"id": "a"}]
    engine = engine_for(state, items_project(rows))
    await engine.initialize()
    await run(engine, ["items"])
    await engine.upkeep.tasks.close()
    key = ("items", "")
    dry = []
    real = KeyIndex.drops_enough

    async def counted(self, plan, endpoints):
        dry.append(plan)
        return await real(self, plan, endpoints)

    monkeypatch.setattr(KeyIndex, "drops_enough", counted)

    def plan(self, endpoints, *, lane="any", busy=frozenset(), rejected=frozenset()):
        if lane == "base" or KeyIndex.rewrite_key(self.state.spans[-1], endpoints) in rejected:
            return None
        return len(self.state.spans) - 1, 1  # the newest span, rewritten alone

    monkeypatch.setattr(KeyIndex, "plan_merge", plan)
    for n in range(3):
        rows.append({"id": f"k{n}"})
        engine.upkeep._checked.clear()
        engine.upkeep.maintain()
        await asyncio.gather(*engine.upkeep.jobs.values())
        if n < 2:
            await run(engine, ["items"])
    rec = state.model.merge_record(key, state.model.indexes[key].life)
    assert rec["rejected"] and not rec["attempts"]  # remembered, never uploaded
    assert len(dry) == len(set(dry)) == len(rec["rejected"])  # each tail checked once


async def test_writes_wait_while_an_outputs_merges_are_far_behind(state):
    """docs/key-index-design.md § Limits: an index at twice the span cap —
    upkeep stopped, or far behind — holds back the attempts that write it,
    until merges bring it down."""

    from solera.keys.index import Span

    rows = [{"id": "a"}]
    engine = engine_for(state, items_project(rows))
    await engine.initialize()
    await run(engine, ["items"])
    key = ("items", "")
    index = state.model.indexes[key]
    head = index.head
    extra = tuple(Span(c, c, ((c, 10**6 + c),), ()) for c in range(head + 1, head + 1 + 2 * Options().fan_in))
    state.model.indexes[key] = replace(index, spans=index.spans + extra)
    request = await engine.submit(["items"])
    engine._dispatch_due()
    (task,) = state.model.runs[request["id"]]["tasks"].values()
    assert task["held"] == ["merges", "items"] and task["status"] == "queued"
    state.model.indexes[key] = index  # merged back down
    engine._dispatch_due()
    assert task["id"] in state.model.claims


async def test_every_index_writer_waits_while_merges_are_far_behind(state, tmp_path):
    """A17 R6: the writer backpressure holds for every key index, not only
    an asset's declared outputs. At twice the span cap a source commit is
    refused, retryable, and a per-key asset's task waits while its failure
    index (`@asset`) is that far behind; both go on once merges catch up."""

    from solera.keys.index import IndexState, Span

    @asset(inputs={"row": Incremental("uploads", each=True)}, outputs=Output("checked", key="id"))
    async def checked(ctx, row: list):
        return [{"ok": True}]

    @source(key="id")
    async def uploads(keys, ctx):
        return {k: Loaded({"id": k}, version="1") for k in keys}

    project = Project(assets=[checked], sources=[uploads], default_store=FileStore(tmp_path / "out"))
    engine = engine_for(state, project)
    await engine.initialize()
    await engine.commit_source("uploads", upsert=["a"])
    deep = tuple(Span(c, c, ((c, 10**6 + c),), ()) for c in range(1, 1 + 2 * Options().fan_in))

    index = state.model.indexes[("uploads", "")]
    state.model.indexes[("uploads", "")] = replace(index, spans=index.spans + deep)
    with pytest.raises(Engine.Conflict, match="far behind on merges"):
        await engine.commit_source("uploads", upsert=["b"])
    state.model.indexes[("uploads", "")] = index  # merged back down
    assert (await engine.commit_source("uploads", upsert=["b"]))["changed"]

    state.model.indexes[("@checked", "")] = IndexState(spans=deep, prefix="failures/")
    request = await engine.submit(["checked"])
    engine._dispatch_due()
    (task,) = state.model.runs[request["id"]]["tasks"].values()
    assert task["held"] == ["merges", "@checked"] and task["status"] == "queued"
    del state.model.indexes[("@checked", "")]
    engine._dispatch_due()
    assert task["id"] in state.model.claims


async def test_a_consumer_at_no_boundary_starts_over(state):
    """§6: an observation record whose head the index can no longer read Δ
    from — merged away while nothing held it — gets a full run."""

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
    for _ in range(3):
        await run(engine, ["items"])
    await run(engine, ["mirror"])
    await settle(engine)
    index = state.model.indexes[("items", "")]
    assert len(index.spans) == 1 and index.generation(1) is None  # merged: no boundary at commit 1
    state.model.partition("mirror", "")["observed"]["items"]["base"]["endpoint"] = 0
    await run(engine, ["mirror"])
    assert deliveries == [(False, ["a", "b"]), (True, ["a", "b"])]


async def test_keyed_source_commits_go_through_the_index(state):
    """§6: a source commit becomes a delta file; a full map replaces the
    content, deleting what it omits; a key at the version it holds is not
    a change (docs/versions.md §2)."""

    got = []

    @asset(inputs={"uploads": Incremental()})
    def ingest(ctx, uploads: list):
        b = ctx.batch["uploads"]
        got.append((list(b.added), list(b.updated), list(b.removed)))
        return []

    class External(FileStore):
        """The source's data lives elsewhere; it serves each asked key, at one version."""

        def serve(self, source, keys, ctx):
            return {k: Loaded({"id": k}, version="1") for k in keys}

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
    assert got == [(["a", "b"], [], []), (["c"], ["b"], ["a"])]  # K44: each key's change
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
    assert m.partition("mirror", "")["observed"]["feed"]["upstream"] == ["source_feed", ""]
    rows["v"] = Patch([{"id": "b", "v": 2}])
    await run(engine, ["mirror"], upstream=True)
    assert m.heads[("source_feed", "")]["commit_number"] == 1
    assert delivered == [(False, ["a", "b"]), (False, ["b"])]  # only the change, not everything
    ref = Ref.from_json(m.heads[("source_feed", "")]["ref"])
    loaded = await project.stores["default"].load(ref, None, await whole(state, "source_feed"))
    assert {r["id"]: r["v"] for r in loaded} == {"a": 1, "b": 2}


def _planning() -> bool:
    """Whether a read is the engine's own, planning a batch or its record:
    a worker's would be anything else."""

    return any(f.filename.endswith(("observing.py", "owed.py")) for f in inspect.stack())


async def test_small_writes_resolve_in_the_engine_and_batches_come_in_the_spec(state, monkeypatch):
    """docs/resolved-commits.md §4: once the engine's cache holds an index, a
    small patch's delta comes from the engine — the worker reads no index
    file — and a consumer's batch comes in its spec (docs/observed-set.md)."""

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
    engine = engine_for(state, Project(assets=[items, mirror]))
    await engine.initialize()
    await run(engine, ["mirror"], upstream=True)
    for n in range(4):
        pending["rows"] = [{"id": f"k{i}", "v": n + 2} for i in range(n, 50, 7)]
        await settle(engine)
        real_read = IO.read

        async def read(self, path, start, end, size, real_read=real_read):
            if not _planning():
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
    assert not [p for p in reads if p.endswith(".kx")]  # its batches come in its spec: no index read
    await engine.keys.stop()


async def test_input_reads_come_from_the_engine_once_warm(state, monkeypatch):
    """A consumer's batches — a first run, later ones — come in its spec,
    planned by the engine (docs/observed-set.md): its worker reads no index
    file to find them, and what it delivers is what the store holds."""

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

    engine = engine_for(state, Project(assets=[items, consumer("mirror"), consumer("copy")]))
    await engine.initialize()
    await run(engine, ["mirror"], upstream=True)

    async def warm():
        await _warm(engine, ("items", ""))

    index_reads, real_read = [], IO.read

    async def read(self, path, start, end, size):
        if not _planning():
            index_reads.append(path)
        return await real_read(self, path, start, end, size)

    for n in range(3):
        rows["v"] = [{"id": f"k{i:03d}", "v": n + 2} for i in range(n, 300, 5)]
        await run(engine, ["items"])
        await warm()
        monkeypatch.setattr(IO, "read", read)
        await run(engine, ["mirror"] if n < 2 else ["mirror", "copy"])  # copy: its first run
        monkeypatch.setattr(IO, "read", real_read)
        truth = await _stored(engine, state)
        assert seen["mirror"] == truth
    assert seen["copy"] == truth
    assert not [p for p in index_reads if p.endswith(".kx")]  # every batch came in its spec
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
    for _ in range(600):  # the engine's own cache may still be filling from the run: it pins too
        await engine.upkeep.collect()
        if await state.get_object(path) is None:
            break
        await asyncio.sleep(0.05)
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

    from solera.sdk import Source
    from solera_server.engine import Engine

    project = Project(sources=[Source("uploads", key="id")])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = Engine(
        state, project.manifest, clock=state.clock, key_options=Options(window=2), resolve_cache=False
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
