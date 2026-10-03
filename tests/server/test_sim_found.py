"""Regressions the deterministic simulation found (tests/sim), each reduced
to its smallest engine-level sequence."""

import asyncio

import pytest
from solera.sdk import Incremental, Output, Project, asset
from solera.stores import FileStore, Patch

from ..conftest import whole
from .test_engine import drive, make_engine, state, status_of  # noqa: F401


async def test_a_keyed_output_moved_to_another_store_stays_readable(state, tmp_path):  # noqa: F811
    """An output moved to another store keeps its key index; the next write
    resolved against it stores only the keys that changed, so a key it did
    not change must still read back — from the store the head now names."""

    content = {"rows": [{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]}

    def project(store):
        @asset(outputs=Output("items", key="id", store=store))
        def items():
            return content["rows"]

        return Project(assets=[items], stores={"other": FileStore(tmp_path / "other")})

    first = project(None)
    engine = make_engine(state, first)
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    await engine.stop()

    second = project("other")
    engine = make_engine(state, second)
    await engine.initialize()
    content["rows"] = [{"id": "a", "v": "2"}, {"id": "b", "v": "1"}]  # only a changes
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    head = state.model.heads[("items", "")]
    assert head["ref"]["store"] == "other"
    rows = await second.stores["other"].load(_ref(head), list[dict], await whole(state, "items"))
    assert sorted((r["id"], r["v"]) for r in rows) == [("a", "2"), ("b", "1")]


def _ref(head):
    from solera.sdk import Ref

    return Ref.from_json(head["ref"])


async def test_a_removed_assets_last_attempt_ends_its_run(state, monkeypatch):  # noqa: F811
    """§11: an attempt launched before its asset was removed settles under
    the contract it was launched with. When it commits a page and asks for
    more, the asset is gone: its task cannot run again, and its run must
    end rather than hold the task forever."""

    from solera_server import attempts

    monkeypatch.setattr(attempts, "AFTER_COMMIT_WAIT", 0.2)  # the stopped engine's answer to `finished`
    entered, release = asyncio.Event(), asyncio.Event()

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a"}, {"id": "b"}]

    @asset(inputs={"files": Incremental(page_size=1)})
    async def pages(files: list):
        entered.set()
        await release.wait()
        return []

    first = Project(assets=[files, pages])
    engine = make_engine(state, first)
    await engine.initialize()
    await drive(engine, await engine.submit(["files"]))
    run = await engine.submit(["pages"])
    while not entered.is_set():
        await engine.tick()
        await asyncio.sleep(0.01)
    await engine.stop()  # the attempt runs on; the next engine adopts it

    second = Project(assets=[files])  # `pages` is gone
    engine = make_engine(state, second)
    await engine.initialize()
    release.set()
    try:
        detail = await drive(engine, run, timeout=10)
    except TimeoutError:
        tasks = state.model.runs[run["id"]]["tasks"].values()
        raise AssertionError(f"the run never ends: {[(t['status'], t.get('held')) for t in tasks]}") from None
    assert status_of(detail) in {"succeeded", "failed", "canceled"}
    assert not state.model.partition("pages", "").get("watermarks")  # its delivery ends with it


async def test_an_attempt_launched_before_a_rename_settles(state, monkeypatch):  # noqa: F811
    """docs/object-store-state.md §2: a rename moves heads, indexes and
    outcomes to the new name; an attempt launched under the old name still
    settles — its run ends and the scope is free for the next one."""

    from solera_server import attempts

    monkeypatch.setattr(attempts, "AFTER_COMMIT_WAIT", 0.2)  # the stopped engine's answer to `finished`
    entered, release = asyncio.Event(), asyncio.Event()

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a"}]

    calls = []

    @asset(inputs={"files": Incremental()})
    async def old(files: list):
        calls.append(1)
        if len(calls) > 1:  # the second run's attempt is in flight across the rename
            entered.set()
            await release.wait()
        return [{"n": len(files)}]

    engine = make_engine(state, Project(assets=[files, old]))
    await engine.initialize()
    await drive(engine, await engine.submit(["old"], upstream=True))
    run = await engine.submit(["old"], mode="full")
    while not entered.is_set():
        await engine.tick()
        await asyncio.sleep(0.01)
    await engine.stop()

    @asset(inputs={"files": Incremental()}, aliases=["old"])
    async def new(files: list):
        return [{"n": len(files)}]

    engine = make_engine(state, Project(assets=[files, new]))
    await engine.initialize()
    release.set()
    try:
        await drive(engine, run, timeout=10)
    except TimeoutError:
        raise AssertionError(f"the run never ends: claims {list(state.model.claims)}") from None
    assert status_of(await drive(engine, await engine.submit(["new"], mode="full"))) == "succeeded"


async def test_a_change_made_during_a_full_delivery_reaches_downstream(state):  # noqa: F811
    """§6, §9: a full keyed delivery begun at batch 0 delivers what changed
    meanwhile afterwards, as a delta. Interrupted after its first page, then
    the upstream changes: the firing for that change resumes the delivery —
    and must also deliver the change, since nothing else will fire for it."""

    from solera.sdk import AutoRefresh
    from solera.stores import Patch

    content = {"a": "1", "b": "1"}

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": k, "v": v} for k, v in content.items()]

    @asset(
        inputs={"items": Incremental(page_size=1)},
        outputs=Output("out", key="id"),
        automations=AutoRefresh(),
    )
    def out(ctx, items: list):
        return Patch([{"id": r["id"], "v": r["v"]} for r in items], remove=list(ctx.changes["items"].deleted))

    project = Project(assets=[items, out])
    engine = make_engine(state, project)
    await engine.initialize()
    await engine.set_automation("out.onchange.0", False)
    await drive(engine, await engine.submit(["items"]))
    run = await engine.submit(["out"])
    while (state.model.watermark("out", "items", "") or {}).get("delivery", {}).get("page") != 1:
        await engine.tick()
        await asyncio.sleep(0.01)
    await engine.cancel(run["id"])  # after its first page: `a` delivered at 1
    await drive(engine, run)
    await engine.set_automation("out.onchange.0", True)
    content.update({"a": "2", "b": "2"})
    await drive(engine, await engine.submit(["items"]))
    for _ in range(200):
        await engine.tick()
        busy = any(r["status"] not in ("succeeded", "failed", "canceled") for r in state.model.runs.values())
        if not busy and not state.model.automations["out.onchange.0"]["pending"]:
            break
        await asyncio.sleep(0.01)
    rows = await project.stores["default"].load(
        _ref(state.model.heads[("out", "")]), list[dict], await whole(state, "out")
    )
    assert sorted((r["id"], r["v"]) for r in rows) == [("a", "2"), ("b", "2")]


async def test_a_slow_new_writer_never_fences_into_a_deleted_segment(tmp_path):
    """docs/object-store-state.md §10: a new writer replays the journal after
    the checkpoint it loaded, then fences at the next free seq. Meanwhile the
    old writer may append, checkpoint and clean up the segments the new one
    has not read yet. The fence must not land in such a hole: the new writer
    then holds a state without acknowledged events, and the old writer's
    later segments follow a fence that never saw what they build on."""

    from obstore.store import LocalStore
    from solera_server.journal import Fenced, Journal

    from .test_journal import Counter, add, open_journal

    store = LocalStore(str(tmp_path), mkdir=True)
    a, sa, _ = await open_journal(store, min_checkpoint=50)
    await add(a, sa, "x", 1)

    b, sb = Journal(store, "control", flush_interval=0.01), Counter()
    fence, opened, go = b._fence, asyncio.Event(), asyncio.Event()

    async def slow(apply):  # replayed what it listed; slow before fencing
        opened.set()
        await go.wait()
        return await fence(apply)

    b._fence = slow
    opening = asyncio.create_task(b.open(sb.restore, sb.apply, sb.snapshot))
    await opened.wait()
    acknowledged = 1
    for _ in range(12):  # the old writer appends, checkpoints and cleans up meanwhile
        await add(a, sa, "x", 1)
        acknowledged += 1
    go.set()
    try:
        await opening
    except Exception:
        return  # refusing to open is safe
    assert sb.counts.get("x") == acknowledged, "the new writer lost acknowledged events"
    with pytest.raises(Fenced):
        await add(a, sa, "x", 1)


async def test_a_batch_upstream_reset_right_after_a_delivery_is_delivered_in_full(state):  # noqa: F811
    """§6: a `full` run starts an unkeyed incremental output over at a new
    `base`; a consumer that read batches before it must be told (`full`),
    or it keeps rows the upstream let go — also when the reset batch is
    exactly the consumer's next one."""

    from solera.sdk import Result
    from solera.stores import Patch

    seen = []

    @asset(outputs=Output("log", incremental=True))
    def log():
        return Patch([{"n": 1}])

    @asset(inputs={"log": Incremental()})
    def tally(ctx, log: list):
        changes = ctx.changes["log"]
        seen.append((changes.full, len(log)))
        base = 0 if (changes.full and changes.first) or ctx.cursor is None else ctx.cursor
        return Result(outputs={"tally": {"rows": base + len(log)}}, cursor=base + len(log))

    engine = make_engine(state, Project(assets=[log, tally]))
    await engine.initialize()
    await drive(engine, await engine.submit(["log"]))  # batch 0
    await drive(engine, await engine.submit(["tally"]))  # delivered through 0: next = 1
    await drive(engine, await engine.submit(["log"], mode="full"))  # starts over at batch 1
    await drive(engine, await engine.submit(["tally"]))
    assert state.model.heads[("log", "")]["base"] == 1
    assert seen[-1][0], f"the reset was delivered as a delta: {seen}"
    assert state.model.partition("tally", "")["cursor"] == 1


@pytest.mark.xfail(
    strict=True, reason="sim finding F10: a reset delivery its patterns take nothing from is skipped"
)
async def test_a_full_delivery_that_takes_no_key_still_starts_over(state):  # noqa: F811
    """§5, §8: a full run makes the output equal to exactly its write, and a
    full delivery starts its consumer over. When the edge's patterns take
    none of the upstream's keys, the delivery is skipped without calling
    the producer — so nothing starts over, and keys the consumer holds from
    before stay, though its upstream holds them no longer."""

    from solera.stores import Patch

    content = {"rows": [{"id": "a", "v": "1"}]}

    @asset(outputs=Output("items", key="id", deploy="v"))
    def items():
        return content["rows"]

    @asset(inputs={"items": Incremental(exclude=["k*"])}, outputs=Output("mirror", key="id", deploy="v"))
    def mirror(ctx, items: list):
        changes = ctx.changes["items"]
        rows = [{"id": r["id"], "v": r["v"]} for r in items]
        return rows if changes.full and changes.first else Patch(rows, remove=list(changes.deleted))

    engine = make_engine(state, Project(assets=[items, mirror]))
    await engine.initialize()
    await drive(engine, await engine.submit(["mirror"], upstream=True))  # mirror holds a
    content["rows"] = [{"id": "k1", "v": "1"}]  # a is gone; k1 is excluded
    await drive(engine, await engine.submit(["items"]))
    await drive(engine, await engine.submit(["mirror"], mode="full"))
    rows = await FileStore().load(
        _ref(state.model.heads[("mirror", "")]), list[dict], await whole(state, "mirror")
    )
    assert rows == []


async def test_a_name_removed_and_added_back_starts_over(state):  # noqa: F811
    """F12: a name the project no longer declares holds no live state. `copy`
    renamed to `mirror` and back without an alias left `mirror`'s first life
    in place, and the next rename onto `mirror` kept it — under a watermark
    already past a deletion. Removed, a name's head, index and scope go (its
    index files to collection); renamed onto, it takes the old name's state."""

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a"}, {"id": "b"}]

    def project(name, aliases=()):
        def body(items: list):
            return items

        body.__name__ = name
        copy = asset(outputs=Output(name, key="id"), inputs={"items": Incremental()}, aliases=list(aliases))(
            body
        )
        return Project(assets=[items, copy])

    m = state.model
    engine = make_engine(state, project("mirror"))
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["mirror"], upstream=True))) == "succeeded"
    first = m.indexes[("mirror", "")]
    files = {first.path(n) for n in first.referenced()}
    await engine.stop()

    engine = make_engine(state, project("copy"))  # `mirror` removed: no alias carries it over
    await engine.initialize()
    assert ("mirror", "") not in m.heads and ("mirror", "") not in m.indexes
    assert not m.partition("mirror", "")
    assert files <= {path for path, _ in m.garbage}
    assert status_of(await drive(engine, await engine.submit(["copy"]))) == "succeeded"
    copied = m.heads[("copy", "")]["ref"]["generation"]
    await engine.stop()

    engine = make_engine(state, project("mirror", aliases=["copy"]))  # renamed onto `mirror`
    await engine.initialize()
    assert ("copy", "") not in m.heads
    assert m.heads[("mirror", "")]["ref"]["generation"] == copied  # copy's state, not the first life


async def test_a_key_a_moved_output_dropped_leaves_its_consumer(state, tmp_path):  # noqa: F811
    """F9: a keyed output moved to another store starts its index over, and
    the move's first write holds only upserts. `copy`, planned under the new
    project but against the old index, commits its delivery after the move
    landed: its watermark then reaches the move's batch, and the move must
    not be read as a plain delta — `k11`, which the move dropped, goes."""

    rows = {"items": [{"id": "k10"}, {"id": "k11"}]}
    entered, release = asyncio.Event(), asyncio.Event()

    def project(store):
        @asset(outputs=Output("items", key="id", store=store))
        def items():
            return rows["items"]

        @asset(outputs=Output("copy", key="id"), inputs={"items": Incremental()})
        async def copy(ctx, items: list):
            if store is not None and not entered.is_set():  # hold until the move landed
                entered.set()
                await release.wait()
            changes = ctx.changes["items"]
            return items if changes.full else Patch(items, remove=changes.deleted)

        return Project(assets=[items, copy], stores={"other": FileStore(tmp_path / "other")})

    engine = make_engine(state, project(None))
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["copy"], upstream=True))) == "succeeded"
    rows["items"] = [{"id": "k10"}, {"id": "k11"}, {"id": "k12"}]
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    await engine.stop()

    engine = make_engine(state, project("other"))
    await engine.initialize()
    held = await engine.submit(["copy"])  # planned against the old index
    while not entered.is_set():
        await engine.tick()
        await asyncio.sleep(0.01)
    rows["items"] = [{"id": "k10"}, {"id": "k12"}]  # the move drops k11
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    assert state.model.heads[("items", "")]["ref"]["store"] == "other"
    release.set()
    assert status_of(await drive(engine, held)) == "succeeded"
    assert status_of(await drive(engine, await engine.submit(["copy"]))) == "succeeded"
    assert sorted((await engine.list_keys("copy"))["keys"]) == ["k10", "k12"]
