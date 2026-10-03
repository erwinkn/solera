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
    the contract it was launched with. When it commits a batch and asks for
    more, the asset is gone: its task cannot run again, and its run must
    end rather than hold the task forever."""

    from solera_server import attempts

    monkeypatch.setattr(attempts, "AFTER_COMMIT_WAIT", 0.2)  # the stopped engine's answer to `finished`
    entered, release = asyncio.Event(), asyncio.Event()

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a"}, {"id": "b"}]

    @asset(inputs={"files": Incremental(batch_size=1)})
    async def batches(files: list):
        entered.set()
        await release.wait()
        return []

    first = Project(assets=[files, batches])
    engine = make_engine(state, first)
    await engine.initialize()
    await drive(engine, await engine.submit(["files"]))
    run = await engine.submit(["batches"])
    while not entered.is_set():
        await engine.tick()
        await asyncio.sleep(0.01)
    await engine.stop()  # the attempt runs on; the next engine adopts it

    second = Project(assets=[files])  # `batches` is gone
    engine = make_engine(state, second)
    await engine.initialize()
    release.set()
    try:
        detail = await drive(engine, run, timeout=10)
    except TimeoutError:
        tasks = state.model.runs[run["id"]]["tasks"].values()
        raise AssertionError(f"the run never ends: {[(t['status'], t.get('held')) for t in tasks]}") from None
    assert status_of(detail) in {"succeeded", "failed", "canceled"}
    assert not state.model.partition("batches", "").get("bookmarks")  # its pass ends with it


async def test_an_attempt_launched_before_a_rename_settles(state, monkeypatch):  # noqa: F811
    """docs/object-store-state.md §2: a rename moves heads, indexes and
    outcomes to the new name; an attempt launched under the old name still
    settles — its run ends and the partition is free for the next one."""

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


async def test_a_change_made_during_a_full_pass_reaches_downstream(state):  # noqa: F811
    """§6, §9: a full keyed pass begun at commit 0 delivers what changed
    meanwhile afterwards, as a delta. Interrupted after its first batch, then
    the upstream changes: the firing for that change resumes the pass —
    and must also deliver the change, since nothing else will fire for it."""

    from solera.sdk import AutoRefresh
    from solera.stores import Patch

    content = {"a": "1", "b": "1"}

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": k, "v": v} for k, v in content.items()]

    @asset(
        inputs={"items": Incremental(batch_size=1)},
        outputs=Output("out", key="id"),
        automations=AutoRefresh(),
    )
    def out(ctx, items: list):
        return Patch([{"id": r["id"], "v": r["v"]} for r in items], remove=list(ctx.batch["items"].removed))

    project = Project(assets=[items, out])
    engine = make_engine(state, project)
    await engine.initialize()
    await engine.set_automation("out.onchange.0", False)
    await drive(engine, await engine.submit(["items"]))
    run = await engine.submit(["out"])
    while (state.model.bookmark("out", "items", "") or {}).get("pass", {}).get("batch") != 1:
        await engine.tick()
        await asyncio.sleep(0.01)
    await engine.cancel(run["id"])  # after its first batch: `a` delivered at 1
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


async def test_an_unkeyed_upstream_reset_right_after_a_pass_is_delivered_in_full(state):  # noqa: F811
    """§6: a `full` run starts an unkeyed incremental output over at a new
    `base`; a consumer that read commits before it must be told (`full`),
    or it keeps rows the upstream let go — also when the reset commit is
    exactly the consumer's next one."""

    from solera.sdk import Result
    from solera.stores import Patch

    seen = []

    @asset(outputs=Output("log", incremental=True))
    def log():
        return Patch([{"n": 1}])

    @asset(inputs={"log": Incremental()})
    def tally(ctx, log: list):
        changes = ctx.batch["log"]
        seen.append((changes.full, len(log)))
        base = 0 if (changes.full and changes.first) or ctx.cursor is None else ctx.cursor
        return Result(outputs={"tally": {"rows": base + len(log)}}, cursor=base + len(log))

    engine = make_engine(state, Project(assets=[log, tally]))
    await engine.initialize()
    await drive(engine, await engine.submit(["log"]))  # commit 0
    await drive(engine, await engine.submit(["tally"]))  # delivered through 0: next = 1
    await drive(engine, await engine.submit(["log"], mode="full"))  # starts over at commit 1
    await drive(engine, await engine.submit(["tally"]))
    assert state.model.heads[("log", "")]["base"] == 1
    assert seen[-1][0], f"the reset was delivered as a delta: {seen}"
    assert state.model.partition("tally", "")["cursor"] == 1


async def test_a_full_pass_that_takes_no_key_still_starts_over(state):  # noqa: F811
    """F10, architecture.md §5: a full pass starts its consumer over, so it
    reaches the producer even when the input's patterns take none of the
    upstream's keys: one empty batch, `full`, `first` and `final`. Keys the
    consumer held from before go, as its upstream no longer holds them."""

    from solera.stores import Patch

    content = {"rows": [{"id": "a", "v": "1"}]}

    @asset(outputs=Output("items", key="id", deploy="v"))
    def items():
        return content["rows"]

    @asset(inputs={"items": Incremental(exclude=["k*"])}, outputs=Output("mirror", key="id", deploy="v"))
    def mirror(ctx, items: list):
        changes = ctx.batch["items"]
        rows = [{"id": r["id"], "v": r["v"]} for r in items]
        return rows if changes.full and changes.first else Patch(rows, remove=list(changes.removed))

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


async def test_a_full_pass_over_an_empty_upstream_reaches_its_producer(state):  # noqa: F811
    """F10: an upstream that holds no key any more still starts its
    consumer over on a full pass: the producer sees one empty batch, `full`,
    `first` and `final`, and what the consumer held from before goes."""

    content, seen = {"rows": [{"id": "a", "v": "1"}]}, []

    @asset(outputs=Output("items", key="id"))
    def items():
        return content["rows"]

    @asset(inputs={"items": Incremental()}, outputs=Output("mirror", key="id"))
    def mirror(ctx, items: list):
        b = ctx.batch["items"]
        seen.append((b.full, b.first, b.final, len(items)))
        return [{"id": r["id"], "v": r["v"]} for r in items]

    engine = make_engine(state, Project(assets=[items, mirror]))
    await engine.initialize()
    await drive(engine, await engine.submit(["mirror"], upstream=True))  # mirror holds a
    content["rows"] = []
    await drive(engine, await engine.submit(["items"], mode="full"))  # the upstream holds nothing
    await drive(engine, await engine.submit(["mirror"], mode="full"))
    assert seen[-1] == (True, True, True, 0)
    assert state.model.index("mirror", "").count == 0


async def test_an_each_full_pass_that_takes_no_key_drops_its_keys(state):  # noqa: F811
    """F10 for `Each`: the producer is written for one key, so a full pass
    taking none is not called; the cleanup after the pass drops the keys the
    asset holds that its input no longer has."""

    from solera.sdk import Each

    content = {"rows": [{"id": "a", "v": "1"}]}

    @asset(outputs=Output("items", key="id"))
    def items():
        return content["rows"]

    @asset(inputs={"item": Each("items", exclude=["k*"])}, outputs=Output("out", key="id"))
    def out(ctx, item: list):
        return [{"v": item[0]["v"]}]

    engine = make_engine(state, Project(assets=[items, out]))
    await engine.initialize()
    await drive(engine, await engine.submit(["out"], upstream=True))
    assert state.model.index("out", "").count == 1
    content["rows"] = [{"id": "k1", "v": "1"}]  # a is gone; k1 is excluded
    await drive(engine, await engine.submit(["items"]))
    await drive(engine, await engine.submit(["out"], mode="full"))
    assert state.model.index("out", "").count == 0


async def test_a_name_removed_and_added_back_starts_over(state):  # noqa: F811
    """F12: a name the project no longer declares holds no live state. `copy`
    renamed to `mirror` and back without an alias left `mirror`'s first life
    in place, and the next rename onto `mirror` kept it — under a bookmark
    already past a deletion. Removed, a name's head, index and partition go (its
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


async def test_a_key_a_moved_output_dropped_leaves_its_consumer(state, tmp_path, monkeypatch):  # noqa: F811
    """F9, under the reset rule: a move makes `items` a new output, so its
    consumer starts over on it. `copy`'s attempt, launched before the move,
    is refused when it settles — its upstream was reset since — and its
    retry reads the new `items` in a full pass: `k11`, which the move's first
    write left out, goes."""

    from solera_server import attempts

    monkeypatch.setattr(attempts, "AFTER_COMMIT_WAIT", 0.2)  # the stopped engine's answer to `finished`
    rows = {"items": [{"id": "k10"}, {"id": "k11"}]}
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    def project(store):
        @asset(outputs=Output("items", key="id", store=store))
        def items():
            return rows["items"]

        @asset(outputs=Output("copy", key="id"), inputs={"items": Incremental()})
        async def copy(ctx, items: list):
            calls.append(1)
            if len(calls) == 2:  # the second attempt is in flight across the move
                entered.set()
                await release.wait()
            changes = ctx.batch["items"]
            return items if changes.full else Patch(items, remove=changes.removed)

        return Project(assets=[items, copy], stores={"other": FileStore(tmp_path / "other")})

    engine = make_engine(state, project(None))
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["copy"], upstream=True))) == "succeeded"
    rows["items"] = [{"id": "k10"}, {"id": "k11"}, {"id": "k12"}]
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    held = await engine.submit(["copy"])  # launched before the move
    while not entered.is_set():
        await engine.tick()
        await asyncio.sleep(0.01)
    await engine.stop()

    engine = make_engine(state, project("other"))
    await engine.initialize()
    assert ("items", "") not in state.model.heads and not state.model.bookmark("copy", "items", "")
    rows["items"] = [{"id": "k10"}, {"id": "k12"}]  # the move's first write drops k11
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    assert state.model.heads[("items", "")]["ref"]["store"] == "other"
    release.set()
    detail = await drive(engine, held, timeout=10)
    assert status_of(detail) == "succeeded"
    outcomes = [a["outcome"] for attempts_ in detail["attempts"].values() for a in attempts_]
    assert outcomes[0] == "failed" and outcomes[-1] == "succeeded", outcomes  # refused, then retried
    assert sorted((await engine.list_keys("copy"))["keys"]) == ["k10", "k12"]


@pytest.mark.parametrize("ends", ["succeeds", "fails", "lost"])
@pytest.mark.parametrize("when", ["before", "during"])
async def test_an_attempt_of_a_removed_and_readded_asset_stays_in_its_life(state, monkeypatch, ends, when):  # noqa: F811
    """Execution spec review: an asset removed and added back under its name
    starts a new life (F12), and an attempt of its first life, launched
    before the removal, ends after the re-add — committing, failing or
    lost, before the new life's first run or while it waits. It must not
    write into the new `copy`, settle into it, nor hold its claim: no head
    of the new life is that attempt's, the new life's content is what its
    own code wrote, and both runs end. (The first life's run may carry on
    with a fresh attempt of the new life's code, as a renamed asset's does.)"""

    await _first_life_across_a_readd(state, monkeypatch, ends, when)


async def test_a_name_removed_while_its_attempt_runs_and_added_back_starts_over(state, monkeypatch):  # noqa: F811
    """F19, F12's rule across a live attempt: removing `copy` resets it at
    that deploy, though its attempt still runs — nothing waits for it, as it
    can commit nothing — so adding it back starts it over: no head, no
    bookmarks of the first life."""

    await _first_life_across_a_readd(state, monkeypatch, "fails", "before", fresh=True)


async def _first_life_across_a_readd(state, monkeypatch, ends: str, when: str, fresh: bool = False):  # noqa: F811
    from solera_server import attempts
    from solera_server.executors.inline import InlinePlacement

    monkeypatch.setattr(attempts, "AFTER_COMMIT_WAIT", 0.2)  # the stopped engine's answer to `finished`
    entered, release = asyncio.Event(), asyncio.Event()

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a"}, {"id": "b"}]

    def project(life: str | None):
        if life is None:
            return Project(assets=[items])

        @asset(outputs=Output("copy", key="id"), inputs={"items": Incremental()}, version=life)
        async def copy(ctx, items: list):
            if life == "1" and ctx.run_id != first["id"]:  # the first life's second run: held
                entered.set()
                await release.wait()
                if ends == "fails":
                    raise RuntimeError("the first life fails")
            return [{"id": r["id"], "life": life} for r in items]

        return Project(assets=[items, copy])

    async def content():
        head = state.model.heads.get(("copy", ""))
        assert head is None or head["attempt"] != held, "the first life's attempt settled into the second"
        if head is None:
            return None
        rows = await FileStore().load(_ref(head), list[dict], await whole(state, "copy"))
        return sorted((r["id"], r["life"]) for r in rows)

    first: dict = {}
    engine = make_engine(state, project("1"))
    await engine.initialize()
    first.update(await engine.submit(["copy"], upstream=True))
    assert status_of(await drive(engine, first)) == "succeeded"
    old = await engine.submit(["copy"], mode="full")
    while not entered.is_set():
        await engine.tick()
        await asyncio.sleep(0.01)
    (held,) = [c["attempt"] for c in state.model.claims.values()]
    await engine.stop()

    engine = make_engine(state, project(None))  # `copy` removed
    await engine.initialize()
    await engine.stop()

    engine = make_engine(state, project("2"))  # and added back
    await engine.initialize()
    if fresh:
        release.set()  # the held worker goes on; what it does is the other test's
        assert ("copy", "") not in state.model.heads, "the second life starts with the first's head"
        assert not state.model.partition("copy", "").get("bookmarks"), "and its bookmarks"
        return
    new = await engine.submit(["copy"], mode="full") if when == "during" else None
    if ends == "lost":
        InlinePlacement._tasks[held].cancel()
    release.set()
    try:
        await drive(engine, old, timeout=10)
        assert await content() in (None, [("a", "2"), ("b", "2")])
        new = new or await engine.submit(["copy"], mode="full")
        assert status_of(await drive(engine, new, timeout=10)) == "succeeded"
    except TimeoutError:
        raise AssertionError(f"a run never ends: claims {state.model.claims}") from None
    assert await content() == [("a", "2"), ("b", "2")]
    assert not state.model.claims
    commits = (await engine.history.commits(outputs=["copy"]))["commits"]
    assert held not in {c["attempt"] for c in commits}, "the first life's attempt committed into the second"


def _moving(tmp_path, rows: dict, seen: list):
    """`items` (keyed) on `store`, and `copy` reading it incrementally."""

    def project(store):
        @asset(outputs=Output("items", key="id", store=store))
        def items():
            return rows["items"]

        @asset(outputs=Output("copy", key="id"), inputs={"items": Incremental()})
        def copy(ctx, items: list):
            b = ctx.batch["items"]
            seen.append((b.full, sorted(r["id"] for r in items)))
            return items if b.full and b.first else Patch(items, remove=list(b.removed))

        return Project(assets=[items, copy], stores={"other": FileStore(tmp_path / "other")})

    return project


async def test_a_move_and_back_with_no_write_between_resets(state, tmp_path):  # noqa: F811
    """K10: each move makes a new output, so `items` moved away and back with
    nothing written in between is reset all the same: its head and index go
    at the deploy, with `copy`'s bookmark on it, and both start over."""

    rows, seen = {"items": [{"id": "a"}, {"id": "b"}]}, []
    project = _moving(tmp_path, rows, seen)
    m = state.model
    engine = make_engine(state, project(None))
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["copy"], upstream=True))) == "succeeded"
    for store in ("other", None):  # away, and back: no run in between
        await engine.stop()
        engine = make_engine(state, project(store))
        await engine.initialize()
    assert ("items", "") not in m.heads and ("items", "") not in m.indexes
    assert m.reset_at["items"] == m.deploy_number and not m.bookmark("copy", "items", "")
    rows["items"] = [{"id": "a"}]  # the new `items` holds no `b`
    assert status_of(await drive(engine, await engine.submit(["copy"], upstream=True))) == "succeeded"
    assert seen[-1] == (True, ["a"])
    assert sorted((await engine.list_keys("copy"))["keys"]) == ["a"]


async def test_an_attempt_launched_before_its_output_moved_commits_nothing(state, tmp_path, monkeypatch):  # noqa: F811
    """K10: an attempt launched before its output moved commits nothing —
    also on a partition with no head yet, which the stale-head check alone
    would let through. Its retry writes into the new store."""

    from solera_server import attempts

    monkeypatch.setattr(attempts, "AFTER_COMMIT_WAIT", 0.2)  # the stopped engine's answer to `finished`
    entered, release = asyncio.Event(), asyncio.Event()

    def project(store):
        @asset(outputs=Output("items", key="id", store=store))
        async def items():
            if not entered.is_set():  # the first attempt is held across the move
                entered.set()
                await release.wait()
            return [{"id": "a"}]

        return Project(assets=[items], stores={"other": FileStore(tmp_path / "other")})

    engine = make_engine(state, project(None))
    await engine.initialize()
    run = await engine.submit(["items"])
    while not entered.is_set():
        await engine.tick()
        await asyncio.sleep(0.01)
    (held,) = [c["attempt"] for c in state.model.claims.values()]
    await engine.stop()
    engine = make_engine(state, project("other"))
    await engine.initialize()
    release.set()
    detail = await drive(engine, run, timeout=10)
    assert status_of(detail) == "succeeded"
    head = state.model.heads[("items", "")]
    assert head["attempt"] != held and head["ref"]["store"] == "other"


async def test_a_keys_run_after_a_move_reads_a_whole_full_pass(state, tmp_path):  # noqa: F811
    """K10, the review's example: `copy` holds {a, b}, moves, and runs
    keys=(a). A move takes its bookmarks, so the run reads a full pass —
    every batch of it before it succeeds — not the one key: the new store
    holds {a, b}, and no pass is left half way."""

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a"}, {"id": "b"}]

    def project(store):
        @asset(outputs=Output("copy", key="id", store=store), inputs={"items": Incremental(batch_size=1)})
        def copy(ctx, items: list):
            b = ctx.batch["items"]
            return items if b.full and b.first else Patch(items, remove=list(b.removed))

        return Project(assets=[items, copy], stores={"other": FileStore(tmp_path / "other")})

    engine = make_engine(state, project(None))
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["copy"], upstream=True))) == "succeeded"
    await engine.stop()
    engine = make_engine(state, project("other"))
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["copy"], keys={"items": {"keys": ["a"]}}))
    assert status_of(detail) == "succeeded"
    assert sorted((await engine.list_keys("copy"))["keys"]) == ["a", "b"]
    assert state.model.heads[("copy", "")]["ref"]["store"] == "other"
    assert "pass" not in state.model.bookmark("copy", "items", "")
    assert "reset" not in state.model.partition("copy", "")


async def test_an_earlier_lifes_objects_are_never_read(state, tmp_path):  # noqa: F811
    """K10 with FileStore, which keeps a name's objects in one place: `log`'s
    first life commits 0–9; it moves away and back; its third life commits
    0–2 in the same place. Reads are bounded by the head, and generations
    only grow: an unkeyed read lists only the head's commits first..last and
    takes each one's highest generation, so commits 3–9 of the first life
    are never read, whole or by range; and a keyed read names only what its
    fresh key index holds, so the first life's keys are gone."""

    from solera.sdk import Result
    from solera.stores import Commits

    life = {"n": "1"}

    def project(store):
        @asset(outputs=[Output("log", incremental=True, store=store), Output("keys", key="id", store=store)])
        def log():
            keys = [{"id": f"k{i}"} for i in range(10)] if life["n"] == "1" else [{"id": "k0"}]
            return Result(outputs={"log": Patch([{"life": life["n"]}]), "keys": keys})

        return Project(assets=[log], stores={"other": FileStore(tmp_path / "other")})

    engine = make_engine(state, project(None))
    await engine.initialize()
    for _ in range(10):
        assert status_of(await drive(engine, await engine.submit(["log"]))) == "succeeded"
    assert state.model.heads[("log", "")]["commit_number"] == 9
    for store in ("other", None):
        await engine.stop()
        engine = make_engine(state, project(store))
        await engine.initialize()
    life["n"] = "3"
    for _ in range(3):
        assert status_of(await drive(engine, await engine.submit(["log"]))) == "succeeded"
    ref = _ref(state.model.heads[("log", "")])
    assert ref.handle["commits"] == [0, 2]
    assert await FileStore().load(ref, list[dict], None) == [{"life": "3"}] * 3
    assert await FileStore().load(ref, list[dict], Commits(0, 9)) == [{"life": "3"}] * 3
    assert sorted((await engine.list_keys("keys"))["keys"]) == ["k0"]


async def test_the_retry_clock_waits_for_an_input_with_no_head(state, tmp_path):  # noqa: F811
    """F20: `checks` (Each, automated) owes a forced retry while a move left
    `items` with no head. The retry clock submitted a run that could not plan
    on every tick, forever — runs and the journal grew without bound. A
    partition whose inputs have no head now waits for them; a run by hand
    still says why it cannot run."""

    from solera.sdk import AutoRefresh, Each

    def project(store):
        @asset(outputs=Output("items", key="id", store=store))
        def items():
            return [{"id": "a"}]

        @asset(inputs={"item": Each("items")}, outputs=Output("checks", key="id"), automations=AutoRefresh())
        def checks(ctx, item: list):
            raise RuntimeError("bad")

        return Project(assets=[items, checks], stores={"other": FileStore(tmp_path / "other")})

    engine = make_engine(state, project(None))
    await engine.initialize()
    await drive(engine, await engine.submit(["checks"], upstream=True))
    engine.retry_keys("checks", ["failed"])
    await engine.stop()
    engine = make_engine(state, project("other"))  # `items` moves: no head until it writes
    await engine.initialize()
    assert ("items", "") not in state.model.heads
    submitted, submit = [], engine.submit

    async def counted(*args, **kwargs):
        run = await submit(*args, **kwargs)
        submitted.append(run)
        return run

    engine.submit = counted
    for _ in range(30):
        await engine.tick()
        await asyncio.sleep(0.01)
    assert [r for r in submitted if r is not None] == []


async def test_a_reset_output_is_due_for_a_rebuild(state, tmp_path):  # noqa: F811
    """K10: a reset leaves an automated output due for a rebuild. `items`
    (OnChange on `feed`) moves to another store, and `feed` does not change:
    the move alone fires `items` again, so it does not stay empty — and its
    consumers waiting — until `feed` next changes."""

    from solera.sdk import AutoRefresh

    @asset(outputs=Output("feed", key="id"))
    def feed():
        return [{"id": "a"}]

    def project(store):
        @asset(
            inputs={"feed": Incremental()},
            outputs=Output("items", key="id", store=store),
            automations=AutoRefresh(),
        )
        def items(ctx, feed: list):
            return feed

        return Project(assets=[feed, items], stores={"other": FileStore(tmp_path / "other")})

    engine = make_engine(state, project(None))
    await engine.initialize()
    await drive(engine, await engine.submit(["items"], upstream=True))
    for _ in range(100):  # the firing `feed`'s commit owes: settled before the move
        await engine.tick()
        busy = any(r["status"] not in ("succeeded", "failed", "canceled") for r in state.model.runs.values())
        if not busy and not state.model.automations["items.onchange.0"]["pending"]:
            break
        await asyncio.sleep(0.01)
    await engine.stop()
    engine = make_engine(state, project("other"))
    await engine.initialize()
    assert ("items", "") not in state.model.heads
    assert state.model.automations["items.onchange.0"]["pending"] == [["items", ""]]  # the reset's alone
    for _ in range(100):
        await engine.tick()
        head = state.model.heads.get(("items", ""))
        if head is not None:
            break
        await asyncio.sleep(0.01)
    assert head is not None and head["ref"]["store"] == "other", "items stays empty until feed changes"
