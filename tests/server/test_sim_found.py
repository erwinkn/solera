"""Regressions the deterministic simulation found (tests/sim), each reduced
to its smallest engine-level sequence."""

import asyncio

import pytest
from solera.sdk import Incremental, Output, Project, asset
from solera.stores import FileStore, Patch

from ..conftest import whole
from .engines import drive, make_engine, status_of


async def test_a_keyed_output_moved_to_another_store_stays_readable(state, tmp_path):
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


async def test_a_removed_assets_last_attempt_ends_its_run(state, monkeypatch):
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
    assert not state.model.partition("batches", "").get("positions")  # its pass ends with it


async def test_an_attempt_launched_before_a_rename_settles(state, monkeypatch):
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


async def test_a_change_made_during_a_full_pass_reaches_downstream(state):
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
    while (state.model.position("out", "items", "") or {}).get("pass", {}).get("batch") != 1:
        await engine.tick()
        await asyncio.sleep(0.01)
    await engine.cancel(run["id"])  # after its first batch: `a` delivered at 1
    await drive(engine, run)
    await engine.set_automation("out.onchange.0", True)
    content.update({"a": "2", "b": "2"})
    await drive(engine, await engine.submit(["items"]))
    for _ in range(6000):
        await engine.tick()
        busy = any(r["status"] not in ("succeeded", "failed", "canceled") for r in state.model.runs.values())
        if not busy and not state.model.automations["out.onchange.0"]["pending"]:
            break
        await asyncio.sleep(0.01)
    rows = await project.stores["default"].load(
        _ref(state.model.heads[("out", "")]), list[dict], await whole(state, "out")
    )
    assert sorted((r["id"], r["v"]) for r in rows) == [("a", "2"), ("b", "2")]


async def test_a_slow_new_engine_never_opens_without_acknowledged_events(tmp_path, monkeypatch):
    """docs/object-store-state.md §10 (F7, F14, F15 under the journal head):
    a new engine reads the journal and loads its checkpoint, then is slow
    to fence. Meanwhile the old engine appends, checkpoints and cleans up.
    The new engine's fence is a swap on the ETag it read, so it fails and
    the engine reads again: it never holds a state without acknowledged
    events, and the old engine is fenced once it has opened."""

    from obstore.store import LocalStore
    from solera_server import journal as journal_module
    from solera_server.journal import Fenced, Journal

    from .test_journal import Counter, open_journal, record

    store = LocalStore(str(tmp_path), mkdir=True)
    a, sa, _ = await open_journal(store, min_checkpoint=50)
    record(a, sa, "x")
    await a.flush()

    b, sb = Journal(store, "control", flush_interval=0.01), Counter()
    real_swap, opened, go = journal_module.swap, asyncio.Event(), asyncio.Event()

    async def slow(st, path, data, etag):  # read and replayed; slow before fencing
        if asyncio.current_task() is opening and not go.is_set():
            opened.set()
            await go.wait()
        return await real_swap(st, path, data, etag)

    monkeypatch.setattr(journal_module, "swap", slow)
    opening = asyncio.create_task(b.open(sb.restore, sb.apply, sb.snapshot))
    await opened.wait()
    acknowledged = 1
    for _ in range(12):  # the old engine appends, checkpoints and cleans up meanwhile
        record(a, sa, "x")
        await a.flush()
        acknowledged += 1
    go.set()
    await opening
    assert sb.counts.get("x") == acknowledged, "the new engine lost acknowledged events"
    record(a, sa, "x")
    with pytest.raises(Fenced):
        await a.flush()


async def test_an_unkeyed_upstream_reset_right_after_a_pass_is_delivered_in_full(state):
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


async def test_a_full_pass_that_takes_no_key_still_starts_over(state):
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


async def test_a_full_pass_over_an_empty_upstream_reaches_its_producer(state):
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


async def test_an_each_full_pass_that_takes_no_key_drops_its_keys(state):
    """F10 for per-key incremental: the producer is written for one key, so a full pass
    taking none is not called; the cleanup after the pass drops the keys the
    asset holds that its input no longer has."""

    content = {"rows": [{"id": "a", "v": "1"}]}

    @asset(outputs=Output("items", key="id"))
    def items():
        return content["rows"]

    @asset(inputs={"item": Incremental("items", exclude=["k*"], each=True)}, outputs=Output("out", key="id"))
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


async def test_a_name_removed_and_added_back_starts_over(state):
    """F12: a name the project no longer declares holds no live state. `copy`
    renamed to `mirror` and back without an alias left `mirror`'s first life
    in place, and the next rename onto `mirror` kept it — under a position
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


async def test_a_key_a_moved_output_dropped_leaves_its_consumer(state, tmp_path, monkeypatch):
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
    assert ("items", "") not in state.model.heads and not state.model.position("copy", "items", "")
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
async def test_an_attempt_of_a_removed_and_readded_asset_stays_in_its_life(state, monkeypatch, ends, when):
    """F19: an asset removed and added back under its name starts a new
    life, and an attempt of its first life, launched
    before the removal, ends after the re-add — committing, failing or
    lost, before the new life's first run or while it waits. It must not
    write into the new `copy`, settle into it, nor hold its claim: no head
    of the new life is that attempt's, the new life's content is what its
    own code wrote, and both runs end. (The first life's run may carry on
    with a fresh attempt of the new life's code, as a renamed asset's does.)"""

    await _first_life_across_a_readd(state, monkeypatch, ends, when)


async def test_a_name_removed_while_its_attempt_runs_and_added_back_starts_over(state, monkeypatch):
    """F19, across a live attempt: removing `copy` resets it at
    that deploy, though its attempt still runs — nothing waits for it, as it
    can commit nothing — so adding it back starts it over: no head, no
    positions of the first life."""

    await _first_life_across_a_readd(state, monkeypatch, "fails", "before", fresh=True)


async def _first_life_across_a_readd(state, monkeypatch, ends: str, when: str, fresh: bool = False):
    from solera_server import attempts
    from solera_server.executors import inline

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
        assert not state.model.partition("copy", "").get("positions"), "and its positions"
        return
    new = await engine.submit(["copy"], mode="full") if when == "during" else None
    if ends == "lost":
        inline._workers.get(held).cancel()
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


async def test_a_move_and_back_with_no_write_between_resets(state, tmp_path):
    """K10: each move makes a new output, so `items` moved away and back with
    nothing written in between is reset all the same: its head and index go
    at the deploy, with `copy`'s position on it, and both start over."""

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
    assert m.reset_at[("output", "items")] == m.deploy_number and not m.position("copy", "items", "")
    rows["items"] = [{"id": "a"}]  # the new `items` holds no `b`
    assert status_of(await drive(engine, await engine.submit(["copy"], upstream=True))) == "succeeded"
    assert seen[-1] == (True, ["a"])
    assert sorted((await engine.list_keys("copy"))["keys"]) == ["a"]


async def test_an_attempt_launched_before_its_output_moved_commits_nothing(state, tmp_path, monkeypatch):
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


async def test_a_keys_run_after_a_move_starts_the_full_pass_a_default_run_finishes(state, tmp_path):
    """K10, the review's example, under K45 and Erwin's correction: `copy`
    holds {a, b}, moves, and runs keys=(a). The move reset it: a full pass
    is due, and the keys= run starts it over with `a` alone. The next
    default run continues that pass with `b`, never `a` again, and
    converges to {a, b}."""

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
    assert sorted((await engine.list_keys("copy"))["keys"]) == ["a"]
    assert state.model.heads[("copy", "")]["ref"]["store"] == "other"
    assert state.model.position("copy", "items", "")["pass"]["mode"] == "full"  # under way
    assert status_of(await drive(engine, await engine.submit(["copy"]))) == "succeeded"
    assert sorted((await engine.list_keys("copy"))["keys"]) == ["a", "b"]
    assert "pass" not in state.model.position("copy", "items", "")


async def test_an_earlier_lifes_objects_are_never_read(state, tmp_path):
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


async def test_the_retry_clock_waits_for_an_input_with_no_head(state, tmp_path):
    """F20: `checks` (Each, automated) owes a forced retry while a move left
    `items` with no head. The retry clock submitted a run that could not plan
    on every tick, forever — runs and the journal grew without bound. A
    partition whose inputs have no head now waits for them; a run by hand
    still says why it cannot run."""

    from solera.sdk import AutoRefresh

    def project(store):
        @asset(outputs=Output("items", key="id", store=store))
        def items():
            return [{"id": "a"}]

        @asset(
            inputs={"item": Incremental("items", each=True)},
            outputs=Output("checks", key="id"),
            automations=AutoRefresh(),
        )
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


async def test_a_reset_output_is_due_for_a_rebuild(state, tmp_path):
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
    for _ in range(6000):  # the firing `feed`'s commit owes: settled before the move
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
    for _ in range(6000):
        await engine.tick()
        head = state.model.heads.get(("items", ""))
        if head is not None:
            break
        await asyncio.sleep(0.01)
    assert head is not None and head["ref"]["store"] == "other", "items stays empty until feed changes"


async def test_a_job_added_back_does_not_take_its_first_lifes_commit(state, monkeypatch):
    """F21: `seen` has no output, so no output's reset covers it.
    Removed while its attempt runs and added back before that attempt
    succeeds, it must not take that attempt's commit — its cursor and
    positions belong to the first life."""

    from solera.sdk import Result, job
    from solera_server import attempts

    monkeypatch.setattr(attempts, "AFTER_COMMIT_WAIT", 0.2)  # the stopped engine's answer to `finished`
    entered, release = asyncio.Event(), asyncio.Event()

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a"}, {"id": "b"}]

    def project(life):
        if life is None:
            return Project(assets=[items])

        @job(inputs={"items": Incremental()}, version=life)
        async def seen(ctx, items: list):
            if life == "1":
                entered.set()
                await release.wait()
            return Result(outputs={}, cursor={"life": life, "keys": sorted(r["id"] for r in items)})

        return Project(assets=[items, seen])

    engine = make_engine(state, project("1"))
    await engine.initialize()
    await drive(engine, await engine.submit(["items"]))
    old = await engine.submit(["seen"])
    while not entered.is_set():
        await engine.tick()
        await asyncio.sleep(0.01)
    await engine.stop()

    engine = make_engine(state, project(None))  # `seen` removed
    await engine.initialize()
    await engine.stop()

    engine = make_engine(state, project("2"))  # and added back
    await engine.initialize()
    release.set()
    await drive(engine, old, timeout=10)
    cursor = state.model.partition("seen", "").get("cursor")
    assert cursor is None or cursor["life"] == "2", f"the second life took the first's commit: {cursor}"


async def test_a_job_removed_while_its_attempt_runs_and_added_back_starts_over(state, monkeypatch):
    """F21: the reset rule for an asset with no output. Job `seen` reads
    `items` incrementally; its cursor is what it saw. Removed while its
    attempt runs, and added back (version 2), it starts over: its partition
    state went at the removal, and the attempt — launched before — commits
    neither its cursor nor its positions into the new `seen`: its run carries
    on with a fresh attempt of the new code, as a renamed asset's does."""

    from solera.sdk import Result, job
    from solera_server import attempts

    monkeypatch.setattr(attempts, "AFTER_COMMIT_WAIT", 0.2)  # the stopped engine's answer to `finished`
    entered, release, calls = asyncio.Event(), asyncio.Event(), []

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a"}]

    def project(life):
        if life is None:
            return Project(assets=[items])

        @job(inputs={"items": Incremental()}, version=life)
        async def seen(ctx, items: list):
            calls.append(life)
            if calls == ["1", "1"]:  # its second run is held across the removal
                entered.set()
                await release.wait()
            return Result(outputs={}, cursor={"life": life})

        return Project(assets=[items, seen])

    engine = make_engine(state, project("1"))
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["seen"], upstream=True))) == "succeeded"
    old = await engine.submit(["seen"], mode="full")
    while not entered.is_set():
        await engine.tick()
        await asyncio.sleep(0.01)
    (held,) = [c["attempt"] for c in state.model.claims.values()]
    await engine.stop()
    for life in (None, "2"):  # removed, and added back
        engine = make_engine(state, project(life))
        await engine.initialize()
        if life is None:
            await engine.stop()
    assert not state.model.partition("seen", ""), "the second life starts with the first's state"
    release.set()
    await drive(engine, old, timeout=10)  # refused, and carried on by the new life's code
    record = state.model.partition("seen", "")
    assert record["cursor"] == {"life": "2"} and record["last"]["attempt"] != held, record


async def test_an_onchange_asset_added_back_is_built(state):
    """F22: `copy` (OnChange on `items`) removed and added back was not built
    until `items` next changed: its automation came back with nothing
    pending. A deploy leaves each OnChange automation owing a firing for
    every partition with no head whose inputs have heads — once, at the
    deploy, never re-checked by a tick."""

    from solera.sdk import AutoRefresh

    @asset(outputs=Output("feed", key="id"), automations=AutoRefresh())
    def feed():
        return [{"id": "a"}]

    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id"), automations=AutoRefresh())
    def items(feed: list):
        return feed

    @asset(inputs={"items": Incremental()}, outputs=Output("copy", key="id"), automations=AutoRefresh())
    def copy(items: list):
        return items

    async def quiet(engine):
        for _ in range(6000):
            await engine.tick()
            busy = any(
                r["status"] not in ("succeeded", "failed", "canceled") for r in state.model.runs.values()
            )
            if not busy and not any(a["pending"] for a in state.model.automations.values()):
                return
            await asyncio.sleep(0.01)
        raise AssertionError("never quiet")

    engine = make_engine(state, Project(assets=[feed, items, copy]))
    await engine.initialize()
    await drive(engine, await engine.submit(["copy"], upstream=True))  # built
    await quiet(engine)
    await engine.stop()
    for assets in ([feed, items], [feed, items, copy]):  # without copy; with it again
        engine = make_engine(state, Project(assets=assets))
        await engine.initialize()
        await quiet(engine)
        await engine.stop()
    assert ("copy", "") in state.model.heads


def _changing(tmp_path, kind: str, automation: str, after: bool, calls: list):
    """`feed` and `items` (reading it incrementally), before or `after` an
    asset change of `kind` to `items`, under `automation`."""

    from solera.sdk import Automation, AutoRefresh, Cron

    @asset(outputs=Output("feed", key="id"))
    def feed():
        return [{"id": "a"}]

    if kind == "added" and not after:
        return Project(assets=[feed]), "items"
    name = "renamed" if kind == "renamed" and after else "items"
    automations = {
        "onchange": AutoRefresh(),
        "schedule": Automation(trigger=Cron("0 7 1 1 *")),  # yearly: not due in the test
        "none": [],
    }[automation]

    def body(feed: list):
        calls.append(after)
        return feed

    body.__name__ = name
    items = asset(
        inputs={"feed": Incremental(batch_size=2 if kind == "changed" and after else 100)},
        outputs=Output(name, key="id", store="other" if kind == "reset" and after else None),
        automations=automations,
        aliases=["items"] if name == "renamed" else [],
    )(body)
    return Project(assets=[feed, items], stores={"other": FileStore(tmp_path / "other")}), name


@pytest.mark.parametrize("automation", ["onchange", "schedule", "none"])
@pytest.mark.parametrize("kind", ["added", "renamed", "changed", "reset"])
async def test_an_asset_change_is_built_by_its_automation_or_marked_stale(state, tmp_path, kind, automation):
    """Erwin's asset-change rule: a deploy that adds an asset (again),
    renames it, changes its declaration or resets it leaves its OnChange
    automation owing a firing, once, per partition whose inputs have heads:
    it is built at once. A schedule waits for its next time (a new one
    too: it never fires at declaration) and no
    automation runs nothing: the partition shows `stale` (or `missing`) until
    a run catches it up, and then `materialized`."""

    async def quiet(engine):
        for _ in range(6000):
            await engine.tick()
            busy = any(
                r["status"] not in ("succeeded", "failed", "canceled") for r in state.model.runs.values()
            )
            if not busy and not any(a["pending"] for a in state.model.automations.values()):
                return
            await asyncio.sleep(0.01)
        raise AssertionError("never quiet")

    async def status(engine, name):
        return [row["status"] for row in (await engine.partition_statuses([name]))[name]]

    calls = []
    project, name = _changing(tmp_path, kind, automation, after=False, calls=calls)
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["feed" if kind == "added" else name], upstream=True))
    await quiet(engine)
    await engine.stop()
    project, name = _changing(tmp_path, kind, automation, after=True, calls=calls)
    engine = make_engine(state, project)
    await engine.initialize()
    await quiet(engine)
    if automation == "onchange":  # built at once, by the firing the deploy owes it
        assert await status(engine, name) == ["materialized"]
        if kind == "renamed":  # its state carried over: the firing is a skip, no call
            assert True not in calls
        return
    expected = "missing" if kind in ("added", "reset") else "stale"
    assert await status(engine, name) == [expected]  # nothing ran: shown, for a run by hand
    assert status_of(await drive(engine, await engine.submit([name]))) == "succeeded"
    assert await status(engine, name) == ["materialized"]  # the marker clears


async def test_a_pool_attempt_is_offered_only_once_its_launch_is_durable(state, monkeypatch):
    """F26: a pool attempt was offered to pool workers from the model, where
    `AttemptLaunched` is applied before it is durable. An engine replaced in
    between handed out an attempt no journal holds: it ran, wrote rows into
    a fenced store, and no engine ever committed or repaired them
    (simulation: `odd` kept a row nobody committed)."""

    from solera.executors import Pool

    @asset(outputs=Output("trained"), executor=Pool("gpu")(cpu=1))
    def trained():
        return {"w": 1}

    engine = make_engine(state, Project(assets=[trained]), placements={})
    await engine.initialize()
    hold, real = asyncio.Event(), state.durable

    async def held():  # the launch's flush, not landed yet
        await hold.wait()
        await real()

    await engine.submit(["trained"])
    monkeypatch.setattr(state, "durable", held)
    ticking = asyncio.create_task(engine.tick())
    for _ in range(6000):
        if state.model.pool:
            break
        await asyncio.sleep(0.01)
    assert state.model.pool, "the launch was recorded"
    offered = await engine.pool_work("gpu", {"cpu": 8}, "host", 0)
    hold.set()
    await ticking
    assert offered == [], "a launch that is not durable is offered to pool workers"


async def test_a_key_a_current_read_missed_reaches_its_consumer_once_restored(state, tmp_path):
    """F33: `feed` is a keyed source read current (a load answers with the
    outside as it is now). `feed` commits k1 at version 3; before `items`
    reads it, the outside loses k1 and the commit saying so never lands;
    `items` runs, finds no row for k1, and commits. The client restores k1 at
    version 3 and commits: the same version, so no change. `items` must
    still end up holding what `feed`'s index lists."""

    from solera.sdk import Source

    from tests.sim.oracle import index_entries
    from tests.sim.project import External, SourceStore, rebuild

    outside = External()

    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id"))
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    project = Project(
        assets=[items],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(tmp_path / "ext", outside)},
        default_store=FileStore(tmp_path / "data"),
    )
    engine = make_engine(state, project)
    await engine.initialize()
    outside.feed.update(k0="1")
    await engine.commit_source("feed", upsert={"k0": "1"})
    await drive(engine, await engine.submit(["items"]))
    outside.feed.clear()
    outside.feed.update(k1="3")
    await engine.commit_source("feed", upsert={"k1": "3"}, remove=["k0"])
    outside.feed.clear()  # the outside loses k1; the commit saying so never lands
    outside.feed.update(k3="3")
    await drive(engine, await engine.submit(["items"]))  # finds no row for k1
    outside.feed.clear()
    outside.feed.update(k1="3")  # restored, at its old version
    await engine.commit_source("feed", keys={"k1": "3"})
    for _ in range(3):  # whatever runs it takes
        await drive(engine, await engine.submit(["items"]))
    held = set(await index_entries(state, "items", ""))
    listed = set(await index_entries(state, "feed", ""))
    assert held == listed, f"items holds {sorted(held)}, feed's index lists {sorted(listed)}"
