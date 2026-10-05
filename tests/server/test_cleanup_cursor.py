"""An immutable keyed output's cleanup cursor (D168, docs/lifecycle.md §9.8):
each commit's delta names the generations its updates and removes replaced;
a cleanup step walks the queued deltas no reader can still need, in commit
order, deletes what they name, and moves the cursor past them. No listing."""

import pytest
from solera.sdk import Output, Project, asset
from solera.stores import Patch
from solera_server.state import State

from tests.conftest import worker_finished
from tests.server.test_collection import GUARD, cleaned, engine_for

KEY = ("items", "")


async def merged(engine):
    """Upkeep's merges, run until none is due."""

    import asyncio

    for _ in range(50):
        engine.upkeep.maintain()
        if not len(engine.upkeep.jobs):
            return
        await asyncio.gather(*engine.upkeep.jobs.values(), return_exceptions=True)


def generations(data) -> dict[str, set[int]]:
    """The store's objects of `items`: key -> generations."""

    out: dict[str, set[int]] = {}
    for p in (data / "items").rglob("*.json"):
        out.setdefault(p.parent.name, set()).add(int(p.stem))
    return out


@pytest.fixture
async def items(tmp_path, data):
    """An engine whose `items` (FileStore: immutable) commits `pending`'s patch per run."""

    pending = {"rows": {}, "removes": []}

    @asset(outputs=Output("items", keyed=True))
    def items():
        return Patch(pending["rows"], remove=pending["removes"])

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    # The hourly job as deployed (not every tick): cleanup after commits is what's under test.
    engine = engine_for(state, Project(assets=[items]), cancel_grace=0, cleanup_interval=3600.0)
    await engine.initialize()

    async def commit(rows=None, removes=(), *, clean=True):
        """One run's commit; with `clean`, then every cleanup step due."""

        pending["rows"], pending["removes"] = dict(rows or {}), list(removes)
        detail = await engine.run_until((await engine.submit(["items"]))["id"], GUARD)
        assert detail["request"]["status"] == "succeeded", [t.get("error") for t in detail["tasks"]]
        await worker_finished()
        if clean:
            await cleaned(engine)
        return state.model.heads[KEY]["ref"]["generation"]

    yield engine, state, commit
    await engine.stop()
    await state.close()


def queued(state):
    """The commits of the queued deltas: the cleanup cursor stands before the first."""

    return [d["commit"] for d in state.model.cleaning.get(KEY) or ()]


async def test_superseded_generations_go_once_no_pin_needs_them(items, data):
    engine, state, commit = items
    g1 = await commit({"a": 1, "b": 1})
    with state.model.reading(state.model.indexes[KEY].prefix):  # a reader pinned before the update
        g2 = await commit({"a": 2})
        assert generations(data)["a"] == {g1, g2}  # its version at its pin stays
        assert queued(state) == [1]
    await cleaned(engine)
    assert generations(data) == {"a": {g2}, "b": {g1}}
    assert queued(state) == []


async def test_an_observation_holds_the_cursor_back(items, data, monkeypatch):
    engine, state, commit = items
    g1 = await commit({"a": 1})
    observed = {"at": 0}
    monkeypatch.setattr(state.model, "oldest_observed", lambda o, p: observed["at"])
    g2 = await commit({"a": 2})
    g3 = await commit({"a": 3})
    await cleaned(engine)
    assert generations(data)["a"] == {g1, g2, g3}  # a reader at commit 0 reads g1
    assert queued(state) == [1, 2]  # the cursor stays at the observed commit
    observed["at"] = 1  # the reader moved to commit 1: what commit 1 replaced is no one's
    await cleaned(engine)
    assert generations(data)["a"] == {g2, g3} and queued(state) == [2]
    observed["at"] = None
    await cleaned(engine)
    assert generations(data)["a"] == {g3} and queued(state) == []


async def test_a_removed_keys_object_goes_and_a_later_re_add_stays(items, data, monkeypatch):
    engine, state, commit = items
    g1 = await commit({"a": 1, "b": 1})
    observed = {"at": None}
    monkeypatch.setattr(state.model, "oldest_observed", lambda o, p: observed["at"])
    await commit(removes=["a"])  # commit 1
    observed["at"] = 1  # the cursor may reach commit 1, not the re-add after it
    g3 = await commit({"a": 9})  # commit 2: an add, which names nothing
    await cleaned(engine)
    assert generations(data) == {"a": {g3}, "b": {g1}}  # the removed object went; the re-add stays
    assert queued(state) == []


async def test_a_lost_acknowledgement_replays_the_step(items, data, monkeypatch):
    """A crash between the deletes and the cursor move: the step is handed
    again, its deletes repeat harmlessly, and the cursor then moves."""

    from solera_worker import worker

    engine, state, commit = items
    await commit({"a": 1})
    with state.model.reading(state.model.indexes[KEY].prefix):  # no step before the crash is set up
        g2 = await commit({"a": 2})
    real, calls = worker._cleanup_due, []

    async def crashing(*args, **kw):
        out = await real(*args, **kw)
        calls.append(out)
        if len(calls) == 1:
            out.pop("cleaned_to", None)  # the deletes landed; the cursor move never did
        return out

    monkeypatch.setattr(worker, "_cleanup_due", crashing)
    await cleaned(engine)  # the step stays due, so it is handed again
    assert len(calls) == 2 and "cleaned_to" not in calls[0] and "items" in calls[1]["cleaned_to"]
    assert generations(data)["a"] == {g2} and queued(state) == []


async def test_a_delta_is_kept_until_the_cursor_passes_it(items, data, monkeypatch):
    """Merges let go of a delta's files; collection keeps them while the
    cursor still has to read them, and deletes them once it passed."""

    import asyncio

    engine, state, commit = items
    observed = {"at": -1}
    monkeypatch.setattr(state.model, "oldest_observed", lambda o, p: observed["at"])
    await commit({"a": 1, "b": 1})
    for i in range(12):
        await commit({"a": i + 2})
    queue = state.model.cleaning[KEY]
    first = f"{queue[0]['prefix']}{queue[0]['files'][0]}"
    observed["at"] = None  # merges may now drop flips: the cut goes to the head
    for _ in range(50):
        engine.upkeep.maintain()
        if not len(engine.upkeep.jobs):
            break
        await asyncio.gather(*engine.upkeep.jobs.values(), return_exceptions=True)
    assert first.rsplit("/", 1)[1] not in state.model.indexes[KEY].referenced()  # merged away
    observed["at"] = -1
    await engine.upkeep.collect()
    assert first in set(await state.list_objects(state.model.indexes[KEY].prefix))  # still queued
    observed["at"] = None
    await cleaned(engine)
    assert queued(state) == []
    await engine.upkeep.collect()
    assert first not in set(await state.list_objects(state.model.indexes[KEY].prefix))
    assert len(generations(data)["a"]) == 1


async def test_a_burst_of_commits_makes_few_cleanup_tasks(items, data):
    """Cleanup after commits is coalesced: one task per output partition per
    `cleanup_coalesce` (a minute), sooner once a step's worth of deltas
    (`CLEANUPS`) is queued — not one per commit."""

    from solera_server.engine import CLEANUPS

    engine, state, commit = items
    made, real = [], engine._cleanup_task

    def counted(cleanup, partition, output):
        made.append(cleanup)
        return real(cleanup, partition, output)

    engine._cleanup_task = counted
    for i in range(30):  # commit 0 adds; 1–29 update, each queueing its delta
        await commit({"a": i}, clean=False)
    assert len(made) == 1  # the first update's; the 28 after it wait for the window
    assert queued(state)
    engine._cleanup_at[KEY] -= engine.cleanup_coalesce + 1  # a minute passes
    engine._cleanup_job()
    assert len(made) == 2  # one task for all that waited
    await cleaned(engine)
    assert queued(state) == []
    assert generations(data)["a"] == {state.model.heads[KEY]["ref"]["generation"]}

    with state.model.reading(state.model.indexes[KEY].prefix):  # no step due: deltas pile up
        for i in range(CLEANUPS):
            await commit({"a": 100 + i}, clean=False)
            if i % 16 == 15:
                await merged(engine)  # as upkeep would: past 64 layers, commits wait (backpressure)
    before = len(made)
    await commit({"a": 999}, clean=False)  # a step's worth queued: at once, inside the window
    assert len(made) == before + 1
