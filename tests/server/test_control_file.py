"""The attempt control file (docs/lifecycle.md §2.4): one file per attempt,
created `open` by the engine before the launch, then only swapped. Each
test is one of `Attempt.tla`'s rules switched off in its calibration
(docs/verification.md, "Formal model: the attempt control file"), played
against the code: the bug TLC finds without the rule cannot happen."""

import asyncio
import contextlib

import pytest
from solera import lifecycle
from solera.objects import Conflict, swap
from solera.sdk import Project, Retry, asset
from solera.stores import FileStore
from solera_server import attempts
from solera_server.state import State, Unavailable
from solera_worker.worker import ENDED, run_attempt

from .test_fence import REMOTE, Fake, Gated, Remote, as_worker, engine_for, fence, launched, own, until


def writes_to(monkeypatch) -> list[str]:
    """Every object a store write puts, from here on."""

    put, puts = FileStore._put, []

    async def counting(self, name, value):
        puts.append(name)
        await put(self, name, value)

    monkeypatch.setattr(FileStore, "_put", counting)
    return puts


async def test_a_worker_never_creates_the_control_file(tmp_path, monkeypatch):
    """`PreCreate`. The engine ends A before its worker reports and settles;
    retention deletes A's control file while the worker boots on the spec it
    read. The worker finds no file and stops: it never creates one, so it
    never owns, takes the gate, or writes (TLC without the rule: an `owned`
    file created by the late worker, and a write after `none`)."""

    puts = writes_to(monkeypatch)
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE)
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    await engine.cancel(run["id"])
    await until(engine, lambda: state.model.claimed(attempt) is None)
    assert await fence(state, run["id"], attempt) == ("ended", "none")
    await state.delete_objects([f"{state.attempt_path(run['id'], attempt)}{lifecycle.CONTROL}"])
    code = await asyncio.wait_for(run_attempt(state.objects_url, attempt, REMOTE, run=run["id"]), 5)
    assert code == ENDED and await fence(state, run["id"], attempt) is None and puts == []
    await engine.stop()
    await state.close()


async def test_a_worker_takes_the_gate_before_its_first_write(tmp_path, monkeypatch):
    """`TakeWriting`. The worker owns A and computes; the engine ends A from
    `owned`, so `none`. The worker's swap to `writing` is refused, and it
    writes nothing (TLC without the rule: it writes anyway, after `none`)."""

    release = asyncio.Event()

    @asset(executor=Fake("fake")(), retries=Retry(0))
    async def held():
        await release.wait()
        return [{"ok": True}]

    project = Project(assets=[held], executors=[Fake("fake")], default_store=Gated())
    puts = writes_to(monkeypatch)
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, heartbeat_seconds=30)  # no report reads the end first
    await engine.initialize()
    run, attempt = await launched(engine, ["held"])
    worker = asyncio.create_task(run_attempt(state.objects_url, attempt, project, run=run["id"]))
    for _ in range(100):
        if (await fence(state, run["id"], attempt))[0] == lifecycle.OWNED:
            break
        await asyncio.sleep(0.02)
    assert (await engine._end(run["id"], attempt))["write"] == lifecycle.NONE
    release.set()
    assert await asyncio.wait_for(worker, 5) == ENDED
    assert puts == [] and await fence(state, run["id"], attempt) == ("ended", "none")
    await engine.stop()
    await state.close()


async def test_the_engine_ends_on_what_it_read(tmp_path, monkeypatch):
    """`EngineSwaps`. The engine reads `open`; before its end lands, the
    worker owns A and takes the gate. The end is a swap on the version it
    read, so it is refused; the engine reads again and ends from `writing`,
    with the intents (TLC without the rule: a blind write replaces `writing`
    with `none`, and the worker's write lands after it)."""

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE)
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    intents = {"remote": {"files": [], "added": 0, "removed": 0, "exact": True}}
    swap, raced = attempts.swap, []

    async def worker_first(store, path, data, etag):
        if not raced:  # between the engine's read and its swap
            raced.append(1)
            await own(state, run["id"], attempt)
            await as_worker(state, run["id"], attempt, lifecycle.WRITING, intents=intents)
        return await swap(store, path, data, etag)

    monkeypatch.setattr(attempts, "swap", worker_first)
    final = await engine._end(run["id"], attempt)
    assert final == {"state": "ended", "engine": state.journal.engine, "write": "writing", "intents": intents}
    assert await fence(state, run["id"], attempt) == ("ended", "writing")
    await engine.stop()
    await state.close()


async def test_an_attempt_ended_from_writing_owes_a_repair(tmp_path):
    """`Classify`. The worker takes the gate and goes silent mid-write: the
    engine ends A from `writing`, so its evidence is `writing`, never
    `none`, and the outputs it meant to change are owed a repair (TLC
    without the rule: recorded `none`, while its write landed)."""

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE, cancel_grace=0.1)
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    intents = {"remote": {"files": [], "added": 0, "removed": 0, "exact": True}}
    await own(state, run["id"], attempt)
    await as_worker(state, run["id"], attempt, lifecycle.WRITING, intents=intents)
    _, held = await lifecycle.read_control(state.objects, run["id"], attempt)  # the worker's version
    await engine.cancel(run["id"])
    await until(engine, lambda: state.model.claimed(attempt) is None)
    assert await fence(state, run["id"], attempt) == ("ended", "writing")
    assert [r["attempt"] for r in state.model.repairs[("remote", "")]] == [attempt]
    late = lifecycle.control(lifecycle.SEALED, worker_id="w", result={"status": "succeeded"})
    with pytest.raises(Conflict):  # the worker, back: its seal, on the version it held, is refused
        await swap(state.objects, f"{state.attempt_path(run['id'], attempt)}{lifecycle.CONTROL}", late, held)
    assert await fence(state, run["id"], attempt) == ("ended", "writing")
    await engine.stop()
    await state.close()


async def test_an_engine_fenced_before_its_launch_is_durable_tells_no_worker(tmp_path):
    """`OfferDurable` (F26). The engine writes the spec and the `open` file,
    records `AttemptLaunched`, and is replaced before that lands. No
    placement launches the attempt and no pool host is offered it; the
    successor never learns of it. The spec and the `open` file wait for
    retention (TLC without the rule: a worker owns the `open` file and
    writes rows no engine commits or repairs, `NoOrphanWrite`)."""

    url = tmp_path.as_uri()
    state = await State.open(url, "test", flush_interval=60)  # nothing lands but through `durable`
    engine = engine_for(state, REMOTE)
    await engine.initialize()
    durable, successor = state.durable, {}

    async def replaced_first():
        if not successor and any(live.launching for live in engine.live.values()):
            successor["state"] = await State.open(url, "test", flush_interval=0.001)
        await durable()

    state.durable = replaced_first
    Remote.launches.clear()
    run = await engine.submit(["remote"])
    for _ in range(50):
        with contextlib.suppress(Unavailable):
            await engine.tick()
        await asyncio.sleep(0.02)
        if successor:
            break
    await asyncio.sleep(0.2)
    assert successor and state.poisoned and Remote.launches == []
    [attempt] = {p.rsplit("/", 1)[-1].split(".")[0] for p in await state.list_objects(f"runs/{run['id']}/")}
    assert await fence(state, run["id"], attempt) == ("open", None)
    assert attempt not in successor["state"].model.attempts
    await engine.stop()
    await successor["state"].close()
