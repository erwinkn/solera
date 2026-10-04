"""A component's background tasks (solera.tasks.Tasks): held until they end,
their failures logged as they happen, cancelled in the order spawned."""

import asyncio
import gc
import logging

import pytest
from solera.tasks import Tasks


async def test_a_task_nobody_else_holds_runs_to_its_end():
    tasks, done = Tasks("t"), asyncio.Event()

    async def work():
        await asyncio.sleep(0.05)
        done.set()

    tasks.spawn(work())  # the caller keeps no reference
    gc.collect()
    await asyncio.wait_for(done.wait(), 1)
    for _ in range(3):  # the task ends, then its done callback runs
        await asyncio.sleep(0)
    assert len(tasks) == 0, "an ended task is let go"


async def test_a_failure_is_logged_when_it_happens(caplog):
    tasks = Tasks("t")

    async def fail():
        raise ValueError("lost work")

    with caplog.at_level(logging.ERROR, logger="solera.tasks"):
        task = tasks.spawn(fail(), key="doomed")
        await asyncio.wait({task})
        await asyncio.sleep(0)
    assert "t:doomed failed" in caplog.text and "lost work" in caplog.text


async def test_an_awaited_failure_is_left_to_whoever_awaits_it(caplog):
    tasks = Tasks("t")

    async def fail():
        raise ValueError("theirs")

    with caplog.at_level(logging.ERROR, logger="solera.tasks"):
        with pytest.raises(ValueError):
            await tasks.spawn(fail(), awaited=True)
        await asyncio.sleep(0)
    assert caplog.text == ""


async def test_close_cancels_in_the_order_spawned_and_waits(caplog):
    tasks, cancelled = Tasks("t"), []

    async def hold(name):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.append(name)
            raise

    for name in ["c", "a", "b"]:
        tasks.spawn(hold(name), key=name)
    await asyncio.sleep(0)
    with caplog.at_level(logging.ERROR, logger="solera.tasks"):
        await tasks.close()
    assert cancelled == ["c", "a", "b"] and len(tasks) == 0
    assert caplog.text == "", "a cancelled task is no failure"


async def test_a_key_names_one_running_task():
    tasks, release = Tasks("t"), asyncio.Event()
    task = tasks.spawn(release.wait(), key="k")
    assert "k" in tasks and tasks.get("k") is task and list(tasks) == ["k"]
    with pytest.raises(ValueError, match="running already"):
        tasks.spawn(release.wait(), key="k")
    release.set()
    await task
    await asyncio.sleep(0)
    assert "k" not in tasks
