"""Reproduction: a recount never lands while commits keep arriving.

    uv run python -m pytest bench/keys/repro_recount_discarded.py

`Upkeep._maintenance` drops a recount's result when any commit landed while
it ran (`if current is index`), and tries again after `recount_interval`. A
cold full scan takes about 14 s at 10M keys and minutes at 100M
(bench/keys/results.md), so an index committing more often than that keeps
an approximate count for as long as the commits continue — §6 says the
recount makes it exact again. Here each recount is held open until a commit
lands; three are dropped, and the fourth, with no commit meanwhile, lands.
"""

import asyncio
import dataclasses
import threading

from solera.keys.index import KeyIndex
from solera.sdk import Output, Project, asset

from tests.conftest import data  # noqa: F401 — autouse: default stores in a temp dir
from tests.server.test_keys import engine_for, run, state  # noqa: F401 — `state` is a fixture


async def test_a_commit_during_the_recount_discards_it(state, monkeypatch):  # noqa: F811
    rows = [{"id": str(i)} for i in range(5)]

    @asset(outputs=Output("items", key="id"))
    def items():
        return rows

    engine = engine_for(state, Project(assets=[items]), recount_interval=0)
    await engine.initialize()
    await run(engine, ["items"])
    key = ("items", "")
    state.model.indexes[key] = dataclasses.replace(state.model.indexes[key], count=3, count_exact=False)

    # A recount still running when the next commit lands, as a large index's is.
    started, release = threading.Event(), threading.Event()
    recount, counted = KeyIndex.recount, []

    async def slow_recount(self, page=100_000):
        started.set()
        await asyncio.to_thread(release.wait, 10)
        counted.append(await recount(self, page))
        return counted[-1]

    monkeypatch.setattr(KeyIndex, "recount", slow_recount)
    for n in range(3):
        if not engine.upkeep.jobs:
            await engine.upkeep.tick()
        assert engine.upkeep.jobs, "a recount should be running"
        await asyncio.to_thread(started.wait, 10)
        started.clear()
        rows.append({"id": f"new-{n}"})
        await run(engine, ["items"])  # a commit lands while it runs
        release.set()
        await asyncio.gather(*engine.upkeep.jobs.values())  # it finishes, and is dropped
        release.clear()
        index = state.model.indexes[key]
        assert not index.count_exact and index.count != len(rows), (n, index.count, len(rows))
        await engine.upkeep.tick()  # and starts over

    # Once the commits stop, the next recount lands.
    release.set()
    await asyncio.gather(*engine.upkeep.jobs.values())
    await engine.upkeep.tick()
    index = state.model.indexes[key]
    assert index.count_exact and index.count == len(rows)
    # Each recount ran to completion and was exact for the index it pinned (5, 6, 7 keys);
    # the commit after it was exact too, so recount + its `added - removed` was the answer.
    assert counted == [5, 6, 7, 8] and engine.upkeep.last_error is None
