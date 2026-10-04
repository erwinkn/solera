"""One program, run twice in one process (as Hypothesis re-runs an example),
runs the same callbacks in the same order: a failure the simulation finds
can then be replayed, shrunk and pinned (tests/sim/determinism.py)."""

import asyncio
import random
from pathlib import Path

from .core import SimLoop
from .determinism import _in_process

PROGRAM = """
from tests.sim.machine import Simulation
from tests.sim.world import Fate
state = Simulation()
state.boot(seed=7, store='table')
state.commit_feed(keys={'k1', 'k2', 'k3'}, op='upsert', version='1')
state.doom_next_worker(fate=Fate('die', 'store', 'after', 0.0))
state.submit(asset='copy', mode='full', partitions='latest', upstream=True)
state.wait(seconds=45.0)
state.takeover(change=None, zombie=5.0)
state.commit_feed(keys={'k2'}, op='remove', version='2')
state.doom_next_worker(fate=Fate('pause', 'result', 'before', 20.0))
state.restart(clean=False, down=5.0)
state.wait(seconds=120.0)
state.teardown()
"""


def test_a_program_runs_the_same_twice_in_one_process(tmp_path):
    """Kills, takeovers and restarts cancel many tasks at once: they were
    cancelled in `asyncio.all_tasks()` order, a set ordered by address, so a
    second run in the same process could take another interleaving."""

    program = tmp_path / "program.py"
    program.write_text(PROGRAM)
    a, b, codes = _in_process(Path(program))
    assert a, "nothing was traced"
    first = next((i for i, (x, y) in enumerate(zip(a, b, strict=False)) if x != y), None)
    assert first is None and len(a) == len(b), (
        f"the runs part at callback {first}: {a[first] if first is not None else ''} / "
        f"{b[first] if first is not None else ''}"
    )


def _interleaving(seed: int | None) -> list[str]:
    """Three tasks woken by one event, each taking three steps and scheduling
    a callback of its own between them: the order they ran in."""

    loop = SimLoop(None if seed is None else random.Random(seed))
    ran: list[str] = []

    async def worker(name: str, go: asyncio.Event):
        await go.wait()
        for step in range(3):
            ran.append(f"{name}{step}")
            loop.call_soon(ran.append, f"{name}{step}+")  # runs before this task's next step
            await asyncio.sleep(0)

    async def main():
        go = asyncio.Event()
        tasks = [asyncio.ensure_future(worker(name, go)) for name in "abc"]
        await asyncio.sleep(0)
        go.set()
        await asyncio.gather(*tasks)

    loop.run_until_complete(main())
    loop.close()
    return ran


def test_a_seed_picks_one_interleaving_of_independent_tasks():
    """Tasks ready together run in an order drawn from the seed; each task's
    own steps and callbacks keep asyncio's order, and a seed always draws the
    same interleaving."""

    fifo = _interleaving(None)
    assert fifo[:7] == ["a0", "b0", "c0", "a0+", "a1", "b0+", "b1"]
    orders = {seed: _interleaving(seed) for seed in range(8)}
    assert all(_interleaving(seed) == order for seed, order in orders.items())
    assert len({tuple(o) for o in orders.values()}) > 1
    for order in orders.values():
        assert sorted(order) == sorted(fifo)
        for name in "abc":
            own = [x for x in order if x.startswith(name)]
            assert own == [f"{name}{i}{plus}" for i in range(3) for plus in ("", "+")]
