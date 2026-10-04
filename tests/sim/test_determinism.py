"""One program, run twice in one process (as Hypothesis re-runs an example),
runs the same callbacks in the same order: a failure the simulation finds
can then be replayed, shrunk and pinned (tests/sim/determinism.py)."""

from pathlib import Path

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
