"""Is a simulation run deterministic? Run one program twice, each in a fresh
process, recording every callback the loop runs, in order: what it is (a
task's coroutine, a function's name: never an address) and the virtual
time. The first place the two runs part is where the leak is.

    uv run python -m tests.sim.determinism replay.py                # two fresh processes
    uv run python -m tests.sim.determinism --in-process replay.py   # twice in one process

`replay.py` is a printed failing case (or any program that drives a
`Simulation`). The second form runs it twice in one process, as Hypothesis
re-runs an example: what one run leaves behind (module or class state, a
cache, a task) shows up as a difference in the next. Exit status 0: the two
runs ran the same callbacks in the same order; 1: they parted, and the
report says where."""

from __future__ import annotations

import asyncio
import hashlib
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def label(handle) -> str:
    """What a ready callback is, the same in every process."""

    callback = handle._callback
    owner = getattr(callback, "__self__", None)
    if isinstance(owner, asyncio.Task):
        coro = owner.get_coro()
        where = getattr(coro, "cr_frame", None)
        line = where.f_lineno if where is not None else "-"
        return f"step {getattr(coro, '__qualname__', type(coro).__name__)}@{line}"
    if isinstance(owner, asyncio.Future):
        return f"future {getattr(callback, '__name__', '?')}"
    name = getattr(callback, "__qualname__", None) or type(callback).__name__
    return name


_LINES: list[str] | None = None  # in-process mode: the current run's callbacks


def record_in_memory() -> None:
    """From here on, append one label per simulation-loop callback to `_LINES`."""

    from asyncio import events

    from .core import SimLoop

    run = events.Handle._run

    def traced(self):
        loop = self._loop
        if _LINES is not None and isinstance(loop, SimLoop):
            _LINES.append(f"{loop.time():.6f} {label(self)}")
        return run(self)

    events.Handle._run = traced
    # And every scheduling, with who scheduled it: an order that differs between runs
    # differs first where something was scheduled in another order.
    soon, at = SimLoop.call_soon, SimLoop.call_at

    def who() -> str:
        try:
            task = asyncio.current_task()
        except RuntimeError:
            return "-"
        if task is None:
            return "-"
        coro = task.get_coro()
        frame = getattr(coro, "cr_frame", None)
        return f"{getattr(coro, '__qualname__', '?')}@{frame.f_lineno if frame else '-'}"

    def call_soon(self, callback, *args, context=None):
        handle = soon(self, callback, *args, context=context)
        if _LINES is not None:
            _LINES.append(f"{self.time():.6f}   soon {label(handle)} by {who()}")
        return handle

    def call_at(self, when, callback, *args, context=None):
        handle = at(self, when, callback, *args, context=context)
        if _LINES is not None:
            _LINES.append(f"{self.time():.6f}   at {when:.6f} {label(handle)} by {who()}")
        return handle

    SimLoop.call_soon, SimLoop.call_at = call_soon, call_at


def record(path: str) -> None:
    """From here on, append one line per callback run to `path`."""

    from asyncio import events

    out = open(path, "w", buffering=1 << 20)  # noqa: SIM115 — closed at exit
    run = events.Handle._run

    from .core import SimLoop

    def traced(self):
        loop = self._loop
        # The simulation's loop only: a loop of real time (a compaction's own, on a thread
        # while the simulation waits for it) runs real threads, in their own order.
        if isinstance(loop, SimLoop):
            out.write(f"{loop.time():.6f} {label(self)}\n")
        return run(self)

    events.Handle._run = traced
    import atexit

    atexit.register(out.close)


def _trace(program: Path, out: Path, env: dict) -> int:
    runner = (
        "import runpy, sys\n"
        "from tests.sim.determinism import record\n"
        f"record({str(out)!r})\n"
        f"runpy.run_path({str(program)!r}, run_name='__main__')\n"
    )
    done = subprocess.run([sys.executable, "-c", runner], env=env, capture_output=True, text=True)
    return done.returncode


def _in_process(program: Path) -> tuple[list[str], list[str], list[int]]:
    import runpy

    global _LINES
    record_in_memory()
    runs, codes = [], []
    for _ in range(2):
        _LINES = []
        try:
            runpy.run_path(str(program), run_name="__main__")
            codes.append(0)
        except BaseException as error:  # noqa: BLE001 — a failing case is expected to raise
            codes.append(f"{type(error).__name__}")
        runs.append(_LINES)
    _LINES = None
    return runs[0], runs[1], codes


def main(program: str, in_process: bool = False) -> int:
    program_path = Path(program).resolve()
    if in_process:
        a, b, codes = _in_process(program_path)
        return _compare(a, b, codes)
    env = {**os.environ, "PYTHONPATH": os.getcwd()}
    with tempfile.TemporaryDirectory(prefix="determinism-") as d:
        runs = [Path(d) / "a.trace", Path(d) / "b.trace"]
        codes = [_trace(program_path, run, env) for run in runs]
        a, b = (r.read_text().splitlines() for r in runs)
    return _compare(a, b, codes)


def _compare(a: list[str], b: list[str], codes) -> int:
    digests = [hashlib.sha256("\n".join(x).encode()).hexdigest()[:16] for x in (a, b)]
    print(f"run 1: exit {codes[0]}, {len(a)} callbacks, digest {digests[0]}")
    print(f"run 2: exit {codes[1]}, {len(b)} callbacks, digest {digests[1]}")
    for i, (x, y) in enumerate(zip(a, b, strict=False)):
        if x != y:
            print(f"they part at callback {i}:")
            for j in range(max(0, i - 8), i):
                print(f"    {a[j]}")
            print(f"  1: {x}")
            print(f"  2: {y}")
            for j in range(i + 1, min(len(a), len(b), i + 6)):
                print(f"  1: {a[j]}    2: {b[j]}")
            return 1
    if len(a) != len(b):
        print(f"one run stopped early, at callback {min(len(a), len(b))}")
        return 1
    print("the same callbacks, in the same order")
    return 0


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--in-process"]
    sys.exit(main(args[0], in_process="--in-process" in sys.argv))
