"""Shrink a failing simulation replay when Hypothesis gives up (it stops after
five minutes of slow progress on long runs).

    uv run python -m tests.sim.shrink replay.py "items after automations alone"

`replay.py` is the failing case as Hypothesis printed it — `state = Simulation()`,
`state.boot(...)`, one line per step, `state.teardown()` — with the invariant
calls between steps kept: they run the loop, so they are part of the run. A
step is a rule line and the invariant calls after it. Each candidate runs in a
process of its own, killed past 4 GB of resident memory, `SHRINK_JOBS` of them
at once (4 by default); the shortest list of steps whose run still prints the
signature is written beside the input as `replay_min.py`."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

MAX_RSS_KB = 4 * 2**20
TIMEOUT = 1800.0


def _run_capped(path: Path) -> str:
    """Run a replay; its output, or '' if it outgrew the memory cap or the
    timeout. Portable: polls the child's resident size with `ps`."""

    with tempfile.TemporaryFile("w+") as out:
        child = subprocess.Popen(
            [sys.executable, str(path)],
            env={**os.environ, "PYTHONPATH": os.getcwd()},
            stdout=out,
            stderr=subprocess.STDOUT,
            text=True,
        )
        started = time.monotonic()
        while child.poll() is None:
            rss = subprocess.run(["ps", "-o", "rss=", "-p", str(child.pid)], capture_output=True, text=True)
            if int(rss.stdout.strip() or 0) > MAX_RSS_KB or time.monotonic() - started > TIMEOUT:
                child.kill()
                child.wait()
                return ""
            time.sleep(1.0)
        out.seek(0)
        return out.read()


def _steps(lines: list[str], invariants: set[str]) -> tuple[list[str], list[list[str]]]:
    head = [x for x in lines if x.startswith(("from ", "import ", "state = Simulation", "state.boot"))]
    steps: list[list[str]] = []
    for line in lines:
        if not line.startswith("state.") or line.startswith(("state.boot", "state.teardown")):
            continue
        if line.split("(", 1)[0].removeprefix("state.") in invariants and steps:
            steps[-1].append(line)
        else:
            steps.append([line])
    return head, steps


def main(path: str, signature: str) -> None:
    from .machine import Simulation

    invariants = {
        name
        for name in dir(Simulation)
        if getattr(getattr(Simulation, name), "hypothesis_stateful_invariant", None)
    }
    source = Path(path)
    head, steps = _steps([x.strip() for x in source.read_text().splitlines() if x.strip()], invariants)
    jobs = int(os.environ.get("SHRINK_JOBS", "4"))

    def program(ss: list[list[str]]) -> str:
        return "\n".join(head + [x for s in ss for x in s] + ["state.teardown()"]) + "\n"

    def fails(ss: list[list[str]], slot: int = 0) -> bool:
        work = source.with_name(f"_shrinking{slot}.py")
        work.write_text(program(ss))
        try:
            return signature in _run_capped(work)
        finally:
            work.unlink(missing_ok=True)

    if not fails(steps):
        sys.exit("the replay does not fail with that signature")
    n = 2
    with ThreadPoolExecutor(jobs) as pool:
        while len(steps) >= 2:
            chunk = max(1, len(steps) // n)
            candidates = [steps[:i] + steps[i + chunk :] for i in range(0, len(steps), chunk)]
            found = None
            for start in range(0, len(candidates), jobs):  # a round of `jobs`; the first that fails wins
                batch = [c for c in candidates[start : start + jobs] if c]
                results = list(pool.map(fails, batch, range(len(batch))))
                found = next((c for c, f in zip(batch, results, strict=True) if f), None)
                if found is not None:
                    break
            if found is not None:
                steps, n = found, max(n - 1, 2)
                print(f"{len(steps)} steps", flush=True)
            elif chunk == 1:
                break
            else:
                n = min(len(steps), n * 2)
    out = source.with_name(source.stem + "_min.py")
    out.write_text(program(steps))
    print(f"{len(steps)} steps: {out}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
