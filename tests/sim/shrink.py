"""Shrink a failing simulation replay when Hypothesis gives up (it stops after
five minutes of slow progress on long runs).

    uv run python -m tests.sim.shrink replay.py "items after automations alone"

`replay.py` is the failing case as Hypothesis printed it — `state = Simulation()`,
`state.boot(...)`, one line per step, `state.teardown()` — with the invariant
calls between steps kept: they run the loop, so they are part of the run. A
step is a rule line and the invariant calls after it. Each candidate runs in a
process of its own, under a memory cap; the shortest list of steps whose run
still prints the signature is written beside the input as `replay_min.py`."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

CAP = ["systemd-run", "--user", "--scope", "-q", "-p", "MemoryMax=4G", "-p", "CPUQuota=150%"]


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
    work = source.with_name("_shrinking.py")
    cap = CAP if os.environ.get("SHRINK_CAP", "1") == "1" else []

    def program(ss: list[list[str]]) -> str:
        return "\n".join(head + [x for s in ss for x in s] + ["state.teardown()"]) + "\n"

    def fails(ss: list[list[str]]) -> bool:
        work.write_text(program(ss))
        run = subprocess.run(
            [*cap, sys.executable, str(work)],
            env={**os.environ, "PYTHONPATH": os.getcwd()},
            capture_output=True,
            text=True,
            timeout=1800,
        )
        return signature in run.stdout + run.stderr

    if not fails(steps):
        sys.exit("the replay does not fail with that signature")
    n = 2
    while len(steps) >= 2:
        chunk = max(1, len(steps) // n)
        for i in range(0, len(steps), chunk):
            candidate = steps[:i] + steps[i + chunk :]
            if candidate and fails(candidate):
                steps, n = candidate, max(n - 1, 2)
                print(f"{len(steps)} steps", flush=True)
                break
        else:
            if chunk == 1:
                break
            n = min(len(steps), n * 2)
    work.unlink(missing_ok=True)
    out = source.with_name(source.stem + "_min.py")
    out.write_text(program(steps))
    print(f"{len(steps)} steps: {out}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
