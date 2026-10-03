"""Trace validation: check that simulation runs are behaviours of a spec
(docs/verification.md, "Trace validation").

    uv run python spec/tla/check-trace.py SPEC              run the simulation (SOLERA_SIM_EXAMPLES, 20) and check each run
    uv run python spec/tla/check-trace.py SPEC RUN.jsonl…   check runs exported before (SOLERA_SIM_REQUESTS=DIR)

SPEC is one of `SPECS` below. The simulation exports
each run's object requests, tagged by actor (`tests/sim/core.py`,
`Objects.trace`). The requests on the objects a spec describes become a
TLA+ module, `{Spec}TraceLog`, holding `Trace`, a sequence of

    [e |-> actor, req |-> "list" | "get" | "readback" | "create" | "delete",
     obj |-> the object's kind, n |-> its number, out |-> how it ended,
     listed |-> what a list returned]

and TLC searches the spec's trace module (`{Spec}Trace.tla`) for a
behaviour that makes them, one by one. It reports the run valid, or the
first request no behaviour explains, with the requests of that actor
before it: either the code does what the design does not allow, or the
spec misses a step. A spec's trace module defines `NotDone` (violated
once every request is explained) and prints `<<"explained", i>>` as it
gets further.

Actors are `e{n}` for the simulation's engine n and `r{k}` for its k-th
read-only open. TLC_HEAP (4g) bounds each TLC run; one worker (the report
needs it).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
JAR = HERE / ".tools" / "tla2tools-1.7.4.jar"


@dataclass
class Spec:
    module: str  # the spec; its trace module is f"{module}Trace"
    path: re.Pattern  # an object it describes: groups (kind, number or None)
    config: str  # the trace module's TLC config


# The specs checked against simulation runs. The first, the segment journal
# (Journal.tla), retired with its code (docs/verification.md); the journal
# object (JournalObject.tla) is next.
SPECS: dict[str, Spec] = {}


def requests(spec: Spec, path: Path) -> list[dict]:
    """The requests of one exported run on the objects `spec` describes."""

    out: list[dict] = []
    readers = 0
    last: dict[str, dict] = {}  # each actor's previous request
    for line in path.read_text().splitlines():
        r = json.loads(line)
        if r.get("reader"):
            readers += 1
            continue
        m = spec.path.search(r.get("path", ""))
        if "kind" not in r or m is None:
            continue
        who = r["who"]
        e = f"e{who[1]}" if who and who[0] == "engine" else f"r{readers}" if who is None and readers else None
        if e is None:
            raise SystemExit(f"{path}: a request by {who}: {r}")
        ev = {"e": e, "req": r["kind"], "obj": m.group(1), "n": int(m.group(2) or 0), "out": r["outcome"]}
        found = (spec.path.search(p) for p in r.get("listed") or [])
        ev["listed"] = sorted(int(f.group(2)) for f in found if f and f.group(2))
        before = last.get(e) or {}
        found = ("create", "exists", ev["obj"], ev["n"])  # a create found something in its way
        if ev["req"] == "get" and tuple(before.get(k) for k in ("req", "out", "obj", "n")) == found:
            ev["req"] = "readback"  # `solera.objects.create` reads back what is in its way
        last[e] = ev
        out.append(ev)
    return out


def tla(value) -> str:
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "{" + ", ".join(tla(v) for v in value) + "}"
    return str(value)


def log_module(spec: Spec, events: list[dict]) -> str:
    actors = sorted({ev["e"] for ev in events})
    top = max([ev["n"] for ev in events] + [n for ev in events for n in ev["listed"]] + [1])
    records = ",\n    ".join(
        "[" + ", ".join(f"{k} |-> {tla(ev[k])}" for k in ("e", "req", "obj", "n", "out", "listed")) + "]"
        for ev in events
    )
    return (
        f"---- MODULE {spec.module}TraceLog ----\nEXTENDS Sequences\n"
        f"TraceActors == {tla(actors)}\nTraceMaxN == {top + 1}\n"
        f"Trace == <<\n    {records}\n    >>\n====\n"
    )


def check(spec: Spec, path: Path, work: Path) -> bool:
    events = requests(spec, path)
    if not events:
        print(f"{path.name}: no requests")
        return True
    trace = f"{spec.module}Trace"
    for name in (spec.module, trace):
        (work / f"{name}.tla").write_text((HERE / f"{name}.tla").read_text())
    (work / f"{trace}Log.tla").write_text(log_module(spec, events))
    (work / f"{trace}.cfg").write_text(spec.config)
    heap = os.environ.get("TLC_HEAP", "4g")
    cmd = ["java", f"-Xmx{heap}", "-cp", str(JAR), "tlc2.TLC", "-workers", "1"]
    cmd += ["-metadir", str(work / "states"), "-config", f"{trace}.cfg", f"{trace}.tla"]
    log = subprocess.run(cmd, cwd=work, capture_output=True, text=True).stdout
    (work / f"{path.stem}.log").write_text(log)
    violated = re.search(r"Invariant (\w+) is violated", log)
    if violated and violated.group(1) == "NotDone":
        print(f"{path.name}: valid ({len(events)} requests)")
        return True
    if violated:
        print(f"{path.name}: the run breaks {violated.group(1)}")
        return False
    done = max([int(n) for n in re.findall(r'<<"explained", (\d+)>>', log)] + [0])
    if "Finished in" not in log or done >= len(events):
        print(log[-3000:])
        print(f"{path.name}: TLC failed")
        return False
    print(f"{path.name}: {done} of {len(events)} requests explained; no behaviour makes request {done + 1}:")
    actor = events[done]["e"]
    for j, ev in [(j, ev) for j, ev in enumerate(events[: done + 1]) if ev["e"] == actor][-8:]:
        print(f"    {j + 1:5d}  {fmt(ev)}")
    start = max(0, done - 6)
    others = [(start + j, ev) for j, ev in enumerate(events[start:done]) if ev["e"] != actor]
    if others:
        print("  the requests just before it, of other actors:")
        for j, ev in others:
            print(f"    {j + 1:5d}  {fmt(ev)}")
    return False


def fmt(ev: dict) -> str:
    listed = f" -> {ev['listed']}" if ev["req"] == "list" else ""
    return f"{ev['e']:4s} {ev['req']:8s} {ev['obj']:11s} {ev['n'] or '':>4} {ev['out']}{listed}"


def simulate(out: Path) -> list[Path]:
    env = {**os.environ, "SOLERA_SIM_REQUESTS": str(out)}
    pytest = [
        sys.executable,
        "-m",
        "pytest",
        "tests/sim/test_simulation.py",
        "-q",
        "-x",
        "-p",
        "no:cacheprovider",
    ]
    subprocess.run(pytest, cwd=ROOT, env=env)
    return sorted(out.glob("*.jsonl"))


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in SPECS:
        raise SystemExit(f"usage: check-trace.py {{{','.join(SPECS)}}} [RUN.jsonl ...]")
    if not JAR.exists():
        raise SystemExit(f"{JAR} is missing: run spec/tla/check-journal.sh once to download it")
    spec = SPECS[sys.argv[1]]
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        runs = [Path(a) for a in sys.argv[2:]] or simulate(work / "runs")
        failed = [run for run in runs if not check(spec, run, work)]
        if failed and os.environ.get("TLA_LOGS"):
            logs = Path(os.environ["TLA_LOGS"])
            logs.mkdir(parents=True, exist_ok=True)
            for run in failed:
                (logs / f"{run.stem}.log").write_text((work / f"{run.stem}.log").read_text())
        print(f"{len(runs) - len(failed)} of {len(runs)} runs valid")
        return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
