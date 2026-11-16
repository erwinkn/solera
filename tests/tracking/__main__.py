"""Run the tracking model: random histories, calibrations, shrinking.

    uv run python -m tests.tracking staleness                 # 3,000 histories
    uv run python -m tests.tracking fullrun --kind table      # on table stores
    uv run python -m tests.tracking fullrun --off gate --shrink
    uv run python -m tests.tracking fullrun --ablate          # every rule switched off in turn
    uv run python -m tests.tracking fullrun --stats           # the work the full runs did

A failing history is shrunk (--shrink) to the shortest list of events that
still breaks the same invariant with every other rule on, and printed as the
design's cast would tell it.
"""

from __future__ import annotations

import argparse
import random
from collections import Counter

from . import fullrun, staleness

MODELS = {"staleness": staleness, "fullrun": fullrun}
STEPS = {"staleness": 40, "fullrun": 60}


def run(model: str, events: list[tuple], kind: str, off: frozenset[str]):
    if model == "staleness":
        return staleness.run(events, off)
    return fullrun.run(events, kind, off)


def sweep(model: str, kind: str, off: frozenset[str], histories: int, seed: int, steps: int):
    """The first failing history of a sweep, and how many failed."""
    first, failed = None, 0
    for s in range(seed, seed + histories):
        events = MODELS[model].history(random.Random(s), steps)
        if run(model, events, kind, off):
            failed += 1
            first = first or (s, events)
    return first, failed


def shrink(model: str, events: list[tuple], kind: str, off: frozenset[str]) -> list[tuple]:
    """The shortest sublist of events (with simpler events) that still fails with
    the same invariant under off, and passes with every rule on."""

    def signature(evs: list[tuple]) -> str | None:
        r = run(model, evs, kind, off)
        return None if r is None else r[1].split(":")[0].split(" ")[0] + "/" + _shape(r[1])

    def fails(evs: list[tuple]) -> bool:
        return signature(evs) == target and (not off or run(model, evs, kind, frozenset()) is None)

    target = signature(events)
    chunk = len(events) // 2
    while chunk >= 1:
        i, changed = 0, False
        while i < len(events):
            candidate = events[:i] + events[i + chunk :]
            if fails(candidate):
                events, changed = candidate, True
            else:
                i += chunk
        if not changed:
            chunk //= 2
    for i in range(len(events)):
        for simpler in _simpler(events[i]):
            candidate = events[:i] + [simpler] + events[i + 1 :]
            if fails(candidate):
                events = candidate
    return events


def _shape(message: str) -> str:
    """An invariant's message with its keys and numbers taken out."""
    return " ".join(w for w in message.split() if not any(c.isdigit() for c in w))[:60]


def _simpler(ev: tuple):
    for j, field in enumerate(ev):
        if field is True:
            yield ev[:j] + (False,) + ev[j + 1 :]
        if isinstance(field, tuple) and len(field) > 1:
            for m in range(len(field)):
                yield ev[:j] + (field[:m] + field[m + 1 :],) + ev[j + 1 :]


def trace(model: str, events: list[tuple], kind: str, off: frozenset[str]) -> str:
    r = run(model, events, kind, off)
    if r is None:
        return "passes"
    step, message, world = r
    return "\n".join([*world.trace, f"=> {message}"])


def main() -> None:
    ap = argparse.ArgumentParser(prog="python -m tests.tracking", description=__doc__.split("\n")[0])
    ap.add_argument("model", choices=MODELS)
    ap.add_argument("--kind", default="file", choices=("file", "table"), help="fullrun: the store kind")
    ap.add_argument("--histories", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--steps", type=int, default=None)
    ap.add_argument("--off", default="", help="rules switched off, comma-separated")
    ap.add_argument("--ablate", action="store_true", help="switch each rule off in turn")
    ap.add_argument("--shrink", action="store_true", help="shrink and print the first failing history")
    ap.add_argument("--stats", action="store_true", help="fullrun: what the sweep did")
    ap.add_argument(
        "--calibrate", action="store_true", help="print a shrunk history per rule, for histories.py"
    )
    a = ap.parse_args()
    rules = MODELS[a.model].RULES
    steps = a.steps or STEPS[a.model]
    off = frozenset(x for x in a.off.split(",") if x)
    if a.calibrate:
        for rule in rules:
            kind = "table" if rule == "hold_back" and a.model == "fullrun" else a.kind
            first, _ = sweep(a.model, kind, frozenset({rule}), a.histories, a.seed, steps)
            events = shrink(a.model, first[1], kind, frozenset({rule})) if first else None
            print(f"    ({a.model!r}, {kind!r}, {rule!r}): {events!r},")
        return
    for case in [(r,) for r in rules] if a.ablate else [tuple(off)]:
        first, failed = sweep(a.model, a.kind, frozenset(case), a.histories, a.seed, steps)
        label = a.model + (f" ({a.kind})" if a.model == "fullrun" else "")
        print(f"{label}, off {list(case) or 'nothing'}: {failed} of {a.histories} histories fail")
        if first and a.shrink:
            events = shrink(a.model, first[1], a.kind, frozenset(case))
            print(f"  seed {first[0]}, shrunk to {len(events)} events: {events}")
            print("  " + trace(a.model, events, a.kind, frozenset(case)).replace("\n", "\n  "))
    if a.stats and a.model == "fullrun":
        print(f"  {dict(stats(a.kind, a.histories, a.seed, steps))}")


def stats(kind: str, histories: int, seed: int, steps: int) -> Counter:
    total: Counter = Counter()
    for s in range(seed, seed + histories):
        events = fullrun.history(random.Random(s), steps)
        if fullrun.run(events, kind) is not None:
            continue
        w = fullrun.World(kind)
        for ev in events:
            w.apply(ev)
        w.settle()
        total.update(w.stats)
        total.update({f"{k} held": v for k, v in w.held.items()})
        total["copy entries"] += sum(len(e) for e in w.copy.output.log.entries)
    return total


if __name__ == "__main__":
    main()
