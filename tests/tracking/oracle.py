"""The tracking model as an oracle for the build: replay a history, then ask
what the engine must answer. The import path and these functions are kept
stable for the build units that test against them; see README.md.

    from tests.tracking import oracle

    w = oracle.replay_staleness([("up", (("k1", 1),), False), ("plan", 2)])
    oracle.holds(w)        # {} : checks holds nothing yet
    oracle.due(w)          # ["k1"]
    oracle.next_plan(w)    # (["k1"], POS): the next batch takes k1, its range runs to +inf
"""

from __future__ import annotations

from . import fullrun, staleness
from .core import NEG


class Broken(AssertionError):
    """The history breaks an invariant of the model: the model has no answer."""


def replay_staleness(events: list[tuple], form: str = "witness") -> staleness.World:
    """One consumer input (checks over items) after the history."""
    w = staleness.World(form)
    for i, ev in enumerate(events):
        w.apply(ev)
        if bad := w.check():
            raise Broken(f"step {i} {ev}: {bad}")
    return w


def replay_fullrun(events: list[tuple], form: str = "floor", kind: str = "file") -> fullrun.World:
    """The cast (copy, tally, archive, report, checks) after the history, unsettled."""
    w = fullrun.World(form, kind)
    for i, ev in enumerate(events):
        w.apply(ev)
        if bad := w.check():
            raise Broken(f"step {i} {ev}: {bad}")
    return w


def holds(w, asset: str | None = None, input: int = 0) -> dict[str, object]:
    """What a consumer holds per key on one input: (version, Cond), or "held"."""
    if isinstance(w, staleness.World):
        return dict(w.holds)
    a = w.assets[asset]
    return dict(a.held[input] if isinstance(a, fullrun.Ordinary) else a.holds)


def owed(w, asset: str | None = None, input: int = 0) -> list[str]:
    """Keys whose upstream state now, under the conditions now, differs from what is held."""
    if isinstance(w, staleness.World):
        return sorted(w.brute()[0])
    a = w.assets[asset]
    prog, log = (
        (a.progress[input], a.inputs[input]) if isinstance(a, fullrun.Ordinary) else (a.progress, a.log)
    )
    return prog.candidates(log.head, NEG, a.cond)


def due(w, asset: str | None = None, input: int = 0) -> list[str]:
    """Owed keys a default run would take now (failing keys wait for their retry)."""
    if isinstance(w, staleness.World):
        return sorted(w.brute()[1])
    return owed(w, asset, input)


def next_plan(w, asset: str | None = None, n: int = 2) -> tuple[list[str], str]:
    """The keys the next batch must take, and where its range ends."""
    if isinstance(w, staleness.World):
        taken, hi, _ = w.prog.plan(w.up.head, w.cursor or NEG, n, w.cond, w.due)
        return taken, hi
    s = w.assets[asset].next_plan(n)
    return s.taken, s.hi
