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
from .core import NEG, POS, Folded


class Broken(AssertionError):
    """The history breaks an invariant of the model: the model has no answer."""


def _check_keys(events: list[tuple], keys: tuple[str, ...]) -> None:
    """The model's brute force ranges over a fixed key space: refuse other keys."""
    found: set[str] = set()
    for ev in events:
        kind = ev[0]
        if kind in ("up", "clear", "items", "feed"):
            found |= {k for k, _ in ev[1]}
        elif kind == "keys":
            found |= set(ev[-1])
        elif kind == "commit" and len(ev) == 3:
            found |= {*ev[1], *ev[2]}
        elif kind == "retry":
            found.add(ev[1])
    if outside := found - set(keys):
        raise ValueError(f"keys {sorted(outside)} are outside the model's key space {list(keys)}")


def replay_staleness(events: list[tuple], form: str = "witness") -> staleness.World:
    """One consumer input (checks over items) after the history."""
    _check_keys(events, staleness.KEYS)
    w = staleness.World(form)
    for i, ev in enumerate(events):
        w.apply(ev)
        if bad := w.check():
            raise Broken(f"step {i} {ev}: {bad}")
    return w


def replay_fullrun(events: list[tuple], form: str = "clear", kind: str = "file") -> fullrun.World:
    """The cast (copy, tally, archive, report, checks) after the history, unsettled."""
    _check_keys(events, fullrun.KEYS)
    w = fullrun.World(form, kind)
    for i, ev in enumerate(events):
        try:
            w.apply(ev)
            bad = w.check()
        except Folded as e:
            bad = str(e)
        if bad:
            raise Broken(f"step {i} {ev}: {bad}")
    return w


def holds(w, asset: str | None = None, input: int = 0) -> dict[str, object]:
    """What a consumer holds per key on one input: (version, Cond), or "held"."""
    if isinstance(w, staleness.World):
        return dict(w.holds)
    a = w.assets[asset]
    return dict(a.held[input] if isinstance(a, fullrun.Ordinary) else a.holds)


def held(w, asset: str | None = None) -> bool:
    """Does the gate hold the consumer: is its upstream mid full run (P12)?"""
    if isinstance(w, staleness.World):
        return w.incomplete
    a = w.assets[asset]
    return isinstance(a, fullrun.Mirror) and a.held()


def owed(w, asset: str | None = None, input: int = 0) -> list[str]:
    """Keys whose upstream state now, under the conditions now, differs from what
    is held; with a full run due, every key upstream holds."""
    if isinstance(w, staleness.World):
        return sorted(w.brute()[0])
    a = w.assets[asset]
    if isinstance(a, fullrun.Ordinary):
        log = a.inputs[input]
        return (
            sorted(log.scan(log.head)) if a.full_due else a.progress[input].candidates(log.head, NEG, a.cond)
        )
    return a.progress.candidates(a.log.head, NEG, a.cond)


def due(w, asset: str | None = None, input: int = 0) -> list[str]:
    """Owed keys a default run would take now: none while held, and failing keys
    wait for their retry."""
    if held(w, asset):
        return []
    if isinstance(w, staleness.World):
        return sorted(w.brute()[1])
    return owed(w, asset, input)


def next_plan(w, asset: str | None = None, n: int = 2) -> tuple[list[str], str]:
    """The keys the next batch must take, and where its range ends: nothing while held."""
    if held(w, asset):
        return [], POS
    if isinstance(w, staleness.World):
        taken, hi, _ = w.prog.plan(w.up.head, w.cursor or NEG, n, w.cond, w.due)
        return taken, hi
    s = w.assets[asset].next_plan(n)
    return s.taken, s.hi
