"""The tracking core against the tracking model (tests/tracking, SOLE-7's
oracle): random staleness histories replayed through both, step by step.

After every step the core's owed keys and classes equal what the model says
the consumer owes, and every plan takes exactly the model's keys with the
model's range. The core reads a real change index (one layer per upstream
commit), applies its own journaled events, and survives restarts through
snapshot and restore.

Clears are left out of the histories: the core does not build them yet (the
full-run form's clear lands with SOLE-6's follow-up).
"""

from __future__ import annotations

import random

import pytest
from solera._native import ChangeIndex, Tracking

oracle = pytest.importorskip("tests.tracking.oracle")
from tests.tracking import staleness  # noqa: E402
from tests.tracking.core import ABSENT, HELD, POS  # noqa: E402

EDGE, ITEMS, OWN = b"checks/items", b"items", b"checks"


def conditions(cond) -> dict:
    pat = None if cond.pat is None else {"include": [{"glob": k} for k in sorted(cond.pat)], "exclude": []}
    return {"patterns": pat, "context": str(cond.ctx).encode(), "definition": repr(cond.defn).encode()}


class Core:
    """The core over one keyed input, with the logs it reads."""

    def __init__(self, root) -> None:
        self.ix = ChangeIndex(f"file://{root}")
        self.logs: dict[bytes, tuple[list, int, int]] = {ITEMS: ([], 0, 0), OWN: ([], 0, 0)}
        self.own: set[str] = set()
        self.t = self.boot(Tracking())
        self.t.apply(Tracking.event_edge_open(EDGE, ITEMS, True))
        self.pending: tuple[bytes, list[str], object] | None = None

    def boot(self, t: Tracking) -> Tracking:
        for out, (layers, head, live) in self.logs.items():
            t.attach(out, self.ix)
            t._set_output(out, layers, head, live)
        return t

    def commit(self, out: bytes, entries: list[tuple[str, object, object]]) -> None:
        layers, head, live = self.logs[out]
        layer = self.ix.write_commit(f"{out.decode()}/{head + 1}", head + 1, entries, live_before=live)
        live += sum(r is None for _, _, r in entries) - sum(n is None for _, n, _ in entries)
        self.logs[out] = ([*layers, layer], head + 1, live)
        self.t._set_output(out, *self.logs[out])

    def sync_own(self, holds: dict) -> None:
        """The consumer's own output holds a key exactly when the consumer does."""
        now = {k for k, v in holds.items() if v is not ABSENT}
        changes = [(k, 1, None) for k in sorted(now - self.own)] + [
            (k, None, 1) for k in sorted(self.own - now)
        ]
        if changes:
            self.commit(OWN, sorted(changes))
        self.own = now

    def request(self, w, **kw) -> dict:
        """A plan request; a full run's first batch starts outcomes over too."""
        inp = {"edge": EDGE, "conditions": conditions(w.cond), "own": OWN}
        if not kw.get("reset"):
            inp["not_due"] = [k for k in staleness.KEYS if w.waiting(k, w.up.head)]
        return {"inputs": [inp], **kw}

    def owed(self, w) -> list[tuple[str, str]]:
        keys, nxt = self.t.owed(EDGE, conditions(w.cond), OWN, limit=100)
        assert nxt is None
        return keys


def model_owed(w) -> list[tuple[str, str]]:
    head = w.up.head
    out = []
    for k in sorted(staleness.KEYS):
        h, want = w.holds.get(k, ABSENT), w.prog.wanted(k, head, w.cond)
        if h == want:
            continue
        out.append((k, "added" if h is ABSENT else "removed" if want is ABSENT else "updated"))
    return out


def replay(events: list[tuple], root) -> int:
    w = staleness.World("witness")
    core = Core(root)
    checked = 0
    for i, ev in enumerate(events):
        kind = ev[0]
        where = f"step {i} {ev}"
        if kind == "up":
            before = w.up.head
            w.apply(ev)
            if w.up.head != before:
                core.commit(ITEMS, [(k, new, old) for k, (new, old) in sorted(w.up.entries[-1].items())])
        elif kind in ("plan", "full", "keys"):
            if w.pending is not None:
                w.apply(ev)
                continue
            if kind == "keys":
                keys = sorted(ev[1])
                req = core.request(w, mode="keys", keys=keys, limit=len(keys))
            else:
                after = None if kind == "full" else w.cursor
                req = core.request(w, after=after, limit=ev[1], reset=kind == "full")
            p = core.t.plan(req)
            w.apply(ev)
            b = w.pending
            taken = Tracking.plan_keys(p["plan"], 0)[0]
            if kind == "keys":
                assert set(taken) <= set(b.taken), where
            else:
                assert taken == b.taken, where
                assert (p["hi"] or POS) == b.hi, where
                assert p["inputs"][0]["held_back"] == len(b.held_back), where
            core.pending = (p["plan"], taken, w.cond.defn, kind)
        elif kind == "commit":
            pending, core.pending = core.pending, None
            if pending is not None and pending[3] == "keys":
                # SEL-2: a listed key upstream lacks and the consumer does not
                # hold is not delivered, so the worker never loads it; the
                # model's key-list batch takes it anyway.
                ev = ("commit", *(tuple(k for k in ks if k in pending[1]) for ks in ev[1:]))
            refused = pending is not None and pending[2] != w.cond.defn
            w.apply(ev)
            if pending is None or refused:
                continue
            plan, taken, _, _ = pending
            fails, moved = ev[1], ev[2]
            failed = sorted(k for k in taken if k in fails)
            planned = dict(zip(taken, Tracking.plan_keys(plan, 0)[2], strict=True))
            loaded = []
            for k in sorted(set(moved) & set(taken) - set(fails)):
                v = w.up.states[-1].get(k)
                if v != planned[k]:
                    loaded.append((k, v))
            event, records = core.t.progress_ops(plan, [(failed, loaded)])
            assert len(records) == 1, where
            core.t.apply(event)
            core.sync_own(w.holds)
        elif kind == "cancel":
            w.apply(ev)
            core.pending = None
        elif kind == "held":
            w.apply(ev)
            core.t.apply(Tracking.event_reset(EDGE, True))
            core.pending = None
        elif kind == "restart":
            w.apply(ev)
            core.t = core.boot(Tracking.restore(core.t.snapshot()))
            core.pending = None
        else:
            w.apply(ev)
        if bad := w.check():
            raise AssertionError(f"the model broke at {where}: {bad}")
        assert core.owed(w) == model_owed(w), where
        counts = core.t.owed_count(EDGE, conditions(w.cond), OWN)
        assert sum(counts.values()) == len(model_owed(w)), where
        checked += 1
    return checked


def histories(n: int, seed: int):
    rnd = random.Random(seed)
    for _ in range(n):
        yield [ev for ev in staleness.history(rnd, rnd.randint(5, 50)) if ev[0] not in ("clear", "complete")]


@pytest.mark.parametrize("seed", range(4))
def test_the_core_answers_as_the_model_over_random_histories(tmp_path, seed):
    steps = sum(replay(h, tmp_path / str(i)) for i, h in enumerate(histories(60, seed)))
    assert steps > 1000


def test_the_design_worked_example_tally(tmp_path):
    """§5's worked example: run 1 at c1 and c2, run 2's first batch at c4."""
    events = [
        ("up", (("k1", 1), ("k2", 1), ("k3", 1)), False),
        ("plan", 2),
        ("commit", (), ()),
        ("up", (("k2", 2), ("k4", 2)), False),
        ("plan", 2),
        ("commit", (), ()),
        ("up", (("k3", ABSENT), ("k5", 3)), False),
        ("up", (("k1", 4),), False),
        ("cancel",),
        ("plan", 2),
        ("commit", (), ()),
    ]
    replay(events, tmp_path)


def test_a_held_edge_owes_every_key_its_own_output_holds(tmp_path):
    events = [
        ("up", (("k1", 1), ("k2", 1)), False),
        ("plan", 3),
        ("commit", (), ()),
        ("held",),
        ("up", (("k3", 2),), False),
        ("plan", 3),
        ("commit", (), ("k3",)),
    ]
    replay(events, tmp_path)
    assert HELD == "held"


def test_a_failed_removal_owes_nothing_once_the_patterns_take_its_key_again(tmp_path):
    """k1 leaves the patterns; its removal fails, so a point keeps k1@1. When
    the patterns take k1 again and k1 never changed, nothing is owed: the
    point's version at the head must come from the index, not from what the
    batch recorded under patterns that did not take k1."""
    events = [
        ("up", (("k1", 1), ("k2", 1)), False),
        ("plan", 5),
        ("commit", (), ()),
        ("pat", 2),
        ("plan", 5),
        ("commit", ("k1",), ()),
        ("pat", 0),
    ]
    replay(events, tmp_path)
