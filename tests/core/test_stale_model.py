"""Staleness in the tracking core against the tracking model (tests/tracking,
SOLE-7's oracle): random staleness histories replayed through both.

The core reads a real change index. Upstream commits go through the commit
builder, so their notices (rule 1 and 2 over the builder's entries) are in
the commit's event; a lost notification commits the layer without them. After
every step, what the core knows (if it is current) and what a refresh finds
are compared with brute force: the edge is stale exactly when some key is
due, and the witness is due; the owed count, the page of owed keys and
completeness (D177) equal the model's. Refreshes are skipped at random, so a
later one catches up over several commits and several batch commits at once.

Clears are left out: the core does not build them yet.
"""

from __future__ import annotations

import os
import random

import pytest
from solera._native import ChangeIndex, Tracking, encode_changes

oracle = pytest.importorskip("tests.tracking.oracle")
from tests.tracking import histories as histories_  # noqa: E402
from tests.tracking import staleness  # noqa: E402
from tests.tracking.core import ABSENT  # noqa: E402

EDGE, ITEMS, OWN = b"checks/items", b"items", b"checks"


def conditions(cond) -> dict:
    pat = None if cond.pat is None else {"include": [{"glob": k} for k in sorted(cond.pat)], "exclude": []}
    return {"patterns": pat, "context": str(cond.ctx).encode(), "definition": repr(cond.defn).encode()}


def word(v) -> str | None:
    return None if v is ABSENT else str(v)


class Core:
    def __init__(self, root, rnd: random.Random) -> None:
        self.ix = ChangeIndex(f"file://{root}")
        self.rnd = rnd
        self.own: set[str] = set()
        self.own_log: tuple[list, int, int] = ([], 0, 0)
        self.t = self.boot(Tracking())
        self.t.apply(Tracking.event_edge_open(EDGE, ITEMS, True))
        self.pending = None
        self.time = 0
        self.listed: set[str] = set()  # keys a key list delivered since progress last started over

    def boot(self, t: Tracking) -> Tracking:
        t.attach(ITEMS, self.ix)
        t.attach(OWN, self.ix)
        t._set_output(OWN, *self.own_log)
        return t

    def watch(self, w) -> None:
        self.t.apply(Tracking.event_watch(EDGE, conditions(w.cond), OWN))

    def upstream(self, entries: dict, lost: bool) -> None:
        self.time += 1
        ups = {k: word(n) for k, (n, _) in entries.items() if n is not ABSENT}
        rms = [k for k, (n, _) in entries.items() if n is ABSENT]
        head = self.t.stale_status([EDGE])  # noqa: F841 (the state before)
        built = self.t.build_commit(
            ITEMS, encode_changes(ups, rms), version=0, path=f"items/{self.time}", time_ms=self.time
        )
        assert built is not None
        if lost:
            layer = built["layer"]
            self.t.apply(Tracking.join([self.output_only(built)]))
            del layer
        else:
            self.t.apply(built["event"])

    def output_only(self, built) -> bytes:
        """The commit's event without its notices: a lost notification."""
        return built["event"][: self.output_part_end(built["event"])]

    @staticmethod
    def output_part_end(ev: bytes) -> int:
        i = 1  # the format byte, then the first part: tag, varint length, body
        i += 1
        n, shift = 0, 0
        while True:
            b = ev[i]
            i += 1
            n |= (b & 0x7F) << shift
            shift += 7
            if b < 0x80:
                break
        return i + n

    def sync_own(self, holds: dict) -> None:
        now = {k for k, v in holds.items() if v is not ABSENT}
        changes = [(k, 1, None) for k in sorted(now - self.own)] + [
            (k, None, 1) for k in sorted(self.own - now)
        ]
        if changes:
            layers, head, live = self.own_log
            layer = self.ix.write_commit(f"own/{head + 1}", head + 1, sorted(changes), live_before=live)
            live += sum(r is None for _, _, r in changes) - sum(n is None for _, n, _ in changes)
            self.own_log = ([*layers, layer], head + 1, live)
            self.t._set_output(OWN, *self.own_log)
        self.own = now

    def not_due(self, w) -> dict:
        return {EDGE: [k for k in staleness.KEYS if w.waiting(k, w.up.head)]}

    def request(self, w, **kw) -> dict:
        inp = {"edge": EDGE, "conditions": conditions(w.cond), "own": OWN}
        if not kw.get("reset"):
            inp["not_due"] = self.not_due(w)[EDGE]
        return {"inputs": [inp], **kw}

    def refresh(self, w) -> None:
        self.time += 1
        ev = self.t.stale_refresh(time_ms=self.time, not_due=self.not_due(w))
        if ev is not None:
            self.t.apply(ev)

    def status(self) -> dict:
        return self.t.stale_status([EDGE])[EDGE]


def check(w, core: Core, where: str, refreshed: bool) -> None:
    owed, due = w.brute()
    s = core.status()
    if refreshed:
        assert s["state"] in ("fresh", "stale"), (where, s)
    if s["state"] == "fresh":
        assert not due, (where, s, sorted(due))
    elif s["state"] == "stale":
        assert due and s["key"] in due, (where, s, sorted(due))
    if not refreshed:
        return
    n = core.t.stale_count(EDGE)
    assert sum(n.values()) == len(owed), (where, n, sorted(owed))
    keys, nxt = core.t.stale_keys(EDGE, limit=100)
    assert nxt is None and [k for k, _ in keys] == sorted(owed), (where, keys)
    assert core.t.stale_complete(EDGE) == complete(w, core), where


def complete(w, core: Core) -> bool:
    """The model's brute force for D177, with the keys a key list delivered: a
    listed key that neither upstream nor the consumer has is not delivered
    (SEL-2), so the core does not count it as processed; the model does."""
    return all(
        k in core.listed or any(lo < k <= hi for lo, hi in w.covered)
        for k in w.up.states[-1]
        if w.cond.takes(k)
    )


def replay(
    events: list[tuple], root, seed: int = 0, rate: float | None = None, at: set[int] | None = None
) -> int:
    rnd = random.Random(seed)
    if rate is None:
        rate = rnd.choice((0.2, 0.6, 1.0))  # how often the engine refreshes
    w = staleness.World("witness")
    core = Core(root, rnd)
    core.watch(w)
    checked = 0
    for i, ev in enumerate(events):
        kind = ev[0]
        where = f"step {i} {ev}"
        if kind == "up":
            before = w.up.head
            w.apply(ev)
            if w.up.head != before:
                core.upstream(w.up.entries[-1], ev[2])
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
            taken = Tracking.plan_keys(p["plan"], 0)[0]
            core.pending = (p["plan"], taken, w.cond.defn, kind)
        elif kind == "commit":
            pending, core.pending = core.pending, None
            if pending is not None and pending[3] == "keys":
                ev = ("commit", *(tuple(k for k in ks if k in pending[1]) for ks in ev[1:]))
            refused = pending is not None and pending[2] != w.cond.defn
            w.apply(ev)
            if pending is not None and pending[3] == "keys" and not refused:
                core.listed |= set(pending[1])
            if pending is None or refused:
                continue
            plan, taken, _, _ = pending
            fails, moved = ev[1], ev[2]
            failed = sorted(k for k in taken if k in fails)
            planned = dict(zip(taken, Tracking.plan_keys(plan, 0)[2], strict=True))
            loaded = []
            for k in sorted(set(moved) & set(taken) - set(fails)):
                v = word(w.up.states[-1].get(k))
                if v != planned[k]:
                    loaded.append((k, v))
            event, _ = core.t.progress_ops(plan, [(failed, loaded)])
            core.t.apply(event)
            core.sync_own(w.holds)
        elif kind in ("pat", "ctx", "defn"):
            w.apply(ev)
            core.watch(w)
        elif kind == "cancel":
            w.apply(ev)
            core.pending = None
        elif kind == "held":
            w.apply(ev)
            core.t.apply(Tracking.event_reset(EDGE, True))
            core.pending = None
        elif kind == "retry":
            k = ev[1]
            due_before = w.outcomes.get(k)
            w.apply(ev)
            if due_before is not None and due_before.retry:
                core.time += 1
                e = core.t.stale_retry_due(EDGE, [k], time_ms=core.time, not_due=core.not_due(w))
                if e is not None:
                    core.t.apply(e)
        elif kind == "restart":
            w.apply(ev)
            core.t = core.boot(Tracking.restore(core.t.snapshot()))
            core.pending = None
            retries = sorted(k for k, f in w.outcomes.items() if f.retry)
            if retries:
                e = core.t.stale_retry_due(EDGE, retries, time_ms=core.time, not_due=core.not_due(w))
                if e is not None:
                    core.t.apply(e)
        else:
            w.apply(ev)
        if not w.listed:
            core.listed = set()
        if bad := w.check():
            raise AssertionError(f"the model broke at {where}: {bad}")
        try:
            check(w, core, where + " (as known)", refreshed=False)
            if (i in at) if at is not None else rnd.random() < rate:
                core.refresh(w)
                check(w, core, where + " (refreshed)", refreshed=True)
        except AssertionError as e:
            raise AssertionError("\n".join([str(e), *w.trace])) from None
        checked += 1
    core.refresh(w)
    check(w, core, "at the end (refreshed)", refreshed=True)
    return checked


def histories(n: int, seed: int):
    rnd = random.Random(seed)
    for _ in range(n):
        yield [ev for ev in staleness.history(rnd, rnd.randint(5, 50)) if ev[0] not in ("clear", "complete")]


@pytest.mark.parametrize("seed", range(4))
def test_staleness_answers_as_the_model_over_random_histories(tmp_path, seed):
    n = int(os.environ.get("STALE_HISTORIES", "150"))
    steps = sum(replay(h, tmp_path / str(i), seed * 1000 + i) for i, h in enumerate(histories(n, seed)))
    assert steps > 1000


STALENESS = {
    rule: h
    for (model, _, _, rule), h in histories_.CALIBRATIONS.items()
    if model == "staleness" and rule != "rule8"
}


@pytest.mark.parametrize("rate", (0.0, 1.0))
@pytest.mark.parametrize("rule", sorted(STALENESS))
def test_the_models_history_for_each_witness_rule(tmp_path, rule, rate):
    """Each history the model shrank to pin one of its witness rules."""
    replay(STALENESS[rule], tmp_path, rate=rate)


def test_a_reset_batch_leaves_everything_after_its_range_unknown(tmp_path):
    """checks is fresh as of c1; k5's removal reaches no notice, then a full
    run's first batch takes k1 only. Past the batch's range the consumer holds nothing any more, so k5's
    removal owes nothing there: k2, never processed, is the witness."""
    history = [
        ("up", (("k1", 1), ("k2", 1), ("k5", 1)), False),
        ("plan", 3),
        ("commit", (), ()),
        ("up", (("k5", ABSENT),), True),
        ("full", 1),
        ("commit", (), ()),
    ]
    replay(history, tmp_path, at={2})
