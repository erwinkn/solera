"""The commit-time hold-back rule (§10 rule 4) against a model, over random
histories of one consumer reading one to three edges, on one or two outputs,
with split and overlapping patterns: upstream commits, clears and full runs
land between the plan, `progress_ops` and the apply, or in the same event as
the batch. After every batch the consumer owes exactly what the model says,
no key is committed from an unchecked load on an unsettled input, a refusal
only ever follows a fenced input becoming unsettled, and once full runs end
the consumer catches up. SOLERA_HOLDBACK_SEEDS sets the histories per
configuration (2,000 per configuration were run when the rule changed)."""

import fnmatch
import os
import random

import pytest
from solera._native import ChangeIndex, Tracking, encode_changes

KEYS = ["a1", "a2", "b1", "b2", "c1"]

CONFIGS = {
    "one": [(b"eA", b"A", None)],
    "two": [(b"eA", b"A", None), (b"eB", b"B", None)],
    "same": [(b"e1", b"A", None), (b"e2", b"A", None)],
    "samepat": [(b"e1", b"A", "a*"), (b"e2", b"A", "b*")],
    "overlap": [(b"e1", b"A", "a*"), (b"e2", b"A", "*1")],
    "twopat": [(b"eA", b"A", "a*"), (b"eB", b"B", "*1")],
    "three": [(b"eA", b"A", None), (b"eB", b"B", None), (b"eA2", b"A", "a*")],
}
SEEDS = int(os.environ.get("SOLERA_HOLDBACK_SEEDS", "200"))


class Out:
    def __init__(self):
        self.state = {}
        self.head = 0
        self.clears = []
        self.incomplete = False


class World:
    def __init__(self, tmp, edges):
        self.ix = ChangeIndex(f"file://{tmp}")
        self.t = Tracking()
        self.edges = edges
        self.outs = {o: Out() for _, o, _ in edges}
        for o in self.outs:
            self.t.attach(o, self.ix)
        self.n = 0
        self.journal = []
        self.hold = {e: {} for e, _, _ in edges}

    @staticmethod
    def conds(glob):
        return {} if glob is None else {"patterns": {"include": [{"glob": glob}]}}

    @staticmethod
    def takes(glob, k):
        return glob is None or fnmatch.fnmatchcase(k, glob)

    def apply(self, ev):
        self.t.apply(ev)
        self.journal.append(bytes(ev))

    def build(self, out, ups, rem=(), mode="patch", ends=False):
        o = self.outs[out]
        self.n += 1
        v = o.head + 1
        b = self.t.build_commit(
            out,
            encode_changes(list(ups), list(rem)),
            version=v,
            path=f"{out.decode()}/{self.n}",
            mode=mode,
            ends_full_run=ends,
        )
        if b is None:
            return None
        return (out, b, list(ups), list(rem), mode, ends, v)

    def landed(self, built):
        out, b, ups, rem, mode, ends, v = built
        o = self.outs[out]
        assert b["commit"] == v
        o.head = v
        if mode == "clear":
            o.state = {}
            o.clears.append(v)
            o.incomplete = True
        for k in ups:
            o.state[k] = v
        if mode != "clear":
            for k in rem:
                o.state.pop(k, None)
        if ends:
            o.incomplete = False

    def commit(self, out, ups, rem=(), mode="patch", ends=False):
        b = self.build(out, ups, rem, mode, ends)
        if b is None:
            return None
        self.apply(b[1]["event"])
        self.landed(b)
        return b

    def done(self, out):
        self.apply(Tracking.event_full_run_done(out))
        self.outs[out].incomplete = False

    def restart(self):
        t = Tracking.restore(self.t.snapshot())
        for o in self.outs:
            t.attach(o, self.ix)
        assert t.snapshot() == self.t.snapshot()
        self.t = t

    def unsettled(self, out, H):
        o = self.outs[out]
        return o.incomplete or any(H < c <= o.head for c in o.clears)

    def check_owed(self, where):
        for e, o, g in self.edges:
            want = sorted(
                k
                for k in KEYS
                if (self.outs[o].state.get(k) if self.takes(g, k) else None) != self.hold[e].get(k)
            )
            got = sorted(k for k, _ in self.t.owed(e, self.conds(g))[0])
            assert got == want, (where, e, "owed", got, "model", want, self.hold[e], self.outs[o].state)


def random_build(w, rnd, out=None):
    """Builds (does not apply) one random upstream event; returns ('commit', built) or ('done', out) or None."""
    out = out or rnd.choice(sorted(w.outs))
    o = w.outs[out]
    kind = rnd.choice(["patch", "patch", "patch", "clear", "clear_ends", "ends", "done"])
    ks = sorted(rnd.sample(KEYS, rnd.randint(1, 3)))
    if kind in ("ends", "done") and not o.incomplete:
        kind = "patch"
    if kind == "patch":
        ups = [k for k in ks if rnd.random() < 0.7]
        rem = [k for k in ks if k not in ups and k in o.state]
        if not ups and not rem:
            ups = ks[:1]
        b = w.build(out, ups, rem)
        return ("commit", b) if b else None
    if kind in ("clear", "clear_ends"):
        return ("commit", w.build(out, ks, mode="clear", ends=kind == "clear_ends"))
    if kind == "ends":
        b = w.build(out, ks, ends=True)
        return ("commit", b) if b else ("done", out)
    return ("done", out)


def land(w, x):
    if x is None:
        return
    if x[0] == "commit":
        w.apply(x[1][1]["event"])
        w.landed(x[1])
    else:
        w.done(x[1])


def ev_of(x):
    return x[1][1]["event"] if x[0] == "commit" else Tracking.event_full_run_done(x[1])


def landed_model(w, x):
    if x[0] == "commit":
        w.landed(x[1])
    else:
        w.outs[x[1]].incomplete = False


def upstream(w, rnd, n):
    for _ in range(n):
        land(w, random_build(w, rnd))


def one_batch(w, rnd, quiet=False):
    edges = w.edges
    n = len(edges)
    mode = "incremental" if quiet else rnd.choice(["incremental"] * 3 + ["all", "reset", "keys"])
    kw = {"limit": 100 if quiet else rnd.randint(1, 6)}
    if mode == "all":
        kw["mode"] = "all"
    elif mode == "reset":
        kw.update(mode="all", reset=True)
    elif mode == "keys":
        kw.update(mode="keys", keys=sorted(rnd.sample(KEYS, rnd.randint(0, 4))))
    req = {"inputs": [{"edge": e, "conditions": w.conds(g)} for e, _, g in edges]}
    if not quiet and mode != "reset" and rnd.random() < 0.3:
        kw["after"] = rnd.choice(KEYS)
    if not quiet and rnd.random() < 0.3:
        for i in req["inputs"]:
            i["not_due"] = rnd.sample(KEYS, rnd.randint(0, 2))
    req.update(kw)
    pl = w.t.plan(req)
    p = pl["plan"]
    H = [i["head"] for i in pl["inputs"]]
    taken, planned = [], []
    for i in range(n):
        ks, _, vs = Tracking.plan_keys(p, i)
        taken.append(list(ks))
        planned.append(dict(zip(ks, vs, strict=True)))
    batch = set().union(*map(set, taken))
    if quiet and not batch:
        return False
    if not quiet:
        upstream(w, rnd, rnd.randint(0, 2))
    F = set() if quiet else {k for k in batch if rnd.random() < 0.15}
    reports = []
    for i, (_, o, g) in enumerate(edges):
        loaded = []
        for k in taken[i]:
            cur = w.outs[o].state.get(k) if w.takes(g, k) else None
            if not quiet and rnd.random() < 0.1:
                cur = rnd.choice([None, 99])
            if cur != planned[i][k]:
                loaded.append((k, cur))
        reports.append((sorted(F & set(taken[i])), loaded))
    moved = [{k for k, _ in reports[i][1]} for i in range(n)]
    unsure = [moved[i] | {k for k in batch - set(taken[i]) if w.takes(edges[i][2], k)} for i in range(n)]
    refusals = 0
    for _ in range(8):
        flags0 = [w.unsettled(o, H[i]) for i, (_, o, _) in enumerate(edges)]
        hold0 = set()
        for i in range(n):
            if flags0[i]:
                hold0 |= unsure[i]
        ev0, _, held0 = w.t.progress_ops(p, reports)
        assert held0 == [sorted(hold0 & set(taken[i])) for i in range(n)], ("held", held0, hold0, taken)
        fence = [(not flags0[i]) and not unsure[i] <= hold0 for i in range(n)]
        if not quiet:
            upstream(w, rnd, rnd.randint(0, 2))
            if rnd.random() < 0.15:
                w.restart()
        # same-event combination
        extra = None
        if not quiet and rnd.random() < 0.25:
            extra = random_build(w, rnd)
        flags1 = [w.unsettled(o, H[i]) for i, (_, o, _) in enumerate(edges)]
        violated = any(fence[i] and flags1[i] for i in range(n))
        before = w.t.snapshot()
        if extra is not None:
            parts = [ev_of(extra), ev0]
            if rnd.random() < 0.5:
                parts.reverse()
            ev = Tracking.join(parts)
        else:
            ev = ev0
        try:
            w.apply(ev)
        except ValueError as e:
            assert "became unsettled" in str(e), str(e)
            assert violated, (
                "refused though no fenced input became unsettled",
                str(e),
                fence,
                flags0,
                flags1,
            )
            assert w.t.snapshot() == before
            refusals += 1
            if extra is not None:
                land(w, extra)
            continue
        assert not violated, ("applied though a fenced input became unsettled", fence, flags0, flags1)
        # No key is committed from a load on an input unsettled when the event
        # applies unless that load was planned and came back as planned: an
        # input whose patterns do not take the key does not load it.
        for i, (_, _, g) in enumerate(edges):
            if flags1[i]:
                for k in batch - hold0:
                    assert not w.takes(g, k) or (k in taken[i] and k not in moved[i]), ("unsafe", i, k, hold0)
        if extra is not None:
            landed_model(w, extra)
        if mode == "reset":
            for e, _, _ in edges:
                w.hold[e] = {}
        for i, (e, _, _) in enumerate(edges):
            got = dict(reports[i][1])
            for k in taken[i]:
                if k in hold0 or k in F:
                    continue
                v = got.get(k, planned[i][k])
                if v is None:
                    w.hold[e].pop(k, None)
                else:
                    w.hold[e][k] = v
        w.check_owed(("after batch", mode, kw, taken, reports, sorted(hold0)))
        break
    else:
        raise AssertionError("eight refusals in a row")
    assert refusals <= len({o for _, o, _ in edges}), refusals
    return True


def run(tmp, cfg, seed):
    rnd = random.Random(seed)
    w = World(tmp, CONFIGS[cfg])
    for o in sorted(w.outs):
        w.commit(o, sorted(rnd.sample(KEYS, 3)))
    for e, o, _ in w.edges:
        w.apply(Tracking.event_edge_open(e, o, True))
    w.check_owed("start")
    for _ in range(rnd.randint(1, 7)):
        upstream(w, rnd, rnd.randint(0, 3))
        if rnd.random() < 0.2:
            w.restart()
        one_batch(w, rnd)
    # quiesce: full runs end, then the consumer catches up
    for o in sorted(w.outs):
        if w.outs[o].incomplete:
            w.done(o)
    rounds = 0
    while one_batch(w, rnd, quiet=True):
        rounds += 1
        assert rounds <= 2, "the consumer does not catch up"
    for e, o, g in w.edges:
        want = {k: v for k, v in w.outs[o].state.items() if w.takes(g, k)}
        assert w.hold[e] == want, (e, w.hold[e], want)
    final = w.t.snapshot()
    t = Tracking()
    for o in w.outs:
        t.attach(o, w.ix)
    for ev in w.journal:
        t.apply(ev)
    assert t.snapshot() == final


@pytest.mark.parametrize("cfg", sorted(CONFIGS))
def test_the_hold_back_rule_matches_the_model(tmp_path_factory, cfg):
    for seed in range(SEEDS):
        try:
            run(tmp_path_factory.mktemp(f"{cfg}{seed}"), cfg, seed)
        except AssertionError as e:
            raise AssertionError((cfg, seed, e.args)) from e
