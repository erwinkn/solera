"""The commit-time hold-back rule (§10 rule 4) against a model, over random
histories of one consumer reading one to three edges on one or two outputs.
Each edge's patterns (globs, excludes, regexes; split, overlapping, or taking
nothing) and context may change between batches. Upstream commits, clears
and full runs land between the plan, `progress_ops` and the apply, or in
the same event as the batch.

The worker's loads are modeled apart from the rule: a taken key not planned
removed is loaded and checked against the plan; a batch key an input did not
take is loaded there, unchecked, if the patterns the batch was planned under
take it. After every batch: no committed key has an unchecked or moved load
on an input unsettled when the event applies; a refusal only follows a
fenced input becoming unsettled; the event gives the same state in either
part order and as two events, batch first; and the consumer owes exactly
what the model says under every pattern set and context. Once full runs end
the consumer catches up, and replay reproduces the snapshot.
SOLERA_HOLDBACK_SEEDS sets the histories per configuration."""

import fnmatch
import os
import random
import re

import pytest
from solera._native import ChangeIndex, Tracking, encode_changes

KEYS = ["a1", "a2", "b1", "b2", "c1"]


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


class P:
    def __init__(self, inc=(), exc=()):
        self.inc, self.exc = list(inc), list(exc)

    @staticmethod
    def _m(p, k):
        kind, pat = p
        return fnmatch.fnmatchcase(k, pat) if kind == "glob" else re.fullmatch(pat, k, re.S) is not None

    def takes(self, k):
        return (not self.inc or any(self._m(p, k) for p in self.inc)) and not any(
            self._m(p, k) for p in self.exc
        )

    def conds(self, ctx=b""):
        d = {"context": ctx}
        if self.inc or self.exc:
            d["patterns"] = {
                "include": [{kind: pat} for kind, pat in self.inc],
                "exclude": [{kind: pat} for kind, pat in self.exc],
            }
        return d

    def __repr__(self):
        return f"P({self.inc},{self.exc})"


ALL = P()
A = P([("glob", "a*")])
B = P([("glob", "b*")])
ONE = P([("glob", "*1")])
AB_NOT2 = P([("glob", "a*"), ("glob", "b*")], [("glob", "*2")])
RX = P([("regex", "[ab]1")])
RX2 = P([("regex", "(a|c).*")], [("regex", ".*2")])
NOT_A = P([], [("glob", "a*")])
NOT_B = P([], [("glob", "b*")])
NONE = P([("glob", "zz*")])

CONFIGS = {
    "one": [(b"eA", b"A", [ALL])],
    "two": [(b"eA", b"A", [ALL]), (b"eB", b"B", [ALL])],
    "same": [(b"e1", b"A", [ALL]), (b"e2", b"A", [ALL])],
    "overlap": [(b"e1", b"A", [A]), (b"e2", b"A", [ONE])],
    "excl_split": [(b"e1", b"A", [NOT_B]), (b"e2", b"A", [NOT_A])],
    "split": [(b"e1", b"A", [A]), (b"e2", b"A", [B])],
    "split_chg": [(b"e1", b"A", [A, ALL, NOT_A]), (b"e2", b"A", [B, ONE])],
    "excl": [(b"e1", b"A", [AB_NOT2, NOT_A]), (b"e2", b"A", [RX2, RX])],
    "two_chg": [(b"eA", b"A", [A, ALL, RX]), (b"eB", b"B", [ONE, NOT_A, ALL])],
    "three_chg": [(b"eA", b"A", [ALL, A]), (b"eB", b"B", [ALL, NOT_A]), (b"eA2", b"A", [A, B, RX2])],
    "none": [(b"e1", b"A", [NONE, A]), (b"e2", b"A", [ALL])],
}
SEEDS = int(os.environ.get("SOLERA_HOLDBACK_SEEDS", "150"))
CTXS = [b"", b"x"]


class W2(World):
    def __init__(self, tmp, edges):
        super().__init__(tmp, [(e, o, None) for e, o, _ in edges])
        self.edges2 = edges
        self.hold = {e: {} for e, _, _ in edges}  # key -> (version, ctx)

    def want(self, o, pat, ctx, k):
        v = self.outs[o].state.get(k)
        return (v, ctx) if v is not None and pat.takes(k) else None

    def check_all(self, where):
        for e, o, pool in self.edges2:
            for pat in pool:
                for ctx in CTXS:
                    want = sorted(k for k in KEYS if self.want(o, pat, ctx, k) != self.hold[e].get(k))
                    got = sorted(k for k, _ in self.t.owed(e, pat.conds(ctx))[0])
                    assert got == want, (
                        where,
                        e,
                        pat,
                        ctx,
                        "owed",
                        got,
                        "model",
                        want,
                        self.hold[e],
                        self.outs[o].state,
                    )


def one_batch(w, rnd, cur, quiet=False):
    edges = w.edges2
    n = len(edges)
    mode = "incremental" if quiet else rnd.choice(["incremental"] * 3 + ["all", "reset", "keys"])
    kw = {"limit": 100 if quiet else rnd.randint(1, 6)}
    if mode == "all":
        kw["mode"] = "all"
    elif mode == "reset":
        kw.update(mode="all", reset=True)
    elif mode == "keys":
        kw.update(mode="keys", keys=sorted(rnd.sample(KEYS, rnd.randint(0, 4))))
    if not quiet:
        for i, (_, _, pool) in enumerate(edges):
            if rnd.random() < 0.35:
                cur[i] = (rnd.choice(pool), cur[i][1])
            if rnd.random() < 0.1:
                cur[i] = (cur[i][0], rnd.choice(CTXS))
    pats = [c[0] for c in cur]
    ctxs = [c[1] for c in cur]
    req = {"inputs": [{"edge": e, "conditions": pats[i].conds(ctxs[i])} for i, (e, _, _) in enumerate(edges)]}
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
    # The worker's loads, written without the rule.
    reports, bad_load, unchecked = [], [], []
    for i, (_, o, _) in enumerate(edges):
        loaded, bad = [], set()
        for k in taken[i]:
            if planned[i][k] is None and (quiet or rnd.random() < 0.8):
                continue  # planned removed: not loaded
            curv = w.outs[o].state.get(k) if pats[i].takes(k) else None
            if not quiet and rnd.random() < 0.1:
                curv = rnd.choice([None, 99])
            if curv != planned[i][k]:
                loaded.append((k, curv))
                seen = curv if pats[i].takes(k) else None  # what the input contributes
                if seen != planned[i][k]:
                    bad.add(k)
        reports.append((sorted(F & set(taken[i])), loaded))
        bad_load.append(bad)
        unchecked.append({k for k in batch - set(taken[i]) if pats[i].takes(k)})
    refusals = 0
    for _ in range(8):
        flags0 = [w.unsettled(o, H[i]) for i, (_, o, _) in enumerate(edges)]
        ev0, _, held0 = w.t.progress_ops(p, reports)
        held = set().union(*map(set, held0)) if held0 else set()
        for i in range(n):
            assert set(held0[i]) == held & set(taken[i]), ("per-input held", held0, taken)
        # model of the rule
        hold_model = set()
        for i in range(n):
            if flags0[i]:
                hold_model |= bad_load[i] | unchecked[i]
        assert held == hold_model & batch, ("held", held0, hold_model, taken, flags0)
        fence = [(not flags0[i]) and bool((bad_load[i] | unchecked[i]) - held) for i in range(n)]
        if not quiet:
            upstream(w, rnd, rnd.randint(0, 2))
            if rnd.random() < 0.15:
                w.restart()
        extra = None
        if not quiet and rnd.random() < 0.3:
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
        if extra is not None:
            # the two orders of one event, and batch-then-extra as two events, end in one state
            after = w.t.snapshot()
            for order in ("rev", "seq"):
                t = Tracking.restore(before)
                for o in w.outs:
                    t.attach(o, w.ix)
                if order == "rev":
                    t.apply(Tracking.join(list(reversed(parts))))
                else:
                    t.apply(ev0)
                    t.apply(ev_of(extra))
                assert t.snapshot() == after, ("order matters", order)
        # Safety: a committed key has, on every input unsettled when the event applies, no
        # unchecked load and no load that came back other than planned.
        for i in range(n):
            if flags1[i]:
                for k in batch - held:
                    assert k not in unchecked[i], ("unsafe: unchecked load committed", i, k, held0)
                    assert k not in bad_load[i], ("unsafe: moved load committed", i, k, held0)
        if extra is not None:
            landed_model(w, extra)
        if mode == "reset":
            for e, _, _ in edges:
                w.hold[e] = {}
        for i, (e, _, _) in enumerate(edges):
            got = dict(reports[i][1])
            for k in taken[i]:
                if k in held or k in F:
                    continue
                v = got.get(k, planned[i][k])
                if v is None or not pats[i].takes(k):
                    w.hold[e].pop(k, None)
                else:
                    w.hold[e][k] = (v, ctxs[i])
        w.check_all(("after batch", mode, kw, pats, ctxs, taken, reports, sorted(held)))
        break
    else:
        raise AssertionError("eight refusals in a row")
    assert refusals <= len({o for _, o, _ in edges}), refusals
    return True


def run(tmp, cfg, seed):
    rnd = random.Random(seed)
    w = W2(tmp, CONFIGS[cfg])
    for o in sorted(w.outs):
        w.commit(o, sorted(rnd.sample(KEYS, 3)))
    for e, o, _ in w.edges2:
        w.apply(Tracking.event_edge_open(e, o, True))
    cur = [(pool[0], b"") for _, _, pool in w.edges2]
    w.check_all("start")
    for _ in range(rnd.randint(1, 8)):
        upstream(w, rnd, rnd.randint(0, 3))
        if rnd.random() < 0.2:
            w.restart()
        one_batch(w, rnd, cur)
    for o in sorted(w.outs):
        if w.outs[o].incomplete:
            w.done(o)
    rounds = 0
    while one_batch(w, rnd, cur, quiet=True):
        rounds += 1
        assert rounds <= 2, "the consumer does not catch up"
    for i, (e, o, _) in enumerate(w.edges2):
        want = {k: (v, cur[i][1]) for k, v in w.outs[o].state.items() if cur[i][0].takes(k)}
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
