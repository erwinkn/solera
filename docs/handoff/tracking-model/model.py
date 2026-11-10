"""A brute-force check of docs/tracking-design.md (T43): output log entries,
diff from entries alone, pieces and points, the batch commit rule, and the
exact owed counts kept by propagating each commit's entries.

Run: python3 model.py           (random histories, then the worked examples)
"""

import random
import sys

ABSENT = None
HELD = "held"
NEG, POS = "", "￿"  # -inf and +inf as key bounds


def within(k, lo, hi):  # (lo, hi]
    return lo < k <= hi


class Log:
    """One output's log: commits of entries (key, commit, new, replaced)."""

    def __init__(self):
        self.commits = [None]  # commit 0 is the empty state
        self.states = [{}]

    @property
    def head(self):
        return len(self.commits) - 1

    def commit(self, changes, clear=False):
        """changes: key -> new version or ABSENT. A clear removes every key it
        does not name. Returns the entries: key -> (new, replaced, since), where
        `since` is the commit of the replaced version (what the engine's index
        lookup returns along with it)."""
        before = self.states[-1]
        entries = {}
        for k, new in sorted(changes.items()):
            old = before.get(k)
            if new != old or clear:
                entries[k] = (new, old, self.since(k))
        after = {} if clear else dict(before)
        for k, (new, _, _) in entries.items():
            if new is ABSENT:
                after.pop(k, None)
            else:
                after[k] = new
        if clear:
            for k, v in before.items():  # named keys kept as they were
                if k in changes and changes[k] == v:
                    after[k] = v
        self.commits.append({"entries": entries, "clear": clear})
        self.states.append(after)
        return entries

    def since(self, k):
        for c in range(self.head, 0, -1):
            com = self.commits[c]
            if k in com["entries"] or com["clear"]:
                return c
        return 0

    def state(self, c):
        return self.states[c]

    def scan(self, c, lo=NEG, hi=POS):
        """The state at c from entries alone: per key the newest entry at or
        before c, ignoring entries older than the latest clear at or before c."""
        last_clear = max([i for i in range(1, c + 1) if self.commits[i]["clear"]], default=0)
        out = {}
        for i in range(max(1, last_clear), c + 1):
            for k, (new, _, _) in self.commits[i]["entries"].items():
                if within(k, lo, hi):
                    out[k] = new
        return {k: v for k, v in out.items() if v is not ABSENT}

    def diff(self, c1, c2, lo=NEG, hi=POS):
        """Keys whose state differs between c1 and c2, from entries alone."""
        if any(self.commits[i]["clear"] for i in range(c1 + 1, c2 + 1)):
            a, b = self.scan(c1, lo, hi), self.scan(c2, lo, hi)
            return {k: (a.get(k), b.get(k)) for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k)}
        first, last = {}, {}
        for i in range(c1 + 1, c2 + 1):
            for k, (new, rep, _) in self.commits[i]["entries"].items():
                if within(k, lo, hi):
                    first.setdefault(k, rep)
                    last[k] = new
        return {k: (first[k], last[k]) for k in sorted(first) if first[k] != last[k]}


def take(pat, k):
    return pat is None or k in pat


class Edge:
    """A consumer's progress on one keyed input: pieces and points, with the
    exact owed count per piece (keys without a point) kept by propagation.

    A piece is [lo, hi, value, count]; value is ("at", P, patterns, context),
    ("empty",) or ("held",); count is an int, or None while the piece waits
    for its upstream to be complete again after a clear (F4). A point's value
    is ABSENT, HELD, or (version, context). Conditions now: self.pat, self.ctx.
    """

    def __init__(self, log, own=None, pat=None, ctx=0):
        self.log = log
        self.own = own if own is not None else set()
        self.pat, self.ctx = pat, ctx
        self.pieces = [[NEG, POS, ("empty",), 0]]
        self.points = {}
        self.applied = []  # (commit, key, delta): what propagation did, for the batch-cut split
        self.lookups = 0
        self.incomplete = False

    # -- observations --------------------------------------------------------
    def piece_of(self, k):
        for p in self.pieces:
            if within(k, p[0], p[1]):
                return p
        raise AssertionError(k)

    def held_by_piece(self, p, k):
        v = p[2]
        if v[0] == "at":
            ver = self.log.state(v[1]).get(k)
            return (ver, v[3]) if ver is not ABSENT and take(v[2], k) else ABSENT
        if v[0] == "empty":
            return ABSENT
        return HELD if k in self.own else ABSENT

    def decode(self, k):
        return self.points[k] if k in self.points else self.held_by_piece(self.piece_of(k), k)

    def wanted(self, ver, k):
        return (ver, self.ctx) if ver is not ABSENT and take(self.pat, k) else ABSENT

    @staticmethod
    def owed(h, w):
        if h is ABSENT and w is ABSENT:
            return False
        return h is HELD or h != w

    # -- brute force -----------------------------------------------------------
    def owed_brute(self, universe, state=None):
        state = self.log.state(self.log.head) if state is None else state
        return {k for k in universe if self.owed(self.decode(k), self.wanted(state.get(k), k))}

    def count_brute(self, p, universe):
        now = self.log.state(self.log.head)
        return sum(
            1
            for k in universe
            if within(k, p[0], p[1]) and k not in self.points
            and self.owed(self.held_by_piece(p, k), self.wanted(now.get(k), k))
        )

    def scan_count(self, p, universe):
        """Event 3: count one piece by a scan (the engine merges S(P) and S(head))."""
        p[3] = self.count_brute(p, universe)

    # -- event 1: an upstream commit -------------------------------------------
    def propagate(self, entries, commit, universe):
        if self.log.commits[commit]["clear"]:
            for p in self.pieces:  # every piece predates the clear: counted once the upstream is complete
                p[3] = None
            self.incomplete = True
            return
        for k, (new, rep, since) in entries.items():
            if k in self.points:
                continue
            p = self.piece_of(k)
            if p[3] is None:
                continue  # waits for the upstream to be complete
            v = p[2]
            if v[0] == "at" and since <= v[1]:
                h = (rep, v[3]) if rep is not ABSENT and take(v[2], k) else ABSENT  # free
                assert h == self.held_by_piece(p, k)
            else:
                if v[0] == "at":
                    self.lookups += 1  # get(P, k) in the warm index
                h = self.held_by_piece(p, k)
            delta = int(self.owed(h, self.wanted(new, k))) - int(self.owed(h, self.wanted(rep, k)))
            p[3] += delta
            self.applied.append((commit, k, delta))

    def upstream_complete(self, universe):
        for p in self.pieces:
            if p[3] is None:
                self.scan_count(p, universe)
        self.incomplete = False

    # -- event 3: conditions change --------------------------------------------
    def change_conditions(self, pat, ctx, universe):
        self.pat, self.ctx = pat, ctx
        for p in self.pieces:
            if p[3] is not None:
                self.scan_count(p, universe)

    # -- planning and event 2: a batch commit ------------------------------------
    def plan(self, H, after, N, universe):
        at_h = self.log.state(H)
        cands = [k for k in sorted(universe) if after < k and self.owed(self.decode(k), self.wanted(at_h.get(k), k))]
        took = cands[:N]
        hi = took[-1] if len(cands) > N else POS
        return took, hi

    def commit_batch(self, H, lo, hi, taken, failed, loaded, universe):
        """Write (lo, hi] at(H, conditions planned under); points for failed keys
        (keep what was held) and keys loaded at another version; counts by the
        split rule (F5): never by recounting the whole edge."""
        head = self.log.head
        assert lo == NEG or any(p[1] == lo for p in self.pieces), "a batch starts at a piece boundary"
        new_val = ("at", H, self.pat, self.ctx)
        keep = {k: self.decode(k) for k in failed}
        dev = {}
        for k, v in loaded.items():
            obs = (v, self.ctx) if v is not ABSENT and take(self.pat, k) else ABSENT
            if obs != self.held_by_piece([lo, hi, new_val, 0], k):
                dev[k] = obs
        at_h = self.log.state(H)
        new = []
        for p in self.pieces:
            plo, phi, val, cnt = p
            if phi <= lo or plo >= hi:
                new.append(p)  # untouched
                continue
            assert plo >= lo, "no left remainder: batches start at boundaries"
            if hi < phi:  # the batch cuts this piece: its right part keeps the rest
                if cnt is None:
                    rest = None
                else:
                    planned = sum(
                        1 for k in universe
                        if within(k, lo, hi) and within(k, plo, phi) and k not in self.points
                        and self.owed(self.held_by_piece(p, k), self.wanted(at_h.get(k), k))
                    )
                    since_h = sum(d for c, k, d in self.applied if c > H and within(k, lo, hi) and within(k, plo, phi))
                    rest = cnt - (planned + since_h)
                new.append([hi, phi, val, rest])
        self.points = {k: v for k, v in self.points.items() if not within(k, lo, hi)}
        newp = [lo, hi, new_val, 0]
        for k, v in keep.items():
            if v != self.held_by_piece(newp, k):
                self.points[k] = v
        for k, v in dev.items():
            self.points[k] = v
        # the new piece owes what changed since H in its range (a short diff), minus its points
        newp[3] = sum(
            1 for k in self.log.diff(H, head, lo, hi)
            if k not in self.points
            and self.owed(self.held_by_piece(newp, k), self.wanted(self.log.state(head).get(k), k))
        )
        new.append(newp)
        new.sort(key=lambda p: p[0])
        self.pieces = merge_adjacent(new, keep_boundary=hi)
        self.applied = [a for a in self.applied if a[0] > head - 3]  # only recent commits matter


def merge_adjacent(pieces, keep_boundary=None):
    out = []
    for p in pieces:
        if out and out[-1][2] == p[2] and out[-1][1] == p[0] and p[0] != keep_boundary:
            a, b = out[-1][3], p[3]
            out[-1][1] = p[1]
            out[-1][3] = None if a is None or b is None else a + b
        else:
            out.append(list(p))
    return out


PATTERNS = [None, frozenset({"k1", "k2", "k3"}), frozenset({"k2", "k4", "k5", "k6"}), frozenset({"k1", "k5"})]


def random_check(trials=3000, seed=1):
    rnd = random.Random(seed)
    steps = 0
    for _ in range(trials):
        keys = [f"k{i}" for i in range(1, rnd.randint(3, 7))] + (["x1"] if rnd.random() < 0.5 else [])
        universe = set(keys)
        log = Log()
        edge = Edge(log, pat=rnd.choice(PATTERNS), ctx=0)
        record = {}  # brute force: what the consumer holds, per key
        cursor = NEG
        stable_from = 0  # the oldest head a plan may sit at: after the last recount by scan
        for _ in range(rnd.randint(5, 40)):
            r = rnd.random()
            if r < 0.03:  # a held reset (a per-key full run's first batch)
                edge.own = {k for k in keys if rnd.random() < 0.5}
                edge.pieces = [[NEG, POS, ("held",), 0]]
                edge.points = {}
                edge.scan_count(edge.pieces[0], universe)
                record = {k: HELD for k in edge.own}
                cursor, stable_from = NEG, log.head
            elif r < 0.09:  # patterns or context change
                edge.change_conditions(rnd.choice(PATTERNS), edge.ctx + rnd.choice([0, 1]), universe)
                stable_from = log.head
            elif r < 0.55:
                clear = not edge.incomplete and rnd.random() < 0.06
                ch = {k: rnd.choice([ABSENT, 1, 2, 3]) for k in rnd.sample(keys, rnd.randint(1, len(keys)))}
                entries = log.commit(ch, clear=clear)
                edge.propagate(entries, log.head, universe)
            elif edge.incomplete:
                if rnd.random() < 0.4:  # the upstream's full run finishes
                    edge.upstream_complete(universe)
                    stable_from = log.head
            else:  # a batch (gated while the upstream is incomplete)
                H = rnd.randint(max(stable_from, log.head - 2), log.head)
                if rnd.random() < 0.15 or cursor == POS:
                    cursor = NEG  # a new run
                lo = cursor
                took, hi = edge.plan(H, lo, rnd.randint(1, 3), universe)
                failed = {k for k in took if rnd.random() < 0.15}
                loaded = {}
                for k in took:
                    if k in failed:
                        continue
                    v = log.state(H).get(k)
                    if rnd.random() < 0.1:
                        v = log.state(log.head).get(k)  # a store that moved on
                    loaded[k] = v
                    obs = (v, edge.ctx) if v is not ABSENT and take(edge.pat, k) else ABSENT
                    if obs is ABSENT:
                        record.pop(k, None)
                    else:
                        record[k] = obs
                edge.commit_batch(H, lo, hi, took, failed, loaded, universe)
                cursor = hi
                stable_from = log.head  # the next batch is planned after this commit
            steps += 1
            for k in universe:  # 1. progress decodes to what the consumer holds
                assert edge.decode(k) == record.get(k), (k, edge.decode(k), record.get(k))
            for p in edge.pieces:  # 2. counts kept by events 1-3 equal brute force
                if p[3] is not None:
                    assert p[3] == edge.count_brute(p, universe), (p, edge.count_brute(p, universe))
                else:
                    assert edge.incomplete
            for c in range(log.head + 1):  # 3. scan and diff from entries equal the true states
                assert log.scan(c) == log.state(c)
            c1 = rnd.randint(0, log.head)
            c2 = rnd.randint(c1, log.head)
            a, b = log.state(c1), log.state(c2)
            assert log.diff(c1, c2) == {k: (a.get(k), b.get(k)) for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k)}
    return steps


# -- the worked examples (docs/tracking-design.md) ----------------------------


def fmt(edge):
    ps = []
    for lo, hi, v, n in edge.pieces:
        lo_s = "(-inf" if lo == NEG else f"({lo}"
        hi_s = "+inf)" if hi == POS else f"{hi}]"
        val = f"at c{v[1]}" + ("" if v[3] == 0 else f"/ctx{v[3]}") if v[0] == "at" else v[0]
        ps.append(f"{lo_s}, {hi_s} {val} owes {'?' if n is None else n}")
    pts = ", ".join(f"{k}->{v if not isinstance(v, tuple) else v[0]}" for k, v in sorted(edge.points.items()))
    return " · ".join(ps) + (f" · points {pts}" if pts else "")


def examples():
    out = []
    p = out.append
    U = {"k1", "k2", "k3", "k4", "k5"}
    items = Log()
    tally = Edge(items)
    tally.propagate(items.commit({"k1": 1, "k2": 1, "k3": 1}), 1, U)
    took, hi = tally.plan(1, NEG, 2, U)
    tally.commit_batch(1, NEG, hi, took, set(), {k: 1 for k in took}, U)
    p(f"run1 b1 at c1 takes {took}: {fmt(tally)}")
    e2 = items.commit({"k2": 2, "k4": 2}); tally.propagate(e2, 2, U)
    p(f"c2: {fmt(tally)}")
    took, hi = tally.plan(2, "k2", 2, U)
    tally.commit_batch(2, "k2", hi, took, set(), {k: items.state(2)[k] for k in took}, U)
    p(f"run1 b2 at c2 takes {took}: {fmt(tally)}")
    tally.propagate(items.commit({"k3": ABSENT, "k5": 3}), 3, U); p(f"c3: {fmt(tally)}")
    tally.propagate(items.commit({"k1": 4}), 4, U); p(f"c4: {fmt(tally)}")
    p(f"diff(c1,c4) = {items.diff(1, 4)}")
    took, hi = tally.plan(4, NEG, 2, U)
    tally.commit_batch(4, NEG, hi, took, set(), {k: items.state(4).get(k) for k in took}, U)
    p(f"run2 b1 at c4 takes {took}: {fmt(tally)}; owed {sorted(tally.owed_brute(U))}")

    # F5: a batch planned at c4, committed after c5 (k2: 2 -> 5), cutting the piece (k2,+inf) at c2
    t2 = Edge(items)
    t2.pieces = [[NEG, "k2", ("at", 2, None, 0), 0], ["k2", POS, ("at", 2, None, 0), 0]]
    for p_ in t2.pieces:
        t2.scan_count(p_, U)
    p(f"split example start (head c4): {fmt(t2)}")
    took, hi = t2.plan(4, NEG, 1, U)
    e5 = items.commit({"k1": 5}); t2.propagate(e5, 5, U)
    p(f"  plan at c4 takes {took} range to {hi}; c5 k1 4->5 lands: {fmt(t2)}")
    t2.commit_batch(4, NEG, hi, took, set(), {"k1": 4}, U)
    p(f"  commit: {fmt(t2)}; brute {[t2.count_brute(q, U) for q in t2.pieces]}")

    # F2: context. checks at (-inf,+inf) at c4 under factor w1 (ctx 0); factor moves to w2 (ctx 1)
    items2 = Log(); items2.commit({"k1": 1, "k2": 1})
    checks = Edge(items2)
    checks.commit_batch(1, NEG, POS, ["k1", "k2"], set(), {"k1": 1, "k2": 1}, {"k1", "k2"})
    checks.change_conditions(None, 1, {"k1", "k2"})
    p(f"context moved: {fmt(checks)}")
    checks.propagate(items2.commit({"k1": 2}), 2, {"k1", "k2"})
    p(f"  k1 1->2 lands: {fmt(checks)} (brute {checks.count_brute(checks.pieces[0], {'k1', 'k2'})})")
    checks.propagate(items2.commit({"k3": 2}), 3, {"k1", "k2", "k3"})
    p(f"  k3 added: {fmt(checks)}")
    # patterns: copy excludes x1
    items3 = Log(); items3.commit({"k1": 1})
    copy = Edge(items3, pat=frozenset({"k1", "k2"}))
    copy.commit_batch(1, NEG, POS, ["k1"], set(), {"k1": 1}, {"k1"})
    copy.propagate(items3.commit({"x1": 1}), 2, {"k1", "x1"})
    p(f"x1 added, excluded: {fmt(copy)}")

    # F4: archive at copy#1; copy runs full: copy#2 a clear, copy#3 the rest
    cp = Log(); cp.commit({"k1": 1, "k2": 1, "k3": 1, "k4": 1})
    archive = Edge(cp)
    V = {"k1", "k2", "k3", "k4", "k5"}
    archive.commit_batch(1, NEG, POS, ["k1", "k2", "k3", "k4"], set(), {k: 1 for k in "k1 k2 k3 k4".split()}, V)
    archive.propagate(cp.commit({"k1": 2, "k2": 2}, clear=True), 2, V)
    p(f"copy#2 clear: {fmt(archive)} incomplete={archive.incomplete}")
    archive.propagate(cp.commit({"k4": 3, "k5": 3}), 3, V)
    archive.upstream_complete(V)
    p(f"copy complete at copy#3, counted once by scan: {fmt(archive)}; diff(1,3)={cp.diff(1, 3)}")
    return out


if __name__ == "__main__":
    n = random_check(int(sys.argv[1]) if len(sys.argv) > 1 else 3000)
    print(f"random histories: {n} steps checked")
    for line in examples():
        print(line)
