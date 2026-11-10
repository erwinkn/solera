"""The core of docs/tracking-design.md as a brute-force model: the output log
(§4: entries that carry both versions, clears, and diff, scan and get computed
from entries alone) and progress (§5: pieces and points, decoding, the planner
of §7 reading the log, and the batch commit rule).

The log also keeps the true state after every commit, so the checks can
compare what the queries compute from entries with the truth.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

ABSENT = None
HELD = "held"
NEG, POS = "", "\uffff"  # the ends of the key space: a range (lo, hi] excludes lo

EMPTY = ("empty",)
HELD_PIECE = ("held",)


def within(k: str, lo: str, hi: str) -> bool:
    return lo < k <= hi


@dataclass(frozen=True)
class Cond:
    """What a key is processed under (§9): the input's patterns (None takes every
    key), the context (the heads of whole and dep inputs) and the definition,
    which for a per-key asset includes its rebuild counter (amended P8)."""

    pat: frozenset[str] | None = None
    ctx: int = 0
    defn: tuple[int, ...] = (0,)

    def takes(self, k: str) -> bool:
        return self.pat is None or k in self.pat


class Log:
    """One output's log: commit c holds entries key -> (new, replaced)."""

    def __init__(self) -> None:
        self.entries: list[dict[str, tuple]] = [{}]
        self.clears: list[int] = []
        self.two_scans = True  # a diff across a clear merges two scans (off: entries only, a calibration)
        self.states: list[dict[str, object]] = [{}]

    @property
    def head(self) -> int:
        return len(self.entries) - 1

    def commit(self, changes: dict[str, object], clear: bool = False) -> dict[str, tuple]:
        """changes: key -> new version, or ABSENT. A clear removes every key it
        does not name; its entries still carry the true replaced version."""
        before = self.states[-1]
        entries = {
            k: (v, before.get(k))
            for k, v in sorted(changes.items())
            if (v is not ABSENT if clear else v != before.get(k))
        }
        after = {} if clear else dict(before)
        for k, (new, _) in entries.items():
            if new is ABSENT:
                after.pop(k, None)
            else:
                after[k] = new
        self.entries.append(entries)
        self.states.append(after)
        if clear:
            self.clears.append(self.head)
        return entries

    def last_clear(self, c: int) -> int:
        return max((x for x in self.clears if x <= c), default=0)

    def since(self, k: str, c: int) -> int:
        """The commit that wrote k's state at c, or the latest clear if later:
        what the engine learns with the replaced version (§6, event 1)."""
        for i in range(c, 0, -1):
            if k in self.entries[i] or i in self.clears:
                return i
        return 0

    def scan(self, c: int, lo: str = NEG, hi: str = POS) -> dict[str, object]:
        out: dict[str, object] = {}
        for i in range(max(1, self.last_clear(c)), c + 1):
            for k, (new, _) in self.entries[i].items():
                if within(k, lo, hi):
                    out[k] = new
        return {k: v for k, v in out.items() if v is not ABSENT}

    def get(self, c: int, k: str) -> object:
        for i in range(c, max(1, self.last_clear(c)) - 1, -1):
            if k in self.entries[i]:
                return self.entries[i][k][0]
        return ABSENT

    def diff(self, c1: int, c2: int, lo: str = NEG, hi: str = POS) -> dict:
        """Keys whose state differs between c1 and c2: (state at c1, state at c2).
        Across a clear it merges two scans."""
        if self.two_scans and any(c1 < x <= c2 for x in self.clears):
            a, b = self.scan(c1, lo, hi), self.scan(c2, lo, hi)
            return {k: (a.get(k), b.get(k)) for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k)}
        first: dict[str, object] = {}
        last: dict[str, object] = {}
        for i in range(c1 + 1, c2 + 1):
            for k, (new, rep) in self.entries[i].items():
                if within(k, lo, hi):
                    first.setdefault(k, rep)
                    last[k] = new
        return {k: (first[k], last[k]) for k in sorted(first) if first[k] != last[k]}


class Progress:
    """A consumer's progress on one keyed input (§5): pieces [lo, hi, value]
    covering the key space, and points overriding them.

    A piece value is ("at", P, cond), EMPTY or HELD_PIECE. An observation (what
    the consumer holds for a key) is ABSENT, HELD, or (version, cond)."""

    def __init__(self, log: Log, own: Callable[[], set[str]] = set) -> None:
        self.log = log
        self.own = own  # the keys the consumer's own output holds, for HELD pieces
        self.pieces: list[list] = [[NEG, POS, EMPTY]]
        self.points: dict[str, object] = {}

    def copy(self) -> Progress:
        p = Progress(self.log, self.own)
        p.pieces = [list(x) for x in self.pieces]
        p.points = dict(self.points)
        return p

    def reset(self, value: tuple = EMPTY) -> None:
        self.pieces = [[NEG, POS, value]]
        self.points = {}

    # -- decoding (§5) -----------------------------------------------------------
    def piece_of(self, k: str) -> list:
        return next(p for p in self.pieces if within(k, p[0], p[1]))

    def held_by(self, value: tuple, k: str) -> object:
        if value[0] == "at":
            v = self.log.get(value[1], k)
            return (v, value[2]) if v is not ABSENT and value[2].takes(k) else ABSENT
        if value == EMPTY:
            return ABSENT
        return HELD if k in self.own() else ABSENT

    def decode(self, k: str) -> object:
        return self.points[k] if k in self.points else self.held_by(self.piece_of(k)[2], k)

    def wanted(self, k: str, c: int, cond: Cond) -> object:
        v = self.log.get(c, k)
        return (v, cond) if v is not ABSENT and cond.takes(k) else ABSENT

    # -- planning (§7: candidates come from the log, per piece) -------------------------
    def candidates(self, H: int, lo: str, cond: Cond) -> list[str]:
        """Every key after lo owed at H under cond, read from the log as the
        table "Where candidates come from" says, never from a key universe."""
        keys: set[str] = {k for k in self.points if k > lo}
        for plo, phi, val in self.pieces:
            if phi <= lo:
                continue
            a = max(plo, lo)
            if val[0] == "at":
                keys |= set(self.log.diff(val[1], H, a, phi))
                if val[2] != cond:
                    keys |= set(self.log.scan(H, a, phi))
            else:
                keys |= set(self.log.scan(H, a, phi))
                if val == HELD_PIECE:
                    keys |= {k for k in self.own() if within(k, a, phi)}
        return sorted(k for k in keys if self.decode(k) != self.wanted(k, H, cond))

    def plan(self, H: int, lo: str, n: int, cond: Cond, due: Callable[[str], bool] = lambda k: True):
        """The next batch after lo: (taken, hi, held back). The range ends at the
        last key taken, or at +inf if the candidates ran out; held back are the
        owed keys in the range that are not due (failing keys awaiting retry)."""
        owed = self.candidates(H, lo, cond)
        due_keys = [k for k in owed if due(k)]
        taken = due_keys[:n]
        hi = taken[-1] if len(due_keys) > n else POS
        return taken, hi, [k for k in owed if within(k, lo, hi) and not due(k)]

    # -- what a batch commit writes (§5) -----------------------------------------------
    def commit_batch(self, H: int, lo: str, hi: str, cond: Cond, results: dict, kept) -> None:
        """One piece (lo, hi] at(H, cond), and a point for exactly the keys whose
        holding differs from it: kept keys (failed, held back) keep what they
        held; results (key -> what the consumer now holds) deviate if they differ
        from what at(H) says. Every other point in the range is dropped."""
        value = ("at", H, cond)
        old = {k: self.decode(k) for k in kept}
        new = []
        for p in self.pieces:
            if p[1] <= lo or p[0] >= hi:
                new.append(p)
                continue
            if p[0] < lo:
                new.append([p[0], lo, p[2]])
            if hi < p[1]:
                new.append([hi, p[1], p[2]])
        new.append([lo, hi, value])
        self.pieces = sorted(new, key=lambda p: p[0])
        self.points = {k: v for k, v in self.points.items() if not within(k, lo, hi)}
        for k, v in [*old.items(), *results.items()]:
            if v != self.held_by(value, k):
                self.points[k] = v

    def commit_points(self, results: dict, failed=()) -> None:
        """A key-list run (§10): points only, the pieces stay. A failing key
        always keeps what it held as a point (P9), even where its piece says the
        same, so that a piece counts no failing key (§6)."""
        for k in failed:
            self.points[k] = self.decode(k)
        for k, v in results.items():
            self.points.pop(k, None)
            if v != self.decode(k):
                self.points[k] = v

    def merge(self, keep: str | None) -> None:
        """Adjacent equal pieces merge, except at the run's progress key (§6, event 3)."""
        out: list[list] = []
        for p in self.pieces:
            if out and out[-1][2] == p[2] and out[-1][1] == p[0] and p[0] != keep:
                out[-1] = [out[-1][0], p[1], p[2]]
            else:
                out.append(list(p))
        self.pieces = out
