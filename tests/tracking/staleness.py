"""One consumer input, checks over items, over random histories, checked after
every step against brute force: what the consumer really holds, the keys it
owes and the keys that are due. Staleness is asked of the planner (STA-3: a
default run would load something), so there is nothing kept to go wrong
between two asks.

A history mixes upstream commits that revert, an upstream full run in the
design's form (a clear, then "upstream complete"), batches planned and
committed later, keys loaded at another version, failing keys that are owed
but not due, retries that come due, pattern, context and definition changes,
key-list runs, an ordinary consumer's full run (its first batch resets
progress to empty), progress reset to held, and restarts.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, replace

from .core import ABSENT, EMPTY, HELD, HELD_PIECE, NEG, POS, Cond, Log, Progress, within

KEYS = ("k1", "k2", "k3", "k4", "k5", "x1")
PATTERNS = (None, frozenset({"k1", "k2", "k3", "k4", "k5"}), frozenset({"k2", "k4", "x1"}))

# The rules, by the names a calibration switches off.
RULES = ("hold_back",)


@dataclass
class Fail:
    on: object  # what the key would have held had it succeeded
    retry: bool = False  # its retry came due: backoff over, or forced


@dataclass
class Batch:
    kind: str  # "range" or "keys"
    H: int
    lo: str
    hi: str
    cond: Cond
    taken: list[str]
    held_back: list[str]
    step: int  # the world's step when it was planned
    reset: bool = False


class World:
    """One consumer input and the truth about it."""

    def __init__(self, off: frozenset[str] = frozenset()) -> None:
        self.off = off
        self.up = Log()
        self.cond = Cond(PATTERNS[0])
        self.holds: dict[str, object] = {}  # brute force: what the consumer holds
        self.outcomes: dict[str, Fail] = {}
        self.failed: dict[str, Fail] = {}  # brute force: keys whose last processing failed, by KEY-3
        self.covered: list[tuple[str, str]] = []  # brute force: ranges processed since the last reset
        self.listed: set[str] = set()  # brute force: keys a key list processed since then
        self.prog = Progress(self.up, own=lambda: {k for k, v in self.holds.items() if v is not ABSENT})
        self.pending: Batch | None = None
        self.cursor: str | None = None  # the run's progress key; None when no run is going
        self.incomplete = False  # items is mid full run, in the design's form (a clear)
        self.mid = {0: False}  # per items commit: whether items was mid full run at it
        self.violation: str | None = None  # an invariant broken inside an event
        self.step = 0
        self.trace: list[str] = []

    def on(self, rule: str) -> bool:
        return rule not in self.off

    def waiting(self, k: str, c: int, outcomes: dict | None = None) -> bool:
        f = (self.outcomes if outcomes is None else outcomes).get(k)
        return f is not None and not f.retry and f.on == self.prog.wanted(k, c, self.cond)

    def due_at(self, k: str, c: int) -> bool:
        return self.prog.decode(k) != self.prog.wanted(k, c, self.cond) and not self.waiting(k, c)

    def due(self, k: str) -> bool:
        return self.due_at(k, self.up.head)

    # -- events ----------------------------------------------------------------
    def apply(self, ev: tuple) -> None:
        self.step += 1
        getattr(self, "ev_" + ev[0])(*ev[1:])

    def ev_up(self, changes: tuple) -> None:
        if any(self.up.states[-1].get(k) != v for k, v in changes):
            self.up.commit(dict(changes))
            self.mid[self.up.head] = self.incomplete
            self.trace.append(f"items c{self.up.head}: {_entries(self.up.entries[-1])}")
            self.reverted()

    def ev_clear(self, changes: tuple) -> None:
        if not self.incomplete:
            self.up.commit(dict(changes), clear=True)
            self.incomplete = True
            self.mid[self.up.head] = True
            self.trace.append(f"items c{self.up.head}: clear; {_entries(self.up.entries[-1])}")
            self.reverted()

    def ev_complete(self) -> None:
        if self.incomplete:
            self.incomplete = False
            self.mid[self.up.head] = False
            self.trace.append("items completes its full run")

    def reverted(self) -> None:
        """A failing key's outcome goes once the key is no longer owed: upstream
        returned to what the consumer holds, or the conditions no longer take it.
        Its point stays (§11, KEY-3, review F8). The truth drops it too."""
        head = self.up.head
        for record in (self.outcomes, self.failed):
            for k in [k for k in record if self.holds.get(k) == self.prog.wanted(k, head, self.cond)]:
                del record[k]

    def ev_plan(self, n: int) -> None:
        if self.pending is None and not self.incomplete:
            lo = self.cursor or NEG
            taken, hi, back = self.prog.plan(self.up.head, lo, n, self.cond, self.due)
            truth = sorted(k for k in self.brute()[1] if k > lo)[:n]
            if taken != truth:
                self.violation = f"the plan after {lo or '-inf'} took {taken}, brute force {truth}"
            self.pending = Batch("range", self.up.head, lo, hi, self.cond, taken, back, self.step)
            self.trace.append(f"checks plans at c{self.up.head}: {taken}, range ({_k(lo)}, {_k(hi)}]")

    def ev_full(self, n: int) -> None:
        if self.pending is None and not self.incomplete:
            fresh = self.prog.copy()
            fresh.reset()
            taken, hi, _ = fresh.plan(self.up.head, NEG, n, self.cond)
            self.pending = Batch("range", self.up.head, NEG, hi, self.cond, taken, [], self.step, reset=True)
            self.trace.append(f"checks starts a full run at c{self.up.head}: {taken}, range (-inf, {_k(hi)}]")

    def ev_keys(self, keys: tuple) -> None:
        if self.pending is None and not self.incomplete:
            self.pending = Batch("keys", self.up.head, NEG, POS, self.cond, sorted(keys), [], self.step)
            self.trace.append(f"checks plans keys={sorted(keys)} at c{self.up.head}")

    def ev_commit(self, fails: tuple, moved: tuple) -> None:
        b, self.pending = self.pending, None
        if b is None:
            return
        if b.cond.defn != self.cond.defn:
            self.trace.append("checks' commit is refused: its definition changed")
            return
        before = {} if b.reset else dict(self.holds)
        if b.reset:
            self.holds, self.outcomes, self.failed = {}, {}, {}
            self.covered, self.listed = [], set()
            self.prog.reset()
        results, kept = {}, set(b.held_back)
        cleared = any(b.H < x <= self.up.head for x in self.up.clears)
        for k in b.taken:
            obs = self.prog.wanted(k, self.up.head if k in moved else b.H, b.cond)
            planned = self.prog.wanted(k, b.H, b.cond)
            if obs != planned and (self.incomplete or cleared) and self.on("hold_back"):
                kept.add(k)
                continue
            if k in fails:
                self.outcomes[k] = Fail(obs)
                self.failed[k] = Fail(obs)
                kept.add(k)
                continue
            real = {self.prog.wanted(k, c, b.cond) for c in range(b.H, self.up.head + 1) if not self.mid[c]}
            if obs not in real and obs != before.get(k):
                self.violation = f"checks processes {_k(k)} as {obs}, a state items had only mid full run"
            results[k] = obs
            self.holds[k] = obs
            self.outcomes.pop(k, None)
            self.failed.pop(k, None)
        self.holds = {k: v for k, v in self.holds.items() if v is not ABSENT}
        if b.kind == "keys":
            self.prog.commit_points(results, kept)
            self.listed |= set(b.taken)
        else:
            self.prog.commit_batch(b.H, b.lo, b.hi, b.cond, results, kept)
            self.covered.append((b.lo, b.hi))
            self.cursor = None if b.hi == POS else b.hi
        self.outcomes = {
            k: f for k, f in self.outcomes.items() if k in self.prog.points
        }  # they go together (§11)
        self.reverted()
        failed = sorted(set(fails) & set(b.taken))
        moved_ = sorted(set(moved) & set(results))
        self.trace.append(
            "checks commits"
            + (f"; {failed} fail" if failed else "")
            + (f"; {moved_} loaded at the head" if moved_ else "")
        )
        self.prog.merge(self.cursor)

    def ev_cancel(self) -> None:
        if self.pending or self.cursor:
            self.pending, self.cursor = None, None
            self.trace.append("checks' run is canceled")

    def ev_pat(self, i: int) -> None:
        if self.cond.pat != PATTERNS[i]:
            self.cond = replace(self.cond, pat=PATTERNS[i])
            self.trace.append(f"checks' patterns become {sorted(PATTERNS[i]) if PATTERNS[i] else 'all'}")
            self.reverted()

    def ev_ctx(self) -> None:
        self.cond = replace(self.cond, ctx=self.cond.ctx + 1)
        self.trace.append(f"factor moves to w{self.cond.ctx + 1}")
        self.reverted()

    def ev_defn(self) -> None:
        self.cond = replace(self.cond, defn=(self.cond.defn[0] + 1,))
        self.trace.append(f"checks' definition becomes d{self.cond.defn[0]}")
        self.reverted()

    def ev_held(self) -> None:
        self.prog.reset(HELD_PIECE)
        self.holds = {k: HELD for k in self.holds}
        self.outcomes, self.failed, self.pending, self.cursor = {}, {}, None, None
        self.covered, self.listed = [(NEG, POS)], set()
        self.trace.append("checks' progress becomes held (it cannot be trusted)")

    def ev_retry(self, k: str) -> None:
        f = self.outcomes.get(k)
        if f is not None and not f.retry:
            f.retry = True
            if k in self.failed:
                self.failed[k].retry = True
            self.trace.append(f"{k}'s retry comes due")

    def ev_restart(self) -> None:
        self.pending = None
        self.trace.append("the engine restarts")

    # -- the truth -------------------------------------------------------------
    def brute(self) -> tuple[set[str], set[str]]:
        """(owed, due) from what the consumer really holds."""
        head = self.up.head
        owed = {k for k in KEYS if self.holds.get(k) != self.prog.wanted(k, head, self.cond)}
        return owed, {k for k in owed if not self.waiting(k, head, self.failed)}

    def brute_complete(self) -> bool:
        """D177 from what was processed: every upstream key the patterns take lies
        in a range processed since the last reset, or a key list processed it."""
        return all(
            k in self.listed or any(within(k, lo, hi) for lo, hi in self.covered)
            for k in self.up.states[-1]
            if self.cond.takes(k)
        )

    def stale(self) -> bool:
        """STA-3: a default run would load something. The planner, asked for one key."""
        return bool(self.prog.plan(self.up.head, NEG, 1, self.cond, self.due)[0])

    def complete(self) -> bool:
        """D177 from progress: no range with nothing processed holds an upstream key."""
        return not any(
            k not in self.prog.points and self.cond.takes(k)
            for p in self.prog.pieces
            if p[2] == EMPTY
            for k in self.up.scan(self.up.head, p[0], p[1])
        )

    def check(self) -> str | None:
        if self.violation:
            return self.violation
        for k in KEYS:
            if self.prog.decode(k) != self.holds.get(k):
                return f"progress decodes {k} as {self.prog.decode(k)}, checks holds {self.holds.get(k)}"
        owed, due = self.brute()
        if self.prog.candidates(self.up.head, NEG, self.cond) != sorted(owed):
            return f"counted on request: {self.prog.candidates(self.up.head, NEG, self.cond)}, owed {sorted(owed)}"
        if self.incomplete:
            return None  # held: nothing is due while items is mid full run
        if self.stale() != bool(due):
            return f"checks reads {'stale' if self.stale() else 'fresh'}; due: {sorted(due)}"
        if self.complete() != self.brute_complete():
            return f"complete reads {self.complete()}, brute force {self.brute_complete()}"
        return None


# -- histories ---------------------------------------------------------------------
def history(rnd: random.Random, steps: int) -> list[tuple]:
    """A random history: a list of events, each applied or skipped by the world."""
    kinds = (
        ("up",) * 10
        + ("plan",) * 5
        + ("commit",) * 6
        + (
            "clear",
            "complete",
            "complete",
            "full",
            "keys",
            "cancel",
            "pat",
            "ctx",
            "defn",
            "held",
            "retry",
            "retry",
            "restart",
        )
    )
    events: list[tuple] = []
    for _ in range(steps):
        kind = rnd.choice(kinds)
        if kind in ("up", "clear"):
            changes = tuple(
                (k, rnd.choice((ABSENT, 1, 2, 3))) for k in sorted(rnd.sample(KEYS, rnd.randint(1, 3)))
            )
            events.append((kind, changes))
        elif kind in ("plan", "full"):
            events.append((kind, rnd.randint(1, 3)))
        elif kind == "commit":
            pick = lambda p: tuple(k for k in KEYS if rnd.random() < p)  # noqa: E731
            events.append((kind, pick(0.15), pick(0.1)))
        elif kind == "keys":
            events.append((kind, tuple(sorted(rnd.sample(KEYS, rnd.randint(1, 2))))))
        elif kind == "pat":
            events.append((kind, rnd.randrange(len(PATTERNS))))
        elif kind == "retry":
            events.append((kind, rnd.choice(KEYS)))
        else:
            events.append((kind,))
    return events


def run(events: list[tuple], off: frozenset[str] = frozenset()) -> tuple[int, str, World] | None:
    """Apply a history, checking after every step: (step, violation, world), or None."""
    w = World(off)
    for i, ev in enumerate(events):
        w.apply(ev)
        bad = w.check()
        if bad:
            return i, bad, w
    return None


def _k(k: str) -> str:
    return "-inf" if k == NEG else "+inf" if k == POS else k


def _entries(entries: dict) -> str:
    return " · ".join(
        f"{k}: {'−' if r is None else r} → {'−' if n is None else n}" for k, (n, r) in entries.items()
    )
