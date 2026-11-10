"""Staleness kept exact (§6): the design's exact owed counts per piece (P14)
against the amendment's one witness due key per input
(handoffs/SOLE-4/amendment-staleness.md in the Captain drive).

Both forms run over the same random histories of one consumer input, checks
on items, and are checked after every step against brute force: what the
consumer really holds, the keys it owes and the keys that are due. A history
mixes upstream commits that revert, an upstream full run in the design's form
(a clear, then "upstream complete"), batches planned and committed later,
keys loaded at another version, failing keys that are owed but not due, retries
that come due by time or by force, pattern, context and definition changes,
key-list runs, an ordinary consumer's full run (its first batch resets progress
to empty), progress reset to held, lost notifications and restarts.
"""

from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, replace

from .core import ABSENT, EMPTY, HELD, HELD_PIECE, NEG, POS, Cond, Log, Progress, within

KEYS = ("k1", "k2", "k3", "k4", "k5", "x1")
PATTERNS = (None, frozenset({"k1", "k2", "k3", "k4", "k5"}), frozenset({"k2", "k4", "x1"}))

# Each form's rules, by the names a calibration switches off. The witness's last
# three are this model's additions to the amendment (see docs/verification.md).
RULES = {
    "counts": (
        "propagate",
        "since",
        "clear",
        "split",
        "new_piece",
        "new_piece_conditions",
        "split_recount",
        "conditions",
        "reset",
        "keys",
        "replay",
    ),
    "witness": (
        "rule1",
        "rule2",
        "rule3_recheck",
        "rule3_range",
        "rule4",
        "rule5",
        "rule7",
        "rule8",
        "reset_probe",
        "held_probe",
        "catch_up_first",
        "rearm",
    ),
}
ADDED = {
    "counts": ("new_piece_conditions", "split_recount", "keys"),
    "witness": ("reset_probe", "held_probe", "catch_up_first", "rearm"),
}


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

    def __init__(self, form: str, off: frozenset[str] = frozenset()) -> None:
        self.off = off
        self.up = Log()
        self.cond = Cond(PATTERNS[0])
        self.holds: dict[str, object] = {}  # brute force: what the consumer holds
        self.outcomes: dict[str, Fail] = {}
        self.prog = Progress(self.up, own=lambda: {k for k, v in self.holds.items() if v is not ABSENT})
        self.pending: Batch | None = None
        self.cursor: str | None = None  # the run's progress key; None when no run is going
        self.incomplete = False  # items is mid full run, in the design's form (a clear)
        self.violation: str | None = None  # an invariant a form breaks inside a rule
        self.step = 0
        self.trace: list[str] = []
        self.form: Counts | Witness = Counts(self) if form == "counts" else Witness(self)

    def on(self, rule: str) -> bool:
        return rule not in self.off

    def waiting(self, k: str, c: int) -> bool:
        f = self.outcomes.get(k)
        return f is not None and not f.retry and f.on == self.prog.wanted(k, c, self.cond)

    def due_at(self, k: str, c: int) -> bool:
        return self.prog.decode(k) != self.prog.wanted(k, c, self.cond) and not self.waiting(k, c)

    def due(self, k: str) -> bool:
        return self.due_at(k, self.up.head)

    # -- events ----------------------------------------------------------------
    def apply(self, ev: tuple) -> None:
        self.step += 1
        getattr(self, "ev_" + ev[0])(*ev[1:])

    def ev_up(self, changes: tuple, lost: bool) -> None:
        if any(self.up.states[-1].get(k) != v for k, v in changes):
            self.up.commit(dict(changes))
            self.trace.append(
                f"items c{self.up.head}: {_entries(self.up.entries[-1])}" + (" (lost)" if lost else "")
            )
            self.form.on_commit(lost)

    def ev_clear(self, changes: tuple, lost: bool) -> None:
        if not self.incomplete:
            self.up.commit(dict(changes), clear=True)
            self.incomplete = True
            self.trace.append(f"items c{self.up.head}: clear; {_entries(self.up.entries[-1])}")
            self.form.on_commit(lost)

    def ev_complete(self) -> None:
        if self.incomplete:
            self.incomplete = False
            self.trace.append("items completes its full run")
            self.form.on_complete()

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
        snap = self.form.before(b)
        if b.reset:
            self.holds, self.outcomes = {}, {}
            self.prog.reset()
        results, kept = {}, set(b.held_back)
        for k in b.taken:
            if k in fails:
                self.outcomes[k] = Fail(self.prog.wanted(k, b.H, b.cond))
                kept.add(k)
                continue
            obs = self.prog.wanted(k, self.up.head if k in moved else b.H, b.cond)
            results[k] = obs
            self.holds[k] = obs
            self.outcomes.pop(k, None)
        self.holds = {k: v for k, v in self.holds.items() if v is not ABSENT}
        if b.kind == "keys":
            self.prog.commit_points(results, kept)
        else:
            self.prog.commit_batch(b.H, b.lo, b.hi, b.cond, results, kept)
            self.cursor = None if b.hi == POS else b.hi
        self.outcomes = {
            k: f for k, f in self.outcomes.items() if k in self.prog.points
        }  # they go together (§11)
        failed = sorted(set(fails) & set(b.taken))
        moved_ = sorted(set(moved) & set(results))
        self.trace.append(
            "checks commits"
            + (f"; {failed} fail" if failed else "")
            + (f"; {moved_} loaded at the head" if moved_ else "")
        )
        self.form.after(b, snap)
        self.prog.merge(self.cursor)
        self.form.remap()
        self.form.save()

    def ev_cancel(self) -> None:
        if self.pending or self.cursor:
            self.pending, self.cursor = None, None
            self.trace.append("checks' run is canceled")

    def ev_pat(self, i: int) -> None:
        if self.cond.pat != PATTERNS[i]:
            self.cond = replace(self.cond, pat=PATTERNS[i])
            self.trace.append(f"checks' patterns become {sorted(PATTERNS[i]) if PATTERNS[i] else 'all'}")
            self.form.on_conditions()

    def ev_ctx(self) -> None:
        self.cond = replace(self.cond, ctx=self.cond.ctx + 1)
        self.trace.append(f"factor moves to w{self.cond.ctx + 1}")
        self.form.on_conditions()

    def ev_defn(self) -> None:
        self.cond = replace(self.cond, defn=(self.cond.defn[0] + 1,))
        self.trace.append(f"checks' definition becomes d{self.cond.defn[0]}")
        self.form.on_conditions()

    def ev_held(self) -> None:
        self.prog.reset(HELD_PIECE)
        self.holds = {k: HELD for k in self.holds}
        self.outcomes, self.pending, self.cursor = {}, None, None
        self.trace.append("checks' progress becomes held (it cannot be trusted)")
        self.form.on_reset()

    def ev_retry(self, k: str, timer: bool) -> None:
        f = self.outcomes.get(k)
        if f is not None and not f.retry:
            f.retry = True
            self.trace.append(f"{k}'s retry comes due" + (" (backoff over)" if timer else " (forced)"))
            self.form.on_retry(k, timer)

    def ev_restart(self) -> None:
        self.pending = None
        self.trace.append("the engine restarts")
        self.form.on_restart()

    # -- the truth -------------------------------------------------------------
    def brute(self) -> tuple[set[str], set[str]]:
        """(owed, due) from what the consumer really holds."""
        head = self.up.head
        owed = {k for k in KEYS if self.holds.get(k) != self.prog.wanted(k, head, self.cond)}
        return owed, {k for k in owed if not self.waiting(k, head)}

    def brute_complete(self) -> bool:
        now = self.up.states[-1]
        return not any(
            k in now
            and self.cond.takes(k)
            and k not in self.prog.points
            and self.prog.piece_of(k)[2] == EMPTY
            for k in KEYS
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
        answer = self.form.answer()
        if self.violation:
            return self.violation
        if self.incomplete:
            return None if answer == ("held",) else f"items is mid full run, but checks reads {answer}"
        if answer[0] != ("stale" if due else "fresh"):
            return f"checks reads {answer[0]}; due: {sorted(due)}"
        if answer[0] == "stale" and answer[1] is not None and answer[1] not in due:
            return f"the witness {answer[1]} is not due; due: {sorted(due)}"
        if self.form.complete() != self.brute_complete():
            return f"complete reads {self.form.complete()}, brute force {self.brute_complete()}"
        return self.form.check()


class Counts:
    """P14: per piece, the exact number of owed keys without a point, kept by
    four events; due points are counted one by one when asked."""

    def __init__(self, w: World) -> None:
        self.w = w
        self.n: dict[tuple[str, str], int | None] = {(NEG, POS): 0}
        self.applied: list[tuple[int, str, int]] = []  # what propagation did since the live attempt planned
        self.stats: Counter = Counter()
        self.recounted = 0  # the step of the last count by scan
        self.save()

    def save(self) -> None:
        """Counts are checkpointed with progress, at every event of the consumer."""
        self.saved = (dict(self.n), list(self.applied), self.w.up.head)

    def owed_under(self, p: list, k: str) -> bool:
        w = self.w
        return w.prog.held_by(p[2], k) != w.prog.wanted(k, w.up.head, w.cond)

    def scan(self, p: list) -> int | None:
        w = self.w
        if w.incomplete:
            return None  # counted once the upstream is complete again (event 2)
        self.stats["scanned"] += 1
        self.recounted = w.step
        return sum(
            1
            for k in w.prog.candidates(w.up.head, p[0], w.cond)
            if within(k, p[0], p[1]) and k not in w.prog.points
        )

    def on_commit(self, lost: bool) -> None:
        w, c = self.w, self.w.up.head
        if c in w.up.clears and self.w.on("clear"):
            self.n = dict.fromkeys(self.n)
            return
        for k, (new, rep) in w.up.entries[c].items():
            if k in w.prog.points or not w.on("propagate"):
                continue
            p = w.prog.piece_of(k)
            if self.n[(p[0], p[1])] is None:
                continue
            self.stats["adjusted"] += 1
            val = p[2]
            if val[0] == "at" and (not w.on("since") or w.up.since(k, c - 1) <= val[1]):
                held = (rep, val[2]) if rep is not ABSENT and val[2].takes(k) else ABSENT
            else:
                self.stats["lookups"] += val[0] == "at"
                held = w.prog.held_by(val, k)
            before = (rep, w.cond) if rep is not ABSENT and w.cond.takes(k) else ABSENT
            after = (new, w.cond) if new is not ABSENT and w.cond.takes(k) else ABSENT
            delta = int(held != after) - int(held != before)
            self.n[(p[0], p[1])] += delta
            self.applied.append((c, k, delta))

    def recount(self) -> None:
        self.n = {(p[0], p[1]): self.scan(p) for p in self.w.prog.pieces}

    def on_complete(self) -> None:
        self.n = {
            (p[0], p[1]): self.scan(p) if self.n[(p[0], p[1])] is None else self.n[(p[0], p[1])]
            for p in self.w.prog.pieces
        }
        self.save()

    def on_conditions(self) -> None:
        if self.w.on("conditions"):
            self.recount()
        self.save()

    def on_reset(self) -> None:
        if self.w.on("reset"):
            self.recount()
        else:
            self.n = {(NEG, POS): 0}
        self.applied = []
        self.save()

    def before(self, b: Batch):
        w = self.w
        counted = {k for k in b.taken if k not in w.prog.points and self.owed_under(w.prog.piece_of(k), k)}
        return [list(p) for p in w.prog.pieces], dict(self.n), set(w.prog.points), counted

    def after(self, b: Batch, snap) -> None:
        w = self.w
        old_pieces, old_n, old_points, counted = snap
        if b.reset:
            self.recount() if w.on("reset") else None
            self.applied = []
            return
        if b.kind == "keys":
            for k in b.taken:
                p = w.prog.piece_of(k)
                if self.n[(p[0], p[1])] is not None and w.on("keys"):
                    now = k not in w.prog.points and self.owed_under(p, k)
                    self.n[(p[0], p[1])] += now - (k in counted)
            return
        n: dict[tuple[str, str], int | None] = {}
        for p in w.prog.pieces:
            key = (p[0], p[1])
            if key == (b.lo, b.hi):
                if w.incomplete:
                    n[key] = None
                elif b.cond != w.cond and w.on("new_piece_conditions"):
                    n[key] = self.scan(p)
                elif w.on("new_piece"):
                    diff = w.up.diff(b.H, w.up.head, b.lo, b.hi)
                    n[key] = sum(1 for k in diff if k not in w.prog.points and self.owed_under(p, k))
                else:
                    n[key] = 0
            elif key in old_n:
                n[key] = old_n[key]
            else:  # what is left of the piece the batch cut
                src = next(q for q in old_pieces if q[0] <= p[0] and p[1] == q[1])
                count = old_n[(src[0], src[1])]
                if self.recounted > b.step and w.on("split_recount"):
                    count = self.scan(p)
                elif count is not None and w.on("split"):

                    def inside(k: str, src: list = src) -> bool:
                        return within(k, b.lo, b.hi) and within(k, src[0], src[1])

                    planned = sum(1 for k in b.taken + b.held_back if inside(k) and k not in old_points)
                    count -= planned + sum(d for c, k, d in self.applied if c > b.H and inside(k))
                n[key] = count
        self.n = n
        self.applied = []

    def remap(self) -> None:
        n = {}
        for p in self.w.prog.pieces:
            parts = [v for (lo, hi), v in self.n.items() if p[0] <= lo and hi <= p[1]]
            n[(p[0], p[1])] = None if None in parts else sum(parts)  # type: ignore[arg-type]
        self.n = n

    def on_retry(self, k: str, timer: bool) -> None:
        pass  # due points are counted one by one, when asked

    def on_restart(self) -> None:
        n, applied, head = self.saved
        self.n, self.applied = dict(n), list(applied)
        if self.w.on("replay"):
            replay, self.w.up.entries = self.w.up.entries, self.w.up.entries[: head + 1]
            for c in range(head + 1, len(replay)):
                self.w.up.entries.append(replay[c])
                self.on_commit(False)
        self.save()

    def answer(self) -> tuple:
        w = self.w
        if None in self.n.values():
            return ("held",)
        if any(v for v in self.n.values()) or any(w.due(k) for k in w.prog.points):
            return ("stale", None)
        return ("fresh",)

    def complete(self) -> bool:
        return not any(p[2] == EMPTY and self.n[(p[0], p[1])] for p in self.w.prog.pieces)

    def check(self) -> str | None:
        w = self.w
        for p in w.prog.pieces:
            truth = sum(
                1 for k in KEYS if within(k, p[0], p[1]) and k not in w.prog.points and self.owed_under(p, k)
            )
            if self.n[(p[0], p[1])] != truth:
                return f"piece ({_k(p[0])}, {_k(p[1])}] counts {self.n[(p[0], p[1])]}, brute force {truth}"
        return None


class Witness:
    """The amendment: per input, fresh(X) or stale(w, X), where w is a due key."""

    def __init__(self, w: World) -> None:
        self.w = w
        self.state: tuple = ("fresh",)
        self.X = 0
        self.stats: Counter = Counter()
        self.save()

    def save(self) -> None:
        """The state is persisted with every event of the consumer; what upstream
        notifications and timers change in between is not (rule 7 restores it)."""
        self.saved = (self.state, self.X)

    def stale(self, k: str) -> None:
        self.state = ("stale", k)

    def probe(self, lo: str = NEG, hi: str = POS, keys: list[str] | None = None) -> None:
        """Rule 6: the planner with a limit of one key, over a range or a key list."""
        w = self.w
        self.stats["probes"] += 1
        if w.incomplete and w.on("rule8"):
            self.state = ("held",)
            return
        cands = [k for k in (keys or w.prog.candidates(w.up.head, lo, w.cond)) if within(k, lo, hi)]
        found = next((k for k in cands if w.due(k)), None)
        self.stats["probe_keys"] += cands.index(found) + 1 if found else len(cands)
        self.state = ("stale", found) if found else ("fresh",)
        if (lo, hi, keys) == (NEG, POS, None):
            self.X = w.up.head

    def catch_up(self, c: int) -> None:
        """Rules 1 and 2 for every upstream commit after X, in order."""
        w = self.w
        while self.X < c:
            self.X += 1
            x, entries = self.X, w.up.entries[self.X]
            if x in w.up.clears and w.on("rule8"):
                self.state = ("held",)
            elif self.state[0] == "fresh" and w.on("rule1"):
                self.rule1(entries, x)
            elif self.state[0] == "stale" and self.state[1] in entries and w.on("rule2"):
                self.stats["rechecks"] += 1
                if not w.due_at(self.state[1], x):
                    self.probe()
                    return

    def rule1(self, entries: dict, x: int) -> None:
        w = self.w
        for k in entries:
            self.stats["entries"] += 1
            if k in w.prog.points:
                if w.due_at(k, x):
                    self.stale(k)
                    return
            elif w.cond.takes(k):
                if not w.due_at(k, x):
                    w.violation = f"rule 1 takes {k} at c{x} as due without a lookup, and it is not"
                self.stale(k)
                return
        self.state = ("fresh",)

    def first(self) -> None:
        if self.w.on("catch_up_first"):
            self.catch_up(self.w.up.head)

    def on_commit(self, lost: bool) -> None:
        if not lost:
            self.catch_up(self.w.up.head)

    def on_complete(self) -> None:
        self.first()
        if self.w.on("rule8"):
            self.probe()
        self.save()

    def on_conditions(self) -> None:
        self.first()
        if self.w.on("rule4"):
            self.probe()
        self.save()

    def on_reset(self) -> None:
        self.first()
        if self.w.on("held_probe"):
            self.probe()
        self.save()

    def before(self, b: Batch):
        self.first()

    def after(self, b: Batch, snap) -> None:
        w = self.w
        if self.state[0] == "held":
            pass
        elif b.reset and w.on("reset_probe"):
            self.probe()
        elif self.state[0] == "stale":
            self.stats["rechecks"] += 1
            if w.on("rule3_recheck") and not w.due(self.state[1]):
                self.probe()
        elif w.on("rule3_range"):
            self.probe(keys=b.taken) if b.kind == "keys" else self.probe(b.lo, b.hi)

    def remap(self) -> None:
        pass

    def on_retry(self, k: str, timer: bool) -> None:
        if not timer:
            self.first()
        if self.w.on("rule5") and self.state[0] == "fresh" and self.w.due(k):
            self.stale(k)
        if not timer:
            self.save()

    def on_restart(self) -> None:
        self.state, self.X = self.saved
        if self.w.on("rearm"):
            for k, f in self.w.outcomes.items():
                if f.retry:
                    self.on_retry(k, timer=True)

    def answer(self) -> tuple:
        kept = self.state, self.X, Counter(self.stats)
        if self.w.on("rule7"):
            self.catch_up(self.w.up.head)
        answer = self.state
        self.state, self.X, self.stats = kept
        return answer if answer[0] != "stale" else ("stale", answer[1])

    def complete(self) -> bool:
        """D177 by witness: does any nothing-processed range hold an upstream key?"""
        w = self.w
        return not any(
            k not in w.prog.points and w.cond.takes(k)
            for p in w.prog.pieces
            if p[2] == EMPTY
            for k in w.up.scan(w.up.head, p[0], p[1])
        )

    def check(self) -> str | None:
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
            events.append((kind, changes, rnd.random() < 0.2))
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
            events.append((kind, rnd.choice(KEYS), rnd.random() < 0.5))
        else:
            events.append((kind,))
    return events


def run(events: list[tuple], form: str, off: frozenset[str] = frozenset()) -> tuple[int, str, World] | None:
    """Apply a history, checking after every step: (step, violation, world), or None."""
    w = World(form, off)
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
