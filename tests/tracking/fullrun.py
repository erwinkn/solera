"""A full run of an ordinary asset (§10), in two forms:

- "clear", the design's: the first batch's commit is a clear and resets the
  asset's progress to empty; readers wait while their upstream is incomplete
  (P7, P12);
- "floor", the amendment's (amendment-full-run.md in the Captain drive): the
  first batch resets progress and sets the rebuild floor V to its version;
  each batch rewrites its keys in place; ctx.load() hides rows below V; the
  commit that completes the run removes every row below V and clears the
  floor; only readers of an accumulating output, and whole readers, wait.

The cast is the design's. items and feed are upstream logs the history drives.
copy is an ordinary keyed copy of items, with feed as a second keyed input,
whose definitions keep every key, drop the even ones, or rename every key.
tally is an ordinary asset that keeps a count per group (odd and even keys)
by building on ctx.load(). archive reads copy, report reads tally, summary
reads copy whole. checks is a per-key asset over items under the amended P8:
nothing resets, and a full run asked for by hand raises a rebuild counter that
is part of its conditions. copy and tally live in one kind of store per
history: "file" (an attempt loads the version it planned) or "table" (it loads
the row as it is now).
"""

from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass

from .core import ABSENT, EMPTY, NEG, POS, Cond, Log, Progress, within

KEYS = ("k1", "k2", "k3", "k4", "k5")
EVEN = {"k2", "k4"}
FORMS = ("clear", "floor")
RULES = {
    "clear": (
        "reset",
        "clear",
        "two_scans",
        "gate",
        "gate_accumulating",
        "gate_whole",
        "hold_back",
        "counter",
    ),
    "floor": (
        "reset",
        "floor_set",
        "load_floor",
        "leftovers",
        "complete_alone",
        "floor_moves_up",
        "gate_accumulating",
        "gate_whole",
        "hold_back",
        "counter",
    ),
}
ADDED = {"clear": ("hold_back",), "floor": ("hold_back",)}


def fmap(d: int, k: str) -> str | None:
    """copy's definition d: the output key an input key gives, if any."""
    return k if d % 3 == 0 else (None if k in EVEN else k) if d % 3 == 1 else k.upper()


def group(k: str) -> str:
    return "even" if k in EVEN else "odd"


@dataclass
class Row:
    version: int
    value: tuple
    rebuild: int | None  # the rebuild that wrote it, if one was in progress


class Output:
    """An output partition: the engine's log of it and the store's rows."""

    def __init__(self, name: str, kind: str) -> None:
        self.name, self.kind = name, kind
        self.log = Log()
        self.rows: dict[str, Row] = {}
        self.objects: dict[tuple[str, int], Row] = {}  # every version written, as files keep them
        self.removals: dict[
            str, list[tuple[int, int | None]]
        ] = {}  # key -> (commit, rebuild) of each removal

    def write(self, version: int, writes: dict, clear: bool, rebuild: int | None) -> None:
        c = self.log.head + 1
        gone = set(self.rows) if clear else set()
        gone |= {k for k, v in writes.items() if v is ABSENT and k in self.rows}
        gone -= {k for k, v in writes.items() if v is not ABSENT}
        for k in gone:
            self.removals.setdefault(k, []).append((c, rebuild))
        if clear:
            self.rows = {}
        for k, v in writes.items():
            if v is ABSENT:
                self.rows.pop(k, None)
            else:
                self.rows[k] = self.objects[(k, version)] = Row(version, v, rebuild)
        self.log.commit({k: ABSENT if v is ABSENT else version for k, v in writes.items()}, clear)

    def removed_by(self, k: str, H: int) -> int | None:
        """The rebuild whose commit last removed k, as the reader saw it."""
        seen = [r for c, r in self.removals.get(k, []) if self.kind == "table" or c <= H]
        return seen[-1] if seen else None

    def load(self, k: str, H: int) -> Row | None:
        if self.kind == "table":
            return self.rows.get(k)
        v = self.log.get(H, k)
        return None if v is ABSENT else self.objects[(k, v)]


@dataclass
class Spec:
    kind: str  # "range" or "keys"
    heads: list[int]
    lo: str
    hi: str
    cond: Cond
    taken: list[str]
    version: int
    reset: bool = False


def obs(log: Log, k: str, c: int, cond: Cond) -> object:
    v = log.get(c, k)
    return ABSENT if v is ABSENT else (v, cond.seen())


class Ordinary:
    """An ordinary asset with keyed inputs and one keyed output (copy, tally)."""

    def __init__(self, world: World, name: str, inputs: list[Log], output: Output, accumulates: bool) -> None:
        self.w, self.name, self.inputs, self.output = world, name, inputs, output
        self.accumulates = accumulates
        self.progress = [Progress(log) for log in inputs]
        self.held: list[dict] = [{} for _ in inputs]  # brute force: what it processed, per input
        self.covered: list[tuple[str, str]] = []  # brute force: ranges processed since it started over
        self.listed: set[str] = set()
        self.d = 0
        self.full_due = False
        self.floor: int | None = None
        self.rebuild: int | None = (
            None  # the rebuild in progress: "clear" from its clear, "floor" while V is set
        )
        self.cursor: str | None = None
        self.pending: Spec | None = None

    @property
    def cond(self) -> Cond:
        return Cond(defn=(self.d,))

    def complete(self) -> bool:
        """D177, the engine's way: no empty piece of an input holds an upstream key."""
        return not any(
            k not in prog.points
            for prog, log in zip(self.progress, self.inputs, strict=True)
            for p in prog.pieces
            if p[2] == EMPTY
            for k in log.scan(log.head, p[0], p[1])
        )

    def brute_complete(self) -> bool:
        return all(
            k in self.listed or any(within(k, lo, hi) for lo, hi in self.covered)
            for log in self.inputs
            for k in log.states[-1]
        )

    def owed(self) -> bool:
        return self.full_due or any(
            prog.candidates(log.head, NEG, self.cond)
            for prog, log in zip(self.progress, self.inputs, strict=True)
        )

    def next_plan(self, n: int, keys: tuple = ()) -> Spec:
        """What the next batch must take: a full run's first batch plans on reset
        progress (every key added), a key list takes its keys, a default batch
        the first n owed keys after the run's progress key, on every input."""
        reset = self.full_due
        progs = [p.copy() for p in self.progress]
        for p in progs if reset else ():
            p.reset()
        heads = [log.head for log in self.inputs]
        lo = NEG if reset or keys else (self.cursor or NEG)
        if keys:
            return Spec("keys", heads, lo, POS, self.cond, sorted(keys), 0, reset)
        owed = sorted({k for p, H in zip(progs, heads, strict=True) for k in p.candidates(H, lo, self.cond)})
        return Spec("range", heads, lo, owed[n - 1] if len(owed) > n else POS, self.cond, owed[:n], 0, reset)

    def plan(self, n: int, keys: tuple = ()) -> None:
        w = self.w
        if self.pending is not None:
            return
        s = self.next_plan(n, keys)
        if not keys:
            held = [{} if s.reset else h for h in self.held]
            truth = sorted(
                {
                    k
                    for h, log, H in zip(held, self.inputs, s.heads, strict=True)
                    for k in KEYS
                    if k > s.lo and h.get(k) != obs(log, k, H, self.cond)
                }
            )[:n]
            if s.taken != truth:
                w.violation = f"{self.name} planned {s.taken}, brute force {truth}"
        w.version += 1
        s.version = w.version
        self.pending = s
        w.trace.append(
            f"{self.name} plans{' its full run' if s.reset else ''} at {self._heads(s.heads)}, version {w.version}: "
            + (f"keys={s.taken}" if keys else f"{s.taken}, range ({_k(s.lo)}, {_k(s.hi)}]")
        )

    def commit(self) -> None:
        w, s = self.w, self.pending
        if s is None:
            return
        self.pending = None
        if s.cond != self.cond:
            w.trace.append(f"{self.name}'s commit is refused: its definition changed")
            return
        before = [{k: (ABSENT if s.reset else prog.decode(k)) for k in s.taken} for prog in self.progress]
        if s.reset and w.on("reset"):
            for prog in self.progress:
                prog.reset()
            self.held = [{} for _ in self.inputs]
            self.covered, self.listed = [], set()
            self.full_due = False
            w.rebuilds += 1
            w.stats["rebuilds begun"] += 1
            w.stats["rebuilds superseded"] += self.rebuild is not None
            w.stats["rebuilds begun by keys="] += s.kind == "keys"
            self.rebuild = w.rebuilds
            since = [log.diff(s.heads[i], log.head) for i, log in enumerate(self.inputs)]
            w.removed_during[self.rebuild] = {k for k, (_, now) in since[0].items() if now is ABSENT}
            w.changed_during[self.rebuild] = {k for d in since for k in d}
            w.rebuild_d[self.rebuild] = self.d
            if w.form == "floor" and w.on("floor_set") and (self.floor is None or w.on("floor_moves_up")):
                self.floor = s.version
        elif s.reset:
            self.full_due = False
        for i, (prog, log) in enumerate(zip(self.progress, self.inputs, strict=True)):
            results = {k: obs(log, k, s.heads[i], s.cond) for k in s.taken}
            for k, v in results.items():
                self.held[i][k] = v
            self.held[i] = {k: v for k, v in self.held[i].items() if v is not ABSENT}
            if s.kind == "keys":
                prog.commit_points(results)
            else:
                prog.commit_batch(s.heads[i], s.lo, s.hi, s.cond, results, ())
        if s.kind == "keys":
            self.listed |= set(s.taken)
        else:
            self.covered.append((s.lo, s.hi))
            self.cursor = None if s.hi == POS else s.hi
        for prog in self.progress:
            prog.merge(self.cursor)
        writes = self.produce(s, before)
        w.trace.append(
            f"{self.name}#{self.output.log.head + 1} (version {s.version}){' clear;' if s.reset and w.form == 'clear' and w.on('clear') else ''} writes {_writes(writes)}"
        )
        self.finish(s.version, writes, clear=s.reset and w.form == "clear" and w.on("clear"))

    def produce(self, s: Spec, before: list[dict]) -> dict:
        """The asset's code: what it writes for the keys and classes it was given."""
        items_log = self.inputs[0]
        if not self.accumulates:
            out: dict[str, object] = {}
            for k in s.taken:
                had, has = before[0][k], obs(items_log, k, s.heads[0], s.cond)
                q = fmap(self.d, k)
                if q is None:
                    continue
                if has is not ABSENT:
                    feed = self.inputs[1].get(s.heads[1], k)
                    out[q] = (has[0], feed, self.d)
                elif had is not ABSENT:
                    out[q] = ABSENT
            return out
        delta: Counter = Counter()
        for k in s.taken:
            had, has = before[0][k], obs(items_log, k, s.heads[0], s.cond)
            delta[group(k)] += (has is not ABSENT) - (had is not ABSENT)
        out = {}
        for g in sorted(delta):
            count = (0 if s.reset else self.load(g)) + delta[g]
            out[g] = (count, self.d) if count else ABSENT
        return out

    def load(self, g: str) -> int:
        """ctx.load() of the asset's own output, as the store has it now."""
        row = self.output.rows.get(g)
        if row is None:
            return 0
        if self.floor is not None and row.version < self.floor:
            if self.w.on("load_floor"):
                return 0
            self.w.violation = (
                f"{self.name}'s ctx.load() returned {g}@{row.version}, below the floor {self.floor}"
            )
        return row.value[0]

    def finish(self, version: int, writes: dict, clear: bool) -> None:
        """Commit the output, removing the leftovers if this completes a rebuild."""
        w = self.w
        done = self.rebuild is not None and self.complete()
        if done and self.floor is not None and w.on("leftovers"):
            after = {k: r.version for k, r in self.output.rows.items()}
            after.update({k: version for k, v in writes.items() if v is not ABSENT})
            left = sorted(k for k, v in after.items() if v < self.floor and writes.get(k, 0) is not ABSENT)
            writes = {**writes, **dict.fromkeys(left, ABSENT)}
            if left:
                w.stats["leftovers removed"] += len(left)
                w.trace.append(f"  and removes the leftovers below {self.floor}: {left}")
        if w.form == "floor" and self.rebuild is not None and not self.accumulates:
            for k, v in writes.items():
                last = self.output.removals.get(k, [(0, None)])[-1][1]
                if v is ABSENT or k in self.output.rows or last != self.rebuild:
                    continue
                if k.lower() not in w.removed_during[self.rebuild]:
                    w.violation = (
                        f"{self.name}'s log removes {k} and adds it back within rebuild {self.rebuild}"
                    )
        if writes or clear:
            self.output.write(version, writes, clear, self.rebuild)
        if done:
            w.stats["rebuilds completed"] += 1
            floor, self.floor, self.rebuild = self.floor, None, None
            w.trace.append(
                f"  {self.name} is complete" + (f"; its floor {floor} is cleared" if floor else "")
            )
            if floor is not None and any(r.version < floor for r in self.output.rows.values()):
                w.violation = f"{self.name} completed its rebuild from {floor} with rows below it"

    def complete_alone(self) -> None:
        """Completion without a batch. Under a clear it is only a status; under a
        floor the engine commits the removal of the leftovers by itself, unless a
        batch in flight will commit it."""
        w = self.w
        if self.rebuild is None or not self.complete():
            return
        if w.form == "clear":
            self.finish(0, {}, clear=False)
        elif self.pending is None and w.on("complete_alone"):
            w.version += 1
            w.stats["completed with no batch"] += 1
            w.trace.append(
                f"{self.name} becomes complete with no batch: the engine commits version {w.version}"
            )
            self.finish(w.version, {}, clear=False)

    @staticmethod
    def _heads(heads: list[int]) -> str:
        return "c" + "/f".join(str(h) for h in heads)


class Mirror:
    """A keyed consumer that copies its input (archive, report) or computes a
    value per key (checks, per-key); one keyed input."""

    def __init__(
        self, world: World, name: str, src: Output | Log, producer: Ordinary | None, gate: str | None
    ):
        self.w, self.name, self.src, self.producer, self.gate = world, name, src, producer, gate
        self.log = src.log if isinstance(src, Output) else src
        self.progress = Progress(self.log)
        self.holds: dict[str, object] = {}  # brute force
        self.store: dict[str, tuple] = {}
        self.last: dict[
            str, tuple[str, int | None]
        ] = {}  # the last class delivered per key, with its rebuild
        self.d = 0
        self.counter = 0
        self.cursor: str | None = None
        self.pending: Spec | None = None

    @property
    def cond(self) -> Cond:
        return Cond(defn=(self.d, self.counter) if self.w.on("counter") else (self.d,))

    def held(self) -> bool:
        """Is this reader held by the gate right now?"""
        p, w = self.producer, self.w
        if p is None or self.gate is None:
            return False
        if w.form == "clear":
            return w.on("gate_accumulating" if self.gate == "accumulating" else "gate") and not p.complete()
        if self.gate == "accumulating":
            return w.on("gate_accumulating") and (not p.complete() or p.floor is not None)
        return False

    def owed(self) -> bool:
        return bool(self.progress.candidates(self.log.head, NEG, self.cond))

    def next_plan(self, n: int) -> Spec:
        H, lo = self.log.head, self.cursor or NEG
        taken, hi, _ = self.progress.plan(H, lo, n, self.cond)
        return Spec("range", [H], lo, hi, self.cond, taken, 0)

    def plan(self, n: int) -> None:
        w = self.w
        if self.pending is not None:
            return
        if self.held():
            w.held[self.name] += 1
            return
        w.stats[f"{self.name} runs during a rebuild"] += bool(
            self.producer and self.producer.rebuild is not None
        )
        s = self.next_plan(n)
        truth = sorted(
            k for k in self._universe() if k > s.lo and self.holds.get(k) != self._obs(k, s.heads[0])
        )[:n]
        if s.taken != truth:
            w.violation = f"{self.name} planned {s.taken}, brute force {truth}"
        w.version += 1
        s.version = w.version
        self.pending = s
        w.trace.append(
            f"{self.name} plans at {self.log_name()}#{s.heads[0]}: {s.taken}, range ({_k(s.lo)}, {_k(s.hi)}]"
        )

    def commit(self) -> None:
        w, s = self.w, self.pending
        if s is None:
            return
        self.pending = None
        if s.cond.defn[0] != self.d:
            w.trace.append(f"{self.name}'s commit is refused: its definition changed")
            return
        results, kept, delivered = {}, set(), []
        for k in s.taken:
            planned = self._obs(k, s.heads[0], s.cond)
            row = self.src.load(k, s.heads[0]) if isinstance(self.src, Output) else None
            loaded = (
                planned
                if row is None and not isinstance(self.src, Output)
                else (ABSENT if row is None else (row.version, s.cond.seen()))
            )
            w.stats["loads at another version"] += loaded != planned
            if loaded != planned and self.held() and w.on("hold_back"):
                w.stats["held back"] += 1
                kept.add(k)
                continue
            p = self.producer
            if row is not None and p is not None and row.rebuild is not None and row.rebuild == p.rebuild:
                if p.accumulates:
                    w.violation = f"{self.name} loaded {k}@{row.version}, a row the unfinished rebuild {row.rebuild} built"
                elif k.lower() not in w.changed_during[row.rebuild]:
                    d, items = w.rebuild_d[row.rebuild], w.items.states[-1]
                    src = k.lower()
                    final = (items[src], w.feed.states[-1].get(src), d) if src in items else None
                    if row.value != final:
                        w.violation = (
                            f"{self.name} loaded {k}@{row.version} as {row.value} mid-rebuild {row.rebuild}, "
                            f"which will hold {final}: a half-built row"
                        )
            self._deliver(k, loaded, row, s.heads[0])
            delivered.append(f"{k}{'' if loaded is ABSENT else '@' + str(loaded[0])}")
            results[k] = loaded
        self.progress.commit_batch(s.heads[0], s.lo, s.hi, s.cond, results, kept)
        self.cursor = None if s.hi == POS else s.hi
        self.progress.merge(self.cursor)
        w.trace.append(
            f"{self.name} commits: {', '.join(delivered) or 'nothing'}"
            + (f"; holds back {sorted(kept)}" if kept else "")
        )

    def _deliver(self, k: str, loaded: object, row: Row | None, H: int) -> None:
        w = self.w
        had = self.holds.get(k)
        if loaded is ABSENT:
            self.holds.pop(k, None)
            self.store.pop(k, None)
        else:
            self.holds[k] = loaded
            self.store[k] = row.value if row is not None else (loaded[0], self.d, self.counter)
        if had == loaded:
            return
        cls = "added" if had is ABSENT else "removed" if loaded is ABSENT else "updated"
        tag = None
        if isinstance(self.src, Output):
            tag = self.src.removed_by(k, H) if cls == "removed" else row.rebuild if row else None
        if cls == "added" and tag is not None and self.last.get(k) == ("removed", tag):
            if k.lower() not in w.removed_during[tag]:
                w.violation = f"{self.name} was given {k} removed, then added back, within rebuild {tag}"
        if cls == "removed" and self.producer is None and self.log.states[H].get(k) is not ABSENT:
            w.violation = f"{self.name} removed {k}, which items has"
        self.last[k] = (cls, tag)

    def _obs(self, k: str, c: int, cond: Cond | None = None) -> object:
        return obs(self.log, k, c, cond or self.cond)

    def _universe(self) -> set[str]:
        return set(KEYS) | {k.upper() for k in KEYS} | {"odd", "even"}

    def log_name(self) -> str:
        return self.producer.name if self.producer else "items c"


class World:
    def __init__(self, form: str, kind: str, off: frozenset[str] = frozenset()) -> None:
        self.form, self.kind, self.off = form, kind, off
        self.version = 0
        self.rebuilds = 0
        self.removed_during: dict[int, set[str]] = {}  # per rebuild: the items keys removed since it planned
        self.changed_during: dict[int, set[str]] = {}  # per rebuild: the input keys changed since it planned
        self.rebuild_d: dict[int, int] = {}  # per rebuild: the definition it builds
        self.violation: str | None = None
        self.trace: list[str] = []
        self.held: Counter = Counter()
        self.stats: Counter = Counter()
        self.items, self.feed = Log(), Log()
        self.copy = Ordinary(self, "copy", [self.items, self.feed], Output("copy", kind), accumulates=False)
        self.tally = Ordinary(self, "tally", [self.items], Output("tally", kind), accumulates=True)
        self.archive = Mirror(self, "archive", self.copy.output, self.copy, gate="keyed")
        self.report = Mirror(self, "report", self.tally.output, self.tally, gate="accumulating")
        self.checks = Mirror(self, "checks", self.items, None, gate=None)
        self.assets = {a.name: a for a in (self.copy, self.tally, self.archive, self.report, self.checks)}
        self.floors = {"copy": 0, "tally": 0}
        self.copy.output.log.two_scans = self.on("two_scans")

    def on(self, rule: str) -> bool:
        return rule not in self.off

    def apply(self, ev: tuple) -> None:
        kind = ev[0]
        if kind in ("items", "feed"):
            log = self.items if kind == "items" else self.feed
            changes = {
                k: (log.head + 1 + (100 if kind == "feed" else 0)) if keep else ABSENT for k, keep in ev[1]
            }
            if not any(log.states[-1].get(k) != v for k, v in changes.items()):
                return
            log.commit(changes)
            for a in (self.copy, self.tally):
                if a.rebuild is not None:
                    self.changed_during[a.rebuild] |= set(log.entries[-1])
                    if kind == "items":
                        self.removed_during[a.rebuild] |= {
                            k for k, (n, _) in log.entries[-1].items() if n is ABSENT
                        }
            self.trace.append(
                f"{kind} {'c' if kind == 'items' else 'f'}{log.head}: "
                + " · ".join(
                    f"{k}: {'−' if r is None else r} → {'−' if n is None else n}"
                    for k, (n, r) in log.entries[-1].items()
                )
            )
        elif kind in ("plan", "step"):
            self.assets[ev[1]].plan(ev[2])
            if kind == "step":
                self.assets[ev[1]].commit()
        elif kind == "keys":
            asset = self.assets[ev[1]]
            if isinstance(asset, Ordinary):
                asset.plan(0, ev[2])
        elif kind == "commit":
            self.assets[ev[1]].commit()
        elif kind == "cancel":
            a = self.assets[ev[1]]
            if a.pending or a.cursor:
                self.stats["cancels mid-rebuild"] += getattr(a, "rebuild", None) is not None
                a.pending, a.cursor = None, None
                self.trace.append(f"{a.name}'s run is canceled")
        elif kind == "define":
            a = self.assets[ev[1]]
            a.d += 1
            if isinstance(a, Ordinary):
                a.full_due = True
            self.trace.append(f"{a.name}'s definition changes to d{a.d}")
        elif kind == "full":
            a = self.assets[ev[1]]
            if isinstance(a, Ordinary):
                a.full_due = True
            else:
                a.counter += 1
            self.trace.append(f"a full run of {a.name} is asked for by hand")
        elif kind == "restart":
            self.stats["restarts mid-rebuild"] += any(a.rebuild is not None for a in (self.copy, self.tally))
            for a in self.assets.values():
                a.pending = None
            self.trace.append("the engine restarts: every attempt in flight dies")
        elif kind == "summary":
            self.summary()
        for a in (self.copy, self.tally):
            a.complete_alone()

    def summary(self) -> None:
        """A whole reader of copy: runs only when the gate lets it."""
        c = self.copy
        held = not c.complete() or (self.form == "floor" and c.floor is not None)
        if held and self.on("gate_whole"):
            self.held["summary"] += 1
            return
        defs = {r.value[2] for r in c.output.rows.values()}
        self.trace.append(f"summary reads copy whole: {sorted(c.output.rows)}")
        if not c.brute_complete() or len(defs) > 1:
            self.violation = f"summary read a copy that is incomplete or mixes definitions {sorted(defs)}"

    # -- invariants -------------------------------------------------------------
    def check(self) -> str | None:
        if self.violation:
            return self.violation
        for a in (self.copy, self.tally):
            out = a.output
            if out.log.scan(out.log.head) != {k: r.version for k, r in out.rows.items()}:
                return f"{a.name}'s log says {out.log.scan(out.log.head)}, its store {_rows(out.rows)}"
            for c in range(out.log.head + 1):
                if out.log.scan(c) != out.log.states[c]:
                    return f"{a.name}'s scan at #{c} differs from its state"
            for prog, held in zip(a.progress, a.held, strict=True):
                for k in KEYS:
                    if prog.decode(k) != held.get(k):
                        return (
                            f"{a.name}'s progress decodes {k} as {prog.decode(k)}, it processed {held.get(k)}"
                        )
            floor = a.floor or 0
            if floor < self.floors[a.name] and a.floor is not None:
                return f"{a.name}'s floor went down to {floor}"
            self.floors[a.name] = max(self.floors[a.name], floor)
        for m in (self.archive, self.report, self.checks):
            for k in m._universe():
                if m.progress.decode(k) != m.holds.get(k):
                    return f"{m.name}'s progress decodes {k} as {m.progress.decode(k)}, it holds {m.holds.get(k)}"
        return None

    def settle(self) -> str | None:
        """Run everything until nothing is owed, then check the outputs at rest."""
        for _ in range(60):
            moved = False
            for a in (self.copy, self.tally, self.checks, self.archive, self.report):
                if a.owed():
                    before = len(self.trace)
                    a.plan(2)
                    a.commit()
                    for o in (self.copy, self.tally):
                        o.complete_alone()
                    moved |= len(self.trace) > before
                    bad = self.check()
                    if bad:
                        return bad
            if not moved:
                break
        self.summary()
        if self.violation:
            return self.violation
        items, feed = self.items.states[-1], self.feed.states[-1]
        copy = {
            fmap(self.copy.d, k): (v, feed.get(k), self.copy.d)
            for k, v in items.items()
            if fmap(self.copy.d, k)
        }
        counts = Counter(group(k) for k in items)
        tally = {g: (n, self.tally.d) for g, n in counts.items()}
        checks = {k: (v, self.checks.d, self.checks.counter) for k, v in items.items()}
        for name, have, want in (
            ("copy", {k: r.value for k, r in self.copy.output.rows.items()}, copy),
            ("tally", {k: r.value for k, r in self.tally.output.rows.items()}, tally),
            ("archive", self.archive.store, copy),
            ("report", self.report.store, tally),
            ("checks", self.checks.store, checks),
        ):
            if have != want:
                return f"at rest {name} holds {have}, it should hold {want}"
        if self.copy.floor is not None or self.tally.floor is not None:
            return "at rest a floor is still set"
        return None


def history(rnd: random.Random, steps: int) -> list[tuple]:
    kinds = (
        ("items",) * 6
        + ("feed",) * 2
        + ("step",) * 10
        + ("plan",) * 4
        + ("commit",) * 4
        + ("keys", "cancel", "define", "define", "full", "restart", "summary")
    )
    names = ("copy", "copy", "tally", "archive", "report", "checks")
    events: list[tuple] = []
    for _ in range(steps):
        kind = rnd.choice(kinds)
        if kind in ("items", "feed"):
            events.append(
                (kind, tuple((k, rnd.random() < 0.8) for k in sorted(rnd.sample(KEYS, rnd.randint(1, 3)))))
            )
        elif kind in ("plan", "step"):
            events.append((kind, rnd.choice(names), rnd.randint(1, 2)))
        elif kind in ("commit", "cancel"):
            events.append((kind, rnd.choice(names)))
        elif kind == "keys":
            events.append(
                (kind, rnd.choice(("copy", "tally")), tuple(sorted(rnd.sample(KEYS, rnd.randint(1, 2)))))
            )
        elif kind in ("define", "full"):
            events.append((kind, rnd.choice(("copy", "copy", "tally", "checks"))))
        else:
            events.append((kind,))
    return events


def run(
    events: list[tuple], form: str, kind: str, off: frozenset[str] = frozenset()
) -> tuple[int, str, World] | None:
    w = World(form, kind, off)
    for i, ev in enumerate(events):
        w.apply(ev)
        bad = w.check()
        if bad:
            return i, bad, w
    bad = w.settle()
    return (len(events), bad, w) if bad else None


def _k(k: str) -> str:
    return "-inf" if k == NEG else "+inf" if k == POS else k


def _writes(writes: dict) -> str:
    return (
        ", ".join(f"{k} removed" if v is ABSENT else f"{k}={v[0]}" for k, v in sorted(writes.items()))
        or "nothing"
    )


def _rows(rows: dict) -> str:
    return "{" + ", ".join(f"{k}@{r.version}" for k, r in sorted(rows.items())) + "}"
