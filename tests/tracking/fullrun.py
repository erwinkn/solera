"""A full run of an ordinary asset (§10), in the decided form ("clear", P7, P12):
the first batch's commit is a clear and resets the asset's progress to empty,
and readers wait while their upstream is incomplete. The amendment's other
form (rebuild in place behind a floor) was checked against it and rejected;
it is in the history of solera#15 and in handoffs/SOLE-7/verdict.md.

The cast is the design's. items and feed are upstream logs the history drives.
copy is an ordinary keyed copy of items, with feed as a second keyed input,
whose definitions keep every key, drop the even ones, or rename every key.
tally is an ordinary asset that keeps a count per group (odd and even keys)
by building on ctx.load(). archive reads copy, report reads tally, summary
reads copy whole, through the store and by reference (the user's table
itself, D211). checks is a per-key asset over items under the amended P8:
nothing resets, and a full run asked for by hand raises a rebuild counter that
is part of its conditions. copy and tally live in one kind of store per
history: "file" (an attempt loads the version it planned) or "table" (it loads
the row as it is now). An attempt writes before it commits: on a table late and
dead writers are possible, and the exact-table contract (review F1, recs 1 to
5) keeps the table equal to its log. Each committed batch leaves a record that
lineage recomputes; merges fold logs at the single cut, and on file stores
cleanup deletes replaced objects exactly (§12, §13).
"""

from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, field

from .core import ABSENT, EMPTY, NEG, POS, Cond, Folded, Log, Progress, within

KEYS = ("k1", "k2", "k3", "k4", "k5")
EVEN = {"k2", "k4"}
ALL = frozenset(KEYS) | {k.upper() for k in KEYS} | {"odd", "even"}  # every key an output can hold
FORMS = ("clear",)
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
        "fence",
        "open_txn",
        "repair",
        "repair_removals",
        "repair_at_once",
        "repair_handover",
        "load_check",
        "cut_records",
        "cut_pieces",
        "cut_cursor",
        "cut_attempts",
        "cleanup_held",
        "swap",
        "solera_build",
        "first_full",
        "switch_commit",
        "switch_alone",
        "switch_release",
        "switch_owed",
        "switch_cleanup",
        "build_keep",
        "reset_replay",
        "notice_match",
        "switch_quiet",
        "switch_overtaken",
        "byref_wait",
        "switch_result",
        "switch_newest",
        "fill_empty",
    ),
}
ADDED = {
    "clear": (
        "hold_back",
        "repair_removals",
        "repair_handover",
        "cut_attempts",
        "swap",
        "solera_build",
        "first_full",
        "switch_commit",
        "switch_alone",
        "switch_release",
        "switch_owed",
        "switch_cleanup",
        "build_keep",
        "reset_replay",
        "notice_match",
        "switch_quiet",
        "switch_overtaken",
        "byref_wait",
        "switch_result",
        "switch_newest",
        "fill_empty",
    )
}


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
    """An output partition: the engine's log of it and the store's rows.

    A table's full run builds beside the published rows (D211): its first
    write opens a build (PostgresStore: a shadow table named by the write's
    version). A write lands where its attempt's context points: the build of
    the full run the log has in progress, else that of the run whose switch
    the engine still owes, else the published rows (the user's table). Solera
    reads the same way, from the log; programs outside Solera read the
    published rows, which change only at the store's switch or by a write
    outside a full run. Off (rule swap), the run writes in place. The one
    exception (`_decide`, rule fill_empty): a full run into a partition
    holding no rows writes in place, and outsiders may see it fill. A run's
    write the store makes nothing for (`made`: no table, and no rows to make
    one from) decides nothing: the run is pending until one does. The cast's
    tables always exist; the store's random history drives that."""

    def __init__(self, name: str, kind: str, on=lambda rule: True) -> None:
        self.name, self.kind, self.on = name, kind, on
        self.log = Log()
        self.swap = kind == "table" and on("swap")
        self.ctx = lambda: (None, None)  # the log's (run in progress, run whose switch is owed)
        self.published: dict[str, Row] = {}  # the user's table, as outsiders read it
        self.builds: dict[
            int, dict[str, Row]
        ] = {}  # open builds (shadow tables), by the version that opened each
        self.published_run = 0  # the build last published
        self.loose: dict[str, int] = {}  # published keys an uncommitted write in place touched (`filling`)
        self.ended: set[int] = set()  # builds whose full run ended in the log
        self.switched: Counter = Counter()  # build -> times the store published it
        self.superseded: set[int] = set()  # ended builds dropped unpublished: a newer run's end overtook them
        self.settled = [0]  # commits an outsider may see: not mid full run, nor in an unfinished first run
        self.ran = False  # a full run (the first run, at least) has ended in the log
        self.shown = 0  # the settled commit the outside view last matched
        self.pending: set[int] = set()  # full runs no write has decided yet: the store made nothing
        self.in_place: set[int] = set()  # full runs that wrote in place, published as they began
        self.filling: int | None = None  # the newest of them, until a switch: its commits are settled
        self.objects: dict[tuple[str, int], Row] = {}  # every version written, as files keep them
        self.removals: dict[
            str, list[tuple[int, int | None]]
        ] = {}  # key -> (commit, rebuild) of each removal
        self.fence = 0  # a table's version row: the newest attempt that has written (F1, rec 1)
        self.cursor = 0  # file stores: cleanup has deleted what every commit up to here replaced (§13)
        self.deleted: set[tuple[str, int]] = set()

    def write(
        self,
        version: int,
        writes: dict,
        clear: bool,
        rebuild: int | None,
        ctx: tuple[int | None, int | None] = (None, None),
        made: bool = True,
    ) -> None:
        """The store's write: a file store names objects by version; a table writes
        its rows in place, so they change before the engine commits."""
        for k, v in writes.items():
            if v is not ABSENT:
                self.objects[(k, version)] = Row(version, v, rebuild)
        if self.kind == "table":
            self._apply(version, writes, clear, ctx, made)

    def commit(self, version: int, writes: dict, clear: bool, rebuild: int | None) -> None:
        """The engine's commit: the log, and on a file store what is current."""
        before, c = self.log.states[-1], self.log.head + 1
        gone = set(before) if clear else set()
        gone |= {k for k, v in writes.items() if v is ABSENT and k in before}
        gone -= {k for k, v in writes.items() if v is not ABSENT}
        for k in gone:
            self.removals.setdefault(k, []).append((c, rebuild))
        if self.kind == "file":
            self._apply(version, writes, clear)
        self.loose = {k: v for k, v in self.loose.items() if v != version}
        self.log.commit({k: ABSENT if v is ABSENT else version for k, v in writes.items()}, clear)

    def _named(self) -> dict[str, Row] | None:
        """The build the log names: its run in progress, else the one owed a switch."""
        for g in self.ctx():
            if g is not None and g in self.builds:
                return self.builds[g]
        return None

    @property
    def rows(self) -> dict[str, Row]:
        """What Solera reads: the build the log names, if any."""
        if self.on("solera_build") and (b := self._named()) is not None:
            return b
        return self.published

    def outside(self) -> dict[str, Row]:
        """What a program reading the table outside Solera sees."""
        return self.published

    def _apply(
        self, version: int, writes: dict, clear: bool, ctx: tuple = (None, None), made: bool = True
    ) -> None:
        run, owed = ctx
        if not self.swap:
            if clear:
                self.published = {}
            rows = self.published
        else:
            keep = {g for g in (run, owed) if g is not None and self.on("build_keep")}
            for g in [g for g in self.builds if g not in keep and g != version]:
                del self.builds[g]  # a build no run in the log names: a dead attempt's, or overtaken
                if g in self.ended and not self.switched[g]:
                    self.superseded.add(g)
            self.pending &= keep | {version}
            if clear and version <= self.published_run and self.on("reset_replay"):
                rows = self.published  # the same attempt again, its run published: same rows, in place
                rows.clear()
            else:
                target = (
                    version
                    if clear
                    else next((g for g in (run, owed) if g in (*self.builds, *self.pending)), None)
                )
                if target is not None and target not in self.builds:  # no write of the run has decided yet
                    if not made:
                        self.pending.add(target)
                        return
                    self.pending.discard(target)
                    self._decide(target)
                rows = self.builds.get(target, self.published)
                if clear:
                    rows.clear()
        fills = self.filling is not None and (version == self.filling or (not clear and run == self.filling))
        if rows is self.published and ((not clear and run is None) or fills):
            self.loose.update(dict.fromkeys(writes, version))  # in place, uncommitted
        for k, v in writes.items():
            if v is ABSENT:
                rows.pop(k, None)
            else:
                rows[k] = self.objects[(k, version)]

    def _decide(self, run: int) -> None:
        """Where full run `run` writes, at its first write into the store
        (PostgresStore._decide): beside, in a build, unless the partition holds
        no rows and no other run is registered (rule fill_empty: only
        then; Erwin, 2026-11-11). Then it writes in place and is published as
        it begins: outsiders may see the partition fill, and a dead attempt's
        rows stay until a later run's switch. Off, it writes in place
        whatever the partition holds."""
        empty = not (self.published or self.builds or self.pending)
        if empty or not self.on("fill_empty"):
            self.published_run = max(self.published_run, run)
            self.in_place.add(run)
            self.filling = run if empty else None  # the excuse is the exception's, never the rule's
        else:
            self.builds[run] = {}

    def switch(self, gen: int, newest: int) -> bool:
        """The store's hook at the end of a full run (D211), one transaction:
        the partition's rows are replaced by the build's, and the build goes.
        Returns whether the run is published. A call delivered after an attempt
        newer than `newest` wrote does nothing and says not published; a
        repeat after the switch does nothing and says published."""
        if self.on("switch_newest") and self.fence > newest:
            return False
        if gen not in self.builds and gen not in self.pending:
            if gen == self.filling:
                self.filling = None  # called once no dead write of it is left: its rows are its commits
            return gen <= self.published_run
        build = self.builds.pop(gen, {})  # a pending run made nothing: it publishes an empty partition
        self.pending = {g for g in self.pending if g > gen}
        for g in [g for g in self.builds if g < gen]:  # older runs' builds: overtaken, never to be published
            del self.builds[g]
            if g in self.ended and not self.switched[g]:
                self.superseded.add(g)
        # switch_cleanup: delete the partition's rows before inserting the build's
        self.published = dict(build) if self.on("switch_cleanup") else {**self.published, **build}
        if self.on("switch_cleanup"):
            self.loose = {}
        self.switched[gen] += 1
        self.published_run = max(self.published_run, gen)
        self.filling = None
        return True

    def removed_by(self, k: str, H: int, planned: bool) -> int | None:
        """The rebuild whose commit last removed k, as the reader saw it: at its
        head if the removal was planned, else (a table) as the row is now."""
        seen = [r for c, r in self.removals.get(k, []) if c <= H or not planned]
        return seen[-1] if seen else None

    def load(self, k: str, H: int) -> Row | None:
        if self.kind == "table":
            return self.rows.get(k)
        v = self.log.states[H].get(k)  # the version the spec planned
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
    repaired: dict = field(default_factory=dict)  # the dead attempts its repair took over: version -> names
    loads: dict | None = None  # a reader's load, once it ran: key -> Row or None
    loading_rebuild: int | None = None  # the producer's rebuild in progress when it loaded
    removals: dict = field(default_factory=dict)  # a loaded absence: the rebuild whose commit removed it then
    clear: bool = False
    rebuild: int | None = None
    ctx: tuple = (None, None)  # the write context: the log's run in progress and the run owed a switch
    before: list[dict] = field(default_factory=list)  # what the asset held of each taken key
    loaded: dict = field(default_factory=dict)  # ctx.load() at the start: group -> Row or None
    written: dict | None = None  # the attempt's write, once its transaction ran
    redo: dict = field(default_factory=dict)  # input keys the repair redid -> what was processed


@dataclass
class Record:
    """A committed batch on one edge (§7): what lineage recomputes its input from."""

    edge: str
    log: Log
    kind: str
    lo: str
    hi: str
    H: int
    cond: Cond
    pieces: list  # the replaced slice: the pieces over (lo, hi] before the batch
    points: dict  # and its points
    listed: dict  # keys processed by name (a key list, a repair): what was held before
    deviations: dict  # keys loaded at another version than planned: what was loaded
    held_back: frozenset
    digest: list  # what the batch processed: (key, version, class)


def replaced(prog: Progress, lo: str, hi: str) -> tuple[list, dict]:
    pieces = [[max(p[0], lo), min(p[1], hi), p[2]] for p in prog.pieces if p[1] > lo and p[0] < hi]
    return pieces, {k: v for k, v in prog.points.items() if within(k, lo, hi)}


def processed(prior: dict, now: dict, listed: dict) -> list[tuple]:
    """What a batch processed on one edge: every key that changed, and every key
    it was given by name (a key list, a repair), changed or not."""
    return [
        (
            k,
            None if now[k] is ABSENT else now[k][0],
            "unchanged"
            if now[k] == p
            else "added"
            if p is ABSENT
            else "removed"
            if now[k] is ABSENT
            else "updated",
        )
        for k, p in sorted(prior.items())
        if now[k] != p or k in listed
    ]


def lineage(rec: Record) -> list[tuple]:
    """Batch lineage (§12): the input recomputed from the record alone, by the
    index, from the replaced slice to the record's head."""
    p = Progress(rec.log)
    p.pieces = [[NEG, rec.lo, EMPTY], *[list(x) for x in rec.pieces], [rec.hi, POS, EMPTY]]
    p.points = dict(rec.points)
    keys = (
        [k for k in p.candidates(rec.H, rec.lo, rec.cond) if within(k, rec.lo, rec.hi)]
        if rec.kind == "range"
        else []
    )
    prior = {k: p.decode(k) for k in keys} | rec.listed
    prior = {k: v for k, v in prior.items() if k not in rec.held_back}
    now = {k: rec.deviations[k] if k in rec.deviations else p.wanted(k, rec.H, rec.cond) for k in prior}
    return processed(prior, now, rec.listed)


def obs(log: Log, k: str, c: int, cond: Cond) -> object:
    """What processing k at c gives: the version comes with the spec, so this is
    not a query of the index."""
    v = log.states[c].get(k)
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
        self.full_due = world.on("first_full")  # an output's first run is a full run of an empty output
        self.rebuild: int | None = None  # the rebuild in progress, from its clear until the asset is complete
        self.full_gen: int | None = None  # the version of the committed clear that began it
        self.released = False  # a person let downstream proceed over the rebuild's partial output
        self.hand = False  # the full run due was asked for by hand, and nothing else makes it due
        self.last: Spec | None = None  # the last attempt that wrote, which a store call may replay
        self.newest = 0  # the newest attempt version the engine has planned for the asset
        output.ctx = self.runs
        self.cursor: str | None = None
        self.pending: Spec | None = None
        self.dead: dict[int, frozenset[str]] = {}  # attempts given up on, unrepaired: the keys each may write
        self.zombies: list[Spec] = []  # given up on before their write: it may still land
        self.excused: set[str] = set()  # brute force: keys a dead attempt wrote and no repair has fixed
        self.abandoned: set[int] = set()  # versions of attempts that ended without a commit

    @property
    def cond(self) -> Cond:
        return Cond(defn=(self.d,))

    def runs(self) -> tuple[int | None, int | None]:
        """What the core says of the output's full runs: the one in progress,
        and the one whose end the engine has not yet told the store (D211)."""
        return (self.full_gen if self.rebuild is not None else None), self.w.ended.get(self.name)

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
        if self.dead and self.w.on("repair_at_once"):
            return True
        return self.full_due or any(
            prog.candidates(log.head, NEG, self.cond)
            for prog, log in zip(self.progress, self.inputs, strict=True)
        )

    def next_plan(self, n: int, keys: tuple | None = None) -> Spec:
        """What the next batch must take: a full run's first batch plans on reset
        progress (every key added), a key list takes its keys, a default batch
        the first n owed keys after the run's progress key, on every input."""
        reset = self.full_due
        progs = [p.copy() for p in self.progress]
        for p in progs if reset else ():
            p.reset()
        heads = [log.head for log in self.inputs]
        lo = NEG if reset or keys is not None else (self.cursor or NEG)
        if keys is not None:
            return Spec("keys", heads, lo, POS, self.cond, sorted(keys), 0, reset)
        owed = sorted({k for p, H in zip(progs, heads, strict=True) for k in p.candidates(H, lo, self.cond)})
        return Spec("range", heads, lo, owed[n - 1] if len(owed) > n else POS, self.cond, owed[:n], 0, reset)

    def plan(self, n: int, keys: tuple | None = None) -> None:
        w = self.w
        if self.pending is not None:
            return
        s = self.next_plan(n, keys)
        if keys is None:
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
        s.version = self.newest = w.version
        s.before = [{k: (ABSENT if s.reset else prog.decode(k)) for k in s.taken} for prog in self.progress]
        s.clear = s.reset and w.on("clear")
        s.ctx = self.runs()
        if s.reset:
            w.rebuilds += 1
            s.rebuild = w.rebuilds
        else:
            s.rebuild = self.rebuild
        w.trace.append(
            f"{self.name} plans{' its full run' if s.reset else ''} at {self._heads(s.heads)}, version {w.version}: "
            + (f"keys={s.taken}" if keys is not None else f"{s.taken}, range ({_k(s.lo)}, {_k(s.hi)}]")
        )
        if self.accumulates and not s.reset:
            log = self.output.log
            s.loaded = {g: self.output.rows.get(g) for g in sorted({group(k) for k in s.taken})}
            for g, row in s.loaded.items():
                if (ABSENT if row is None else row.version) != log.states[-1].get(g):
                    w.stats["ctx.load() found a row no commit of its own wrote"] += 1
                    if w.on("load_check"):
                        self.full_due, self.hand = True, False
                        w.trace.append(
                            f"  its ctx.load() finds {g}@{row and row.version}, its last commit wrote "
                            f"{log.states[-1].get(g)}: it cannot build on it and owes a full run"
                        )
                        return
        self.pending = s

    def write(self) -> bool:
        """The attempt's write transaction, once. False if the attempt ends without
        a write it can commit."""
        s = self.pending
        if s is None:
            return False
        if s.written is None and not self.transact(s, live=True):
            self.pending = None
            return False
        return True

    def transact(self, s: Spec, live: bool, again: bool = False) -> bool:
        """One write transaction. On a table it first raises the partition's version
        row, or is refused if a newer attempt has written (F1, rec 1); the first
        live attempt after one given up on always opens it (rec 3) and repairs what
        the dead attempts wrote (rec 2)."""
        w, out = self.w, self.output
        writes = dict(s.written) if again else self.produce(s, s.before)
        if out.kind == "table":
            if w.on("fence") and out.fence > s.version:
                w.stats["writes refused"] += 1
                w.trace.append(f"  {self.name} v{s.version}'s write is refused: v{out.fence} has written")
                return False
            repair = live and bool(self.dead)
            if not (writes or s.clear or (repair and w.on("open_txn"))):
                s.written = {}
                return True
            out.fence = max(out.fence, s.version)
            if repair and w.on("repair") and not self.repair(s, writes):
                return False
        out.write(s.version, writes, s.clear, s.rebuild, s.ctx)
        s.written, self.last = writes, s
        w.trace.append(
            f"  {self.name} v{s.version}{'' if live else ' again,' if again else ', given up on,'} writes"
            f"{' clear;' if s.clear else ''} {_writes(writes)}"
        )
        return True

    def repair(self, s: Spec, writes: dict) -> bool:
        """Rec 2: list the rows the dead attempts wrote (by version), and with
        repair_removals the keys their specs name; delete what the log does not
        have, redo what it has. An output built on its own rows cannot be redone
        key by key: it owes a full run (rec 5). False if the attempt ends here."""
        w, out = self.w, self.output
        state = out.log.states[-1]
        keys = {k for k, r in out.rows.items() if r.version in self.dead}
        if w.on("repair_removals"):
            for named in self.dead.values():
                keys |= set(state) | set(out.rows) if named is ALL else named
        wrong = sorted(
            k for k in keys - set(writes) if (r.version if (r := out.rows.get(k)) else ABSENT) != state.get(k)
        )
        w.trace.append(f"  {self.name} v{s.version} repairs v{sorted(self.dead)}: {wrong}")
        s.repaired, self.dead = self.dead, {}
        if s.clear:
            return True
        if wrong and self.accumulates:
            self.dead = s.repaired  # unrepaired until the full run's clear: no switch may publish its rows
            self.full_due, self.hand = True, False
            w.trace.append(f"  {self.name} owes a full run: a dead attempt wrote its own rows")
            return False
        for q in wrong:
            items, feed = self.inputs
            k = q.lower()
            v = items.get(s.heads[0], k)
            has = ABSENT if v is ABSENT else (v, s.cond.seen())
            d = s.cond.defn[0]
            ok = has is not ABSENT and fmap(d, k) == q
            writes[q] = (has[0], feed.get(s.heads[1], k), d) if ok else ABSENT
            s.redo[k] = has
        self.excused = set()
        return True

    def cleanup(self) -> None:
        """Exact cleanup (§13, D183), on a file store: a version replaced at commit r
        goes once r is at or below the oldest head a live attempt holds on the
        output; an abandoned attempt's objects go by version (CLN-5)."""
        w, out = self.w, self.output
        if out.kind != "file":
            return
        held = [out.log.head]
        if w.on("cleanup_held"):
            held += [m.pending.heads[0] for m in (w.archive, w.report) if m.src is out and m.pending]
        while out.cursor < min(held):
            r = out.cursor + 1
            gone = (
                out.log.scan(r - 1)
                if r in out.log.clears
                else {k: rep for k, (_, rep) in out.log.entry(r).items() if rep is not ABSENT}
            )
            out.deleted |= set(gone.items())
            out.cursor = r
        out.deleted |= {key for key in out.objects if key[1] in self.abandoned}
        w.trace.append(f"cleanup of {self.name} reaches #{out.cursor}")

    def giveup(self) -> None:
        """The engine ends the attempt without a commit; its write may land later."""
        s, w = self.pending, self.w
        if s is None:
            return
        self.pending = None
        w.trace.append(f"the engine gives up on {self.name} v{s.version}")
        self.abandoned.add(s.version)
        self._dead(s)
        if s.written is None:
            self.zombies.append(s)

    def replay(self) -> None:
        """A store call delivered twice: the last write transaction runs again,
        at its own version (STO-6), before or after its commit and switch."""
        if self.last is not None:
            self.transact(self.last, live=False, again=True)

    def late(self, newest: bool = False) -> None:
        if self.zombies:
            s = self.zombies.pop(-1 if newest else 0)
            if self.transact(s, live=False) and s.version in self.dead:
                self.excused |= self._keys(s.written or {}, s.clear)

    def _dead(self, s: Spec) -> None:
        if self.output.kind == "table":
            d = s.cond.defn[0]
            names = {group(k) if self.accumulates else fmap(d, k) for k in s.taken} - {None}
            self.dead[s.version] = ALL if s.reset else frozenset(names)
            if self.w.on("repair_handover"):
                self.dead |= s.repaired
            if s.written is not None:
                self.excused |= self._keys(s.written, s.clear)

    @staticmethod
    def _keys(written: dict, clear: bool) -> set[str]:
        return set(ALL) if clear else set(written)

    def commit(self) -> None:
        w = self.w
        if not self.write():
            return
        s = self.pending
        self.pending = None
        if s.cond != self.cond:
            w.trace.append(f"{self.name}'s commit is refused: its definition changed")
            self.abandoned.add(s.version)
            self._dead(s)
            return
        if s.reset and w.on("reset"):
            for prog in self.progress:
                prog.reset()
            self.held = [{} for _ in self.inputs]
            self.covered, self.listed = [], set()
            self.full_due = False
            self.cursor = None
            w.stats["rebuilds begun"] += 1
            w.stats["rebuilds superseded"] += self.rebuild is not None
            w.stats["rebuilds begun by keys="] += s.kind == "keys"
            self.rebuild = s.rebuild
            then, now = self.inputs[0].states[s.heads[0]], self.inputs[0].states[-1]
            w.removed_during[self.rebuild] = {k for k in then if k not in now}
        elif s.reset:
            self.full_due = False
        if s.clear:
            self.full_gen, self.released = s.version, False
            if w.on("switch_overtaken"):
                w.ended.pop(
                    self.name, None
                )  # a newer run's clear is journaled: the older one is never published
        for i, (prog, log) in enumerate(zip(self.progress, self.inputs, strict=True)):
            pieces, points = ([[s.lo, s.hi, EMPTY]], {}) if s.reset else replaced(prog, s.lo, s.hi)
            prior = dict(s.before[i])
            outside = {k for k in s.redo if k not in prior}
            prior |= {k: prog.decode(k) for k in outside}
            listed = {k: v for k, v in prior.items() if s.kind == "keys" or k in s.redo}
            now = {k: obs(log, k, s.heads[i], s.cond) for k in prior}
            w.record(
                Record(
                    f"{self.name}<{i}",
                    log,
                    s.kind,
                    s.lo,
                    s.hi,
                    s.heads[i],
                    s.cond,
                    pieces,
                    points,
                    listed,
                    {},
                    frozenset(),
                    processed(prior, now, listed),
                )
            )
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
        if s.redo:
            self.listed |= set(s.redo)
            for i, (prog, log) in enumerate(zip(self.progress, self.inputs, strict=True)):
                redone = {k: obs(log, k, s.heads[i], s.cond) for k in s.redo}
                prog.commit_points(redone)
                self.held[i] = {k: v for k, v in {**self.held[i], **redone}.items() if v is not ABSENT}
        for prog in self.progress:
            prog.merge(self.cursor)
        w.trace.append(f"{self.name}#{self.output.log.head + 1} commits v{s.version}")
        if s.clear:
            self.excused = set()
        self.finish(s.version, s.written, s.clear, s.rebuild)

    def produce(self, s: Spec, before: list[dict]) -> dict:
        """The asset's code: what it writes for the keys and classes it was given."""
        items_log, d = self.inputs[0], s.cond.defn[0]
        if not self.accumulates:
            out: dict[str, object] = {}
            for k in s.taken:
                had, has = before[0][k], obs(items_log, k, s.heads[0], s.cond)
                q = fmap(d, k)
                if q is None:
                    continue
                if has is not ABSENT:
                    feed = self.inputs[1].states[s.heads[1]].get(k)
                    out[q] = (has[0], feed, d)
                elif had is not ABSENT:
                    out[q] = ABSENT
            return out
        delta: Counter = Counter()
        for k in s.taken:
            had, has = before[0][k], obs(items_log, k, s.heads[0], s.cond)
            delta[group(k)] += (has is not ABSENT) - (had is not ABSENT)
        out = {}
        for g in sorted(delta):
            row = s.loaded.get(g)
            count = (0 if s.reset or row is None else row.value[0]) + delta[g]
            out[g] = (count, d) if count else ABSENT
        return out

    def finish(
        self, version: int, writes: dict, clear: bool, rebuild: int | None = None, via: str = "commit"
    ) -> None:
        """Commit the output; the asset is complete once no input is owed a key
        it has not processed since the clear (D177). The commit that ends a full
        run carries ends_full_run; an end with no commit journals FullRunDone."""
        if writes or clear:
            self.output.commit(version, writes, clear, rebuild)
        if self.rebuild is not None and self.complete():
            self.w.stats["rebuilds completed"] += 1
            self.rebuild = None
            self.w.trace.append(f"  {self.name} is complete")
            self.w.owe(self, via)

    def complete_alone(self) -> None:
        """Completion without a batch is only a status (FullRunDone)."""
        if self.rebuild is not None and self.complete():
            self.finish(0, {}, clear=False, via="alone")

    def release(self) -> None:
        """D205: after a cancel or a failure, a person lets downstream proceed over
        the partial output. The full run ends in the log (FullRunDone); the keys
        it did not reach stay owed to the producer."""
        if self.rebuild is None or self.pending is not None:
            return
        self.w.stats["releases"] += 1
        self.w.released.add(self.rebuild)
        self.rebuild, self.released = None, True
        self.w.trace.append(f"a person releases {self.name}'s canceled full run: downstream proceeds")
        self.w.owe(self, "release")

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
        self.delivered: list[
            tuple
        ] = []  # brute force: (key, version or ABSENT, planned head, head) since the last check
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
        return (
            w.on("gate_accumulating" if self.gate == "accumulating" else "gate")
            and not p.complete()
            and not p.released
        )

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

    def load(self) -> None:
        """The attempt's load, before its commit: a table gives its rows as they
        are now, a file store the versions planned."""
        s = self.pending
        if s is None or s.loads is not None:
            return
        s.loads = {k: self.src.load(k, s.heads[0]) if isinstance(self.src, Output) else None for k in s.taken}
        for k, row in s.loads.items():
            if row is not None and (k, row.version) in self.src.deleted:
                self.w.violation = f"{self.name} loads {k}@{row.version}, which cleanup deleted"
        s.loading_rebuild = self.producer.rebuild if self.producer else None
        if isinstance(self.src, Output):
            s.removals = {
                k: self.src.removed_by(k, s.heads[0], self._obs(k, s.heads[0], s.cond) is ABSENT)
                for k, row in s.loads.items()
                if row is None
            }
        self.w.trace.append(
            f"{self.name} loads "
            + (", ".join(f"{k}@{r.version}" if r else f"{k} absent" for k, r in s.loads.items()) or "nothing")
        )

    def commit(self) -> None:
        w = self.w
        self.load()
        s = self.pending
        if s is None:
            return
        self.pending = None
        if s.cond.defn[0] != self.d:
            w.trace.append(f"{self.name}'s commit is refused: its definition changed")
            return
        cleared = any(s.heads[0] < x <= self.log.head for x in self.log.clears)
        results, kept, delivered = {}, set(), []
        for k in s.taken:
            planned = self._obs(k, s.heads[0], s.cond)
            row = s.loads[k]
            loaded = (
                planned
                if row is None and not isinstance(self.src, Output)
                else (ABSENT if row is None else (row.version, s.cond.seen()))
            )
            w.stats["loads at another version"] += loaded != planned
            recorded = loaded == planned or self._recorded(k, loaded, s.heads[0])
            w.stats["loads of a version no commit recorded"] += not recorded
            if loaded != planned and (self.held() or cleared or not recorded) and w.on("hold_back"):
                w.stats["held back"] += 1
                kept.add(k)
                continue
            p = self.producer
            if (
                row is not None
                and p is not None
                and row.rebuild is not None
                and row.rebuild in (p.rebuild, s.loading_rebuild)
            ):
                w.violation = (
                    f"{self.name} loaded {k}@{row.version}, a row the unfinished rebuild {row.rebuild} built"
                )
            self._deliver(k, loaded, row, s.heads[0], s.removals.get(k))
            self.delivered.append((k, ABSENT if loaded is ABSENT else loaded[0], s.heads[0], self.log.head))
            delivered.append(f"{k}{'' if loaded is ABSENT else '@' + str(loaded[0])}")
            results[k] = loaded
        pieces, points = replaced(self.progress, s.lo, s.hi)
        prior = {k: self.progress.decode(k) for k in results}
        planned = {k: self._obs(k, s.heads[0], s.cond) for k in results}
        w.record(
            Record(
                self.name,
                self.log,
                "range",
                s.lo,
                s.hi,
                s.heads[0],
                s.cond,
                pieces,
                points,
                {},
                {k: v for k, v in results.items() if v != planned[k]},
                frozenset(kept),
                processed(prior, results, {}),
            )
        )
        self.progress.commit_batch(s.heads[0], s.lo, s.hi, s.cond, results, kept)
        self.cursor = None if s.hi == POS else s.hi
        self.progress.merge(self.cursor)
        w.trace.append(
            f"{self.name} commits: {', '.join(delivered) or 'nothing'}"
            + (f"; holds back {sorted(kept)}" if kept else "")
        )

    def _deliver(
        self, k: str, loaded: object, row: Row | None, H: int, removed_by: int | None = None
    ) -> None:
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
            tag = removed_by if cls == "removed" else row.rebuild if row else None
        if cls == "added" and tag is not None and self.last.get(k) == ("removed", tag):
            if k.lower() not in w.removed_during[tag] and tag not in w.released:
                w.violation = f"{self.name} was given {k} removed, then added back, within rebuild {tag}"
        if cls == "removed" and self.producer is None and self.log.states[H].get(k) is not ABSENT:
            w.violation = f"{self.name} removed {k}, which items has"
        self.last[k] = (cls, tag)

    def _obs(self, k: str, c: int, cond: Cond | None = None) -> object:
        return obs(self.log, k, c, cond or self.cond)

    def _recorded(self, k: str, loaded: object, H: int) -> bool:
        """Did a commit at or after the planned head record what was loaded?"""
        v = ABSENT if loaded is ABSENT else loaded[0]
        return any(self.log.get(c, k) == v for c in range(H, self.log.head + 1))

    def _universe(self) -> set[str]:
        return set(KEYS) | {k.upper() for k in KEYS} | {"odd", "even"}

    def log_name(self) -> str:
        return self.producer.name if self.producer else "items c"


class World:
    def __init__(self, form: str, kind: str, off: frozenset[str] = frozenset()) -> None:
        if form not in FORMS:
            raise ValueError(f"unknown form {form!r}: the model keeps {list(FORMS)}")
        self.form, self.kind, self.off = form, kind, off
        self.version = 0
        self.rebuilds = 0
        self.removed_during: dict[int, set[str]] = {}  # per rebuild: the items keys removed since it planned
        self.violation: str | None = None
        self.trace: list[str] = []
        self.held: Counter = Counter()
        self.stats: Counter = Counter()
        self.items, self.feed = Log(), Log()
        self.ended: dict[str, int] = {}  # core: per output, the newest run whose end the store was not told
        self.acks: list[
            tuple[str, int]
        ] = []  # hook calls that returned, their FullRunNoticed not journaled yet
        self.calls: list[
            tuple[str, int, int, bool]
        ] = []  # hook calls issued, not yet run by the store: (asset, run, newest, still awaited)
        self.released: set[int] = set()  # rebuilds a person released
        self.copy = Ordinary(
            self, "copy", [self.items, self.feed], Output("copy", kind, self.on), accumulates=False
        )
        self.tally = Ordinary(self, "tally", [self.items], Output("tally", kind, self.on), accumulates=True)
        self.archive = Mirror(self, "archive", self.copy.output, self.copy, gate="keyed")
        self.report = Mirror(self, "report", self.tally.output, self.tally, gate="accumulating")
        self.checks = Mirror(self, "checks", self.items, None, gate=None)
        self.assets = {a.name: a for a in (self.copy, self.tally, self.archive, self.report, self.checks)}
        self.copy.output.log.two_scans = self.on("two_scans")
        self.logs = {
            "items": self.items,
            "feed": self.feed,
            "copy": self.copy.output.log,
            "tally": self.tally.output.log,
        }
        self.records: list[Record] = []  # the batch records still in history, oldest first

    def on(self, rule: str) -> bool:
        return rule not in self.off

    def owe(self, a: Ordinary, via: str) -> None:
        """A full run ended in the log: the same journal event records that the
        engine owes the store its switch (D211). Only tables have the hook."""
        if a.output.kind != "table" or a.full_gen is None:
            return
        a.output.ended.add(a.full_gen)
        if self.on(f"switch_{via}"):
            self.ended[a.name] = a.full_gen  # the core keeps the newest; an older run's switch was overtaken

    def switch(self, name: str, ack: bool | str) -> None:
        """The engine calls the hook for the run it owes the asset's store. With
        the ack it then journals FullRunNoticed; "late", the notice is journaled
        by a later step; without the ack (a crash after the call) it stays owed."""
        if (run := self.ended.get(name)) is None:
            return
        a = self.assets[name]
        if self.on("switch_quiet") and (a.pending is not None or a.dead):
            self.trace.append(f"the engine waits to switch {name}: an attempt is in flight or unrepaired")
            return
        if ack == "delayed":
            self.calls.append((name, run, a.newest, True))
            self.trace.append(
                f"the engine calls {name}'s store to switch to build v{run}; the call arrives later"
            )
            return
        self.deliver(name, run, a.newest, ack)

    def deliver(self, name: str, run: int, newest: int, ack: bool | str) -> None:
        """The store runs the hook; the engine journals FullRunNoticed only if it
        reports the run published (switch_result), and never for a call it gave
        up on at a restart."""
        published = self.assets[name].output.switch(run, newest)
        self.stats["switches called"] += 1
        self.trace.append(
            f"the engine's call to switch {name}'s store to build v{run} (newest v{newest}) runs: "
            + ("published" if published else "not published")
            + ("" if ack else "; the engine does not record it")
        )
        if not published and self.on("switch_result"):
            return
        if ack == "late":
            self.acks.append((name, run))
        elif ack:
            self.notice(name, run)

    def notice(self, name: str, run: int) -> None:
        """FullRunNoticed: the store was told run ended. It clears the debt only
        if no newer run's end replaced it meanwhile (notice_match)."""
        self.trace.append(f"the engine journals that {name}'s store was told v{run} ended")
        if self.ended.get(name) == run or (name in self.ended and not self.on("notice_match")):
            del self.ended[name]

    def observe(self) -> None:
        """Note each commit an outsider may see: one at which the output is
        neither mid full run nor in a first run that has never been complete,
        and each commit of a run filling a partition in place."""
        for a in (self.copy, self.tally):
            out = a.output
            filling = out.filling is not None and out.filling == a.full_gen  # its commits land in place
            if a.rebuild is not None and not filling:
                continue
            out.ran |= out.log.head > 0 and (a.complete() or a.released)  # its first run has completed
            if (out.ran or filling) and out.settled[-1] != out.log.head:
                out.settled.append(out.log.head)

    def record(self, rec: Record) -> None:
        if (got := lineage(rec)) != rec.digest:
            self.violation = f"lineage of {rec.edge}'s batch recomputes {got}, it processed {rec.digest}"
        self.records.append(rec)

    def readers(self, log: Log) -> list[Progress]:
        return {
            id(self.items): [self.copy.progress[0], self.tally.progress[0], self.checks.progress],
            id(self.feed): [self.copy.progress[1]],
            id(self.copy.output.log): [self.archive.progress],
            id(self.tally.output.log): [self.report.progress],
        }[id(log)]

    def cut(self, log: Log) -> int:
        """The single cut (§13, P11): the oldest commit a retained batch record names
        (its head and replaced slice), a live piece, the cleanup cursor, or (added)
        the head a live attempt planned at."""
        marks = [log.head]
        if self.on("cut_records"):
            for rec in self.records:
                if rec.log is log:
                    marks += [rec.H] + [p[2][1] for p in rec.pieces if p[2][0] == "at"]
        if self.on("cut_pieces"):
            marks += [p[2][1] for prog in self.readers(log) for p in prog.pieces if p[2][0] == "at"]
        if self.on("cut_attempts"):
            for a in (self.copy, self.tally):
                if a.pending:
                    marks += [H for H, inp in zip(a.pending.heads, a.inputs, strict=True) if inp is log]
            for m in (self.archive, self.report, self.checks):
                if m.pending and m.log is log:
                    marks.append(m.pending.heads[0])
        if self.on("cut_cursor"):
            marks += [
                a.output.cursor
                for a in (self.copy, self.tally)
                if a.output.log is log and a.output.kind == "file"
            ]
        return min(marks)

    def merge(self, name: str) -> None:
        log = self.logs[name]
        cut = self.cut(log)
        if cut > log.base:
            log.fold(cut)
            self.trace.append(f"{name}'s index merges: the base moves to #{cut}")
            for rec in self.records:
                if (got := lineage(rec)) != rec.digest:
                    self.violation = (
                        f"lineage of {rec.edge}'s batch recomputes {got}, it processed {rec.digest}"
                    )

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
        elif kind == "load":
            a = self.assets[ev[1]]
            if isinstance(a, Mirror):
                a.load()
        elif kind in ("write", "giveup", "late"):
            a = self.assets[ev[1]]
            if isinstance(a, Ordinary):
                getattr(a, kind)(*ev[2:])
            elif kind == "giveup" and a.pending:
                a.pending = None
                self.trace.append(f"the engine gives up on {a.name}'s attempt")
        elif kind == "cancel":
            a = self.assets[ev[1]]
            if a.pending or a.cursor:
                self.stats["cancels mid-rebuild"] += getattr(a, "rebuild", None) is not None
                if isinstance(a, Ordinary):
                    a.giveup()
                    if a.hand and a.full_due:
                        a.full_due = a.hand = False  # the run asked for by hand goes with it
                a.pending, a.cursor = None, None
                self.trace.append(f"{a.name}'s run is canceled")
        elif kind == "define":
            a = self.assets[ev[1]]
            a.d += 1
            if isinstance(a, Ordinary):
                a.full_due, a.hand = True, False
            self.trace.append(f"{a.name}'s definition changes to d{a.d}")
        elif kind == "full":
            a = self.assets[ev[1]]
            if isinstance(a, Ordinary):
                a.hand = a.hand or not a.full_due
                a.full_due = True
            else:
                a.counter += 1
            self.trace.append(f"a full run of {a.name} is asked for by hand")
        elif kind == "switch":
            self.switch(ev[1], ev[2])
        elif kind == "notice":
            if self.calls:
                self.deliver(*self.calls.pop(0))
            elif self.acks:
                self.notice(*self.acks.pop(0))
        elif kind == "replay":
            self.assets[ev[1]].replay()
        elif kind == "release":
            a = self.assets[ev[1]]
            if isinstance(a, Ordinary):
                a.release()
        elif kind == "restart":
            self.acks = []  # notices not journaled are lost: the hook runs again
            self.calls = [
                (n, r, v, False) for n, r, v, _ in self.calls
            ]  # still run by the store, never noticed
            if not self.on("switch_owed"):
                self.ended = {}  # switches held in memory are lost
            self.stats["restarts mid-rebuild"] += any(a.rebuild is not None for a in (self.copy, self.tally))
            for a in self.assets.values():
                if isinstance(a, Ordinary):
                    a.giveup()
                a.pending = None
            self.trace.append("the engine restarts: it gives up on every attempt in flight")
        elif kind == "summary":
            self.summary()
        elif kind == "merge":
            self.merge(ev[1])
        elif kind == "retire" and self.records:
            rec = self.records.pop(0)
            self.trace.append(f"{rec.edge}'s oldest batch record leaves history")
        elif kind == "cleanup":
            self.assets[ev[1]].cleanup()
        for a in (self.copy, self.tally):
            a.complete_alone()
        self.observe()

    def summary(self) -> None:
        """A whole reader of copy: runs only when the gate lets it."""
        c = self.copy
        if not c.complete() and not c.released and self.on("gate_whole"):
            self.held["summary"] += 1
            return
        self.by_reference()
        read = {k: r.version for k, r in c.output.rows.items()}
        if read != c.output.log.scan(c.output.log.head):
            self.stats["summary reads a version no commit recorded"] += 1
            if self.on("hold_back"):
                self.held["summary"] += 1
                self.trace.append(
                    f"summary reads copy whole, finds {_rows(c.output.rows)}, no commit's: it holds back"
                )
                return
        if read != c.output.log.states[-1]:
            self.violation = f"summary read {_rows(c.output.rows)}, rows no commit of copy recorded"
        defs = {r.value[2] for r in c.output.rows.values()}
        self.trace.append(f"summary reads copy whole: {sorted(c.output.rows)}")
        if (not c.brute_complete() and not c.released) or len(defs) > 1:
            self.violation = f"summary read a copy that is incomplete or mixes definitions {sorted(defs)}"

    def by_reference(self) -> None:
        """A whole reader of copy that takes it by reference queries the user's
        table itself: no store load serves it, so its gate also waits while the
        store's switch is owed (D211), and it records copy's head as read."""
        c = self.copy
        out = c.output
        if out.kind != "table":
            return
        if "copy" in self.ended and self.on("byref_wait"):
            self.held["by reference"] += 1
            return
        view, state = out.outside(), out.log.states[-1]
        if any(
            (r.version if (r := view.get(k)) else ABSENT) != state.get(k)
            for k in (set(view) | set(state)) - set(out.loose)
        ):
            self.violation = (
                f"a reader of copy by reference reads {_rows(view)}, copy#{out.log.head} records {state}"
            )

    # -- invariants -------------------------------------------------------------
    def check(self) -> str | None:
        if self.violation:
            return self.violation
        for a in (self.copy, self.tally):
            if a.complete() != a.brute_complete():
                return f"{a.name} reads {'' if a.complete() else 'in'}complete, brute force says otherwise"
            out = a.output
            s = a.pending
            state = out.log.scan(out.log.head)
            excused = set(a.excused)
            if s is not None and s.written is not None:
                excused |= a._keys(s.written, s.clear)
            if bad := self.outsiders(a):
                return bad
            for k in sorted(set(state) | set(out.rows)):
                if (r.version if (r := out.rows.get(k)) else ABSENT) != state.get(k) and k not in excused:
                    return f"{a.name}'s log says {_k(k)}@{state.get(k)}, its store {_rows(out.rows)}"
            if out.kind == "file":
                needed = [out.log.head] + [
                    m.pending.heads[0] for m in (self.archive, self.report) if m.src is out and m.pending
                ]
                if gone := sorted({o for c in needed for o in out.log.states[c].items()} & out.deleted):
                    return f"cleanup of {a.name} deleted {gone}, which the log or an attempt still needs"
            for c in range(out.log.base, out.log.head + 1):
                if out.log.scan(c) != out.log.states[c]:
                    return f"{a.name}'s scan at #{c} differs from its state"
            if a.accumulates:
                counts = Counter(group(k) for k in a.held[0])
                got = {g: out.objects[(g, v)].value[0] for g, v in state.items()}
                if got != dict(counts):
                    return f"{a.name}'s log holds the counts {got}, it processed {dict(counts)}"
            for prog, held in zip(a.progress, a.held, strict=True):
                for k in KEYS:
                    if prog.decode(k) != held.get(k):
                        return (
                            f"{a.name}'s progress decodes {k} as {prog.decode(k)}, it processed {held.get(k)}"
                        )
        for m in (self.archive, self.report):
            for k, v, H, head in m.delivered:
                if all(st.get(k, ABSENT) != v for st in m.log.states[H : head + 1]):
                    return f"{m.name} processed {k} as {v}, which no commit of {m.log_name()} since #{H} recorded"
            m.delivered = []
        for m in (self.archive, self.report, self.checks):
            for k in m._universe():
                if m.progress.decode(k) != m.holds.get(k):
                    return f"{m.name}'s progress decodes {k} as {m.progress.decode(k)}, it holds {m.holds.get(k)}"
        return None

    def outsiders(self, a: Ordinary) -> str | None:
        """D211, on tables. (iii) The store switches to a build once at most, and
        only once its full run ended. (iv) After a switch, no row of an older
        build is left. (i) A program outside Solera reads the log's state at a
        commit at which the output was neither mid full run nor in an
        unfinished first run, or of a run filling a partition in place, never
        an earlier one than it read before; only keys an uncommitted write in
        place touched are excused: outside a full run, or of the filling run's
        own attempts, never another run's. (ii) is the check above:
        Solera reads the log."""
        out = a.output
        if out.kind != "table":
            return None
        view = out.outside()
        for g, n in out.switched.items():
            if n > 1:
                return f"{a.name}'s store switched to build v{g} {n} times"
            if g not in out.ended:
                return f"{a.name}'s store switched to build v{g}, whose full run had not ended"
        if out.swap and (old := {k: r for k, r in view.items() if r.version < out.published_run}):
            return f"{a.name}'s store still holds {_rows(old)} of an older build after its switch"
        dirty = set(out.loose)
        self.stats["outside reads during a build"] += bool(out.builds)
        self.stats["outside reads of a partition filling"] += out.filling is not None
        for c in out.settled:
            st = out.log.states[c]
            if c >= out.shown and all(
                (r.version if (r := view.get(k)) else ABSENT) == st.get(k)
                for k in (set(view) | set(st)) - dirty
            ):
                out.shown = c
                break
        else:
            return (
                f"outsiders read {a.name} as {_rows(view)}, which no commit since #{out.shown} outside a full "
                f"run recorded"
            )
        return None

    def settle(self) -> str | None:
        """Run everything until nothing is owed, then check the outputs at rest."""
        for _ in range(60):
            moved = False
            for a in (self.copy, self.tally, self.checks, self.archive, self.report):
                if a.owed() or a.pending:
                    before = len(self.trace)
                    a.plan(2)
                    a.commit()
                    for o in (self.copy, self.tally):
                        o.complete_alone()
                    self.observe()
                    moved |= len(self.trace) > before
                    bad = self.check()
                    if bad:
                        return bad
            if not moved:
                for a in (self.copy, self.tally):
                    while a.zombies:
                        a.late()
                        moved = True
                        if bad := self.check():
                            return bad
            if not moved:
                moved = bool(self.acks or self.calls)
                while self.calls:
                    self.deliver(*self.calls.pop(0))
                while self.acks:
                    self.notice(*self.acks.pop(0))
                for name in list(self.ended):
                    self.switch(name, True)
                    moved |= name not in self.ended
                    if bad := self.check():
                        return bad
            if not moved:
                break
        self.summary()
        for name in self.logs:
            self.merge(name)
        if self.violation:
            return self.violation
        for a in (self.copy, self.tally):
            a.cleanup()
            out = a.output
            state = out.log.states[-1]
            if out.kind == "file" and (left := set(out.objects) - out.deleted) != set(state.items()):
                return f"at rest {a.name}'s files are {sorted(left)}, its log holds {sorted(state.items())}"
            if {k: r.version for k, r in out.rows.items()} != state:
                return (
                    f"at rest {a.name}'s log says {dict(sorted(state.items()))}, its store {_rows(out.rows)}"
                )
            if lost := sorted(
                g for g in out.ended - out.in_place if not out.switched[g] and g not in out.superseded
            ):
                return f"at rest {a.name}'s full runs that opened builds {lost} ended, but no switch published them"
            if stray := sorted(g for g in out.builds if g not in a.abandoned):
                return f"at rest {a.name}'s store holds builds {stray} that no run in the log names"
            if out.kind == "table" and {k: r.version for k, r in out.outside().items()} != state:
                return (
                    f"at rest outsiders read {a.name} as {_rows(out.outside())}, "
                    f"its log says {dict(sorted(state.items()))}"
                )
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
        return None


def history(rnd: random.Random, steps: int) -> list[tuple]:
    kinds = (
        ("items",) * 6
        + ("feed",) * 2
        + ("step",) * 10
        + ("plan",) * 4
        + ("commit",) * 4
        + ("keys", "cancel", "define", "define", "full", "restart", "summary")
        + ("load",) * 3
        + ("write",) * 3
        + ("giveup",) * 2
        + ("late",) * 2
        + ("merge",) * 3
        + ("cleanup",) * 2
        + ("retire",)
        + ("switch",) * 3
        + ("release", "notice", "replay")
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
        elif kind in ("commit", "cancel", "giveup"):
            events.append((kind, rnd.choice(names)))
        elif kind == "load":
            events.append((kind, rnd.choice(("archive", "report"))))
        elif kind in ("write", "cleanup"):
            events.append((kind, rnd.choice(("copy", "tally"))))
        elif kind == "late":
            events.append((kind, rnd.choice(("copy", "tally")), rnd.random() < 0.5))
        elif kind == "switch":
            events.append(
                (kind, rnd.choice(("copy", "tally")), rnd.choice((True, True, False, "late", "delayed")))
            )
        elif kind == "replay":
            events.append((kind, rnd.choice(("copy", "tally"))))
        elif kind == "release":
            events.append((kind, rnd.choice(("copy", "tally"))))
        elif kind == "merge":
            events.append((kind, rnd.choice(("items", "feed", "copy", "tally"))))
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
    i = 0
    try:
        for i, ev in enumerate(events):
            w.apply(ev)
            bad = w.check()
            if bad:
                return i, bad, w
        bad = w.settle()
    except Folded as e:
        return i, str(e), w
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
