"""Staleness at every level (K43, the approved design; docs/positions-from-reads.md),
for the simulation and the property tests: the engine's answers, behind
placeholder names, and a reference model of what they must be.

The rules (W22 builds them after the control file; until then the tests
that use this module are strict xfails or off):

- Output unit: a key within a partition if the output is keyed, else the
  partition. Input unit, per input: one upstream key for an each=True
  input; otherwise the upstream partition(s) it reads.
- Dependency: with each=True, output key k depends on input key k (and on
  every shared whole or dep input). Otherwise every output unit of a
  partition depends on its whole input units: a keyed non-each output's
  keys go stale together.
- An output unit is stale iff an input unit it depends on changed after
  it was written (for an input with patterns, only keys they take count:
  K39), or its asset changed since. Missing keys count for each=True: a
  key the upstream has under the patterns and the output does not, or one
  the output holds and the upstream removed. Roll-ups use "any": key ->
  partition -> asset.
- Runs (K45, amending K43): `keys=` on an each=True asset makes each named
  key its patterns take match its upstream, and never touches a key it
  does not name (R2). On a plain incremental input it delivers the named
  keys' changes past the snapshot, all read as of one upstream commit; the
  partition's record becomes "snapshot N, plus (commit, attempt) per keys=
  run since", the attempt's spec listing the keys. A default run skips a
  changed key that an entry names at a commit at or after its last change,
  and collapses the record into its own snapshot: nothing is delivered
  twice. At most a configured number of entries (10,000) per partition: a
  keys= run past it is refused with "run the partition first"; one run may
  name any number of keys.
- One record for every asset, each=True included (K47): a position and
  read-ahead entries (the keys from the attempt's spec, the versions from
  the commit), no per-key payloads. An each=True asset's stale keys are
  derived from it, exact per key, and its keys= runs count toward the cap.
  A retry pass that leaves nothing uncovered collapses the record, as a
  default run does.
- A full pass (after an asset change or a reset) may take several runs:
  its first delivery, keys= or default, starts over (first batch full and
  first); later keys= runs continue it with their keys; a default run
  delivers what the pass has not, and finishes it. Once every key under
  the patterns has been delivered in the pass at its current version, the
  asset is fresh, and the record collapses to a snapshot.
- A plain incremental partition is stale while some change past its
  snapshot, under the patterns, is covered by no entry, or while a full
  pass is due and unfinished; its keys share the partition's answer.
- "Changed" is the net delta: a key added and removed again past a read,
  or updated and reverted, has not changed.
- A whole or dep input moving (`knob`) makes every key stale ("input
  changed") and a full pass due: the next default run writes every key
  the pass has not (keys= runs may have written some). With no pass due,
  a default run writes the keys whose upstream changed. The partition
  record holds one `knob` version, so every key stays stale until the
  pass completes, a key it already rewrote included, and after a reset
  too (semantic change (d), the coordinator's ruling).
- Staleness is transitive (K46): a unit is also stale when an upstream unit
  it depends on is. Each stale status says why: "input changed", "upstream
  stale", "definition changed", one or more.
- An output reset itself starts empty: a `keys=` run leaves just its keys;
  the next default run converges (R6).
- Positions are derived from what each attempt read; nothing here asserts one.

Every engine call the tests make about staleness is in this module, so
adapting to the names W22 picks is one edit here.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

LANDED = False  # every piece of K43–K46 built: True also shrinks their failures


class NotBuilt(NotImplementedError):
    """The engine has no such answer yet: what the strict xfails expect."""


# -- the engine's answers --------------------------------------------------------------


async def stale_keys(engine, asset: str, partition: str = "") -> set[str] | None:
    """The stale keys of a keyed asset's partition, every page of them
    (a non-each output's are all its keys, or none); None for an unkeyed
    output, which has no keys."""

    listing = getattr(engine, "stale_keys", None)
    if listing is None:
        raise NotBuilt("Engine.stale_keys (K37)")
    keys, after = set(), None
    while True:
        page = await listing(asset, partition, after=after)
        if not page.get("tracked", True):
            return None
        keys.update(page["keys"])
        after = page.get("next")
        if after is None:
            return keys


async def partition_stale(engine, asset: str, partition: str = "") -> bool:
    """Whether the partition statuses (API, CLI) report it `stale`."""

    rows = (await engine.partition_statuses([asset], every=False))[asset]
    return any(r["partition"] == partition and r["status"] == "stale" for r in rows)


async def stale_partitions(engine, asset: str) -> set[str]:
    rows = (await engine.partition_statuses([asset], every=False))[asset]
    return {r["partition"] for r in rows if r["status"] == "stale"}


async def stale_reasons(engine, asset: str, partition: str = "") -> set[str]:
    """Why the partition statuses report it `stale` (K46); empty if not."""

    rows = (await engine.partition_statuses([asset], every=False))[asset]
    for row in rows:
        if row["partition"] == partition:
            if row["status"] != "stale":
                return set()
            if "reasons" not in row:
                raise NotBuilt("partition_statuses rows' `reasons` (K46)")
            return set(row["reasons"])
    return set()


async def asset_stale(engine, asset: str) -> bool:
    """The asset rollup's own flag (asset_statuses, the console's graph)."""

    rollup = (await engine.asset_statuses())[asset]
    if "stale" not in rollup:
        raise NotBuilt("asset_statuses()[asset]['stale'] (K38)")
    return bool(rollup["stale"])


def engine_with_read_ahead_cap(state, project, cap: int):
    """An engine whose partitions take at most `cap` keys= runs between two
    default runs."""

    from tests.server.engines import make_engine

    return make_engine(state, project, read_ahead_cap=cap)


# -- the reference -------------------------------------------------------------------

INPUT, UPSTREAM, DEFINITION = "input changed", "upstream stale", "definition changed"


def everything(key: str) -> bool:
    return True


@dataclass
class EachAsset:
    """An each=True output: whether it has a head (without one it is
    `missing`, never `stale`); for each key it holds, the upstream version
    it read and the counter of its write; its asset's last change; the
    keys its per-key incremental input's patterns take. (The engine keeps no such per-key
    record: K47 derives the same answers from the position and the
    read-ahead entries; this is the model of what they mean.)"""

    takes: Callable[[str], bool] = everything
    built: bool = False
    held: dict[str, tuple[int, int]] = field(default_factory=dict)
    changed_at: int = 0
    seen: int = -1  # its last completed pass: `knob` as of then (-1: none)
    entries: int = 0  # keys= runs since the last default run: the cap counts them (K47)


@dataclass
class ByPartition:
    """A plain incremental output. Its record: a snapshot (the counter
    through which it read every change of `items`) and an entry per keys=
    run since (the commit it read at, the keys it named); or, while a full
    pass is due (`snapshot` None), whether it has started,
    and what it delivered (key -> the version delivered). Also: whether it
    has a head, the keys its patterns take and, keyed, the keys it holds."""

    takes: Callable[[str], bool] = everything
    built: bool = False
    snapshot: int | None = None
    input_reset: bool = False  # its upstream was replaced; no completed pass has read it since
    changed_at: int = 0  # its asset's last change
    definition_seen: int = 0  # the asset change its last completed pass ran under
    pass_base: int = 0  # `items`' newest commit when the pass due began
    started: bool = False
    passed: dict[str, int] = field(default_factory=dict)
    entries: list[tuple[int, frozenset[str]]] = field(default_factory=list)
    keys: set[str] = field(default_factory=set)


class Reference:
    """`feed` (a keyed source, its keys versioned) -> `items` (plain
    incremental, run by hand) and `fchecks` (each=True); `items` ->
    `checks` (each=True, with the source `knob` a dep), `copy` (plain
    incremental, keyed) and `count` (plain incremental, unkeyed). What
    K43–K46 say of each after any history of feed commits, runs of `items`,
    its resets, `knob` changes, `checks`' own resets, asset changes, keys=
    runs and default runs: what each run delivers, and what is stale, why."""

    def __init__(
        self,
        takes: Callable[[str], bool] = everything,
        cap: int = 10_000,
    ):
        self.cap = cap  # keys= runs a plain incremental partition takes between default runs
        self.now = 0
        self.feed: dict[str, str] = {}  # key -> its version
        self.feed_read: dict[str, str] = {}  # `feed` as `items` last read it
        self.up: dict[str, int] = {}  # `items`: key -> the counter of its write (its generation)
        self.changed: dict[str, int] = {}  # `items` key -> the counter of its last change, a removal too
        self.history: dict[
            str, list[tuple[int, int | None]]
        ] = {}  # `items` key -> (when, generation or None)
        self.fchecks = EachAsset()  # each=True over `feed`: held = key -> (version read, written)
        self.last_commit = 0  # `items`' newest commit
        self.knob = 0
        self.checks = EachAsset(takes=takes)
        self.others = {"copy": ByPartition(takes=takes), "count": ByPartition()}

    def _tick(self) -> int:
        self.now += 1
        return self.now

    # history

    def commit_feed(self, upserts: set[str] | dict[str, str], removes: set[str]) -> None:
        """Upserts at the versions given, or at fresh ones (a set)."""

        t = self._tick()
        versions = upserts if isinstance(upserts, dict) else dict.fromkeys(upserts, f"v{t}")
        for k in removes - set(versions):
            self.feed.pop(k, None)
        self.feed.update(versions)

    def _write(self, k: str, generation: int | None, t: int) -> None:
        if generation is None:
            self.up.pop(k, None)
        else:
            self.up[k] = generation
        self.changed[k] = t
        self.history.setdefault(k, []).append((t, generation))
        self.last_commit = t

    def run_items(self) -> None:
        """`items` reads the net delta of `feed` since it last read it."""

        t = self._tick()
        for k in sorted(set(self.feed) | set(self.feed_read)):
            if self.feed.get(k) != self.feed_read.get(k):
                if k in self.feed or k in self.up:
                    self._write(k, t if k in self.feed else None, t)
        self.feed_read = dict(self.feed)

    def _at(self, k: str, when: int | None) -> int | None:
        """`items`' key `k` as of counter `when`: its generation, or None."""

        state = None
        for at, generation in self.history.get(k, ()):
            if when is not None and at > when:
                break
            state = generation
        return state

    def commit(self, upserts: set[str], removes: set[str]) -> None:
        """A feed commit, and the run of `items` that takes it."""

        self.commit_feed(upserts, removes)
        self.run_items()

    def reset_upstream(self) -> None:
        """`items` moved, and rebuilt from `feed`: every key at a new version;
        its consumers owe a full pass."""

        t = self._tick()
        for k in sorted(set(self.up) | set(self.feed)):
            self._write(k, t if k in self.feed else None, t)
        self.feed_read = dict(self.feed)
        for o in self.others.values():
            self._owe_a_pass(o)
            o.input_reset = True

    def _owe_a_pass(self, o: ByPartition) -> None:
        if o.snapshot is not None:
            o.pass_base = self.last_commit
        o.snapshot, o.started, o.passed, o.entries = None, False, {}, []

    @staticmethod
    def _complete(o: ByPartition, snapshot: int) -> None:
        """A pass is complete: it read the upstream as it is, under the asset's
        current definition."""

        o.snapshot, o.entries, o.passed, o.started = snapshot, [], {}, False
        o.input_reset, o.definition_seen = False, o.changed_at

    def change_knob(self) -> None:
        self.knob = self._tick()

    def reset_checks(self) -> None:
        """`checks` itself reset (R6): empty; a reset is an asset change."""

        t = self._tick()
        self.checks = EachAsset(takes=self.checks.takes, changed_at=t)

    def change_asset(self, name: str) -> None:
        t = self._tick()
        if name == "checks":
            self.checks.changed_at = t
        else:
            o = self.others[name]
            o.changed_at = t
            self._owe_a_pass(o)

    def run_keys(self, keys: set[str], name: str = "checks"):
        """A keys= run. On `checks` (each=True), R2: the named keys its
        patterns take are fresh after; returns None. On a plain incremental
        asset: (what it delivers, whether it starts over), or "refused" past
        the cap."""

        t = self._tick()
        if name == "checks":
            if self.checks.entries >= self.cap:
                return "refused"
            self.checks.entries += 1
            self.checks.built = True
            for k in keys:
                if not self.checks.takes(k):
                    continue
                if k in self.up:
                    self.checks.held[k] = (self.up[k], t)
                else:
                    self.checks.held.pop(k, None)
            if not self._unwritten():  # nothing left uncovered: the record collapses
                self.checks.entries, self.checks.seen = 0, t  # and any pass due is complete
            return None
        o = self.others[name]
        if len(o.entries) >= self.cap:
            return "refused"
        delivered = keys & self.pending(name)
        start_over = self._deliver(o, delivered)
        o.entries.append((self.last_commit, frozenset(keys)))
        if not self.pending(name):  # nothing past the snapshot uncovered: the record collapses
            if o.snapshot is None:
                self._complete(o, self.last_commit)
            else:
                o.snapshot, o.entries = self.last_commit, []
        return delivered, start_over

    def run_default(self, name: str):
        """A default run: (what it delivers, whether it starts over) on a
        plain incremental asset; it finishes any pass due, and collapses the
        record into its snapshot. On `checks`, the keys it writes."""

        t = self._tick()
        if name == "checks":
            c = self.checks
            behind = set(self._unwritten())  # what a pass due has not written, or the delta
            c.built, c.entries, c.seen = True, 0, t
            for k in behind:
                if k in self.up:
                    c.held[k] = (self.up[k], t)
                else:
                    c.held.pop(k, None)
            return behind & set(self.up)
        o = self.others[name]
        delivered = self.pending(name)
        start_over = self._deliver(o, delivered)
        if o.snapshot is None:
            self._complete(o, self.last_commit)
        else:
            o.snapshot, o.entries = self.last_commit, []
        return delivered, start_over

    def run_full(self) -> set[str]:
        """A full run of `checks` (`mode="full"`): every key rewritten,
        `knob` caught up. Returns the keys it writes."""

        t, c = self._tick(), self.checks
        c.built, c.entries, c.seen = True, 0, t
        c.held = {k: (v, t) for k, v in self.up.items() if c.takes(k)}
        return set(c.held)

    def _deliver(self, o: ByPartition, delivered: set[str]) -> bool:
        start_over = o.snapshot is None and not o.started
        if start_over:
            o.keys, o.started = set(), True  # the consumer rebuilds
        o.built = True
        for k in delivered:
            (o.keys.add if k in self.up else o.keys.discard)(k)
            if o.snapshot is None:
                o.passed[k] = self.changed[k]
        return start_over

    # answers

    def pending(self, name: str) -> set[str]:
        """What a plain incremental asset's record lacks. In a full pass:
        the keys under its patterns not delivered in it at their current
        version, and the ones it delivered that were removed since. Else:
        the changes past its snapshot, under its patterns, named by no entry
        read at or after the change."""

        o = self.others[name]
        if o.snapshot is None:
            present = {k for k in self.up if o.takes(k) and o.passed.get(k) != self.changed[k]}
            gone = {k for k, v in o.passed.items() if k not in self.up and v != self.changed[k]}
            return present | gone
        return {  # the net delta: a key back where it was at the snapshot has not changed
            k
            for k, t in self.changed.items()
            if o.takes(k)
            and self._at(k, o.snapshot) != self.up.get(k)
            and not any(k in named and at >= t for at, named in o.entries)
        }

    def items_stale(self) -> bool:
        return self.feed != self.feed_read

    def run_fchecks(self, keys: set[str] | None = None) -> None:
        """A run of `fchecks`, each=True over `feed`: every key (default), or
        the named ones (keys=), made to match `feed`."""

        t = self._tick()
        f = self.fchecks
        f.built = True
        for k in set(self.feed) | set(f.held) if keys is None else keys:
            if k in self.feed:
                f.held[k] = (self.feed[k], t)
            else:
                f.held.pop(k, None)

    def fchecks_stale_keys(self) -> set[str]:
        f = self.fchecks
        return {
            k for k in set(self.feed) | set(f.held) if k not in f.held or f.held[k][0] != self.feed.get(k)
        }

    def direct_stale_keys(self) -> dict[str, set[str]]:
        """`checks`' stale keys by its own inputs, with why: each key by its
        own write, and every key while `knob` moved past the record's."""

        c, out = self.checks, self._unwritten()
        if c.built and self.knob > c.seen:  # one `knob` version per partition (semantic change (d))
            for k in {k for k in self.up if c.takes(k)} | set(c.held):
                out.setdefault(k, set()).add(INPUT)
        return out

    def _unwritten(self) -> dict[str, set[str]]:
        """What a run has yet to write, with why: each key `checks` lacks,
        holds past its upstream, or wrote before `knob`'s move or its
        asset's change."""

        c, out = self.checks, {}
        for k in {k for k in self.up if c.takes(k)} | set(c.held):
            why = set()
            if k not in c.held or k not in self.up:
                why.add(INPUT)
            else:
                read, written = c.held[k]
                if self.up[k] > read or written < self.knob:
                    why.add(INPUT)
                if written < c.changed_at:
                    why.add(DEFINITION)
            if why:
                out[k] = why
        return out

    def stale_keys(self) -> set[str]:
        """`checks`' stale keys: its own, and, while `items` is stale, every
        key whose upstream key is (all of them: `items` is stale by partition)."""

        c = self.checks
        direct = set(self.direct_stale_keys())
        if self.items_stale():
            return direct | {k for k in self.up if c.takes(k)} | set(c.held)
        return direct

    def stale_keys_of(self, name: str) -> set[str]:
        """The stale keys of a keyed asset: per key for `checks`; for `copy`
        all its keys, or none."""

        if name == "checks":
            return self.stale_keys()
        return set(self.others[name].keys) if self.stale(name) else set()

    # each reason its own predicate over the records (Erwin's ruling): those that
    # hold are reported, and nothing remembers why a unit went stale

    def input_changed(self, name: str) -> bool:
        o = self.others[name]
        if o.snapshot is not None:
            return bool(self.pending(name))
        moved = {k for k, t in self.changed.items() if t > o.pass_base}  # since the pass began
        return o.input_reset or bool(self.pending(name) & moved)

    def definition_changed(self, name: str) -> bool:
        o = self.others[name]
        return o.changed_at > o.definition_seen

    def upstream_stale(self) -> bool:
        return self.items_stale()

    def reasons(self, name: str) -> set[str]:
        """Why `name` is stale (K46): the reasons whose predicate holds."""

        if name == "items":
            return {INPUT} if self.items_stale() else set()
        if name == "fchecks":
            return {INPUT} if self.fchecks.built and self.fchecks_stale_keys() else set()
        if name == "checks":
            if not self.checks.built:
                return set()  # never built: `missing`, not `stale`
            why = set().union(*self.direct_stale_keys().values())
        else:
            if not self.others[name].built:
                return set()
            why = {INPUT} if self.input_changed(name) else set()
            why |= {DEFINITION} if self.definition_changed(name) else set()
        return why | ({UPSTREAM} if self.upstream_stale() else set())

    def stale(self, name: str) -> bool:
        return bool(self.reasons(name))
