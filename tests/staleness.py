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
- A plain incremental partition is stale while some change past its
  snapshot, under the patterns, is covered by no entry, or its asset
  changed (cleared by a default run); its keys share the partition's answer.
- An output reset itself starts empty: a `keys=` run leaves just its keys;
  the next default run converges (R6).
- Positions are derived from what each attempt read; nothing here asserts one.

Every engine call the tests make about staleness is in this module, so
adapting to the names W22 picks is one edit here.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

LANDED = False  # W22's K43 build; True turns the tests on


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


async def asset_stale(engine, asset: str) -> bool:
    """The asset rollup's own flag (asset_statuses, the console's graph)."""

    rollup = (await engine.asset_statuses())[asset]
    if "stale" not in rollup:
        raise NotBuilt("asset_statuses()[asset]['stale'] (K38)")
    return bool(rollup["stale"])


def engine_with_read_ahead_cap(state, project, cap: int):
    """An engine whose partitions take at most `cap` keys= runs between two
    default runs: the cap's option, placeholder name `max_read_ahead`."""

    import inspect

    from solera_server.engine import Engine

    from tests.server.engines import make_engine

    if "max_read_ahead" not in inspect.signature(Engine.__init__).parameters:
        raise NotBuilt("Engine(max_read_ahead=) (K45)")
    return make_engine(state, project, max_read_ahead=cap)


# -- the reference -------------------------------------------------------------------


def everything(key: str) -> bool:
    return True


@dataclass
class EachAsset:
    """An each=True output: whether it has a head (without one it is
    `missing`, never `stale`); for each key it holds, the upstream version
    it read and the counter of its write; its asset's last change; the
    keys its `Each` input's patterns take."""

    takes: Callable[[str], bool] = everything
    built: bool = False
    held: dict[str, tuple[int, int]] = field(default_factory=dict)
    changed_at: int = 0


@dataclass
class ByPartition:
    """A plain incremental output: whether it has a head; its record, a
    snapshot (the counter through which it read every change; None after a
    reset or an asset change: the next default run reads a full pass) and
    one entry per keys= run since (the commit it read at, the keys it
    named); its last default run and its asset's last change; the keys its
    input's patterns take, and, keyed, the keys it holds."""

    takes: Callable[[str], bool] = everything
    built: bool = False
    snapshot: int | None = None
    entries: list[tuple[int, frozenset[str]]] = field(default_factory=list)
    caught_up_at: int = 0
    changed_at: int = 0
    keys: set[str] = field(default_factory=set)


class Reference:
    """One keyed upstream (`items`) and one shared input (`knob`); `checks`,
    each=True over `items` with `knob` a dep; `copy`, a keyed incremental
    consumer of `items`; `count`, an unkeyed one. What K43 and K45 say each is
    after any history of upstream commits, upstream resets, shared-input
    changes, the each=True output's own resets, asset changes, `keys=` runs
    and default runs."""

    def __init__(self, takes: Callable[[str], bool] = everything, cap: int = 10_000):
        self.cap = cap  # keys= runs a plain incremental partition takes between default runs
        self.now = 0
        self.up: dict[str, int] = {}  # key -> the counter of its last write
        self.changed: dict[str, int] = {}  # key -> the counter of its last change, a removal too
        self.last_commit = 0
        self.knob = 0  # the counter of the shared input's last change
        self.checks = EachAsset(takes=takes)
        self.others = {"copy": ByPartition(takes=takes), "count": ByPartition()}

    def _tick(self) -> int:
        self.now += 1
        return self.now

    # history

    def commit(self, upserts: set[str], removes: set[str]) -> None:
        t = self._tick()
        removes = {k for k in removes - upserts if k in self.up}
        for k in upserts:
            self.up[k] = t
        for k in removes:
            del self.up[k]
        for k in upserts | removes:
            self.changed[k] = t
        if upserts or removes:
            self.last_commit = t

    def reset_upstream(self) -> None:
        """`items` moved and rebuilt: the same keys at new versions."""

        t = self._tick()
        self.up = dict.fromkeys(self.up, t)
        self.changed.update(dict.fromkeys(self.up, t))
        self.last_commit = t
        for o in self.others.values():
            o.snapshot, o.entries = None, []

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
            o.changed_at, o.snapshot, o.entries = t, None, []

    def run_keys(self, keys: set[str], name: str = "checks") -> set[str] | str | None:
        """A keys= run. On `checks` (each=True), R2: the named keys its
        patterns take are fresh after. On a plain incremental asset: what it
        delivers, the named keys among the changes its record lacks, read as
        of the newest commit, and an entry for it; "refused" past the cap."""

        t = self._tick()
        if name == "checks":
            self.checks.built = True
            for k in keys:
                if not self.checks.takes(k):
                    continue
                if k in self.up:
                    self.checks.held[k] = (self.up[k], t)
                else:
                    self.checks.held.pop(k, None)
            return None
        o = self.others[name]
        if len(o.entries) >= self.cap:
            return "refused"
        delivered = keys & self.pending(name)
        o.built = True
        o.entries.append((self.last_commit, frozenset(keys)))
        for k in delivered:
            (o.keys.add if k in self.up else o.keys.discard)(k)
        return delivered

    def run_default(self, name: str) -> set[str] | None:
        """A default run, caught up at its end. On a plain incremental
        asset: what it delivers, the changes its record lacks (None for a
        full pass, which has no position to start from)."""

        t = self._tick()
        if name == "checks":
            c = self.checks
            c.built = True
            c.held = {k: (v, t) for k, v in self.up.items() if c.takes(k)}
            return None
        o = self.others[name]
        delivered = None if o.snapshot is None else self.pending(name)
        o.built, o.snapshot, o.entries, o.caught_up_at = True, self.last_commit, [], t
        o.keys = {k for k in self.up if o.takes(k)} if name == "copy" else set()
        return delivered

    # answers

    def stale_keys(self) -> set[str]:
        """`checks`' stale keys."""

        c, out = self.checks, set()
        for k in {k for k in self.up if c.takes(k)} | set(c.held):
            if k not in c.held or k not in self.up:
                out.add(k)
                continue
            read, written = c.held[k]
            if self.up[k] > read or written < c.changed_at or written < self.knob:
                out.add(k)
        return out

    def stale_keys_of(self, name: str) -> set[str]:
        """The stale keys of a keyed asset: per key for `checks`; for `copy`
        all its keys, or none."""

        if name == "checks":
            return self.stale_keys()
        return set(self.others[name].keys) if self.stale(name) else set()

    def pending(self, name: str) -> set[str]:
        """The changes a plain incremental asset's record lacks: past its
        snapshot (all of them without one), under its patterns, named by no
        entry read at or after the change."""

        o = self.others[name]
        since = -1 if o.snapshot is None else o.snapshot
        return {
            k
            for k, t in self.changed.items()
            if t > since and o.takes(k) and not any(k in named and at >= t for at, named in o.entries)
        }

    def stale(self, name: str) -> bool:
        if name == "checks":
            return self.checks.built and bool(self.stale_keys())
        o = self.others[name]
        if not o.built:
            return False  # never built: `missing`, not `stale`
        return bool(self.pending(name)) or o.caught_up_at < o.changed_at
