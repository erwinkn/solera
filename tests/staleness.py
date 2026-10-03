"""Staleness at every level (K36–K41), for the simulation and the property
tests: the engine's answers, behind placeholder names, and a reference
model of what they must be.

The rules (W22 builds them after the journal head; until then the tests
that use this module are strict xfails or off):

- Two kinds of asset (K40). An `each=True` asset (one `Each` input; any
  others whole or deps, shared by every key) has exact per-key staleness.
  Every other asset, keyed or not, is stale by partition only, and its
  stale-key listing answers "not tracked per key".
- Its keys (K39, K40). An each=True asset's keys are its `Each` input's
  upstream keys under that input's patterns; nothing else counts.
- A key of an each=True asset is stale if its upstream key changed after
  it read it, if it is missing (the upstream has it under the patterns and
  the output does not), if the output holds it and the upstream removed
  it, if it predates its asset's last change, or if a shared input changed
  after it was written. Its partition is stale exactly when a key is:
  `keys=` runs that leave no stale key leave it fresh.
- Any other partition is stale when it has not seen each input at its
  latest version since its last catch-up, or its asset changed since; for
  an incremental input with patterns, only commits holding a key they take
  count (K39). A `keys=` run never clears it; a default run does.
- A `keys=` run makes each named key its patterns take match its upstream:
  written, removed, or left alone if neither side has it. Never a reset
  write: every key not named keeps its value (R2).
- An output reset itself starts empty: a `keys=` run leaves just its keys;
  the next default run converges (R6).
- An asset is stale exactly when one of its partitions is (K38).
- Positions follow what an attempt read (K41): nothing here says when one
  moves, only what is stale.

Every engine call the tests make about staleness is in this module, so
adapting to the names W22 picks is one edit here.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

LANDED = False  # W22's K36–K41 build; True turns the tests on


class NotBuilt(NotImplementedError):
    """The engine has no such answer yet: what the strict xfails expect."""


# -- the engine's answers --------------------------------------------------------------


async def stale_keys(engine, asset: str, partition: str = "") -> set[str] | None:
    """The exact stale keys of a per-key asset's partition, every page of
    them; None where staleness is not tracked per key."""

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
    """Any other output: the counter of its last catch-up (None: never
    built), and its asset's last change; the keys its input's patterns take."""

    takes: Callable[[str], bool] = everything
    caught_up_at: int | None = None
    changed_at: int = 0


class Reference:
    """One keyed upstream (`items`) and one shared input (`knob`); `checks`,
    each=True over `items` with `knob` a dep; `copy`, a keyed incremental
    consumer of `items`; `count`, an unkeyed one. What K36–K41 say each is
    after any history of upstream commits, upstream resets, shared-input
    changes, the each=True output's own resets, asset changes, `keys=` runs
    and default runs."""

    def __init__(self, takes: Callable[[str], bool] = everything):
        self.now = 0
        self.up: dict[str, int] = {}  # key -> the counter of its last write
        self.commits: list[tuple[int, frozenset[str]]] = []  # every change of `items`: when, which keys
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
        if upserts or removes:
            self.commits.append((t, frozenset(upserts | removes)))

    def reset_upstream(self) -> None:
        """`items` moved and rebuilt: the same keys at new versions."""

        t = self._tick()
        self.up = dict.fromkeys(self.up, t)
        self.commits.append((t, frozenset(self.up)))

    def change_knob(self) -> None:
        self.knob = self._tick()

    def reset_checks(self) -> None:
        """`checks` itself reset (R6): empty; a reset is an asset change."""

        t = self._tick()
        self.checks = EachAsset(takes=self.checks.takes, changed_at=t)

    def change_asset(self, name: str) -> None:
        t = self._tick()
        (self.checks if name == "checks" else self.others[name]).changed_at = t

    def run_keys(self, name: str, keys: set[str]) -> None:
        """R2. On `checks`, the named keys its patterns take are fresh after;
        on any other asset nothing about staleness changes."""

        t = self._tick()
        if name != "checks":
            return
        self.checks.built = True
        for k in keys:
            if not self.checks.takes(k):
                continue
            if k in self.up:
                self.checks.held[k] = (self.up[k], t)
            else:
                self.checks.held.pop(k, None)

    def run_default(self, name: str) -> None:
        """A default run, caught up at its end."""

        t = self._tick()
        if name == "checks":
            c = self.checks
            c.built = True
            c.held = {k: (v, t) for k, v in self.up.items() if c.takes(k)}
        else:
            self.others[name].caught_up_at = t

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

    def stale(self, name: str) -> bool:
        if name == "checks":
            return self.checks.built and bool(self.stale_keys())
        o = self.others[name]
        if o.caught_up_at is None:
            return False  # never built: `missing`, not `stale`
        seen = any(t > o.caught_up_at and any(o.takes(k) for k in keys) for t, keys in self.commits)
        return seen or o.caught_up_at < o.changed_at
