"""Staleness at every level (K36–K38), for the simulation and the property
tests: the engine's answers, behind placeholder names, and a reference
model of what they must be.

The rules (W22 builds them after the journal head; until then the tests
that use this module are strict xfails or off):

- R1. A default run whose input has no position (after an upstream reset,
  or brand new) reads a full pass, and its write is a reset write.
- R2. A `keys=` run makes each named key match its upstream: written,
  removed, or left alone if neither side has it. Never a reset write:
  every key not named keeps its value.
- R3. For a per-key asset (an `Each` input: one output key per input key),
  a key is stale if its upstream key has a newer version than the one it
  last read, if the upstream has it and the output does not, if the output
  has it and the upstream removed it, or if it was written before its
  asset's last change.
- R4. A per-key partition is stale exactly when one of its keys is. Any
  other partition is stale when an upstream it reads changed past its
  position, or its asset changed, until a default run catches it up.
  `keys=` runs that leave no stale key catch the partition up.
- R5. A `keys=` run that leaves a key stale moves no position.
- R6. An output reset itself starts empty: a `keys=` run leaves just its
  keys; the next default run converges.
- K38. An asset is stale exactly when one of its partitions is.

Every engine call the tests make about staleness is in this module, so
adapting to the names W22 picks is one edit here.
"""

from __future__ import annotations

from dataclasses import dataclass, field

LANDED = False  # W22's K36–K38 build; True turns the tests on


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


@dataclass
class Upstream:
    """A keyed upstream: each key's version (the counter of its last
    write), the keys present."""

    versions: dict[str, int] = field(default_factory=dict)


@dataclass
class PerKey:
    """A per-key output: whether it has a head (without one it is
    `missing`, never `stale`); for each key it holds, the upstream version
    it read and the counter of its write; `changed_at`, its asset's last
    change; `position`, the upstream counter it is caught up to (None: a
    default run reads a full pass)."""

    built: bool = False
    held: dict[str, tuple[int, int]] = field(default_factory=dict)
    changed_at: int = 0
    position: int | None = None


@dataclass
class Unkeyed:
    """An unkeyed output: the counter of its last catch-up (None: never),
    the upstream counter it read up to."""

    caught_up_at: int | None = None
    position: int | None = None
    changed_at: int = 0


class Reference:
    """One upstream, one per-key consumer, one unkeyed consumer: what
    K36–K38 say each is after any history of upstream changes, resets,
    asset changes, `keys=` runs and default runs."""

    def __init__(self):
        self.now = 0
        self.up = Upstream()
        self.up_changed = 0  # counter of the upstream's last change
        self.per_key = PerKey()
        self.unkeyed = Unkeyed()

    def _tick(self) -> int:
        self.now += 1
        return self.now

    # history

    def commit(self, upserts: set[str], removes: set[str]) -> None:
        t = self._tick()
        for k in upserts:
            self.up.versions[k] = t
        for k in removes:
            self.up.versions.pop(k, None)
        if upserts or removes:
            self.up_changed = t

    def reset_upstream(self) -> None:
        """The upstream moved (or was removed and added back) and was
        rebuilt: the same keys at new versions; its consumers lose their
        positions."""

        t = self._tick()
        self.up.versions = dict.fromkeys(self.up.versions, t)
        self.up_changed = t
        self.per_key.position = None
        self.unkeyed.position = None

    def reset_per_key(self) -> None:
        """The per-key output itself reset (R6): empty, no position; a
        reset is an asset change."""

        t = self._tick()
        self.per_key = PerKey(changed_at=t)

    def change_asset(self, which: str) -> None:
        t = self._tick()
        if which == "per_key":
            self.per_key.changed_at = t
            self.per_key.position = None  # a new fingerprint reads a full pass
        else:
            self.unkeyed.changed_at = t
            self.unkeyed.position = None

    def run_keys(self, keys: set[str]) -> None:
        """R2 on the per-key output; R4/R5: catches it up only if no key is
        left stale."""

        t = self._tick()
        self.per_key.built = True
        for k in keys:
            if k in self.up.versions:
                self.per_key.held[k] = (self.up.versions[k], t)
            else:
                self.per_key.held.pop(k, None)
        if not self.stale_keys():
            self.per_key.position = self.up_changed

    def run_default(self, which: str) -> None:
        """A default run, caught up at its end: every key fresh."""

        t = self._tick()
        if which == "per_key":
            self.per_key.built = True
            self.per_key.held = {k: (v, t) for k, v in self.up.versions.items()}
            self.per_key.position = self.up_changed
        else:
            self.unkeyed.caught_up_at = t
            self.unkeyed.position = self.up_changed

    # answers

    def stale_keys(self) -> set[str]:
        out, p = set(), self.per_key
        for k in set(self.up.versions) | set(p.held):
            if k not in p.held or k not in self.up.versions:
                out.add(k)
                continue
            read, written = p.held[k]
            if self.up.versions[k] > read or written < p.changed_at:
                out.add(k)
        return out

    def per_key_stale(self) -> bool:
        return self.per_key.built and bool(self.stale_keys())

    def unkeyed_stale(self) -> bool:
        u = self.unkeyed
        if u.caught_up_at is None:
            return False  # never built: `missing`, not `stale`
        return u.position != self.up_changed or u.caught_up_at < u.changed_at
