"""A per-key asset's key outcomes (docs/per-key-processing.md §9): each
key's latest outcome in a partition. Only outcomes other than ok are
stored, in the partition's outcome index (`@asset`); the rest are derived
from the each input's observation record and the upstream index:

- a stored outcome — `rejected`, `failed`, `retrying`, `canceled` or
  `timed_out` — is the key's;
- `ok`: the record holds the key, processed at the version it holds —
  None where upstream has replaced that version since (the index keeps no
  older versions);
- `unmatched`: upstream holds the key and the input's patterns leave it out;
- `removed`: upstream had the key at its index's cut (its first commit
  where nothing is cut), lacks it now, and the record no longer holds it;
- None: never processed — or removed before the cut, or added and removed
  again since: the index answers Δ between two commits, not every flip in
  between. The cut follows the oldest reader; T39 keeps it back to the
  oldest retained run (D180), so that a removal is known as long as its run.

Every key also says whether the input owes it (`owed`): `f3`, processed ok
at v5 with upstream now at v6, is `ok`, owed. An input with no record of
its upstream's current life has derived nothing yet, and owes every key
upstream holds and every stored one."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass

from solera.key_outcomes import NAMES, OK, REMOVED, UNMATCHED, StoredOutcome
from solera.keys.delta import delta, version_of
from solera.keys.layers import LayerIndex, key_bytes, key_str
from solera.patterns import Matcher

from . import owed

OUTCOMES = (OK, REMOVED, UNMATCHED, *NAMES.values())


@dataclass(frozen=True)
class Derived:
    """One key's latest outcome (None: none yet), the upstream version it
    came at, whether the input owes the key, and its stored outcome if any."""

    key: str
    outcome: str | None
    version: object | None
    owed: bool
    stored: StoredOutcome | None


@dataclass(frozen=True)
class Sources:
    """What a partition's outcomes are read from: the each input's upstream
    index (None: the partition reads no upstream now), its observation
    record (None: none of the upstream's current life), the comparison's
    new side, the consumer's own indexes, and its outcome index."""

    upstream: LayerIndex | None
    rec: dict | None
    now: owed.Now | None
    held: list[LayerIndex]
    stored: LayerIndex | None

    def _since(self) -> int:
        """The oldest commit the upstream index answers Δ from: its cut, or
        its first commit where nothing is cut. A key present there and
        absent now was removed since."""

        return max(self.upstream.state.cut, 0)

    async def stored_keys(self, after: str | None) -> AsyncIterator[str]:
        """The keys with a stored outcome past `after`, in order."""

        if self.stored is not None:
            async for d in owed._live(self.stored, self.stored.state.head, after, None):
                yield d.key

    async def keys(self, after: str | None) -> AsyncIterator[str]:
        """Every key that may have an outcome past `after`, in order: those
        upstream holds, removed since its cut, owed or stored."""

        streams = [self.stored_keys(after)]
        if self.upstream is not None:
            head, since = self.upstream.state.head, self._since()
            streams.append(_keys(owed._live(self.upstream, head, after, None)))
            if since < head:
                streams.append(_keys(_removed(owed._diffs(self.upstream, since, None, after, None))))
            if self.rec is not None:
                found = owed.candidates(self.upstream, self.rec, self.now, after, self.held)
                streams.append(_keys(_owing(found)))
        async for key in _union(streams):
            yield key

    async def derive(self, keys: list[str]) -> list[Derived]:
        """Each of `keys`' latest outcome, in key order."""

        keys = sorted(set(keys))
        named = [key_bytes(k) for k in keys]
        stored = {}
        if self.stored is not None:
            found = await self.stored.lookup(named)
            stored = {key_str(k): StoredOutcome.decode(p) for k, (_, p) in found.items()}
        up, owes, gone = {}, {}, set()
        if self.upstream is not None:
            found = await self.upstream.lookup(named)
            up = {key_str(k): version_of(g, p) for k, (g, p) in found.items()}
            take = Matcher(self.now.patterns)
        if self.upstream is not None and self.rec is not None:
            batch = await owed.batch(self.upstream, self.rec, self.now, len(keys), keys=keys, held=self.held)
            owes = {o.key: o for o in batch.keys}  # one left out is held absent, and absent now
            if self._since() < self.upstream.state.head:
                diffs = (await delta(self.upstream, self._since(), None, keys=keys)).diffs
                gone = {d.key for d in diffs if d.before and not d.after}
        out = []
        for k in keys:
            o, s = owes.get(k), stored.get(k)
            if s is not None:
                outcome, version = s.name, s.upstream
            elif o is not None and o.old is not None:
                outcome, version = OK, o.old[0]
            elif k in up and not take(k):
                outcome, version = UNMATCHED, up[k]
            elif k not in up and k in gone:
                outcome, version = REMOVED, None
            else:
                outcome, version = None, None
            if self.upstream is None:  # the partition reads nothing now: it owes nothing
                owing = False
            elif self.rec is None:  # nothing derived yet: a full run is due
                owing = s is not None or (k in up and take(k))
            else:
                owing = o is not None and o.cls != "unchanged"
            out.append(Derived(k, outcome, version, owing, s))
        return out


async def _keys(stream) -> AsyncIterator[str]:
    async for x in stream:
        yield x.key


async def _removed(diffs) -> AsyncIterator:
    async for d in diffs:
        if d.before and not d.after:
            yield d


async def _owing(found) -> AsyncIterator:
    async for o in found:
        if o.cls is not None:
            yield o


async def _union(streams: list[AsyncIterator[str]]) -> AsyncIterator[str]:
    """Key-ordered streams as one, each key once."""

    heads = [await anext(s, None) for s in streams]
    while any(h is not None for h in heads):
        key = min(h for h in heads if h is not None)
        yield key
        for i, s in enumerate(streams):
            if heads[i] == key:
                heads[i] = await anext(s, None)
