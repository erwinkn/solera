"""Staleness (K43, K45, K46; docs/positions-from-reads.md): whether an output
unit is due a rebuild, and why, computed on demand from the records of what
was read — never a stored dirty flag.

A materialized partition is stale for one or more reasons:

- `input changed`: an input unit it depends on changed since it read it —
  an incremental input with changes past its position its read-ahead lacks
  (or no position, or a pass under way), or a whole or dep input at another
  version than it caught up to;
- `upstream stale`: a partition it reads is itself stale, to any depth;
- `definition changed`: its asset changed since it last caught up.

A key of a keyed output that is not `each` shares its partition's answer.
"""

from __future__ import annotations

import logging

from solera.keys.index import KeyIndex, key_str
from solera.patterns import Matcher

from . import planning
from .positions import outstanding

log = logging.getLogger(__name__)

INPUT, UPSTREAM, DEFINITION = "input changed", "upstream stale", "definition changed"
BEHIND_PAGE = 1000


class Staleness:
    async def stale_reasons(self, asset: str, partition: str, memo: dict | None = None) -> list[str]:
        """Why a partition is stale: exactly the reasons whose predicate holds
        (Erwin's ruling: each its own function of the records, nothing kept
        of earlier causes), in a stable order; empty if it is fresh or was
        never built (missing is not stale). `memo` shares one walk up the
        lineage between calls: a project's statuses come from one pass."""

        memo = {} if memo is None else memo
        key = (asset, partition)
        if key in memo:
            return memo[key]
        memo[key] = []  # a DAG: never revisited while it is being computed
        planner = self.planner()
        if not self.built(asset, partition, planner):
            return memo[key]
        try:
            inputs = planner.inputs(asset, partition)
        except planning.UpstreamOnly:
            inputs = []
        reasons = []
        if await self.input_changed(asset, partition, planner, inputs):
            reasons.append(INPUT)
        if await self.upstream_stale(asset, partition, planner, inputs, memo):
            reasons.append(UPSTREAM)
        if self.definition_changed(asset, partition):
            reasons.append(DEFINITION)
        memo[key] = reasons
        return reasons

    def definition_changed(self, asset: str, partition: str) -> bool:
        """Its asset changed (version, configuration, patterns, rename, added or
        reset) since it last caught up: a pass under the new definition clears it."""

        caught_up_at = int(self.m.partition(asset, partition).get("caught_up_at", 0))
        return caught_up_at < int(self.m.changed_at.get(asset, 0))

    async def input_changed(self, asset: str, partition: str, planner, inputs) -> bool:
        """An input unit it depends on changed past what its record covers: an
        upstream reset replaced its content since it caught up; an incremental
        input has changes past its position its read-ahead lacks (a pass due
        only to its own definition is no input change); a whole or dep input
        is at another version than the one it caught up to. A pass that
        reads the new upstream clears it."""

        record = self.m.partition(asset, partition)
        if int(record.get("input_reset_at", -1)) > int(record.get("caught_up_at", -1)):
            return True
        definition = self.definition_changed(asset, partition)
        seen = record.get("seen")
        for input in inputs:
            if input.kind == "incremental":
                if await self._input_behind(asset, partition, input, definition):
                    return True
            elif seen is not None and self._versioned(input):
                if seen.get(input.param) != self._input_version(planner, input):
                    return True
        return False

    async def upstream_stale(self, asset: str, partition: str, planner, inputs, memo: dict) -> bool:
        """A partition it reads is itself stale, to any depth (K46)."""

        for input in inputs:
            for upstream in self._upstreams(planner, input):
                if input.owner is not None and await self.stale_reasons(input.owner, upstream, memo):
                    return True
        return False

    def built(self, asset: str, partition: str, planner=None) -> bool:
        """Whether a partition was built since its outputs were last reset — a
        head of one of its outputs, or a commit (a keys= run that took no key
        included): what can be stale. Never built, it is missing."""

        planner = planner or self.planner()
        if planner.materialized(asset, partition):
            return True
        if any(self.m.heads.get((o["name"], partition)) for o in self.manifest["assets"][asset]["outputs"]):
            return True
        return "caught_up" in self.m.partition(asset, partition)

    @staticmethod
    def _upstreams(planner, input) -> list[str]:
        """The upstream partitions an input reads: one, or a fan-in's."""

        if input.kind == "all_partitions" or input.fan_in:
            return list(planner.fan_in(input, materialized=True))
        return [input.partition]

    async def _input_behind(self, asset: str, partition: str, input, definition: bool = False) -> bool:
        """Whether an incremental input has changes past its position that it has
        not read: commits past `next` — for a keyed upstream, a key its
        patterns take changed past `next`, not read ahead at or after its
        change. No position, or a pass, pattern change or cleanup under way,
        counts only when its own `definition` did not make the pass due; a
        full pass under way counts again for commits past its base.

        Conservative where the index cannot tell yet: a key removed past
        `next` counts even when it was also added past `next`, so it may
        report a change that nets out (docs/positions-from-reads.md)."""

        position = self.m.position(asset, input.param, partition)
        head = self.m.heads.get((input.output, input.partition))
        hi = int((head or {}).get("commit_number", -1))
        if position is None:
            return not definition
        under_way = position.get("pass") or {}
        if under_way.get("mode") == "full":
            return not definition or hi >= int(under_way["from"])
        if outstanding(position) and under_way.get("mode") != "delta":
            return not definition
        if head is None:
            return False
        lo = int(position["next"])
        if lo > hi:
            return False
        if position.get("kind") != "keys":
            return True  # an unkeyed upstream: any commit past `next`
        patterns = input.spec.get("patterns")
        ahead = position.get("ahead") or []
        memo_key = (input.output, input.partition, lo, hi, repr(patterns), repr(ahead))
        cache = self.__dict__.setdefault("_behind_cache", {})
        if memo_key in cache:
            return cache[memo_key]
        state = self.m.index(input.output, input.partition)
        if not state.covers(lo, hi):
            return True  # the log no longer holds it: the next pass is full
        read = (await self._read_ahead_of(asset, input.param, ahead)) if ahead else {}
        taken = Matcher(patterns)
        behind = False
        with self.m.reading(state.prefix):
            index = KeyIndex(self._key_io(), None, state.slice(lo, hi), self.key_options)
            after = None
            while not behind:
                keys, generations, _, _, after = await index.pending(lo, hi, after, BEHIND_PAGE)
                behind = any(
                    taken(key_str(k)) and read.get(key_str(k), -1) < g
                    for k, g in zip(keys, generations, strict=True)
                )
                if after is None:
                    break
        if len(cache) > 10_000:
            cache.clear()
        cache[memo_key] = behind
        return behind

    async def _read_ahead_of(self, asset: str, param: str, entries: list) -> dict[str, int]:
        """One input's read-ahead as key -> the latest generation an entry read."""

        out: dict[str, int] = {}
        for _, run, attempt in entries:
            spec = await self.state.attempt_spec(run, attempt)
            pin = ((spec or {}).get("inputs") or {}).get(param)
            if pin is None:
                continue
            generation = int(pin["ref"].get("generation") or 0)
            for key in pin["batch"]["keys"]:
                out[key] = max(out.get(key, -1), generation)
        return out

    async def stale_keys(
        self, asset: str, partition: str = "", *, after: str | None = None, limit=1000
    ) -> dict:
        """A page of a keyed asset's stale keys in `partition`, and why
        (K43): a keyed output that is not `each` has all its keys stale or
        none. `tracked` is false for an unkeyed asset, which has no keys.
        `next` is the cursor for the page after, None at the end."""

        if asset not in self.manifest["assets"]:
            raise KeyError(asset)
        keyed = [o["name"] for o in self.manifest["assets"][asset]["outputs"] if o.get("key") is not None]
        reasons = await self.stale_reasons(asset, partition)
        if not keyed:
            return {"tracked": False, "keys": [], "next": None, "reasons": reasons}
        if not reasons or (keyed[0], partition) not in self.m.heads:
            return {"tracked": True, "keys": [], "next": None, "reasons": reasons}
        page = await self.list_keys(keyed[0], partition, after=after, limit=limit)
        return {"tracked": True, "keys": list(page["keys"]), "next": page.get("next"), "reasons": reasons}
