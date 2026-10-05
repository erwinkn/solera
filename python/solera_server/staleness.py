"""Staleness (K43, K46; docs/observed-set.md): whether an output unit is due
a rebuild, and why, computed on demand from the records of what was read —
never a stored dirty flag.

A materialized partition is stale for one or more reasons:

- `input changed`: an input unit it depends on changed since it read it —
  a keyed incremental input owing a key (its observation record compared
  with upstream now, under the current patterns and context), an unkeyed
  one with commits past its position, or a whole or dep input at another
  version than it caught up to;
- `upstream stale`: a partition it reads is itself stale, to any depth;
- `definition changed`: its asset changed since it last caught up.

A key of a keyed output that is not `each` shares its partition's answer.
"""

from __future__ import annotations

import logging

from solera.keys.index import KeyIndex, key_bytes, key_str
from solera.patterns import Matcher

from . import observed, owed, planning

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
            # An each=True asset's by key: a key it holds not rewritten since (K47).
            if self._each_input(asset) is None or await self._each_own(
                asset, partition, planner, inputs, DEFINITION
            ):
                reasons.append(DEFINITION)
        memo[key] = reasons
        return reasons

    def definition_changed(self, asset: str, partition: str) -> bool:
        """Its asset changed (version, configuration, patterns, rename, added or
        reset) since it was written — since it last caught up, else since its
        first commit: a pass under the new definition clears it."""

        record = self.m.partition(asset, partition)
        written = int(record.get("caught_up_at", record.get("built_at", 0)))
        return written < int(self.m.changed_at.get(asset, 0))

    async def input_changed(self, asset: str, partition: str, planner, inputs) -> bool:
        """An input unit it depends on changed past what its record covers: an
        upstream reset replaced its content since it caught up; an incremental
        input has changes past its position its read-ahead lacks (a pass due
        only to its own definition is no input change); a whole or dep input
        is at another version than the one it caught up to. A pass that
        reads the new upstream clears it."""

        definition = self.definition_changed(asset, partition)
        return await self._input_changed_here(asset, partition, planner, inputs, definition)

    async def _input_changed_here(
        self, asset: str, partition: str, planner, inputs, definition: bool
    ) -> bool:
        """`input_changed` from the partition's records. A partition with a
        keyed incremental input follows its whole and dep inputs through
        its layers' context, key by key: one moving owes the keys it holds."""

        record = self.m.partition(asset, partition)
        if int(record.get("input_reset_at", -1)) > int(record.get("caught_up_at", -1)):
            return True
        keyed = any(self._keyed(i) for i in inputs)
        seen = record.get("seen")
        if not keyed and seen is None and any(self._versioned(i) for i in inputs):
            return True  # never caught up: which whole and dep versions it saw, no record says
        for input in inputs:
            if self._keyed(input):
                if await self._owed(asset, partition, input, planner, inputs, first=True):
                    return True
            elif input.kind == "incremental":
                if self._input_behind(asset, partition, input):
                    return True
            elif not keyed and seen is not None and self._versioned(input):
                if seen.get(input.param) != self._input_version(planner, input):
                    return True
        return False

    async def _owed(self, asset: str, partition: str, input, planner, inputs, *, first=False) -> list:
        """What a keyed incremental input owes (`owed.Owe`s): its record
        compared with upstream now — all of upstream if its record names an
        upstream's earlier life. `first`: the first owed key, if any."""

        rec = (self.m.partition(asset, partition).get("observed") or {}).get(input.param)
        index, life = self._upstream(input.output, input.partition)
        each = input.spec.get("each") is not None
        if rec is None or not self._decodable(rec, input.output, input.partition):
            rec = observed.record(life, held=each and rec is not None)
        now = owed.Now(index.state.head, input.spec.get("patterns"), self._context(planner, inputs), life)
        held = self._held(asset, partition) if each else None
        out = []
        with self.m.reading(index.state.prefix, *(h.state.prefix for h in held or ())):
            async for o in owed.candidates(index, rec, now, None, held):
                if o.cls is not None:
                    out.append(o)
                    if first:
                        break
        return out

    async def upstream_stale(self, asset: str, partition: str, planner, inputs, memo: dict) -> bool:
        """A partition it reads is itself stale, to any depth (K46). Along an
        each chain, only through a stale upstream key its patterns take: a
        key depends on its own upstream key and nothing else."""

        each = self._each_input(asset)
        for input in inputs:
            for upstream in self._upstreams(planner, input):
                if input.owner is None or not await self.stale_reasons(input.owner, upstream, memo):
                    continue
                if each is None or input.param != each[0] or self._each_input(input.owner) is None:
                    return True  # every key depends on it
                taken = Matcher(each[1].get("patterns"))
                if any(taken(k) for k in await self._each_keys(input.owner, upstream, {})):
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

        if input.fan_in:
            return list(planner.fan_in(input, materialized=True))
        return [input.partition]

    def _input_behind(self, asset: str, partition: str, input) -> bool:
        """Whether an unkeyed incremental input has commits past the last one
        it read — or what it read no longer holds (its upstream started
        over), or it never read one."""

        rec = (self.m.partition(asset, partition).get("observed") or {}).get(input.param)
        head = self.m.heads.get((input.output, input.partition))
        if head is None:
            return False
        if rec is None or not self._still(rec, input):
            return True
        return int(rec["commit"]) < int(head.get("commit_number", -1))

    def _committed(self, planner, input) -> int:
        """The event counter of the latest commit of the heads a whole or dep input reads."""

        if input.fan_in:
            heads = planner.fan_in(input, materialized=False).values()
        else:
            heads = [self.m.heads.get((input.output, input.partition)) or {}]
        return max((int(h.get("n") or 0) for h in heads), default=0)

    async def _holds(self, state, keys: list[str]) -> set[str]:
        """Which of `keys` an index holds."""

        if state is None or not keys:
            return set()
        with self.m.reading(state.prefix):
            index = KeyIndex(self._key_io(), None, state.slice(), self.key_options)
            found = await index.lookup([key_bytes(k) for k in keys])
        return {key_str(k) for k in found}

    async def stale_keys(
        self, asset: str, partition: str = "", *, after: str | None = None, limit=1000
    ) -> dict:
        """A page of a keyed asset's stale keys in `partition`, and why
        (K43, K47): an `each=True` asset's key by key, derived from its one
        record; a keyed output that is not `each` has all its keys stale or
        none. `tracked` is false for an unkeyed asset, which has no keys.
        `next` is the cursor for the page after, None at the end."""

        if asset not in self.manifest["assets"]:
            raise KeyError(asset)
        keyed = [o["name"] for o in self.manifest["assets"][asset]["outputs"] if o.get("key") is not None]
        reasons = await self.stale_reasons(asset, partition)
        if not keyed:
            return {"tracked": False, "keys": [], "next": None, "reasons": reasons}
        if not reasons:
            return {"tracked": True, "keys": [], "next": None, "reasons": reasons}
        if self._each_input(asset) is not None:
            stale = sorted(await self._each_keys(asset, partition, {}))
            page = [k for k in stale if after is None or k > after][:limit]
            more = len(page) == limit and page[-1] != stale[-1]
            return {"tracked": True, "keys": page, "next": page[-1] if more else None, "reasons": reasons}
        if (keyed[0], partition) not in self.m.heads:
            return {"tracked": True, "keys": [], "next": None, "reasons": reasons}
        page = await self.list_keys(keyed[0], partition, after=after, limit=limit)
        return {"tracked": True, "keys": list(page["keys"]), "next": page.get("next"), "reasons": reasons}

    async def _each_keys(self, asset: str, partition: str, memo: dict) -> set[str]:
        """An `each=True` partition's stale keys (`_each_own`), and along an
        each chain the keys whose upstream key is stale; any other stale
        upstream makes every key stale."""

        if (asset, partition) in memo:
            return memo[(asset, partition)]
        memo[(asset, partition)] = set()
        planner = self.planner()
        try:
            inputs = planner.inputs(asset, partition)
        except planning.UpstreamOnly:
            inputs = []
        if not self.built(asset, partition, planner):
            return set()
        param, spec = self._each_input(asset)
        keys = await self._each_own(asset, partition, planner, inputs)
        taken = Matcher(spec.get("patterns"))
        output = next((o["name"] for o in self.manifest["assets"][asset]["outputs"] if o.get("key")), None)
        for i in inputs:
            for upstream_partition in self._upstreams(planner, i):
                if i.owner is None or not await self.stale_reasons(i.owner, upstream_partition):
                    continue
                if i.param == param and self._each_input(i.owner) is not None:
                    keys |= {k for k in await self._each_keys(i.owner, upstream_partition, memo) if taken(k)}
                else:  # a stale upstream every key depends on
                    keys |= {k async for k, _, _ in _entries(self, self.m.indexes.get((output, partition)))}
        memo[(asset, partition)] = keys
        return keys

    async def _each_own(
        self, asset: str, partition: str, planner, inputs, only: str | None = None
    ) -> set[str]:
        """An `each=True` partition's stale keys by its own inputs (`_each_keys`):
        the keys its input owes — with its definition changed, a full run
        due, every key upstream has under the patterns and every key it
        holds too; `only=DEFINITION`, the keys it holds, present upstream,
        written before its asset changed: an output key's generation is its
        writer's claim, and a claim before the change never commits."""

        param, spec = self._each_input(asset)
        input = next((i for i in inputs if i.param == param), None)
        output = next((o["name"] for o in self.manifest["assets"][asset]["outputs"] if o.get("key")), None)
        if input is None or output is None:
            return set()
        taken = Matcher(spec.get("patterns"))
        up_state = self.m.indexes.get((input.output, input.partition))
        if only == DEFINITION:
            changed = int(self.m.changed_at.get(asset, 0))
            held = [(k, g) async for k, g, _ in _entries(self, self.m.indexes.get((output, partition)))]
            old = [k for k, g in held if g < changed]
            # A key the patterns no longer take is owed its removal.
            dropped = {k for k, _ in held if not taken(k)}
            return ({k for k in old if taken(k)} & await self._holds(up_state, old)) | dropped
        keys = {o.key for o in await self._owed(asset, partition, input, planner, inputs)}
        if only is None and self.definition_changed(asset, partition):
            upstream = {k async for k, _, _ in _entries(self, up_state) if taken(k)}
            keys |= upstream
            for name in (output, f"@{asset}"):
                keys |= {k async for k, _, _ in _entries(self, self.m.indexes.get((name, partition)))}
        return keys


async def _entries(engine, state):
    """An index's live entries in key order: `(key, generation, payload)`."""

    if state is None:
        return
    with engine.m.reading(state.prefix):
        index = KeyIndex(engine._key_io(), None, state.slice(), engine.key_options)
        after = None
        while True:
            keys, generations, payloads, after = await index.page(after, BEHIND_PAGE)
            for k, g, p in zip(keys, generations, payloads, strict=True):
                yield key_str(k), g, p
            if after is None:
                return
