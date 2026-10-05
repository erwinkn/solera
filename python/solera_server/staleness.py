"""Staleness (K43, K46; docs/observed-set.md): whether an output unit is due
a rebuild, and why, computed on demand by comparing what was observed with
what is there now — never a stored dirty flag.

A built partition is stale for one or more reasons:

- `input changed`: an input owes it something — a keyed incremental input a
  key (its observation record compared with upstream now, under the current
  patterns and context: a whole or dep input that moved is in every layer's
  context), an unkeyed one a commit past the last it read; with no keyed
  input, a whole or dep input at another version than its last commit read;
- `upstream stale`: a partition it reads is itself stale, to any depth;
- `definition changed`: its asset's definition is not the one its last
  commit was made under — its next run is a full run.

A key of a keyed output that is not `each` shares its partition's answer.
"""

from __future__ import annotations

import logging

from solera.keys.index import KeyIndex, key_str
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
            reasons.append(DEFINITION)
        memo[key] = reasons
        return reasons

    def definition_changed(self, asset: str, partition: str) -> bool:
        """Its asset's definition — version, configuration, input bindings;
        not patterns or batch size, and a rename keeps it — is not the one
        its last commit was made under, under that commit's run config."""

        record = self.m.partition(asset, partition)
        made = record.get("definition")
        return made is not None and made != self._definition(asset, {"config": record.get("config") or {}})

    async def input_changed(self, asset: str, partition: str, planner, inputs) -> bool:
        """An input owes it something (the module docstring)."""

        keyed = any(self._keyed(i) for i in inputs)
        for input in inputs:
            if self._keyed(input):
                if await self._owed(asset, partition, input, planner, inputs, first=True):
                    return True
            elif input.kind == "incremental" and self._input_behind(asset, partition, input):
                return True
        if keyed or not any(self._versioned(i) for i in inputs):
            return False
        return self.m.partition(asset, partition).get("context") != self._context(planner, inputs)

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

        if any(self.m.heads.get((o["name"], partition)) for o in self.manifest["assets"][asset]["outputs"]):
            return True
        return "definition" in self.m.partition(asset, partition)

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

    async def stale_keys(
        self, asset: str, partition: str = "", *, after: str | None = None, limit=1000
    ) -> dict:
        """A page of a keyed asset's stale keys in `partition`, and why:
        `keys` is `[{key, reasons}]`, `reasons` the partition's. A key's
        reasons are only what is specific to it, each naming its input —
        `{kind: "input changed", input}` for a key that input owes,
        `{kind: "upstream stale", input}` for one whose upstream key is
        stale along an each chain. A partition-wide reason (its definition
        changed, a stale upstream every key depends on, a keyed output that
        is not `each`, whose keys go stale together) stays on the
        partition's reasons alone: a key it covers lists none. `tracked` is
        false for an unkeyed asset, which has no keys; `next` is the cursor
        for the page after, None at the end."""

        if asset not in self.manifest["assets"]:
            raise KeyError(asset)
        keyed = [o["name"] for o in self.manifest["assets"][asset]["outputs"] if o.get("key") is not None]
        reasons = await self.stale_reasons(asset, partition)
        if not keyed:
            return {"tracked": False, "keys": [], "next": None, "reasons": reasons}
        if not reasons:
            return {"tracked": True, "keys": [], "next": None, "reasons": reasons}
        if self._each_input(asset) is not None:
            stale = await self._each_keys(asset, partition, {})
            page = [k for k in sorted(stale) if after is None or k > after][:limit]
            more = len(page) == limit and page[-1] != max(stale)
            keys = [{"key": k, "reasons": stale[k]} for k in page]
            return {"tracked": True, "keys": keys, "next": page[-1] if more else None, "reasons": reasons}
        if (keyed[0], partition) not in self.m.heads:
            return {"tracked": True, "keys": [], "next": None, "reasons": reasons}
        page = await self.list_keys(keyed[0], partition, after=after, limit=limit)
        keys = [{"key": k, "reasons": []} for k in page["keys"]]
        return {"tracked": True, "keys": keys, "next": page.get("next"), "reasons": reasons}

    async def _each_keys(self, asset: str, partition: str, memo: dict) -> dict[str, list]:
        """An `each=True` partition's stale keys, each with its own reasons
        (`stale_keys`): those its input owes (`_each_own`), and along an each
        chain those whose upstream key is stale; any other stale upstream,
        partition-wide, makes every key it holds stale."""

        if (asset, partition) in memo:
            return memo[(asset, partition)]
        memo[(asset, partition)] = {}
        planner = self.planner()
        try:
            inputs = planner.inputs(asset, partition)
        except planning.UpstreamOnly:
            inputs = []
        if not self.built(asset, partition, planner):
            return {}
        param, spec = self._each_input(asset)
        keys = await self._each_own(asset, partition, planner, inputs)
        taken = Matcher(spec.get("patterns"))
        output = next((o["name"] for o in self.manifest["assets"][asset]["outputs"] if o.get("key")), None)
        for i in inputs:
            for upstream_partition in self._upstreams(planner, i):
                if i.owner is None or not await self.stale_reasons(i.owner, upstream_partition):
                    continue
                if i.param == param and self._each_input(i.owner) is not None:
                    for k in await self._each_keys(i.owner, upstream_partition, memo):
                        if taken(k):
                            keys.setdefault(k, []).append({"kind": UPSTREAM, "input": i.param})
                else:  # a stale upstream every key depends on: partition-wide
                    async for k, _, _ in _entries(self, self.m.indexes.get((output, partition))):
                        keys.setdefault(k, [])
        memo[(asset, partition)] = keys
        return keys

    async def _each_own(self, asset: str, partition: str, planner, inputs) -> dict[str, list]:
        """An `each=True` partition's stale keys by its own input (`_each_keys`):
        the keys it owes — and, its definition changed, a full run due,
        every key upstream has under the patterns and every key it holds,
        for that partition-wide reason alone."""

        param, spec = self._each_input(asset)
        input = next((i for i in inputs if i.param == param), None)
        output = next((o["name"] for o in self.manifest["assets"][asset]["outputs"] if o.get("key")), None)
        if input is None or output is None:
            return {}
        owed = {o.key for o in await self._owed(asset, partition, input, planner, inputs)}
        keys = {k: [{"kind": INPUT, "input": param}] for k in sorted(owed)}
        if self.definition_changed(asset, partition):
            taken = Matcher(spec.get("patterns"))
            async for k, _, _ in _entries(self, self.m.indexes.get((input.output, input.partition))):
                if taken(k):
                    keys.setdefault(k, [])
            for name in (output, f"@{asset}"):
                async for k, _, _ in _entries(self, self.m.indexes.get((name, partition))):
                    keys.setdefault(k, [])
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
