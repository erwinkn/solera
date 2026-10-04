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

from solera.keys.index import KeyIndex, key_bytes, key_str
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

        record = self.m.partition(asset, partition)
        if int(record.get("input_reset_at", -1)) > int(record.get("caught_up_at", -1)):
            return True
        definition = self.definition_changed(asset, partition)
        seen = record.get("seen")
        each = self._each_input(asset)
        for input in inputs:
            if input.kind == "incremental":
                if await self._input_behind(asset, partition, input, definition):
                    return True
                # A pass its definition made due may also owe keys an input change did: never delivered,
                # or changed past the snapshot — what is stale as if the definition had not changed.
                if definition and each and input.param == each[0]:
                    if await self._each_own(asset, partition, planner, inputs, INPUT):
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

        if input.fan_in:
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
            pages = index.pending_pages(lo, hi, None, BEHIND_PAGE)  # one merge of the commits
            try:
                async for keys, generations, _, _ in pages:
                    behind = any(
                        taken(key_str(k)) and read.get(key_str(k), -1) < g
                        for k, g in zip(keys, generations, strict=True)
                    )
                    if behind:
                        break
            finally:
                await pages.aclose()
        if len(cache) > 10_000:
            cache.clear()
        cache[memo_key] = behind
        return behind

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

    async def _read_ahead_of(self, asset: str, param: str, entries: list, since: int = 0) -> dict[str, int]:
        """One input's read-ahead as key -> the latest generation an entry read."""

        out: dict[str, int] = {}
        for _, run, attempt in entries:
            spec = await self.state.attempt_spec(run, attempt)
            pin = ((spec or {}).get("inputs") or {}).get(param)
            if pin is None or int(spec.get("generation") or 0) < since:  # claimed before `since`
                continue
            generation = int(pin["ref"].get("generation") or 0)
            for key in pin["batch"]["keys"]:
                out[key] = max(out.get(key, -1), generation)
        return out

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
        """An `each=True` partition's stale keys, from its one record (K47): the
        position and the read-ahead entries, whose specs name the keys.

        With a snapshot, a key its patterns take changed past `next` — removed
        too — and read by no entry at or after its change. With a full pass due
        or under way (no position, an upstream reset since it caught up, its
        definition changed, a whole or dep input moved), every key under the
        patterns the pass has not delivered at its current version — read
        ahead, or walked by the pass's own batches — and every key its output
        holds the upstream has not. And along an each chain, the keys whose
        upstream key is stale; any other stale upstream makes every key stale."""

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
        """An `each=True` partition's stale keys by its own inputs (`_each_keys`);
        `only=INPUT`, those an input change made stale: as if its definition
        had not changed — a pass it made due reads as the snapshot it left;
        `only=DEFINITION`, the keys it holds, present upstream, written
        before its asset changed: an output key's generation is its writer's
        claim, and a claim before the change never commits (Positions.tla)."""

        param, spec = self._each_input(asset)
        input = next((i for i in inputs if i.param == param), None)
        output = next((o["name"] for o in self.manifest["assets"][asset]["outputs"] if o.get("key")), None)
        if input is None or output is None:
            return set()
        taken = Matcher(spec.get("patterns"))
        record = self.m.partition(asset, partition)
        position = self.m.position(asset, param, partition)
        seen = record.get("seen")
        shared_moved = seen is not None and any(
            seen.get(i.param) != self._input_version(planner, i) for i in inputs if self._versioned(i)
        )
        under_way = (position or {}).get("pass") or {}
        changed = int(self.m.changed_at.get(asset, 0))
        since_change = int(under_way.get("began") or 0) >= changed  # a pass under the definition as it is
        up_state = self.m.indexes.get((input.output, input.partition))
        if only == DEFINITION:
            old = [
                k async for k, g, _ in _entries(self, self.m.indexes.get((output, partition))) if g < changed
            ]
            return {k for k in old if taken(k)} & await self._holds(up_state, old)
        input_only = only == INPUT
        definition = not input_only and self.definition_changed(asset, partition)
        # An upstream reset drops the position: one with no full pass under way delivered what replaced it.
        reset = int(record.get("input_reset_at", -1)) > int(record.get("caught_up_at", -1)) and (
            position is None or under_way.get("mode") == "full"
        )
        passing = under_way.get("mode") == "full" and (not input_only or "caught_up_at" not in record)
        full = position is None or passing or definition or shared_moved or reset
        # An entry counts if it read every whole and dep input as it is, and, with a full
        # pass due, it was claimed since the pass became due.
        due = [self._committed(planner, i) for i in inputs if self._versioned(i)]
        due += [changed] if full and definition else []
        due += [int(record["input_reset_at"])] if full and reset else []
        since = max(due, default=0)
        read = (
            (await self._read_ahead_of(asset, param, position["ahead"], since))
            if (position or {}).get("ahead")
            else {}
        )
        # A pass begun before its definition changed delivered under the old one.
        walking = under_way.get("mode") == "full" and (not definition or since_change)
        walked_at = under_way.get("at") if walking else None
        walked_gen = int(under_way.get("read_from") or 0)
        keys: set[str] = set()
        head = self.m.heads.get((input.output, input.partition)) or {}
        lo, hi = int((position or {}).get("next") or 0), int(head.get("commit_number", -1))
        if not full and lo <= hi and up_state is not None and not up_state.covers(lo, hi):
            full = True  # the log no longer holds the delta: what was read is all it can tell
        if not full:
            if lo <= hi and up_state is not None:
                with self.m.reading(up_state.prefix):
                    index = KeyIndex(self._key_io(), None, up_state.slice(lo, hi), self.key_options)
                    removed = set()
                    async for page in index.pending_pages(lo, hi, None, BEHIND_PAGE):
                        for k, g, gone in zip(page[0], page[1], page[2], strict=True):
                            key = key_str(k)
                            if not taken(key) or read.get(key, -1) >= g:
                                continue
                            if walked_at is not None and key <= walked_at and g <= walked_gen:
                                continue  # delivered by the pass under way
                            (removed if gone else keys).add(key)
                # A key removed upstream is stale where its output holds it.
                keys |= await self._holds(self.m.indexes.get((output, partition)), sorted(removed))
            if "reconcile" in (position or {}):  # what its cleanup will remove: keys gone upstream
                upstream = {k async for k, _, _ in _entries(self, up_state) if taken(k)}
                async for key, _, _ in _entries(self, self.m.indexes.get((output, partition))):
                    if key not in upstream:
                        keys.add(key)
        else:
            upstream = set()
            async for key, g, _ in _entries(self, up_state):
                if not taken(key):
                    continue
                upstream.add(key)
                if read.get(key, -1) >= g or (walked_at is not None and key <= walked_at and g <= walked_gen):
                    continue
                keys.add(key)
            async for key, _, _ in _entries(self, self.m.indexes.get((output, partition))):
                if key not in upstream:
                    keys.add(key)
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
