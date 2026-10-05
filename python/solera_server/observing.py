"""The engine's side of the observed set (docs/observed-set.md): each
incremental input's batch planned from what its consumer partition
observed, what a batch's commit records there, and the decode. A keyed
input keeps an observation record; an unkeyed one, the last commit it
read (`{upstream, commit, base}`: an append output's commits run from its
`base`).

A run's task walks an input's owed keys in key order from its **progress**
— its last committed batch's index and end key (`{batch, key}`, `key` None
once the walk is final), run state kept on the task from its commits,
never in the record. Each batch classes its keys at the upstream's head
when it is planned, `H`, which its claim reserves; its commit writes the
range it covered `@ H` — or a point per key it observed outright — and
folds the older layers, the record decoding to the observed set after
every commit."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass

from solera.key_outcomes import StoredOutcome, eligible, minima
from solera.keys.delta import delta, version_of
from solera.keys.layers import LayerIndex, key_bytes, key_str
from solera.patterns import Matcher

from . import observed, owed, planning
from .state import Conflict

WALK = 100  # stored outcomes a retry batch walks at most, for each key it may take


@dataclass(frozen=True)
class Observation:
    """What a consumer observed of one key: the version it processed, and
    the whole and dep versions it processed it under (`None`: at no
    upstream version, a per-key consumer's held key in a full run)."""

    version: object
    context: dict


class Observing:
    def _keyed(self, input: planning.Input) -> bool:
        return input.kind == "incremental" and self.manifest["outputs"][input.output].get("key") is not None

    def _upstream(self, output: str, partition: str) -> tuple[LayerIndex, str]:
        """An upstream's key index as it is, and its life (`""`: none yet)."""

        state = self.m.indexes.get((output, partition))
        index = LayerIndex(self._key_io(), state or self.m.index(output, partition), cache=self._key_cache())
        return index, state.life if state is not None else ""

    def _held(self, asset: str, partition: str) -> list[LayerIndex]:
        """A per-key consumer's own indexes — its keyed outputs' and its
        stored outcomes' — what a held base decodes from."""

        names = [o["name"] for o in self.manifest["assets"][asset]["outputs"] if o.get("key") is not None]
        return [
            LayerIndex(self._key_io(), self.m.indexes[(n, partition)], cache=self._key_cache())
            for n in [*names, f"@{asset}"]
            if (n, partition) in self.m.indexes
        ]

    def _context(self, planner: planning.Planner, inputs) -> dict:
        """The whole and dep versions a batch is processed under, its layers'
        context: each such input's version — a source's own word where it
        gives one, else its head's generation — by partition for a fan-in."""

        out = {}
        for input in inputs:
            if not self._versioned(input):
                continue
            versions = []
            for key, ref in self._input_refs(planner, input).items():
                head = self.m.heads.get((input.output, (ref or {}).get("partition") or "")) or {}
                given = head.get("version") if input.output in self.manifest["sources"] else None
                versions.append([key, given if given is not None else (ref or {}).get("generation")])
            out[input.param] = sorted(versions) if input.fan_in else (versions[0][1] if versions else None)
        return out

    # -- complete (D176): derived and live, never stored -------------------------------

    def _gappy(self, asset: str, partition: str) -> list | None:
        """What deciding whether a partition is complete takes: None if it is
        not — never committed, or an incremental input with no record —
        else each keyed input record with gaps (`observed.gaps`), as
        `(key, stamp, input, rec)`, `stamp` what its answer depends on."""

        record = self.m.partition(asset, partition)
        if "definition" not in record:
            return None
        try:
            inputs = self.planner().inputs(asset, partition)
        except planning.UpstreamOnly:
            return None
        records, out = record.get("observed") or {}, []
        for input in inputs:
            if input.kind != "incremental":
                continue
            rec = records.get(input.param)
            if rec is None:
                return None
            holes = observed.gaps(rec)
            if not holes:
                continue
            state = self.m.indexes.get((input.output, input.partition))
            stamp = (
                json.dumps(holes),
                tuple(sorted(rec["points"])),
                state.head if state is not None else -1,
                state.life if state is not None else "",
                json.dumps(input.spec.get("patterns"), sort_keys=True),
            )
            out.append(((asset, partition, input.param), stamp, input, rec))
        return out

    def _complete_known(self, asset: str, partition: str) -> bool:
        """`complete` as far as it is known now, for what plans and views
        synchronously (fan-ins, skipped missing inputs, statuses): a gap
        not yet checked counts as not complete until the tick checks it."""

        checks = self._gappy(asset, partition)
        if checks is None:
            return False
        return all(self._covers.get(key) == (stamp, True) for key, stamp, _, _ in checks)

    async def complete(self, asset: str, partition: str) -> bool:
        """Whether a partition's content is complete (D176): it has committed,
        and no key present upstream decodes from the empty base in what any
        incremental input observed — keys absent upstream it decodes as
        absent anyway. A record with no gaps is complete outright; one with
        gaps (a first or full run not done, keys= runs alone) asks the
        upstream, a scan of each gap stopping at the first key
        (`owed.uncovered`). Derived and live, never stored."""

        checks = self._gappy(asset, partition)
        if checks is None:
            return False
        for key, stamp, input, rec in checks:
            known = self._covers.get(key)
            if known is None or known[0] != stamp:
                index, _ = self._upstream(input.output, input.partition)
                with self.m.reading(index.state.prefix):
                    covered = not await owed.uncovered(index, rec, Matcher(input.spec.get("patterns")))
                self._covers[key] = known = (stamp, covered)
            if not known[1]:
                return False
        return True

    async def _cover_tick(self) -> None:
        """Bring `_complete_known` up to date: every partition with a gap
        whose answer is not known for the upstream as it is now."""

        gappy = [
            key
            for key, record in self.m.partitions.items()
            if any(observed.gaps(rec) for rec in (record.get("observed") or {}).values())
        ]
        for asset, partition in gappy:
            if asset in self.manifest["assets"]:
                await self.complete(asset, partition)

    def _decodable(self, rec: dict, output: str, partition: str) -> bool:
        """Whether a record still decodes: its layers name the upstream
        index's current life, and Δ from each of their heads is served — at or
        after the index's cut, below which flips are gone."""

        index, life = self._upstream(output, partition)
        if observed.lives(rec) != {life}:
            return False
        state, head = index.state, index.state.head
        layers = [rec["base"], *rec["ranges"]]
        return all(e is None or e >= head or e >= state.cut for e in (x["endpoint"] for x in layers))

    def _full_run(self, task: dict, run: dict, incremental) -> str | None:
        """Why the task's next batch is a full run's, if it is: its run is
        one (on its first batch), the partition's definition is not the one
        its observations were made under, or what an input observed no
        longer holds — an upstream reset, an append output started over,
        history the index no longer serves."""

        record = self.m.partition(task["asset"], task["partition"])
        if not task.get("progress") and (
            run["mode"] == "full" or any((run.get("keys") or {}).get(i.output) == "full" for i in incremental)
        ):
            return "full run"
        if record.get("definition") not in (None, self._definition(task["asset"], run)):
            return "definition changed"
        for input in incremental:
            rec = (record.get("observed") or {}).get(input.param)
            if rec is not None and not self._still(rec, input):
                return "input reset"
        return None

    def _still(self, rec: dict, input: planning.Input) -> bool:
        """Whether what an input observed still holds of its upstream: a
        record still decodes; an unkeyed upstream's commits since its base
        still include the one it read."""

        if self._keyed(input):
            return self._decodable(rec, input.output, input.partition)
        head = self.m.heads.get((input.output, input.partition)) or {}
        base, last = int(head.get("base", 0)), int(head.get("commit_number", -1))
        return not rec.get("reset") and int(rec["base"]) == base and int(rec["commit"]) <= last

    async def _observe(self, task: dict, run: dict, attempt: str | None = None) -> dict:
        """Each keyed incremental input's next batch (docs/observed-set.md, "A
        run"): past the task's progress, the next `batch_size` owed keys — or
        of the keys a `keys=` run names — classed at the upstream's head now.
        A full run compares against an empty record — a per-key consumer's,
        against what it holds — from the first key. An input whose walk the
        task finished gets an empty batch. Each head is reserved on the
        attempt's claim before anything is awaited: a merge planned
        meanwhile keeps it."""

        partition = task["partition"]
        planner = self.planner()
        try:
            inputs = planner.inputs(task["asset"], partition)
        except planning.UpstreamOnly:
            return {}  # `_prepare` refuses it
        incremental = [i for i in inputs if i.kind == "incremental"]
        keyed = [i for i in incremental if self._keyed(i)]
        if not incremental:
            return {}
        context = self._context(planner, inputs)
        full = self._full_run(task, run, incremental) is not None
        records = self.m.partition(task["asset"], partition).get("observed") or {}
        progress = {} if full else task.get("progress") or {}
        upstreams = {i.param: self._upstream(i.output, i.partition) for i in keyed}
        claim = self.m.claimed(attempt) if attempt is not None else None
        if claim is not None:
            claim["reads"] = [(i.output, i.partition, upstreams[i.param][0].state.head) for i in keyed]
        out = {}
        for input in incremental:
            if input not in keyed:
                out[input.param] = self._commits(
                    input, records.get(input.param), progress.get(input.param), full
                )
        for input in keyed:
            spec, mine = input.spec, progress.get(input.param)
            index, life = upstreams[input.param]
            now = owed.Now(index.state.head, spec.get("patterns"), context, life)
            each = spec.get("each") is not None
            rec = records.get(input.param)
            if full or rec is None:
                rec = observed.record(life, held=full and each)
            override = (run.get("keys") or {}).get(input.output)
            named = sorted({str(k) for k in override["keys"]}) if isinstance(override, dict) else None
            held = self._held(task["asset"], partition) if each else None
            if mine is not None and mine["key"] is None:  # walked to the end in this task
                b = owed.Batch([], None, None, True)
            else:
                prefixes = [index.state.prefix, *(h.state.prefix for h in held or ())]
                with self.m.reading(*prefixes):
                    b = await owed.batch(
                        index,
                        rec,
                        now,
                        int(spec["batch_size"]),
                        after=(mine or {}).get("key"),
                        keys=named,
                        held=held,
                    )
            out[input.param] = {
                "now": now,
                "batch": b,
                "full": full,
                "index": 0 if mine is None else int(mine["batch"]) + 1,
                "named": named is not None,
                "done": mine is not None and mine["key"] is None,
            }
            if each:
                out[input.param].update(
                    await self._failed(task, input, index, now, b, full, named is not None)
                )
        return out

    async def _failed(
        self, task: dict, input: planning.Input, index: LayerIndex, now, b, full: bool, named: bool
    ):
        """What a per-key batch needs of its stored outcomes (§9), read here
        so that its worker reads no index: the prior records of its keys —
        none in a full run's first batch, whose records start over — and,
        when retries may be due, the retry batch the stored outcomes make due
        next (`_retry`), for `_each_plan` to choose between them."""

        state = self.m.index(f"@{task['asset']}", task["partition"])
        stored = LayerIndex(self._key_io(), state, cache=self._key_cache())
        record = self.m.partition(task["asset"], task["partition"]).get("outcomes") or {}
        keys = [key_bytes(w.key) for w in b.keys]
        with self.m.reading(state.prefix, index.state.prefix):
            found = await stored.lookup(keys) if keys and not full else {}
            retry = None
            if not full and not named and self._has_retries(record):
                retry = await self._retry(task, input, stored, index, now, record)
        return {"priors": {key_str(k): bytes(p).hex() for k, (_, p) in found.items()}, "retry": retry}

    async def _retry(self, task, input, stored: LayerIndex, index: LayerIndex, now, record: dict) -> dict:
        """The next retry batch: the stored outcomes walked from the retry pass's
        place, taking the ones that are due, `batch_size` at most and
        `WALK` records each at most (§9), each at its version at H — gone
        upstream, removed; left out by the patterns, unmatched. With their
        prior records, where the walk ended (None: the pass is complete),
        and the bounds of the records walked but not taken, which the
        worker folds into the pass's accumulators."""

        limit = int(input.spec["batch_size"])
        current, retry = self._forced_at(record), record.get("retry")
        if retry is not None and (retry["deploy"] != self.m.deploy_number or retry["forced_at"] != current):
            retry = None  # its predicate's inputs moved: the pass starts over (§9)
        after = (retry or {}).get("after")
        cursor = key_bytes(after) if after is not None else None
        forced, clock = dict(record.get("forced") or {}), self.clock()
        walked: dict[str, StoredOutcome] = {}
        due: list[str] = []
        end = None
        while len(due) < limit and len(walked) < WALK * limit:
            rows, nxt = await stored.delta(None, after=cursor, first=limit)
            for k, _, _, _, p in rows:
                key = key_str(k)
                walked[key] = StoredOutcome.decode(bytes(p))
                if eligible(walked[key], clock, self.m.deploy_number, forced):
                    due.append(key)
                end = key
                if len(due) >= limit or len(walked) >= WALK * limit:
                    break
            else:
                if nxt is None:
                    end = None  # the whole index walked: the pass is complete
                    break
                cursor = nxt
                continue
            break
        found = {d.key: d for d in (await delta(index, None, now.head, keys=due)).diffs} if due else {}
        take = Matcher(input.spec.get("patterns"))
        keys = []
        for key in due:
            d = found.get(key)
            if not take(key):
                keys.append([key, "unmatched", None, None, None])
            elif d is None:
                keys.append([key, "removed", None, None, None])
            else:
                keys.append([key, "updated", version_of(d.generation, d.payload), d.generation, None])
        taken = set(due)
        rest = minima(r for k, r in walked.items() if k not in taken)
        return {
            "keys": keys,
            "after": end,
            "rest": list(rest),
            "priors": {k: walked[k].encode().hex() for k in due},
        }

    def _commits(self, input: planning.Input, rec: dict | None, mine: dict | None, full: bool) -> dict:
        """An unkeyed input's next batch: the next `batch_size` commits past
        the task's progress, else past the commit it last read — from its
        upstream's base, in a full run or never read."""

        head = self.m.heads.get((input.output, input.partition)) or {}
        base, last = int(head.get("base", 0)), int(head.get("commit_number", -1))
        if mine is not None:
            lo = last + 1 if mine["key"] is None else int(mine["key"]) + 1
        else:
            lo = base if full or rec is None else int(rec["commit"]) + 1
        size = int(input.spec["batch_size"])
        hi = min(last, lo + size - 1)
        index = 0 if mine is None else int(mine["batch"]) + 1
        return {
            "commits": [lo, hi],
            "head": last,
            "base": base,
            "full": full,
            "index": index,
            "count": index + max(1, -(-(last - lo + 1) // size)),
            "final": hi >= last,
            "done": mine is not None and mine["key"] is None,
        }

    def _commits_pin(self, input: planning.Input, ref: dict, o: dict) -> tuple[dict, dict, bool]:
        """An unkeyed input's pin and plan for its batch of commits."""

        lo, hi = o["commits"]
        batch = {
            "commits": [lo, hi],
            "full": o["full"],
            "more": not o["final"],
            "index": o["index"],
            "count": o["count"],
        }
        plan = {
            "kind": "commits",
            "output": input.output,
            "upstream_partition": input.partition,
            "head": o["head"],
            "base": o["base"],
            "index": o["index"],
            "count": o["count"],
            "after": lo - 1,
            "end": hi,
            "final": o["final"],
            "full": o["full"],
            "classes": {},
        }
        if o["done"]:
            plan["done"] = True
        return {"ref": ref, "batch": batch}, plan, hi < lo

    def _observed_pin(self, input: planning.Input, ref: dict, o: dict) -> tuple[dict, dict, bool]:
        """A keyed input's pin and plan for its batch: each key with its
        class, its version and generation at `H`, and its old observation
        — `[version, same context]` — for the worker to load and class again
        from what a source served."""

        b, now = o["batch"], o["now"]
        keys = [
            [
                w.key,
                w.cls,
                w.new,
                w.generation,
                None if w.old is None else [w.old[0], w.old[1] == now.context],
            ]
            for w in b.keys
        ]
        state = self.m.index(input.output, input.partition)
        size = int(input.spec["batch_size"])
        count = o["index"] + 1 if b.final else max(o["index"] + 2, -(-state.count // size))
        batch = {"keys": keys, "index": o["index"], "count": count, "final": b.final, "full": o["full"]}
        pin = {"ref": ref, "index": state.to_json(), "batch": batch}
        if input.spec.get("patterns") is not None:
            pin["patterns"] = input.spec["patterns"]
        classes = {
            c: sum(1 for w in b.keys if w.cls == c) for c in ("added", "updated", "removed", "unchanged")
        }
        plan = {
            "kind": "observed",
            "output": input.output,
            "upstream_partition": input.partition,
            "head": now.head,
            "life": now.life,
            "patterns": now.patterns,
            "context": now.context,
            "index": o["index"],
            "count": count,
            "after": b.after,
            "end": b.end,
            "final": b.final,
            "full": o["full"],
            "classes": classes,
        }
        if o["named"]:
            plan["named"] = True
        if o["done"]:
            plan["done"] = True
        return pin, plan, not b.keys

    async def _observations(
        self, task: dict, prepared: dict, result: dict, attempt: str | None = None
    ) -> dict:
        """What a batch's commit records of its keyed inputs: each one's
        record operations — a full run's reset first, then its range `@ H`,
        or a point per key it observed outright (a `keys=` list, a retry,
        a drained per-key batch's finished keys), a point per key a source
        served otherwise, and the fold — its progress, and the definition
        its observations were made under. Decided before the commit, which
        installs them as they are; refused if an upstream was reset since
        the batch was planned (the commit check).

        A batch's keys are its pin's, in the attempt's spec (D178): read
        from `attempt`'s spec when its preparation no longer holds them —
        the journal keeps none, a batch of 10,000 keys being ~600 KB."""

        plans = {p: plan for p, plan in (prepared.get("plans") or {}).items() if plan}
        if not plans:
            return {}
        asset, partition = task["asset"], task["partition"]
        records = self.m.partition(asset, partition).get("observed") or {}
        delivered = result.get("delivered") or {}
        inputs = prepared.get("inputs")

        async def pinned(param: str) -> dict:
            nonlocal inputs
            if inputs is None:
                spec = await self.state.attempt_spec(task["run"], attempt)
                if spec is None:
                    raise Conflict(f"attempt {attempt}: its spec is gone")
                inputs = spec["inputs"]
            return {k[0]: k[2] for k in inputs[param]["batch"]["keys"]}

        out = {"observed": {}, "progress": {}}
        for param, plan in plans.items():
            if plan["kind"] == "commits":  # an unkeyed input: the last commit it read
                if not plan.get("done"):
                    out["observed"][param] = {
                        "upstream": [plan["output"], plan["upstream_partition"]],
                        "commit": max(plan["end"], plan["after"]),  # an empty batch: what it had read
                        "base": plan["base"],
                    }
                    out["progress"][param] = {
                        "batch": plan["index"],
                        "key": None if plan["final"] else plan["end"],
                    }
                continue
            index, life = self._upstream(plan["output"], plan["upstream_partition"])
            if life != plan["life"]:
                raise Conflict(f"input {param}: its upstream was reset since this attempt launched")
            if plan.get("done"):
                continue
            each = (self.manifest["assets"][asset]["inputs"].get(param) or {}).get("each") is not None
            now = owed.Now(plan["head"], plan.get("patterns"), plan["context"], plan["life"])
            rec, fresh = records.get(param), None
            if plan["full"] or rec is None:
                rec = fresh = observed.record(plan["life"], held=plan["full"] and each)
                fresh["upstream"] = [plan["output"], plan["upstream_partition"]]
            report = delivered.get(param) or {}
            seen = report.get("observed")
            named = bool(plan.get("named")) or not report.get("whole", True)
            if named and seen is not None:
                keys = seen
            else:  # a retry observes only what it reports
                keys = {} if plan.get("retry") else await pinned(param)
            b = owed.Batch(
                [owed.Owe(k, None, None, v, None) for k, v in sorted(keys.items())],
                plan["after"],
                plan["end"],
                plan["final"],
            )
            held = self._held(asset, partition) if each else None
            with self.m.reading(index.state.prefix, *(h.state.prefix for h in held or ())):
                ops = await owed.commit_ops(
                    index, rec, now, b, None if named else seen, named=named, held=held
                )
            if fresh is not None:
                ops = [{"op": "reset", "record": copy.deepcopy(fresh)}, *ops]
            out["observed"][param] = ops
            if not plan.get("retry"):
                out["progress"][param] = {
                    "batch": plan["index"],
                    "key": None if plan["final"] else plan["end"],
                }
        return out

    async def observed(self, asset: str, partition: str, param: str) -> dict:
        """What a consumer partition has observed of an input: for a keyed one,
        each key it holds as observed (`Observation`) — observed absent and
        never observed are one, not in the dict; for an unkeyed one, the
        upstream commit it last read (None: none)."""

        spec = self.manifest["assets"][asset]["inputs"][param]
        rec = (self.m.partition(asset, partition).get("observed") or {}).get(param)
        if self.manifest["outputs"][spec["output"]].get("key") is None:
            return None if rec is None else int(rec["commit"])
        if rec is None:
            return {}
        output, upstream_partition = rec["upstream"]
        index, life = self._upstream(output, upstream_partition)
        if not self._decodable(rec, output, upstream_partition):
            return {}  # its observations were of an upstream's earlier life: nothing decodes
        held = self._held(asset, partition) if spec.get("each") is not None else []
        endpoints = {
            layer["endpoint"] for layer in [rec["base"], *rec["ranges"]] if layer["endpoint"] is not None
        }
        with self.m.reading(index.state.prefix, *(h.state.prefix for h in held)):
            at = {e: {d.key: d for d in await _all(index, e)} for e in endpoints}
            holds = set()
            for h in held:
                holds |= {d.key for d in await _all(h, None)}
        candidates = {k for found in at.values() for k in found} | set(rec["points"]) | holds

        def version(endpoint, key):
            d = at.get(endpoint, {}).get(key)
            return None if d is None else version_of(d.generation, d.payload)

        out = {}
        for key in sorted(candidates):
            found = observed.decode(rec, key, version, lambda k: k in holds)
            if found is not None:
                out[key] = Observation(*found)
        return out

    @staticmethod
    def _observed_at(rec: dict | None) -> int | None:
        """The oldest commit any part of what an input observed was observed
        at — every key is observed at least as of it — or None: nothing."""

        if rec is None:
            return None
        if "commit" in rec:
            return int(rec["commit"])
        found = [
            layer["endpoint"] for layer in [rec["base"], *rec["ranges"]] if layer["endpoint"] is not None
        ]
        return min(found, default=None)


async def _all(index: LayerIndex, at: int | None) -> list:
    """Every key live after commit `at` (None: the head)."""

    out, after = [], None
    while True:
        page = await delta(index, None, at, after=after)
        out += page.diffs
        if page.cursor is None:
            return out
        after = page.cursor
