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

import json
from dataclasses import dataclass

from solera.keys.layers import LayerIndex

from . import observed, planning
from .tracking import Generation, Redeployed
from .tracking import stale as core_stale

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
        not — never committed, or an incremental input with no record or
        (on the core) an unkeyed one's walk from empty not done —
        else each keyed input record with gaps (`observed.gaps`), as
        `(key, stamp, input, rec)`, `stamp` what its answer depends on."""

        record = self.m.partition(asset, partition)
        if "definition" not in record:
            return None
        try:
            inputs = self.planner().inputs(asset, partition)
        except (planning.UpstreamOnly, ValueError):  # a partition that no longer fits its dimensions
            return None
        records, out = record.get("observed") or {}, []
        for input in inputs:
            if input.kind != "incremental":
                continue
            if self._keyed(input):  # the new path: the edge's empty part
                edge = self.m.core_edges.get((asset, partition, input.param))
                read = self.m.core_read.get((asset, partition, input.param))
                if edge is None or read != self.m.core_outputs.get((input.output, input.partition)):
                    return None  # never read, or what it committed is of a life of its upstream that is gone
                empty = self.m.core_empty.get((asset, partition, input.param))
                if empty is not None:
                    up = self.m.core_output(input.output, input.partition) or {}
                    patterns = json.dumps(input.spec.get("patterns"), sort_keys=True)
                    walked = self._walked.get((asset, partition, input.param), 0)
                    stamp = (empty, edge[1], up.get("head", 0), patterns, walked)
                    out.append(((asset, partition, input.param), stamp, input, {"core": empty}))
                continue
            rec = records.get(input.param)
            if rec is None or (asset, partition, input.param) in self.m.core_empty:
                return None  # never read, or a walk from empty not done (an unkeyed input's, on the core)
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

        checks, generation = self._gappy(asset, partition), Generation(self.m)
        if checks is None:
            return False
        for key, stamp, input, rec in checks:
            known = self._covers.get(key)
            if "core" in rec and (known is None or known[0] != stamp):
                try:
                    with generation:
                        generation.check()
                        edge = self.m.core_edges[(asset, partition, input.param)][0]
                        covered = not await core_stale.taken_after(self.core, input, rec["core"], edge)
                    generation.check()
                except Redeployed:  # a deploy while a check awaited: not known, the next tick asks again
                    return False
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
        gappy += sorted({(asset, partition) for asset, partition, _ in self.m.core_empty})
        for asset, partition in gappy:
            if asset in self.manifest["assets"]:
                await self.complete(asset, partition)

    def _full_run(self, task: dict, run: dict, incremental) -> str | None:
        """Why the task's next batch is a full run's, if it is: its run is
        one (on its first batch), the partition's definition is not the one
        its observations were made under, or what an input observed no
        longer holds — an upstream reset, an append output started over,
        history the index no longer serves."""

        record = self.m.partition(task["asset"], task["partition"])
        if not task.get("progress") and run["mode"] == "full":
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

        head = self.m.heads.get((input.output, input.partition)) or {}
        base, last = int(head.get("base", 0)), int(head.get("commit_number", -1))
        return not rec.get("reset") and int(rec["base"]) == base and int(rec["commit"]) <= last

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
            "reset": o["full"],
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
        return out
