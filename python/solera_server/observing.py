"""The engine's side of the observed set (docs/observed-set.md): each keyed
incremental input's batch planned from its consumer partition's
observation record, what a batch's commit records there, and the decode.

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
from dataclasses import dataclass

from solera.keys.delta import delta, version_of
from solera.keys.index import KeyIndex

from . import observed, owed, planning
from .state import Conflict


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

    def _upstream(self, output: str, partition: str) -> tuple[KeyIndex, str]:
        """An upstream's key index as it is, and its life (`""`: none yet)."""

        state = self.m.indexes.get((output, partition))
        index = KeyIndex(
            self._key_io(), None, (state or self.m.index(output, partition)).slice(), self.key_options
        )
        return index, state.life if state is not None else ""

    def _held(self, asset: str, partition: str) -> list[KeyIndex]:
        """A per-key consumer's own indexes — its keyed outputs' and its
        failure records' — what a held base decodes from."""

        names = [o["name"] for o in self.manifest["assets"][asset]["outputs"] if o.get("key") is not None]
        return [
            KeyIndex(self._key_io(), None, self.m.indexes[(n, partition)].slice(), self.key_options)
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

    def _decodable(self, rec: dict, output: str, partition: str) -> bool:
        """Whether a record still decodes: its layers name the upstream
        index's current life, and Δ from each of their heads is served."""

        index, life = self._upstream(output, partition)
        if observed.lives(rec) != {life}:
            return False
        state, head = index.state, index.state.head
        layers = [rec["base"], *rec["ranges"]]
        return all(
            e is None or e >= head or state.covers(e + 1, head) for e in (x["endpoint"] for x in layers)
        )

    def _full_run(self, task: dict, run: dict, keyed) -> str | None:
        """Why the task's next batch is a full run's, if it is: its run is
        one (on its first batch), the partition's definition is not the one
        its observations were made under, or a record no longer decodes —
        an upstream reset, or history the index no longer serves."""

        record = self.m.partition(task["asset"], task["partition"])
        if not task.get("progress") and (
            run["mode"] == "full" or any((run.get("keys") or {}).get(i.output) == "full" for i in keyed)
        ):
            return "full run"
        if record.get("definition") not in (None, self._fingerprint(task["asset"], run)):
            return "definition changed"
        for input in keyed:
            rec = (record.get("observed") or {}).get(input.param)
            if rec is not None and not self._decodable(rec, input.output, input.partition):
                return "input reset"
        return None

    async def _observe(self, task: dict, run: dict) -> dict:
        """Each keyed incremental input's next batch (docs/observed-set.md, "A
        run"): past the task's progress, the next `batch_size` owed keys — or
        of the keys a `keys=` run names — classed at the upstream's head now.
        A full run compares against an empty record — a per-key consumer's,
        against what it holds — from the first key. An input whose walk the
        task finished gets an empty batch."""

        partition = task["partition"]
        planner = self.planner()
        try:
            inputs = planner.inputs(task["asset"], partition)
        except planning.UpstreamOnly:
            return {}  # `_prepare` refuses it
        keyed = [i for i in inputs if self._keyed(i)]
        if not keyed:
            return {}
        context = self._context(planner, inputs)
        full = self._full_run(task, run, keyed) is not None
        records = self.m.partition(task["asset"], partition).get("observed") or {}
        progress = {} if full else task.get("progress") or {}
        out = {}
        for input in keyed:
            spec, mine = input.spec, progress.get(input.param)
            index, life = self._upstream(input.output, input.partition)
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
        return out

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
        pin = {"ref": ref, "index": state.slice().to_json(), "batch": batch}
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
            "keys": {w.key: w.new for w in b.keys},
            "classes": classes,
        }
        if o["named"]:
            plan["named"] = True
        if o["done"]:
            plan["done"] = True
        return pin, plan, not b.keys

    async def _observations(self, task: dict, prepared: dict, result: dict) -> dict:
        """What a batch's commit records of its keyed inputs: each one's
        record operations — a full run's reset first, then its range `@ H`,
        or a point per key it observed outright (a `keys=` list, a retry,
        a drained per-key batch's finished keys), a point per key a source
        served otherwise, and the fold — its progress, and the definition
        its observations were made under. Decided before the commit, which
        installs them as they are; refused if an upstream was reset since
        the batch was planned (the commit check)."""

        plans = {
            p: plan
            for p, plan in (prepared.get("plans") or {}).items()
            if plan and plan["kind"] == "observed"
        }
        if not plans:
            return {}
        asset, partition = task["asset"], task["partition"]
        records = self.m.partition(asset, partition).get("observed") or {}
        delivered = result.get("delivered") or {}
        out = {"observed": {}, "progress": {}, "definition": prepared["definition"]}
        for param, plan in plans.items():
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
            keys = seen if named and seen is not None else plan["keys"]
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
        if self.manifest["outputs"][spec["output"]].get("key") is None:
            position = self.m.position(asset, param, partition)
            return None if position is None else int(position["next"]) - 1
        rec = (self.m.partition(asset, partition).get("observed") or {}).get(param)
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


async def _all(index: KeyIndex, at: int | None) -> list:
    """Every key live after commit `at` (None: the head)."""

    out, after = [], None
    while True:
        page = await delta(index, None, at, after=after)
        out += page.diffs
        if page.cursor is None:
            return out
        after = page.cursor
