"""What a partition was made under and whether its content is complete: the
context a batch is processed under (the versions of its whole and dep
inputs), why a batch starts its partition over, and completeness (D176),
derived from its incremental inputs' edges and never stored."""

from __future__ import annotations

import json

from . import planning
from .model import edge_id
from .tracking import Generation, Redeployed
from .tracking import stale as core_stale


class Observing:
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
        not (never committed, or an incremental input it never read), else
        each incremental input whose edge still holds nothing past some key,
        as `(key, stamp, input, empty)`, `stamp` what its answer depends on."""

        if "definition" not in self.m.partition(asset, partition):
            return None
        try:
            inputs = self.planner().inputs(asset, partition)
        except (planning.UpstreamOnly, ValueError):  # a partition that no longer fits its dimensions
            return None
        out = []
        for input in inputs:
            if input.kind != "incremental":
                continue
            key = (asset, partition, input.param)
            edge = self.m.edge(*key)
            if edge is None or not edge["read"]:
                return None  # never read, or a deploy closed its edge since
            if (log := edge.get("log")) is not None:
                if log["walking"]:
                    return None  # its walk from the log's start goes on
                continue
            if (empty := edge["empty"]) is not None:
                up = self.m.core_output(input.output, input.partition) or {}
                patterns = json.dumps(input.spec.get("patterns"), sort_keys=True)
                stamp = (empty, up.get("life"), up.get("head", 0), patterns, self._walked.get(key, 0))
                out.append((key, stamp, input, empty))
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
        and upstream holds no key its patterns take that an incremental input
        has not processed since it last started over. An edge walked to the
        end is complete outright; one that is not (a first or full run not
        done, key lists alone) asks the upstream, one scan stopping at the
        first such key. Derived and live, never stored."""

        checks, generation = self._gappy(asset, partition), Generation(self.m)
        if checks is None:
            return False
        for key, stamp, input, empty in checks:
            known = self._covers.get(key)
            if known is None or known[0] != stamp:
                try:
                    with generation:
                        generation.check()
                        covered = not await core_stale.taken_after(self.core, input, empty, edge_id(*key))
                    generation.check()
                except Redeployed:  # a deploy while a check awaited: not known, the next tick asks again
                    return False
                self._covers[key] = known = (stamp, covered)
            if not known[1]:
                return False
        return True

    async def _cover_tick(self) -> None:
        """Bring `_complete_known` up to date: every partition with an edge
        not walked to the end, for the upstream as it is now."""

        for asset, partition in sorted(self.m.walking()):
            if asset in self.manifest["assets"]:
                await self.complete(asset, partition)

    def _full_run(self, task: dict, run: dict) -> str | None:
        """Why the task's next batch is a full run's, if it is: its run is
        one (on its first batch), or the partition's definition is not the
        one it was made under."""

        record = self.m.partition(task["asset"], task["partition"])
        if not task.get("progress") and run["mode"] == "full":
            return "full run"
        if record.get("definition") not in (None, self._definition(task["asset"], run)):
            return "definition changed"
        return None
