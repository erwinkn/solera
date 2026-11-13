"""The console's read models (§8, §10): per-asset rollups, a per-key asset's
stored outcomes and `explain`, every partition of every input, and what holds
partitions back. Reads only: they record nothing, and never scan a run's tasks.
A mixin of the engine, as `Attempts` and `Sensors` are."""

from __future__ import annotations

from collections import Counter

from . import planning
from .model import BAD_OUTCOME

SCAN = 100  # an outcome listing reads at most this many entries per key it returns
PAGE = 1000  # keys whose outcomes are derived at a time


class Views:
    # -- partitions and assets (§7, §8) -------------------------------------------------

    async def partition_statuses(self, assets: list[str], *, every: bool = True) -> dict[str, list[dict]]:
        """Each partition of each asset, by status: `materialized` (its head is),
        `stale` (materialized, but due a rebuild: its `reasons` say why — an
        input changed since it read it, an upstream it reads is itself stale,
        or its definition changed; `staleness.py`), `running` (a task is pending),
        `failed` (its last outcome failed, was canceled or blocked, and nothing
        is pending: failing, not stale; its `reasons` too if stale), `missing`,
        or `removed` (no longer a current key). A job has no head: it is
        materialized when its last outcome succeeded. `every` lists every
        current partition, enumerated — refused past `MAX_PARTITIONS`; else
        only the partitions with a record (a head, an outcome or a pending
        task), the domain never enumerated. From heads, partition records and
        the pending index, one pass over each — never a task scan."""

        heads: dict[str, dict] = {}
        for (output, partition), head in self.m.heads.items():
            heads.setdefault(output, {})[partition] = head
        running: dict[str, set] = {}
        for (asset, partition), ids in self.m.pending.items():
            if ids:
                running.setdefault(asset, set()).add(partition)
        planner, out, memo = self.planner(), {}, {}
        for asset in assets:
            outputs = self.manifest["assets"][asset]["outputs"]
            scoped: dict[str, dict] = {}
            for output in outputs:  # a partition's head: its last declared output's, among those it has
                scoped.update(heads.get(output["name"]) or {})
            records = self.m.partitions.of(asset)
            recorded = {s: r["last"] for s, r in records.items() if "last" in r}
            pending = running.get(asset) or set()
            partitions = set(scoped) | set(recorded) | pending
            if every:
                listed = set(planner.partitions(asset, "all"))
                partitions, current = partitions | listed, listed.__contains__
            else:
                current = planning.membership(planner.dims(asset), planner.time, planner.dynamic_partitions)
            rows = out[asset] = []
            for partition in sorted(partitions):
                head, record = scoped.get(partition), recorded.get(partition)
                last = (record or {}).get("outcome")
                done = planner.materialized(asset, partition)
                reasons = await self.stale_reasons(asset, partition, memo) if current(partition) else []
                status = (
                    "removed"
                    if not current(partition)
                    else "running"
                    if partition in pending
                    else "failed"
                    if last in BAD_OUTCOME
                    else "stale"
                    if reasons
                    else "materialized"
                    if done
                    else "missing"
                )
                view = self.outcome_view(record) if record else {}
                row = {
                    "partition": partition,
                    "status": status,
                    "last_outcome": view.get("last_outcome"),
                    "last_attempt": view.get("last_attempt"),
                }
                if reasons:
                    row["reasons"] = reasons
                rows.append(row)
        return out

    async def asset_statuses(self) -> dict[str, dict]:
        """One rollup per asset, for the console's graph and list: its partitions
        by status — `total` counts the current ones, `removed` those past
        them — its newest outcome, a per-key asset's failing keys by class
        (null for any other), its held partitions, the partitions of its outputs a
        dead writer left owing a repair, and when an output last changed. Counted
        from the dimensions and the partitions with a record, so a domain too big
        to list still rolls up: `missing` is every current partition not
        otherwise counted."""

        names = list(self.manifest["assets"])
        statuses = await self.partition_statuses(names, every=False)
        owner = {o["name"]: a for a in names for o in self.manifest["assets"][a]["outputs"]}
        planner, out = self.planner(), {}
        for name in names:
            counts = Counter(row["status"] for row in statuses[name])
            total = planning.size(planner.dims(name), planner.time, planner.dynamic_partitions)
            missing = total - sum(counts[s] for s in ("materialized", "stale", "failed", "running"))
            out[name] = {
                "partitions": {
                    "total": total,
                    "missing": missing,
                    **{s: counts[s] for s in ("materialized", "stale", "failed", "running", "removed")},
                },
                "partitioned": bool(planner.dims(name)),
                "stale": counts["stale"] > 0,  # any of its partitions (K38, K46)
                "last": None,
                "stored_counts": {} if self._each_input(name) else None,
                "repairs": 0,
                "updated_at": None,
            }
        for (output, _), head in self.m.heads.items():
            if (entry := out.get(owner.get(output))) is not None:
                entry["updated_at"] = max(entry["updated_at"] or head["at"], head["at"])
        for (asset, partition), state in self.m.partitions.items():
            entry, record = out.get(asset), state.get("last")
            if entry is None or record is None:
                continue
            if entry["last"] is None or record["at"] > entry["last"]["at"]:
                view = self.outcome_view(record)
                entry["last"] = {
                    "partition": partition,
                    "outcome": view["last_outcome"],
                    "at": view["at"],
                    "attempt": view["last_attempt"],
                }
        for (asset, _), state in self.m.partitions.items():
            if (counts := (out.get(asset) or {}).get("stored_counts")) is not None:
                for name, n in ((state.get("outcomes") or {}).get("counts") or {}).items():
                    counts[name] = counts.get(name, 0) + n
        for output, _ in self.m.repairs:
            if (entry := out.get(owner.get(output))) is not None:
                entry["repairs"] += 1
        return out

    # -- key outcomes (docs/per-key-processing.md §9) -------------------------------------

    def _each_input(self, asset: str) -> tuple[str, dict] | None:
        inputs = self.manifest["assets"][asset]["inputs"]
        return next(((p, e) for p, e in inputs.items() if e.get("each") is not None), None)

    # -- inputs (§6; per-key-processing.md §11) -----------------------------------------------

    # -- explain (per-key-processing.md §10) -------------------------------------------------

    # -- what an operator may clear (docs/lifecycle.md §9.6, §9.8) -----------------------------------------

    def cleanups_view(self) -> list[dict]:
        """Output partitions whose cleanups have stuck entries, for an operator
        to clear (docs/lifecycle.md §9.8); and every output life removed or
        moved away whose leftovers await a cleanup task, with when each is
        due (K25)."""

        stuck = {k for k, entries in self.m.cleanups.items() if any(e.get("stuck") for e in entries)}
        stuck |= {k for k, queue in self.m.cleaning.items() if any(d.get("stuck") for d in queue)}
        partitions = [self.partition_cleanups(output, partition) for output, partition in sorted(stuck)]
        return partitions + self.retired_cleanups()
