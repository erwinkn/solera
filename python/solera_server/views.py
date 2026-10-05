"""The console's read models (§8, §10): per-asset rollups, a per-key asset's
failed keys and `explain`, every partition of every input, and what holds
partitions back. Reads only: they record nothing, and never scan a run's tasks.
A mixin of the engine, as `Attempts` and `Sensors` are."""

from __future__ import annotations

import asyncio
import json
from collections import Counter

from solera.failed_keys import GONE, NAMES, OK, Record, eligible
from solera.keys.index import KeyIndex, key_bytes, key_str
from solera.patterns import Matcher

from . import observed, owed, planning
from .model import BAD_OUTCOME, REPAIR_RUNS

SCAN = 100  # a failure listing reads at most this many entries per key it returns
PAGE = 1000  # entries read from a failed keys at a time


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
                "failures": {} if self._each_input(name) else None,
                "repairs": 0,
                "repairs_stuck": 0,  # the repair clock gave up: waiting for a run of a user's or a trigger's
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
            if (failures := (out.get(asset) or {}).get("failures")) is not None:
                for name, n in ((state.get("failures") or {}).get("counts") or {}).items():
                    failures[name] = failures.get(name, 0) + n
        for output, partition in self.m.repairs:
            if (entry := out.get(owner.get(output))) is not None:
                entry["repairs"] += 1
                entry["repairs_stuck"] += self.m.repair_runs(output, partition)[0] >= REPAIR_RUNS
        return out

    # -- failing keys (docs/per-key-processing.md §9) -------------------------------------

    def _each_input(self, asset: str) -> tuple[str, dict] | None:
        inputs = self.manifest["assets"][asset]["inputs"]
        return next(((p, e) for p, e in inputs.items() if e.get("each") is not None), None)

    def _failure_view(self, asset: str, partition: str, key: str, record: Record, forced: dict) -> dict:
        """A failing key's record, its times null where unset; `eligible`:
        whether a retry pass would take it now."""

        return {
            "partition": partition,
            "key": key,
            "outcome": record.name,
            "tries": record.tries,
            "since": record.since,
            "last": record.last,
            "next_at": record.next_at or None,
            "until": record.until or None,
            "generation": record.upstream,  # the upstream key's that failed
            "message": record.message,
            "eligible": eligible(record, self.clock(), self.m.deploy_number, forced),
        }

    async def _failure_entries(self, asset: str, partitions: list[str], start: list | None):
        """`(partition, key, record)` of an asset's failed keys in partition and key
        order, from just past `start` (`[partition, key]`)."""

        for partition in partitions:
            if start is not None and partition < start[0]:
                continue
            state = self.m.indexes.get((f"@{asset}", partition))
            if state is None:
                continue
            cursor = key_bytes(start[1]) if start is not None and partition == start[0] else None
            with self.m.reading(state.prefix):  # its files outlive merges until the walk ends (aclose)
                index = KeyIndex(self._key_io(), None, state.slice(), self.key_options)
                while True:
                    keys, _, payloads, cursor = await index.page(cursor, PAGE)
                    for k, p in zip(keys, payloads, strict=True):
                        yield partition, key_str(k), Record.decode(p)
                    if cursor is None:
                        break

    async def key_failures(
        self,
        asset: str,
        partition: str | None = None,
        *,
        outcomes=(),
        after: str | None = None,
        limit: int = 100,
    ) -> dict:
        """A per-key asset's failed keys (§9): the record of every partition
        with one, or of `partition`, and a page of its failing keys in partition and
        key order, of the classes in `outcomes` if any. `after` is the
        previous page's `next`: `[partition, key]` as JSON. A page reads at most
        `SCAN` entries per key it may return, so a rare class can come back
        as a short page with a `next`."""

        if self._each_input(asset) is None:
            raise ValueError(f"{asset} has no per-key input: it keeps no failing keys")
        unknown = set(outcomes) - set(NAMES.values())
        if unknown:
            raise ValueError(f"unknown key classes: {sorted(unknown)}")
        records = {
            s: r["failures"]
            for s, r in self.m.partitions.of(asset).items()
            if "failures" in r and partition in (None, s)
        }
        start = json.loads(after) if after else None
        if start is not None and not (isinstance(start, list) and [type(s) for s in start] == [str, str]):
            raise ValueError("after= takes a page's `next`")
        keys, nxt, read, last = [], None, 0, None
        entries = self._failure_entries(asset, sorted(records), start)
        try:
            async for s, key, record in entries:
                if read == SCAN * limit:  # read enough: the next page resumes after the last entry read
                    nxt = last
                    break
                read, last = read + 1, [s, key]
                if outcomes and record.name not in outcomes:
                    continue
                if len(keys) == limit:  # another one: the page is full
                    nxt = [keys[-1]["partition"], keys[-1]["key"]]
                    break
                keys.append(self._failure_view(asset, s, key, record, records[s].get("forced") or {}))
        finally:
            await entries.aclose()
        return {
            "asset": asset,
            "partitions": [
                {
                    "partition": s,
                    "counts": r.get("counts") or {},
                    "due": r.get("due"),
                    "deploy_min": r.get("deploy_min"),
                    "passes": r.get("passes") or 0,
                    "retry": r.get("retry"),
                    "forced": r.get("forced") or {},
                    "last": r.get("last"),
                    "has_retries": self._has_retries(r),
                }
                for s, r in sorted(records.items())
            ],
            "deploy": self.m.deploy_number,
            "now": self.clock(),
            "keys": keys,
            "next": json.dumps(nxt) if nxt else None,
        }

    # -- inputs (§6; per-key-processing.md §11) -----------------------------------------------

    async def _input_observed(self, asset: str, param: str, partition: str) -> dict | None:
        """One partition of an Incremental input, as what it observed says
        (docs/observed-set.md): what it owes — a keyed input's keys by
        class, an unkeyed one's commits; why a full run is due, if one is;
        and `observed_at`, the oldest commit anything it observed was
        observed at — every key is observed at least as of it (None:
        nothing observed)."""

        planner = self.planner()
        try:
            inputs = planner.inputs(asset, partition)
        except (planning.UpstreamOnly, KeyError, ValueError):
            return None
        input = next((i for i in inputs if i.param == param), None)
        if input is None:
            return None
        rec = (self.m.partition(asset, partition).get("observed") or {}).get(param)
        due = None
        if rec is not None and not self._still(rec, input):
            due = "input reset"
        elif self.definition_changed(asset, partition) and self.built(asset, partition, planner):
            due = "definition changed"
        if self._keyed(input):
            owes = await self._owed(asset, partition, input, planner, inputs)
            owed = {c: sum(1 for o in owes if o.cls == c) for c in ("added", "updated", "removed")}
        else:
            head = self.m.heads.get((input.output, input.partition)) or {}
            last, base = int(head.get("commit_number", -1)), int(head.get("base", 0))
            read = int(rec["commit"]) if rec is not None and due is None else base - 1
            owed = {"commits": max(0, last - read)}
        return {"owed": owed, "full_run_due": due, "observed_at": self._observed_at(rec)}

    def _upstream_partition(self, asset: str, param: str, partition: str) -> str | None:
        """The upstream partition an input of `partition` reads."""

        try:
            return next(e.partition for e in self.planner().inputs(asset, partition) if e.param == param)
        except (ValueError, KeyError, StopIteration, planning.UpstreamOnly):
            return None

    async def asset_inputs(self, asset: str) -> dict:
        """Every input of an asset, deps included (kind `dep`); for an
        Incremental or Each input, what each partition observed and owes —
        the asset's current partitions and every partition that observed
        anything (`_input_observed`)."""

        info = self.manifest["assets"][asset]
        every = set(info.get("deps_all_partitions") or ())
        inputs = [
            *info["inputs"].items(),
            *(
                (d, {"kind": "dep", "output": d, **({"all_partitions": True} if d in every else {})})
                for d in info["deps"]
            ),
        ]
        current = set(self.planner().partitions(asset, "all"))
        marked: dict[str, set] = {}
        for partition, record in self.m.partitions.of(asset).items():
            for param in record.get("observed") or ():
                marked.setdefault(param, set()).add(partition)
        out = []
        for param, input in inputs:
            output = input["output"]
            partitions = []
            if input["kind"] == "incremental":
                for partition in sorted(current | marked.get(param, set())):
                    partitions.append(
                        {
                            "partition": partition,
                            "upstream_partition": self._upstream_partition(asset, param, partition),
                            "observed": await self._input_observed(asset, param, partition),
                        }
                    )
            out.append(
                {
                    "param": param,
                    "kind": "each"
                    if input.get("each") is not None
                    else "all_partitions"
                    if input["kind"] == "in" and input.get("all_partitions")
                    else input["kind"],
                    "output": output,
                    "upstream_asset": self.manifest["outputs"][output].get("asset"),
                    "source": output in self.manifest["sources"],
                    "batch_size": input.get("batch_size"),
                    "concurrency": (input.get("each") or {}).get("concurrency"),
                    "patterns": input.get("patterns"),
                    "partitions": partitions,
                }
            )
        return {"asset": asset, "inputs": out}

    # -- explain (per-key-processing.md §10) -------------------------------------------------

    async def _lookup(self, output: str, partition: str, key: str) -> tuple[int, bytes | None] | None:
        """A key's live entry in an index, exactly — `(generation, payload)`;
        `None` if it holds none."""

        state = self.m.indexes.get((output, partition))
        if state is None:
            return None
        with self.m.reading(state.prefix):
            found = await KeyIndex(self._key_io(), None, state.slice(), self.key_options).lookup(
                [key_bytes(key)]
            )
        return found.get(key_bytes(key))

    async def _newest_outcome(self, asset: str, partition: str, key: str, outcomes=None) -> dict | None:
        found = await self.history.key_outcomes(
            asset, partition=partition, key=key, outcomes=outcomes, limit=1
        )
        return next(iter(found["outcomes"]), None)

    async def explain(self, asset: str, key: str, partition: str = "", input: str | None = None) -> dict:
        """Why `key` is, or is not, in an asset's output (§10), through one
        Incremental input: `input`, else its per-key input, else its one keyed
        Incremental input. The verdict is the first of these that holds:

        - `not_matched`: no `include` pattern of the input matches it;
        - `excluded`: an `exclude` pattern does (`patterns.excluded_by`);
        - `failing`: the asset's failed keys holds it (`failure`);
        - `removed`: the upstream no longer holds it, and it was processed
          once or an output still does (its removal may be undelivered);
        - `absent`: the upstream does not hold it, and nothing shows it did;
        - `ok`: processed at the upstream key's current generation — its
          newest `ok`, `removed` or `unmatched` row is an `ok` at it, or the
          input is caught up (a row expires with its run, and a plain
          Incremental input records none);
        - `pending`: the upstream holds a write of it the input has not
          delivered yet.

        The patterns are the input's as declared: new patterns are compared
        at once, with no transition to run (`pending` is None)."""

        info = self.manifest["assets"][asset]
        keyed = {
            p: e
            for p, e in info["inputs"].items()
            if e["kind"] == "incremental" and self.manifest["outputs"][e["output"]].get("key") is not None
        }
        if input is None:
            each = self._each_input(asset)
            if each is None and len(keyed) != 1:
                raise ValueError(
                    f"{asset} has {len(keyed) or 'no'} keyed Incremental inputs: name one with input="
                )
            input = each[0] if each is not None else next(iter(keyed))
        if input not in keyed:
            raise ValueError(f"{asset} has no keyed Incremental input {input!r}")
        spec, is_each = keyed[input], keyed[input].get("each") is not None
        if partition not in self.planner().partitions(asset, [partition]) and input not in (
            self.m.partition(asset, partition).get("observed") or {}
        ):
            raise KeyError(f"{asset}/{partition}")
        output = spec["output"]
        where = {"upstream_partition": self._upstream_partition(asset, input, partition)}
        if where["upstream_partition"] is None:
            raise ValueError(f"{asset}: no upstream partition for {partition!r}")
        outputs = [o["name"] for o in info["outputs"] if o.get("key") is not None]
        indexes = [(output, where["upstream_partition"]), *((name, partition) for name in outputs)]
        if is_each:
            indexes.append((f"@{asset}", partition))
        upstream, *held = await asyncio.gather(*(self._lookup(o, s, key) for o, s in indexes))
        failing = held.pop() if is_each else None
        last = last_ok = kept = None
        if is_each:
            last = await self._newest_outcome(asset, partition, key)
            # The outputs keep what the newest `ok` delivered, unless a `removed` or `unmatched` came since.
            settled = (
                last
                if last is None or last["outcome"] in GONE
                else await self._newest_outcome(asset, partition, key, list(GONE))
            )
            if settled is not None and settled["outcome"] == OK:
                kept = last_ok = settled
            elif settled is not None:
                last_ok = await self._newest_outcome(asset, partition, key, [OK])
        served = spec.get("patterns")
        matcher = Matcher(served)
        included, excluded_by = matcher.included(key), matcher.excluded_by(key)
        generation = upstream[0] if upstream is not None else None
        failure = None
        if failing is not None:
            forced = (self.m.partition(asset, partition).get("failures") or {}).get("forced") or {}
            failure = self._failure_view(asset, partition, key, Record.decode(failing[1]), forced)
        present = {name: v is not None for name, v in zip(outputs, held, strict=True)}
        if not included:
            verdict = "not_matched"
        elif excluded_by is not None:
            verdict = "excluded"
        elif failure is not None:
            verdict = "failing"
        elif upstream is None:
            verdict = "removed" if last is not None or any(present.values()) else "absent"
        elif (kept is not None and kept["generation"] == generation) or not await self._owes_key(
            asset, partition, input, key
        ):
            verdict = "ok"
        else:
            verdict = "pending"
        return {
            "asset": asset,
            "partition": partition,
            "key": key,
            "input": input,
            "upstream": output,
            "upstream_asset": self.manifest["outputs"][output].get("asset"),
            "upstream_partition": where["upstream_partition"],
            "upstream_generation": generation,
            "outputs": {
                name: {"present": v is not None, "generation": None if v is None else v[0]}
                for name, v in zip(outputs, held, strict=True)
            },
            "patterns": {
                "spec": served,
                "included": included,
                "excluded_by": excluded_by,
                "pending": None,
            },
            "failure": failure,
            "last": last,
            "last_ok": last_ok,
            "verdict": verdict,
        }

    async def _owes_key(self, asset: str, partition: str, param: str, key: str) -> bool:
        """Whether a keyed input owes `key`: its record decodes it otherwise
        than upstream has it now, under the current patterns and context."""

        planner = self.planner()
        inputs = planner.inputs(asset, partition)
        input = next(i for i in inputs if i.param == param)
        rec = (self.m.partition(asset, partition).get("observed") or {}).get(param)
        index, life = self._upstream(input.output, input.partition)
        if rec is None or observed.lives(rec) != {life}:
            return True
        now = owed.Now(index.state.head, input.spec.get("patterns"), self._context(planner, inputs), life)
        held = self._held(asset, partition) if input.spec.get("each") is not None else None
        with self.m.reading(index.state.prefix, *(h.state.prefix for h in held or ())):
            b = await owed.batch(index, rec, now, 1, keys=[key], held=held)
        return any(o.cls not in (None, "unchanged") for o in b.keys)

    # -- what an operator may clear (docs/lifecycle.md §9.6, §9.8) -----------------------------------------

    def repairs_view(self) -> list[dict]:
        """Output partitions a writer that died left owing a repair, with each
        intent's files, for the next attempt (docs/lifecycle.md §9.6): how many
        runs the repair clock gave it, and whether it gave up (`stuck`)."""

        return [
            {
                "output": output,
                "partition": partition,
                "repair_runs": self.m.repair_runs(output, partition)[0],
                "stuck": self.m.repair_runs(output, partition)[0] >= REPAIR_RUNS,
                "intents": [
                    {
                        "run": i.get("run"),
                        "attempt": i.get("attempt"),
                        "files": [f["name"] for f in i.get("files") or ()],
                    }
                    for i in intents
                ],
            }
            for (output, partition), intents in sorted(self.m.repairs.items())
        ]

    def cleanups_view(self) -> list[dict]:
        """Output partitions whose cleanups have stuck entries, for an operator
        to clear (docs/lifecycle.md §9.8); and every output life removed or
        moved away whose leftovers await a cleanup task, with when each is
        due (K25)."""

        partitions = [
            self.partition_cleanups(output, partition)
            for (output, partition), entries in sorted(self.m.cleanups.items())
            if any(e.get("stuck") for e in entries)
        ]
        return partitions + self.retired_cleanups()
