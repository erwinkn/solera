"""The console's read models (§8, §10): per-asset rollups, an `Each` asset's
failed keys and `explain`, every partition of every edge, and what holds
partitions back. Reads only: they record nothing, and never scan a run's tasks.
A mixin of the engine, as `Attempts` and `Sensors` are."""

from __future__ import annotations

import asyncio
import json
from collections import Counter

from solera.failed_keys import GONE, NAMES, OK, Record, eligible
from solera.keys.index import KeyIndex, key_bytes, key_str
from solera.patterns import Matcher

from . import planning
from .model import BAD_OUTCOME

SCAN = 100  # a failure listing reads at most this many entries per key it returns
PAGE = 1000  # entries read from a failed keys at a time


class Views:
    # -- partitions and assets (§7, §8) -------------------------------------------------

    async def partition_statuses(self, assets: list[str], *, every: bool = True) -> dict[str, list[dict]]:
        """Each partition of each asset, by status: `complete` (its head is),
        `running` (a task is pending), `failed` (its last outcome failed, was
        canceled or blocked), `missing`, or `retired` (no longer a current
        key). A job has no head: it is complete when its last outcome
        succeeded. `every` lists every current partition, enumerated — refused
        past `MAX_SCOPES`; else only the partitions with a record (a head, an
        outcome or a pending task), the domain never enumerated. From heads,
        partition outcomes and the pending index, one pass over each — never a
        task scan."""

        heads: dict[str, dict] = {}
        for (output, partition), head in self.m.heads.items():
            heads.setdefault(output, {})[partition] = head
        running: dict[str, set] = {}
        for (asset, partition), ids in self.m.pending.items():
            if ids:
                running.setdefault(asset, set()).add(partition)
        planner, out = self.planner(), {}
        for asset in assets:
            outputs = self.manifest["assets"][asset]["outputs"]
            scoped: dict[str, dict] = {}
            for output in outputs:  # a partition's head: its last declared output's, among those it has
                scoped.update(heads.get(output["name"]) or {})
            recorded = {s: r["last"] for s, r in self.m.partitions.of(asset).items() if "last" in r}
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
                status = (
                    "removed"
                    if not current(partition)
                    else "materialized"
                    if done
                    else "running"
                    if partition in pending
                    else "failed"
                    if last in BAD_OUTCOME
                    else "missing"
                )
                view = self.outcome_view(record) if record else {}
                rows.append(
                    {
                        "partition": partition,
                        "status": status,
                        "last_outcome": view.get("last_outcome"),
                        "last_attempt": view.get("last_attempt"),
                    }
                )
        return out

    async def asset_statuses(self) -> dict[str, dict]:
        """One rollup per asset, for the console's graph and list: its partitions
        by status — `total` counts the current ones, `removed` those past
        them — its newest outcome, an `Each` asset's failing keys by class
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
            missing = total - counts["materialized"] - counts["failed"] - counts["running"]
            out[name] = {
                "partitions": {
                    "total": total,
                    "missing": missing,
                    **{s: counts[s] for s in ("materialized", "failed", "running", "removed")},
                },
                "partitioned": bool(planner.dims(name)),
                "last": None,
                "failures": {} if self._each_input(name) else None,
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
            if (failures := (out.get(asset) or {}).get("failures")) is not None:
                for name, n in ((state.get("failures") or {}).get("counts") or {}).items():
                    failures[name] = failures.get(name, 0) + n
        for output, _ in self.m.repairs:
            if (entry := out.get(owner.get(output))) is not None:
                entry["repairs"] += 1
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
            with self.m.reading(state.prefix):  # its files outlive compaction until the walk ends (aclose)
                index = KeyIndex(self._key_io(), None, state.pinned(), self.key_options)
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
        """An `Each` asset's failed keys (§9): the record of every partition
        with one, or of `partition`, and a page of its failing keys in partition and
        key order, of the classes in `outcomes` if any. `after` is the
        previous page's `next`: `[partition, key]` as JSON. A page reads at most
        `SCAN` entries per key it may return, so a rare class can come back
        as a short page with a `next`."""

        if self._each_input(asset) is None:
            raise ValueError(f"{asset} has no Each edge: it keeps no failing keys")
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

    # -- edges (§6; per-key-processing.md §11) -----------------------------------------------

    def _input_partition(self, asset: str, param: str, input: dict, partition: str) -> dict:
        """One partition of an Incremental edge: its bookmark and how far it is
        behind the upstream head.

        `bookmark.next` is the first upstream batch the edge has not yet
        delivered (`pass`, a pass under way, keeps its boundary and
        position until its last page — see `pass`). So `lag` = head
        batch + 1 − `next`: the upstream batches committed and not yet
        delivered in full — counted from the head's `base` for an unkeyed
        upstream, which starts over there; every batch without a bookmark.
        A change of fingerprint (the asset's version, its run config, a
        pinned input) resets the edge at its next run: not shown here.

        `state`: `never` (no bookmark), `pattern change` (a pattern change,
        per-key §11), `full` (a full pass under way), `reconcile` (the
        cleanup after a full Each pass), `paging` (a delta delivered over
        several attempts), `behind` (lag), else `caught_up`."""

        wm = self.m.bookmark(asset, param, partition)
        upstream_partition = (wm or {}).get("upstream_partition")
        if upstream_partition is None:
            try:
                upstream_partition = next(
                    e.partition for e in self.planner().inputs(asset, partition) if e.param == param
                )
            except (ValueError, KeyError, StopIteration):
                upstream_partition = None
        head = (
            self.m.heads.get((input["output"], upstream_partition))
            if upstream_partition is not None
            else None
        )
        head_commit = int(head.get("commit_number", -1)) if head is not None else None
        lag = 0
        if head_commit is not None:
            first = int(head.get("base", 0))
            lag = max(0, head_commit + 1 - max(int(wm["next"]) if wm else first, first))
        mode = ((wm or {}).get("pass") or {}).get("mode")
        if wm is None:
            state = "never"
        elif wm.get("pattern_change"):
            state = "pattern_change"
        elif mode == "full":
            state = "full"
        elif wm.get("reconcile") is not None:
            state = "reconcile"
        elif mode is not None:
            state = "paging"
        else:
            state = "behind" if lag else "caught_up"
        view = None
        if wm is not None:  # less the pattern change's snapshot: an index state, too big to show
            view = dict(wm)
            if wm.get("pattern_change"):
                view["pattern_change"] = {k: v for k, v in wm["pattern_change"].items() if k != "snapshot"}
        return {
            "partition": partition,
            "upstream_partition": upstream_partition,
            "bookmark": view,
            "head_commit": head_commit,
            "lag": lag,
            "state": state,
        }

    async def asset_inputs(self, asset: str) -> dict:
        """Every input edge of an asset, deps included (kind `dep`); for an
        Incremental or Each edge, each partition's bookmark and lag — the
        asset's current partitions and every partition with a bookmark."""

        info = self.manifest["assets"][asset]
        inputs = [*info["inputs"].items(), *((d, {"kind": "dep", "output": d}) for d in info["deps"])]
        current = set(self.planner().partitions(asset, "all"))
        marked: dict[str, set] = {}
        for partition, record in self.m.partitions.of(asset).items():
            for param in record.get("bookmarks") or ():
                marked.setdefault(param, set()).add(partition)
        out = []
        for param, input in inputs:
            output = input["output"]
            partitions = []
            if input["kind"] == "incremental":
                for partition in sorted(current | marked.get(param, set())):
                    partitions.append(self._input_partition(asset, param, input, partition))
            out.append(
                {
                    "param": param,
                    "kind": "each" if input.get("each") is not None else input["kind"],
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
            found = await KeyIndex(self._key_io(), None, state.pinned(), self.key_options).lookup(
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
        Incremental edge: `edge`, else its Each edge, else its one keyed
        Incremental edge. The verdict is the first of these that holds:

        - `not_matched`: no `include` pattern of the edge matches it;
        - `excluded`: an `exclude` pattern does (`patterns.excluded_by`);
        - `failing`: the asset's failed keys holds it (`failure`);
        - `removed`: the upstream no longer holds it, and it was processed
          once or an output still does (its removal may be undelivered);
        - `absent`: the upstream does not hold it, and nothing shows it did;
        - `ok`: processed at the upstream key's current generation — its
          newest `ok`, `removed` or `unmatched` row is an `ok` at it, or the
          edge is caught up (a row expires with its run, and a plain
          Incremental edge records none);
        - `pending`: the upstream holds a write of it the edge has not
          delivered yet.

        The patterns are those the edge delivers under — its bookmark's,
        else the manifest's; `pending` the manifest's, when a transition to
        them has yet to run."""

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
            raise ValueError(f"{asset} has no keyed Incremental edge {input!r}")
        spec, is_each = keyed[input], keyed[input].get("each") is not None
        if (
            partition not in self.planner().partitions(asset, [partition])
            and self.m.bookmark(asset, input, partition) is None
        ):
            raise KeyError(f"{asset}/{partition}")
        output = spec["output"]
        where = self._input_partition(asset, input, spec, partition)
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
        wm = self.m.bookmark(asset, input, partition)
        served = wm.get("patterns") if wm is not None else spec.get("patterns")
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
        elif (kept is not None and kept["generation"] == generation) or where["state"] == "caught_up":
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
            "input_state": where["state"],
            "outputs": {
                name: {"present": v is not None, "generation": None if v is None else v[0]}
                for name, v in zip(outputs, held, strict=True)
            },
            "patterns": {
                "spec": served,
                "included": included,
                "excluded_by": excluded_by,
                "pending": spec.get("patterns") if spec.get("patterns") != served else None,
            },
            "failure": failure,
            "last": last,
            "last_ok": last_ok,
            "verdict": verdict,
        }

    # -- what an operator may clear (docs/lifecycle.md §9.6, §9.8) -----------------------------------------

    def repairs_view(self) -> list[dict]:
        """Output partitions a writer that died left owing a repair, with each
        intent's files, for the next attempt (docs/lifecycle.md §9.6)."""

        return [
            {
                "output": output,
                "partition": partition,
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
        to clear (docs/lifecycle.md §9.8)."""

        return [
            self.partition_cleanups(output, partition)
            for (output, partition), entries in sorted(self.m.cleanups.items())
            if any(e.get("stuck") for e in entries)
        ]
