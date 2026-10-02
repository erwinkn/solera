"""The console's read models (§8, §10): per-asset rollups, an `Each` asset's
failure index and `explain`, every scope of every edge, and what holds
scopes back. Reads only: they record nothing, and never scan a run's tasks.
A mixin of the engine, as `Attempts` and `Sensors` are."""

from __future__ import annotations

import asyncio
import json
from collections import Counter

from solera.failures import NAMES, Record, eligible
from solera.keys.index import KeyIndex, key_bytes, key_str
from solera.patterns import Matcher

from .model import BAD_OUTCOME
from .state import Conflict

SCAN = 100  # a failure listing reads at most this many entries per key it returns
PAGE = 1000  # entries read from a failure index at a time


class Views:
    # -- partitions and assets (§7, §8) -------------------------------------------------

    async def scope_statuses(self, assets: list[str]) -> dict[str, list[dict]]:
        """Each scope of each asset, by status: `complete` (its head is),
        `running` (a task is pending), `failed` (its last outcome failed, was
        canceled or blocked), `missing`, or `retired` (no longer a current
        key). A job has no head: it is complete when its last outcome
        succeeded. From heads, scope outcomes and the pending index, one
        pass over each — never a task scan."""

        heads: dict[str, dict] = {}
        for (output, scope), head in self.m.heads.items():
            heads.setdefault(output, {})[scope] = head
        outcomes: dict[str, dict] = {}
        for (asset, scope), record in self.m.outcomes.items():
            outcomes.setdefault(asset, {})[scope] = record
        running: dict[str, set] = {}
        for (asset, scope), ids in self.m.pending.items():
            if ids:
                running.setdefault(asset, set()).add(scope)
        out = {}
        for asset in assets:
            outputs = self.manifest["assets"][asset]["outputs"]
            current = set(await self._scopes(asset, "all"))
            scoped: dict[str, dict] = {}
            for output in outputs:  # a scope's head: its last declared output's, among those it has
                scoped.update(heads.get(output["name"]) or {})
            recorded = outcomes.get(asset) or {}
            rows = out[asset] = []
            for scope in sorted(current | set(scoped) | set(recorded)):
                head, record = scoped.get(scope), recorded.get(scope)
                last = (record or {}).get("outcome")
                done = head["complete"] if head else not outputs and last in ("succeeded", "skipped")
                status = (
                    "retired"
                    if scope not in current
                    else "complete"
                    if done
                    else "running"
                    if scope in running.get(asset, ())
                    else "failed"
                    if last in BAD_OUTCOME
                    else "missing"
                )
                view = self.outcome_view(record) if record else {}
                rows.append(
                    {
                        "scope": scope,
                        "status": status,
                        "last_outcome": view.get("last_outcome"),
                        "last_attempt": view.get("last_attempt"),
                    }
                )
        return out

    async def asset_statuses(self) -> dict[str, dict]:
        """One rollup per asset, for the console's graph and list: its scopes
        by status — `total` counts the current ones, `retired` those past
        them — its newest outcome, an `Each` asset's failing keys by class
        (null for any other), its held scopes, the scopes of its outputs a
        dead writer left unsettled, and when an output last changed."""

        names = list(self.manifest["assets"])
        statuses = await self.scope_statuses(names)
        owner = {o["name"]: a for a in names for o in self.manifest["assets"][a]["outputs"]}
        out = {}
        for name in names:
            counts = Counter(row["status"] for row in statuses[name])
            partitions = {s: counts[s] for s in ("complete", "missing", "failed", "running", "retired")}
            out[name] = {
                "partitions": {"total": sum(partitions.values()) - partitions["retired"], **partitions},
                "partitioned": bool(self._dims(name)),
                "last": None,
                "failures": {} if self._each_edge(name) else None,
                "held": 0,
                "unsettled": 0,
                "updated_at": None,
            }
        for (output, _), head in self.m.heads.items():
            if (entry := out.get(owner.get(output))) is not None:
                entry["updated_at"] = max(entry["updated_at"] or head["at"], head["at"])
        for (asset, scope), record in self.m.outcomes.items():
            entry = out.get(asset)
            if entry is not None and (entry["last"] is None or record["at"] > entry["last"]["at"]):
                view = self.outcome_view(record)
                entry["last"] = {
                    "scope": scope,
                    "outcome": view["last_outcome"],
                    "at": view["at"],
                    "attempt": view["last_attempt"],
                }
        for (asset, _), record in self.m.failures.items():
            if (failures := (out.get(asset) or {}).get("failures")) is not None:
                for name, n in (record.get("counts") or {}).items():
                    failures[name] = failures.get(name, 0) + n
        for asset, _ in self.m.holds:
            if asset in out:
                out[asset]["held"] += 1
        for output, _ in self.m.unsettled:
            if (entry := out.get(owner.get(output))) is not None:
                entry["unsettled"] += 1
        return out

    # -- failing keys (docs/per-key-processing.md §9) -------------------------------------

    def _each_edge(self, asset: str) -> tuple[str, dict] | None:
        inputs = self.manifest["assets"][asset]["inputs"]
        return next(((p, e) for p, e in inputs.items() if e.get("each") is not None), None)

    def _failure_view(self, asset: str, scope: str, key: str, record: Record, forced: dict) -> dict:
        """A failing key's record, its times null where unset; `eligible`:
        whether a retry pass would take it now."""

        output = self._each_edge(asset)[1]["output"]
        return {
            "scope": scope,
            "key": key,
            "outcome": record.name,
            "tries": record.tries,
            "since": record.since,
            "last": record.last,
            "next_at": record.next_at or None,
            "until": record.until or None,
            "revision": self._rendered(output, record.revision),
            "message": record.message,
            "eligible": eligible(record, self.clock(), self.m.epoch, forced),
        }

    async def _failure_entries(self, asset: str, scopes: list[str], start: list | None):
        """`(scope, key, record)` of an asset's failure index in scope and key
        order, from just past `start` (`[scope, key]`)."""

        for scope in scopes:
            if start is not None and scope < start[0]:
                continue
            state = self.m.indexes.get((f"@{asset}", scope))
            if state is None:
                continue
            cursor = key_bytes(start[1]) if start is not None and scope == start[0] else None
            with self.m.reading():  # the scope's files outlive compaction until its walk ends (aclose)
                index = KeyIndex(self._key_io(), None, state.pinned(), self.key_options)
                while True:
                    keys, versions, _, cursor = await index.page(cursor, PAGE)
                    for k, v in zip(keys, versions, strict=True):
                        yield scope, key_str(k), Record.decode(v)
                    if cursor is None:
                        break

    async def key_failures(
        self, asset: str, scope: str | None = None, *, outcomes=(), after: str | None = None, limit: int = 100
    ) -> dict:
        """An `Each` asset's failure index (§9): the record of every scope
        with one, or of `scope`, and a page of its failing keys in scope and
        key order, of the classes in `outcomes` if any. `after` is the
        previous page's `next`: `[scope, key]` as JSON. A page reads at most
        `SCAN` entries per key it may return, so a rare class can come back
        as a short page with a `next`."""

        if self._each_edge(asset) is None:
            raise ValueError(f"{asset} has no Each edge: it keeps no failing keys")
        unknown = set(outcomes) - set(NAMES.values())
        if unknown:
            raise ValueError(f"unknown key classes: {sorted(unknown)}")
        records = {s: r for (a, s), r in self.m.failures.items() if a == asset and scope in (None, s)}
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
                    nxt = [keys[-1]["scope"], keys[-1]["key"]]
                    break
                keys.append(self._failure_view(asset, s, key, record, records[s].get("forced") or {}))
        finally:
            await entries.aclose()
        return {
            "asset": asset,
            "scopes": [
                {
                    "scope": s,
                    "counts": r.get("counts") or {},
                    "due": r.get("due"),
                    "epoch_min": r.get("epoch_min"),
                    "passes": r.get("passes") or 0,
                    "retry": r.get("retry"),
                    "forced": r.get("forced") or {},
                    "last": r.get("last"),
                    "has_retries": self._has_retries(r),
                }
                for s, r in sorted(records.items())
            ],
            "epoch": self.m.epoch,
            "now": self.clock(),
            "keys": keys,
            "next": json.dumps(nxt) if nxt else None,
        }

    # -- edges (§6; per-key-processing.md §11) -----------------------------------------------

    def _edge_scope(self, asset: str, param: str, edge: dict, scope: str) -> dict:
        """One scope of an Incremental edge: its watermark and how far it is
        behind the upstream head.

        `watermark.batch` is the first upstream batch the edge has not yet
        delivered: the next window runs from it to the head's `batch` (a
        window paged over several attempts keeps it, with `until` its end and
        `after` the last key delivered). So `lag` = head batch + 1 −
        watermark batch: the upstream batches committed and not yet
        delivered in full — counted from the head's `base` for an unkeyed
        upstream, which starts over there; every batch without a watermark.
        A change of fingerprint (the asset's version, its run config, a
        pinned input) resets the edge at its next run: not shown here.

        `state`: `never` (no watermark), `rescope` (a pattern transition,
        per-key §11), `full` (a full delivery in progress), `reconcile` (the
        cleanup after a full Each delivery), `paging` (a window delivered
        over several attempts), `behind` (lag), else `caught_up`."""

        wm = self.m.watermarks.get((asset, param, scope))
        up_scope = (wm or {}).get("up")
        if up_scope is None:
            up_dims = self._dims(self.manifest["outputs"][edge["output"]].get("asset"))
            try:
                up_scope = self._project(self.manifest["assets"][asset], scope, up_dims)
            except (Conflict, ValueError, KeyError):
                up_scope = None
        head = self.m.heads.get((edge["output"], up_scope)) if up_scope is not None else None
        head_batch = int(head.get("batch", -1)) if head is not None else None
        lag = 0
        if head_batch is not None:
            first = int(head.get("base", 0))
            lag = max(0, head_batch + 1 - max(int(wm["batch"]) if wm else first, first))
        if wm is None:
            state = "never"
        elif wm.get("rescope"):
            state = "rescope"
        elif wm.get("full"):
            state = "full"
        elif wm.get("reconcile") is not None:
            state = "reconcile"
        elif wm.get("after") is not None:
            state = "paging"
        else:
            state = "behind" if lag else "caught_up"
        view = None
        if wm is not None:  # less the rescope's snapshot: an index state, too big to show
            view = dict(wm)
            if wm.get("rescope"):
                view["rescope"] = {k: v for k, v in wm["rescope"].items() if k != "snapshot"}
        return {
            "scope": scope,
            "up_scope": up_scope,
            "watermark": view,
            "head_batch": head_batch,
            "lag": lag,
            "state": state,
        }

    async def asset_edges(self, asset: str) -> dict:
        """Every input edge of an asset, deps included (kind `dep`); for an
        Incremental or Each edge, each scope's watermark and lag — the
        asset's current scopes and every scope with a watermark."""

        info = self.manifest["assets"][asset]
        edges = [*info["inputs"].items(), *((d, {"kind": "dep", "output": d}) for d in info["deps"])]
        current = set(await self._scopes(asset, "all"))
        marked: dict[str, set] = {}
        for a, param, scope in self.m.watermarks:
            if a == asset:
                marked.setdefault(param, set()).add(scope)
        out = []
        for param, edge in edges:
            output = edge["output"]
            scopes = []
            if edge["kind"] == "incremental":
                for scope in sorted(current | marked.get(param, set())):
                    scopes.append(self._edge_scope(asset, param, edge, scope))
            out.append(
                {
                    "param": param,
                    "kind": "each" if edge.get("each") is not None else edge["kind"],
                    "output": output,
                    "upstream_asset": self.manifest["outputs"][output].get("asset"),
                    "source": output in self.manifest["sources"],
                    "batch_size": edge.get("batch_size"),
                    "concurrency": (edge.get("each") or {}).get("concurrency"),
                    "patterns": edge.get("patterns"),
                    "scopes": scopes,
                }
            )
        return {"asset": asset, "edges": out}

    # -- explain (per-key-processing.md §10) -------------------------------------------------

    async def _lookup(self, output: str, scope: str, key: str) -> bytes | None:
        """A key's live version in an index, exactly; `None` if it holds none."""

        state = self.m.indexes.get((output, scope))
        if state is None:
            return None
        with self.m.reading():
            found = await KeyIndex(self._key_io(), None, state.pinned(), self.key_options).lookup(
                [key_bytes(key)]
            )
        hit = found.get(key_bytes(key))
        return hit[0] if hit else None

    async def _newest_outcome(self, asset: str, scope: str, key: str, outcomes=None) -> dict | None:
        found = await self.history.key_outcomes(asset, scope=scope, key=key, outcomes=outcomes, limit=1)
        return next(iter(found["outcomes"]), None)

    async def explain(self, asset: str, key: str, scope: str = "", edge: str | None = None) -> dict:
        """Why `key` is, or is not, in an asset's output (§10), through one
        Incremental edge: `edge`, else its Each edge, else its one keyed
        Incremental edge. The verdict is the first of these that holds:

        - `not_matched`: no `include` pattern of the edge matches it;
        - `excluded`: an `exclude` pattern does (`patterns.excluded_by`);
        - `failing`: the asset's failure index holds it (`failure`);
        - `removed`: the upstream no longer holds it, and it was processed
          once or an output still does (its removal may be undelivered);
        - `absent`: the upstream does not hold it, and nothing shows it did;
        - `ok`: processed at the upstream's current revision — a `key_outcomes`
          row says so, or the edge is caught up (a row expires with its run,
          and a plain Incremental edge records none);
        - `pending`: the upstream holds a revision the edge has not
          delivered yet.

        The patterns are those the edge delivers under — its watermark's,
        else the manifest's; `pending` the manifest's, when a transition to
        them has yet to run."""

        info = self.manifest["assets"][asset]
        keyed = {
            p: e
            for p, e in info["inputs"].items()
            if e["kind"] == "incremental" and self.manifest["outputs"][e["output"]].get("key") is not None
        }
        if edge is None:
            each = self._each_edge(asset)
            if each is None and len(keyed) != 1:
                raise ValueError(
                    f"{asset} has {len(keyed) or 'no'} keyed Incremental edges: name one with edge="
                )
            edge = each[0] if each is not None else next(iter(keyed))
        if edge not in keyed:
            raise ValueError(f"{asset} has no keyed Incremental edge {edge!r}")
        spec, is_each = keyed[edge], keyed[edge].get("each") is not None
        if scope not in await self._scopes(asset, "all") and (asset, edge, scope) not in self.m.watermarks:
            raise KeyError(f"{asset}/{scope}")
        output = spec["output"]
        where = self._edge_scope(asset, edge, spec, scope)
        if where["up_scope"] is None:
            raise ValueError(f"{asset}: no upstream scope for {scope!r}")
        outputs = [o["name"] for o in info["outputs"] if o.get("key") is not None]
        indexes = [(output, where["up_scope"]), *((name, scope) for name in outputs)]
        if is_each:
            indexes.append((f"@{asset}", scope))
        upstream, *held = await asyncio.gather(*(self._lookup(o, s, key) for o, s in indexes))
        failing = held.pop() if is_each else None
        last = last_ok = None
        if is_each:
            last = await self._newest_outcome(asset, scope, key)
            ok = last is None or last["outcome"] == "ok"
            last_ok = last if ok else await self._newest_outcome(asset, scope, key, ["ok"])
        wm = self.m.watermarks.get((asset, edge, scope))
        served = wm.get("patterns") if wm is not None else spec.get("patterns")
        matcher = Matcher(served)
        included, excluded_by = matcher.included(key), matcher.excluded_by(key)
        revision = self._rendered(output, upstream) if upstream is not None else None
        failure = None
        if failing is not None:
            forced = (self.m.failures.get((asset, scope)) or {}).get("forced") or {}
            failure = self._failure_view(asset, scope, key, Record.decode(failing), forced)
        present = {name: v is not None for name, v in zip(outputs, held, strict=True)}
        if not included:
            verdict = "not_matched"
        elif excluded_by is not None:
            verdict = "excluded"
        elif failure is not None:
            verdict = "failing"
        elif upstream is None:
            verdict = "removed" if last is not None or any(present.values()) else "absent"
        elif (last_ok is not None and last_ok["revision"] == revision) or where["state"] == "caught_up":
            verdict = "ok"
        else:
            verdict = "pending"
        return {
            "asset": asset,
            "scope": scope,
            "key": key,
            "edge": edge,
            "upstream": output,
            "upstream_asset": self.manifest["outputs"][output].get("asset"),
            "up_scope": where["up_scope"],
            "upstream_revision": revision,
            "edge_state": where["state"],
            "outputs": {
                name: {"present": v is not None, "revision": None if v is None else self._rendered(name, v)}
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

    # -- holds (docs/lifecycle.md §9.6, §9.8, §9.9) -----------------------------------------

    def holds_view(self) -> dict:
        """What holds scopes back, and what only an operator can clear:
        scopes held for an uncertain writer — `grace` the seconds a grace hold
        lasts (its asset's longest `late_write_grace`), `releases_at` when it
        ends on this engine's clock (null for a strict hold, or one not yet
        timed); outputs left unsettled by writers that died, with each
        intent's files; and the scopes whose data garbage has stuck entries."""

        loop = asyncio.get_running_loop().time()
        holds = []
        for (asset, scope), hold in sorted(self.m.holds.items()):
            grace = since = None
            if hold["mode"] == "grace" and asset in self.manifest["assets"]:
                grace = self._grace(asset)
                since = self._held_since.get((asset, scope, hold["attempt"]))
            holds.append(
                {
                    "asset": asset,
                    "scope": scope,
                    **{k: hold.get(k) for k in ("attempt", "run", "mode", "at")},
                    "grace": grace,
                    "releases_at": None if since is None else self.clock() + max(0.0, grace - (loop - since)),
                }
            )
        unsettled = [
            {
                "output": output,
                "scope": scope,
                "intents": [
                    {
                        "run": i.get("run"),
                        "attempt": i.get("attempt"),
                        "files": [f["name"] for f in i.get("files") or ()],
                    }
                    for i in intents
                ],
            }
            for (output, scope), intents in sorted(self.m.unsettled.items())
        ]
        discards = [
            self.scope_discards(output, scope)
            for (output, scope), entries in sorted(self.m.discards.items())
            if any(e.get("stuck") for e in entries)
        ]
        return {"holds": holds, "unsettled": unsettled, "discards": discards}
