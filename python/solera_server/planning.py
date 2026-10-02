"""Planning (§7, §8): which scopes a request selects, what each scope reads
upstream, and the run a request becomes — synchronous domain code over an
explicit view: the manifest, the heads (with any heads a sensor's commits
are about to install over them) and the time. No engine state, no I/O,
nothing awaited.

A selection is answered without enumerating the partition domain unless it
asks for the whole of it: one explicit scope checks each of its parts'
membership, `latest` builds only the latest time window, and only `all` and
`missing` list every combination — up to `MAX_SCOPES`, past which they are an
error rather than a silent truncation.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable, Mapping
from functools import partial
from itertools import product

from solera.ids import ulid
from solera.sdk import MAX_PARTITION_KEYS, TimePartitions, canonical_partition, split_partition

MAX_SCOPES = 100_000  # scopes a request may enumerate


def time_partitions(dim: dict) -> TimePartitions:
    return TimePartitions(
        dim["start"],
        dim["every"],
        end=dim.get("end"),
        end_offset=dim.get("end_offset"),
        timezone=dim.get("timezone") or "UTC",
        format=dim.get("format"),
    )


def canonical(dims: dict, key: str) -> str:
    if len(dims) == 1:
        return str(key)
    return canonical_partition(dims, split_partition(dims, key))


def dim_keys(dim: dict, now: dt.datetime, elements: Callable[[str], list[str] | None]) -> list[str]:
    """One dimension's current keys, enumerated."""

    if dim["kind"] == "static":
        return [str(k) for k in dim["keys"]]
    if dim["kind"] == "time":
        return time_partitions(dim).keys(now, MAX_PARTITION_KEYS)
    return sorted(elements(dim["output"]) or ())


def _size(dim: dict, now: dt.datetime, elements) -> int:
    if dim["kind"] == "static":
        return len(dim["keys"])
    if dim["kind"] == "time":
        return time_partitions(dim).count(now, MAX_SCOPES)
    return len(elements(dim["output"]) or ())


def size(dims: dict, now: dt.datetime, elements) -> int:
    """How many scopes `dims` spans, counted per dimension — never enumerated.
    A cron time dimension is counted only to just past `MAX_SCOPES`."""

    total = 1
    for dim in dims.values():
        total *= _size(dim, now, elements)
    return total


def enumerate_scopes(dims: dict, now: dt.datetime, elements, *, what: str = "") -> list[str]:
    """Every scope of `dims`, in dimension order — refused past `MAX_SCOPES`."""

    if not dims:
        return [""]
    total = size(dims, now, elements)
    if total > MAX_SCOPES:
        raise ValueError(
            f"{what or 'the selection'} spans {total} partitions, more than {MAX_SCOPES}: "
            "select partitions explicitly, or the latest"
        )
    keys = [dim_keys(dim, now, elements) for dim in dims.values()]
    return [canonical_partition(dims, dict(zip(dims, combo, strict=True))) for combo in product(*keys)]


def membership(dims: dict, now: dt.datetime, elements) -> Callable[[str], bool]:
    """Whether a scope is one `enumerate_scopes` would list: canonical, each
    of its parts a member of its dimension — checked part by part, never by
    enumeration. Each dimension's members are read once, for any number of
    scopes."""

    if not dims:
        return lambda scope: scope == ""
    checks = {}
    for name, dim in dims.items():
        if dim["kind"] == "time":
            checks[name] = partial(time_partitions(dim).contains, as_of=now)
        elif dim["kind"] == "static":
            checks[name] = {str(k) for k in dim["keys"]}.__contains__
        else:
            checks[name] = set(elements(dim["output"]) or ()).__contains__

    def member(scope: str) -> bool:
        try:
            parts = split_partition(dims, scope)
        except ValueError:
            return False
        return canonical_partition(dims, parts) == scope and all(
            check(parts[name]) for name, check in checks.items()
        )

    return member


def select_scopes(
    dims: dict,
    selection,
    *,
    now: dt.datetime,
    elements: Callable[[str], list[str] | None],
    missing: Callable[[str], bool],
    what: str = "",
) -> list[str]:
    """The scopes `selection` names: `"all"` (or None), `"latest"`,
    `"missing"`, or explicit keys. `elements(output)` gives a set
    dimension's members; `missing(scope)` whether a scope lacks a complete
    head."""

    if selection is None or selection == "all":
        return enumerate_scopes(dims, now, elements, what=what)
    if selection == "missing":
        return [s for s in enumerate_scopes(dims, now, elements, what=what) if missing(s)]
    if selection == "latest":
        if not dims:
            return [""]
        chosen = []
        for dim in dims.values():
            if dim["kind"] == "time":
                latest = time_partitions(dim).latest(now)
                chosen.append([latest] if latest else [])
            else:
                chosen.append(dim_keys(dim, now, elements))
        return [canonical_partition(dims, dict(zip(dims, combo, strict=True))) for combo in product(*chosen)]
    wanted = list(dict.fromkeys(selection or ()))
    if not dims:
        return [""] if "" in wanted else []
    member, out = membership(dims, now, elements), []
    for key in wanted:
        try:
            scope = canonical(dims, key)
        except (ValueError, KeyError):
            continue
        if scope not in out and member(scope):
            out.append(scope)
    return out


class UpstreamOnly(ValueError):
    """An edge reads an upstream dimension its consumer lacks without
    AllPartitions (§7)."""


def check_tags(tags) -> dict[str, str]:
    """Run tags: up to 32 short string pairs."""

    tags = tags or {}
    if not isinstance(tags, dict) or len(tags) > 32:
        raise ValueError("tags must be an object of at most 32 entries")
    for key, value in tags.items():
        if not isinstance(key, str) or not isinstance(value, str) or not key or "=" in key:
            raise ValueError("tags map non-empty names without '=' to strings")
        if len(key) > 64 or len(value) > 256:
            raise ValueError("A tag name is at most 64 characters, its value at most 256")
    return dict(sorted(tags.items()))


class Planner:
    """Planning over one view: `manifest`; `head(output, scope)` and
    `heads_of(output)` — the committed heads, with `projected` heads (what a
    sensor's commits will install) over them; and `now` (epoch seconds)."""

    def __init__(
        self,
        manifest: dict,
        head: Callable[[str, str], dict | None],
        heads_of: Callable[[str], Iterable[tuple[str, dict]]],
        now: float,
        projected: Mapping[tuple[str, str], dict] | None = None,
    ):
        self.manifest, self.now = manifest, now
        self.projected = dict(projected or {})
        self._head, self._heads_of = head, heads_of
        self.time = dt.datetime.fromtimestamp(now, dt.UTC)

    # -- heads -------------------------------------------------------------------

    def head(self, output: str, scope: str) -> dict | None:
        found = self.projected.get((output, scope))
        return found if found is not None else self._head(output, scope)

    def heads_of(self, output: str) -> dict[str, dict]:
        """Every head of `output`, by scope: what exists, never the domain."""

        heads = dict(self._heads_of(output))
        heads.update({scope: h for (o, scope), h in self.projected.items() if o == output})
        return heads

    def elements(self, output: str) -> list[str] | None:
        """A set dimension's current keys: the element list its head carries (§7)."""

        head = self.head(output, "")
        return None if head is None else [str(e) for e in head.get("elements") or ()]

    # -- dimensions --------------------------------------------------------------

    def dims(self, asset: str | None) -> dict:
        if asset is None:
            return {}
        return dims_of(self.manifest["assets"][asset])

    def owner(self, output: str) -> str | None:
        return self.manifest["outputs"][output].get("asset")

    def dim_keys(self, dims: dict) -> list[list[str]]:
        return [dim_keys(dim, self.time, self.elements) for dim in dims.values()]

    def shared(self, consumer: dict, consumer_scope: str, upstream_dims: dict) -> tuple[dict, dict]:
        """`(pinned, free)`: the upstream dimensions the consumer shares, at its
        scope's keys, and those it lacks."""

        c_dims = dims_of(consumer)
        parts = split_partition(c_dims, consumer_scope) if c_dims else {}
        pinned, free = {}, {}
        for name, dim in upstream_dims.items():
            match = next((cn for cn, cd in c_dims.items() if same_dim(dim, cd)), None)
            if match is not None:
                pinned[name] = parts[match]
            else:
                free[name] = dim
        return pinned, free

    def project(self, consumer: dict, consumer_scope: str, upstream_dims: dict) -> str:
        """The projection rule (§7): shared dims take the consumer key;
        consumer-only dims broadcast away; upstream-only dims must be collapsed."""

        if not upstream_dims:
            return ""
        pinned, free = self.shared(consumer, consumer_scope, upstream_dims)
        if free:
            raise UpstreamOnly(f"upstream-only dimension {next(iter(free))!r} requires AllPartitions")
        return canonical_partition(upstream_dims, pinned)

    def project_downstream(self, producer: str | None, scope: str, target: str) -> dict[str, str]:
        """Shared dims pinned by a changed scope of `producer`; the target's
        others are left for the caller to expand (§7, §9). A source (`None`)
        pins none."""

        p_dims, t_dims = self.dims(producer), self.dims(target)
        if not p_dims or not t_dims:
            return {}
        parts = split_partition(p_dims, scope)
        pinned = {}
        for t_name, t_dim in t_dims.items():
            for p_name, p_dim in p_dims.items():
                if same_dim(t_dim, p_dim):
                    pinned[t_name] = parts[p_name]
                    break
        return pinned

    def matches(self, upstream_dims: dict, up_scope: str, pinned: dict) -> bool:
        """Whether an upstream scope agrees with the consumer's shared keys."""

        if not pinned:
            return True
        try:
            parts = split_partition(upstream_dims, up_scope)
        except (ValueError, KeyError):
            return False
        return all(parts.get(name) == value for name, value in pinned.items())

    def fan_in(self, consumer: dict, scope: str, output: str, *, complete: bool) -> dict[str, dict]:
        """The upstream heads a scope reads across the upstream-only dimensions
        it lacks — `AllPartitions`, a dep — chosen among the heads that exist
        and agree with its shared keys: never by expanding the domain. Keyed by
        the upstream scope."""

        up_dims = self.dims(self.owner(output))
        pinned, _ = self.shared(consumer, scope, up_dims)
        return {
            up_scope: head
            for up_scope, head in sorted(self.heads_of(output).items())
            if self.matches(up_dims, up_scope, pinned) and (head["complete"] or not complete)
        }

    def spread(self, consumer: dict, scope: str, upstream_dims: dict) -> list[str]:
        """Every upstream scope a scope could read across its upstream-only
        dimensions: the domain, enumerated — only to build upstream work, and
        refused past `MAX_SCOPES`."""

        pinned, free = self.shared(consumer, scope, upstream_dims)
        if not free:
            return [canonical_partition(upstream_dims, pinned)] if upstream_dims else [""]
        free_scopes = enumerate_scopes(free, self.time, self.elements, what="an upstream build")
        out = []
        for free_scope in free_scopes:
            parts = split_partition(free, free_scope) if len(free) > 1 else {next(iter(free)): free_scope}
            out.append(canonical_partition(upstream_dims, {**pinned, **parts}))
        return out

    # -- reads -------------------------------------------------------------------

    def reads(self, asset: str, scope: str, *, build: bool = False) -> list[tuple]:
        """`(kind, output, owner, upstream scope, fan_in)` for every edge, dep
        and partition-set dimension of (asset, scope). A fan-in read — across
        upstream-only dimensions — names the heads that exist, unless `build`
        asks for every scope to build (bounded)."""

        info = self.manifest["assets"][asset]
        edges = list(info["inputs"].values()) + [{"kind": "dep", "output": d} for d in info["deps"]]
        out = []
        for edge in edges:
            output, owner = edge["output"], self.owner(edge["output"])
            up_dims = self.dims(owner)
            if edge["kind"] in {"all_partitions", "dep"} and owner is not None:
                _, free = self.shared(info, scope, up_dims)
                if free:
                    scopes = (
                        self.spread(info, scope, up_dims)
                        if build
                        else self.fan_in(info, scope, output, complete=False)
                    )
                    out.extend((edge["kind"], output, owner, s, True) for s in scopes)
                    continue
            out.append((edge["kind"], output, owner, self.project(info, scope, up_dims), False))
        for dim in self.dims(asset).values():
            if dim["kind"] == "set" and self.owner(dim["output"]) is not None:
                out.append(("dep", dim["output"], self.owner(dim["output"]), "", False))
        return out

    def missing(self, asset: str, scope: str, planned: dict) -> bool:
        """Whether (asset, scope) reads an input never written that the run
        doesn't build — preparing it would fail. A fan-in reads what there is."""

        for kind, output, owner, up_scope, fan_in in self.reads(asset, scope):
            if kind == "all_partitions" or fan_in or up_scope in planned.get(owner, ()):
                continue
            source = owner is None and output in self.manifest["sources"] and up_scope == ""
            if not source and self.head(output, up_scope) is None:
                return True
        return False

    def scopes(self, asset: str, selection) -> list[str]:
        outputs = self.manifest["assets"][asset]["outputs"]

        def missing(scope: str) -> bool:
            heads = [self.head(o["name"], scope) for o in outputs]
            return not heads or any(h is None or not h["complete"] for h in heads)

        return select_scopes(
            self.dims(asset), selection, now=self.time, elements=self.elements, missing=missing, what=asset
        )

    def asset_of(self, name: str) -> str:
        if name in self.manifest["assets"]:
            return name
        if name in self.manifest["outputs"] and self.manifest["outputs"][name].get("asset"):
            return self.manifest["outputs"][name]["asset"]
        raise ValueError(f"Unknown asset or output: {name!r}")

    # -- runs --------------------------------------------------------------------

    def plan_run(
        self,
        targets,
        partitions="latest",
        mode="incremental",
        upstream=False,
        config=None,
        keys=None,
        *,
        active: Callable[[str, str], bool] = lambda a, s: False,
        automation=None,
        sensor=None,
        skip_active=False,
        skip_missing_inputs=False,
        by=None,
        tags=None,
        retry_of=None,
    ) -> dict | None:
        """The run a request becomes, without submitting it; `None` if the
        skips leave nothing. `active(asset, scope)` says whether a scope is in
        flight, for `skip_active`."""

        if isinstance(targets, str):
            targets = [targets]
        if not targets:
            raise ValueError("A run needs at least one target")
        if mode not in {"incremental", "full"}:
            raise ValueError(f"Unknown mode: {mode!r}")
        config = config or {}
        if not isinstance(config, dict):
            raise ValueError("config must be a JSON object")
        tags = check_tags(tags)
        assets: dict[str, list[str]] = {}
        for target in targets:
            name = self.asset_of(target)
            # A map names each asset's own scopes (a retry's).
            assets[name] = self.scopes(name, partitions[name] if isinstance(partitions, dict) else partitions)
        if upstream:
            queue = [(n, s) for n, scopes in assets.items() for s in scopes]
            seen = set(queue)
            while queue:
                name, scope = queue.pop()
                for _, _, owner, up_scope, _ in self.reads(name, scope, build=True):
                    if owner is None or (owner, up_scope) in seen:
                        continue
                    seen.add((owner, up_scope))
                    assets.setdefault(owner, []).append(up_scope)
                    queue.append((owner, up_scope))
        if keys:
            incremental_outputs = {
                e["output"]
                for name in assets
                for e in self.manifest["assets"][name]["inputs"].values()
                if e["kind"] == "incremental"
            }
            unknown = set(keys) - incremental_outputs
            if unknown:
                raise ValueError(f"keys= names no Incremental edge: {sorted(unknown)}")
        if skip_active:
            for name in list(assets):
                assets[name] = [s for s in assets[name] if not active(name, s)]
            if not any(assets.values()):
                return None  # §9: the tick is skipped — every scope is in flight
        if skip_missing_inputs:
            while dropped := [
                (n, s) for n, scopes in assets.items() for s in scopes if self.missing(n, s, assets)
            ]:
                for name, scope in dropped:
                    assets[name].remove(scope)
            if not any(assets.values()):
                return None  # nothing can run until its inputs are written
        run_id = ulid(self.now)
        tasks = {}
        for name, scopes in assets.items():
            for scope in sorted(set(scopes)):
                task_id = f"{run_id}/{name}:{scope}"
                tasks[task_id] = {
                    "id": task_id,
                    "run": run_id,
                    "asset": name,
                    "scope": scope,
                    "status": "queued",
                    "deps": [],
                    "max_attempts": 1 + self.manifest["assets"][name].get("retries", {}).get("n", 0),
                    "retry": self.manifest["assets"][name].get("retries"),
                    "ready_at": self.now,
                    "queued_at": self.now,
                    "wait": 0.0,
                }
        self._order(tasks, assets)
        return {
            "id": run_id,
            "targets": sorted(assets),
            "partitions": partitions if isinstance(partitions, str) else list(partitions),
            "mode": mode,
            "upstream": bool(upstream),
            "config": config,
            "keys": keys,
            "automation": automation,
            **({"sensor": sensor} if sensor else {}),
            **({"retry_of": retry_of} if retry_of else {}),
            "by": by,
            "tags": tags,
            "status": "running",
            "paused": False,
            "created_at": self.now,
            "updated_at": self.now,
            "events": 0,
            "tasks": tasks,
        }

    def _order(self, tasks: dict, assets: dict) -> None:
        """A task waits for the run's tasks it reads: the projected scope of
        each edge, or — across upstream-only dimensions — every scope of the
        owner in this run that agrees with its shared keys."""

        for task in tasks.values():
            info = self.manifest["assets"][task["asset"]]
            edges = list(info["inputs"].values()) + [{"kind": "dep", "output": d} for d in info["deps"]]
            edges += [
                {"kind": "dep", "output": d["output"]}
                for d in self.dims(task["asset"]).values()
                if d["kind"] == "set"
            ]
            for edge in edges:
                owner = self.owner(edge["output"])
                if owner is None or owner not in assets:
                    continue
                up_dims = self.dims(owner)
                pinned, free = self.shared(info, task["scope"], up_dims) if up_dims else ({}, {})
                if edge.get("kind") not in ("all_partitions", "dep") and free:
                    continue  # registration forbids it; preparation says so
                for up_scope in sorted(set(assets[owner])):
                    dep_id = f"{task['run']}/{owner}:{up_scope}"
                    if dep_id == task["id"] or dep_id in task["deps"]:
                        continue
                    if self.matches(up_dims, up_scope, pinned):
                        task["deps"].append(dep_id)
                        task["status"], task["queued_at"] = "waiting", None


def same_dim(a: dict, b: dict) -> bool:
    if a["kind"] != b["kind"]:
        return False
    if a["kind"] == "set":
        return a["output"] == b["output"]
    return a == b


def dims_of(asset: dict) -> dict:
    return (asset.get("partitions") or {}).get("dims") or {}
