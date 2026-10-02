"""Planning (§7, §8): which scopes a request selects, what each scope reads
upstream, and the run a request becomes — synchronous domain code over an
explicit view: the manifest, the heads (with any heads a sensor's commits
are about to install over them) and the time. No engine state, no I/O,
nothing awaited.

A selection is answered without enumerating the partition domain unless it
asks for the whole of it: one explicit scope checks each of its parts'
membership. Any other is counted before it is listed — `latest` holds each
time dimension at its latest window and lists the rest — and refused past
`MAX_SCOPES` rather than silently truncated.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass, field
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


def enumerate_scopes(
    dims: dict, now: dt.datetime, elements, *, pinned: Mapping[str, str] | None = None, what: str = ""
) -> list[str]:
    """Every scope of `dims` — those in `pinned` held at one key each — in
    dimension order: counted first, refused past `MAX_SCOPES`."""

    if not dims:
        return [""]
    pinned = pinned or {}
    free = {name: dim for name, dim in dims.items() if name not in pinned}
    total = size(free, now, elements)
    if total > MAX_SCOPES:
        raise ValueError(
            f"{what or 'the selection'} spans {total} partitions, more than {MAX_SCOPES}: "
            "select partitions explicitly"
        )
    keys = [dim_keys(dim, now, elements) for dim in free.values()]
    return [
        canonical_partition(dims, {**pinned, **dict(zip(free, combo, strict=True))})
        for combo in product(*keys)
    ]


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
    if selection == "latest":  # each time dimension at its latest window; the others in full
        pinned = {}
        for name, dim in dims.items():
            if dim["kind"] == "time":
                if (latest := time_partitions(dim).latest(now)) is None:
                    return []
                pinned[name] = latest
        return enumerate_scopes(dims, now, elements, pinned=pinned, what=what)
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


COLLAPSING = frozenset({"all_partitions", "dep"})  # edge kinds that may read across free dimensions


@dataclass(frozen=True)
class Edge:
    """One read of a scope (§5, §7): an input, a dep, or the dep a
    partition-set dimension implies (`set_dim`: lineage, never the
    fingerprint). Of the owner's dimensions `dims`, the consumer shares
    `pinned` — at its scope's keys — and lacks `free`. A fan-in (an
    `AllPartitions` or a dep with free dimensions) reads the heads that
    exist across them; any other edge reads its one projected `scope`."""

    param: str
    kind: str
    output: str
    owner: str | None
    dims: dict
    pinned: dict
    free: dict
    spec: dict = field(compare=False, repr=False)
    set_dim: bool = False

    @property
    def fan_in(self) -> bool:
        return bool(self.free)

    @property
    def scope(self) -> str | None:
        """The one upstream scope it reads; None for a fan-in."""

        return None if self.free else canonical_partition(self.dims, self.pinned) if self.dims else ""

    def key(self, up_scope: str) -> str:
        """A fan-in head's key: its parts on the collapsed dimensions."""

        parts = split_partition(self.dims, up_scope)
        return canonical_partition(self.free, {name: parts[name] for name in self.free})


class Planner:
    """Planning over one view: `manifest`; `head(output, scope)` and
    `heads_of(output)` — the committed heads, with `projected` heads (what a
    sensor's commits will install) over them; `progress(asset, scope)`, a
    scope's delivery progress; and `now` (epoch seconds). The view is read as
    of each call; what a call derives from it (heads by output, set members)
    is kept for the planner's life — one operation's."""

    def __init__(
        self,
        manifest: dict,
        head: Callable[[str, str], dict | None],
        heads_of: Callable[[str], Iterable[tuple[str, dict]]],
        now: float,
        projected: Mapping[tuple[str, str], dict] | None = None,
        progress: Callable[[str, str], dict | None] = lambda asset, scope: None,
    ):
        self.manifest, self.now = manifest, now
        self.projected = dict(projected or {})
        self._head, self._heads_of, self._progress = head, heads_of, progress
        self.time = dt.datetime.fromtimestamp(now, dt.UTC)
        self._groups: dict[tuple, dict] = {}

    # -- heads -------------------------------------------------------------------

    def head(self, output: str, scope: str) -> dict | None:
        found = self.projected.get((output, scope))
        return found if found is not None else self._head(output, scope)

    def heads_of(self, output: str) -> dict[str, dict]:
        """Every head of `output`, by scope: what exists, never the domain."""

        heads = dict(self._heads_of(output))
        heads.update({scope: h for (o, scope), h in self.projected.items() if o == output})
        return heads

    def drained(self, asset: str, scope: str, heads: Iterable[dict]) -> bool:
        """Whether the scope's last commit finished its delivery — its outputs'
        `heads` say so of a scope committed before progress was kept."""

        record = self._progress(asset, scope)
        return record["drained"] if record is not None else all(h.get("complete", True) for h in heads)

    def complete(self, asset: str, scope: str) -> bool:
        """Whether a scope is complete (§7): each of its outputs has a head,
        and its delivery drained — however many of them its last pages wrote.
        A job, which has no output, once a run of it succeeded. The one answer
        for selection, fan-in and the views."""

        heads = [self.head(o["name"], scope) for o in self.manifest["assets"][asset]["outputs"]]
        if any(h is None for h in heads):
            return False
        return (bool(heads) or self._progress(asset, scope) is not None) and self.drained(asset, scope, heads)

    def head_complete(self, output: str, scope: str, head: dict) -> bool:
        """Whether a head is of a complete delivery: a source's always is."""

        owner = self.owner(output)
        return owner is None or self.drained(owner, scope, [head])

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

    def reach(self, producer: str | None, scope: str, target: str) -> list[str]:
        """The target scopes a change of `producer` at `scope` reaches (§7,
        §9): the dimensions it shares pinned, the others — every one, for a
        source — over their current keys. Bounded by `MAX_SCOPES`."""

        pinned = self.project_downstream(producer, scope, target)
        return enumerate_scopes(
            self.dims(target), self.time, self.elements, pinned=pinned, what=f"a change reaching {target}"
        )

    # -- edges -------------------------------------------------------------------

    def edges(self, asset: str, scope: str) -> list[Edge]:
        """What (asset, scope) reads: its inputs, its deps, then the partition
        sets its dimensions are bound to. An edge that is no fan-in may not
        lack an upstream dimension (`UpstreamOnly`)."""

        info = self.manifest["assets"][asset]
        named = list(info["inputs"].items()) + [(d, {"kind": "dep", "output": d}) for d in info["deps"]]
        outputs = {spec["output"] for _, spec in named}
        named += [
            (d["output"], {"kind": "dep", "output": d["output"], "set_dim": True})
            for d in self.dims(asset).values()
            if d["kind"] == "set" and d["output"] not in outputs
        ]
        out = []
        for param, spec in named:
            owner = self.owner(spec["output"])
            dims = self.dims(owner)
            pinned, free = self.shared(info, scope, dims)
            if free and spec["kind"] not in COLLAPSING:
                raise UpstreamOnly(f"upstream-only dimension {next(iter(free))!r} requires AllPartitions")
            out.append(
                Edge(
                    param,
                    spec["kind"],
                    spec["output"],
                    owner,
                    dims,
                    pinned,
                    free,
                    spec,
                    bool(spec.get("set_dim")),
                )
            )
        return out

    def fan_in(self, edge: Edge, *, complete: bool) -> dict[str, dict]:
        """The heads a fan-in reads, by upstream scope: among those that exist,
        the current partitions — a retired one's head is kept, never read —
        that agree with its shared keys (`complete` ones only, for
        `AllPartitions`). Never by expanding the domain: the owner's heads are
        grouped by their shared keys once per planner."""

        names = tuple(sorted(edge.pinned))
        groups = self._groups.get((edge.output, names))
        if groups is None:
            member, groups = membership(edge.dims, self.time, self.elements), {}
            for up_scope, head in sorted(self.heads_of(edge.output).items()):
                if member(up_scope):
                    parts = split_partition(edge.dims, up_scope)
                    groups.setdefault(tuple(parts[n] for n in names), {})[up_scope] = head
            self._groups[(edge.output, names)] = groups
        heads = groups.get(tuple(edge.pinned[n] for n in names)) or {}
        return {s: h for s, h in heads.items() if not complete or self.head_complete(edge.output, s, h)}

    def spread(self, edge: Edge) -> list[str]:
        """Every upstream scope an edge could read: for a fan-in, the domain
        across its free dimensions, enumerated — only to build upstream work,
        and bounded by `MAX_SCOPES`."""

        if not edge.fan_in:
            return [edge.scope]
        return enumerate_scopes(
            edge.dims, self.time, self.elements, pinned=edge.pinned, what="an upstream build"
        )

    def missing(self, asset: str, scope: str, planned: Mapping[str, Collection[str]]) -> bool:
        """Whether (asset, scope) reads an input never written that the run
        doesn't build — preparing it would fail. A fan-in reads what there is,
        so it is missing only when there is nothing: no current upstream head
        agrees with its shared keys (a complete one, for `AllPartitions`)."""

        for edge in self.edges(asset, scope):
            built = planned.get(edge.owner) or () if edge.owner is not None else ()
            if edge.fan_in:
                if any(self._agrees(edge, s) for s in built):
                    continue
                if not self.fan_in(edge, complete=edge.kind == "all_partitions"):
                    return True
            elif edge.kind != "all_partitions" and edge.scope not in built:
                source = edge.owner is None and edge.output in self.manifest["sources"] and edge.scope == ""
                if not source and self.head(edge.output, edge.scope) is None:
                    return True
        return False

    @staticmethod
    def _agrees(edge: Edge, up_scope: str) -> bool:
        parts = split_partition(edge.dims, up_scope)
        return all(parts.get(name) == value for name, value in edge.pinned.items())

    def scopes(self, asset: str, selection) -> list[str]:
        return select_scopes(
            self.dims(asset),
            selection,
            now=self.time,
            elements=self.elements,
            missing=lambda scope: not self.complete(asset, scope),
            what=asset,
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
        skips leave nothing. `partitions` selects every target's scopes, or —
        a map — each one's own. `active(asset, scope)` says whether a scope is
        in flight, for `skip_active`. A run is at most `MAX_SCOPES` tasks."""

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
        assets: dict[str, set[str]] = {}
        for target in targets:
            name = self.asset_of(target)
            selection = (
                partitions.get(target, partitions.get(name)) if isinstance(partitions, dict) else partitions
            )
            assets.setdefault(name, set()).update(self.scopes(name, selection))
        if upstream:
            queue = [(n, s) for n, scopes in assets.items() for s in scopes]
            seen = set(queue)
            while queue:
                name, scope = queue.pop()
                for edge in self.edges(name, scope):
                    if edge.owner is None:
                        continue
                    for up_scope in self.spread(edge):
                        if (edge.owner, up_scope) in seen:
                            continue
                        seen.add((edge.owner, up_scope))
                        if len(seen) > MAX_SCOPES:
                            raise ValueError(
                                f"the run spans more than {MAX_SCOPES} tasks: select fewer partitions"
                            )
                        assets.setdefault(edge.owner, set()).add(up_scope)
                        queue.append((edge.owner, up_scope))
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
            for name in assets:
                assets[name] = {s for s in assets[name] if not active(name, s)}
        if skip_missing_inputs:
            while dropped := [
                (n, s) for n, scopes in assets.items() for s in scopes if self.missing(n, s, assets)
            ]:
                for name, scope in dropped:
                    assets[name].discard(scope)
        if (skip_active or skip_missing_inputs) and not any(assets.values()):
            return None  # §9: every scope is in flight, or can't run until its inputs are written
        if sum(len(scopes) for scopes in assets.values()) > MAX_SCOPES:
            raise ValueError(f"the run spans more than {MAX_SCOPES} tasks: select fewer partitions")
        run_id = ulid(self.now)
        tasks = {}
        for name, scopes in assets.items():
            for scope in sorted(scopes):
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
            "partitions": partitions if isinstance(partitions, (str, dict)) else list(partitions),
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

    def _order(self, tasks: dict, assets: Mapping[str, set[str]]) -> None:
        """A task waits for the run's tasks it reads: the one scope an edge
        projects to, looked up; or — a fan-in — the owner's scopes in this run
        that agree with its shared keys, grouped by them once per owner and
        set of shared dimensions. Linear in tasks plus links."""

        groups: dict[tuple, dict] = {}
        for task in tasks.values():
            deps = {}
            for edge in self.edges(task["asset"], task["scope"]):
                planned = assets.get(edge.owner)
                if not planned:
                    continue
                if edge.fan_in:
                    names = tuple(sorted(edge.pinned))
                    if (edge.owner, names) not in groups:
                        grouped = groups[(edge.owner, names)] = {}
                        for up_scope in sorted(planned):
                            parts = split_partition(edge.dims, up_scope)
                            grouped.setdefault(tuple(parts[n] for n in names), []).append(up_scope)
                    ups = groups[(edge.owner, names)].get(tuple(edge.pinned[n] for n in names), ())
                else:
                    ups = (edge.scope,) if edge.scope in planned else ()
                for up_scope in ups:
                    dep_id = f"{task['run']}/{edge.owner}:{up_scope}"
                    if dep_id != task["id"]:
                        deps[dep_id] = None
            if deps:
                task["deps"] = list(deps)
                task["status"], task["queued_at"] = "waiting", None


def same_dim(a: dict, b: dict) -> bool:
    if a["kind"] != b["kind"]:
        return False
    if a["kind"] == "set":
        return a["output"] == b["output"]
    return a == b


def dims_of(asset: dict) -> dict:
    return (asset.get("partitions") or {}).get("dims") or {}
