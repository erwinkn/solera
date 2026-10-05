"""Planning (§7, §8): which partitions a request selects, what each partition reads
upstream, and the run a request becomes — synchronous domain code over an
explicit view: the manifest, the heads (with any heads a sensor's commits
are about to install over them) and the time. No engine state, no I/O,
nothing awaited.

A selection is answered without enumerating the partition domain unless it
asks for the whole of it: one explicit partition checks each of its parts'
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

MAX_PARTITIONS = 100_000  # partitions a request may enumerate


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


def dim_keys(dim: dict, now: dt.datetime, partitions: Callable[[str], list[str] | None]) -> list[str]:
    """One dimension's current keys, enumerated."""

    if dim["kind"] == "static":
        return [str(k) for k in dim["keys"]]
    if dim["kind"] == "time":
        return time_partitions(dim).keys(now, MAX_PARTITION_KEYS)
    return sorted(partitions(dim["output"]) or ())


def _size(dim: dict, now: dt.datetime, partitions) -> int:
    if dim["kind"] == "static":
        return len(dim["keys"])
    if dim["kind"] == "time":
        return time_partitions(dim).count(now, MAX_PARTITIONS)
    return len(partitions(dim["output"]) or ())


def size(dims: dict, now: dt.datetime, partitions) -> int:
    """How many partitions `dims` spans, counted per dimension — never enumerated.
    A cron time dimension is counted only to just past `MAX_SCOPES`."""

    total = 1
    for dim in dims.values():
        total *= _size(dim, now, partitions)
    return total


def enumerate_partitions(
    dims: dict, now: dt.datetime, partitions, *, pinned: Mapping[str, str] | None = None, what: str = ""
) -> list[str]:
    """Every partition of `dims` — those in `pinned` held at one key each — in
    dimension order: counted first, refused past `MAX_SCOPES`."""

    if not dims:
        return [""]
    pinned = pinned or {}
    free = {name: dim for name, dim in dims.items() if name not in pinned}
    total = size(free, now, partitions)
    if total > MAX_PARTITIONS:
        raise ValueError(
            f"{what or 'the selection'} spans {total} partitions, more than {MAX_PARTITIONS}: "
            "select partitions explicitly"
        )
    keys = [dim_keys(dim, now, partitions) for dim in free.values()]
    return [
        canonical_partition(dims, {**pinned, **dict(zip(free, combo, strict=True))})
        for combo in product(*keys)
    ]


def membership(dims: dict, now: dt.datetime, partitions) -> Callable[[str], bool]:
    """Whether a partition is one `enumerate_scopes` would list: canonical, each
    of its parts a member of its dimension — checked part by part, never by
    enumeration. Each dimension's members are read once, for any number of
    partitions."""

    if not dims:
        return lambda partition: partition == ""
    checks = {}
    for name, dim in dims.items():
        if dim["kind"] == "time":
            checks[name] = partial(time_partitions(dim).contains, as_of=now)
        elif dim["kind"] == "static":
            checks[name] = {str(k) for k in dim["keys"]}.__contains__
        else:
            checks[name] = set(partitions(dim["output"]) or ()).__contains__

    def member(partition: str) -> bool:
        try:
            parts = split_partition(dims, partition)
        except ValueError:
            return False
        return canonical_partition(dims, parts) == partition and all(
            check(parts[name]) for name, check in checks.items()
        )

    return member


def select_partitions(
    dims: dict,
    selection,
    *,
    now: dt.datetime,
    partitions: Callable[[str], list[str] | None],
    missing: Callable[[str], bool],
    what: str = "",
) -> list[str]:
    """The partitions `selection` names: `"all"` (or None), `"latest"`,
    `"missing"`, or explicit keys. `elements(output)` gives a set
    dimension's members; `missing(partition)` whether a partition lacks a complete
    head."""

    if selection is None or selection == "all":
        return enumerate_partitions(dims, now, partitions, what=what)
    if selection == "missing":
        return [s for s in enumerate_partitions(dims, now, partitions, what=what) if missing(s)]
    if selection == "latest":  # each time dimension at its latest window; the others in full
        pinned = {}
        for name, dim in dims.items():
            if dim["kind"] == "time":
                if (latest := time_partitions(dim).latest(now)) is None:
                    return []
                pinned[name] = latest
        return enumerate_partitions(dims, now, partitions, pinned=pinned, what=what)
    wanted = list(dict.fromkeys(selection or ()))
    if not dims:
        return [""] if "" in wanted else []
    member, out, seen = membership(dims, now, partitions), [], set()
    for key in wanted:
        try:
            partition = canonical(dims, key)
        except (ValueError, KeyError):
            continue
        if partition not in seen and member(partition):  # two spellings may name one partition
            seen.add(partition)
            out.append(partition)
    return out


class UpstreamOnly(ValueError):
    """An incremental input over an upstream dimension its consumer lacks
    (§7): a broadcast incremental read is undefined."""


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


@dataclass(frozen=True)
class Input:
    """One read of a partition (§5, §7): an input, a dep, or the dep a
    partition-set dimension implies (`set_dim`: lineage, never the
    definition). Of the owner's dimensions `dims`, the consumer shares
    `pinned` — at its partition's keys — and lacks `free`. A fan-in — a
    whole input or a dep with free dimensions, or one that reads
    `all_partitions`, every dimension free — reads the heads that exist
    across them; any other input reads its one projected `partition`."""

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
        return bool(self.free) or bool(self.spec.get("all_partitions"))

    @property
    def partition(self) -> str | None:
        """The one upstream partition it reads; None for a fan-in."""

        return None if self.fan_in else canonical_partition(self.dims, self.pinned) if self.dims else ""

    def key(self, upstream_partition: str) -> str:
        """A fan-in head's key: its parts on the collapsed dimensions — the
        whole partition, with `all_partitions`; "" for an unpartitioned upstream."""

        if not self.dims:
            return ""
        parts = split_partition(self.dims, upstream_partition)
        return canonical_partition(self.free, {name: parts[name] for name in self.free})


class Planner:
    """Planning over one view: `manifest`; `head(output, partition)` and
    `heads_of(output)` — the committed heads, with `projected` heads (what a
    sensor's commits will install) over them; `drained(asset, partition)`,
    whether a partition's last commit finished its pass; and `now` (epoch seconds). The view is read as
    of each call; what a call derives from it (heads by output, set members)
    is kept for the planner's life — one operation's."""

    def __init__(
        self,
        manifest: dict,
        head: Callable[[str, str], dict | None],
        heads_of: Callable[[str], Iterable[tuple[str, dict]]],
        now: float,
        projected: Mapping[tuple[str, str], dict] | None = None,
        complete: Callable[[str, str], bool] = lambda asset, partition: False,
    ):
        self.manifest, self.now = manifest, now
        self.projected = dict(projected or {})
        self._head, self._heads_of, self._complete = head, heads_of, complete
        self.time = dt.datetime.fromtimestamp(now, dt.UTC)
        self._groups: dict[tuple, dict] = {}

    # -- heads -------------------------------------------------------------------

    def head(self, output: str, partition: str) -> dict | None:
        found = self.projected.get((output, partition))
        return found if found is not None else self._head(output, partition)

    def heads_of(self, output: str) -> dict[str, dict]:
        """Every head of `output`, by partition: what exists, never the domain."""

        heads = dict(self._heads_of(output))
        heads.update({partition: h for (o, partition), h in self.projected.items() if o == output})
        return heads

    def complete(self, asset: str, partition: str) -> bool:
        """Whether the partition's last commit ended its run's walk."""

        return self._complete(asset, partition)

    def materialized(self, asset: str, partition: str) -> bool:
        """Whether a partition is complete (§7): each of its outputs has a head,
        and its last commit ended its run's walk — however many of them its
        last batches wrote.
        A job, which has no output, once a run of it succeeded. The one answer
        for selection, fan-in and the views."""

        outputs = self.manifest["assets"][asset]["outputs"]
        return all(self.head(o["name"], partition) is not None for o in outputs) and self.complete(
            asset, partition
        )

    def head_materialized(self, output: str, partition: str) -> bool:
        """Whether an output's head at `partition` is of a complete walk: a
        source's always is."""

        owner = self.owner(output)
        return owner is None or self.complete(owner, partition)

    def dynamic_partitions(self, output: str) -> list[str] | None:
        """A set dimension's current keys: the element list its head carries (§7)."""

        head = self.head(output, "")
        return None if head is None else [str(e) for e in head.get("partitions") or ()]

    # -- dimensions --------------------------------------------------------------

    def dims(self, asset: str | None) -> dict:
        if asset is None:
            return {}
        return dims_of(self.manifest["assets"][asset])

    def owner(self, output: str) -> str | None:
        return self.manifest["outputs"][output].get("asset")

    def dim_keys(self, dims: dict) -> list[list[str]]:
        return [dim_keys(dim, self.time, self.dynamic_partitions) for dim in dims.values()]

    def shared(self, consumer: dict, consumer_partition: str, upstream_dims: dict) -> tuple[dict, dict]:
        """`(pinned, free)`: the upstream dimensions the consumer shares, at its
        partition's keys, and those it lacks."""

        c_dims = dims_of(consumer)
        parts = split_partition(c_dims, consumer_partition) if c_dims else {}
        pinned, free = {}, {}
        for name, dim in upstream_dims.items():
            match = next((cn for cn, cd in c_dims.items() if same_dim(dim, cd)), None)
            if match is not None:
                pinned[name] = parts[match]
            else:
                free[name] = dim
        return pinned, free

    def project_downstream(self, producer: str | None, partition: str, target: str) -> dict[str, str]:
        """Shared dims pinned by a changed partition of `producer`; the target's
        others are left for the caller to expand (§7, §9). A source (`None`)
        pins none."""

        p_dims, t_dims = self.dims(producer), self.dims(target)
        if not p_dims or not t_dims:
            return {}
        parts = split_partition(p_dims, partition)
        pinned = {}
        for t_name, t_dim in t_dims.items():
            for p_name, p_dim in p_dims.items():
                if same_dim(t_dim, p_dim):
                    pinned[t_name] = parts[p_name]
                    break
        return pinned

    def visible(self, producer: str | None, partition: str, target: str) -> bool:
        """Whether `target` can read a change of `producer` at `partition` yet:
        a whole fan-in reads complete passes only, so a change made by a
        pass still under way is not, until that pass drains."""

        if producer is None:
            return True
        inputs = self.manifest["assets"][target]["inputs"].values()
        t_dims, p_dims = self.dims(target), self.dims(producer)
        lacks = any(not any(same_dim(d, td) for td in t_dims.values()) for d in p_dims.values())
        whole = any(
            e["kind"] == "in" and self.owner(e["output"]) == producer and (lacks or e.get("all_partitions"))
            for e in inputs
        )
        return not whole or self.complete(producer, partition)

    def reach(self, producer: str | None, partition: str, target: str) -> list[str]:
        """The target partitions a change of `producer` at `partition` reaches (§7,
        §9): the dimensions it shares pinned, the others — every one, for a
        source — over their current keys. Bounded by `MAX_SCOPES`."""

        pinned = self.project_downstream(producer, partition, target)
        return enumerate_partitions(
            self.dims(target),
            self.time,
            self.dynamic_partitions,
            pinned=pinned,
            what=f"a change reaching {target}",
        )

    # -- inputs -------------------------------------------------------------------

    def inputs(self, asset: str, partition: str) -> list[Input]:
        """What (asset, partition) reads: its inputs, its deps, then the partition
        sets its dimensions are bound to. A whole input or a dep fans in over
        the upstream dimensions the consumer lacks — over all of them with
        `all_partitions` —; an incremental input may lack none
        (`UpstreamOnly`): a broadcast incremental read is undefined."""

        info = self.manifest["assets"][asset]
        every = set(info.get("deps_all_partitions") or ())
        named = list(info["inputs"].items()) + [
            (d, {"kind": "dep", "output": d, **({"all_partitions": True} if d in every else {})})
            for d in info["deps"]
        ]
        outputs = {spec["output"] for _, spec in named}
        named += [
            (d["output"], {"kind": "dep", "output": d["output"], "set_dim": True})
            for d in self.dims(asset).values()
            if d["kind"] == "dynamic" and d["output"] not in outputs
        ]
        out = []
        for param, spec in named:
            owner = self.owner(spec["output"])
            dims = self.dims(owner)
            if spec.get("all_partitions"):  # every upstream partition: no projection
                pinned, free = {}, dict(dims)
            else:
                pinned, free = self.shared(info, partition, dims)
            if free and spec["kind"] == "incremental":
                raise UpstreamOnly(
                    f"upstream-only dimension {next(iter(free))!r}: an incremental input cannot read across it"
                )
            out.append(
                Input(
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

    def fan_in(self, input: Input, *, materialized: bool) -> dict[str, dict]:
        """The heads a fan-in reads, by upstream partition: among those that exist,
        the current partitions — a retired one's head is kept, never read —
        that agree with its shared keys (`complete` ones only, for
        a whole input). Never by expanding the domain: the owner's heads are
        grouped by their shared keys once per planner."""

        names = tuple(sorted(input.pinned))
        groups = self._groups.get((input.output, names))
        if groups is None:
            member, groups = membership(input.dims, self.time, self.dynamic_partitions), {}
            for upstream_partition, head in sorted(self.heads_of(input.output).items()):
                if member(upstream_partition):
                    parts = split_partition(input.dims, upstream_partition) if input.dims else {}
                    groups.setdefault(tuple(parts[n] for n in names), {})[upstream_partition] = head
            self._groups[(input.output, names)] = groups
        heads = groups.get(tuple(input.pinned[n] for n in names)) or {}
        return {s: h for s, h in heads.items() if not materialized or self.head_materialized(input.output, s)}

    def spread(self, input: Input) -> list[str]:
        """Every upstream partition an input could read: for a fan-in, the domain
        across its free dimensions, enumerated — only to build upstream work,
        and bounded by `MAX_SCOPES`."""

        if not input.fan_in:
            return [input.partition]
        return enumerate_partitions(
            input.dims, self.time, self.dynamic_partitions, pinned=input.pinned, what="an upstream build"
        )

    def missing(self, asset: str, partition: str, planned: Mapping[str, Collection[str]]) -> bool:
        """Whether (asset, partition) reads an input never written that the run
        doesn't build — preparing it would fail. A fan-in reads what there is,
        so it is missing only when there is nothing: no current upstream head
        agrees with its shared keys (a complete one, for a whole input)."""

        for input in self.inputs(asset, partition):
            built = planned.get(input.owner) or () if input.owner is not None else ()
            if input.fan_in:
                if any(self._agrees(input, s) for s in built):
                    continue
                if not self.fan_in(input, materialized=input.kind == "in"):
                    return True
            elif input.partition not in built:
                source = (
                    input.owner is None and input.output in self.manifest["sources"] and input.partition == ""
                )
                if not source and self.head(input.output, input.partition) is None:
                    return True
        return False

    @staticmethod
    def _agrees(input: Input, upstream_partition: str) -> bool:
        parts = split_partition(input.dims, upstream_partition) if input.dims else {}
        return all(parts.get(name) == value for name, value in input.pinned.items())

    def partitions(self, asset: str, selection) -> list[str]:
        return select_partitions(
            self.dims(asset),
            selection,
            now=self.time,
            partitions=self.dynamic_partitions,
            missing=lambda partition: not self.materialized(asset, partition),
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
        skips leave nothing. `partitions` selects every target's partitions, or —
        a map — each one's own. `active(asset, partition)` says whether a partition is
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
            assets.setdefault(name, set()).update(self.partitions(name, selection))
        if upstream:
            queue = [(n, s) for n, held in assets.items() for s in held]
            seen = set(queue)
            while queue:
                name, partition = queue.pop()
                for input in self.inputs(name, partition):
                    if input.owner is None:
                        continue
                    for upstream_partition in self.spread(input):
                        if (input.owner, upstream_partition) in seen:
                            continue
                        seen.add((input.owner, upstream_partition))
                        if len(seen) > MAX_PARTITIONS:
                            raise ValueError(
                                f"the run spans more than {MAX_PARTITIONS} tasks: select fewer partitions"
                            )
                        assets.setdefault(input.owner, set()).add(upstream_partition)
                        queue.append((input.owner, upstream_partition))
        if keys:
            incremental_outputs = {
                e["output"]
                for name in assets
                for e in self.manifest["assets"][name]["inputs"].values()
                if e["kind"] == "incremental"
            }
            unknown = set(keys) - incremental_outputs
            if unknown:
                raise ValueError(f"keys= names no Incremental input: {sorted(unknown)}")
            for output, override in keys.items():
                if override != "all" and not (
                    isinstance(override, list) and all(isinstance(k, str) for k in override)
                ):
                    raise ValueError(
                        f"keys= takes, per input, a list of keys or 'all', not {override!r} for {output!r}"
                        " (a full run is mode='full')"
                    )
                if self.manifest["outputs"][output].get("key") is None:
                    raise ValueError(f"keys= selects keys of {output!r}, which has none")
                if mode == "full":
                    raise ValueError(
                        "keys= selects what a run reads without starting over: not in a full run"
                    )
        if skip_active:
            for name in assets:
                assets[name] = {s for s in assets[name] if not active(name, s)}
        if skip_missing_inputs:
            while dropped := [
                (n, s) for n, held in assets.items() for s in held if self.missing(n, s, assets)
            ]:
                for name, partition in dropped:
                    assets[name].discard(partition)
        if (skip_active or skip_missing_inputs) and not any(assets.values()):
            return None  # §9: every partition is in flight, or can't run until its inputs are written
        if sum(len(held) for held in assets.values()) > MAX_PARTITIONS:
            raise ValueError(f"the run spans more than {MAX_PARTITIONS} tasks: select fewer partitions")
        run_id = ulid(self.now)
        tasks = {}
        for name, held in assets.items():
            for partition in sorted(held):
                task_id = f"{run_id}/{name}:{partition}"
                tasks[task_id] = {
                    "id": task_id,
                    "run": run_id,
                    "asset": name,
                    "partition": partition,
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
        """A task waits for the run's tasks it reads: the one partition an input
        projects to, looked up; or — a fan-in — the owner's partitions in this run
        that agree with its shared keys, grouped by them once per owner and
        set of shared dimensions. Linear in tasks plus links."""

        groups: dict[tuple, dict] = {}
        for task in tasks.values():
            deps = {}
            for input in self.inputs(task["asset"], task["partition"]):
                planned = assets.get(input.owner)
                if not planned:
                    continue
                if input.fan_in:
                    names = tuple(sorted(input.pinned))
                    if (input.owner, names) not in groups:
                        grouped = groups[(input.owner, names)] = {}
                        for upstream_partition in sorted(planned):
                            parts = split_partition(input.dims, upstream_partition) if input.dims else {}
                            grouped.setdefault(tuple(parts[n] for n in names), []).append(upstream_partition)
                    ups = groups[(input.owner, names)].get(tuple(input.pinned[n] for n in names), ())
                else:
                    ups = (input.partition,) if input.partition in planned else ()
                for upstream_partition in ups:
                    dep_id = f"{task['run']}/{input.owner}:{upstream_partition}"
                    if dep_id != task["id"]:
                        deps[dep_id] = None
            if deps:
                task["deps"] = list(deps)
                task["status"], task["queued_at"] = "waiting", None


def same_dim(a: dict, b: dict) -> bool:
    if a["kind"] != b["kind"]:
        return False
    if a["kind"] == "dynamic":
        return a["output"] == b["output"]
    return a == b


def dims_of(asset: dict) -> dict:
    return (asset.get("partitions") or {}).get("dims") or {}
