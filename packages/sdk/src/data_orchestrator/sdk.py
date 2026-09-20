"""Declarations a project file imports. No engine code lives here (§1, §11)."""

from __future__ import annotations

import datetime as dt
import hashlib
import inspect
import json
import re
import typing
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import quote, unquote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,127}$")
DEFAULT_STORE = "json"
MAX_PARTITION_KEYS = 5000


class RegistrationError(ValueError):
    """A project declaration violates §11."""


class UnresolvedAnnotation:
    pass


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _jsonable(value: Any, where: str) -> Any:
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise RegistrationError(f"{where} must be JSON-serializable: {error}") from error
    return value


# ---------------------------------------------------------------------------
# Refs (§3)
# ---------------------------------------------------------------------------

_REF_KINDS: dict[str, type[Ref]] = {}


@dataclass(frozen=True)
class Ref:
    """A self-contained pointer into a store, with a version (§3)."""

    output: str
    store: str
    handle: Any
    version: str
    partition: str = ""
    meta: dict = field(default_factory=dict)

    kind: ClassVar[str | None] = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        _REF_KINDS[cls.kind] = cls

    def to_json(self) -> dict:
        meta = dict(self.meta)
        if self.kind:
            meta["ref"] = self.kind
        return {
            "output": self.output,
            "store": self.store,
            "handle": self.handle,
            "version": self.version,
            "partition": self.partition,
            "meta": meta,
        }

    @staticmethod
    def from_json(data: dict) -> Ref:
        meta = dict(data.get("meta") or {})
        cls = _REF_KINDS.get(meta.pop("ref", None), Ref)
        return cls(
            output=data["output"],
            store=data["store"],
            handle=data["handle"],
            version=data["version"],
            partition=data.get("partition", ""),
            meta=meta,
        )


@dataclass(frozen=True)
class JsonRef(Ref):
    kind: ClassVar[str | None] = "json"

    @property
    def object(self) -> str:
        return self.handle["object"]


@dataclass(frozen=True)
class BlobRef(Ref):
    kind: ClassVar[str | None] = "blob"

    @property
    def object(self) -> str:
        return self.handle["object"]


@dataclass(frozen=True)
class TableRef(Ref):
    """A Postgres ref: a table plus the slice and key columns (§3)."""

    kind: ClassVar[str | None] = "table"

    @property
    def table(self) -> str:
        return self.handle["table"]

    @property
    def where(self) -> dict:
        return self.handle.get("where") or {}

    @property
    def batch(self) -> int | None:
        return self.handle.get("batch")

    def where_sql(self) -> str:
        clauses = [f"{_ident(k)} = {_literal(v)}" for k, v in sorted(self.where.items())]
        if self.batch is not None:
            clauses.append(f"_batch <= {int(self.batch)}")
        return " AND ".join(clauses) or "TRUE"

    def sql(self) -> str:
        return f"SELECT * FROM {self.table} WHERE {self.where_sql()}"


def _ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return f'"{name}"'


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def is_ref_type(t: Any) -> bool:
    return inspect.isclass(t) and issubclass(t, Ref)


# ---------------------------------------------------------------------------
# Outputs and sources (§2, §5)
# ---------------------------------------------------------------------------


class Output:
    """A named slot on a store (§2)."""

    is_partition_set = False

    def __init__(
        self,
        name: str | None = None,
        store: str | None = None,
        key: str | None = None,
        revision: str | None = None,
        mode: str | None = None,
        **config: Any,
    ):
        if mode not in (None, "append"):
            raise RegistrationError(f"Output {name or '?'}: unknown mode {mode!r}")
        if mode == "append" and key is not None:
            raise RegistrationError(f"Output {name or '?'}: mode='append' cannot declare its own key (§2)")
        self.name, self.store = name, store
        self.key, self.revision, self.mode, self.config = key, revision, mode, config

    def spec(self, default_name: str) -> dict:
        name = self.name or default_name
        return {
            "name": name,
            "store": self.store or DEFAULT_STORE,
            "key": self.key,
            "revision": self.revision,
            "mode": self.mode,
            "config": self.config,
            "partition_set": self.is_partition_set,
        }


class PartitionSet(Output):
    """An output whose value is a list of partition keys (§2, §7)."""

    is_partition_set = True

    def __init__(self, name: str | None = None, store: str | None = None, **config: Any):
        super().__init__(name, store, key="<elements>", **config)


class Source:
    """An output with no producer (§5)."""

    def __init__(self, name: str, store: str | None = None, key: str | None = None, **handle: Any):
        if not NAME.fullmatch(name):
            raise RegistrationError(f"Invalid source name: {name!r}")
        self.name, self.store, self.key, self.handle = name, store, key, dict(handle)

    @classmethod
    def _from_partition_set(cls, ps: PartitionSet) -> Source:
        if ps.name is None:
            raise RegistrationError("A source PartitionSet requires an explicit name")
        source = cls(ps.name, store=ps.store, key="<elements>", **{})
        source.handle = {"name": ps.name}
        return source

    def head(self) -> Ref:
        return Ref(
            output=self.name,
            store=self.store or DEFAULT_STORE,
            handle={"name": self.name, **self.handle},
            version=digest(self.handle),
            partition="",
            meta={"external": True},
        )


class _Unset:
    def __repr__(self):
        return "UNSET"


# `cursor` absent means "leave the committed cursor"; explicit None clears it.
UNSET = _Unset()


@dataclass(frozen=True)
class Result:
    """Multi-output return: `outputs` plus an optional `cursor` (§2)."""

    outputs: dict[str, Any]
    cursor: Any = UNSET


# ---------------------------------------------------------------------------
# Edges (§5)
# ---------------------------------------------------------------------------


class In:
    """Whole value (or ref) of an output at its pinned head."""

    kind = "in"

    def __init__(self, output: str | None = None, *, meta: dict | None = None):
        self.output, self.meta = output, _jsonable(meta, "edge meta") if meta is not None else None

    def spec(self, param: str) -> dict:
        return {"kind": self.kind, "output": self.output or param, "meta": self.meta}


class ByKey(In):
    """Incremental edge: only keys whose revision changed (§5, §6)."""

    kind = "bykey"

    def __init__(self, output: str | None = None, batch_size: int = 100, meta: dict | None = None):
        super().__init__(output, meta=meta)
        if batch_size < 1:
            raise RegistrationError("ByKey batch_size must be positive")
        self.batch_size = batch_size

    def spec(self, param: str) -> dict:
        return {**super().spec(param), "batch_size": self.batch_size}


class AllPartitions(In):
    """Collapse the upstream dimensions this asset lacks (§5, §7)."""

    kind = "all_partitions"


@dataclass(frozen=True)
class Changes:
    upserted: list[str]
    deleted: list[str]


# ---------------------------------------------------------------------------
# Partition dimensions (§7)
# ---------------------------------------------------------------------------


class StaticPartitions:
    def __init__(self, keys: list[str]):
        if not keys or len(set(keys)) != len(keys):
            raise RegistrationError("StaticPartitions requires a nonempty list of unique keys")
        self.keys = [str(k) for k in keys]

    def spec(self) -> dict:
        return {"kind": "static", "keys": self.keys}

    def __eq__(self, other):
        return type(other) is StaticPartitions and other.keys == self.keys

    def __hash__(self):
        return hash(tuple(self.keys))


_DURATION = re.compile(r"^(\d+)(m|h|d|w)$")
_DURATION_FORMATS = {"m": "%Y-%m-%dT%H:%M", "h": "%Y-%m-%dT%H:00", "d": "%Y-%m-%d", "w": "%Y-%m-%d"}


def _parse_dt(value: str, zone: ZoneInfo) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=zone)
    return parsed.astimezone(zone)


def _duration(value: str) -> dt.timedelta | None:
    match = _DURATION.fullmatch(value)
    if not match:
        return None
    n, unit = int(match.group(1)), match.group(2)
    return {
        "m": dt.timedelta(minutes=n),
        "h": dt.timedelta(hours=n),
        "d": dt.timedelta(days=n),
        "w": dt.timedelta(weeks=n),
    }[unit]


class TimePartitions:
    """Half-open `[start, start+every)` windows aligned in `timezone` (§7)."""

    def __init__(
        self,
        start: str,
        every: str,
        *,
        end: str | None = None,
        end_offset: str | None = None,
        timezone: str = "UTC",
        format: str | None = None,
    ):
        try:
            self.zone = ZoneInfo(timezone)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise RegistrationError(f"Unknown partition timezone: {timezone}") from error
        self.start, self.every = start, every
        self.duration = _duration(every)
        if self.duration is None:
            try:
                croniter(every)
            except Exception as error:
                raise RegistrationError(f"Invalid TimePartitions 'every': {every!r}") from error
        if end_offset is not None and _duration(end_offset) is None:
            raise RegistrationError(f"Invalid end_offset duration: {end_offset!r}")
        self.end, self.end_offset, self.timezone = end, end_offset, timezone
        if format is None:
            if self.duration is None:
                raise RegistrationError("TimePartitions with a cron 'every' requires format=")
            format = _DURATION_FORMATS[re.fullmatch(r"\d+(m|h|d|w)", every).group(1)]
        self.format = format

    def spec(self) -> dict:
        return {
            "kind": "time",
            "start": self.start,
            "every": self.every,
            "end": self.end,
            "end_offset": self.end_offset,
            "timezone": self.timezone,
            "format": self.format,
        }

    def _window_starts(self, as_of: dt.datetime) -> list[dt.datetime]:
        start = _parse_dt(self.start, self.zone)
        horizon = as_of.astimezone(self.zone)
        if self.end_offset:
            horizon -= _duration(self.end_offset)
        if self.end:
            horizon = min(horizon, _parse_dt(self.end, self.zone))
        if horizon <= start:
            return []
        starts = []
        if self.duration is not None:
            current = start
            while current + self.duration <= horizon and len(starts) < MAX_PARTITION_KEYS:
                starts.append(current)
                current += self.duration
        else:
            # Calendar slices: a window closes when the next cron fire passes.
            fires = croniter(self.every, start)
            previous = start
            while len(starts) < MAX_PARTITION_KEYS:
                nxt = fires.get_next(dt.datetime)
                if nxt > horizon:
                    break
                starts.append(previous)
                previous = nxt
        return starts

    def key(self, window_start: dt.datetime) -> str:
        return window_start.strftime(self.format)

    def window(self, key: str) -> tuple[str, str] | None:
        naive = dt.datetime.strptime(key, self.format).replace(tzinfo=self.zone)
        if self.duration is not None:
            return naive.isoformat(), (naive + self.duration).isoformat()
        nxt = croniter(self.every, naive).get_next(dt.datetime)
        return naive.isoformat(), nxt.isoformat()

    def keys(self, as_of: dt.datetime | None = None) -> list[str]:
        as_of = as_of or dt.datetime.now(dt.UTC)
        return [self.key(s) for s in self._window_starts(as_of)]

    def latest(self, as_of: dt.datetime | None = None) -> str | None:
        keys = self.keys(as_of)
        return keys[-1] if keys else None

    def __eq__(self, other):
        return type(other) is TimePartitions and other.spec() == self.spec()

    def __hash__(self):
        return hash(digest(self.spec()))


# ---------------------------------------------------------------------------
# Triggers and automations (§9)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Every:
    seconds: int

    def __post_init__(self):
        if not isinstance(self.seconds, (int, float)) or self.seconds < 1:
            raise RegistrationError("Every requires an interval of at least 1 second")

    def spec(self) -> dict:
        return {"kind": "every", "seconds": self.seconds}


@dataclass(frozen=True)
class Cron:
    expression: str
    timezone: str = "UTC"

    def __post_init__(self):
        try:
            croniter(self.expression)
        except Exception as error:
            raise RegistrationError(f"Invalid cron expression: {self.expression}") from error
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise RegistrationError(f"Unknown cron timezone: {self.timezone}") from error

    def spec(self) -> dict:
        return {"kind": "cron", "expression": self.expression, "timezone": self.timezone}


@dataclass(frozen=True)
class OnChange:
    outputs: tuple[str, ...] = ()

    def __init__(self, *outputs: str):
        object.__setattr__(self, "outputs", tuple(outputs))

    def spec(self) -> dict:
        return {"kind": "onchange", "outputs": list(self.outputs)}


class Automation:
    """When `trigger` fires, submit this run in §8 vocabulary."""

    def __init__(
        self,
        name: str | None = None,
        targets: Any = None,
        trigger: Every | Cron | OnChange | None = None,
        *,
        enabled: bool = True,
        partitions: str | list[str] | None = None,
        mode: str = "incremental",
        upstream: bool = False,
        config: dict | None = None,
        keys: dict | None = None,
    ):
        if trigger is None:
            raise RegistrationError("Automation() requires a trigger (§11)")
        if not isinstance(trigger, (Every, Cron, OnChange)):
            raise RegistrationError(f"Unknown trigger: {trigger!r}")
        if mode not in ("incremental", "recompute"):
            raise RegistrationError(f"Unknown automation mode: {mode!r}")
        self.name, self.targets, self.trigger = name, targets, trigger
        self.enabled, self.partitions, self.mode = enabled, partitions, mode
        self.upstream, self.config, self.keys = upstream, config, keys


def AutoRefresh(**kwargs) -> Automation:
    """OnChange over the asset's inputs + deps (§9)."""

    return Automation(trigger=OnChange(), **kwargs)


@dataclass(frozen=True)
class Retry:
    n: int = 3
    delay: float = 1.0
    backoff: str = "exponential"

    def __post_init__(self):
        if self.n < 0 or self.delay < 0 or self.backoff not in ("exponential", "none"):
            raise RegistrationError(f"Invalid retry policy: {self}")

    def wait(self, attempt: int) -> float:
        return self.delay * (2**attempt if self.backoff == "exponential" else 1)

    def spec(self) -> dict:
        return {"n": self.n, "delay": self.delay, "backoff": self.backoff}


# ---------------------------------------------------------------------------
# Partition key encoding (§7)
# ---------------------------------------------------------------------------


def canonical_partition(dims: dict[str, dict], parts: Mapping[str, str]) -> str:
    """`"Richmond"`, or `"day=2024-01-01,site=Richmond"` sorted by dimension."""

    if len(dims) == 1:
        (name,) = dims
        return str(parts[name])
    return ",".join(f"{name}={quote(str(parts[name]), safe='')}" for name in sorted(dims))


def split_partition(dims: dict[str, dict], key: str) -> dict[str, str]:
    if len(dims) == 1:
        (name,) = dims
        return {name: key}
    parts = {}
    for segment in key.split(","):
        name, _, value = segment.partition("=")
        parts[name] = unquote(value)
    if set(parts) != set(dims):
        raise ValueError(f"Partition key {key!r} does not match dimensions {sorted(dims)}")
    return parts


# ---------------------------------------------------------------------------
# Assets (§2)
# ---------------------------------------------------------------------------


class Asset:
    def __init__(
        self,
        fn: Callable,
        *,
        outputs: Any = None,
        inputs: dict | None = None,
        deps: tuple | list = (),
        partitions: Any = None,
        executor: Any = None,
        retries: Retry | None = None,
        timeout: float = 3600,
        version: str = "1",
        on_version_change: str = "fail",
        automations: Any = (),
    ):
        self.fn = fn
        self.name = fn.__name__
        if outputs is None:
            outputs = (Output(fn.__name__),)
        elif isinstance(outputs, Output):
            outputs = (outputs,)
        self.outputs = tuple(outputs)
        self.inputs = inputs
        self.deps = tuple(deps)
        self.partitions = partitions
        self.executor = executor
        self.retries = retries or Retry()
        self.timeout = timeout
        self.version = str(version)
        if on_version_change not in ("fail", "recompute"):
            raise RegistrationError(f"{self.name}: on_version_change must be 'fail' or 'recompute'")
        self.on_version_change = on_version_change
        if isinstance(automations, Automation) or automations.__class__ in (Every, Cron, OnChange):
            automations = (automations,)
        self.automations = tuple(
            a if isinstance(a, Automation) else Automation(trigger=a) for a in automations
        )

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)


def asset(fn=None, **decl):
    def wrap(f):
        return Asset(f, **decl)

    return wrap(fn) if fn is not None else wrap


def job(fn=None, **decl):
    decl["outputs"] = ()
    return asset(fn, **decl) if fn is not None else asset(**decl)


# ---------------------------------------------------------------------------
# Type introspection for the manifest (§11)
# ---------------------------------------------------------------------------


def type_name(t: Any) -> Any:
    if t is None or t is inspect.Parameter.empty:
        return None
    origin = typing.get_origin(t)
    if origin is not None:
        return {
            "generic": getattr(origin, "__name__", str(origin)),
            "args": [type_name(a) for a in typing.get_args(t)],
        }
    if inspect.isclass(t):
        return {"class": f"{t.__module__}.{t.__qualname__}"}
    return {"repr": str(t)}


def hints(fn: Callable) -> dict[str, Any]:
    try:
        return typing.get_type_hints(fn)
    except Exception:
        return {}


def _dict_arg(t: Any) -> Any | None:
    """For `dict[str, X]` annotations return X, else None."""

    if typing.get_origin(t) in (dict, dict):
        args = typing.get_args(t)
        if len(args) == 2:
            return args[1]
    return None


# ---------------------------------------------------------------------------
# Project (§11)
# ---------------------------------------------------------------------------


class Project:
    def __init__(
        self,
        assets: list[Asset] | None = None,
        sources: list[Source | PartitionSet] | None = None,
        *,
        stores: dict[str, Any] | None = None,
        executors: list | None = None,
        resources: dict[str, Any] | None = None,
        automations: list[Automation] | None = None,
        name: str = "default",
    ):
        from .stores import JsonStore

        self.name = name
        self.assets: dict[str, Asset] = {}
        self.stores = {DEFAULT_STORE: JsonStore(), **(stores or {})}
        self.executors = list(executors or [])
        self.resources = dict(resources or {})
        self.sources: dict[str, Source] = {}
        for source in sources or ():
            if isinstance(source, PartitionSet):
                source = Source._from_partition_set(source)
            if source.name in self.sources:
                raise RegistrationError(f"Duplicate source: {source.name}")
            self.sources[source.name] = source
        for a in assets or ():
            if not isinstance(a, Asset):
                raise RegistrationError(f"Not an asset: {a!r}")
            if a.name in self.assets:
                raise RegistrationError(f"Duplicate asset: {a.name}")
            self.assets[a.name] = a
        self.automations = list(automations or ())
        self.manifest = self._build()

    @classmethod
    def from_package(cls, package: str, **kwargs) -> Project:
        import importlib

        module = importlib.import_module(package)
        found = [v for v in vars(module).values() if isinstance(v, Asset)]
        return cls(assets=found, **kwargs)

    # -- registration -----------------------------------------------------

    def _dim_spec(self, decl, asset_name) -> dict[str, dict] | None:
        if decl is None:
            return None
        named = decl if isinstance(decl, dict) else {"_": decl}
        dims = {}
        for dim_name, dim in named.items():
            name = dim_name
            if isinstance(dim, StaticPartitions | TimePartitions):
                spec = dim.spec()
            elif isinstance(dim, PartitionSet):
                if dim.name is None and dim_name == "_":
                    raise RegistrationError(f"{asset_name}: a PartitionSet dimension requires a name")
                spec = {"kind": "set", "output": dim.name}
                if dim_name == "_":
                    name = dim.name
            elif isinstance(dim, Asset):
                if len(dim.outputs) != 1:
                    raise RegistrationError(
                        f"{asset_name}: partitions= asset {dim.name} must have exactly one output"
                    )
                spec = {"kind": "set", "output": dim.outputs[0].name or dim.name}
                if dim_name == "_":
                    name = spec["output"]
            elif isinstance(dim, str):
                spec = {"kind": "set", "output": dim}
                if dim_name == "_":
                    name = dim
            else:
                raise RegistrationError(f"{asset_name}: unknown partition dimension {dim!r}")
            if name in dims:
                raise RegistrationError(f"{asset_name}: duplicate partition dimension {name}")
            dims[name] = spec
        return dims

    def _output_table(self) -> dict[str, dict]:
        """output name -> declaration record used by registration checks."""

        table = {}
        for name, source in self.sources.items():
            table[name] = {
                "name": name,
                "asset": None,
                "source": True,
                "store": source.store or DEFAULT_STORE,
                "key": source.key,
                "revision": None,
                "mode": None,
                "config": source.handle,
                "partition_set": source.key == "<elements>",
                "dims": None,
            }
        for asset in self.assets.values():
            default = asset.name
            seen = set()
            for output in asset.outputs:
                output.name = output.name or default
                if output.name in table or output.name in seen:
                    raise RegistrationError(f"Duplicate output name: {output.name}")
                seen.add(output.name)
                if output.name in self.resources:
                    raise RegistrationError(f"Output name collides with a resource: {output.name}")
                if output.store and output.store not in self.stores:
                    raise RegistrationError(f"{asset.name}: unknown store {output.store!r}")
                table[output.name] = {
                    "name": output.name,
                    "asset": asset.name,
                    "source": False,
                    "store": output.store or DEFAULT_STORE,
                    "key": output.key,
                    "revision": output.revision,
                    "mode": output.mode,
                    "config": output.config,
                    "partition_set": output.is_partition_set,
                    "output": output,
                }
        return table

    def _edge(self, value, param, asset_name) -> In:
        if isinstance(value, str):
            value = In(value)
        if type(value) not in (In, ByKey, AllPartitions):
            raise RegistrationError(
                f"{asset_name}: inputs[{param!r}] must be a str or one of In/ByKey/AllPartitions"
            )
        return value

    def _build(self) -> dict:
        for store_name in self.stores:
            if not NAME.fullmatch(store_name):
                raise RegistrationError(f"Invalid store name: {store_name!r}")
        outputs = self._output_table()

        # Resolve every asset's inputs/deps/partitions and validate edges.
        assets = {}
        for asset in self.assets.values():
            name = asset.name
            signature = inspect.signature(asset.fn)
            params = signature.parameters
            if any(p.kind in (p.POSITIONAL_ONLY, p.VAR_POSITIONAL, p.VAR_KEYWORD) for p in params.values()):
                raise RegistrationError(f"{name}: parameters must be named and keyword-bindable")
            declared = (
                asset.inputs
                if asset.inputs is not None
                else {p: In() for p in params if p != "ctx" and p not in self.resources}
            )
            edges = {param: self._edge(v, param, name) for param, v in declared.items()}
            for param in edges:
                if param not in params:
                    raise RegistrationError(f"{name}: input {param!r} is not a producer parameter")
                if param in self.resources or param == "ctx":
                    raise RegistrationError(f"{name}: input {param!r} collides with a resource/ctx")
            for param, p in params.items():
                if (
                    param not in edges
                    and param not in self.resources
                    and param != "ctx"
                    and p.default is p.empty
                ):
                    raise RegistrationError(f"{name}: parameter {param!r} has no input or resource binding")
            deps = list(asset.deps)
            for dep in deps:
                if not isinstance(dep, str) or dep not in outputs:
                    raise RegistrationError(f"{name}: deps entry {dep!r} names an unknown output")
                if dep in (e.output or p for p, e in edges.items()):
                    raise RegistrationError(f"{name}: dep {dep!r} is already a bound input")
            dims = self._dim_spec(asset.partitions, name)
            assets[name] = {"edges": edges, "deps": deps, "dims": dims}

        # Edge validity: output exists, projection rule, store checks.
        hints_by_asset = {n: hints(a.fn) for n, a in self.assets.items()}
        for name, info in assets.items():
            asset = self.assets[name]
            dims = info["dims"] or {}
            for param, edge in info["edges"].items():
                output_name = edge.output or param
                if output_name not in outputs:
                    raise RegistrationError(f"{name}: input {param!r} names unknown output {output_name!r}")
                upstream = outputs[output_name]
                store = self.stores[upstream["store"]]
                annotation = hints_by_asset[name].get(param)
                up_dims = self._output_dims(output_name, outputs, assets)
                missing = set(up_dims) - set(dims)
                shared = set(up_dims) & set(dims)
                for d in shared:
                    if up_dims[d] != dims[d]:
                        raise RegistrationError(
                            f"{name}: dimension {d!r} differs from upstream {output_name}"
                        )
                if missing and not isinstance(edge, AllPartitions):
                    raise RegistrationError(
                        f"{name}: {output_name} has upstream-only dimensions {sorted(missing)}; "
                        "collapse them with AllPartitions() (§7)"
                    )
                if isinstance(edge, ByKey):
                    if missing:
                        raise RegistrationError(
                            f"{name}: ByKey edge {param!r} cannot have upstream-only dimensions (§7)"
                        )
                    if upstream["key"] is None:
                        raise RegistrationError(
                            f"{name}: ByKey edge {param!r} upstream {output_name} declares no key (§2)"
                        )
                    if is_ref_type(annotation):
                        raise RegistrationError(f"{name}: ByKey edge {param!r} cannot be ref-annotated (§5)")
                    if annotation is None:
                        raise RegistrationError(f"{name}: store-bound input {param!r} is unannotated (§11)")
                    if not store.can_load(annotation, _keys_class()):
                        raise RegistrationError(
                            f"{name}: store {upstream['store']} cannot load {annotation} under Keys"
                        )
                elif isinstance(edge, AllPartitions):
                    inner = _dict_arg(annotation)
                    if inner is None:
                        raise RegistrationError(
                            f"{name}: AllPartitions input {param!r} must be annotated dict[str, T] (§5)"
                        )
                    if is_ref_type(inner):
                        if not store.can_load(inner, None):
                            raise RegistrationError(
                                f"{name}: store {upstream['store']} does not hand out {inner.__name__}"
                            )
                    elif not store.can_load(inner, None):
                        raise RegistrationError(f"{name}: store {upstream['store']} cannot load {inner}")
                else:  # In
                    if is_ref_type(annotation):
                        if not store.can_load(annotation, None):
                            raise RegistrationError(
                                f"{name}: store {upstream['store']} does not hand out {annotation.__name__}"
                            )
                    elif annotation is None:
                        raise RegistrationError(f"{name}: store-bound input {param!r} is unannotated (§11)")
                    elif not store.can_load(annotation, None):
                        raise RegistrationError(f"{name}: store {upstream['store']} cannot load {annotation}")
            # Output/store checks.
            return_t = hints_by_asset[name].get("return")
            partitioned = bool(info["dims"])
            for output in asset.outputs:
                record = outputs[output.name]
                store = self.stores[record["store"]]
                t = return_t if len(asset.outputs) == 1 else None
                if not store.can_store(t, output):
                    raise RegistrationError(
                        f"{name}: store {record['store']} cannot store output {output.name} "
                        f"(type {t}, config {output.config})"
                    )
                if (
                    partitioned
                    and getattr(store, "shared_table", False)
                    and "partition_column" not in output.config
                ):
                    raise RegistrationError(
                        f"{name}: partitioned output {output.name} on shared-table store "
                        f"{record['store']} lacks partition_column (§3)"
                    )
            for spec in (info["dims"] or {}).values():
                if spec["kind"] == "set":
                    target = outputs.get(spec["output"])
                    if target is None:
                        raise RegistrationError(f"{name}: partitions= names unknown output {spec['output']}")
                    if target["key"] is None:
                        raise RegistrationError(
                            f"{name}: partitions= output {spec['output']} has no key (§7)"
                        )

        # DAG check over inputs + deps.
        visiting, visited = set(), set()

        def visit(n):
            if n in visiting:
                raise RegistrationError("Asset dependency cycle (§12)")
            if n in visited:
                return
            visiting.add(n)
            for output_name in [e.output or p for p, e in assets[n]["edges"].items()] + assets[n]["deps"]:
                owner = outputs[output_name]["asset"]
                if owner:
                    visit(owner)
            visiting.remove(n)
            visited.add(n)

        for n in assets:
            visit(n)

        # Automations.
        automation_records = {}
        trigger_name = {Every: "every", Cron: "cron", OnChange: "onchange"}

        def add_automation(auto: Automation, *, targets: list[str], attached: Asset | None):
            name = auto.name
            if attached is not None:
                kind = trigger_name[type(auto.trigger)]
                index = sum(1 for n in automation_records if n.startswith(f"{attached.name}.{kind}"))
                name = f"{attached.name}.{kind}.{index}"
            elif not name or not targets:
                raise RegistrationError("A standalone automation requires name and targets (§9)")
            if name in automation_records or not NAME.fullmatch(name):
                raise RegistrationError(f"Invalid or duplicate automation name: {name}")
            for target in targets:
                if target not in self.assets:
                    raise RegistrationError(f"Automation {name}: unknown target {target}")
            watched = []
            if isinstance(auto.trigger, OnChange):
                watched = list(auto.trigger.outputs)
                if not watched:
                    for target in targets:
                        info = assets[target]
                        watched += [e.output or p for p, e in info["edges"].items()] + info["deps"]
                    watched = sorted(set(watched))
                own = {o.name for t in targets for o in self.assets[t].outputs}
                if set(watched) & own:
                    raise RegistrationError(f"Automation {name}: OnChange cannot name its own outputs")
                for w in watched:
                    if w not in outputs:
                        raise RegistrationError(f"Automation {name}: OnChange names unknown output {w}")
            automation_records[name] = {
                "name": name,
                "targets": targets,
                "trigger": auto.trigger.spec(),
                "enabled": bool(auto.enabled),
                "partitions": auto.partitions,
                "mode": auto.mode,
                "upstream": bool(auto.upstream),
                "config": auto.config,
                "keys": auto.keys,
                "watched": watched,
            }

        for asset in self.assets.values():
            for auto in asset.automations:
                add_automation(auto, targets=[asset.name], attached=asset)
        for auto in self.automations:
            targets = []
            for target in auto.targets or ():
                targets.append(target.name if isinstance(target, Asset) else str(target))
            add_automation(auto, targets=targets, attached=None)

        manifest_assets = {}
        known_kinds = {"Local", "AWSECS", "K8sJob", "Modal", "Pool"} | {
            getattr(e, "kind", type(e).__name__) for e in self.executors
        }
        for name, asset in self.assets.items():
            info = assets[name]
            placement = asset.executor.serialized() if asset.executor else _default_placement()
            if placement["kind"] not in known_kinds:
                raise RegistrationError(
                    f"{name}: placement kind {placement['kind']!r} is not registered; "
                    "declare it with Project(executors=[...])"
                )
            manifest_assets[name] = {
                "outputs": [o.spec(asset.name) for o in asset.outputs],
                "inputs": {p: e.spec(p) for p, e in info["edges"].items()},
                "deps": info["deps"],
                "partitions": {"dims": info["dims"]} if info["dims"] else None,
                "placement": placement,
                "retries": asset.retries.spec(),
                "timeout": asset.timeout,
                "version": asset.version,
                "on_version_change": asset.on_version_change,
                "code_hash": _code_hash(asset.fn),
                "doc": inspect.getdoc(asset.fn) or "",
                "types": {
                    "params": {p: type_name(t) for p, t in hints_by_asset[name].items() if p != "return"},
                    "return": type_name(hints_by_asset[name].get("return")),
                },
                "automations": [n for n, a in automation_records.items() if a["targets"] == [name]],
            }

        source_records = {
            name: {
                "name": name,
                "store": s.store or DEFAULT_STORE,
                "key": s.key,
                "handle": s.handle,
                "head": s.head().to_json(),
            }
            for name, s in self.sources.items()
        }
        store_records = {
            name: {
                "version": getattr(store, "version", "1"),
                "ref": getattr(getattr(store, "ref_type", None), "kind", None) or "ref",
            }
            for name, store in self.stores.items()
        }
        body = {
            "name": self.name,
            "assets": manifest_assets,
            "outputs": {
                name: {k: v for k, v in rec.items() if k != "output"} for name, rec in outputs.items()
            },
            "sources": source_records,
            "stores": store_records,
            "executors": sorted(
                {getattr(e, "kind", type(e).__name__) for e in self.executors}
                | {a["placement"]["kind"] for a in manifest_assets.values()}
            ),
            "automations": automation_records,
        }
        return {**body, "revision": digest(body)}

    def _output_dims(self, output_name, outputs, assets):
        owner = outputs[output_name]["asset"]
        if owner is None:
            return {}
        return assets[owner]["dims"] or {}


def _keys_class():
    from .stores import Keys

    return Keys


def _default_placement() -> dict:
    from .executors import Local

    return Local()().serialized()


def _code_hash(fn: Callable) -> str:
    try:
        source_file = inspect.getsourcefile(fn)
        code = Path(source_file).read_text() if source_file else inspect.getsource(fn)
    except (OSError, TypeError):
        code = fn.__name__
    return digest(code)
