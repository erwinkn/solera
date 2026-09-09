from __future__ import annotations

import hashlib
import inspect
import json
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def valid_name(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,127}", value):
        raise ValueError(f"Invalid name: {value!r}")
    return value


@dataclass(frozen=True)
class Output:
    store: str = "json"
    description: str = ""


@dataclass(frozen=True)
class DailyPartitions:
    start: str

    def __post_init__(self) -> None:
        date.fromisoformat(self.start)

    def keys(self, start: str, end: str, *, limit: int = 366) -> list[str]:
        first, last = date.fromisoformat(start), date.fromisoformat(end)
        if first < date.fromisoformat(self.start) or last < first:
            raise ValueError("Invalid partition range")
        count = (last - first).days + 1
        if count > limit:
            raise ValueError(f"At most {limit} partitions per request")
        return [(first + timedelta(days=i)).isoformat() for i in range(count)]


@dataclass(frozen=True)
class ByKey:
    input: str
    key: str = "id"
    revision: str = "revision"
    batch_size: int = 500

    def __post_init__(self) -> None:
        if not 1 <= self.batch_size <= 10000:
            raise ValueError("batch_size must be between 1 and 10000")


@dataclass(frozen=True)
class Cursor:
    initial: Any = None
    state_version: str = "1"


@dataclass(frozen=True)
class Inventory:
    items: list[dict[str, Any]]
    complete: bool = True

    def encode(self) -> dict[str, Any]:
        return {"__inventory__": 1, "items": self.items, "complete": self.complete}


@dataclass(frozen=True)
class Changes:
    upserted: tuple[dict[str, Any], ...]
    deleted_keys: tuple[str, ...]
    key: str
    token: str

    @property
    def upserted_keys(self) -> tuple[str, ...]:
        return tuple(str(row[self.key]) for row in self.upserted)


@dataclass(frozen=True)
class Replace:
    value: Any


@dataclass(frozen=True)
class ReplaceKeys:
    column: str
    keys: Iterable[str]
    rows: list[dict[str, Any]]


@dataclass(frozen=True)
class Upsert:
    primary_key: str
    rows: list[dict[str, Any]]
    delete_keys: Iterable[str] = ()


@dataclass(frozen=True)
class Append:
    batch_id: str
    rows: list[dict[str, Any]]


UNSET = object()


@dataclass
class CommitBatch:
    outputs: Mapping[str, Any]
    acknowledge: str | None = None
    cursor: Any = UNSET
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AssetContext:
    partition: str | None
    run_id: str
    attempt: int
    cursor: Any = None
    _changes: dict[str, Changes] = field(default_factory=dict, repr=False)
    _logger: Callable[[str, dict[str, Any]], None] = field(
        default=lambda message, fields: None, repr=False
    )

    def changes(self, input: str) -> Changes:
        return self._changes[input]

    def log(self, message: str, **fields: Any) -> None:
        self._logger(message, fields)


@dataclass
class AssetDefinition:
    fn: Callable[..., Any]
    outputs: dict[str, Output]
    inputs: dict[str, str] | None
    resources: tuple[str, ...]
    group: str
    description: str
    version: str
    partitions: DailyPartitions | None
    incremental: ByKey | Cursor | None
    retries: int

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fn(*args, **kwargs)

    @property
    def key(self) -> str:
        return self.fn.__name__

    def manifest(self) -> dict[str, Any]:
        parameters = inspect.signature(self.fn).parameters
        inputs = (
            self.inputs
            if self.inputs is not None
            else {key: key for key in parameters if key != "ctx" and key not in self.resources}
        )
        expected = set(inputs) | set(self.resources) | ({"ctx"} if "ctx" in parameters else set())
        if expected != set(parameters) or set(inputs) & set(self.resources):
            raise ValueError(
                f"{self.key}: input/resource bindings do not match function parameters"
            )
        if isinstance(self.incremental, ByKey) and self.incremental.input not in inputs:
            raise ValueError(f"{self.key}: incremental input is not bound")
        try:
            source = inspect.getsource(self.fn)
        except (OSError, TypeError):
            source = self.fn.__code__.co_code.hex()
        return {
            "key": self.key,
            "outputs": {valid_name(key): asdict(output) for key, output in self.outputs.items()},
            "inputs": inputs,
            "resources": list(self.resources),
            "group": self.group,
            "description": self.description,
            "version": digest(
                [
                    self.version,
                    source,
                    inputs,
                    {key: output.store for key, output in self.outputs.items()},
                    asdict(self.partitions) if self.partitions else None,
                    asdict(self.incremental) if self.incremental else None,
                ]
            ),
            "partitions": asdict(self.partitions) if self.partitions else None,
            "incremental": (
                {
                    "kind": "key" if isinstance(self.incremental, ByKey) else "cursor",
                    **asdict(self.incremental),
                }
                if self.incremental
                else None
            ),
            "retries": self.retries,
        }


def asset(
    fn: Callable[..., Any] | None = None,
    *,
    outputs: Mapping[str, Output] | None = None,
    inputs: Mapping[str, str] | None = None,
    resources: Iterable[str] = (),
    group: str = "default",
    description: str = "",
    version: str = "1",
    partitions: DailyPartitions | None = None,
    incremental: ByKey | Cursor | None = None,
    retries: int = 1,
) -> Any:
    def decorate(function: Callable[..., Any]) -> AssetDefinition:
        valid_name(function.__name__)
        if not 0 <= retries <= 10:
            raise ValueError("retries must be between 0 and 10")
        declared = dict(outputs) if outputs is not None else {function.__name__: Output()}
        if not declared:
            raise ValueError("An asset must declare at least one output")
        return AssetDefinition(
            function,
            declared,
            dict(inputs) if inputs is not None else None,
            tuple(resources),
            group,
            description or inspect.getdoc(function) or "",
            version,
            partitions,
            incremental,
            retries,
        )

    return decorate(fn) if fn is not None else decorate


@dataclass(frozen=True)
class Every:
    seconds: int

    def __post_init__(self) -> None:
        if self.seconds < 1:
            raise ValueError("Interval must be positive")


@dataclass(frozen=True)
class Cron:
    expression: str
    timezone: str = "UTC"


@dataclass(frozen=True)
class OnCommit:
    assets: tuple[str, ...]


@dataclass(frozen=True)
class Automation:
    name: str
    targets: tuple[str, ...]
    trigger: Every | Cron | OnCommit
    enabled: bool = False

    def manifest(self) -> dict[str, Any]:
        return {
            "name": valid_name(self.name),
            "targets": list(self.targets),
            "enabled": self.enabled,
            "trigger": {
                "kind": {Every: "interval", Cron: "cron", OnCommit: "commit"}[type(self.trigger)],
                **asdict(self.trigger),
            },
        }


@dataclass
class Definitions:
    assets: list[AssetDefinition]
    stores: dict[str, Any] = field(default_factory=dict)
    resources: dict[str, Any] = field(default_factory=dict)
    automations: list[Automation] = field(default_factory=list)
    name: str = "Workspace"

    def manifest(self) -> dict[str, Any]:
        producers = [definition.manifest() for definition in self.assets]
        by_output: dict[str, dict[str, Any]] = {}
        keys: set[str] = set()
        for producer in producers:
            if producer["key"] in keys:
                raise ValueError(f"Duplicate producer: {producer['key']}")
            keys.add(producer["key"])
            for output, spec in producer["outputs"].items():
                if output in by_output:
                    raise ValueError(f"Duplicate asset: {output}")
                by_output[output] = producer
                if spec["store"] != "json" and spec["store"] not in self.stores:
                    raise ValueError(f"Unknown store: {spec['store']}")
            for resource in producer["resources"]:
                if resource not in self.resources:
                    raise ValueError(f"Unknown resource: {resource}")
        for producer in producers:
            for dependency in producer["inputs"].values():
                if dependency not in by_output:
                    raise ValueError(f"Unknown upstream asset: {dependency}")
                if by_output[dependency]["partitions"] and not producer["partitions"]:
                    raise ValueError(
                        "An unpartitioned asset cannot implicitly collect a partitioned input"
                    )
        ordered: list[dict[str, Any]] = []
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(producer: dict[str, Any]) -> None:
            key = producer["key"]
            if key in visiting:
                raise ValueError(f"Dependency cycle at {key}")
            if key in visited:
                return
            visiting.add(key)
            for upstream in producer["inputs"].values():
                visit(by_output[upstream])
            visiting.remove(key)
            visited.add(key)
            ordered.append(producer)

        for producer in producers:
            visit(producer)
        automations = [automation.manifest() for automation in self.automations]
        if len({a["name"] for a in automations}) != len(automations):
            raise ValueError("Duplicate automation name")
        for automation in automations:
            if not automation["targets"] or set(automation["targets"]) - by_output.keys():
                raise ValueError("Automation targets must reference known assets")
            trigger = automation["trigger"]
            if trigger["kind"] == "commit":
                if not trigger["assets"] or set(trigger["assets"]) - by_output.keys():
                    raise ValueError("Unknown automation event asset")
                if set(trigger["assets"]) & set(automation["targets"]):
                    raise ValueError("An automation cannot trigger itself")
            if trigger["kind"] == "cron":
                from zoneinfo import ZoneInfo

                from croniter import croniter

                ZoneInfo(trigger["timezone"])
                if not croniter.is_valid(trigger["expression"]):
                    raise ValueError("Invalid cron expression")
        return {"format": 1, "name": self.name, "producers": ordered, "automations": automations}
