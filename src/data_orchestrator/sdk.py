from __future__ import annotations

import hashlib
import inspect
import json
import re
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import Any

NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,127}$")


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


@dataclass(frozen=True)
class ByKey:
    input: str
    key: str = "id"
    revision: str = "revision"
    batch_size: int = 100


@dataclass(frozen=True)
class Replace:
    value: Any


@dataclass(frozen=True)
class Inventory:
    rows: list[dict[str, Any]]
    complete: bool = True


@dataclass(frozen=True)
class ReplaceKeys:
    column: str
    keys: list[str]
    rows: list[dict[str, Any]]


@dataclass(frozen=True)
class Upsert:
    keys: list[str]
    rows: list[dict[str, Any]]
    deletes: list[list[Any]] = field(default_factory=list)


@dataclass(frozen=True)
class AppendBatch:
    batch_id: str
    rows: list[dict[str, Any]]


@dataclass(frozen=True)
class Batch:
    outputs: dict[str, Any]
    cursor: Any = None


@dataclass(frozen=True)
class AssetContext:
    partition: str
    cursor: Any
    changes: dict[str, Any]
    run_id: str
    config: dict[str, Any]


@dataclass(frozen=True)
class Automation:
    name: str
    targets: tuple[str, ...]
    every_seconds: int = 300
    enabled: bool = False


@dataclass
class Asset:
    fn: Callable[..., Any]
    outputs: tuple[str, ...]
    inputs: dict[str, str] | None
    incremental: ByKey | None
    partitions: str | None
    version: str
    group: str


def asset(
    fn=None, *, outputs=None, inputs=None, incremental=None, partitions=None, version="1", group="default"
):
    def wrap(f):
        return Asset(f, tuple(outputs or (f.__name__,)), inputs, incremental, partitions, str(version), group)

    return wrap(fn) if fn is not None else wrap


class Project:
    def __init__(self, assets: list[Asset], *, resources=None, automations=()):
        self.assets = assets
        self.resources = resources or {}
        self.automations = tuple(automations)
        self.producers: dict[str, Asset] = {}
        self.manifest = self._manifest()

    def _manifest(self):
        producers, owners = {}, {}
        for a in self.assets:
            name = a.fn.__name__
            if not NAME.fullmatch(name) or name in producers:
                raise ValueError(f"Invalid or duplicate producer: {name}")
            if a.partitions not in (None, "daily"):
                raise ValueError("Only identity-mapped daily partitions are currently supported")
            if not a.outputs or len(set(a.outputs)) != len(a.outputs):
                raise ValueError("Outputs must be unique and nonempty")
            for out in a.outputs:
                if not NAME.fullmatch(out) or out in owners or out in self.resources:
                    raise ValueError(f"Invalid or duplicate output: {out}")
                owners[out] = name
            inputs = (
                a.inputs
                if a.inputs is not None
                else {
                    p: p for p in inspect.signature(a.fn).parameters if p != "ctx" and p not in self.resources
                }
            )
            if a.incremental and (a.incremental.input not in inputs or a.incremental.batch_size < 1):
                raise ValueError("ByKey must reference an input and have a positive batch size")
            source_file = inspect.getsourcefile(a.fn)
            code = Path(source_file).read_text() if source_file else inspect.getsource(a.fn)
            producers[name] = {
                "name": name,
                "outputs": list(a.outputs),
                "inputs": inputs,
                "incremental": asdict(a.incremental) if a.incremental else None,
                "partitions": a.partitions,
                "version": a.version,
                "group": a.group,
                "description": inspect.getdoc(a.fn) or "",
                "code_hash": digest(code),
            }
            self.producers[name] = a
        visiting, visited = set(), set()

        def visit(name):
            if name in visiting:
                raise ValueError("Asset dependency cycle")
            if name in visited:
                return
            visiting.add(name)
            for source in producers[name]["inputs"].values():
                if source not in owners:
                    raise ValueError(f"Unknown input: {source}")
                parent = owners[source]
                if producers[parent]["partitions"] and not producers[name]["partitions"]:
                    raise ValueError("A partitioned input requires a partitioned consumer")
                visit(parent)
            visiting.remove(name)
            visited.add(name)

        for name in producers:
            visit(name)
        automation_names = set()
        for a in self.automations:
            if not NAME.fullmatch(a.name) or a.name in automation_names or a.every_seconds < 1:
                raise ValueError("Invalid or duplicate automation")
            automation_names.add(a.name)
            if not a.targets or any(x not in owners or producers[owners[x]]["partitions"] for x in a.targets):
                raise ValueError("Interval automations currently select unpartitioned assets")
        body = {
            "producers": producers,
            "owners": owners,
            "automations": [asdict(a) for a in self.automations],
        }
        return {**body, "revision": digest(body)}


def normalize_result(result, outputs: list[str]) -> dict:
    if isinstance(result, Batch):
        values, cursor = result.outputs, result.cursor
    elif len(outputs) == 1:
        values, cursor = {outputs[0]: result}, None
    else:
        values, cursor = result, None
    if not isinstance(values, dict) or set(values) != set(outputs):
        raise ValueError(f"Producer must return exactly these outputs: {outputs}")
    encoded = {}
    for name, value in values.items():
        if isinstance(value, (Replace, Inventory, ReplaceKeys, Upsert, AppendBatch)):
            encoded[name] = {"kind": type(value).__name__, **asdict(value)}
        elif is_dataclass(value):
            raise TypeError(f"Unsupported output type: {type(value).__name__}")
        else:
            encoded[name] = {"kind": "Replace", "value": value}
    payload = {"outputs": encoded, "cursor": cursor}
    json.dumps(payload, allow_nan=False)
    return payload
