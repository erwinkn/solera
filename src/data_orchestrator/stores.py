from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from psycopg.types.json import Jsonb

from .sdk import Append, Inventory, Replace, ReplaceKeys, Upsert, canonical, digest


class Store(Protocol):
    """Immutable storage: stage durably before the engine publishes a reference.

    Failed attempts may leave unreferenced objects. Neither method may overwrite
    published data. load must reproduce the exact version addressed by a reference.
    """

    def stage(self, value: Any) -> dict[str, Any]: ...
    def load(self, reference: dict[str, Any]) -> Any: ...


@dataclass
class JsonStore:
    database: Any

    def stage(self, value: Any) -> dict[str, Any]:
        version = digest(value)
        with self.database.connect() as conn:
            conn.execute(
                "INSERT INTO objects(id, value) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (version, Jsonb(value)),
            )
        return {"kind": "json", "id": version}

    def load(self, reference: dict[str, Any]) -> Any:
        with self.database.connect() as conn:
            row = conn.execute(
                "SELECT value FROM objects WHERE id=%s", (reference["id"],)
            ).fetchone()
        if not row:
            raise FileNotFoundError(reference["id"])
        return row["value"]


@dataclass
class FileStore:
    root: str

    def stage(self, value: Any) -> dict[str, Any]:
        content = canonical(value).encode()
        version = digest(value)
        root = Path(self.root).resolve()
        root.mkdir(parents=True, exist_ok=True)
        target = root / f"{version}.json"
        with tempfile.NamedTemporaryFile(dir=root, delete=False) as handle:
            temporary = Path(handle.name)
            try:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            except BaseException:
                temporary.unlink(missing_ok=True)
                raise
        try:
            try:
                os.link(temporary, target)
            except FileExistsError:
                if target.read_bytes() != content:
                    raise RuntimeError("Content-addressed object mismatch") from None
            descriptor = os.open(root, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            temporary.unlink(missing_ok=True)
        return {"kind": "file", "id": version}

    def load(self, reference: dict[str, Any]) -> Any:
        version = reference["id"]
        if not re.fullmatch(r"[a-f0-9]{64}", version):
            raise ValueError("Invalid object reference")
        value = json.loads((Path(self.root) / f"{version}.json").read_text())
        if digest(value) != version:
            raise ValueError("Object integrity check failed")
        return value


def apply_operation(previous: Any, operation: Any, *, append_seen: bool = False) -> Any:
    if isinstance(operation, Inventory):
        return operation.encode()
    if isinstance(operation, Replace):
        return (
            operation.value.encode() if isinstance(operation.value, Inventory) else operation.value
        )
    if isinstance(operation, (ReplaceKeys, Upsert, Append)):
        if previous is not None and (
            not isinstance(previous, list) or any(not isinstance(row, dict) for row in previous)
        ):
            raise ValueError("Incremental row operations require a list of objects")
        if any(not isinstance(row, dict) for row in operation.rows):
            raise ValueError("Rows must be objects")
        rows = list(previous or [])
        if isinstance(operation, ReplaceKeys):
            keys = {str(key) for key in operation.keys}
            if any(
                operation.column not in row or str(row[operation.column]) not in keys
                for row in operation.rows
            ):
                raise ValueError(
                    "Replacement rows must belong to an explicitly replaced ownership key"
                )
            return [
                row for row in rows if str(row.get(operation.column)) not in keys
            ] + operation.rows
        if isinstance(operation, Upsert):
            if any(operation.primary_key not in row for row in [*rows, *operation.rows]):
                raise ValueError("Missing primary key")
            upsert_keys = [str(row[operation.primary_key]) for row in operation.rows]
            if len(set(upsert_keys)) != len(upsert_keys):
                raise ValueError("Duplicate upsert keys")
            deleted = {str(key) for key in operation.delete_keys}
            if set(upsert_keys) & deleted:
                raise ValueError("A key cannot be both upserted and deleted")
            values = {str(row[operation.primary_key]): row for row in rows}
            for key in deleted:
                values.pop(key, None)
            values.update(zip(upsert_keys, operation.rows, strict=True))
            return list(values.values())
        if not operation.batch_id:
            raise ValueError("An append requires a stable batch_id")
        return rows if append_seen else rows + operation.rows
    return operation
