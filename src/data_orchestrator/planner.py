from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from .sdk import DailyPartitions


def plan(
    manifest: dict[str, Any],
    targets: list[str],
    partitions: list[str] | None = None,
    *,
    include_upstream: bool = True,
    mode: str = "incremental",
) -> list[dict[str, Any]]:
    producers = manifest["producers"]
    by_output = {key: producer for producer in producers for key in producer["outputs"]}
    if not targets or set(targets) - by_output.keys():
        raise ValueError("Select at least one known asset")
    if mode not in {"incremental", "recompute", "fill_missing"}:
        raise ValueError("Unknown materialization mode")
    partitions = list(dict.fromkeys(partitions or []))
    if len(partitions) > 366:
        raise ValueError("At most 366 partitions per request")
    if partitions and not any(by_output[target]["partitions"] for target in targets):
        raise ValueError("Partition keys were supplied for unpartitioned targets")
    nodes: dict[tuple[str, str], dict[str, Any]] = {}

    def add(producer: dict[str, Any], scope: str) -> dict[str, Any]:
        scope = scope if producer["partitions"] else ""
        if producer["partitions"]:
            parsed = date.fromisoformat(scope)
            if parsed.isoformat() != scope:
                raise ValueError("Partition keys must use YYYY-MM-DD")
            DailyPartitions(**producer["partitions"]).keys(scope, scope)
        incremental = producer["incremental"]
        if mode == "recompute" and incremental and incremental["kind"] == "cursor":
            raise ValueError(
                "Recompute cannot reset a live cursor. Use a new state scope or an explicit migration."
            )
        key = (producer["key"], scope)
        if key in nodes:
            return nodes[key]
        node = {
            "id": str(uuid4()),
            "producer": producer["key"],
            "partition_key": scope,
            "max_attempts": producer["retries"] + 1,
            "dependencies": [],
        }
        nodes[key] = node
        if include_upstream:
            for upstream in producer["inputs"].values():
                dep = add(by_output[upstream], scope)
                if dep["id"] not in node["dependencies"]:
                    node["dependencies"].append(dep["id"])
        return node

    for target in targets:
        producer = by_output[target]
        scopes = (
            partitions or [datetime.now(UTC).date().isoformat()] if producer["partitions"] else [""]
        )
        for scope in scopes:
            add(producer, scope)
    if len(nodes) > 10000:
        raise ValueError("Request exceeds the 10000-task planning limit")
    return list(nodes.values())
