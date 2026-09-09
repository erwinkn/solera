from __future__ import annotations

import asyncio
import inspect
import traceback
from collections.abc import Awaitable
from typing import Any
from uuid import uuid4

from psycopg.types.json import Jsonb

from .database import Database, LostLease, load_definitions
from .sdk import (
    UNSET,
    Append,
    AssetContext,
    Changes,
    CommitBatch,
    Definitions,
    Inventory,
    canonical,
    digest,
)
from .stores import JsonStore, apply_operation


def preview(value: Any) -> Any:
    selected = value[:20] if isinstance(value, list) else value
    if len(canonical(selected)) > 16000:
        return {"note": "Preview omitted: value exceeds 16 KB"}
    return selected


class Engine:
    def __init__(self, database: Database):
        self.db = database

    def execute(self, task_id: str, token: str, definitions: Definitions | None = None) -> None:
        try:
            self._execute(task_id, token, definitions)
        except LostLease:
            # Publication is fenced; only the current owner may change task status.
            return
        except Exception:
            self.db.fail(task_id, token, traceback.format_exc())

    def _execute(self, task_id: str, token: str, definitions: Definitions | None) -> None:
        with self.db.connect() as conn:
            task = self.db.owned(conn, task_id, token)
            registered = self.db.manifest(conn, task["manifest_id"])
        definitions = definitions or load_definitions(registered["entrypoint"])
        if digest([registered["entrypoint"], definitions.manifest()]) != registered["id"]:
            raise ValueError(
                "Worker code does not match the pinned manifest. Restore its code artifact or submit a new request."
            )
        producer = next(
            p for p in registered["definition"]["producers"] if p["key"] == task["producer"]
        )
        function = next(p.fn for p in definitions.assets if p.key == task["producer"])
        stores = {"json": JsonStore(self.db), **definitions.stores}
        with self.db.connect() as conn:
            task = self.db.owned(conn, task_id, token)
            input_refs = task["input_refs"]
            if input_refs is None:
                input_refs = self._bind_inputs(conn, task, producer, registered["definition"])
                conn.execute(
                    "UPDATE tasks SET input_refs=%s WHERE id=%s", (Jsonb(input_refs), task_id)
                )
            heads = self._heads(conn, producer, task["partition_key"])
            checkpoint = conn.execute(
                "SELECT * FROM checkpoints WHERE producer=%s AND partition_key=%s",
                (producer["key"], task["partition_key"]),
            ).fetchone()
        signature = digest(
            [
                producer["version"],
                task["partition_key"],
                {key: ref["version"] for key, ref in input_refs.items()},
            ]
        )
        existing = next(iter(heads.values()), None)
        all_exist = len(heads) == len(producer["outputs"])
        if (
            not producer["incremental"]
            and task["mode"] != "recompute"
            and all_exist
            and existing is not None
        ):
            with self.db.connect() as conn:
                cached = conn.execute(
                    "SELECT signature FROM commits WHERE id=%s", (existing["commit_id"],)
                ).fetchone()
            same_bundle = len({head["commit_id"] for head in heads.values()}) == 1
            if (
                same_bundle
                and cached["signature"] == signature
                and (input_refs or task["mode"] == "fill_missing")
            ):
                self._finish(
                    task_id,
                    token,
                    "skipped",
                    existing["commit_id"],
                    "Inputs and transformation are unchanged",
                )
                return
        values = {key: stores[ref["store"]].load(ref["ref"]) for key, ref in input_refs.items()}
        incremental = producer["incremental"]
        batches: list[Changes | None] = [None]
        if incremental and incremental["kind"] == "key":
            batches = list(self._change_batches(task, producer, values[incremental["input"]]))
            if not batches:
                if all_exist and existing is not None:
                    self._finish(
                        task_id,
                        token,
                        "skipped",
                        existing["commit_id"],
                        "No unprocessed source changes",
                    )
                    return
                batches = [Changes((), (), incremental["key"], digest([task_id, "empty"]))]
        if (
            incremental
            and incremental["kind"] == "cursor"
            and checkpoint
            and checkpoint["state_version"] != incremental["state_version"]
        ):
            raise ValueError("Cursor state version changed. Explicit state migration is required.")
        generation = checkpoint["generation"] if checkpoint else 0
        for changes in batches:

            def log(message: str, detail: dict[str, Any]) -> None:
                with self.db.connect() as conn:
                    self.db.event(conn, task["request_id"], task_id, "log", message, detail)

            ctx = AssetContext(
                partition=task["partition_key"] or None,
                run_id=str(task["request_id"]),
                attempt=task["attempt"],
                cursor=checkpoint["cursor_value"]
                if checkpoint
                else (incremental or {}).get("initial"),
                _changes={incremental["input"]: changes} if changes else {},
                _logger=log,
            )
            arguments = {
                key: Inventory(value["items"], value["complete"])
                if isinstance(value, dict) and value.get("__inventory__") == 1
                else value
                for key, value in values.items()
            }
            arguments.update({key: definitions.resources[key] for key in producer["resources"]})
            if "ctx" in inspect.signature(function).parameters:
                arguments["ctx"] = ctx
            result = function(**arguments)
            if inspect.isawaitable(result):

                async def resolve(awaitable: Awaitable[Any]) -> Any:
                    return await awaitable

                result = asyncio.run(resolve(result))
            if not isinstance(result, CommitBatch):
                result = CommitBatch(
                    {next(iter(producer["outputs"])): result}
                    if len(producer["outputs"]) == 1
                    else result
                )
            if set(result.outputs) != set(producer["outputs"]):
                raise ValueError("A producer must return exactly its declared outputs")
            if changes and result.acknowledge != changes.token:
                raise ValueError(
                    "Incremental outputs must acknowledge the exact change batch token"
                )
            if incremental and incremental["kind"] == "cursor" and result.cursor is UNSET:
                raise ValueError(
                    "A cursor asset must return CommitBatch(cursor=...) with its outputs"
                )
            if not incremental and (result.acknowledge is not None or result.cursor is not UNSET):
                raise ValueError("Checkpoint updates require an incremental asset definition")
            commit_id = str(uuid4())
            prepared: dict[str, Any] = {}
            receipts: list[tuple[str, str, str]] = []
            for key, operation in result.outputs.items():
                store_name = producer["outputs"][key]["store"]
                store = stores[store_name]
                previous = (
                    stores[heads[key]["store"]].load(heads[key]["ref"]) if key in heads else None
                )
                append_seen = False
                if isinstance(operation, Append):
                    payload_hash = digest(operation.rows)
                    with self.db.connect() as conn:
                        receipt = conn.execute(
                            "SELECT payload_hash FROM append_receipts WHERE asset_key=%s AND partition_key=%s AND batch_id=%s",
                            (key, task["partition_key"], operation.batch_id),
                        ).fetchone()
                    if receipt and receipt["payload_hash"] != payload_hash:
                        raise ValueError("Append batch_id was reused with a different payload")
                    append_seen = receipt is not None
                    receipts.append((key, operation.batch_id, payload_hash))
                value = apply_operation(previous, operation, append_seen=append_seen)
                version = digest(value)
                prepared[key] = {
                    "store": store_name,
                    "ref": store.stage(value),
                    "version": version,
                    "rows": len(value) if isinstance(value, list) else None,
                    "preview": preview(value),
                }
            self._publish(
                task_id,
                token,
                producer,
                input_refs,
                signature,
                commit_id,
                prepared,
                result,
                changes,
                generation,
                receipts,
            )
            generation += 1 if incremental else 0
            heads = {key: {**ref, "commit_id": commit_id} for key, ref in prepared.items()}
        self._finish(task_id, token, "succeeded", commit_id, "Materialization committed")

    def _bind_inputs(
        self, conn: Any, task: dict[str, Any], producer: dict[str, Any], manifest: dict[str, Any]
    ) -> dict[str, Any]:
        by_output = {key: p for p in manifest["producers"] for key in p["outputs"]}
        refs = {}
        for parameter, asset_key in producer["inputs"].items():
            upstream = by_output[asset_key]
            scope = task["partition_key"] if upstream["partitions"] else ""
            planned = conn.execute(
                "SELECT u.output_commit FROM dependencies d JOIN tasks u ON u.id=d.upstream_id WHERE d.task_id=%s AND u.producer=%s AND u.partition_key=%s",
                (task["id"], upstream["key"], scope),
            ).fetchone()
            head = (
                planned
                or conn.execute(
                    "SELECT commit_id AS output_commit FROM asset_heads WHERE asset_key=%s AND partition_key=%s",
                    (asset_key, scope),
                ).fetchone()
            )
            if not head or not head["output_commit"]:
                raise ValueError(
                    f"Missing upstream materialization: {asset_key} [{scope or 'whole asset'}]"
                )
            commit = conn.execute(
                "SELECT outputs FROM commits WHERE id=%s", (head["output_commit"],)
            ).fetchone()
            refs[parameter] = {
                **commit["outputs"][asset_key],
                "asset": asset_key,
                "commit_id": str(head["output_commit"]),
            }
        return refs

    @staticmethod
    def _heads(conn: Any, producer: dict[str, Any], scope: str) -> dict[str, Any]:
        rows = conn.execute(
            "SELECT h.asset_key,h.commit_id,c.outputs FROM asset_heads h JOIN commits c ON c.id=h.commit_id WHERE h.asset_key=ANY(%s) AND h.partition_key=%s",
            (list(producer["outputs"]), scope),
        ).fetchall()
        return {
            row["asset_key"]: {
                **row["outputs"][row["asset_key"]],
                "commit_id": str(row["commit_id"]),
            }
            for row in rows
        }

    def _change_batches(
        self, task: dict[str, Any], producer: dict[str, Any], inventory: Any
    ) -> list[Changes]:
        spec = producer["incremental"]
        if not isinstance(inventory, dict) or inventory.get("__inventory__") != 1:
            raise ValueError("ByKey inputs must be produced as Inventory(items, complete=...)")
        items: dict[str, dict[str, Any]] = {}
        for row in inventory["items"]:
            if spec["key"] not in row or spec["revision"] not in row or row[spec["key"]] is None:
                raise ValueError("Inventory item is missing its key or revision")
            key = str(row[spec["key"]])
            if key in items:
                raise ValueError(f"Duplicate inventory key: {key}")
            items[key] = row
        with self.db.connect() as conn:
            state = {
                row["item_key"]: row
                for row in conn.execute(
                    "SELECT * FROM item_state WHERE producer=%s AND partition_key=%s",
                    (producer["key"], task["partition_key"]),
                ).fetchall()
            }
        updated = [
            key
            for key in sorted(items)
            if key not in state
            or state[key]["revision"] != canonical(items[key][spec["revision"]])
            or state[key]["transform_version"] != producer["version"]
            or (task["mode"] == "recompute" and str(state[key]["last_task_id"]) != str(task["id"]))
        ]
        deleted = sorted(state.keys() - items.keys()) if inventory["complete"] else []
        actions = [(key, True) for key in updated] + [(key, False) for key in deleted]
        batches = []
        for offset in range(0, len(actions), spec["batch_size"]):
            batch = actions[offset : offset + spec["batch_size"]]
            upserts = tuple(items[key] for key, update in batch if update)
            deletes = tuple(key for key, update in batch if not update)
            batches.append(
                Changes(
                    upserts,
                    deletes,
                    spec["key"],
                    digest([str(task["id"]), producer["version"], upserts, deletes]),
                )
            )
        return batches

    def _publish(
        self,
        task_id: str,
        token: str,
        producer: dict[str, Any],
        input_refs: dict[str, Any],
        signature: str,
        commit_id: str,
        outputs: dict[str, Any],
        result: CommitBatch,
        changes: Changes | None,
        generation: int,
        receipts: list[tuple[str, str, str]],
    ) -> None:
        with self.db.connect() as conn:
            task = self.db.owned(conn, task_id, token)
            scope = task["partition_key"]
            for key in sorted(outputs):
                lock = conn.execute(
                    "SELECT owner FROM scope_locks WHERE asset_key=%s AND partition_key=%s FOR UPDATE",
                    (key, scope),
                ).fetchone()
                if not lock or str(lock["owner"]) != token:
                    raise LostLease("Write scope was reassigned")
            old = self._heads(conn, producer, scope)
            incremental = producer["incremental"]
            if incremental:
                conn.execute(
                    "INSERT INTO checkpoints(producer,partition_key) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (producer["key"], scope),
                )
                checkpoint = conn.execute(
                    "UPDATE checkpoints SET generation=generation+1,cursor_value=%s,state_version=%s,updated_at=now() WHERE producer=%s AND partition_key=%s AND generation=%s RETURNING generation",
                    (
                        Jsonb(result.cursor if result.cursor is not UNSET else None),
                        incremental.get("state_version"),
                        producer["key"],
                        scope,
                        generation,
                    ),
                ).fetchone()
                if not checkpoint:
                    raise LostLease("Checkpoint generation changed")
            conn.execute(
                "INSERT INTO commits(id,task_id,producer,partition_key,definition_version,signature,input_refs,outputs,metadata,checkpoint_generation) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    commit_id,
                    task_id,
                    producer["key"],
                    scope,
                    producer["version"],
                    signature,
                    Jsonb(input_refs),
                    Jsonb(outputs),
                    Jsonb(result.metadata),
                    generation + 1 if incremental else None,
                ),
            )
            changed = []
            for key, reference in outputs.items():
                conn.execute(
                    "INSERT INTO asset_heads(asset_key,partition_key,commit_id,version) VALUES (%s,%s,%s,%s) ON CONFLICT(asset_key,partition_key) DO UPDATE SET commit_id=excluded.commit_id,version=excluded.version,updated_at=now()",
                    (key, scope, commit_id, reference["version"]),
                )
                if key not in old or old[key]["version"] != reference["version"]:
                    changed.append(key)
            if changes:
                with conn.cursor() as cursor:
                    cursor.executemany(
                        "INSERT INTO item_state(producer,partition_key,item_key,revision,transform_version,last_task_id) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(producer,partition_key,item_key) DO UPDATE SET revision=excluded.revision,transform_version=excluded.transform_version,last_task_id=excluded.last_task_id",
                        [
                            (
                                producer["key"],
                                scope,
                                str(row[changes.key]),
                                canonical(row[incremental["revision"]]),
                                producer["version"],
                                task_id,
                            )
                            for row in changes.upserted
                        ],
                    )
                conn.execute(
                    "DELETE FROM item_state WHERE producer=%s AND partition_key=%s AND item_key=ANY(%s)",
                    (producer["key"], scope, list(changes.deleted_keys)),
                )
            for key, batch_id, payload_hash in receipts:
                conn.execute(
                    "INSERT INTO append_receipts(asset_key,partition_key,batch_id,payload_hash,commit_id) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (key, scope, batch_id, payload_hash, commit_id),
                )
            conn.execute(
                "UPDATE tasks SET output_commit=%s,signature=%s WHERE id=%s",
                (commit_id, signature, task_id),
            )
            self.db.event(
                conn,
                task["request_id"],
                task_id,
                "committed",
                "Published outputs and checkpoint",
                {
                    "commit_id": commit_id,
                    "changed": changed,
                    "upserted": len(changes.upserted) if changes else 0,
                    "deleted": len(changes.deleted_keys) if changes else 0,
                },
            )
            for automation in conn.execute(
                "SELECT name,definition FROM automations WHERE enabled AND definition->'trigger'->>'kind'='commit'"
            ).fetchall():
                if set(changed) & set(automation["definition"]["trigger"]["assets"]):
                    conn.execute(
                        "INSERT INTO outbox(id,automation_name,commit_id) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                        (str(uuid4()), automation["name"], commit_id),
                    )

    def _finish(self, task_id: str, token: str, status: str, commit_id: Any, message: str) -> None:
        with self.db.connect() as conn:
            task = self.db.owned(conn, task_id, token)
            conn.execute(
                "UPDATE tasks SET status=%s,output_commit=%s,finished_at=now(),owner=NULL WHERE id=%s",
                (status, commit_id, task_id),
            )
            conn.execute(
                "UPDATE attempts SET status=%s,finished_at=now() WHERE token=%s", (status, token)
            )
            conn.execute(
                "UPDATE scope_locks SET owner=NULL,lease_expires=NULL WHERE owner=%s", (token,)
            )
            self.db.event(conn, task["request_id"], task_id, status, message)
            self.db.refresh(conn)
