from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .planner import plan
from .sdk import Definitions, digest


class LostLease(RuntimeError):
    pass


def load_definitions(entrypoint: str) -> Definitions:
    module, separator, attribute = entrypoint.partition(":")
    if not separator or not module or not attribute:
        raise ValueError("Expected a module:definitions entrypoint")
    definitions = getattr(importlib.import_module(module), attribute)
    if not isinstance(definitions, Definitions):
        raise TypeError("Entrypoint must resolve to Definitions")
    return definitions


class Database:
    def __init__(self, url: str | None = None):
        self.url: str = url or os.getenv("DORC_DATABASE_URL") or "postgresql:///orchestrator"

    def connect(self) -> Any:
        return psycopg.connect(self.url, row_factory=dict_row)

    @staticmethod
    def control(conn: Any) -> None:
        # Short metadata transitions are serialized in the alpha; computation is concurrent.
        conn.execute("SELECT pg_advisory_xact_lock(1637850124)")

    def migrate(self) -> None:
        with self.connect() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(1637850123)")
            conn.execute(Path(__file__).with_name("schema.sql").read_text())
            versions = conn.execute("SELECT version FROM schema_version").fetchall()
            if [row["version"] for row in versions] != [1]:
                raise RuntimeError("Unsupported metadata schema version")

    def register(self, entrypoint: str, definitions: Definitions | None = None) -> str:
        definition = (definitions or load_definitions(entrypoint)).manifest()
        manifest_id = digest([entrypoint, definition])
        with self.connect() as conn:
            self.control(conn)
            conn.execute(
                "INSERT INTO manifests(id,entrypoint,definition) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                (manifest_id, entrypoint, Jsonb(definition)),
            )
            conn.execute(
                "INSERT INTO workspace(singleton,manifest_id) VALUES (true,%s) ON CONFLICT(singleton) DO UPDATE SET manifest_id=excluded.manifest_id",
                (manifest_id,),
            )
            names = [a["name"] for a in definition["automations"]]
            conn.execute("UPDATE automations SET enabled=false WHERE NOT(name=ANY(%s))", (names,))
            for automation in definition["automations"]:
                conn.execute(
                    "INSERT INTO automations(name,manifest_id,definition,enabled) VALUES (%s,%s,%s,%s) ON CONFLICT(name) DO UPDATE SET manifest_id=excluded.manifest_id,definition=excluded.definition",
                    (automation["name"], manifest_id, Jsonb(automation), automation["enabled"]),
                )
            self.event(
                conn,
                None,
                None,
                "registered",
                "Registered asset definitions",
                {"manifest_id": manifest_id},
            )
        return manifest_id

    @staticmethod
    def event(
        conn: Any,
        request_id: Any,
        task_id: Any,
        kind: str,
        message: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO events(request_id,task_id,kind,message,detail) VALUES (%s,%s,%s,%s,%s)",
            (request_id, task_id, kind, message[:8000], Jsonb(detail or {})),
        )

    @staticmethod
    def manifest(conn: Any, manifest_id: str | None = None) -> dict[str, Any]:
        row = (
            conn.execute("SELECT * FROM manifests WHERE id=%s", (manifest_id,)).fetchone()
            if manifest_id
            else conn.execute(
                "SELECT m.* FROM manifests m JOIN workspace w ON w.manifest_id=m.id"
            ).fetchone()
        )
        if not row:
            raise ValueError("No registered definitions. Run: dorc register module:definitions")
        return row

    def submit(
        self,
        targets: list[str],
        *,
        partitions: list[str] | None = None,
        mode: str = "incremental",
        include_upstream: bool = True,
        reason: str = "manual",
        idempotency_key: str | None = None,
        manifest_id: str | None = None,
        conn: Any = None,
    ) -> str:
        if conn is None:
            with self.connect() as transaction:
                return self.submit(
                    targets,
                    partitions=partitions,
                    mode=mode,
                    include_upstream=include_upstream,
                    reason=reason,
                    idempotency_key=idempotency_key,
                    manifest_id=manifest_id,
                    conn=transaction,
                )
        self.control(conn)
        manifest = self.manifest(conn, manifest_id)
        nodes = plan(
            manifest["definition"],
            targets,
            partitions,
            include_upstream=include_upstream,
            mode=mode,
        )
        request_id = str(uuid4())
        row = conn.execute(
            "INSERT INTO requests(id,manifest_id,targets,partitions,mode,reason,idempotency_key,include_upstream) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(idempotency_key) DO NOTHING RETURNING id",
            (
                request_id,
                manifest["id"],
                Jsonb(sorted(set(targets))),
                Jsonb(partitions or []),
                mode,
                reason,
                idempotency_key,
                include_upstream,
            ),
        ).fetchone()
        if not row:
            previous = conn.execute(
                "SELECT * FROM requests WHERE idempotency_key=%s", (idempotency_key,)
            ).fetchone()
            if (
                previous["targets"] != sorted(set(targets))
                or previous["partitions"] != (partitions or [])
                or previous["mode"] != mode
                or previous["manifest_id"] != manifest["id"]
                or previous["include_upstream"] != include_upstream
            ):
                raise ValueError("Idempotency key already belongs to a different request")
            return str(previous["id"])
        for node in nodes:
            conn.execute(
                "INSERT INTO tasks(id,request_id,producer,partition_key,max_attempts) VALUES (%s,%s,%s,%s,%s)",
                (
                    node["id"],
                    request_id,
                    node["producer"],
                    node["partition_key"],
                    node["max_attempts"],
                ),
            )
        for node in nodes:
            for upstream in node["dependencies"]:
                conn.execute(
                    "INSERT INTO dependencies(task_id,upstream_id) VALUES (%s,%s)",
                    (node["id"], upstream),
                )
        self.event(
            conn,
            request_id,
            None,
            "requested",
            f"Requested {len(nodes)} execution scopes",
            {"targets": targets, "mode": mode, "reason": reason},
        )
        self.refresh(conn)
        return request_id

    @staticmethod
    def refresh(conn: Any) -> None:
        Database.control(conn)
        # Propagate blocked status through arbitrarily deep graphs.
        while True:
            updated = conn.execute(
                "UPDATE tasks t SET status='blocked',finished_at=now(),error='An upstream task did not complete' WHERE t.status IN ('waiting','queued') AND EXISTS (SELECT 1 FROM dependencies d JOIN tasks u ON u.id=d.upstream_id WHERE d.task_id=t.id AND u.status IN ('failed','blocked','canceled')) RETURNING t.id"
            ).fetchall()
            if not updated:
                break
        conn.execute(
            "UPDATE tasks t SET status='queued' WHERE t.status='waiting' AND NOT EXISTS (SELECT 1 FROM dependencies d JOIN tasks u ON u.id=d.upstream_id WHERE d.task_id=t.id AND u.status NOT IN ('succeeded','skipped'))"
        )
        conn.execute(
            "UPDATE requests r SET status=CASE WHEN EXISTS(SELECT 1 FROM tasks t WHERE t.request_id=r.id AND t.status IN ('waiting','queued','running')) THEN CASE WHEN EXISTS(SELECT 1 FROM tasks t WHERE t.request_id=r.id AND t.attempt>0) THEN 'running' ELSE 'queued' END WHEN EXISTS(SELECT 1 FROM tasks t WHERE t.request_id=r.id AND t.status IN ('failed','blocked')) THEN 'failed' ELSE 'succeeded' END WHERE r.status IN ('queued','running')"
        )
        conn.execute(
            "UPDATE requests SET finished_at=now() WHERE status IN ('succeeded','failed','canceled') AND finished_at IS NULL"
        )

    def recover(self) -> int:
        with self.connect() as conn:
            self.control(conn)
            expired = conn.execute(
                "SELECT * FROM tasks WHERE status='running' AND lease_expires<clock_timestamp() FOR UPDATE SKIP LOCKED"
            ).fetchall()
            for task in expired:
                conn.execute(
                    "UPDATE scope_locks SET owner=NULL,lease_expires=NULL WHERE owner=%s",
                    (task["owner"],),
                )
                status = "queued" if task["attempt"] < task["max_attempts"] else "failed"
                conn.execute(
                    "UPDATE tasks SET status=%s,owner=NULL,error='Execution lease expired',retry_at=now() WHERE id=%s",
                    (status, task["id"]),
                )
                conn.execute(
                    "UPDATE attempts SET status='lost',finished_at=now(),error='Execution lease expired' WHERE token=%s",
                    (task["owner"],),
                )
                self.event(
                    conn,
                    task["request_id"],
                    task["id"],
                    "lease_expired",
                    "Execution lease expired; late publication is fenced",
                )
            self.refresh(conn)
        return len(expired)

    def claim(self, *, lease_seconds: int = 30) -> dict[str, Any] | None:
        with self.connect() as conn:
            self.refresh(conn)
            candidates = conn.execute(
                "SELECT t.*,r.manifest_id,r.mode FROM tasks t JOIN requests r ON r.id=t.request_id WHERE t.status='queued' AND t.retry_at<=now() AND NOT r.paused AND r.status IN ('queued','running') ORDER BY r.created_at,t.id FOR UPDATE OF t SKIP LOCKED LIMIT 20"
            ).fetchall()
            for task in candidates:
                manifest = self.manifest(conn, task["manifest_id"])
                producer = next(
                    p for p in manifest["definition"]["producers"] if p["key"] == task["producer"]
                )
                locks = []
                for asset_key in sorted(producer["outputs"]):
                    conn.execute(
                        "INSERT INTO scope_locks(asset_key,partition_key) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                        (asset_key, task["partition_key"]),
                    )
                    locks.append(
                        conn.execute(
                            "SELECT *,lease_expires>clock_timestamp() AS active FROM scope_locks WHERE asset_key=%s AND partition_key=%s FOR UPDATE",
                            (asset_key, task["partition_key"]),
                        ).fetchone()
                    )
                if any(lock["active"] for lock in locks):
                    continue
                token = str(uuid4())
                for lock in locks:
                    conn.execute(
                        "UPDATE scope_locks SET owner=%s,lease_expires=clock_timestamp()+make_interval(secs=>%s) WHERE asset_key=%s AND partition_key=%s",
                        (token, lease_seconds, lock["asset_key"], lock["partition_key"]),
                    )
                task = conn.execute(
                    "UPDATE tasks SET status='running',owner=%s,lease_expires=clock_timestamp()+make_interval(secs=>%s),attempt=attempt+1,started_at=coalesce(started_at,now()),error=NULL WHERE id=%s RETURNING *",
                    (token, lease_seconds, task["id"]),
                ).fetchone()
                conn.execute(
                    "INSERT INTO attempts(token,task_id,number) VALUES (%s,%s,%s)",
                    (token, task["id"], task["attempt"]),
                )
                self.event(
                    conn,
                    task["request_id"],
                    task["id"],
                    "started",
                    f"Started attempt {task['attempt']}",
                )
                self.refresh(conn)
                return task
        return None

    def heartbeat(self, task_id: str, token: str, lease_seconds: int = 30) -> bool:
        with self.connect() as conn:
            self.control(conn)
            row = conn.execute(
                "UPDATE tasks SET lease_expires=clock_timestamp()+make_interval(secs=>%s) WHERE id=%s AND owner=%s AND status='running' AND lease_expires>clock_timestamp() RETURNING id",
                (lease_seconds, task_id, token),
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE scope_locks SET lease_expires=clock_timestamp()+make_interval(secs=>%s) WHERE owner=%s",
                    (lease_seconds, token),
                )
            return row is not None

    @staticmethod
    def owned(conn: Any, task_id: str, token: str) -> dict[str, Any]:
        Database.control(conn)
        task = conn.execute(
            "SELECT t.*,r.manifest_id,r.mode FROM tasks t JOIN requests r ON r.id=t.request_id WHERE t.id=%s AND t.owner=%s AND t.status='running' AND r.status IN ('queued','running') AND t.lease_expires>clock_timestamp() FOR UPDATE OF t",
            (task_id, token),
        ).fetchone()
        if not task:
            raise LostLease("Attempt no longer owns publication rights")
        return task

    def fail(self, task_id: str, token: str, error: str) -> None:
        with self.connect() as conn:
            try:
                task = self.owned(conn, task_id, token)
            except LostLease:
                return
            retry = task["attempt"] < task["max_attempts"]
            conn.execute(
                "UPDATE tasks SET status=%s,error=%s,owner=NULL,retry_at=now()+make_interval(secs=>%s),finished_at=CASE WHEN %s THEN NULL ELSE now() END WHERE id=%s",
                (
                    "queued" if retry else "failed",
                    error[-16000:],
                    min(2 ** task["attempt"], 60),
                    retry,
                    task_id,
                ),
            )
            conn.execute(
                "UPDATE attempts SET status='failed',error=%s,finished_at=now() WHERE token=%s",
                (error[-16000:], token),
            )
            conn.execute(
                "UPDATE scope_locks SET owner=NULL,lease_expires=NULL WHERE owner=%s", (token,)
            )
            self.event(
                conn, task["request_id"], task_id, "retrying" if retry else "failed", error[-8000:]
            )
            self.refresh(conn)
