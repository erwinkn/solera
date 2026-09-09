from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from croniter import croniter
from psycopg.types.json import Jsonb

from .database import Database


class Service:
    def __init__(self, database: Database):
        self.db = database

    def catalog(self) -> dict[str, Any]:
        with self.db.connect() as conn:
            registered = self.db.manifest(conn)
            manifest = registered["definition"]
            rows = conn.execute(
                "SELECT h.*,c.outputs,c.input_refs,c.definition_version FROM asset_heads h JOIN commits c ON c.id=h.commit_id ORDER BY h.updated_at DESC"
            ).fetchall()
            latest: dict[str, Any] = {}
            scoped = {}
            for row in rows:
                latest.setdefault(row["asset_key"], row)
                scoped[(row["asset_key"], row["partition_key"])] = row
            active = {
                r["producer"]
                for r in conn.execute(
                    "SELECT DISTINCT producer FROM tasks WHERE status='running'"
                ).fetchall()
            }
            recent = {
                r["producer"]: r
                for r in conn.execute(
                    "SELECT DISTINCT ON(producer) producer,status,error FROM tasks ORDER BY producer,coalesce(started_at,retry_at) DESC"
                ).fetchall()
            }
            queue = conn.execute(
                "SELECT count(*) FILTER(WHERE status='running') AS running,count(*) FILTER(WHERE status IN ('waiting','queued')) AS queued FROM tasks"
            ).fetchone()
            assets = []
            by_output = {key: p for p in manifest["producers"] for key in p["outputs"]}
            for producer in manifest["producers"]:
                for key, output in producer["outputs"].items():
                    head = latest.get(key)
                    status = "missing" if not head else "materialized"
                    stale = head and head["definition_version"] != producer["version"]
                    if head:
                        for ref in head["input_refs"].values():
                            upstream = by_output.get(ref["asset"])
                            scope = (
                                head["partition_key"] if upstream and upstream["partitions"] else ""
                            )
                            current = scoped.get((ref["asset"], scope))
                            if current is None or current["version"] != ref["version"]:
                                stale = True
                    if stale:
                        status = "stale"
                    if recent.get(producer["key"], {}).get("status") == "failed":
                        status = "failed"
                    if producer["key"] in active:
                        status = "running"
                    reference = head["outputs"][key] if head else {}
                    assets.append(
                        {
                            "key": key,
                            "producer": producer["key"],
                            "group": producer["group"],
                            "description": output["description"] or producer["description"],
                            "inputs": list(producer["inputs"].values()),
                            "outputs": list(producer["outputs"]),
                            "store": output["store"],
                            "incremental": producer["incremental"],
                            "partitions": producer["partitions"],
                            "status": status,
                            "last_materialized": head["updated_at"] if head else None,
                            "version": head["version"] if head else None,
                            "rows": reference.get("rows"),
                            "partition_count": sum(1 for asset, _ in scoped if asset == key),
                        }
                    )
        return {
            "name": manifest["name"],
            "manifest_id": registered["id"],
            "assets": assets,
            "queue": queue,
        }

    def asset(self, key: str, partition: str | None = None) -> dict[str, Any]:
        catalog = self.catalog()
        definition = next((a for a in catalog["assets"] if a["key"] == key), None)
        if not definition:
            raise KeyError(key)
        with self.db.connect() as conn:
            history = conn.execute(
                "SELECT c.* FROM commits c WHERE c.outputs ? %s AND (%s::text IS NULL OR c.partition_key=%s) ORDER BY c.created_at DESC LIMIT 50",
                (key, partition, partition),
            ).fetchall()
            scopes = conn.execute(
                "SELECT partition_key,version,updated_at FROM asset_heads WHERE asset_key=%s ORDER BY partition_key DESC LIMIT 366",
                (key,),
            ).fetchall()
            scope = (
                partition
                if partition is not None
                else (history[0]["partition_key"] if history else "")
            )
            checkpoint = conn.execute(
                "SELECT *, (SELECT count(*) FROM item_state i WHERE i.producer=c.producer AND i.partition_key=c.partition_key) AS tracked_items FROM checkpoints c WHERE c.producer=%s AND c.partition_key=%s",
                (definition["producer"], scope),
            ).fetchone()
        return {
            "definition": definition,
            "commits": history,
            "partitions": scopes,
            "checkpoint": checkpoint,
            "preview": history[0]["outputs"][key].get("preview") if history else None,
        }

    def runs(
        self, *, limit: int = 50, offset: int = 0, backfills: bool = False
    ) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            return conn.execute(
                "SELECT r.*, (SELECT jsonb_object_agg(s.status,s.n) FROM (SELECT status,count(*) AS n FROM tasks WHERE request_id=r.id GROUP BY status) s) AS counts FROM requests r WHERE (NOT %s OR r.reason='backfill') ORDER BY r.created_at DESC,r.id DESC LIMIT %s OFFSET %s",
                (backfills, limit, offset),
            ).fetchall()

    def run(self, request_id: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=%s", (request_id,)).fetchone()
            if not request:
                raise KeyError(request_id)
            tasks = conn.execute(
                "SELECT * FROM tasks WHERE request_id=%s ORDER BY started_at NULLS LAST,producer,partition_key",
                (request_id,),
            ).fetchall()
            attempts = conn.execute(
                "SELECT a.* FROM attempts a JOIN tasks t ON t.id=a.task_id WHERE t.request_id=%s ORDER BY a.started_at",
                (request_id,),
            ).fetchall()
            events = conn.execute(
                "SELECT * FROM (SELECT * FROM events WHERE request_id=%s ORDER BY id DESC LIMIT 250) e ORDER BY id",
                (request_id,),
            ).fetchall()
        return {"request": request, "tasks": tasks, "attempts": attempts, "events": events}

    def cancel(self, request_id: str) -> None:
        with self.db.connect() as conn:
            self.db.control(conn)
            request = conn.execute("SELECT * FROM requests WHERE id=%s", (request_id,)).fetchone()
            if not request:
                raise KeyError(request_id)
            if request["status"] in {"succeeded", "failed", "canceled"}:
                return
            conn.execute(
                "UPDATE attempts SET status='canceled',finished_at=now() WHERE token IN (SELECT owner FROM tasks WHERE request_id=%s AND status='running')",
                (request_id,),
            )
            conn.execute(
                "UPDATE scope_locks SET owner=NULL,lease_expires=NULL WHERE owner IN (SELECT owner FROM tasks WHERE request_id=%s)",
                (request_id,),
            )
            conn.execute(
                "UPDATE tasks SET status='canceled',owner=NULL,finished_at=now() WHERE request_id=%s AND status IN ('waiting','queued','running')",
                (request_id,),
            )
            conn.execute(
                "UPDATE requests SET status='canceled',finished_at=now() WHERE id=%s", (request_id,)
            )
            self.db.event(
                conn,
                request_id,
                None,
                "canceled",
                "Canceled unpublished work; existing commits are retained",
            )

    def pause(self, request_id: str, paused: bool) -> None:
        with self.db.connect() as conn:
            self.db.control(conn)
            row = conn.execute(
                "UPDATE requests SET paused=%s WHERE id=%s AND status IN ('queued','running') RETURNING id",
                (paused, request_id),
            ).fetchone()
            if not row:
                raise ValueError("Only active requests can be paused or resumed")
            self.db.event(
                conn,
                request_id,
                None,
                "paused" if paused else "resumed",
                "Paused new work; running tasks may finish" if paused else "Resumed request",
            )

    def repair(self, request_id: str) -> str:
        with self.db.connect() as conn:
            self.db.control(conn)
            request = conn.execute("SELECT * FROM requests WHERE id=%s", (request_id,)).fetchone()
            if not request:
                raise KeyError(request_id)
            if request["status"] not in {"failed", "canceled"}:
                raise ValueError("Only failed or canceled requests can be repaired")
            new_id = str(uuid4())
            conn.execute(
                "INSERT INTO requests(id,manifest_id,targets,partitions,mode,reason,parent_id) VALUES (%s,%s,%s,%s,%s,'repair',%s)",
                (
                    new_id,
                    request["manifest_id"],
                    Jsonb(request["targets"]),
                    Jsonb(request["partitions"]),
                    request["mode"],
                    request_id,
                ),
            )
            tasks = conn.execute(
                "SELECT * FROM tasks WHERE request_id=%s", (request_id,)
            ).fetchall()
            identifiers = {task["id"]: str(uuid4()) for task in tasks}
            for task in tasks:
                reusable = task["status"] in {"succeeded", "skipped"}
                conn.execute(
                    "INSERT INTO tasks(id,request_id,producer,partition_key,max_attempts,status,output_commit,input_refs,signature) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        identifiers[task["id"]],
                        new_id,
                        task["producer"],
                        task["partition_key"],
                        task["max_attempts"],
                        "skipped" if reusable else "waiting",
                        task["output_commit"] if reusable else None,
                        Jsonb(task["input_refs"]) if reusable else None,
                        task["signature"] if reusable else None,
                    ),
                )
            for dependency in conn.execute(
                "SELECT d.* FROM dependencies d JOIN tasks t ON t.id=d.task_id WHERE t.request_id=%s",
                (request_id,),
            ).fetchall():
                conn.execute(
                    "INSERT INTO dependencies(task_id,upstream_id) VALUES (%s,%s)",
                    (identifiers[dependency["task_id"]], identifiers[dependency["upstream_id"]]),
                )
            self.db.event(
                conn,
                new_id,
                None,
                "requested",
                "Repairing failed work while retaining successful input commits",
                {"parent_id": request_id},
            )
            self.db.refresh(conn)
            return new_id

    def automations(self) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            return conn.execute(
                "SELECT * FROM automations WHERE manifest_id=(SELECT manifest_id FROM workspace) ORDER BY name"
            ).fetchall()

    def set_automation(self, name: str, enabled: bool) -> None:
        with self.db.connect() as conn:
            self.db.control(conn)
            row = conn.execute(
                "UPDATE automations SET enabled=%s WHERE name=%s RETURNING name", (enabled, name)
            ).fetchone()
            if not row:
                raise KeyError(name)
            self.db.event(
                conn,
                None,
                None,
                "automation_changed",
                f"{'Enabled' if enabled else 'Disabled'} {name}",
            )

    def tick(self) -> int:
        count = 0
        with self.db.connect() as conn:
            self.db.control(conn)
            automations = conn.execute(
                "SELECT * FROM automations WHERE enabled ORDER BY name FOR UPDATE SKIP LOCKED"
            ).fetchall()
            now = datetime.now(UTC)
            for automation in automations:
                trigger = automation["definition"]["trigger"]
                outbox = []
                if trigger["kind"] == "commit":
                    outbox = conn.execute(
                        "SELECT id FROM outbox WHERE automation_name=%s AND delivered_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 100",
                        (automation["name"],),
                    ).fetchall()
                    if not outbox:
                        continue
                    tick_key = str(outbox[0]["id"])
                else:
                    if automation["next_tick"] > now:
                        continue
                    tick_key = automation["next_tick"].isoformat()
                request_id = self.db.submit(
                    automation["definition"]["targets"],
                    include_upstream=trigger["kind"] != "commit",
                    reason=f"automation:{automation['name']}",
                    manifest_id=automation["manifest_id"],
                    idempotency_key=f"automation:{automation['name']}:{tick_key}",
                    conn=conn,
                )
                next_tick = (
                    now + timedelta(seconds=trigger["seconds"])
                    if trigger["kind"] == "interval"
                    else (
                        croniter(
                            trigger["expression"], now.astimezone(ZoneInfo(trigger["timezone"]))
                        ).get_next(datetime)
                        if trigger["kind"] == "cron"
                        else now
                    )
                )
                conn.execute(
                    "UPDATE automations SET last_tick=%s,next_tick=%s,last_request=%s WHERE name=%s",
                    (now, next_tick, request_id, automation["name"]),
                )
                if outbox:
                    conn.execute(
                        "UPDATE outbox SET delivered_at=now() WHERE id=ANY(%s)",
                        ([row["id"] for row in outbox],),
                    )
                count += 1
        return count
