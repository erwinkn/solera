from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from typing import Any, Protocol

import psycopg

from .database import Database
from .service import Service


@dataclass(frozen=True)
class ExecutionSpec:
    task_id: str
    token: str


class Backend(Protocol):
    """Execution infrastructure only; no asset planning or checkpoint semantics."""

    def submit(self, spec: ExecutionSpec, submission_key: str) -> str: ...
    def lookup(self, submission_key: str) -> str | None: ...
    def inspect(self, handle: str) -> int | None: ...
    def cancel(self, handle: str) -> None: ...
    def logs(self, handle: str) -> str: ...
    def release(self, handle: str) -> None: ...


class LocalBackend:
    """Subprocess isolation, not a security sandbox.

    Submission lookup is process-local. Supervisor loss is recovered by leases;
    duplicate computation is permitted but stale publication is rejected.
    """

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.handles: dict[str, tuple[subprocess.Popen[Any], Any]] = {}

    def lookup(self, submission_key: str) -> str | None:
        return submission_key if submission_key in self.handles else None

    def submit(self, spec: ExecutionSpec, submission_key: str) -> str:
        if submission_key not in self.handles:
            output = tempfile.TemporaryFile()
            environment = {**os.environ, "DORC_DATABASE_URL": self.database_url}
            try:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "data_orchestrator.cli",
                        "_execute",
                        spec.task_id,
                        spec.token,
                    ],
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    start_new_session=True,
                )
            except BaseException:
                output.close()
                raise
            self.handles[submission_key] = (process, output)
        return submission_key

    def inspect(self, handle: str) -> int | None:
        return self.handles[handle][0].poll()

    def cancel(self, handle: str) -> None:
        process = self.handles[handle][0]
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=3)
            except ProcessLookupError:
                pass
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()

    def logs(self, handle: str) -> str:
        output = self.handles[handle][1]
        output.seek(0, 2)
        output.seek(max(0, output.tell() - 16000))
        return output.read().decode(errors="replace")

    def release(self, handle: str) -> None:
        _, output = self.handles.pop(handle)
        output.close()


class Worker:
    def __init__(self, database: Database, *, concurrency: int = 4, backend: Backend | None = None):
        if not 1 <= concurrency <= 128:
            raise ValueError("Concurrency must be between 1 and 128")
        self.db = database
        self.concurrency = concurrency
        self.backend = backend or LocalBackend(database.url)
        self.running: dict[str, tuple[dict[str, Any], float]] = {}

    def step(self) -> int:
        self.db.recover()
        Service(self.db).tick()
        for handle, (task, heartbeat) in list(self.running.items()):
            status = self.backend.inspect(handle)
            if status is not None:
                logs = self.backend.logs(handle)
                if logs:
                    with self.db.connect() as conn:
                        self.db.event(conn, task["request_id"], task["id"], "process_output", logs)
                self.db.fail(
                    str(task["id"]),
                    str(task["owner"]),
                    f"Execution process exited ({status}) without finalizing its attempt",
                )
                self.backend.release(handle)
                del self.running[handle]
            elif time.monotonic() - heartbeat >= 5:
                if not self.db.heartbeat(str(task["id"]), str(task["owner"])):
                    self.backend.cancel(handle)
                    self.backend.release(handle)
                    del self.running[handle]
                else:
                    self.running[handle] = (task, time.monotonic())
        while len(self.running) < self.concurrency:
            candidate = self.db.claim()
            if candidate is None:
                break
            spec = ExecutionSpec(str(candidate["id"]), str(candidate["owner"]))
            try:
                handle = self.backend.lookup(spec.token) or self.backend.submit(spec, spec.token)
                self.running[handle] = (candidate, time.monotonic())
            except Exception as error:
                self.db.fail(spec.task_id, spec.token, f"Execution submission failed: {error}")
        return len(self.running)

    def run(self, stop: threading.Event | None = None) -> None:
        stop = stop or threading.Event()
        try:
            while not stop.is_set():
                try:
                    self.step()
                except psycopg.OperationalError as error:
                    print(f"Worker database unavailable: {type(error).__name__}", file=sys.stderr)
                stop.wait(0.5)
        finally:
            for handle, (task, _) in list(self.running.items()):
                self.backend.cancel(handle)
                self.backend.release(handle)
                try:
                    self.db.fail(
                        str(task["id"]), str(task["owner"]), "Worker stopped before task completion"
                    )
                except psycopg.OperationalError:
                    pass  # The durable lease will expire and be recovered.
            self.running.clear()
