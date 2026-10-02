"""Local placement (§10): the harness runs as a subprocess on the engine host.

Handles carry `{launch, pid, started_at, ticks, host}`. `launch` names this
launch: only a handle this engine process launched finds its child in the
registry, so an adopted handle never matches another child that happens to
hold the same pid. An adopted handle is followed only when it is provably
the same process — same host, and the same /proc start time — and is
signaled only then. Anything else cannot be told: `wait` raises, `cancel`
does nothing, and the engine follows the worker's own reports.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import socket
import sys
import tempfile
import time
import uuid
from pathlib import Path

ENV_BLOCKLIST_PREFIXES = ("AWS_", "SOLERA_API_TOKEN", "GITHUB_", "GH_TOKEN", "RAILWAY_TOKEN")
_running: dict[str, asyncio.subprocess.Process] = {}  # launch id -> this process's child
_tails: dict[str, list[bytearray]] = {}


def _env() -> dict:
    env = {k: v for k, v in os.environ.items() if not k.startswith(ENV_BLOCKLIST_PREFIXES)}
    return env


async def _start_ticks(pid: int) -> str | None:
    """Linux /proc start-time token; None where /proc is unavailable."""

    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        return stat.rsplit(") ", 1)[-1].split(" ")[19]
    except (OSError, IndexError):
        return None


async def _same(run: dict) -> bool | None:
    """Whether an adopted handle's process still runs: `None` if that cannot
    be told — another host, or no /proc start time to tell it from a later
    process given the same pid."""

    if run.get("host") != socket.gethostname() or run.get("ticks") is None:
        return None
    return await _start_ticks(run["pid"]) == run["ticks"]


def _process_log(launch: str | None) -> str:
    tails = _tails.pop(launch, None)
    if not tails:
        return ""
    return (bytes(tails[0]) + bytes(tails[1])).decode(errors="replace")[-65536:]


class LocalPlacement:
    """`Local()()` — subprocess with an explicit env allow-list (§10)."""

    max_concurrent = None

    def __init__(self, ctx, log_limit: int = 8 * 1024 * 1024):
        self.ctx, self.log_limit = ctx, log_limit

    async def launch(self, stage: dict) -> dict:
        env = _env()
        env["SOLERA_PROJECT"] = self.ctx.project
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "solera_worker",
            "run",
            "--objects",
            stage["objects"],
            "--attempt",
            stage["attempt"],
            "--run",
            stage["run"],
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        tails = [bytearray(), bytearray()]
        total = [0]

        async def drain(stream, tail):
            while chunk := await stream.read(16384):
                total[0] += len(chunk)
                tail.extend(chunk)
                del tail[:-65536]
                if total[0] > self.log_limit:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                    return

        for stream, tail in zip((process.stdout, process.stderr), tails, strict=True):
            asyncio.create_task(drain(stream, tail))
        launch = uuid.uuid4().hex
        _running[launch] = process
        _tails[launch] = tails
        return {
            "launch": launch,
            "pid": process.pid,
            "started_at": self.ctx.clock(),
            "ticks": await _start_ticks(process.pid),
            "host": socket.gethostname(),
        }

    async def wait(self, run: dict, timeout: float) -> dict | None:
        launch = run.get("launch")
        process = _running.get(launch)
        if process is not None:
            # Our own child: its exit wakes us.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(process.wait(), timeout)
            if process.returncode is None:
                return None
            del _running[launch]
            return {"code": process.returncode, "reason": None, "meta": {"log": _process_log(launch)}}
        # Adopted after a restart, so not our child: watch whether it lives.
        deadline = time.monotonic() + timeout
        while True:
            alive = await _same(run)
            if alive is None:
                raise LookupError(f"process {run['pid']} on {run.get('host')} cannot be told from here")
            if not alive:
                return {"code": None, "reason": "lost", "meta": {}}
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(0.2, remaining))

    async def cancel(self, run: dict) -> None:
        launch = run.get("launch")
        process = _running.get(launch)
        if process is not None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(process.pid, signal.SIGTERM)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(process.wait(), 5)
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(process.pid, signal.SIGKILL)
            _running.pop(launch, None)
            _process_log(launch)
            return
        # Adopted: signal only the very process the handle names, checked each time.
        if not await _same(run):
            return
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.kill(run["pid"], signal.SIGTERM)
        for _ in range(50):
            await asyncio.sleep(0.1)
            if not await _same(run):
                return
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.kill(run["pid"], signal.SIGKILL)


async def load_manifest(project: str, *, timeout: float = 60):
    """`manifest` runs through Local only, at server start (§10)."""

    env = _env()
    env["SOLERA_PROJECT"] = project
    with tempfile.TemporaryDirectory(prefix="solera-manifest-") as directory:
        out = Path(directory) / "manifest.json"
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "solera_worker",
            "manifest",
            project,
            str(out),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout)
        except BaseException:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            raise
        if process.returncode:
            tail = (stdout + stderr).decode(errors="replace")[-8192:]
            raise RuntimeError(f"Project manifest failed ({process.returncode}):\n{tail}")
        if out.stat().st_size > 4 * 1024 * 1024:
            raise ValueError("Manifest exceeds the 4 MiB limit")
        return json.loads(out.read_text())
