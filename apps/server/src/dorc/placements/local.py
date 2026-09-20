"""Local placement (§10): the harness runs as a subprocess on the engine host.

Handles carry `{pid, started_at}` and treat a mismatch as lost. `wait` polls the
process; after an engine restart the pid is re-checked via /proc so an orphaned
or replaced pid is not mistaken for the same run.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import sys
import tempfile
from pathlib import Path

ENV_BLOCKLIST_PREFIXES = ("AWS_", "DORC_API_TOKEN", "GITHUB_", "GH_TOKEN", "RAILWAY_TOKEN")
_running: dict[int, asyncio.subprocess.Process] = {}
_tails: dict[int, list[bytearray]] = {}


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


async def _alive(pid: int, started_ticks: str | None) -> bool:
    if pid in _running:
        return _running[pid].returncode is None
    ticks = await _start_ticks(pid)
    if ticks is None:
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return started_ticks is None
    return ticks == started_ticks


def _process_log(pid: int) -> str:
    tails = _tails.pop(pid, None)
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
        env["DORC_PROJECT"] = self.ctx.project
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "dorc_worker",
            "run",
            "--objects",
            stage["objects"],
            "--attempt",
            stage["attempt"],
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
        _running[process.pid] = process
        _tails[process.pid] = tails
        return {
            "pid": process.pid,
            "started_at": self.ctx.clock(),
            "ticks": await _start_ticks(process.pid),
        }

    async def wait(self, run: dict, timeout: float) -> dict | None:
        deadline = self.ctx.clock() + timeout
        pid = run["pid"]
        while True:
            process = _running.get(pid)
            if process is not None:
                if process.returncode is not None:
                    log = _process_log(pid)
                    del _running[pid]
                    return {"code": process.returncode, "reason": None, "meta": {"log": log}}
            elif not await _alive(pid, run.get("ticks")):
                return {"code": None, "reason": "lost", "meta": {"log": _process_log(pid)}}
            remaining = deadline - self.ctx.clock()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(0.2, remaining))

    async def cancel(self, run: dict) -> None:
        pid = run["pid"]
        process = _running.get(pid)
        with contextlib.suppress(ProcessLookupError, PermissionError):
            if process is not None:
                os.killpg(process.pid, signal.SIGTERM)
            else:
                os.kill(pid, signal.SIGTERM)
        for _ in range(50):
            if not await _alive(pid, run.get("ticks")):
                break
            await asyncio.sleep(0.1)
        with contextlib.suppress(ProcessLookupError, PermissionError):
            if process is not None:
                os.killpg(process.pid, signal.SIGKILL)
            else:
                os.kill(pid, signal.SIGKILL)
        _running.pop(pid, None)
        _process_log(pid)


async def load_manifest(project: str, *, timeout: float = 60, log_limit: int = 8 * 1024 * 1024):
    """`manifest` runs through Local only, at server start (§10)."""

    env = _env()
    env["DORC_PROJECT"] = project
    with tempfile.TemporaryDirectory(prefix="dorc-manifest-") as directory:
        out = Path(directory) / "manifest.json"
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "dorc_worker",
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
