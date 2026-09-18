from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import sys
import tempfile
from pathlib import Path
from typing import Protocol


class Backend(Protocol):
    async def execute(self, spec: dict) -> tuple[dict, str]: ...


class LocalSubprocess:
    def __init__(self, project: str, *, timeout=300, log_limit=8 * 1024 * 1024):
        if timeout <= 0 or log_limit < 1:
            raise ValueError("Timeout and log limit must be positive")
        self.project, self.timeout, self.log_limit = project, timeout, log_limit

    async def _run(self, *args):
        # Defense in depth only. Trusted user code still shares the host identity;
        # this is process isolation, not a security sandbox.
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("AWS_", "DORC_API_TOKEN", "GITHUB_", "GH_TOKEN", "RAILWAY_TOKEN"))
        }
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "dorc_worker",
            *args,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        tails = [bytearray(), bytearray()]
        total = 0

        async def drain(stream, tail):
            nonlocal total
            while chunk := await stream.read(16384):
                total += len(chunk)
                tail.extend(chunk)
                del tail[:-65536]
                if total > self.log_limit:
                    raise RuntimeError(f"Worker log output exceeded {self.log_limit} bytes")

        tasks = [
            asyncio.create_task(drain(process.stdout, tails[0])),
            asyncio.create_task(drain(process.stderr, tails[1])),
            asyncio.create_task(process.wait()),
        ]
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), self.timeout)
        except BaseException:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            for task in tasks:
                task.cancel()
            # On failure, discard remaining pipe contents without retaining logs.
            await asyncio.gather(*tasks, return_exceptions=True)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(process.communicate(), 10)
            raise
        logs = (bytes(tails[0]) + bytes(tails[1])).decode(errors="replace")[-65536:]
        if process.returncode:
            raise RuntimeError(f"Asset process exited {process.returncode}\n{logs}")
        return logs

    async def manifest(self):
        # A separate protocol file keeps arbitrary project import/initialization
        # messages on stdout from corrupting the manifest.
        with tempfile.TemporaryDirectory(prefix="dorc-manifest-") as directory:
            result = Path(directory) / "manifest.json"
            await self._run("manifest", self.project, str(result))
            if result.stat().st_size > 4 * 1024 * 1024:
                raise ValueError("Manifest exceeds the alpha's 4 MiB limit")
            return json.loads(result.read_text())

    async def execute(self, spec):
        with tempfile.TemporaryDirectory(prefix="dorc-attempt-") as directory:
            source, result = Path(directory) / "input.json", Path(directory) / "output.json"
            source.write_text(json.dumps(spec, allow_nan=False))
            logs = await self._run("run", self.project, str(source), str(result))
            if result.stat().st_size > 64 * 1024 * 1024:
                raise ValueError("Worker result exceeds the alpha's 64 MiB JSON limit")
            return json.loads(result.read_text()), logs
