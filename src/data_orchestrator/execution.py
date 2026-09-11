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
    def __init__(self, project: str, *, timeout=300):
        self.project, self.timeout = project, timeout

    async def _run(self, *args):
        # Defense in depth only: subprocesses are not a security sandbox. This
        # trusted-project alpha does not isolate the host filesystem or identity.
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("AWS_", "DORC_API_TOKEN", "GITHUB_", "GH_TOKEN", "RAILWAY_TOKEN"))
        }
        with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "data_orchestrator.worker",
                *args,
                env=env,
                stdout=output,
                stderr=errors,
                start_new_session=True,
            )
            try:
                await asyncio.wait_for(process.wait(), self.timeout)
            except BaseException:
                if process.returncode is None:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
                raise
            output.seek(0, 2)
            size = output.tell()
            output.seek(max(0, size - 65536))
            logs = output.read().decode(errors="replace")
            errors.seek(0, 2)
            errors.seek(max(0, errors.tell() - 65536))
            logs = (logs + errors.read().decode(errors="replace"))[-65536:]
            if process.returncode:
                raise RuntimeError(f"Asset process exited {process.returncode}\n{logs}")
            output.seek(0)
            # Manifest output is bounded; user-code logs are separate from results.
            stdout = output.read(4 * 1024 * 1024).decode()
            return stdout, logs

    async def manifest(self):
        output, _ = await self._run("manifest", self.project)
        return json.loads(output)

    async def execute(self, spec):
        with tempfile.TemporaryDirectory(prefix="dorc-attempt-") as directory:
            source, result = Path(directory) / "input.json", Path(directory) / "output.json"
            source.write_text(json.dumps(spec, allow_nan=False))
            _, logs = await self._run("run", self.project, str(source), str(result))
            if result.stat().st_size > 64 * 1024 * 1024:
                raise ValueError("Worker result exceeds the alpha's 64 MiB JSON limit")
            return json.loads(result.read_text()), logs
