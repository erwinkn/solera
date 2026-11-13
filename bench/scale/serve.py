"""The scale harness's engine (README.md): the engine and its API as
`solera serve` runs them, with its event loop timed. run.py starts it, pinned to the engine's CPUs.

    python bench/scale/serve.py STATE_URL PROJECT PORT

Every second it appends a line to `$SCALE_ROOT/engine.jsonl`: the loop's busy
and idle seconds so far, its longest iteration since the last line (a stall:
nothing else ran on the loop meanwhile), the process's CPU seconds and
resident memory, and the CPU seconds so far of native work on threads
(`in_thread`: the core's plans and commit builds, the index's reads), by the
part of the engine that awaited it (run.py's `BUCKETS`, matched against the
awaiting stack as py-spy would write it). A sampler sees such a thread's stack
end where the native call starts, so only this says whose work it was. The
API token is `$SOLERA_API_TOKEN`."""

from __future__ import annotations

import asyncio
import json
import os
import resource
import sys
import time
import traceback
from collections import defaultdict
from pathlib import Path

import solera.threads
import uvicorn
from solera_server.api import create_app
from solera_server.engine import Engine
from solera_server.executors.local import load_manifest
from solera_server.state import State

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from run import BUCKETS  # noqa: E402

ROOT = Path(os.environ.get("SCALE_ROOT", ".solera/scale"))
THREADS: defaultdict[str, float] = defaultdict(float)  # thread CPU seconds by BUCKETS part
_in_thread = solera.threads.in_thread
_PREFIXES = sorted({p.rstrip("/") + "/" for p in sys.path if p}, key=len, reverse=True)


def _short(filename: str) -> str:
    return next((filename[len(p) :] for p in _PREFIXES if filename.startswith(p)), filename)


async def in_thread(fn, /, *args, **kwargs):
    """`solera.threads.in_thread`, its thread's CPU added to the awaiting part's."""

    stack = ";".join(f"{f.name} ({_short(f.filename)}:{f.lineno})" for f in traceback.extract_stack())
    part = next((b for b, patterns in BUCKETS if any(p in stack for p in patterns)), "other")

    def timed(*a, **k):
        start = time.thread_time()
        try:
            return fn(*a, **k)
        finally:
            THREADS[part] += time.thread_time() - start

    return await _in_thread(timed, *args, **kwargs)


for _module in list(sys.modules.values()):
    if getattr(_module, "in_thread", None) is _in_thread:
        _module.in_thread = in_thread


class Loop:
    """Times the event loop through its selector: the time between one
    `select` returning and the next being called is the loop at work."""

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.idle = self.busy = self.longest = 0.0
        self.iterations = 0
        self.returned = time.perf_counter()
        selector = loop._selector  # the one seam every asyncio loop on Linux has
        select = selector.select

        def timed(timeout=None):
            entered = time.perf_counter()
            ran = entered - self.returned
            self.busy += ran
            self.longest = max(self.longest, ran)
            self.iterations += 1
            try:
                return select(timeout)
            finally:
                self.returned = time.perf_counter()
                self.idle += self.returned - entered

        selector.select = timed

    async def report(self, out: Path) -> None:
        with out.open("a") as f:
            while True:
                await asyncio.sleep(1.0)
                usage = resource.getrusage(resource.RUSAGE_SELF)
                line = {
                    "at": time.time(),
                    "busy": self.busy,
                    "idle": self.idle,
                    "iterations": self.iterations,
                    "longest": self.longest,
                    "cpu": usage.ru_utime + usage.ru_stime,
                    "rss": _rss(),
                    "peak_rss": usage.ru_maxrss * 1024,
                    "threads": dict(THREADS),
                }
                self.longest = 0.0
                f.write(json.dumps(line) + "\n")
                f.flush()


def _rss() -> int:
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")


async def serve(state_url: str, project: str, port: int) -> None:
    loop = Loop(asyncio.get_running_loop())
    manifest = await load_manifest(project)
    state = await State.open(state_url, "scale")
    engine = Engine(
        state,
        manifest,
        project=project,
        concurrency=int(os.environ.get("SOLERA_CONCURRENCY", "4")),
        engine_url=f"http://127.0.0.1:{port}",
    )
    await engine.initialize()
    app = create_app(engine=engine, token=os.environ["SOLERA_API_TOKEN"])
    reporting = asyncio.create_task(loop.report(ROOT / "engine.jsonl"))
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    try:
        await server.serve()
    finally:
        reporting.cancel()
        await state.close()


if __name__ == "__main__":
    asyncio.run(serve(sys.argv[1], sys.argv[2], int(sys.argv[3])))
