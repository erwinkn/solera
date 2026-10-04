"""Inline placement for tests: runs the real worker in-process against
the real object store."""

from __future__ import annotations

import asyncio

from solera.tasks import Tasks

# Attempt id -> its worker's task. The process's, not an engine's: each stands for a
# worker process, which outlives the engine that launched it, and a placement is built
# anew for every use, so the next engine in this process adopts them.
_workers = Tasks("inline workers")
_launched: dict[
    str, asyncio.Task
] = {}  # attempt id -> its worker's task, until how it ended is read or released


class InlinePlacement:
    """`launch` starts `run_attempt` as an asyncio task; `wait` awaits it."""

    max_concurrent = None

    def __init__(self, ctx, project):
        self.ctx, self.project = ctx, project

    async def launch(self, stage: dict) -> dict:
        attempt = stage["attempt"]
        # Its end, failure included, is `wait`'s answer: not logged as well.
        _launched[attempt] = _workers.spawn(self._work(stage), key=attempt, awaited=True)
        return {"id": attempt}

    def _work(self, stage: dict):
        """The worker an attempt runs: `run_attempt`, on the engine's channel."""

        from solera_worker.channel import LocalChannel
        from solera_worker.worker import run_attempt

        engine = self.ctx.engine
        channel = LocalChannel(engine, stage["attempt"]) if engine is not None else None
        return run_attempt(
            stage["objects"], stage["attempt"], self.project, run=stage["run"], channel=channel
        )

    async def wait(self, handle: dict, timeout: float) -> dict | None:
        task = _launched.get(handle["id"])
        if task is None:
            return {"code": None, "reason": "lost", "meta": {}}
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if not done:
            return None
        del _launched[handle["id"]]
        if task.cancelled():
            return {"code": None, "reason": "canceled", "meta": {}}
        error = task.exception()
        if error is not None:
            return {"code": 1, "reason": f"{type(error).__name__}: {error}", "meta": {}}
        return {"code": task.result(), "reason": None, "meta": {}}

    def release(self, run: dict) -> None:
        """Settled: the worker's task goes once it ends (it may still be
        cleaning up after its commit, docs/lifecycle.md §9.8)."""

        task = _launched.get(run["id"])
        if task is not None:
            task.add_done_callback(lambda _: _launched.pop(run["id"], None))

    async def cancel(self, handle: dict) -> None:
        task = _launched.pop(handle["id"], None)
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
