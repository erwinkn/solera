"""Inline placement for tests: runs the real worker harness in-process against
the real object store (implementation plan, Phase 3)."""

from __future__ import annotations

import asyncio


class InlinePlacement:
    """`launch` starts `run_attempt` as an asyncio task; `wait` awaits it."""

    max_concurrent = None
    _tasks: dict[str, asyncio.Task] = {}

    def __init__(self, ctx, project):
        self.ctx, self.project = ctx, project

    async def launch(self, stage: dict) -> dict:
        from cursus_worker.worker import run_attempt

        task = asyncio.create_task(run_attempt(stage["objects"], stage["attempt"], self.project))
        self._tasks[stage["attempt"]] = task
        return {"id": stage["attempt"]}

    async def wait(self, run: dict, timeout: float) -> dict | None:
        task = self._tasks.get(run["id"])
        if task is None:
            return {"code": None, "reason": "lost", "meta": {}}
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if not done:
            return None
        del self._tasks[run["id"]]
        if task.cancelled():
            return {"code": None, "reason": "canceled", "meta": {}}
        error = task.exception()
        if error is not None:
            return {"code": 1, "reason": f"{type(error).__name__}: {error}", "meta": {}}
        return {"code": task.result(), "reason": None, "meta": {}}

    async def cancel(self, run: dict) -> None:
        task = self._tasks.pop(run["id"], None)
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
