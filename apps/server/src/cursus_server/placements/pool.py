"""Pool placement (§10): the pull path. `launch` is a no-op — the engine stages
the claimable task in memory when it dispatches; `wait` reports when the
result object appears, the task is completed, or the claim lease expired."""

from __future__ import annotations

import asyncio


class PoolPlacement:
    max_concurrent = None

    def __init__(self, ctx, name: str):
        self.ctx, self.name = ctx, name

    async def launch(self, stage: dict) -> dict:
        return {"task": stage["attempt"], "run": stage["run"], "pool": self.name}

    async def wait(self, run: dict, timeout: float) -> dict | None:
        state = self.ctx.state
        deadline = self.ctx.clock() + timeout
        attempt = run["task"]
        while True:
            if await state.attempt_finished(run["run"], attempt):
                return {"code": 0, "reason": None, "meta": {}}
            record = state.model.pool.get(attempt)
            if record is None:
                # complete() removed the claim without a result we can see.
                if await state.attempt_finished(run["run"], attempt):
                    return {"code": 0, "reason": None, "meta": {}}
                return {"code": None, "reason": "lost", "meta": {}}
            if record["status"] == "claimed" and record["lease_until"] <= self.ctx.clock():
                # The worker's claim expired: offer the task to another worker.
                record.update(status="queued", claimed_by=None, lease_until=None)
            remaining = deadline - self.ctx.clock()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(0.2, remaining))

    async def cancel(self, run: dict) -> None:
        # Best-effort: an unclaimed task is withdrawn; a claimed one is fenced by
        # its lost scope claim — the worker's result can no longer commit.
        self.ctx.state.model.pool.pop(run["task"], None)
