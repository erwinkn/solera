"""Pool placement (§10): the pull path. `launch` is a no-op — the engine's
`AttemptLaunched` event makes the task claimable; `wait` reports when the
result object appears, the task is completed, or the worker's claim lease
expired. A lost worker is not replaced: the engine aborts or settles its
attempt, and retries the task under a new one (§8)."""

from __future__ import annotations

import asyncio

GRACE_SECONDS = 30.0


class PoolPlacement:
    max_concurrent = None

    def __init__(self, ctx, name: str):
        self.ctx, self.name = ctx, name

    async def launch(self, stage: dict) -> dict:
        return {"task": stage["attempt"], "run": stage["run"], "pool": self.name}

    resume = launch  # an adopted attempt waits on the same durable pool record

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
            if record["status"] == "claimed":
                if record["lease_until"] is None:
                    # Restored after a restart: give the worker time to renew.
                    record["lease_until"] = self.ctx.clock() + GRACE_SECONDS
                elif record["lease_until"] <= self.ctx.clock():
                    return {"code": None, "reason": "lost", "meta": {}}
            remaining = deadline - self.ctx.clock()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(0.2, remaining))

    async def cancel(self, run: dict) -> None:
        # An unclaimed task is withdrawn; a claimed one was aborted by the
        # engine's write fence — the worker can no longer write (§8).
        self.ctx.state.model.pool.pop(run["task"], None)
