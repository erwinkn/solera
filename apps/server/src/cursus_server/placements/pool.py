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
        return {"task": stage["attempt"], "pool": self.name}

    async def wait(self, run: dict, timeout: float) -> dict | None:
        deadline = self.ctx.clock() + timeout
        attempt = run["task"]
        while True:
            if await self.ctx.state.get_object(f"results/{attempt}.json") is not None:
                return {"code": 0, "reason": None, "meta": {}}
            record = await self.ctx.state.get_pool_task(attempt)
            if record is None:
                # complete() removed the claim without a result we can see.
                if await self.ctx.state.get_object(f"results/{attempt}.json") is not None:
                    return {"code": 0, "reason": None, "meta": {}}
                return {"code": None, "reason": "lost", "meta": {}}
            if record["status"] == "claimed" and record["lease_until"] <= self.ctx.clock():
                await self.ctx.state.sweep_pool_leases()
            remaining = deadline - self.ctx.clock()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(0.2, remaining))

    async def cancel(self, run: dict) -> None:
        # Best-effort: an unclaimed task is withdrawn; a claimed one is fenced by
        # lease expiry and swept back to queued for a retry.
        async with self.ctx.state.transaction() as tx:
            await tx.del_pool_task(run["task"])
