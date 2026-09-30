"""Pool placement (§10): the pull path. `launch` is a no-op — the engine's
`AttemptLaunched` event makes the task claimable; `wait` wakes when the
worker completes the task or its claim lease expires, and reports whether
the result object is there. A lost worker is not replaced: the engine aborts or settles its
attempt, and retries the task under a new one (§8)."""

from __future__ import annotations

import asyncio
import contextlib

GRACE_SECONDS = 30.0

_woken: dict[str, asyncio.Event] = {}  # attempt -> set when a worker claims or completes it


def wake(attempt: str) -> None:
    """A worker claimed or completed `attempt`: wake whoever waits on it."""

    woken = _woken.get(attempt)
    if woken is not None:
        woken.set()


class PoolPlacement:
    max_concurrent = None

    def __init__(self, ctx, name: str):
        self.ctx, self.name = ctx, name

    async def launch(self, stage: dict) -> dict:
        return {"task": stage["attempt"], "run": stage["run"], "pool": self.name}

    resume = launch  # an adopted attempt waits on the same durable pool record

    async def wait(self, run: dict, timeout: float) -> dict | None:
        state, clock = self.ctx.state, self.ctx.clock
        attempt = run["task"]
        deadline = clock() + timeout
        woken = _woken.setdefault(attempt, asyncio.Event())
        while True:
            record = state.model.pool.get(attempt)
            over = record is None  # completed, or withdrawn
            if record is not None and record["status"] == "claimed":
                if record["lease_until"] is None:
                    # Restored after a restart: give the worker time to renew.
                    record["lease_until"] = clock() + GRACE_SECONDS
                over = record["lease_until"] <= clock()
            if over:
                _woken.pop(attempt, None)
                if await state.attempt_finished(run["run"], attempt):
                    return {"code": 0, "reason": None, "meta": {}}
                return {"code": None, "reason": "lost", "meta": {}}
            remaining = deadline - clock()
            if remaining <= 0:
                return None
            if record["lease_until"] is not None:
                remaining = min(remaining, record["lease_until"] - clock())
            woken.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(woken.wait(), remaining)

    async def cancel(self, run: dict) -> None:
        # An unclaimed task is withdrawn; a claimed one was aborted by the
        # engine's write fence — the worker can no longer write (§8).
        self.ctx.state.model.pool.pop(run["task"], None)
