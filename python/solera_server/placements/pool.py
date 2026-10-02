"""Pool placement (docs/lifecycle.md §10): the pull path. `launch` makes no
call — the durable `AttemptLaunched` makes the attempt discoverable, and a
pool worker claims it by creating its `.worker`. There is no provider to
ask, so there is no handle: the engine follows the worker's reports, and a
cancel before the claim simply ends the attempt."""

from __future__ import annotations


class PoolPlacement:
    max_concurrent = None
    provision_seconds = None  # an attempt waits for a worker as long as it takes

    def __init__(self, ctx, name: str):
        self.ctx, self.name = ctx, name

    async def launch(self, stage: dict) -> None:
        return None

    async def wait(self, run: dict, timeout: float) -> dict | None:
        return None

    async def cancel(self, run: dict) -> None:
        return None
