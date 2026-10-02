"""Native work on a thread, for as long as the work runs.

`asyncio.to_thread` lets a cancelled caller go while its thread runs on:
what the caller held — a semaphore, a pin, a reservation, a temporary
file — would be released under work still using it. `in_thread` waits for
the thread whatever happens to its caller, however often it is cancelled,
and only then lets the cancellation through: what the caller holds, the
work holds, until it ends.
"""

from __future__ import annotations

import asyncio


async def in_thread(fn, /, *args, **kwargs):
    task = asyncio.ensure_future(asyncio.to_thread(fn, *args, **kwargs))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = cancelled or not task.done()
        except BaseException:  # the work's own error: taken from the task below
            pass
    if cancelled:
        if not task.cancelled():
            task.exception()  # retrieved: the cancellation is what the caller sees
        raise asyncio.CancelledError
    return task.result()
