"""Imported by the pool's forkserver (worker._attempts) before it forks an
attempt: what an attempt's channel would otherwise import and build on its
first call (about 90 ms), httpx and the transports and asyncio backend its
clients import lazily, and the two SSL contexts they take, so each attempt
inherits them. It makes no client (whose proxy lookup calls macOS's system
frameworks, which are not fork-safe) and starts no thread. A failure here
only leaves an attempt to do the same work itself."""

import contextlib
import importlib

with contextlib.suppress(Exception):
    import httpx

    from .channel import _spare

    for name in ("httpcore", "httpcore._backends.anyio", "anyio._backends._asyncio"):
        importlib.import_module(name)
    _spare.extend(httpx.create_ssl_context() for _ in range(2))
