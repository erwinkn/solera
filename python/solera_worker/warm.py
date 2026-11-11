"""Imported by the pool's forkserver (worker._attempts) before it forks an
attempt: what an attempt's channel would otherwise import and build on its
first call (about 90 ms), httpx, its transports and asyncio backend, and the
two SSL contexts its clients take, so each attempt inherits them. It starts
no thread: the throwaway clients skip the environment (trust_env), whose
proxy lookup calls macOS's system frameworks, which are not fork-safe. A
failure here only leaves an attempt to do the same work itself."""

import asyncio
import contextlib
import importlib

with contextlib.suppress(Exception):
    import httpx

    from .channel import _spare

    httpx.Client(trust_env=False, verify=False).close()
    asyncio.run(httpx.AsyncClient(trust_env=False, verify=False).aclose())
    for name in ("httpcore._backends.anyio", "anyio._backends._asyncio"):
        importlib.import_module(name)
    _spare.extend(httpx.create_ssl_context() for _ in range(2))
