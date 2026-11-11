"""Imported by the pool's forkserver (worker._attempts) before it forks an
attempt: what an attempt's channel would otherwise import and build on its
first call (about 90 ms), httpx, its transports and asyncio backend, and
the SSL context its clients share, so each attempt inherits them. It
starts no thread, so forking from it stays safe."""

import asyncio
import contextlib
import importlib

from .channel import HttpTransport

asyncio.run(HttpTransport("http://localhost", None).close())
for name in ("httpcore._backends.anyio", "anyio._backends._asyncio"):
    with contextlib.suppress(ImportError):
        importlib.import_module(name)
