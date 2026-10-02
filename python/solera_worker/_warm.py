"""The project a pool worker's attempts run, imported once, in the process
they are forked from (`worker._forked`): a forkserver, which starts no
thread, so a child forked from it is safe on any OS and starts warm. A
project that fails to import here is imported again by each attempt,
which then fails with the error in its result."""

from __future__ import annotations

import os

project = None
try:
    from .worker import load_project

    project = load_project(os.environ["SOLERA_PROJECT"])
except Exception:  # noqa: S110 — each attempt reports it
    pass
