"""A local serve's reload (`solera serve`, on by default with `--insecure` on
loopback, `--reload` elsewhere): when the code the project runs changes,
the engine serves the new deploy in place, with no restart and no
"deploy mismatch" for the attempts after it.

It polls the project's source files (`Project.files`, the files its build
identity hashes) once a `POLL`, and waits for them to stay unchanged for
a `QUIET` — a burst of saves makes one deploy. Then it builds the manifest
again, in a process of its own as at start: a file that fails to import or
register (a half-saved edit, a syntax error) leaves the current deploy
served, the error shown in the console's health (`failing["reload"]`) and
logged, until the next change. A code change without a version bump makes
nothing stale (a definition change needs `version=`): reloading never
starts anything over by itself."""

from __future__ import annotations

import asyncio
import logging
import os

from .executors.local import load_manifest

log = logging.getLogger(__name__)

POLL, QUIET = 1.0, 1.0


def _stamps(files: list[str]) -> dict[str, tuple | None]:
    out = {}
    for path in files:
        try:
            st = os.stat(path)
            out[path] = (st.st_mtime_ns, st.st_size)
        except OSError:
            out[path] = None
    return out


async def reload(engine, project: str, files: list[str], *, poll: float = POLL, quiet: float = QUIET) -> None:
    """Serve each new deploy of `project` as its `files` change (the module docstring)."""

    seen = _stamps(files)
    while True:
        await asyncio.sleep(poll)
        now = _stamps(files)
        if now == seen:
            continue
        while True:  # until a quiet interval passes with nothing changed
            await asyncio.sleep(quiet)
            later = _stamps(files)
            if later == now:
                break
            now = later
        seen = now
        try:
            manifest, files = await load_manifest(project, watch=True)
        except Exception as error:
            engine.failing["reload"] = f"{project} did not load; still serving the previous deploy: {error}"
            log.error("reload: %s did not load; still serving the previous deploy:\n%s", project, error)
            continue
        engine.failing.pop("reload", None)
        seen = _stamps(files)
        if engine.redeploy(manifest):
            log.info("reload: serving deploy %s", manifest["deploy"][:12])
