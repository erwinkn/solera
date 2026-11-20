"""Code that fails to load, shown in the console of the server still
running, and a local serve's reload.

Reload (`solera serve`, on by default with `--insecure` on loopback,
`--reload` elsewhere): when the code the project runs changes, the engine
serves the new deploy in place, with no restart and no "deploy mismatch"
for the attempts after it. It polls the project's source files
(`Project.files`, the files its build identity hashes) once a `POLL`, and
waits for them to stay unchanged for a `QUIET` — a burst of saves makes one
deploy. Then it builds the manifest again, in a process of its own: a file
that fails to import or register (a half-saved edit, a syntax error) leaves
the current deploy served. A code change without a version bump makes
nothing stale (a definition change needs `version=`): reloading never
starts anything over by itself.

A load error: the latest code that failed to load, kept in the namespace's
object store (`LOAD_ERROR`) until code loads again. A reload that fails
writes it, and so does a `solera serve` whose project fails to import as
it starts — before it takes the namespace over, so the server it was to
replace keeps running and shows why (`/api/diagnostics`)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import time

import obstore
from obstore.exceptions import NotFoundError

from .executors.local import load_manifest

log = logging.getLogger(__name__)

POLL, QUIET = 1.0, 1.0
LOAD_ERROR = "engine/load-error.json"
TAIL = 8192  # characters of an error kept: a traceback's end says what failed


async def failed(objects, error: str) -> None:
    """Record that the latest code did not load: `error`, its traceback."""

    record = {"at": time.time(), "error": error[-TAIL:]}
    await obstore.put_async(objects, LOAD_ERROR, json.dumps(record).encode())


async def loaded(objects) -> None:
    """The code served is the latest: no load error to show."""

    with contextlib.suppress(NotFoundError, FileNotFoundError):
        await obstore.delete_async(objects, LOAD_ERROR)


async def load_error(objects) -> dict | None:
    """`{"at", "error"}` of the latest code that did not load, if any did not."""

    try:
        found = await obstore.get_async(objects, LOAD_ERROR)
    except (NotFoundError, FileNotFoundError):
        return None
    return json.loads(bytes(await found.bytes_async()))


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
            log.error("reload: %s did not load; still serving the previous deploy:\n%s", project, error)
            await failed(engine.state.objects, str(error))
            continue
        await loaded(engine.state.objects)
        seen = _stamps(files)
        if engine.redeploy(manifest):
            log.info("reload: serving deploy %s", manifest["deploy"][:12])
