"""The project's build identity (docs/per-key-processing.md §13): what the
deploy says about the code, beyond the manifest.

In order:

1. an explicit id — `Project(build=…)`, else `$SOLERA_BUILD` — for builds
   CI or an image names (a commit of a clean checkout, an image digest);
2. otherwise, the code the project runs: the source of every module under
   the project's directory that its own objects reach — the module that
   builds it, its assets', sources', sensors' and stores' modules, and
   every module their globals name, transitively (`modules`). A doc, a
   test, data or state beside it changes nothing; an edit to its code
   does, committed or not.

The git commit, where there is one, is recorded for display only. Where
the manifest is built and where workers import the project compute the
same identity from the same code; hosts that import it from elsewhere
(an image) should set `SOLERA_BUILD` everywhere.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import types


def identity(directory: str | None, explicit: str | None = None, modules=()) -> dict:
    """`{"id", "source"}`, plus `commit` from git, for display."""

    explicit = explicit or os.environ.get("SOLERA_BUILD")
    if explicit:
        return {"id": str(explicit), "source": "explicit"}
    if not directory:
        return {"id": "", "source": "none"}
    h = hashlib.sha256(b"modules\0")
    for path in sorted(modules):
        h.update(os.path.relpath(path, directory).encode(errors="surrogateescape") + b"\0")
        h.update(_file_digest(path))
    out = {"id": h.hexdigest(), "source": "modules"}
    head = _run(directory, "rev-parse", "HEAD")
    if head:
        out["commit"] = head.decode().strip()
    return out


def modules(directory: str | None, roots) -> list[str]:
    """The source files of the modules under `directory` that `roots` — a
    project's own objects and its module — reach: each object's module,
    and every module the globals of one found name, transitively."""

    if not directory:
        return []
    top = os.path.join(os.path.abspath(directory), "")
    found: dict[str, None] = {}
    queue = [m for m in (_module_of(r) for r in roots) if m is not None]
    while queue:
        module = queue.pop()
        path = getattr(module, "__file__", None)
        if not path or not os.path.abspath(path).startswith(top) or path in found:
            continue
        found[path] = None
        queue.extend(m for m in map(_module_of, list(vars(module).values())) if m is not None)
    return sorted(found)


def _module_of(value) -> types.ModuleType | None:
    if isinstance(value, types.ModuleType):
        return value
    try:
        name = getattr(value, "__module__", None)
    except Exception:  # an object that refuses attribute reads
        return None
    return sys.modules.get(name) if isinstance(name, str) else None


def _run(directory: str, *args: str) -> bytes | None:
    try:
        done = subprocess.run(["git", "-C", directory, *args], capture_output=True, timeout=30, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout if done.returncode == 0 else None


def _file_digest(path: str) -> bytes:
    """A file's content digest; a deleted one digests as absent."""

    try:
        with open(path, "rb") as f:
            return hashlib.file_digest(f, "sha256").digest()
    except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
        return b"absent"


METHODS = {
    "explicit": "an explicit build id",
    "modules": "the source of the project's modules",
}


def method_note(served: dict | None, reported: dict | None) -> str | None:
    """Why two deploys differ when it is the method, not the code: the
    engine serves a deploy computed one way and a worker or sensor worker
    computed its own another way — an explicit id against the project's
    modules — so they never agree. `None` when the methods match."""

    a, b = (served or {}).get("source"), (reported or {}).get("source")
    if not a or not b or a == b:
        return None
    return (
        f"the engine's deploy comes from {METHODS.get(a, a)}, this host's from {METHODS.get(b, b)}: "
        "they will never match — set SOLERA_BUILD (e.g. the git commit) wherever the project is "
        "registered and wherever workers and sensor hosts import it"
    )
