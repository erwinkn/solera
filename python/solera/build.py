"""The project's build identity (docs/per-key-processing.md §13): what the
project revision says about the code, beyond the manifest.

In order:

1. an explicit id — `Project(build=…)`, else `$SOLERA_BUILD` — for builds
   CI or an image names (a commit of a clean checkout, an image digest);
2. inside a git work tree, its content: the `HEAD` commit, plus every path
   that differs from it — modified, deleted, or untracked and not ignored —
   with a digest of its bytes;
3. otherwise, the digest of every Python file under the project's
   directory.

A commit with a dirty flag is not an identity — two different uncommitted
edits share it — so the commit and the flag are recorded for display only.
Where the manifest is built and where workers import the project must
agree on the identity: images without `.git` should set `SOLERA_BUILD`.
"""

from __future__ import annotations

import hashlib
import os
import subprocess

SKIPPED_DIRS = {"__pycache__", "node_modules", "site-packages", "venv"}


def identity(directory: str | None, explicit: str | None = None) -> dict:
    """`{"id", "source"}`, plus `commit` and `dirty` from git, for display."""

    explicit = explicit or os.environ.get("SOLERA_BUILD")
    if explicit:
        return {"id": str(explicit), "source": "explicit"}
    if directory:
        found = _git(directory)
        if found is not None:
            return found
        return {"id": _python_files(directory), "source": "files"}
    return {"id": "", "source": "none"}


def _run(directory: str, *args: str) -> bytes | None:
    try:
        done = subprocess.run(["git", "-C", directory, *args], capture_output=True, timeout=30, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout if done.returncode == 0 else None


def _git(directory: str) -> dict | None:
    top = _run(directory, "rev-parse", "--show-toplevel")
    if top is None:
        return None
    top = top.decode().strip()
    head = (_run(directory, "rev-parse", "HEAD") or b"").decode().strip()
    status = _run(top, "status", "--porcelain=v1", "-z", "--untracked-files=all", "--no-renames")
    if status is None:
        return None
    h = hashlib.sha256(b"git\0" + head.encode() + b"\0")
    changed = sorted({entry[3:] for entry in status.decode(errors="surrogateescape").split("\0") if entry})
    for path in changed:
        h.update(path.encode(errors="surrogateescape") + b"\0")
        h.update(_file_digest(os.path.join(top, path)))
    return {"id": h.hexdigest(), "source": "git", "commit": head, "dirty": bool(changed)}


def _python_files(directory: str) -> str:
    h = hashlib.sha256(b"files\0")
    paths = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = sorted(d for d in dirs if not d.startswith(".") and d not in SKIPPED_DIRS)
        paths.extend(os.path.join(root, f) for f in files if f.endswith(".py"))
    for path in sorted(paths):
        h.update(os.path.relpath(path, directory).encode(errors="surrogateescape") + b"\0")
        h.update(_file_digest(path))
    return h.hexdigest()


def _file_digest(path: str) -> bytes:
    """A file's content digest; a deleted path, or a directory git lists,
    digests as absent."""

    try:
        with open(path, "rb") as f:
            return hashlib.file_digest(f, "sha256").digest()
    except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
        return b"absent"
