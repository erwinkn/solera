"""Where a pool worker's attempts start (docs/lifecycle.md §10): a
forkserver, a process that imported the framework and, when it safely can,
the project, and forks each attempt from that state. An attempt still has
a process of its own, so a forced cancel ends it and nothing else, but it
imports nothing again.

Fork copies only the forking thread: a lock another thread held, or a
connection another thread used, would reach every attempt held or
half-used. So the forkserver keeps the project only if importing it left
the process as it found it: no exception, no thread but the main one
(counted by the OS where it can, so a native library's threads count), and
no new socket or pipe, which every attempt forked from it would share,
and within `PRELOAD_TIMEOUT`: the import runs before an attempt owns
anything, where nothing could cancel it. Otherwise it says why and exits,
and the pool worker starts one with the framework alone, whose attempts
import the project themselves, for as long as the pool worker runs. Signal
dispositions the import set are the attempts', not the forkserver's: it
restores its own and its children set the project's again.

A preloaded forkserver serves one deploy: the manifest digest of the code
it imported, its files hashed as they were imported (`Project.manifest`).
An attempt whose spec names another deploy exits `STALE` before it touches
anything; the pool worker replaces the forkserver, which imports the code
afresh, and runs the attempt from the new one.
"""

from __future__ import annotations

import contextlib
import ctypes
import json
import os
import signal
import socket
import stat
import subprocess
import sys

from .worker import LOST, STALE

PRELOAD_TIMEOUT = 60.0  # seconds a forkserver may take to import the project before it is refused


class Attempts:
    """A pool worker's attempts, each in a child of its forkserver, which
    starts with the first attempt and is replaced when the deploy changes.
    `run` blocks: call it on a thread."""

    def __init__(self, entrypoint: str, preload_timeout: float = PRELOAD_TIMEOUT):
        self.entrypoint, self.preload_timeout = entrypoint, preload_timeout
        self.server: Forkserver | None = None
        self.preload = True  # until a forkserver refuses the project

    def run(self, stage: dict, engine: str | None) -> int:
        """Run one attempt in a process of its own; its exit code."""

        if self.server is not None and self.server.process.poll() is not None:
            self.close()  # it died between attempts: this one was never forked
        check = self.server is not None
        if not check:
            self.server = self._start()
        code = self._run(stage, engine, check)
        if code == STALE:  # the engine serves other code than this forkserver imported
            self.close()
            self.server = self._start()
            code = self._run(stage, engine, False)
        return code

    def _run(self, stage: dict, engine: str | None, check: bool) -> int:
        """`check`: the forkserver may hold an older deploy than the spec's.
        One that has just imported the code runs it as an import of its own
        would."""

        server = self.server
        if server is None:  # closed meanwhile
            return LOST
        try:
            return server.run(stage, engine, check)
        except (
            OSError,
            ValueError,
        ) as error:  # it died, or was closed: how its attempt ended is the engine's to find
            print(f"[pool] the forkserver failed: {error}", file=sys.stderr, flush=True)
            self.close()
            return LOST

    def _start(self) -> Forkserver:
        if self.preload:
            server = Forkserver(self.entrypoint, timeout=self.preload_timeout)
            if server.refused is None:
                return server
            self.preload = False
            print(
                f"[pool] warning: attempts import the project themselves: {server.refused}",
                file=sys.stderr,
                flush=True,
            )
        server = Forkserver(self.entrypoint, preload=False, timeout=self.preload_timeout)
        if server.refused is not None:  # the framework alone did not start: no attempt can
            raise RuntimeError(f"the forkserver did not start: {server.refused}")
        return server

    def close(self) -> None:
        """Safe while `run` waits on another thread: that run returns `LOST`."""

        server, self.server = self.server, None
        if server is not None:
            server.close()


class Forkserver:
    """The pool worker's end of a forkserver: `deploy`, that of the project
    it imported (None with the framework alone), or `refused`, why it could
    not keep the project; that one has exited."""

    def __init__(self, entrypoint: str, preload: bool = True, timeout: float = PRELOAD_TIMEOUT):
        ours, theirs = socket.socketpair()
        boot = "import json, sys; sys.path[:] = json.loads(sys.argv[4]); from solera_worker.forkserver import serve; serve()"
        with theirs:
            self.process = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    boot,
                    str(theirs.fileno()),
                    entrypoint,
                    "preload" if preload else "",
                    json.dumps(sys.path),
                ],
                pass_fds=[theirs.fileno()],
            )
        self.socket, self.stream = ours, ours.makefile("rwb")
        ours.settimeout(timeout)
        try:
            line = self.stream.readline()
            hello = (
                json.loads(line)
                if line
                else {"refused": "the forkserver exited while it imported the project"}
            )
        except TimeoutError:
            self.process.kill()
            hello = {"refused": f"importing the project took more than {timeout:g} s"}
        ours.settimeout(None)
        self.deploy, self.refused = hello.get("deploy"), hello.get("refused")
        if self.refused is not None:
            self.close()

    def run(self, stage: dict, engine: str | None, check: bool) -> int:
        """Fork one attempt and wait for it to end; its exit code. `check`:
        it exits `STALE` if its spec names another deploy than this one."""

        self.stream.write(json.dumps({"stage": stage, "engine": engine, "check": check}).encode() + b"\n")
        self.stream.flush()
        line = self.stream.readline()
        if not line:
            raise ConnectionError("the forkserver exited")
        return json.loads(line)["code"]

    def close(self) -> None:
        """At the end of its stream it exits, or is killed if it waits on an
        attempt; that attempt runs on. A `run` waiting on another thread
        returns at once."""

        with contextlib.suppress(OSError):
            self.socket.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(OSError, ValueError):
            self.stream.close()
        self.socket.close()
        try:
            self.process.wait(1)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()


def serve() -> None:
    """The forkserver: `python -c ... FD ENTRYPOINT PRELOAD SYS_PATH`,
    PRELOAD empty for the framework alone. It says hello, then forks one
    attempt per request, waits for it and answers with its exit code."""

    signal.signal(signal.SIGINT, signal.SIG_IGN)  # Ctrl-C is the pool worker's to handle
    connection = socket.socket(fileno=int(sys.argv[1]))
    stream = connection.makefile("rwb")

    def send(message: dict) -> None:
        stream.write(json.dumps(message).encode() + b"\n")
        stream.flush()

    from . import worker

    _warm()
    entrypoint, project, theirs = sys.argv[2], None, {}
    if sys.argv[3]:
        own = _dispositions()
        project, refused = _preload(entrypoint, worker.load_project)
        if refused is not None:
            send({"refused": refused})
            return
        theirs = {
            number: handler
            for number, handler in _dispositions().items()
            if own.get(number, handler) != handler
        }
        for number in theirs:
            signal.signal(number, own[number])  # SIGCHLD ignored would leave no child to wait for
    send({"deploy": project.manifest["deploy"] if project is not None else None})
    for line in stream:
        request = json.loads(line)
        sys.stdout.flush()  # or each child would write what is buffered here once more
        sys.stderr.flush()
        with contextlib.suppress(Exception):
            ctypes.CDLL(None).fflush(None)  # C's stdio buffers too
        pid = os.fork()
        if pid == 0:
            try:
                stream.close()
                connection.close()  # the pool worker's and this server's alone
                signal.signal(signal.SIGINT, signal.default_int_handler)
                for number, handler in theirs.items():
                    signal.signal(number, handler)
                stale = request["check"] and project is not None
                worker._child(request["stage"], request["engine"], project or entrypoint, stale=stale)
            finally:
                os._exit(LOST)
        _, status = os.waitpid(pid, 0)
        try:
            send({"code": os.waitstatus_to_exitcode(status)})
        except OSError:
            return  # the pool worker is gone


def _warm() -> None:
    """Import what an attempt's channel would on its first call (about 80
    ms): httpx, its transports and asyncio backend, and the SSL context its
    clients share."""

    import asyncio
    import importlib

    from .channel import HttpTransport

    asyncio.run(HttpTransport("http://localhost", None).close())
    for name in ("httpcore._backends.anyio", "anyio._backends._asyncio"):
        with contextlib.suppress(ImportError):
            importlib.import_module(name)


def _preload(entrypoint: str, load):
    """`(project, None)`, the project imported here, or `(None, reason)`:
    why it cannot stay."""

    before = _channels()
    try:
        project = load(entrypoint)
    except BaseException as error:  # SystemExit too: each attempt reports its own import's
        return None, f"importing the project raised {type(error).__name__}: {error}"
    threads = _threads()
    if threads is None:
        return (
            None,
            "this platform does not count a process's threads, so none left by the import can be seen",
        )
    if threads > 1:
        return None, f"importing the project left {threads - 1} thread(s) running beside the main one"
    if opened := _channels() - before:
        return (
            None,
            f"importing the project left {len(opened)} socket(s) or pipe(s) open, which every attempt would share",
        )
    return project, None


def _threads() -> int | None:
    """This process's threads as the OS counts them, a native library's
    included; None where it gives no count (no /proc): Python's own count
    would miss those."""

    try:
        return len(os.listdir("/proc/self/task"))
    except OSError:
        return None


def _dispositions() -> dict[int, object]:
    """This process's signal dispositions set from Python, by signal."""

    found = {}
    for number in signal.valid_signals():
        with contextlib.suppress(OSError, ValueError):
            if (handler := signal.getsignal(number)) is not None:
                found[number] = handler
    return found


def _channels() -> set[tuple[int, int, int]]:
    """This process's open sockets and pipes: (descriptor, device, inode)."""

    found = set()
    for name in os.listdir("/proc/self/fd" if os.path.isdir("/proc/self/fd") else "/dev/fd"):
        with contextlib.suppress(OSError):
            info = os.fstat(int(name))
            if stat.S_ISSOCK(info.st_mode) or stat.S_ISFIFO(info.st_mode):
                found.add((int(name), info.st_dev, info.st_ino))
    return found
