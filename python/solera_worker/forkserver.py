"""Where a pool worker's attempts start (docs/lifecycle.md §10): a
forkserver, a process that imported the framework and, when it safely can,
the project, and forks each attempt from that state. An attempt still has
a process of its own, so a forced cancel ends it and nothing else, but it
imports nothing again.

Two rules make that safe: after the import, the forkserver runs no project
code; and each attempt starts from what a fresh import would have left.
The first holds by construction where it can: once the import returns the
forkserver blocks every signal (each attempt restores the mask), so no
handler the project installed runs in it; it freezes what the import made
out of the garbage collector, so no finalizer runs in it; and it leaves
through `os._exit`, so no exit handler does. The rest it checks, and keeps
the project only if importing it raised nothing, within `PRELOAD_TIMEOUT`
(the import runs before an attempt owns anything, where nothing could
cancel it), and left:

- no thread but the main one: fork copies only the forking thread, so a
  lock another thread held would reach every attempt held. Counted by the
  OS, so a native library's threads count; refused where it cannot count.
- no new socket or pipe, which every attempt would share half-used.
- SIGCHLD not ignored, natively either (`SA_NOCLDWAIT`), so the
  forkserver can wait for its attempts. It sets SIGCHLD to its default
  before it imports anything; a handler the project installs is blocked
  here and runs in its attempts.
- the signal mask as it was, and no timer running, which no attempt forked
  from it would have.
- nothing the interpreter or the forkserver would call later with project
  code in it: an at-fork hook that runs in the parent, an audit hook, a
  trace, profile, monitoring or garbage collector callback, or a function
  or stream of a module the forkserver calls (gevent's `os.fork`, a new
  `sys.stdout`).

Otherwise it says why and exits, and the pool worker starts one with the
framework alone, whose attempts import the project themselves, for as long
as the pool worker runs.

A preloaded forkserver serves one deploy: the manifest digest of the code
it imported, its files hashed as they were imported (`Project.manifest`).
An attempt whose spec names another deploy exits `STALE` before it touches
anything; the pool worker replaces the forkserver, which imports the code
afresh, and runs the attempt from the new one.
"""

from __future__ import annotations

import builtins
import contextlib
import ctypes
import gc
import json
import os
import signal
import socket
import stat
import subprocess
import sys
import threading

from .worker import LOST, STALE

PRELOAD_TIMEOUT = 60.0  # seconds a forkserver may take to import the project before it is refused


class Attempts:
    """A pool worker's attempts, each in a child of its forkserver, which
    starts with the first attempt and is replaced when the deploy changes.
    `run` blocks: call it on a thread. Once `close` returns, on any thread,
    no forkserver starts and no request reaches one: a `run` under way
    returns `LOST`, and an attempt already requested runs on."""

    def __init__(self, entrypoint: str, preload_timeout: float = PRELOAD_TIMEOUT):
        self.entrypoint, self.preload_timeout = entrypoint, preload_timeout
        self.server: Forkserver | None = None
        self.preload = True  # until a forkserver refuses the project
        self.closed = False
        self.lock = threading.Lock()

    def run(self, stage: dict, engine: str | None) -> int:
        """Run one attempt in a process of its own; its exit code."""

        server = self.server
        if server is not None and server.process.poll() is not None:
            self._drop(server)  # it died between attempts: this one was never forked
            server = None
        check = server is not None
        if server is None and (server := self._start()) is None:
            return LOST
        code = self._run(server, stage, engine, check)
        if code == STALE:  # the engine serves other code than this forkserver imported
            self._drop(server)
            if (server := self._start()) is None:
                return LOST
            code = self._run(server, stage, engine, False)
        return code

    def _run(self, server: Forkserver, stage: dict, engine: str | None, check: bool) -> int:
        """`check`: the forkserver may hold an older deploy than the spec's.
        One that has just imported the code runs it as an import of its own
        would."""

        try:
            return server.run(stage, engine, check)
        except (
            OSError,
            ValueError,
        ) as error:  # it died, or was closed: how its attempt ended is the engine's to find
            if not self.closed:
                print(f"[pool] the forkserver failed: {error}", file=sys.stderr, flush=True)
            self._drop(server)
            return LOST

    def _start(self) -> Forkserver | None:
        """A forkserver ready to fork, or None once closed."""

        if self.preload:
            server = self._ready(preload=True)
            if server is None or server.refused is None:
                return server
            self.preload = False
            print(
                f"[pool] warning: attempts import the project themselves: {server.refused}",
                file=sys.stderr,
                flush=True,
            )
        server = self._ready(preload=False)
        if (
            server is not None and server.refused is not None
        ):  # the framework alone did not start: no attempt can
            raise RuntimeError(f"the forkserver did not start: {server.refused}")
        return server

    def _ready(self, preload: bool) -> Forkserver | None:
        """A forkserver once it said hello, or None if closed first or
        meanwhile: it starts and is published under the lock, so `close`
        ends its import too."""

        with self.lock:
            if self.closed:
                return None
            server = self.server = Forkserver(self.entrypoint, preload)
        server.hello(self.preload_timeout)
        if self.closed:
            return None
        if server.refused is not None:
            self._drop(server)
        return server

    def _drop(self, server: Forkserver) -> None:
        with self.lock:
            if self.server is server:
                self.server = None
        server.close()

    def close(self) -> None:
        with self.lock:  # every caller returns once the forkserver is gone: a request sent later fails
            self.closed = True
            if self.server is not None:
                self.server.close()
                self.server = None


class Forkserver:
    """The pool worker's end of a forkserver: `deploy`, that of the project
    it imported (None with the framework alone), or `refused`, why it could
    not keep the project; that one has exited. It runs in a process group
    of its own, so a Ctrl-C meant for the pool worker does not reach it; the
    attempts it forks join the pool worker's, so it reaches them."""

    def __init__(self, entrypoint: str, preload: bool = True):
        ours, theirs = socket.socketpair()
        boot = "import json, sys; sys.path[:] = json.loads(sys.argv[5]); from solera_worker.forkserver import serve; serve()"
        with theirs:
            self.process = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    boot,
                    str(theirs.fileno()),
                    entrypoint,
                    "preload" if preload else "",
                    str(os.getpgrp()),
                    json.dumps(sys.path),
                ],
                pass_fds=[theirs.fileno()],
                process_group=0,
            )
        self.socket, self.stream = ours, ours.makefile("rwb")
        self.deploy = self.refused = None

    def hello(self, timeout: float) -> None:
        """Wait for it to import what it imports, within `timeout` seconds."""

        try:
            self.socket.settimeout(timeout)
            line = self.stream.readline()
            hello = (
                json.loads(line)
                if line
                else {"refused": "the forkserver exited while it imported the project"}
            )
        except TimeoutError:
            self.process.kill()
            hello = {"refused": f"importing the project took more than {timeout:g} s"}
        except (OSError, ValueError):  # closed meanwhile
            hello = {"refused": "the forkserver was closed while it imported the project"}
        with contextlib.suppress(OSError):
            self.socket.settimeout(None)
        self.deploy, self.refused = hello.get("deploy"), hello.get("refused")

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
        """At the end of its stream it exits, or is killed if it is still
        importing or waits on an attempt; that attempt runs on. A `hello` or
        `run` waiting on another thread returns at once."""

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
    """The forkserver: `python -c ... FD ENTRYPOINT PRELOAD PGID SYS_PATH`,
    PRELOAD empty for the framework alone, PGID the pool worker's process
    group. It says hello, then forks one attempt per request, waits for it
    and answers with its exit code. Before it imports anything it sets
    SIGCHLD to the default, so it can wait for its children whatever the
    pool worker's was; once it has, it blocks every signal, and each
    attempt restores the mask it started with: an attempt has the handlers
    and dispositions the project's import left, as if it imported it."""

    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    connection = socket.socket(fileno=int(sys.argv[1]))
    stream = connection.makefile("rwb")

    def send(message: dict) -> bool:
        """False once the pool worker hung up."""

        try:
            stream.write(json.dumps(message).encode() + b"\n")
            stream.flush()
        except OSError:
            return False
        return True

    from . import worker

    _warm()
    entrypoint, project, refused, group = sys.argv[2], None, None, int(sys.argv[4])
    mask = signal.pthread_sigmask(signal.SIG_BLOCK, ())
    if sys.argv[3]:
        project, refused = _preload(entrypoint, worker.load_project)
    signal.pthread_sigmask(signal.SIG_BLOCK, signal.valid_signals())
    if refused is not None:
        send({"refused": refused})
    elif send({"deploy": project.manifest["deploy"] if project is not None else None}):
        for line in stream:
            request = json.loads(line)
            sys.stdout.flush()  # or each child would write what is buffered here once more
            sys.stderr.flush()
            with contextlib.suppress(Exception):
                ctypes.CDLL(None).fflush(None)  # C's stdio buffers too
            pid = os.fork()
            if pid == 0:
                try:
                    signal.pthread_sigmask(signal.SIG_SETMASK, mask)
                    stream.close()
                    connection.close()  # the pool worker's and this server's alone
                    with contextlib.suppress(OSError):
                        os.setpgid(0, group)
                    stale = request["check"] and project is not None
                    worker._child(request["stage"], request["engine"], project or entrypoint, stale=stale)
                finally:
                    os._exit(LOST)
            _, status = os.waitpid(pid, 0)
            if not send({"code": os.waitstatus_to_exitcode(status)}):
                break  # the pool worker is gone
    with contextlib.suppress(OSError, ValueError):
        sys.stdout.flush()
        sys.stderr.flush()
    os._exit(0)


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
    why it cannot stay. Every signal is blocked as soon as the import
    returns, and a handler already due runs then, before the checks: none
    can run here after them. What the import registers through the
    interpreter's own functions is recorded as it registers it."""

    mask, channels = signal.pthread_sigmask(signal.SIG_BLOCK, ()), _channels()
    framework, hooks, registered = _framework(), _hooks(), []
    register_at_fork, addaudithook = os.register_at_fork, sys.addaudithook

    def at_fork(**callbacks) -> None:
        if callbacks.keys() & {"before", "after_in_parent"}:
            registered.append("an at-fork hook")
        register_at_fork(**callbacks)

    def audit(hook) -> None:
        registered.append("an audit hook")
        addaudithook(hook)

    os.register_at_fork, sys.addaudithook = at_fork, audit
    try:
        project = load(entrypoint)
    except BaseException as error:  # SystemExit too: each attempt reports its own import's
        return None, f"importing the project raised {type(error).__name__}: {error}"
    finally:
        os.register_at_fork, sys.addaudithook = register_at_fork, addaudithook
        blocked = signal.pthread_sigmask(signal.SIG_BLOCK, signal.valid_signals())
    if replaced := sorted(
        f"{module.__name__}.{name}"
        for (module, name), value in framework.items()
        if module.__dict__.get(name) is not value
    ):
        return None, f"importing the project replaced {', '.join(replaced)}, which the forkserver calls"
    threads = _threads()
    if threads is None:
        return (
            None,
            "this platform does not count a process's threads, so none left by the import can be seen",
        )
    if threads > 1:
        return None, f"importing the project left {threads - 1} thread(s) running beside the main one"
    if opened := _channels() - channels:
        return (
            None,
            f"importing the project left {len(opened)} socket(s) or pipe(s) open, which every attempt would share",
        )
    if _ignored() & 1 << (signal.SIGCHLD - 1):
        return (
            None,
            "importing the project ignored SIGCHLD, so the forkserver could not wait for its attempts",
        )
    if blocked != mask:
        return None, "importing the project changed which signals are blocked"
    if any(
        signal.getitimer(timer)[0]
        for timer in (signal.ITIMER_REAL, signal.ITIMER_VIRTUAL, signal.ITIMER_PROF)
    ):
        return None, "importing the project left a timer running, which no attempt forked from it would have"
    registered += [name for name, value in _hooks().items() if hooks[name] != value]
    if registered:
        return (
            None,
            f"importing the project registered {', '.join(registered)}, which would run its code here",
        )
    pid = os.fork()  # SIGCHLD's default, but with SA_NOCLDWAIT set natively: no child to wait for
    if pid == 0:
        os._exit(0)
    try:
        os.waitpid(pid, 0)
    except ChildProcessError:
        return None, "importing the project left the forkserver no child to wait for (SA_NOCLDWAIT)"
    gc.freeze()
    return project, None


def _framework() -> dict:
    """What the forkserver calls after the import, by (module, name): every
    function of the modules it calls, and the streams it flushes."""

    found = {(sys, name): getattr(sys, name) for name in ("stdout", "stderr")}
    for module in (builtins, contextlib, ctypes, gc, json, os, os.path, signal, stat):
        found |= {(module, name): value for name, value in vars(module).items() if callable(value)}
    return found


def _hooks() -> dict[str, object]:
    """What the interpreter calls on its own, by what it is."""

    return {
        "a trace function": sys.gettrace(),
        "a profile function": sys.getprofile(),
        "a monitoring tool": [sys.monitoring.get_tool(tool) for tool in range(6)],
        "a garbage collector callback": list(gc.callbacks),
    }


def _ignored() -> int:
    """The signals this process ignores (/proc/self/status SigIgn), as a
    mask: native code's settings included."""

    with open("/proc/self/status") as status:
        for line in status:
            if line.startswith("SigIgn:"):
                return int(line.split()[1], 16)
    return 0


def _threads() -> int | None:
    """This process's threads as the OS counts them, a native library's
    included; None where it gives no count (no /proc): Python's own count
    would miss those."""

    try:
        return len(os.listdir("/proc/self/task"))
    except OSError:
        return None


def _channels() -> set[tuple[int, int, int]]:
    """This process's open sockets and pipes: (descriptor, device, inode)."""

    found = set()
    for name in os.listdir("/proc/self/fd" if os.path.isdir("/proc/self/fd") else "/dev/fd"):
        with contextlib.suppress(OSError):
            info = os.fstat(int(name))
            if stat.S_ISSOCK(info.st_mode) or stat.S_ISFIFO(info.st_mode):
                found.add((int(name), info.st_dev, info.st_ino))
    return found
