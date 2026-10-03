"""The attempt lifecycle on the engine (docs/lifecycle.md §3–§8, §10): the
launch, what workers report over the channel, watching, the two-phase
cancel, and how an attempt ends — from its result, or from its control file.

Mixed into `Engine`. Liveness is evidence only: reports decide when the
engine stops waiting, never whether an attempt's writes may still land
(§6, §2.3).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
from collections import deque
from dataclasses import dataclass, field

from solera import errors, lifecycle
from solera.build import method_note
from solera.lifecycle import Cancel, Ended
from solera.objects import Conflict, swap

from .state import LostOwnership, Unavailable

log = logging.getLogger(__name__)

LIVE_LINES = 10_000  # live log lines kept per attempt for the console
AFTER_COMMIT_WAIT = 10.0  # seconds a worker's `finished` waits for its commit, for its cleanups
POOL_OFFERED_GRACE = 10.0  # an offered pool attempt not started this long: look for its claim
POOL_PAGE = 8  # attempts one discovery answer offers


async def _unless(stirred: asyncio.Event, work):
    """Await `work`, but give up on it, returning `None`, once `stirred` is set."""

    job, woken = asyncio.ensure_future(work), asyncio.ensure_future(stirred.wait())
    try:
        await asyncio.wait((job, woken), return_when=asyncio.FIRST_COMPLETED)
    finally:
        for pending in (job, woken):
            pending.cancel()
        await asyncio.gather(job, woken, return_exceptions=True)
        stirred.clear()
    return job.result() if not job.cancelled() else None


def _number(value) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value) if math.isfinite(value) else None


def _names(value) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}
    return {str(k): [str(i) for i in v] for k, v in value.items() if isinstance(v, list)}


def cleanup_report(body) -> dict:
    """A worker's clean up acknowledgement as the model applies it: its
    `partition`, and by output the entry ids it cleaned up (`cleaned up`) or
    could not read the names of (`discard_unresolved`), and the index files
    it deleted (`discarded_files`)."""

    if not isinstance(body, dict) or not isinstance(body.get("partition"), str):
        raise ValueError("a cleanup report names its partition")
    out = {"partition": body["partition"]}
    for name in ("cleaned_up", "cleanup_unresolved"):
        value = body.get(name)
        if value is None:
            continue
        if not isinstance(value, dict) or not all(
            isinstance(v, list) and all(isinstance(i, str) for i in v) for v in value.values()
        ):
            raise ValueError(f"{name}: entry ids by output")
        out[name] = value
    files = body.get("cleaned_files")
    if files is not None:
        if not isinstance(files, list) or not all(isinstance(f, str) for f in files):
            raise ValueError("discarded_files: a list of paths")
        out["cleaned_files"] = files
    return out


def worker_output(info: dict) -> dict:
    """An output's launch record as its worker sees it: the committed head
    only as its ref (`before`), where the content is."""

    head = info["head"]
    return {**{k: v for k, v in info.items() if k != "head"}, "before": head["ref"] if head else None}


def current_names(prepared: dict, by_output: dict) -> dict:
    """What a worker reports by output, under the outputs' current names: it
    knows them by the names it was launched with (`as`), which a rename
    while it ran has since moved (§2)."""

    names = {info.get("as", name): name for name, info in (prepared.get("outputs") or {}).items()}
    return {names.get(name, name): value for name, value in by_output.items()}


def worker_report(worker: dict | None) -> dict:
    """What a worker's result or report may put in an event, as values the
    model applies with no parsing (review round 3, B5): its timeline
    events, its usage, the cleanup it cleaned up, and its keys by
    outcome. A malformed part is left out; a worker cannot make a reducer
    fail half-way."""

    if not isinstance(worker, dict):
        return {}
    out = {}
    events = []
    for event in worker.get("events") or ():
        if not isinstance(event, dict) or not isinstance(event.get("type"), str):
            continue
        at = _number(event.get("at"))
        if at is None:
            continue
        kept = {"type": event["type"][:64], "at": at}
        if isinstance(event.get("name"), str):
            kept["name"] = event["name"][:200]
        if isinstance(event.get("rows"), int) and not isinstance(event.get("rows"), bool):
            kept["rows"] = event["rows"]
        events.append(kept)
    if events:
        out["events"] = events
    usage = worker.get("usage") if isinstance(worker.get("usage"), dict) else {}
    usage = {k: _number(v) for k, v in usage.items() if isinstance(k, str)}
    if usage := {k: v for k, v in usage.items() if v is not None}:
        out["usage"] = usage
    for name in ("cleaned_up", "cleanup_unresolved"):
        if found := _names(worker.get(name)):
            out[name] = found
    files = worker.get("cleaned_files")
    if isinstance(files, list) and (files := [str(f) for f in files]):
        out["cleaned_files"] = files
    keys = worker.get("keys")
    if isinstance(keys, dict) and (
        keys := {str(k): int(v) for k, v in keys.items() if _number(v) is not None}
    ):
        out["keys"] = keys
    if read := [r for r in map(_read, worker.get("read") or ()) if r is not None]:
        out["read"] = read
    return out


def _read(entry) -> dict | None:
    """One partition a worker's read saw (`solera_worker.observed`): its output
    and partition, and the generation whose write it saw (None: no fenced write
    says, as for an external table)."""

    if not isinstance(entry, dict) or not isinstance(entry.get("output"), str):
        return None
    generation = entry.get("generation")
    return {
        "output": entry["output"],
        "partition": str(entry.get("partition") or ""),
        "generation": int(generation) if _number(generation) is not None else None,
    }


@dataclass
class Live:
    """What the engine knows of a launched attempt's worker, in memory only:
    rebuilt from the control file and `.beat` after a restart (§5.3)."""

    worker_id: str | None = None  # the bound worker: the claim's owner
    started: bool = False  # it reported: the runtime clock runs
    started_at: float = 0.0  # monotonic
    reported: float | None = None  # when it last reported (monotonic)
    via: str = "channel"  # how: over the channel, or through the object store
    seq: int = 0
    report: dict = field(default_factory=dict)  # its last timeline and usage
    beat: bytes | None = None  # the `.beat` bytes last read
    cancel: Cancel | None = None  # the latched cancel record (§2.2)
    finished: bool = False  # the worker said its result is written
    offered_at: float | None = None  # a pool attempt: when discovery first offered it
    launching: bool = False  # its launch is recorded, not yet durable: no one outside sees it
    lines: deque = field(default_factory=lambda: deque(maxlen=LIVE_LINES))
    log_offset: int = 0
    fresh: bool = False  # launched by this engine process: its first `start` binds unread
    reads: dict | None = None  # its spec's inputs and outputs, until `start` reads them

    def heard(self, now: float, via: str) -> None:
        self.reported, self.via = now, via
        if not self.started:
            self.started, self.started_at = True, now


class Attempts:
    """The lifecycle half of the engine."""

    def _live(self, attempt: str) -> Live:
        """The live record of an attempt that still holds its claim."""

        claim = self.m.claimed(attempt)
        if claim is None or not claim.get("launched"):
            raise Ended("ended")
        return self.live.setdefault(attempt, Live())

    def _authority(self) -> None:
        """This engine still owns the namespace: checked right before every
        external effect — a launch, a cancel, a gate, a deletion — and
        again after each await, since a successor may take over meanwhile.
        A replaced engine changes nothing its successor owns."""

        if self.state.poisoned:
            raise LostOwnership("this engine was replaced: its successor owns the namespace")

    def _serving(self) -> None:
        """A worker's request reaches an engine that still owns the
        namespace; a replaced one answers 503 — never 409, which would tell
        a worker its successor still wants to stop writing."""

        if self.state.poisoned:
            raise Unavailable("this engine was replaced; ask again: its successor answers")

    def _stir(self, attempt: str) -> None:
        self._stirred.setdefault(attempt, asyncio.Event()).set()

    def _speaking(self, live: Live, now: float) -> bool:
        """Has the worker reported recently: three beats over the channel,
        three `.beat` updates (two beats apart) through the object store."""

        if live.reported is None:
            return False
        window = 3 if live.via == "channel" else 6
        return now - live.reported <= window * self.heartbeat_seconds

    async def _owner(self, run_id: str, attempt: str) -> str | None:
        """The worker that owns the attempt, as its control file says (§2.4)."""

        found = await lifecycle.read_control(self.state.objects, run_id, attempt)
        return found[0].get("worker_id") if found is not None else None

    async def _bind(self, attempt: str, live: Live, worker_id: str, start: bool = False) -> None:
        """The first `start` of an attempt this engine launched binds its
        worker: only the claim's winner sends one. Any other token, or
        any request after a restart, is checked against the claim itself
        (§5.3)."""

        if live.worker_id == worker_id:
            return
        if start and live.worker_id is None and live.fresh:
            live.worker_id = worker_id
            return
        owner = await self._owner(self.m.task(self.m.attempts[attempt])["run"], attempt)
        if owner != worker_id:
            raise Ended("not_owner")
        live.worker_id = owner

    def _cancel_answer(self, live: Live) -> dict:
        return {"cancel": live.cancel.to_json() if live.cancel else None}

    # -- the channel's handlers (§5) ---------------------------------------------------

    async def attempt_start(self, attempt: str, body: dict) -> dict:
        self._serving()
        live = self._live(attempt)
        await self._bind(attempt, live, body["worker_id"], start=True)
        live.heard(asyncio.get_running_loop().time(), "channel")
        self._stir(attempt)
        answer = self._cancel_answer(live)
        spec, live.reads = live.reads, None  # answered once: a retried start reads the store
        if spec is not None and self.keys is not None:
            # The attempt's input reads, from the engine's cache (docs/resolved-commits.md §7).
            reads = await self.keys.reads(spec, self.m.event_counter)
            if reads is not None:
                answer["reads"] = reads
        return answer

    async def attempt_beat(self, attempt: str, body: dict) -> dict:
        self._serving()
        live = self._live(attempt)
        await self._bind(attempt, live, body["worker_id"])
        if int(body.get("seq", 0)) > live.seq:  # a retried or reordered beat changes nothing
            live.seq = int(body["seq"])
            live.report = {k: body[k] for k in ("events", "usage") if k in body}
        live.heard(asyncio.get_running_loop().time(), "channel")
        return self._cancel_answer(live)

    async def attempt_logs(self, attempt: str, body: dict) -> dict:
        self._serving()
        live = self._live(attempt)
        await self._bind(attempt, live, body["worker_id"])
        offset, lines = int(body["offset"]), list(body["lines"])
        if offset > live.log_offset:  # lines lost for good: the chunks hold them
            live.log_offset = offset
        new = lines[live.log_offset - offset :]
        live.lines.extend(new)
        live.log_offset += len(new)
        return {"offset": live.log_offset}

    async def attempt_finished(self, attempt: str, body: dict) -> dict:
        self._serving()
        live = self._live(attempt)
        await self._bind(attempt, live, body["worker_id"])
        task_id = self.m.attempts.get(attempt)
        live.finished = True
        self._stir(attempt)
        return await self._due_after(attempt, task_id)

    async def _due_after(self, attempt: str, task_id: str | None) -> dict:
        """Once the attempt is settled and its commit durable, the data
        garbage due in its partition — what its commit let go of that no reader
        pins, and what was waiting — for its worker to clean up at once
        (docs/lifecycle.md §9.8). Nothing if settling takes longer: the
        partition's next attempt cleanups it, as ever."""

        task = self.m.task(task_id or "")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + AFTER_COMMIT_WAIT
        while task is not None and self.m.claimed(attempt) is not None and loop.time() < deadline:
            await asyncio.sleep(0.02)
        if task is None or self.m.claimed(attempt) is not None:
            return {}
        await self.state.durable()  # never delete what a replay would still name
        due = {}
        for output in self.manifest["assets"].get(task["asset"], {}).get("outputs") or ():
            name, head = output["name"], self.m.heads.get((output["name"], task["partition"]))
            if self.m.immutable(name) and head is not None:
                entries = self._due_cleanups(name, task["partition"], None)
                if entries:
                    due[name] = {"cleanup": entries, "before": head["ref"]}
        return {"cleanup": due, "partition": task["partition"]} if due else {}

    async def attempt_cleaned_up(self, attempt: str, body) -> None:
        """A worker's acknowledgement of what it cleaned up after its commit,
        checked whole before anything is recorded: one that is malformed is
        refused (`ValueError`), and no reducer ever sees it."""

        self._serving()
        report = cleanup_report(body)
        if report.get("cleaned_up") or report.get("cleanup_unresolved"):
            self.state.record({"type": "CleanupsDone", **report})

    async def attempt_resolve(self, attempt: str, body: bytes) -> bytes | None:
        """A small write's delta from the engine's cache (docs/resolved-commits.md
        §4), or None when the engine keeps no cache. Every output is checked
        against what the engine prepared for the attempt, never trusted."""

        self._serving()
        from solera.keys.resolver import MAX_BODY, Malformed, Prepared, _outputs, unframe

        live = self._live(attempt)
        if len(body) > MAX_BODY:
            raise Malformed(f"a body over {MAX_BODY} bytes")
        header, payloads = unframe(body)
        outputs_asked = _outputs(header, payloads)
        worker_id = header.get("worker_id")
        if not isinstance(worker_id, str) or not worker_id:
            raise Ended("not_owner")  # no identity is never the owner's
        await self._bind(attempt, live, worker_id)
        if self.keys is None:
            return None
        task = self.m.task(self.m.attempts[attempt])
        launched = task.get("launched") or {}
        partition, outputs = task["partition"], (launched.get("prepared") or {}).get("outputs") or {}

        def prepared(name):
            info = outputs.get(name)
            if not info or "prefix" not in info or "commit_number" not in info:
                return None
            index = self.m.indexes.get((name, partition))
            head = self.m.heads.get((name, partition)) or {}
            if index is None and int(info["commit_number"]) == 0:
                index = self.m.index(name, partition)  # the first write of the output
            if index is None or index.prefix != info["prefix"]:
                return None
            return Prepared(
                partition,
                int(info["commit_number"]),
                int(launched["generation"]),
                index,
                int(head.get("commit_number", -1)),
                True,
                self.m.event_counter,  # the index as of now: what a fill of it reads
            )

        def still_live():
            if self.m.claimed(attempt) is None:
                return False
            return live.cancel is None or live.cancel.phase != "forced"

        asked = current_names(
            launched.get("prepared") or {}, {o["name"]: o["name"] for o, _ in outputs_asked}
        )
        resolved = {name: prepared(current) for current, name in asked.items()}  # by the worker's names
        return await self.keys.resolve(attempt, body, resolved.get, still_live, self.m.event_counter)

    def attempt_lines(self, attempt: str) -> list[str] | None:
        live = self.live.get(attempt)
        return list(live.lines) if live is not None else None

    # -- pool discovery (§10) ------------------------------------------------------------

    async def pool_work(self, pool: str, capacity: dict, host: str, wait: float) -> list[dict]:
        """Launched pool attempts that fit `capacity` and have not started,
        oldest first; waits up to `wait` seconds for one. A hint: workers
        that get the same attempt race for its claim."""

        self._serving()
        loop = asyncio.get_running_loop()
        self.pollers[host] = {"id": host, "pools": [pool], "capacity": capacity, "seen_at": self.clock()}
        deadline = loop.time() + max(0.0, min(wait, 30.0))
        while True:
            changed = self._pool_changed
            found = []
            for record in sorted(self.m.pool.values(), key=lambda r: r["created_at"]):
                live = self.live.get(record["attempt"])
                if record["pool"] != pool or (live is not None and (live.started or live.launching)):
                    continue
                if (self.m.task(record["task"]) or {}).get("status") == "canceled":
                    continue  # being ended
                needs = record.get("needs") or {}
                if any(capacity.get(d) is None or capacity[d] < want for d, want in needs.items()):
                    continue
                found.append(record)
                if len(found) == POOL_PAGE:
                    break
            remaining = deadline - loop.time()
            if found or remaining <= 0:
                break
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(changed.wait(), remaining)
        now = loop.time()
        for record in found:
            live = self.live.setdefault(record["attempt"], Live())
            if live.offered_at is None:
                live.offered_at = now
        return [{"attempt": r["attempt"], "run": r["run"], "objects": self.state.objects_url} for r in found]

    def _pool_wake(self) -> None:
        changed, self._pool_changed = self._pool_changed, asyncio.Event()
        changed.set()

    # -- launching (§3) ------------------------------------------------------------------

    async def _launch(self, task, run, attempt, prepared) -> dict:
        """Write the spec and make the launch durable (§3): from here on, the
        attempt outlives this engine. Returns the placement's stage."""

        claim = self.m.claimed(attempt)
        if claim is None:
            raise LostOwnership(attempt)
        live = self.live.setdefault(attempt, Live())
        live.fresh = True
        spec = {
            "attempt": attempt,
            "deploy": self.manifest["deploy"],
            "project": self.manifest["name"],
            "asset": task["asset"],
            "partition": task["partition"],
            "run": {"id": task["run"], "config": run.get("config") or {}},
            "outputs": {name: worker_output(info) for name, info in prepared["outputs"].items()},
            "inputs": prepared["inputs"],
            "placement": self.manifest["assets"][task["asset"]]["placement"],
            "heartbeat": self.heartbeat_seconds,
            "engine": self.engine_url,
            "token": lifecycle.token(await self._load_secret(), attempt),
            "generation": claim["generation"],  # chosen before the spec (§9.7)
        }
        if prepared["cursor"] is not None:
            spec["cursor"] = prepared["cursor"]
        base = lifecycle.base(task["run"], attempt)
        await self.state.create_object(f"{base}{lifecycle.SPEC}", json.dumps(spec).encode())
        # Its control file, before the launch: a worker never creates it, so one that
        # finds none stops (§2.4). An engine replaced before `AttemptLaunched` is
        # durable leaves the file `open`, and no worker ever learns of it (F26).
        opened = lifecycle.control(lifecycle.OPEN, engine=self.state.journal.engine)
        await self.state.create_object(f"{base}{lifecycle.CONTROL}", opened)
        if self.keys is not None:  # what `start` answers its reads from (resolved-commits.md §7)
            live.reads = {"inputs": spec["inputs"], "outputs": spec["outputs"]}
        claim = self.m.claimed(attempt)
        if claim is None:
            raise LostOwnership(attempt)  # canceled while the spec was written
        execution = spec["placement"]
        event = {
            "type": "AttemptLaunched",
            "run": task["run"],
            "task": task["id"],
            "attempt": attempt,
            "started_at": claim["started_at"],
            "generation": claim["generation"],
            "at": self.clock(),
            "execution": execution,
            "prepared": self._durable(prepared),
        }
        if execution["kind"] == "Pool":
            needs = {k: v for k in ("cpu", "memory", "gpu") if (v := execution["options"].get(k)) is not None}
            event["pool"] = {"name": execution["executor"], "needs": needs}
        # Launch only what a restarted engine would adopt, never an orphan: until the
        # launch is durable, pool discovery does not offer it either (F26). If it never
        # is (this engine was replaced), it never is offered.
        live.launching = True
        self.state.record(event)
        await self.state.durable()
        live.launching = False
        if execution["kind"] == "Pool":
            self._pool_wake()
        return {"attempt": attempt, "run": task["run"], "objects": self.state.objects_url}

    def _placed(self, attempt: str, handle: dict | None) -> None:
        """Record where a launched attempt runs, so a restarted engine follows
        it there. Lazily: it rides the next journal segment (§13); a handle
        lost to a crash is found again by `resume`, or through the control file."""

        if handle is not None and self.m.claimed(attempt) is not None:
            self.state.record({"type": "AttemptPlaced", "attempt": attempt, "handle": handle}, lazy=True)

    def _left(self, launched: dict, seconds: float) -> float:
        """What an adopted attempt has left of its provisioning allowance,
        `seconds` from its launch. The launch time is the launching engine's
        clock, which may disagree with this one: it is trusted, but it never
        leaves less than three heartbeats, nor more than all of `seconds`."""

        return min(seconds, max(seconds - (self.clock() - launched["at"]), 3 * self.heartbeat_seconds))

    # -- watching (§6–§8) ----------------------------------------------------------------

    async def _read_worker(self, run_id: str, attempt: str, live: Live, now: float) -> None:
        """The worker's ownership, from the control file until it is known,
        then its reports while its channel fails, from `.beat`: a change is
        evidence that it lives, and the owner rebuilds the binding after a
        restart (§2.4, §6)."""

        heard = False
        if live.worker_id is None:
            owner = await self._owner(run_id, attempt)
            if owner is None:
                return
            live.worker_id, heard = owner, True
        data = await self.state.get_object(f"{lifecycle.base(run_id, attempt)}{lifecycle.BEAT}")
        if data is not None and data != live.beat:
            live.beat = data
            body = json.loads(data)
            if body["worker_id"] == live.worker_id:
                live.report = {k: body[k] for k in ("events", "usage") if k in body}
                heard = True
        if not heard:
            return
        if live.reported is None or live.via == "worker" or not self._speaking(live, now):
            live.heard(now, "worker")

    async def _watch(self, task_id: str, attempt: str, placement, handle, adopted=False):
        """Wait for a launched attempt to end, then settle it (§7).

        Evidence comes from the worker's reports — over the channel, else
        through the object store, read only while the channel is quiet — and from
        the placement's handle, where there is one. A provider exit while
        the claim's owner still reports is a duplicate's (§4). Until its
        first report the attempt is provisioning, under a deadline of its
        own; its `timeout` runs from then. A cancel, a timeout or the
        provisioning deadline follows the two phases of §7: requested,
        with `cancel_grace` to drain, then forced. Deadlines run on this
        process's monotonic clock (§8)."""

        task = self.m.task(task_id)
        run_id, launched = task["run"], self._launched(task, attempt)
        live = self.live.setdefault(attempt, Live())
        loop = asyncio.get_running_loop()
        heartbeat = self.heartbeat_seconds
        info = self.manifest["assets"].get(task["asset"]) or {}
        limit = info.get("timeout") or 3600
        grace = info.get("cancel_grace") or self.cancel_grace
        provision = getattr(placement, "provision_seconds", self.provision_seconds)
        is_pool = launched.get("pool") is not None
        began = loop.time()
        provisioned_by = math.inf
        if provision is not None:
            provisioned_by = began + (self._left(launched, provision) if adopted else provision)
        forced_by = math.inf  # the end of a requested cancel's grace
        read_at = -math.inf
        if adopted:  # it may have ended, or been owned, while no engine looked: look now
            found = await lifecycle.read_control(self.state.objects, run_id, attempt)
            if found is not None and found[0]["state"] == lifecycle.SEALED:
                return await self._settle(
                    task_id, attempt, found[0]["result"], {"code": None, "reason": None, "meta": {}}
                )
            if found is not None and found[0]["state"] == lifecycle.ENDED:  # by an engine since gone
                return await self._fail(task_id, attempt, "ended by a replaced engine", retryable=True)
            read_at = loop.time()
            await self._read_worker(run_id, attempt, live, read_at)
        poll = heartbeat / 3
        stirred = self._stirred.setdefault(attempt, asyncio.Event())

        async def look(timeout):
            if handle is None:
                await asyncio.sleep(timeout)
                return None
            return await placement.wait(handle, timeout)

        while True:
            polled = loop.time()
            deadline = live.started_at + limit if live.started else provisioned_by
            timeout = max(0.0, min(poll, deadline - polled, forced_by - polled))
            observed = handle is not None
            try:
                exit_ = await _unless(stirred, look(timeout))
            except Exception as error:
                # It cannot tell for now: keep the handle, and hear from the worker.
                log.warning("attempt %s: placement cannot tell, following reports: %s", attempt, error)
                observed, exit_ = False, None
                await _unless(stirred, asyncio.sleep(max(0.0, timeout - (loop.time() - polled))))
            # A placement that returns before its timeout must not spin the loop.
            await asyncio.sleep(max(0.0, 0.05 - (loop.time() - polled)))
            now = loop.time()
            quiet = not self._speaking(live, now) or live.via == "worker"
            offered = not is_pool or (
                live.offered_at is not None and now - live.offered_at >= self.pool_offered_grace
            )
            if quiet and (offered or live.started) and now - read_at >= heartbeat:
                read_at = now
                await self._read_worker(run_id, attempt, live, now)
            # Only reports the engine received itself prove the owner outlived
            # the exit: a `.beat` seen now may have been written before it.
            if (
                exit_ is not None
                and not live.finished
                and live.via == "channel"
                and self._speaking(live, now)
            ):
                log.info("attempt %s: an exit while its owner reports: a duplicate's", attempt)
                handle, exit_ = None, None
            if (
                live.finished
                or exit_ is not None
                or (live.started and not observed and not self._speaking(live, now))
            ):
                result = await self.state.attempt_result(run_id, attempt)
                if result is not None or not live.finished:
                    reason = exit_ or {"code": None, "reason": "no heartbeat", "meta": {}}
                    return await self._settle(task_id, attempt, result, reason)
                live.finished = False  # a hint ahead of its result: look again
            task = self.m.task(task_id)
            if task is None or task["status"] == "canceled":
                reason = "user"
            elif live.started and now > live.started_at + limit:
                reason = "timeout"
            elif not live.started and now > provisioned_by:
                reason = "provisioning"
            else:
                continue
            phase = "requested" if live.started and now < forced_by else "forced"
            record = lifecycle.latch(live.cancel, Cancel(phase, reason, self.m.event_counter))
            if record != live.cancel:
                live.cancel = record
                if record.phase == "requested" and forced_by == math.inf:
                    forced_by = now + grace
                    continue
            if live.cancel.phase == "requested" and now < forced_by:
                continue
            live.cancel = lifecycle.latch(live.cancel, Cancel("forced", reason, self.m.event_counter))
            result = await self.state.attempt_result(run_id, attempt)
            if result is not None:  # published before the force: it stands
                return await self._settle(
                    task_id, attempt, result, {"code": None, "reason": "done", "meta": {}}
                )
            if handle is not None:
                await self._cancel(placement, handle)
            reason = live.cancel.reason
            if reason == "user":
                await self._fail(
                    task_id, attempt, "canceled", outcome="canceled", end="aborted", reason="canceled"
                )
            else:
                error = (
                    f"the worker did not report within {provision:g}s of its launch"
                    if reason == "provisioning"
                    else "timeout"
                )
                await self._fail(task_id, attempt, error, retryable=True, end="aborted", reason=reason)
            return

    # -- ending (§2.3, §2.4, §7) ---------------------------------------------------------

    async def _settle(self, task_id: str, attempt: str, result: dict | None, exit_: dict):
        """End an attempt from its result: commit it, or fail it."""

        task = self.m.task(task_id)
        prepared = self._launched(task, attempt)["prepared"]
        if result is None:
            await self._fail(
                task_id,
                attempt,
                f"the worker exited without a result: {exit_}",
                retryable=True,
                end="lost",
                reason=exit_.get("reason") or f"exit code {exit_.get('code')}",
            )
            return
        status = result.get("status")
        if status == "canceled" and "failures" in result:
            # A drained Each batch (docs/lifecycle.md §7): what finished commits, as one
            # decision with its interrupted keys and its position.
            reason = (result.get("cancel") or {}).get("reason") or "user"
            user = reason == "user"
            try:
                self.commit_attempt(
                    attempt,
                    prepared,
                    result,
                    outcome="canceled" if user else "failed",
                    error="canceled" if user else reason,
                    retryable=not user,
                    delay=0.0 if user else self._retry_delay(task),
                )
            except LostOwnership:
                return
            except self.Conflict as error:
                await self._fail(
                    task_id, attempt, str(error), retryable=True, result=result, reason="conflict"
                )
            return
        if status == "canceled":
            reason = (result.get("cancel") or {}).get("reason") or "user"
            user = reason == "user"
            await self._fail(
                task_id,
                attempt,
                "canceled" if user else reason,
                outcome="canceled" if user else "failed",
                retryable=not user,
                result=result,
                end="canceled",
                reason=reason,
            )
            return
        if status == "failed":
            error = result.get("error") or {}
            transient = error.get("class") == errors.TRANSIENT
            told = f"{error.get('type', 'Error')}: {error.get('message', '')}"
            if note := method_note(self.manifest.get("build"), error.get("build")):
                log.warning("attempt %s: %s", attempt, note)
                told = f"{told} ({note})"
            await self._fail(
                task_id,
                attempt,
                told,
                retryable=bool(error.get("retryable")),
                delay=self._transient_delay(task, error) if transient else self._retry_delay(task),
                result=result,
                retry_for=error.get("retry_for") if transient else None,
            )
            return
        try:
            self.commit_attempt(attempt, prepared, result)
        except LostOwnership:
            return
        except self.Conflict as error:
            await self._fail(
                task_id,
                attempt,
                str(error),
                retryable=getattr(error, "retryable", True),
                result=result,
                reason="conflict",
            )
            return

    @staticmethod
    def _launched(task: dict | None, attempt: str) -> dict:
        launched = (task or {}).get("launched")
        if launched is None or launched["attempt"] != attempt:
            raise LostOwnership(attempt)  # it finished meanwhile
        return launched

    def partition_cleanups(self, output: str, partition: str) -> dict:
        """An output partition's cleanup awaiting its next attempt (§9.8):
        how much is pending, and the entries stuck — their names could not
        be read three times — for an operator to see and clear."""

        entries = self.m.cleanups.get((output, partition)) or []
        stuck = [
            {k: e[k] for k in ("id", "kind", "misses", "attempt", "files") if k in e}
            for e in entries
            if e.get("stuck")
        ]
        return {
            "output": output,
            "partition": partition,
            "pending": len(entries) - len(stuck),
            "stuck": stuck,
        }

    def clear_cleanups(self, output: str, partition: str, by: str) -> dict:
        """An operator's `solera cleanups --clear`: forget the stuck
        entries. Their objects stay where they are."""

        stuck = [e["id"] for e in self.m.cleanups.get((output, partition)) or [] if e.get("stuck")]
        if stuck:
            event = {"output": output, "partition": partition, "ids": stuck, "by": by, "at": self.clock()}
            self.state.record({"type": "CleanupsCleared", **event})
        return {"output": output, "partition": partition, "cleared": stuck}

    async def _end(self, run_id: str, attempt: str) -> dict | None:
        """End the attempt in its control file (§2.4), on what it reads
        there: the final body, `sealed` or `ended`, found or written. Ended
        from `open` or `owned`, its evidence is `none` — the worker never
        took the gate, and now never can; from `writing`, `writing`, with
        its intents (§2.3). A refused swap reads again and decides again.
        `None` if there is no file. An object store that does not answer
        establishes nothing: retried, then raised."""

        path = f"{lifecycle.base(run_id, attempt)}{lifecycle.CONTROL}"
        for retry in range(6):
            try:
                found = await lifecycle.read_control(self.state.objects, run_id, attempt)
                while found is not None and found[0]["state"] not in lifecycle.FINAL:
                    body, version = found
                    end = {"engine": self.state.journal.engine, "write": lifecycle.NONE}
                    if body["state"] == lifecycle.WRITING:
                        end.update(write=lifecycle.WRITING, intents=body.get("intents") or {})
                    self._authority()
                    try:
                        await swap(
                            self.state.objects, path, lifecycle.control(lifecycle.ENDED, **end), version
                        )
                        return {"state": lifecycle.ENDED, **end}
                    except Conflict:  # the worker moved on, or another engine ended it
                        found = await lifecycle.read_control(self.state.objects, run_id, attempt)
                return found[0] if found is not None else None
            except LostOwnership:
                raise
            except Exception:
                if retry == 5:
                    raise
                await asyncio.sleep(0.2 * 2**retry)

    async def _fail(
        self,
        task_id: str,
        attempt: str,
        error: str,
        *,
        retryable=False,
        delay=0.0,
        outcome="failed",
        result=None,
        end=None,
        reason=None,
        retry_for=None,
    ):
        """End a launched attempt without a commit. With no result, its
        control file is ended first, so it can never write after this, and
        what was there says what it may have written (§2.3); a result sealed
        meanwhile stands, and is settled instead. If it had begun writing, the
        keyed outputs it meant to change stay owing a repair until a later
        commit takes in what landed; their intent files are kept for that.
        `result` is the worker's, when it sealed one."""

        task = self.m.task(task_id)
        prepared = self._launched(task, attempt)["prepared"]
        if result is None:
            final = await self._end(task["run"], attempt) or {}
            if final.get("state") == lifecycle.SEALED:
                exit_ = {"code": None, "reason": "done", "meta": {}}
                return await self._settle(task_id, attempt, final["result"], exit_)
        else:
            final = result
        write = final.get("write", lifecycle.NONE)
        repairs = current_names(prepared, final.get("intents") or {})
        worker = result if result is not None else self._last_report(attempt)
        claim = self.m.claimed(attempt)
        if claim is None:
            return
        self._finish(
            task,
            claim,
            outcome,
            error=error,
            retryable=retryable,
            delay=delay,
            repairs=repairs,
            worker=worker,
            end=end,
            reason=reason,
            write=write,
            retry_for=retry_for,
        )
        await self._cleanup(attempt, prepared, keep=set(repairs))

    def _last_report(self, attempt: str) -> dict:
        live = self.live.get(attempt)
        return dict(live.report) if live is not None else {}

    async def _cleanup(self, attempt: str, prepared: dict, keep=()):
        """Delete the delta files an attempt wrote but never committed, except
        the intents of outputs it left owing a repair. They are named after the
        attempt, so nothing else can hold them (§6)."""

        for name, info in (prepared.get("outputs") or {}).items():
            if info.get("prefix") is None or name in keep or info["contract"]["writes"] == "immutable":
                continue  # an immutable output's are collected with what they name (§9.8)
            prefix = f"{info['prefix']}{int(info['commit_number']):012d}-{attempt}"
            self._authority()
            with contextlib.suppress(Exception):
                await self.state.delete_objects(await self.state.list_objects(prefix))
        failures = prepared.get("failures")
        if failures is not None:  # an Each batch's failure delta (docs/per-key-processing.md §9)
            prefix = f"{failures['prefix']}{int(failures['commit_number']):012d}-{attempt}"
            self._authority()
            with contextlib.suppress(Exception):
                await self.state.delete_objects(await self.state.list_objects(prefix))

    def _transient_delay(self, task, error: dict) -> float:
        """A `Transient` failure's wait (docs/per-key-processing.md §8):
        its `retry_after`, else one minute doubling to six hours."""

        if error.get("retry_after") is not None:
            return float(error["retry_after"])
        failures = (task.get("outcomes") or {}).get("failed", 0)
        return errors.backoff(failures + 1)

    def _retry_delay(self, task) -> float:
        retry = task.get("retry") or {}
        delay = float(retry.get("delay", 1.0))
        if retry.get("backoff") == "exponential":
            failures = (task.get("outcomes") or {}).get("failed", 0)
            delay *= 2**failures
        return delay

    async def _cancel(self, placement, handle):
        self._authority()
        with contextlib.suppress(Exception):
            await placement.cancel(handle)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(placement.wait(handle, self.GRACE_SECONDS), self.GRACE_SECONDS + 1)
