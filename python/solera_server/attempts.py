"""The attempt lifecycle on the engine (docs/lifecycle.md §3–§8, §10): the
launch, what workers report over the channel, watching, the two-phase
cancel, and how an attempt ends — from its result, or from its gate.

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

from obstore.exceptions import AlreadyExistsError
from solera import errors, lifecycle
from solera.lifecycle import Cancel, Ended

from .state import LostOwnership

log = logging.getLogger(__name__)

LIVE_LINES = 10_000  # live log lines kept per attempt for the console
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


@dataclass
class Live:
    """What the engine knows of a launched attempt's worker, in memory only:
    rebuilt from `.worker` after a restart (§5.3)."""

    invocation: str | None = None  # the bound invocation: the claim's owner
    started: bool = False  # it reported: the runtime clock runs
    started_at: float = 0.0  # monotonic
    reported: float | None = None  # when it last reported (monotonic)
    via: str = "channel"  # how: over the channel, or through `.worker`
    seq: int = 0
    report: dict = field(default_factory=dict)  # its last timeline and usage
    worker: bytes | None = None  # the `.worker` bytes last read
    cancel: Cancel | None = None  # the latched cancel record (§2.2)
    finished: bool = False  # the worker said its result is written
    offered_at: float | None = None  # a pool attempt: when discovery first offered it
    lines: deque = field(default_factory=lambda: deque(maxlen=LIVE_LINES))
    log_offset: int = 0

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

    def _stir(self, attempt: str) -> None:
        self._stirred.setdefault(attempt, asyncio.Event()).set()

    def _speaking(self, live: Live, now: float) -> bool:
        """Has the worker reported recently: three beats over the channel,
        three `.worker` updates (two beats apart) through the object store."""

        if live.reported is None:
            return False
        window = 3 if live.via == "channel" else 6
        return now - live.reported <= window * self.heartbeat_seconds

    async def _owner(self, attempt: str) -> str | None:
        task = self.m.task(self.m.attempts.get(attempt, ""))
        data = await self.state.get_object(f"{lifecycle.base(task['run'], attempt)}{lifecycle.WORKER}")
        return json.loads(data)["invocation"] if data else None

    async def _bind(self, attempt: str, live: Live, invocation: str, start: bool = False) -> None:
        """The first `start` binds its invocation: only the claim's winner
        sends one. Any other token, or any request after a restart, is
        checked against the claim itself (§5.3)."""

        if live.invocation == invocation:
            return
        if start and live.invocation is None:
            live.invocation = invocation
            return
        owner = await self._owner(attempt)
        if owner != invocation:
            raise Ended("not_owner")
        live.invocation = owner

    def _cancel_answer(self, live: Live) -> dict:
        return {"cancel": live.cancel.to_json() if live.cancel else None}

    # -- the channel's handlers (§5) ---------------------------------------------------

    async def attempt_start(self, attempt: str, body: dict) -> dict:
        live = self._live(attempt)
        await self._bind(attempt, live, body["invocation"], start=True)
        live.heard(asyncio.get_running_loop().time(), "channel")
        self._stir(attempt)
        return self._cancel_answer(live)

    async def attempt_beat(self, attempt: str, body: dict) -> dict:
        live = self._live(attempt)
        await self._bind(attempt, live, body["invocation"])
        if int(body.get("seq", 0)) > live.seq:  # a retried or reordered beat changes nothing
            live.seq = int(body["seq"])
            live.report = {k: body[k] for k in ("events", "usage") if k in body}
        live.heard(asyncio.get_running_loop().time(), "channel")
        return self._cancel_answer(live)

    async def attempt_logs(self, attempt: str, body: dict) -> dict:
        live = self._live(attempt)
        await self._bind(attempt, live, body["invocation"])
        offset, lines = int(body["offset"]), list(body["lines"])
        if offset > live.log_offset:  # lines lost for good: the chunks hold them
            live.log_offset = offset
        new = lines[live.log_offset - offset :]
        live.lines.extend(new)
        live.log_offset += len(new)
        return {"offset": live.log_offset}

    async def attempt_finished(self, attempt: str, body: dict) -> None:
        live = self._live(attempt)
        await self._bind(attempt, live, body["invocation"])
        live.finished = True
        self._stir(attempt)

    def attempt_lines(self, attempt: str) -> list[str] | None:
        live = self.live.get(attempt)
        return list(live.lines) if live is not None else None

    # -- pool discovery (§10) ------------------------------------------------------------

    async def pool_work(self, pool: str, capacity: dict, host: str, wait: float) -> list[dict]:
        """Launched pool attempts that fit `capacity` and have not started,
        oldest first; waits up to `wait` seconds for one. A hint: workers
        that get the same attempt race for its claim."""

        loop = asyncio.get_running_loop()
        self.pollers[host] = {"id": host, "pools": [pool], "capacity": capacity, "seen_at": self.clock()}
        deadline = loop.time() + max(0.0, min(wait, 30.0))
        while True:
            changed = self._pool_changed
            found = []
            for record in sorted(self.m.pool.values(), key=lambda r: r["created_at"]):
                live = self.live.get(record["attempt"])
                if record["pool"] != pool or (live is not None and live.started):
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
        spec = {
            "attempt": attempt,
            "revision": self.manifest["revision"],
            "project": self.manifest["name"],
            "asset": task["asset"],
            "partition": task["scope"],
            "run": {"id": task["run"], "config": run.get("config") or {}},
            "prior": prepared["prior"],
            "outputs": prepared["outputs"],
            "inputs": prepared["inputs"],
            "execution": self.manifest["assets"][task["asset"]]["placement"],
            "heartbeat": self.heartbeat_seconds,
            "engine": self.engine_url,
            "token": lifecycle.token(await self._load_secret(), attempt),
            "generation": claim["pin"],  # chosen before the spec (§9.7)
        }
        if prepared["cursor"] is not None:
            spec["cursor"] = prepared["cursor"]
        path = f"{lifecycle.base(task['run'], attempt)}{lifecycle.SPEC}"
        await self.state.create_object(path, json.dumps(spec).encode())
        claim = self.m.claimed(attempt)
        if claim is None:
            raise LostOwnership(attempt)  # canceled while the spec was written
        execution = spec["execution"]
        event = {
            "type": "AttemptLaunched",
            "run": task["run"],
            "task": task["id"],
            "attempt": attempt,
            "started_at": claim["started_at"],
            "pin": claim["pin"],
            "at": self.clock(),
            "execution": execution,
            "prepared": self._durable(prepared),
        }
        if execution["kind"] == "Pool":
            needs = {
                k: v for k in ("cpu", "memory", "gpu") if (v := execution["placement"].get(k)) is not None
            }
            event["pool"] = {"name": execution["executor"], "needs": needs}
        self.state.record(event)
        # Launch only what a restarted engine would adopt, never an orphan.
        await self.state.durable()
        if execution["kind"] == "Pool":
            self._pool_wake()
        return {"attempt": attempt, "run": task["run"], "objects": self.state.objects_url}

    def _placed(self, attempt: str, handle: dict | None) -> None:
        """Record where a launched attempt runs, so a restarted engine follows
        it there. Lazily: it rides the next journal segment (§13); a handle
        lost to a crash is found again by `resume`, or through `.worker`."""

        if handle is not None and self.m.claimed(attempt) is not None:
            self.state.record({"type": "AttemptPlaced", "attempt": attempt, "handle": handle}, lazy=True)

    def _left(self, launched: dict, seconds: float) -> float:
        """What an adopted attempt has left of its provisioning allowance,
        `seconds` from its launch. The launch time is the launching engine's
        clock, which may disagree with this one: it is trusted, but it never
        leaves less than three heartbeats, nor more than all of `seconds`."""

        return min(seconds, max(seconds - (self.clock() - launched["at"]), 3 * self.heartbeat_seconds))

    # -- watching (§6–§8) ----------------------------------------------------------------

    async def _read_worker(self, base: str, live: Live, now: float) -> None:
        """The worker's claim, or its reports while its channel fails: a
        change is evidence that it lives, and the claim rebuilds the
        binding after a restart."""

        data = await self.state.get_object(f"{base}{lifecycle.WORKER}")
        if data is None or data == live.worker:
            return
        live.worker = data
        body = json.loads(data)
        if live.invocation is None:
            live.invocation = body["invocation"]
        if body["invocation"] != live.invocation:
            return
        if body.get("events") is not None:
            live.report = {k: body[k] for k in ("events", "usage") if k in body}
        if live.reported is None or live.via == "worker" or not self._speaking(live, now):
            live.heard(now, "worker")

    async def _watch(self, task_id: str, attempt: str, placement, handle, adopted=False):
        """Wait for a launched attempt to end, then settle it (§7).

        Evidence comes from the worker's reports — over the channel, else
        through `.worker`, read only while the channel is quiet — and from
        the placement's handle, where there is one. A provider exit while
        the claim's owner still reports is a duplicate's (§4). Until its
        first report the attempt is provisioning, under a deadline of its
        own; its `timeout` runs from then. A cancel, a timeout or the
        provisioning deadline follows the two phases of §7: requested,
        with `cancel_grace` to drain, then forced. Deadlines run on this
        process's monotonic clock (§8)."""

        task = self.m.task(task_id)
        run_id, launched = task["run"], self._launched(task, attempt)
        base = lifecycle.base(run_id, attempt)
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
                await self._read_worker(base, live, now)
            # Only reports the engine received itself prove the owner outlived
            # the exit: a `.worker` seen now may have been written before it.
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
            record = lifecycle.latch(live.cancel, Cancel(phase, reason, self.m.applied))
            if record != live.cancel:
                live.cancel = record
                if record.phase == "requested" and forced_by == math.inf:
                    forced_by = now + grace
                    continue
            if live.cancel.phase == "requested" and now < forced_by:
                continue
            live.cancel = lifecycle.latch(live.cancel, Cancel("forced", reason, self.m.applied))
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
            await self._fail(
                task_id,
                attempt,
                f"{error.get('type', 'Error')}: {error.get('message', '')}",
                retryable=bool(error.get("retryable")),
                delay=self._transient_delay(task, error) if transient else self._retry_delay(task),
                result=result,
                retry_for=error.get("retry_for") if transient else None,
            )
            return
        try:
            await self.commit_attempt(attempt, prepared, result)
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
        if result.get("writes") == lifecycle.NONE and self._gated(prepared):
            with contextlib.suppress(Exception):
                await self._gate(task["run"], attempt, lifecycle.CLOSED)

    @staticmethod
    def _launched(task: dict | None, attempt: str) -> dict:
        launched = (task or {}).get("launched")
        if launched is None or launched["attempt"] != attempt:
            raise LostOwnership(attempt)  # it finished meanwhile
        return launched

    KINDS = ("immutable", "fenced", "overwrite")

    def _store_of(self, output: str) -> dict:
        return self.manifest["stores"].get(self.manifest["outputs"][output]["store"]) or {}

    def _kind(self, outputs) -> tuple[str, bool]:
        """The strictest write kind among the stores of `outputs`, and
        whether one of its overwrite stores is strict (§9.6)."""

        kind, strict = "immutable", False
        for name in outputs:
            store = self._store_of(name)
            writes = store.get("writes", "overwrite")
            if self.KINDS.index(writes) > self.KINDS.index(kind):
                kind = writes
            strict = strict or (writes == "overwrite" and bool(store.get("strict")))
        return kind, strict

    def _gated(self, prepared: dict) -> bool:
        """Whether the attempt writes outputs on stores that take a gate."""

        return any(
            self._store_of(n).get("writes", "overwrite") != "immutable" for n in prepared.get("outputs") or {}
        )

    def _hold(self, prepared: dict, writes: str) -> str | None:
        """How an attempt that ended with `writes` holds its scope: never for
        `immutable` and `fenced` stores, nor once writes are known; for an
        `overwrite` store, a grace, or — `strict` — until completion is
        established (§9.9)."""

        if writes != lifecycle.UNCERTAIN:
            return None
        kind, strict = self._kind(prepared.get("outputs") or {})
        if kind != "overwrite":
            return None
        return "strict" if strict else "grace"

    def _grace(self, asset: str) -> float:
        if asset in self.late_write_grace:
            return self.late_write_grace[asset]
        outputs = [o["name"] for o in self.manifest["assets"][asset]["outputs"]]
        return max([float(self._store_of(n).get("late_write_grace", 120.0)) for n in outputs] or [0.0])

    async def _release_holds(self) -> None:
        """Release scopes held for an uncertain writer: after the grace, on
        this engine's monotonic clock (a restart starts it again), or — for a
        strict store — once the writer's own result says its store calls
        returned (§9.9). An operator releases with `release_scope`."""

        now = asyncio.get_running_loop().time()
        for (asset, scope), hold in list(self.m.holds.items()):
            key = (asset, scope, hold["attempt"])
            since = self._held_since.setdefault(key, now)
            if hold["mode"] == "grace":
                if now - since >= self._grace(asset):
                    self._release(asset, scope, hold, "grace")
                continue
            if now - self._held_looked.get(key, -math.inf) < 3 * self.heartbeat_seconds:
                continue
            self._held_looked[key] = now
            result = await self.state.attempt_result(hold["run"], hold["attempt"])
            if result is not None and result.get("writes") in (lifecycle.NONE, lifecycle.COMPLETE):
                self._release(asset, scope, hold, "result")

    def _release(self, asset: str, scope: str, hold: dict, by: str) -> None:
        if self.m.holds.get((asset, scope)) is hold:
            event = {"type": "ScopeReleased", "asset": asset, "scope": scope, "attempt": hold["attempt"]}
            self.state.record({**event, "by": by, "at": self.clock()})

    def release_scope(self, asset: str, scope: str, by: str) -> dict:
        """An operator's release of a held scope (`solera scopes release`)."""

        hold = self.m.holds.get((asset, scope))
        if hold is None:
            raise KeyError(f"{asset}/{scope}")
        self._release(asset, scope, hold, f"operator:{by}")
        return {"asset": asset, "scope": scope, "released": hold["attempt"]}

    async def _gate(self, run_id: str, attempt: str, state: str) -> tuple[str, dict | None]:
        """Create the attempt's gate as `state` (`aborted` or `closed`), or
        find the one there: the write-completion evidence it establishes
        (§2.3) and the gate found. Winning: `none` — the worker never took
        it, and now never can. Finding `writing`: `uncertain`. An object
        store that does not answer establishes nothing: retried, then raised."""

        path = f"{lifecycle.base(run_id, attempt)}{lifecycle.GATE}"
        for retry in range(6):
            try:
                await self.state.create_object(path, lifecycle.gate(state))
                return lifecycle.NONE, None
            except AlreadyExistsError:
                found = json.loads(await self.state.get_object(path))
                if found["state"] == lifecycle.WRITING:
                    return lifecycle.UNCERTAIN, found
                return lifecycle.NONE, found
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
        """End a launched attempt without a commit. Its gate is taken as
        `aborted` first, so it can never write after this, and the gate
        found says what it may have written (§2.3). If it had begun writing,
        the keyed outputs it meant to change stay unsettled until a later
        commit takes in what landed; their intent files are kept for that.
        `result` is the worker's, when it published one."""

        task = self.m.task(task_id)
        prepared = self._launched(task, attempt)["prepared"]
        writes, gate = lifecycle.NONE, None
        if self._gated(prepared):
            writes, gate = await self._gate(task["run"], attempt, lifecycle.ABORTED)
        if result is not None:
            writes = result.get("writes", writes)
        unsettled = (
            (gate or {}).get("intents") or {} if (gate or {}).get("state") == lifecycle.WRITING else {}
        )
        hold = self._hold(prepared, writes)
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
            unsettled=unsettled,
            worker=worker,
            end=end,
            reason=reason,
            writes=writes,
            hold=hold,
            retry_for=retry_for,
        )
        await self._discard(attempt, prepared, keep=set(unsettled))

    def _last_report(self, attempt: str) -> dict:
        live = self.live.get(attempt)
        return dict(live.report) if live is not None else {}

    async def _discard(self, attempt: str, prepared: dict, keep=()):
        """Delete the delta files an attempt wrote but never committed, except
        the intents of outputs it left unsettled. They are named after the
        attempt, so nothing else can hold them (§6)."""

        for name, info in (prepared.get("outputs") or {}).items():
            if info.get("prefix") is None or name in keep or self.m.immutable(name):
                continue  # an immutable output's are collected with what they name (§9.8)
            prefix = f"{info['prefix']}{int(info['batch']):012d}-{attempt}"
            with contextlib.suppress(Exception):
                await self.state.delete_objects(await self.state.list_objects(prefix))

    def _transient_delay(self, task, error: dict) -> float:
        """A `Transient` failure's wait (docs/per-key-processing.md §8):
        its `retry_after`, else one minute doubling to six hours."""

        if error.get("retry_after") is not None:
            return float(error["retry_after"])
        failures = sum(1 for a in task["attempts"] if a["outcome"] == "failed")
        return errors.backoff(failures + 1)

    def _retry_delay(self, task) -> float:
        retry = task.get("retry") or {}
        delay = float(retry.get("delay", 1.0))
        if retry.get("backoff") == "exponential":
            failures = sum(1 for a in task["attempts"] if a["outcome"] == "failed")
            delay *= 2**failures
        return delay

    async def _cancel(self, placement, handle):
        with contextlib.suppress(Exception):
            await placement.cancel(handle)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(placement.wait(handle, self.GRACE_SECONDS), self.GRACE_SECONDS + 1)
