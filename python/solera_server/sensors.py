"""Sensors (docs/lifecycle.md §11): checks a sensor host runs every
interval, which may or may not lead to a change. A tick is not an attempt:
no attempt objects, no run of its own, no journal event unless it changes
something. The engine dispatches a due tick with the sensor's cursor and a
snapshot of the sources it may commit to, keeps one claim per sensor in
memory, and applies the posted outcome all or nothing."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import os
import signal
import sys

from solera import lifecycle
from solera.ids import ulid

from .placements.local import _env

log = logging.getLogger(__name__)

POST_GRACE = 5.0  # seconds past its timeout a tick's outcome is still taken
SENSOR_MAP_MAX = 1_000_000  # keys of a full map the engine resolves itself (§11.4)
HOST_TOKEN = "sensors"  # what the local host's token signs (§5.2)


class Sensors:
    """The sensor half of the engine: a mixin on `Engine`."""

    def _sensors_init(self, sensor_host) -> None:
        # `sensor_host(engine)`: runs the local host in process (tests); else
        # a served engine keeps a `solera_worker sensors` subprocess.
        self.sensor_host = sensor_host
        self.sensor_due: dict[str, float] = {}  # sensor -> loop time it is next due; memory only
        self.sensor_hosts: dict[str, dict] = {}  # hosts that asked for ticks lately
        self._sensors_changed = asyncio.Event()
        self._host_task: asyncio.Task | None = None

    def _sensors(self, executor: str | None = None) -> dict[str, dict]:
        declared = self.manifest.get("sensors") or {}
        return {
            n: s for n, s in declared.items() if executor is None or s["placement"]["executor"] == executor
        }

    def _head_id(self, source: str) -> str:
        """A source head's identity (§11.3): the event position that
        installed it, whatever its version says."""

        return f"h:{(self.m.heads.get((source, '')) or {}).get('n', 0)}"

    def _sensors_wake(self) -> None:
        changed, self._sensors_changed = self._sensors_changed, asyncio.Event()
        changed.set()

    def sensor_views(self) -> list[dict]:
        now = asyncio.get_running_loop().time()
        views = []
        for name, sensor in self._sensors().items():
            record, claim = self.m.sensors.get(name) or {}, self.m.ticks.get(name)
            due = self.sensor_due.get(name)
            views.append(
                {
                    **sensor,
                    "cursor": record.get("cursor"),
                    "accepted": record.get("accepted"),
                    "ticking": {k: claim[k] for k in ("tick", "host", "started_at")} if claim else None,
                    "due_in": max(0.0, due - now) if due is not None else 0.0,
                }
            )
        return views

    # -- dispatch (§11.2) -----------------------------------------------------------------

    def _sensor_sweep(self) -> None:
        """Drop the ticks over their timeout (a late post gets 409), and wake
        the hosts waiting when a sensor comes due."""

        now = asyncio.get_running_loop().time()
        for name, claim in list(self.m.ticks.items()):
            if now > claim["deadline"]:
                del self.m.ticks[name]
                self._tick_row(name, claim, "failed", error="timed out")
        if any(self._due(name, now) for name in self._sensors()):
            self._sensors_wake()

    def _due(self, name: str, now: float) -> bool:
        return name not in self.m.ticks and self.sensor_due.get(name, -math.inf) <= now

    async def sensor_next(self, executor: str, revision: str, host: str, slots: int, wait: float) -> dict:
        """Due ticks for a host of `executor` on `revision`, up to `slots`;
        waits up to `wait` seconds for one. A host on another revision gets
        none: it is told the current one."""

        loop = asyncio.get_running_loop()
        self.sensor_hosts[host] = {
            "id": host,
            "executor": executor,
            "revision": revision,
            "seen_at": self.clock(),
        }
        current = self.manifest["revision"]
        deadline = loop.time() + max(0.0, min(wait, 30.0))
        due: list[str] = []
        while revision == current and slots > 0:
            changed, now = self._sensors_changed, loop.time()
            mine = self._sensors(executor)
            due = [n for n in mine if self._due(n, now)][:slots]
            if due or now >= deadline:
                break
            soonest = min(
                (self.sensor_due.get(n, now) for n in mine if n not in self.m.ticks), default=deadline
            )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(changed.wait(), max(0.0, min(deadline, soonest) - now))
        return {"revision": current, "ticks": [self._dispatch(name, host) for name in due]}

    def _dispatch(self, name: str, host: str) -> dict:
        """Claim a tick of `name`, with the snapshot of its sources (§11.3):
        their head identities, and a keyed source's pinned index, read under
        the tick's reader pin until it is decided."""

        sensor = self._sensors()[name]
        snapshot = {}
        for source in sensor["commits"]:
            entry = {"head": self._head_id(source)}
            index = self.m.indexes.get((source, ""))
            if index is not None:
                entry["index"] = index.pinned().to_json()
            snapshot[source] = entry
        now = asyncio.get_running_loop().time()
        claim = {
            "tick": ulid(self.clock()),
            "cursor": (self.m.sensors.get(name) or {}).get("cursor"),
            "snapshot": snapshot,
            "pin": self.m.applied,
            "host": host,
            "started_at": self.clock(),
            "deadline": now + sensor["timeout"] + POST_GRACE,
        }
        self.m.ticks[name] = claim
        self.sensor_due[name] = now + sensor["every"]
        return {
            "sensor": name,
            **{k: claim[k] for k in ("tick", "cursor", "snapshot")},
            "timeout": sensor["timeout"],
        }

    # -- applying an outcome (§11.4) -------------------------------------------------------

    async def sensor_post(self, name: str, tick: str, outcome: dict) -> dict:
        """Decide a tick's posted outcome: `{"error"}` for a body that
        raised, else the `Tick` (`cursor`, `commits`, `runs`). Answers what
        was accepted; raises `Conflict` for a tick that is not current or
        saw a source since moved, `ValueError` for one that asks what it
        may not. Either way nothing of it is applied."""

        if not isinstance(outcome, dict):
            raise ValueError("a tick's outcome is a JSON object")
        accepted = (self.m.sensors.get(name) or {}).get("accepted") or {}
        if accepted.get("tick") == tick:  # a retry of an outcome applied
            return {"accepted": True, **{k: accepted[k] for k in ("runs", "commits")}}
        claim = self.m.ticks.get(name)
        if claim is None or claim["tick"] != tick:
            raise self.Conflict(f"tick {tick} of {name} is not current: late, or from before a restart")
        del self.m.ticks[name]  # decided now, whatever follows
        if outcome.get("error") is not None:
            self._tick_row(name, claim, "failed", error=str(outcome["error"])[:2000])
            return {"accepted": False}
        try:
            return await self._apply_tick(name, claim, outcome)
        except self.Conflict as error:
            self._tick_row(name, claim, "refused", error=str(error))
            raise
        except (ValueError, KeyError, TypeError) as error:
            self._tick_row(name, claim, "failed", error=f"{type(error).__name__}: {error}")
            raise ValueError(str(error)) from error

    async def _apply_tick(self, name: str, claim: dict, outcome: dict) -> dict:
        sensor = self._sensors()[name]
        commits, runs = outcome.get("commits") or [], outcome.get("runs") or []
        sources = [c["source"] for c in commits]
        undeclared = sorted(set(sources) - set(sensor["commits"]))
        if undeclared:
            raise ValueError(f"{name} commits to {undeclared}, not in its commits=")
        if len(set(sources)) != len(sources):
            raise ValueError(f"{name}: one commit per source per tick")
        for source in sources:  # every snapshot together: one stale source refuses the tick
            if self._head_id(source) != claim["snapshot"][source]["head"]:
                raise self.Conflict(f"source {source!r} moved since the tick was dispatched")
        for c in commits:
            if len(c.get("keys") or ()) > SENSOR_MAP_MAX:
                raise ValueError(f"{name}: a full map over {SENSOR_MAP_MAX} keys; use a cursor")
        heads = {source: self.m.heads.get((source, "")) for source in sources}
        by, tags = f"sensor {name}", {"sensor": name, "tick": claim["tick"]}
        prepared, planned = [], []
        try:
            for c in commits:
                event, _ = await self._prepare_commit(
                    c["source"], c.get("version"), c.get("keys"), c.get("upsert"), c.get("remove"), by, tags
                )
                if event is not None:
                    prepared.append(event)
            for n, request in enumerate(runs):
                run = await self._plan_run(
                    request["targets"],
                    request.get("partitions") or "latest",
                    config=request.get("config"),
                    keys=request.get("keys"),
                    sensor=name,
                    by=by,
                    tags={**(request.get("tags") or {}), **tags},
                )
                planned.append((f"{claim['tick']}/{n}", run))
            if any(self.m.heads.get((source, "")) != head for source, head in heads.items()):
                raise self.Conflict("a source moved while the tick was applied")
        except BaseException:
            await self._drop_prepared(prepared)
            raise
        moved = "cursor" in outcome and outcome["cursor"] != claim["cursor"]
        if not prepared and not planned and not moved:
            self._tick_row(name, claim, "skipped")
            return {"accepted": True, "runs": [], "commits": {}}
        now, base = self.clock(), self.m.applied
        for event in prepared:
            event["at"] = now
        accepted = {
            "tick": claim["tick"],
            "runs": [run["id"] for _, run in planned],
            # each commit's head identity: the position its event is applied at
            "commits": {e["source"]: f"h:{base + i + 1}" for i, e in enumerate(prepared)},
        }
        self.state.record(
            *prepared,
            *({"type": "RunSubmitted", "run": run, "command": command} for command, run in planned),
            {
                "type": "SensorAdvanced",
                "sensor": name,
                "cursor": outcome["cursor"] if moved else claim["cursor"],
                "accepted": accepted,
            },
        )
        self._committed_keys(prepared)
        status = "requested" if planned else "committed" if prepared else "advanced"
        self._tick_row(name, claim, status, runs=accepted["runs"])
        return {"accepted": True, "runs": accepted["runs"], "commits": accepted["commits"]}

    def _tick_row(self, name: str, claim: dict, outcome: str, *, error: str | None = None, runs=()) -> None:
        """A `ticks` history row (§11.5): buffered in memory, never journaled."""

        self.history.tick(
            {
                "sensor": name,
                "tick": claim["tick"],
                "started_at": claim["started_at"],
                "ended_at": self.clock(),
                "host": claim["host"],
                "outcome": outcome,
                "error": error,
                "runs": list(runs),
            }
        )

    # -- the local host (§11.2) -------------------------------------------------------------

    def _start_sensor_host(self) -> None:
        if not self._sensors("local") or self._host_task is not None:
            return
        if self.sensor_host is None and not (self.engine_url and self.project):
            log.warning("sensors on the local host need an engine URL to report to; none tick")
            return
        self._host_task = asyncio.create_task(self._keep_host())

    async def _stop_sensor_host(self) -> None:
        if self._host_task is not None:
            self._host_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._host_task
            self._host_task = None

    async def _keep_host(self) -> None:
        """Keep the local host running: restarted with backoff if it exits.
        (A host process replaces itself after `max_ticks`, or a tick that
        overran; one in process returns.)"""

        loop, delay = asyncio.get_running_loop(), 1.0
        while True:
            started = loop.time()
            try:
                if self.sensor_host is not None:
                    await self.sensor_host(self)
                else:
                    await self._host_process()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("the sensor host failed")
            delay = 1.0 if loop.time() - started > 60 else min(delay * 2, 60.0)
            await asyncio.sleep(delay)

    async def _host_process(self) -> None:
        env = _env()
        env["SOLERA_PROJECT"] = self.project
        env["SOLERA_SENSOR_TOKEN"] = lifecycle.token(await self._load_secret(), HOST_TOKEN)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "solera_worker",
            "sensors",
            "--pool",
            "local",
            "--server",
            self.engine_url,
            env=env,
            start_new_session=True,
        )
        try:
            code = await process.wait()
            log.warning("the sensor host exited with %s", code)
        finally:
            if process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGTERM)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(process.wait(), 5)
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
