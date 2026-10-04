"""The simulation as a Hypothesis state machine: rules are what clients,
operators, the platform and bad luck do; invariants are checked after
every step; the teardown makes the world quiet and checks that it
converged. Shrinking turns a failing run into its shortest sequence.

`Simulation.trace` is the readable log of a run: every step, as the
regression test that replays it would spell it."""

from __future__ import annotations

import asyncio
import dataclasses
import functools
import json
import logging
import os
import shutil
import tempfile
import traceback
from pathlib import Path

from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, precondition, rule
from solera.keys.index import Options
from solera.sdk import Ref

from . import postgres
from .core import EPOCH, Killed
from .oracle import Journal, Violation, commit_rows, index_entries, keyed_content, value_content
from .project import (
    FLAKY,
    POOL,
    VARIANTS,
    External,
    Variant,
    build,
    expected_checks,
    expected_copy,
    expected_items,
    expected_split,
)
from .stores import Database
from .world import POINTS, Fate, World

log = logging.getLogger("sim")
STATS = {
    "examples": 0,
    "steps": 0,
    "virtual": 0.0,
    "seconds": 0.0,
}  # across runs of this process

KEYS = ["k0", "k1", "k2", "k3", "k10", "k11"]
SITES = ["east", "west", "north"]
TARGETS = ["items", "copy", "per_site", "log", "tally", "summary", "checks", "split", "seen"]
KEYED_INPUT = {
    "items": "feed",
    "copy": "items",
    "checks": "items",
    "split": "items",
    "seen": "items",
}  # what a run's `keys=` overrides
TERMINAL = {"succeeded", "failed", "canceled", "skipped"}
# Key cache budgets, (disk, candidates) bytes: the default, or room for a few
# of the simulation's index files, or for none of them.
CACHE = {None: None, "tight": (6 * 1024, 2 * 1024), "starved": (2 * 1024, 512)}
# Where `items` lives. Always the same three, so a seed draws the same runs with or
# without Postgres: without SOLERA_TEST_DATABASE_URL, "pg" runs on the table store.
STORES = ["file", "table", "pg"]
CHANGES = sorted(VARIANTS)  # re-registrations the rules make


fates = st.one_of(
    st.builds(
        Fate,
        kind=st.sampled_from(["die", "pause"]),
        point=st.sampled_from(POINTS),
        when=st.sampled_from(["before", "after"]),
        seconds=st.sampled_from([5.0, 45.0, 400.0]),
    ),
    st.builds(Fate, kind=st.just("mute")),
    st.builds(Fate, kind=st.just("twice"), seconds=st.sampled_from([0.0, 3.0, 30.0])),
)


class Simulation(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.tmp = Path(tempfile.mkdtemp(prefix="solera-sim-"))
        self.world: World | None = None
        self.trace: list[str] = []

    # -- setup ----------------------------------------------------------------------------

    @initialize(seed=st.integers(0, 2**16), store=st.sampled_from(STORES), cache=st.sampled_from(list(CACHE)))
    def boot(self, seed, store="file", cache=None):
        self.trace.append(f"boot(seed={seed}, store={store!r}" + (f", cache={cache!r})" if cache else ")"))
        if store == "pg" and not postgres.DSN:
            store = "table"
            self.trace.append("  # no Postgres: 'pg' runs on the table store")
        self.world = world = World(self.tmp, seed, key_options=Options(window=2))
        world.project_now = lambda: self.project
        world.cache_budget = CACHE[cache]
        self.journal = Journal(now=world.now)
        world.objects.tap = self.journal.landed
        world.on_record = self.journal.recorded
        self.requests: list[dict] = []
        # SOLERA_SIM_REQUESTS: a directory for each example's requests (spec/tla/check-trace.py)
        if os.environ.get("SOLERA_SIM_REQUESTS"):
            world.objects.trace = self._traced
            world.objects.tap = self._landed
        self.db, self.outside = Database(), External()
        self.db.fault = self._db_fault
        self.data_root = self.tmp / "data"
        self.variant = Variant(items_store=store, alt="table" if store == "file" else store)
        self.schema = None
        if "pg" in (store, self.variant.alt):
            self.schema = postgres.fresh_schema()
            world.pg = postgres.Ledger()
            self._pg_checked = 0
        self.project = self._build()
        self.feed = self.outside.feed  # what clients asked for, acknowledged or not
        self.sites: set[str] = set()
        self.knob = "0"
        self.runs: list[str] = []
        self.serial = 0
        self._ensure_engine()
        world.start_pool_hosts(POOL, 2)

    def _build(self):
        return build(self.variant, self.data_root, self.db, self.outside, self.schema)

    def _traced(self, who, kind, path, outcome, listed, data):
        root = str(self.tmp.resolve())
        line = {"at": self.world.now(), "who": list(who) if who else None, "kind": kind}
        line |= {"path": path.removeprefix(root), "outcome": outcome}
        if who and who[0] == "engine" and self.world.slots[who[1]].state is not None:
            line["counter"] = self.world.slots[who[1]].state.model.event_counter  # what it had applied
        if listed is not None:
            line["listed"] = [p.removeprefix(root) for p in listed]
        if data is not None and path.endswith("/control/journal.json"):
            line["names"] = json.loads(data)["checkpoint"]  # a move names a new one; an append keeps it
        elif data is not None and path.endswith(".control"):
            line["state"] = json.loads(data).get("state")  # what a control file write wrote
        elif data is not None and path.endswith(".spec"):
            spec = json.loads(data)  # the partition an attempt runs
            line["partition"] = [spec.get("asset"), spec.get("partition")]
        self.requests.append(line)

    def _landed(self, path, data):
        """The journal's durable events, each after the request that made it
        durable (Spans.tla's trace: what the index's lifecycle decided)."""

        known = len(self.journal.history)
        self.journal.landed(path, data)
        self.requests.extend({"event": event} for event in self.journal.history[known:])

    def _db_fault(self, kind, partition):
        if not self.world.plan.enabled:
            return None, 0.0
        fate, delay = self.world.plan.decide()
        return fate, delay

    def _run(self, coro, timeout: float = 3600.0):
        return self.world.run(coro, timeout=timeout)

    def _restarted(self) -> None:
        """The platform's own restart after a crash (`stall_launch`), if
        one is under way, finishes before anything else replaces the serving
        engine: what it replaces must be the engine that serves."""

        world = self.world
        if world.restarting is not None:
            self._run(asyncio.wait([world.restarting]))
            world.restarting = None

    def _ensure_engine(self, project=None) -> None:
        """The platform keeps one engine serving: start one if none is."""

        world = self.world
        self._restarted()
        if world.engine is not None and project is None:
            return
        project = project or self.project
        for attempt in range(8):
            try:
                self._run(world.start_engine(project))
                return
            except (Exception, Killed) as error:
                log.info("engine start failed: %r", error)
                if attempt >= 3:
                    world.plan.enabled = (
                        False  # an outage of the store long enough to block every start is not this test's
                    )
                self._run(asyncio.sleep(2.0))
        world.plan.enabled = True
        raise Violation("no engine could start, with the object store healthy")

    def _request(self, call, what: str):
        """A client's request: its answer, or None if it failed (outcome unknown)."""

        try:
            return self._run(self.world.request(call))
        except (Exception, Killed) as error:
            if isinstance(error, Violation):
                raise
            self.trace.append(f"  # {what} failed: {type(error).__name__}: {str(error)[:120]}")
            return None

    # -- clients ------------------------------------------------------------------------

    @rule(
        op=st.sampled_from(["upsert", "remove", "replace"]),
        keys=st.sets(st.sampled_from(KEYS), min_size=1, max_size=3),
        version=st.sampled_from(["1", "2", "3"]),
    )
    def commit_feed(self, op, keys, version):
        keys = sorted(keys)
        self.trace.append(f"commit_feed({op!r}, {keys}, {version!r})")
        if op == "upsert":
            self.feed.update({k: version for k in keys})
            kw = {"upsert": {k: version for k in keys}}
        elif op == "remove":
            for k in keys:
                self.feed.pop(k, None)
            kw = {"remove": keys}
        else:
            self.feed.clear()
            self.feed.update({k: version for k in keys})
            kw = {"keys": dict(self.feed)}
        self._request(lambda e: e.commit_source("feed", **kw), "commit")

    @rule(op=st.sampled_from(["upsert", "remove"]), site=st.sampled_from(SITES))
    def commit_sites(self, op, site):
        self.trace.append(f"commit_sites({op!r}, {site!r})")
        if op == "upsert":
            self.sites.add(site)
            kw = {"upsert": [site]}
        else:
            self.sites.discard(site)
            kw = {"remove": [site]}
        self._request(lambda e: e.commit_source("sites", **kw), "commit")

    @rule()
    def commit_knob(self):
        self.knob = str(int(self.knob) + 1)
        self.trace.append(f"commit_knob({self.knob!r})")
        version = self.outside.knob = self.knob
        self._request(lambda e: e.commit_source("knob", version=version), "commit")

    @rule(keys=st.dictionaries(st.sampled_from(KEYS), st.sampled_from(["1", "2"]), max_size=3))
    def change_outside(self, keys):
        self.trace.append(f"change_outside({keys})")
        self.outside.keys = dict(keys)

    @rule(keys=st.sets(st.sampled_from(KEYS), max_size=2), error=st.sampled_from(sorted(FLAKY)))
    def flaky(self, keys, error="transient"):
        """`checks` fails on these keys, raising `error`'s class, until the
        next change of mind."""

        self.trace.append(f"flaky({sorted(keys)}, {error!r})")
        self.outside.flaky = dict.fromkeys(keys, error)

    @rule(classes=st.sampled_from([["failed"], ["rejected"], ["canceled"], ["all"]]))
    def retry_keys(self, classes):
        """`solera keys retry checks`: a forced retry of its failing keys of
        `classes`, and the run that takes it."""

        self.trace.append(f"retry_keys({classes})")
        self._retry(classes)

    def _retry(self, classes) -> dict | None:
        async def retry(e):
            found = e.retry_keys("checks", classes, by="sim")
            if found["partitions"]:
                await e.submit_retries("checks", found["partitions"], "sim")
            return found

        return self._request(retry, "retry")

    @rule(
        asset=st.sampled_from(TARGETS),
        mode=st.sampled_from(["incremental", "incremental", "full"]),
        upstream=st.booleans(),
        partitions=st.sampled_from(["latest", "all", "missing"]),
        keys=st.sampled_from([None, None, "full", ("k1", "k10"), ("k2",)]),
    )
    def submit(self, asset, mode, upstream, partitions, keys=None):
        """A manual run; `keys` overrides what the target's keyed input
        reads: a full pass of it, or the keys named (`KEYED_INPUT`)."""

        name = self._asset(asset)
        if name is None:
            return
        self.serial += 1
        command = f"c{self.serial}"
        upstream_output = KEYED_INPUT.get(asset)
        keys = None if upstream_output is None else keys
        self.trace.append(
            f"submit({asset!r}, mode={mode!r}, upstream={upstream}, partitions={partitions!r}"
            + (f", keys={keys!r})" if keys else ")")
        )
        override = None
        if keys:
            override = {upstream_output: keys if keys == "full" else {"keys": list(keys)}}
        run = self._request(
            lambda e: e.submit(
                [name], partitions=partitions, mode=mode, upstream=upstream, keys=override, command_id=command
            ),
            "submit",
        )
        if run is not None:
            self.runs.append(run["id"])

    @precondition(lambda self: self.runs)
    @rule(newest=st.booleans())
    def cancel(self, newest):
        engine = self.world.engine
        live = [
            r
            for r in self.runs
            if engine is not None and (engine.m.runs.get(r) or {}).get("status") not in TERMINAL | {None}
        ]
        if not live:
            return
        run = live[-1] if newest else live[0]
        self.trace.append(f"cancel({'newest' if newest else 'oldest'})")
        self._request(lambda e: e.cancel(run, by="sim"), "cancel")

    @rule(delay=st.sampled_from([0.0, 20.0, 90.0]), twice=st.booleans())
    def sensor_round(self, delay, twice):
        """The sensor worker asks for due ticks, runs them, posts what they found
        (`delay` later; `twice`: the post is retried)."""

        self.trace.append(f"sensor_round(delay={delay}, twice={twice})")
        world, deploy = self.world, self.project.manifest["deploy"]
        answer = self._request(lambda e: e.sensor_next("local", deploy, "sim-host", 4, 0.0), "sensor poll")
        for tick in (answer or {}).get("ticks", []):
            try:
                value = self.project.sensors[tick["sensor"]].fn(None)
                outcome = value.to_json() if value is not None else {}
            except Exception as error:  # posted as the host does (solera_worker.sensors)
                outcome = {"error": "".join(traceback.format_exception_only(error)).strip()}
            if delay:
                self._run(asyncio.sleep(delay))
            for _ in range(2 if twice else 1):
                self._request(
                    lambda e, t=tick, o=outcome: e.sensor_post(t["sensor"], t["tick"], o), "sensor post"
                )
        del world

    @rule(cache=st.sampled_from(list(CACHE)))
    def cache_budget(self, cache):
        """The key cache budget of engines started from now on (`CACHE`)."""

        self.trace.append(f"cache_budget({cache!r})")
        self.world.cache_budget = CACHE[cache]

    @rule(kind=st.sampled_from(["wipe", "corrupt"]))
    def cache_trouble(self, kind):
        """The serving engine's key cache files are deleted, or a byte of each
        flipped, under it."""

        self.trace.append(f"cache_trouble({kind!r})")
        self.world.cache_trouble(kind)

    @rule(between=st.sampled_from(["nothing", "keys", "write"]), clean=st.booleans())
    def round_trip(self, between, clean):
        """`items` moves to its other store and back (st1 -> st2 -> st1):
        with nothing written on st2, only a `keys=` run (which moves no
        position), or a feed change its automation writes. Its steps
        are rules of their own: the trace replays them."""

        self.redeploy("table", clean)
        if between == "keys":
            self.submit("items", "incremental", False, "latest", ("k1", "k10"))
        elif between == "write":
            self.commit_feed("upsert", {"k2"}, str(self.serial % 3 + 1))
        if between != "nothing":
            self.wait(45.0)
        self.redeploy("table", clean)

    @precondition(lambda self: self.variant.seen)
    @rule(clean=st.booleans(), ends=st.sampled_from(["succeeds", "dies"]))
    def readd_live(self, clean, ends):
        """`seen` has an attempt in flight, its worker paused before its
        result (20 s: not yet silent); a deploy removes `seen`, another adds
        it back; then the worker goes on, or dies. Its commit must not land
        in the new life (`a_life_is_its_own`)."""

        self.doom_next_worker(Fate("pause" if ends == "succeeds" else "die", "result", "before", 20.0))
        self.submit("seen", "full", False, "latest")  # the next worker launched
        self.wait(2.0)
        self.redeploy("seen", clean)
        self.redeploy("seen", clean)
        self.wait(45.0)

    @rule(broken=st.booleans())
    def break_watch(self, broken):
        """`watch` raises on every tick from now on, or works again."""

        self.trace.append(f"break_watch({broken})")
        self.outside.broken = broken

    @rule(hosts=st.sampled_from([0, 1, 2]))
    def pool_hosts(self, hosts):
        """How many hosts poll `split`'s pool: none (its attempts wait),
        one, or two (racing for each claim)."""

        self.trace.append(f"pool_hosts({hosts})")
        self.world.pool_hosts = hosts

    # -- time and bad luck --------------------------------------------------------------------

    @rule(seconds=st.sampled_from([0.5, 2.0, 10.0, 45.0, 120.0, 700.0]))
    def wait(self, seconds):
        self.trace.append(f"wait({seconds})")
        self._run(asyncio.sleep(seconds))

    @rule(fate=fates)
    def doom_next_worker(self, fate):
        self.trace.append(f"doom_next_worker({fate})")
        self.world.fates.append(fate)

    @rule(
        point=st.sampled_from(["spec", "control", "flush"]),
        seconds=st.sampled_from([0.0, 5.0, 30.0]),
        pool=st.booleans(),
        crash=st.booleans(),
    )
    def stall_launch(self, point, seconds, pool, crash):
        """The serving engine stalls `seconds` in the middle of its next
        launch (a pool attempt's, with `pool`) — writing the spec, the
        control file, or waiting for the flush that makes `AttemptLaunched`
        durable — while pool hosts, workers and its own other work go on;
        then it carries on, or, with `crash`, crashes there."""

        self.trace.append(f"stall_launch({point!r}, {seconds:g}s, pool={pool}, crash={crash})")
        self.world.launch_fate = (point, seconds, pool, crash)

    @rule(
        error=st.sampled_from([0.0, 0.02, 0.1]),
        lost=st.sampled_from([0.0, 0.02, 0.1]),
        delay=st.sampled_from([0.0, 1.0, 15.0]),
    )
    def store_weather(self, error, lost, delay):
        self.trace.append(f"store_weather(error={error}, lost={lost}, delay={delay})")
        plan = self.world.plan
        plan.error, plan.lost, plan.delay = error, lost, delay

    @rule(clean=st.booleans(), down=st.sampled_from([0.0, 5.0, 120.0]))
    def restart(self, clean, down):
        self.trace.append(f"restart(clean={clean}, down={down})")
        world = self.world
        self._restarted()
        self._run(world.stop() if clean else world.crash())
        if down:
            self._run(asyncio.sleep(down))
        self._ensure_engine(self.project)

    # A rename twice as often as any other change: it alone interleaves an asset's lives (F34).
    @rule(zombie=st.sampled_from([0.0, 5.0, 60.0, 600.0]), change=st.sampled_from([None, *CHANGES, "rename"]))
    def takeover(self, zombie, change):
        """A second engine starts while the first still runs (a rolling
        deploy, a split brain); the platform kills the first `zombie`
        seconds later, so takeovers within that time leave several engines
        running at once. With `change`, the newcomer serves a new variant."""

        self.trace.append(f"takeover(zombie={zombie}, change={change!r})")
        world = self.world
        self._restarted()
        old = world.slot
        if change is not None:
            self._change(change)
        self._ensure_engine(self.project)

        async def reap():
            await asyncio.sleep(zombie)
            await world.crash(old)

        if old is not None and not old.dead:
            if zombie:
                world._spawn(None, reap())
            else:
                self._run(world.crash(old))

    @rule(count=st.sampled_from([2, 3]), zombie=st.sampled_from([0.0, 5.0]))
    def rename_burst(self, count, zombie):
        """Renames in quick succession, no time between: an attempt of one
        life still runs while its name goes away and comes back (F34)."""

        for _ in range(count):
            self.takeover(zombie, "rename")

    def _change(self, change: str) -> None:
        """The project moves to another variant (`VARIANTS`)."""

        self.variant = VARIANTS[change](self.variant)
        self.project = self._build()

    @rule(change=st.sampled_from(CHANGES), clean=st.booleans())
    def redeploy(self, change, clean):
        """Register a changed project: the engine restarts on it."""

        self.trace.append(f"redeploy({change!r}, clean={clean})")
        self._change(change)
        world = self.world
        self._restarted()
        self._run(world.stop() if clean else world.crash())
        self._ensure_engine(self.project)

    @rule(keep=st.sampled_from([0, 2]))
    def prune(self, keep):
        self.trace.append(f"prune(keep={keep})")
        self._request(lambda e: e.prune(keep=keep), "prune")

    # -- invariants ------------------------------------------------------------------------

    @invariant()
    def no_state_broke(self):
        if self.world is not None and self.world.exits:
            raise Violation(f"an engine's state broke and it exited: {self.world.exits}")

    @invariant()
    def one_end_per_attempt(self):
        if self.world is None:
            return
        if self.journal.problems:
            raise Violation("; ".join(self.journal.problems))
        for attempt, ends in self.journal.finished.items():
            if len(ends) > 1:
                raise Violation(
                    f"attempt {attempt} ended {len(ends)} times: "
                    + ", ".join(f"event {s}: {e['outcome']}" for s, e in ends)
                )

    @invariant()
    def nothing_read_after_collection(self):
        """No live reader finds an index file or a data object gone because
        collection deleted it (docs/lifecycle.md §9.8, reader pins)."""

        world = self.world
        if world is None:
            return
        log, start = world.objects.log, getattr(self, "_read_checked", 0)
        self._read_checked = len(log)
        for op in log[start:]:
            if op.kind not in ("get", "range") or op.found or op.gone is None or op.who is None:
                continue
            if not (op.path.endswith(".kx") or op.path.startswith(world.data_root)):
                continue
            if op.who in world.objects.dead:
                continue
            if op.who[0] == "worker":
                ends = self.journal.finished.get(op.who[1]) or []
                if any(e["finished_at"] <= op.at + EPOCH for _, e in ends):
                    continue  # the attempt had ended: a straggler reads what it no longer may
            elif op.who[0] == "engine" and world.slots[op.who[1]] is not world.slot:
                continue  # a zombie engine
            when, by = op.gone
            raise Violation(
                f"{op.who} read {op.path.removeprefix(str(self.tmp))} at t={op.at:g}, "
                f"deleted at t={when:g} by {by}"
            )

    @invariant()
    def reads_say_what_they_read(self):
        """A current-read store's reads (PostgresStore): the generation each
        reports wrote the rows it loaded, and was the newest write before its
        snapshot (lineage of what was read)."""

        world = self.world
        if world is None or world.pg is None:
            return
        self._pg_checked = postgres.check(world.pg, self._pg_checked)

    @invariant()
    def a_life_is_its_own(self):
        """An asset removed and added back starts over: no attempt launched
        in its first life installs a commit into the second."""

        if self.world is not None and (crossed := self.journal.a_life_crossed()):
            raise Violation(f"a first life's attempt committed into the second: {crossed}")

    @invariant()
    def reads_at_endpoints_are_exact(self):
        """Every key-index read at an endpoint — `page` and `lookup` at a
        position, pin or snapshot, `changes` between two — equals the fold of
        the commits its index holds up to there (`tests/sim/reads.py`)."""

        if self.world is not None and self.world.reads.wrong:
            raise Violation(f"a read at an endpoint is not exact: {self.world.reads.wrong[0]}")

    @invariant()
    def one_attempt_per_partition(self):
        """A claim holds an asset partition for one attempt at a time: no
        attempt launches on one another launched attempt holds, in the
        journal or in the serving engine's memory."""

        if self.world is None:
            return
        if clash := self.journal.two_attempts_at_once():
            raise Violation(f"two attempts at once: {clash}")
        engine = self.world.engine
        if engine is None:
            return
        held: dict[tuple, str] = {}
        m = engine.m
        for task_id, claim in list(m.claims.items()):
            task = m.task(task_id)
            if m.earlier_life(task):
                continue  # an earlier life's attempt (F12's rule): another asset, of the same name
            partition = (task["asset"], task["partition"])
            if partition in held:
                raise Violation(f"{partition} claimed by {held[partition]} and {claim['attempt']} at once")
            held[partition] = claim["attempt"]
            if m.claimed_partitions.get(partition) != claim["attempt"]:  # never overwritten (F34)
                raise Violation(f"{partition}: the claim index lost {claim['attempt']}'s claim")

    @invariant()
    def a_ticks_runs_are_submitted_once(self):
        """A sensor tick's outcome is applied all or nothing, once: posted
        late, twice, or to an engine that restarted, its run requests are
        submitted at most once each."""

        if self.world is not None and (twice := self.journal.ticks_submitted_twice()):
            raise Violation("; ".join(twice))

    @invariant()
    def fenced_writes_hold_their_gate(self):
        """docs/lifecycle.md §2.4, §3: a worker writes to a fenced store only
        after it took its attempt's gate (its control file `writing`, its own
        worker id): an attempt the engine ended, or a duplicate worker,
        writes nothing."""

        if self.world is None:
            return
        db, pg = getattr(self, "_gates_checked", (0, 0))
        writes = [(at, who, worker_id) for at, who, _, worker_id in self.db.writes[db:]]
        if self.world.pg is not None:
            writes += [
                (w.at, w.who, w.worker_id) for ws in self.world.pg.writes.values() for w in ws if w.seq > pg
            ]
        self._gates_checked = (len(self.db.writes), self.world.pg.seq if self.world.pg is not None else 0)
        for at, who, worker_id in writes:
            if who is None or who[0] != "worker":
                continue
            gate = self.journal.gates.get(who[1])
            if gate is None or gate[0] != "writing" or gate[1] != worker_id or gate[2] > at:
                raise Violation(
                    f"{who} wrote to a fenced store at t={at:g} (worker {worker_id}); "
                    f"its gate: {gate and gate[:2]}{f' from t={gate[2]:g}' if gate else ''}"
                )

    @invariant()
    def index_spans_tile(self):
        """docs/key-index-design.md: an index's spans tile its commits from 0
        to the head, and a span's files are in key order (a key's versions
        may cross from one into the next), so a read takes one file per span
        for a key."""

        engine = self.world.engine if self.world is not None else None
        if engine is None:
            return
        for (output, partition), index in list(engine.m.indexes.items()):
            starts = [s.a for s in index.spans]
            if starts != [0, *(s.b + 1 for s in index.spans)][: len(starts)]:
                raise Violation(
                    f"{output}[{partition!r}]: spans {[(s.a, s.b) for s in index.spans]} do not tile"
                )
            for s in index.spans:
                for a, b in zip(s.files, s.files[1:], strict=False):
                    if a.max > b.min:
                        raise Violation(
                            f"{output}[{partition!r}] span {s.a}..{s.b}: {a.name} and {b.name} overlap"
                        )

    @invariant()
    def committed_keys_are_readable(self):
        """Every key an immutable store's head lists reads back at its
        generation, from an object a committed attempt wrote."""

        world = self.world
        if world is None or world.engine is None:
            return
        self.index_spans_tile()  # a read of spans that do not tile fails as that, not as this
        engine = world.engine

        async def check():
            for (output, partition), head in list(engine.m.heads.items()):
                if head["ref"].get("meta", {}).get("source") or (output, partition) not in engine.m.indexes:
                    continue
                store = self.project.stores.get(head["ref"]["store"])
                if getattr(store, "writes", None) != "immutable" or output == "sites":
                    continue
                await keyed_content(engine, self.project, output, partition, column=None)
                entries = await index_entries(engine.state, output, partition)
                committed = self.journal.committed_generations()  # after the reads: commits land meanwhile
                for key, (generation, _) in entries.items():
                    if generation and generation not in committed:
                        raise Violation(
                            f"{output}[{partition!r}] key {key}: its object was written by generation "
                            f"{generation}, which never committed"
                        )

        self._run(check())

    @invariant()
    def a_fenced_scope_at_rest_holds_its_index_keys(self):
        """docs/versions.md §9: a fenced store's partition that no attempt holds
        and no dead writer left owing a repair holds exactly the keys its index
        lists — a repair by presence leaves no key without rows, and no row
        without its key — and, in Postgres, reads as written by its head's
        generation: a repair always writes, so a dead attempt's generation,
        which no commit has, is never what a later read reports (break 1)."""

        world = self.world
        if world is None or world.engine is None:
            return
        engine = world.engine  # the one checked, should another replace it meanwhile
        m = engine.m

        async def check():
            for (output, partition), head in list(m.heads.items()):
                store = self.project.stores.get(head["ref"]["store"])
                if getattr(store, "writes", None) != "fenced":
                    continue
                if (output, partition) in m.repairs or (head.get("asset"), partition) in m.claimed_partitions:
                    continue
                if (output, partition) in m.indexes:
                    await keyed_content(engine, self.project, output, partition, whole=True, column=None)
                if head["ref"]["store"] == "pg":
                    ref = Ref.from_json(head["ref"])
                    with store._connect() as conn, conn.cursor() as cur:
                        written = store._written(cur, ref)
                    if written is not None and written != ref.generation:
                        raise Violation(
                            f"{output}[{partition!r}] reads as written by generation {written}; "
                            f"its head is generation {ref.generation}"
                        )

        self._run(check())

    # -- the end: quiet, then converged ---------------------------------------------------

    def teardown(self):
        import sys

        try:
            if self.world is not None and sys.exc_info()[0] is None:  # not after a failed step
                self._converge()
        finally:
            if getattr(self, "schema", None):
                _drop(self.schema)
            if self.world is not None:
                STATS["examples"] += 1
                STATS["steps"] += len([t for t in self.trace if not t.startswith(("  #", "#"))])
                STATS["virtual"] += self.world.now()
                self.world.close()
            if os.environ.get("SOLERA_SIM_TRACE"):
                print("\n".join(self.trace), flush=True)
            if os.environ.get("SOLERA_SIM_REQUESTS") and self.world is not None:
                self._write_requests(Path(os.environ["SOLERA_SIM_REQUESTS"]))
            shutil.rmtree(self.tmp, ignore_errors=True)

    def _write_requests(self, out: Path) -> None:
        """This example's object requests as JSONL, after its steps (`{"step": …}`)."""

        out.mkdir(parents=True, exist_ok=True)
        n = STATS["examples"]
        with open(out / f"{n:05d}.jsonl", "w") as f:
            for step in self.trace:
                f.write(json.dumps({"step": step}) + "\n")
            for line in self.requests:
                f.write(json.dumps(line) + "\n")

    def _quiet(self) -> str | None:
        """Why the system is not quiet yet, or None."""

        world, engine = self.world, self.world.engine
        if engine is None:
            return "no engine"
        busy = [r for r, run in engine.m.runs.items() if run["status"] not in TERMINAL]
        if busy:
            run = engine.m.runs[busy[0]]
            tasks = {
                t: (x["status"], x.get("held"))
                for t, x in run["tasks"].items()
                if x["status"] not in TERMINAL
            }
            return f"run {busy[0]} {run['status']}: {tasks}"
        if engine.m.claims:
            return f"claims {list(engine.m.claims)}"
        if world.live_workers():
            return f"workers {[w.who for w in world.live_workers()]}"
        pending = {n: a["pending"] for n, a in engine.m.automations.items() if a.get("pending")}
        if pending:
            return f"automations pending {pending}"
        return None

    def _settle(self, budget: float = 4 * 3600.0) -> None:
        world, waited = self.world, 0.0
        while (why := self._quiet()) is not None:
            if waited >= budget:
                raise Violation(f"never quiet after {waited:g}s: {why}")
            self._ensure_engine()
            self._run(asyncio.sleep(30.0))
            waited += 30.0
        del world

    def _converge(self) -> None:
        world = self.world
        self.trace.append("# converge")
        world.plan.enabled = False
        world.fates.clear()
        world.launch_fate, world.calm = None, True
        self.outside.flaky, self.outside.broken = {}, False
        world.pool_hosts = max(world.pool_hosts, 1)
        for slot in world.slots:
            if slot is not world.slot and not slot.dead:
                self._run(world.crash(slot))
        self._ensure_engine()
        # A fresh change to every source: what automations deliver from here
        # on covers everything before it.
        self.feed["k3"] = str(int(self.feed.get("k3", "0")) + 10)
        self.knob = self.outside.knob = str(int(self.knob) + 1)
        for name, kw in (
            ("feed", {"keys": dict(self.feed)}),
            ("sites", {"keys": sorted(self.sites)}),
            ("knob", {"version": self.knob}),
        ):
            if self._request(lambda e, n=name, k=kw: e.commit_source(n, **k), f"final {name}") is None:
                raise Violation(f"a final commit to {name} failed with the store healthy")
        self._settle()
        for _ in range(4):  # the sensor brings `outside` in line with the world
            self.sensor_round(0.0, False)
            self._run(asyncio.sleep(31.0))
        # Keys failed, rejected or canceled are retried only on request (or a
        # new deploy, or a change of their input): ask, as an operator would.
        if self._retry(["canceled", "failed", "rejected"]) is None:
            raise Violation("a forced retry failed with the store healthy")
        self._settle()
        self._check_content(automated=True)
        names = [n for n in (self._asset(t) for t in TARGETS) if n]
        run = self._request(lambda e: e.submit(names, partitions="all", upstream=True), "catch-up")
        if run is None:
            raise Violation("the catch-up run could not be submitted")
        self._settle()
        self._check_content(automated=False)
        self._check_replay()
        self.one_attempt_per_partition()  # convergence ran no invariant
        self.a_life_is_its_own()
        self.a_ticks_runs_are_submitted_once()
        self.fenced_writes_hold_their_gate()

    def _asset(self, target: str) -> str | None:
        if target == "copy":
            return self.variant.copy_name
        if (target == "summary" and not self.variant.summary) or (target == "seen" and not self.variant.seen):
            return None
        return target

    def _check_content(self, automated: bool) -> None:
        self.index_spans_tile()  # convergence ran no invariant
        engine, project, variant = self.world.engine, self.project, self.variant
        stage = "after automations alone" if automated else "after a catch-up run"
        # A read at an endpoint that went wrong is the cause; contents that differ, its effect.
        self.reads_at_endpoints_are_exact()

        async def check():
            items = await keyed_content(engine, project, "items", whole=True)
            want = expected_items(self.feed, variant)
            if items != want:
                raise Violation(f"items {stage}: {items} != {want} (feed {self.feed})")
            copy = await keyed_content(engine, project, variant.copy_name, whole=True)
            if copy != expected_copy(want, variant):
                raise Violation(f"{variant.copy_name} {stage}: {copy} != {expected_copy(want, variant)}")
            for output, want_split in zip(("odd", "even"), expected_split(want), strict=True):
                got = await keyed_content(engine, project, output, whole=True)
                if got != want_split:
                    raise Violation(f"{output} {stage}: {got} != {want_split}")
            if variant.seen:
                seen = engine.m.partition("seen", "").get("cursor") or {}
                if seen != want:
                    raise Violation(f"the job seen's cursor {stage}: {seen} != {want}")
            checks = await keyed_content(engine, project, "checks", whole=True, column="w")
            if checks != expected_checks(want, self.knob):
                raise Violation(f"checks {stage}: {checks} != {expected_checks(want, self.knob)}")
            outside = {  # each key at the version its last tick gave it
                k: p.decode() for k, (_, p) in (await index_entries(engine.state, "outside", "")).items()
            }
            if outside != self.outside.keys:
                raise Violation(f"outside {stage}: {outside} != {self.outside.keys}")
            for site in sorted(self.sites):
                got = await value_content(engine, project, "per_site", site)
                if got != {"site": site}:
                    raise Violation(f"per_site[{site}] {stage}: {got}")
            if variant.summary:
                got = await value_content(engine, project, "summary")
                heads = sorted(s for (o, s) in engine.m.heads if o == "per_site" and s in self.sites)
                if got is not None and sorted(got) != heads and not automated:
                    raise Violation(f"summary {stage}: {got} != {heads}")
            if not automated:
                rows = await commit_rows(engine, project, "log")
                tally = await value_content(engine, project, "tally")
                if tally != {"rows": len(rows)}:
                    raise Violation(f"tally {stage}: {tally} for {len(rows)} rows of log")

        self._run(check())

    def _check_replay(self) -> None:
        """The journal alone rebuilds the engine's state."""

        from solera_server.state import State

        world = self.world
        engine = world.engine

        async def check():
            await world.request(lambda e: e.state.durable())
            if world.objects.trace is not None:
                self.requests.append({"reader": True})  # the requests with no actor that follow are its
            replayed = await State.open(world.url, "sim", writer=False, clock=world.wall)
            live = _normal(engine.state.model.snapshot())
            again = _normal(replayed.model.snapshot())
            if live != again:
                diff = _first_diff(live, again)
                raise Violation(f"replaying the journal gives another state: {diff}")

        self._run(check())


def _normal(snapshot: dict) -> dict:
    return json.loads(json.dumps(snapshot, sort_keys=True, default=str))


def _first_diff(a, b, path="") -> str:
    if type(a) is not type(b):
        return f"{path}: {str(a)[:200]} != {str(b)[:200]}"
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b), key=str):
            if k not in a or k not in b:
                return f"{path}/{k}: only in {'live' if k in a else 'replay'}"
            if a[k] != b[k]:
                return _first_diff(a[k], b[k], f"{path}/{k}")
    if isinstance(a, list) and len(a) == len(b):
        for i, (x, y) in enumerate(zip(a, b, strict=True)):
            if x != y:
                return _first_diff(x, y, f"{path}[{i}]")
    return f"{path}: {str(a)[:300]} != {str(b)[:300]}"


def _observing(check):
    """An invariant observes: a platform restart under way finishes first,
    and no fate strikes while it reads (`World.observing`)."""

    @functools.wraps(check)
    def observed(self):
        if self.world is None:
            return check(self)
        self._restarted()
        self.world.observing = True
        try:
            return check(self)
        finally:
            self.world.observing = False

    # Hypothesis calls the function its marker holds: the marker must hold this one.
    marker = check.hypothesis_stateful_invariant
    observed.hypothesis_stateful_invariant = dataclasses.replace(marker, function=observed)
    return observed


for _name in [n for n, v in vars(Simulation).items() if getattr(v, "hypothesis_stateful_invariant", None)]:
    setattr(Simulation, _name, _observing(getattr(Simulation, _name)))


def _drop(schema: str) -> None:
    import psycopg

    with psycopg.connect(postgres.DSN, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
