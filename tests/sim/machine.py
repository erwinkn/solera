"""The simulation as a Hypothesis state machine: rules are what clients,
operators, the platform and bad luck do; invariants are checked after
every step; the teardown makes the world quiet and checks that it
converged. Shrinking turns a failing run into its shortest sequence.

`Simulation.trace` is the readable log of a run: every step, as the
regression test that replays it would spell it."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path

from hypothesis import assume
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
    "known": {},
}  # across runs of this process

KEYS = ["k0", "k1", "k2", "k3", "k10", "k11"]
SITES = ["east", "west", "north"]
TARGETS = ["items", "copy", "per_site", "log", "tally", "summary", "checks", "split"]
KEYED_INPUT = {
    "items": "feed",
    "copy": "items",
    "checks": "items",
    "split": "items",
}  # what a run's `keys=` overrides
TERMINAL = {"succeeded", "failed", "canceled", "skipped"}
STORES = ["file", "table"] + (["pg"] if postgres.DSN else [])  # where `items` lives
# Re-registrations the rules make. Those that trip an open finding on most
# runs are left out until it is fixed (tests/server/test_sim_found.py);
# SOLERA_SIM_KNOWN=1 puts them back.
KNOWN = {
    "exclude": "a reset delivery its patterns take nothing from never starts the consumer over (F10)",
}
CHANGES = sorted(set(VARIANTS) - (set() if os.environ.get("SOLERA_SIM_KNOWN") else set(KNOWN)))

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

    @initialize(seed=st.integers(0, 2**16), store=st.sampled_from(STORES))
    def boot(self, seed, store="file"):
        self.trace.append(f"boot(seed={seed}, store={store!r})")
        self.world = world = World(self.tmp, seed, key_options=Options(l0_max_files=2))
        self.journal = Journal(now=world.now)
        world.objects.tap = self.journal.landed
        world.on_record = self.journal.recorded
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

    def _db_fault(self, kind, partition):
        if not self.world.plan.enabled:
            return None, 0.0
        fate, delay = self.world.plan.decide()
        return fate, delay

    def _run(self, coro, timeout: float = 3600.0):
        return self.world.run(coro, timeout=timeout)

    def _ensure_engine(self, project=None) -> None:
        """The platform keeps one engine serving: start one if none is."""

        world = self.world
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

    def _known(self, finding: str, detail: str) -> None:
        """An open finding struck (tests/server/test_sim_found.py): the run
        is set aside, unless SOLERA_SIM_KNOWN asks to see it fail."""

        self.trace.append(f"  # known {finding}: {detail}")
        STATS["known"][finding] = STATS["known"].get(finding, 0) + 1
        if os.environ.get("SOLERA_SIM_KNOWN"):
            raise Violation(f"{finding}: {detail}")
        assume(False)

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
        version = self.knob
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
        """The sensor host asks for due ticks, runs them, posts what they found
        (`delay` later; `twice`: the post is retried)."""

        self.trace.append(f"sensor_round(delay={delay}, twice={twice})")
        world, deploy = self.world, self.project.manifest["deploy"]
        answer = self._request(lambda e: e.sensor_next("local", deploy, "sim-host", 4, 0.0), "sensor poll")
        for tick in (answer or {}).get("ticks", []):
            value = self.project.sensors[tick["sensor"]].fn(None)
            outcome = value.to_json() if value is not None else {}
            if delay:
                self._run(asyncio.sleep(delay))
            for _ in range(2 if twice else 1):
                self._request(
                    lambda e, t=tick, o=outcome: e.sensor_post(t["sensor"], t["tick"], o), "sensor post"
                )
        del world

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
        self._run(world.stop() if clean else world.crash())
        if down:
            self._run(asyncio.sleep(down))
        self._ensure_engine(self.project)

    @rule(zombie=st.sampled_from([0.0, 5.0, 60.0, 600.0]), change=st.sampled_from([None, *CHANGES]))
    def takeover(self, zombie, change):
        """A second engine starts while the first still runs (a rolling
        deploy, a split brain); the platform kills the first `zombie`
        seconds later, so takeovers within that time leave several engines
        running at once. With `change`, the newcomer serves a new variant."""

        self.trace.append(f"takeover(zombie={zombie}, change={change!r})")
        world = self.world
        old = world.slot
        if change is not None:
            self.variant = VARIANTS[change](self.variant)
            self.project = self._build()
        self._ensure_engine(self.project)

        async def reap():
            await asyncio.sleep(zombie)
            await world.crash(old)

        if old is not None and not old.dead:
            if zombie:
                world._spawn(None, reap())
            else:
                self._run(world.crash(old))

    @rule(change=st.sampled_from(CHANGES), clean=st.booleans())
    def redeploy(self, change, clean):
        """Register a changed project: the engine restarts on it."""

        self.trace.append(f"redeploy({change!r}, clean={clean})")
        self.variant = VARIANTS[change](self.variant)
        self.project = self._build()
        world = self.world
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
        if overwritten := self.journal.overwritten(self.world.objects.deleted, self.world.now()):
            raise Violation(
                f"journal segments {overwritten} landed twice with different bytes, both readable"
            )
        for attempt, ends in self.journal.finished.items():
            if len(ends) > 1:
                raise Violation(
                    f"attempt {attempt} ended {len(ends)} times: "
                    + ", ".join(f"seq {s}: {e['outcome']}" for s, e in ends)
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
            if op.who[0] == "worker" and self._handed_to_cleanup(op.who[1], op.path):
                self._known("F11", f"{op.who} read a delta its discard entry names, deleted at t={when:g}")
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

    def _handed_to_cleanup(self, attempt: str, path: str) -> bool:
        """F11's signature: the file is named by a discard entry of the attempt's spec."""

        launched = self.journal.launched.get(attempt)
        if launched is None or self.world.engine is None:
            return False
        spec = self._run(self.world.engine.state.attempt_spec(launched["run"], attempt)) or {}
        name = path.rsplit("/", 1)[-1].removesuffix(".kx")
        return any(
            name in entry.get("files", ())
            for out in (spec.get("outputs") or {}).values()
            for entry in out.get("cleanup") or ()
        )

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
        for task_id, claim in list(engine.m.claims.items()):
            task = engine.m.task(task_id)
            partition = (task["asset"], task["partition"])
            if partition in held:
                raise Violation(f"{partition} claimed by {held[partition]} and {claim['attempt']} at once")
            held[partition] = claim["attempt"]

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
        after it took its attempt's gate (`writing`, its own worker id): an
        attempt the engine ended (`aborted`, `closed`), or a duplicate
        worker, writes nothing."""

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
    def index_levels_never_overlap(self):
        """docs/key-index-format.md: an index's level 0 holds one file per
        commit, overlapping; within each deeper level, files cover disjoint
        key ranges, so a read takes one file per level. While F16 is open,
        an overlap sets the run aside."""

        engine = self.world.engine if self.world is not None else None
        if engine is None:
            return
        for (output, partition), index in list(engine.m.indexes.items()):
            for level in range(1, index.depth + 1):
                files = sorted(index.level(level), key=lambda f: f.min)
                for a, b in zip(files, files[1:], strict=False):
                    if a.max >= b.min:
                        detail = f"{output}[{partition!r}] level {level}: {a.name} and {b.name} overlap"
                        self._known("F16", detail)

    @invariant()
    def committed_keys_are_readable(self):
        """Every key an immutable store's head lists reads back at its
        generation, from an object a committed attempt wrote."""

        world = self.world
        if world is None or world.engine is None:
            return
        self.index_levels_never_overlap()  # a read of overlapping levels is F16's, not this one's
        engine = world.engine

        async def check():
            for (output, partition), head in list(engine.m.heads.items()):
                if head["ref"].get("meta", {}).get("external") or (output, partition) not in engine.m.indexes:
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
        """docs/versions.md §9: a fenced store's scope that no attempt holds
        and no dead writer left unsettled holds exactly the keys its index
        lists — a repair by presence leaves no key without rows, and no row
        without its key — and, in Postgres, reads as written by its head's
        generation: a repair always writes, so a dead attempt's generation,
        which no commit has, is never what a later read reports (break 1)."""

        world = self.world
        if world is None or world.engine is None:
            return
        m = world.engine.m

        async def check():
            for (output, partition), head in list(m.heads.items()):
                store = self.project.stores.get(head["ref"]["store"])
                if getattr(store, "writes", None) != "fenced":
                    continue
                if (output, partition) in m.unsettled or (head.get("asset"), partition) in m.locks:
                    continue
                if (output, partition) in m.indexes:
                    await keyed_content(
                        world.engine, self.project, output, partition, whole=True, column=None
                    )
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
            shutil.rmtree(self.tmp, ignore_errors=True)

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
        self.outside.flaky = {}
        world.pool_hosts = max(world.pool_hosts, 1)
        for slot in world.slots:
            if slot is not world.slot and not slot.dead:
                self._run(world.crash(slot))
        self._ensure_engine()
        # A fresh change to every source: what automations deliver from here
        # on covers everything before it.
        self.feed["k3"] = str(int(self.feed.get("k3", "0")) + 10)
        self.knob = str(int(self.knob) + 1)
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
        self.a_ticks_runs_are_submitted_once()
        self.fenced_writes_hold_their_gate()

    def _asset(self, target: str) -> str | None:
        if target == "copy":
            return self.variant.copy_name
        if target == "summary" and not self.variant.summary:
            return None
        return target

    def _check_content(self, automated: bool) -> None:
        self.index_levels_never_overlap()  # convergence ran no invariant
        engine, project, variant = self.world.engine, self.project, self.variant
        stage = "after automations alone" if automated else "after a catch-up run"

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
            checks = await keyed_content(engine, project, "checks", whole=True, column="w")
            if checks != expected_checks(want):
                raise Violation(f"checks {stage}: {checks} != {expected_checks(want)}")
            outside = {  # each key at the version its last observation gave it
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
                    if tally and tally["rows"] > len(rows):
                        self._known(
                            "F8",
                            f"tally counted {tally['rows']} rows of {len(rows)}: a reset read as a delta",
                        )
                    raise Violation(f"tally {stage}: {tally} for {len(rows)} rows of log")

        self._run(check())

    def _check_replay(self) -> None:
        """The journal alone rebuilds the engine's state."""

        from solera_server.state import State

        world = self.world
        engine = world.engine

        async def check():
            await world.request(lambda e: e.state.durable())
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


def _drop(schema: str) -> None:
    import psycopg

    with psycopg.connect(postgres.DSN, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
