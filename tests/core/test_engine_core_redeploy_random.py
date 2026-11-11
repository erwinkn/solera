"""Deploys at every await of the new tracking core's operations (the deploy generation rule):
a random history of runs fires a deploy (a rename, a rename back, an axis change, a store
move that resets the output, an edge closed by removing a reader) after any native call,
before any output lock, after the state's awaits a launch and a build make, and when a worker
starts. No tick raises, a whole reader never reads
its upstream's partial content, every run ends, everything converges once the deploys stop,
and a replay rebuilds the same model. Each history starts with a full run of the producer
stopped midway, whose reader `nxt` has a reader of its own (`tail`), and asks to proceed at
random: the new core refuses (ERW-42), no span is ever proceeded, and no reader reads partial
content; the held readers go on once the producer is whole. `REDEPLOY_SEEDS` sets the seeds (default 0-5),
`REDEPLOY_TRACE=1` prints what each commit was planned under, `REDEPLOY_FIRED=<file>` appends
where each seed's deploys landed."""

import asyncio
import json
import os
import random
import re

import pytest
from solera.sdk import In, Incremental, Output, Project, Retry, asset
from solera.stores import FileStore
from solera_server.state import State
from solera_server.tracking import full

from tests.contract.harness import Refused, Solera, rebuild

SEEDS = [int(s) for s in os.environ.get("REDEPLOY_SEEDS", "0,1,2,3,4,5").split(",")]
STABLE = ["a0", "m0", "z0"]  # never removed: a whole read without one of them read partial content


class History:
    def __init__(self, solera, seed):
        self.s, self.rng = solera, random.Random(seed)
        self.variant = {"name": "copy", "axis": "keyed", "store": "default", "edge": True}
        self.rows = {k: 1 for k in [*STABLE, "c1", "k1", "t1"]}
        self.projects, self.fired, self.budget, self.armed = {}, [], 8, False
        self.reads, self.read, self.committed, self.trace = 0, {}, [], []
        self.removed, self.refused, self.proceeded = set(), 0, []  # assets a deploy removed
        self.hold, self.waiting, self.stop = asyncio.Event(), False, None
        self.hold.set()

    def project(self, variant):
        h, name = self, variant["name"]
        other = "copy2" if name == "copy" else "copy"
        keyed = variant["axis"] == "keyed"

        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in h.rows.items()]

        async def body(ctx, files: list):
            await asyncio.sleep(0.005)
            b = ctx.batch["files"]
            if h.stop is not None and b.index >= h.stop:  # stopped midway
                h.waiting = True
                await h.hold.wait()
            if keyed:
                return rebuild(b, [{"id": r["id"], "v": r["v"]} for r in files])
            held = {} if b.reset else dict(await ctx.load() or {})
            held.update({r["id"]: r["v"] for r in files})
            for k in b.removed:
                held.pop(k, None)
            return held

        body.__name__ = body.__qualname__ = name
        out = Output(key="id", store=variant["store"]) if keyed else Output(store=variant["store"])
        producer = asset(
            inputs={"files": Incremental(batch_size=1)}, outputs=out, aliases=[other], retries=Retry(n=1)
        )(body)

        def seen(ctx, ids, who="report"):
            h.reads += 1
            h.read[h.reads] = (ids, dict(h.variant), list(h.fired), who)
            ctx.metadata(tok=h.reads)
            return ids

        if keyed:

            @asset(inputs={"rows": In(name)}, retries=Retry(n=1))
            async def report(ctx, rows: list):
                return seen(ctx, sorted(r["id"] for r in rows))

        else:

            @asset(inputs={"rows": In(name)}, retries=Retry(n=1))
            async def report(ctx, rows: dict):
                return seen(ctx, sorted(rows))

        assets = [files, producer, report]
        if keyed and variant["edge"]:

            @asset(
                inputs={"rows": Incremental(name, batch_size=2)},
                outputs=Output("nxt", key="id"),
                retries=Retry(n=1),
            )
            async def nxt(ctx, rows: list):
                return rebuild(ctx.batch["rows"], [{"id": r["id"], "v": r["v"]} for r in rows])

            @asset(inputs={"rows": In("nxt")}, retries=Retry(n=1))
            async def tail(ctx, rows: list):
                return seen(ctx, sorted(r["id"] for r in rows), "tail")

            assets += [nxt, tail]
        stores = {"other": FileStore(self.s.root / "other")}
        made = Project(assets=assets, stores=stores, default_store=FileStore(self.s.root / "data"))
        self.projects[made.manifest["deploy"]] = made
        return made

    def fire(self, where):
        if not self.armed or self.budget <= 0 or self.rng.random() > 0.12:
            return
        variant = dict(self.variant)
        what = self.rng.choice(["rename", "axis", "store", "edge"])
        if what == "rename":
            variant["name"] = "copy2" if variant["name"] == "copy" else "copy"
        elif what == "axis":
            variant["axis"] = "value" if variant["axis"] == "keyed" else "keyed"
        elif what == "store":
            variant["store"] = "other" if variant["store"] == "default" else "default"
        else:
            variant["edge"] = not variant["edge"]
        self.budget -= 1
        self.variant = variant
        self.fired.append((where, what))
        self.trace.append(("FIRE", where, what, dict(variant)))
        manifest = self.project(variant).manifest
        self.removed |= set(self.s.engine.manifest["assets"]) - set(manifest["assets"])
        self.s.engine.redeploy(manifest)

    async def proceed(self):
        """An operator asks to proceed over the producer's stopped run: the new
        core refuses it until ERW-42."""

        name = self.variant["name"]
        if not any(p["asset"] == name for p in full.stopped(self.s.engine)):
            return
        try:
            await self.s.engine.proceed(name, "", "test")
        except full.ProceedNotYet:
            self.refused += 1
        else:
            raise AssertionError("proceed was not refused on the new core")

    def instrument(self, monkeypatch):
        from solera_server.executors.inline import InlinePlacement

        engine, h = self.s.engine, self
        native, lock, work = engine.core.run, engine.core.lock, InlinePlacement._work

        async def run(fn, *args, **kwargs):
            found = await native(fn, *args, **kwargs)
            h.fire(getattr(fn, "__name__", str(fn)))
            return found

        class Locked:
            def __init__(self, id_):
                self.inner = lock(id_)

            async def __aenter__(self):
                h.fire("lock")
                return await self.inner.__aenter__()

            async def __aexit__(self, *exc):
                return await self.inner.__aexit__(*exc)

        def current_worker(placement, stage):
            placement.project = h.projects[engine.manifest["deploy"]]
            h.fire("worker")
            return work(placement, stage)

        apply = engine.m.apply

        def applied(e):
            commit = e.get("commit") if e.get("type") == "AttemptFinished" else None
            for meta in ((commit or {}).get("metadata") or {}).values():
                if (meta or {}).get("tok") is not None:
                    h.committed.append(meta["tok"])
            pre = {}
            if e.get("type") == "AttemptFinished":
                pre = ((engine.m.task(e["task"]) or {}).get("launched") or {}).get("prepared") or {}
            out = apply(e)
            h.proceeded += [str(k) for k, v in engine.m.partial_spans.items() if any(f for *_, f in v)]
            if os.environ.get("REDEPLOY_TRACE"):  # what each commit was planned under and made
                m, core = engine.m, ((commit or {}).get("core") or {})
                if e.get("type") == "AttemptFinished":
                    task = m.task(e["task"]) or {}
                    h.trace.append(
                        (
                            "fin",
                            task.get("asset"),
                            e.get("outcome"),
                            pre.get("deploy_number"),
                            pre.get("full"),
                        )
                        + (core.get("counts"), core.get("walk"), e.get("error"))
                    )
                elif e.get("type") in ("ProjectRegistered", "Settled", "Proceeded", "Core"):
                    h.trace.append((e["type"], m.deploy_number))
                spans = {str(k): v for k, v in m.partial_spans.items()}
                h.trace.append(
                    ("state", spans, {str(k): v for k, v in m.core_empty.items()}, dict(m.core_outputs))
                )
            return out

        def awaited(owner, name):
            inner = getattr(owner, name)

            async def call(*args, **kwargs):
                found = await inner(*args, **kwargs)
                h.fire(name)
                return found

            monkeypatch.setattr(owner, name, call)

        for name in ("attempt_spec", "create_object", "durable"):  # the other awaits of a launch and a build
            awaited(engine.state, name)
        monkeypatch.setattr(engine.m, "apply", applied)
        monkeypatch.setattr(engine.core, "run", run)
        monkeypatch.setattr(engine.core, "lock", Locked)
        monkeypatch.setattr(InlinePlacement, "_work", current_worker)


async def assert_replay(s):
    def view(m):
        return {
            "core": m.core.snapshot() if m.core is not None else None,
            "outputs": {str(k): v for k, v in m.core_outputs.items()},
            "edges": {str(k): list(v) for k, v in m.core_edges.items()},
            "empty": {str(k): v for k, v in m.core_empty.items()},
            "read": {str(k): v for k, v in m.core_read.items()},
            "spans": json.dumps({str(k): v for k, v in m.partial_spans.items()}, sort_keys=True),
            "reset_at": {str(k): v for k, v in m.reset_at.items()},
            "heads": {
                str(k): (v["ref"].get("generation"), v.get("commit_number")) for k, v in m.heads.items()
            },
            "counter": m.event_counter,
        }

    await s.state.durable()
    live = view(s.engine.m)
    again = await State.open((s.root / "state").as_uri(), "contract", writer=False)
    try:
        replayed = view(again.model)
    finally:
        await again.close()
    assert live == replayed, {k: (live[k], replayed[k]) for k in live if live[k] != replayed[k]}


@pytest.mark.parametrize("seed", SEEDS)
async def test_deploys_at_every_await(tmp_path, monkeypatch, seed):
    s = Solera(tmp_path, "core")
    h = History(s, seed)
    try:
        await s.deploy(h.project(h.variant))
        await s.run("files")
        await s.run(h.variant["name"])
        h.stop = 1
        h.hold.clear()
        stopped = await s.submit(h.variant["name"], mode="full")
        await s.until(lambda: h.waiting)
        await s.cancel(stopped["id"])
        await s.wait(stopped)
        h.stop = None
        h.hold.set()
        await h.proceed()
        assert h.refused == 1
        tails = [
            await s.submit("nxt"),
            await s.submit("tail"),
        ]  # held until copy is whole: waited for at the end
        h.instrument(monkeypatch)
        h.armed = True
        runs = []
        for _ in range(24):
            op = h.rng.choice(
                [
                    "files",
                    "producer",
                    "producer",
                    "report",
                    "report",
                    "nxt",
                    "tail",
                    "settle",
                    "cancel",
                    "proceed",
                ]
            )
            if op == "files":
                churn = h.rng.choice(["c1", "k1", "t1", "c2", "k2"])
                if churn in h.rows and h.rng.random() < 0.5:
                    del h.rows[churn]
                else:
                    h.rows[churn] = h.rng.randint(2, 9)
                await s.run("files", expect=None)
            try:  # a deploy may land between naming a target and the request: refused, as it should be
                if op == "producer":
                    runs.append(await s.submit(h.variant["name"], mode=h.rng.choice(["full", "incremental"])))
                elif op == "report":
                    runs.append(await s.submit("report"))
                elif op == "nxt" and "nxt" in s.engine.manifest["assets"]:
                    runs.append(await s.submit("nxt"))
                elif op == "tail" and "tail" in s.engine.manifest["assets"]:
                    tails.append(await s.submit("tail"))
            except Refused as error:
                assert error.status == 400 and "Unknown asset" in str(error), error
            if op == "cancel" and runs:
                await s.cancel(h.rng.choice(runs)["id"])
            if op == "proceed":
                await h.proceed()
            await s.settle(h.rng.choice([0.05, 0.2]))
        h.armed = False
        await s.settle(1.0)
        for run in runs:
            try:
                await s.wait(run, 30)
            except TimeoutError:  # a run never ends: say how every run went
                if os.environ.get("REDEPLOY_TRACE"):
                    for t in h.trace:
                        print("TRACE", t)
                tries = []
                for other in runs:
                    d = await s.get(f"/runs/{other['id']}")
                    tries.append([(a["outcome"], a.get("error")) for t in d["attempts"].values() for a in t])
                    tries[-1].insert(0, [(t["asset"], t["status"], t.get("held")) for t in d["tasks"]])
                raise AssertionError((h.fired, tries)) from None
        # No attempt fails by the engine's own error (a try spent on a deploy), but one whose asset a
        # deploy removed while it was claimed (ERW-27).
        crashed = []
        for run in runs:
            for tries in (await s.get(f"/runs/{run['id']}"))["attempts"].values():
                crashed += [a["error"] for a in tries if (a.get("error") or "").startswith("engine:")]
        removed = [e for e in crashed if (m := re.fullmatch(r"engine: '(\w+)'", e)) and m[1] in h.removed]
        assert len(removed) == len(crashed), (crashed, h.removed, h.fired)
        base = {"name": "copy", "axis": "keyed", "store": "default", "edge": True}
        h.variant = base
        s.engine.redeploy(h.project(base).manifest)
        await s.run("files")
        await s.run("copy", mode="full")
        await s.run("report")
        await s.run("nxt")
        await s.run("tail")
        for run in tails:  # held while copy was partial; each ends once it is whole
            await s.wait(run, 30)
        assert not h.proceeded, h.proceeded[:3]
        await s.settle(0.3)
        assert await s.read("report") == sorted(h.rows), (await s.read("report"), h.fired)
        assert sorted(await s.rows("nxt")) == sorted(h.rows), h.fired
        assert await s.read("tail") == sorted(h.rows), (await s.read("tail"), h.fired)
        bad = [h.read[t] for t in h.committed if t in h.read and not set(STABLE) <= set(h.read[t][0])]
        if bad and os.environ.get("REDEPLOY_TRACE"):
            last = None
            for t in h.trace:
                if t != last:
                    print("TRACE", t)
                last = t
        if os.environ.get("REDEPLOY_TRACE"):
            print("BADTOKS", [t for t in h.committed if t in h.read and not set(STABLE) <= set(h.read[t][0])])
        assert h.committed and not bad, bad[:3]
        if path := os.environ.get("REDEPLOY_FIRED"):
            with open(path, "a") as out:
                out.write(json.dumps(h.fired) + chr(10))
        await assert_replay(s)
    finally:
        await s.close()
