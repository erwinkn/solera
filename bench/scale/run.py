"""The scale harness's driver (README.md): one end-to-end run of the cast at
scale, through the real engine, its API and pool workers, on the tracking
path `$SOLERA_TRACKING` names.

    uv run --no-sync python bench/scale/run.py --keys 100000000 --days 30 --root /data/scale

Phases: `source` commits feed's keys (1M a commit); `items`, `tally`, `copy`
and `archive` each run in full, alone; then each day changes `--change` of
the keys (updates, plus a few removals and additions) in source commits of
`--batch` keys and runs the four assets as one run. Each phase records its
wall time, the engine's CPU, event loop and memory (serve.py), the workers'
calls (project.py), and with `--profile` where the engine's CPU went
(py-spy). `report.md` and `results.json` under the root hold the numbers."""

from __future__ import annotations

import argparse
import asyncio
import json
import mmap
import os
import random
import secrets
import shutil
import signal
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import httpx

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
ABSENT = 255
SOURCE_CHUNK = 1_000_000
VERIFY = 1_000_000  # up to this many keys, the check reads every value back
CAST = ("items", "tally", "copy", "archive")
KEYED = ("items", "copy", "archive")
INDEX = ("scale/keys", "scale/core")  # the change index: `today`'s, then the new core's
STEPS = ("preparing", "provisioning", "importing", "loading", "computing", "writing", "settling")
FULL_RUN_TARGET_S = 360  # build-plan.md: at N9, a full run over 100M keys costs the engine under six minutes

# Where the engine's CPU goes, by the first rule whose pattern a sampled stack
# holds (a py-spy frame reads "function (path:line)"). Journaling comes first:
# it is part of every commit.
BUCKETS = (
    ("journaling", ("journal.py", "(solera_server/state.py")),
    ("source commits", ("commit_source (", "_prepare_commit (", "tracking/sources.py")),
    ("building commits", ("commit_attempt (", "tracking/commit.py", "_install (")),
    ("planning", ("_prepare (", "tracking/prepare.py", "planning.py", "observing.py", "owed.py")),
    ("API", ("api.py", "starlette", "fastapi", "uvicorn", "pydantic", "h11")),
    ("upkeep", ("upkeep.py", "_cleanup", "merge")),
)


def key_of(i: int) -> str:
    return f"k{i:09d}"


def cpus(spec: str) -> list[str]:
    return ["taskset", "-c", spec] if spec else []


class Run:
    def __init__(self, args):
        self.args, self.root = args, Path(args.root).resolve()
        self.path = os.environ.get("SOLERA_TRACKING", "today")
        self.token = secrets.token_hex(16)
        self.url = f"http://127.0.0.1:{args.port}"
        self.env = {
            **os.environ,
            "SCALE_ROOT": str(self.root),
            "SCALE_BATCH": str(args.batch),
            "SOLERA_API_TOKEN": self.token,
            "SOLERA_PROJECT": str(HERE / "project.py"),
            "SOLERA_TRACKING": self.path,
            "SOLERA_CONCURRENCY": str(args.concurrency),
            "PYTHONPATH": os.pathsep.join(filter(None, [str(REPO), os.environ.get("PYTHONPATH")])),
        }
        self.processes: dict[str, subprocess.Popen] = {}
        self.phases: list[dict] = []
        self.rng = random.Random(args.seed)
        self.live = args.keys
        self.next_key = args.keys

    # -- processes -------------------------------------------------------------------

    def start(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)
        (self.root / "logs").mkdir(parents=True)
        capacity = self.args.keys + self.args.days * self.adds() + 1
        with open(self.root / "versions", "wb") as f:
            f.write(bytes(self.args.keys))
            f.write(bytes([ABSENT]) * (capacity - self.args.keys))
        with open(self.root / "versions", "r+b") as f:
            self.versions = mmap.mmap(f.fileno(), 0)
        state = (self.root / "state").as_uri()
        self.engine = self.spawn(
            "engine",
            [
                *cpus(self.args.engine_cpus),
                sys.executable,
                str(HERE / "serve.py"),
                state,
                self.env["SOLERA_PROJECT"],
                str(self.args.port),
            ],
        )
        for i in range(self.args.workers):
            self.spawn(
                f"worker-{i}",
                [
                    *cpus(self.args.worker_cpus),
                    sys.executable,
                    "-m",
                    "solera_worker",
                    "pool",
                    "--pool",
                    "scale",
                    "--server",
                    self.url,
                ],
            )

    def spawn(self, name: str, command: list[str]) -> subprocess.Popen:
        log = open(self.root / "logs" / f"{name}.log", "w")
        process = subprocess.Popen(
            command, env=self.env, stdout=log, stderr=subprocess.STDOUT, cwd=REPO, start_new_session=True
        )
        self.processes[name] = process
        return process

    def alive(self) -> None:
        """Raise if the engine or a worker exited: nothing would finish the work."""

        for name, process in self.processes.items():
            if process.poll() is not None:
                raise RuntimeError(
                    f"{name} exited ({process.returncode}): see {self.root / 'logs' / name}.log"
                )

    def stop(self) -> None:
        """End every process group it started, the leader's descendants too,
        whether the leader is still alive or not."""

        for process in reversed(self.processes.values()):
            _signal_group(process.pid, signal.SIGTERM)
        for process in self.processes.values():
            try:
                process.wait(30)
            except subprocess.TimeoutExpired:
                pass
            _signal_group(process.pid, signal.SIGKILL)

    # -- the API ---------------------------------------------------------------------

    async def call(self, method: str, path: str, **kw):
        response = await self.client.request(method, f"/api/projects/scale{path}", **kw)
        if response.status_code >= 400:
            raise RuntimeError(f"{method} {path}: {response.status_code} {response.text[:2000]}")
        return response.json() if response.content else None

    async def ready(self) -> None:
        self.client = httpx.AsyncClient(
            base_url=self.url, timeout=600, headers={"Authorization": f"Bearer {self.token}"}
        )
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            self.alive()
            try:
                if (await self.client.get("/healthz")).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.5)
        raise RuntimeError("the engine did not come up")

    async def commit(self, upsert: dict, remove: list) -> float:
        """A source commit of feed; the seconds the driver spent encoding it."""

        start = time.perf_counter()
        body = json.dumps({"upsert": upsert, "remove": remove, "by": "scale"})
        encoded = time.perf_counter() - start
        await self.call(
            "POST", "/sources/feed/commit", content=body, headers={"Content-Type": "application/json"}
        )
        return encoded

    async def run(self, targets: list[str]) -> dict:
        """Run `targets` to the end; its batches (its succeeded attempts, by
        asset) and the attempts' time by step (lifecycle.md).

        It polls the newest run of the listing, not the run's detail: the
        detail holds every attempt, and at 100M keys a run has 10,000."""

        run = await self.call("POST", "/runs", json={"targets": targets, "by": "scale"})
        while True:
            await asyncio.sleep(1.0)
            listing = await self.call("GET", "/runs", params={"limit": 1})
            rows = listing.get("runs", listing.get("items")) if isinstance(listing, dict) else listing
            status = next((r["status"] for r in rows if r["id"] == run["id"]), None)
            if status is None:  # not the newest: a run that took nothing ended at once
                status = (await self.call("GET", f"/runs/{run['id']}"))["request"]["status"]
            if status in ("succeeded", "failed", "canceled", "blocked"):
                break
            self.alive()
        detail = await self.call("GET", f"/runs/{run['id']}")
        if status != "succeeded":
            errors = [t.get("error") for t in detail.get("tasks", []) if t.get("error")]
            raise RuntimeError(f"run of {targets} ended {status}: {errors[:3]}")
        attempts = [a for task in detail["attempts"].values() for a in task]
        steps = {s: round(sum(a.get(s) or 0 for a in attempts), 1) for s in STEPS}
        batches = Counter({name: 0 for name in targets})
        for task in detail["tasks"]:
            batches[task["asset"]] += sum(a["outcome"] == "succeeded" for a in detail["attempts"][task["id"]])
        return {
            "batches_by_asset": dict(batches),
            "attempts": len(attempts),
            "attempt_wall_s": round(sum(a["finished_at"] - a["started_at"] for a in attempts), 1),
            "attempt_steps_s": steps,
            "attempt_cpu_s": round(sum(a.get("cpu_seconds") or 0 for a in attempts), 1),
        }

    async def heads(self) -> dict:
        out = {}
        for name in CAST:
            heads = (await self.call("GET", f"/outputs/{name}/heads"))["heads"]
            out[name] = heads[0] if heads else None
        return out

    # -- measuring -------------------------------------------------------------------

    async def phase(self, name: str, work) -> dict:
        stored = _state_bytes(self.root / "state")
        before = await self.snapshot()
        profile = self.profile(name) if self.args.profile else None
        try:
            result = await work()
        finally:
            if profile is not None:
                profile.send_signal(signal.SIGINT)
                profile.wait(120)
        after = await self.snapshot()
        measured = self.measure(name, before, after, result or {})
        grown = Counter(_state_bytes(self.root / "state"))
        grown.subtract(stored)
        measured["state_bytes_added"] = {k: v for k, v in grown.most_common() if v}
        written = measured.get("source_keys", 0) + sum(
            measured["workers"].get(f"store_write:{name}", {}).get(field, 0)
            for name in KEYED
            for field in ("keys", "removed")
        )
        measured["entries_committed"] = written
        if written:
            measured["index_bytes_per_entry"] = round(sum(grown[d] for d in INDEX) / written, 2)
            measured["engine_bytes_written_per_entry"] = round(measured["engine_write_bytes"] / written, 1)
        self.phases.append(measured)
        self.write(self.results())
        print(json.dumps({k: v for k, v in measured.items() if k != "workers"}), flush=True)
        return measured

    def profile(self, name: str) -> subprocess.Popen:
        out = self.root / "profiles" / f"{name}.txt"
        out.parent.mkdir(exist_ok=True)
        command = [
            "sudo",
            "-n",
            shutil.which("py-spy") or "py-spy",
            "record",
            "--pid",
            str(self.engine.pid),
            "--rate",
            "50",
            "--nonblocking",
            "--format",
            "raw",
            "--output",
            str(out),
        ]
        return subprocess.Popen(
            command, stdout=subprocess.DEVNULL, stderr=open(self.root / "logs" / f"py-spy-{name}.log", "w")
        )

    async def snapshot(self) -> dict:
        """The counters now, at a fresh line of the engine's (one a second)."""

        lines, deadline = self.engine_lines(), time.monotonic() + 60
        while len(self.engine_lines()) <= len(lines):
            self.alive()
            if time.monotonic() > deadline:
                raise RuntimeError("the engine wrote no sample line for 60 s")
            await asyncio.sleep(0.1)
        lines = self.engine_lines()
        with open("/proc/stat") as f:
            fields = [int(x) for x in f.readline().split()[1:]]
        return {
            "at": time.time(),
            "engine_lines": len(lines),
            "machine_busy": (sum(fields) - fields[3] - fields[4]) / os.sysconf("SC_CLK_TCK"),
            "engine_cpu": _process_cpu(self.engine.pid),
            "engine_write_bytes": _process_write_bytes(self.engine.pid),
            "heads": await self.heads(),
        }

    def engine_lines(self) -> list[str]:
        path = self.root / "engine.jsonl"
        return path.read_text().splitlines() if path.exists() else []

    def measure(self, name: str, before: dict, after: dict, extra: dict) -> dict:
        wall = after["at"] - before["at"]
        lines = [json.loads(x) for x in self.engine_lines()]
        # A line covers the second before it: the one before the phase is the
        # baseline of the cumulative counters, the phase's are those after it.
        first = lines[before["engine_lines"] - 1]
        window = lines[before["engine_lines"] : after["engine_lines"]]
        last = window[-1]
        busy, idle = last["busy"] - first["busy"], last["idle"] - first["idle"]
        engine_cpu = after["engine_cpu"] - before["engine_cpu"]
        commits = extra.pop("batches_by_asset", {})
        batches = sum(commits.values())
        workers = self.worker_stats(before["at"], after["at"])
        produced = [v for (what, asset), v in workers.items() if what == "produce" and asset in CAST]
        planned = sum(v.get("planned", 0) for v in produced)
        out = {
            "phase": name,
            "wall_s": round(wall, 1),
            "engine_cpu_s": round(engine_cpu, 1),
            "engine_cores": round(engine_cpu / wall, 2) if wall else None,
            "engine_write_bytes": after["engine_write_bytes"] - before["engine_write_bytes"],
            "machine_cpu_s": round(after["machine_busy"] - before["machine_busy"], 1),
            "loop_busy": round(busy / (busy + idle), 3) if busy + idle else None,
            "loop_longest_ms": round(1000 * max(x["longest"] for x in window), 1),
            "engine_rss_peak_mb": round(max(x["rss"] for x in window) / 2**20),
            "engine_rss_peak_so_far_mb": round(last["peak_rss"] / 2**20),
            "batches": batches,
            "batches_by_asset": commits,
            "keys_planned": planned,
            "rows_produced": sum(v.get("keys", 0) for v in produced),
            "batches_per_s": round(batches / wall, 2) if wall else None,
            "engine_cpu_ms_per_batch": round(1000 * engine_cpu / batches, 1) if batches else None,
            "keys_per_s": round(planned / wall) if wall else None,
            "key_counts": {n: (after["heads"][n] or {}).get("key_count") for n in KEYED},
            "workers": {f"{what}:{asset}": v for (what, asset), v in sorted(workers.items())},
            **extra,
        }
        breakdown = self.breakdown(name, engine_cpu)
        if breakdown:
            out["engine_cpu_by_part_s"] = breakdown
            if planning := breakdown.get("planning"):
                out["keys_per_planning_cpu_s"] = round(planned / planning)
            if batches:
                out["commit_cpu_ms_per_batch"] = round(
                    1000 * breakdown.get("building commits", 0) / batches, 1
                )
        return out

    def worker_stats(self, lo: float, hi: float) -> dict:
        totals: dict = defaultdict(Counter)
        for path in (self.root / "stats").glob("*.jsonl"):
            for line in path.read_text().splitlines():
                row = json.loads(line)
                if lo <= row["at"] <= hi:
                    t = totals[(row["what"], row.get("asset") or row.get("output"))]
                    t["calls"] += 1
                    t["wall_s"] += row["wall"]
                    t["cpu_s"] += row["cpu"]
                    for field in ("keys", "planned", "removed", "packs"):
                        t[field] += row.get(field, 0)
        return {k: {f: round(v, 2) for f, v in t.items()} for k, t in totals.items()}

    def breakdown(self, name: str, engine_cpu: float) -> dict | None:
        """The phase's engine CPU split by `BUCKETS`, in proportion to py-spy's samples."""

        path = self.root / "profiles" / f"{name}.txt"
        if not path.exists():
            return None
        samples: Counter = Counter()
        for line in path.read_text().splitlines():
            stack, _, count = line.rpartition(" ")
            part = next((b for b, patterns in BUCKETS if any(p in stack for p in patterns)), "other")
            samples[part] += int(count)
        total = sum(samples.values())
        return (
            {part: round(engine_cpu * n / total, 1) for part, n in samples.most_common()} if total else None
        )

    # -- the run ---------------------------------------------------------------------

    def adds(self) -> int:
        """Keys a day adds, and removes: the live count stays `--keys`."""

        return int(self.args.keys * self.args.change * self.args.adds)

    async def source(self) -> dict:
        encoded = 0.0
        for lo in range(0, self.args.keys, SOURCE_CHUNK):
            hi = min(self.args.keys, lo + SOURCE_CHUNK)
            encoded += await self.commit({key_of(i): "0" for i in range(lo, hi)}, [])
        return {"source_keys": self.args.keys, "driver_encode_s": round(encoded, 1)}

    async def day(self, day: int) -> dict:
        """About `--change` of the keys change: `--adds` of them are new keys,
        as many live ones are removed, the rest are updated."""

        changes = int(self.args.keys * self.args.change)
        adds = removes = self.adds()
        chosen: set[int] = set()
        while len(chosen) < changes - adds:
            i = self.rng.randrange(self.next_key)
            if self.versions[i] != ABSENT:
                chosen.add(i)
        chosen_list = sorted(chosen)
        self.rng.shuffle(chosen_list)
        removed = set(chosen_list[:removes])
        new = list(range(self.next_key, self.next_key + adds))
        self.next_key += adds
        self.live += adds - removes
        work = chosen_list + new
        self.rng.shuffle(work)
        encoded, start = 0.0, time.perf_counter()
        for lo in range(0, len(work), self.args.batch):
            upsert, remove = {}, []
            for i in work[lo : lo + self.args.batch]:
                if i in removed:
                    self.versions[i] = ABSENT
                    remove.append(key_of(i))
                else:
                    self.versions[i] = day
                    upsert[key_of(i)] = str(day)
            encoded += await self.commit(upsert, remove)
        committed = time.perf_counter() - start
        ran = await self.run(list(CAST))
        return {
            "changes": len(work),
            "source_keys": len(work),
            "source_commits_s": round(committed, 1),
            "driver_encode_s": round(encoded, 1),
            **ran,
        }

    async def check(self) -> dict:
        """Every keyed output holds exactly the live keys, and tally's every
        group counts them: each output's keys streamed against the driver's
        model (the versions file), in key order. Up to VERIFY keys, every
        key's value is also the day it last changed."""

        from solera.sdk import Ref
        from solera.stores import Keys

        sys.path.insert(0, str(HERE))
        os.environ["SCALE_ROOT"] = str(self.root)
        from project import GROUPS, project

        heads = await self.heads()
        days = self.versions[:]
        live = len(days) - days.count(ABSENT)
        groups = [0] * GROUPS
        for i, day in enumerate(days):
            if day != ABSENT:
                groups[i % GROUPS] += 1
        ref = Ref.from_json(heads["tally"]["ref"])
        tally = list(await project.stores[ref.store].load(ref, object, None))
        out = {"live": live, "key_counts": {n: heads[n]["key_count"] for n in KEYED}, "tally": tally}
        out["keys_wrong"], wrong = {}, {}
        for name in KEYED:
            expected = (i for i, day in enumerate(days) if day != ABSENT)
            want, missing, extra, generations = next(expected, None), 0, 0, {}
            async for page in self.keys(name):
                if live <= VERIFY:
                    generations.update(page)
                for key in page:
                    got = int(key[1:])
                    while want is not None and want < got:
                        missing, want = missing + 1, next(expected, None)
                    if want == got and key == key_of(got):
                        want = next(expected, None)
                    else:
                        extra += 1
            missing += (want is not None) + sum(1 for _ in expected)
            out["keys_wrong"][name] = missing + extra
            if live <= VERIFY:
                ref = Ref.from_json(heads[name]["ref"])
                rows = await project.stores[ref.store].load(ref, list, Keys(generations))
                wrong[name] = sum(r["v"] != days[int(r["id"][1:])] for r in rows) + abs(
                    len(rows) - len(generations)
                )
        if live <= VERIFY:
            out["values_wrong"] = wrong
        ok = (
            live == self.live
            and all(c == live for c in out["key_counts"].values())
            and tally == groups
            and not any(out["keys_wrong"].values())
            and not any(wrong.values())
        )
        return {**out, "ok": ok}

    async def keys(self, name: str):
        """An output's keys and generations, a page at a time, in key order."""

        after = None
        while True:
            params = {"limit": 100_000, **({"after": after} if after else {})}
            page = await self.call("GET", f"/outputs/{name}/keys", params=params)
            yield page["keys"]
            if (after := page["next"]) is None:
                return

    def results(self) -> dict:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True
        ).stdout.strip()
        machine = {
            "cpus": os.cpu_count(),
            "memory_gb": round(os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") / 2**30),
        }
        return {
            "path": self.path,
            "commit": head,
            "machine": machine,
            "args": vars(self.args),
            "phases": self.phases,
        }

    def write(self, results: dict) -> None:
        (self.root / "results.json").write_text(json.dumps(results, indent=1))
        (self.root / "report.md").write_text(report(results))

    async def main(self) -> None:
        try:
            self.start()
            await self.ready()
            await self.phase("source", self.source)
            for name in CAST:
                await self.phase(name, lambda name=name: self.run([name]))
            for day in range(1, self.args.days + 1):
                await self.phase(f"day {day}", lambda day=day: self.day(day))
                if self.live <= VERIFY and day < self.args.days and not (checked := await self.check())["ok"]:
                    raise SystemExit(f"day {day}: the outputs do not hold the source's keys: {checked}")
            checked = await self.check()
            print(json.dumps(checked), flush=True)
            self.write({**self.results(), "check": checked, "state_bytes": _state_bytes(self.root / "state")})
            if not checked["ok"]:
                raise SystemExit(f"the outputs do not hold the source's keys: {checked}")
        finally:
            await self.client.aclose() if hasattr(self, "client") else None
            self.stop()


def report(results: dict) -> str:
    """results.json as a page: the numbers build-plan.md names, then each phase."""

    phases = results["phases"]
    full = [p for p in phases if p["phase"] in KEYED]
    days = [p for p in phases if p["phase"].startswith("day ")]
    a = results["args"]
    out = [
        f"# Scale run: {a['keys']:,} keys, {a['days']} days, `{results['path']}` path",
        "",
        f"Commit {results['commit'][:10]}; {results['machine']['cpus']} CPUs, {results['machine']['memory_gb']} GB; "
        f"engine on CPUs `{a['engine_cpus'] or 'all'}`, {a['workers']} workers, concurrency {a['concurrency']}, "
        f"batches of {a['batch']:,}, {a['change']:.0%} of the keys changing a day.",
        "",
    ]
    if "check" in results:
        out += [
            f"Check: {'every output holds exactly the live keys, and tally counts each group' if results['check']['ok'] else 'FAILED'} "
            f"({results['check']['live']:,} live keys).",
            "",
        ]

    def worst(rows, field, f=max):
        values = [r[field] for r in rows if r.get(field) is not None]
        return f(values) if values else None

    def ratio(rows, top, bottom):
        b = sum(r.get(bottom) or 0 for r in rows)
        return round(sum(r.get(top) or 0 for r in rows) / b, 2) if b else None

    index_days = sum(sum(v for k, v in r["state_bytes_added"].items() if k in INDEX) for r in days)
    entries_days = sum(r["entries_committed"] for r in days)
    rows = [
        (
            "N1 index bytes per entry committed, daily runs",
            round(index_days / entries_days, 2) if entries_days else None,
            "≤ 13",
        ),
        (
            "N1 engine bytes written per entry committed, daily runs (rewrites included)",
            ratio(days, "engine_write_bytes", "entries_committed"),
            "—",
        ),
        (
            "N3 keys planned per engine CPU second spent planning (py-spy), worst full run",
            worst(full, "keys_per_planning_cpu_s", min),
            "≥ 500,000",
        ),
        (
            "Proxy for N3: keys planned per second of the whole run's wall time, worst full run",
            worst(full, "keys_per_s", min),
            "— (loading, writing and polling included)",
        ),
        ("N9 batches per second end to end, worst full run", worst(full, "batches_per_s", min), "≥ 30"),
        ("N9 event loop busy, worst phase", worst(phases, "loop_busy"), "≤ 0.5"),
        ("N9 longest event-loop stall (ms), worst phase", worst(phases, "loop_longest_ms"), "≤ 50"),
        (
            "Engine memory peak (MB), this four-asset cast",
            worst(phases, "engine_rss_peak_so_far_mb"),
            "— (N12's ≤ 1,536 is for 50 tasks, not run here)",
        ),
        *(
            (f"Engine CPU of the full run of {p['phase']} (s)", p["engine_cpu_s"], f"< {FULL_RUN_TARGET_S}")
            for p in full
        ),
        (
            "N7 engine CPU building commits per batch (py-spy), full runs, keys in one range (ms)",
            worst(full, "commit_cpu_ms_per_batch"),
            "≤ 50",
        ),
        (
            "Engine CPU building commits per batch (py-spy), daily runs, keys scattered (ms)",
            worst(days, "commit_cpu_ms_per_batch"),
            "—",
        ),
        ("Proxy: whole engine CPU per batch, full runs (ms)", worst(full, "engine_cpu_ms_per_batch"), "—"),
        ("Proxy: whole engine CPU per batch, daily runs (ms)", worst(days, "engine_cpu_ms_per_batch"), "—"),
    ]
    out += ["| Number | Measured | Threshold |", "|---|---|---|"]
    out += [f"| {name} | {'—' if v is None else f'{v:,}'} | {t} |" for name, v, t in rows]
    columns = (
        "wall_s",
        "batches",
        "batches_per_s",
        "keys_per_s",
        "keys_per_planning_cpu_s",
        "engine_cpu_s",
        "engine_cpu_ms_per_batch",
        "commit_cpu_ms_per_batch",
        "loop_busy",
        "loop_longest_ms",
        "engine_rss_peak_mb",
        "index_bytes_per_entry",
    )
    out += ["", "## Phases", "", "| Phase | " + " | ".join(columns) + " |", "|---" * (len(columns) + 1) + "|"]
    out += [f"| {p['phase']} | " + " | ".join(str(p.get(c, "—")) for c in columns) + " |" for p in phases]
    out += [
        "",
        "## Where an attempt's time goes (seconds, summed over attempts)",
        "",
        "| Phase | attempts | " + " | ".join(STEPS) + " |",
        "|---" * (len(STEPS) + 2) + "|",
    ]
    out += [
        f"| {p['phase']} | {p['attempts']} | "
        + " | ".join(str(p["attempt_steps_s"][s]) for s in STEPS)
        + " |"
        for p in phases
        if "attempts" in p
    ]
    out += ["", "## Worker calls (seconds)", ""]
    for p in phases:
        calls = ", ".join(
            f"{k} {v['wall_s']} s over {int(v['keys']):,} keys" for k, v in p["workers"].items()
        )
        out.append(f"- {p['phase']}: {calls or 'none'}")
    parts = [p for p in phases if "engine_cpu_by_part_s" in p]
    if parts:
        out += ["", "## Engine CPU by part (seconds, from py-spy samples)", ""]
        out += [
            f"- {p['phase']}: " + ", ".join(f"{k} {v}" for k, v in p["engine_cpu_by_part_s"].items())
            for p in parts
        ]
    return "\n".join(out) + "\n"


def _process_cpu(pid: int) -> float:
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rpartition(")")[2].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def _process_write_bytes(pid: int) -> int:
    with open(f"/proc/{pid}/io") as f:
        return next(int(line.split()[1]) for line in f if line.startswith("write_bytes:"))


def _signal_group(pid: int, sig: int) -> None:
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def _state_bytes(state: Path) -> dict:
    """Bytes under the engine's state, by its top two directories."""

    sizes: Counter = Counter()
    for path in state.rglob("*"):
        if path.is_file():
            sizes["/".join(path.relative_to(state).parts[:2])] += path.stat().st_size
    return dict(sizes)


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--keys", type=int, default=1_000_000)
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--change", type=float, default=0.01, help="share of the keys a day changes")
    parser.add_argument(
        "--adds", type=float, default=0.01, help="share of a day's changes that add keys (and remove)"
    )
    parser.add_argument(
        "--batch", type=int, default=10_000, help="keys per batch and per daily source commit"
    )
    parser.add_argument("--workers", type=int, default=8, help="pool worker processes")
    parser.add_argument("--concurrency", type=int, default=8, help="the engine's attempts at once")
    parser.add_argument("--engine-cpus", default="0-3", help="taskset CPU list for the engine ('' for none)")
    parser.add_argument("--worker-cpus", default="", help="taskset CPU list for the workers ('' for none)")
    parser.add_argument("--root", default=".solera/scale")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--profile", action="store_true", help="sample the engine with py-spy (needs sudo)")
    args = parser.parse_args()
    if args.days > 254:
        parser.error("--days: at most 254 (a key's day is one byte)")
    if not 0 <= args.change <= 0.5:
        parser.error("--change: from 0 to 0.5 (a day draws its changed keys from the live ones)")
    if not 0 <= args.adds <= 0.5:
        parser.error("--adds: from 0 to 0.5 (a day removes as many keys as it adds, from those it changes)")
    if args.keys >= 10**9:
        parser.error("--keys: under 1e9 (a key is k and nine digits)")
    return args


if __name__ == "__main__":
    asyncio.run(Run(parse()).main())
