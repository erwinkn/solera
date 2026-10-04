"""Trace validation: check that simulation runs are behaviours of a spec
(docs/verification.md, "Trace validation").

    uv run python spec/tla/check-trace.py SPEC              run the simulation (SOLERA_SIM_EXAMPLES, 20) and check each run
    uv run python spec/tla/check-trace.py SPEC RUN.jsonl…   check runs exported before (SOLERA_SIM_REQUESTS=DIR)

SPEC is one of `SPECS` below. The simulation exports
each run's object requests, tagged by actor (`tests/sim/core.py`,
`Objects.trace`). The requests on the objects a spec describes become a
TLA+ module, `{Spec}TraceLog`, holding `Trace`, a sequence of

    [e |-> actor, req |-> "list" | "get" | "readback" | "create" | "delete",
     obj |-> the object's kind, n |-> its name, out |-> how it ended,
     listed |-> what a list returned]

and TLC searches the spec's trace module (`{Spec}Trace.tla`) for a
behaviour that makes them, one by one. It reports the run valid, or the
first request no behaviour explains, with the requests of that actor
before it: either the code does what the design does not allow, or the
spec misses a step. A spec's trace module defines `NotDone` (violated
once every request is explained) and prints `<<"explained", i>>` as it
gets further.

Actors are `e{n}` for the simulation's engine n and `r{k}` for its k-th
read-only open. TLC_HEAP (4g) bounds each TLC run; one worker (the report
needs it). TRACE_TIMEOUT (seconds) stops a trace's search, reported as
stopped, neither valid nor not.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
JAR = HERE / ".tools" / "tla2tools-1.7.4.jar"


@dataclass
class Spec:
    module: str  # the spec; its trace module is f"{module}Trace"
    # (request, actor, names) -> (kind, name) of the object a request is on,
    # if the spec describes it, else None; `names` is the run's own memory,
    # for names the spec gives otherwise.
    parse: Callable[[dict, str, dict], tuple | None] | None
    config: str  # the trace module's TLC config
    # A run's traces, if not one of `parse`'s requests: (label, requests, constants).
    traces: Callable[[Path], list[tuple[str, list[dict], dict]]] | None = None


CHECKPOINT = re.compile(r"/control/checkpoints/([0-9a-f]+-\d+)\.json$")


def journal_object(request: dict, actor: str, names: dict) -> tuple | None:
    """`control/journal.json`, and `control/checkpoints/{engine id}-{k}.json`
    as the spec names it, <<engine, k>>: the engine id is new on every open,
    so it is mapped to the engine that first creates a checkpoint under it.
    A write of the journal is named by the checkpoint its body names: a move
    names a new one, an append the one it extends."""

    def checkpoint(name: str) -> tuple:
        engine_id, k = name.rsplit("-", 1)
        return names.setdefault(engine_id, actor), int(k)

    path = request["path"]
    if path.endswith("/control/journal.json"):
        named = request.get("names")
        return "journal", checkpoint(named) if named else ()
    if path.endswith("/control/checkpoints/"):
        return "checkpoints", ()
    m = CHECKPOINT.search(path)
    return ("checkpoints", checkpoint(m.group(1))) if m else None


ATTEMPT = re.compile(r"/runs/[^/]+/([^/.]+)\.(control|spec)$")


def attempt_traces(path: Path) -> list[tuple[str, list[dict], dict]]:
    """A run's control-file requests, one trace per partition (Attempt.tla
    models one): attempts numbered in the order their files were created,
    workers by copy, and an engine older than the newest one that has
    acted as the zombie."""

    rows = [json.loads(line) for line in path.read_text().splitlines()]
    rows = [r for r in rows if "kind" in r and ATTEMPT.search(r["path"])]
    partition = {
        ATTEMPT.search(r["path"]).group(1): tuple(r["partition"]) for r in rows if r.get("partition")
    }
    by: dict[tuple, list[dict]] = {}
    number: dict[str, int] = {}
    newest = -1
    for r in rows:
        attempt, obj = ATTEMPT.search(r["path"]).groups()
        if attempt not in partition:
            continue
        who = r["who"] or ["engine", newest]
        if who[0] == "engine":
            newest = max(newest, who[1])
        events = by.setdefault(partition[attempt], [])
        if obj == "control" and r["kind"] == "create" and attempt not in number:
            number[attempt] = 1 + sum(1 for a in number if partition[a] == partition[attempt])
        if attempt not in number:
            continue  # its spec, before the file: the engine's own write
        role = "worker" if who[0] == "worker" else "zombie" if who[1] < newest else "engine"
        events.append(
            {
                "e": role,
                "i": number[attempt],
                "c": who[2] if who[0] == "worker" else 0,
                "obj": obj,
                "req": r["kind"],
                "out": r["outcome"],
                "st": r.get("state") or "",
            }
        )
    out = []
    for (asset, part), events in by.items():
        n = max(ev["i"] for ev in events)
        copies = sorted({ev["c"] for ev in events if ev["c"]} | {1})
        constants = {"TraceN": n, "TraceCopies": copies, "TraceRestarts": newest + 2}
        out.append((f"{path.stem}-{asset}{('-' + part) if part else ''}", events, constants))
    return out


KX = re.compile(r"/(keys/.+/)([^/]+)\.kx$")


def spans_traces(path: Path) -> list[tuple[str, list[dict], dict]]:
    """A run's key index lifecycles, one trace per index (Spans.tla models
    one). The journal's durable events, replayed by the engine's own
    `Model`, give each index's commits, publications and resets, and the
    claims that read it: each placed at its pin, the event counter it was
    claimed at (`Model.claim`), as its reads were endpoints from then on.
    The object requests give the merges' uploads, the deletions, by whom,
    and the reads. Spans are numbered as Spans.tla's files are, in the
    order they appear; commits are the code's plus one (the spec's first
    is 1, the code's 0). Takeovers are the fences: an engine's first write
    of the journal. What the spec does not hold is left out: an attempt's
    delta before its commit, a zombie's merges (never published)."""

    from solera_server.model import Model

    rows = [json.loads(line) for line in path.read_text().splitlines()]
    model = Model()
    out: dict[tuple, list[tuple]] = {}  # index -> [(order, record)]
    seq = 0

    def emit(key, record, order=None, during=False):
        """Records sort by the event counter they follow: an event's own, then
        the claims made right after it, then the requests until the next."""

        nonlocal seq
        seq += 1
        out.setdefault(key, []).append((order or (model.event_counter, 0 if during else 2, seq), record))

    blank = {
        "act": "",
        "e": "serving",
        "c": 0,
        "land": 0,
        "id": 0,
        "ids": [],
        "b": 0,
        "bounds": [],
        "ok": True,
        "k": 0,
        "pass": False,
        "empty": False,
        "task": "",
    }

    def rec(**kw):
        return blank | kw

    files: dict[tuple, dict[str, int]] = {}  # index -> file name -> span id
    nfile: dict[tuple, int] = {}
    lives: dict[tuple, str] = {}
    gone: dict[tuple, set] = {}  # spans deleted
    jobs: dict[tuple, dict[str, dict]] = {}  # index -> output stem -> {id, by, open}
    consumers: dict[tuple, dict] = {}
    prefixes: dict[str, tuple] = {}
    takeovers, serving, fenced = 0, None, set()
    claimed: dict[str, list] = {}  # task id -> [(index, c)]
    heads_at: dict[tuple, dict[int, int]] = {}  # index -> its head -> the event that made it

    def span_name(s, life) -> str:
        """A span's first file names it; one with no files, its life and start."""

        return s.files[0].name if s.files else f"empty-{life}-{s.a}"

    def span_id(key, names) -> int:
        nfile[key] = nfile.get(key, 0) + 1
        for name in names:
            files.setdefault(key, {})[name] = nfile[key]
        return nfile[key]

    def request(row):
        """One object request, once the engine that made it had applied
        what it had (its `counter`): the state it acted on."""

        nonlocal takeovers, serving
        who = row["who"] or []
        where = row["path"]
        if who[:1] == ["engine"] and where.endswith("/control/journal.json"):
            if row["kind"] in ("create", "swap", "put") and row["outcome"] == "ok" and who[1] not in fenced:
                fenced.add(who[1])
                if serving is not None and who[1] > serving:
                    takeovers += 1
                    for key in set(files) | set(lives):
                        emit(key, rec(act="takeover"))
                serving = who[1] if serving is None else max(serving, who[1])
            return
        if (
            who[:1] == ["engine"]
            and row["kind"] == "list"
            and where.endswith("/keys/")
            and row["outcome"] == "ok"
        ):
            role = "zombie" if serving is not None and who[1] < serving else "serving"
            for key in set(files) | set(lives):  # the orphan collector's listing
                emit(key, rec(act="list", e=role))
            return
        m = KX.search(where)
        if not m or m.group(1) not in prefixes:
            return
        key, name = prefixes[m.group(1)], m.group(2)
        engine = who[1] if who[:1] == ["engine"] else None
        role = "zombie" if engine is not None and serving is not None and engine < serving else "serving"
        if row["kind"] == "create" and name.startswith("m") and engine is not None and row["outcome"] == "ok":
            stem = name.rsplit(".", 1)[0]
            if role == "zombie" or stem in jobs.get(key, {}):
                return
            a, b = int(name[1:13]) + 1, int(name[14:26]) + 1
            index = model.indexes.get(key)
            ids = [
                files[key].get(s.files[0].name if s.files else f"empty-{index.life}-{s.a}")
                for s in (index.spans if index else ())
                if a <= s.a + 1 and s.b + 1 <= b
            ]
            if not ids and os.environ.get("DEBUG_SPANS"):
                print(
                    "upload over nothing",
                    key,
                    name,
                    row.get("counter"),
                    model.event_counter,
                    index and [(s.a, s.b) for s in index.spans],
                )
            job = jobs.setdefault(key, {})[stem] = {"id": span_id(key, []), "by": engine, "open": True}
            job["record"] = rec(act="upload", id=job["id"], ids=ids, bounds=None)
            emit(key, job["record"])
            return
        id = files.get(key, {}).get(name.rsplit(".", 1)[0] if name.startswith("m") else name)
        if id is None:
            stem = name.rsplit(".", 1)[0]
            job = jobs.get(key, {}).get(stem)
            id = job["id"] if job else None
        if id is None:
            return
        if row["kind"] == "delete" and row["outcome"] == "ok":
            if id in gone.setdefault(key, set()):
                return
            gone[key].add(id)
            job = next((j for j in jobs.get(key, {}).values() if j["id"] == id), None)
            if job is not None and job["open"] and job.get("by") == engine and role == "serving":
                job["open"] = False
                job["refused"] = True
                emit(key, rec(act="refuse", id=id))
            else:
                emit(key, rec(act="delete", e=role if engine is not None else "worker", id=id))
        elif row["kind"] in ("get", "range") and row["outcome"] in ("ok", "missing"):  # not an injected fault
            emit(key, rec(act="read", id=id, ok=row["outcome"] == "ok"))

    pending: list[dict] = []
    for row in rows:
        if "event" in row:
            e = row["event"]
            before = {k: v for k, v in model.indexes.items()}
            claims_before = {t: c for t, c in model.claims.items()}
            model.apply(e)
            for key in set(before) | set(model.indexes):
                old, new = before.get(key), model.indexes.get(key)
                if new is not None:
                    prefixes[new.prefix] = key
                if old is new:
                    continue
                if old is not None and (new is None or new.life != old.life):
                    emit(key, rec(act="reset"), during=True)
                    heads_at.pop(key, None)
                    lives.pop(key, None)
                if new is None:
                    continue
                lives[key] = new.life
                kept = old.spans if old is not None and old.life == new.life else ()
                if len(new.spans) > len(kept) and new.spans[: len(kept)] == kept:
                    for s in new.spans[len(kept) :]:  # commits
                        names = [f.name for f in s.files] or [f"empty-{new.life}-{s.a}"]
                        if s.files and s.files[0].name in files.get(key, {}):
                            continue
                        emit(
                            key,
                            rec(act="commit", id=span_id(key, names), b=s.b + 1, empty=not s.files),
                            during=True,
                        )
                        heads_at.setdefault(key, {})[s.b] = model.event_counter
                elif kept:  # a merge published
                    [s] = [s for s in new.spans if s not in kept]
                    stem = s.files[0].name.rsplit(".", 1)[0] if s.files else None
                    job = jobs.get(key, {}).get(stem)
                    if job is None:  # an output with no files: never uploaded
                        life = new.life
                        ids = [files[key][span_name(x, life)] for x in kept if x not in new.spans]
                        job = {"id": span_id(key, [span_name(s, life)]), "open": True}
                        bounds = sorted({x + 1 for x, _ in s.starts} - {s.a + 1})
                        emit(key, rec(act="upload", id=job["id"], ids=ids, bounds=bounds), during=True)
                    job["open"] = False
                    job["published"] = s
                    for f in s.files:
                        files[key][f.name] = job["id"]
                    emit(key, rec(act="publish", id=job["id"]), during=True)
                    emit(key, rec(act="flush"), during=True)  # durable: it is the journal's
            for task, claim in model.claims.items():
                if task in claims_before or not claim.get("launched"):
                    continue
                launched = model.task(task)["launched"]
                pin = int(claim["generation"])
                for name, plan in (launched["prepared"].get("plans") or {}).items():
                    if not plan or plan["kind"] not in ("keys", "selection") or plan.get("head") is None:
                        continue
                    position = plan["position"]
                    key = (position["output"], position["upstream_partition"])
                    who = consumers.setdefault(key, {})
                    c = who.setdefault(f"{task.split('/', 1)[1]}/{name}", len(who) + 1)
                    claimed.setdefault(task, []).append((key, c, name, int(plan["head"]) + 1))
                    ispass = plan.get("pass") is not None
                    # Claimed at its pin, prepared (its reads, its manifest) once its
                    # plan's head was: later, if a commit came between. The spec
                    # does both at once, here at the later: a later pin is laxer.
                    at = (max(pin, heads_at.get(key, {}).get(int(plan["head"]), pin)), 1, seq)
                    emit(
                        key,
                        rec(
                            act="claim",
                            c=c,
                            land=int(plan["head"]) + 2,
                            task=f"{task}/{name}",
                            **{"pass": ispass},
                        ),
                        order=at,
                    )
            for task in set(claims_before) - set(model.claims):
                asset, _, partition = task.split("/", 1)[1].partition(":")
                done = e["type"] == "AttemptFinished" and e.get("outcome") == "succeeded"
                for key, c, name, end in claimed.pop(task, []):
                    position = model.position(asset, name, partition) or {}
                    moved = position.get("next") is not None and int(position["next"]) == end
                    if done and position.get("pass") is not None:  # a batch: the pass goes on
                        emit(key, rec(act="batch", c=c, task=f"{task}/{name}"), during=True)
                        continue
                    emit(key, rec(act="settle" if moved else "fail", c=c, task=f"{task}/{name}"), during=True)
            ready = [r for r in pending if r["counter"] <= model.event_counter]
            pending = [r for r in pending if r["counter"] > model.event_counter]
            for r in ready:
                request(r)
            continue
        if "kind" not in row:
            continue
        who = row["who"] or []
        zombie = who[:1] == ["engine"] and serving is not None and who[1] < serving
        if row.get("counter", 0) > model.event_counter and not zombie:  # a zombie's never land
            pending.append(row)
        else:
            request(row)
    for row in pending:
        request(row)
    traces = []
    for key, records in out.items():
        events, numbers = [], {}
        for _, r in sorted(records, key=lambda x: x[0]):
            job = next((j for j in jobs.get(key, {}).values() if j.get("record") is r), {})
            if r["act"] == "claim":  # claims are numbered in the spec's order
                numbers[r["task"]] = r["k"] = sum(x["act"] == "claim" for x in events) + 1
            elif r["act"] in ("settle", "batch", "fail"):
                r["k"] = numbers.get(r["task"], 0)
            r = {f: v for f, v in r.items() if f != "task"}
            if r["act"] == "upload" and r["bounds"] is None:
                s = job.get("published")
                r = r | {
                    "bounds": sorted({x + 1 for x, _ in s.starts} - {s.a + 1}) if s else [],
                    "any": s is None,
                }
            events.append(r | {"any": r.get("any", False)})
            if events[-1]["any"] and not job.get("refused"):
                crash = {f: v for f, v in rec(act="crash", id=r["id"], any=False).items() if f != "task"}
                events.append(crash)  # its merge failed or its engine stopped
        constants = {
            "TraceConsumers": sorted(set(consumers.get(key, {}).values())) or [1],
            "TraceMaxCommits": max([r["b"] for r in events] + [1]),
            "TraceFiles": nfile.get(key, 0),
            "TraceResets": sum(r["act"] == "reset" for r in events),
            "TraceTakeovers": takeovers,
            "TraceClaims": sum(r["act"] == "claim" for r in events),
        }
        label = f"{path.stem}-{key[0]}{('-' + key[1]) if key[1] else ''}".replace("@", "failed-")
        traces.append((label, events, constants))
    return traces


# The specs checked against simulation runs. The first, the segment journal
# (Journal.tla), retired with its code (docs/verification.md).
SPECS = {
    "attempt": Spec(
        "Attempt",
        None,
        """\
INIT TInit
NEXT TNext
CONSTANTS
    N <- TraceN
    Copies <- TraceCopies
    MaxRestarts <- TraceRestarts
    Zombie = TRUE
    LostAnswers = TRUE
    Missing = Missing
    Unread = Unread
    PreCreate = TRUE
    TakeWriting = TRUE
    EngineSwaps = TRUE
    Classify = TRUE
    OfferDurable = TRUE
INVARIANT NotDone TypeOK NoWriteAfterNone NoOrphanWrite CompleteLanded WritesInOrder
CHECK_DEADLOCK FALSE
""",
        attempt_traces,
    ),
    "spans": Spec(
        "Spans",
        None,
        """\
INIT TInit
NEXT TNext
CONSTANTS
    Consumers <- TraceConsumers
    MaxCommits <- TraceMaxCommits
    MaxFiles <- TraceFiles
    MaxResets <- TraceResets
    MaxTakeovers <- TraceTakeovers
    MaxClaims <- TraceClaims
    MaxJobs = 2
    R = 3
    MergeFailures = FALSE
    EmptySpans = TRUE
    Passes = TRUE
    Collectors = TRUE
    FixLanding = TRUE
    FixBounds = TRUE
    FixLanes = TRUE
    FixInputs = TRUE
    FixLife = TRUE
    FixSettleLife = TRUE
    FixPinFloor = TRUE
    FixDurable = TRUE
    FixRetries = TRUE
    FixEpoch = TRUE
    FixJudgeAfter = TRUE
    FixGarbageNamed = TRUE
INVARIANT NotDone Tiling ReadsExact StateStored ReadersStored PublishingStored AttemptsBounded
CHECK_DEADLOCK FALSE
""",
        spans_traces,
    ),
    "journal": Spec(
        "JournalObject",
        journal_object,
        """\
INIT TInit
NEXT TNext
CONSTANTS
    Engines <- TraceActors
    MaxWrites <- TraceMaxN
    None = None
    Nobody = Nobody
    Overlap = TRUE
    LostAnswers = TRUE
    Conflicts = TRUE
    Unreadable = FALSE
    Failures = TRUE
    EngineId = TRUE
    AskJournal = TRUE
    ReGet = TRUE
    Verify = TRUE
    ListFirst = TRUE
INVARIANT NotDone TypeOK NoAckedLoss OneWriter FencedSeesAcked StatesArePrefixes JournalResolves OpensNeverFail
CHECK_DEADLOCK FALSE
""",
    ),
}


def requests(spec: Spec, path: Path) -> list[dict]:
    """The requests of one exported run on the objects `spec` describes."""

    out: list[dict] = []
    readers = 0
    names: dict = {}
    last: dict[str, dict] = {}  # each actor's previous request
    for line in path.read_text().splitlines():
        r = json.loads(line)
        if r.get("reader"):
            readers += 1
            continue
        if "kind" not in r:
            continue
        who = r["who"]
        e = f"e{who[1]}" if who and who[0] == "engine" else f"r{readers}" if who is None and readers else ""
        obj = spec.parse(r, e, names)
        if obj is None:
            continue
        if not e:
            raise SystemExit(f"{path}: a request by {who}: {r}")
        ev = {"e": e, "req": r["kind"], "obj": obj[0], "n": obj[1], "out": r["outcome"]}
        found = (spec.parse({"path": p}, e, names) for p in r.get("listed") or [])
        ev["listed"] = sorted(f[1] for f in found if f and f[1])
        before = last.get(e) or {}
        found = ("create", "exists", ev["obj"], ev["n"])  # a create found something in its way
        if ev["req"] == "get" and tuple(before.get(k) for k in ("req", "out", "obj", "n")) == found:
            ev["req"] = "readback"  # `solera.objects.create` reads back what is in its way
        last[e] = ev
        out.append(ev)
    return out


def tla(value) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "{" + ", ".join(tla(v) for v in value) + "}"
    if isinstance(value, tuple):
        return "<<" + ", ".join(tla(v) for v in value) + ">>"
    return str(value)


def log_module(spec: Spec, events: list[dict], constants: dict) -> str:
    fields = sorted(events[0])
    records = ",\n    ".join("[" + ", ".join(f"{k} |-> {tla(ev[k])}" for k in fields) + "]" for ev in events)
    defs = "".join(f"{k} == {tla(v)}\n" for k, v in constants.items())
    return (
        f"---- MODULE {spec.module}TraceLog ----\nEXTENDS Sequences\n{defs}"
        f"Trace == <<\n    {records}\n    >>\n====\n"
    )


def traces(spec: Spec, path: Path) -> list[tuple[str, list[dict], dict]]:
    """A run's traces: one, of the requests `parse` keeps, unless the spec
    splits them itself."""

    if spec.traces is not None:
        return spec.traces(path)
    events = requests(spec, path)
    constants = {"TraceActors": sorted({ev["e"] for ev in events}), "TraceMaxN": len(events) + 1}
    return [(path.stem, events, constants)] if events else []


def check(spec: Spec, label: str, events: list[dict], constants: dict, work: Path) -> bool | None:
    """Whether TLC finds a behaviour that makes `events` (None: stopped)."""

    trace = f"{spec.module}Trace"
    for name in (spec.module, trace):
        (work / f"{name}.tla").write_text((HERE / f"{name}.tla").read_text())
    (work / f"{trace}Log.tla").write_text(log_module(spec, events, constants))
    (work / f"{trace}.cfg").write_text(spec.config)
    heap = os.environ.get("TLC_HEAP", "4g")
    cmd = ["java", f"-Xmx{heap}", "-cp", str(JAR), "tlc2.TLC", "-workers", "1"]
    cmd += ["-metadir", str(work / "states"), "-config", f"{trace}.cfg", f"{trace}.tla"]
    limit = float(os.environ.get("TRACE_TIMEOUT", "0")) or None
    try:
        log = subprocess.run(cmd, cwd=work, capture_output=True, text=True, timeout=limit).stdout
    except subprocess.TimeoutExpired:  # TLC is killed with it
        print(f"{label}: stopped after {limit:g} s ({len(events)} requests)")
        return None
    (work / f"{label}.log").write_text(log)
    violated = re.search(r"Invariant (\w+) is violated", log)
    if violated and violated.group(1) == "NotDone":
        print(f"{label}: valid ({len(events)} requests)")
        return True
    if violated:
        print(f"{label}: the run breaks {violated.group(1)}")
        return False
    done = max([int(n) for n in re.findall(r'<<"explained", (\d+)>>', log)] + [0])
    if "Finished in" not in log or done >= len(events):
        print(log[-3000:])
        print(f"{label}: TLC failed")
        return False
    print(f"{label}: {done} of {len(events)} requests explained; no behaviour makes request {done + 1}:")
    actor = events[done]["e"]
    for j, ev in [(j, ev) for j, ev in enumerate(events[: done + 1]) if ev["e"] == actor][-8:]:
        print(f"    {j + 1:5d}  {fmt(ev)}")
    start = max(0, done - 6)
    others = [(start + j, ev) for j, ev in enumerate(events[start:done]) if ev["e"] != actor]
    if others:
        print("  the requests just before it, of other actors:")
        for j, ev in others:
            print(f"    {j + 1:5d}  {fmt(ev)}")
    return False


def fmt(ev: dict) -> str:
    return "  ".join(f"{k}={ev[k]}" for k in sorted(ev) if ev[k] not in ((), [], "", 0) or k == "out")


def simulate(out: Path) -> list[Path]:
    env = {**os.environ, "SOLERA_SIM_REQUESTS": str(out)}
    pytest = [
        sys.executable,
        "-m",
        "pytest",
        "tests/sim/test_simulation.py",
        "-q",
        "-x",
        "-p",
        "no:cacheprovider",
    ]
    subprocess.run(pytest, cwd=ROOT, env=env)
    return sorted(out.glob("*.jsonl"))


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in SPECS:
        raise SystemExit(f"usage: check-trace.py {{{','.join(SPECS)}}} [RUN.jsonl ...]")
    if not JAR.exists():
        raise SystemExit(f"{JAR} is missing: run spec/tla/check.sh once to download it")
    spec = SPECS[sys.argv[1]]
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        runs = [Path(a) for a in sys.argv[2:]] or simulate(work / "runs")
        checked = [
            (label, check(spec, label, events, constants, work))
            for run in runs
            for label, events, constants in traces(spec, run)
        ]
        failed = [label for label, ok in checked if ok is False]
        stopped = sum(1 for _, ok in checked if ok is None)
        if failed and os.environ.get("TLA_LOGS"):
            logs = Path(os.environ["TLA_LOGS"])
            logs.mkdir(parents=True, exist_ok=True)
            for label in failed:
                (logs / f"{label}.log").write_text((work / f"{label}.log").read_text())
        valid = len(checked) - len(failed) - stopped
        print(f"{valid} of {len(checked)} traces valid, {stopped} stopped, from {len(runs)} runs")
        return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
