"""Key outcomes on the new tracking core (docs/per-key-processing.md §9,
docs/tracking-design.md §11; build plan B6): the engine's half.

The core keeps every failing key of a per-key edge as a record, in outputs
of its own beside the edge (`Tracking.outcome_outputs`): the failures, and
the retry clock. The engine never holds a record. It gives the core what
it alone knows (its clock, the deploy number, the asset's `retries=`, and
where each record commit's layer goes) and records the events the core
builds:

- a batch's outcomes ride its commit's event (`commit.progress`);
- a catch-up (`catch_up`) drops the failures gone moot since the edge last
  looked: a key whose input went back to what the partition held (KEY-3),
  or one that never succeeded and is removed upstream (KEY-12). It runs
  before an edge is planned and before its batch's outcomes are built, both
  of which the core refuses otherwise, and after each commit of an output a
  per-key edge reads (`settle`), so such a failure goes at once.

Retries coming due (a pass of the retry clock, a deploy's or a forced
retry) are a later unit's: forced counters are 0 here.
"""

from __future__ import annotations

import logging

import orjson
from solera._native import OutcomesMovedError, Tracking
from solera.patterns import Matcher

from ..model import edge_id, output_id
from ..state import Conflict
from . import Core, Generation

TRIES = 5  # builds of a catch-up or a batch's outcomes the state may move under before giving up

log = logging.getLogger(__name__)


def retries(core: Core, asset: str) -> int:
    """The asset's `retries=`: how many timeouts a key is retried for."""

    found = (core.m.manifest or {}).get("assets", {}).get(asset) or {}
    return int((found.get("retries") or {}).get("n", 0))


def clock(core: Core, edge: bytes, asset: str, by: str) -> dict:
    """What a record commit of `edge` is built with: the engine's clock, the
    deploy number, the asset's `retries=`, and where each record output's
    next layer goes (`Core.path`, under the output's life; 0 before its
    first commit, which gives it one). Attaches the record outputs."""

    failures, due = Tracking.outcome_outputs(edge)
    core.attach(failures, due)
    now = core.engine.clock()
    out = {
        "now": now,
        "deploy": core.m.deploy_number,
        "retries": retries(core, asset),
        "time_ms": int(now * 1000),
    }
    for name, id_ in (("failures_path", failures), ("due_path", due)):
        found = core.tracking.output(id_) or {"life": 0, "head": 0}
        out[name] = core.path(found["life"], found["head"] + 1, by)
    return out


def failing(core: Core, edge: bytes) -> bool:
    """Whether `edge` holds a failing key's record: else a catch-up has nothing to drop."""

    return any(core.tracking.outcome_counts(edge).values())


async def catch_up(core: Core, edge: bytes, conditions: dict, asset: str) -> None:
    """Drop `edge`'s failures gone moot since it last looked, under
    `conditions`, recorded at once. Built again if the state moves before
    it is recorded; a deploy meanwhile raises `Redeployed`."""

    if not failing(core, edge):
        return
    for _ in range(TRIES):
        generation = Generation(core.m, "its failures were caught up")
        with generation:
            event = await core.run(
                core.tracking.outcome_catch_up, edge, conditions, **clock(core, edge, asset, "catch-up")
            )
        generation.check()
        if event is None:
            return
        try:
            core.record(event)
            return
        except Conflict as error:
            if not isinstance(error.__cause__, OutcomesMovedError):
                raise
    raise Conflict(f"the per-key records of {edge.decode(errors='replace')} kept moving while caught up")


async def settle(engine, outputs) -> None:
    """After commits of `outputs` (`(output, partition)` pairs): every
    per-key edge that reads one and holds a failing key catches up, under
    the conditions it would be processed under now. One that fails here
    catches up again before its edge is planned, so it is logged, not
    raised: the commits are recorded."""

    from .stale import conditions_now  # stale imports prepare, which imports this module

    core = engine.core
    upstreams = {output_id(output, partition) for output, partition in outputs}
    for edge, upstream in core.tracking.edges():
        if upstream not in upstreams or not failing(core, edge):
            continue
        try:
            asset, partition, param = orjson.loads(edge)
            planner = engine.planner()
            inputs = planner.inputs(asset, partition)
            input = next(i for i in inputs if i.param == param)
            conditions = conditions_now(core, asset, partition, input, planner, inputs)
            await catch_up(core, edge, conditions, asset)
        except Exception as error:  # noqa: BLE001 - the commits stand; the edge catches up before it plans
            log.warning("per-key edge %s did not catch up: %s", edge.decode(errors="replace"), error)


# -- what a batch's keys came to ------------------------------------------------------------

MADE = {"transient": "retrying"}  # the stored outcome a worker's class makes, where its name differs


def stored(result: dict, after: dict) -> dict:
    """A per-key batch's keys whose stored outcome is not the one their
    class makes (`MADE`), from what the core said each ended with (`after`):
    a transient key past its `retry_for` is failed. The history's rows of
    the attempt are the result's, corrected by these."""

    out = {}
    for rows in (result.get("outcomes") or {}).values():
        for key, kind, *_ in rows:
            made = after.get(key)
            if made is not None and made != MADE.get(kind, kind):
                out[key] = made
    return out


def counts(result: dict, after: dict) -> dict | None:
    """A per-key attempt's keys by outcome (`result["keys"]`), a failing key
    counted by its stored outcome rather than its class."""

    keys, failing = result.get("keys"), result.get("outcomes") or {}
    if not keys or not failing:
        return keys
    out = {name: n for name, n in keys.items() if name in ("ok", "removed", "unmatched")}
    for rows in failing.values():
        for key, kind, *_ in rows:
            name = after.get(key) or MADE.get(kind, kind)
            out[name] = out.get(name, 0) + 1
    return out


# -- every key's latest outcome (KEY-5) ------------------------------------------------------

STORED = ("rejected", "failed", "retrying", "canceled", "timed_out")  # the outcomes a record keeps
OUTCOMES = ("ok", "unmatched", "removed", *STORED)


async def latest(
    engine, asset: str, partition: str, param: str, *, keys=None, after=None, limit=100, outcomes=()
):
    """One partition's keys with their latest outcome (KEY-5), in key order,
    and where the next page starts: `keys` alone, else a page after `after`
    of every key upstream has, the partition holds or a record keeps; of
    those, the rows whose outcome is in `outcomes` (all, if none). A row
    is `{key, partition, outcome, owed, version}`, a record's fields too:
    `ok` with the version it processed (None once upstream moved on),
    `removed` while its removal is owed, `unmatched` where the patterns
    leave it out, a stored outcome, or None for a key never processed.
    Whether a key is owed is the planner's word on it, as a key list asks,
    under the conditions it would be processed under now. A filtered page
    may come back short, with a `next`. None when the partition has no
    edge for `param` yet."""

    from .stale import conditions_now  # stale imports prepare, which imports this module

    core, m = engine.core, engine.m
    edge = edge_id(asset, partition, param)
    if core.tracking.edge(edge) is None:
        return None
    planner = engine.planner()
    inputs = planner.inputs(asset, partition)
    input = next(i for i in inputs if i.param == param)
    upstream = m.core_output(input.output, input.partition)
    name = engine.manifest["assets"][asset]["outputs"][0]["name"]
    own_id = output_id(name, partition)
    own = m.core_output(name, partition)
    core.attach(output_id(input.output, input.partition), own_id, *Tracking.outcome_outputs(edge))
    stored_only = bool(outcomes) and set(outcomes) <= set(STORED)
    if keys is not None:
        candidates, next_ = sorted(set(keys)), None
    elif stored_only:
        rows, next_ = await core.run(
            core.tracking.outcome_records, edge, after=after, limit=limit, outcomes=list(outcomes)
        )
        candidates = [key for key, _ in rows]
    else:
        candidates, next_ = await _page(core, edge, upstream, own, after, limit)
    if not candidates:
        return [], next_
    present, planned = set(), {}
    if upstream is not None and upstream["head"]:
        found, _ = await core.run(core.index.get, upstream["layers"], upstream["head"], candidates)
        present = {row[0] for row in found}
        request = {
            "mode": "keys",
            "keys": candidates,
            "after": None,
            "limit": len(candidates),
            "inputs": [
                {
                    "edge": edge,
                    "conditions": conditions_now(core, asset, partition, input, planner, inputs),
                    **({"own": own_id} if own is not None else {}),
                }
            ],
        }
        summary = await core.run(core.tracking.plan, request)
        for key, cls, version in zip(*Tracking.plan_keys(summary["plan"], 0), strict=True):
            planned[key] = (cls, version)
    records = dict((await core.run(core.tracking.outcome_records, edge, keys=candidates))[0])
    taken = Matcher(input.spec.get("patterns"))
    rows = []
    for key in candidates:
        cls, version = planned.get(key, (None, None))
        row = {"key": key, "partition": partition, "owed": cls not in (None, "unchanged"), "version": None}
        if (record := records.get(key)) is not None:
            row.update(record)
        elif cls == "unchanged":
            row.update(outcome="ok", version=version)
        elif cls == "updated":
            row["outcome"] = "ok"  # processed, at a version upstream has since replaced
        elif cls == "removed":
            row["outcome"] = "removed"
        elif cls == "added":
            row["outcome"] = None  # never processed
        elif key in present and not taken(key):
            row["outcome"] = "unmatched"
        else:
            continue  # neither side holds it
        if not outcomes or row["outcome"] in outcomes:
            rows.append(row)
    return rows, next_


async def _page(core: Core, edge: bytes, upstream: dict | None, own: dict | None, after, limit: int):
    """The next `limit` keys after `after` that upstream has, the partition
    holds, or a record of the edge keeps, and where the page after starts."""

    sources = []
    for found in (upstream, own):
        if found is not None and found["head"]:
            rows, more = await core.run(
                core.index.scan, found["layers"], found["head"], after=after, limit=limit
            )
            sources.append(([row[0] for row in rows], more))
    rows, more = await core.run(core.tracking.outcome_records, edge, after=after, limit=limit)
    sources.append(([key for key, _ in rows], more))
    # A source with more keys is read up to its last: past it, another key of its may come first.
    bound = min((keys[-1] for keys, more in sources if more is not None and keys), default=None)
    keys = sorted({key for found, _ in sources for key in found if bound is None or key <= bound})
    if len(keys) > limit:
        return keys[:limit], keys[limit - 1]
    return keys, bound
