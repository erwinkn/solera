"""Per-key incremental: an asset written for one key, run over a batch of keys
(docs/per-key-processing.md §5, §9); and a keyed batch as the engine
planned it, classed again from what a source served (`observe`).

A batch is either the input's owed keys (`changes`) or the stored outcomes'
keys that are due again (`retry`). Each key is one call,
`concurrency` at a time; its outcome is classified (`solera.errors`), the
outputs of the keys that succeeded become one `Patch({key: value})` per
output, and every key's outcome moves its stored outcome (`solera.key_outcomes`)
— all of it committed together.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from solera import errors
from solera.key_outcomes import REMOVED, UNMATCHED, Outcome, StoredOutcome, lower, minima, transition
from solera.keys import SortedEntries
from solera.keys.layers import LayerIndex, LayerState, key_bytes
from solera.sdk import UNSET, Ref, Result
from solera.stores import Keys, Patch, SourceBehind, missing_keys
from solera.tasks import Tasks

from .sources import reader

INTERRUPTED = "interrupted"  # a key a drain stopped: canceled or timed out once the result is sealed
CLASSES = ("added", "updated", "removed", "unchanged")


def observe(keys: list, served: dict | None) -> tuple[dict[str, list[str]], dict]:
    """A keyed batch's classes, and what it observed of each key (key ->
    version, None for absent), from the engine's plan — `[key, class,
    version, generation, old]` each, `old` None (not held) or `[version,
    same context]` — classed again from what a source served, when it says
    (docs/observed-set.md, "Observations"): served absent, a removal if the
    key was held, else nothing; served at the version held, under the same
    context, unchanged; served otherwise, added or updated by whether it
    was held. A key planned removed was not loaded. Where the index names
    no version of the source's (its commit gave none: the version is a
    generation), what was served says only whether the key is there."""

    classes: dict[str, list[str]] = {c: [] for c in CLASSES}
    seen = {}
    for key, cls, version, _, old in keys:
        if served is not None and cls != "removed":
            got = served.get(key)
            if got is None:
                cls = "removed" if old is not None else None
            elif not isinstance(version, str):  # nothing to compare its word with: as planned
                pass
            elif old is None:
                cls, version = "added", got
            else:
                cls, version = ("unchanged" if old[1] and str(old[0]) == got else "updated"), got
        seen[key] = None if cls in ("removed", None) else version
        if cls is not None:
            classes[cls].append(key)
    return classes, seen


@dataclass
class Batch:
    """The keys one per-key attempt calls, and those it removes: `deleted`
    are keys gone upstream, or held keys the input's patterns no longer
    take (`unmatched`); the consumer's outputs drop both. A batch has a
    `kind` (§9) — the input's owed keys, `changes`, or a `retry` of failed
    keys — the stored outcomes it read, and what it observed of each key
    (`observed`, `observe`)."""

    upserted: dict[str, int]  # key -> the generation of its upstream entry
    deleted: list[str]
    after: str | None  # where the retry walk ended (None: the walk is done)
    unmatched: list[str] = field(default_factory=list)
    kind: str = "changes"
    priors: dict[str, StoredOutcome] = field(default_factory=dict)  # the touched keys' stored outcomes
    rest: tuple = (None, None)  # retry: the bounds of the records walked but not taken
    observed: dict = field(default_factory=dict)  # key -> the version observed, None: absent


class _Abort(Exception):
    def __init__(self, error: BaseException):
        self.error = error


async def gone_since(output: str, key: str | None, value, expected: dict, pin: dict, keys_io) -> list[str]:
    """The keys of a batch a load by `Keys(expected)` did not answer, decided
    against the source's head index, not the pass's commit: a store of
    current rows serves only its newest state. A key the head still names
    the store lacks: the store is behind its index, `SourceBehind`,
    retryable and bounded. One the head lacks too was removed since:
    returned. A plain batch still delivers it in its class, with no row
    (D100); a per-key batch drops its outputs now, which its own index
    makes harmless to do twice."""

    missing = missing_keys(key, value, expected)
    if not missing:
        return []
    head = LayerIndex(keys_io, LayerState.from_json(pin.get("head") or pin["index"]))
    held = await head.lookup([key_bytes(k) for k in missing])
    for k in missing:
        if key_bytes(k) in held:
            raise SourceBehind(f"{output}: the source index says {k}@{expected[k]} but the source has no {k}")
    return missing


def read_each_batch(pin: dict) -> Batch:
    """A per-key batch as the engine planned it (§9): its keys — owed ones,
    or a retry's due ones, each with its class — and their prior failure
    records. Nothing here reads an index."""

    each = pin["each"]
    keys = pin["batch"]["keys"]
    gone = {"removed", "unmatched"}
    batch = Batch(
        {k: g for k, cls, _, g, _ in keys if cls not in gone},
        [k for k, cls, *_ in keys if cls == "removed"],
        (pin["batch"].get("retry") or {}).get("after"),
        unmatched=[k for k, cls, *_ in keys if cls == "unmatched"],
        kind=each["kind"],
        observed={k: (None if cls in gone else v) for k, cls, v, *_ in keys},
        rest=tuple(each.get("rest") or (None, None)),
    )
    batch.priors = {k: StoredOutcome.decode(bytes.fromhex(p)) for k, p in (each.get("priors") or {}).items()}
    return batch


async def run(spec, project, asset, param: str, pin: dict, args: dict, ctx, keys_io, timeline, control):
    """Run one batch: returns what to store (`values`), the result's parts,
    and — when a key raised `Abort` — the error that fails the attempt.

    `control["drain"]` is set when a cancel is requested (docs/lifecycle.md
    §7): no key starts after it, the calls in flight are cancelled, and the
    keys they leave are interrupted — canceled or timed out, by the cancel
    record's reason (§2.2) — while the keys that finished are stored.
    Which of the two is decided by `finish(cancel)`, after the store writes,
    with the record the result is sealed with: a user cancel that arrives
    while a timeout drain stores still makes its keys canceled."""

    drain = control["drain"]
    each = pin["each"]
    batch = read_each_batch(pin)
    timeline.add("loaded", param, len(batch.upserted))
    ref = Ref.from_json(pin["ref"])
    store = reader(project, ref)
    t = project.hints[asset.name].get(param)
    loaded = (
        await ctx._observed.load(store, ref, dict[str, t], Keys(batch.upserted)) if batch.upserted else {}
    )
    served = getattr(store, "served", None)
    if served is not None:  # a source says what it served: the batch is classed by it
        if batch.kind == "changes":
            classes, batch.observed = observe(pin["batch"]["keys"], served)
            keep = {*classes["added"], *classes["updated"], *classes["unchanged"]}
            batch.deleted += [k for k in classes["removed"] if k not in batch.deleted]
        else:  # a retry's keys are held: one served absent goes
            keep = {k for k in batch.upserted if served.get(k) is not None}
            for k in batch.upserted:
                if k not in keep:
                    batch.deleted.append(k)
                    batch.observed[k] = None
                elif isinstance(batch.observed.get(k), str):  # the index has the source's word
                    batch.observed[k] = served[k]
        batch.upserted = {k: g for k, g in batch.upserted.items() if k in keep}
    else:
        for key in await gone_since(ref.output, None, loaded, batch.upserted, pin, keys_io):  # by key
            del batch.upserted[key]
            batch.deleted.append(key)
            batch.observed[key] = None
    await ctx._observed.close()  # the inputs' moment ends before the calls
    decls = {o.name or asset.name: o for o in asset.outputs}
    is_async = inspect.iscoroutinefunction(asset.fn)
    signature = inspect.signature(asset.fn)
    # `concurrency` keys at once within the partition (D111): that many workers.
    at_once = max(1, min(int(each["concurrency"]), len(batch.upserted)))
    pool = None if is_async else ThreadPoolExecutor(max_workers=at_once)
    loop = asyncio.get_running_loop()
    outputs: dict[str, dict] = {}
    outcomes: dict[str, Outcome] = {}
    durations: dict[str, float] = {}
    abort: list[BaseException] = []

    def split(key: str, value) -> dict:
        """One call's value per output: a bare value for a single output, or
        `Result(outputs=…)`. An output it does not return, or returns as
        `None`, is left as it is for this key; `Patch(None, remove=[key])`
        removes the key from it (§5)."""

        if isinstance(value, Result):
            if value.cursor is not UNSET:
                raise errors.Failed("a per-key asset keeps no cursor: its input is its iteration")
            unknown = set(value.outputs) - set(decls)
            if unknown:
                raise errors.Failed(f"returned undeclared output {sorted(unknown)[0]!r}")
            values = dict(value.outputs)
        elif len(decls) == 1:
            values = {next(iter(decls)): value}
        else:
            raise errors.Failed("a multi-output Each asset returns Result(outputs={...})")
        for name, v in values.items():
            if isinstance(v, Patch) and (v.rows or [str(k) for k in v.remove] != [key]):
                raise errors.Failed(
                    f"{name}: a per-key call removes its own key, Patch(None, remove=[ctx.key]), nothing else"
                )
        return values

    todo = iter(batch.upserted.items())

    async def worker():
        """Calls key after key — `concurrency` workers pulling from the batch,
        never a task per key — until the keys run out, a drain begins, or a
        key aborts the batch. Keys never started are interrupted (below)."""

        for key, generation in todo:
            if drain.is_set() or abort:
                return
            try:
                await call(key, generation)
            except asyncio.CancelledError:
                if not (drain.is_set() or abort):
                    raise
                outcomes[key] = Outcome(INTERRUPTED, generation)
                raise

    async def call(key: str, generation: int):
        kwargs = dict(args)
        if "ctx" in signature.parameters:
            kwargs["ctx"] = ctx._for_key(key, generation)
        kwargs[param] = loaded.get(key)
        start = time.monotonic()
        try:
            if key not in loaded:
                raise errors.Failed("the upstream store returned no value for this key")
            if is_async:
                value = await asset.fn(**kwargs)
            else:
                value = await loop.run_in_executor(pool, functools.partial(asset.fn, **kwargs))
                if inspect.isawaitable(value):
                    value = await value
            outputs[key] = split(key, value)
            outcomes[key] = Outcome("ok", generation)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            kind, timing = errors.classify(error, project.errors)
            if kind == errors.ABORT:
                abort.append(error)
                raise _Abort(error) from error
            message = f"{type(error).__name__}: {error}"
            outcomes[key] = Outcome(
                kind, generation, message, timing.get("retry_after"), timing.get("retry_for")
            )
            ctx._for_key(key, generation).log(message, "error")
        finally:
            durations[key] = time.monotonic() - start

    timeline.add("computing")
    running = Tasks("each")  # their failures are raised here: awaited
    workers = [running.spawn(worker(), awaited=True) for _ in range(at_once)]
    stopper = running.spawn(drain.wait(), awaited=True)
    try:
        pending = set(workers)
        while pending:
            done, pending = await asyncio.wait(pending | {stopper}, return_when=asyncio.FIRST_COMPLETED)
            pending.discard(stopper)
            failed = [t for t in done if t is not stopper and not t.cancelled() and t.exception() is not None]
            if failed and not isinstance(failed[0].exception(), _Abort):
                raise failed[0].exception()
            if stopper in done or abort:
                break  # a cancel was requested, or a key aborted the attempt
    finally:
        # The calls still in flight stop; the keys they leave are interrupted (§5).
        await running.close()
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
    timeline.add("computed")
    if abort:
        return {"abort": abort[0]}
    for key, generation in batch.upserted.items():
        # Every key of the batch has an outcome before its batch commits.
        outcomes.setdefault(key, Outcome(INTERRUPTED, generation))
    for key in batch.deleted:
        outcomes[key] = Outcome(REMOVED)
    for key in batch.unmatched:
        outcomes[key] = Outcome(UNMATCHED)

    # What to store: the keys that succeeded, by output; removed keys go.
    groups = {name: {} for name in decls}
    removes = {name: {*batch.deleted, *batch.unmatched} for name in decls}
    for key, values in outputs.items():
        for name in decls:
            value = values.get(name)
            if isinstance(value, Patch):  # an explicit removal of this key
                removes[name].add(key)
            elif value is not None:  # None, or not returned: no change for this key
                groups[name][key] = value
    exists = {name for name, info in (spec.get("outputs") or {}).items() if info.get("before")}
    values = {
        name: Patch(groups[name], remove=sorted(removes[name] - set(groups[name])))
        for name in decls
        if groups[name] or (removes[name] and name in exists)
    }

    async def finish(cancel) -> dict:
        """The batch's outcome delta, key outcomes and counts, with interrupted
        keys made what `cancel` — the record the result is sealed with — says:
        timed out for a timeout, canceled otherwise (lifecycle.md §2.2)."""

        made = "timed_out" if cancel is not None and cancel.reason == "timeout" else "canceled"
        final = {k: Outcome(made, o.upstream) if o.kind == INTERRUPTED else o for k, o in outcomes.items()}
        stored = await _outcomes(spec, each, batch, final, keys_io)
        rows, counts = [], Counter()
        for key, outcome in sorted(final.items()):
            record = stored["records"].get(key)
            name = outcome.kind if outcome.kind in ("ok", "removed", "unmatched") else record.name
            counts[name] += 1
            rows.append(
                {
                    "key": key,
                    "generation": outcome.upstream or None,
                    "outcome": name,
                    "error": outcome.message or None,
                    "duration": round(durations.get(key, 0.0), 6),
                }
            )
        report = {k: v for k, v in stored.items() if k != "records"}
        return {"outcomes": report, "key_outcomes": rows, "keys": dict(counts)}

    # What it observed: every key of the batch, or — drained — those that finished; an
    # interrupted key is not observed, so it stays owed (docs/observed-set.md, "Outcomes").
    observed = {
        k: v for k, v in batch.observed.items() if k not in outcomes or outcomes[k].kind != INTERRUPTED
    }
    delivered = {"kind": batch.kind, "after": batch.after, "observed": observed, "whole": not drain.is_set()}
    return {
        "values": values,
        "delivered": delivered,
        "finish": finish,
        "drained": drain.is_set(),
        "skipped": not outcomes,  # nothing on the batch to call or remove
    }


async def _outcomes(spec, each: dict, batch: Batch, outcomes: dict, keys_io) -> dict:
    """Move each touched key's record (§9's transition table), write the
    outcome delta, and report the outcome counts' transitions and
    the bounds the commit lowers or accumulates."""

    deploy, forced = int(each["deploy"]), int(each.get("forced_at") or 0)
    retries = int(each.get("retries") or 0)
    records, transitions = {}, Counter()
    upsert_keys, upsert_records, removes = [], [], []
    for key, outcome in sorted(outcomes.items()):
        prior = batch.priors.get(key)
        record = transition(prior, outcome, now=time.time(), deploy=deploy, forced=forced, retries=retries)
        records[key] = record
        if prior is not None:
            transitions[prior.name] -= 1
        if record is not None:
            transitions[record.name] += 1
            if record != prior:
                upsert_keys.append(key_bytes(key))
                upsert_records.append(record.encode())
        elif prior is not None:
            removes.append(key_bytes(key))
    state = LayerState.from_json(each["outcomes"])
    if each.get("start_over"):  # the commit replaces the index: resolved against an empty one
        state = LayerState(prefix=state.prefix)
    files, _ = await LayerIndex(keys_io, state).write_patch(
        SortedEntries.of(upsert_keys, upsert_records, removes),
        name=f"{int(each['commit_number']):012d}-{spec['attempt']}",
        generation=int(spec.get("generation") or 0),
    )
    due, deploy_min = minima(records.values())
    report = {
        "keys": files.to_json(),
        "counts": {k: v for k, v in transitions.items() if v},
        "due": due,
        "deploy_min": deploy_min,
        "records": records,
    }
    pass_after = each.get("pass_after")
    if batch.kind == "retry":
        # The walked range's records, as this batch leaves them: the pass's accumulators (§9).
        mine = minima(records.values())
        report["range"] = {
            "due": lower(batch.rest[0], mine[0]),
            "deploy_min": lower(batch.rest[1], mine[1]),
        }
    elif pass_after is not None:
        bound = key_bytes(pass_after)
        behind = [r for k, r in records.items() if key_bytes(k) <= bound]
        report["fold"] = dict(zip(("due", "deploy_min"), minima(behind), strict=True))
    return report
