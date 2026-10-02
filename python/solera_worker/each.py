"""`Each`: an asset written for one key, run over a page of keys
(docs/per-key-processing.md §5, §9).

A page is either the changes of the edge's window (`changes`) or the
failure index's keys that are due again (`retry`). Each key is one call,
`concurrency` at a time; its outcome is classified (`solera.errors`), the
outputs of the keys that succeeded become one `Patch({key: value})` per
output, and every key's outcome moves its failure record (`solera.failures`)
— all of it committed together.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import time
import typing
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from solera import errors
from solera.failures import REMOVED, UNMATCHED, Outcome, Record, eligible, minima, transition
from solera.keys.index import IndexState, KeyIndex, key_bytes, key_str
from solera.sdk import UNSET, Ref, Result
from solera.stores import Keys, Patch

WALK = 100  # failure records walked per retry page, at most, for each key it may take


@dataclass
class Page:
    kind: str  # "changes" or "retry"
    upserted: dict[str, tuple[bytes, int]]  # key -> upstream (version, locator)
    deleted: list[str]
    after: str | None  # where the window's page, or the retry walk, ended (None: done)
    walked: dict[str, Record] = field(default_factory=dict)  # retry: every record walked
    priors: dict[str, Record] = field(default_factory=dict)


class _Abort(Exception):
    def __init__(self, error: BaseException):
        self.error = error


async def read_window(pin: dict, keys_io) -> tuple[dict, tuple, str | None]:
    """An Incremental page of a keyed upstream, as the spec pins it: the
    keys= override, a full delivery's page, or a window of pending deltas.
    Returns `(upserted, deleted, after)`."""

    ch = pin["changes"]
    index = KeyIndex(keys_io, None, IndexState.from_json(pin["index"]))
    if "keys" in ch:  # a run's keys= override: a one-off selection, of the keys that exist
        found = await index.lookup([key_bytes(str(k)) for k in ch["keys"]])
        return {key_str(k): entry for k, entry in found.items()}, (), None
    start = key_bytes(ch["after"]) if ch.get("after") is not None else None
    if ch.get("full"):
        keys, versions, locators, nxt = await index.page(start, int(ch["limit"]))
        flags = bytes(len(keys))
    else:
        keys, versions, flags, locators, nxt = await index.pending(
            int(ch["from"]), int(ch["to"]), start, int(ch["limit"])
        )
    upserted = {
        key_str(k): (v, loc) for k, v, d, loc in zip(keys, versions, flags, locators, strict=True) if not d
    }
    deleted = tuple(key_str(k) for k, d in zip(keys, flags, strict=True) if d)
    return upserted, deleted, key_str(nxt) if nxt is not None else None


async def read_page(pin: dict, keys_io) -> Page:
    each = pin["each"]
    failures = KeyIndex(keys_io, None, IndexState.from_json(each["failures"]))
    if each["kind"] != "retry":
        upserted, deleted, after = await read_window(pin, keys_io)
        touched = [key_bytes(k) for k in [*upserted, *deleted]]
        priors = await failures.lookup(touched) if touched else {}
        return Page(
            "changes",
            upserted,
            list(deleted),
            after,
            priors={key_str(k): Record.decode(v) for k, (v, _) in priors.items()},
        )
    # A retry page: walk the failure index from the pass's position, taking the
    # keys that are due, `limit` at most (§9).
    limit = int(pin["changes"]["limit"])
    after = pin["changes"]["retry"].get("after")
    cursor = key_bytes(after) if after is not None else None
    walked: dict[str, Record] = {}
    due: list[str] = []
    end = None
    while len(due) < limit and len(walked) < WALK * limit:
        keys, versions, _, nxt = await failures.page(cursor, limit)
        for k, v in zip(keys, versions, strict=True):
            key = key_str(k)
            record = walked[key] = Record.decode(v)
            if eligible(record, each["now"], int(each["epoch"]), each.get("forced") or {}):
                due.append(key)
            end = key
            if len(due) >= limit or len(walked) >= WALK * limit:
                break
        else:
            if nxt is None:
                end = None  # the whole index walked: the pass is complete
                break
            cursor = nxt
            continue
        break
    upstream = KeyIndex(keys_io, None, IndexState.from_json(pin["index"]))
    current = await upstream.lookup([key_bytes(k) for k in due]) if due else {}
    upserted, deleted = {}, []
    for key in due:
        entry = current.get(key_bytes(key))
        if entry is None:
            deleted.append(key)  # gone upstream: its outputs and its record go
        elif entry[0] == walked[key].revision:
            upserted[key] = entry
        # else: its upstream moved on — the change window brings it, at its new version
    return Page("retry", upserted, deleted, end, walked=walked, priors={k: walked[k] for k in due})


async def run(spec, project, asset, param: str, pin: dict, args: dict, ctx, keys_io, timeline, control):
    """Run one page: returns what to store (`values`), the result's parts,
    and — when a key raised `Abort` — the error that fails the attempt.

    `control["drain"]` is set when a cancel is requested (docs/lifecycle.md
    §7): no key starts after it, the calls in flight are cancelled, and the
    keys they leave are interrupted — canceled or timed out, by the cancel
    record's reason (§2.2) — while the keys that finished are stored."""

    drain = control["drain"]
    each = pin["each"]
    page = await read_page(pin, keys_io)
    timeline.add("loaded", param, len(page.upserted))
    ref = Ref.from_json(pin["ref"])
    store = project.stores[ref.store]
    t = typing.get_type_hints(asset.fn).get(param)
    loaded = await store.load(ref, dict[str, t], Keys(page.upserted)) if page.upserted else {}
    up = project.manifest["outputs"][ref.output]
    textual = bool(up.get("revision") or up.get("source") or up.get("partition_set"))

    def rendered(version: bytes) -> str:
        return version.decode(errors="replace") if textual else version.hex()

    decls = {o.name or asset.name: o for o in asset.outputs}
    is_async = inspect.iscoroutinefunction(asset.fn)
    signature = inspect.signature(asset.fn)
    pool = None if is_async else ThreadPoolExecutor(max_workers=int(each["concurrency"]))
    loop = asyncio.get_running_loop()
    gate = asyncio.Semaphore(int(each["concurrency"]))
    outputs: dict[str, dict] = {}
    outcomes: dict[str, Outcome] = {}
    durations: dict[str, float] = {}
    abort: list[BaseException] = []

    def interrupted() -> str:
        """What a key the drain stops becomes: by the cancel record's reason
        (docs/lifecycle.md §2.2), timed out or canceled."""

        cancel = control.get("cancel")
        return "timed_out" if cancel is not None and cancel.reason == "timeout" else "canceled"

    def split(value) -> dict:
        """One call's value per output: a bare value for a single output, or
        `Result(outputs=…)`; an output it does not return holds nothing."""

        if isinstance(value, Result):
            if value.cursor is not UNSET:
                raise errors.Failed("an Each asset keeps no cursor: its edge is its iteration")
            unknown = set(value.outputs) - set(decls)
            if unknown:
                raise errors.Failed(f"returned undeclared output {sorted(unknown)[0]!r}")
            return dict(value.outputs)
        if len(decls) == 1:
            return {next(iter(decls)): value}
        raise errors.Failed("a multi-output Each asset returns Result(outputs={...})")

    async def one(key: str):
        version = page.upserted[key][0]
        async with gate:
            if drain.is_set() or abort:
                outcomes[key] = Outcome(interrupted(), version)
                return
            call = dict(args)
            if "ctx" in signature.parameters:
                call["ctx"] = ctx._for_key(key, rendered(version))
            call[param] = loaded.get(key)
            start = time.monotonic()
            try:
                if key not in loaded:
                    raise errors.Failed("the upstream store returned no value for this key")
                if is_async:
                    value = await asset.fn(**call)
                else:
                    value = await loop.run_in_executor(pool, functools.partial(asset.fn, **call))
                    if inspect.isawaitable(value):
                        value = await value
                outputs[key] = split(value)
                outcomes[key] = Outcome("ok", version)
            except asyncio.CancelledError:
                if not (drain.is_set() or abort):
                    raise
                outcomes[key] = Outcome(interrupted(), version)
            except Exception as error:
                kind, timing = errors.classify(error, project.errors)
                if kind == errors.ABORT:
                    abort.append(error)
                    raise _Abort(error) from error
                message = f"{type(error).__name__}: {error}"
                outcomes[key] = Outcome(
                    kind, version, message, timing.get("retry_after"), timing.get("retry_for")
                )
                ctx._for_key(key, rendered(version)).log(message, "error")
            finally:
                durations[key] = time.monotonic() - start

    timeline.add("computing")
    tasks = {key: asyncio.create_task(one(key)) for key in page.upserted}
    stopper = asyncio.create_task(drain.wait())
    try:
        pending = set(tasks.values())
        while pending:
            done, pending = await asyncio.wait(pending | {stopper}, return_when=asyncio.FIRST_COMPLETED)
            pending.discard(stopper)
            failed = [t for t in done if t is not stopper and not t.cancelled() and t.exception() is not None]
            if failed and not isinstance(failed[0].exception(), _Abort):
                raise failed[0].exception()
            if (stopper in done or abort) and pending:
                # A cancel was requested (or a key aborted the attempt): stop the calls
                # in flight; the keys they leave are interrupted (§5).
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                pending = set()
    finally:
        stopper.cancel()
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
    timeline.add("computed")
    if abort:
        return {"abort": abort[0]}
    for key in page.deleted:
        outcomes[key] = Outcome(REMOVED)
    for key in each.get("unmatched") or ():
        outcomes.setdefault(key, Outcome(UNMATCHED))

    # What to store: the keys that succeeded, by output; removed keys go.
    groups = {name: {} for name in decls}
    removes = {name: set(page.deleted) for name in decls}
    for key, values in outputs.items():
        for name in decls:
            if values.get(name) is None:
                removes[name].add(key)
            else:
                groups[name][key] = values[name]
    exists = {name for name, info in (spec.get("outputs") or {}).items() if info.get("exists")}
    values = {
        name: Patch(groups[name], remove=sorted(removes[name] - set(groups[name])))
        for name in decls
        if groups[name] or (removes[name] and name in exists)
    }

    failures = await _failures(spec, each, page, outcomes, keys_io)
    rows, counts = [], Counter()
    for key, outcome in sorted(outcomes.items()):
        record = failures["records"].get(key)
        name = outcome.kind if outcome.kind in ("ok", "removed", "unmatched") else record.name
        counts[name] += 1
        rows.append(
            {
                "key": key,
                "revision": rendered(outcome.revision) if outcome.revision else None,
                "outcome": name,
                "error": outcome.message or None,
                "duration": round(durations.get(key, 0.0), 6),
            }
        )
    delivered = {
        "kind": page.kind,
        "after": page.after,
        "upserted": sorted(page.upserted),
        "deleted": page.deleted,
    }
    report = {k: v for k, v in failures.items() if k != "records"}
    return {
        "values": values,
        "delivered": delivered,
        "failures": report,
        "key_outcomes": rows,
        "keys": dict(counts),
        "drained": drain.is_set(),
    }


async def _failures(spec, each: dict, page: Page, outcomes: dict, keys_io) -> dict:
    """Move each touched key's record (§9's transition table), write the
    failure index's delta, and report the outcome counts' transitions and
    the bounds the commit lowers or accumulates."""

    epoch, forced = int(each["epoch"]), int(each.get("forced_pos") or 0)
    retries = int(each.get("retries") or 0)
    records, transitions = {}, Counter()
    upsert_keys, upsert_versions, removes = [], [], []
    for key, outcome in sorted(outcomes.items()):
        prior = page.priors.get(key)
        record = transition(prior, outcome, now=time.time(), epoch=epoch, forced=forced, retries=retries)
        records[key] = record
        if prior is not None:
            transitions[prior.name] -= 1
        if record is not None:
            transitions[record.name] += 1
            if record != prior:
                upsert_keys.append(key_bytes(key))
                upsert_versions.append(record.encode())
        elif prior is not None:
            removes.append(key_bytes(key))
    index = KeyIndex(keys_io, None, IndexState.from_json(each["failures"]))
    files, _ = await index.resolve(
        upsert_keys,
        upsert_versions,
        removes,
        batch=int(each["batch"]),
        attempt=spec["attempt"],
        generation=int(spec.get("generation") or 0),
        exact=True,
    )
    due, epoch_min = minima(records.values())
    report = {
        "keys": files.to_json(),
        "counts": {k: v for k, v in transitions.items() if v},
        "due": due,
        "epoch_min": epoch_min,
        "records": records,
    }
    pass_after = each.get("pass_after")
    if page.kind == "retry":
        # The walked range's records, as this page leaves them: the pass's accumulators (§9).
        walked = [records[k] if k in records else r for k, r in page.walked.items()]
        report["range"] = dict(zip(("due", "epoch_min"), minima(walked), strict=True))
    elif pass_after is not None:
        bound = key_bytes(pass_after)
        behind = [r for k, r in records.items() if key_bytes(k) <= bound]
        report["fold"] = dict(zip(("due", "epoch_min"), minima(behind), strict=True))
    return report
