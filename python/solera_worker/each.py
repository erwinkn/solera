"""`Each`: an asset written for one key, run over a page of keys
(docs/per-key-processing.md §5, §9).

A page is either the changes of the edge's window (`changes`) or the
failure index's keys that are due again (`retry`). Each key is one call,
`concurrency` at a time; its outcome is classified (`solera.errors`), the
outputs of the keys that succeeded become one `Patch({key: value})` per
output, and every key's outcome moves its failure record (`solera.failed_keys`)
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
from solera.failed_keys import REMOVED, UNMATCHED, Outcome, Record, eligible, minima, transition
from solera.keys import SortedEntries
from solera.keys.index import IndexState, KeyIndex, key_bytes, key_str
from solera.patterns import Matcher
from solera.sdk import UNSET, Ref, Result
from solera.stores import Keys, Patch

WALK = 100  # failure records walked per retry page, at most, for each key it may take
INTERRUPTED = "interrupted"  # a key a drain stopped: canceled or timed out once the result is sealed
LOOKAHEAD = 100_000  # index entries a page examines at most, to fill itself and to prove it final


@dataclass
class Page:
    kind: str  # "changes" or "retry"
    upserted: dict[str, int]  # key -> the generation of its upstream entry: its version
    deleted: list[str]
    after: str | None  # where the window's page, or the retry walk, ended (None: done)
    unmatched: list[str] = field(default_factory=list)  # keys that stopped matching the edge's patterns
    walked: dict[str, Record] = field(default_factory=dict)  # retry: every record walked
    priors: dict[str, Record] = field(default_factory=dict)


class _Abort(Exception):
    def __init__(self, error: BaseException):
        self.error = error


@dataclass
class Window:
    """An Incremental page of a keyed upstream, its keys filtered by the
    edge's patterns: `read` says how many keys the page held before, and
    `unmatched` that its deletions are keys that stopped matching (a
    rescope's diff) rather than keys gone upstream."""

    upserted: dict[str, int]  # key -> generation
    deleted: tuple
    after: str | None
    read: int
    unmatched: bool = False


async def _fill(chunk, start: bytes | None, limit: int, kind) -> tuple[list, str | None, int]:
    """A page of `limit` entries that `kind` takes, read ahead past the ones
    it does not: `chunk(after, n)` returns `(entries, next)` — at most `n`
    entries as `(key, generation, deleted)` in key order past `after`,
    and where to go on (None: exhausted). Past a full page it looks on for
    one more entry it takes, so that a page is `final` exactly when nothing
    follows and no delivery ends on an empty page (§5).

    Entries are read a page's worth and one more at a time — never just what
    the page still lacks, so a sparse pattern costs scans in proportion to
    the entries it passes over, divided by the page — and at most
    `LOOKAHEAD` of them: past that the page goes as it is — not
    final, not full, perhaps empty (then its attempt is skipped, the producer
    not called). The next page starts after the last entry examined, so no
    entry is read twice. Returns the page's entries, where the next page
    starts (None: this one is final), and how many entries were read."""

    page, cursor, read, last = [], start, 0, None
    while True:
        entries, nxt = await chunk(cursor, limit + 1)
        for n, entry in enumerate(entries, 1):
            taken = kind(entry)
            if taken is not None:
                if len(page) == limit:  # one more is taken: the page is full, not final
                    return page, key_str(last), read
                page.append((taken, entry))
            read, last = read + 1, entry[0]
            if read >= LOOKAHEAD and (n < len(entries) or nxt is not None):
                return page, key_str(last), read  # examined enough: the rest is the next page's
        if nxt is None:
            return page, None, read
        cursor = nxt


async def read_window(pin: dict, keys_io) -> Window:
    """An Incremental page of a keyed upstream, as the spec pins it: the
    keys= override, a full delivery's page, a window of pending deltas — all
    filtered by the edge's patterns (per-key §11), read ahead past keys they
    leave out until the page holds `page_size` keys or the delivery runs
    out — or a rescope's diff of the index as of its cutover: the keys whose
    membership changed. A pure function of the pin: it reads the index
    through `KeyIndex.page`, `pending` and `lookup` only, so the engine can
    run it on its own copies to serve the same page."""

    ch = pin["batch"]
    index = KeyIndex(keys_io, None, IndexState.from_json(pin["index"]))
    limit = int(ch.get("limit") or 1)
    start = key_bytes(ch["after"]) if ch.get("after") is not None else None

    async def whole(after, n):
        keys, generations, _, nxt = await index.page(after, n)
        return list(zip(keys, generations, bytes(len(keys)), strict=True)), nxt

    async def window(after, n):
        keys, generations, flags, _, nxt = await index.pending(int(ch["from"]), int(ch["to"]), after, n)
        return list(zip(keys, generations, flags, strict=True)), nxt

    if "pattern_change" in ch:
        old, new = Matcher(ch["pattern_change"]["from"]), Matcher(ch["pattern_change"]["to"])

        def changed(entry):
            key = key_str(entry[0])
            before, now = old(key), new(key)
            return "upsert" if now and not before else "delete" if before and not now else None

        page, after, read = await _fill(whole, start, limit, changed)
        upserted = {key_str(e[0]): e[1] for kind, e in page if kind == "upsert"}
        deleted = tuple(key_str(e[0]) for kind, e in page if kind == "delete")
        return Window(upserted, deleted, after, read, unmatched=True)
    taken = Matcher(pin.get("patterns"))
    if "keys" in ch:  # a run's keys= override: a one-off selection, of the keys that exist
        found = await index.lookup([key_bytes(str(k)) for k in ch["keys"]])
        upserted = {key_str(k): generation for k, (generation, _) in found.items()}
        return Window({k: e for k, e in upserted.items() if taken(k)}, (), None, len(upserted))

    def kind(entry):
        return ("delete" if entry[2] else "upsert") if taken(key_str(entry[0])) else None

    page, after, read = await _fill(whole if ch.get("full") else window, start, limit, kind)
    upserted = {key_str(e[0]): e[1] for k, e in page if k == "upsert"}
    deleted = tuple(key_str(e[0]) for k, e in page if k == "delete")
    return Window(upserted, deleted, after, read)


async def read_page(spec: dict, pin: dict, keys_io) -> Page:
    each = pin["each"]
    failures = KeyIndex(keys_io, None, IndexState.from_json(each["failures"]))
    if each["kind"] == "reconcile":
        return await _reconcile_page(spec, pin, keys_io, failures)
    if each["kind"] != "retry":
        window = await read_window(pin, keys_io)
        touched = [key_bytes(k) for k in [*window.upserted, *window.deleted]]
        priors = await failures.lookup(touched) if touched else {}
        return Page(
            "changes",
            window.upserted,
            [] if window.unmatched else list(window.deleted),
            window.after,
            unmatched=list(window.deleted) if window.unmatched else [],
            priors={key_str(k): Record.decode(p) for k, (_, p) in priors.items()},
        )
    # A retry page: walk the failure index from the pass's position, taking the
    # keys that are due, `limit` at most (§9).
    limit = int(pin["batch"]["limit"])
    after = pin["batch"]["retry"].get("after")
    cursor = key_bytes(after) if after is not None else None
    walked: dict[str, Record] = {}
    due: list[str] = []
    end = None
    while len(due) < limit and len(walked) < WALK * limit:
        keys, _, payloads, nxt = await failures.page(cursor, limit)
        for k, p in zip(keys, payloads, strict=True):
            key = key_str(k)
            record = walked[key] = Record.decode(p)
            if eligible(record, each["now"], int(each["deploy"]), each.get("forced") or {}):
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
    taken = Matcher(pin.get("patterns"))
    upserted, deleted, unmatched = {}, [], []
    for key in due:
        entry = current.get(key_bytes(key))
        if not taken(key):
            unmatched.append(key)  # no longer one of the edge's keys: its outputs and record go
        elif entry is None:
            deleted.append(key)  # gone upstream: its outputs and its record go
        elif entry[0] == walked[key].upstream:
            upserted[key] = entry[0]
        # else: its upstream was written since — the change window brings it, at its new generation
    return Page(
        "retry",
        upserted,
        deleted,
        end,
        unmatched=unmatched,
        walked=walked,
        priors={k: walked[k] for k in due},
    )


async def _reconcile_page(spec: dict, pin: dict, keys_io, failures: KeyIndex) -> Page:
    """After a full delivery: the next `limit` keys the asset's outputs or its
    failure index hold, and which of them the edge no longer has — gone
    upstream, or left out by its patterns. Those go (§11); the rest stay."""

    limit = int(pin["batch"]["limit"])
    after = pin["batch"]["reconcile"].get("after")
    start = key_bytes(after) if after is not None else None
    indexes = [
        KeyIndex(keys_io, None, IndexState.from_json(info["index"]))
        for info in (spec.get("outputs") or {}).values()
        if info.get("index") is not None
    ] + [failures]
    found, bound = set(), None
    for index in indexes:
        keys, _, _, nxt = await index.page(start, limit)
        found.update(keys)
        if nxt is not None:  # this index holds more, past `nxt`: nothing beyond it is known yet
            bound = nxt if bound is None else min(bound, nxt)
    candidates = sorted(k for k in found if bound is None or k <= bound)
    take = candidates[:limit]
    more = bound is not None or len(candidates) > limit
    end = (key_str(take[-1]) if take else key_str(bound)) if more else None
    upstream = KeyIndex(keys_io, None, IndexState.from_json(pin["index"]))
    current = await upstream.lookup(take) if take else {}
    taken = Matcher(pin.get("patterns"))
    deleted, unmatched = [], []
    for k in take:
        key = key_str(k)
        if not taken(key):
            unmatched.append(key)
        elif k not in current:
            deleted.append(key)
    touched = [key_bytes(k) for k in [*deleted, *unmatched]]
    priors = await failures.lookup(touched) if touched else {}
    return Page(
        "reconcile",
        {},
        deleted,
        end,
        unmatched=unmatched,
        priors={key_str(k): Record.decode(p) for k, (_, p) in priors.items()},
    )


async def run(spec, project, asset, param: str, pin: dict, args: dict, ctx, keys_io, timeline, control):
    """Run one page: returns what to store (`values`), the result's parts,
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
    page = await read_page(spec, pin, keys_io)
    timeline.add("loaded", param, len(page.upserted))
    ref = Ref.from_json(pin["ref"])
    store = project.stores[ref.store]
    t = project.hints[asset.name].get(param)
    loaded = await ctx._observed.load(store, ref, dict[str, t], Keys(page.upserted)) if page.upserted else {}
    await ctx._observed.close()  # the inputs' moment ends before the calls
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

    def split(key: str, value) -> dict:
        """One call's value per output: a bare value for a single output, or
        `Result(outputs=…)`. An output it does not return, or returns as
        `None`, is left as it is for this key; `Patch(None, remove=[key])`
        removes the key from it (§5)."""

        if isinstance(value, Result):
            if value.cursor is not UNSET:
                raise errors.Failed("an Each asset keeps no cursor: its edge is its iteration")
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
                    f"{name}: an Each call removes its own key, Patch(None, remove=[ctx.key]), nothing else"
                )
        return values

    async def one(key: str):
        generation = page.upserted[key]
        try:
            async with gate:  # a cancel may reach a key still waiting here: it is interrupted too
                if drain.is_set() or abort:
                    outcomes[key] = Outcome(INTERRUPTED, generation)
                    return
                await call(key, generation)
        except asyncio.CancelledError:
            if not (drain.is_set() or abort):
                raise
            outcomes[key] = Outcome(INTERRUPTED, generation)

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
    for key, generation in page.upserted.items():
        # Every key of the page has an outcome before its watermark moves past it.
        outcomes.setdefault(key, Outcome(INTERRUPTED, generation))
    for key in page.deleted:
        outcomes[key] = Outcome(REMOVED)
    for key in page.unmatched:
        outcomes[key] = Outcome(UNMATCHED)

    # What to store: the keys that succeeded, by output; removed keys go.
    groups = {name: {} for name in decls}
    removes = {name: {*page.deleted, *page.unmatched} for name in decls}
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
        """The page's failure delta, key outcomes and counts, with interrupted
        keys made what `cancel` — the record the result is sealed with — says:
        timed out for a timeout, canceled otherwise (lifecycle.md §2.2)."""

        made = "timed_out" if cancel is not None and cancel.reason == "timeout" else "canceled"
        final = {k: Outcome(made, o.upstream) if o.kind == INTERRUPTED else o for k, o in outcomes.items()}
        failures = await _failures(spec, each, page, final, keys_io)
        rows, counts = [], Counter()
        for key, outcome in sorted(final.items()):
            record = failures["records"].get(key)
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
        report = {k: v for k, v in failures.items() if k != "records"}
        return {"failures": report, "key_outcomes": rows, "keys": dict(counts)}

    delivered = {
        "kind": page.kind,
        "after": page.after,
        "upserted": sorted(page.upserted),
        "deleted": [*page.deleted, *page.unmatched],
    }
    return {
        "values": values,
        "delivered": delivered,
        "finish": finish,
        "drained": drain.is_set(),
        "skipped": not outcomes and page.kind != "reconcile",  # nothing on the page was the edge's
    }


async def _failures(spec, each: dict, page: Page, outcomes: dict, keys_io) -> dict:
    """Move each touched key's record (§9's transition table), write the
    failure index's delta, and report the outcome counts' transitions and
    the bounds the commit lowers or accumulates."""

    deploy, forced = int(each["deploy"]), int(each.get("forced_pos") or 0)
    retries = int(each.get("retries") or 0)
    records, transitions = {}, Counter()
    upsert_keys, upsert_records, removes = [], [], []
    for key, outcome in sorted(outcomes.items()):
        prior = page.priors.get(key)
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
    index = KeyIndex(keys_io, None, IndexState.from_json(each["failures"]))
    files, _ = await index.resolve(
        SortedEntries.of(upsert_keys, upsert_records, removes),
        commit_number=int(each["commit_number"]),
        attempt=spec["attempt"],
        generation=int(spec.get("generation") or 0),
        exact=True,
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
    if page.kind == "retry":
        # The walked range's records, as this page leaves them: the pass's accumulators (§9).
        walked = [records[k] if k in records else r for k, r in page.walked.items()]
        report["range"] = dict(zip(("due", "deploy_min"), minima(walked), strict=True))
    elif pass_after is not None:
        bound = key_bytes(pass_after)
        behind = [r for k, r in records.items() if key_bytes(k) <= bound]
        report["fold"] = dict(zip(("due", "deploy_min"), minima(behind), strict=True))
    return report
