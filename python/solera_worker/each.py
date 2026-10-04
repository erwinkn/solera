"""Per-key incremental: an asset written for one key, run over a batch of keys
(docs/per-key-processing.md §5, §9).

A batch is either the changes of the input's pass (`changes`) or the
failed keys's keys that are due again (`retry`). Each key is one call,
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
from solera.stores import Keys, Patch, SourceBehind, missing_keys
from solera.tasks import Tasks

from .sources import reader

WALK = 100  # failure records walked per retry batch, at most, for each key it may take
INTERRUPTED = "interrupted"  # a key a drain stopped: canceled or timed out once the result is sealed
LOOKAHEAD = 100_000  # index entries a batch examines at most, to fill itself and to prove it final
CHANGE = ("added", "updated", "removed", None)  # `KeyIndex.changes` classes, by number; 3 is neither


@dataclass
class Batch:
    """The keys one attempt reads from a keyed incremental input, filtered
    by its patterns. `deleted` are keys gone upstream and `unmatched` keys
    that stopped matching the patterns: the consumer's outputs drop both.
    `updated`: the keys of `upserted` the consumer held at its position (K44);
    the others are added.
    A per-key batch also has a `kind` (§9) — the input's `changes`, a
    `retry` of failed keys, or the `reconcile` after a full pass — and the
    failure records it read."""

    upserted: dict[str, int]  # key -> the generation of its upstream entry: its version
    deleted: list[str]
    after: str | None  # where the batch, or the retry walk, ended (None: the pass is done)
    read: int = 0  # keys examined before the patterns filtered them
    unmatched: list[str] = field(default_factory=list)
    kind: str = "changes"
    walked: dict[str, Record] = field(default_factory=dict)  # retry: every record walked
    priors: dict[str, Record] = field(default_factory=dict)  # the touched keys' failure records
    covers: bool = False  # a keys= selection past `next`: nothing it did not name is left (K45)
    updated: set[str] = field(default_factory=set)


class _Abort(Exception):
    def __init__(self, error: BaseException):
        self.error = error


async def _fill(chunk, start: bytes | None, limit: int, kind) -> tuple[list, str | None, int]:
    """A batch of `limit` entries that `kind` takes, read ahead past the ones
    it does not: `chunk(after, n)` returns `(entries, next)` — at most `n`
    entries as `(key, generation, deleted)` in key order past `after`,
    and where to go on (None: exhausted). Past a full batch it looks on for
    one more entry it takes, so that a batch is `final` exactly when nothing
    follows and no pass ends on an empty batch (§5).

    Entries are read a batch's worth and one more at a time — never just what
    the batch still lacks, so a sparse pattern costs scans in proportion to
    the entries it passes over, divided by the batch — and at most
    `LOOKAHEAD` of them: past that the batch goes as it is — not
    final, not full, perhaps empty (then its attempt is skipped, the producer
    not called). The next batch starts after the last entry examined, so no
    entry is read twice. Returns the batch's entries, where the next batch
    starts (None: this one is final), and how many entries were read."""

    batch, cursor, read, last = [], start, 0, None
    while True:
        entries, nxt = await chunk(cursor, limit + 1)
        for n, entry in enumerate(entries, 1):
            taken = kind(entry)
            if taken is not None:
                if len(batch) == limit:  # one more is taken: the batch is full, not final
                    return batch, key_str(last), read
                batch.append((taken, entry))
            read, last = read + 1, entry[0]
            if read >= LOOKAHEAD and (n < len(entries) or nxt is not None):
                return batch, key_str(last), read  # examined enough: the rest is the next batch's
        if nxt is None:
            return batch, None, read
        cursor = nxt


async def _changes(index: KeyIndex, first: int, last: int, after, n: int, ahead: dict):
    """A page of `changes(first -> last)` past `after`, read-ahead applied:
    `(key, generation, deleted, class)` entries, and the cursor."""

    lower = {key_bytes(k): (int(g), bool(live)) for k, (g, live) in ahead.items()}
    pages = index.changes(first, last, after=after, limit=n, lower=lower)
    try:
        page = await anext(pages)
    finally:
        await pages.aclose()
    return list(zip(page.keys, page.generations, page.deleted, page.classes, strict=True)), page.cursor


async def read_batch(pin: dict, keys_io) -> Batch:
    """An Incremental batch of a keyed upstream, as the spec pins it: the
    keys= override, a full pass's batch, a delta pass's changes — all
    filtered by the input's patterns (per-key §11), read ahead past keys they
    leave out until the batch holds `batch_size` keys or the pass runs
    out — or a pattern change's diff of the index as of its pattern change: the keys whose
    membership changed. Each key is classed against the consumer's position
    (K44): a delta's net change, a full pass's keys all added. A pure
    function of the pin: it reads the index through `KeyIndex.page`,
    `changes` and `lookup` only, so the engine can run it on its own copies
    to serve the same batch."""

    ch = pin["batch"]
    index = KeyIndex(keys_io, None, IndexState.from_json(pin["index"]))
    limit = int(ch.get("limit") or 1)
    start = key_bytes(ch["after"]) if ch.get("after") is not None else None
    # What keys= runs read past `next` (K45): key -> the latest upstream generation one
    # read it at, and whether it was delivered live. Not delivered again unless changed since.
    ahead = pin.get("ahead") or {}
    read_at = {k: int(g) for k, (g, _) in ahead.items()}

    snapshot = ch.get("snapshot")  # a full pass reads its start, a reserved endpoint (D93)

    async def whole(after, n):
        keys, generations, _, nxt = await index.page(after, n, at=snapshot)
        return [(k, g, 0, 0) for k, g in zip(keys, generations, strict=True)], nxt

    async def delta(after, n):
        return await _changes(index, int(ch["from"]), int(ch["to"]), after, n, ahead)

    def batch_of(page, after, read) -> Batch:
        upserted = {key_str(e[0]): e[1] for kind, e in page if kind in ("added", "updated")}
        updated = {key_str(e[0]) for kind, e in page if kind == "updated"}
        deleted = [key_str(e[0]) for kind, e in page if kind == "removed"]
        return Batch(upserted, deleted, after, read, updated=updated)

    if "pattern_change" in ch:
        old, new = Matcher(ch["pattern_change"]["from"]), Matcher(ch["pattern_change"]["to"])

        def changed(entry):
            key = key_str(entry[0])
            before, now = old(key), new(key)
            if now and not before and read_at.get(key, -1) >= entry[1]:
                return None  # a selection merged it meanwhile, at this version
            return "added" if now and not before else "removed" if before and not now else None

        page, after, read = await _fill(whole, start, limit, changed)
        upserted = {key_str(e[0]): e[1] for kind, e in page if kind == "added"}
        unmatched = [key_str(e[0]) for kind, e in page if kind == "removed"]
        return Batch(upserted, [], after, read, unmatched=unmatched)
    taken = Matcher(pin.get("patterns"))
    if "keys" in ch and "from" in ch:
        # A keys= selection of a plain input past its snapshot: the named keys' changes
        # past `next` its read-ahead lacks, and whether any it did not name are left.
        named, page, left, read = {str(k) for k in ch["keys"]}, [], False, 0
        after = None
        while int(ch["from"]) <= int(ch["to"]):
            entries, after = await delta(after, 1000)
            for entry in entries:
                key, kind, read = key_str(entry[0]), CHANGE[entry[3]], read + 1
                if not taken(key) or kind is None:
                    continue
                if key not in named:
                    left = True
                else:
                    page.append((kind, entry))
            if after is None:
                break
        batch = batch_of(page, None, read)
        batch.covers = not left
        return batch
    if "keys" in ch:  # a run's keys= override: each named key as the upstream holds it, or removed (R2)
        named = sorted({str(k) for k in ch["keys"]})
        found = await index.lookup([key_bytes(k) for k in named], at=snapshot)
        upserted = {key_str(k): generation for k, (generation, _) in found.items()}
        # A named key the upstream has not is removed (R2) — but not within a full pass,
        # whose consumer holds only what the pass delivered: never there, never removed.
        gone = (
            []
            if ch.get("scan") and not ch.get("removes")
            else [k for k in named if k not in upserted and taken(k)]
        )
        upserted = {k: g for k, g in upserted.items() if taken(k)}
        updated = set()
        if ch.get("scan"):  # within a full pass: a key it delivered at this version is not delivered twice
            walked = ch.get("walked")
            upserted = {
                k: g
                for k, g in upserted.items()
                if read_at.get(k, -1) < g and not (walked and k <= walked["at"] and g <= walked["generation"])
            }
        elif upserted:  # merged while a pattern change is under way: held if the old patterns took it then
            held = ch["held_at"]
            had = Matcher(held["patterns"])
            was = await index.lookup([key_bytes(k) for k in upserted if had(k)], at=held["next"])
            updated = {key_str(k) for k in was}
        batch = Batch(upserted, gone, None, len(named), updated=updated)
        if ch.get("scan"):  # a full pass's delivery: whether it leaves any key undelivered (K45)
            batch.covers = await _covers(index, taken, set(named), read_at, ch.get("walked"), snapshot)
            for held in ch.get("held") or ():  # and leaves nothing its reconcile would remove
                if not batch.covers:
                    break
                held = KeyIndex(keys_io, None, IndexState.from_json(held))
                batch.covers = await _holds_only(held, index, taken, set(named))
        return batch

    def kind(entry):
        """A full pass's key is added, unless read ahead within it at this
        version; a delta's is its net change, `changes` applying the read-ahead."""

        key = key_str(entry[0])
        if not taken(key) or (ch.get("full") and read_at.get(key, -1) >= entry[1]):
            return None
        return CHANGE[entry[3]]

    page, after, read = await _fill(whole if ch.get("full") else delta, start, limit, kind)
    return batch_of(page, after, read)


async def _covers(index, taken, named: set[str], read_at: dict, walked: dict | None, at=None) -> bool:
    """Whether every key of the pass's snapshot (`at`) under the patterns has
    been delivered within it: named now, read ahead at or after its version,
    or walked by the pass's own batches (at or before `walked["at"]`, at a
    generation they read). A key removed after the snapshot is the next
    delta's, which delivers its removal."""

    after = None
    while True:
        keys, generations, _, after = await index.page(after, 1000, at=at)
        for k, generation in zip(keys, generations, strict=True):
            key = key_str(k)
            if not taken(key) or key in named or read_at.get(key, -1) >= generation:
                continue
            if walked is not None and key <= walked["at"] and generation <= walked["generation"]:
                continue
            return False
        if after is None:
            return True


async def _holds_only(held, index, taken, named: set[str]) -> bool:
    """Whether a per-key output (or its failed keys) holds no key its reconcile
    would remove — gone upstream or left out by the patterns — but those
    this run names, which it removes itself (R2)."""

    after = None
    while True:
        keys, _, _, after = await held.page(after, 1000)
        rest = [k for k in keys if key_str(k) not in named]
        if any(not taken(key_str(k)) for k in rest):
            return False
        if rest and len(await index.lookup(rest)) < len(rest):
            return False
        if after is None:
            return True


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
    head = KeyIndex(keys_io, None, IndexState.from_json(pin.get("head") or pin["index"]))
    held = await head.lookup([key_bytes(k) for k in missing])
    for k in missing:
        if key_bytes(k) in held:
            raise SourceBehind(f"{output}: the source index says {k}@{expected[k]} but the source has no {k}")
    return missing


async def read_each_batch(spec: dict, pin: dict, keys_io) -> Batch:
    each = pin["each"]
    failures = KeyIndex(keys_io, None, IndexState.from_json(each["failures"]))
    if each["kind"] == "reconcile":
        return await _reconcile_batch(spec, pin, keys_io, failures)
    if each["kind"] != "retry":
        batch = await read_batch(pin, keys_io)
        touched = [key_bytes(k) for k in [*batch.upserted, *batch.deleted, *batch.unmatched]]
        # A start-over reads no prior record: its failed keys start over (K47).
        priors = await failures.lookup(touched) if touched and not each.get("start_over") else {}
        batch.priors = {key_str(k): Record.decode(p) for k, (_, p) in priors.items()}
        return batch
    # A retry batch: walk the failed keys from the pass's place, taking the
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
    # A due key written since it failed is left to the next delta only if that delta
    # delivers it: a write that nets out (v1 -> v2 -> v1) delivers nothing.
    moved = [k for k in due if (e := current.get(key_bytes(k))) is not None and e[0] != walked[k].upstream]
    delta, owed = pin["batch"]["retry"].get("delta"), set()
    if moved and delta is not None:
        index = KeyIndex(keys_io, None, IndexState.from_json(delta["index"]))
        lower = {key_bytes(k): (int(g), bool(live)) for k, (g, live) in (delta.get("ahead") or {}).items()}
        async for page in index.changes(
            int(delta["from"]), int(delta["to"]), keys=[key_bytes(k) for k in moved], lower=lower
        ):
            owed |= {
                key_str(k) for k, c in zip(page.keys, page.classes, strict=True) if CHANGE[c] is not None
            }
    upserted, deleted, unmatched = {}, [], []
    for key in due:
        entry = current.get(key_bytes(key))
        if not taken(key):
            unmatched.append(key)  # no longer one of the input's keys: its outputs and record go
        elif entry is None:
            deleted.append(key)  # gone upstream: its outputs and its record go
        elif key not in owed:
            upserted[key] = entry[0]  # at its version now
        # else: the next delta brings it, at its new version
    batch = Batch(
        upserted,
        deleted,
        end,
        unmatched=unmatched,
        kind="retry",
        walked=walked,
        priors={k: walked[k] for k in due},
    )
    if end is None and pin.get("cover"):  # the pass's last batch: is anything past the snapshot left?
        batch.covers = await _retry_covers(pin["cover"], taken, upserted, set(deleted), keys_io)
    return batch


async def _retry_covers(cover: dict, taken, upserted: dict, deleted: set, keys_io) -> bool:
    """Whether every key the patterns take changed past the snapshot has been
    delivered: read ahead since its change, or read by this batch (K47: a
    retry pass that leaves nothing uncovered collapses the record)."""

    index = KeyIndex(keys_io, None, IndexState.from_json(cover["index"]))
    first, last, after = int(cover["from"]), int(cover["to"]), None
    while True:
        entries, after = await _changes(index, first, last, after, 1000, cover.get("ahead") or {})
        for k, generation, removed, change in entries:
            key = key_str(k)
            if not taken(key) or CHANGE[change] is None:
                continue
            if (removed and key in deleted) or (not removed and upserted.get(key, -1) >= generation):
                continue
            return False
        if after is None:
            return True


async def _reconcile_batch(spec: dict, pin: dict, keys_io, failures: KeyIndex) -> Batch:
    """After a full pass: the next `limit` keys the asset's outputs or its
    failed keys hold, and which of them the input no longer has — gone
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
    return Batch(
        {},
        deleted,
        end,
        unmatched=unmatched,
        kind="reconcile",
        priors={key_str(k): Record.decode(p) for k, (_, p) in priors.items()},
    )


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
    batch = await read_each_batch(spec, pin, keys_io)
    timeline.add("loaded", param, len(batch.upserted))
    ref = Ref.from_json(pin["ref"])
    store = reader(project, ref)
    t = project.hints[asset.name].get(param)
    loaded = (
        await ctx._observed.load(store, ref, dict[str, t], Keys(batch.upserted)) if batch.upserted else {}
    )
    for key in await gone_since(
        ref.output, None, loaded, batch.upserted, pin, keys_io
    ):  # by key: the mapping's
        del batch.upserted[key]
        batch.deleted.append(key)
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
        # Every key of the batch has an outcome before its position moves past it.
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
        """The batch's failure delta, key outcomes and counts, with interrupted
        keys made what `cancel` — the record the result is sealed with — says:
        timed out for a timeout, canceled otherwise (lifecycle.md §2.2)."""

        made = "timed_out" if cancel is not None and cancel.reason == "timeout" else "canceled"
        final = {k: Outcome(made, o.upstream) if o.kind == INTERRUPTED else o for k, o in outcomes.items()}
        failures = await _failures(spec, each, batch, final, keys_io)
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
        "kind": batch.kind,
        "after": batch.after,
        "upserted": sorted(batch.upserted),
        "deleted": [*batch.deleted, *batch.unmatched],
    }
    if batch.covers:  # nothing it did not take is left undelivered: the record collapses (K45, K47)
        delivered["covers"] = True
    return {
        "values": values,
        "delivered": delivered,
        "finish": finish,
        "drained": drain.is_set(),
        "skipped": not outcomes and batch.kind != "reconcile",  # nothing on the batch was the input's
    }


async def _failures(spec, each: dict, batch: Batch, outcomes: dict, keys_io) -> dict:
    """Move each touched key's record (§9's transition table), write the
    failed keys's delta, and report the outcome counts' transitions and
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
    state = IndexState.from_json(each["failures"])
    if each.get("start_over"):  # the commit replaces the index: resolved against an empty one
        state = IndexState(prefix=state.prefix)
    index = KeyIndex(keys_io, None, state)
    files, _ = await index.resolve(
        SortedEntries.of(upsert_keys, upsert_records, removes),
        commit_number=int(each["commit_number"]),
        attempt=spec["attempt"],
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
        walked = [records[k] if k in records else r for k, r in batch.walked.items()]
        report["range"] = dict(zip(("due", "deploy_min"), minima(walked), strict=True))
    elif pass_after is not None:
        bound = key_bytes(pass_after)
        behind = [r for k, r in records.items() if key_bytes(k) <= bound]
        report["fold"] = dict(zip(("due", "deploy_min"), minima(behind), strict=True))
    return report
