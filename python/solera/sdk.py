"""Declarations a project file imports. No engine code lives here (§1, §11)."""

from __future__ import annotations

import datetime as dt
import hashlib
import inspect
import json
import os
import re
import sys
import types
import typing
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar
from urllib.parse import quote, unquote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

from .build import identity as build_identity
from .errors import describe as describe_errors

NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,127}$")
DEFAULT_STORE = "default"
KEYS = "<keys>"  # the key of a keyed output: its value is a dict[str, Any]
MAX_PARTITION_KEYS = 100_000  # enumerated keys of one dimension, at most: past it, an error
CLEANUP_AFTER = dt.timedelta(days=7)  # a whole output's data stays this long once removed or moved (D145)


class RegistrationError(ValueError):
    """A project declaration violates §11."""


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _jsonable(value: Any, where: str) -> Any:
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise RegistrationError(f"{where} must be JSON-serializable: {error}") from error
    return value


def _tags(tags: dict | None, where: str) -> dict[str, str]:
    """Labels to find things by (§7): short names without `=`, string values."""

    tags = dict(tags or {})
    for key, value in tags.items():
        if not isinstance(key, str) or not key or "=" in key or len(key) > 64:
            raise RegistrationError(f"{where}: tag names are 1–64 characters without '='")
        if not isinstance(value, str) or len(value) > 256:
            raise RegistrationError(f"{where}: tag {key!r} needs a string value of at most 256 characters")
    return dict(sorted(tags.items()))


# ---------------------------------------------------------------------------
# Refs (§3)
# ---------------------------------------------------------------------------

_REF_KINDS: dict[str, type[Ref]] = {}


@dataclass(frozen=True)
class Ref:
    """A self-contained pointer into a store (§3), and its version: the
    `generation` of the write that made it (docs/versions.md) — the attempt's
    generation, or a source commit's event counter. A store builds a ref
    without it; the worker sets it."""

    output: str
    store: str
    handle: Any
    partition: str = ""
    generation: int = 0
    meta: dict = field(default_factory=dict)

    kind: ClassVar[str | None] = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        _REF_KINDS[cls.kind] = cls

    def to_json(self) -> dict:
        meta = dict(self.meta)
        if self.kind:
            meta["ref"] = self.kind
        return {
            "output": self.output,
            "store": self.store,
            "handle": self.handle,
            "partition": self.partition,
            "generation": self.generation,
            "meta": meta,
        }

    @staticmethod
    def from_json(data: dict) -> Ref:
        meta = dict(data.get("meta") or {})
        cls = _REF_KINDS.get(meta.pop("ref", None), Ref)
        return cls(
            output=data["output"],
            store=data["store"],
            handle=data["handle"],
            partition=data.get("partition", ""),
            generation=int(data.get("generation") or 0),
            meta=meta,
        )


@dataclass(frozen=True)
class ObjectRef(Ref):
    """A FileStore or S3Store ref: where the partition's objects live."""

    kind: ClassVar[str | None] = "object"

    @property
    def path(self) -> str:
        return self.handle["path"]


@dataclass(frozen=True)
class TableRef(Ref):
    """A Postgres ref: a table plus the partition and key columns (§3)."""

    kind: ClassVar[str | None] = "table"

    @property
    def table(self) -> str:
        return self.handle["table"]

    @property
    def where(self) -> dict:
        return self.handle.get("where") or {}

    @property
    def commit_number(self) -> int | None:
        return self.handle.get("commit_number")

    def where_sql(self) -> str:
        clauses = [f"{_ident(k)} = {_literal(v)}" for k, v in sorted(self.where.items())]
        if self.commit_number is not None:
            clauses.append(f"_commit <= {int(self.commit_number)}")
        return " AND ".join(clauses) or "TRUE"

    def sql(self) -> str:
        return f"SELECT * FROM {self.table} WHERE {self.where_sql()}"


def _ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return f'"{name}"'


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def _payload_type(t: Any) -> Any:
    """What a producer's return annotation says its store receives: nothing
    known when it is, or may be, a `Result` or a `Patch` — envelopes around
    a payload, not its type (§4)."""

    from .stores import Patch

    args = typing.get_args(t) if typing.get_origin(t) in (typing.Union, types.UnionType) else (t,)
    if any(isinstance(a, type) and issubclass(a, (Result, Patch)) for a in args):
        return None
    return t


def _load_intent(input, annotation) -> str:
    """What an input receives: its store's data ("data"), or a `Ref` to it
    ("ref") — decided once, from its annotation, and carried in its pin so the
    worker that loads it and the engine that reads ahead for it agree."""

    t = (dict_arg(annotation) or annotation) if input.kind == "in" else annotation  # a fan-in: by partition
    return "ref" if t is not None and is_ref_type(t) else "data"


def is_ref_type(t: Any) -> bool:
    return inspect.isclass(t) and issubclass(t, Ref)


# ---------------------------------------------------------------------------
# Outputs and sources (§2, §5)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Migration:
    """Schema, not data: applied by the output's store before the first write
    of an attempt, recorded in the store's own ledger (§4)."""

    name: str
    payload: Any


class Output:
    """A named slot on a store (§2). `incremental=True` emits a per-commit
    delta; `key=` implies it. Public `mode=` is gone (§2.1).

    `keyed=True` makes the value a `dict[str, Any]`, one entry per key;
    `key="id"` makes it rows, keyed by their `id` column. `cleanup_after`
    (a timedelta) overrides its store's: how long the data of this output,
    once removed or moved to another store, stays before a cleanup task
    deletes it (docs/glossary.md)."""

    is_dynamic_partitions = False

    def __init__(
        self,
        name: str | None = None,
        store: str | None = None,
        key: str | None = None,
        incremental: bool | None = None,
        migrations: tuple | list = (),
        keyed: bool = False,
        cleanup_after: dt.timedelta | None = None,
        **config: Any,
    ):
        if cleanup_after is not None and (
            not isinstance(cleanup_after, dt.timedelta) or cleanup_after < dt.timedelta(0)
        ):
            raise RegistrationError(f"Output {name or '?'}: cleanup_after= is a timedelta of zero or more")
        if "mode" in config:
            raise RegistrationError(
                f"Output {name or '?'}: mode= was removed; declare incremental= instead (§2.1)"
            )
        if keyed:
            if key is not None:
                raise RegistrationError(
                    f"Output {name or '?'}: keyed=True takes a dict[str, Any]; key= is for rows"
                )
            key = KEYS
        if incremental is None:
            incremental = key is not None
        elif not incremental and key is not None:
            raise RegistrationError(f"Output {name or '?'}: key= implies incremental=True (§2.1)")
        self.name, self.store = name, store
        self.key, self.incremental = key, bool(incremental)
        self.config = config
        self.migrations = tuple(migrations)
        self.cleanup_after = cleanup_after

    def spec(self, default_name: str) -> dict:
        name = self.name or default_name
        return {
            "name": name,
            "store": self.store or DEFAULT_STORE,
            "key": self.key,
            "incremental": self.incremental,
            "migrations": [m.name for m in self.migrations],
            "config": self.config,
            "dynamic_partitions": self.is_dynamic_partitions,
            "cleanup_after": None if self.cleanup_after is None else self.cleanup_after.total_seconds(),
        }


class DynamicPartitions(Output):
    """An output whose value is a list of partition keys (§2, §7)."""

    is_dynamic_partitions = True

    def __init__(self, name: str | None = None, store: str | None = None, **config: Any):
        super().__init__(name, store, key="<partitions>", incremental=True, **config)


class Source:
    """An output with no producer (§5). Its data is loaded through its
    store's `serve` or a function (`@source`), which says the version it
    served (docs/stores.md, "Sources: how data is loaded"); `version` is the
    loader's own. With `observe=Every(…)`, a subclass's `observe(ctx,
    …resources)` is called on that schedule and commits to the source: sugar
    for a sensor `{name}.observe` (docs/lifecycle.md §11)."""

    def __init__(
        self,
        name: str,
        store: str | None = None,
        key: str | None = None,
        *,
        observe: Every | None = None,
        executor: Any = None,
        timeout: float = 60,
        version: str = "1",
        **handle: Any,
    ):
        if not NAME.fullmatch(name):
            raise RegistrationError(f"Invalid source name: {name!r}")
        self.name, self.store, self.key, self.handle = name, store, key, dict(handle)
        self.version, self.loader = str(version), None  # a function's, `@source`
        self.observing, self.executor, self.timeout = observe, executor, timeout
        if observe is not None and type(self).observe is Source.observe:
            raise RegistrationError(f"Source {name!r}: observe= needs a subclass that defines observe()")

    def observe(self, ctx) -> Any:
        """`str`: an unkeyed source's version; a map: the full key map, each
        key to its version (docs/versions.md §2); `Observed`: a patch and a
        cursor; `None`: nothing changed."""

        raise NotImplementedError

    def _sensor(self) -> Sensor:
        def body(ctx, **resources):
            value = self.observe(ctx, **resources)
            if inspect.isawaitable(value):  # the host runs it, under the tick's timeout

                async def observed():
                    return _observed(self.name, await value)

                return observed()
            return _observed(self.name, value)

        params = [p for p in inspect.signature(self.observe).parameters if p != "ctx"]
        return Sensor(
            body,
            name=f"{self.name}.observe",
            every=self.observing,
            commits=[self.name],
            executor=self.executor,
            timeout=self.timeout,
            params=params,
            code=type(self).observe,
        )

    @classmethod
    def _from_dynamic_partitions(cls, ps: DynamicPartitions) -> Source:
        if ps.name is None:
            raise RegistrationError("A source DynamicPartitions requires an explicit name")
        source = cls(ps.name, store=ps.store, key="<partitions>", **{})
        source.handle = {"name": ps.name}
        return source

    def head(self) -> Ref:
        return Ref(
            output=self.name,
            store=self.store or DEFAULT_STORE,
            handle={"name": self.name, **self.handle},
            partition="",
            meta={"source": True},
        )


@dataclass(frozen=True)
class Loaded:
    """What a source's loader served: a key's row, or an unkeyed source's
    value, with the version it was served at — the source's own word (an
    etag, an `updated_at`). docs/stores.md, "Sources: how data is loaded"."""

    value: Any
    version: str | None = None


def source(fn=None, *, key: str | None = None, version: str = "1"):
    """A source loaded through a function, `async def name(keys, ctx)`.
    Keyed, it is called with the keys a batch reads and returns `{key:
    Loaded(row, version=…)}`: a key it leaves out was observed absent.
    Unkeyed, it is called with None and returns the value, or
    `Loaded(value, version=…)`. `version` is the loader's own: bump it when
    what it serves for the same data changes."""

    def wrap(f):
        made = Source(f.__name__, key=key, version=version)
        made.loader = f
        return made

    return wrap(fn) if fn is not None else wrap


class _Unset:
    def __repr__(self):
        return "UNSET"


# `cursor` absent means "leave the committed cursor"; explicit None clears it.
UNSET = _Unset()


@dataclass(frozen=True)
class Result:
    """Multi-output return: `outputs` plus an optional `cursor` (§2), and
    `metadata` per output to record with the versions written (§7)."""

    outputs: dict[str, Any]
    cursor: Any = UNSET
    metadata: dict[str, dict] | None = None


# ---------------------------------------------------------------------------
# Inputs (§5)
# ---------------------------------------------------------------------------


class In:
    """A whole input: the value (or ref) of an output at its pinned head.
    Over upstream dimensions the consumer lacks it fans in, as
    `dict[partition, value]` over them (§7); `all_partitions=True` reads
    every partition of the upstream so, the shared dimensions too, with no
    projection of the consumer's partition."""

    kind = "in"

    def __init__(self, output: str | None = None, *, meta: dict | None = None, all_partitions: bool = False):
        self.output, self.meta = output, _jsonable(meta, "input meta") if meta is not None else None
        self.all_partitions = bool(all_partitions)

    def spec(self, param: str) -> dict:
        spec = {"kind": self.kind, "output": self.output or param, "meta": self.meta}
        if self.all_partitions:
            spec["all_partitions"] = True
        return spec


class Incremental(In):
    """An incremental input: the position-planned changes since its last
    pass (§5, §6). On a keyed upstream, `include` and `exclude` select the
    keys it takes by name (`solera.patterns`, docs/per-key-processing.md
    §11).

    `each=True` makes it per-key incremental (docs/per-key-processing.md
    §5): the asset is written for one key, the parameter receives one key's
    value, `ctx.key` names it, and every output is keyed by it. Two knobs
    (D111): `batch_size` keys (10,000 by default) make one attempt and one
    commit, and `concurrency` of them (64 by default) run at once within a
    partition. The asset's `concurrency=` caps its partitions at once, each
    cap counting its own unit.
    A key whose call raises is recorded in the asset's failure index and
    retried by its class (§8, §9); it never blocks the others."""

    kind = "incremental"

    def __init__(
        self,
        output: str | None = None,
        batch_size: int = 10_000,
        meta: dict | None = None,
        *,
        include=None,
        exclude=None,
        each: bool = False,
        concurrency: int | None = None,
    ):
        from . import patterns

        super().__init__(output, meta=meta)
        if batch_size < 1:
            raise RegistrationError("Incremental batch_size must be positive")
        if concurrency is not None and not each:
            raise RegistrationError("concurrency= is for a per-key incremental input (each=True)")
        if concurrency is not None and concurrency < 1:
            raise RegistrationError("Incremental concurrency must be positive")
        self.batch_size = batch_size
        self.each = bool(each)
        self.concurrency = (64 if concurrency is None else concurrency) if each else None
        try:
            self.patterns = patterns.spec(include, exclude)
        except (ValueError, TypeError) as error:
            raise RegistrationError(str(error)) from None

    def spec(self, param: str) -> dict:
        spec = {**super().spec(param), "batch_size": self.batch_size}
        if self.patterns is not None:
            spec["patterns"] = self.patterns
        if self.each:
            spec["each"] = {"concurrency": self.concurrency}
        return spec


@dataclass(frozen=True)
class Upstream:
    """Facts about the upstream a batch came from (§5.1): its `output`, and —
    for an unkeyed upstream — the range of upstream `commits` the batch
    covers."""

    output: str | None = None
    commits: range | None = None


@dataclass(frozen=True)
class Batch:
    """What an `Incremental` input delivered to a parameter (§5.1). The rows
    arrive as the parameter; `ctx.batch[name]` says what they are and where
    they sit in their pass:

    - `rows`: the delivered rows (the object the parameter received);
    - `added`, `updated` and `removed` (keyed upstreams): each key's change
      since the input's position, net over the batch's commits. `added`:
      absent at the position, present now; `updated`: present at both, at
      another version; `removed`: present at the position, absent now. A
      key added and removed again appears nowhere; one removed and added
      back is updated. The rows are those of `added` and `updated`. A full
      pass delivers every key as added. On a store of current rows only,
      rows show the store's newest state: a key in `added` or `updated`
      may come without a row if it was removed since — its removal follows
      in a later batch — and a key changed after the pass's version may
      arrive once more as updated (D100);
    - `full`: the batch is part of a full pass — the whole head as of the
      pass's start (its snapshot: what changes after comes as the next
      delta) after a
      reset, not a delta;
    - `index`: this batch's 0-based index in its pass, exact;
    - `count`: how many batches the pass was planned to take when it
      started, by `batch_size`. Exact when the input has no patterns and the
      upstream's key count is exact; otherwise an estimate, and the pass
      may take fewer batches, or more;
    - `first`: `index == 0` — on a full pass, the moment to start over;
    - `final`: no batch of this pass follows, known from the pass itself
      running out, never from `count`. Batches are formed from the keys the
      input's patterns take, so every batch holds some: `final` is always
      on a real batch, and a pass that takes no key at all does not call
      the producer — but for a full pass, which always reaches it, as one
      empty batch that is `full`, `first` and `final`: starting over must
      happen;
    - `upstream`: facts about the upstream (`Upstream`);
    - `served` (a source's): the version each key was served at, as its
      loader said; None for a key it did not have. Consumed by nothing yet:
      the observed set's rebuild records it.

    A consumer that rebuilds starts over when `full and first`, appends every
    batch, and swaps or finalizes on `final`. One that keeps a total moves it
    by the changes, starting from what it holds (`ctx.load()`):

        before = 0 if batch.full and batch.first else (await ctx.load())["count"]
        return {"count": before + len(batch.added) - len(batch.removed)}"""

    rows: Any = ()
    added: tuple = ()
    updated: tuple = ()
    removed: tuple = ()
    full: bool = False
    index: int = 0
    count: int = 1
    final: bool = True
    upstream: Upstream = field(default_factory=Upstream)
    served: dict = field(default_factory=dict)

    @property
    def first(self) -> bool:
        return self.index == 0


# ---------------------------------------------------------------------------
# Partition dimensions (§7)
# ---------------------------------------------------------------------------


class StaticPartitions:
    def __init__(self, keys: list[str]):
        if not keys or len(set(keys)) != len(keys):
            raise RegistrationError("StaticPartitions requires a nonempty list of unique keys")
        self.keys = [str(k) for k in keys]

    def spec(self) -> dict:
        return {"kind": "static", "keys": self.keys}

    def __eq__(self, other):
        return type(other) is StaticPartitions and other.keys == self.keys

    def __hash__(self):
        return hash(tuple(self.keys))


_DURATION = re.compile(r"^(\d+)(m|h|d|w)$")
_DURATION_FORMATS = {"m": "%Y-%m-%dT%H:%M", "h": "%Y-%m-%dT%H:00", "d": "%Y-%m-%d", "w": "%Y-%m-%d"}


def _parse_dt(value: str, zone: ZoneInfo) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=zone)
    return parsed.astimezone(zone)


def _duration(value: str) -> dt.timedelta | None:
    match = _DURATION.fullmatch(value)
    if not match:
        return None
    n, unit = int(match.group(1)), match.group(2)
    return {
        "m": dt.timedelta(minutes=n),
        "h": dt.timedelta(hours=n),
        "d": dt.timedelta(days=n),
        "w": dt.timedelta(weeks=n),
    }[unit]


class TimePartitions:
    """Half-open `[start, start+every)` windows aligned in `timezone` (§7)."""

    def __init__(
        self,
        start: str,
        every: str,
        *,
        end: str | None = None,
        end_offset: str | None = None,
        timezone: str = "UTC",
        format: str | None = None,
    ):
        try:
            self.zone = ZoneInfo(timezone)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise RegistrationError(f"Unknown partition timezone: {timezone}") from error
        self.start, self.every = start, every
        self.duration = _duration(every)
        if self.duration is None:
            try:
                croniter(every)
            except Exception as error:
                raise RegistrationError(f"Invalid TimePartitions 'every': {every!r}") from error
        if end_offset is not None and _duration(end_offset) is None:
            raise RegistrationError(f"Invalid end_offset duration: {end_offset!r}")
        self.end, self.end_offset, self.timezone = end, end_offset, timezone
        if format is None:
            if self.duration is None:
                raise RegistrationError("TimePartitions with a cron 'every' requires format=")
            format = _DURATION_FORMATS[re.fullmatch(r"\d+(m|h|d|w)", every).group(1)]
        self.format = format

    def spec(self) -> dict:
        return {
            "kind": "time",
            "start": self.start,
            "every": self.every,
            "end": self.end,
            "end_offset": self.end_offset,
            "timezone": self.timezone,
            "format": self.format,
        }

    def _horizon(self, as_of: dt.datetime) -> tuple[dt.datetime, dt.datetime]:
        """`(start, horizon)` in the partitions' zone: windows ending at or
        before the horizon exist — `as_of` less `end_offset`, capped by `end`."""

        start = _parse_dt(self.start, self.zone)
        horizon = as_of.astimezone(self.zone)
        if self.end_offset:
            horizon -= _duration(self.end_offset)
        if self.end:
            horizon = min(horizon, _parse_dt(self.end, self.zone))
        return start, horizon

    def _complete(self, start: dt.datetime, horizon: dt.datetime) -> int:
        """How many fixed-length windows have closed by the horizon. Datetimes
        in one zone subtract and add as wall-clock times, as windows step."""

        if horizon <= start:
            return 0
        n = int((horizon - start) / self.duration)
        while n > 0 and start + n * self.duration > horizon:
            n -= 1
        while start + (n + 1) * self.duration <= horizon:
            n += 1
        return n

    def _last_fire(self, start: dt.datetime, horizon: dt.datetime) -> dt.datetime | None:
        """A cron `every`'s latest fire after `start`, at or before the horizon:
        where the last complete window ends."""

        if horizon <= start:
            return None
        fire = croniter(self.every, horizon + dt.timedelta(microseconds=1)).get_prev(dt.datetime)
        return fire if fire > start else None

    def latest(self, as_of: dt.datetime | None = None) -> str | None:
        """The last complete window's key, computed directly — never by
        listing the windows before it."""

        start, horizon = self._horizon(as_of or dt.datetime.now(dt.UTC))
        if self.duration is not None:
            n = self._complete(start, horizon)
            return self.key(start + (n - 1) * self.duration) if n else None
        fire = self._last_fire(start, horizon)
        if fire is None:
            return None
        before = croniter(self.every, fire).get_prev(dt.datetime)
        return self.key(before if before > start else start)

    def contains(self, key: str, as_of: dt.datetime | None = None) -> bool:
        """Whether `key` names a complete window: well formed, aligned, and
        between the first window and the latest."""

        try:
            begin = dt.datetime.strptime(key, self.format).replace(tzinfo=self.zone)
        except ValueError:
            return False
        if self.key(begin) != key:
            return False
        start, horizon = self._horizon(as_of or dt.datetime.now(dt.UTC))
        if begin < start:
            return False
        if self.duration is not None:
            n = round((begin - start) / self.duration)
            return start + n * self.duration == begin and begin + self.duration <= horizon
        if begin != start and not croniter.match(self.every, begin):
            return False
        return croniter(self.every, begin).get_next(dt.datetime) <= horizon

    def count(self, as_of: dt.datetime | None = None, limit: int | None = None) -> int:
        """How many windows have closed; with a cron `every`, counting stops
        past `limit`."""

        start, horizon = self._horizon(as_of or dt.datetime.now(dt.UTC))
        if self.duration is not None:
            return self._complete(start, horizon)
        n, fires = 0, croniter(self.every, start)
        while limit is None or n <= limit:
            if fires.get_next(dt.datetime) > horizon:
                break
            n += 1
        return n

    def _window_starts(self, as_of: dt.datetime, limit: int) -> list[dt.datetime]:
        start, horizon = self._horizon(as_of)
        if self.count(as_of, limit) > limit:
            raise ValueError(
                f"time partitions from {self.start} every {self.every} have more than {limit} windows: "
                "select partitions explicitly, or the latest"
            )
        if self.duration is not None:
            return [start + i * self.duration for i in range(self._complete(start, horizon))]
        starts, previous, fires = [], start, croniter(self.every, start)
        while True:
            nxt = fires.get_next(dt.datetime)
            if nxt > horizon:
                return starts
            starts.append(previous)
            previous = nxt

    def key(self, window_start: dt.datetime) -> str:
        return window_start.strftime(self.format)

    def window(self, key: str) -> tuple[str, str] | None:
        naive = dt.datetime.strptime(key, self.format).replace(tzinfo=self.zone)
        if self.duration is not None:
            return naive.isoformat(), (naive + self.duration).isoformat()
        nxt = croniter(self.every, naive).get_next(dt.datetime)
        return naive.isoformat(), nxt.isoformat()

    def keys(self, as_of: dt.datetime | None = None, limit: int = MAX_PARTITION_KEYS) -> list[str]:
        """Every complete window's key. More than `limit` is an error, never a
        silent truncation: the domain is not what fits in memory."""

        as_of = as_of or dt.datetime.now(dt.UTC)
        return [self.key(s) for s in self._window_starts(as_of, limit)]

    def __eq__(self, other):
        return type(other) is TimePartitions and other.spec() == self.spec()

    def __hash__(self):
        return hash(digest(self.spec()))


# ---------------------------------------------------------------------------
# Triggers and automations (§9)
# ---------------------------------------------------------------------------


class Automation:
    """When it fires, submit a run in §8 vocabulary: every trigger is an
    automation (`Every`, `Cron`, `OnChange`, `OnDeploy`), and an asset's
    `automations=` a list of them, any of which fires it. On an asset, it
    runs the asset; standalone (`Project(automations=)`), it names itself
    and its `targets`. `skip_missing_inputs`: skip a partition whose inputs
    have never been written (and that the run doesn't build), rather than
    run it to fail."""

    kind = ""

    def _run(
        self,
        *,
        name: str | None = None,
        targets: Any = None,
        enabled: bool = True,
        partitions: str | list[str] | None = None,
        mode: str = "incremental",
        upstream: bool = False,
        config: dict | None = None,
        keys: dict | None = None,
        tags: dict[str, str] | None = None,
        skip_missing_inputs: bool = False,
    ) -> None:
        if mode not in ("incremental", "full"):
            raise RegistrationError(f"Unknown automation mode: {mode!r}")
        self.name, self.targets = name, targets
        self.enabled, self.partitions, self.mode = enabled, partitions, mode
        self.upstream, self.config, self.keys = upstream, config, keys
        self.tags = _tags(tags, f"Automation {name or ''}".strip())
        self.skip_missing_inputs = bool(skip_missing_inputs)

    def spec(self) -> dict:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.spec()})"


class Every(Automation):
    """Every `seconds` (§9)."""

    kind = "every"

    def __init__(self, seconds: int, **run):
        if not isinstance(seconds, (int, float)) or seconds < 1:
            raise RegistrationError("Every requires an interval of at least 1 second")
        self.seconds = seconds
        self._run(**run)

    def spec(self) -> dict:
        return {"kind": "every", "seconds": self.seconds}


class Cron(Automation):
    """On a cron schedule, in `timezone` (§9)."""

    kind = "cron"

    def __init__(self, expression: str, timezone: str = "UTC", **run):
        try:
            croniter(expression)
        except Exception as error:
            raise RegistrationError(f"Invalid cron expression: {expression}") from error
        try:
            ZoneInfo(timezone)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise RegistrationError(f"Unknown cron timezone: {timezone}") from error
        self.expression, self.timezone = expression, timezone
        self._run(**run)

    def spec(self) -> dict:
        return {"kind": "cron", "expression": self.expression, "timezone": self.timezone}


class OnChange(Automation):
    """When an output it watches changes: the named ones, or, naming none,
    every input and dep of its targets (§9)."""

    kind = "onchange"

    def __init__(self, *outputs: str, **run):
        self.outputs = tuple(outputs)
        self._run(**run)

    def spec(self) -> dict:
        return {"kind": "onchange", "outputs": list(self.outputs)}


class OnDeploy(Automation):
    """Once per new deploy, for the latest deploy only (§9)."""

    kind = "ondeploy"

    def __init__(self, **run):
        self._run(**run)

    def spec(self) -> dict:
        return {"kind": "ondeploy"}


# ---------------------------------------------------------------------------
# Sensors (docs/lifecycle.md §11)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Commit:
    """A source commit a tick asks for, as the commit API takes it: an
    unkeyed source's `version`, a full `keys` map, or `upsert`/`remove`.
    A map gives each key its version; a list names keys with none
    (docs/versions.md §2)."""

    source: str
    version: str | None = None
    keys: Mapping | list | None = None
    upsert: Mapping | list | None = None
    remove: list | None = None

    def to_json(self) -> dict:
        out = {"source": self.source}
        if self.version is not None:
            out["version"] = str(self.version)
        for name in ("keys", "upsert"):
            value = getattr(self, name)
            if value is not None:
                out[name] = (
                    {str(k): str(v) for k, v in value.items()}
                    if isinstance(value, Mapping)
                    else [str(k) for k in value]
                )
        if self.remove is not None:
            out["remove"] = [str(k) for k in self.remove]
        return out


@dataclass(frozen=True)
class RunRequest:
    """A run a tick submits, as the API takes it."""

    targets: list[str] | str
    partitions: str | list[str] = "latest"
    config: dict | None = None
    keys: dict | None = None
    tags: dict[str, str] | None = None

    def to_json(self) -> dict:
        targets = [self.targets] if isinstance(self.targets, str) else list(self.targets)
        out = {"targets": targets, "partitions": self.partitions}
        for name in ("config", "keys", "tags"):
            if getattr(self, name) is not None:
                out[name] = getattr(self, name)
        return out


@dataclass(frozen=True)
class Tick:
    """What a sensor's tick found: a new `cursor`, source `commits`, `runs`
    to submit; all optional. The engine applies it all or nothing."""

    cursor: Any = UNSET
    commits: list[Commit] = field(default_factory=list)
    runs: list[RunRequest] = field(default_factory=list)

    def to_json(self) -> dict:
        out = {"commits": [c.to_json() for c in self.commits], "runs": [r.to_json() for r in self.runs]}
        if self.cursor is not UNSET:
            out["cursor"] = _jsonable(self.cursor, "A sensor cursor")
        return out


@dataclass(frozen=True)
class Observed:
    """What an observable source's `observe()` saw since its cursor."""

    upsert: Mapping | list | None = None
    remove: list | None = None
    cursor: Any = UNSET


def _observed(source: str, value: Any) -> Tick | None:
    """The `Tick` of an `observe()` result (per-key-processing.md §12)."""

    if value is None:
        return None
    if isinstance(value, Observed):
        empty = not value.upsert and not value.remove
        commits = [] if empty else [Commit(source, upsert=value.upsert or {}, remove=value.remove)]
        return Tick(cursor=value.cursor, commits=commits)
    if isinstance(value, str):
        return Tick(commits=[Commit(source, version=value)])
    if isinstance(value, Mapping):
        return Tick(commits=[Commit(source, keys=value)])
    raise TypeError(f"{source}.observe returned {type(value).__name__}: a str, a map, Observed or None")


class Sensor:
    """A check run every `every` on a sensor worker: `fn(ctx, …resources)`
    returns a `Tick` or `None`. `commits` names every source it may commit
    to; `executor` is the host: the engine's own (`Local`, the default) or a
    `Pool` of `solera_worker sensors` hosts."""

    def __init__(
        self,
        fn: Callable,
        *,
        every: Every | int | float,
        commits: list[str] | tuple = (),
        executor: Any = None,
        timeout: float = 60,
        name: str | None = None,
        params: list[str] | None = None,
        code: Callable | None = None,
    ):
        self.fn, self.name = fn, name or fn.__name__
        if not NAME.fullmatch(self.name):
            raise RegistrationError(f"Invalid sensor name: {self.name!r}")
        self.every = every if isinstance(every, Every) else Every(every)
        self.commits = [str(c) for c in commits]
        self.executor, self.timeout = executor, float(timeout)
        if self.timeout <= 0:
            raise RegistrationError(f"Sensor {self.name}: timeout must be positive")
        self.params = (
            params if params is not None else [p for p in inspect.signature(fn).parameters if p != "ctx"]
        )
        self.code = code or fn

    def placement(self) -> dict:
        from .executors import Executor, Local, Placement

        executor = self.executor or Local()
        if isinstance(executor, Executor):
            executor = executor()
        if not isinstance(executor, Placement) or executor.kind not in ("Local", "Pool"):
            raise RegistrationError(f"Sensor {self.name}: its executor is Local or a Pool")
        return executor.serialized()


def sensor(fn=None, **decl):
    def wrap(f):
        return Sensor(f, **decl)

    return wrap(fn) if fn is not None else wrap


@dataclass(frozen=True)
class Retry:
    n: int = 3
    delay: float = 1.0
    backoff: str = "exponential"

    def __post_init__(self):
        if self.n < 0 or self.delay < 0 or self.backoff not in ("exponential", "none"):
            raise RegistrationError(f"Invalid retry policy: {self}")

    def wait(self, attempt: int) -> float:
        return self.delay * (2**attempt if self.backoff == "exponential" else 1)

    def spec(self) -> dict:
        return {"n": self.n, "delay": self.delay, "backoff": self.backoff}


@dataclass(frozen=True)
class Retention:
    """How long an asset's history is kept (docs/object-store-state.md §11).

    `days`: runs older than that go. `runs`: keep the runs of the newest
    `runs` commits. Both: whichever keeps more. `forever=True` keeps everything, overriding a project default.
    Current state — heads, key indexes, cursors, positions, and the data
    in stores — never expires, and neither does a run still in progress."""

    days: float | None = None
    runs: int | None = None
    forever: bool = False

    def __post_init__(self):
        if self.forever:
            if self.days is not None or self.runs is not None:
                raise RegistrationError("Retention(forever=True) takes no days or runs")
            return
        if self.days is None and self.runs is None:
            raise RegistrationError("Retention() needs days, runs or forever=True")
        if self.days is not None and self.days <= 0:
            raise RegistrationError(f"Invalid retention days: {self.days}")
        if self.runs is not None and self.runs < 1:
            raise RegistrationError(f"Invalid retention runs: {self.runs}")

    def spec(self) -> dict:
        return {"days": self.days, "runs": self.runs, "forever": self.forever}


# ---------------------------------------------------------------------------
# Partition key encoding (§7)
# ---------------------------------------------------------------------------


def canonical_partition(dims: dict[str, dict], parts: Mapping[str, str]) -> str:
    """`"Richmond"`, or `"day=2024-01-01,site=Richmond"` sorted by dimension."""

    if len(dims) == 1:
        (name,) = dims
        return str(parts[name])
    return ",".join(f"{name}={quote(str(parts[name]), safe='')}" for name in sorted(dims))


def split_partition(dims: dict[str, dict], key: str) -> dict[str, str]:
    if len(dims) == 1:
        (name,) = dims
        return {name: key}
    parts = {}
    for segment in key.split(","):
        name, _, value = segment.partition("=")
        parts[name] = unquote(value)
    if set(parts) != set(dims):
        raise ValueError(f"Partition key {key!r} does not match dimensions {sorted(dims)}")
    return parts


# ---------------------------------------------------------------------------
# Assets (§2)
# ---------------------------------------------------------------------------


class Asset:
    def __init__(
        self,
        fn: Callable,
        *,
        outputs: Any = None,
        inputs: dict | None = None,
        deps: tuple | list = (),
        partitions: Any = None,
        executor: Any = None,
        retries: Retry | None = None,
        timeout: float = 3600,
        concurrency: int | None = None,
        version: str = "1",
        retention: Retention | None = None,
        automations: Any = (),
        aliases: tuple | list = (),
        tags: dict[str, str] | None = None,
    ):
        self.fn = fn
        self.name = fn.__name__
        self.aliases = tuple(str(a) for a in aliases)
        self.tags = _tags(tags, self.name)
        if outputs is None:
            outputs = (Output(fn.__name__),)
        elif isinstance(outputs, Output):
            outputs = (outputs,)
        self.outputs = tuple(outputs)
        self.inputs = inputs
        self.deps = tuple(deps)
        self.partitions = partitions
        self.executor = executor
        self.retries = retries or Retry()
        self.timeout = timeout
        if concurrency is not None and (
            isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1
        ):
            raise RegistrationError(
                f"{self.name}: concurrency= is a positive number of partitions, not {concurrency!r}"
            )
        self.concurrency = concurrency  # partitions running at once, across runs: the engine holds the rest
        self.version = str(version)
        self.retention = retention
        if isinstance(automations, Automation):
            automations = (automations,)
        self.automations = tuple(automations)
        for auto in self.automations:
            if not isinstance(auto, Automation):
                raise RegistrationError(
                    f"{self.name}: automations= takes Every, Cron, OnChange or OnDeploy, not {auto!r}"
                )

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)


def asset(fn=None, **decl):
    def wrap(f):
        return Asset(f, **decl)

    return wrap(fn) if fn is not None else wrap


def job(fn=None, **decl):
    decl["outputs"] = ()
    return asset(fn, **decl) if fn is not None else asset(**decl)


# ---------------------------------------------------------------------------
# Type introspection for the manifest (§11)
# ---------------------------------------------------------------------------


def type_name(t: Any) -> Any:
    if t is None or t is inspect.Parameter.empty:
        return None
    origin = typing.get_origin(t)
    if origin is not None:
        return {
            "generic": getattr(origin, "__name__", str(origin)),
            "args": [type_name(a) for a in typing.get_args(t)],
        }
    if inspect.isclass(t):
        return {"class": f"{t.__module__}.{t.__qualname__}"}
    return {"repr": str(t)}


def hints(name: str, fn: Callable) -> dict[str, Any]:
    """A producer's annotations, resolved once, at registration: one that
    does not resolve (a type never imported) is a registration error, not
    the first run's. `Project.hints` keeps them for the worker."""

    try:
        return typing.get_type_hints(fn)
    except Exception as error:
        raise RegistrationError(f"{name}: its annotations do not resolve: {error}") from error


def dict_arg(t: Any) -> Any | None:
    """`X` of a `dict[str, X]` or `Mapping[str, X]` annotation — an input by
    partition (a whole fan-in) or by key (a per-key incremental batch) — else None."""

    if typing.get_origin(t) in (dict, Mapping):
        args = typing.get_args(t)
        if len(args) == 2 and args[0] is str:
            return args[1]
    return None


# ---------------------------------------------------------------------------
# Project (§11)
# ---------------------------------------------------------------------------


def _caller_dir() -> str | None:
    """The directory of the file that called into this module."""

    frame = sys._getframe(1)
    while frame is not None and frame.f_code.co_filename == __file__:
        frame = frame.f_back
    path = frame.f_globals.get("__file__") if frame is not None else None
    return os.path.dirname(os.path.abspath(path)) if path else None


class Project:
    def __init__(
        self,
        assets: list[Asset] | None = None,
        sources: list[Source | DynamicPartitions] | None = None,
        *,
        stores: dict[str, Any] | None = None,
        default_store: Any = None,
        executors: list | None = None,
        resources: dict[str, Any] | None = None,
        automations: list[Automation] | None = None,
        sensors: list[Sensor] | None = None,
        retention: Retention | None = None,
        errors: Mapping[type, type] | None = None,
        build: str | None = None,
        name: str = "default",
    ):
        """`default_store` holds every output that names no store: unless
        given, a FileStore. A FileStore without a path keeps its data in
        `.solera/data` next to the file that builds the project, or in
        `$SOLERA_DATA`. `errors` classifies exceptions user code cannot
        subclass: `{httpx.TimeoutException: Transient}` (`solera.errors`).
        `build` names the code explicitly (else `$SOLERA_BUILD`, else the
        work tree's content: `solera.build`); it is part of the deploy."""

        from .errors import check_mapping
        from .stores import FileStore

        try:
            self.errors = check_mapping(errors)
        except ValueError as error:
            raise RegistrationError(str(error)) from None
        self.name = name
        self.retention = retention
        self.assets: dict[str, Asset] = {}
        self.stores = {DEFAULT_STORE: default_store or FileStore(), **(stores or {})}
        home = self.home = _caller_dir()
        self.build = build
        for store in self.stores.values():
            if isinstance(store, FileStore) and store.home is None:
                store.home = home
        self.executors = list(executors or [])
        self.resources = dict(resources or {})
        self.sources: dict[str, Source] = {}
        for source in sources or ():
            if isinstance(source, DynamicPartitions):
                source = Source._from_dynamic_partitions(source)
            if source.name in self.sources:
                raise RegistrationError(f"Duplicate source: {source.name}")
            self.sources[source.name] = source
        for a in assets or ():
            if not isinstance(a, Asset):
                raise RegistrationError(f"Not an asset: {a!r}")
            if a.name in self.assets:
                raise RegistrationError(f"Duplicate asset: {a.name}")
            self.assets[a.name] = a
        self.automations = list(automations or ())
        self.sensors: dict[str, Sensor] = {}
        for s in [*(sensors or ()), *(src._sensor() for src in self.sources.values() if src.observing)]:
            if not isinstance(s, Sensor):
                raise RegistrationError(f"Not a sensor: {s!r}")
            if s.name in self.sensors:
                raise RegistrationError(f"Duplicate sensor: {s.name}")
            self.sensors[s.name] = s
        self.manifest = self._build()

    @classmethod
    def from_package(cls, package: str, **kwargs) -> Project:
        import importlib

        module = importlib.import_module(package)
        found = [v for v in vars(module).values() if isinstance(v, Asset)]
        return cls(assets=found, **kwargs)

    # -- registration -----------------------------------------------------

    def _dim_spec(self, decl, asset_name) -> dict[str, dict] | None:
        if decl is None:
            return None
        named = decl if isinstance(decl, dict) else {"_": decl}
        dims = {}
        for dim_name, dim in named.items():
            name = dim_name
            if isinstance(dim, StaticPartitions | TimePartitions):
                spec = dim.spec()
            elif isinstance(dim, DynamicPartitions):
                if dim.name is None and dim_name == "_":
                    raise RegistrationError(f"{asset_name}: a DynamicPartitions dimension requires a name")
                spec = {"kind": "dynamic", "output": dim.name}
                if dim_name == "_":
                    name = dim.name
            elif isinstance(dim, Asset):
                if len(dim.outputs) != 1:
                    raise RegistrationError(
                        f"{asset_name}: partitions= asset {dim.name} must have exactly one output"
                    )
                spec = {"kind": "dynamic", "output": dim.outputs[0].name or dim.name}
                if dim_name == "_":
                    name = spec["output"]
            elif isinstance(dim, str):
                spec = {"kind": "dynamic", "output": dim}
                if dim_name == "_":
                    name = dim
            else:
                raise RegistrationError(f"{asset_name}: unknown partition dimension {dim!r}")
            if name in dims:
                raise RegistrationError(f"{asset_name}: duplicate partition dimension {name}")
            dims[name] = spec
        return dims

    def _output_table(self) -> dict[str, dict]:
        """output name -> declaration record used by registration checks."""

        table = {}
        for name, source in self.sources.items():
            table[name] = {
                "name": name,
                "asset": None,
                "source": True,
                "store": source.store or DEFAULT_STORE,
                "key": source.key,
                "incremental": source.key is not None,
                "migrations": [],
                "config": source.handle,
                "dynamic_partitions": source.key == "<partitions>",
                "dims": None,
            }
        for asset in self.assets.values():
            default = asset.name
            seen = set()
            for output in asset.outputs:
                output.name = output.name or default
                if not NAME.fullmatch(output.name):
                    # Names are letters, digits, `_.-`: never `@asset`, the namespace of
                    # failed keys (docs/per-key-processing.md §9), nor a path.
                    raise RegistrationError(f"{asset.name}: invalid output name {output.name!r}")
                if output.name in table or output.name in seen:
                    raise RegistrationError(f"Duplicate output name: {output.name}")
                seen.add(output.name)
                if output.name in self.resources:
                    raise RegistrationError(f"Output name collides with a resource: {output.name}")
                if output.store and output.store not in self.stores:
                    raise RegistrationError(f"{asset.name}: unknown store {output.store!r}")
                table[output.name] = {
                    "name": output.name,
                    "asset": asset.name,
                    "source": False,
                    "store": output.store or DEFAULT_STORE,
                    "key": output.key,
                    "incremental": output.incremental,
                    "migrations": [m.name for m in output.migrations],
                    "config": output.config,
                    "dynamic_partitions": output.is_dynamic_partitions,
                    # How long its data stays once removed or moved away; None: its store's (K25).
                    "cleanup_after": None
                    if output.cleanup_after is None
                    else output.cleanup_after.total_seconds(),
                    "output": output,
                }
        return table

    def _input(self, value, param, asset_name) -> In:
        if isinstance(value, str):
            value = In(value)
        if type(value) not in (In, Incremental):
            raise RegistrationError(f"{asset_name}: inputs[{param!r}] must be a str, In or Incremental")
        return value

    @staticmethod
    def _dep(entry, asset_name: str) -> str:
        """A deps entry's output: a name, or `In(output, all_partitions=…)`. A
        dep is never loaded, so nothing else of an input means anything."""

        if isinstance(entry, str):
            return entry
        if type(entry) is not In:
            raise RegistrationError(
                f"{asset_name}: deps entry {entry!r} must be a name or In(output, all_partitions=…): "
                "a dep is never loaded"
            )
        if entry.output is None:
            raise RegistrationError(f"{asset_name}: an In(...) in deps= names its output")
        if entry.meta is not None:
            raise RegistrationError(f"{asset_name}: dep {entry.output!r}: meta= has no meaning on a dep")
        return entry.output

    @staticmethod
    def _check_each(name: str, asset: Asset, info: dict, param: str, upstream: dict) -> None:
        """A per-key incremental input (`each=True`, docs/per-key-processing.md
        §5): one per asset, over a keyed upstream, the asset's only
        incremental input, and every output keyed by the input's key."""

        if upstream["key"] is None:
            raise RegistrationError(f"{name}: per-key input {param!r} (each=True) needs a keyed upstream")
        others = [p for p, e in info["inputs"].items() if p != param and isinstance(e, Incremental)]
        if others:
            raise RegistrationError(
                f"{name}: a per-key asset reads its other inputs whole; {others[0]!r} is incremental"
            )
        if not asset.outputs:
            raise RegistrationError(f"{name}: a per-key asset needs outputs")
        for output in asset.outputs:
            if output.key is None or output.is_dynamic_partitions:
                raise RegistrationError(
                    f"{name}: output {output.name} of a per-key asset must be keyed (key= or keyed=True)"
                )

    def _build(self) -> dict:
        for store_name in self.stores:
            if not NAME.fullmatch(store_name):
                raise RegistrationError(f"Invalid store name: {store_name!r}")
        outputs = self._output_table()
        claimed = {}
        for asset in self.assets.values():
            for alias in asset.aliases:
                if not NAME.fullmatch(alias) or alias == asset.name:
                    raise RegistrationError(f"{asset.name}: invalid alias {alias!r}")
                if alias in self.assets or alias in outputs:
                    raise RegistrationError(f"{asset.name}: alias {alias!r} names a current asset or output")
                if alias in claimed:
                    raise RegistrationError(
                        f"{asset.name}: alias {alias!r} is also claimed by {claimed[alias]}"
                    )
                claimed[alias] = asset.name

        # Resolve every asset's inputs/deps/partitions and validate inputs.
        assets = {}
        for asset in self.assets.values():
            name = asset.name
            signature = inspect.signature(asset.fn)
            params = signature.parameters
            if any(p.kind in (p.POSITIONAL_ONLY, p.VAR_POSITIONAL, p.VAR_KEYWORD) for p in params.values()):
                raise RegistrationError(f"{name}: parameters must be named and keyword-bindable")
            declared = (
                asset.inputs
                if asset.inputs is not None
                else {p: In() for p in params if p != "ctx" and p not in self.resources}
            )
            inputs = {param: self._input(v, param, name) for param, v in declared.items()}
            for param in inputs:
                if param not in params:
                    raise RegistrationError(f"{name}: input {param!r} is not a producer parameter")
                if param in self.resources or param == "ctx":
                    raise RegistrationError(f"{name}: input {param!r} collides with a resource/ctx")
            for param, p in params.items():
                if (
                    param not in inputs
                    and param not in self.resources
                    and param != "ctx"
                    and p.default is p.empty
                ):
                    raise RegistrationError(f"{name}: parameter {param!r} has no input or resource binding")
            deps, every = [], []  # names; those read over every partition (`all_partitions`)
            for entry in asset.deps:
                dep = self._dep(entry, name)
                if dep not in outputs:
                    raise RegistrationError(f"{name}: deps entry {dep!r} names an unknown output")
                if dep in (e.output or p for p, e in inputs.items()):
                    raise RegistrationError(f"{name}: dep {dep!r} is already a bound input")
                deps.append(dep)
                if isinstance(entry, In) and entry.all_partitions:
                    every.append(dep)
            dims = self._dim_spec(asset.partitions, name)
            assets[name] = {"inputs": inputs, "deps": deps, "deps_all_partitions": every, "dims": dims}

        # Input validity: output exists, projection rule, store checks.
        hints_by_asset = self.hints = {n: hints(n, a.fn) for n, a in self.assets.items()}
        for name, info in assets.items():
            asset = self.assets[name]
            dims = info["dims"] or {}
            for param, input in info["inputs"].items():
                output_name = input.output or param
                if output_name not in outputs:
                    raise RegistrationError(f"{name}: input {param!r} names unknown output {output_name!r}")
                upstream = outputs[output_name]
                store = self.stores[upstream["store"]]
                annotation = hints_by_asset[name].get(param)
                up_dims = self._output_dims(output_name, outputs, assets)
                missing = set(up_dims) - set(dims)
                shared = set(up_dims) & set(dims)
                for d in shared:
                    if up_dims[d] != dims[d]:
                        raise RegistrationError(
                            f"{name}: dimension {d!r} differs from upstream {output_name}"
                        )
                if isinstance(input, Incremental) and input.each:
                    self._check_each(name, asset, info, param, upstream)
                if isinstance(input, Incremental):
                    if missing:
                        raise RegistrationError(
                            f"{name}: Incremental input {param!r} cannot have upstream-only dimensions (§7)"
                        )
                    if not upstream["incremental"]:
                        raise RegistrationError(
                            f"{name}: Incremental input {param!r} upstream {output_name} "
                            "is not incremental (declare incremental=True) (§2.1)"
                        )
                    if input.patterns is not None and upstream["key"] is None:
                        raise RegistrationError(
                            f"{name}: include=/exclude= on {param!r} select keys; {output_name} has none"
                        )
                    if is_ref_type(annotation):
                        raise RegistrationError(
                            f"{name}: Incremental input {param!r} cannot be ref-annotated (§5)"
                        )
                    if annotation is None:
                        raise RegistrationError(f"{name}: store-bound input {param!r} is unannotated (§11)")
                    Keys, Commits = _selection_classes()
                    selection = Keys if upstream["key"] is not None else Commits
                    loaded = dict[str, annotation] if input.each else annotation
                    if not store.can_load(loaded, selection):
                        raise RegistrationError(
                            f"{name}: store {upstream['store']} cannot load {annotation} "
                            f"under {selection.__name__}"
                        )
                elif missing or input.all_partitions:  # a whole fan-in: by upstream partition
                    inner = dict_arg(annotation)
                    if inner is None:
                        over = "every partition" if input.all_partitions else f"dimensions {sorted(missing)}"
                        raise RegistrationError(
                            f"{name}: input {param!r} fans in over {output_name}'s {over}: "
                            "annotate it dict[str, T] (§5, §7)"
                        )
                    if is_ref_type(inner):
                        if not store.can_load(inner, None):
                            raise RegistrationError(
                                f"{name}: store {upstream['store']} does not hand out {inner.__name__}"
                            )
                    elif not store.can_load(inner, None):
                        raise RegistrationError(f"{name}: store {upstream['store']} cannot load {inner}")
                else:  # In
                    if is_ref_type(annotation):
                        if not store.can_load(annotation, None):
                            raise RegistrationError(
                                f"{name}: store {upstream['store']} does not hand out {annotation.__name__}"
                            )
                    elif annotation is None:
                        raise RegistrationError(f"{name}: store-bound input {param!r} is unannotated (§11)")
                    elif not store.can_load(annotation, None):
                        raise RegistrationError(f"{name}: store {upstream['store']} cannot load {annotation}")
            # Output/store checks.
            return_t = hints_by_asset[name].get("return")
            partitioned = bool(info["dims"])
            for output in asset.outputs:
                record = outputs[output.name]
                store = self.stores[record["store"]]
                each = any(isinstance(e, Incremental) and e.each for e in info["inputs"].values())
                # A per-key producer returns one key's value: the output holds them all.
                t = _payload_type(return_t) if len(asset.outputs) == 1 and not each else None
                if not store.can_store(t, output):
                    raise RegistrationError(
                        f"{name}: store {record['store']} cannot store output {output.name} "
                        f"(type {t}, config {output.config}): a store takes what its `can_store` "
                        "says, plain Python unless it reads more itself (`Store.prepare`, docs/stores.md)"
                    )
                if output.migrations:
                    names = []
                    for migration in output.migrations:
                        if not isinstance(migration, Migration):
                            raise RegistrationError(
                                f"{name}: migrations entries must be Migration(), "
                                f"got {migration!r} on {output.name}"
                            )
                        names.append(migration.name)
                    if len(set(names)) != len(names):
                        raise RegistrationError(
                            f"{name}: duplicate migration name on output {output.name} (§4)"
                        )
                    if not callable(getattr(store, "migrate", None)):
                        raise RegistrationError(
                            f"{name}: output {output.name} declares migrations but store "
                            f"{record['store']!r} has no migrate (§4)"
                        )
                if (
                    partitioned
                    and getattr(store, "shared_table", False)
                    and "partition_column" not in output.config
                ):
                    raise RegistrationError(
                        f"{name}: partitioned output {output.name} on shared-table store "
                        f"{record['store']} lacks partition_column (§3)"
                    )
            for spec in (info["dims"] or {}).values():
                if spec["kind"] == "dynamic":
                    target = outputs.get(spec["output"])
                    if target is None:
                        raise RegistrationError(f"{name}: partitions= names unknown output {spec['output']}")
                    if target["key"] is None:
                        raise RegistrationError(
                            f"{name}: partitions= output {spec['output']} has no key (§7)"
                        )

        # DAG check over inputs + deps.
        visiting, visited = set(), set()

        def visit(n):
            if n in visiting:
                raise RegistrationError("Asset dependency cycle (§12)")
            if n in visited:
                return
            visiting.add(n)
            for output_name in [e.output or p for p, e in assets[n]["inputs"].items()] + assets[n]["deps"]:
                owner = outputs[output_name]["asset"]
                if owner:
                    visit(owner)
            visiting.remove(n)
            visited.add(n)

        for n in assets:
            visit(n)

        # Automations.
        automation_records = {}

        def add_automation(auto: Automation, *, targets: list[str], attached: Asset | None):
            name = auto.name
            if attached is not None:
                kind = auto.kind
                index = sum(1 for n in automation_records if n.startswith(f"{attached.name}.{kind}"))
                name = f"{attached.name}.{kind}.{index}"
            elif not name or not targets:
                raise RegistrationError("A standalone automation requires name and targets (§9)")
            if name in automation_records or not NAME.fullmatch(name):
                raise RegistrationError(f"Invalid or duplicate automation name: {name}")
            for target in targets:
                if target not in self.assets:
                    raise RegistrationError(f"Automation {name}: unknown target {target}")
            watched = []
            if isinstance(auto, OnChange):
                watched = list(auto.outputs)
                if not watched:
                    for target in targets:
                        info = assets[target]
                        watched += [e.output or p for p, e in info["inputs"].items()] + info["deps"]
                    watched = sorted(set(watched))
                own = {o.name for t in targets for o in self.assets[t].outputs}
                if set(watched) & own:
                    raise RegistrationError(f"Automation {name}: OnChange cannot name its own outputs")
                for w in watched:
                    if w not in outputs:
                        raise RegistrationError(f"Automation {name}: OnChange names unknown output {w}")
            automation_records[name] = {
                "name": name,
                "targets": targets,
                "trigger": auto.spec(),
                "enabled": bool(auto.enabled),
                "partitions": auto.partitions,
                "mode": auto.mode,
                "upstream": bool(auto.upstream),
                "config": auto.config,
                "keys": auto.keys,
                "tags": auto.tags,
                "skip_missing_inputs": auto.skip_missing_inputs,
                "watched": watched,
            }

        for asset in self.assets.values():
            for auto in asset.automations:
                add_automation(auto, targets=[asset.name], attached=asset)
        for auto in self.automations:
            targets = []
            for target in auto.targets or ():
                targets.append(target.name if isinstance(target, Asset) else str(target))
            add_automation(auto, targets=targets, attached=None)

        manifest_assets = {}
        executors = self._executors()
        for name, asset in self.assets.items():
            info = assets[name]
            placement = asset.executor.serialized() if asset.executor else default_placement()
            declared = executors.get(placement["executor"])
            if declared is None and placement["kind"] not in _builtin_kinds():
                raise RegistrationError(
                    f"{name}: executor {placement['executor']!r} of kind {placement['kind']!r} "
                    "is not registered; declare it with Project(executors=[...])"
                )
            executor = {"kind": placement["kind"], "config": placement["config"]}
            if declared not in (None, executor):
                raise RegistrationError(
                    f"{name}: executor {placement['executor']!r} is declared as {declared}, "
                    f"but the asset places it as {executor}"
                )
            executors[placement["executor"]] = executor
            for p, e in info["inputs"].items():
                src = self.sources.get(getattr(e, "output", None) or p)
                if src is None or src.key is None or src.key.startswith("<"):
                    continue
                store = self.stores.get(src.store or DEFAULT_STORE)
                serves = callable(getattr(store, "serve", None)) and getattr(
                    store, "can_serve", lambda _: True
                )(src)
                if src.loader is None and not serves:
                    if _load_intent(e, hints_by_asset[name].get(p)) == "data":
                        raise RegistrationError(
                            f"{name}: source {src.name!r} is keyed and loaded, but nothing says which version "
                            "of a key it served: load it through a function (@source, returning Loaded(row, "
                            'version=…)) or a store that serves versions (docs/stores.md, "Sources")'
                        )
            manifest_assets[name] = {
                "outputs": [o.spec(asset.name) for o in asset.outputs],
                "inputs": {
                    p: {**e.spec(p), "load": _load_intent(e, hints_by_asset[name].get(p))}
                    for p, e in info["inputs"].items()
                },
                "deps": info["deps"],
                **(
                    {"deps_all_partitions": info["deps_all_partitions"]}
                    if info["deps_all_partitions"]
                    else {}
                ),
                "partitions": {"dims": info["dims"]} if info["dims"] else None,
                "placement": placement,
                "retries": asset.retries.spec(),
                "timeout": asset.timeout,
                "concurrency": asset.concurrency,
                "version": asset.version,
                "retention": asset.retention.spec() if asset.retention else None,
                "aliases": list(asset.aliases),
                "tags": asset.tags,
                "doc": inspect.getdoc(asset.fn) or "",
                "types": {
                    "params": {p: type_name(t) for p, t in hints_by_asset[name].items() if p != "return"},
                    "return": type_name(hints_by_asset[name].get("return")),
                },
                "automations": [n for n, a in automation_records.items() if a["targets"] == [name]],
            }

        sensor_records = {}
        for name, s in self.sensors.items():
            for source in s.commits:
                if source not in self.sources:
                    raise RegistrationError(f"Sensor {name}: commits= names {source!r}, not a source")
            for param in s.params:
                if param not in self.resources:
                    raise RegistrationError(f"Sensor {name}: parameter {param!r} is not a resource")
            sensor_records[name] = {
                "name": name,
                "every": s.every.seconds,
                "commits": s.commits,
                "placement": s.placement(),
                "timeout": s.timeout,
                "doc": inspect.cleandoc(s.code.__doc__ or ""),
            }
        source_records = {
            name: {
                "name": name,
                "store": s.store or DEFAULT_STORE,
                "key": s.key,
                "handle": s.handle,
                "head": s.head().to_json(),
            }
            for name, s in self.sources.items()
        }
        for name, store in self.stores.items():
            # How a store keeps a writer the engine gave up on from writing over a
            # newer one (docs/stores.md): it writes only names no one else uses, or
            # every write checks the attempt's generation — and says which keys a
            # partition holds, for a repair (docs/versions.md §5). Either kind cleans up
            # an output removed or moved away from it (K25).
            writes = getattr(store, "writes", None)
            needs = {"immutable": ("cleanup",), "fenced": ("acquire", "keys", "cleanup")}.get(writes)
            if needs is None:
                raise RegistrationError(
                    f"store {name!r}: writes must be 'immutable' or 'fenced' (docs/stores.md)"
                )
            for method in needs:
                if not callable(getattr(store, method, None)):
                    raise RegistrationError(
                        f"store {name!r}: a {writes} store implements {method}() (docs/stores.md)"
                    )
        store_records = {}
        for name, store in self.stores.items():
            store_records[name] = {
                "version": getattr(store, "version", "1"),
                "ref": getattr(getattr(store, "ref_type", None), "kind", None) or "ref",
                "writes": store.writes,
                # How long a removed or moved output's data stays (K25): a week unless it says.
                "cleanup_after": getattr(store, "cleanup_after", CLEANUP_AFTER).total_seconds(),
            }
            # A built-in store's class and config: a worker rebuilds it to clean up after
            # an output the project no longer declares there (K25). Never a literal secret.
            try:
                built_in = store.describe() if callable(getattr(store, "describe", None)) else None
            except ValueError as error:
                raise RegistrationError(f"store {name!r}: {error}") from None
            if built_in is not None:
                store_records[name]["built_in"] = built_in
        body = {
            "name": self.name,
            "assets": manifest_assets,
            "outputs": {
                name: {k: v for k, v in rec.items() if k != "output"} for name, rec in outputs.items()
            },
            "sources": source_records,
            "stores": store_records,
            "executors": dict(sorted(executors.items())),
            "automations": automation_records,
            "sensors": sensor_records,
            "retention": self.retention.spec() if self.retention else None,
            "build": build_identity(self.home, self.build),
            # How user errors are classified changes what failures become (per-key §8).
            "errors": describe_errors(self.errors),
        }
        return {**body, "deploy": digest(body)}

    def _executors(self) -> dict[str, dict]:
        """`Project(executors=)`, by name: one kind and configuration per name."""

        executors: dict[str, dict] = {}
        for e in self.executors:
            record = {"kind": e.kind, "config": dict(e.config)}
            if executors.setdefault(e.name, record) != record:
                raise RegistrationError(f"Executor {e.name!r} is declared twice, differently")
        return executors

    def _output_dims(self, output_name, outputs, assets):
        owner = outputs[output_name]["asset"]
        if owner is None:
            return {}
        return assets[owner]["dims"] or {}


def _selection_classes():
    from .stores import Commits, Keys

    return Keys, Commits


def _builtin_kinds() -> dict:
    from .executors import BUILTIN_KINDS

    return BUILTIN_KINDS


def default_placement() -> dict:
    from .executors import Local

    return Local()().serialized()
