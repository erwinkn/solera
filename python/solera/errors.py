"""How an error raised by user code behaves (docs/per-key-processing.md §8).

Users subclass one of four classes to say what their own errors mean:

    class Unprocessable(solera.Rejected): ...
    class Throttled(solera.Transient):
        retry_for = "2h"
    raise Throttled(retry_after=30)

and classify exceptions they cannot subclass with
`Project(errors={httpx.TimeoutException: Transient})`. Anything else is
`Failed`.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

REJECTED, FAILED, TRANSIENT, ABORT = "rejected", "failed", "transient", "abort"

BACKOFF_FIRST = 60.0  # seconds before a transient error's first retry
BACKOFF_MAX = 6 * 3600.0  # the backoff doubles up to this


class Rejected(Exception):
    """The input is bad, and that is expected: an empty file, a template.
    Not retried until the input changes."""


class Failed(Exception):
    """Something unexpected: a bug, a format nobody handles. Also what any
    unclassified exception is."""


class Transient(Exception):
    """A retry later will probably work: throttling, a timeout.

    `retry_after` is the earliest next try; `retry_for` how long to keep
    retrying, counted from the first failure, before the error counts as
    failed. Both take seconds or a duration ("30s", "2h", "1d"). A subclass
    sets its default `retry_for` as a class attribute."""

    retry_for: float | str = "24h"

    def __init__(self, *args, retry_after: float | str | None = None, retry_for: float | str | None = None):
        super().__init__(*args)
        self.retry_after = None if retry_after is None else seconds(retry_after)
        if retry_for is not None:
            self.retry_for = retry_for
        seconds(self.retry_for)  # fail at the raise, not later


class Abort(Exception):
    """The attempt itself is broken: credentials, the database. Nothing it
    did commits; it is retried as a whole, per `retries=`."""


CLASSES = {Rejected: REJECTED, Failed: FAILED, Transient: TRANSIENT, Abort: ABORT}

_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)?\s*$")
_UNITS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800, None: 1}


def seconds(value: float | int | str) -> float:
    """A duration in seconds: a number, or a string like `"90s"` or `"2h"`."""

    if isinstance(value, bool):
        raise ValueError(f"not a duration: {value!r}")
    if isinstance(value, int | float):
        result = float(value)
    else:
        match = _DURATION.fullmatch(str(value))
        if not match:
            raise ValueError(f"not a duration: {value!r}")
        result = float(match.group(1)) * _UNITS[match.group(2)]
    if result < 0:
        raise ValueError(f"not a duration: {value!r}")
    return result


def describe(errors: Mapping[type, type]) -> list[list]:
    """The error policy as the manifest records it, so that changing it changes
    the deploy: `[raised, class, retry_for]` per mapping entry, by
    qualified name, with the transient budget a mapped class carries."""

    def name(t: type) -> str:
        return f"{t.__module__}.{t.__qualname__}"

    return sorted(
        [name(raised), name(kind), seconds(kind.retry_for) if issubclass(kind, Transient) else None]
        for raised, kind in errors.items()
    )


def check_mapping(errors: Mapping | None) -> dict[type, type]:
    """`Project(errors=)`: exception types to one of the four classes."""

    mapping = {}
    for raised, kind in (errors or {}).items():
        if not (isinstance(raised, type) and issubclass(raised, BaseException)):
            raise ValueError(f"errors=: {raised!r} is not an exception type")
        if not (isinstance(kind, type) and any(issubclass(kind, c) for c in CLASSES)):
            raise ValueError(
                f"errors=: {raised.__name__} maps to {kind!r}, not Rejected, Failed, Transient or Abort"
            )
        mapping[raised] = kind
    return mapping


def classify(error: BaseException, errors: Mapping[type, type] | None = None) -> tuple[str, dict]:
    """What `error` means: its class and, for a transient one, its timing
    (`retry_after`, `retry_for` in seconds). The first match along the
    error's method resolution order wins — a Solera class it subclasses,
    or an entry of `errors`."""

    for base in type(error).__mro__:
        kind = CLASSES.get(base)
        mapped = (errors or {}).get(base)
        if kind is None and mapped is not None:
            kind = next(k for c, k in CLASSES.items() if issubclass(mapped, c))
            if kind == TRANSIENT:
                return kind, {"retry_after": None, "retry_for": seconds(mapped.retry_for)}
            return kind, {}
        if kind is not None:
            if kind == TRANSIENT:
                return kind, {
                    "retry_after": getattr(error, "retry_after", None),
                    "retry_for": seconds(getattr(error, "retry_for", Transient.retry_for)),
                }
            return kind, {}
    return FAILED, {}


def backoff(tries: int) -> float:
    """Seconds before the next try after `tries` transient failures: one
    minute, doubling, at most six hours."""

    doublings = min(max(tries - 1, 0), 32)  # past the cap long before: never overflows
    return min(BACKOFF_FIRST * 2**doublings, BACKOFF_MAX)
