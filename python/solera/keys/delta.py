"""Δ(P, H, keys): every key whose state differs between P and H, with its
presence at P and at H and its version at H (docs/observed-set.md,
docs/key-index-design.md), over the stamped-layer index (`LayerIndex`).

- P is None (−∞: every key live at H is added) or a commit an observation
  was made at: the state after commit P.
- H is None (the head) or a commit a batch pinned: the state after commit H.
- keys is a sorted list, or a range — the first `first` keys after `after`
  that differ; either may carry a pattern filter (`take`). A range page may
  come back short, with its cursor: it holds every differing key up to it.

Any P at or after the index's cut is answered (below it, `CutError`), and
an H the state holds a layer boundary at: a batch passes the state it
pinned at its H (else `NotHeld`)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from .layers import LayerIndex, key_bytes, key_str


@dataclass(frozen=True)
class Diff:
    """One key's difference between P and H: present at P, present at H, and
    its generation and payload at H (None where absent there)."""

    key: str
    before: bool
    after: bool
    generation: int | None = None
    payload: bytes | None = None


@dataclass(frozen=True)
class DeltaPage:
    """Differences in key order, and where a range read resumes (None: done)."""

    diffs: list[Diff]
    cursor: str | None = None


async def delta(
    index: LayerIndex,
    p: int | None,
    h: int | None = None,
    *,
    keys: list[str] | None = None,
    after: str | None = None,
    first: int = 10_000,
    take: Callable[[str], bool] | None = None,
) -> DeltaPage:
    """Δ(P, H, keys): `keys` named outright, else the first `first` keys after
    `after` that differ, both filtered by `take`. One read for a key list and
    a range (A31 R1)."""

    state = index.state.at(h)
    if state is not index.state:
        index = LayerIndex(index.io, state, cache=index._small)
    if p is not None and p >= state.head:
        return DeltaPage([])
    pick = None if take is None else (lambda k: take(key_str(k)))
    rows, cursor = await index.delta(
        p,
        keys=None if keys is None else [key_bytes(k) for k in keys],
        after=None if after is None else key_bytes(after),
        first=first,
        take=pick,
    )
    diffs = [Diff(key_str(k), before, now, g, pl) for k, before, now, g, pl in rows]
    return DeltaPage(diffs, None if cursor is None else key_str(cursor))


def version_of(generation: int | None, payload) -> object:
    """A key's version: its source's own word where its entry carries one
    (the payload), else the generation that wrote it."""

    if isinstance(payload, bytes):
        return payload.decode()
    return payload if payload is not None else generation
