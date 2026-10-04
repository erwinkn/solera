"""Δ(P, H, keys): every key whose state differs between P and H, with its
presence at P and at H and its version at H (docs/observed-set.md; the
interface of docs/key-index-from-first-principles.md). A thin adapter over
today's `KeyIndex`; a new index replaces it behind the same interface.

- P is None (−∞: every key live at H is added) or a commit an observation
  was made at: the state after commit P.
- H is None (the head) or a commit a batch pinned: the state after commit H.
- keys is a sorted list, or a range — the first `first` keys after `after`
  that differ; either may carry a pattern filter (`take`).

Today's index reads a change between commits only at endpoints it keeps:
P + 1 must be one (and H + 1, unless H is the head), as the records that
hold those commits reserve them."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from .index import KeyIndex, key_bytes, key_str


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
    index: KeyIndex,
    p: int | None,
    h: int | None = None,
    *,
    keys: list[str] | None = None,
    after: str | None = None,
    first: int = 10_000,
    take: Callable[[str], bool] | None = None,
) -> DeltaPage:
    """Δ(P, H, keys): `keys` named outright, else the first `first` keys after
    `after` that differ, both filtered by `take`."""

    head = index.state.head
    h = head if h is None else h
    at = None if h == head else h + 1  # the state after commit h
    if p is not None and p >= h:
        return DeltaPage([])
    if keys is not None:
        named = [k for k in sorted(set(keys)) if take is None or take(k)]
        if not named:
            return DeltaPage([])
        if p is None:
            found = await index.lookup([key_bytes(k) for k in named], at=at)
            return DeltaPage([_added(key_str(k), g, pl) for k, (g, pl) in sorted(found.items())])
        page = await index.changes_page(p + 1, h, None, len(named), keys=[key_bytes(k) for k in named])
        return DeltaPage(_diffs(page, take))
    diffs: list[Diff] = []
    cursor = None if after is None else key_bytes(after)
    while len(diffs) < first:
        want = first - len(diffs)
        if p is None:
            ks, gens, pls, nxt = await index.page(cursor, want, at=at)
            diffs += [
                _added(key_str(k), g, pl)
                for k, g, pl in zip(ks, gens, pls, strict=True)
                if take is None or take(key_str(k))
            ]
        else:
            page = await index.changes_page(p + 1, h, cursor, want)
            diffs += _diffs(page, take)
            nxt = page.cursor
        if nxt is None:
            return DeltaPage(diffs)
        cursor = nxt
    return DeltaPage(diffs, key_str(cursor))


def _added(key: str, generation: int, payload) -> Diff:
    return Diff(key, False, True, generation, payload)


def _diffs(page, take) -> list[Diff]:
    """A `changes` page as differences: class 3 (added and removed again)
    differs at neither end, so it is none."""

    out = []
    for i, k in enumerate(page.keys):
        cls = page.classes[i]
        if cls == 3:
            continue
        key = key_str(k)
        if take is not None and not take(key):
            continue
        live = cls in (0, 1)
        out.append(
            Diff(
                key,
                cls in (1, 2),
                live,
                page.generations[i] if live else None,
                page.payloads[i] if live else None,
            )
        )
    return out
