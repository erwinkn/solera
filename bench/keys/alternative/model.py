"""Executable snapshot semantics, independent of the native numeric fixture.

Arbitrary byte keys, immutable single-version leaves and a flat fence directory.
Splits only; empty fences are retained. No object storage, GC or engine adapter.
"""

from __future__ import annotations

import bisect
import itertools
import random
from dataclasses import dataclass


@dataclass(frozen=True)
class Value:
    generation: int
    payload: bytes | None = None


@dataclass(frozen=True)
class Leaf:
    rows: tuple[tuple[bytes, Value], ...]


@dataclass(frozen=True)
class Snapshot:
    fences: tuple[bytes, ...] = ()
    leaves: tuple[Leaf, ...] = (Leaf(()),)
    capacity: int = 4

    def get(self, key: bytes) -> Value | None:
        rows = self.leaves[bisect.bisect_right(self.fences, key)].rows
        i = bisect.bisect_left(rows, key, key=lambda row: row[0])
        return rows[i][1] if i < len(rows) and rows[i][0] == key else None

    def commit(self, changes: dict[bytes, Value | None]) -> Snapshot:
        grouped: dict[int, dict[bytes, Value | None]] = {}
        for key, value in changes.items():
            assert value is None or 0 <= value.generation <= 2**64 - 1
            grouped.setdefault(bisect.bisect_right(self.fences, key), {})[key] = value
        fences, leaves = [], []
        for i, leaf in enumerate(self.leaves):
            if i:
                fences.append(self.fences[i - 1])
            if i not in grouped:
                leaves.append(leaf)
                continue
            rows = dict(leaf.rows)
            for key, value in grouped[i].items():
                if value is None:
                    rows.pop(key, None)
                else:
                    rows[key] = value
            ordered = tuple(sorted(rows.items()))
            chunks = [ordered[j : j + self.capacity] for j in range(0, len(ordered), self.capacity)] or [()]
            for j, chunk in enumerate(chunks):
                if j:
                    fences.append(chunk[0][0])
                leaves.append(Leaf(chunk))
        return Snapshot(tuple(fences), tuple(leaves), self.capacity)

    def rows(self):
        return itertools.chain.from_iterable(leaf.rows for leaf in self.leaves)

    def page(self, after=None, limit=100, prefix=b""):
        assert limit > 0
        return [(k, v) for k, v in self.rows() if (after is None or k > after) and k.startswith(prefix)][
            :limit
        ]


def classify(before, after, source=False):
    if before is None:
        return "neither" if after is None else "added"
    if after is None:
        return "removed"
    if before == after or source and before.payload == after.payload:
        return "neither"
    return "updated"


def changes(before, after, *, delivered=None, source=False, cursor=None, limit=100, prefix=b""):
    # Reference implementation deliberately materializes the union.
    # Production diff uses ordered tree cursors and skips shared intervals.
    delivered = delivered or {}
    candidates = sorted(set(dict(before.rows())) | set(dict(after.rows())) | set(delivered))
    out = []
    for key in candidates:
        if (cursor is not None and key <= cursor) or not key.startswith(prefix):
            continue
        old = delivered[key] if key in delivered else before.get(key)
        new = after.get(key)
        kind = classify(old, new, source)
        if kind != "neither":
            out.append((key, kind, new))
        if len(out) == limit:
            break
    return out


def checks():
    count = 0
    # Endpoint cancellation and read-ahead: d is absent at both roots,
    # but a selection delivered it in between. It must be removed.
    s = Snapshot().commit({b"a": Value(1), b"b": Value(1)})
    t = s.commit({b"d": Value(2)}).commit({b"d": None, b"a": Value(3)})
    assert changes(s, t, delivered={b"d": Value(2)}) == [(b"a", "updated", Value(3)), (b"d", "removed", None)]
    assert changes(s, t, delivered={b"a": Value(3)}) == []
    # Empty byte keys need an optional cursor, not b'' as the beginning.
    maxgen = 2**64 - 1
    a = Snapshot(capacity=1).commit({b"": Value(maxgen - 1), b"\xff": Value(1), b"a\x00": Value(4)})
    b = a.commit({b"": Value(maxgen)})
    assert a.get(b"").generation == maxgen - 1
    assert b.page(limit=1) == [(b"", Value(maxgen))]
    assert b.page(after=b"", prefix=b"\xff") == [(b"\xff", Value(1))]
    assert classify(Value(1, b""), Value(4, b""), source=True) == "neither"
    assert classify(Value(1, b""), Value(4, b"")) == "updated"
    assert classify(Value(1, b"a"), Value(4, b"a"), source=True) == "neither"
    # Single hot key over many roots: each lookup reads one version.
    roots = [Snapshot().commit({b"hot": Value(g, bytes([g]) * 65536)}) for g in range(64)]
    assert all(len(list(r.rows())) == 1 and r.get(b"hot").generation == g for g, r in enumerate(roots))
    # Complete expected mappings, not just counts. Exercise leaf splits,
    # empty fences, adds/removes/re-adds, all endpoint pairs and page sizes.
    rng = random.Random(93)
    keys = [b"", b"\x00", b"\xff", b"a", b"a\x00", b"a\xff", "é".encode(), b"long/" * 30]
    roots, reference = [Snapshot(capacity=2)], [{}]
    for g in range(1, 101):
        edit = {
            rng.choice(keys): None if rng.random() < 0.25 else Value(g, rng.choice([b"", b"x", b"y"]))
            for _ in range(3)
        }
        roots.append(roots[-1].commit(edit))
        expected = reference[-1].copy()
        for key, value in edit.items():
            if value is None:
                expected.pop(key, None)
            else:
                expected[key] = value
        reference.append(expected)
        assert dict(roots[-1].rows()) == expected
        for key in keys:
            assert roots[-1].get(key) == expected.get(key)
            count += 1
    for i in range(len(roots)):
        for j in range(i, len(roots)):
            delivered = {key: reference[(i + j) // 2].get(key) for key in keys[::3]}
            for source in (False, True):
                expected = []
                for key in sorted(set(reference[i]) | set(reference[j]) | set(delivered)):
                    old = delivered[key] if key in delivered else reference[i].get(key)
                    new = reference[j].get(key)
                    # Independent truth-table oracle.
                    kind = (
                        ("added" if new else "neither")
                        if old is None
                        else (
                            "removed"
                            if new is None
                            else (
                                "neither"
                                if (
                                    old.payload == new.payload if source else old.generation == new.generation
                                )
                                else "updated"
                            )
                        )
                    )
                    if kind != "neither":
                        expected.append((key, kind, new))
                for limit in (1, 3, 20):
                    actual, cursor = [], None
                    while True:
                        page = changes(
                            roots[i], roots[j], delivered=delivered, source=source, cursor=cursor, limit=limit
                        )
                        if not page:
                            break
                        assert cursor is None or page[-1][0] > cursor
                        actual.extend(page)
                        cursor = page[-1][0]
                    assert actual == expected, (i, j, source, actual, expected)
                    count += 1
    print(
        f"{count} mapping/lookup checks passed; explicit byte-key, u64, read-ahead and hot-key cases passed"
    )


if __name__ == "__main__":
    checks()
