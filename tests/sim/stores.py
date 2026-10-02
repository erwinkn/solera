"""A fenced store for the simulation: a table per output in an in-memory
database that outlives engines and workers, written in transactions that
take the slice's fence first — `examples/json_table_store.py`, without
Postgres. A transaction holds its slice's lock from its fence check to its
commit, so a newer `acquire` waits behind an open older one (docs/stores.md,
invariant 8), and the simulation can hold a transaction open (`delay`) or
lose its answer after it committed (`lost`)."""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable
from dataclasses import dataclass, field

from solera.sdk import Ref, digest
from solera.stores import MISSING, Batches, KeyedWrite, Keys, StoreError, Written, by_key_type, takes


@dataclass
class Database:
    """The backend: rows per (table, partition), fences per (table, partition)."""

    tables: dict[str, dict[str, list[tuple]]] = field(
        default_factory=dict
    )  # table -> part -> [(k, batch, row)]
    fences: dict[tuple[str, str], tuple[int, str]] = field(default_factory=dict)
    locks: dict[tuple[str, str], asyncio.Lock] = field(default_factory=dict)
    commits: int = 0
    # (kind, scope) -> None | "error" | "lost", plus a delay: the simulation's say over one transaction
    fault: Callable[[str, object], tuple[str | None, float]] | None = None

    def lock(self, domain: str, part: str) -> asyncio.Lock:
        return self.locks.setdefault((domain, part), asyncio.Lock())


class TableStore:
    writes = "fenced"
    version = "1"

    def __init__(self, db: Database):
        self.db = db

    def can_load(self, t, selection) -> bool:
        return True

    def can_store(self, t, output) -> bool:
        return takes(t, output, values=False)

    @staticmethod
    def _table(output) -> str:
        return f"rows_{output.name}"

    def _fence(self, scope, table: str) -> tuple | None:
        """The fence row this transaction would write, or `StoreError`."""

        if scope.generation is None:
            return None
        key = (table, scope.partition)
        held = self.db.fences.get(key)
        mine = (int(scope.generation), scope.invocation or "")
        if held is not None and not (held[0] < mine[0] or held == mine):
            raise StoreError(
                f"{scope.output.name}: a newer attempt holds {table} {scope.partition!r} "
                f"(generation {scope.generation} of {scope.invocation!r} refused)"
            )
        return mine

    async def _transaction(self, kind: str, scope, body: Callable[[dict], object]):
        """Lock the slice, fence, apply `body` to a copy of its rows, commit."""

        table = self._table(scope.output)
        fate, delay = self.db.fault(kind, scope) if self.db.fault is not None else (None, 0.0)
        async with self.db.lock(table, scope.partition):
            if fate == "error":
                raise StoreError(f"injected: the database refused the {kind} transaction")
            fenced = self._fence(scope, table)  # the fence row is ours until the commit
            rows = copy.deepcopy(self.db.tables.get(table, {}).get(scope.partition, []))
            box = {"rows": rows}
            value = body(box)
            if delay:
                await asyncio.sleep(delay)  # open, holding the fence row
            if fenced is not None:
                self.db.fences[(table, scope.partition)] = fenced
            self.db.tables.setdefault(table, {})[scope.partition] = box["rows"]
            self.db.commits += 1
        if fate == "lost":
            raise StoreError(f"injected: the {kind} transaction committed, its answer was lost")
        return value

    async def acquire(self, scope, prior=None) -> None:
        await self._transaction("acquire", scope, lambda box: None)

    async def store(self, write, prior, scope) -> Written:
        out = scope.output

        def body(box):
            rows = box["rows"]
            if out.key is None:  # an unkeyed incremental output: a batch of rows
                batch = list(write.rows)
                if prior is None:
                    rows.clear()
                else:
                    rows[:] = [r for r in rows if r[1] != scope.batch]
                rows.extend((None, scope.batch, dict(r)) for r in batch)
                version = digest([prior.version if prior else "", scope.batch, batch])
                return Written(Ref(out.name, "", {"batch": scope.batch}, version, scope.partition))
            keyed = KeyedWrite.of(self, write, out, prior)
            if keyed.whole:
                rows.clear()
            for page in keyed.iter_pages():
                keys = {k for k, _, _ in page}
                if not keyed.whole:
                    rows[:] = [r for r in rows if r[0] not in keys]
                rows.extend((k, None, dict(r)) for k, _, group in page for r in group)
            if keyed.removes:
                rows[:] = [r for r in rows if r[0] not in keyed.removes]
            return Written(Ref(out.name, "", {}, keyed.version(prior), scope.partition))

        return await self._transaction("store", scope, body)

    async def load(self, ref, t, selection) -> list[dict]:
        rows = self.db.tables.get(f"rows_{ref.output}", {}).get(ref.partition, [])
        if isinstance(selection, Keys):
            rows = [r for r in rows if r[0] in selection.revisions]
        elif isinstance(selection, Batches):
            rows = [r for r in rows if r[1] is not None and selection.lo <= r[1] <= selection.hi]
        elif (ref.handle or {}).get("batch") is not None:
            rows = [r for r in rows if r[1] is None or r[1] <= ref.handle["batch"]]
        if by_key_type(t) is not MISSING:  # dict[str, T]: each key's group (an Each page)
            groups: dict[str, list] = {}
            for k, _, row in rows:
                groups.setdefault(k, []).append(dict(row))
            return groups
        return [dict(r[2]) for r in rows]
