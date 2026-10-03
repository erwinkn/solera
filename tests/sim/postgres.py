"""A real PostgresStore in the simulation, watched: every write transaction
it commits (the slice, its generation, the slice's rows after it), every
acquisition, and every read through `reads()` (the reader's snapshot, the
generation it reported, the rows it loaded). The machine checks what
lineage says against this record (docs/stores.md, "What a read sees").

Its transactions run on threads the loop waits for, one at a time, so the
order they are recorded in is the order Postgres committed them."""

from __future__ import annotations

import itertools
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field

from .core import EPOCH

DSN = os.environ.get("SOLERA_TEST_DATABASE_URL")
_schemas = itertools.count()


def fresh_schema() -> str:
    return f"sim_{os.getpid()}_{next(_schemas)}"


@dataclass
class Write:
    seq: int
    generation: int | None
    rows: frozenset  # (id, v) after the transaction
    who: tuple | None = None
    worker_id: str | None = None
    at: float = 0.0  # when the transaction began


@dataclass
class Read:
    seq: int  # when the reader's snapshot was taken: its first load
    who: tuple | None
    table: str
    part: str
    generation: int | None
    rows: list  # (id, v) loaded
    keys: list[str] | None  # the selection's keys, if any


@dataclass
class Ledger:
    writes: dict[tuple, list[Write]] = field(default_factory=lambda: defaultdict(list))
    acquired: dict[tuple, set] = field(default_factory=lambda: defaultdict(set))
    reads: list[Read] = field(default_factory=list)
    seq: int = 0

    def tick(self) -> int:
        self.seq += 1
        return self.seq


def _pairs(value) -> list[tuple[str, str]]:
    rows = [r for group in value.values() for r in group] if isinstance(value, dict) else list(value or ())
    return sorted((str(r["id"]), str(r["v"])) for r in rows)


def patches(ledger: Ledger, current_actor) -> list[tuple]:
    """(owner, name, value) patches recording into `ledger`."""

    from solera.stores import Keys
    from solera_postgres import PostgresStore, _Reader

    store_real, acquire_real, load_real = PostgresStore._store, PostgresStore._acquire, _Reader.load

    def table_of(self, output, prior):
        table, _, _ = self._table(output, prior)
        return table

    def _store(self, write, prior, context):
        at = time.time() - EPOCH  # virtual: the loop waits for this thread
        written = store_real(self, write, prior, context)
        ref = written.ref
        table = (ref.handle or {}).get("table") or table_of(self, context.output, prior)
        rows = self._load(ref, list[dict], None)
        ledger.writes[(table, context.partition)].append(
            Write(
                ledger.tick(),
                context.generation,
                frozenset(_pairs(rows)),
                current_actor(),
                context.worker_id,
                at,
            )
        )
        return written

    def _acquire(self, context, prior):
        acquire_real(self, context, prior)
        ledger.acquired[(table_of(self, context.output, prior), context.partition)].add(context.generation)

    async def load(self, ref, t, selection):
        if not hasattr(self, "_sim_snapshot"):
            self._sim_snapshot = ledger.tick()
        value, generation = await load_real(self, ref, t, selection)
        if not (isinstance(t, type) and t.__name__.endswith("Ref")):
            keys = sorted(selection.generations) if isinstance(selection, Keys) else None
            table = (ref.handle or {}).get("table")
            ledger.reads.append(
                Read(
                    self._sim_snapshot, current_actor(), table, ref.partition, generation, _pairs(value), keys
                )
            )
        return value, generation

    return [(PostgresStore, "_store", _store), (PostgresStore, "_acquire", _acquire), (_Reader, "load", load)]


def check(ledger: Ledger, start: int) -> int:
    """Every read since `start`: the generation it reports committed a write
    to its slice — the newest before its snapshot — and the rows it loaded
    are that write's rows (of the keys it asked for). Returns where it stopped."""

    from .oracle import Violation

    for read in ledger.reads[start:]:
        writes = [w for w in ledger.writes.get((read.table, read.part), []) if w.seq < read.seq]
        last = writes[-1] if writes else None
        where = f"{read.who} reading {read.table}[{read.part!r}]"
        if read.generation is None:
            if last is not None and last.generation is not None:
                raise Violation(
                    f"{where} reported no generation; generation {last.generation} wrote before it"
                )
            continue
        if last is None or last.generation != read.generation:
            acquired = read.generation in ledger.acquired.get((read.table, read.part), set())
            raise Violation(
                f"{where} reported generation {read.generation}"
                + (" (which only acquired)" if acquired else "")
                + f"; the newest write before its snapshot was generation {last.generation if last else None}"
            )
        want = (
            sorted(last.rows) if read.keys is None else sorted(p for p in last.rows if p[0] in set(read.keys))
        )
        if read.rows != want:
            raise Violation(f"{where} at generation {read.generation} loaded {read.rows}, which wrote {want}")
    return len(ledger.reads)
