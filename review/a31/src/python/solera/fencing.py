"""Generation fencing for SQL stores (docs/stores.md, "Fenced stores"): one
call inside a write's own transaction makes a store `fenced`.

    with conn.transaction(), conn.cursor() as cur:
        fence(cur, partition, "public.events")   # before the write changes anything
        cur.execute("INSERT INTO public.events ...")

The fence table holds, per write domain (a table, say) and partition, the
newest generation that took it, that generation's worker, and the
generation whose write last changed it (`written`), which a read reports
(`written()`, in the read's own snapshot). `fence`
takes the row for `partition`'s generation — inserting it, or raising an older
one — and keeps its lock until the transaction ends; it raises
`StoreError`, changing nothing, when a newer generation or another
worker of this one holds it. So a newer attempt's `fence` (its
`acquire`) waits behind an older writer's open transaction, and from then
on every transaction of the older writer is refused.

The SQL is PostgreSQL's — `INSERT … ON CONFLICT … DO UPDATE … WHERE …
RETURNING`, which locks the conflicting row even when the `WHERE` refuses
— with `%s` placeholders (psycopg); `param` changes them for another
driver. PostgresStore fences the same way, keyed by its table's OID so a
renamed table keeps its fence."""

from __future__ import annotations

from .stores import StoreError, WriteContext

FENCE_TABLE = "solera_fences"
FENCE_DDL = (
    "CREATE TABLE IF NOT EXISTS {table} (domain text NOT NULL, part text NOT NULL, "
    "generation bigint NOT NULL, worker_id text NOT NULL, written bigint, PRIMARY KEY (domain, part))"
)


def fence_table(cur, table: str = FENCE_TABLE) -> None:
    """Create the fence table: once, when the store's schema is set up (two
    transactions creating it at once may collide)."""

    cur.execute(FENCE_DDL.format(table=table))


def fence(
    cur,
    context: WriteContext,
    domain: str,
    *,
    write: bool = False,
    table: str = FENCE_TABLE,
    param: str = "%s",
) -> None:
    """Take `(domain, partition.partition)` for `partition`'s generation and
    worker, in the caller's transaction, until it ends; raise
    `StoreError` if a newer generation, or another worker of this one,
    holds it. A `write` transaction — one that changes the partition, not an
    `acquire` — also marks it written by its generation. Outside an attempt
    (no generation) there is nothing to check."""

    if context.generation is None:
        return
    p = param
    generation = int(context.generation)
    cur.execute(
        f"INSERT INTO {table} (domain, part, generation, worker_id, written) VALUES ({p}, {p}, {p}, {p}, {p}) "
        f"ON CONFLICT (domain, part) DO UPDATE SET generation = EXCLUDED.generation, "
        f"worker_id = EXCLUDED.worker_id{', written = EXCLUDED.written' if write else ''} "
        f"WHERE {table}.generation < EXCLUDED.generation "
        f"OR ({table}.generation = EXCLUDED.generation AND {table}.worker_id = EXCLUDED.worker_id) "
        "RETURNING worker_id",
        (domain, context.partition, generation, context.worker_id or "", generation if write else None),
    )
    if cur.fetchone() is None:
        raise StoreError(
            f"{context.output.name}: a newer attempt holds {domain} {context.partition!r} "
            f"(generation {context.generation} of {context.worker_id!r} refused)"
        )


def written(cur, domain: str, partition: str, *, table: str = FENCE_TABLE, param: str = "%s") -> int | None:
    """The generation whose write last changed `(domain, partition)`, as the
    caller's transaction sees it: in a read's snapshot, the generation of
    what the read sees (docs/stores.md, "What a read sees"). None if no
    fenced write changed it."""

    cur.execute(f"SELECT written FROM {table} WHERE domain = {param} AND part = {param}", (domain, partition))
    found = cur.fetchone()
    value = None if found is None else (found["written"] if isinstance(found, dict) else found[0])
    return None if value is None else int(value)


def cleaned(
    cur,
    domain: str,
    *,
    partition: str | None = None,
    generation: int | None = None,
    before: int | None = None,
    keep: bool = False,
    table: str = FENCE_TABLE,
    param: str = "%s",
) -> tuple[list[str], bool]:
    """The partitions of `domain` a cleanup pattern takes (docs/stores.md
    § Cleanup), in the caller's transaction, and whether they are all of
    its partitions. Rows carry no generation, so a partition's is the one
    that last wrote it: taken when that is exactly `generation`, or older
    than `before` — a later life of the same name keeps what it wrote. Their
    fence rows go, unless `keep` (only some of their rows go, a key's)."""

    p = param
    cur.execute(f"SELECT part, written FROM {table} WHERE domain = {p}", (domain,))
    found = {
        (r["part"] if isinstance(r, dict) else r[0]): (r["written"] if isinstance(r, dict) else r[1])
        for r in cur.fetchall()
    }

    def due(g) -> bool:
        return (generation is None or g == generation) and (before is None or g is None or g < before)

    parts = [part for part, g in found.items() if (partition is None or part == partition) and due(g)]
    if partition is not None and partition not in found:
        parts.append(partition)  # never fenced: nothing newer wrote it
    if not keep:
        for part in parts:
            cur.execute(f"DELETE FROM {table} WHERE domain = {p} AND part = {p}", (domain, part))
    return parts, len(set(parts) & set(found)) == len(found)
