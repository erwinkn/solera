"""Generation fencing for SQL stores (docs/stores.md, "Fenced stores"): one
call inside a write's own transaction makes a store `fenced`.

    with conn.transaction(), conn.cursor() as cur:
        fence(cur, scope, "public.events")   # before the write changes anything
        cur.execute("INSERT INTO public.events ...")

The fence table holds, per write domain (a table, say) and partition, the
newest generation that took it, that generation's invocation, and the
generation whose write last changed it (`written`), which a read reports
(`written()`, in the read's own snapshot). `fence`
takes the row for `scope`'s generation — inserting it, or raising an older
one — and keeps its lock until the transaction ends; it raises
`StoreError`, changing nothing, when a newer generation or another
invocation of this one holds it. So a newer attempt's `fence` (its
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
    "generation bigint NOT NULL, invocation text NOT NULL, written bigint, PRIMARY KEY (domain, part))"
)


def fence_table(cur, table: str = FENCE_TABLE) -> None:
    """Create the fence table: once, when the store's schema is set up (two
    transactions creating it at once may collide)."""

    cur.execute(FENCE_DDL.format(table=table))
    cur.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_name = %s AND column_name = 'written'",
        (table.rsplit(".", 1)[-1],),
    )
    if cur.fetchone() is None:  # a table made before `written`
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS written bigint")


def fence(
    cur,
    context: WriteContext,
    domain: str,
    *,
    write: bool = False,
    table: str = FENCE_TABLE,
    param: str = "%s",
) -> None:
    """Take `(domain, scope.partition)` for `scope`'s generation and
    invocation, in the caller's transaction, until it ends; raise
    `StoreError` if a newer generation, or another invocation of this one,
    holds it. A `write` transaction — one that changes the slice, not an
    `acquire` — also marks it written by its generation. Outside an attempt
    (no generation) there is nothing to check."""

    if context.generation is None:
        return
    p = param
    generation = int(context.generation)
    cur.execute(
        f"INSERT INTO {table} (domain, part, generation, invocation, written) VALUES ({p}, {p}, {p}, {p}, {p}) "
        f"ON CONFLICT (domain, part) DO UPDATE SET generation = EXCLUDED.generation, "
        f"invocation = EXCLUDED.invocation{', written = EXCLUDED.written' if write else ''} "
        f"WHERE {table}.generation < EXCLUDED.generation "
        f"OR ({table}.generation = EXCLUDED.generation AND {table}.invocation = EXCLUDED.invocation) "
        "RETURNING invocation",
        (domain, context.partition, generation, context.invocation or "", generation if write else None),
    )
    if cur.fetchone() is None:
        raise StoreError(
            f"{context.output.name}: a newer attempt holds {domain} {context.partition!r} "
            f"(generation {context.generation} of {context.invocation!r} refused)"
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
