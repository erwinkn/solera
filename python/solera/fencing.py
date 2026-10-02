"""Generation fencing for SQL stores (docs/stores.md, "Fenced stores"): one
call inside a write's own transaction makes a store `fenced`.

    with conn.transaction(), conn.cursor() as cur:
        fence(cur, scope, "public.events")   # before the write changes anything
        cur.execute("INSERT INTO public.events ...")

The fence table holds, per write domain (a table, say) and partition, the
newest generation that took it and that generation's invocation. `fence`
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

from .stores import Scope, StoreError

FENCE_TABLE = "solera_fences"
FENCE_DDL = (
    "CREATE TABLE IF NOT EXISTS {table} (domain text NOT NULL, part text NOT NULL, "
    "generation bigint NOT NULL, invocation text NOT NULL, PRIMARY KEY (domain, part))"
)


def fence_table(cur, table: str = FENCE_TABLE) -> None:
    """Create the fence table: once, when the store's schema is set up (two
    transactions creating it at once may collide)."""

    cur.execute(FENCE_DDL.format(table=table))


def fence(cur, scope: Scope, domain: str, *, table: str = FENCE_TABLE, param: str = "%s") -> None:
    """Take `(domain, scope.partition)` for `scope`'s generation and
    invocation, in the caller's transaction, until it ends; raise
    `StoreError` if a newer generation, or another invocation of this one,
    holds it. Outside an attempt (no generation) there is nothing to check."""

    if scope.generation is None:
        return
    p = param
    cur.execute(
        f"INSERT INTO {table} (domain, part, generation, invocation) VALUES ({p}, {p}, {p}, {p}) "
        f"ON CONFLICT (domain, part) DO UPDATE SET generation = EXCLUDED.generation, "
        f"invocation = EXCLUDED.invocation WHERE {table}.generation < EXCLUDED.generation "
        f"OR ({table}.generation = EXCLUDED.generation AND {table}.invocation = EXCLUDED.invocation) "
        "RETURNING invocation",
        (domain, scope.partition, int(scope.generation), scope.invocation or ""),
    )
    if cur.fetchone() is None:
        raise StoreError(
            f"{scope.output.name}: a newer attempt holds {domain} {scope.partition!r} "
            f"(generation {scope.generation} of {scope.invocation!r} refused)"
        )
