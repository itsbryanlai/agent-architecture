"""Refuse to start on a database that is behind the code.

``scripts/migrate.py`` records each migration it applies in ``schema_migrations``, and
``init_db.sql`` records the ones a new install contains. The worker, the gateway and the
console call ``require_current_schema`` at start-up, so a process never runs against a
table or column that a migration it has not had would add.

A new migration adds its number to ``EXPECTED_VERSIONS``; a test in ``tests/pg`` fails
when the list and ``scripts/migrations`` disagree.
"""

from __future__ import annotations

from psycopg_pool import AsyncConnectionPool

EXPECTED_VERSIONS = (
    "000",
    "001",
    "002",
    "003",
    "004",
    "005",
    "006",
    "007",
    "008",
    "009",
    "010",
    "011",
    "012",
    "013",
    "014",
    "015",
    "016",
    "017",
    "018",
)

MIGRATE_COMMAND = "uv run python scripts/migrate.py"


async def missing_versions(pool: AsyncConnectionPool) -> list[str]:
    """The expected migrations the database has not had, in order."""
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT to_regclass('public.schema_migrations') IS NOT NULL")
        row = await cur.fetchone()
        applied: set[str] = set()
        if row and row[0]:
            cur = await conn.execute("SELECT version FROM schema_migrations")
            applied = {version for (version,) in await cur.fetchall()}
    return [version for version in EXPECTED_VERSIONS if version not in applied]


async def require_current_schema(pool: AsyncConnectionPool) -> None:
    """Exit with a message naming the missing migrations when the database is behind."""
    missing = await missing_versions(pool)
    if missing:
        raise SystemExit(
            f"the database is behind the code: migration(s) {', '.join(missing)} not applied. "
            f"Run '{MIGRATE_COMMAND}' as the owner role (see the README), then start again."
        )
