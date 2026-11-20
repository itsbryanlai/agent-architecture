"""Refuse to start on a database that is behind the code.

``scripts/migrate.py`` records each migration it applies in ``schema_migrations``, and
``init_db.sql`` records the ones a new install contains. The worker, the gateway and the
console call ``require_current_schema`` at start-up, so a process never runs against a
table or column that a migration it has not had would add.

``python -m agentos.schema_version`` is the read-only check for a hosted status run:
it connects with ``AGENTOS_DATABASE_URL`` (the app role), prints
``schema current (latest <NNN>)`` and exits 0, or prints the missing migration numbers
and exits 1. It prints no URL and no password.

A new migration adds its number to ``EXPECTED_VERSIONS``; a test in ``tests/pg`` fails
when the list and ``scripts/migrations`` disagree.
"""

from __future__ import annotations

import argparse
import asyncio
import os

import psycopg
from psycopg_pool import AsyncConnectionPool, PoolTimeout

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
    "019",
    "020",
    "021",
    "022",
    "023",
    "024",
    "025",
    "026",
    "027",
    "028",
    "029",
    "030",
    "031",
    "032",
    "033",
    "034",
    "035",
    "036",
    "037",
    "038",
    "039",
)

MIGRATE_COMMAND = "uv run python scripts/migrate.py"
DATABASE_URL_VAR = "AGENTOS_DATABASE_URL"


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


async def check_schema(url: str) -> int:
    """Print the schema state of the database at ``url``: 0 when current, 1 when behind."""
    pool = AsyncConnectionPool(
        url, open=False, min_size=1, max_size=1, kwargs={"connect_timeout": 5}
    )
    await pool.open(wait=True, timeout=10)
    try:
        missing = await missing_versions(pool)
    finally:
        await pool.close()
    if missing:
        print(f"schema behind: migration(s) {', '.join(missing)} not applied")
        return 1
    print(f"schema current (latest {EXPECTED_VERSIONS[-1]})")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m agentos.schema_version",
        description="Check the database schema against the code's migrations, read-only.",
    )
    parser.parse_args(argv)
    url = os.environ.get(DATABASE_URL_VAR)
    if not url:
        print(f"{DATABASE_URL_VAR} is not set. Export the app role's URL in this shell.")
        return 2
    try:
        return asyncio.run(check_schema(url))
    except (psycopg.Error, PoolTimeout):
        # The exception text can echo the connection string; name the variable only.
        print(f"cannot check the schema: the database did not answer on {DATABASE_URL_VAR}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
