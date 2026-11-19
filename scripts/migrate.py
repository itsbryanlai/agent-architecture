"""Apply the database migrations that this database has not had, in order.

Run it as the database owner, not as the app role::

    export AGENTOS_OWNER_DATABASE_URL=postgresql://<owner>:<password>@localhost:55432/agentos
    export AGENTOS_APP_DB_PASSWORD=<the password agentos_app will have>
    uv run python scripts/migrate.py

``schema_migrations`` records each migration applied. Each migration runs in its own
transaction together with its row there, so a failed one leaves no trace and the next
run starts from it.

An empty database (no tables at all, as a fresh hosted Postgres is) is built from
``scripts/init_db.sql`` in one transaction and every migration file is recorded as
applied, so a new install needs this one command. A database that already has tables
is never rebuilt: it gets the migrations it lacks.

The role ``agentos_app`` is created with the password in AGENTOS_APP_DB_PASSWORD when it
does not exist. An existing role keeps its password; ``--set-app-password`` changes it.
The password is never written to a file and never printed.

A database that already has migrations but no ``schema_migrations`` table (one built
before this script) is brought in with ``--baseline <number>``: it records every
migration up to that number as applied without running it. The migrations were written to
be rerunnable on an empty table, not on one holding rows (007 narrows a constraint that
later migrations widened), so do not run them again over data.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

import psycopg
from psycopg import sql

APP_ROLE = "agentos_app"
OWNER_URL_VAR = "AGENTOS_OWNER_DATABASE_URL"
APP_PASSWORD_VAR = "AGENTOS_APP_DB_PASSWORD"
MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
INIT_DB = Path(__file__).resolve().parent / "init_db.sql"
_FILE = re.compile(r"(\d{3})_[a-z0-9_]+\.sql")
# The migration that creates schema_migrations. It runs before the table can be read.
BOOTSTRAP = "000"
# A database still starting (a hosted redeploy) gets about two minutes.
CONNECT_ATTEMPTS = 24
CONNECT_WAIT_SECONDS = 5.0
CONNECT_TIMEOUT_SECONDS = 5


class MigrateError(Exception):
    """Something the operator must fix; the message says what."""


def migration_files(directory: Path = MIGRATIONS_DIR) -> dict[str, Path]:
    """Version (the number in the file name) to file, in order."""
    found: dict[str, Path] = {}
    for path in sorted(directory.glob("*.sql")):
        match = _FILE.fullmatch(path.name)
        if match is None:
            raise MigrateError(f"{path.name}: a migration is named <3 digits>_<words>.sql")
        if match[1] in found:
            raise MigrateError(f"two migrations are numbered {match[1]}")
        found[match[1]] = path
    return found


def ensure_app_role(
    conn: psycopg.Connection, password: str | None, set_password: bool, role: str = APP_ROLE
) -> str:
    """Create the app role, or reset its password when asked. Returns what was done."""
    exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone()
    if exists and not set_password:
        return "kept"
    if not password:
        raise MigrateError(f"{APP_PASSWORD_VAR} is not set. Export it in this shell.")
    # CREATE and ALTER ROLE take the password as a literal, not a bind parameter.
    statement = (
        "ALTER ROLE {} PASSWORD {}"
        if exists
        else ("CREATE ROLE {} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD {}")
    )
    conn.execute(sql.SQL(statement).format(sql.Identifier(role), sql.Literal(password)))
    return "password set" if exists else "created"


def applied_versions(conn: psycopg.Connection) -> set[str]:
    if not conn.execute("SELECT to_regclass('public.schema_migrations')").fetchone()[0]:
        return set()
    return {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}


def database_is_empty(conn: psycopg.Connection) -> bool:
    """No tables in the public schema: nothing has been built in this database."""
    return (
        conn.execute("SELECT 1 FROM pg_tables WHERE schemaname = 'public' LIMIT 1").fetchone()
        is None
    )


def record(conn: psycopg.Connection, version: str) -> None:
    conn.execute(
        "INSERT INTO schema_migrations (version) VALUES (%s) ON CONFLICT DO NOTHING", (version,)
    )


def apply_one(conn: psycopg.Connection, version: str, path: Path) -> None:
    with conn.transaction():
        conn.execute(path.read_text())
        record(conn, version)


def migrate(
    conn: psycopg.Connection,
    files: dict[str, Path],
    baseline: str | None = None,
    log=print,
) -> list[str]:
    """Apply the missing migrations, or record the ones up to ``baseline``. Returns versions.

    An empty database is built from init_db.sql with every migration recorded, so the
    returned list of applied migrations is empty.
    """
    if BOOTSTRAP not in files:
        raise MigrateError(f"migration {BOOTSTRAP} (schema_migrations) is missing")
    done = applied_versions(conn)
    if not done and baseline is None and database_is_empty(conn):
        # One transaction: a failed build leaves the database empty and the next run
        # builds it again. init_db.sql records the migrations it contains; record
        # every file so schema_migrations lists them all.
        with conn.transaction():
            conn.execute(INIT_DB.read_text())
            for version in files:
                record(conn, version)
        log("empty database: built from init_db.sql, recorded every migration as applied")
        return []
    pending = [v for v in files if v not in done]
    if baseline is not None:
        if baseline not in files:
            raise MigrateError(f"--baseline {baseline}: there is no migration with that number")
        if done:
            raise MigrateError("--baseline is only for a database with no schema_migrations rows")
        # 000 creates the table that records the rest, so it runs; the others are only recorded.
        apply_one(conn, BOOTSTRAP, files[BOOTSTRAP])
        for version in pending:
            if version != BOOTSTRAP and version <= baseline:
                with conn.transaction():
                    record(conn, version)
        recorded = [v for v in pending if v <= baseline]
        log(f"recorded migrations {', '.join(recorded)} as already applied")
        pending = [v for v in files if v > baseline]
    applied = []
    for version in pending:
        log(f"applying {files[version].name}")
        apply_one(conn, version, files[version])
        applied.append(version)
    return applied


def connect(
    url: str,
    attempts: int = CONNECT_ATTEMPTS,
    wait: float = CONNECT_WAIT_SECONDS,
    sleep=time.sleep,
    log=print,
) -> psycopg.Connection:
    """Connect as the owner, waiting for a database that is still starting.

    A hosted deploy can run this while Postgres restarts; a refused or timed-out
    connection is tried again ``attempts`` times in all before the error stands.
    """
    for attempt in range(1, attempts + 1):
        try:
            return psycopg.connect(url, autocommit=True, connect_timeout=CONNECT_TIMEOUT_SECONDS)
        except psycopg.OperationalError:
            if attempt == attempts:
                raise
            log(f"the database is not answering yet; trying again ({attempt}/{attempts})")
            sleep(wait)
    raise AssertionError("unreachable")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--set-app-password",
        action="store_true",
        help=f"reset the password of {APP_ROLE} to {APP_PASSWORD_VAR} (otherwise left alone)",
    )
    parser.add_argument(
        "--baseline",
        metavar="NUMBER",
        help="record every migration up to NUMBER as applied without running it",
    )
    args = parser.parse_args(argv)
    url = os.environ.get(OWNER_URL_VAR)
    if not url:
        print(f"{OWNER_URL_VAR} is not set. Export the owner role's URL in this shell.")
        return 2
    try:
        files = migration_files()
        with connect(url) as conn:
            role = ensure_app_role(conn, os.environ.get(APP_PASSWORD_VAR), args.set_app_password)
            print(f"role {APP_ROLE}: {role}")
            applied = migrate(conn, files, args.baseline)
    except (MigrateError, psycopg.Error) as err:
        print(f"migration stopped: {err}")
        return 1
    print(f"applied {len(applied)} migration(s)" if applied else "the database is up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
