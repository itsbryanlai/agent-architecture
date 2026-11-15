"""Apply the database migrations that this database has not had, in order.

Run it as the database owner, not as the app role::

    export AGENTOS_OWNER_DATABASE_URL=postgresql://<owner>:<password>@localhost:55432/agentos
    export AGENTOS_APP_DB_PASSWORD=<the password agentos_app will have>
    uv run python scripts/migrate.py

``schema_migrations`` records each migration applied. Each migration runs in its own
transaction together with its row there, so a failed one leaves no trace and the next
run starts from it.

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
from pathlib import Path

import psycopg
from psycopg import sql

APP_ROLE = "agentos_app"
OWNER_URL_VAR = "AGENTOS_OWNER_DATABASE_URL"
APP_PASSWORD_VAR = "AGENTOS_APP_DB_PASSWORD"
MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
_FILE = re.compile(r"(\d{3})_[a-z0-9_]+\.sql")
# The migration that creates schema_migrations. It runs before the table can be read.
BOOTSTRAP = "000"


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
    """Apply the missing migrations, or record the ones up to ``baseline``. Returns versions."""
    if BOOTSTRAP not in files:
        raise MigrateError(f"migration {BOOTSTRAP} (schema_migrations) is missing")
    done = applied_versions(conn)
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
        with psycopg.connect(url, autocommit=True) as conn:
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
