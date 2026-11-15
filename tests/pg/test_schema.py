"""init_db.sql, the migrations, migrate.py and the start-up version check, against Postgres."""

from __future__ import annotations

import os

import pytest

from agentos.models import AUDIT_KINDS
from agentos.schema_version import EXPECTED_VERSIONS, missing_versions, require_current_schema
from tests.pg import catalog
from tests.pg.conftest import BASE_SCHEMA, MIGRATIONS, Databases, TestDatabase, load_migrate


@pytest.fixture(scope="session")
def migrated(databases: Databases) -> TestDatabase:
    """The pre-migration schema, then every migration through migrate.py."""
    migrate = load_migrate()
    database = databases.create()
    database.execute_file(BASE_SCHEMA)
    with database.owner() as conn:
        migrate.migrate(conn, migrate.migration_files(), log=lambda _line: None)
    return database


def test_init_db_equals_the_migrations(db: TestDatabase, migrated: TestDatabase):
    """Fails if init_db.sql builds a different schema than the migrations do, or records other versions."""
    with db.owner() as a, migrated.owner() as b:
        assert catalog.difference(catalog.dump(a), catalog.dump(b)) == []


def test_every_migration_can_run_again(db: TestDatabase, migrated: TestDatabase):
    """Fails if rerunning the migrations in order over an empty database changes the schema."""
    with migrated.owner() as conn:
        before = catalog.dump(conn)
        for path in sorted(MIGRATIONS.glob("*.sql")):
            conn.execute(path.read_text())
        assert catalog.difference(before, catalog.dump(conn)) == []


def test_baseline_then_migrate_reaches_the_same_schema(databases: Databases, db: TestDatabase):
    """Fails if a database built through 015 by hand cannot be brought in with --baseline 015."""
    migrate = load_migrate()
    database = databases.create()
    database.execute_file(BASE_SCHEMA)
    with database.owner() as conn:
        for path in sorted(MIGRATIONS.glob("*.sql")):
            if path.name < "016":
                conn.execute(path.read_text())
        applied = migrate.migrate(
            conn, migrate.migration_files(), baseline="015", log=lambda _line: None
        )
        assert applied == ["016", "017"]
        with db.owner() as reference:
            assert catalog.difference(catalog.dump(reference), catalog.dump(conn)) == []


def test_code_lists_the_migrations_and_audit_kinds_the_database_has(db: TestDatabase):
    """Fails if EXPECTED_VERSIONS or AUDIT_KINDS drifts from the migration files or audit_kinds."""
    with db.owner() as conn:
        kinds = {row[0] for row in conn.execute("SELECT kind FROM audit_kinds")}
    assert kinds == set(AUDIT_KINDS)
    assert EXPECTED_VERSIONS == tuple(sorted(load_migrate().migration_files()))


@pytest.mark.parametrize(
    ("break_it", "named"),
    [
        ("DELETE FROM schema_migrations WHERE version = '016'", ["016"]),
        ("DROP TABLE schema_migrations", list(EXPECTED_VERSIONS)),
        ("SELECT 1", []),
    ],
    ids=["one migration missing", "never migrated", "current"],
)
async def test_start_up_refuses_a_database_behind_the_code(
    databases: Databases, break_it: str, named: list[str]
):
    """Fails if a process starts on a database missing migrations, or refuses a current one."""
    database = databases.create(init=True)
    with database.owner() as conn:
        conn.execute(break_it)
    pool = database.pool()
    await pool.open()
    try:
        assert await missing_versions(pool) == named
        if named:
            with pytest.raises(SystemExit) as stopped:
                await require_current_schema(pool)
            assert all(version in str(stopped.value) for version in named)
        else:
            await require_current_schema(pool)
    finally:
        await pool.close()


async def test_migrate_creates_the_app_role_only_when_missing(databases: Databases):
    """Fails if migrate.py resets an existing role's password, or creates a role with none."""
    import psycopg

    migrate = load_migrate()
    role = f"agentos_app_t{os.getpid()}"
    database = databases.create()
    login = lambda password: psycopg.connect(
        database.owner_url, user=role, password=password, connect_timeout=3
    ).close()
    with database.owner() as conn:
        try:
            with pytest.raises(migrate.MigrateError, match="AGENTOS_APP_DB_PASSWORD"):
                migrate.ensure_app_role(conn, None, False, role=role)
            assert migrate.ensure_app_role(conn, "first-pw", False, role=role) == "created"
            assert migrate.ensure_app_role(conn, "other-pw", False, role=role) == "kept"
            login("first-pw")
            with pytest.raises(psycopg.OperationalError):
                login("other-pw")
            assert migrate.ensure_app_role(conn, "other-pw", True, role=role) == "password set"
            login("other-pw")
        finally:
            conn.execute(f'DROP ROLE IF EXISTS "{role}"')


def test_migrate_script_applies_everything_once_and_names_a_missing_url(
    databases: Databases, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
):
    """Fails if the script cannot build a database from the base schema, repeats work, or runs with no URL."""
    migrate = load_migrate()
    database = databases.create()
    database.execute_file(BASE_SCHEMA)

    monkeypatch.delenv("AGENTOS_OWNER_DATABASE_URL", raising=False)
    assert migrate.main([]) == 2
    assert "AGENTOS_OWNER_DATABASE_URL is not set" in capsys.readouterr().out

    monkeypatch.setenv("AGENTOS_OWNER_DATABASE_URL", database.owner_url)
    assert migrate.main([]) == 0
    assert f"applied {len(EXPECTED_VERSIONS)} migration(s)" in capsys.readouterr().out
    assert migrate.main([]) == 0
    assert "up to date" in capsys.readouterr().out
    with database.owner() as conn:
        versions = [row[0] for row in conn.execute("SELECT version FROM schema_migrations")]
    assert sorted(versions) == list(EXPECTED_VERSIONS)
