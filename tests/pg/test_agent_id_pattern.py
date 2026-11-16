"""The agent ID pattern, as a CHECK on every column that holds an agent ID."""

from __future__ import annotations

import pytest
from psycopg import errors

from agentos.schema_version import EXPECTED_VERSIONS
from tests.pg.conftest import BASE_SCHEMA, Databases, TestDatabase, load_migrate
from tests.pg.test_policies import seed_version

# One row per column, with the agent ID as the only thing that varies (the test fills %s).
COLUMNS = {
    "agents": "INSERT INTO agents (tenant_id, agent_id, created_by) VALUES (%(t)s, %(a)s, 'u')",
    "agent_versions": "INSERT INTO agent_versions (tenant_id, agent_id, version, status, "
    "definition, created_by) VALUES (%(t)s, %(a)s, 1, 'draft', '{}', 'u')",
    "audit_events": "INSERT INTO audit_events (tenant_id, event_key, kind, agent_id, "
    "agent_version, requester_user_id, identity, outcome) "
    "VALUES (%(t)s, 'e', 'answer', %(a)s, 1, 'u', 'agent:x', 'ok')",
    "usage_events": "INSERT INTO usage_events (tenant_id, usage_key, kind, agent_id, "
    "agent_version, model) VALUES (%(t)s, 'k', 'llm', %(a)s, 1, 'm')",
    "budgets": "INSERT INTO budgets (tenant_id, agent_id, amount_usd, updated_by) "
    "VALUES (%(t)s, %(a)s, 10, 'u')",
    "audience_pauses": "INSERT INTO audience_pauses (tenant_id, agent_id, channel, reason) "
    "VALUES (%(t)s, %(a)s, 'C1', 'channel_shared')",
    "grant_picks": "INSERT INTO grant_picks (tenant_id, agent_id, notion_id, kind, picked_by, "
    "picked_at) VALUES (%(t)s, %(a)s, 'n', 'page', 'u', now())",
    "kill_switches": "INSERT INTO kill_switches (tenant_id, scope, target, turned_off_by) "
    "VALUES (%(t)s, 'agent', %(a)s, 'u')",
}

BAD_IDS = ["a", "A1", "1ab", "a/b", "a:b", "a b", "it_helper", "it-hеlper", "a" * 41, "ab\n"]


def insert(conn, tenant: str, table: str, agent_id: str) -> None:
    if table == "agent_versions":
        conn.execute(
            "INSERT INTO agents (tenant_id, agent_id, created_by) VALUES (%s, %s, 'u') "
            "ON CONFLICT DO NOTHING",
            (tenant, "ok-parent" if agent_id in BAD_IDS else agent_id),
        )
    conn.execute(COLUMNS[table], {"t": tenant, "a": agent_id})


@pytest.mark.parametrize("bad", BAD_IDS, ids=repr)
@pytest.mark.parametrize("table", sorted(COLUMNS))
def test_a_column_refuses_an_agent_id_that_breaks_the_pattern(
    db: TestDatabase, tenant: str, table: str, bad: str
):
    """Fails if any table that holds an agent ID accepts a slash, a colon, a Cyrillic
    look-alike, a capital, an underscore, a one-letter ID or an ID over 40 characters."""
    with db.owner() as conn, pytest.raises(errors.CheckViolation):
        insert(conn, tenant, table, bad)


@pytest.mark.parametrize("good", ["it-helper", "a1", "a" + "b" * 39])
@pytest.mark.parametrize("table", sorted(COLUMNS))
def test_a_column_accepts_an_agent_id_the_pattern_allows(
    db: TestDatabase, tenant: str, table: str, good: str
):
    """Fails if the CHECK refuses an ID within the pattern (2 or 40 characters, digits, hyphens)."""
    with db.owner() as conn:
        insert(conn, tenant, table, good)


def test_migrating_over_a_row_that_breaks_the_pattern_stops_and_names_it(databases: Databases):
    """Fails if 023 skips a stored ID that breaks the pattern, or names neither its table,
    tenant nor ID, or leaves the constraint half-applied."""
    migrate = load_migrate()
    database = databases.create()
    database.execute_file(BASE_SCHEMA)
    files = migrate.migration_files()
    with database.owner() as conn:
        migrate.migrate(
            conn, {v: p for v, p in files.items() if v <= "021"}, log=lambda _line: None
        )
        seed_version(conn, "acme", "fine-one", "draft")
        conn.execute(
            "INSERT INTO agents (tenant_id, agent_id, created_by) VALUES ('acme', 'a/b', 'u')"
        )
        with pytest.raises(errors.RaiseException) as raised:
            migrate.migrate(conn, files, log=lambda _line: None)
        assert "agents acme/a/b" in str(raised.value)
        conn.execute("DELETE FROM agents WHERE agent_id = 'a/b'")
        applied = migrate.migrate(conn, files, log=lambda _line: None)
        assert applied == [v for v in EXPECTED_VERSIONS if v >= "022"]
