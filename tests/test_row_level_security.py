"""Row-level security: the store side (every call sets the tenant first) and the SQL text.

The policies themselves are checked against a real Postgres outside this suite.
"""

import hashlib
import inspect
import re
import types
import typing
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import pytest

from agentos import agent_store, audit, members, sessions, stores
from agentos.agent_store import PostgresAgentStore
from agentos.audit import PostgresAuditStore
from agentos.config import (
    DEFAULT_DATABASE_URL,
    AgentVersion,
    agent_definition,
    parse_agents_config,
)
from agentos.knowledge import store as knowledge_store
from agentos.knowledge.store import PostgresKnowledgeStore
from agentos.members import PostgresMemberStore
from agentos.models import AuditEvent
from agentos.sessions import PostgresSessionStore
from agentos.stores import (
    PostgresHistoryStore,
    PostgresIdempotencyStore,
    PostgresTaskLinkStore,
    PostgresToolOutputStore,
    tenant_transaction,
)
from tests.conftest import agents_dict

ROOT = Path(__file__).resolve().parent.parent
INIT_DB = (ROOT / "scripts" / "init_db.sql").read_text()
MIGRATION = (ROOT / "scripts" / "migrations" / "004_row_level_security.sql").read_text()
AUDIT_MIGRATION = (ROOT / "scripts" / "migrations" / "005_audit_log.sql").read_text()
AGENTS_MIGRATION = (ROOT / "scripts" / "migrations" / "006_agents.sql").read_text()
MEMBERS_MIGRATION = (ROOT / "scripts" / "migrations" / "007_members.sql").read_text()
SESSIONS_MIGRATION = (ROOT / "scripts" / "migrations" / "008_sessions.sql").read_text()

POSTGRES_STORES = [
    PostgresHistoryStore,
    PostgresIdempotencyStore,
    PostgresToolOutputStore,
    PostgresTaskLinkStore,
    PostgresKnowledgeStore,
    PostgresAuditStore,
    PostgresAgentStore,
    PostgresMemberStore,
    PostgresSessionStore,
]


class FakeCursor:
    rowcount = 0

    def __init__(self, row=None):
        self.row = row

    async def fetchone(self):
        return (1,) if self.row is None else self.row

    async def fetchall(self):
        return [] if self.row is None else [self.row]


class FakeConnection:
    def __init__(self, events: list, row=None):
        self.events = events
        self.row = row

    async def execute(self, sql, params=None):
        self.events.append(("execute", " ".join(sql.split()), params))
        return FakeCursor(self.row)

    @asynccontextmanager
    async def transaction(self):
        self.events.append(("transaction",))
        yield
        self.events.append(("commit",))


class FakePool:
    def __init__(self, row=None):
        self.events: list = []
        self.taken = 0
        # A row every query returns, for stores that read what they fetch.
        self.row = row

    @asynccontextmanager
    async def connection(self):
        self.taken += 1
        yield FakeConnection(self.events, self.row)


def sample_value(hint):
    if hint is str:
        return "value"
    if hint is int:
        return 1
    if hint is float:
        return 0.5
    if hint is bool:
        return False
    if hint is dict:
        return {"k": "v"}
    if hint is datetime:
        return datetime(2026, 1, 1)
    if hint is timedelta:
        return timedelta(hours=8)
    if typing.get_origin(hint) is types.UnionType:
        return sample_value(next(a for a in typing.get_args(hint) if a is not type(None)))
    if hint is AuditEvent:
        return AuditEvent("acme", "k1", "answer", "a", 1, "U1", "agent:a")
    if hint is AgentVersion:
        return SAMPLE_AGENT
    origin = typing.get_origin(hint)
    args = typing.get_args(hint)
    if origin is dict:
        return {}
    if origin is list:
        return [sample_value(args[0])]
    if origin is tuple:
        return tuple(sample_value(a) for a in args)
    raise AssertionError(f"no sample value for {hint}")


SAMPLE_AGENT = parse_agents_config(agents_dict()).agents["it-helper"]
# The agent store parses what it stores and what it reads back, so it is given a
# real definition and a real row.
AGENT_ROW = (
    "acme",
    "it-helper",
    1,
    "pending",
    agent_definition(SAMPLE_AGENT),
    "U1",
    None,
    "U1",
    None,
    None,
    None,
)


AGENT_RECORD_ROW = ("acme", "it-helper", "U1", None)
MEMBER_ROW = ("acme", "U1", "admin", "U0", None)
SESSION_ROW = ("acme", "U1", "csrf-secret", datetime(2026, 1, 1), datetime(2026, 1, 2))


def row_for(cls, method_name):
    """The row a store's reads need, since the fake returns it for every query."""
    if cls is PostgresMemberStore:
        return MEMBER_ROW
    if cls is PostgresSessionStore:
        return SESSION_ROW
    if cls is PostgresAgentStore:
        return AGENT_RECORD_ROW if method_name in ("get_agent", "list_agents") else AGENT_ROW
    return None


def public_methods(cls):
    return [
        name
        for name, _ in inspect.getmembers(cls, inspect.iscoroutinefunction)
        if not name.startswith("_")
    ]


def test_store_classes_are_all_covered():
    # A new Postgres store in any of these modules must be added to POSTGRES_STORES.
    found = set()
    for module in (stores, knowledge_store, audit, agent_store, members, sessions):
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if name.startswith("Postgres") and cls.__module__ == module.__name__:
                found.add(cls)
    assert found == set(POSTGRES_STORES)


async def test_every_postgres_store_method_sets_tenant_first():
    checked = 0
    for cls in POSTGRES_STORES:
        for name in public_methods(cls):
            pool = FakePool(row_for(cls, name))
            method = getattr(cls(pool), name)
            hints = typing.get_type_hints(getattr(cls, name))
            kwargs = {
                p: sample_value(hints[p])
                for p in inspect.signature(method).parameters
                if p != "tenant_id" and p in hints
            }
            if "definition" in kwargs:
                kwargs["definition"] = agent_definition(SAMPLE_AGENT)
            if "role" in kwargs:
                kwargs["role"] = "creator"
            if "agents" in kwargs:
                kwargs["agents"] = {"it-helper": SAMPLE_AGENT}
            if "tenant_id" in inspect.signature(method).parameters:
                kwargs["tenant_id"] = "acme"
            await method(**kwargs)

            label = f"{cls.__name__}.{name}"
            assert pool.taken == 1, label
            events = pool.events
            assert events[0] == ("transaction",), label
            first = events[1]
            assert first[0] == "execute", label
            assert first[1] == "SELECT set_config('app.tenant_id', %s, true)", label
            assert first[2] == ("acme",), label
            # Everything runs inside the one transaction, which is closed last.
            assert events[-1] == ("commit",), label
            assert events.count(("transaction",)) == 1, label
            checked += 1
    assert checked >= 16


async def test_tenant_transaction_rejects_empty_tenant():
    pool = FakePool()
    for bad in ("", None):
        with pytest.raises(ValueError):
            async with tenant_transaction(pool, bad):
                pass
    assert pool.taken == 0
    assert pool.events == []


def tenant_tables(sql: str) -> set[str]:
    tables = set()
    for match in re.finditer(
        r"CREATE TABLE(?: IF NOT EXISTS)?\s+(\w+)\s*\((.*?)\n\);", sql, re.DOTALL | re.IGNORECASE
    ):
        if re.search(r"^\s*tenant_id\s", match.group(2), re.MULTILINE):
            tables.add(match.group(1))
    return tables


def rls_tables(sql: str) -> set[str]:
    match = re.search(r"FOREACH t IN ARRAY ARRAY\[(.*?)\]", sql, re.DOTALL)
    assert match, "no RLS table list found"
    return set(re.findall(r"'(\w+)'", match.group(1)))


def rls_tables_forced(sql: str) -> set[str]:
    return set(re.findall(r"ALTER TABLE (\w+) FORCE ROW LEVEL SECURITY", sql))


def test_migration_and_init_db_cover_every_tenant_table():
    tables = tenant_tables(INIT_DB)
    assert len(tables) >= 6
    assert tables <= (
        rls_tables(MIGRATION)
        | rls_tables_forced(AUDIT_MIGRATION)
        | rls_tables_forced(AGENTS_MIGRATION)
        | rls_tables_forced(MEMBERS_MIGRATION)
        | rls_tables_forced(SESSIONS_MIGRATION)
    )
    # init_db.sql carries the same block, after the tables it protects.
    assert tables <= rls_tables(INIT_DB) | rls_tables_forced(INIT_DB)
    for table in rls_tables_forced(INIT_DB):
        assert INIT_DB.index(f"CREATE TABLE IF NOT EXISTS {table}") < INIT_DB.index(
            f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY"
        ), table
    block_start = INIT_DB.index("FOREACH t IN ARRAY")
    for table in tables & rls_tables(INIT_DB):
        created = re.search(rf"CREATE TABLE (?:IF NOT EXISTS )?{table}\b", INIT_DB)
        assert created and created.start() < block_start, table
    assert "FORCE ROW LEVEL SECURITY" in INIT_DB
    assert "NOBYPASSRLS" in INIT_DB


def test_default_database_url_uses_app_role():
    assert urlparse(DEFAULT_DATABASE_URL).username == "agentos_app"


@pytest.mark.parametrize("sql", [MEMBERS_MIGRATION, INIT_DB], ids=["migration", "init_db"])
def test_members_sql_is_tenant_scoped_and_lets_the_app_role_delete(sql):
    assert "PRIMARY KEY (tenant_id, user_id)" in sql
    assert "CHECK (role IN ('admin', 'creator'))" in sql
    assert "ALTER TABLE members ENABLE ROW LEVEL SECURITY" in sql
    assert "ALTER TABLE members FORCE ROW LEVEL SECURITY" in sql
    assert "CREATE POLICY tenant_isolation ON members" in sql
    assert "USING (tenant_id = current_setting('app.tenant_id', true))" in sql
    assert "WITH CHECK (tenant_id = current_setting('app.tenant_id', true))" in sql
    assert "REVOKE ALL ON members FROM agentos_app" in sql
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON members TO agentos_app" in sql


def test_init_db_matches_the_members_migration_and_widens_the_audit_log():
    block = MEMBERS_MIGRATION[MEMBERS_MIGRATION.index("CREATE TABLE IF NOT EXISTS members") :]
    block = block[: block.index("ALTER TABLE audit_events")].rstrip()
    assert block in INIT_DB
    kinds = [
        *("agent_create", "agent_draft", "agent_submit", "agent_withdraw"),
        *("agent_reject", "agent_publish", "member_add", "member_remove", "member_role"),
    ]
    for kind in kinds:
        assert f"'{kind}'" in MEMBERS_MIGRATION
        assert f"'{kind}'" in INIT_DB
    assert "ALTER COLUMN agent_id DROP NOT NULL" in MEMBERS_MIGRATION
    assert "ALTER COLUMN agent_version DROP NOT NULL" in MEMBERS_MIGRATION
    assert "audit_events_agent_required" in INIT_DB


@pytest.mark.parametrize("sql", [SESSIONS_MIGRATION, INIT_DB], ids=["migration", "init_db"])
def test_sessions_sql_is_tenant_scoped_and_never_updated_by_the_app_role(sql):
    assert "PRIMARY KEY (tenant_id, session_hash)" in sql
    assert "ALTER TABLE sessions ENABLE ROW LEVEL SECURITY" in sql
    assert "ALTER TABLE sessions FORCE ROW LEVEL SECURITY" in sql
    assert "CREATE POLICY tenant_isolation ON sessions" in sql
    assert "USING (tenant_id = current_setting('app.tenant_id', true))" in sql
    assert "WITH CHECK (tenant_id = current_setting('app.tenant_id', true))" in sql
    assert "REVOKE ALL ON sessions FROM agentos_app" in sql
    assert "GRANT SELECT, INSERT, DELETE ON sessions TO agentos_app" in sql
    assert "UPDATE ON sessions" not in sql
    # The session ID itself has no column: only its hash is stored.
    assert "session_hash" in sql and "session_id" not in sql


def test_init_db_matches_the_sessions_migration():
    block = SESSIONS_MIGRATION[SESSIONS_MIGRATION.index("CREATE TABLE IF NOT EXISTS sessions") :]
    assert block.rstrip() in INIT_DB


async def test_session_store_queries_are_tenant_scoped_and_store_only_the_hash():
    pool = FakePool(SESSION_ROW)
    store = PostgresSessionStore(pool)
    now = datetime(2026, 1, 1)
    issued = await store.create("acme", "U1", now)
    await store.get("acme", issued.token, now)
    await store.delete("acme", issued.token)
    sql = [e for e in pool.events if e[0] == "execute" and "set_config" not in e[1]]
    assert all("tenant_id = %s" in e[1] or "INSERT" in e[1] for e in sql)
    for event in sql:
        params = event[2]
        assert issued.token not in params
    inserted = next(e for e in sql if "INSERT" in e[1])
    assert hashlib.sha256(issued.token.encode()).hexdigest() in inserted[2]
    selected = next(e for e in sql if e[1].startswith("SELECT"))
    assert "expires_at > %s" in selected[1]
