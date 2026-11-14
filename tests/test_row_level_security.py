"""Row-level security: the store side (every call sets the tenant first) and the SQL text.

The policies themselves are checked against a real Postgres outside this suite.
"""

import inspect
import re
import types
import typing
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pytest

from agentos import audit, stores
from agentos.audit import PostgresAuditStore
from agentos.config import DEFAULT_DATABASE_URL
from agentos.knowledge import store as knowledge_store
from agentos.knowledge.store import PostgresKnowledgeStore
from agentos.models import AuditEvent
from agentos.stores import (
    PostgresHistoryStore,
    PostgresIdempotencyStore,
    PostgresTaskLinkStore,
    PostgresToolOutputStore,
    tenant_transaction,
)

ROOT = Path(__file__).resolve().parent.parent
INIT_DB = (ROOT / "scripts" / "init_db.sql").read_text()
MIGRATION = (ROOT / "scripts" / "migrations" / "004_row_level_security.sql").read_text()
AUDIT_MIGRATION = (ROOT / "scripts" / "migrations" / "005_audit_log.sql").read_text()

POSTGRES_STORES = [
    PostgresHistoryStore,
    PostgresIdempotencyStore,
    PostgresToolOutputStore,
    PostgresTaskLinkStore,
    PostgresKnowledgeStore,
    PostgresAuditStore,
]


class FakeCursor:
    rowcount = 0

    async def fetchone(self):
        return (1,)

    async def fetchall(self):
        return []


class FakeConnection:
    def __init__(self, events: list):
        self.events = events

    async def execute(self, sql, params=None):
        self.events.append(("execute", " ".join(sql.split()), params))
        return FakeCursor()

    @asynccontextmanager
    async def transaction(self):
        self.events.append(("transaction",))
        yield
        self.events.append(("commit",))


class FakePool:
    def __init__(self):
        self.events: list = []
        self.taken = 0

    @asynccontextmanager
    async def connection(self):
        self.taken += 1
        yield FakeConnection(self.events)


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
    if typing.get_origin(hint) is types.UnionType:
        return sample_value(next(a for a in typing.get_args(hint) if a is not type(None)))
    if hint is AuditEvent:
        return AuditEvent("acme", "k1", "answer", "a", 1, "U1", "agent:a")
    origin = typing.get_origin(hint)
    args = typing.get_args(hint)
    if origin is list:
        return [sample_value(args[0])]
    if origin is tuple:
        return tuple(sample_value(a) for a in args)
    raise AssertionError(f"no sample value for {hint}")


def public_methods(cls):
    return [
        name
        for name, _ in inspect.getmembers(cls, inspect.iscoroutinefunction)
        if not name.startswith("_")
    ]


def test_store_classes_are_all_covered():
    # A new Postgres store in any of these modules must be added to POSTGRES_STORES.
    found = set()
    for module in (stores, knowledge_store, audit):
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if name.startswith("Postgres") and cls.__module__ == module.__name__:
                found.add(cls)
    assert found == set(POSTGRES_STORES)


async def test_every_postgres_store_method_sets_tenant_first():
    checked = 0
    for cls in POSTGRES_STORES:
        for name in public_methods(cls):
            pool = FakePool()
            method = getattr(cls(pool), name)
            hints = typing.get_type_hints(getattr(cls, name))
            kwargs = {
                p: sample_value(hints[p])
                for p in inspect.signature(method).parameters
                if p != "tenant_id" and p in hints
            }
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
    assert checked >= 11


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
    assert tables <= rls_tables(MIGRATION) | rls_tables_forced(AUDIT_MIGRATION)
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
