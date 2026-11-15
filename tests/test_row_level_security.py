"""Row-level security, store side: every call sets the tenant first.

The policies, grants and triggers themselves run against a real Postgres in tests/pg.
"""

import hashlib
import inspect
import types
import typing
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from agentos import (
    agent_store,
    audience_pauses,
    audit,
    budgets,
    members,
    sessions,
    stores,
    switches,
    usage,
)
from agentos.agent_store import PostgresAgentStore
from agentos.audience_pauses import PostgresPauseStore
from agentos.audit import PostgresAuditStore
from agentos.budgets import PostgresBudgetStore
from agentos.config import (
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
from agentos.switches import PostgresSwitchStore
from agentos.usage import PostgresUsageStore, UsageEvent
from tests.conftest import agents_dict

ROOT = Path(__file__).resolve().parent.parent
USAGE_MIGRATION = (ROOT / "scripts" / "migrations" / "011_usage.sql").read_text()

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
    PostgresUsageStore,
    PostgresBudgetStore,
    PostgresPauseStore,
    PostgresSwitchStore,
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
    if hint is Decimal:
        return Decimal("5.00")
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
    if hint is UsageEvent:
        return UsageEvent(
            tenant_id="acme", usage_key="k", kind="llm", agent_id="a", agent_version=1, model="m"
        )
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
    "value",  # definition_sha256: the sample hash every str parameter gets
)


AGENT_RECORD_ROW = ("acme", "it-helper", "U1", None)
MEMBER_ROW = ("acme", "U1", "admin", "U0", None)
BUDGET_ROW = ("acme", None, Decimal("5.00"), "U1", datetime(2026, 1, 1))
PAUSE_ROW = ("acme", "it-helper", "C0AAAA111", "channel_shared", datetime(2026, 1, 1), None, None)
SESSION_ROW = ("acme", "U1", "csrf-secret", datetime(2026, 1, 1), datetime(2026, 1, 2))


def row_for(cls, method_name):
    """The row a store's reads need, since the fake returns it for every query."""
    if cls is PostgresMemberStore:
        return MEMBER_ROW
    if cls is PostgresSessionStore:
        return SESSION_ROW
    if cls is PostgresPauseStore:
        return PAUSE_ROW
    if cls is PostgresUsageStore:
        return None
    if cls is PostgresBudgetStore:
        return BUDGET_ROW
    if cls is PostgresAuditStore and method_name == "get":
        event = AuditEvent("acme", "k1", "answer", "a", 1, "U1", "agent:a")
        return tuple(getattr(event, name) for name in audit._READ_COLUMN_NAMES)
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
    for module in (
        stores,
        knowledge_store,
        audit,
        agent_store,
        members,
        sessions,
        usage,
        budgets,
        audience_pauses,
        switches,
    ):
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if name.startswith("Postgres") and cls.__module__ == module.__name__:
                found.add(cls)
    assert found == set(POSTGRES_STORES)


async def test_tenant_transaction_rejects_empty_tenant():
    pool = FakePool()
    for bad in ("", None):
        with pytest.raises(ValueError):
            async with tenant_transaction(pool, bad):
                pass
    assert pool.taken == 0
    assert pool.events == []


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


async def test_the_postgres_import_audits_in_the_same_transaction_as_the_insert():
    pool = FakePool(AGENT_ROW)
    store = PostgresAgentStore(pool)

    await store.import_agents("acme", {"it-helper": SAMPLE_AGENT})

    assert pool.taken == 1 and pool.events.count(("transaction",)) == 1
    executed = [e for e in pool.events if e[0] == "execute"]
    insert_version = next(i for i, e in enumerate(executed) if "INTO agent_versions" in e[1])
    audit = next(e for e in executed if "INTO audit_events" in e[1])
    assert executed.index(audit) > insert_version
    assert "ON CONFLICT (tenant_id, event_key) DO NOTHING" in audit[1]
    assert audit[2][:5] == ("acme", "acme:audit:import:it-helper", "agent_import", "it-helper", 1)
    assert pool.events[-1] == ("commit",)


def test_a_usage_row_has_no_requester_column_and_no_content_column():
    columns = USAGE_MIGRATION[USAGE_MIGRATION.index("CREATE TABLE") :]
    columns = columns[: columns.index("\n);")]
    for word in ("user", "requester", "text_body", "content", "prompt"):
        assert word not in columns


async def test_the_usage_insert_ignores_a_repeat_and_names_the_tenant():
    pool = FakePool()
    store = PostgresUsageStore(pool)

    await store.record(sample_value(UsageEvent))

    insert = next(e for e in pool.events if e[0] == "execute" and "INSERT" in e[1])
    assert "ON CONFLICT (tenant_id, usage_key) DO NOTHING" in insert[1]
    assert insert[2][0] == "acme"
