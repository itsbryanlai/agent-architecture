"""Row-level security, grants, triggers and indexes, exercised as the app role."""

from __future__ import annotations

import pytest
from psycopg import errors

from agentos.stores import tenant_transaction
from tests.helpers.pg_seed import seed_version
from tests.pg.conftest import TestDatabase

VECTOR = "(SELECT array_fill(0.5::real, ARRAY[1024])::vector)"
# One valid row per tenant table, written in this order (a child after its parent). A
# table added to the schema without a row here fails the first test below.
SEEDS = {
    "conversation_messages": "INSERT INTO conversation_messages "
    "(tenant_id, conversation_id, seq, role, content) VALUES (%(t)s, 'c', 1, 'user', '[]')",
    "idempotency_records": "INSERT INTO idempotency_records (tenant_id, idem_key, value) "
    "VALUES (%(t)s, 'k', '{}')",
    "task_links": "INSERT INTO task_links (tenant_id, page_id, task_id) VALUES (%(t)s, 'p', 'k')",
    "tool_outputs": "INSERT INTO tool_outputs "
    "(tenant_id, conversation_id, tool_use_id, content, is_error) "
    "VALUES (%(t)s, 'c', 'u', 'x', false)",
    "knowledge_pages": "INSERT INTO knowledge_pages "
    "(tenant_id, scope, page_id, title, url, content_hash) "
    "VALUES (%(t)s, 's', 'p', 'T', 'u', 'h')",
    "knowledge_chunks": "INSERT INTO knowledge_chunks "
    "(tenant_id, scope, page_id, chunk_no, text, embedding) "
    f"VALUES (%(t)s, 's', 'p', 0, 'x', {VECTOR})",
    "knowledge_scope_health": "INSERT INTO knowledge_scope_health "
    "(tenant_id, scope, consecutive_gone, last_run) VALUES (%(t)s, 's', 1, 'r')",
    "audit_events": "INSERT INTO audit_events (tenant_id, event_key, kind, agent_id, "
    "agent_version, requester_user_id, identity, outcome) "
    "VALUES (%(t)s, 'e', 'answer', 'aa', 1, 'u', 'agent:aa', 'ok')",
    "agents": "INSERT INTO agents (tenant_id, agent_id, created_by) VALUES (%(t)s, 'aa', 'u')",
    "agent_versions": "INSERT INTO agent_versions "
    "(tenant_id, agent_id, version, status, definition, created_by) "
    "VALUES (%(t)s, 'aa', 1, 'draft', '{}', 'u')",
    "members": "INSERT INTO members (tenant_id, user_id, role, added_by) "
    "VALUES (%(t)s, 'u', 'admin', 'u')",
    "sessions": "INSERT INTO sessions (tenant_id, session_hash, user_id, csrf_secret, expires_at) "
    "VALUES (%(t)s, 'h', 'u', 'c', now() + interval '1 hour')",
    "usage_events": "INSERT INTO usage_events (tenant_id, usage_key, kind, model) "
    "VALUES (%(t)s, 'k', 'embedding', 'm')",
    "usage_adjustments": "INSERT INTO usage_adjustments "
    "(tenant_id, usage_key, cost_usd, price_model, price_effective_from) "
    "VALUES (%(t)s, 'k', 1, 'm', '2026-11-01')",
    "usage_month_totals": "INSERT INTO usage_month_totals (tenant_id, month, agent_id, kind) "
    "VALUES (%(t)s, '2026-11-01', '', 'llm')",
    "budgets": "INSERT INTO budgets (tenant_id, agent_id, amount_usd, updated_by) "
    "VALUES (%(t)s, NULL, 10, 'u')",
    "audience_pauses": "INSERT INTO audience_pauses (tenant_id, agent_id, channel, reason) "
    "VALUES (%(t)s, 'aa', 'C1', 'channel_shared')",
    "kill_switches": "INSERT INTO kill_switches (tenant_id, scope, target, turned_off_by) "
    "VALUES (%(t)s, 'writes', '', 'u')",
    "tenants": "INSERT INTO tenants (tenant_id, slack_team_id, default_agent) "
    "VALUES (%(t)s, 'T-' || %(t)s, 'aa')",
    "grant_picks": "INSERT INTO grant_picks "
    "(tenant_id, agent_id, notion_id, kind, picked_by, picked_at) "
    "VALUES (%(t)s, 'aa', 'n', 'page', 'u', now())",
    "threads": "INSERT INTO threads (tenant_id, thread_key, agent_id) VALUES (%(t)s, 'k', 'aa')",
    "owner_pings": "INSERT INTO owner_pings "
    "(tenant_id, agent_id, channel, asker_user_id, notified_on) "
    "VALUES (%(t)s, 'aa', 'C1', 'u', '2026-11-16')",
    "inbound_events": "INSERT INTO inbound_events (tenant_id, event_id, payload) "
    "VALUES (%(t)s, 'e', '{}')",
    "model_steps": "INSERT INTO model_steps "
    "(tenant_id, conversation_id, step_key, stop_reason, content) "
    "VALUES (%(t)s, 'c', 'k', 'end_turn', '[]')",
    "tool_inputs": "INSERT INTO tool_inputs (tenant_id, conversation_id, tool_use_id, input) "
    "VALUES (%(t)s, 'c', 'u', '{}')",
    "task_drafts": "INSERT INTO task_drafts (tenant_id, task_id, revision, payload) "
    "VALUES (%(t)s, 'k', 1, '{}')",
    "external_writes": "INSERT INTO external_writes "
    "(tenant_id, idem_key, state, lease_until, lease_token) "
    "VALUES (%(t)s, 'k', 'claimed', now(), 'tok')",
}

# Tables only the owner writes (the app role has SELECT): seeded through the owner connection.
OWNER_WRITTEN = {"usage_adjustments"}

# What the app role may do, per table (the other three privileges are never granted).
PRIVILEGES = {
    "conversation_messages": "SELECT INSERT UPDATE DELETE",
    "idempotency_records": "SELECT INSERT UPDATE DELETE",
    "task_links": "SELECT INSERT UPDATE DELETE",
    "tool_outputs": "SELECT INSERT UPDATE DELETE",
    "knowledge_pages": "SELECT INSERT UPDATE DELETE",
    "knowledge_chunks": "SELECT INSERT UPDATE DELETE",
    "knowledge_scope_health": "SELECT INSERT UPDATE DELETE",
    "audit_events": "SELECT INSERT",
    "agents": "SELECT INSERT",
    "agent_versions": "SELECT INSERT UPDATE",
    "members": "SELECT INSERT UPDATE DELETE",
    "sessions": "SELECT INSERT DELETE",
    "usage_events": "SELECT INSERT",
    "usage_adjustments": "SELECT",
    "usage_month_totals": "SELECT INSERT UPDATE",
    "budgets": "SELECT INSERT UPDATE DELETE",
    "audience_pauses": "SELECT INSERT UPDATE",
    "kill_switches": "SELECT INSERT DELETE",
    "tenants": "SELECT INSERT UPDATE",
    "external_writes": "SELECT INSERT UPDATE",
    "grant_picks": "SELECT INSERT UPDATE",
    "threads": "SELECT INSERT UPDATE DELETE",
    "owner_pings": "SELECT INSERT UPDATE DELETE",
    "inbound_events": "SELECT INSERT UPDATE DELETE",
    "model_steps": "SELECT INSERT DELETE",
    "tool_inputs": "SELECT INSERT DELETE",
    "task_drafts": "SELECT INSERT DELETE",
    "audit_kinds": "",
    "schema_migrations": "SELECT",
}
ALL_PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")


def public_tables(db: TestDatabase, with_tenant_id: bool = False) -> set[str]:
    extra = (
        " AND EXISTS (SELECT 1 FROM information_schema.columns k WHERE k.table_name = t.table_name"
        " AND k.table_schema = 'public' AND k.column_name = 'tenant_id')"
        if with_tenant_id
        else ""
    )
    with db.owner() as conn:
        rows = conn.execute(
            "SELECT t.table_name FROM information_schema.tables t "
            f"WHERE t.table_schema = 'public' AND t.table_type = 'BASE TABLE'{extra}"
        ).fetchall()
    return {name for (name,) in rows}


async def count(pool, tenant: str | None, table: str, where: str = "true", args=()) -> int:
    """Rows of a table the app role sees, with the tenant set (or not set)."""
    query = f"SELECT count(*) FROM {table} WHERE {where}"
    if tenant is None:
        async with pool.connection() as conn:
            return (await (await conn.execute(query, args)).fetchone())[0]
    async with tenant_transaction(pool, tenant) as conn:
        return (await (await conn.execute(query, args)).fetchone())[0]


async def test_every_tenant_table_shows_a_tenant_only_its_own_rows(
    db: TestDatabase, pool, tenant: str, other_tenant: str
):
    """Fails if any tenant table lets a tenant read or write another's rows, or any row with none set."""
    tables = public_tables(db, with_tenant_id=True)
    assert tables == set(SEEDS), "add a seed row to SEEDS for each new tenant table"
    third = f"{tenant}-x"
    failures = []
    for t in (tenant, other_tenant):
        for table, statement in SEEDS.items():
            if table in OWNER_WRITTEN:
                with db.owner() as owner:
                    owner.execute(statement, {"t": t})
                continue
            async with tenant_transaction(pool, t) as conn:
                await conn.execute(statement, {"t": t})
    for table, seed in SEEDS.items():
        if await count(pool, tenant, table) != 1:
            failures.append(f"{table}: a tenant sees rows that are not its own")
        if await count(pool, tenant, table, "tenant_id = %s", (other_tenant,)) != 0:
            failures.append(f"{table}: another tenant's rows are readable")
        if await count(pool, None, table) != 0:
            failures.append(f"{table}: rows are readable with no tenant set")
        try:
            async with tenant_transaction(pool, tenant) as conn:
                await conn.execute(seed, {"t": third})
            failures.append(f"{table}: a row of another tenant was written")
        except errors.InsufficientPrivilege:
            pass
    with db.owner() as conn:
        flags = conn.execute(
            "SELECT relname FROM pg_class WHERE relnamespace = 'public'::regnamespace "
            "AND relkind = 'r' AND NOT (relrowsecurity AND relforcerowsecurity)"
        ).fetchall()
    failures += [
        f"{name}: row-level security is not enabled and forced"
        for (name,) in flags
        if name in SEEDS
    ]
    assert failures == []


def test_the_privilege_table_names_every_table(db: TestDatabase):
    """Fails if a table is added without saying what the app role may do with it."""
    assert public_tables(db) == set(PRIVILEGES)


@pytest.mark.parametrize("table", sorted(PRIVILEGES))
def test_app_role_has_exactly_the_privileges_a_table_allows(db: TestDatabase, table: str):
    """Fails if the app role can change an append-only table, or lost a privilege a store needs."""
    with db.owner() as conn:
        held = {
            privilege
            for privilege in ALL_PRIVILEGES
            if conn.execute(
                "SELECT has_table_privilege('agentos_app', %s, %s)", (table, privilege)
            ).fetchone()[0]
        }
    assert held == set(PRIVILEGES[table].split())


@pytest.mark.parametrize("table", ["audit_events", "usage_events"])
@pytest.mark.parametrize(
    "change", ["UPDATE {table} SET tenant_id = tenant_id", "DELETE FROM {table}"]
)
async def test_app_role_cannot_change_or_delete_a_recorded_row(
    pool, tenant: str, table: str, change: str
):
    """Fails if the app role can rewrite or remove an audit or usage row."""
    async with tenant_transaction(pool, tenant) as conn:
        await conn.execute(SEEDS[table], {"t": tenant})
    with pytest.raises(errors.InsufficientPrivilege):
        async with tenant_transaction(pool, tenant) as conn:
            await conn.execute(change.format(table=table))
    assert await count(pool, tenant, table) == 1


@pytest.mark.parametrize(
    "change",
    ["UPDATE audit_events SET kind = 'write'", "DELETE FROM audit_events", "TRUNCATE audit_events"],
)
def test_audit_log_refuses_changes_from_every_role(db: TestDatabase, tenant: str, change: str):
    """Fails if the owner (or any role) can UPDATE, DELETE or TRUNCATE the audit log."""
    with db.owner() as conn:
        conn.execute(SEEDS["audit_events"], {"t": tenant})
        with pytest.raises(errors.RaiseException, match="append-only"):
            conn.execute(change)


ALLOWED_MOVES = {
    ("draft", "pending"),
    ("pending", "draft"),
    ("pending", "published"),
    ("published", "superseded"),
}
STATUSES = ("draft", "pending", "published", "superseded")
# A version has a definition hash exactly when it is not a draft.
MOVE = "UPDATE agent_versions SET status = %s, definition_sha256 = %s"


def hash_for(status: str) -> str | None:
    return None if status == "draft" else "h"


@pytest.mark.parametrize(
    ("start", "end"),
    [(a, b) for a in STATUSES for b in STATUSES if a != b],
    ids=lambda status: status,
)
async def test_agent_version_moves_only_along_the_lifecycle(
    db: TestDatabase, pool, tenant: str, start: str, end: str
):
    """Fails if a version can change status outside draft, pending, published, superseded."""
    with db.owner() as conn:
        seed_version(conn, tenant, "aa", start)
    move = tenant_transaction(pool, tenant)
    if (start, end) in ALLOWED_MOVES:
        async with move as conn:
            await conn.execute(MOVE, (end, hash_for(end)))
    else:
        with pytest.raises(errors.RaiseException):
            async with move as conn:
                await conn.execute(MOVE, (end, hash_for(end)))


@pytest.mark.parametrize(
    ("status", "allowed"), [("draft", True), ("pending", False), ("published", False)]
)
async def test_definition_changes_only_while_a_version_is_a_draft(
    db: TestDatabase, pool, tenant: str, status: str, allowed: bool
):
    """Fails if a submitted or published definition can be edited in place."""
    with db.owner() as conn:
        seed_version(conn, tenant, "aa", status)
    edit = tenant_transaction(pool, tenant)
    if allowed:
        async with edit as conn:
            await conn.execute("""UPDATE agent_versions SET definition = '{"x": 1}'""")
    else:
        with pytest.raises(errors.RaiseException):
            async with edit as conn:
                await conn.execute("""UPDATE agent_versions SET definition = '{"x": 1}'""")


@pytest.mark.parametrize(("status", "allowed"), [("draft", True), ("pending", False)])
def test_only_a_draft_version_can_be_deleted(
    db: TestDatabase, tenant: str, status: str, allowed: bool
):
    """Fails if a submitted version can be deleted."""
    with db.owner() as conn:
        seed_version(conn, tenant, "aa", status)
        if allowed:
            conn.execute("DELETE FROM agent_versions WHERE tenant_id = %s", (tenant,))
        else:
            with pytest.raises(errors.RaiseException):
                conn.execute("DELETE FROM agent_versions WHERE tenant_id = %s", (tenant,))


def test_an_agent_has_at_most_one_published_version(db: TestDatabase, tenant: str):
    """Fails if two versions of one agent can be published at once."""
    with db.owner() as conn:
        seed_version(conn, tenant, "aa", "published", version=1)
        with pytest.raises(errors.UniqueViolation):
            seed_version(conn, tenant, "aa", "published", version=2)
        seed_version(conn, tenant, "bb", "published", version=1)


async def test_directory_functions_return_ids_only_and_need_no_tenant(
    pool, tenant: str, other_tenant: str
):
    """Fails if the lookup functions return settings, or the app role can read tenants without them."""
    for t in (tenant, other_tenant):
        async with tenant_transaction(pool, t) as conn:
            await conn.execute(SEEDS["tenants"], {"t": t})
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM tenant_for_slack_team(%s)", (f"T-{tenant}",))
        assert [column.name for column in cur.description] == [
            "tenant_id",
            "enabled",
            "updated_at",
        ]
        assert [row[0] for row in await cur.fetchall()] == [tenant]
        cur = await conn.execute("SELECT * FROM list_tenants()")
        assert [column.name for column in cur.description] == [
            "tenant_id",
            "enabled",
            "updated_at",
        ]
        assert {tenant, other_tenant} <= {row[0] for row in await cur.fetchall()}
    assert await count(pool, None, "tenants") == 0


def test_directory_functions_are_not_open_to_every_role(db: TestDatabase):
    """Fails if the lookup functions that read across tenants can be run by any role, or the app role holds the directory role."""
    with db.owner() as conn:
        member = conn.execute("SELECT pg_has_role('agentos_app', 'agentos_directory', 'member')")
        assert member.fetchone()[0] is False
        public = conn.execute(
            "SELECT p.proname FROM pg_proc p, aclexplode(p.proacl) a "
            "WHERE p.proname IN ('tenant_for_slack_team', 'list_tenants') AND a.grantee = 0"
        ).fetchall()
    assert public == []


@pytest.mark.parametrize("change", ["enabled = false", "updated_at = '2020-01-01'"])
async def test_updating_a_tenant_always_moves_updated_at(pool, tenant: str, change: str):
    """Fails if an UPDATE of a tenant, even one that sets updated_at itself, leaves it in the past."""
    async with tenant_transaction(pool, tenant) as conn:
        await conn.execute(
            "INSERT INTO tenants (tenant_id, slack_team_id, default_agent, updated_at) "
            "VALUES (%(t)s, 'T-' || %(t)s, 'aa', '2020-01-01')",
            {"t": tenant},
        )
    async with tenant_transaction(pool, tenant) as conn:
        cur = await conn.execute(f"UPDATE tenants SET {change} RETURNING updated_at > '2021-01-01'")
        assert (await cur.fetchone())[0] is True


@pytest.mark.parametrize(
    ("kind", "agent_id", "accepted"),
    [
        ("answer", "aa", True),
        ("answer", None, False),
        ("agent_publish", None, False),
        *[
            (kind, None, True)
            for kind in (
                "member_add",
                "member_remove",
                "member_role",
                "budget_set",
                "switch_off",
                "switch_on",
            )
        ],
    ],
)
async def test_only_membership_budget_and_switch_events_may_name_no_agent(
    pool, tenant: str, kind: str, agent_id: str | None, accepted: bool
):
    """Fails if an agent event can be stored without its agent, or a tenant-wide event is refused."""
    insert = tenant_transaction(pool, tenant)
    statement = (
        "INSERT INTO audit_events (tenant_id, event_key, kind, agent_id, agent_version, "
        "requester_user_id, identity, outcome) VALUES (%s, 'e', %s, %s, %s, 'u', 'user:u', 'ok')"
    )
    args = (tenant, kind, agent_id, None if agent_id is None else 1)
    if accepted:
        async with insert as conn:
            await conn.execute(statement, args)
    else:
        with pytest.raises(errors.CheckViolation):
            async with insert as conn:
                await conn.execute(statement, args)
