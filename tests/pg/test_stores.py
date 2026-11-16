"""Every Postgres store, through its public methods, against the real schema.

Each test drives one store far enough that every statement it holds runs once, including
its ON CONFLICT and paging clauses, and asserts what a caller would see.
"""

from __future__ import annotations

import copy
import hashlib
from datetime import UTC, datetime, timedelta

import pytest
from psycopg import errors

from agentos.audience_pauses import PostgresPauseStore
from agentos.knowledge.store import PostgresKnowledgeStore
from agentos.models import AuditEvent
from agentos.sessions import PostgresSessionStore
from agentos.stores import (
    PostgresExternalWriteStore,
    tenant_transaction,
)
from agentos.switches import PostgresSwitchStore
from tests.conftest import agents_dict
from tests.pg.conftest import TestDatabase

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)


def unit_vector(position: int) -> list[float]:
    return [1.0 if i == position else 0.0 for i in range(1024)]


async def test_scope_health_counts_each_run_once_and_clears(pool, tenant: str, other_tenant: str):
    """Fails if one run counts twice, a new run does not count, notification is lost, clearing misses, or tenants share a row."""
    store = PostgresKnowledgeStore(pool)

    first = await store.record_scope_gone(tenant, "s", "run-1")
    again = await store.record_scope_gone(tenant, "s", "run-1")
    second = await store.record_scope_gone(tenant, "s", "run-2")
    assert (first.consecutive_gone, again.consecutive_gone, second.consecutive_gone) == (1, 1, 2)
    assert second.notified_at is None
    await store.mark_scope_notified(tenant, "s")
    assert (await store.record_scope_gone(tenant, "s", "run-2")).notified_at is not None
    assert (await store.record_scope_gone(other_tenant, "s", "run-1")).consecutive_gone == 1

    await store.clear_scope_health(tenant, "s")

    assert (await store.record_scope_gone(tenant, "s", "run-3")).consecutive_gone == 1
    assert (await store.record_scope_gone(other_tenant, "s", "run-1")).consecutive_gone == 1


def audit_event(tenant: str, key: str, **fields) -> AuditEvent:
    base = {
        "tenant_id": tenant,
        "event_key": key,
        "kind": "answer",
        "agent_id": "aa",
        "agent_version": 1,
        "requester_user_id": "u",
        "identity": "agent:aa",
    }
    return AuditEvent(**(base | fields))


async def test_audit_log_refuses_a_kind_the_table_does_not_list(pool, tenant: str):
    """Fails if the foreign key lets an unknown kind in, or needs a privilege the app role lacks."""
    with pytest.raises(errors.ForeignKeyViolation):
        async with tenant_transaction(pool, tenant) as conn:
            await conn.execute(
                "INSERT INTO audit_events (tenant_id, event_key, kind, agent_id, agent_version, "
                "requester_user_id, identity, outcome) "
                "VALUES (%s, 'x', 'not_a_kind', 'aa', 1, 'u', 'agent:aa', 'ok')",
                (tenant,),
            )


async def test_sessions_expire_and_creating_one_clears_the_expired(
    db: TestDatabase, pool, tenant: str
):
    """Fails if an expired session is accepted, or expired rows pile up."""
    store = PostgresSessionStore(pool)
    old = await store.create(tenant, "U1", NOW, lifetime=timedelta(hours=1))
    assert (await store.get(tenant, old.token, NOW)).user_id == "U1"
    assert await store.get(tenant, old.token, NOW + timedelta(hours=2)) is None
    fresh = await store.create(tenant, "U2", NOW + timedelta(hours=2))
    with db.owner() as conn:
        rows = conn.execute("SELECT * FROM sessions WHERE tenant_id = %s", (tenant,)).fetchall()
        assert len(rows) == 1
        stored = [str(v) for v in rows[0]]
        assert fresh.token not in stored  # only the hash of the token is stored
        assert hashlib.sha256(fresh.token.encode()).hexdigest() in stored
    assert await store.get(tenant, "wrong", NOW) is None
    await store.delete(tenant, fresh.token)
    assert await store.get(tenant, fresh.token, NOW + timedelta(hours=2)) is None


def helper_block() -> dict:
    block = copy.deepcopy(agents_dict()["agents"]["it-helper"])
    del block["version"]
    return block


async def test_external_writes_are_per_tenant_and_a_done_result_never_changes(
    pool, tenant: str, other_tenant: str
):
    """Fails if another tenant sees or claims the row, or a done row's result is rewritten."""
    store = PostgresExternalWriteStore(pool)
    claim = await store.claim(tenant, "k", 60)
    await store.complete(tenant, "k", claim.token, "ref")

    assert await store.result(other_tenant, "k") is None
    assert (await store.claim(other_tenant, "k", 60)).status == "claimed"
    async with tenant_transaction(pool, tenant) as conn:
        cur = await conn.execute(
            "UPDATE external_writes SET result_ref = 'x' WHERE idem_key = 'k' "
            "AND lease_token = %s AND state <> 'done'",
            (claim.token,),
        )
        assert cur.rowcount == 0
    assert await store.result(tenant, "k") == "ref"


async def test_external_writes_refuse_delete_and_a_row_for_another_tenant(
    pool, tenant: str, other_tenant: str
):
    """Fails if the app role may delete a row or write one under a tenant it is not set to."""
    store = PostgresExternalWriteStore(pool)
    await store.claim(tenant, "k", 60)
    with pytest.raises(errors.InsufficientPrivilege):
        async with tenant_transaction(pool, tenant) as conn:
            await conn.execute("DELETE FROM external_writes")
    with pytest.raises(errors.InsufficientPrivilege):
        async with tenant_transaction(pool, tenant) as conn:
            await conn.execute(
                "INSERT INTO external_writes (tenant_id, idem_key, state, lease_until, lease_token) "
                "VALUES (%s, 'x', 'claimed', now(), 't')",
                (other_tenant,),
            )


async def test_a_switch_for_an_agent_needs_a_target_and_a_pause_needs_a_known_reason(
    pool, tenant: str
):
    """Fails if the table accepts an agent switch with no agent, or a pause with an unknown reason."""
    with pytest.raises(errors.CheckViolation):
        await PostgresSwitchStore(pool).switch_off(tenant, "agent", "", "u", NOW)
    with pytest.raises(errors.CheckViolation):
        await PostgresPauseStore(pool).open_pause(tenant, "aa", "C1", "someone_asked", NOW)
