"""Store behaviour, one contract per store, run against the in-memory twin and the real
Postgres store. A behaviour the two disagree on fails here; the SQL-only checks (CHECK
constraints, grants, policies) stay in the other modules of this folder."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from agentos.audience_pauses import InMemoryPauseStore, PostgresPauseStore
from agentos.audit import InMemoryAuditStore, PostgresAuditStore
from agentos.budgets import BudgetNotFound, InMemoryBudgetStore, PostgresBudgetStore
from agentos.grant_picks import InMemoryGrantPickStore, PickedObject, PostgresGrantPickStore
from agentos.members import (
    InMemoryMemberStore,
    MemberError,
    MemberNotFound,
    PostgresMemberStore,
)
from agentos.models import AuditEvent
from agentos.notion_webhook import InMemoryWebhookTokenStore, PostgresWebhookTokenStore
from agentos.sessions import InMemorySessionStore, PostgresSessionStore
from agentos.switches import InMemorySwitchStore, PostgresSwitchStore
from agentos.usage import InMemoryUsageStore, PostgresUsageStore, UsageEvent

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)


def twin(store_pool, fake, real):
    return fake() if store_pool is None else real(store_pool)


# --- audit log ---------------------------------------------------------------------


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


@pytest.fixture
def audit(store_pool):
    return twin(store_pool, InMemoryAuditStore, PostgresAuditStore)


async def test_audit_log_records_once_and_pages_newest_first(audit, tenant: str):
    """Fails if a repeated event key stores a second row, or paging skips or repeats an event."""
    for n in range(5):
        await audit.record(audit_event(tenant, f"e{n}"))
    await audit.record(dataclasses.replace(audit_event(tenant, "e0"), outcome="error"))
    await audit.record(
        audit_event(tenant, "m", kind="member_add", agent_id=None, agent_version=None)
    )

    page1 = await audit.list(tenant, limit=4)
    page2 = await audit.list(tenant, limit=4, before_seq=page1[-1].seq)
    assert [e.event_key for e in page1 + page2] == ["m", "e4", "e3", "e2", "e1", "e0"]
    assert [e.outcome for e in page1 + page2].count("error") == 0
    assert page1[0].at is not None
    assert await audit.list(tenant, before=datetime(2000, 1, 1, tzinfo=UTC)) == []
    assert len(await audit.list(tenant, before=datetime.now(UTC) + timedelta(hours=1))) == 6


async def test_the_same_event_key_in_two_tenants_is_two_events(
    audit, tenant: str, other_tenant: str
):
    """Fails if an event key shared by two tenants is stored once, or a tenant lists another's events."""
    await audit.record(audit_event(tenant, "k1"))
    await audit.record(audit_event(other_tenant, "k1"))

    assert [e.tenant_id for e in await audit.list(tenant)] == [tenant]
    assert [e.tenant_id for e in await audit.list(other_tenant)] == [other_tenant]


async def test_an_audit_list_refuses_a_limit_below_one(audit, tenant: str):
    """Fails if a list with a zero limit is answered."""
    with pytest.raises(ValueError):
        await audit.list(tenant, limit=0)


async def test_a_resume_is_recorded_for_an_agent_with_no_published_version(audit, tenant: str):
    """Fails if the store refuses an audience_resume whose agent has no published version, or reads it back with one."""
    await audit.record(audit_event(tenant, "r1", kind="audience_resume", agent_version=None))

    [event] = await audit.list(tenant)

    assert (event.kind, event.agent_id, event.agent_version) == ("audience_resume", "aa", None)


# --- usage -------------------------------------------------------------------------


@pytest.fixture
def usage(store_pool):
    return twin(store_pool, InMemoryUsageStore, PostgresUsageStore)


def usage_event(tenant: str, key: str, at: datetime, cost: str | None, **fields) -> UsageEvent:
    base = {
        "tenant_id": tenant,
        "usage_key": key,
        "kind": "llm",
        "agent_id": "aa",
        "agent_version": 1,
        "model": "m",
        "input_tokens": 10,
        "output_tokens": 5,
        "cost": None if cost is None else Decimal(cost),
        "at": at,
    }
    return UsageEvent(**(base | fields))


async def test_usage_totals_group_a_month_and_count_unpriced_calls(usage, tenant: str):
    """Fails if a repeated usage key counts twice, the window is wrong, or an unpriced call is lost."""
    await usage.record(usage_event(tenant, "k1", NOW, "0.50000000"))
    await usage.record(usage_event(tenant, "k1", NOW, "9.00000000"))
    await usage.record(usage_event(tenant, "k2", NOW + timedelta(days=1), None))
    await usage.record(usage_event(tenant, "k3", NOW + timedelta(days=40), "1.00000000"))
    await usage.record(
        usage_event(
            tenant, "k4", NOW, "0.25000000", kind="embedding", agent_id=None, agent_version=None
        )
    )

    groups = await usage.totals(tenant, NOW - timedelta(days=1), NOW + timedelta(days=30))
    by_kind = {g.kind: g for g in groups}
    llm = by_kind["llm"]
    assert (llm.calls, llm.input_tokens, llm.output_tokens) == (2, 20, 10)
    assert (llm.cost, llm.unpriced_calls) == (Decimal("0.5"), 1)
    assert by_kind["embedding"].cost == Decimal("0.25")
    assert await usage.totals(tenant, NOW + timedelta(days=60), NOW + timedelta(days=90)) == []


async def test_one_tenants_usage_is_not_in_another_tenants_totals(
    usage, tenant: str, other_tenant: str
):
    """Fails if a tenant's totals include another tenant's usage."""
    await usage.record(usage_event(tenant, "1", NOW, "1"))
    await usage.record(usage_event(other_tenant, "2", NOW, "2"))

    [group] = await usage.totals(tenant, NOW - timedelta(days=1), NOW + timedelta(days=1))

    assert group.cost == Decimal(1)


# --- budgets -----------------------------------------------------------------------


@pytest.fixture
def budgets(store_pool):
    return twin(store_pool, InMemoryBudgetStore, PostgresBudgetStore)


async def test_budgets_upsert_one_row_per_scope_and_remove_by_scope(budgets, tenant: str):
    """Fails if setting a budget twice makes two rows, or removing a missing budget passes silently."""
    first = await budgets.set_budget(tenant, None, Decimal("10.00"), "u1")
    again = await budgets.set_budget(tenant, None, Decimal("20.00"), "u2")
    await budgets.set_budget(tenant, "aa", Decimal("5.00"), "u1")
    rows = {b.agent_id: b for b in await budgets.list(tenant)}
    assert set(rows) == {None, "aa"}
    assert (rows[None].amount, rows[None].updated_by) == (Decimal("20.00"), "u2")
    assert first.updated_at <= again.updated_at
    await budgets.remove_budget(tenant, "aa")
    with pytest.raises(BudgetNotFound):
        await budgets.remove_budget(tenant, "aa")
    await budgets.remove_budget(tenant, None)
    assert await budgets.list(tenant) == []


# --- switches ----------------------------------------------------------------------


@pytest.fixture
def switches(store_pool):
    return twin(store_pool, InMemorySwitchStore, PostgresSwitchStore)


async def test_switch_off_and_on_report_whether_anything_changed(switches, tenant: str):
    """Fails if turning a switch off twice reports a change, or the list is not ordered."""
    assert await switches.switch_off(tenant, "writes", "", "u", NOW) is True
    assert await switches.switch_off(tenant, "writes", "", "u2", NOW) is False
    assert await switches.switch_off(tenant, "agent", "aa", "u", NOW) is True
    assert [(s.scope, s.target, s.turned_off_by) for s in await switches.list(tenant)] == [
        ("agent", "aa", "u"),
        ("writes", "", "u"),
    ]
    assert await switches.switch_on(tenant, "writes", "") is True
    assert await switches.switch_on(tenant, "writes", "") is False


# --- audience pauses ---------------------------------------------------------------


@pytest.fixture
def pauses(store_pool):
    return twin(store_pool, InMemoryPauseStore, PostgresPauseStore)


async def test_pause_opens_once_and_a_resumed_channel_can_pause_again(pauses, tenant: str):
    """Fails if a second open creates a second active pause, or a resumed one blocks the next."""
    first = await pauses.open_pause(tenant, "aa", "C1", "channel_shared", NOW)
    again = await pauses.open_pause(tenant, "aa", "C1", "channel_shared", NOW + timedelta(hours=1))
    assert again.paused_at == first.paused_at
    await pauses.open_pause(tenant, "bb", "C1", "channel_shared", NOW)
    assert (await pauses.get_active(tenant, "aa", "C1")).paused_at == first.paused_at
    assert [p.agent_id for p in await pauses.list_active(tenant)] == ["aa", "bb"]
    assert [p.agent_id for p in await pauses.list_active(tenant, "bb")] == ["bb"]
    closed = await pauses.close_pause(tenant, "aa", "C1", "admin", NOW + timedelta(hours=2))
    assert (closed.resumed_by, closed.resumed_at) == ("admin", NOW + timedelta(hours=2))
    assert await pauses.close_pause(tenant, "aa", "C1", "admin", NOW) is None
    assert await pauses.get_active(tenant, "aa", "C1") is None
    renewed = await pauses.open_pause(tenant, "aa", "C1", "channel_shared", NOW + timedelta(days=1))
    assert renewed.paused_at == NOW + timedelta(days=1)


# --- members -----------------------------------------------------------------------


@pytest.fixture
def members(store_pool):
    return twin(store_pool, InMemoryMemberStore, PostgresMemberStore)


async def test_members_are_added_once_and_changed_only_when_they_exist(members, tenant: str):
    """Fails if a member can be added twice, or a change to a non-member passes silently."""
    await members.add_member(tenant, "U2", "creator", "U1")
    await members.add_member(tenant, "U1", "admin", "U1")
    with pytest.raises(MemberError, match="already a member"):
        await members.add_member(tenant, "U1", "creator", "U2")
    await members.set_member_role(tenant, "U2", "admin")
    assert [(m.user_id, m.role) for m in await members.list_members(tenant)] == [
        ("U1", "admin"),
        ("U2", "admin"),
    ]
    assert (await members.get_member(tenant, "U2")).added_by == "U1"
    await members.remove_member(tenant, "U2")
    assert await members.get_member(tenant, "U2") is None
    for change in (
        members.remove_member(tenant, "U2"),
        members.set_member_role(tenant, "U2", "admin"),
    ):
        with pytest.raises(MemberNotFound):
            await change


# --- sessions ----------------------------------------------------------------------


@pytest.fixture
def sessions(store_pool):
    return twin(store_pool, InMemorySessionStore, PostgresSessionStore)


async def test_sessions_expire_and_a_wrong_token_or_tenant_finds_nothing(
    sessions, tenant: str, other_tenant: str
):
    """Fails if an expired session is accepted, or a token works for another tenant or after deletion."""
    old = await sessions.create(tenant, "U1", NOW, lifetime=timedelta(hours=1))
    assert (await sessions.get(tenant, old.token, NOW)).user_id == "U1"
    assert await sessions.get(tenant, old.token, NOW + timedelta(hours=2)) is None
    assert await sessions.get(other_tenant, old.token, NOW) is None
    assert await sessions.get(tenant, "wrong", NOW) is None
    fresh = await sessions.create(tenant, "U2", NOW + timedelta(hours=2))
    await sessions.delete(tenant, fresh.token)
    assert await sessions.get(tenant, fresh.token, NOW + timedelta(hours=2)) is None


# --- grant picks -------------------------------------------------------------------


@pytest.fixture
def picks(store_pool):
    return twin(store_pool, InMemoryGrantPickStore, PostgresGrantPickStore)


async def test_a_pick_is_kept_per_agent_by_normalised_id_and_choosing_again_refreshes_it(
    picks, tenant: str, other_tenant: str
):
    """Fails if a repeated pick adds a second row, keeps the first chooser, or shows another agent's or tenant's pick."""
    dashed = "11111111-2222-3333-4444-555555555555"
    plain = dashed.replace("-", "")
    await picks.record(tenant, "aa", [PickedObject(dashed, "database")], "U1", NOW)
    await picks.record(
        tenant, "aa", [PickedObject(plain, "database")], "U2", NOW + timedelta(days=1)
    )

    mine = await picks.for_agent(tenant, "aa")

    assert list(mine) == [plain]
    assert (mine[plain].kind, mine[plain].picked_by) == ("database", "U2")
    assert mine[plain].picked_at == NOW + timedelta(days=1)
    assert await picks.for_agent(tenant, "bb") == {}
    assert await picks.for_agent(other_tenant, "aa") == {}


# --- notion webhook tokens -----------------------------------------------------------


@pytest.fixture
def webhook_tokens(store_pool):
    return twin(store_pool, InMemoryWebhookTokenStore, PostgresWebhookTokenStore)


async def test_a_webhook_token_is_pending_once_then_active_then_gone(webhook_tokens, tenant: str):
    """Fails if a second handshake replaces the stored token, an active token confirms again, or clear leaves the row behind."""
    assert await webhook_tokens.get(tenant) is None
    assert await webhook_tokens.put_pending(tenant, b"ct1", 1) is True
    assert await webhook_tokens.put_pending(tenant, b"ct2", 1) is False
    row = await webhook_tokens.get(tenant)
    assert (row.state, row.ciphertext, row.key_version) == ("pending", b"ct1", 1)
    assert await webhook_tokens.confirm(tenant, NOW) is True
    assert await webhook_tokens.confirm(tenant, NOW) is False
    row = await webhook_tokens.get(tenant)
    assert (row.state, row.confirmed_at) == ("active", NOW)
    assert await webhook_tokens.clear(tenant) is True
    assert await webhook_tokens.clear(tenant) is False
    assert await webhook_tokens.get(tenant) is None
    assert await webhook_tokens.put_pending(tenant, b"ct3", 2) is True
