"""Store behaviour, one contract per store, run against the in-memory twin and the real
Postgres store. A behaviour the two disagree on fails here; the SQL-only checks (CHECK
constraints, grants, policies) stay in the other modules of this folder."""

from __future__ import annotations

import dataclasses
import json
import secrets
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.parse import urlencode

import httpx
import pytest
from slack_sdk.signature import SignatureVerifier

from agentos import ids
from agentos.agent_health import (
    AVAILABLE,
    UNAVAILABLE,
    AgentHealth,
    InMemoryAgentHealthStore,
    PostgresAgentHealthStore,
)
from agentos.agent_store import InMemoryAgentStore, PostgresAgentStore
from agentos.approval_policy import (
    NO_APPROVAL,
    REQUESTER_APPROVAL,
    InMemoryApprovalPolicyStore,
    PostgresApprovalPolicyStore,
)
from agentos.audience_pauses import InMemoryPauseStore, PostgresPauseStore
from agentos.audit import InMemoryAuditStore, PostgresAuditStore
from agentos.budgets import BudgetNotFound, InMemoryBudgetStore, PostgresBudgetStore
from agentos.config import Settings, TenantsConfig, parse_agents_config
from agentos.creator_access import (
    CANNOT_VERIFY,
    LOST,
    VERIFIED,
    AccessResult,
    InMemoryCreatorAccessStore,
    PostgresCreatorAccessStore,
)
from agentos.feedback import (
    AnswerFeedback,
    InMemoryFeedbackStore,
    PostgresFeedbackStore,
    feedback_blocks,
)
from agentos.gateway.app import create_app
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
from agentos.slack_client import FakeSlack
from agentos.stores import InMemoryTaskLinkStore
from agentos.switches import InMemorySwitchStore, PostgresSwitchStore
from agentos.tasks import InMemoryTaskStore, PostgresTaskStore, TaskRecord
from agentos.test_sets import (
    DONE,
    PASS,
    RUNNING,
    CaseResult,
    InMemoryTestSetStore,
    PostgresTestSetStore,
    TestCase,
    TestRun,
)
from agentos.unit_of_work import PostgresUnitOfWork
from agentos.usage import InMemoryUsageStore, PostgresUsageStore, UsageEvent
from tests.conftest import TEAM, agents_dict
from tests.helpers.agents import helper_block
from tests.helpers.gateway import FakeTemporalClient, signed_headers

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


async def test_the_latest_event_of_a_kind_and_tool_is_the_newest_match(
    audit, tenant: str, other_tenant: str
):
    """Fails if the narrow newest-event read returns an older match, another kind or tool,
    or another tenant's event."""
    connect = {"kind": "connector", "tool": "connect", "agent_id": None, "agent_version": None}
    await audit.record(audit_event(tenant, "c1", requester_user_id="U1", **connect))
    await audit.record(
        audit_event(
            tenant,
            "d1",
            kind="connector",
            tool="disconnect",
            agent_id=None,
            agent_version=None,
        )
    )
    await audit.record(audit_event(tenant, "c2", requester_user_id="U2", **connect))
    await audit.record(audit_event(other_tenant, "c3", requester_user_id="U3", **connect))

    event = await audit.latest(tenant, "connector", "connect")

    assert (event.event_key, event.requester_user_id) == ("c2", "U2")
    assert await audit.latest(tenant, "connector", "no-such-tool") is None


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


async def test_spend_for_conversations_sums_the_named_conversations_only(
    usage, tenant: str, other_tenant: str
):
    """Fails if a run's spend read counts another conversation's or tenant's rows, or loses an unpriced one."""
    await usage.record(usage_event(tenant, "a1", NOW, "0.15", conversation_id="run:case-a"))
    await usage.record(usage_event(tenant, "a2", NOW, "0.10", conversation_id="run:case-a"))
    await usage.record(usage_event(tenant, "b1", NOW, None, conversation_id="run:case-b"))
    await usage.record(usage_event(tenant, "c1", NOW, "9", conversation_id="elsewhere"))
    await usage.record(usage_event(other_tenant, "d1", NOW, "9", conversation_id="run:case-a"))

    cost, unpriced = await usage.spend_for_conversations(tenant, ["run:case-a", "run:case-b"])

    assert (cost, unpriced) == (Decimal("0.25"), 1)
    assert await usage.spend_for_conversations(tenant, []) == (Decimal(0), 0)


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


# --- creator access -------------------------------------------------------------------


@pytest.fixture
def access(store_pool):
    return twin(store_pool, InMemoryCreatorAccessStore, PostgresCreatorAccessStore)


async def test_creator_access_records_verdicts_and_prunes_objects_no_longer_granted(
    access, tenant: str, other_tenant: str
):
    """Fails if a verdict does not read back, a second pass keeps a stale object's row or the
    old verdict, or another agent's or tenant's rows show."""
    await access.record(
        tenant,
        "aa",
        [AccessResult("n1", VERIFIED), AccessResult("n2", LOST)],
        NOW,
        {"n1", "n2"},
    )
    rows = await access.for_agent(tenant, "aa")
    assert {nid: (row.status, row.checked_at) for nid, row in rows.items()} == {
        "n1": (VERIFIED, NOW),
        "n2": (LOST, NOW),
    }

    later = NOW + timedelta(days=1)
    await access.record(tenant, "aa", [AccessResult("n1", CANNOT_VERIFY)], later, {"n1"})

    rows = await access.for_agent(tenant, "aa")
    assert {nid: (row.status, row.checked_at) for nid, row in rows.items()} == {
        "n1": (CANNOT_VERIFY, later)
    }
    assert await access.for_agent(tenant, "bb") == {}
    assert await access.for_agent(other_tenant, "aa") == {}


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


# --- agent health ---------------------------------------------------------------------


@pytest.fixture
def health(store_pool):
    return twin(store_pool, InMemoryAgentHealthStore, PostgresAgentHealthStore)


async def test_agent_health_reports_a_transition_once_and_reads_the_last_check_back(
    health, tenant: str, other_tenant: str
):
    """Fails if a repeated status or a first available check reports a transition, or the row read back is not the last check."""
    broken = AgentHealth(tenant, "aa", 1, UNAVAILABLE, "grant 'g': problem", NOW)
    assert await health.record(broken) is True
    assert await health.get(tenant, "aa") == broken

    later = dataclasses.replace(
        broken, reason="grant 'g': still broken", checked_at=NOW + timedelta(minutes=5)
    )
    assert await health.record(later) is False
    assert await health.get(tenant, "aa") == later

    recovered = dataclasses.replace(
        broken, status=AVAILABLE, reason=None, checked_at=NOW + timedelta(minutes=10)
    )
    assert await health.record(recovered) is True
    assert await health.list(tenant) == [recovered]

    assert await health.record(AgentHealth(tenant, "bb", 1, AVAILABLE, None, NOW)) is False
    assert (await health.get(tenant, "bb")).status == AVAILABLE
    assert await health.get(other_tenant, "aa") is None
    assert await health.list(other_tenant) == []


# --- tasks ---------------------------------------------------------------------------


@pytest.fixture
def tasks(store_pool):
    return twin(store_pool, InMemoryTaskStore, PostgresTaskStore)


def task_record(tenant: str, task_id: str, **fields) -> TaskRecord:
    base = {
        "tenant_id": tenant,
        "task_id": task_id,
        "requester_user_id": "u1",
        "agent_id": "aa",
        "title": "New monitor",
        "url": "https://www.notion.so/monitor",
        "page_id": "p1",
        "state": "watching",
        "last_status": "To do",
        "updated_at": NOW,
        "channel": "C1",
        "thread_ts": "1.1",
    }
    return TaskRecord(**(base | fields))


async def test_a_task_report_replaces_the_row_and_reads_back(tasks, tenant: str):
    """Fails if a second report for one task adds a row, or the row read back is not the latest report."""
    await tasks.record(task_record(tenant, "k1"))
    later = task_record(
        tenant, "k1", state="done", last_status="Done", updated_at=NOW + timedelta(hours=1)
    )

    await tasks.record(later)

    assert await tasks.get(tenant, "k1") == later
    assert await tasks.list_for_requester(tenant, "u1") == [later]
    assert await tasks.get(tenant, "missing") is None


async def test_a_requester_lists_only_their_own_tasks(tasks, tenant: str, other_tenant: str):
    """Fails if a listing shows a second user's task in the same tenant, or another tenant's task."""
    mine = task_record(tenant, "k1")
    theirs = task_record(tenant, "k2", requester_user_id="u2")
    elsewhere = task_record(other_tenant, "k3")
    for row in (mine, theirs, elsewhere):
        await tasks.record(row)

    assert await tasks.list_for_requester(tenant, "u1") == [mine]
    assert await tasks.list_for_requester(tenant, "u2") == [theirs]
    assert await tasks.list_for_requester(other_tenant, "u1") == [elsewhere]


async def test_tasks_list_open_first_then_newest_and_at_most_ten(tasks, tenant: str):
    """Fails if a closed task sorts before an open one, the newest sorts last, or more than ten are listed."""
    rows = [
        task_record(tenant, "open-old", state="watching", updated_at=NOW),
        task_record(tenant, "closed-newest", state="done", updated_at=NOW + timedelta(hours=6)),
        task_record(
            tenant, "open-new", state="awaiting_approval", updated_at=NOW + timedelta(hours=3)
        ),
        task_record(tenant, "closed-2", state="failed", updated_at=NOW + timedelta(hours=5)),
        task_record(tenant, "open-mid", state="filed", updated_at=NOW + timedelta(hours=1)),
        task_record(tenant, "closed-3", state="cancelled", updated_at=NOW + timedelta(hours=4)),
        task_record(
            tenant,
            "open-newest",
            state="watching",
            updated_at=NOW + timedelta(hours=5, minutes=30),
        ),
        task_record(
            tenant, "closed-4", state="done", updated_at=NOW + timedelta(hours=3, minutes=30)
        ),
        task_record(tenant, "closed-5", state="done", updated_at=NOW + timedelta(hours=2)),
        task_record(
            tenant, "closed-6", state="cancelled", updated_at=NOW + timedelta(hours=1, minutes=30)
        ),
        task_record(tenant, "closed-7", state="failed", updated_at=NOW + timedelta(hours=1)),
        task_record(tenant, "closed-8", state="done", updated_at=NOW + timedelta(minutes=30)),
    ]
    for row in rows:
        await tasks.record(row)

    listed = await tasks.list_for_requester(tenant, "u1")

    assert [r.task_id for r in listed] == [
        "open-newest",
        "open-new",
        "open-mid",
        "open-old",
        "closed-newest",
        "closed-2",
        "closed-3",
        "closed-4",
        "closed-5",
        "closed-6",
    ]


async def test_the_thread_lookup_finds_the_askers_newest_watching_task(tasks, tenant: str):
    """Fails if the thread lookup returns another user's task, a closed task, another thread's, or an older watching one."""
    older = task_record(tenant, "k1", updated_at=NOW)
    newer = task_record(tenant, "k2", updated_at=NOW + timedelta(hours=1))
    await tasks.record(older)
    await tasks.record(newer)
    await tasks.record(task_record(tenant, "k3", requester_user_id="u2"))
    await tasks.record(task_record(tenant, "k4", state="done"))
    await tasks.record(task_record(tenant, "k5", thread_ts="9.9"))

    assert await tasks.watching_in_thread(tenant, "C1", "1.1", "u1") == newer
    assert await tasks.watching_in_thread(tenant, "C1", "1.1", "u3") is None
    assert await tasks.watching_in_thread(tenant, "C9", "1.1", "u1") is None


# --- answer feedback ------------------------------------------------------------------


@pytest.fixture
def feedback(store_pool):
    return twin(store_pool, InMemoryFeedbackStore, PostgresFeedbackStore)


async def test_a_vote_is_one_per_person_and_answer_and_a_second_click_replaces_it(
    feedback, tenant: str, other_tenant: str
):
    """Fails if a second vote by the same person adds a row or keeps the first vote and reason,
    another person's vote is merged, a reason attaches to a 👍, or a tenant reads another's votes."""
    conv = f"{tenant}:conv:C1:1.1"
    await feedback.record_vote(AnswerFeedback(tenant, conv, "1.2", "U1", "aa", 1, "up", None, NOW))
    await feedback.record_vote(
        AnswerFeedback(tenant, conv, "1.2", "U1", "aa", 1, "down", None, NOW + timedelta(hours=1))
    )
    await feedback.record_vote(AnswerFeedback(tenant, conv, "1.2", "U2", "aa", 1, "up", None, NOW))
    assert await feedback.set_reason(tenant, conv, "1.2", "U1", "cited the wrong page") is True

    mine = await feedback.get(tenant, conv, "1.2", "U1")
    assert (mine.vote, mine.reason, mine.at) == (
        "down",
        "cited the wrong page",
        NOW + timedelta(hours=1),
    )
    assert (mine.agent_id, mine.agent_version) == ("aa", 1)
    assert (await feedback.get(tenant, conv, "1.2", "U2")).vote == "up"
    assert await feedback.set_reason(tenant, conv, "1.2", "U2", "not for an upvote") is False
    assert (await feedback.get(tenant, conv, "1.2", "U2")).reason is None

    # A changed vote replaces the reason too; a 👍 carries none.
    await feedback.record_vote(
        AnswerFeedback(tenant, conv, "1.2", "U1", "aa", 1, "up", None, NOW + timedelta(hours=2))
    )
    assert (await feedback.get(tenant, conv, "1.2", "U1")).reason is None
    assert await feedback.get(other_tenant, conv, "1.2", "U1") is None
    assert await feedback.set_reason(other_tenant, conv, "1.2", "U1", "x") is False


class RecordThenFail(PostgresAuditStore):
    """Writes the event on the unit's connection, then fails as a lost connection would."""

    async def record(self, event, conn=None):
        await super().record(event, conn=conn)
        raise RuntimeError("audit failed after the insert")


async def test_a_failed_audit_insert_leaves_no_vote(pool, tenant: str):
    """Fails if a vote commits although its audit insert failed, so the vote is left without
    an audit event."""
    sec = secrets.token_hex(16)
    raw = agents_dict()
    raw["tenant_id"] = tenant
    slack = FakeSlack()

    async def slack_for(t):
        return slack

    feedback = PostgresFeedbackStore(pool)
    app = create_app(
        Settings(),
        TenantsConfig.of(parse_agents_config(raw)),
        FakeTemporalClient(),
        InMemoryTaskLinkStore(),
        SignatureVerifier(sec),
        slack_for=slack_for,
        feedback=feedback,
        audit=RecordThenFail(pool),
        uow=PostgresUnitOfWork(pool),
        clock=lambda: NOW,
    )
    conv = ids.conversation_id(tenant, "C1", "1.1")
    blocks = feedback_blocks(conv, "1.2", "it-helper", 1)
    button = next(e for e in blocks[0]["elements"] if e["action_id"] == "answer_feedback_up")
    payload = {
        "type": "block_actions",
        "team": {"id": TEAM},
        "user": {"id": "U1"},
        "trigger_id": "trig-1",
        "channel": {"id": "C1"},
        "message": {"ts": "1.2", "thread_ts": "1.1"},
        "actions": [{"action_id": "answer_feedback_up", "value": button["value"]}],
    }
    body = urlencode({"payload": json.dumps(payload)}).encode()
    headers = signed_headers(sec, body, "application/x-www-form-urlencoded")

    # The ASGI transport runs the gateway in this test's loop, on the pool's own connections.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.post("/slack/interactivity", content=body, headers=headers)

    assert resp.status_code == 200  # the clicker is told to try again; nothing is half done
    assert await feedback.get(tenant, conv, "1.2", "U1") is None
    assert await PostgresAuditStore(pool).list(tenant) == []


# --- approval policy -------------------------------------------------------------------


@pytest.fixture
def policies(store_pool):
    return twin(store_pool, InMemoryApprovalPolicyStore, PostgresApprovalPolicyStore)


async def test_the_policy_reads_back_what_was_set_per_tenant_and_defaults_to_approval(
    policies, tenant: str, other_tenant: str
):
    """Fails if an action with no row reads as anything but requester approval, a set does not
    replace the stored policy, or one tenant's change reaches another."""
    assert await policies.get(tenant, "add_comment") == REQUESTER_APPROVAL
    assert await policies.get(tenant, "an-action-no-manifest-declares") == REQUESTER_APPROVAL

    await policies.set(tenant, "add_comment", NO_APPROVAL)

    assert await policies.get(tenant, "add_comment") == NO_APPROVAL
    assert await policies.get(other_tenant, "add_comment") == REQUESTER_APPROVAL

    await policies.set(tenant, "add_comment", REQUESTER_APPROVAL)

    assert await policies.get(tenant, "add_comment") == REQUESTER_APPROVAL


# --- test sets ---------------------------------------------------------------------


@pytest.fixture
def test_sets(store_pool):
    return twin(store_pool, InMemoryTestSetStore, PostgresTestSetStore)


def a_case(tenant: str, case_id: str, version: int = 1, **fields) -> TestCase:
    base = {
        "tenant_id": tenant,
        "agent_id": "aa",
        "version": version,
        "case_id": case_id,
        "question": "how do I reset my vpn?",
        "expect": "cite",
        "target": "n",
        "created_by": "u",
    }
    return TestCase(**(base | fields))


async def test_cases_are_kept_per_version_chosen_removed_and_copied_forward(
    test_sets, store_pool, tenant: str, other_tenant: str
):
    """Fails if a case is lost, an expectation or removal does not take, versions share rows, or a copy forward drops a case."""
    # The cases hang off agent versions (the real store's foreign key): make the parents.
    agents = twin(store_pool, InMemoryAgentStore, PostgresAgentStore)
    for t in (tenant, other_tenant):
        await agents.create_agent(t, "aa", helper_block(), "u")
    await agents.create_draft(tenant, "aa", "u", from_version=1)

    await test_sets.add_case(tenant, a_case(tenant, "k1"))
    await test_sets.add_case(tenant, a_case(tenant, "k2", expect="", target=""))
    await test_sets.add_case(other_tenant, a_case(other_tenant, "k1"))

    cases = await test_sets.cases(tenant, "aa", 1)
    assert [c.case_id for c in cases] == ["k1", "k2"]
    assert (cases[0].question, cases[0].expect, cases[0].target) == (
        "how do I reset my vpn?",
        "cite",
        "n",
    )
    assert cases[1].expect == ""

    assert await test_sets.set_expect(tenant, "aa", 1, "k2", "refuse", "") is True
    assert (await test_sets.cases(tenant, "aa", 1))[1].expect == "refuse"
    assert await test_sets.set_expect(tenant, "aa", 1, "gone", "refuse", "") is False

    assert await test_sets.copy_cases(tenant, "aa", 1, 2) == 2
    copied = await test_sets.cases(tenant, "aa", 2)
    assert [(c.case_id, c.question, c.expect, c.seq) for c in copied] == [
        (c.case_id, c.question, c.expect, c.seq) for c in await test_sets.cases(tenant, "aa", 1)
    ]

    assert await test_sets.remove_case(tenant, "aa", 2, "k1") is True
    assert await test_sets.remove_case(tenant, "aa", 2, "k1") is False
    assert [c.case_id for c in await test_sets.cases(tenant, "aa", 1)] == ["k1", "k2"]
    assert [c.case_id for c in await test_sets.cases(other_tenant, "aa", 1)] == ["k1"]


async def test_a_run_is_started_finished_and_the_newest_is_read_back(test_sets, tenant: str):
    """Fails if a run's row, results or spend are lost, or the latest run of a version is not the newest."""
    assert await test_sets.latest_run(tenant, "aa", 1) is None
    first = TestRun(tenant, "r1", "aa", 1, RUNNING, "u", "a" * 64)
    await test_sets.start_run(tenant, first)
    stored = await test_sets.get_run(tenant, "r1")
    assert (stored.status, stored.started_by, stored.definition_sha256) == (RUNNING, "u", "a" * 64)
    assert stored.started_at is not None

    results = [CaseResult("k1", PASS, tool="create_item", target="n")]
    await test_sets.finish_run(tenant, "r1", results, Decimal("0.12"))
    done = await test_sets.get_run(tenant, "r1")
    assert (done.status, done.spend_usd, done.results) == (DONE, Decimal("0.12"), results)
    assert done.finished_at is not None

    await test_sets.start_run(tenant, TestRun(tenant, "r2", "aa", 1, RUNNING, "u", "b" * 64))
    await test_sets.finish_run(tenant, "r2", [], Decimal(0))
    assert (await test_sets.latest_run(tenant, "aa", 1)).run_id == "r2"
    assert await test_sets.latest_run(tenant, "aa", 2) is None
    assert await test_sets.get_run(tenant, "gone") is None
