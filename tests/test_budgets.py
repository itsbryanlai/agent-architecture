"""Monthly budgets: the stop before a model call, the warning, who may set one, and the SQL.

Fakes only: scripted model, in-memory stores, a fake Slack sign-in, a clock the test sets.
"""

import asyncio
import logging
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from temporalio.testing import ActivityEnvironment

from agentos import usage as usage_module
from agentos.activities import AgentActivities
from agentos.agent_store import AgentNotFound, seeded_agent_store
from agentos.audit import InMemoryAuditStore
from agentos.budgets import (
    STOP_REASON,
    BudgetGuard,
    BudgetNotFound,
    InMemoryBudgetStore,
    InvalidBudget,
    stop_text,
)
from agentos.config import Settings, parse_agents_config
from agentos.connectors.notion import FakeNotionAPI
from agentos.control import Forbidden
from agentos.knowledge.embedder import FakeEmbedder
from agentos.knowledge.indexing import IndexActivities
from agentos.knowledge.store import InMemoryKnowledgeStore
from agentos.llm import ScriptedLLMClient
from agentos.models import AUDIT_KINDS, AuditEvent, LLMResponse, TurnContext, Usage
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for
from agentos.usage import InMemoryUsageStore, Prices, UsageEvent, UsageRecorder
from tests.conftest import ALICE, ROOT_A, TENANT, a_tree, make_fake_notion
from tests.helpers.console_harness import (
    OTHER,
    OWNER,
    ROOT,
    ConsoleHarness,
    csrf_token,
    post,
    using,
)
from tests.helpers.usage import SCOPE, _page_agents, block, price

REPO = Path(__file__).resolve().parent.parent
D = Decimal
NOV = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)
UI = f"/ui/{TENANT}"
JSON = {"Content-Type": "application/json"}


class Clock:
    def __init__(self, now=NOV):
        self.now = now

    def __call__(self):
        return self.now


def usage_row(key, agent="it-helper", cost="1", kind="llm", at=NOV, tenant=TENANT):
    return UsageEvent(
        tenant_id=tenant,
        usage_key=key,
        kind=kind,
        agent_id=None if kind == "embedding" else agent,
        agent_version=None if kind == "embedding" else 1,
        model="m",
        cost=None if cost is None else D(cost),
        at=at,
    )


def make_guard(ttl=60, clock=None):
    budgets, usage = InMemoryBudgetStore(), InMemoryUsageStore()
    guard = BudgetGuard(budgets, usage, clock or Clock(), ttl_seconds=ttl)
    return guard, budgets, usage


# --- the stop, by budget ----------------------------------------------------------------


async def test_no_budget_means_no_limit():
    guard, _, usage = make_guard()
    await usage.record(usage_row("1", cost="1000000"))

    assert await guard.stopped_by(TENANT, "it-helper") is None


@pytest.mark.parametrize(
    ("spent", "stopped"),
    [("49.99", False), ("50", True), ("50.01", True)],
    ids=["under", "at", "over"],
)
async def test_an_agent_budget_stops_the_agent_at_its_amount(spent, stopped):
    guard, budgets, usage = make_guard()
    await budgets.set_budget(TENANT, "it-helper", D("50"), ROOT)
    await usage.record(usage_row("1", cost=spent))

    assert await guard.stopped_by(TENANT, "it-helper") == ("agent" if stopped else None)


@pytest.mark.parametrize(
    ("spent", "stopped"),
    [("99.99", False), ("100", True), ("100.01", True)],
    ids=["under", "at", "over"],
)
async def test_a_tenant_budget_stops_every_agent_at_its_amount(spent, stopped):
    guard, budgets, usage = make_guard()
    await budgets.set_budget(TENANT, None, D("100"), ROOT)
    await usage.record(usage_row("1", agent="someone-else", cost=spent))

    assert await guard.stopped_by(TENANT, "it-helper") == ("tenant" if stopped else None)


async def test_the_tenant_budget_counts_embeddings_but_an_agent_budget_does_not():
    guard, budgets, usage = make_guard()
    await budgets.set_budget(TENANT, None, D("10"), ROOT)
    await budgets.set_budget(TENANT, "it-helper", D("5"), ROOT)
    await usage.record(usage_row("e", kind="embedding", cost="10"))

    assert await guard.stopped_by(TENANT, "it-helper") == "tenant"
    await budgets.remove_budget(TENANT, None)
    guard.forget()
    assert await guard.stopped_by(TENANT, "it-helper") is None


async def test_an_agent_budget_counts_only_that_agents_model_calls_over_all_its_versions():
    guard, budgets, usage = make_guard()
    await budgets.set_budget(TENANT, "it-helper", D("3"), ROOT)
    await usage.record(usage_row("1", agent="other", cost="100"))
    await usage.record(usage_row("2", cost="1"))
    second = usage_row("3", cost="2")
    second.agent_version = 2
    await usage.record(second)

    assert await guard.stopped_by(TENANT, "it-helper") == "agent"
    assert await guard.stopped_by(TENANT, "other") is None


async def test_only_this_utc_month_counts_and_it_starts_over_on_the_first():
    clock = Clock()
    guard, budgets, usage = make_guard(clock=clock)
    await budgets.set_budget(TENANT, "it-helper", D("5"), ROOT)
    await usage.record(usage_row("old", cost="9", at=datetime(2026, 10, 31, 23, 59, tzinfo=UTC)))
    await usage.record(usage_row("new", cost="5", at=datetime(2026, 11, 1, 0, 0, tzinfo=UTC)))

    assert await guard.stopped_by(TENANT, "it-helper") == "agent"
    clock.now = datetime(2026, 12, 1, 0, 0, tzinfo=UTC)
    assert await guard.stopped_by(TENANT, "it-helper") is None


@pytest.mark.parametrize("scope", ["it-helper", None], ids=["agent budget", "tenant budget"])
async def test_a_budgeted_scope_with_an_unpriced_call_is_stopped_not_treated_as_free(scope):
    """Fails if a call with no price lets a scope with a budget keep spending unseen."""
    guard, budgets, usage = make_guard()
    await budgets.set_budget(TENANT, scope, D("1000"), ROOT)
    await usage.record(usage_row("1", cost=None))

    assert await guard.stopped_by(TENANT, "it-helper") == "unpriced"
    assert await guard.tenant_stopped(TENANT) is (scope is None)


async def test_the_unpriced_stop_has_its_own_reply_and_reason():
    """Fails if an unknown-spend stop says the budget is reached, or carries the wrong reason."""
    guard, budgets, usage = make_guard()
    await budgets.set_budget(TENANT, "it-helper", D("1000"), ROOT)
    await usage.record(usage_row("1", cost=None))

    resp = await guard.stop_response(TENANT, "it-helper", "IT Helper")

    assert resp.text == (
        "IT Helper's spending can't be measured this month, so it is paused. "
        "An admin can check prices in the console."
    )
    assert resp.stop_reason == "spend_unknown" and resp.tool_calls == []


async def test_raising_the_budget_does_not_clear_an_unpriced_stop_but_a_price_does():
    """Fails if a bigger budget lifts the unknown-spend stop, or an adjustment does not."""
    from agentos.usage import Adjustment

    clock = Clock()
    guard, budgets, usage = make_guard(clock=clock)
    await budgets.set_budget(TENANT, "it-helper", D("5"), ROOT)
    await usage.record(usage_row("1", cost=None))
    assert await guard.stopped_by(TENANT, "it-helper") == "unpriced"

    await budgets.set_budget(TENANT, "it-helper", D("5000"), ROOT)
    clock.now += timedelta(seconds=61)
    assert await guard.stopped_by(TENANT, "it-helper") == "unpriced"

    await usage.adjust(TENANT, [Adjustment("1", D("1"), "m", NOV.date())])
    clock.now += timedelta(seconds=61)
    assert await guard.stopped_by(TENANT, "it-helper") is None


async def test_no_budget_means_an_unpriced_call_stops_nothing():
    """Fails if a scope with no budget is stopped by unpriced rows."""
    guard, _, usage = make_guard()
    await usage.record(usage_row("1", cost=None))

    assert await guard.stopped_by(TENANT, "it-helper") is None


async def test_indexing_stops_for_the_tenant_scope_when_spend_is_unknown():
    """Fails if an unpriced call under a tenant budget still lets pages be embedded."""
    guard, budgets, usage = make_guard()
    await budgets.set_budget(TENANT, None, D("1000"), ROOT)
    await usage.record(usage_row("e", kind="embedding", cost=None))

    assert await guard.tenant_stopped(TENANT) is True


async def test_another_tenants_spend_and_budget_are_not_seen():
    guard, budgets, usage = make_guard()
    await budgets.set_budget("globex", None, D("1"), ROOT)
    await usage.record(usage_row("1", cost="50", tenant="globex"))

    assert await guard.stopped_by(TENANT, "it-helper") is None
    assert await guard.stopped_by("globex", "it-helper") == "tenant"


async def test_the_agent_budget_is_named_before_the_tenant_budget_when_both_are_reached():
    guard, budgets, usage = make_guard()
    await budgets.set_budget(TENANT, None, D("1"), ROOT)
    await budgets.set_budget(TENANT, "it-helper", D("1"), ROOT)
    await usage.record(usage_row("1", cost="2"))

    assert await guard.stopped_by(TENANT, "it-helper") == "agent"


# --- the cache -------------------------------------------------------------------------


async def test_spend_is_read_at_most_once_in_the_cache_window_so_overspend_is_bounded_by_it():
    clock = Clock()
    guard, budgets, usage = make_guard(ttl=60, clock=clock)
    await budgets.set_budget(TENANT, "it-helper", D("5"), ROOT)
    await usage.record(usage_row("1", cost="1"))
    assert await guard.stopped_by(TENANT, "it-helper") is None

    await usage.record(usage_row("2", cost="500"))
    clock.now += timedelta(seconds=59)
    assert await guard.stopped_by(TENANT, "it-helper") is None

    clock.now += timedelta(seconds=1)
    assert await guard.stopped_by(TENANT, "it-helper") == "agent"


async def test_a_stop_is_lifted_when_the_cache_expires_after_an_admin_raises_the_budget():
    clock = Clock()
    guard, budgets, usage = make_guard(ttl=60, clock=clock)
    await budgets.set_budget(TENANT, "it-helper", D("5"), ROOT)
    await usage.record(usage_row("1", cost="5"))
    assert await guard.stopped_by(TENANT, "it-helper") == "agent"

    await budgets.set_budget(TENANT, "it-helper", D("50"), ROOT)
    clock.now += timedelta(seconds=60)

    assert await guard.stopped_by(TENANT, "it-helper") is None


async def test_the_default_cache_window_is_at_most_a_minute():
    assert BudgetGuard(InMemoryBudgetStore(), InMemoryUsageStore()).ttl_seconds <= 60


# --- call_llm -------------------------------------------------------------------------


def turn_ctx() -> TurnContext:
    return TurnContext(
        tenant_id=TENANT,
        agent_id="it-helper",
        agent_version=1,
        requester_user_id=ALICE,
        channel="C1",
        thread_ts="1.1",
        channel_kind="channel",
    )


def activities(agents, llm, guard=None, usage_store=None):
    recorder = None
    if usage_store is not None:
        recorder = UsageRecorder(usage_store, Prices([price("m")]), Clock())
    return AgentActivities(
        settings=Settings(model="configured-model"),
        registry=TenantRegistry(
            {agents.tenant_id: runtime_for(agents, FakeSlack(), FakeNotionAPI())}
        ),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=llm,
        tool_outputs=InMemoryToolOutputStore(),
        audit=InMemoryAuditStore(),
        agents=seeded_agent_store(agents),
        usage=recorder,
        budgets=guard,
    )


def answer() -> LLMResponse:
    return LLMResponse(
        text="done", tool_calls=[], stop_reason="end_turn", usage=Usage(1000, 500, 0, 0, "m")
    )


async def over_budget_guard(scope):
    guard, budgets, usage = make_guard()
    await budgets.set_budget(TENANT, scope, D("2"), ROOT)
    await usage.record(usage_row("spent", cost="2"))
    return guard, usage


@pytest.mark.parametrize("scope", ["it-helper", None], ids=["agent budget", "tenant budget"])
async def test_a_reached_budget_answers_with_the_fixed_text_and_never_calls_the_model(
    agents, scope
):
    guard, _ = await over_budget_guard(scope)
    llm = ScriptedLLMClient([answer()])
    written = InMemoryUsageStore()
    acts = activities(agents, llm, guard, written)

    resp = await ActivityEnvironment().run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")

    assert resp.text == (
        "IT helper has reached its monthly budget. An admin can raise it in the console."
    )
    assert resp.tool_calls == [] and resp.stop_reason == STOP_REASON == "budget_reached"
    assert resp.usage == Usage()
    assert llm.calls == []
    assert written.events == []


def test_the_fixed_text_names_the_agent():
    assert stop_text("Docs bot") == (
        "Docs bot has reached its monthly budget. An admin can raise it in the console."
    )


async def test_a_stopped_turn_logs_one_line_with_ids_and_no_content(agents, caplog):
    guard, _ = await over_budget_guard(None)
    acts = activities(agents, ScriptedLLMClient([answer()]), guard)

    with caplog.at_level(logging.INFO, logger="agentos.budgets"):
        await ActivityEnvironment().run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")

    [line] = [r.getMessage() for r in caplog.records if r.name == "agentos.budgets"]
    assert (
        f"tenant_id={TENANT}" in line and "agent_id=it-helper" in line and "budget=tenant" in line
    )
    assert ALICE not in line and "reached its monthly budget" not in line


async def test_under_budget_the_model_is_called_and_the_call_is_recorded(agents):
    guard, budgets, _ = make_guard()
    await budgets.set_budget(TENANT, "it-helper", D("2"), ROOT)
    llm = ScriptedLLMClient([answer()])
    written = InMemoryUsageStore()
    acts = activities(agents, llm, guard, written)

    resp = await ActivityEnvironment().run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")

    assert resp.text == "done" and len(llm.calls) == 1 and len(written.events) == 1


async def test_without_a_guard_the_activity_behaves_as_before(agents):
    llm = ScriptedLLMClient([answer()])

    resp = await ActivityEnvironment().run(
        activities(agents, llm).call_llm, turn_ctx(), "acme:conv:C1:1.1"
    )

    assert resp.text == "done"


async def test_a_budget_check_that_cannot_read_fails_the_activity_so_temporal_retries(agents):
    class Broken(InMemoryBudgetStore):
        async def list(self, tenant_id):
            raise ConnectionError("down")

    guard = BudgetGuard(Broken(), InMemoryUsageStore(), Clock())
    llm = ScriptedLLMClient([answer()])

    with pytest.raises(ConnectionError):
        await ActivityEnvironment().run(
            activities(agents, llm, guard).call_llm, turn_ctx(), "acme:conv:C1:1.1"
        )
    assert llm.calls == []


# --- the control plane --------------------------------------------------------------------


@pytest.fixture
async def h():
    harness = ConsoleHarness()
    await harness.seed_members()
    harness.usage.events.clear()
    await harness.plane.add_agent(TENANT, OWNER, "mine", block("IT"))
    await harness.plane.add_agent(TENANT, OTHER, "theirs", block("HR"))
    harness.audit.events.clear()
    return harness


def kinds(h):
    return [e.kind for e in h.audit.events]


async def test_an_admin_sets_raises_and_removes_a_budget_and_each_change_is_audited(h):
    plane = h.plane

    await plane.set_budget(TENANT, ROOT, "mine", D("50"))
    await plane.set_budget(TENANT, ROOT, "mine", "75.5")
    await plane.set_budget(TENANT, ROOT, None, D("1000"))
    await plane.remove_budget(TENANT, ROOT, "mine")

    assert kinds(h) == ["budget_set"] * 4
    first, second, tenant_wide, removed = h.audit.events
    assert (first.agent_id, first.agent_version, first.target) == ("mine", None, "50.00")
    assert second.target == "75.50"
    assert (tenant_wide.agent_id, tenant_wide.target) == (None, "1000.00")
    assert (removed.agent_id, removed.target, removed.tool) == ("mine", "0", "remove")
    assert {e.requester_user_id for e in h.audit.events} == {ROOT}
    assert {e.identity for e in h.audit.events} == {f"user:{ROOT}"}
    [left] = await h.budgets.list(TENANT)
    assert (left.agent_id, left.amount, left.updated_by) == (None, D("1000.00"), ROOT)


async def test_raising_a_budget_replaces_the_row_rather_than_adding_one(h):
    await h.plane.set_budget(TENANT, ROOT, "mine", D("50"))
    await h.plane.set_budget(TENANT, ROOT, "mine", D("60"))

    [only] = await h.budgets.list(TENANT)
    assert only.amount == D("60.00")


async def test_a_creator_and_a_stranger_may_not_set_or_remove_a_budget(h):
    await h.plane.set_budget(TENANT, ROOT, "mine", D("50"))
    h.audit.events.clear()

    for who in (OWNER, OTHER, "U_NOBODY"):
        with pytest.raises(Forbidden):
            await h.plane.set_budget(TENANT, who, "mine", D("999"))
        with pytest.raises(Forbidden):
            await h.plane.remove_budget(TENANT, who, "mine")
        with pytest.raises(Forbidden):
            await h.plane.set_budget(TENANT, who, None, D("999"))
    [kept] = await h.budgets.list(TENANT)
    assert kept.amount == D("50.00") and h.audit.events == []


@pytest.mark.parametrize(
    "bad",
    ["0", "-5", "1.005", "abc", "", "NaN", "Infinity", "1e3", "10000000000", 5.5, True, None],
)
async def test_an_amount_that_is_not_a_positive_sum_of_dollars_and_cents_is_refused(h, bad):
    with pytest.raises(InvalidBudget):
        await h.plane.set_budget(TENANT, ROOT, "mine", bad)
    assert await h.budgets.list(TENANT) == [] and h.audit.events == []


async def test_a_budget_for_an_unknown_agent_is_refused(h):
    with pytest.raises(AgentNotFound):
        await h.plane.set_budget(TENANT, ROOT, "ghost", D("5"))


async def test_removing_a_budget_that_is_not_set_is_refused_and_not_audited(h):
    with pytest.raises(BudgetNotFound):
        await h.plane.remove_budget(TENANT, ROOT, "mine")
    assert h.audit.events == []


async def seed_spend(h, agent, cost, key=None, kind="llm"):
    await h.usage.record(usage_row(key or f"{agent}-{cost}", agent, cost, kind))


async def test_the_budgets_view_gives_spend_percent_and_level_per_agent(h):
    await h.plane.add_agent(TENANT, OWNER, "third", block())
    await h.plane.set_budget(TENANT, ROOT, "mine", D("100"))
    await h.plane.set_budget(TENANT, ROOT, "theirs", D("100"))
    await h.plane.set_budget(TENANT, ROOT, "third", D("100"))
    await seed_spend(h, "mine", "79.99")
    await seed_spend(h, "theirs", "80")
    await seed_spend(h, "third", "100")

    view = await h.plane.budgets(TENANT, ROOT)

    lines = {line.agent_id: line for line in view.agents}
    assert (lines["mine"].percent, lines["mine"].level) == (79, "ok")
    assert (lines["theirs"].percent, lines["theirs"].level) == (80, "warning")
    assert (lines["third"].percent, lines["third"].level) == (100, "stopped")
    assert lines["third"].spend == D("100") and lines["third"].amount == D("100.00")


async def test_an_agent_without_a_budget_is_listed_with_its_spend_and_no_level(h):
    await seed_spend(h, "mine", "4")

    view = await h.plane.budgets(TENANT, ROOT)

    line = next(x for x in view.agents if x.agent_id == "mine")
    assert (line.amount, line.percent, line.level, line.spend) == (None, None, "none", D("4"))
    assert view.tenant.amount is None


async def test_the_tenant_line_counts_every_agent_and_embeddings(h):
    await h.plane.set_budget(TENANT, ROOT, None, D("20"))
    await seed_spend(h, "mine", "5")
    await seed_spend(h, "theirs", "5")
    await seed_spend(h, None, "6", key="emb", kind="embedding")

    view = await h.plane.budgets(TENANT, ROOT)

    assert (view.tenant.spend, view.tenant.percent, view.tenant.level) == (D("16"), 80, "warning")


async def test_unpriced_rows_are_counted_and_flagged_on_the_line_not_treated_as_free(h):
    await h.plane.set_budget(TENANT, ROOT, "mine", D("10"))
    await h.plane.set_budget(TENANT, ROOT, None, D("10"))
    await seed_spend(h, "mine", None, key="np1")
    await seed_spend(h, "mine", None, key="np2")
    await seed_spend(h, "theirs", None, key="np3")

    view = await h.plane.budgets(TENANT, ROOT)

    assert next(x for x in view.agents if x.agent_id == "mine").unpriced_calls == 2
    assert next(x for x in view.agents if x.agent_id == "theirs").unpriced_calls == 1
    assert view.tenant.unpriced_calls == 3


async def test_a_creator_sees_the_budgets_of_their_own_agents_and_no_tenant_line(h):
    await h.plane.set_budget(TENANT, ROOT, "mine", D("10"))
    await h.plane.set_budget(TENANT, ROOT, "theirs", D("10"))
    await h.plane.set_budget(TENANT, ROOT, None, D("10"))

    view = await h.plane.budgets(TENANT, OWNER)

    assert [x.agent_id for x in view.agents] == ["mine"]
    assert view.tenant is None


async def test_someone_with_no_role_may_not_read_budgets(h):
    with pytest.raises(Forbidden):
        await h.plane.budgets(TENANT, "U_NOBODY")


async def test_the_budgets_view_reads_the_month_of_the_clock(h):
    await h.plane.set_budget(TENANT, ROOT, "mine", D("10"))
    await h.usage.record(
        usage_row("last-month", "mine", "10", at=datetime(2026, 10, 31, 23, 0, tzinfo=UTC))
    )

    view = await h.plane.budgets(TENANT, ROOT)

    assert view.month == "2026-11"
    assert next(x for x in view.agents if x.agent_id == "mine").spend == D("0")


# --- the JSON API ---------------------------------------------------------------------------


async def api(h, user, method, path, body=None):
    headers = {"Origin": h.base_url, **JSON}
    async with using(await h.signed_in(user)) as client:
        return await client.request(method, path, headers=headers, content=body or "")


async def test_the_api_sets_reads_raises_and_removes_budgets(h):
    put = await api(h, ROOT, "PUT", f"/tenants/{TENANT}/agents/mine/budget", '{"amount": "25.5"}')
    tenant = await api(h, ROOT, "PUT", f"/tenants/{TENANT}/budget", '{"amount": 400}')
    raised = await api(h, ROOT, "PUT", f"/tenants/{TENANT}/agents/mine/budget", '{"amount": "30"}')
    read = await api(h, ROOT, "GET", f"/tenants/{TENANT}/budgets")

    assert (put.status_code, tenant.status_code, raised.status_code) == (200, 200, 200)
    assert put.json()["amount"] == "25.50"
    body = read.json()
    assert body["month"] == "2026-11"
    assert body["tenant"]["amount"] == "400.00" and body["tenant"]["agent_id"] is None
    line = next(x for x in body["agents"] if x["agent_id"] == "mine")
    assert line["amount"] == "30.00" and line["spend"] == "0" and line["level"] == "ok"
    gone = await api(h, ROOT, "DELETE", f"/tenants/{TENANT}/agents/mine/budget")
    assert gone.status_code == 200
    again = await api(h, ROOT, "DELETE", f"/tenants/{TENANT}/agents/mine/budget")
    assert again.status_code == 404
    assert kinds(h) == ["budget_set"] * 4


async def test_the_api_refuses_a_creator_a_bad_amount_and_an_unknown_agent(h):
    forged = await api(h, OWNER, "PUT", f"/tenants/{TENANT}/agents/mine/budget", '{"amount": "1"}')
    forged_remove = await api(h, OWNER, "DELETE", f"/tenants/{TENANT}/budget")
    bad = await api(h, ROOT, "PUT", f"/tenants/{TENANT}/budget", '{"amount": "-1"}')
    wrong = await api(h, ROOT, "PUT", f"/tenants/{TENANT}/budget", '{"amount": "x"}')
    extra = await api(h, ROOT, "PUT", f"/tenants/{TENANT}/budget", '{"amount": "1", "user": "U1"}')
    ghost = await api(h, ROOT, "PUT", f"/tenants/{TENANT}/agents/ghost/budget", '{"amount": "1"}')

    assert (forged.status_code, forged_remove.status_code) == (403, 403)
    assert bad.status_code == 422 and "positive" in bad.json()["error"]
    assert wrong.status_code == 422 and extra.status_code == 422
    assert ghost.status_code == 404
    assert await h.budgets.list(TENANT) == []


async def test_the_api_narrows_a_creator_to_their_agents(h):
    await h.plane.set_budget(TENANT, ROOT, None, D("9"))
    await h.plane.set_budget(TENANT, ROOT, "theirs", D("9"))

    read = await api(h, OWNER, "GET", f"/tenants/{TENANT}/budgets")

    body = read.json()
    assert body["tenant"] is None and [x["agent_id"] for x in body["agents"]] == ["mine"]


# --- the console --------------------------------------------------------------------------------


async def page(h, user, path=f"{UI}/usage"):
    async with using(await h.signed_in(user)) as client:
        return await client.get(path)


def text_of(response):
    return " ".join(re.sub(r"<[^>]+>", " ", response.text).split())


async def test_the_usage_page_warns_at_eighty_percent_with_spend_budget_and_percent(h):
    await h.plane.set_budget(TENANT, ROOT, "mine", D("100"))
    await seed_spend(h, "mine", "80")

    response = await page(h, ROOT)

    text = text_of(response)
    assert response.status_code == 200
    assert "mine has used 80% of its monthly budget" in text
    assert "$80.0000" in text and "$100.00" in text and "80%" in text


async def test_the_usage_page_says_when_an_agent_is_stopped(h):
    await h.plane.set_budget(TENANT, ROOT, "mine", D("10"))
    await seed_spend(h, "mine", "10")

    text = text_of(await page(h, ROOT))

    assert "mine has reached its monthly budget and is not answering" in text


async def test_the_usage_page_has_no_warning_under_eighty_percent(h):
    await h.plane.set_budget(TENANT, ROOT, "mine", D("100"))
    await seed_spend(h, "mine", "79")

    text = text_of(await page(h, ROOT))

    assert "of its monthly budget" not in text and "has reached" not in text


async def test_the_usage_page_warns_about_the_tenant_budget_for_admins_only(h):
    await h.plane.set_budget(TENANT, ROOT, None, D("10"))
    await seed_spend(h, "mine", "9")

    admin, creator = await page(h, ROOT), await page(h, OWNER)

    assert "The organisation has used 90% of its monthly budget" in text_of(admin)
    assert "organisation has used" not in text_of(creator)


async def test_the_usage_page_says_the_budget_cannot_see_unpriced_calls(h):
    await h.plane.set_budget(TENANT, ROOT, "mine", D("10"))
    await seed_spend(h, "mine", None, key="np")

    text = text_of(await page(h, ROOT))

    assert "1 model call has no price" in text and "cannot count" in text
    assert "mine is paused" in text or "is paused until" in text
    body = (await api(h, ROOT, "GET", f"/tenants/{TENANT}/budgets")).json()
    line = next(x for x in body["agents"] if x["agent_id"] == "mine")
    assert line["stop_reason"] == "spend_unknown"


async def test_no_unpriced_warning_when_no_budget_is_set_or_every_call_is_priced(h):
    await seed_spend(h, "mine", None, key="np")
    assert "cannot count" not in text_of(await page(h, ROOT))

    await h.plane.set_budget(TENANT, ROOT, "theirs", D("10"))
    assert "cannot count" not in text_of(await page(h, ROOT))


async def test_an_admin_sees_budget_forms_and_a_creator_sees_the_budget_read_only(h):
    await h.plane.set_budget(TENANT, ROOT, "mine", D("10"))

    admin, creator = await page(h, ROOT), await page(h, OWNER)

    assert f'action="{UI}/budgets"' in admin.text and f'action="{UI}/budgets/remove"' in admin.text
    assert 'name="amount"' in admin.text
    text = text_of(creator)
    assert "$10.00" in text and "mine" in text
    assert "/budgets" not in creator.text and 'name="amount"' not in creator.text
    assert "theirs" not in creator.text


async def test_the_agent_page_shows_its_budget_warning_to_its_creator(h):
    await h.plane.set_budget(TENANT, ROOT, "mine", D("10"))
    await seed_spend(h, "mine", "8")

    text = text_of(await page(h, OWNER, f"{UI}/agents/mine"))

    assert "mine has used 80% of its monthly budget" in text
    assert "$8.0000 of $10.00" in text


async def test_the_agent_page_of_another_creators_agent_shows_no_budget(h):
    await h.plane.set_budget(TENANT, ROOT, "theirs", D("10"))
    await seed_spend(h, "theirs", "9")

    text = text_of(await page(h, OWNER, f"{UI}/agents/theirs"))

    assert "monthly budget" not in text and "$10.00" not in text


async def test_an_admin_sets_and_removes_a_budget_from_the_page(h):
    async with using(await h.signed_in(ROOT)) as client:
        token = await csrf_token(client)
        set_it = await post(client, f"{UI}/budgets", token, agent_id="mine", amount="12.5")
        tenant = await post(client, f"{UI}/budgets", token, agent_id="", amount="300")
        shown = await client.get(f"{UI}/usage")
        removed = await post(client, f"{UI}/budgets/remove", token, agent_id="mine")

    assert set_it.status_code == 303 and tenant.status_code == 303
    assert set_it.headers["location"] == f"{UI}/usage?done=budget-set"
    assert removed.headers["location"] == f"{UI}/usage?done=budget-removed"
    assert "$12.50" in shown.text and "$300.00" in shown.text
    assert [(e.agent_id, e.target) for e in h.audit.events] == [
        ("mine", "12.50"),
        (None, "300.00"),
        ("mine", "0"),
    ]
    [left] = await h.budgets.list(TENANT)
    assert left.agent_id is None


async def test_the_page_tells_what_changed(h):
    text = text_of(await page(h, ROOT, f"{UI}/usage?done=budget-set"))

    assert "Budget saved." in text


async def test_a_forged_budget_post_from_a_creator_is_refused_and_changes_nothing(h):
    async with using(await h.signed_in(OWNER)) as client:
        token = await csrf_token(client)
        forged = await post(client, f"{UI}/budgets", token, agent_id="mine", amount="999")
        forged_remove = await post(client, f"{UI}/budgets/remove", token, agent_id="mine")

    assert forged.status_code == 403 and forged_remove.status_code == 403
    assert await h.budgets.list(TENANT) == [] and h.audit.events == []


async def test_a_budget_post_without_the_csrf_token_is_refused(h):
    async with using(await h.signed_in(ROOT)) as client:
        missing = await post(client, f"{UI}/budgets", None, agent_id="mine", amount="5")
        wrong = await post(client, f"{UI}/budgets", "not-the-token", agent_id="mine", amount="5")

    assert missing.status_code == 403 and wrong.status_code == 403
    assert await h.budgets.list(TENANT) == []


async def test_a_bad_amount_in_the_form_is_shown_as_a_refusal(h):
    async with using(await h.signed_in(ROOT)) as client:
        token = await csrf_token(client)
        bad = await post(client, f"{UI}/budgets", token, agent_id="mine", amount="-3")
        ghost = await post(client, f"{UI}/budgets", token, agent_id="ghost", amount="3")

    assert bad.status_code == 422 and "positive" in bad.text
    assert ghost.status_code == 404
    assert await h.budgets.list(TENANT) == []


async def test_the_audit_page_shows_a_budget_event_without_a_version(h):
    await h.plane.set_budget(TENANT, ROOT, "mine", D("5"))
    await h.plane.set_budget(TENANT, ROOT, None, D("7"))

    response = await page(h, ROOT, f"{UI}/audit")

    assert response.status_code == 200
    assert "budget_set" in response.text and "vNone" not in response.text


# --- audit kinds and SQL ------------------------------------------------------------------------

MIGRATION_012 = (REPO / "scripts" / "migrations" / "012_budgets.sql").read_text()


def test_budget_set_is_an_audit_kind_that_needs_no_agent_version():
    assert "budget_set" in AUDIT_KINDS
    AuditEvent("acme", "k", "budget_set", None, None, "U1", "user:U1", target="50.00")
    AuditEvent("acme", "k", "budget_set", "mine", None, "U1", "user:U1", target="50.00")
    with pytest.raises(ValueError):
        AuditEvent("acme", "k", "answer", None, None, "U1", "agent:a")


def test_a_budget_row_holds_no_requester_and_no_content():
    columns = MIGRATION_012[MIGRATION_012.index("CREATE TABLE") :]
    columns = columns[: columns.index("\n);")]
    for word in ("requester", "content", "prompt", "text_body"):
        assert word not in columns


# --- indexing stops at the tenant budget ---------------------------------------------------------


def index_world(clock=None, ttl=60):
    cfg = parse_agents_config(_page_agents())
    notion = make_fake_notion()
    notion.add_page_tree(ROOT_A, a_tree())
    registry = TenantRegistry({TENANT: runtime_for(cfg, FakeSlack(), notion)})
    guard, budgets, usage = make_guard(ttl=ttl, clock=clock)
    embedder = FakeEmbedder()
    store = InMemoryKnowledgeStore()
    acts = IndexActivities(
        registry,
        store,
        embedder,
        seeded_agent_store(cfg),
        usage=UsageRecorder(usage, Prices([])),
        budgets=guard,
    )
    return acts, embedder, store, budgets, usage


async def sync_root(acts):
    return await ActivityEnvironment().run(acts.sync_page, TENANT, SCOPE, ROOT_A)


async def test_a_tenant_at_its_budget_makes_no_embedding_call_and_says_so_once(caplog):
    acts, embedder, store, budgets, usage = index_world()
    await budgets.set_budget(TENANT, None, D("10"), ROOT)
    await usage.record(usage_row("1", cost="10"))

    with caplog.at_level(logging.INFO, logger="agentos.indexing"):
        result = await sync_root(acts)

    assert embedder.texts_embedded == 0 and store.pages == {}
    assert result.budget_stopped and not result.skipped and not result.changed
    assert result.child_page_ids
    [line] = [r.getMessage() for r in caplog.records if "budget" in r.getMessage()]
    assert line == f"indexing stopped tenant_id={TENANT} scope={SCOPE} budget=tenant"


async def test_a_tenant_over_its_budget_makes_no_embedding_call():
    acts, embedder, _, budgets, usage = index_world()
    await budgets.set_budget(TENANT, None, D("10"), ROOT)
    await usage.record(usage_row("1", cost="10.01"))

    await sync_root(acts)

    assert embedder.texts_embedded == 0


async def test_a_tenant_under_its_budget_is_indexed():
    acts, embedder, store, budgets, usage = index_world()
    await budgets.set_budget(TENANT, None, D("10"), ROOT)
    await usage.record(usage_row("1", cost="9.99"))

    result = await sync_root(acts)

    assert result.changed and not result.budget_stopped
    assert embedder.texts_embedded > 0 and len(store.pages) == 1


async def test_indexing_resumes_when_the_budget_is_raised_or_removed_or_the_month_turns():
    clock = Clock()
    acts, embedder, store, budgets, usage = index_world(clock=clock)
    await budgets.set_budget(TENANT, None, D("10"), ROOT)
    await usage.record(usage_row("1", cost="10"))
    assert (await sync_root(acts)).budget_stopped

    await budgets.set_budget(TENANT, None, D("20"), ROOT)
    clock.now += timedelta(seconds=60)
    assert not (await sync_root(acts)).budget_stopped
    assert embedder.texts_embedded > 0

    store.pages.clear()
    await budgets.set_budget(TENANT, None, D("1"), ROOT)
    clock.now += timedelta(seconds=60)
    assert (await sync_root(acts)).budget_stopped

    await budgets.remove_budget(TENANT, None)
    clock.now += timedelta(seconds=60)
    assert (await sync_root(acts)).changed

    store.pages.clear()
    await budgets.set_budget(TENANT, None, D("1"), ROOT)
    clock.now += timedelta(seconds=60)
    assert (await sync_root(acts)).budget_stopped
    clock.now = datetime(2026, 12, 1, 0, 0, tzinfo=UTC)
    assert (await sync_root(acts)).changed


async def test_an_agent_budget_at_its_limit_never_stops_indexing():
    acts, embedder, _, budgets, usage = index_world()
    await budgets.set_budget(TENANT, "it-helper", D("1"), ROOT)
    await usage.record(usage_row("1", cost="50"))

    result = await sync_root(acts)

    assert result.changed and not result.budget_stopped and embedder.texts_embedded > 0


async def test_an_unchanged_page_is_not_a_budget_stop():
    acts, _, _, budgets, usage = index_world()
    await sync_root(acts)
    await budgets.set_budget(TENANT, None, D("1"), ROOT)
    await usage.record(usage_row("1", cost="5"))

    result = await sync_root(acts)

    assert not result.changed and not result.budget_stopped


async def test_a_page_event_over_budget_is_not_reported_as_indexed_or_removed():
    acts, embedder, _, budgets, usage = index_world()
    await budgets.set_budget(TENANT, None, D("1"), ROOT)
    await usage.record(usage_row("1", cost="5"))

    result = await ActivityEnvironment().run(acts.refresh_page, TENANT, ROOT_A)

    assert (result.indexed, result.removed) == ([], [])
    assert embedder.texts_embedded == 0


async def test_an_unreadable_budget_fails_the_indexing_step_instead_of_embedding():
    class Broken(InMemoryBudgetStore):
        async def list(self, tenant_id):
            raise RuntimeError("database down")

    acts, embedder, *_ = index_world()
    acts.budgets = BudgetGuard(Broken(), InMemoryUsageStore(), Clock())

    with pytest.raises(RuntimeError):
        await sync_root(acts)

    assert embedder.texts_embedded == 0


# --- spend per cost type -------------------------------------------------------------------------


async def test_the_tenant_line_splits_its_spend_by_cost_type_and_the_agent_line_is_model_only(h):
    await h.plane.set_budget(TENANT, ROOT, None, D("100"))
    await seed_spend(h, "mine", "5")
    await seed_spend(h, None, "2.5", key="emb", kind="embedding")

    view = await h.plane.budgets(TENANT, ROOT)

    assert [(k.kind, k.label, k.cost) for k in view.tenant.kinds] == [
        ("llm", "Model", D("5")),
        ("embedding", "Embedding", D("2.5")),
    ]
    mine = next(x for x in view.agents if x.agent_id == "mine")
    assert [(k.kind, k.cost) for k in mine.kinds] == [("llm", D("5"))]


async def test_the_api_reads_spend_per_cost_type(h):
    await h.plane.set_budget(TENANT, ROOT, None, D("100"))
    await seed_spend(h, "mine", "5")
    await seed_spend(h, None, "2.5", key="emb", kind="embedding")

    body = (await api(h, ROOT, "GET", f"/tenants/{TENANT}/budgets")).json()

    assert body["tenant"]["spend"] == "7.5"
    assert body["tenant"]["kinds"] == [
        {"kind": "llm", "label": "Model", "spend": "5", "unpriced_calls": 0},
        {"kind": "embedding", "label": "Embedding", "spend": "2.5", "unpriced_calls": 0},
    ]
    mine = next(x for x in body["agents"] if x["agent_id"] == "mine")
    assert [k["kind"] for k in mine["kinds"]] == ["llm"]


async def test_the_usage_page_shows_model_and_embedding_spend_side_by_side(h):
    await h.plane.set_budget(TENANT, ROOT, None, D("100"))
    await seed_spend(h, "mine", "5")
    await seed_spend(h, None, "2.5", key="emb", kind="embedding")

    text = text_of(await page(h, ROOT))

    assert "Model $5.0000" in text and "Embedding $2.5000" in text


async def test_the_eighty_percent_warning_names_each_cost_type(h):
    await h.plane.set_budget(TENANT, ROOT, None, D("10"))
    await seed_spend(h, "mine", "6")
    await seed_spend(h, None, "2.5", key="emb", kind="embedding")

    text = text_of(await page(h, ROOT))

    assert "The organisation has used 85% of its monthly budget" in text
    assert "Model $6.0000" in text and "Embedding $2.5000" in text


async def test_the_stopped_notice_for_the_tenant_says_indexing_is_paused_too(h):
    await h.plane.set_budget(TENANT, ROOT, None, D("10"))
    await seed_spend(h, None, "10", key="emb", kind="embedding")

    text = text_of(await page(h, ROOT))

    assert "The organisation has reached its monthly budget" in text
    assert "indexing is paused" in text


async def test_the_agent_page_shows_its_spend_by_cost_type(h):
    await h.plane.set_budget(TENANT, ROOT, "mine", D("10"))
    await seed_spend(h, "mine", "8")

    text = text_of(await page(h, OWNER, f"{UI}/agents/mine"))

    assert "$8.0000 of $10.00" in text and "Model $8.0000" in text


async def test_a_new_cost_type_in_the_kinds_list_shows_on_the_page_and_in_the_api(h, monkeypatch):
    monkeypatch.setitem(usage_module.KIND_LABELS, "storage", "Storage")
    await h.plane.set_budget(TENANT, ROOT, None, D("100"))
    await seed_spend(h, None, "3", key="s", kind="storage")

    text = text_of(await page(h, ROOT))
    body = (await api(h, ROOT, "GET", f"/tenants/{TENANT}/budgets")).json()

    assert "Storage $3.0000" in text
    assert [k["kind"] for k in body["tenant"]["kinds"]] == ["llm", "embedding", "storage"]
    assert body["tenant"]["spend"] == "3"


async def test_unpriced_calls_are_warned_per_cost_type(h):
    await h.plane.set_budget(TENANT, ROOT, None, D("100"))
    await seed_spend(h, "mine", None, key="np1")
    await seed_spend(h, "mine", None, key="np2")
    await seed_spend(h, None, None, key="np3", kind="embedding")

    view = await h.plane.budgets(TENANT, ROOT)
    text = text_of(await page(h, ROOT))

    assert [(k.kind, k.unpriced_calls) for k in view.tenant.kinds] == [("llm", 2), ("embedding", 1)]
    assert "2 model calls have no price" in text
    assert "1 embedding call has no price" in text


# --- the cache: single flight, immediate increments, unmetered calls -------------------


class CountingUsage(InMemoryUsageStore):
    def __init__(self):
        super().__init__()
        self.reads = 0

    async def totals(self, tenant_id, start, end):
        self.reads += 1
        await asyncio.sleep(0)
        return await super().totals(tenant_id, start, end)


async def test_concurrent_checks_after_expiry_share_one_aggregate_read():
    """Fails if N checks that find the cache expired each run the month's GROUP BY."""
    budgets, usage = InMemoryBudgetStore(), CountingUsage()
    guard = BudgetGuard(budgets, usage, Clock(), ttl_seconds=60)
    await budgets.set_budget(TENANT, "it-helper", D("5"), ROOT)

    await asyncio.gather(*(guard.stopped_by(TENANT, "it-helper") for _ in range(10)))

    assert usage.reads == 1


async def test_a_recorded_cost_raises_the_cached_spend_at_once():
    """Fails if a call recorded inside the cache window is not counted until the cache expires."""
    guard, budgets, usage = make_guard()
    recorder = UsageRecorder(usage, Prices([price("m", i="1000000", o="0", cr="0", cw="0")]))
    recorder.clock = Clock()
    recorder.listener = guard
    await budgets.set_budget(TENANT, "it-helper", D("5"), ROOT)
    assert await guard.stopped_by(TENANT, "it-helper") is None

    await recorder.llm_call(
        tenant_id=TENANT,
        agent_id="it-helper",
        agent_version=1,
        conversation_id="c",
        key="k",
        usage=Usage(5, 0, 0, 0, "m"),
        fallback_model="m",
    )

    assert await guard.stopped_by(TENANT, "it-helper") == "agent"


async def test_a_call_whose_row_was_lost_makes_the_agent_unmeasured_for_the_window():
    """Fails if a paid call dropped on a database failure leaves the budget blind to it."""
    clock = Clock()
    guard, budgets, _ = make_guard(clock=clock)
    await budgets.set_budget(TENANT, "it-helper", D("1000"), ROOT)
    assert await guard.stopped_by(TENANT, "it-helper") is None

    guard.note_recorded(usage_row("lost"), stored=False)

    assert await guard.stopped_by(TENANT, "it-helper") == "unpriced"
    clock.now += timedelta(seconds=61)
    assert await guard.stopped_by(TENANT, "it-helper") is None
