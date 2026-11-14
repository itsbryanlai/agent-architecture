"""Monthly budgets: the stop before a model call, the warning, who may set one, and the SQL.

Fakes only: scripted model, in-memory stores, a fake Slack sign-in, a clock the test sets.
"""

import logging
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from temporalio.testing import ActivityEnvironment

from agentos.activities import AgentActivities
from agentos.agent_store import AgentNotFound, seeded_agent_store
from agentos.audit import InMemoryAuditStore
from agentos.budgets import (
    STOP_REASON,
    BudgetGuard,
    BudgetNotFound,
    InMemoryBudgetStore,
    InvalidBudget,
    PostgresBudgetStore,
    stop_text,
)
from agentos.config import Settings
from agentos.connectors.notion import FakeNotionAPI
from agentos.control import Forbidden
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
from tests.conftest import ALICE, TENANT
from tests.console_harness import OTHER, OWNER, ROOT, ConsoleHarness, csrf_token, post, using
from tests.test_row_level_security import FakePool
from tests.test_usage import block, price

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


async def test_unpriced_rows_cannot_reach_a_dollar_budget():
    guard, budgets, usage = make_guard()
    await budgets.set_budget(TENANT, "it-helper", D("1"), ROOT)
    await usage.record(usage_row("1", cost=None))

    assert await guard.stopped_by(TENANT, "it-helper") is None


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

    assert "1 call has no price" in text and "cannot count" in text


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

MIGRATION_010 = (REPO / "scripts" / "migrations" / "010_audience.sql").read_text()
MIGRATION_012 = (REPO / "scripts" / "migrations" / "012_budgets.sql").read_text()
INIT_DB = (REPO / "scripts" / "init_db.sql").read_text()


def kinds_in(sql):
    block_ = sql[sql.index("kind IN (") :]
    return set(re.findall(r"'(\w+)'", block_[: block_.index("))")]))


def test_budget_set_is_an_audit_kind_that_needs_no_agent_version():
    assert "budget_set" in AUDIT_KINDS
    AuditEvent("acme", "k", "budget_set", None, None, "U1", "user:U1", target="50.00")
    AuditEvent("acme", "k", "budget_set", "mine", None, "U1", "user:U1", target="50.00")
    with pytest.raises(ValueError):
        AuditEvent("acme", "k", "answer", None, None, "U1", "agent:a")


def test_migration_012_keeps_every_010_kind_and_adds_budget_set():
    old, new = kinds_in(MIGRATION_010), kinds_in(MIGRATION_012)
    assert old < new and new - old == {"budget_set"}
    assert "DROP CONSTRAINT IF EXISTS audit_events_kind_check" in MIGRATION_012


def test_init_db_and_migration_012_allow_the_same_kinds_as_the_code():
    assert kinds_in(MIGRATION_012) == kinds_in(INIT_DB) == set(AUDIT_KINDS)


def test_a_budget_event_may_leave_out_the_agent_in_both_sql_files():
    for sql in (MIGRATION_012, INIT_DB):
        rule = sql[sql.index("audit_events_agent_required CHECK") :]
        rule = rule[: rule.index("agent_id IS NOT NULL")]
        assert "'budget_set'" in rule
        for kind in ("member_add", "member_remove", "member_role"):
            assert f"'{kind}'" in rule
    assert "DROP CONSTRAINT IF EXISTS audit_events_agent_required" in MIGRATION_012


@pytest.mark.parametrize("sql", [MIGRATION_012, INIT_DB], ids=["migration", "init_db"])
def test_budgets_sql_is_tenant_scoped_one_row_per_scope_and_positive(sql):
    assert "CREATE TABLE IF NOT EXISTS budgets" in sql
    assert "CHECK (amount_usd > 0)" in sql
    assert "UNIQUE INDEX IF NOT EXISTS budgets_one_per_scope" in sql
    assert "(tenant_id, (COALESCE(agent_id, '')))" in sql
    assert "ALTER TABLE budgets ENABLE ROW LEVEL SECURITY" in sql
    assert "ALTER TABLE budgets FORCE ROW LEVEL SECURITY" in sql
    assert "CREATE POLICY tenant_isolation ON budgets" in sql
    assert "USING (tenant_id = current_setting('app.tenant_id', true))" in sql
    assert "WITH CHECK (tenant_id = current_setting('app.tenant_id', true))" in sql
    assert "REVOKE ALL ON budgets FROM agentos_app" in sql
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON budgets TO agentos_app" in sql


def test_init_db_matches_the_budgets_migration():
    block_ = MIGRATION_012[MIGRATION_012.index("CREATE TABLE IF NOT EXISTS budgets") :]
    block_ = block_[: block_.index("ALTER TABLE audit_events")].rstrip()
    assert block_ in INIT_DB


def test_a_budget_row_holds_no_requester_and_no_content():
    columns = MIGRATION_012[MIGRATION_012.index("CREATE TABLE") :]
    columns = columns[: columns.index("\n);")]
    for word in ("requester", "content", "prompt", "text_body"):
        assert word not in columns


async def test_the_postgres_store_upserts_by_scope_and_names_the_tenant_in_every_statement():
    pool = FakePool(row=(TENANT, None, D("5.00"), ROOT, NOV))
    store = PostgresBudgetStore(pool)

    await store.set_budget(TENANT, None, D("5.00"), ROOT)
    await store.set_budget(TENANT, "mine", D("5.00"), ROOT)
    await store.list(TENANT)
    await store.remove_budget(TENANT, "mine")

    executed = [e for e in pool.events if e[0] == "execute" and "set_config" not in e[1]]
    upsert, upsert_agent, listing, delete = executed
    assert "ON CONFLICT (tenant_id, (COALESCE(agent_id, ''))) DO UPDATE" in upsert[1]
    assert upsert[2][0] == TENANT and upsert[2][1] is None and upsert_agent[2][1] == "mine"
    assert "tenant_id = %s" in listing[1] and listing[2] == (TENANT,)
    assert "DELETE FROM budgets" in delete[1] and delete[2][0] == TENANT
