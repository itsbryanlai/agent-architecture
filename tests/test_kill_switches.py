"""Kill switches: what each one stops, who may use them, and the SQL.

Fakes only: scripted model, in-memory stores, fake Slack and Notion, a fake Slack sign-in
and Temporal's time-skipping test server.
"""

import json
import logging
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlencode

import httpx
import pytest
from slack_sdk.signature import SignatureVerifier
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_store import AgentNotFound, seeded_agent_store
from agentos.audit import InMemoryAuditStore
from agentos.budgets import BudgetGuard, InMemoryBudgetStore
from agentos.config import (
    TASK_QUEUE,
    Settings,
    TenantsConfig,
    parse_agents_config,
)
from agentos.control import Forbidden
from agentos.gateway.app import create_app
from agentos.knowledge.embedder import FakeEmbedder
from agentos.knowledge.indexing import IndexActivities
from agentos.knowledge.store import InMemoryKnowledgeStore
from agentos.llm import ScriptedLLMClient
from agentos.models import (
    AUDIT_KINDS,
    ApprovalDecision,
    AuditEvent,
    ConversationRef,
    InboundMessage,
    LLMResponse,
    TaskSpec,
    ToolCall,
    TurnContext,
    Usage,
)
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.switches import (
    AGENT,
    ALL_AGENTS,
    CONNECTOR,
    NOTION,
    SWITCHED_OFF,
    WRITES,
    InMemorySwitchStore,
    InvalidSwitch,
    PostgresSwitchStore,
    agent_off_text,
    build_parser,
    check_switch,
    operator_identity,
    read_state,
    set_switch,
)
from agentos.switches import _targets as cli_targets
from agentos.switches import execute as cli_execute
from agentos.switches import run as cli_run
from agentos.tenants import TenantRegistry, runtime_for
from agentos.usage import InMemoryUsageStore, Prices, UsageRecorder
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import (
    ALICE,
    DB_ID,
    ROOT_A,
    TEAM,
    TENANT,
    a_tree,
    eventually,
    knowledge_agents_dict,
    make_fake_notion,
    title_prop,
)
from tests.console_harness import OTHER, OWNER, ROOT, ConsoleHarness, csrf_token, post, using
from tests.test_gateway import FakeTemporalClient
from tests.test_row_level_security import FakePool
from tests.test_task_workflow import approve, start_task, thread_posts, wait_state
from tests.test_usage import price

REPO = Path(__file__).resolve().parent.parent
NOV = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)
UI = f"/ui/{TENANT}"
JSON = {"Content-Type": "application/json"}
GLOBEX = "globex"
ADMIN = "U_ADMIN"
CONV = "acme:conv:C1:1.1"


async def off(store, scope, target="", tenant=TENANT):
    assert await store.switch_off(tenant, scope, target, ADMIN, NOV)


def turn_ctx(agent="it-helper", kind="channel", tenant=TENANT) -> TurnContext:
    return TurnContext(
        tenant_id=tenant,
        agent_id=agent,
        agent_version=1,
        requester_user_id=ALICE,
        channel="C1" if kind == "channel" else "D1",
        thread_ts="1.1",
        channel_kind=kind,
    )


def answer() -> LLMResponse:
    return LLMResponse(
        text="done", tool_calls=[], stop_reason="end_turn", usage=Usage(1000, 500, 0, 0, "m")
    )


class World:
    """Activities over two tenants and three agents, with one switch store shared by all."""

    def __init__(self, guard=None, usage_store=None):
        self.switches = InMemorySwitchStore()
        self.llm = ScriptedLLMClient([answer(), answer(), answer(), answer()])
        self.audit = InMemoryAuditStore()
        self.notion = make_fake_notion()
        self.usage = usage_store if usage_store is not None else InMemoryUsageStore()
        acme = parse_agents_config(knowledge_agents_dict())
        globex_raw = knowledge_agents_dict()
        globex_raw["tenant_id"], globex_raw["slack_team_id"] = GLOBEX, "T0GLOBEX"
        globex = parse_agents_config(globex_raw)
        registry = TenantRegistry(
            {
                TENANT: runtime_for(acme, FakeSlack(), self.notion),
                GLOBEX: runtime_for(globex, FakeSlack(), make_fake_notion()),
            }
        )
        self.acts = AgentActivities(
            settings=Settings(model="configured-model"),
            registry=registry,
            history=InMemoryHistoryStore(),
            idem=InMemoryIdempotencyStore(),
            links=InMemoryTaskLinkStore(),
            llm=self.llm,
            tool_outputs=InMemoryToolOutputStore(),
            audit=self.audit,
            agents=seeded_agent_store(acme, globex),
            usage=UsageRecorder(self.usage, Prices([price("m")])),
            budgets=guard,
            switches=self.switches,
        )

    async def turn(self, agent="it-helper", kind="channel", tenant=TENANT) -> LLMResponse:
        return await ActivityEnvironment().run(
            self.acts.call_llm, turn_ctx(agent, kind, tenant), f"{tenant}:conv:C1:1.1"
        )


@pytest.fixture
def w():
    return World()


# --- an agent, every agent ----------------------------------------------------------------


async def test_an_agent_that_is_off_answers_with_the_fixed_text_and_never_calls_the_model(w):
    await off(w.switches, AGENT, "it-helper")

    resp = await w.turn()

    assert resp.text == "IT helper is turned off by an admin."
    assert resp.tool_calls == [] and resp.stop_reason == SWITCHED_OFF == "switched_off"
    assert resp.usage == Usage()
    assert w.llm.calls == []
    assert w.usage.events == []


async def test_an_agent_switch_stops_that_agent_only(w):
    await off(w.switches, AGENT, "it-helper")

    other = await w.turn("hr-helper")
    other_tenant = await w.turn("it-helper", tenant=GLOBEX)

    assert other.text == "done" and other_tenant.text == "done"
    assert len(w.llm.calls) == 2 and len(w.usage.events) == 2


async def test_the_all_agents_switch_stops_every_agent_of_the_tenant_and_no_other_tenant(w):
    await off(w.switches, ALL_AGENTS)

    stopped = [await w.turn(a) for a in ("it-helper", "hr-helper", "plain")]
    other_tenant = await w.turn("it-helper", tenant=GLOBEX)

    assert [r.text for r in stopped] == [
        "IT helper is turned off by an admin.",
        "HR helper is turned off by an admin.",
        "Plain helper is turned off by an admin.",
    ]
    assert other_tenant.text == "done" and len(w.llm.calls) == 1


@pytest.mark.parametrize("kind", ["channel", "im"])
async def test_a_switch_stops_a_direct_message_as_it_stops_a_channel(w, kind):
    await off(w.switches, AGENT, "it-helper")

    resp = await w.turn(kind=kind)

    assert resp.stop_reason == SWITCHED_OFF and w.llm.calls == []


def test_the_fixed_text_names_the_agent():
    assert agent_off_text("Docs bot") == "Docs bot is turned off by an admin."


async def test_turning_an_agent_back_on_resumes_it(w):
    await off(w.switches, AGENT, "it-helper")
    assert (await w.turn()).stop_reason == SWITCHED_OFF

    assert await w.switches.switch_on(TENANT, AGENT, "it-helper")

    assert (await w.turn()).text == "done"


async def test_a_switch_is_read_again_at_every_step_with_no_cache(w):
    first = await w.turn()
    await off(w.switches, ALL_AGENTS)
    second = await w.turn()
    await w.switches.switch_on(TENANT, ALL_AGENTS, "")
    third = await w.turn()

    assert [first.text, second.stop_reason, third.text] == ["done", SWITCHED_OFF, "done"]


async def test_the_agent_switch_is_checked_before_the_budget():
    class Unreadable(InMemoryBudgetStore):
        async def list(self, tenant_id):
            raise AssertionError("the budget was read")

    world = World(BudgetGuard(Unreadable(), InMemoryUsageStore()))
    await off(world.switches, AGENT, "it-helper")

    resp = await world.turn()

    assert resp.stop_reason == SWITCHED_OFF


async def test_a_reached_budget_still_stops_an_agent_that_is_on():
    budgets, usage = InMemoryBudgetStore(), InMemoryUsageStore()
    await budgets.set_budget(TENANT, "it-helper", Decimal(1), ROOT)
    from tests.test_budgets import usage_row

    await usage.record(usage_row("spent", cost="2"))
    world = World(BudgetGuard(budgets, usage, lambda: NOV))

    resp = await world.turn()

    assert resp.stop_reason == "budget_reached" and world.llm.calls == []


async def test_a_stopped_turn_logs_one_line_with_ids_and_the_switch_and_no_content(w, caplog):
    await off(w.switches, AGENT, "it-helper")

    with caplog.at_level(logging.INFO, logger="agentos.switches"):
        await w.turn()

    [line] = [r.getMessage() for r in caplog.records if r.name == "agentos.switches"]
    assert f"tenant_id={TENANT}" in line and "agent_id=it-helper" in line and "switch=agent" in line
    assert ALICE not in line and "turned off by an admin" not in line


async def test_a_switch_that_cannot_be_read_fails_the_step_so_temporal_retries(w):
    class Broken(InMemorySwitchStore):
        async def list(self, tenant_id):
            raise ConnectionError("down")

    w.acts.switches = Broken()

    with pytest.raises(ConnectionError):
        await w.turn()
    assert w.llm.calls == []


async def test_an_agent_that_is_off_answers_a_whole_conversation_turn_without_the_model(harness):
    h = harness
    await off(h.switches, AGENT, "it-helper")
    msg = InboundMessage("Ev1", ALICE, "hello", "C_GENERAL", "1700000000.000100", "channel")

    await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "it-helper", "C_GENERAL", "1700000000.000100"),
        id=ids.conversation_id(TENANT, "C_GENERAL", "1700000000.000100"),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )
    await eventually(lambda: any("turned off" in p["text"] for p in h.slack.posts()))

    assert h.llm.calls == []
    assert [p["text"] for p in h.slack.posts()] == ["IT helper is turned off by an admin."]


# --- a connector --------------------------------------------------------------------------


class Knowledge:
    """An agent granted a page tree that is already indexed, with a switch store."""

    def __init__(self):
        self.cfg = parse_agents_config(knowledge_agents_dict())
        self.notion = make_fake_notion()
        self.notion.add_page_tree(ROOT_A, a_tree())
        self.knowledge = InMemoryKnowledgeStore()
        self.embedder = FakeEmbedder()
        self.switches = InMemorySwitchStore()
        self.tool_outputs = InMemoryToolOutputStore()
        registry = TenantRegistry({TENANT: runtime_for(self.cfg, FakeSlack(), self.notion)})
        agents = seeded_agent_store(self.cfg)
        self.audit = InMemoryAuditStore()
        self.acts = AgentActivities(
            settings=Settings(),
            registry=registry,
            history=InMemoryHistoryStore(),
            idem=InMemoryIdempotencyStore(),
            links=InMemoryTaskLinkStore(),
            llm=ScriptedLLMClient(),
            tool_outputs=self.tool_outputs,
            audit=self.audit,
            agents=agents,
            knowledge=self.knowledge,
            embedder=self.embedder,
            switches=self.switches,
        )
        self.index = IndexActivities(
            registry, self.knowledge, self.embedder, agents, switches=self.switches
        )
        self.scope = ids.knowledge_scope(ROOT_A)
        self.root = ROOT_A.replace("-", "")

    async def run(self, fn, *args):
        return await ActivityEnvironment().run(fn, *args)

    async def read(self, call: ToolCall, agent="it-helper"):
        result = await self.run(self.acts.run_read_tool, turn_ctx(agent), CONV, call, 1)
        text, is_error = (await self.tool_outputs.get_many(TENANT, CONV, [call.id]))[call.id]
        return text, is_error, result


@pytest.fixture
def kn():
    return Knowledge()


def search_call(tool_id="tu_1") -> ToolCall:
    return ToolCall(tool_id, "search_knowledge", {"query": "reset VPN password"})


async def test_a_connector_that_is_off_returns_an_error_for_a_tool_call_and_reads_nothing(kn):
    await off(kn.switches, CONNECTOR, NOTION)
    calls_before = len(kn.notion.calls)

    text, is_error, result = await kn.read(ToolCall("tu_1", "search_items", {"query": "vpn"}))

    assert is_error and result.is_error and result.stored
    assert text == "Notion is turned off by an admin, so this can't be done now."
    assert len(kn.notion.calls) == calls_before
    [event] = kn.audit.events
    assert event.kind == "tool_call" and event.outcome == "error"


async def test_knowledge_search_refuses_while_the_connector_is_off_and_works_when_it_is_on(kn):
    await kn.run(kn.index.sync_page, TENANT, kn.scope, kn.root)
    await off(kn.switches, CONNECTOR, NOTION)
    embedded = kn.embedder.texts_embedded

    text, is_error, result = await kn.read(search_call("tu_off"))

    assert is_error and "turned off by an admin" in text and result.citations == []
    assert kn.embedder.texts_embedded == embedded

    await kn.switches.switch_on(TENANT, CONNECTOR, NOTION)
    text, is_error, result = await kn.read(search_call("tu_on"))

    assert not is_error and result.citations and "VPN" in text


async def test_indexing_does_nothing_while_the_connector_is_off_and_prunes_nothing(kn):
    await kn.run(kn.index.sync_page, TENANT, kn.scope, kn.root)
    stored = await kn.knowledge.page_hashes(TENANT, kn.scope)
    assert stored
    await off(kn.switches, CONNECTOR, NOTION)
    calls_before = len(kn.notion.calls)

    for fn, args in (
        (kn.index.sync_page, (TENANT, kn.scope, kn.root)),
        (kn.index.prune_scope, (TENANT, kn.scope, [])),
    ):
        with pytest.raises(ApplicationError) as err:
            await kn.run(fn, *args)
        assert err.value.non_retryable and err.value.type == "ConnectorOff"
    refreshed = await kn.run(kn.index.refresh_page, TENANT, kn.root)

    assert refreshed.indexed == [] and refreshed.removed == []
    assert len(kn.notion.calls) == calls_before
    assert await kn.knowledge.page_hashes(TENANT, kn.scope) == stored

    await kn.switches.switch_on(TENANT, CONNECTOR, NOTION)
    result = await kn.run(kn.index.sync_page, TENANT, kn.scope, kn.root)

    assert not result.skipped and len(kn.notion.calls) > calls_before


async def test_a_page_event_is_dropped_while_off_and_indexed_when_back_on(kn):
    await off(kn.switches, CONNECTOR, NOTION)

    assert (await kn.run(kn.index.refresh_page, TENANT, kn.root)).indexed == []
    assert await kn.knowledge.page_hashes(TENANT, kn.scope) == {}

    await kn.switches.switch_on(TENANT, CONNECTOR, NOTION)

    assert (await kn.run(kn.index.refresh_page, TENANT, kn.root)).indexed == [kn.scope]


async def test_follow_up_polling_makes_no_notion_call_while_off_and_the_task_keeps_watching(
    harness,
):
    h = harness
    handle = await start_task(h)
    await approve(h, handle)
    await wait_state(handle, "watching")
    page_id = h.notion.all_pages(DB_ID)[0]["id"]
    await off(h.switches, CONNECTOR, NOTION)
    reads_before = h.notion.count("retrieve_page")
    h.notion.set_status(page_id, "Status", "Done")

    await h.wf_env.sleep(timedelta(minutes=16))

    assert h.notion.count("retrieve_page") == reads_before
    assert await handle.query(TaskWorkflow.state) == "watching"
    assert thread_posts(h, "was marked Done") == []

    await h.switches.switch_on(TENANT, CONNECTOR, NOTION)
    await h.wf_env.sleep(timedelta(minutes=16))

    assert await handle.result() == "done"
    assert len(thread_posts(h, "was marked Done")) == 1


async def test_a_connector_switch_does_not_reach_another_tenant(w, kn):
    await off(kn.switches, CONNECTOR, NOTION, tenant=GLOBEX)

    text, is_error, _ = await kn.read(ToolCall("tu_1", "search_items", {"query": "vpn"}))

    assert not is_error and text == "No matching items."


# --- all writes ---------------------------------------------------------------------------


def secret() -> str:
    return "signing-secret"


def sign(body: bytes, key="signing-secret") -> dict:
    import hashlib
    import hmac
    import time

    stamp = str(int(time.time()))
    base = f"v0:{stamp}:{body.decode()}".encode()
    sig = "v0=" + hmac.new(key.encode(), base, hashlib.sha256).hexdigest()
    return {
        "X-Slack-Request-Timestamp": stamp,
        "X-Slack-Signature": sig,
        "Content-Type": "application/x-www-form-urlencoded",
    }


class Gateway:
    def __init__(self, agents):
        self.temporal = FakeTemporalClient()
        self.slack = FakeSlack()
        self.switches = InMemorySwitchStore()
        self.app = create_app(
            Settings(),
            TenantsConfig.of(agents),
            self.temporal,
            InMemoryTaskLinkStore(),
            SignatureVerifier(secret()),
            slack_for=lambda tenant: self.slack,
            switches=self.switches,
        )

    async def click(self, action_id: str, task_id: str):
        payload = {
            "type": "block_actions",
            "team": {"id": TEAM},
            "user": {"id": ALICE},
            "channel": {"id": "D_ALICE"},
            "message": {"ts": "1700000000.000500"},
            "actions": [
                {"action_id": action_id, "value": json.dumps({"task_id": task_id, "nonce": "n1"})}
            ],
        }
        body = urlencode({"payload": json.dumps(payload)}).encode()
        transport = httpx.ASGITransport(app=self.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://gateway") as client:
            return await client.post("/slack/interactivity", content=body, headers=sign(body))


async def test_submit_is_refused_while_writes_are_off_and_works_when_they_are_on(agents):
    g = Gateway(agents)
    task_id = ids.task_id(TENANT, "abc")
    await off(g.switches, WRITES)

    refused = await g.click("task_approve", task_id)

    assert refused.status_code == 200
    assert g.temporal.signals == []
    [post_] = g.slack.posts()
    assert post_["text"] == "Writes are turned off by your admin; nothing was changed."
    assert post_["channel"] == "D_ALICE" and post_["thread_ts"] == "1700000000.000500"

    await g.switches.switch_on(TENANT, WRITES, "")
    await g.click("task_approve", task_id)

    assert g.temporal.signals == [(task_id, "decision", ApprovalDecision("n1", ALICE, True))]


async def test_cancel_still_works_while_writes_are_off(agents):
    g = Gateway(agents)
    task_id = ids.task_id(TENANT, "abc")
    await off(g.switches, WRITES)

    await g.click("task_cancel", task_id)

    assert g.temporal.signals == [(task_id, "decision", ApprovalDecision("n1", ALICE, False))]
    assert g.slack.posts() == []


async def test_another_tenants_writes_switch_does_not_refuse_this_tenants_submit(agents):
    g = Gateway(agents)
    task_id = ids.task_id(TENANT, "abc")
    await off(g.switches, WRITES, tenant=GLOBEX)
    await off(g.switches, ALL_AGENTS)  # agents off, writes on: Submit still goes through

    await g.click("task_approve", task_id)

    assert len(g.temporal.signals) == 1


async def test_a_write_approved_before_writes_went_off_is_refused_and_nothing_is_changed(harness):
    h = harness
    handle = await start_task(h)
    await off(h.switches, WRITES)

    await approve(h, handle)

    assert await handle.result() == "failed"
    assert h.notion.count("create_page") == 0 and h.notion.all_pages(DB_ID) == []
    [told] = thread_posts(h, "I couldn't file 'New monitor'")
    assert "Writes are turned off by your admin; nothing was changed." in told["text"]
    assert "Ask again once they are turned back on" in told["text"]
    assert [e.kind for e in h.audit.events] == ["approval", "write"]
    assert h.audit.events[1].outcome == "error"
    assert any(u["text"].startswith("Failed: Writes are turned off") for u in h.slack.updates())

    await h.switches.switch_on(TENANT, WRITES, "")
    again = await start_task(h)
    await approve(h, again)
    await wait_state(again, "watching")

    assert len(h.notion.all_pages(DB_ID)) == 1 and h.notion.count("create_page") == 1


async def test_drafting_still_works_while_writes_are_off(harness):
    h = harness
    await off(h.switches, WRITES)

    handle = await start_task(h)

    assert await handle.query(TaskWorkflow.state) == "awaiting_approval"
    assert [p for p in h.slack.posts() if p["blocks"]]  # the approval card was posted


async def test_an_approved_comment_is_refused_while_writes_are_off(harness):
    h = harness
    h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    page = h.notion.all_pages(DB_ID)[0]
    spec = TaskSpec(
        turn_ctx(),
        "add_comment",
        {
            "page_id": page["id"],
            "database_id": DB_ID,
            "item_title": "Broken VPN",
            "item_url": page["url"],
            "comment": "Still broken",
        },
        "n1",
    )
    acts = AgentActivities(
        settings=Settings(),
        registry=TenantRegistry({TENANT: runtime_for(h.agents, h.slack, h.notion)}),
        history=h.history,
        idem=h.idem,
        links=h.links,
        llm=h.llm,
        tool_outputs=h.tool_outputs,
        audit=h.audit,
        agents=h.agent_store,
        switches=h.switches,
    )
    await off(h.switches, WRITES)

    with pytest.raises(ApplicationError) as err:
        await ActivityEnvironment().run(acts.add_notion_comment, spec, "acme:task:1")

    assert err.value.non_retryable
    assert "nothing was changed" in err.value.message
    assert h.notion.count("create_comment") == 0
    assert [e.kind for e in h.audit.events] == ["approval", "write"]


async def test_a_connector_that_is_off_also_refuses_an_approved_write(harness):
    h = harness
    handle = await start_task(h)
    await off(h.switches, CONNECTOR, NOTION)

    await approve(h, handle)

    assert await handle.result() == "failed"
    assert h.notion.count("create_page") == 0
    [told] = thread_posts(h, "I couldn't file")
    assert "Notion is turned off by an admin" in told["text"]


# --- the control plane, the API and the console ---------------------------------------------


@pytest.fixture
async def h():
    harness = ConsoleHarness()
    await harness.seed_members()
    return harness


async def seed_agents(harness):
    from agentos.config import agent_definition

    sample = parse_agents_config(knowledge_agents_dict()).agents["it-helper"]
    for agent_id, owner in (("mine", OWNER), ("theirs", OTHER)):
        await harness.agents.create_agent(TENANT, agent_id, agent_definition(sample), owner)


@pytest.fixture
async def hs(h):
    await seed_agents(h)
    return h


def audit_rows(harness):
    return [(e.kind, e.agent_id, e.agent_version, e.tool, e.target) for e in harness.audit.events]


async def test_an_admin_turns_switches_off_and_on_and_each_change_is_audited(hs):
    plane = hs.plane

    assert await plane.turn_off(TENANT, ROOT, "agent", "mine")
    assert await plane.turn_off(TENANT, ROOT, "all_agents")
    assert await plane.turn_off(TENANT, ROOT, "writes")
    assert await plane.turn_off(TENANT, ROOT, "connector", "notion")
    state = await read_state(hs.switches, TENANT)
    assert state.agent_stopped_by("mine") == "agent" and state.writes_off
    assert state.connector_off("notion")
    assert await plane.turn_on(TENANT, ROOT, "agent", "mine")
    assert await plane.turn_on(TENANT, ROOT, "all_agents")

    assert audit_rows(hs) == [
        ("switch_off", "mine", None, "agent", "mine"),
        ("switch_off", None, None, "all_agents", None),
        ("switch_off", None, None, "writes", None),
        ("switch_off", None, None, "connector", "notion"),
        ("switch_on", "mine", None, "agent", "mine"),
        ("switch_on", None, None, "all_agents", None),
    ]
    assert {e.identity for e in hs.audit.events} == {f"user:{ROOT}"}
    assert {e.requester_user_id for e in hs.audit.events} == {ROOT}


async def test_turning_off_what_is_off_or_on_what_is_on_changes_and_records_nothing(hs):
    assert await hs.plane.turn_off(TENANT, ROOT, "writes")

    assert not await hs.plane.turn_off(TENANT, ROOT, "writes")
    assert not await hs.plane.turn_on(TENANT, ROOT, "all_agents")

    assert len(hs.audit.events) == 1


async def test_a_creator_and_a_stranger_may_not_turn_a_switch_off_or_on_not_even_their_own(hs):
    await hs.plane.turn_off(TENANT, ROOT, "agent", "mine")
    hs.audit.events.clear()

    for who in (OWNER, OTHER, "U_NOBODY"):
        with pytest.raises(Forbidden):
            await hs.plane.turn_off(TENANT, who, "agent", "mine")
        with pytest.raises(Forbidden):
            await hs.plane.turn_on(TENANT, who, "agent", "mine")
        with pytest.raises(Forbidden):
            await hs.plane.turn_off(TENANT, who, "writes")

    assert hs.audit.events == []
    assert (await read_state(hs.switches, TENANT)).agent_stopped_by("mine") == "agent"


async def test_an_admin_of_one_tenant_cannot_change_another_tenants_switches(hs):
    with pytest.raises(Forbidden):
        await hs.plane.turn_off("globex", "U_ADMIN2", "writes")

    assert await read_state(hs.switches, "globex") == await read_state(
        InMemorySwitchStore(), "globex"
    )


async def test_an_unknown_agent_connector_or_scope_is_refused(hs):
    with pytest.raises(AgentNotFound):
        await hs.plane.turn_off(TENANT, ROOT, "agent", "ghost")
    with pytest.raises(InvalidSwitch):
        await hs.plane.turn_off(TENANT, ROOT, "connector", "slack")
    with pytest.raises(InvalidSwitch):
        await hs.plane.turn_off(TENANT, ROOT, "everything")
    with pytest.raises(InvalidSwitch):
        await hs.plane.turn_off(TENANT, ROOT, "writes", "mine")

    assert hs.audit.events == [] and await hs.switches.list(TENANT) == []


async def test_a_creator_reads_the_state_of_their_own_agents_and_an_admin_of_all(hs):
    await hs.plane.turn_off(TENANT, ROOT, "agent", "theirs")
    await hs.plane.turn_off(TENANT, ROOT, "all_agents")

    mine = await hs.plane.switches(TENANT, OWNER)
    admin = await hs.plane.switches(TENANT, ROOT)

    assert [a.target for a in mine.agents] == ["mine"] and mine.stopped_by == {"mine": "all_agents"}
    assert [a.target for a in admin.agents] == ["mine", "theirs"]
    assert admin.stopped_by == {"mine": "all_agents", "theirs": "agent"}
    with pytest.raises(Forbidden):
        await hs.plane.switches(TENANT, "U_NOBODY")


async def api(h, user, method, path):
    async with using(await h.signed_in(user)) as client:
        return await client.request(
            method, path, headers={"Origin": h.base_url, **JSON}, content=""
        )


async def test_the_api_turns_switches_off_and_on_and_reads_them(hs):
    base = f"/tenants/{TENANT}/switches"

    paths = [
        f"{base}/agents/mine",
        f"{base}/all-agents",
        f"{base}/writes",
        f"{base}/connectors/notion",
    ]
    puts = [await api(hs, ROOT, "PUT", p) for p in paths]
    again = await api(hs, ROOT, "PUT", paths[2])
    read = await api(hs, ROOT, "GET", base)
    deletes = [await api(hs, ROOT, "DELETE", p) for p in paths]

    assert [r.status_code for r in puts + deletes] == [200] * 8
    assert puts[0].json() == {"scope": "agent", "target": "mine", "off": True, "changed": True}
    assert again.json()["changed"] is False
    body = read.json()
    assert body["writes"]["off"] and body["all_agents"]["off"]
    assert body["connectors"][0]["target"] == "notion" and body["connectors"][0]["off"]
    assert {a["target"]: a["off"] for a in body["agents"]} == {"mine": True, "theirs": False}
    assert body["writes"]["turned_off_by"] == ROOT
    assert await hs.switches.list(TENANT) == []
    assert len(hs.audit.events) == 8


async def test_the_api_refuses_a_creator_and_bad_names(hs):
    base = f"/tenants/{TENANT}/switches"

    forged = await api(hs, OWNER, "PUT", f"{base}/agents/mine")
    forged_off = await api(hs, OWNER, "DELETE", f"{base}/writes")
    unknown_agent = await api(hs, ROOT, "PUT", f"{base}/agents/ghost")
    unknown_connector = await api(hs, ROOT, "PUT", f"{base}/connectors/slack")
    creator_read = await api(hs, OWNER, "GET", base)

    assert (forged.status_code, forged_off.status_code) == (403, 403)
    assert unknown_agent.status_code == 404 and unknown_connector.status_code == 422
    assert [a["target"] for a in creator_read.json()["agents"]] == ["mine"]
    assert await hs.switches.list(TENANT) == [] and hs.audit.events == []


async def page(h, user, path):
    async with using(await h.signed_in(user)) as client:
        return await client.get(path)


def text_of(response):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", response.text))


async def test_an_admin_turns_switches_off_and_on_from_the_page(hs):
    async with using(await hs.signed_in(ROOT)) as client:
        token = await csrf_token(client)
        agent = await post(client, f"{UI}/switches/off", token, scope="agent", target="mine")
        connector = await post(
            client, f"{UI}/switches/off", token, scope="connector", target="notion"
        )
        shown = await client.get(f"{UI}/switches")
        on = await post(client, f"{UI}/switches/on", token, scope="agent", target="mine")

    assert agent.status_code == 303 and connector.status_code == 303
    assert agent.headers["location"] == f"{UI}/switches?done=switch-off"
    assert on.headers["location"] == f"{UI}/switches?done=switch-on"
    assert "Connector: notion" in shown.text and "Off" in shown.text
    assert [(s.scope, s.target) for s in await hs.switches.list(TENANT)] == [
        ("connector", "notion")
    ]
    assert [e.kind for e in hs.audit.events] == ["switch_off", "switch_off", "switch_on"]


async def test_all_agents_and_all_writes_need_the_confirm_box(hs):
    async with using(await hs.signed_in(ROOT)) as client:
        token = await csrf_token(client)
        bare = await post(client, f"{UI}/switches/off", token, scope="all_agents", target="")
        bare_writes = await post(client, f"{UI}/switches/off", token, scope="writes", target="")
        assert await hs.switches.list(TENANT) == []
        confirmed = await post(
            client, f"{UI}/switches/off", token, scope="all_agents", target="", confirm="yes"
        )

    assert bare.status_code == 422 and bare_writes.status_code == 422
    assert "Tick the box to confirm" in bare.text
    assert confirmed.status_code == 303
    assert [s.scope for s in await hs.switches.list(TENANT)] == ["all_agents"]


async def test_a_forged_switch_post_from_a_creator_is_refused_and_changes_nothing(hs):
    async with using(await hs.signed_in(OWNER)) as client:
        token = await csrf_token(client)
        forged = await post(client, f"{UI}/switches/off", token, scope="agent", target="mine")
        forged_all = await post(client, f"{UI}/switches/off", token, scope="all_agents", target="")
        forged_on = await post(client, f"{UI}/switches/on", token, scope="agent", target="mine")

    assert [r.status_code for r in (forged, forged_all, forged_on)] == [403, 403, 403]
    assert await hs.switches.list(TENANT) == [] and hs.audit.events == []


async def test_a_switch_post_without_the_csrf_token_is_refused(hs):
    async with using(await hs.signed_in(ROOT)) as client:
        refused = await post(client, f"{UI}/switches/off", None, scope="writes", target="")

    assert refused.status_code == 403 and await hs.switches.list(TENANT) == []


async def test_the_switches_page_is_for_admins_and_linked_from_their_menu(hs):
    creator = await page(hs, OWNER, f"{UI}/switches")
    admin_agents = await page(hs, ROOT, f"{UI}/agents")
    creator_agents = await page(hs, OWNER, f"{UI}/agents")

    assert creator.status_code == 403
    assert f'href="{UI}/switches"' in admin_agents.text
    assert "/switches" not in creator_agents.text


async def test_the_agent_page_shows_its_state_read_only_to_its_creator(hs):
    await hs.plane.turn_off(TENANT, ROOT, "agent", "mine")
    await hs.plane.turn_off(TENANT, ROOT, "writes")

    creator = text_of(await page(hs, OWNER, f"{UI}/agents/mine"))
    other = text_of(await page(hs, OTHER, f"{UI}/agents/theirs"))

    assert "This agent is turned off by an admin and is not answering." in creator
    assert "Writes are turned off by an admin" in creator
    assert "Turn off" not in creator and "Turn on" not in creator
    assert "turned off by an admin and is not answering" not in other

    await hs.plane.turn_on(TENANT, ROOT, "agent", "mine")
    await hs.plane.turn_off(TENANT, ROOT, "all_agents")
    all_off = text_of(await page(hs, OWNER, f"{UI}/agents/mine"))

    assert "All agents are turned off by an admin" in all_off


# --- the command line -----------------------------------------------------------------------


def test_the_command_names_one_scope_per_switch_and_everything_names_them_all():
    parse = build_parser().parse_args

    assert cli_targets(parse(["--tenant", "t", "off", "--agent", "a"])) == [(AGENT, "a")]
    assert cli_targets(parse(["--tenant", "t", "on", "--writes"])) == [(WRITES, None)]
    assert cli_targets(parse(["--tenant", "t", "off", "--connector", "notion"])) == [
        (CONNECTOR, "notion")
    ]
    assert cli_targets(parse(["--tenant", "t", "off", "--everything"])) == [
        (ALL_AGENTS, None),
        (WRITES, None),
        (CONNECTOR, "notion"),
    ]
    with pytest.raises(SystemExit):
        parse(["--tenant", "t", "off"])
    with pytest.raises(SystemExit):
        parse(["off", "--writes"])
    with pytest.raises(SystemExit):
        parse(["--tenant", "t", "off", "--writes", "--all-agents"])


async def test_the_command_turns_a_switch_off_and_on_and_audits_a_runtime_operator(hs, monkeypatch):
    monkeypatch.setattr("agentos.switches.pwd.getpwuid", lambda uid: SimpleNamespace(pw_name="ops"))
    identity = operator_identity()
    assert identity == "operator:ops"

    async def run(off_: bool, scope: str, target: str | None = None) -> bool:
        return await set_switch(
            hs.switches,
            hs.audit,
            hs.agents,
            TENANT,
            scope,
            target,
            off=off_,
            actor=identity,
            identity=identity,
        )

    assert await run(True, AGENT, "mine")
    assert not await run(True, AGENT, "mine")
    assert (await read_state(hs.switches, TENANT)).agent_stopped_by("mine") == "agent"
    assert await run(False, AGENT, "mine")

    assert audit_rows(hs) == [
        ("switch_off", "mine", None, "agent", "mine"),
        ("switch_on", "mine", None, "agent", "mine"),
    ]
    assert {(e.identity, e.requester_user_id) for e in hs.audit.events} == {
        ("operator:ops", "operator:ops")
    }
    with pytest.raises(AgentNotFound):
        await run(True, AGENT, "ghost")


def test_the_operator_is_the_account_not_a_login_variable(monkeypatch):
    """Fails if LOGNAME or USER, which anyone can set for one command, changes the recorded operator."""
    before = operator_identity()
    for name in ("LOGNAME", "USER"):
        monkeypatch.setenv(name, "forged")

    assert operator_identity() == before != "operator:forged"


async def test_the_command_does_not_read_the_tenants_table(hs, monkeypatch, capsys):
    """Fails if the command line refuses to run when the tenants table cannot be read."""

    class PoolOpened(Exception):
        pass

    async def unreadable(url):
        raise AssertionError("the command read the tenants table")

    class Pool:
        def __init__(self, *args, **kwargs):
            pass

        async def open(self):
            raise PoolOpened

    monkeypatch.setattr("agentos.tenant_store.load_stored_tenants", unreadable)
    monkeypatch.setattr("agentos.switches.AsyncConnectionPool", Pool)
    monkeypatch.setenv("AGENTOS_DATABASE_URL", "postgresql://unused")
    args = build_parser().parse_args(["--tenant", TENANT, "off", "--writes"])
    with pytest.raises(PoolOpened):
        await cli_run(args)

    assert await cli_execute(args, hs.switches, hs.audit, hs.agents) == 0
    assert "writes -: turned off" in capsys.readouterr().out
    assert (await read_state(hs.switches, TENANT)).writes_off


def test_a_target_must_fit_its_scope():
    assert check_switch(AGENT, " a ") == "a"
    assert check_switch(WRITES, None) == "" and check_switch(ALL_AGENTS, "") == ""
    assert check_switch(CONNECTOR, "notion") == "notion"
    for scope, target in ((AGENT, ""), (CONNECTOR, ""), (CONNECTOR, "slack"), (WRITES, "x")):
        with pytest.raises(InvalidSwitch):
            check_switch(scope, target)


# --- the SQL and the store ------------------------------------------------------------------

MIGRATION = (REPO / "scripts" / "migrations" / "014_kill_switches.sql").read_text()


def test_the_code_knows_the_two_new_kinds_and_they_need_no_agent_version():
    assert {"switch_off", "switch_on"} <= set(AUDIT_KINDS)
    for kind in ("switch_off", "switch_on"):
        AuditEvent("acme", "k", kind, None, None, "U1", "user:U1", tool="writes")
        AuditEvent("acme", "k", kind, "mine", None, "U1", "user:U1", tool="agent", target="mine")


def test_a_switch_row_holds_no_content_and_no_requester():
    columns = MIGRATION[MIGRATION.index("CREATE TABLE") :]
    columns = columns[: columns.index("\n);")]
    for word in ("requester", "content", "prompt", "text_body"):
        assert word not in columns


async def test_the_postgres_store_names_the_tenant_in_every_statement():
    pool = FakePool(row=(TENANT, "writes", "", ADMIN, NOV))
    store = PostgresSwitchStore(pool)

    assert await store.switch_off(TENANT, "writes", "", ADMIN, NOV)
    assert await store.switch_on(TENANT, "writes", "")
    await store.list(TENANT)

    executed = [e for e in pool.events if e[0] == "execute" and "set_config" not in e[1]]
    insert, delete, listing = executed
    assert "ON CONFLICT (tenant_id, scope, target) DO NOTHING" in insert[1]
    assert insert[2][:3] == (TENANT, "writes", "")
    assert "DELETE FROM kill_switches" in delete[1] and delete[2][0] == TENANT
    assert "tenant_id = %s" in listing[1] and listing[2] == (TENANT,)
    for _, sql, _params in executed:
        assert "tenant_id" in sql


def test_a_switch_id_is_never_built_outside_ids_py():
    # A switch needs no workflow, schedule or event key of its own: the audit key is the
    # control plane's, built in ids.py.
    assert ids.audit_control_key("acme", "switch_off", "n").startswith("acme:")
    source = (REPO / "src" / "agentos" / "switches.py").read_text()
    assert 'f"{tenant' not in source and "f'{tenant" not in source
