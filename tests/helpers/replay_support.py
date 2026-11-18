"""Recorded workflow histories: the rig that records them, and what replays them.

``scripts/record_replay_fixtures.py`` runs every scenario here against Temporal's time-skipping
server with fakes and the production encrypting data converter, and writes each history to
``tests/fixtures/replay``. ``tests/core/test_replay.py`` replays every file there under the current
workflow code, which fails with a non-determinism error when a change alters the commands an
existing history would issue.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import re
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from temporalio import workflow
from temporalio.client import Client, WorkflowHistory
from temporalio.converter import DataConverter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_store import seeded_agent_store
from agentos.audit import InMemoryAuditStore
from agentos.budgets import BudgetGuard, InMemoryBudgetStore
from agentos.config import TASK_QUEUE, Settings, parse_agents_config
from agentos.crypto_keys import LocalKeyStore
from agentos.knowledge.embedder import FakeEmbedder
from agentos.knowledge.indexing import IndexActivities
from agentos.knowledge.store import InMemoryKnowledgeStore
from agentos.llm import ScriptedLLMClient
from agentos.models import (
    ApprovalDecision,
    ConversationRef,
    EditRequest,
    InboundMessage,
    LLMResponse,
    TaskSpec,
    ToolCall,
    TurnContext,
)
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.switches import InMemorySwitchStore
from agentos.temporal_client import tenant_data_converter
from agentos.tenants import TenantRegistry, runtime_for
from agentos.usage import InMemoryUsageStore, UsageEvent
from agentos.workflows.conversation import (
    EMPTY_MESSAGE_REPLY,
    OUT_OF_STEPS,
    TURN_FAILED,
    ConversationWorkflow,
)
from agentos.workflows.indexing import IndexingWorkflow
from agentos.workflows.page_refresh import PageRefreshWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import (
    ALICE,
    BOB,
    DB_ID,
    ROOT_A,
    ROOT_B,
    TENANT,
    a_tree,
    b_tree,
    eventually,
    knowledge_agents_dict,
    make_fake_notion,
)
from tests.helpers.llm_errors import StatusError

FIXTURES = Path(__file__).parent.parent / "fixtures" / "replay"
CONVERTER = "tenant payload codec (encrypting), fixed test key"
ADMIN = "U_ADMIN"
HOP_EVENTS = 8


# --- the encrypting converter -------------------------------------------------------------


def fixture_key_store(directory: Path) -> LocalKeyStore:
    """A key store holding one fixed test key for the tenant.

    Fixtures are encrypted, so replaying them needs the key they were recorded with. It is a
    value derived from a fixed phrase, good for nothing outside these fixtures.
    """
    directory.mkdir(parents=True, exist_ok=True)
    os.chmod(directory, 0o700)
    path = LocalKeyStore(directory).path(TENANT)
    if not path.exists():
        material = hashlib.sha256(b"agentos replay fixtures").digest()
        path.write_bytes(base64.b64encode(material))
        os.chmod(path, 0o600)
    return LocalKeyStore(directory)


def converter(directory: Path):
    return tenant_data_converter(fixture_key_store(directory))


# --- one rig for every scenario -----------------------------------------------------------


def replay_agents_dict() -> dict:
    """The knowledge agents plus a front door (``concierge``) and an agent nothing routes to."""
    raw = knowledge_agents_dict()
    helper = raw["agents"]["it-helper"]
    helper["description"] = "Laptops, accounts, VPN, access and other IT requests."
    concierge = copy.deepcopy(helper)
    concierge["name"] = "Concierge"
    concierge["description"] = "General questions about the company."
    concierge["system_prompt"] = "You answer general questions."
    concierge["grants"] = {}
    concierge["specialists"] = ["it-helper"]
    raw["agents"]["concierge"] = concierge
    raw["agents"]["unlisted"] = {**copy.deepcopy(helper), "name": "Unlisted"}
    raw["default_agent"] = "concierge"
    return raw


@dataclass
class Rig:
    env: WorkflowEnvironment
    llm: ScriptedLLMClient
    slack: FakeSlack
    notion: object
    acts: AgentActivities
    switches: InMemorySwitchStore
    budgets: InMemoryBudgetStore
    usage: InMemoryUsageStore
    history: InMemoryHistoryStore

    @property
    def client(self) -> Client:
        return self.env.client


@asynccontextmanager
async def rig(key_dir: Path, *, workflows=None):
    """A time-skipping server and a worker, both using the encrypting converter."""
    cfg = parse_agents_config(replay_agents_dict())
    # Own Temporal server, not the session's: the recorded histories use the encrypting data converter.
    env = await WorkflowEnvironment.start_time_skipping(data_converter=converter(key_dir))
    notion = make_fake_notion()
    notion.add_page_tree(ROOT_A, a_tree())
    notion.add_page_tree(ROOT_B, b_tree())
    slack = FakeSlack(emails={ALICE: "alice@example.com", BOB: "bob@example.com"})
    llm = ScriptedLLMClient()
    switches, budgets, usage = InMemorySwitchStore(), InMemoryBudgetStore(), InMemoryUsageStore()
    history = InMemoryHistoryStore()
    knowledge, embedder = InMemoryKnowledgeStore(), FakeEmbedder()
    agent_store = seeded_agent_store(cfg)
    registry = TenantRegistry({TENANT: runtime_for(cfg, slack, notion)})
    acts = AgentActivities(
        settings=Settings(),
        registry=registry,
        history=history,
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=llm,
        tool_outputs=InMemoryToolOutputStore(),
        audit=InMemoryAuditStore(),
        agents=agent_store,
        knowledge=knowledge,
        embedder=embedder,
        switches=switches,
        budgets=BudgetGuard(budgets, usage, ttl_seconds=0),
    )
    acts.visibility_interval = 0
    index = IndexActivities(
        registry,
        knowledge,
        embedder,
        agent_store,
        switches=switches,
        budgets=BudgetGuard(budgets, usage, ttl_seconds=0),
    )
    worker = Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=workflows
        or [ConversationWorkflow, TaskWorkflow, IndexingWorkflow, PageRefreshWorkflow],
        activities=acts.all() + index.all(),
    )
    async with worker:
        try:
            yield Rig(env, llm, slack, notion, acts, switches, budgets, usage, history)
        finally:
            await env.shutdown()


# --- driving workflows --------------------------------------------------------------------


def answer(text: str) -> LLMResponse:
    return LLMResponse(text=text, tool_calls=[], stop_reason="end_turn")


def tool_turn(*calls: ToolCall) -> LLMResponse:
    return LLMResponse(text=None, tool_calls=list(calls), stop_reason="tool_use")


def thread_ts(n: int) -> str:
    return f"1700000000.{n:06d}"


async def start_conversation(
    r: Rig,
    agent: str,
    thread: str,
    text: str,
    *,
    channel: str = "C_GENERAL",
    kind: str = "channel",
    stored: bool = False,
    event_id: str | None = None,
):
    """Start (or signal) a conversation; ``stored`` leaves the text in the content store."""
    event_id = event_id or f"Ev-{thread}"
    if stored:
        await r.acts.content.put_inbound(
            TENANT,
            event_id,
            {
                "kind": "message",
                "agent_id": agent,
                "user_id": ALICE,
                "text": text,
                "channel": channel,
                "thread_ts": thread,
                "channel_kind": kind,
            },
        )
    msg = InboundMessage(
        event_id, ALICE, "" if stored else text, channel, thread, kind, text_stored=stored
    )
    return await r.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, agent, channel, thread),
        id=ids.conversation_id(TENANT, channel, thread),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )


async def replies(r: Rig, count: int) -> list[str]:
    await eventually(lambda: len(r.slack.updates()) >= count, timeout=30)
    return [u["text"] for u in r.slack.updates()]


async def answered(r: Rig) -> None:
    """Wait for the one thing a refused or stopped turn says: an update or a post."""

    def said() -> bool:
        posts = [p for p in r.slack.posts() if p["text"] != "Thinking…"]
        return bool(r.slack.updates() or posts)

    await eventually(said, timeout=30)


def said_text(r: Rig) -> str:
    return " ".join(
        [u["text"] for u in r.slack.updates()]
        + [p["text"] for p in r.slack.posts() if p["text"] != "Thinking…"]
    )


def legacy_spec(thread: str = thread_ts(1)) -> TaskSpec:
    """A task as it was recorded before drafts were stored: the payload rides in the spec."""
    ctx = TurnContext(
        tenant_id=TENANT,
        agent_id="it-helper",
        agent_version=1,
        requester_user_id=ALICE,
        channel="C_GENERAL",
        thread_ts=thread,
        channel_kind="channel",
    )
    return TaskSpec(
        ctx=ctx,
        tool_name="create_item",
        payload={"title": "New monitor", "details": "Mine flickers.", "database_id": DB_ID},
        approval_nonce="nonce-1",
    )


async def start_legacy_task(r: Rig, thread: str):
    handle = await r.client.start_workflow(
        TaskWorkflow.run,
        legacy_spec(thread),
        id=ids.task_id(TENANT, f"replay-{thread}"),
        task_queue=TASK_QUEUE,
    )
    await wait_state(handle, "awaiting_approval")
    return handle


async def wait_state(handle, expected: str) -> None:
    async def check():
        return await handle.query(TaskWorkflow.state) == expected

    await eventually(check, timeout=30)


async def stored_task(r: Rig, thread: str, text: str = "please file a ticket"):
    """A conversation whose model asks for a task; returns (task handle, its card's nonce)."""
    r.llm.queue(
        tool_turn(
            ToolCall("tu_c", "create_item", {"title": "New monitor", "details": "Flickers."})
        ),
        answer("Sent you a card."),
    )
    await start_conversation(r, "it-helper", thread, text, stored=True)
    cards = await eventually(lambda: [p for p in r.slack.posts() if p["blocks"]], timeout=30)
    actions = next(b for b in cards[0]["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    handle = r.client.get_workflow_handle(value["task_id"])
    await wait_state(handle, "awaiting_approval")
    return handle, value["nonce"]


async def approve(handle, nonce: str, approved: bool = True) -> None:
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, approved))


async def finish_watch(r: Rig, handle) -> None:
    """Mark the filed item Done and let the next poll find it."""
    page = r.notion.all_pages(DB_ID)[0]["id"]
    r.notion.set_status(page, "Status", "Done")
    await r.env.sleep(timedelta(minutes=15))
    assert await handle.result() == "done"


@contextmanager
def suggest_continue_as_new_after(events: int):
    """Make Temporal suggest a continue-as-new once a run's history has ``events`` events.

    The time-skipping server never makes the suggestion itself. The workflow's own call is
    ``workflow.info().is_continue_as_new_suggested()``; this stands in for the server.
    """
    original = workflow.Info.is_continue_as_new_suggested

    def suggested(self) -> bool:
        return self.get_current_history_length() >= events

    workflow.Info.is_continue_as_new_suggested = suggested
    try:
        yield
    finally:
        workflow.Info.is_continue_as_new_suggested = original


async def hop_chain(client: Client, workflow_id: str, first_run_id: str) -> list[WorkflowHistory]:
    """The history of every run of a workflow that continued as new, oldest first."""
    chain = []
    run_id = first_run_id
    while run_id:
        history = await client.get_workflow_handle(workflow_id, run_id=run_id).fetch_history()
        chain.append(history)
        last = history.events[-1]
        run_id = (
            last.workflow_execution_continued_as_new_event_attributes.new_execution_run_id
            if last.HasField("workflow_execution_continued_as_new_event_attributes")
            else ""
        )
    return chain


# --- scenarios ----------------------------------------------------------------------------


@dataclass
class Recorded:
    """One history to write: the workflow it belongs to and the events."""

    name: str
    history: WorkflowHistory
    # The server makes the suggestion to continue as new from a long history; the test server
    # never does, so a recording that hops notes the length it was made to suggest at.
    suggested_at: int = 0


Scenario = Callable[[Rig], Awaitable[list[Recorded]]]


async def current(r: Rig, name: str, workflow_id: str) -> Recorded:
    history = await r.client.get_workflow_handle(workflow_id).fetch_history()
    return Recorded(name, history)


async def dm_answer(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(2 if stored else 1)
    r.llm.queue(answer("Hello from the helper."))
    await start_conversation(r, "it-helper", thread, "hi", channel="D1", kind="im", stored=stored)
    await replies(r, 1)
    conv = ids.conversation_id(TENANT, "D1", thread)
    return [await current(r, "", conv)]


async def hand_off(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(12 if stored else 11)
    r.llm.queue(
        tool_turn(ToolCall("tu_h", "hand_off", {"agent": "it-helper"})),
        answer("As the specialist."),
    )
    await start_conversation(r, "concierge", thread, "my laptop battery swells", stored=stored)
    await replies(r, 2)
    return [await current(r, "", ids.conversation_id(TENANT, "C_GENERAL", thread))]


async def hand_off_notice(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(132 if stored else 131)
    r.llm.queue(
        tool_turn(ToolCall("tu_h", "hand_off", {"agent": "it-helper"})),
        answer("As the specialist."),
    )
    await start_conversation(r, "concierge", thread, "my laptop battery swells", stored=stored)
    await replies(r, 2)
    notices = await eventually(
        lambda: [e for e in r.acts.audit.events if e.kind == "notice"], timeout=30
    )
    assert [e.tool for e in notices] == ["hand_off"]
    return [await current(r, "", ids.conversation_id(TENANT, "C_GENERAL", thread))]


async def knowledge_answer(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(22 if stored else 21)
    scope = ids.knowledge_scope(ROOT_A)
    await r.client.execute_workflow(
        IndexingWorkflow.run,
        args=[TENANT, scope, ROOT_A.replace("-", "")],
        id=ids.index_workflow_id(TENANT, scope),
        task_queue=TASK_QUEUE,
    )
    r.llm.queue(
        tool_turn(
            ToolCall("tu_s", "search_knowledge", {"query": "reset your vpn password"}),
        ),
        answer("Use the portal [1]."),
    )
    await start_conversation(r, "it-helper", thread, "how do I reset my vpn?", stored=stored)
    await replies(r, 1)
    return [await current(r, "", ids.conversation_id(TENANT, "C_GENERAL", thread))]


async def knowledge_two_searches(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(24 if stored else 23)
    scope = ids.knowledge_scope(ROOT_A)
    await r.client.execute_workflow(
        IndexingWorkflow.run,
        args=[TENANT, scope, ROOT_A.replace("-", "")],
        id=ids.index_workflow_id(TENANT, scope),
        task_queue=TASK_QUEUE,
    )
    r.llm.queue(
        tool_turn(
            ToolCall("tu_s1", "search_knowledge", {"query": "reset your vpn password"}),
            ToolCall("tu_s2", "search_knowledge", {"query": "laptops replaced"}),
        ),
        answer("The portal [1] and the laptop policy [2]."),
    )
    await start_conversation(r, "it-helper", thread, "vpn reset and laptop policy?", stored=stored)
    await replies(r, 1)
    conv = ids.conversation_id(TENANT, "C_GENERAL", thread)
    outputs = await r.acts.tool_outputs.get_many(TENANT, conv, ["tu_s1", "tu_s2"])
    markers = [
        {int(n) for n in re.findall(r'<source n="(\d+)"', outputs[t][0])}
        for t in ("tu_s1", "tu_s2")
    ]
    assert all(markers) and markers[0].isdisjoint(markers[1])
    union = markers[0] | markers[1]
    assert union == set(range(1, len(union) + 1))
    return [await current(r, "", conv)]


async def audience_refusal(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(32 if stored else 31)
    await start_conversation(r, "unlisted", thread, "hello", stored=stored)
    await answered(r)
    assert "isn't available" in said_text(r)
    return [await current(r, "", ids.conversation_id(TENANT, "C_GENERAL", thread))]


async def failed_turn(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(112 if stored else 111)
    await start_conversation(r, "gone", thread, "hello", stored=stored)
    await answered(r)
    assert TURN_FAILED in said_text(r)
    if stored:
        # The discard runs after the failure notice; the history must hold it.
        async def consumed() -> bool:
            row = await r.acts.content.get_inbound(TENANT, f"Ev-{thread}")
            return row is not None and bool(row.get("consumed"))

        await eventually(consumed, timeout=30)
    return [await current(r, "", ids.conversation_id(TENANT, "C_GENERAL", thread))]


async def step_limit_notice(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(122 if stored else 121)
    r.llm.queue(
        *[tool_turn(ToolCall(f"tu_{n}", "search_items", {"query": "laptop"})) for n in range(6)]
    )
    await start_conversation(r, "it-helper", thread, "keep looking", stored=stored)
    await replies(r, 1)
    assert OUT_OF_STEPS in said_text(r)
    await eventually(lambda: [e for e in r.acts.audit.events if e.kind == "notice"], timeout=30)
    return [await current(r, "", ids.conversation_id(TENANT, "C_GENERAL", thread))]


async def failure_notice(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(142 if stored else 141)
    r.llm.queue(StatusError(401))
    handle = await start_conversation(r, "it-helper", thread, "hello", stored=stored)
    await replies(r, 1)
    assert TURN_FAILED in said_text(r)
    conv = ids.conversation_id(TENANT, "C_GENERAL", thread)
    event_id = f"Ev-{thread}-2"
    if stored:
        await r.acts.content.put_inbound(
            TENANT,
            event_id,
            {
                "kind": "message",
                "agent_id": "it-helper",
                "user_id": ALICE,
                "text": "",
                "channel": "C_GENERAL",
                "thread_ts": thread,
                "channel_kind": "channel",
            },
        )
    await handle.signal(
        ConversationWorkflow.message,
        InboundMessage(event_id, ALICE, "", "C_GENERAL", thread, "channel", text_stored=stored),
    )
    await eventually(
        lambda: [p for p in r.slack.posts() if p["text"] == EMPTY_MESSAGE_REPLY], timeout=30
    )

    async def two_notices():
        found = [e for e in r.acts.audit.events if e.kind == "notice"]
        return found if len(found) == 2 else None

    notices = await eventually(two_notices, timeout=30)
    assert [e.tool for e in notices] == ["failure", "empty_message"]
    return [await current(r, "", conv)]


async def failed_turn_notice(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(152 if stored else 151)
    await start_conversation(r, "gone", thread, "hello", stored=stored)
    await answered(r)
    assert TURN_FAILED in said_text(r)
    if stored:

        async def consumed() -> bool:
            row = await r.acts.content.get_inbound(TENANT, f"Ev-{thread}")
            return row is not None and bool(row.get("consumed"))

        await eventually(consumed, timeout=30)
    notices = await eventually(
        lambda: [e for e in r.acts.audit.events if e.kind == "notice"], timeout=30
    )
    assert [e.tool for e in notices] == ["failure"]
    return [await current(r, "", ids.conversation_id(TENANT, "C_GENERAL", thread))]


async def budget_stop(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(42 if stored else 41)
    await r.budgets.set_budget(TENANT, "hr-helper", Decimal(2), ADMIN)
    await r.usage.record(
        UsageEvent(
            tenant_id=TENANT,
            usage_key=f"spent-{thread}",
            kind="llm",
            agent_id="hr-helper",
            agent_version=1,
            model="m",
            cost=Decimal(2),
            at=datetime.now(UTC),
        )
    )
    await start_conversation(
        r, "hr-helper", thread, "hello", channel="D2", kind="im", stored=stored
    )
    await answered(r)
    assert "monthly budget" in said_text(r)
    return [await current(r, "", ids.conversation_id(TENANT, "D2", thread))]


async def switch_off(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(52 if stored else 51)
    assert await r.switches.switch_off(TENANT, "agent", "plain", ADMIN, datetime.now(UTC))
    await start_conversation(r, "plain", thread, "hello", stored=stored)
    await answered(r)
    assert "turned off" in said_text(r)
    return [await current(r, "", ids.conversation_id(TENANT, "C_GENERAL", thread))]


async def conversation_hop(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(62 if stored else 61)
    r.llm.queue(answer("First answer."), answer("Second answer."))
    with suggest_continue_as_new_after(HOP_EVENTS):
        handle = await start_conversation(r, "it-helper", thread, "one", stored=stored)
        await replies(r, 1)
        conv = ids.conversation_id(TENANT, "C_GENERAL", thread)
        text = "two"
        event_id = f"Ev-{thread}-2"
        if stored:
            await r.acts.content.put_inbound(
                TENANT,
                event_id,
                {
                    "kind": "message",
                    "agent_id": "it-helper",
                    "user_id": ALICE,
                    "text": text,
                    "channel": "C_GENERAL",
                    "thread_ts": thread,
                    "channel_kind": "channel",
                },
            )
        await r.client.get_workflow_handle(conv).signal(
            ConversationWorkflow.message,
            InboundMessage(
                event_id,
                ALICE,
                "" if stored else text,
                "C_GENERAL",
                thread,
                "channel",
                text_stored=stored,
            ),
        )
        await replies(r, 2)
    chain = await hop_chain(r.client, conv, handle.result_run_id)
    assert len(chain) >= 2, "the conversation never continued as new"
    return [Recorded(f"run{i + 1}", h, HOP_EVENTS) for i, h in enumerate(chain)]


async def task_with_approval(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(72 if stored else 71)
    if stored:
        handle, nonce = await stored_task(r, thread)
    else:
        handle = await start_legacy_task(r, thread)
        nonce = "nonce-1"
    await approve(handle, nonce)
    await wait_state(handle, "watching")
    out = [await current(r, "", handle.id)]
    if stored:
        out.append(
            await current(r, "conversation", ids.conversation_id(TENANT, "C_GENERAL", thread))
        )
    return out


async def task_with_edit(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(82 if stored else 81)
    if stored:
        handle, nonce = await stored_task(r, thread)
        edit_id = ids.edit_event_id(f"replay{thread[-3:]}")
        await r.acts.content.put_inbound(
            TENANT,
            edit_id,
            {
                "kind": "edit",
                "task_id": handle.id,
                "fields": {"title": "Edited title", "details": "Edited details."},
            },
            delivered=True,
        )
        await handle.signal(TaskWorkflow.edit, EditRequest(nonce, ALICE, {}, edit_id))
    else:
        handle = await start_legacy_task(r, thread)
        nonce = "nonce-1"
        await handle.signal(
            TaskWorkflow.edit,
            EditRequest(nonce, ALICE, {"title": "Edited title", "details": "Edited details."}),
        )

    async def new_nonce():
        view = await handle.query(TaskWorkflow.draft)
        return view.nonce != nonce and view.nonce

    fresh = await eventually(new_nonce, timeout=30)
    await approve(handle, fresh)
    await wait_state(handle, "watching")
    out = [await current(r, "", handle.id)]
    if stored:
        out.append(
            await current(r, "conversation", ids.conversation_id(TENANT, "C_GENERAL", thread))
        )
    return out


async def task_declined(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(162 if stored else 161)
    if stored:
        handle, nonce = await stored_task(r, thread)
    else:
        handle = await start_legacy_task(r, thread)
        nonce = "nonce-1"
    await approve(handle, nonce, approved=False)
    assert await handle.result() == "cancelled"
    return [await current(r, "", handle.id)]


async def task_expired(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(172 if stored else 171)
    if stored:
        handle, _nonce = await stored_task(r, thread)
    else:
        handle = await start_legacy_task(r, thread)
    # Advance past the 72-hour expiry so the task closes; then result() returns at once.
    await r.env.sleep(timedelta(hours=73))
    assert await handle.result() == "expired"
    return [await current(r, "", handle.id)]


async def watch_done(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(92 if stored else 91)
    if stored:
        handle, nonce = await stored_task(r, thread)
    else:
        handle = await start_legacy_task(r, thread)
        nonce = "nonce-1"
    await approve(handle, nonce)
    await wait_state(handle, "watching")
    await finish_watch(r, handle)
    return [await current(r, "", handle.id)]


async def task_hop(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(102 if stored else 101)
    if stored:
        handle, nonce = await stored_task(r, thread)
    else:
        handle = await start_legacy_task(r, thread)
        nonce = "nonce-1"
    first_run = handle.result_run_id or (await handle.describe()).run_id
    await approve(handle, nonce)
    await wait_state(handle, "watching")
    await r.env.sleep(timedelta(days=2))
    await finish_watch(r, handle)
    chain = await hop_chain(r.client, handle.id, first_run)
    assert len(chain) >= 2, "the task never continued as new"
    return [Recorded(f"run{i + 1}", h) for i, h in enumerate(chain)]


def _both(fn):
    return {
        False: lambda r: fn(r, False),
        True: lambda r: fn(r, True),
    }


# name -> scenario; "_stored" is the shape a message and a task have now, the other the one
# recorded before content moved to Postgres (text in the signal, the payload in the spec).
SCENARIOS: dict[str, Scenario] = {}
for _name, _fn in {
    "dm_answer": dm_answer,
    "hand_off": hand_off,
    "hand_off_notice": hand_off_notice,
    "knowledge_answer": knowledge_answer,
    "knowledge_two_searches": knowledge_two_searches,
    "audience_refusal": audience_refusal,
    "failed_turn": failed_turn,
    "step_limit_notice": step_limit_notice,
    "failure_notice": failure_notice,
    "failed_turn_notice": failed_turn_notice,
    "budget_stop": budget_stop,
    "switch_off": switch_off,
    "conversation_hop": conversation_hop,
    "task_with_approval": task_with_approval,
    "task_with_edit": task_with_edit,
    "task_declined": task_declined,
    "task_expired": task_expired,
    "watch_done": watch_done,
    "task_hop": task_hop,
}.items():
    for _stored, _run in _both(_fn).items():
        SCENARIOS[f"{_name}{'_stored' if _stored else '_inline'}"] = _run


# --- files --------------------------------------------------------------------------------


def write_fixture(directory: Path, scenario: str, rec: Recorded, commit: str) -> Path:
    stem = scenario + (f".{rec.name}" if rec.name else "")
    events = rec.history.to_json_dict()["events"]
    first = events[0]["workflowExecutionStartedEventAttributes"]
    body = {
        "recorded": {
            "commit": commit,
            "date": datetime.now(UTC).date().isoformat(),
            "converter": CONVERTER,
            "scenario": scenario,
            "suggested_at": rec.suggested_at,
        },
        "workflow_id": rec.history.workflow_id,
        "workflow_type": first["workflowType"]["name"],
        "events": events,
    }
    path = directory / f"{stem}.json"
    path.write_text(json.dumps(body, indent=2) + "\n")
    return path


def load_fixture(path: Path) -> tuple[WorkflowHistory, dict]:
    """A fixture's history and its header. An older file has no header and a plain history."""
    raw = json.loads(path.read_text())
    header = raw.get("recorded")
    workflow_id = raw.get("workflow_id", f"acme:conv:{path.stem}")
    return WorkflowHistory.from_json(workflow_id, raw), header or {}


async def replay(path: Path, key_dir: Path):
    """Replay one fixture on the current workflow code; raises on non-determinism."""
    history, header = load_fixture(path)
    replayer = Replayer(
        workflows=[ConversationWorkflow, TaskWorkflow],
        # An old file was recorded with the default converter; a new one with the encrypting one.
        data_converter=converter(key_dir) if header else DataConverter.default,
    )
    with suggest_continue_as_new_after(header.get("suggested_at") or 1 << 60):
        await replayer.replay_workflow(history)


async def replay_all(directory: Path, key_dir: Path) -> list[str]:
    """Replay every fixture; return the ones that failed (empty when all pass)."""
    failed = []
    for path in sorted(directory.glob("*.json")):
        try:
            await replay(path, key_dir)
        except Exception as err:  # noqa: BLE001 - every kind of replay failure is reported
            failed.append(f"{path.name}: {err}")
    return failed
