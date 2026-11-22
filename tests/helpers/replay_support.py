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
import dataclasses
import hashlib
import json
import os
import re
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from slack_sdk.errors import SlackApiError
from slack_sdk.web.slack_response import SlackResponse
from temporalio import workflow
from temporalio.client import Client, WorkflowHistory
from temporalio.converter import DataConverter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_health import (
    AVAILABLE,
    UNAVAILABLE,
    AgentHealthChecker,
    InMemoryAgentHealthStore,
)
from agentos.agent_store import seeded_agent_store
from agentos.approval_policy import NO_APPROVAL
from agentos.audit import InMemoryAuditStore, canonical_sha256
from agentos.budgets import BudgetGuard, InMemoryBudgetStore
from agentos.config import TASK_QUEUE, Settings, parse_agents_config
from agentos.connectors.drive import DriveReader
from agentos.connectors.notion import normalize_id
from agentos.creator_access import CreatorAccessActivities, InMemoryCreatorAccessStore
from agentos.credentials import InMemoryCredentialStore, TenantCredentials
from agentos.crypto_keys import LocalKeyStore
from agentos.drive_connections import InMemoryDriveConnectionStore
from agentos.grant_picks import InMemoryGrantPickStore, PickedObject
from agentos.knowledge.drive_indexing import DriveIndexActivities
from agentos.knowledge.embedder import FakeEmbedder
from agentos.knowledge.indexing import IndexActivities
from agentos.knowledge.store import InMemoryKnowledgeStore
from agentos.llm import ScriptedLLMClient
from agentos.members import InMemoryMemberStore
from agentos.models import (
    ApprovalDecision,
    ConversationRef,
    EditRequest,
    InboundMessage,
    LLMResponse,
    NudgeRequest,
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
from agentos.test_sets import (
    CITE,
    FAILED,
    REFUSE,
    RUNNING,
    WRITE,
    TestCase,
    TestRun,
    case_set_sha256,
)
from agentos.usage import InMemoryUsageStore, PriceEntry, Prices, UsageEvent, UsageRecorder
from agentos.workflows.conversation import (
    EMPTY_MESSAGE_REPLY,
    OUT_OF_STEPS,
    PROGRESS_AFTER,
    PROGRESS_GAP,
    TURN_FAILED,
    ConversationWorkflow,
)
from agentos.workflows.creator_access import CreatorAccessWorkflow
from agentos.workflows.drive_indexing import DriveIndexingWorkflow
from agentos.workflows.indexing import IndexingWorkflow
from agentos.workflows.page_refresh import PageRefreshWorkflow
from agentos.workflows.task import TaskWorkflow
from agentos.workflows.test_run import TestRunWorkflow
from tests.conftest import (
    ALICE,
    BOB,
    BOB_NOTION,
    DB_ID,
    PAGE_A1,
    ROOT_A,
    ROOT_B,
    TENANT,
    a_tree,
    b_tree,
    default_schema,
    eventually,
    knowledge_agents_dict,
    make_fake_notion,
)
from tests.helpers.agents import republish
from tests.helpers.drive import (
    DRIVE_DOC,
    DRIVE_FOLDER,
    DRIVE_SECTION,
    DRIVE_SHEET,
    DRIVE_TEMPLATE,
    SHEET_TAB,
    TEMPLATE_FOLDER,
    add_roster_sheet,
    add_section_doc,
    add_template_tree,
    drive_account_email,
    make_drive_google,
    section_grant_block,
    sheet_grant_block,
    template_grant_block,
)
from tests.helpers.health import FlipsAfterReads
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
    picks: InMemoryGrantPickStore
    credentials: TenantCredentials
    access: InMemoryCreatorAccessStore
    # The creator's own Notion, separate from the tenant integration's: the nightly check
    # reads with it, so a scenario can lose the creator's access while the grants pass.
    creator_notion: object
    health: InMemoryAgentHealthStore
    google: object
    drive_connections: InMemoryDriveConnectionStore

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
    google = make_drive_google()
    drive_connections = InMemoryDriveConnectionStore()
    registry = TenantRegistry(
        {
            TENANT: runtime_for(
                cfg, slack, notion, drive=DriveReader(google, drive_account_email(google))
            )
        }
    )
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
    picks = InMemoryGrantPickStore()
    access = InMemoryCreatorAccessStore()
    credentials = TenantCredentials(InMemoryCredentialStore(), fixture_key_store(key_dir))
    health = InMemoryAgentHealthStore()
    health_checker = AgentHealthChecker(
        registry,
        agent_store,
        health,
        acts.audit,
        acts.idem,
        InMemoryMemberStore(),
        creator_access=access,
    )
    creator_notion = make_fake_notion()
    creator_notion.add_page_tree(ROOT_A, a_tree())
    creator_notion.add_page_tree(ROOT_B, b_tree())
    creator_access_acts = CreatorAccessActivities(
        registry,
        agent_store,
        picks,
        credentials,
        access,
        health_checker,
        client_for=lambda _token, _tenant: creator_notion,
    )
    drive_index = DriveIndexActivities(
        registry,
        knowledge,
        embedder,
        agent_store,
        google,
        drive_connections,
        switches=switches,
        budgets=BudgetGuard(budgets, usage, ttl_seconds=0),
    )
    worker = Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=workflows
        or [
            ConversationWorkflow,
            TaskWorkflow,
            IndexingWorkflow,
            PageRefreshWorkflow,
            CreatorAccessWorkflow,
            TestRunWorkflow,
            DriveIndexingWorkflow,
        ],
        activities=acts.all() + index.all() + creator_access_acts.all() + drive_index.all(),
    )
    async with worker:
        try:
            yield Rig(
                env,
                llm,
                slack,
                notion,
                acts,
                switches,
                budgets,
                usage,
                history,
                picks,
                credentials,
                access,
                creator_notion,
                health,
                google,
                drive_connections,
            )
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


async def knowledge_uncited(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(26 if stored else 25)
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
        answer("Try the steps in the guidebook."),
    )
    await start_conversation(r, "it-helper", thread, "how do I reset my vpn?", stored=stored)
    await replies(r, 1)
    # The answer cites none of the passages the search returned, so the reply names the
    # page it searched instead of leaving the user without a source.
    meta = await r.acts.knowledge.page_meta(TENANT, [scope], [ROOT_A.replace("-", "")])
    title, url = meta[ROOT_A.replace("-", "")]
    assert (
        r.slack.updates()[0]["text"]
        == f"Try the steps in the guidebook.\n\nSearched: <{url}|{title}>"
    )
    return [await current(r, "", ids.conversation_id(TENANT, "C_GENERAL", thread))]


async def knowledge_uncited_dropped(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(206 if stored else 205)
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
        answer("Try the steps in the guidebook."),
    )
    # The search runs long enough for a progress edit and both attempts of that edit are
    # refused, so the uncited answer is posted as a new message that still names the page.
    read_hang = r.notion.hang_next_read("retrieve_page")
    r.slack.fail_next_update(ValueError("slack down"), times=2)
    await start_conversation(r, "it-helper", thread, "how do I reset my vpn?", stored=stored)
    await read_hang.reached.wait()
    await r.env.sleep(PROGRESS_AFTER + timedelta(seconds=2))
    read_hang.release()
    await r.env.sleep(timedelta(seconds=1))
    await eventually(lambda: [p for p in r.slack.posts() if "Searched:" in p["text"]], timeout=30)
    assert not r.slack.updates()
    return [await current(r, "", ids.conversation_id(TENANT, "C_GENERAL", thread))]


async def audience_refusal(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(32 if stored else 31)
    await start_conversation(r, "unlisted", thread, "hello", stored=stored)
    await answered(r)
    assert "isn't available" in said_text(r)
    return [await current(r, "", ids.conversation_id(TENANT, "C_GENERAL", thread))]


async def hand_off_refused(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(182 if stored else 181)
    specialist = await republish(
        r.acts.agents,
        "it-helper",
        lambda definition: definition.update(audience={"dm": "everyone"}),
    )
    r.llm.queue(
        tool_turn(ToolCall("tu_h", "hand_off", {"agent": "it-helper"})),
        answer("I'll handle it myself."),
    )
    await start_conversation(r, "concierge", thread, "my laptop battery swells", stored=stored)
    await replies(r, 1)
    refusals = [e for e in r.acts.audit.events if e.kind == "audience_refused"]
    assert [(e.agent_id, e.agent_version, e.target) for e in refusals] == [
        ("it-helper", specialist.version, "concierge")
    ]
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


async def task_recorded(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(222 if stored else 221)
    if stored:
        handle, nonce = await stored_task(r, thread)
    else:
        handle = await start_legacy_task(r, thread)
        nonce = "nonce-1"
    await approve(handle, nonce)
    await wait_state(handle, "watching")
    await finish_watch(r, handle)
    row = await r.acts.tasks.get(TENANT, handle.id)
    assert (row.state, row.last_status, row.title) == ("done", "Done", "New monitor")
    return [await current(r, "", handle.id)]


async def my_tasks(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(232 if stored else 231)
    if stored:
        handle, nonce = await stored_task(r, thread)
    else:
        handle = await start_legacy_task(r, thread)
        nonce = "nonce-1"
    await approve(handle, nonce)
    await wait_state(handle, "watching")
    # The first poll lands, so the poke the question sends must wait out POKE_GAP.
    await r.env.sleep(timedelta(minutes=16))
    page = r.notion.all_pages(DB_ID)[0]["id"]
    r.notion.set_status(page, "Status", "In progress")
    r.llm.queue(
        tool_turn(ToolCall("tu_mt", "my_tasks", {})),
        answer("It is in progress."),
    )
    conv = ids.conversation_id(TENANT, "C_GENERAL", thread)
    text = "where is my request?"
    event_id = f"Ev-{thread}-2"
    if stored:
        # The thread's conversation already ran the turn that started the task.
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
            InboundMessage(event_id, ALICE, "", "C_GENERAL", thread, "channel", text_stored=True),
        )
    else:
        await start_conversation(r, "it-helper", thread, text, event_id=event_id)
    # Skip the gap in short steps so the turn's own steps run between them; the wait
    # inside the tool sees the poke-answered poll's report land.
    expected = 4 if stored else 2
    for _ in range(30):
        if len(r.llm.calls) >= expected:
            break
        await r.env.sleep(timedelta(seconds=10))
    await eventually(lambda: len(r.llm.calls) >= expected, timeout=30)
    outputs = await r.acts.tool_outputs.get_many(TENANT, conv, ["tu_mt"])
    assert 'state="watching"' in outputs["tu_mt"][0]
    assert 'status="In progress"' in outputs["tu_mt"][0]
    row = await r.acts.tasks.get(TENANT, handle.id)
    assert row.last_status == "In progress"
    return [await current(r, "", handle.id), await current(r, "conversation", conv)]


async def my_tasks_once(r: Rig, stored: bool) -> list[Recorded]:
    """Two my_tasks calls in one turn refresh once; a question right after the report
    finds the task fresh and sends no poke. Records the once-per-turn patch's path."""
    thread = thread_ts(242 if stored else 241)
    if stored:
        handle, nonce = await stored_task(r, thread)
    else:
        handle = await start_legacy_task(r, thread)
        nonce = "nonce-1"
    await approve(handle, nonce)
    await wait_state(handle, "watching")
    # The first poll lands, so the record the question finds is the watching report,
    # stale by more than POKE_GAP: the turn's first call pokes and waits.
    await r.env.sleep(timedelta(minutes=16))
    page = r.notion.all_pages(DB_ID)[0]["id"]
    r.notion.set_status(page, "Status", "In progress")
    r.llm.queue(
        tool_turn(ToolCall("tu_m1", "my_tasks", {})),
        tool_turn(ToolCall("tu_m2", "my_tasks", {})),
        answer("Both checked."),
        tool_turn(ToolCall("tu_m3", "my_tasks", {})),
        answer("Still in progress."),
    )
    conv = ids.conversation_id(TENANT, "C_GENERAL", thread)
    expected = 5 if stored else 3
    # No virtual time may pass on its own between the turns, or the next scheduled
    # poll would make the second question's record stale again.
    with r.env.auto_time_skipping_disabled():
        event_id = f"Ev-{thread}-2"
        if stored:
            await r.acts.content.put_inbound(
                TENANT,
                event_id,
                {
                    "kind": "message",
                    "agent_id": "it-helper",
                    "user_id": ALICE,
                    "text": "where is my request?",
                    "channel": "C_GENERAL",
                    "thread_ts": thread,
                    "channel_kind": "channel",
                },
            )
            await r.client.get_workflow_handle(conv).signal(
                ConversationWorkflow.message,
                InboundMessage(
                    event_id, ALICE, "", "C_GENERAL", thread, "channel", text_stored=True
                ),
            )
        else:
            await start_conversation(
                r, "it-helper", thread, "where is my request?", event_id=event_id
            )
        for _ in range(30):
            if len(r.llm.calls) >= expected:
                break
            await r.env.sleep(timedelta(seconds=10))
        await eventually(lambda: len(r.llm.calls) >= expected, timeout=30)
        outputs = await r.acts.tool_outputs.get_many(TENANT, conv, ["tu_m1", "tu_m2"])
        assert 'status="In progress"' in outputs["tu_m1"][0]
        assert 'status="In progress"' in outputs["tu_m2"][0]
        polls = r.notion.count("retrieve_page")

        event_id = f"Ev-{thread}-3"
        if stored:
            await r.acts.content.put_inbound(
                TENANT,
                event_id,
                {
                    "kind": "message",
                    "agent_id": "it-helper",
                    "user_id": ALICE,
                    "text": "and now?",
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
                "" if stored else "and now?",
                "C_GENERAL",
                thread,
                "channel",
                text_stored=stored,
            ),
        )
        await eventually(lambda: len(r.llm.calls) >= expected + 2, timeout=30)
        outputs = await r.acts.tool_outputs.get_many(TENANT, conv, ["tu_m3"])
        assert 'status="In progress"' in outputs["tu_m3"][0]
        assert r.notion.count("retrieve_page") == polls
    row = await r.acts.tasks.get(TENANT, handle.id)
    assert row.last_status == "In progress"
    return [await current(r, "", handle.id), await current(r, "conversation", conv)]


def _nudge_grant(definition: dict) -> None:
    db = definition["grants"]["notion"]["databases"][0]
    db["properties"]["assignee"] = "Assignee"
    db["property_ids"]["assignee"] = "asg1"


async def _stale_nudge_task(r: Rig, stored: bool, thread: str, *, dm: bool):
    """A watching task gone stale, its nudge approved and run; the clock restarts.

    With ``dm`` the grant maps an assignee who matches a Slack user, so the nudge
    comments with a mention and sends a DM; without it the nudge is a comment alone.
    """
    if dm:
        r.notion.set_schema(
            DB_ID, {**default_schema(), "Assignee": {"id": "asg1", "type": "people"}}
        )
        await republish(r.acts.agents, "it-helper", _nudge_grant)
    if stored:
        handle, nonce = await stored_task(r, thread)
    else:
        handle = await start_legacy_task(r, thread)
        nonce = "nonce-1"
    await approve(handle, nonce)
    await wait_state(handle, "watching")
    page = r.notion.all_pages(DB_ID)[0]
    if dm:
        r.notion.set_people(page["id"], "Assignee", [BOB_NOTION])
    await r.env.sleep(timedelta(days=4))
    stale = await eventually(
        lambda: [p for p in r.slack.posts() if p["thread_ts"] and "No movement" in p["text"]],
        timeout=30,
    )
    button = next(b for b in stale[-1]["blocks"] if b["type"] == "actions")
    value = json.loads(button["elements"][0]["value"])
    await handle.signal(TaskWorkflow.nudge, NudgeRequest(value["nonce"], ALICE))
    cards = await eventually(
        lambda: [
            p
            for p in r.slack.posts()
            if p["blocks"] and "wants to nudge" in json.dumps(p["blocks"])
        ],
        timeout=30,
    )
    actions = next(b for b in cards[-1]["blocks"] if b["type"] == "actions")
    child_value = json.loads(actions["elements"][0]["value"])
    child = r.client.get_workflow_handle(child_value["task_id"])
    await wait_state(child, "awaiting_approval")
    await child.signal(TaskWorkflow.decision, ApprovalDecision(child_value["nonce"], ALICE, True))
    assert await child.result() == "done"
    # Settle the restart: the next stale period's notice proves the watch carried the
    # report, and fixes the parent's history before it is recorded.
    await r.env.sleep(timedelta(days=4))
    await eventually(
        lambda: (
            len([p for p in r.slack.posts() if p["thread_ts"] and "No movement" in p["text"]]) == 2
        ),
        timeout=30,
    )
    return handle, child, page


async def task_nudge(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(272 if stored else 271)
    handle, child, page = await _stale_nudge_task(r, stored, thread, dm=True)
    comments = r.notion.comments[page["id"].replace("-", "")]
    assert len(comments) == 1
    assert comments[0]["rich_text"][0]["mention"]["user"]["id"] == BOB_NOTION
    dms = [p for p in r.slack.posts() if p["channel"] == f"D-{BOB}"]
    assert len(dms) == 1 and "asked me to check on 'New monitor'" in dms[0]["text"]
    out = [await current(r, "", handle.id), await current(r, "nudge", child.id)]
    if stored:
        out.append(
            await current(r, "conversation", ids.conversation_id(TENANT, "C_GENERAL", thread))
        )
    return out


async def task_nudge_comment_only(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(282 if stored else 281)
    handle, child, page = await _stale_nudge_task(r, stored, thread, dm=False)
    comments = r.notion.comments[page["id"].replace("-", "")]
    assert len(comments) == 1
    assert all("mention" not in t for t in comments[0]["rich_text"])
    assert [p for p in r.slack.posts() if p["channel"] == f"D-{BOB}"] == []
    out = [await current(r, "", handle.id), await current(r, "nudge", child.id)]
    if stored:
        out.append(
            await current(r, "conversation", ids.conversation_id(TENANT, "C_GENERAL", thread))
        )
    return out


async def task_comment_policy(r: Rig, stored: bool) -> list[Recorded]:
    """A comment on the requester's own filed item runs without a card under the policy.

    The create task files the item and records who filed it; the admin's policy then
    turns approval off for the comment action, and a second turn's comment runs at once.
    """
    thread = thread_ts(302 if stored else 301)
    if stored:
        handle, nonce = await stored_task(r, thread)
    else:
        handle = await start_legacy_task(r, thread)
        nonce = "nonce-1"
    await approve(handle, nonce)
    await wait_state(handle, "watching")
    page = r.notion.all_pages(DB_ID)[0]
    await r.acts.policies.set(TENANT, "add_comment", NO_APPROVAL)
    r.llm.queue(
        tool_turn(ToolCall("tu_cp", "add_comment", {"item": page["id"], "comment": "Any update?"})),
        answer("Commented."),
    )
    conv = ids.conversation_id(TENANT, "C_GENERAL", thread)
    text = "any update on my request?"
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
            InboundMessage(event_id, ALICE, "", "C_GENERAL", thread, "channel", text_stored=True),
        )
    else:
        await start_conversation(r, "it-helper", thread, text, event_id=event_id)
    await eventually(
        lambda: [
            p for p in r.slack.posts() if p["thread_ts"] and "no approval needed" in p["text"]
        ],
        timeout=30,
    )
    comments = r.notion.comments[page["id"].replace("-", "")]
    assert len(comments) == 1
    write = next(e for e in r.acts.audit.events if e.kind == "write" and e.outcome == "policy")
    assert [e.kind for e in r.acts.audit.events if e.task_id == write.task_id] == ["write"]
    return [await current(r, "", write.task_id), await current(r, "conversation", conv)]


async def drive_comment(r: Rig) -> list[Recorded]:
    """A Drive comment task (D-47): the model drafts comment_on_file on a granted file,
    the requester approves the card, and the comment posts once with its marker."""
    thread = thread_ts(321)
    await republish(
        r.acts.agents,
        "it-helper",
        lambda d: d["grants"].update(
            drive={"folders": [{"id": DRIVE_FOLDER, "name": "Team folder"}]}
        ),
    )
    await r.drive_connections.connect(
        TENANT, drive_account_email(r.google), datetime(2026, 11, 21, tzinfo=UTC)
    )
    r.llm.queue(
        tool_turn(
            ToolCall("tu_dc", "comment_on_file", {"file": DRIVE_DOC, "comment": "Please review."})
        ),
        answer("Sent you a card."),
    )
    await start_conversation(r, "it-helper", thread, "comment on the guide", stored=True)
    cards = await eventually(lambda: [p for p in r.slack.posts() if p["blocks"]], timeout=30)
    actions = next(b for b in cards[0]["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    handle = r.client.get_workflow_handle(value["task_id"])
    await wait_state(handle, "awaiting_approval")
    await approve(handle, value["nonce"])
    assert await handle.result() == "done"
    [comment] = r.google.drive_comments[DRIVE_DOC]
    assert comment["content"].startswith("Please review.")
    assert "ref " in comment["content"]
    assert any(p["thread_ts"] and "Commented on Guide" in p["text"] for p in r.slack.posts())
    return [
        await current(r, "", handle.id),
        await current(r, "conversation", ids.conversation_id(TENANT, "C_GENERAL", thread)),
    ]


async def drive_append(r: Rig) -> list[Recorded]:
    """A Drive sheet append task (D-48): the model drafts append_row on a granted Sheet, the
    requester approves the card, and the row appends once with its developer metadata."""
    thread = thread_ts(322)
    await republish(
        r.acts.agents,
        "it-helper",
        lambda d: d["grants"].setdefault("drive", {}).update(sheets=[sheet_grant_block()]),
    )
    await r.drive_connections.connect(
        TENANT, drive_account_email(r.google), datetime(2026, 11, 21, tzinfo=UTC)
    )
    add_roster_sheet(r.google)
    r.llm.queue(
        tool_turn(
            ToolCall(
                "tu_da",
                "append_row",
                {"sheet": DRIVE_SHEET, "row": {"name": "bob", "role": "people"}},
            )
        ),
        answer("Sent you a card."),
    )
    await start_conversation(r, "it-helper", thread, "add bob to the roster", stored=True)
    cards = await eventually(lambda: [p for p in r.slack.posts() if p["blocks"]], timeout=30)
    actions = next(b for b in cards[0]["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    handle = r.client.get_workflow_handle(value["task_id"])
    await wait_state(handle, "awaiting_approval")
    await approve(handle, value["nonce"])
    assert await handle.result() == "done"
    rows = r.google.sheets[DRIVE_SHEET][SHEET_TAB]["rows"]
    assert rows[-1] == ["bob", "people"]
    assert len(r.google.sheet_metadata[DRIVE_SHEET]) == 1
    assert any(p["thread_ts"] and "Appended a row to Roster" in p["text"] for p in r.slack.posts())
    return [
        await current(r, "", handle.id),
        await current(r, "conversation", ids.conversation_id(TENANT, "C_GENERAL", thread)),
    ]


async def drive_create_doc(r: Rig) -> list[Recorded]:
    """A Drive create-from-template task (D-49): the model drafts create_doc_from_template on
    a granted template, the requester approves the card, and one filled Doc is copied into the
    shared-drive folder with its marker appProperty."""
    thread = thread_ts(323)
    await republish(
        r.acts.agents,
        "it-helper",
        lambda d: d["grants"].setdefault("drive", {}).update(templates=[template_grant_block()]),
    )
    await r.drive_connections.connect(
        TENANT, drive_account_email(r.google), datetime(2026, 11, 21, tzinfo=UTC)
    )
    add_template_tree(r.google)
    r.llm.queue(
        tool_turn(
            ToolCall(
                "tu_dcd",
                "create_doc_from_template",
                {
                    "template": DRIVE_TEMPLATE,
                    "title": "Offer for Bob",
                    "values": {"name": "Bob", "role": "engineer"},
                },
            )
        ),
        answer("Sent you a card."),
    )
    await start_conversation(r, "it-helper", thread, "draft an offer for bob", stored=True)
    cards = await eventually(lambda: [p for p in r.slack.posts() if p["blocks"]], timeout=30)
    actions = next(b for b in cards[0]["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    handle = r.client.get_workflow_handle(value["task_id"])
    await wait_state(handle, "awaiting_approval")
    await approve(handle, value["nonce"])
    assert await handle.result() == "done"
    [new_id] = r.google.drive_children[TEMPLATE_FOLDER]
    assert r.google.drive_content[new_id][0].text == "Dear Bob,\nYou are hired as engineer."
    assert len(r.google.drive_app_properties[new_id]) == 1
    assert any(p["thread_ts"] and "Created Offer for Bob" in p["text"] for p in r.slack.posts())
    return [
        await current(r, "", handle.id),
        await current(r, "conversation", ids.conversation_id(TENANT, "C_GENERAL", thread)),
    ]


async def drive_replace_section(r: Rig) -> list[Recorded]:
    """A Drive section-replace task (D-50): the model drafts replace_section on a granted Doc
    section, the requester approves the card, and the section's content is replaced once at the
    read revision, keeping the heading."""
    thread = thread_ts(324)
    await republish(
        r.acts.agents,
        "it-helper",
        lambda d: d["grants"].setdefault("drive", {}).update(sections=[section_grant_block()]),
    )
    await r.drive_connections.connect(
        TENANT, drive_account_email(r.google), datetime(2026, 11, 21, tzinfo=UTC)
    )
    add_section_doc(r.google)
    r.llm.queue(
        tool_turn(
            ToolCall(
                "tu_drs",
                "replace_section",
                {"section": DRIVE_SECTION, "text": "The VPN is back up."},
            )
        ),
        answer("Sent you a card."),
    )
    await start_conversation(r, "it-helper", thread, "update the VPN status", stored=True)
    cards = await eventually(lambda: [p for p in r.slack.posts() if p["blocks"]], timeout=30)
    actions = next(b for b in cards[0]["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    handle = r.client.get_workflow_handle(value["task_id"])
    await wait_state(handle, "awaiting_approval")
    await approve(handle, value["nonce"])
    assert await handle.result() == "done"
    paras = r.google.docs[DRIVE_SECTION]["paragraphs"]
    assert paras[0]["text"] == "Status\n"
    assert paras[1]["text"] == "The VPN is back up.\n"
    assert any(
        p["thread_ts"] and "Replaced a section of VPN guide" in p["text"] for p in r.slack.posts()
    )
    return [
        await current(r, "", handle.id),
        await current(r, "conversation", ids.conversation_id(TENANT, "C_GENERAL", thread)),
    ]


def _slack_update_error() -> SlackApiError:
    """A chat.update refusal as the SDK raises it: Slack's error code in the response."""
    return SlackApiError(
        "The request to the Slack API failed.",
        SlackResponse(
            client=None,
            http_verb="POST",
            api_url="https://slack.com/api/chat.update",
            req_args={},
            data={"ok": False, "error": "message_not_found"},
            headers={},
            status_code=200,
        ),
    )


async def task_card_update_failure(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(192 if stored else 191)
    if stored:
        handle, nonce = await stored_task(r, thread)
        # The conversation's reply update lands before the card redraw that will fail.
        await replies(r, 1)
    else:
        handle = await start_legacy_task(r, thread)
        nonce = "nonce-1"
    # The agent is turned off after the card was shown, so the pre-write check refuses and
    # the refusal redraw runs through _try; chat.update then refuses every attempt.
    assert await r.switches.switch_off(TENANT, "agent", "it-helper", ADMIN, datetime.now(UTC))
    r.slack.fail_next_update(_slack_update_error(), times=None)
    await approve(handle, nonce)
    assert await handle.result() == "cancelled"
    return [await current(r, "", handle.id)]


async def task_unavailable_write(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(212 if stored else 211)
    if stored:
        task_id = ids.task_id(TENANT, f"replay-{thread}")
        await r.acts.content.put_draft(
            TENANT,
            task_id,
            1,
            {"title": "New monitor", "details": "Mine flickers.", "database_id": DB_ID},
        )
        spec = dataclasses.replace(legacy_spec(thread), payload={}, draft_ref=task_id, revision=1)
        handle = await r.client.start_workflow(
            TaskWorkflow.run, spec, id=task_id, task_queue=TASK_QUEUE
        )
        await wait_state(handle, "awaiting_approval")
    else:
        handle = await start_legacy_task(r, thread)
    # The card is posted; the pre-write check reads health once, and the write's own gate
    # is the next read: the agent becomes unavailable between the check and the write.
    r.acts.health = FlipsAfterReads(quiet_reads=1)
    await approve(handle, "nonce-1")
    assert await handle.result() == "cancelled"
    assert not r.notion.all_pages(DB_ID)
    history = await r.client.get_workflow_handle(handle.id).fetch_history()
    assert any(
        e.HasField("activity_task_scheduled_event_attributes")
        and e.activity_task_scheduled_event_attributes.activity_type.name == "create_notion_item"
        for e in history.events
    ), "the flip landed before the write activity, not on it"
    return [await current(r, "", handle.id)]


async def task_tenant_disabled(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(252 if stored else 251)
    if stored:
        handle, nonce = await stored_task(r, thread)
    else:
        handle = await start_legacy_task(r, thread)
        nonce = "nonce-1"
    # The app is uninstalled: the runtime stops serving the tenant and reports it
    # disabled, as the worker's sync does after the gateway's uninstall event.
    posts_before = len(r.slack.posts())
    r.acts.registry.remove(TENANT)
    r.acts.registry.set_disabled({TENANT})
    await approve(handle, nonce)
    assert await handle.result() == "cancelled"
    assert len(r.slack.posts()) == posts_before
    assert not r.notion.all_pages(DB_ID)
    return [await current(r, "", handle.id)]


async def conversation_tenant_disabled(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(262 if stored else 261)
    r.llm.queue(
        tool_turn(ToolCall("tu_i", "search_items", {"query": "laptop"})),
        answer("Back online."),
    )
    # The turn is inside its tool step, placeholder posted, when the app is uninstalled:
    # the runtime stops serving the tenant, as the worker's sync does after the gateway's
    # uninstall event. The tool itself finishes (it holds the runtime it started with);
    # the next model step is the one refused.
    items_hang = r.notion.hang_next_read("query_database")
    handle = await start_conversation(r, "it-helper", thread, "any laptop requests?", stored=stored)
    await items_hang.reached.wait()
    conv = ids.conversation_id(TENANT, "C_GENERAL", thread)
    # No virtual time passes on its own in this window, so no progress edit lands.
    with r.env.auto_time_skipping_disabled():
        runtime = r.acts.registry.get(TENANT)
        r.acts.registry.remove(TENANT)
        r.acts.registry.set_disabled({TENANT})
        items_hang.release()

        async def recovered() -> bool:
            messages = await r.acts.history.load(TENANT, conv)
            return any(
                m["role"] == "assistant" and any(b.get("text") == TURN_FAILED for b in m["content"])
                for m in messages
            )

        await eventually(recovered, timeout=30)
    # A reinstall restores the runtime, and the same workflow answers the next message.
    # That reply is the settle: nothing was posted for the turn the uninstall stopped.
    r.acts.registry.set(TENANT, runtime)
    event_id = f"Ev-{thread}-2"
    text = "hello again"
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
    await handle.signal(
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
    await replies(r, 1)
    assert [u["text"] for u in r.slack.updates()] == ["Back online."]
    assert len(r.slack.posts()) == 2  # the two placeholders; no failure notice
    history = await r.client.get_workflow_handle(conv).fetch_history()
    assert not any(
        e.HasField("activity_task_scheduled_event_attributes")
        and e.activity_task_scheduled_event_attributes.activity_type.name
        in ("finalize_notice", "post_final_notice", "post_in_thread", "post_notice_in_thread")
        for e in history.events
    ), "the stopped turn posted a failure notice or finalized its placeholder"
    return [await current(r, "", conv)]


async def progress(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(202 if stored else 201)
    scope = ids.knowledge_scope(ROOT_A)
    await r.client.execute_workflow(
        IndexingWorkflow.run,
        args=[TENANT, scope, ROOT_A.replace("-", "")],
        id=ids.index_workflow_id(TENANT, scope),
        task_queue=TASK_QUEUE,
    )
    r.llm.queue(
        tool_turn(ToolCall("tu_s", "search_knowledge", {"query": "reset your vpn password"})),
        tool_turn(ToolCall("tu_i", "search_items", {"query": "laptop"})),
        answer("The portal [1] resets it."),
    )
    # Each step is held open past the two-second mark so its label shows; the second
    # step's label also waits out the three-second gap from the first.
    search_hang = r.notion.hang_next_read("retrieve_page")
    items_hang = r.notion.hang_next_read("query_database")
    await start_conversation(
        r, "it-helper", thread, "vpn reset and any laptop requests?", stored=stored
    )
    await search_hang.reached.wait()
    await r.env.sleep(PROGRESS_AFTER)
    await eventually(
        lambda: [u for u in r.slack.updates() if u["text"] == "Searching IT wiki…"], timeout=30
    )
    search_hang.release()
    await items_hang.reached.wait()
    await r.env.sleep(PROGRESS_AFTER + PROGRESS_GAP)
    await eventually(
        lambda: [u for u in r.slack.updates() if u["text"] == "Reading Notion…"], timeout=30
    )
    items_hang.release()
    await replies(r, 3)
    texts = [u["text"] for u in r.slack.updates()]
    assert texts[:2] == ["Searching IT wiki…", "Reading Notion…"]
    assert len(texts) == 3 and texts[2].startswith("The portal")
    return [await current(r, "", ids.conversation_id(TENANT, "C_GENERAL", thread))]


async def progress_dropped(r: Rig, stored: bool) -> list[Recorded]:
    thread = thread_ts(204 if stored else 203)
    r.llm.queue(
        tool_turn(ToolCall("tu_i", "search_items", {"query": "laptop"})),
        answer("The laptop policy."),
    )
    # The step runs long enough for a progress edit, and both attempts of that edit are
    # refused, so it is dropped: the answer is posted as a new message, not an edit.
    read_hang = r.notion.hang_next_read("query_database")
    r.slack.fail_next_update(ValueError("slack down"), times=2)
    await start_conversation(r, "it-helper", thread, "any laptop requests?", stored=stored)
    await read_hang.reached.wait()
    await r.env.sleep(PROGRESS_AFTER + timedelta(seconds=2))
    read_hang.release()
    await r.env.sleep(timedelta(seconds=1))
    await eventually(
        lambda: [p for p in r.slack.posts() if p["text"] == "The laptop policy."], timeout=30
    )
    assert not r.slack.updates()
    return [await current(r, "", ids.conversation_id(TENANT, "C_GENERAL", thread))]


async def creator_access(r: Rig) -> list[Recorded]:
    """One nightly pass: a consented creator's picked grants read back, so both verify and
    the run's health step leaves the agent available."""
    await r.picks.record(
        TENANT,
        "it-helper",
        [PickedObject(DB_ID, "database"), PickedObject(ROOT_A, "page")],
        "U_CREATOR",
        datetime(2026, 11, 20, tzinfo=UTC),
    )
    await r.credentials.store_with_current_key(
        TENANT, ids.notion_creator_kind("U_CREATOR"), "ntn_creator_token_for_the_fixture"
    )
    counts = await r.client.execute_workflow(
        CreatorAccessWorkflow.run,
        TENANT,
        id=ids.creator_access_workflow_id(TENANT),
        task_queue=TASK_QUEUE,
    )
    assert (counts.verified, counts.lost, counts.cannot_verify, counts.unchanged) == (2, 0, 0, 0)
    rows = await r.access.for_agent(TENANT, "it-helper")
    assert {row.status for row in rows.values()} == {"verified"}
    assert (await r.health.get(TENANT, "it-helper")).status == AVAILABLE
    return [await current(r, "", ids.creator_access_workflow_id(TENANT))]


async def creator_access_lost(r: Rig) -> list[Recorded]:
    """One nightly pass where the creator can no longer see the database they picked: the
    run's health step leaves the agent unavailable with the grant named, though the grants
    themselves still pass."""
    await r.picks.record(
        TENANT,
        "it-helper",
        [PickedObject(DB_ID, "database"), PickedObject(ROOT_A, "page")],
        "U_CREATOR",
        datetime(2026, 11, 20, tzinfo=UTC),
    )
    await r.credentials.store_with_current_key(
        TENANT, ids.notion_creator_kind("U_CREATOR"), "ntn_creator_token_for_the_fixture"
    )
    # The database is gone from the creator's own Notion; the tenant's integration still
    # reads it, so only the creator-access source fails.
    del r.creator_notion.databases[normalize_id(DB_ID)]
    counts = await r.client.execute_workflow(
        CreatorAccessWorkflow.run,
        TENANT,
        id=ids.creator_access_workflow_id(TENANT),
        task_queue=TASK_QUEUE,
    )
    assert (counts.verified, counts.lost, counts.cannot_verify, counts.unchanged) == (1, 1, 0, 0)
    rows = await r.access.for_agent(TENANT, "it-helper")
    assert rows[normalize_id(DB_ID)].status == "lost"
    assert rows[normalize_id(ROOT_A)].status == "verified"
    health = await r.health.get(TENANT, "it-helper")
    assert health.status == UNAVAILABLE
    assert "its creator can no longer see 'IT requests'" in health.reason
    return [await current(r, "", ids.creator_access_workflow_id(TENANT))]


async def creator_access_tenant_disabled(r: Rig) -> list[Recorded]:
    """The schedule fires for an uninstalled tenant: the pass completes with zero counts,
    no reads and no retries."""
    r.acts.registry.remove(TENANT)
    r.acts.registry.set_disabled({TENANT})
    counts = await r.client.execute_workflow(
        CreatorAccessWorkflow.run,
        TENANT,
        id=ids.creator_access_workflow_id(TENANT),
        task_queue=TASK_QUEUE,
    )
    assert (counts.verified, counts.lost, counts.cannot_verify, counts.unchanged) == (0, 0, 0, 0)
    assert r.notion.calls == [] and r.creator_notion.calls == []
    history = await r.client.get_workflow_handle(
        ids.creator_access_workflow_id(TENANT)
    ).fetch_history()
    scheduled = [
        e.activity_task_scheduled_event_attributes.activity_type.name
        for e in history.events
        if e.HasField("activity_task_scheduled_event_attributes")
    ]
    # One attempt each: the listing finds no served tenant, and the health step ends too.
    assert scheduled == ["creator_access_agents", "creator_access_health"]
    return [await current(r, "", ids.creator_access_workflow_id(TENANT))]


async def test_run(r: Rig) -> list[Recorded]:
    """One run of a draft's three cases: a cited answer, a drafted write that stops its
    case, and a refusal. Nothing is posted to Slack and nothing is written."""
    # The cap check reads the run's spend: price the rig's model at zero, so the run's
    # calls are priced (not unknown spend) without depending on the shipped prices file.
    r.acts.usage = UsageRecorder(
        r.usage,
        Prices(
            [
                PriceEntry(
                    Settings().model,
                    date(2026, 1, 1),
                    Decimal(0),
                    Decimal(0),
                    Decimal(0),
                    Decimal(0),
                )
            ]
        ),
    )
    scope = ids.knowledge_scope(ROOT_A)
    await r.client.execute_workflow(
        IndexingWorkflow.run,
        args=[TENANT, scope, ROOT_A.replace("-", "")],
        id=ids.index_workflow_id(TENANT, scope),
        task_queue=TASK_QUEUE,
    )
    draft = await r.acts.agents.create_draft(TENANT, "it-helper", ADMIN)
    cases = [
        TestCase(
            TENANT,
            "it-helper",
            draft.version,
            f"{TENANT}:test-case:c1",
            "when are laptops replaced?",
            CITE,
            normalize_id(PAGE_A1),
            created_by=ADMIN,
        ),
        TestCase(
            TENANT,
            "it-helper",
            draft.version,
            f"{TENANT}:test-case:c2",
            "file a ticket for my monitor",
            WRITE,
            normalize_id(DB_ID),
            created_by=ADMIN,
        ),
        TestCase(
            TENANT,
            "it-helper",
            draft.version,
            f"{TENANT}:test-case:c3",
            "something out of scope",
            REFUSE,
            created_by=ADMIN,
        ),
    ]
    for c in cases:
        await r.acts.test_sets.add_case(TENANT, c)
    run_id = ids.test_run_id(TENANT, "replay")
    await r.acts.test_sets.start_run(
        TENANT,
        TestRun(
            TENANT,
            run_id,
            "it-helper",
            draft.version,
            RUNNING,
            ADMIN,
            canonical_sha256(draft.definition),
            case_set_sha256(cases),
        ),
    )
    r.llm.queue(
        tool_turn(ToolCall("tu_s", "search_knowledge", {"query": "laptops replaced"})),
        answer("Laptops are replaced every three years [1]."),
        tool_turn(
            ToolCall("tu_w", "create_item", {"title": "New monitor", "details": "Mine flickers."})
        ),
        LLMResponse(text="I cannot help with that.", tool_calls=[], stop_reason="refusal"),
    )
    await r.client.execute_workflow(
        TestRunWorkflow.run,
        args=[TENANT, run_id],
        id=run_id,
        task_queue=TASK_QUEUE,
    )
    run = await r.acts.test_sets.get_run(TENANT, run_id)
    assert [(x.outcome, x.reason) for x in run.results] == [("pass", "")] * 3, run.results
    assert run.results[1].tool == "create_item"
    assert run.results[1].target == normalize_id(DB_ID)
    assert r.slack.posts() == [] and r.slack.updates() == []
    assert [n for n, _ in r.notion.calls if n in ("create_page", "create_comment")] == []
    return [await current(r, "", run_id)]


async def test_run_tenant_disabled(r: Rig) -> list[Recorded]:
    """A run whose tenant is uninstalled mid-case: the read finishes, the next model step
    is refused, and the run's row closes as failed with nothing posted."""
    r.acts.usage = UsageRecorder(
        r.usage,
        Prices(
            [
                PriceEntry(
                    Settings().model,
                    date(2026, 1, 1),
                    Decimal(0),
                    Decimal(0),
                    Decimal(0),
                    Decimal(0),
                )
            ]
        ),
    )
    draft = await r.acts.agents.create_draft(TENANT, "it-helper", ADMIN)
    case = TestCase(
        TENANT,
        "it-helper",
        draft.version,
        f"{TENANT}:test-case:d1",
        "any laptop requests?",
        REFUSE,
        created_by=ADMIN,
    )
    await r.acts.test_sets.add_case(TENANT, case)
    run_id = ids.test_run_id(TENANT, "replay-disabled")
    await r.acts.test_sets.start_run(
        TENANT,
        TestRun(
            TENANT,
            run_id,
            "it-helper",
            draft.version,
            RUNNING,
            ADMIN,
            canonical_sha256(draft.definition),
            case_set_sha256([case]),
        ),
    )
    r.llm.queue(
        tool_turn(ToolCall("tu_i", "search_items", {"query": "laptop"})),
        answer("Back online."),  # never consumed: the next model step is refused
    )
    # The case is inside its read when the app is uninstalled: the read finishes (it
    # holds the runtime it started with); the next model step is the one refused.
    items_hang = r.notion.hang_next_read("query_database")
    handle = await r.client.start_workflow(
        TestRunWorkflow.run, args=[TENANT, run_id], id=run_id, task_queue=TASK_QUEUE
    )
    await items_hang.reached.wait()
    with r.env.auto_time_skipping_disabled():
        r.acts.registry.remove(TENANT)
        r.acts.registry.set_disabled({TENANT})
        items_hang.release()
        assert await handle.result() == run_id
    run = await r.acts.test_sets.get_run(TENANT, run_id)
    assert (run.status, run.results) == (FAILED, [])
    assert r.slack.posts() == [] and r.slack.updates() == []
    return [await current(r, "", run_id)]


async def drive_indexing(r: Rig) -> list[Recorded]:
    """One nightly Drive walk, continued as new mid-walk: a granted folder's Doc, Sheet and
    Slides are indexed, a non-exportable type is counted, and the walk's outcome is recorded
    on the connection."""
    await republish(
        r.acts.agents,
        "it-helper",
        lambda d: d["grants"].update(
            drive={"folders": [{"id": DRIVE_FOLDER, "name": "Team folder"}]}
        ),
    )
    await r.drive_connections.connect(
        TENANT, drive_account_email(r.google), datetime(2026, 11, 20, tzinfo=UTC)
    )
    scope = ids.drive_scope(DRIVE_FOLDER)
    with suggest_continue_as_new_after(HOP_EVENTS):
        handle = await r.client.start_workflow(
            DriveIndexingWorkflow.run,
            args=[TENANT, scope, DRIVE_FOLDER],
            id=ids.drive_index_workflow_id(TENANT, scope),
            task_queue=TASK_QUEUE,
        )
        summary = await handle.result()
    assert (summary.files, summary.changed, summary.skipped_type, summary.failed) == (5, 4, 1, 0)
    chain = await hop_chain(r.client, handle.id, handle.result_run_id)
    assert len(chain) >= 2, "the Drive walk never continued as new"
    return [Recorded(f"run{i + 1}", h, HOP_EVENTS) for i, h in enumerate(chain)]


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
    "knowledge_uncited": knowledge_uncited,
    "progress": progress,
    "progress_dropped": progress_dropped,
    "knowledge_uncited_dropped": knowledge_uncited_dropped,
    "audience_refusal": audience_refusal,
    "hand_off_refused": hand_off_refused,
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
    "task_card_update_failure": task_card_update_failure,
    "task_unavailable_write": task_unavailable_write,
    "task_tenant_disabled": task_tenant_disabled,
    "conversation_tenant_disabled": conversation_tenant_disabled,
    "task_recorded": task_recorded,
    "my_tasks": my_tasks,
    "my_tasks_once": my_tasks_once,
    "task_nudge": task_nudge,
    "task_nudge_comment_only": task_nudge_comment_only,
    "task_comment_policy": task_comment_policy,
}.items():
    for _stored, _run in _both(_fn).items():
        SCENARIOS[f"{_name}{'_stored' if _stored else '_inline'}"] = _run

# The nightly creator-access passes have one shape: no message or task payload rides in them.
SCENARIOS["creator_access"] = creator_access
SCENARIOS["creator_access_lost"] = creator_access_lost
SCENARIOS["creator_access_tenant_disabled"] = creator_access_tenant_disabled
# A test run carries references only: one shape, like the creator-access passes.
SCENARIOS["test_run"] = test_run
SCENARIOS["test_run_tenant_disabled"] = test_run_tenant_disabled
# The Drive walk carries references only: one shape, no message or task payload rides in it.
SCENARIOS["drive_indexing"] = drive_indexing
# A Drive comment task is new: one shape (the stored draft), no legacy inline history exists.
SCENARIOS["drive_comment"] = drive_comment
# A Drive sheet append task is new too: one shape (the stored draft), no legacy inline history.
SCENARIOS["drive_append"] = drive_append
# A Drive create-from-template task is new: one shape (the stored draft), no legacy inline history.
SCENARIOS["drive_create_doc"] = drive_create_doc
# A Drive section-replace task is new: one shape (the stored draft), no legacy inline history.
SCENARIOS["drive_replace_section"] = drive_replace_section


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
        workflows=[
            ConversationWorkflow,
            TaskWorkflow,
            CreatorAccessWorkflow,
            TestRunWorkflow,
            DriveIndexingWorkflow,
        ],
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
