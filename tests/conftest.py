"""Shared fixtures. Everything here is a fake: no test talks to Claude, Slack
or Notion. Workflow tests use Temporal's time-skipping test server."""

from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agentos.activities import AgentActivities
from agentos.config import TASK_QUEUE, AgentsConfig, Settings, parse_agents_config
from agentos.connectors.notion import FakeNotionAPI, text_block
from agentos.index_knowledge import granted_roots
from agentos.knowledge.embedder import FakeEmbedder
from agentos.knowledge.indexing import IndexActivities
from agentos.knowledge.store import InMemoryKnowledgeStore
from agentos.llm import ScriptedLLMClient
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.indexing import IndexingWorkflow
from agentos.workflows.page_refresh import PageRefreshWorkflow
from agentos.workflows.task import TaskWorkflow

TENANT = "acme"
TEAM = "T0TEST"
DB_ID = "11111111-2222-3333-4444-555555555555"
OTHER_DB_ID = "99999999-8888-7777-6666-555555555555"
ALICE = "U_ALICE"
BOB = "U_BOB"
ALICE_NOTION = "notion-user-alice"
BOB_NOTION = "notion-user-bob"
PROPERTY_IDS = {
    "title": "title",
    "status": "stat1",
    "requested_by": "reqby1",
    "request_id": "reqid1",
}


def default_schema() -> dict:
    """The schema of the shared test database, matching the grant's names."""
    return {
        "Name": {"id": "title", "type": "title"},
        "Status": {"id": "stat1", "type": "status", "options": ["To do", "Done"]},
        "Requested by": {"id": "reqby1", "type": "people"},
        "Agent request ID": {"id": "reqid1", "type": "rich_text"},
    }


def agents_dict() -> dict:
    return {
        "tenant_id": TENANT,
        "slack_team_id": TEAM,
        "agents": {
            "it-helper": {
                "version": 1,
                "name": "IT helper",
                "system_prompt": "You help employees with IT requests.",
                "max_steps": 6,
                "grants": {
                    "notion": {
                        "databases": [
                            {
                                "id": DB_ID,
                                "name": "IT requests",
                                "properties": {
                                    "title": "Name",
                                    "status": "Status",
                                    "done_values": ["Done"],
                                    "requested_by": "Requested by",
                                    "request_id": "Agent request ID",
                                },
                                "property_ids": dict(PROPERTY_IDS),
                            }
                        ]
                    }
                },
                "follow_up": {
                    "poll_interval_minutes": 15,
                    "stale_after_days": 3,
                    "max_watch_days": 30,
                },
                "approval": {"expires_after_hours": 72, "reminder_after_hours": 24},
            }
        },
        "default_agent": "it-helper",
    }


@pytest.fixture
def agents() -> AgentsConfig:
    return parse_agents_config(agents_dict())


def make_fake_notion() -> FakeNotionAPI:
    api = FakeNotionAPI(
        users=[
            {
                "object": "user",
                "id": ALICE_NOTION,
                "type": "person",
                "person": {"email": "Alice@Example.com"},
            },
            {
                "object": "user",
                "id": BOB_NOTION,
                "type": "person",
                "person": {"email": "bob@example.com"},
            },
            {"object": "user", "id": "bot-1", "type": "bot", "bot": {}},
        ]
    )
    api.add_database(DB_ID)
    api.add_database(OTHER_DB_ID)
    api.set_schema(DB_ID, default_schema())
    return api


def title_prop(title: str) -> dict:
    return {"title": [{"text": {"content": title}}]}


@dataclass
class Harness:
    wf_env: WorkflowEnvironment
    agents: AgentsConfig
    llm: ScriptedLLMClient
    slack: FakeSlack
    notion: FakeNotionAPI
    history: InMemoryHistoryStore
    idem: InMemoryIdempotencyStore
    links: InMemoryTaskLinkStore
    tool_outputs: InMemoryToolOutputStore

    @property
    def client(self):
        return self.wf_env.client


@pytest.fixture
async def harness(agents):
    wf_env = await WorkflowEnvironment.start_time_skipping()
    h = Harness(
        wf_env=wf_env,
        agents=agents,
        llm=ScriptedLLMClient(),
        slack=FakeSlack(emails={ALICE: "alice@example.com", BOB: "bob@example.com"}),
        notion=make_fake_notion(),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        tool_outputs=InMemoryToolOutputStore(),
    )
    acts = AgentActivities(
        settings=Settings(),
        agents=agents,
        history=h.history,
        idem=h.idem,
        links=h.links,
        llm=h.llm,
        slack=h.slack,
        notion_api=h.notion,
        tool_outputs=h.tool_outputs,
    )
    worker = Worker(
        wf_env.client,
        task_queue=TASK_QUEUE,
        workflows=[ConversationWorkflow, TaskWorkflow],
        activities=acts.all(),
    )
    async with worker:
        yield h
    await wf_env.shutdown()


async def eventually(check, timeout: float = 10.0, interval: float = 0.05):
    """Poll ``check`` (sync or async) in real time until it returns truthy."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        result = check()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return result
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(interval)


# --- Knowledge fixtures: agents with page grants, indexed from a fake Notion tree.

ROOT_A = "aaaaaaaa-0000-0000-0000-000000000001"
ROOT_B = "bbbbbbbb-0000-0000-0000-000000000001"
PAGE_A1 = "aaaaaaaa-0000-0000-0000-000000000002"
PAGE_A2 = "aaaaaaaa-0000-0000-0000-000000000003"
PAGE_B1 = "bbbbbbbb-0000-0000-0000-000000000002"


def knowledge_agents_dict() -> dict:
    """``it-helper`` reads root A, ``hr-helper`` reads root B, ``plain`` has no pages."""
    raw = agents_dict()
    base = raw["agents"]["it-helper"]
    hr = copy.deepcopy(base)
    hr["name"] = "HR helper"
    plain = copy.deepcopy(base)
    plain["name"] = "Plain helper"
    base["grants"]["notion"]["pages"] = [{"id": ROOT_A, "name": "IT wiki"}]
    hr["grants"]["notion"]["pages"] = [{"id": ROOT_B, "name": "HR wiki"}]
    raw["agents"]["hr-helper"] = hr
    raw["agents"]["plain"] = plain
    return raw


def a_tree() -> dict:
    """Three levels: the IT wiki, a Laptops page, and a Batteries page under it."""
    return {
        "title": "IT wiki",
        "blocks": [
            text_block("heading_1", "VPN"),
            text_block("paragraph", "Reset your VPN password at the portal."),
        ],
        "children": [
            {
                "id": PAGE_A1,
                "title": "Laptops",
                "blocks": [text_block("paragraph", "Laptops are replaced every three years.")],
                "children": [
                    {
                        "id": PAGE_A2,
                        "title": "Batteries",
                        "blocks": [text_block("paragraph", "Swollen batteries must be reported.")],
                    }
                ],
            }
        ],
    }


def b_tree() -> dict:
    return {
        "title": "HR wiki",
        "blocks": [text_block("paragraph", "Parental leave lasts sixteen weeks.")],
        "children": [
            {
                "id": PAGE_B1,
                "title": "Benefits",
                "blocks": [text_block("paragraph", "Dental cover starts after one month.")],
            }
        ],
    }


@dataclass
class KnowledgeHarness(Harness):
    knowledge: InMemoryKnowledgeStore = None  # type: ignore[assignment]
    embedder: FakeEmbedder = None  # type: ignore[assignment]
    acts: AgentActivities = None  # type: ignore[assignment]


@pytest.fixture
async def kharness():
    cfg = parse_agents_config(knowledge_agents_dict())
    wf_env = await WorkflowEnvironment.start_time_skipping()
    notion = make_fake_notion()
    notion.add_page_tree(ROOT_A, a_tree())
    notion.add_page_tree(ROOT_B, b_tree())
    h = KnowledgeHarness(
        wf_env=wf_env,
        agents=cfg,
        llm=ScriptedLLMClient(),
        slack=FakeSlack(emails={ALICE: "alice@example.com", BOB: "bob@example.com"}),
        notion=notion,
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        tool_outputs=InMemoryToolOutputStore(),
        knowledge=InMemoryKnowledgeStore(),
        embedder=FakeEmbedder(),
    )
    h.acts = AgentActivities(
        settings=Settings(),
        agents=cfg,
        history=h.history,
        idem=h.idem,
        links=h.links,
        llm=h.llm,
        slack=h.slack,
        notion_api=notion,
        tool_outputs=h.tool_outputs,
        knowledge=h.knowledge,
        embedder=h.embedder,
    )
    index_acts = IndexActivities(notion, h.knowledge, h.embedder, granted_roots(cfg))
    worker = Worker(
        wf_env.client,
        task_queue=TASK_QUEUE,
        workflows=[ConversationWorkflow, TaskWorkflow, IndexingWorkflow, PageRefreshWorkflow],
        activities=h.acts.all() + index_acts.all(),
    )
    async with worker:
        yield h
    await wf_env.shutdown()
