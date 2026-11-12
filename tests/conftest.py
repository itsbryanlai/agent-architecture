"""Shared fixtures. Everything here is a fake: no test talks to Claude, Slack
or Notion. Workflow tests use Temporal's time-skipping test server."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agentos.activities import AgentActivities
from agentos.config import TASK_QUEUE, AgentsConfig, Settings, parse_agents_config
from agentos.connectors.notion import FakeNotionAPI
from agentos.llm import ScriptedLLMClient
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.workflows.conversation import ConversationWorkflow
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
