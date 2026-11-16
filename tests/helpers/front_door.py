"""Helpers for conversations that go through the front door and its hand-offs."""

import copy
from contextlib import asynccontextmanager

from temporalio.worker import Worker

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_store import seeded_agent_store
from agentos.config import TASK_QUEUE, Settings, parse_agents_config
from agentos.knowledge.search import ITEM_RULES
from agentos.llm import ScriptedLLMClient
from agentos.models import ConversationRef, InboundMessage, LLMResponse, ToolCall
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for
from agentos.workflows.conversation import (
    ConversationWorkflow,
)
from agentos.workflows.task import TaskWorkflow
from tests.conftest import (
    ALICE,
    BOB,
    TENANT,
    Harness,
    agents_dict,
    make_fake_notion,
)
from tests.helpers.temporal import new_scope, replace_server_if_stuck

CHANNEL = "C_GENERAL"


THREAD = "1700000000.000100"


CONV_ID = ids.conversation_id(TENANT, CHANNEL, THREAD)


ANNOUNCEMENT = "IT helper here, picking this up."


def with_item_rules(prompt: str) -> str:
    """The system prompt an agent with the Notion item tools is given."""
    return f"{prompt}\n\n{ITEM_RULES}"


def front_door_dict(concierge_steps: int = 4) -> dict:
    """``concierge`` is a front door for ``it-helper``; ``it-helper`` has the Notion grant."""
    raw = agents_dict()
    helper = raw["agents"]["it-helper"]
    helper["description"] = "Laptops, accounts, VPN, access and other IT requests."
    concierge = copy.deepcopy(helper)
    concierge["name"] = "Concierge"
    concierge["description"] = "General questions about the company."
    concierge["system_prompt"] = "You answer general questions."
    concierge["max_steps"] = concierge_steps
    concierge["grants"] = {}
    concierge["specialists"] = ["it-helper"]
    raw["agents"]["concierge"] = concierge
    raw["agents"]["unlisted"] = {**copy.deepcopy(helper), "name": "Unlisted"}
    raw["default_agent"] = "concierge"
    return raw


@asynccontextmanager
async def running(raw: dict, interceptors=()):
    config = parse_agents_config(raw)
    wf_env = new_scope()
    h = Harness(
        wf_env=wf_env,
        agents=config,
        llm=ScriptedLLMClient(),
        slack=FakeSlack(emails={ALICE: "alice@example.com", BOB: "bob@example.com"}),
        notion=make_fake_notion(),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        tool_outputs=InMemoryToolOutputStore(),
        agent_store=seeded_agent_store(config),
    )
    acts = AgentActivities(
        settings=Settings(),
        registry=TenantRegistry({TENANT: runtime_for(config, h.slack, h.notion)}),
        history=h.history,
        idem=h.idem,
        links=h.links,
        llm=h.llm,
        tool_outputs=h.tool_outputs,
        audit=h.audit,
        agents=h.agent_store,
        switches=h.switches,
    )
    h.acts = acts
    worker = Worker(
        wf_env.client,
        task_queue=wf_env.task_queue,
        workflows=[ConversationWorkflow, TaskWorkflow],
        activities=acts.all(),
        interceptors=[wf_env.children, *interceptors],
    )
    async with worker:
        try:
            yield h
        finally:
            await wf_env.finish()
            await replace_server_if_stuck()


def message(event_id: str, text: str = "my laptop battery is swelling") -> InboundMessage:
    return InboundMessage(event_id, ALICE, text, CHANNEL, THREAD, "channel")


async def converse(h, agent_id: str, msg: InboundMessage):
    return await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, agent_id, CHANNEL, THREAD),
        id=CONV_ID,
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )


def hand_off_to(agent: str, call_id: str = "tu_h") -> ToolCall:
    return ToolCall(id=call_id, name="hand_off", input={"agent": agent})


def tool_turn(*calls: ToolCall) -> LLMResponse:
    return LLMResponse(text=None, tool_calls=list(calls), stop_reason="tool_use")


def answer(text: str) -> LLMResponse:
    return LLMResponse(text=text, tool_calls=[], stop_reason="end_turn")
