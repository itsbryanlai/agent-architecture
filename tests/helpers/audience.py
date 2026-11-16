"""Helpers that run conversations in channels with an audience rule."""

import copy
from contextlib import asynccontextmanager

from temporalio.worker import Worker

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_store import seeded_agent_store
from agentos.config import (
    TASK_QUEUE,
    Settings,
    parse_agents_config,
)
from agentos.llm import ScriptedLLMClient
from agentos.models import (
    ConversationRef,
    InboundMessage,
)
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import (
    TENANT,
    Harness,
    agents_dict,
    make_fake_notion,
)
from tests.helpers.temporal import new_scope, replace_server_if_stuck

CHAN = "C0AAAA111"


def tenant_dict(audience=None, channels=None, specialist_audience=None, front_door=False) -> dict:
    raw = agents_dict()
    helper = raw["agents"]["it-helper"]
    helper["description"] = "IT requests."
    if audience is not None:
        helper["audience"] = audience
    if channels:
        raw["channels"] = channels
    if front_door:
        door = copy.deepcopy(helper)
        door.pop("audience", None)
        door.update(name="Concierge", description="General.", grants={}, specialists=["it-helper"])
        raw["agents"]["concierge"] = door
        raw["default_agent"] = "concierge"
        if specialist_audience is not None:
            helper["audience"] = specialist_audience
    return raw


OWNER = "U_OWNER"


THREAD = "1700000000.000100"


@asynccontextmanager
async def running(raw: dict, slack: FakeSlack | None = None):
    config = parse_agents_config(raw)
    wf_env = new_scope()
    h = Harness(
        wf_env=wf_env,
        agents=config,
        llm=ScriptedLLMClient(),
        slack=slack or FakeSlack(),
        notion=make_fake_notion(),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        tool_outputs=InMemoryToolOutputStore(),
        agent_store=seeded_agent_store(config),
    )
    h.agent_store = type(h.agent_store).seeded(config, by=OWNER)
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
    )
    h.acts = acts
    worker = Worker(
        wf_env.client,
        task_queue=wf_env.task_queue,
        workflows=[ConversationWorkflow, TaskWorkflow],
        activities=acts.all(),
        interceptors=[wf_env.children],
    )
    async with worker:
        try:
            yield h
        finally:
            await wf_env.finish()
            await replace_server_if_stuck()


def say(event_id: str, user: str, channel: str, kind: str, text: str = "help me") -> InboundMessage:
    return InboundMessage(event_id, user, text, channel, THREAD, kind)


async def converse(h, agent_id: str, msg: InboundMessage):
    return await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, agent_id, msg.channel, msg.thread_ts),
        id=ids.conversation_id(TENANT, msg.channel, msg.thread_ts),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )
