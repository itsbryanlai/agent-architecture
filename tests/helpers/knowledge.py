"""Helpers that index a fake Notion tree and run knowledge-search conversations."""

from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.models import (
    ConversationRef,
    InboundMessage,
    LLMResponse,
    ToolCall,
    TurnContext,
)
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.indexing import IndexingWorkflow
from tests.conftest import (
    ALICE,
    ROOT_A,
    ROOT_B,
    TENANT,
)

SCOPE_A = ids.knowledge_scope(ROOT_A)


SCOPE_B = ids.knowledge_scope(ROOT_B)


CHANNEL = "C_GENERAL"


def plain(page_id: str) -> str:
    return page_id.replace("-", "")


async def index(h, scope, root):
    return await h.client.execute_workflow(
        IndexingWorkflow.run,
        args=[TENANT, scope, plain(root)],
        id=ids.index_workflow_id(TENANT, scope),
        task_queue=TASK_QUEUE,
    )


async def index_both(h):
    await index(h, SCOPE_A, ROOT_A)
    await index(h, SCOPE_B, ROOT_B)


def turn_ctx(agent_id: str) -> TurnContext:
    return TurnContext(
        tenant_id=TENANT,
        agent_id=agent_id,
        agent_version=1,
        requester_user_id=ALICE,
        channel=CHANNEL,
        thread_ts="1.1",
        channel_kind="channel",
    )


async def search(h, agent_id: str, query: str, first_marker: int = 1):
    """Run search_knowledge as an activity; return (output text, result)."""
    # Stored tool output is first-write-wins per tool_use_id, so each search gets its own.
    tool_id = f"tu_s{len(h.tool_outputs.outputs)}"
    call = ToolCall(id=tool_id, name="search_knowledge", input={"query": query})
    result = await ActivityEnvironment().run(
        h.acts.run_read_tool, turn_ctx(agent_id), f"conv-{agent_id}", call, first_marker
    )
    stored = await h.tool_outputs.get_many(TENANT, f"conv-{agent_id}", [tool_id])
    return stored[tool_id][0], result


async def converse(h, agent_id: str, thread: str, text: str = "hello"):
    msg = InboundMessage(f"Ev-{thread}", ALICE, text, CHANNEL, thread, "channel")
    return await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, agent_id, CHANNEL, thread),
        id=ids.conversation_id(TENANT, CHANNEL, thread),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )


def call(tool_id: str, query: str) -> ToolCall:
    return ToolCall(id=tool_id, name="search_knowledge", input={"query": query})


def tool_turn(*calls: ToolCall) -> LLMResponse:
    return LLMResponse(text=None, tool_calls=list(calls), stop_reason="tool_use")


def answer(text: str) -> LLMResponse:
    return LLMResponse(text=text, tool_calls=[], stop_reason="end_turn")


class FakeScheduleHandle:
    def __init__(self, client, schedule_id):
        self.client, self.id = client, schedule_id

    async def update(self, updater):
        self.client.schedules[self.id] = updater(None).schedule
        self.client.log.append(("update", self.id))

    async def delete(self):
        del self.client.schedules[self.id]
        self.client.log.append(("delete", self.id))


class FakeScheduleClient:
    """Stands in for the Temporal client: the time-skipping test server does not
    implement the schedule listing call, so apply_schedules runs against this."""

    def __init__(self, existing=()):
        self.schedules = {i: None for i in existing}
        self.log: list[tuple[str, str]] = []

    async def list_schedules(self):
        async def gen():
            for schedule_id in list(self.schedules):
                yield type("S", (), {"id": schedule_id})()

        return gen()

    async def create_schedule(self, schedule_id, schedule):
        self.schedules[schedule_id] = schedule
        self.log.append(("create", schedule_id))

    def get_schedule_handle(self, schedule_id):
        return FakeScheduleHandle(self, schedule_id)
