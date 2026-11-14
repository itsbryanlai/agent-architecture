"""Partial tool failure, the step limit, lost messages and idle expiry."""

import asyncio
from datetime import timedelta

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_store import seeded_agent_store
from agentos.audit import InMemoryAuditStore
from agentos.config import TASK_QUEUE, Settings, parse_agents_config
from agentos.llm import ScriptedLLMClient
from agentos.models import ConversationRef, LLMResponse, ToolCall
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for
from agentos.workflows.conversation import (
    IDLE_TIMEOUT,
    OUT_OF_STEPS,
    TOOL_NOT_COMPLETED,
    TURN_FAILED,
    ConversationWorkflow,
)
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, BOB, TENANT, agents_dict, eventually, make_fake_notion
from tests.test_conversation_workflow import (
    CHANNEL,
    THREAD,
    inbound,
    placeholders,
    start_conversation,
)
from tests.test_turn_failure import tool_result_ids, tool_use_ids

CONV_ID = ids.conversation_id(TENANT, CHANNEL, THREAD)


def tool_results(messages):
    return {
        b["tool_use_id"]: b
        for m in messages
        for b in m["content"]
        if isinstance(b, dict) and b.get("type") == "tool_result"
    }


def create_call(call_id="tu_c"):
    return ToolCall(
        id=call_id, name="create_item", input={"title": "New monitor", "details": "Flickers."}
    )


def search_call(call_id="tu_s"):
    return ToolCall(id=call_id, name="search_items", input={"query": "laptop"})


async def test_failed_tool_does_not_hide_a_task_that_was_started(harness):
    h = harness
    h.notion.fail_next_query(ValueError("boom"))
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[create_call(), search_call()],
            stop_reason="tool_use",
        ),
    )

    await start_conversation(h, inbound("Ev1", "order a monitor and look for laptops"))
    await eventually(lambda: h.slack.updates())

    assert h.slack.updates()[0]["text"] == TURN_FAILED
    assert [p for p in h.slack.posts() if p["blocks"]], "the approval card went out"
    stored = await h.history.load(TENANT, CONV_ID)
    assert tool_use_ids(stored) == tool_result_ids(stored) == ["tu_c", "tu_s"]
    results = tool_results(stored)
    assert "approval card" in results["tu_c"]["content"]
    assert not results["tu_c"]["is_error"]
    assert results["tu_s"]["content"] == TOOL_NOT_COMPLETED
    assert results["tu_s"]["is_error"] is True


async def test_last_step_does_not_dispatch_tools(harness):
    h = harness
    steps = 6  # max_steps of the test agent
    h.llm.queue(
        *[
            LLMResponse(text=None, tool_calls=[search_call(f"tu_{n}")], stop_reason="tool_use")
            for n in range(steps - 1)
        ],
        LLMResponse(text=None, tool_calls=[create_call("tu_last")], stop_reason="tool_use"),
    )

    await start_conversation(h, inbound("Ev1", "keep going"))
    await eventually(lambda: h.slack.updates())

    assert h.slack.updates()[0]["text"] == OUT_OF_STEPS
    assert not [p for p in h.slack.posts() if p["blocks"]], "no write task was started"
    stored = await h.history.load(TENANT, CONV_ID)
    assert tool_results(stored)["tu_last"]["content"] == "step limit reached"
    assert tool_results(stored)["tu_last"]["is_error"] is True
    assert stored[-1] == {"role": "assistant", "content": [{"type": "text", "text": OUT_OF_STEPS}]}
    assert tool_use_ids(stored) == tool_result_ids(stored)


async def test_agent_removed_from_config_still_gets_a_reply(harness):
    h = harness
    msg = inbound("Ev1")
    await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "ghost-agent", CHANNEL, THREAD),
        id=CONV_ID,
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )
    await eventually(lambda: h.slack.posts())

    post = h.slack.posts()[0]
    assert post["text"] == TURN_FAILED
    assert post["thread_ts"] == THREAD
    assert post["channel"] == CHANNEL
    assert await h.idem.get(TENANT, "Ev1:err") is not None


async def test_failing_placeholder_post_still_gets_a_reply(harness):
    h = harness
    real_post = h.slack.post_message

    async def post(channel, text, thread_ts=None, blocks=None):
        if text == "Thinking…":
            raise ValueError("slack refused")
        return await real_post(channel, text, thread_ts=thread_ts, blocks=blocks)

    h.slack.post_message = post  # type: ignore[method-assign]

    await start_conversation(h, inbound("Ev1"))
    await eventually(lambda: h.slack.posts())

    assert [p["text"] for p in h.slack.posts()] == [TURN_FAILED]
    assert not placeholders(h)


async def test_early_failure_notice_is_posted_once(harness):
    # The error post is idempotent: a second message with the same event ID is
    # dropped as a duplicate and never produces a second notice.
    h = harness
    msg = inbound("Ev1")
    handle = await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "ghost-agent", CHANNEL, THREAD),
        id=CONV_ID,
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )
    await handle.signal(ConversationWorkflow.message, msg)
    await eventually(lambda: h.slack.posts())
    assert len(h.slack.posts()) == 1


async def test_message_signalled_with_the_idle_timeout_is_not_dropped():
    """The idle timer and a new message land in the same workflow task.

    No worker runs while the clock is moved past the idle timeout and the
    message is signalled, so the next task holds both events.
    """
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        agents = parse_agents_config(agents_dict())
        llm = ScriptedLLMClient()
        slack = FakeSlack(emails={ALICE: "alice@example.com", BOB: "bob@example.com"})
        acts = AgentActivities(
            settings=Settings(),
            registry=TenantRegistry({TENANT: runtime_for(agents, slack, make_fake_notion())}),
            history=InMemoryHistoryStore(),
            idem=InMemoryIdempotencyStore(),
            links=InMemoryTaskLinkStore(),
            llm=llm,
            tool_outputs=InMemoryToolOutputStore(),
            audit=InMemoryAuditStore(),
            agents=seeded_agent_store(agents),
        )

        def worker():
            return Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[ConversationWorkflow, TaskWorkflow],
                activities=acts.all(),
                # No sticky queue: the task must not wait for a stopped worker.
                max_cached_workflows=0,
            )

        llm.queue(
            LLMResponse(text="first", tool_calls=[], stop_reason="end_turn"),
            LLMResponse(text="second", tool_calls=[], stop_reason="end_turn"),
        )
        async with worker():
            handle = await env.client.start_workflow(
                ConversationWorkflow.run,
                ConversationRef(TENANT, "it-helper", CHANNEL, THREAD),
                id=CONV_ID,
                task_queue=TASK_QUEUE,
                start_signal="message",
                start_signal_args=[inbound("Ev1")],
            )
            await eventually(lambda: slack.updates())

        skip = asyncio.create_task(env.sleep(IDLE_TIMEOUT + timedelta(hours=1)))
        await asyncio.sleep(2)
        await handle.signal(ConversationWorkflow.message, inbound("Ev2", "still there?"))

        skip.cancel()
        async with worker():
            await eventually(lambda: len(slack.updates()) >= 2)
        assert slack.updates()[1]["text"] == "second"
    finally:
        await env.shutdown()
