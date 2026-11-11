"""A failed turn tells the user, keeps history valid and keeps the thread alive."""

from temporalio.client import WorkflowExecutionStatus

from agentos import ids
from agentos.models import LLMResponse, ToolCall
from agentos.workflows.conversation import TOOL_NOT_COMPLETED, TURN_FAILED, ConversationWorkflow
from tests.conftest import TENANT, eventually
from tests.test_conversation_workflow import (
    CHANNEL,
    THREAD,
    inbound,
    placeholders,
    start_conversation,
)
from tests.test_llm_errors import StatusError


def tool_use_ids(messages):
    return [
        b["id"]
        for m in messages
        for b in m["content"]
        if isinstance(b, dict) and b.get("type") == "tool_use"
    ]


def tool_result_ids(messages):
    return [
        b["tool_use_id"]
        for m in messages
        for b in m["content"]
        if isinstance(b, dict) and b.get("type") == "tool_result"
    ]


async def test_failed_turn_replaces_placeholder(harness):
    h = harness
    h.llm.queue(StatusError(401))

    handle = await start_conversation(h, inbound("Ev1"))
    await eventually(lambda: h.slack.updates())

    update = h.slack.updates()[0]
    assert update["text"] == TURN_FAILED
    assert update["ts"] == placeholders(h)[0]["ts"]
    desc = await handle.describe()
    assert desc.status == WorkflowExecutionStatus.RUNNING


async def test_conversation_answers_after_failed_turn(harness):
    h = harness
    h.llm.queue(
        StatusError(401),
        LLMResponse(text="Back again", tool_calls=[], stop_reason="end_turn"),
    )

    handle = await start_conversation(h, inbound("Ev1"))
    await eventually(lambda: len(h.slack.updates()) >= 1)
    await handle.signal(ConversationWorkflow.message, inbound("Ev2", "are you there?"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    assert h.slack.updates()[0]["text"] == TURN_FAILED
    assert h.slack.updates()[1]["text"] == "Back again"
    assert len(placeholders(h)) == 2


async def test_failed_turn_after_tool_use_keeps_history_valid(harness):
    h = harness
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[ToolCall(id="tu_1", name="search_items", input={"query": "laptop"})],
            stop_reason="tool_use",
        ),
        StatusError(401),
        LLMResponse(text="Recovered", tool_calls=[], stop_reason="end_turn"),
    )

    handle = await start_conversation(h, inbound("Ev1", "any laptop requests?"))
    await eventually(lambda: h.slack.updates())
    assert h.slack.updates()[0]["text"] == TURN_FAILED

    conv_id = ids.conversation_id(TENANT, CHANNEL, THREAD)
    stored = await h.history.load(TENANT, conv_id)
    assert tool_use_ids(stored) == ["tu_1"]
    assert tool_result_ids(stored) == ["tu_1"]

    await handle.signal(ConversationWorkflow.message, inbound("Ev2", "try again"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    assert h.slack.updates()[1]["text"] == "Recovered"
    last_call = h.llm.calls[-1]["messages"]
    assert tool_use_ids(last_call) == tool_result_ids(last_call) == ["tu_1"]


async def test_failed_tool_call_closes_pending_tool_use(harness):
    h = harness
    # A ValueError from the Notion fake fails run_read_tool as an ActivityError
    # at once: the retry policy treats ValueError as non-retryable.
    h.notion.fail_next_query(ValueError("boom"))
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[ToolCall(id="tu_1", name="search_items", input={"query": "laptop"})],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Recovered", tool_calls=[], stop_reason="end_turn"),
    )

    handle = await start_conversation(h, inbound("Ev1", "any laptop requests?"))
    await eventually(lambda: h.slack.updates())

    assert h.slack.updates()[0]["text"] == TURN_FAILED
    assert h.slack.updates()[0]["ts"] == placeholders(h)[0]["ts"]
    assert len(h.llm.calls) == 1

    conv_id = ids.conversation_id(TENANT, CHANNEL, THREAD)
    stored = await h.history.load(TENANT, conv_id)
    closing = [
        b
        for m in stored
        for b in m["content"]
        if isinstance(b, dict) and b.get("type") == "tool_result"
    ]
    assert closing == [
        {
            "type": "tool_result",
            "tool_use_id": "tu_1",
            "content": TOOL_NOT_COMPLETED,
            "is_error": True,
        }
    ]

    await handle.signal(ConversationWorkflow.message, inbound("Ev2", "try again"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    assert h.slack.updates()[1]["text"] == "Recovered"
    next_call = h.llm.calls[-1]["messages"]
    assert tool_use_ids(next_call) == tool_result_ids(next_call) == ["tu_1"]
