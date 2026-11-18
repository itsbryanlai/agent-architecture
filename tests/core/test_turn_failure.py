"""A failed turn tells the user, keeps history valid and keeps the thread alive."""

from temporalio.client import WorkflowExecutionStatus

from agentos import ids
from agentos.audit import canonical_sha256
from agentos.config import TASK_QUEUE
from agentos.models import ConversationRef, LLMResponse, ToolCall
from agentos.workflows.conversation import TOOL_NOT_COMPLETED, TURN_FAILED, ConversationWorkflow
from tests.conftest import ALICE, TENANT, eventually
from tests.helpers.conversation import (
    CHANNEL,
    THREAD,
    inbound,
    placeholders,
    start_conversation,
    stored_history,
)
from tests.helpers.llm_errors import StatusError
from tests.helpers.turns import tool_result_ids, tool_use_ids


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


async def test_failed_turn_after_tool_use_keeps_history_valid(harness):
    """Fails if a failed turn leaves a tool_use unanswered, or the next message in the thread is not answered."""
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

    async def results_stored() -> bool:
        return tool_result_ids(await h.history.load(TENANT, conv_id)) == ["tu_1"]

    await eventually(results_stored)
    stored = await h.history.load(TENANT, conv_id)
    assert tool_use_ids(stored) == ["tu_1"]

    await handle.signal(ConversationWorkflow.message, inbound("Ev2", "try again"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    assert h.slack.updates()[1]["text"] == "Recovered"
    assert len(placeholders(h)) == 2
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
    stored = await stored_history(h, conv_id, lambda s: tool_result_ids(s) == ["tu_1"])
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


async def test_failure_notice_records_a_notice_event(harness):
    """Fails if a failure notice is shown without an audit row."""
    h = harness
    h.llm.queue(
        StatusError(401),
        LLMResponse(text="Recovered", tool_calls=[], stop_reason="end_turn"),
    )

    handle = await start_conversation(h, inbound("Ev1"))
    await eventually(lambda: h.slack.updates())
    assert h.slack.updates()[0]["text"] == TURN_FAILED
    await handle.signal(ConversationWorkflow.message, inbound("Ev2", "try again"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    conv_id = ids.conversation_id(TENANT, CHANNEL, THREAD)
    [notice] = [e for e in h.audit.events if e.kind == "notice"]
    assert notice.tool == "failure"
    assert notice.agent_id == "it-helper"
    assert notice.agent_version == 1
    assert notice.requester_user_id == ALICE
    assert notice.conversation_id == conv_id
    assert notice.output_sha256 == canonical_sha256(TURN_FAILED)
    assert notice.event_key == ids.audit_notice_key(conv_id, "failure", placeholders(h)[0]["ts"])


async def test_early_failure_notice_records_a_notice_event(harness):
    """Fails if a failure notice posted without a placeholder records no audit row."""
    h = harness
    msg = inbound("Ev1")
    handle = await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "ghost-agent", CHANNEL, THREAD),
        id=ids.conversation_id(TENANT, CHANNEL, THREAD),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )
    await handle.signal(ConversationWorkflow.message, msg)
    await handle.signal(ConversationWorkflow.message, inbound("Ev2-sentinel"))
    await eventually(lambda: len(h.slack.posts()) >= 2)

    async def two_notices():
        found = [e for e in h.audit.events if e.kind == "notice"]
        return found if len(found) == 2 else None

    [first, second] = await eventually(two_notices)
    conv_id = ids.conversation_id(TENANT, CHANNEL, THREAD)
    assert first.event_key == ids.audit_notice_key(conv_id, "failure", "Ev1:err")
    assert second.event_key == ids.audit_notice_key(conv_id, "failure", "Ev2-sentinel:err")
    assert first.tool == "failure"
    assert first.agent_id == "ghost-agent"
    assert first.agent_version == 0
    assert first.requester_user_id == ALICE
    assert first.conversation_id == conv_id
    assert first.output_sha256 == canonical_sha256(TURN_FAILED)
