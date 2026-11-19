"""my_tasks: the asker's tasks from the platform's store, fresh in the task's own thread."""

from datetime import UTC, datetime, timedelta

import pytest
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.models import ConversationRef, InboundMessage, LLMResponse, ToolCall, TurnContext
from agentos.tasks import TaskRecord
from agentos.workflows import POKE_GAP
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, BOB, DB_ID, TENANT, default_schema, eventually
from tests.helpers.agents import republish
from tests.helpers.tasks import approve, spec, start_task, wait_state

CHANNEL = "C_GENERAL"
THREAD = "1700000000.000100"
NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("workflow_state", "listed"),
    [
        ("drafted", "awaiting_approval"),
        ("awaiting_approval", "awaiting_approval"),
        ("executing", "filed"),
        ("watching", "watching"),
        ("done", "done"),
        ("failed", "failed"),
        ("cancelled", "cancelled"),
        ("expired", "cancelled"),
    ],
)
async def test_the_listed_state_of_each_workflow_state(harness, workflow_state, listed):
    """Fails if a task in a workflow state is listed in another state."""
    h = harness
    task_id = ids.task_id(TENANT, f"map-{workflow_state}")

    await ActivityEnvironment().run(
        h.acts.record_task_state, spec(), task_id, workflow_state, "To do", NOW
    )

    row = await h.acts.tasks.get(TENANT, task_id)
    assert row.state == listed


@pytest.mark.parametrize(("grants", "offered"), [(None, True), ({}, False)])
async def test_my_tasks_is_offered_only_to_an_agent_with_a_write_grant(harness, grants, offered):
    """Fails if an agent without a database grant is offered my_tasks, or one with a grant is not."""
    h = harness
    if grants is not None:
        await republish(h.agent_store, "it-helper", lambda d: d.update(grants=grants))
    agent = await h.agent_store.published(TENANT, "it-helper")
    ctx = TurnContext(TENANT, "it-helper", agent.version, ALICE, CHANNEL, THREAD, "channel")
    h.llm.queue(LLMResponse(text="ok", tool_calls=[], stop_reason="end_turn"))

    await ActivityEnvironment().run(
        h.acts.call_llm, ctx, ids.conversation_id(TENANT, CHANNEL, THREAD)
    )

    names = [t["name"] for t in h.llm.calls[0]["tools"]]
    assert ("my_tasks" in names) is offered


async def _inbound(h, event_id: str, text: str, channel: str, thread: str, kind: str) -> None:
    await h.acts.content.put_inbound(
        TENANT,
        event_id,
        {
            "kind": "message",
            "agent_id": "it-helper",
            "user_id": ALICE,
            "text": text,
            "channel": channel,
            "thread_ts": thread,
            "channel_kind": kind,
        },
    )


async def _start_ask(h, event_id: str, text: str, channel: str, thread: str, kind: str = "channel"):
    await _inbound(h, event_id, text, channel, thread, kind)
    return await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "it-helper", channel, thread),
        id=ids.conversation_id(TENANT, channel, thread),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[
            InboundMessage(event_id, ALICE, "", channel, thread, kind, text_stored=True)
        ],
    )


def _my_tasks_turn(tool_use_id: str, reply: str) -> tuple[LLMResponse, LLMResponse]:
    return (
        LLMResponse(
            text=None, tool_calls=[ToolCall(tool_use_id, "my_tasks", {})], stop_reason="tool_use"
        ),
        LLMResponse(text=reply, tool_calls=[], stop_reason="end_turn"),
    )


async def test_my_tasks_in_the_task_thread_pokes_the_watch_and_respects_the_gap(harness):
    """Fails if a question in the task's thread lists a stale status, or forces a poll sooner than POKE_GAP."""
    h = harness
    schema = default_schema()
    schema["Status"]["options"] = ["To do", "In progress", "Done"]
    h.notion.set_schema(DB_ID, schema)
    handle = await start_task(h)
    await approve(h, handle)
    await wait_state(handle, "watching")
    page = h.notion.all_pages(DB_ID)[0]

    # The watching report lands, then the first poll: the watch has a last poll the
    # gap is measured from.
    async def recorded():
        return await h.acts.tasks.get(TENANT, handle.id) is not None

    await eventually(recorded)
    await h.wf_env.sleep(timedelta(minutes=16))
    await eventually(lambda: h.notion.count("retrieve_page") >= 1)
    conv = ids.conversation_id(TENANT, CHANNEL, THREAD)

    # The poke's poll is owed, but not before POKE_GAP: the answer waits for it and
    # carries the fresh status. Skip the gap in short steps so the turn's own steps
    # run between them; the wait inside the tool sees the report land.
    h.notion.set_status(page["id"], "Status", "In progress")
    h.llm.queue(*_my_tasks_turn("tu_t", "It is in progress."))
    await _start_ask(h, "EvP1", "where is my request?", CHANNEL, THREAD)
    for _ in range(30):
        if len(h.llm.calls) >= 2:
            break
        await h.wf_env.sleep(timedelta(seconds=10))
    await eventually(lambda: len(h.llm.calls) >= 2, timeout=30)
    output = (await h.tool_outputs.get_many(TENANT, conv, ["tu_t"]))["tu_t"][0]
    assert 'state="watching"' in output
    assert 'status="In progress"' in output

    # A second question right after that poll must not force another within the gap.
    # The wait gives up quickly here; the gap has not elapsed, so no poll may run.
    h.acts.refresh_wait_seconds = 2
    polls = h.notion.count("retrieve_page")
    with h.wf_env.auto_time_skipping_disabled():
        h.llm.queue(*_my_tasks_turn("tu_t2", "Still in progress."))
        await _inbound(h, "EvP2", "and now?", CHANNEL, THREAD, "channel")
        await h.client.get_workflow_handle(conv).signal(
            ConversationWorkflow.message,
            InboundMessage("EvP2", ALICE, "", CHANNEL, THREAD, "channel", text_stored=True),
        )
        await eventually(
            lambda: any(u["text"] == "Still in progress." for u in h.slack.updates()), timeout=30
        )
        assert h.notion.count("retrieve_page") == polls
    assert await handle.query(TaskWorkflow.state) == "watching"

    # The poke is still pending: once the gap elapses, the watch answers it with one poll.
    await h.wf_env.sleep(POKE_GAP + timedelta(seconds=5))
    await eventually(lambda: h.notion.count("retrieve_page") == polls + 1, timeout=10)


async def test_my_tasks_in_a_fresh_dm_answers_from_the_store_without_a_notion_search(harness):
    """Fails if a fresh DM's my_tasks searches Notion, or lists a task the asker did not request."""
    h = harness
    handle = await start_task(h)
    await approve(h, handle)
    await wait_state(handle, "watching")
    page = h.notion.all_pages(DB_ID)[0]
    await h.acts.tasks.record(
        TaskRecord(
            tenant_id=TENANT,
            task_id=ids.task_id(TENANT, "bobs"),
            requester_user_id=BOB,
            agent_id="it-helper",
            title="Bob's laptop",
            url="https://www.notion.so/bob",
            page_id="p-bob",
            state="watching",
            last_status="To do",
            updated_at=NOW,
            channel=CHANNEL,
            thread_ts="1700000000.000900",
        )
    )
    queries = h.notion.count("query_database")
    h.llm.queue(*_my_tasks_turn("tu_t", "Here it is."))

    await _start_ask(h, "EvD1", "where is my laptop request?", "D_ALICE", "1700000000.000900", "im")

    await eventually(lambda: len(h.llm.calls) >= 2, timeout=30)
    conv = ids.conversation_id(TENANT, "D_ALICE", "1700000000.000900")
    output = (await h.tool_outputs.get_many(TENANT, conv, ["tu_t"]))["tu_t"][0]
    assert "New monitor" in output
    assert 'state="watching"' in output
    assert page["url"] in output
    assert "Bob's laptop" not in output
    assert h.notion.count("query_database") == queries
