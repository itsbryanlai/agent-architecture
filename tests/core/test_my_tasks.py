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
from tests.helpers.drive import DRIVE_DOC
from tests.helpers.tasks import ClosesTaskOnLookup, approve, spec, start_task, wait_state

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


@pytest.mark.parametrize(
    ("grants", "offered"),
    [
        (None, True),
        ({}, False),
        ({"drive": {"files": [{"id": DRIVE_DOC, "name": "Guide"}]}}, True),
    ],
    ids=["notion database", "no grants", "drive only"],
)
async def test_my_tasks_is_offered_only_to_an_agent_with_a_write_grant(harness, grants, offered):
    """Fails if an agent without a write grant is offered my_tasks, or one with a Notion
    database or Drive grant is not."""
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


async def test_an_invented_my_tasks_call_without_a_write_grant_is_a_tool_error(harness):
    """Fails if an agent without a database grant lists tasks from a my_tasks call it was never offered."""
    h = harness
    await republish(h.agent_store, "it-helper", lambda d: d.update(grants={}))
    await h.acts.tasks.record(
        TaskRecord(
            tenant_id=TENANT,
            task_id=ids.task_id(TENANT, "alice-1"),
            requester_user_id=ALICE,
            agent_id="it-helper",
            title="New monitor",
            url="https://www.notion.so/monitor",
            page_id="p-1",
            state="watching",
            last_status="To do",
            updated_at=NOW,
            channel=CHANNEL,
            thread_ts=THREAD,
        )
    )
    agent = await h.agent_store.published(TENANT, "it-helper")
    ctx = TurnContext(TENANT, "it-helper", agent.version, ALICE, CHANNEL, THREAD, "channel")
    conv = ids.conversation_id(TENANT, CHANNEL, THREAD)

    result = await ActivityEnvironment().run(
        h.acts.run_read_tool, ctx, conv, ToolCall("tu_x", "my_tasks", {}), 1
    )

    assert result.is_error and result.stored
    stored, is_error = (await h.tool_outputs.get_many(TENANT, conv, ["tu_x"]))["tu_x"]
    assert is_error
    assert "New monitor" not in stored


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


async def _task_recorded(h, task_id: str) -> bool:
    return await h.acts.tasks.get(TENANT, task_id) is not None


async def _watching_task_after_first_poll(h):
    """A watching task whose first scheduled poll has landed: the record is the watching
    report, stale by more than POKE_GAP, and the watch's last poll is a minute old."""
    schema = default_schema()
    schema["Status"]["options"] = ["To do", "In progress", "Done"]
    h.notion.set_schema(DB_ID, schema)
    handle = await start_task(h)
    await approve(h, handle)
    await wait_state(handle, "watching")
    await eventually(lambda: _task_recorded(h, handle.id))
    await h.wf_env.sleep(timedelta(minutes=16))
    await eventually(lambda: h.notion.count("retrieve_page") >= 1)
    return handle


async def test_a_question_in_the_task_thread_pokes_the_watch_and_lists_the_fresh_status(harness):
    """Fails if a question in the task's thread lists a stale status instead of the poke's fresh report."""
    h = harness
    handle = await _watching_task_after_first_poll(h)
    page = h.notion.all_pages(DB_ID)[0]
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
    assert await handle.query(TaskWorkflow.state) == "watching"


async def test_a_question_right_after_a_report_does_not_poke_the_watch(harness):
    """Fails if a question asked within POKE_GAP of the task's last report pokes the watch again."""
    h = harness
    handle = await _watching_task_after_first_poll(h)
    page = h.notion.all_pages(DB_ID)[0]
    conv = ids.conversation_id(TENANT, CHANNEL, THREAD)
    h.notion.set_status(page["id"], "Status", "In progress")
    h.llm.queue(*_my_tasks_turn("tu_t", "It is in progress."))
    await _start_ask(h, "EvP1", "where is my request?", CHANNEL, THREAD)
    await eventually(lambda: len(h.llm.calls) >= 2, timeout=30)

    # The poke-answered poll just reported: a second question right after is fresh
    # already, so it sends no poke and leaves none pending. No virtual time may pass
    # on its own between the two turns, or the next scheduled poll would muddy the count.
    h.acts.refresh_wait_seconds = 2
    with h.wf_env.auto_time_skipping_disabled():
        polls = h.notion.count("retrieve_page")
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
        await h.wf_env.sleep(POKE_GAP + timedelta(seconds=5))
        assert h.notion.count("retrieve_page") == polls
    assert await handle.query(TaskWorkflow.state) == "watching"


async def test_two_my_tasks_calls_in_one_turn_poke_and_wait_only_once(harness):
    """Fails if a second my_tasks call in the same turn refreshes the thread's task again."""
    h = harness
    schema = default_schema()
    schema["Status"]["options"] = ["To do", "In progress", "Done"]
    h.notion.set_schema(DB_ID, schema)
    handle = await start_task(h)
    await approve(h, handle)
    await wait_state(handle, "watching")
    await eventually(lambda: _task_recorded(h, handle.id))
    # Half a minute past the first poll: the record is stale, so the first call pokes,
    # but the watch answers the poke only after POKE_GAP from that poll. The wait gives
    # up quickly, so the record stays stale for the second call.
    await h.wf_env.sleep(timedelta(minutes=15, seconds=30))
    await eventually(lambda: h.notion.count("retrieve_page") >= 1)
    conv = ids.conversation_id(TENANT, CHANNEL, THREAD)

    with h.wf_env.auto_time_skipping_disabled():
        h.acts.refresh_wait_seconds = 2
        h.llm.queue(
            LLMResponse(
                text=None, tool_calls=[ToolCall("tu_1", "my_tasks", {})], stop_reason="tool_use"
            ),
            LLMResponse(
                text=None, tool_calls=[ToolCall("tu_2", "my_tasks", {})], stop_reason="tool_use"
            ),
            LLMResponse(text="Both checked.", tool_calls=[], stop_reason="end_turn"),
        )
        await _start_ask(h, "EvO1", "where is my request?", CHANNEL, THREAD)
        await eventually(
            lambda: any(u["text"] == "Both checked." for u in h.slack.updates()), timeout=30
        )
        outputs = await h.tool_outputs.get_many(TENANT, conv, ["tu_1", "tu_2"])
        assert not outputs["tu_1"][1] and not outputs["tu_2"][1]
        assert "New monitor" in outputs["tu_1"][0] and "New monitor" in outputs["tu_2"][0]

        # One poke reached the task, and one wait ran in the turn.
        task_history = await handle.fetch_history()
        pokes = sum(
            1
            for e in task_history.events
            if e.HasField("workflow_execution_signaled_event_attributes")
            and e.workflow_execution_signaled_event_attributes.signal_name == "external_update"
        )
        assert pokes == 1
        conv_history = await h.client.get_workflow_handle(conv).fetch_history()
        waits = sum(
            1
            for e in conv_history.events
            if e.HasField("activity_task_scheduled_event_attributes")
            and e.activity_task_scheduled_event_attributes.activity_type.name == "wait_task_refresh"
        )
        assert waits == 1

        # The one poke is still pending: once the gap elapses, the watch answers it
        # with exactly one poll.
        polls = h.notion.count("retrieve_page")
        assert polls == 1
        await h.wf_env.sleep(POKE_GAP + timedelta(seconds=5))
        assert h.notion.count("retrieve_page") == polls + 1
    assert await handle.query(TaskWorkflow.state) == "watching"


async def test_a_task_that_closed_between_the_lookup_and_the_poke_still_answers(harness):
    """Fails if a task that closed between the lookup and the poke fails the turn instead of answering from the store."""
    h = harness
    handle = await start_task(h)
    await approve(h, handle)
    await wait_state(handle, "watching")
    await eventually(lambda: _task_recorded(h, handle.id))
    h.acts.tasks = ClosesTaskOnLookup(h.acts.tasks, h, handle)
    h.llm.queue(*_my_tasks_turn("tu_t", "Here is where it stands."))

    await _start_ask(h, "EvC1", "where is my request?", CHANNEL, THREAD)

    await eventually(lambda: len(h.llm.calls) >= 2, timeout=30)
    conv = ids.conversation_id(TENANT, CHANNEL, THREAD)
    output, is_error = (await h.tool_outputs.get_many(TENANT, conv, ["tu_t"]))["tu_t"]
    assert not is_error
    assert 'state="done"' in output
    assert 'status="Done"' in output
    await eventually(
        lambda: any(u["text"] == "Here is where it stands." for u in h.slack.updates()), timeout=30
    )
    assert await handle.result() == "done"


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
