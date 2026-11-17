"""A task the user was told is in hand ends with a message, not silently.

While watching, a transient Notion failure keeps the task watching; a
non-retryable one (404, item moved out of the grant) or a trashed item posts one
"stopped watching" line and ends. In the approval phase a terminal failure
posts one failure line and ends cleanly. The watch poll carries only IDs.
"""

import uuid
from datetime import timedelta

import pytest

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.connectors.notion import FakeNotFound, normalize_id
from agentos.models import ApprovalDecision, EditRequest, TaskSpec, TurnContext
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, DB_ID, TENANT, eventually

NONCE = "n-1"
SECRET = "details-that-must-not-reach-history"


def _spec() -> TaskSpec:
    ctx = TurnContext(TENANT, "it-helper", 1, ALICE, "C_GENERAL", "1700000000.000100", "channel")
    payload = {"title": "New monitor", "details": SECRET, "database_id": DB_ID}
    return TaskSpec(ctx, "create_item", payload, NONCE)


async def _start(h):
    handle = await h.client.start_workflow(
        TaskWorkflow.run, _spec(), id=ids.task_id(TENANT, str(uuid.uuid4())), task_queue=TASK_QUEUE
    )
    await _state(handle, "awaiting_approval")
    return handle


async def _state(handle, expected: str):
    async def check():
        return await handle.query(TaskWorkflow.state) == expected

    await eventually(check, timeout=20)


async def _watching(h):
    handle = await _start(h)
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))
    await _state(handle, "watching")
    return handle, h.notion.all_pages(DB_ID)[0]["id"]


def _thread(h, needle: str):
    return [p for p in h.slack.posts() if p["thread_ts"] and needle in p["text"]]


def _fail(obj, name: str, error: Exception, times: int | None = None):
    """Make ``obj.name`` raise ``error`` (``times`` times, or always)."""
    original = getattr(obj, name)
    state = {"left": times}

    async def wrapper(*args, **kwargs):
        if state["left"] is None or state["left"] > 0:
            if state["left"] is not None:
                state["left"] -= 1
            raise error
        return await original(*args, **kwargs)

    setattr(obj, name, wrapper)


# --- watching ---------------------------------------------------------------------


async def test_transient_notion_failure_keeps_watching(harness):
    h = harness
    handle, page_id = await _watching(h)
    _fail(h.notion, "retrieve_page", TimeoutError("notion is down"), times=12)

    await h.wf_env.sleep(timedelta(minutes=16))  # first poll fails past its retry budget
    assert await handle.query(TaskWorkflow.state) == "watching"
    assert _thread(h, "stopped watching") == []

    h.notion.set_status(page_id, "Status", "Done")
    await h.wf_env.sleep(timedelta(hours=1))

    assert await handle.result() == "done"
    assert len(_thread(h, "was marked Done")) == 1


async def test_non_retryable_failure_stops_watching_with_one_line(harness):
    h = harness
    handle, _ = await _watching(h)
    _fail(h.notion, "retrieve_page", FakeNotFound("moved out of the grant"))

    await h.wf_env.sleep(timedelta(minutes=16))

    await _state(handle, "done")
    [line] = _thread(h, "stopped watching")
    assert "New monitor" in line["text"]


async def test_trashed_item_stops_watching(harness):
    h = harness
    handle, page_id = await _watching(h)
    h.notion.databases[normalize_id(DB_ID)][page_id]["in_trash"] = True

    await h.wf_env.sleep(timedelta(minutes=16))

    await _state(handle, "done")  # well before the 30-day watch limit
    assert len(_thread(h, "stopped watching")) == 1


async def test_watch_poll_sends_ids_not_details(harness):
    h = harness
    handle, _ = await _watching(h)
    await h.wf_env.sleep(timedelta(minutes=50))

    polls = []
    async for event in handle.fetch_history_events():
        attrs = event.activity_task_scheduled_event_attributes
        if attrs.activity_type.name == "get_notion_item":
            polls.append(b"".join(p.data for p in attrs.input.payloads))
    assert len(polls) >= 3
    assert all(SECRET.encode() not in raw for raw in polls)


# --- approval phase ---------------------------------------------------------------


async def test_card_failure_posts_one_failure_line_and_ends(harness):
    h = harness
    _fail(h.slack, "open_dm", RuntimeError("slack is down"))
    handle = await h.client.start_workflow(
        TaskWorkflow.run, _spec(), id=ids.task_id(TENANT, str(uuid.uuid4())), task_queue=TASK_QUEUE
    )

    assert await handle.result() == "failed"
    [line] = _thread(h, "couldn't")
    assert "New monitor" in line["text"]
    assert [p for p in h.slack.posts() if p["blocks"]] == []


async def test_refresh_failure_posts_one_failure_line_and_ends(harness):
    h = harness
    handle = await _start(h)
    _fail(h.slack, "update_message", RuntimeError("slack is down"))

    await handle.signal(
        TaskWorkflow.edit, EditRequest(NONCE, ALICE, {"title": "Edited", "details": "d"})
    )

    assert await handle.result() == "failed"
    assert len(_thread(h, "couldn't")) == 1
    assert h.notion.count("create_page") == 0


async def test_card_update_failure_does_not_stop_a_cancel(harness):
    h = harness
    handle = await _start(h)
    _fail(h.slack, "update_message", RuntimeError("slack is down"))

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, False))

    assert await handle.result() == "cancelled"


async def test_reminder_failure_does_not_stop_the_wait(harness):
    h = harness
    handle = await _start(h)
    _fail(h.slack, "post_message", RuntimeError("slack is down"))
    _fail(h.slack, "update_message", RuntimeError("slack is down"))

    assert await handle.result() == "expired"


@pytest.fixture(autouse=True)
def _quiet_workflow_logs(caplog):
    caplog.set_level("ERROR", logger="temporalio.workflow")
    caplog.set_level("ERROR", logger="temporalio.activity")
