import asyncio
import uuid
from datetime import timedelta

import pytest

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.models import ApprovalDecision, ExternalUpdate, TaskSpec, TurnContext
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, BOB, DB_ID, TENANT, eventually

NONCE = "nonce-123"


def spec() -> TaskSpec:
    ctx = TurnContext(
        tenant_id=TENANT,
        agent_id="it-helper",
        agent_version=1,
        requester_user_id=ALICE,
        channel="C_GENERAL",
        thread_ts="1700000000.000100",
        channel_kind="channel",
    )
    return TaskSpec(
        ctx=ctx,
        tool_name="create_item",
        payload={"title": "New monitor", "details": "Mine flickers.", "database_id": DB_ID},
        approval_nonce=NONCE,
    )


async def start_task(h):
    handle = await h.client.start_workflow(
        TaskWorkflow.run,
        spec(),
        id=ids.task_id(TENANT, str(uuid.uuid4())),
        task_queue=TASK_QUEUE,
    )
    await wait_state(handle, "awaiting_approval")
    return handle


async def wait_state(handle, expected: str):
    async def check():
        return await handle.query(TaskWorkflow.state) == expected

    await eventually(check, timeout=20)


def thread_posts(h, needle: str):
    return [p for p in h.slack.posts() if p["thread_ts"] and needle in p["text"]]


async def approve(h, handle):
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))


async def test_approval_from_wrong_user_or_nonce_ignored(harness):
    h = harness
    handle = await start_task(h)

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, BOB, True))
    await handle.signal(TaskWorkflow.decision, ApprovalDecision("wrong-nonce", ALICE, True))
    await asyncio.sleep(0.5)

    assert await handle.query(TaskWorkflow.state) == "awaiting_approval"
    assert h.notion.count("create_page") == 0
    assert h.notion.all_pages(DB_ID) == []


async def test_approve_creates_exactly_once_and_survives_retry(harness):
    h = harness
    handle = await start_task(h)
    h.notion.fail_next_create_after_write()

    await approve(h, handle)
    await wait_state(handle, "watching")

    assert len(h.notion.all_pages(DB_ID)) == 1
    assert h.notion.count("create_page") == 1
    filed = thread_posts(h, "Filed: New monitor")
    assert len(filed) == 1
    card_updates = [u for u in h.slack.updates() if u["text"].startswith("Submitted")]
    assert len(card_updates) == 1


async def test_cancel(harness):
    h = harness
    handle = await start_task(h)

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, False))

    assert await handle.result() == "cancelled"
    assert await handle.query(TaskWorkflow.state) == "cancelled"
    assert h.notion.all_pages(DB_ID) == []
    assert h.notion.count("create_page") == 0


async def test_expiry_with_reminder(harness):
    h = harness
    handle = await start_task(h)

    # Awaiting the result lets the test server skip the full 72 hours.
    assert await handle.result() == "expired"

    assert len(thread_posts(h, "Reminder")) == 1
    assert len(thread_posts(h, "expired without approval")) == 1
    assert [u["text"] for u in h.slack.updates()] == ["Expired: New monitor"]
    assert h.notion.count("create_page") == 0


async def _approved_and_watching(h):
    handle = await start_task(h)
    await approve(h, handle)
    await wait_state(handle, "watching")
    page = h.notion.all_pages(DB_ID)[0]
    return handle, page["id"]


async def test_follow_up_by_polling(harness):
    h = harness
    handle, page_id = await _approved_and_watching(h)

    h.notion.set_status(page_id, "Status", "Done")
    await h.wf_env.sleep(timedelta(minutes=15))

    assert await handle.result() == "done"
    assert len(thread_posts(h, "was marked Done")) == 1


async def test_follow_up_by_external_update(harness):
    h = harness
    handle, page_id = await _approved_and_watching(h)

    h.notion.set_status(page_id, "Status", "Done")
    with h.wf_env.auto_time_skipping_disabled():
        await handle.signal(
            TaskWorkflow.external_update, ExternalUpdate(page_id=page_id, source="notion_webhook")
        )
        result = await asyncio.wait_for(handle.result(), timeout=10)

    assert result == "done"
    assert len(thread_posts(h, "was marked Done")) == 1


async def test_stale_notice_once(harness):
    h = harness
    handle, _ = await _approved_and_watching(h)

    await h.wf_env.sleep(timedelta(days=4))

    assert len(thread_posts(h, "No movement on 'New monitor' in 3 days.")) == 1
    assert await handle.query(TaskWorkflow.state) == "watching"


@pytest.fixture(autouse=True)
def _quiet_workflow_logs(caplog):
    caplog.set_level("ERROR", logger="temporalio.workflow")
