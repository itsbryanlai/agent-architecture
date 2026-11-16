import asyncio
from datetime import timedelta

import pytest
from temporalio.client import WorkflowExecutionStatus

from agentos.models import ApprovalDecision, ExternalUpdate, TaskSpec
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, BOB, DB_ID, title_prop
from tests.helpers.tasks import NONCE, approve, spec, start_task, thread_posts, wait_state


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
    h.notion.fail_after_write()

    await approve(h, handle)
    await wait_state(handle, "watching")

    assert len(h.notion.all_pages(DB_ID)) == 1
    assert h.notion.count("create_page") == 1
    filed = thread_posts(h, "Filed: New monitor")
    assert len(filed) == 1
    card_updates = [u for u in h.slack.updates() if u["text"].startswith("Submitted")]
    assert len(card_updates) == 1


@pytest.mark.parametrize("tool", ["create_item", "add_comment"])
async def test_cancel(harness, tool):
    """Fails if cancelling the approval still writes the page or the comment."""
    h = harness
    if tool == "create_item":
        handle = await start_task(h)
    else:
        page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
        base = spec()
        payload = {"item": page["id"], "comment": "Any update on this?"}
        handle = await start_task(h, TaskSpec(base.ctx, "add_comment", payload, NONCE))

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, False))

    assert await handle.result() == "cancelled"
    assert await handle.query(TaskWorkflow.state) == "cancelled"
    assert h.notion.count("create_page") == 0
    assert h.notion.count("create_comment") == 0
    assert len(h.notion.all_pages(DB_ID)) == (0 if tool == "create_item" else 1)


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


async def test_long_watch_continues_as_new_and_marks_done_once(harness):
    """Fails if a watch that outgrows its history never starts a new run, or posts Done twice."""
    h = harness
    handle, page_id = await _approved_and_watching(h)
    first_run = handle.result_run_id

    # A month of 15-minute polls cannot fit in one history.
    await h.wf_env.sleep(timedelta(days=10))
    h.notion.set_status(page_id, "Status", "Done")
    await h.wf_env.sleep(timedelta(minutes=15))

    assert await handle.result() == "done"
    first = await h.client.get_workflow_handle(handle.id, run_id=first_run).describe()
    assert first.status == WorkflowExecutionStatus.CONTINUED_AS_NEW
    assert len(thread_posts(h, "was marked Done")) == 1
    assert len(thread_posts(h, "Filed: New monitor")) == 1


async def test_stale_notice_is_not_repeated_after_a_hop(harness):
    """Fails if the new run forgets it already said the item has not moved."""
    h = harness
    handle, _ = await _approved_and_watching(h)

    await h.wf_env.sleep(timedelta(days=10))

    first = await h.client.get_workflow_handle(handle.id, run_id=handle.result_run_id).describe()
    assert first.status == WorkflowExecutionStatus.CONTINUED_AS_NEW
    assert len(thread_posts(h, "No movement on 'New monitor'")) == 1


async def test_a_burst_of_pokes_polls_at_most_once_per_gap(harness):
    """Fails if every poke from a webhook burst reads the page again."""
    h = harness
    handle, page_id = await _approved_and_watching(h)
    before = h.notion.count("retrieve_page")

    with h.wf_env.auto_time_skipping_disabled():
        for _ in range(6):
            await handle.signal(
                TaskWorkflow.external_update,
                ExternalUpdate(page_id=page_id, source="notion_webhook"),
            )
    await h.wf_env.sleep(timedelta(minutes=5))

    assert 1 <= h.notion.count("retrieve_page") - before <= 2


@pytest.fixture(autouse=True)
def _quiet_workflow_logs(caplog):
    caplog.set_level("ERROR", logger="temporalio.workflow")
