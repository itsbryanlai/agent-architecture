import asyncio
import dataclasses
from datetime import timedelta

import pytest
from temporalio.client import WorkflowExecutionStatus

from agentos import ids
from agentos.audit import canonical_sha256
from agentos.config import TASK_QUEUE
from agentos.models import ApprovalDecision, EditRequest, ExternalUpdate, TaskSpec
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, BOB, DB_ID, TENANT, eventually, title_prop
from tests.helpers.tasks import NONCE, approve, spec, start_task, thread_posts, wait_state


async def _nonce_changed(handle):
    d = await handle.query(TaskWorkflow.draft)
    return d if d.nonce != NONCE else None


async def test_approval_from_wrong_user_or_nonce_ignored(harness):
    h = harness
    handle = await start_task(h)

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, BOB, True))
    await handle.signal(TaskWorkflow.decision, ApprovalDecision("wrong-nonce", ALICE, True))
    # Settle point: an edit is accepted only while the task still awaits approval.
    await handle.signal(
        TaskWorkflow.edit, EditRequest(NONCE, ALICE, {"title": "Settled", "details": "d"})
    )
    await eventually(lambda: _nonce_changed(handle))

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


async def test_declined_approval_is_audited(harness):
    """Fails if a declined approval closes without an audit row."""
    h = harness
    handle = await start_task(h)

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, False))
    assert await handle.result() == "cancelled"

    [event] = [e for e in h.audit.events if e.kind == "approval_closed"]
    assert event.tool == "declined"
    assert event.requester_user_id == ALICE
    assert event.approver_user_id == ALICE
    assert event.agent_id == "it-helper"
    assert event.agent_version == 1
    assert event.task_id == handle.id
    assert event.input_sha256 == canonical_sha256(spec().payload)


async def test_an_audit_outage_still_closes_the_card(harness):
    """Fails if an audit outage stops the card from closing."""
    h = harness
    handle = await start_task(h)
    # Down through the activity's whole retry ladder (RETRY.maximum_attempts).
    h.audit.fail_next_records(5)

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, False))
    assert await handle.result() == "cancelled"

    assert h.slack.updates()[-1]["text"] == "Cancelled: New monitor"
    assert [e for e in h.audit.events if e.kind == "approval_closed"] == []


async def test_expired_approval_is_audited(harness):
    """Fails if an expired approval closes without an audit row."""
    h = harness
    handle = await start_task(h)

    assert await handle.result() == "expired"

    [event] = [e for e in h.audit.events if e.kind == "approval_closed"]
    assert event.tool == "expired"
    assert event.requester_user_id == ALICE
    assert event.approver_user_id is None
    assert event.task_id == handle.id
    assert event.input_sha256 == canonical_sha256(spec().payload)


async def test_an_approved_write_records_approval_and_write_not_approval_closed(harness):
    """Fails if an approved write loses its approval/write rows or gains a closed-approval row."""
    h = harness
    handle = await start_task(h)

    await approve(h, handle)
    await wait_state(handle, "watching")

    kinds = [e.kind for e in h.audit.events]
    assert "approval" in kinds
    assert "write" in kinds
    assert "approval_closed" not in kinds


async def test_a_declined_stored_draft_hashes_the_revision_the_card_named(harness):
    """Fails if a declined stored draft hashes the carried payload or the latest draft, not the named revision."""
    h = harness
    task_id = ids.task_id(TENANT, "stored-decline")
    first = {"title": "Stored monitor", "details": "Flickers too.", "database_id": DB_ID}
    await h.acts.content.put_draft(TENANT, task_id, 1, first)
    stored = dataclasses.replace(
        spec(), payload={}, draft_ref=task_id, revision=1, database_id=DB_ID
    )
    handle = await h.client.start_workflow(
        TaskWorkflow.run, stored, id=task_id, task_queue=TASK_QUEUE
    )
    await wait_state(handle, "awaiting_approval")

    # An edit stores revision 2 with a new nonce; the decline binds to that card.
    await h.acts.content.put_inbound(
        TENANT,
        "edit:sd1",
        {
            "kind": "edit",
            "task_id": task_id,
            "fields": {"title": "Edited monitor", "details": "Edited details."},
        },
        delivered=True,
    )
    await handle.signal(TaskWorkflow.edit, EditRequest(NONCE, ALICE, {}, "edit:sd1"))

    async def edited():
        view = await handle.query(TaskWorkflow.draft)
        return view if view.revision == 2 else None

    nonce2 = (await eventually(edited)).nonce
    # A later revision exists, but it is not the one the declined card named.
    await h.acts.content.put_draft(
        TENANT, task_id, 3, {"title": "Sneaked", "details": "x", "database_id": DB_ID}
    )

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(nonce2, ALICE, False))
    assert await handle.result() == "cancelled"

    [event] = [e for e in h.audit.events if e.kind == "approval_closed"]
    named = await h.acts.content.get_draft(TENANT, task_id, 2)
    assert event.input_sha256 == canonical_sha256(named)
    assert event.input_sha256 != canonical_sha256({})
    assert event.event_key == ids.audit_approval_closed_key(task_id, nonce2)


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
