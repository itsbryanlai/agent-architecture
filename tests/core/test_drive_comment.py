"""A Drive comment task: the tool's offer, the grant boundary, the approved write (D-47)."""

import json
import uuid

import pytest
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.connectors.google_iam import MIME_DOC, DriveSection
from agentos.models import (
    ApprovalDecision,
    EditRequest,
    LLMResponse,
    TaskSpec,
    ToolCall,
    TurnContext,
)
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, TENANT, eventually
from tests.helpers.agents import republish
from tests.helpers.budgets import NOV
from tests.helpers.conversation import CHANNEL, THREAD, start_stored_conversation
from tests.helpers.drive import DRIVE_DOC

NONCE = "nonce-drive"
OUTSIDE_FOLDER = "other-folder-0001"
OUTSIDE_DOC = "outside-doc-0001"
GUIDE_LINK = f"https://drive.google.com/file/d/{DRIVE_DOC}"


def ctx(version: int = 1) -> TurnContext:
    return TurnContext(TENANT, "it-helper", version, ALICE, CHANNEL, THREAD, "channel")


def cards(h):
    return [p for p in h.slack.posts() if p["blocks"]]


def thread_posts(h, needle: str):
    return [p for p in h.slack.posts() if p["thread_ts"] and needle in p["text"]]


async def start_comment_task(h, ref: str, comment: str = "Please review.") -> object:
    """A comment task as a turn starts it now: the draft stored, the spec a reference."""
    task_id = ids.task_id(TENANT, str(uuid.uuid4()))
    await h.acts.content.put_draft(TENANT, task_id, 1, {"item": ref, "comment": comment})
    spec = TaskSpec(
        ctx(), "add_comment", {}, NONCE, tool="comment_on_file", draft_ref=task_id, revision=1
    )
    return await h.client.start_workflow(TaskWorkflow.run, spec, id=task_id, task_queue=TASK_QUEUE)


async def wait_state(handle, expected: str):
    async def check():
        return await handle.query(TaskWorkflow.state) == expected

    await eventually(check, timeout=20)


@pytest.mark.parametrize(
    ("grants", "offered"),
    [("drive granted", True), ("notion only", False)],
)
async def test_the_drive_comment_tool_is_offered_only_to_an_agent_with_drive_grants(
    dharness, grants, offered
):
    """Fails if the model is offered comment_on_file without a Drive grant, or an agent
    with Drive grants is not offered it beside its Notion tools."""
    h = dharness
    version = 1
    if grants == "notion only":
        record = await republish(
            h.agent_store, "it-helper", lambda d: d["grants"].pop("drive", None)
        )
        version = record.version
    h.llm.queue(LLMResponse(text="Hi", tool_calls=[], stop_reason="end_turn"))

    await ActivityEnvironment().run(h.acts.call_llm, ctx(version), f"{TENANT}:conv:C1:1.1")

    tools = {t["name"] for t in h.llm.calls[0]["tools"]}
    assert ("comment_on_file" in tools) is offered
    assert "add_comment" in tools


@pytest.mark.parametrize("case", ["outside the grants", "grant removed after approval"])
async def test_a_drive_comment_needs_a_live_grant_before_the_card_and_before_the_write(
    dharness, case
):
    """Fails if a file outside the grants gets a card, or a grant removed after approval
    still lets the comment post."""
    h = dharness
    ref = DRIVE_DOC
    if case == "outside the grants":
        h.google.add_drive_folder(OUTSIDE_FOLDER, "Other folder")
        h.google.add_drive_file(
            OUTSIDE_DOC, "Outside", MIME_DOC, OUTSIDE_FOLDER, [DriveSection("", "text")]
        )
        ref = OUTSIDE_DOC
    handle = await start_comment_task(h, ref)
    if case == "grant removed after approval":
        await wait_state(handle, "awaiting_approval")
        await republish(h.agent_store, "it-helper", lambda d: d["grants"].pop("drive", None))
        await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))

    assert await handle.result() == "failed"

    if case == "outside the grants":
        assert cards(h) == []
        [told] = thread_posts(h, "I can't comment on that file")
        assert "folder I have access to" in told["text"]
    else:
        assert len(cards(h)) == 1
        [told] = thread_posts(h, "I couldn't comment on 'Guide'")
        assert "couldn't find a Google Drive file" in told["text"]
    assert h.google.comment_calls == []
    assert not h.google.drive_comments.get(ref, [])


async def test_the_drive_switch_refuses_an_approved_drive_comment(dharness):
    """Fails if an approved Drive comment posts while the Drive connector switch is off."""
    h = dharness
    handle = await start_comment_task(h, DRIVE_DOC)
    await wait_state(handle, "awaiting_approval")
    assert await h.switches.switch_off(TENANT, "connector", "drive", "U_ADMIN", NOV)

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))

    assert await handle.result() == "failed"
    assert h.google.comment_calls == []
    [told] = thread_posts(h, "I couldn't comment on 'Guide'")
    assert "Drive is turned off by an admin" in told["text"]
    assert [(e.kind, e.outcome) for e in h.audit.events if e.task_id == handle.id] == [
        ("approval", "ok"),
        ("write", "error"),
    ]


async def test_an_edited_drive_comment_posts_the_edited_text_after_a_fresh_approval(dharness):
    """Fails if a Drive comment card cannot be edited (it has no database grant to look
    up), or the comment posts the pre-edit text or runs without a fresh approval."""
    h = dharness
    handle = await start_comment_task(h, DRIVE_DOC)
    await wait_state(handle, "awaiting_approval")
    edit_id = ids.edit_event_id(uuid.uuid4().hex)
    await h.acts.content.put_inbound(
        TENANT,
        edit_id,
        {"kind": "edit", "task_id": handle.id, "fields": {"comment": "Edited: check step 2."}},
        delivered=True,
    )

    await handle.signal(TaskWorkflow.edit, EditRequest(NONCE, ALICE, {}, edit_id))

    async def redrawn():
        updates = h.slack.updates()
        if updates and "Edited: check step 2." in json.dumps(updates[-1]["blocks"]):
            return updates[-1]
        return None

    card = await eventually(redrawn)
    text = json.dumps(card["blocks"], ensure_ascii=False)
    assert "Edited — review and approve again" in text
    actions = next(b for b in card["blocks"] if b["type"] == "actions")
    nonce = json.loads(actions["elements"][0]["value"])["nonce"]
    assert nonce != NONCE

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, True))
    assert await handle.result() == "done"

    [comment] = h.google.drive_comments[DRIVE_DOC]
    assert comment["content"].startswith("Edited: check step 2.")


async def test_an_approved_drive_comment_posts_once_with_the_marker_and_tells_the_thread(
    dharness,
):
    """Fails if an approved Drive comment posts without the marker, posts twice, or the
    thread does not name the file and its link; the card previews the file and the text."""
    h = dharness
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[
                ToolCall(
                    "tu_1", "comment_on_file", {"file": GUIDE_LINK, "comment": "Fix the VPN steps."}
                )
            ],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Drafted.", tool_calls=[], stop_reason="end_turn"),
    )
    await start_stored_conversation(h, "Ev1", "comment on the guide")
    [card] = await eventually(lambda: cards(h))

    assert card["channel"] == f"D-{ALICE}"
    text = json.dumps(card["blocks"])
    assert f"wants to comment on *<{GUIDE_LINK}|Guide>*" in text
    assert "Fix the VPN steps." in text
    actions = next(b for b in card["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    handle = h.client.get_workflow_handle(value["task_id"])
    await wait_state(handle, "awaiting_approval")

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(value["nonce"], ALICE, True))
    assert await handle.result() == "done"

    [comment] = h.google.drive_comments[DRIVE_DOC]
    assert comment["content"].startswith("Fix the VPN steps.")
    assert "ref " in comment["content"] and "IT helper" in comment["content"]
    assert comment["author"]["me"] is True
    [told] = thread_posts(h, f"Commented on Guide ({GUIDE_LINK}).")
    assert told["text"] == f"Commented on Guide ({GUIDE_LINK})."
    assert [u["text"] for u in h.slack.updates()][-1] == "Commented: Guide"
    task_events = [e for e in h.audit.events if e.task_id == value["task_id"]]
    assert [(e.kind, e.tool, e.target) for e in task_events] == [
        ("approval", "comment_on_file", DRIVE_DOC),
        ("write", "comment_on_file", DRIVE_DOC),
    ]
    assert task_events[1].outcome == "ok"
