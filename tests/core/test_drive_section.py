"""A Drive section-replace task: the tool's offer, the approved replacement and its revision
lock, the moved-Doc and republish refusals, the draft validation and the edit (D-50)."""

import json
import uuid

import pytest
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.connectors.drive import (
    DOC_MOVED_TEXT,
    SECTION_SHAPE_CHANGED_TEXT,
    DriveReader,
    heading_matches,
    section_bounds,
)
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
from tests.helpers.conversation import CHANNEL, THREAD, start_stored_conversation
from tests.helpers.drive import (
    DRIVE_SECTION,
    SECTION_HEADING,
    add_section_doc,
    section_grant_block,
)

NONCE = "nonce-section"
DOC_LINK = f"https://drive.google.com/file/d/{DRIVE_SECTION}"
NEW_TEXT = "The VPN is back up."


def ctx(version: int = 1) -> TurnContext:
    return TurnContext(TENANT, "it-helper", version, ALICE, CHANNEL, THREAD, "channel")


def cards(h):
    return [p for p in h.slack.posts() if p["blocks"]]


def thread_posts(h, needle: str):
    return [p for p in h.slack.posts() if p["thread_ts"] and needle in p["text"]]


async def grant_section(h) -> int:
    """Publish it-helper with the VPN guide's Status section grant, and serve the Doc."""
    record = await republish(
        h.agent_store,
        "it-helper",
        lambda d: d["grants"].setdefault("drive", {}).update(sections=[section_grant_block()]),
    )
    add_section_doc(h.google)
    return record.version


async def start_replace_task(h, text: str = NEW_TEXT, heading: str = SECTION_HEADING) -> object:
    """A section-replace task as a turn starts it now: the draft stored, the spec a reference.

    The draft carries the resolved section (before text, read revision and content range) as
    the prepare step would have frozen it."""
    task_id = ids.task_id(TENANT, str(uuid.uuid4()))
    grant = section_grant_block()
    doc = await DriveReader(h.google, "agent@x.iam.gserviceaccount.com").read_doc(DRIVE_SECTION)
    start, end, before = section_bounds(doc.paragraphs, heading_matches(doc.paragraphs, heading)[0])
    await h.acts.content.put_draft(
        TENANT,
        task_id,
        1,
        {
            "section_id": DRIVE_SECTION,
            "doc_id": DRIVE_SECTION,
            "heading": heading,
            "text": text,
            "page_id": DRIVE_SECTION,
            "before": before,
            "revision_id": doc.revision_id,
            "content_start": start,
            "content_end": end,
            "item_title": grant["name"],
            "item_url": DOC_LINK,
        },
    )
    spec = TaskSpec(
        ctx(), "replace_section", {}, NONCE, tool="replace_section", draft_ref=task_id, revision=1
    )
    return await h.client.start_workflow(TaskWorkflow.run, spec, id=task_id, task_queue=TASK_QUEUE)


async def wait_state(handle, expected: str):
    async def check():
        return await handle.query(TaskWorkflow.state) == expected

    await eventually(check, timeout=20)


@pytest.mark.parametrize(
    ("grants", "offered"),
    [("section granted", True), ("drive folder only", False), ("notion only", False)],
)
async def test_the_section_tool_is_offered_only_to_an_agent_with_a_granted_section(
    dharness, grants, offered
):
    """Fails if the model is offered replace_section without a granted section, or an agent
    granted a section is not offered it with the section's heading listed."""
    h = dharness
    version = 1
    if grants == "section granted":
        version = await grant_section(h)
    elif grants == "notion only":
        record = await republish(
            h.agent_store, "it-helper", lambda d: d["grants"].pop("drive", None)
        )
        version = record.version
    h.llm.queue(LLMResponse(text="Hi", tool_calls=[], stop_reason="end_turn"))

    await ActivityEnvironment().run(h.acts.call_llm, ctx(version), f"{TENANT}:conv:C1:1.1")

    tools = {t["name"] for t in h.llm.calls[0]["tools"]}
    assert ("replace_section" in tools) is offered
    if offered:
        description = next(
            t["description"] for t in h.llm.calls[0]["tools"] if t["name"] == "replace_section"
        )
        assert "VPN guide" in description and "Status" in description


async def test_an_approved_replace_writes_the_section_and_the_card_previews_before_after(
    dharness,
):
    """Fails if an approved replace writes no section, writes it twice, removes the heading or
    the text under the next heading, or the card does not preview the Doc, the heading and the
    exact before and after text that is written."""
    h = dharness
    await grant_section(h)
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[
                ToolCall("tu_1", "replace_section", {"section": DRIVE_SECTION, "text": NEW_TEXT})
            ],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Drafted.", tool_calls=[], stop_reason="end_turn"),
    )
    await start_stored_conversation(h, "Ev1", "update the VPN status section")
    [card] = await eventually(lambda: cards(h))

    assert card["channel"] == f"D-{ALICE}"
    text = json.dumps(card["blocks"], ensure_ascii=False)
    assert f"wants to replace the section *{SECTION_HEADING}* in *<{DOC_LINK}|VPN guide>*" in text
    assert "*Before*" in text and "The VPN is down." in text
    assert "*New text*" in text and NEW_TEXT in text
    actions = next(b for b in card["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    handle = h.client.get_workflow_handle(value["task_id"])
    await wait_state(handle, "awaiting_approval")

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(value["nonce"], ALICE, True))
    assert await handle.result() == "done"

    paras = h.google.docs[DRIVE_SECTION]["paragraphs"]
    assert [(p["text"], p["style"]) for p in paras] == [
        ("Status\n", "HEADING_1"),
        (NEW_TEXT + "\n", "NORMAL_TEXT"),
        ("Other\n", "HEADING_1"),
        ("Keep this.\n", "NORMAL_TEXT"),
    ]
    assert len(h.google.doc_replace_calls) == 1
    [told] = thread_posts(h, f"Replaced a section of VPN guide ({DOC_LINK}).")
    assert told["text"] == f"Replaced a section of VPN guide ({DOC_LINK})."
    task_events = [e for e in h.audit.events if e.task_id == value["task_id"]]
    assert [(e.kind, e.tool, e.target) for e in task_events] == [
        ("approval", "replace_section", DRIVE_SECTION),
        ("write", "replace_section", DRIVE_SECTION),
    ]
    assert task_events[1].outcome == "ok"


async def test_a_moved_doc_refuses_the_replace_with_nothing_written(dharness):
    """Fails if a Doc someone else edited between approval and the write still gets the section
    replaced, or the refusal does not tell the requester the document changed and to draft again."""
    h = dharness
    await grant_section(h)
    handle = await start_replace_task(h)
    await wait_state(handle, "awaiting_approval")
    # A collaborator edits the Doc: the read revision no longer holds, and the section's text
    # is not the approved text.
    h.google.set_doc_revision(DRIVE_SECTION, "9")
    h.google.docs[DRIVE_SECTION]["paragraphs"][1]["text"] = "Someone else changed this.\n"

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))

    assert await handle.result() == "failed"
    # Nothing written: the collaborator's text and revision stand (the batchUpdate the revision
    # lock refused made no change).
    paras = h.google.docs[DRIVE_SECTION]["paragraphs"]
    assert paras[1]["text"] == "Someone else changed this.\n"
    assert h.google.docs[DRIVE_SECTION]["revision_id"] == "9"
    [told] = thread_posts(h, "I couldn't replace the section in 'VPN guide'")
    assert DOC_MOVED_TEXT in told["text"]


async def test_a_republished_heading_refuses_the_replace_with_nothing_written(dharness):
    """Fails if a definition republished with another heading between draft and approval still
    replaces the section, or the refusal does not tell the requester to ask the creator to
    republish."""
    h = dharness
    await grant_section(h)
    handle = await start_replace_task(h)
    await wait_state(handle, "awaiting_approval")
    await republish(
        h.agent_store,
        "it-helper",
        lambda d: d["grants"]["drive"].update(
            sections=[{**section_grant_block(), "heading": "Overview"}]
        ),
    )

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))

    assert await handle.result() == "failed"
    assert h.google.doc_replace_calls == []
    [told] = thread_posts(h, "I couldn't replace the section in 'VPN guide'")
    assert SECTION_SHAPE_CHANGED_TEXT in told["text"]


@pytest.mark.parametrize(
    ("tool_input", "needle"),
    [
        ({"section": DRIVE_SECTION, "text": "   "}, "needs a non-empty 'text'"),
        ({"section": "  ", "text": NEW_TEXT}, "needs a non-empty 'section'"),
        ({"section": "drive-nope-0001", "text": NEW_TEXT}, "not been granted a section"),
        ({"section": DRIVE_SECTION, "text": "x" * 2001}, "the limit is 2000"),
    ],
    ids=["empty-text", "empty-section", "unknown-section", "text-too-long"],
)
async def test_bad_section_input_refuses_the_draft_so_the_model_can_correct(
    dharness, tool_input, needle
):
    """Fails if a draft with an empty text or section, an ungranted section, or an over-long
    text still posts a card, instead of telling the model what to correct."""
    h = dharness
    await grant_section(h)
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[ToolCall("tu_1", "replace_section", tool_input)],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Let me fix that.", tool_calls=[], stop_reason="end_turn"),
    )

    await start_stored_conversation(h, "Ev1", "update the VPN status section")

    async def model_told():
        return len(h.llm.calls) >= 2 and needle in json.dumps(h.llm.calls[1]["messages"])

    await eventually(model_told, timeout=20)
    assert cards(h) == []
    assert h.google.doc_replace_calls == []


async def test_an_edited_replace_writes_the_edited_text_after_a_fresh_approval(dharness):
    """Fails if a section card cannot be edited, the edit changes the read revision, or the
    section writes the pre-edit text or runs without a fresh approval."""
    h = dharness
    await grant_section(h)
    handle = await start_replace_task(h)
    await wait_state(handle, "awaiting_approval")
    edit_id = ids.edit_event_id(uuid.uuid4().hex)
    await h.acts.content.put_inbound(
        TENANT,
        edit_id,
        {"kind": "edit", "task_id": handle.id, "fields": {"text": "The VPN is degraded."}},
        delivered=True,
    )

    await handle.signal(TaskWorkflow.edit, EditRequest(NONCE, ALICE, {}, edit_id))

    async def redrawn():
        updates = h.slack.updates()
        if updates and "The VPN is degraded." in json.dumps(updates[-1]["blocks"]):
            return updates[-1]
        return None

    card = await eventually(redrawn)
    text = json.dumps(card["blocks"], ensure_ascii=False)
    assert "Edited — review and approve again" in text
    # The edit keeps the same read revision: the before text is unchanged.
    assert "The VPN is down." in text
    actions = next(b for b in card["blocks"] if b["type"] == "actions")
    nonce = json.loads(actions["elements"][0]["value"])["nonce"]
    assert nonce != NONCE

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, True))
    assert await handle.result() == "done"

    paras = h.google.docs[DRIVE_SECTION]["paragraphs"]
    assert paras[1]["text"] == "The VPN is degraded.\n"
    assert paras[0]["text"] == "Status\n"
    assert len(h.google.doc_replace_calls) == 1
