"""A Drive create-from-template task: the tool's offer, the approved Doc and its marker,
the blank validation, the republish refusal and the edit (D-49)."""

import json
import uuid

import pytest
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.connectors.drive import DOC_APP_PROPERTY_KEY, DOC_SHAPE_CHANGED_TEXT
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
    DRIVE_TEMPLATE,
    TEMPLATE_BLANKS,
    TEMPLATE_FOLDER,
    add_template_tree,
    template_grant_block,
)

NONCE = "nonce-template"
FOLDER_LINK = f"https://drive.google.com/drive/folders/{TEMPLATE_FOLDER}"
TITLE = "Offer for Bob"
VALUES = {"name": "Bob", "role": "engineer"}


def ctx(version: int = 1) -> TurnContext:
    return TurnContext(TENANT, "it-helper", version, ALICE, CHANNEL, THREAD, "channel")


def cards(h):
    return [p for p in h.slack.posts() if p["blocks"]]


def thread_posts(h, needle: str):
    return [p for p in h.slack.posts() if p["thread_ts"] and needle in p["text"]]


async def grant_template(h) -> int:
    """Publish it-helper with the offer-template grant, and serve the template tree."""
    record = await republish(
        h.agent_store,
        "it-helper",
        lambda d: d["grants"].setdefault("drive", {}).update(templates=[template_grant_block()]),
    )
    add_template_tree(h.google)
    return record.version


async def start_create_task(h, values: dict | None = None) -> object:
    """A create-from-template task as a turn starts it now: the draft stored, the spec a reference."""
    task_id = ids.task_id(TENANT, str(uuid.uuid4()))
    await h.acts.content.put_draft(
        TENANT,
        task_id,
        1,
        {
            "template_id": DRIVE_TEMPLATE,
            "folder_id": TEMPLATE_FOLDER,
            "blanks": list(TEMPLATE_BLANKS),
            "title": TITLE,
            "values": dict(values or VALUES),
        },
    )
    spec = TaskSpec(
        ctx(),
        "create_doc",
        {},
        NONCE,
        tool="create_doc_from_template",
        draft_ref=task_id,
        revision=1,
    )
    return await h.client.start_workflow(TaskWorkflow.run, spec, id=task_id, task_queue=TASK_QUEUE)


async def wait_state(handle, expected: str):
    async def check():
        return await handle.query(TaskWorkflow.state) == expected

    await eventually(check, timeout=20)


@pytest.mark.parametrize(
    ("grants", "offered"),
    [("template granted", True), ("drive folder only", False), ("notion only", False)],
)
async def test_the_template_tool_is_offered_only_to_an_agent_with_a_granted_template(
    dharness, grants, offered
):
    """Fails if the model is offered create_doc_from_template without a granted template, or
    an agent granted a template is not offered it with the template's blanks listed."""
    h = dharness
    version = 1
    if grants == "template granted":
        version = await grant_template(h)
    elif grants == "notion only":
        record = await republish(
            h.agent_store, "it-helper", lambda d: d["grants"].pop("drive", None)
        )
        version = record.version
    h.llm.queue(LLMResponse(text="Hi", tool_calls=[], stop_reason="end_turn"))

    await ActivityEnvironment().run(h.acts.call_llm, ctx(version), f"{TENANT}:conv:C1:1.1")

    tools = {t["name"] for t in h.llm.calls[0]["tools"]}
    assert ("create_doc_from_template" in tools) is offered
    if offered:
        description = next(
            t["description"]
            for t in h.llm.calls[0]["tools"]
            if t["name"] == "create_doc_from_template"
        )
        assert "Offer template" in description and "name, role" in description


async def test_an_approved_create_writes_one_filled_doc_and_the_card_previews_it(dharness):
    """Fails if an approved create copies no Doc, copies it twice, leaves a blank unfilled,
    leaves the copy unmarked, or the card does not preview the title, the folder and every
    filled blank that is written."""
    h = dharness
    await grant_template(h)
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[
                ToolCall(
                    "tu_1",
                    "create_doc_from_template",
                    {"template": DRIVE_TEMPLATE, "title": TITLE, "values": dict(VALUES)},
                )
            ],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Drafted.", tool_calls=[], stop_reason="end_turn"),
    )
    await start_stored_conversation(h, "Ev1", "draft an offer for bob")
    [card] = await eventually(lambda: cards(h))

    assert card["channel"] == f"D-{ALICE}"
    text = json.dumps(card["blocks"])
    assert f"wants to create *{TITLE}* in <{FOLDER_LINK}|Offer letters>" in text
    assert "*name*" in text and "Bob" in text and "*role*" in text and "engineer" in text
    actions = next(b for b in card["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    handle = h.client.get_workflow_handle(value["task_id"])
    await wait_state(handle, "awaiting_approval")

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(value["nonce"], ALICE, True))
    assert await handle.result() == "done"

    [new_id] = h.google.drive_children[TEMPLATE_FOLDER]
    assert h.google.drive_files[new_id].name == TITLE
    assert h.google.drive_content[new_id][0].text == "Dear Bob,\nYou are hired as engineer."
    assert len(h.google.drive_app_properties[new_id][DOC_APP_PROPERTY_KEY]) == 64
    assert len(h.google.copy_calls) == 1
    assert len(h.google.fill_calls) == 1
    link = f"https://drive.google.com/file/d/{new_id}"
    [told] = thread_posts(h, f"Created {TITLE} ({link}).")
    assert told["text"] == f"Created {TITLE} ({link})."
    task_events = [e for e in h.audit.events if e.task_id == value["task_id"]]
    assert [(e.kind, e.tool) for e in task_events] == [
        ("approval", "create_doc_from_template"),
        ("write", "create_doc_from_template"),
    ]
    assert task_events[0].target == DRIVE_TEMPLATE
    assert task_events[1].target == new_id
    assert task_events[1].outcome == "ok"


@pytest.mark.parametrize(
    ("values", "needle"),
    [
        ({"name": "Bob"}, "needs a value for blank 'role'"),
        ({"name": "Bob", "role": "engineer", "bogus": "x"}, "'bogus' is not a blank"),
        ({"name": "Bob", "role": "  "}, "'role' needs a non-empty value"),
    ],
    ids=["missing-blank", "unknown-blank", "empty-value"],
)
async def test_bad_blanks_refuse_the_draft_so_the_model_can_correct(dharness, values, needle):
    """Fails if a draft missing a blank, naming an unknown blank or giving an empty value
    still posts a card, instead of telling the model what to correct."""
    h = dharness
    await grant_template(h)
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[
                ToolCall(
                    "tu_1",
                    "create_doc_from_template",
                    {"template": DRIVE_TEMPLATE, "title": TITLE, "values": values},
                )
            ],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Let me fix that.", tool_calls=[], stop_reason="end_turn"),
    )

    await start_stored_conversation(h, "Ev1", "draft an offer for bob")

    async def model_told():
        return len(h.llm.calls) >= 2 and needle in json.dumps(h.llm.calls[1]["messages"])

    await eventually(model_told, timeout=20)
    assert cards(h) == []
    assert h.google.copy_calls == []


@pytest.mark.parametrize(
    "case", ["republished blanks", "republished folder"], ids=["blanks", "folder"]
)
async def test_a_republished_grant_refuses_the_create_with_nothing_written(dharness, case):
    """Fails if a definition republished with other blanks or another folder between draft
    and approval still copies the Doc, or the refusal does not tell the requester to ask the
    creator to republish."""
    h = dharness
    await grant_template(h)
    handle = await start_create_task(h)
    await wait_state(handle, "awaiting_approval")
    if case == "republished blanks":
        await republish(
            h.agent_store,
            "it-helper",
            lambda d: d["grants"]["drive"].update(
                templates=[{**template_grant_block(), "blanks": ["name", "role", "team"]}]
            ),
        )
    else:
        await republish(
            h.agent_store,
            "it-helper",
            lambda d: d["grants"]["drive"].update(
                templates=[{**template_grant_block(), "folder_id": "drive-tmpl-plain-0001"}]
            ),
        )

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))

    assert await handle.result() == "failed"
    assert h.google.drive_children[TEMPLATE_FOLDER] == []
    assert h.google.copy_calls == []
    [told] = thread_posts(h, f"I couldn't create '{TITLE}'")
    assert DOC_SHAPE_CHANGED_TEXT in told["text"]


async def test_an_edited_blank_writes_the_edited_value_after_a_fresh_approval(dharness):
    """Fails if a create card cannot be edited per blank, or the Doc fills the pre-edit
    values or runs without a fresh approval."""
    h = dharness
    await grant_template(h)
    handle = await start_create_task(h)
    await wait_state(handle, "awaiting_approval")
    edit_id = ids.edit_event_id(uuid.uuid4().hex)
    await h.acts.content.put_inbound(
        TENANT,
        edit_id,
        {"kind": "edit", "task_id": handle.id, "fields": {"name": "Bobby", "role": "engineer"}},
        delivered=True,
    )

    await handle.signal(TaskWorkflow.edit, EditRequest(NONCE, ALICE, {}, edit_id))

    async def redrawn():
        updates = h.slack.updates()
        if updates and "Bobby" in json.dumps(updates[-1]["blocks"]):
            return updates[-1]
        return None

    card = await eventually(redrawn)
    text = json.dumps(card["blocks"], ensure_ascii=False)
    assert "Edited — review and approve again" in text
    assert "*name*" in text and "*role*" in text
    actions = next(b for b in card["blocks"] if b["type"] == "actions")
    nonce = json.loads(actions["elements"][0]["value"])["nonce"]
    assert nonce != NONCE

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, True))
    assert await handle.result() == "done"

    [new_id] = h.google.drive_children[TEMPLATE_FOLDER]
    assert h.google.drive_content[new_id][0].text == "Dear Bobby,\nYou are hired as engineer."
