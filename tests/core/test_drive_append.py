"""A Drive sheet append task: the tool's offer, the approved row and its marker, the
header refusal and the edit (D-48)."""

import json
import uuid

import pytest
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.connectors.drive import HEADER_CHANGED_TEXT, SHEET_METADATA_KEY
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
    DRIVE_SHEET,
    SHEET_COLUMNS,
    SHEET_TAB,
    add_roster_sheet,
    sheet_grant_block,
)

NONCE = "nonce-append"
SHEET_LINK = f"https://drive.google.com/file/d/{DRIVE_SHEET}"
ROW = {"name": "bob", "role": "people"}


def ctx(version: int = 1) -> TurnContext:
    return TurnContext(TENANT, "it-helper", version, ALICE, CHANNEL, THREAD, "channel")


def cards(h):
    return [p for p in h.slack.posts() if p["blocks"]]


def thread_posts(h, needle: str):
    return [p for p in h.slack.posts() if p["thread_ts"] and needle in p["text"]]


async def grant_sheet(h) -> int:
    """Publish it-helper with the Roster sheet grant, and serve its Q1 tab from the fake."""
    record = await republish(
        h.agent_store,
        "it-helper",
        lambda d: d["grants"].setdefault("drive", {}).update(sheets=[sheet_grant_block()]),
    )
    add_roster_sheet(h.google)
    return record.version


async def start_append_task(h, row: dict | None = None) -> object:
    """An append task as a turn starts it now: the draft stored, the spec a reference."""
    task_id = ids.task_id(TENANT, str(uuid.uuid4()))
    await h.acts.content.put_draft(
        TENANT,
        task_id,
        1,
        {
            "sheet_id": DRIVE_SHEET,
            "tab": SHEET_TAB,
            "columns": list(SHEET_COLUMNS),
            "row": dict(row or ROW),
        },
    )
    spec = TaskSpec(
        ctx(), "append_row", {}, NONCE, tool="append_row", draft_ref=task_id, revision=1
    )
    return await h.client.start_workflow(TaskWorkflow.run, spec, id=task_id, task_queue=TASK_QUEUE)


async def wait_state(handle, expected: str):
    async def check():
        return await handle.query(TaskWorkflow.state) == expected

    await eventually(check, timeout=20)


@pytest.mark.parametrize(
    ("grants", "offered"),
    [("sheet granted", True), ("drive folder only", False), ("notion only", False)],
)
async def test_the_append_tool_is_offered_only_to_an_agent_with_a_granted_sheet(
    dharness, grants, offered
):
    """Fails if the model is offered append_row without a granted Sheet, or an agent granted
    a Sheet is not offered it beside its other tools."""
    h = dharness
    version = 1
    if grants == "sheet granted":
        version = await grant_sheet(h)
    elif grants == "notion only":
        record = await republish(
            h.agent_store, "it-helper", lambda d: d["grants"].pop("drive", None)
        )
        version = record.version
    h.llm.queue(LLMResponse(text="Hi", tool_calls=[], stop_reason="end_turn"))

    await ActivityEnvironment().run(h.acts.call_llm, ctx(version), f"{TENANT}:conv:C1:1.1")

    tools = {t["name"] for t in h.llm.calls[0]["tools"]}
    assert ("append_row" in tools) is offered
    if grants != "notion only":
        assert "comment_on_file" in tools
    if offered:
        description = next(
            t["description"] for t in h.llm.calls[0]["tools"] if t["name"] == "append_row"
        )
        assert "Roster" in description and "name, role" in description


async def test_an_approved_append_writes_one_marked_row_and_the_card_previews_it(dharness):
    """Fails if an approved append writes no row, writes it twice, leaves it unmarked, or
    the card does not preview the sheet, the tab and the exact row that is written."""
    h = dharness
    await grant_sheet(h)
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[ToolCall("tu_1", "append_row", {"sheet": DRIVE_SHEET, "row": dict(ROW)})],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Drafted.", tool_calls=[], stop_reason="end_turn"),
    )
    await start_stored_conversation(h, "Ev1", "add bob to the roster")
    [card] = await eventually(lambda: cards(h))

    assert card["channel"] == f"D-{ALICE}"
    text = json.dumps(card["blocks"])
    assert f"wants to append a row to *<{SHEET_LINK}|Roster>*" in text
    assert "Q1" in text
    assert "*name*" in text and "bob" in text and "*role*" in text and "people" in text
    actions = next(b for b in card["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    handle = h.client.get_workflow_handle(value["task_id"])
    await wait_state(handle, "awaiting_approval")

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(value["nonce"], ALICE, True))
    assert await handle.result() == "done"

    rows = h.google.sheets[DRIVE_SHEET][SHEET_TAB]["rows"]
    assert rows == [["name", "role"], ["alice", "engineer"], ["bob", "people"]]
    [meta] = h.google.sheet_metadata[DRIVE_SHEET]
    assert meta["metadataKey"] == SHEET_METADATA_KEY
    assert len(meta["metadataValue"]) == 64
    assert meta["location"]["dimensionRange"]["startIndex"] == 2
    assert len(h.google.append_calls) == 1
    [told] = thread_posts(h, f"Appended a row to Roster ({SHEET_LINK}).")
    assert told["text"] == f"Appended a row to Roster ({SHEET_LINK})."
    task_events = [e for e in h.audit.events if e.task_id == value["task_id"]]
    assert [(e.kind, e.tool, e.target) for e in task_events] == [
        ("approval", "append_row", DRIVE_SHEET),
        ("write", "append_row", DRIVE_SHEET),
    ]
    assert task_events[1].outcome == "ok"


async def test_a_changed_header_refuses_the_append_with_nothing_written(dharness):
    """Fails if a sheet whose header changed since publish still gets the row appended, or
    the refusal does not tell the requester to ask the creator to republish."""
    h = dharness
    await grant_sheet(h)
    handle = await start_append_task(h)
    await wait_state(handle, "awaiting_approval")
    h.google.set_sheet_header(DRIVE_SHEET, SHEET_TAB, ["name", "role", "team"])

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))

    assert await handle.result() == "failed"
    rows = h.google.sheets[DRIVE_SHEET][SHEET_TAB]["rows"]
    assert len(rows) == 2 and rows[1] == ["alice", "engineer"]
    assert h.google.sheet_metadata.get(DRIVE_SHEET, []) == []
    assert h.google.append_calls == []
    [told] = thread_posts(h, "I couldn't append a row to 'Roster'")
    assert HEADER_CHANGED_TEXT in told["text"]


async def test_a_changed_header_refuses_the_draft_so_the_model_can_redraft(dharness):
    """Fails if a sheet whose header changed since publish still drafts a card, instead of
    telling the model to re-draft after the creator republishes."""
    h = dharness
    await grant_sheet(h)
    h.google.set_sheet_header(DRIVE_SHEET, SHEET_TAB, ["name", "role", "team"])
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[ToolCall("tu_1", "append_row", {"sheet": DRIVE_SHEET, "row": dict(ROW)})],
            stop_reason="tool_use",
        ),
        LLMResponse(
            text="The columns changed; ask the creator to republish.",
            tool_calls=[],
            stop_reason="end_turn",
        ),
    )

    await start_stored_conversation(h, "Ev1", "add bob to the roster")

    async def model_told():
        return len(h.llm.calls) >= 2 and HEADER_CHANGED_TEXT in json.dumps(
            h.llm.calls[1]["messages"]
        )

    await eventually(model_told, timeout=20)
    assert cards(h) == []
    assert h.google.append_calls == []


async def test_an_edited_append_row_writes_the_edited_values_after_a_fresh_approval(dharness):
    """Fails if an append card cannot be edited per column, or the row writes the pre-edit
    values or runs without a fresh approval."""
    h = dharness
    await grant_sheet(h)
    handle = await start_append_task(h)
    await wait_state(handle, "awaiting_approval")
    edit_id = ids.edit_event_id(uuid.uuid4().hex)
    await h.acts.content.put_inbound(
        TENANT,
        edit_id,
        {
            "kind": "edit",
            "task_id": handle.id,
            "fields": {"name": "bob edited", "role": "people"},
        },
        delivered=True,
    )

    await handle.signal(TaskWorkflow.edit, EditRequest(NONCE, ALICE, {}, edit_id))

    async def redrawn():
        updates = h.slack.updates()
        if updates and "bob edited" in json.dumps(updates[-1]["blocks"]):
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

    rows = h.google.sheets[DRIVE_SHEET][SHEET_TAB]["rows"]
    assert rows[-1] == ["bob edited", "people"]
