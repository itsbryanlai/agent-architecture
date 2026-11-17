"""One table of field limits, chosen so an accepted value fits Notion and the card.

Notion takes 2000 characters per text item; a Slack section field takes 2000
and a section's text 3000. The card shows the escaped value, and a comment is
posted with a signature, so each limit leaves room for both.
"""

import uuid

import pytest

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.models import (
    EDITABLE_FIELDS,
    ApprovalDecision,
    ConversationRef,
    EditRequest,
    InboundMessage,
    LLMResponse,
    TaskSpec,
    ToolCall,
    TurnContext,
    check_field,
)
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, DB_ID, TENANT, eventually, title_prop

NONCE = "n-1"
CHANNEL = "C_GENERAL"
THREAD = "1700000000.000100"


@pytest.mark.parametrize(
    ("tool", "field"),
    [("create_item", "title"), ("create_item", "details"), ("add_comment", "comment")],
)
def test_check_field_at_and_over_the_limit(tool, field):
    limit = EDITABLE_FIELDS[tool][field]
    assert check_field(tool, field, "x" * limit) is None
    err = check_field(tool, field, "x" * (limit + 1))
    assert err is not None and str(limit) in err and field in err


def test_check_field_counts_the_escaped_card_text():
    # 399 "&" fit raw (<= 1900) but encode to 1995 characters, over the card's 1990.
    assert check_field("create_item", "details", "&" * 398) is None
    err = check_field("create_item", "details", "&" * 399)
    assert err is not None and "&" in err and "<" in err


def _ctx() -> TurnContext:
    return TurnContext(TENANT, "it-helper", 1, ALICE, CHANNEL, THREAD, "channel")


async def _start(h, spec: TaskSpec):
    handle = await h.client.start_workflow(
        TaskWorkflow.run, spec, id=ids.task_id(TENANT, str(uuid.uuid4())), task_queue=TASK_QUEUE
    )

    async def awaiting():
        return await handle.query(TaskWorkflow.state) == "awaiting_approval"

    await eventually(awaiting, timeout=20)
    return handle


async def _converse(h, call: ToolCall):
    h.llm.queue(
        LLMResponse(text=None, tool_calls=[call], stop_reason="tool_use"),
        LLMResponse(text="ok", tool_calls=[], stop_reason="end_turn"),
    )
    msg = InboundMessage("Ev1", ALICE, "do it", CHANNEL, THREAD, "channel")
    await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "it-helper", CHANNEL, THREAD),
        id=ids.conversation_id(TENANT, CHANNEL, THREAD),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )
    await eventually(lambda: len(h.llm.calls) >= 2)
    return h.llm.calls[1]["messages"][-1]["content"][0]


@pytest.mark.parametrize(
    ("tool", "field", "limit"),
    [("create_item", "details", 1900), ("add_comment", "comment", 1900)],
)
async def test_first_draft_over_limit_is_an_error_the_model_can_act_on(harness, tool, field, limit):
    """Fails if a first draft over a field limit gets a card instead of an error the model can fix."""
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    args = (
        {"title": "T", "details": "d" * 1901}
        if tool == "create_item"
        else {
            "item": page["url"],
            "comment": "c" * 1901,
        }
    )
    result = await _converse(h, ToolCall("tu", tool, args))
    assert result["is_error"] is True
    assert field in result["content"] and str(limit) in result["content"]
    assert [p for p in h.slack.posts() if p["blocks"]] == []


async def test_first_draft_at_limit_gets_a_card(harness):
    h = harness
    result = await _converse(
        h, ToolCall("tu", "create_item", {"title": "t" * 150, "details": "d" * 1900})
    )
    assert not result.get("is_error")
    await eventually(lambda: [p for p in h.slack.posts() if p["blocks"]])


async def test_details_at_the_limit_reach_notion(harness):
    """Fails if a details text at the limit is cut or dropped on its way into the Notion page body."""
    h = harness
    spec = TaskSpec(
        _ctx(),
        "create_item",
        {"title": "t" * 150, "details": "d" * 1900, "database_id": DB_ID},
        NONCE,
    )
    handle = await _start(h, spec)
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))

    async def watching():
        return await handle.query(TaskWorkflow.state) == "watching"

    await eventually(watching, timeout=20)
    [body] = h.notion.bodies.values()
    assert body[0]["paragraph"]["rich_text"][0]["text"]["content"] == "d" * 1900


async def test_comment_at_the_limit_with_signature_reaches_notion(harness):
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    spec = TaskSpec(_ctx(), "add_comment", {"item": page["url"], "comment": "c" * 1900}, NONCE)
    handle = await _start(h, spec)
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))

    assert await handle.result() == "done"
    [comment] = next(iter(h.notion.comments.values()))
    assert len(comment["rich_text"][0]["text"]["content"]) <= 2000


async def test_edit_over_the_limit_is_ignored_and_at_the_limit_accepted(harness):
    h = harness
    spec = TaskSpec(
        _ctx(), "create_item", {"title": "T", "details": "d", "database_id": DB_ID}, NONCE
    )
    handle = await _start(h, spec)

    await handle.signal(
        TaskWorkflow.edit, EditRequest(NONCE, ALICE, {"title": "T", "details": "d" * 1901})
    )

    await handle.signal(
        TaskWorkflow.edit, EditRequest(NONCE, ALICE, {"title": "T", "details": "d" * 1900})
    )

    async def changed():
        return (await handle.query(TaskWorkflow.draft)).nonce != NONCE

    await eventually(changed, timeout=20)
    # Signals are handled in order: the over-limit edit was refused, or it would
    # have replaced the nonce and the at-limit edit would have been ignored.
    assert (await handle.query(TaskWorkflow.draft)).fields["details"] == "d" * 1900
