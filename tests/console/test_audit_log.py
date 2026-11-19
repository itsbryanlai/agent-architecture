"""The audit log: one event per action, with IDs and hashes and no content."""

import uuid

import pytest

from agentos import ids
from agentos.audit import (
    canonical_sha256,
    event_json,
)
from agentos.config import TASK_QUEUE
from agentos.connectors.notion import FakeNotFound
from agentos.models import (
    TURN_FAILED,
    ApprovalDecision,
    AuditEvent,
    ConversationRef,
    InboundMessage,
    LLMResponse,
    TaskSpec,
    ToolCall,
    TurnContext,
)
from agentos.workflows.conversation import EMPTY_MESSAGE_REPLY, ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, DB_ID, TENANT, eventually, title_prop
from tests.helpers.front_door import ANNOUNCEMENT, front_door_dict, running

CHANNEL = "C_GENERAL"
THREAD = "1700000000.000100"
CONV_ID = ids.conversation_id(TENANT, CHANNEL, THREAD)
NONCE = "nonce-1"

# Strings the scripted conversation carries. None may appear in an event.
USER_TEXT = "ZZ-USER-TEXT where is the vpn doc"
QUERY = "ZZ-QUERY-vpn"
ANSWER = "ZZ-ANSWER-the vpn page is on the wiki"
TITLE = "ZZ-TITLE-new monitor"
DETAILS = "ZZ-DETAILS-mine flickers"
COMMENT = "ZZ-COMMENT-any update"
ITEM_TITLE = "ZZ-ITEM-vpn broken"
SECRETS = [USER_TEXT, QUERY, ANSWER, TITLE, DETAILS, COMMENT, ITEM_TITLE]


def tool_turn(*calls: ToolCall) -> LLMResponse:
    return LLMResponse(text=None, tool_calls=list(calls), stop_reason="tool_use")


def answer(text: str) -> LLMResponse:
    return LLMResponse(text=text, tool_calls=[], stop_reason="end_turn")


def inbound(event_id: str, text: str = USER_TEXT) -> InboundMessage:
    return InboundMessage(event_id, ALICE, text, CHANNEL, THREAD, "channel")


async def converse(h, agent_id: str, msg: InboundMessage):
    return await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, agent_id, CHANNEL, THREAD),
        id=CONV_ID,
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )


def ctx(agent_id: str = "it-helper") -> TurnContext:
    return TurnContext(TENANT, agent_id, 1, ALICE, CHANNEL, THREAD, "channel")


async def events(h, kind: str | None = None) -> list[AuditEvent]:
    found = await h.audit.list(TENANT, limit=100)
    return [e for e in found if kind is None or e.kind == kind]


def assert_actor(e: AuditEvent, agent_id: str = "it-helper") -> None:
    assert e.tenant_id == TENANT
    assert e.agent_id == agent_id
    assert e.agent_version == 1
    assert e.requester_user_id == ALICE
    assert e.identity == f"agent:{agent_id}"
    assert e.at is not None


# --- one event per action ------------------------------------------------------


async def test_read_tool_call_and_answer_each_record_one_event(harness):
    h = harness
    h.notion.add_page(DB_ID, {"Name": title_prop(ITEM_TITLE)})
    call = ToolCall("tu_1", "search_items", {"query": QUERY})
    h.llm.queue(tool_turn(call), answer(ANSWER))

    await converse(h, "it-helper", inbound("Ev1"))
    await eventually(lambda: h.slack.updates())

    [tool_event] = await events(h, "tool_call")
    assert_actor(tool_event)
    assert tool_event.tool == "search_items"
    assert tool_event.conversation_id == CONV_ID
    assert tool_event.outcome == "ok"
    assert tool_event.input_sha256 == canonical_sha256({"query": QUERY})
    assert tool_event.output_sha256 == canonical_sha256(
        (await h.tool_outputs.get_many(TENANT, CONV_ID, ["tu_1"]))["tu_1"][0]
    )
    assert tool_event.event_key == ids.audit_tool_call_key(CONV_ID, "tu_1")

    [answer_event] = await events(h, "answer")
    assert_actor(answer_event)
    assert answer_event.conversation_id == CONV_ID
    assert answer_event.output_sha256 == canonical_sha256(ANSWER)
    placeholder_ts = h.slack.updates()[0]["ts"]
    assert answer_event.event_key == ids.audit_answer_key(CONV_ID, placeholder_ts)
    assert len(await events(h)) == 2


async def test_failure_notice_is_not_an_answer(harness):
    h = harness
    h.llm.queue(RuntimeError("model down"))

    await converse(h, "it-helper", inbound("Ev1"))
    await eventually(lambda: h.slack.updates(), timeout=60)

    assert await events(h, "answer") == []


async def test_model_answer_equal_to_a_notice_is_still_an_answer(harness):
    h = harness
    h.llm.queue(answer(TURN_FAILED))

    await converse(h, "it-helper", inbound("Ev1"))
    await eventually(lambda: h.slack.updates())

    [answer_event] = await events(h, "answer")
    assert answer_event.output_sha256 == canonical_sha256(TURN_FAILED)


async def test_step_limit_notice_is_not_an_answer(harness):
    h = harness
    calls = [tool_turn(ToolCall(f"tu_{i}", "search_items", {"query": "x"})) for i in range(6)]
    h.llm.queue(*calls)

    await converse(h, "it-helper", inbound("Ev1"))
    await eventually(lambda: h.slack.updates(), timeout=30)

    assert await events(h, "answer") == []
    assert len(await events(h, "tool_call")) >= 1


async def test_write_after_approval_records_approval_then_write(harness):
    h = harness
    spec = TaskSpec(
        ctx(),
        "create_item",
        {"title": TITLE, "details": DETAILS, "database_id": DB_ID},
        NONCE,
    )
    task_id = ids.task_id(TENANT, str(uuid.uuid4()))
    handle = await h.client.start_workflow(
        TaskWorkflow.run, spec, id=task_id, task_queue=TASK_QUEUE
    )
    await eventually(lambda: h.slack.posts())
    assert await events(h) == []  # nothing is recorded before approval

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))
    await eventually(lambda: events(h, "write"))

    [approval] = await events(h, "approval")
    [write] = await events(h, "write")
    for e in (approval, write):
        assert_actor(e)
        assert e.task_id == task_id
        assert e.tool == "create_item"
        assert e.approver_user_id == ALICE
        assert e.input_sha256 == canonical_sha256(spec.payload)
    assert approval.event_key == ids.audit_approval_key(task_id, NONCE)
    assert approval.target == DB_ID
    assert write.event_key == ids.audit_write_key(f"{task_id}:create", "ok")
    page_id = h.notion.all_pages(DB_ID)[0]["id"]
    assert write.target == page_id
    assert write.output_sha256 == canonical_sha256({"page_id": page_id})
    assert write.outcome == "ok"
    assert approval.at <= write.at


async def test_comment_write_records_approval_and_write(harness):
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop(ITEM_TITLE)})
    spec = TaskSpec(ctx(), "add_comment", {"item": page["url"], "comment": COMMENT}, NONCE)
    task_id = ids.task_id(TENANT, str(uuid.uuid4()))
    handle = await h.client.start_workflow(
        TaskWorkflow.run, spec, id=task_id, task_queue=TASK_QUEUE
    )
    await eventually(lambda: h.slack.posts())

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))
    assert await handle.result() == "done"

    [approval] = await events(h, "approval")
    [write] = await events(h, "write")
    assert approval.tool == write.tool == "add_comment"
    assert approval.target == write.target == page["id"]
    assert write.approver_user_id == ALICE
    assert write.outcome == "ok"
    assert write.event_key == ids.audit_write_key(f"{task_id}:comment", "ok")


async def test_cancelled_task_records_only_approval_closed(harness):
    """Fails if a declined task records an approval or write, or skips its approval_closed row."""
    h = harness
    spec = TaskSpec(
        ctx(), "create_item", {"title": TITLE, "details": DETAILS, "database_id": DB_ID}, NONCE
    )
    task_id = ids.task_id(TENANT, "t1")
    handle = await h.client.start_workflow(
        TaskWorkflow.run, spec, id=task_id, task_queue=TASK_QUEUE
    )
    await eventually(lambda: h.slack.posts())
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, False))
    assert await handle.result() == "cancelled"

    [closed] = await events(h, "approval_closed")
    assert closed.tool == "declined"
    assert closed.approver_user_id == ALICE
    assert closed.input_sha256 == canonical_sha256(spec.payload)
    assert closed.event_key == ids.audit_approval_closed_key(task_id, NONCE)
    assert await events(h, "approval") == []
    assert await events(h, "write") == []


async def test_failed_write_records_error_outcome(harness):
    h = harness
    spec = TaskSpec(
        ctx(), "create_item", {"title": TITLE, "details": DETAILS, "database_id": DB_ID}, NONCE
    )

    # Notion refuses the create for good (a 404), so the activity fails without retries.
    async def refuse(*args, **kwargs):
        raise FakeNotFound("gone")

    h.notion.create_page = refuse
    task_id = ids.task_id(TENANT, "t-denied")
    handle = await h.client.start_workflow(
        TaskWorkflow.run, spec, id=task_id, task_queue=TASK_QUEUE
    )
    await eventually(lambda: h.slack.posts())
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))
    await eventually(lambda: events(h, "write"))

    [write] = await events(h, "write")
    assert write.outcome == "error"
    assert write.output_sha256 is None
    assert write.event_key == ids.audit_write_key(f"{task_id}:create", "error")
    assert len(await events(h, "approval")) == 1


async def test_hand_off_records_one_event_then_the_specialists_answer():
    async with running(front_door_dict()) as h:
        h.llm.queue(tool_turn(ToolCall("tu_h", "hand_off", {"agent": "it-helper"})), answer(ANSWER))

        await converse(h, "concierge", inbound("Ev1"))
        await eventually(lambda: len(h.slack.updates()) >= 2)

        [hand_off] = await events(h, "hand_off")
        assert_actor(hand_off, "concierge")
        assert hand_off.target == "it-helper"
        assert hand_off.conversation_id == CONV_ID
        assert hand_off.event_key == ids.audit_hand_off_key(TENANT, "Ev1")
        # The announcement is not an answer; the specialist's reply is, under its own ID.
        [answer_event] = await events(h, "answer")
        assert_actor(answer_event, "it-helper")
        assert answer_event.output_sha256 == canonical_sha256(ANSWER)
        [notice] = await events(h, "notice")
        assert_actor(notice, "concierge")
        assert notice.tool == "hand_off"
        assert notice.output_sha256 == canonical_sha256(ANNOUNCEMENT)
        assert len(await events(h)) == 3


# --- retries -------------------------------------------------------------------


async def test_activity_retry_records_once(harness):
    h = harness
    h.llm.queue(tool_turn(ToolCall("tu_1", "search_items", {"query": QUERY})), answer(ANSWER))
    h.audit.fail_next_records(2)  # the tool-call record fails, then the retry succeeds

    await converse(h, "it-helper", inbound("Ev1"))
    # The answer is recorded after the tool step completes; a progress edit on the
    # placeholder can land earlier while the read retries, so it is not the settle point.
    await eventually(lambda: events(h, "answer"), timeout=30)

    assert len(await events(h, "tool_call")) == 1
    assert len(await events(h, "answer")) == 1


async def test_an_audit_outage_fails_the_activity_instead_of_skipping_the_event(harness):
    h = harness
    h.llm.queue(answer(ANSWER))
    h.audit.fail_next_records(1)

    await converse(h, "it-helper", inbound("Ev1"))
    # The first answer record fails after the post; the retry posts again and records.
    await eventually(lambda: events(h, "answer"), timeout=30)

    assert len(await events(h, "answer")) == 1


async def test_notice_retry_records_once(harness):
    """Fails if a retried notice post records a second notice row."""
    h = harness
    h.llm.queue(answer("Real answer"))
    h.audit.fail_next_records(1)

    handle = await converse(h, "it-helper", inbound("Ev1", ""))
    await eventually(lambda: events(h, "notice"), timeout=30)
    await handle.signal(ConversationWorkflow.message, inbound("Ev2"))
    await eventually(lambda: h.slack.updates(), timeout=30)

    [notice] = await events(h, "notice")
    assert notice.tool == "empty_message"
    assert notice.event_key == ids.audit_notice_key(
        CONV_ID, "empty_message", ids.empty_message_key("Ev1", "it-helper")
    )
    assert len([p for p in h.slack.posts() if p["text"] == EMPTY_MESSAGE_REPLY]) == 1


# --- no content ------------------------------------------------------------------


async def test_no_event_holds_message_or_tool_text(harness):
    h = harness
    h.notion.add_page(DB_ID, {"Name": title_prop(ITEM_TITLE)})
    h.llm.queue(tool_turn(ToolCall("tu_1", "search_items", {"query": QUERY})), answer(ANSWER))
    await converse(h, "it-helper", inbound("Ev1"))
    await eventually(lambda: h.slack.updates())

    spec = TaskSpec(
        ctx(), "create_item", {"title": TITLE, "details": DETAILS, "database_id": DB_ID}, NONCE
    )
    handle = await h.client.start_workflow(
        TaskWorkflow.run, spec, id=ids.task_id(TENANT, "t1"), task_queue=TASK_QUEUE
    )
    await eventually(lambda: [p for p in h.slack.posts() if p["blocks"]])
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))
    await eventually(lambda: events(h, "write"))

    found = await events(h)
    assert {e.kind for e in found} == {"answer", "tool_call", "approval", "write"}
    dump = "\n".join(event_json(e) for e in found)
    for secret in SECRETS:
        assert secret not in dump
    hashes = [e.input_sha256 for e in found] + [e.output_sha256 for e in found]
    assert all(x is None or len(x) == 64 for x in hashes)


BAD_EVENTS = [
    pytest.param({"output_sha256": "some text"}, id="a hash that is not a hash"),
    pytest.param({"kind": "chat"}, id="an unknown kind"),
    pytest.param({"outcome": "maybe"}, id="an unknown outcome"),
    pytest.param(
        {"kind": "answer", "agent_id": None, "agent_version": None}, id="answer, no agent"
    ),
    pytest.param(
        {"kind": "agent_publish", "agent_id": None, "agent_version": None}, id="publish, no agent"
    ),
    pytest.param({"kind": "answer", "agent_version": None}, id="answer, no version"),
    pytest.param(
        {"kind": "audience_resume", "agent_id": None, "agent_version": None},
        id="resume, no agent",
    ),
]


@pytest.mark.parametrize("fields", BAD_EVENTS)
def test_an_event_that_breaks_the_rules_is_refused(fields):
    """Fails if an event with a bad hash, kind or outcome, or an agent action with no agent version, is built."""
    base = {"agent_id": "a", "agent_version": 1, "kind": "answer"}
    with pytest.raises(ValueError):
        AuditEvent(TENANT, "k", requester_user_id=ALICE, identity="agent:a", **(base | fields))


@pytest.mark.parametrize("kind", ["member_add", "budget_set", "switch_off", "switch_on"])
def test_a_membership_budget_or_switch_event_may_name_no_agent(kind):
    """Fails if an event about the organisation, not an agent, must name an agent."""
    event = AuditEvent(TENANT, "k", kind, None, None, ALICE, f"user:{ALICE}", target="x")
    assert (event.agent_id, event.agent_version) == (None, None)


def test_a_resume_may_name_an_agent_with_no_published_version():
    """Fails if audience_resume refuses an agent whose published version is gone."""
    event = AuditEvent(TENANT, "k", "audience_resume", "a", None, ALICE, f"user:{ALICE}")
    assert (event.agent_id, event.agent_version) == ("a", None)


def test_canonical_hash_ignores_key_order_and_spacing():
    assert canonical_sha256({"a": 1, "b": [1, 2]}) == canonical_sha256({"b": [1, 2], "a": 1})
    assert canonical_sha256({"a": 1}) != canonical_sha256({"a": 2})


# --- listing -----------------------------------------------------------------------
