"""The approval policy at run time (D-43): a low-risk comment on the requester's own
item runs without a card when the admin turned approval off; every other case shows it."""

import json
import uuid
from datetime import UTC, datetime

import pytest

from agentos import ids
from agentos.approval_policy import NO_APPROVAL
from agentos.config import TASK_QUEUE
from agentos.connectors.notion import normalize_id
from agentos.models import (
    ApprovalDecision,
    ConversationRef,
    InboundMessage,
    LLMResponse,
    ToolCall,
    TurnContext,
)
from agentos.switches import AGENT
from agentos.tasks import TaskRecord
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, BOB, DB_ID, TENANT, eventually, title_prop

THREAD = "1700000000.000100"
NOW = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)
POLICY_LINE = "Commented on 'New monitor' (no approval needed: your admin's setting)."


def ctx() -> TurnContext:
    return TurnContext(
        tenant_id=TENANT,
        agent_id="it-helper",
        agent_version=1,
        requester_user_id=ALICE,
        channel="C_GENERAL",
        thread_ts=THREAD,
        channel_kind="channel",
    )


async def say(h, text: str, event_id: str):
    """A message in the thread; the conversation starts with the first one."""
    msg = InboundMessage(event_id, ALICE, text, "C_GENERAL", THREAD, "channel")
    return await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "it-helper", "C_GENERAL", THREAD),
        id=ids.conversation_id(TENANT, "C_GENERAL", THREAD),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )


def approval_cards(h):
    """The posts that are approval cards: they carry the Submit button."""
    return [
        p
        for p in h.slack.posts()
        if p["blocks"]
        and any(
            b.get("type") == "actions"
            and any(e.get("action_id") == "task_approve" for e in b["elements"])
            for b in p["blocks"]
        )
    ]


def thread_posts(h, needle: str):
    return [p for p in h.slack.posts() if p["thread_ts"] and needle in p["text"]]


async def file_an_item(h):
    """A create task the requester approved, now watching its filed item."""
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[
                ToolCall("tu_f", "create_item", {"title": "New monitor", "details": "Flickers."})
            ],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Sent you a card.", tool_calls=[], stop_reason="end_turn"),
    )
    await say(h, "please file a ticket", "Ev-file")
    (card,) = await eventually(lambda: approval_cards(h))
    actions = next(b for b in card["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    handle = h.client.get_workflow_handle(value["task_id"])
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(value["nonce"], ALICE, True))

    async def watching():
        return await handle.query(TaskWorkflow.state) == "watching"

    await eventually(watching, timeout=20)
    return h.notion.all_pages(DB_ID)[0]


async def ask_for_comment(h, page, event_id: str):
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[
                ToolCall("tu_c", "add_comment", {"item": page["id"], "comment": "Any update?"})
            ],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Done.", tool_calls=[], stop_reason="end_turn"),
    )
    await say(h, "please comment on my request", event_id)


async def seed_filing(h, page_id: str, requester: str):
    """The platform's record of an item filed through a task: its row and its page link."""
    task_id = ids.task_id(TENANT, str(uuid.uuid4()))
    await h.acts.tasks.record(
        TaskRecord(
            tenant_id=TENANT,
            task_id=task_id,
            requester_user_id=requester,
            agent_id="it-helper",
            title="Broken VPN",
            url="https://www.notion.so/brokenvpn",
            page_id=page_id,
            state="watching",
            last_status="To do",
            updated_at=NOW,
            channel="C_GENERAL",
            thread_ts=THREAD,
        )
    )
    await h.acts.links.link(TENANT, normalize_id(page_id), task_id)


async def test_a_policy_allowed_comment_on_the_requesters_own_item_runs_without_a_card(harness):
    """Fails if the comment waits for a card, is posted twice, is not audited as a policy write
    with no approval row and no approver, or the thread is not told no approval was needed."""
    h = harness
    page = await file_an_item(h)
    await h.acts.policies.set(TENANT, "add_comment", NO_APPROVAL)

    await ask_for_comment(h, page, "Ev-comment")

    # Settle point: the policy line is the task's last post; nothing follows it.
    (line,) = await eventually(lambda: thread_posts(h, "no approval needed"))
    assert line["text"] == POLICY_LINE
    assert len(approval_cards(h)) == 1  # the create card only
    comments = h.notion.comments[page["id"].replace("-", "")]
    assert len(comments) == 1
    assert comments[0]["rich_text"][0]["text"]["content"].startswith("Any update?")
    write = next(e for e in h.audit.events if e.kind == "write" and e.outcome == "policy")
    assert (write.tool, write.target, write.approver_user_id) == ("add_comment", page["id"], None)
    assert [e.kind for e in h.audit.events if e.task_id == write.task_id] == ["write"]


@pytest.mark.parametrize(
    "case",
    ["policy default", "item filed by someone else", "item with no creating task"],
    ids=["default", "not-mine", "no-link"],
)
async def test_a_comment_the_policy_does_not_allow_shows_the_card(harness, case):
    """Fails if a comment runs without approval when the policy is the default, the item was
    filed by someone else, or the platform has no record of who filed it."""
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    if case == "policy default":
        await seed_filing(h, page["id"], ALICE)
    elif case == "item filed by someone else":
        await seed_filing(h, page["id"], BOB)
        await h.acts.policies.set(TENANT, "add_comment", NO_APPROVAL)
    else:
        await h.acts.policies.set(TENANT, "add_comment", NO_APPROVAL)

    await ask_for_comment(h, page, "Ev-comment")

    (card,) = await eventually(lambda: approval_cards(h))
    assert "wants to comment on *Broken VPN*" in json.dumps(card["blocks"])
    assert h.notion.count("create_comment") == 0
    assert thread_posts(h, "no approval needed") == []


async def test_an_agent_turned_off_stops_a_policy_allowed_comment(harness):
    """Fails if a policy-allowed comment runs while the agent is off, or the stop is not said
    in the thread and audited as a write error with no approval row."""
    h = harness
    page = await file_an_item(h)
    await h.acts.policies.set(TENANT, "add_comment", NO_APPROVAL)
    # The admin flips the switch while the task reads who filed the item: the decision
    # still allows the comment, and the write's own gate must stop it.
    original = h.acts.tasks.get

    async def get_then_flip(tenant_id, task_id):
        row = await original(tenant_id, task_id)
        h.acts.tasks.get = original
        await h.switches.switch_off(TENANT, AGENT, "it-helper", "U_ADMIN", NOW)
        return row

    h.acts.tasks.get = get_then_flip

    await ask_for_comment(h, page, "Ev-comment")

    # Settle point: the refusal line is the task's last post.
    (line,) = await eventually(lambda: thread_posts(h, "I couldn't comment on 'New monitor'"))
    assert "turned off by an admin" in line["text"]
    assert h.notion.comments == {}
    write = next(
        e
        for e in h.audit.events
        if e.kind == "write" and e.outcome == "error" and e.tool == "add_comment"
    )
    assert write.approver_user_id is None
    assert [e.kind for e in h.audit.events if e.task_id == write.task_id] == ["write"]
    state = await h.client.get_workflow_handle(write.task_id).query(TaskWorkflow.state)
    assert state == "cancelled"
