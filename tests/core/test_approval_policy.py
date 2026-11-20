"""The approval policy at run time (D-43): a low-risk comment on the requester's own
item runs without a card when the admin turned approval off; every other case shows it."""

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from agentos import ids
from agentos.agent_health import UNAVAILABLE, AgentHealth
from agentos.approval_policy import NO_APPROVAL
from agentos.config import TASK_QUEUE
from agentos.connectors.notion import normalize_id
from agentos.models import (
    ApprovalDecision,
    ConversationRef,
    InboundMessage,
    LLMResponse,
    NudgeRequest,
    TaskSpec,
    ToolCall,
    TurnContext,
)
from agentos.slack_client import SlackUser
from agentos.switches import AGENT
from agentos.tasks import TaskRecord
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, BOB, DB_ID, TEAM, TENANT, eventually, title_prop
from tests.helpers.tasks import approve, start_task, wait_state

THREAD = "1700000000.000100"
NOW = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)


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
    with no approval row and no approver, or the thread line does not link the item and say
    no approval was needed."""
    h = harness
    page = await file_an_item(h)
    await h.acts.policies.set(TENANT, "add_comment", NO_APPROVAL)

    await ask_for_comment(h, page, "Ev-comment")

    # Settle point: the policy line is the task's last post; nothing follows it.
    (line,) = await eventually(lambda: thread_posts(h, "no approval needed"))
    assert line["text"] == (
        f"Commented on 'New monitor' ({page['url']}) (no approval needed: your admin's setting)."
    )
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


# --- the pre-write re-check on the no-card path -------------------------------------------

OFF_LINE = "IT helper is turned off by an admin. Nothing was changed."
STOPPED_LINE = "IT helper can no longer act for you here; nothing was changed."
UNAVAILABLE_LINE = (
    "IT helper is unavailable right now: an admin needs to fix its access. Nothing was changed."
)


async def start_policy_comment(h, page, change):
    """A comment task on the requester's own filed item, the policy allowing it.

    The change lands while the task reads who filed the item: the decision still allows
    the comment, and the pre-write re-check must stop it.
    """
    await seed_filing(h, page["id"], ALICE)
    await h.acts.policies.set(TENANT, "add_comment", NO_APPROVAL)
    task_id = ids.task_id(TENANT, str(uuid.uuid4()))
    await h.acts.content.put_draft(
        TENANT, task_id, 1, {"item": page["id"], "comment": "Any update?"}
    )
    original = h.acts.tasks.get

    async def get_then_change(tenant_id, row_id):
        row = await original(tenant_id, row_id)
        h.acts.tasks.get = original
        await change(h)
        return row

    h.acts.tasks.get = get_then_change
    return await h.client.start_workflow(
        TaskWorkflow.run,
        TaskSpec(ctx(), "add_comment", {}, "nonce-1", draft_ref=task_id, revision=1),
        id=task_id,
        task_queue=TASK_QUEUE,
    )


async def pause_channel(h):
    await h.acts.pauses.open_pause(TENANT, "it-helper", "C_GENERAL", "channel_shared", NOW)


async def lose_audience(h):
    h.slack.users[ALICE] = SlackUser(ALICE, TEAM, deleted=True)


async def break_grants(h):
    await h.acts.health.record(
        AgentHealth(TENANT, "it-helper", 1, UNAVAILABLE, "grant 'IT requests': problem", NOW)
    )


async def turn_agent_off(h):
    await h.switches.switch_off(TENANT, AGENT, "it-helper", "U_ADMIN", NOW)


@pytest.mark.parametrize(
    ("change", "line"),
    [
        (pause_channel, STOPPED_LINE),
        (lose_audience, STOPPED_LINE),
        (break_grants, UNAVAILABLE_LINE),
        (turn_agent_off, OFF_LINE),
    ],
    ids=["channel-paused", "audience-lost", "agent-unavailable", "agent-off"],
)
async def test_a_change_after_the_turns_check_stops_a_policy_allowed_comment(harness, change, line):
    """Fails if a policy-allowed comment still runs when the channel was paused, the requester
    left the audience, the agent's grants broke or the agent was turned off after the turn's
    check, or the stop is not said in the thread with the card's line and audited as a write
    error with no approval row."""
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    handle = await start_policy_comment(h, page, change)

    # Settle point: the refusal line is the task's last post, and the task has ended.
    (posted,) = await eventually(lambda: thread_posts(h, "was changed"))
    assert posted["text"] == line
    assert await handle.result() == "cancelled"
    assert h.notion.count("create_comment") == 0
    write = next(
        e
        for e in h.audit.events
        if e.kind == "write" and e.outcome == "error" and e.tool == "add_comment"
    )
    assert write.approver_user_id is None
    assert [e.kind for e in h.audit.events if e.task_id == write.task_id] == ["write"]


async def uninstall_tenant(h):
    h.acts.registry.remove(TENANT)
    h.acts.registry.set_disabled({TENANT})


async def test_an_uninstalled_tenant_ends_a_policy_allowed_comment_cleanly(harness):
    """Fails if a policy-allowed comment writes, posts or retries after the tenant's app was
    uninstalled, or leaves the task's end unrecorded."""
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    handle = await start_policy_comment(h, page, uninstall_tenant)

    # No virtual time may pass on its own: a retry's backoff timer would never fire,
    # so the result arriving proves the end took no retry.
    with h.wf_env.auto_time_skipping_disabled():
        assert await asyncio.wait_for(handle.result(), timeout=10) == "cancelled"

    assert h.slack.posts() == []
    assert h.slack.updates() == []
    assert h.notion.count("create_comment") == 0
    row = await h.acts.tasks.get(TENANT, handle.id)
    assert row.state == "cancelled"


async def test_a_nudge_under_the_no_approval_policy_still_shows_its_card(harness):
    """Fails if the no-approval policy lets a nudge's comment run without its approval card."""
    h = harness
    await h.acts.policies.set(TENANT, "add_comment", NO_APPROVAL)
    handle = await start_task(h)
    await approve(h, handle)
    await wait_state(handle, "watching")
    await h.wf_env.sleep(timedelta(days=4))
    (stale,) = await eventually(
        lambda: [p for p in h.slack.posts() if p["thread_ts"] and "No movement" in p["text"]]
    )
    actions = next(b for b in stale["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    await handle.signal(TaskWorkflow.nudge, NudgeRequest(value["nonce"], ALICE))

    # Settle point: the nudge card is posted; a comment run without it would have landed first.
    await eventually(
        lambda: [
            p
            for p in h.slack.posts()
            if p["blocks"] and "wants to nudge" in json.dumps(p["blocks"])
        ]
    )
    assert h.notion.count("create_comment") == 0
