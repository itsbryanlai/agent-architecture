"""The 👍/👎 buttons on an answer, and the gateway's handling of a vote and its reason."""

import json
import secrets
from datetime import UTC, datetime
from urllib.parse import urlencode

import pytest
from fastapi.testclient import TestClient
from slack_sdk.signature import SignatureVerifier

from agentos import ids
from agentos.audit import InMemoryAuditStore
from agentos.config import TASK_QUEUE, Settings, TenantsConfig
from agentos.feedback import (
    FEEDBACK_ACTIONS,
    FEEDBACK_CALLBACK,
    REASON_MAX,
    THANKS,
    InMemoryFeedbackStore,
    feedback_blocks,
)
from agentos.gateway.app import create_app
from agentos.models import InboundMessage, LLMResponse, TaskSpec, TurnContext
from agentos.slack_client import FakeSlack
from agentos.stores import InMemoryTaskLinkStore
from agentos.switches import AGENT, InMemorySwitchStore
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, BOB, DB_ID, TEAM, TENANT, eventually, title_prop
from tests.helpers.conversation import CHANNEL, THREAD, start_conversation
from tests.helpers.gateway import FakeTemporalClient, signed_headers
from tests.helpers.llm_errors import StatusError

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
CONV = ids.conversation_id(TENANT, CHANNEL, THREAD)
ANSWER_TS = "1700000000.000500"


# --- the gateway on fakes -----------------------------------------------------------------


def feedback_gateway(agents):
    sec = secrets.token_hex(16)
    slack = FakeSlack()
    store = InMemoryFeedbackStore()
    audit = InMemoryAuditStore()

    async def slack_for(tenant):
        return slack

    app = create_app(
        Settings(),
        TenantsConfig.of(agents),
        FakeTemporalClient(),
        InMemoryTaskLinkStore(),
        SignatureVerifier(sec),
        slack_for=slack_for,
        feedback=store,
        audit=audit,
        clock=lambda: NOW,
    )
    return TestClient(app), sec, store, audit, slack


def post_interaction(client, sec, payload: dict):
    body = urlencode({"payload": json.dumps(payload)}).encode()
    headers = signed_headers(sec, body, "application/x-www-form-urlencoded")
    return client.post("/slack/interactivity", content=body, headers=headers)


def click_payload(action_id: str, clicker: str = ALICE, conversation_id: str = CONV) -> dict:
    """A click on the button the runtime builds, with the signed clicker in the payload."""
    blocks = feedback_blocks(conversation_id, ANSWER_TS, "it-helper", 1)
    button = next(e for e in blocks[0]["elements"] if e["action_id"] == action_id)
    return {
        "type": "block_actions",
        "team": {"id": TEAM},
        "user": {"id": clicker},
        "trigger_id": "trig-1",
        "channel": {"id": CHANNEL},
        "message": {"ts": ANSWER_TS, "thread_ts": THREAD},
        "actions": [{"action_id": action_id, "value": button["value"]}],
    }


def reason_submission(slack, reason: str, user: str = ALICE) -> dict:
    """The reason form as the 👎 click opened it, submitted by the signed user."""
    view = slack.views()[-1]["view"]
    return {
        "type": "view_submission",
        "team": {"id": TEAM},
        "user": {"id": user},
        "view": {
            "callback_id": view["callback_id"],
            "private_metadata": view["private_metadata"],
            "state": {
                "values": {"reason": {"value": {"type": "plain_text_input", "value": reason}}}
            },
        },
    }


@pytest.mark.parametrize(
    "action_id,vote,opens_form",
    [("answer_feedback_up", "up", False), ("answer_feedback_down", "down", True)],
    ids=["thumbs-up", "thumbs-down"],
)
async def test_a_click_records_the_signed_voters_vote_and_acknowledges_it(
    agents, action_id, vote, opens_form
):
    """Fails if a click records a vote for anyone but the signed clicker, skips the audit event
    or the ephemeral thanks, posts in the thread, or opens the reason form for the wrong vote."""
    client, sec, store, audit, slack = feedback_gateway(agents)

    assert post_interaction(client, sec, click_payload(action_id, clicker=BOB)).status_code == 200

    row = await store.get(TENANT, CONV, ANSWER_TS, BOB)
    assert (row.vote, row.reason, row.at) == (vote, None, NOW)
    assert (row.agent_id, row.agent_version, row.conversation_id) == ("it-helper", 1, CONV)
    assert await store.get(TENANT, CONV, ANSWER_TS, ALICE) is None
    [event] = audit.events
    assert (event.kind, event.agent_id, event.agent_version, event.target) == (
        "feedback",
        "it-helper",
        1,
        vote,
    )
    assert event.requester_user_id == BOB
    [thanks] = slack.ephemerals()
    assert (thanks["user_id"], thanks["text"], thanks["channel"]) == (BOB, THANKS, CHANNEL)
    assert slack.posts() == []
    if opens_form:
        [opened] = slack.views()
        assert opened["trigger_id"] == "trig-1"
        view = opened["view"]
        assert view["callback_id"] == FEEDBACK_CALLBACK
        assert json.loads(view["private_metadata"]) == {
            "conversation_id": CONV,
            "answer_ts": ANSWER_TS,
        }
        [block] = view["blocks"]
        assert block["optional"] is True
        assert block["element"]["max_length"] == REASON_MAX
    else:
        assert slack.views() == []


@pytest.mark.parametrize(
    "reason,stored,keeps_open",
    [
        ("  cited the wrong page  ", "cited the wrong page", False),
        ("   ", None, False),
        ("x" * (REASON_MAX + 1), None, True),
    ],
    ids=["reason", "empty", "too-long"],
)
async def test_the_reason_form_stores_an_optional_one_line_reason(
    agents, reason, stored, keeps_open
):
    """Fails if a submitted reason is not trimmed and stored on the voter's 👎, an empty reason
    stores text, or an over-long reason closes the form instead of showing an error."""
    client, sec, store, _, slack = feedback_gateway(agents)
    assert post_interaction(client, sec, click_payload("answer_feedback_down")).status_code == 200

    resp = post_interaction(client, sec, reason_submission(slack, reason))

    assert resp.status_code == 200
    row = await store.get(TENANT, CONV, ANSWER_TS, ALICE)
    assert (row.vote, row.reason) == ("down", stored)
    if keeps_open:
        body = resp.json()
        assert body["response_action"] == "errors"
        assert "reason" in body["errors"]
    else:
        assert resp.content == b""


@pytest.mark.parametrize(
    "conversation_id,recorded",
    [
        (CONV, True),
        (ids.conversation_id("other-tenant", CHANNEL, THREAD), False),
    ],
    ids=["own answer", "another tenant's answer"],
)
async def test_a_vote_on_another_tenants_answer_is_refused(agents, conversation_id, recorded):
    """Fails if a click naming another tenant's conversation records a vote, audits it or
    acknowledges it, or a click naming this tenant's own answer is refused."""
    client, sec, store, audit, slack = feedback_gateway(agents)

    resp = post_interaction(
        client, sec, click_payload("answer_feedback_up", conversation_id=conversation_id)
    )

    assert resp.status_code == 200
    if recorded:
        assert (await store.get(TENANT, CONV, ANSWER_TS, ALICE)).vote == "up"
        assert [e.kind for e in audit.events] == ["feedback"]
        assert [t["text"] for t in slack.ephemerals()] == [THANKS]
    else:
        assert await store.get(TENANT, conversation_id, ANSWER_TS, ALICE) is None
        assert audit.events == []
        assert slack.ephemerals() == []
        assert slack.views() == []


# --- the buttons on the posted answer ------------------------------------------------------


async def stored_turn(h, event_id: str = "Ev1"):
    """A turn whose text the gateway kept in Postgres: the content-refs path."""
    await h.acts.content.put_inbound(
        TENANT,
        event_id,
        {
            "kind": "message",
            "agent_id": "it-helper",
            "user_id": ALICE,
            "text": "hello",
            "channel": CHANNEL,
            "thread_ts": THREAD,
            "channel_kind": "channel",
        },
    )
    msg = InboundMessage(event_id, ALICE, "", CHANNEL, THREAD, "channel", text_stored=True)
    return await start_conversation(h, msg)


def last_update(h):
    return h.slack.updates()[-1] if h.slack.updates() else None


async def an_answer(h):
    h.llm.queue(LLMResponse(text="Report it.", tool_calls=[], stop_reason="end_turn"))
    await stored_turn(h)
    return await eventually(lambda: last_update(h))


async def an_off_reply(h):
    await h.switches.switch_off(TENANT, AGENT, "it-helper", "U_ADMIN", NOW)
    await stored_turn(h)
    return await eventually(
        lambda: next((p for p in h.slack.posts() if p["text"] != "Thinking…"), None)
    )


class OffAfterAudience(InMemorySwitchStore):
    """No switch for the audience read, then the agent switch: off between check and model."""

    def __init__(self):
        super().__init__()
        self.reads = 0

    async def list(self, tenant_id):
        self.reads += 1
        if self.reads > 1 and not await super().list(tenant_id):
            await self.switch_off(tenant_id, AGENT, "it-helper", "U_ADMIN", NOW)
        return await super().list(tenant_id)


async def an_off_reply_mid_turn(h):
    h.acts.switches = OffAfterAudience()
    await stored_turn(h)
    return await eventually(lambda: last_update(h))


async def a_failure_notice(h):
    h.llm.queue(StatusError(401))
    await stored_turn(h)
    return await eventually(lambda: last_update(h))


async def an_approval_card(h):
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    ctx = TurnContext(TENANT, "it-helper", 1, ALICE, CHANNEL, THREAD, "channel")
    spec = TaskSpec(ctx, "add_comment", {"item": page["id"], "comment": "Any update?"}, "nonce-1")
    await h.client.start_workflow(
        TaskWorkflow.run, spec, id=ids.task_id(TENANT, "feedback-card"), task_queue=TASK_QUEUE
    )
    return await eventually(lambda: next((p for p in h.slack.posts() if p["blocks"]), None))


@pytest.mark.parametrize(
    "scenario,buttons",
    [
        (an_answer, True),
        (an_off_reply, False),
        (an_off_reply_mid_turn, False),
        (a_failure_notice, False),
        (an_approval_card, False),
    ],
    ids=["answer", "off reply", "off mid-turn", "failure notice", "approval card"],
)
async def test_feedback_buttons_appear_on_an_answer_and_nowhere_else(harness, scenario, buttons):
    """Fails if a final answer lacks the 👍/👎 buttons naming that answer, or a fixed off reply,
    a failure notice or an approval card carries them."""
    h = harness

    message = await scenario(h)

    elements = [
        e for b in (message["blocks"] or []) if b["type"] == "actions" for e in b["elements"]
    ]
    found = {e["action_id"] for e in elements}
    if buttons:
        assert found == set(FEEDBACK_ACTIONS)
        value = json.loads(
            next(e for e in elements if e["action_id"] == "answer_feedback_up")["value"]
        )
        assert value["conversation_id"] == CONV
        assert value["answer_ts"] == message["ts"]
        assert (value["agent_id"], value["agent_version"]) == ("it-helper", 1)
    else:
        assert found.isdisjoint(FEEDBACK_ACTIONS)
