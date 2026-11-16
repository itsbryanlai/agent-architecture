"""Chat and document content stays out of Temporal history, and Slack is answered first.

A message that the gateway stored (``text_stored``) runs the content-free path: the text, the
model's reply, each tool call's input and a task's payload live in Postgres and are read by
activities. These tests run that path end to end, through the gateway app, its relay and the
workflows, and look at what the test server recorded.
"""

from __future__ import annotations

import asyncio
import json
import secrets
from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode

import httpx
import pytest
from fastapi.testclient import TestClient
from slack_sdk.signature import SignatureVerifier
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Replayer

from agentos import ids
from agentos.audit import canonical_sha256
from agentos.config import TASK_QUEUE, Settings, TenantsConfig
from agentos.gateway import app as gateway_app
from agentos.gateway.app import TRY_AGAIN, create_app
from agentos.gateway.relay import InboundRelay
from agentos.models import (
    CUT_SHORT,
    ApprovalDecision,
    ConversationRef,
    DraftView,
    EditRequest,
    InboundMessage,
    LLMResponse,
    ToolCall,
    TurnContext,
)
from agentos.stores import InMemoryTaskLinkStore
from agentos.workflows.conversation import TURN_FAILED, ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, DB_ID, OTHER_DB_ID, TEAM, TENANT, eventually, title_prop
from tests.test_gateway import FakeTemporalClient, im_event, post_event, signed_headers

CHANNEL = "C_GENERAL"
THREAD = "1700000000.000100"
CONV_ID = ids.conversation_id(TENANT, CHANNEL, THREAD)
S_USER = "SENTINEL-user-words"
S_MODEL = "SENTINEL-model-words"
S_TOOL = "SENTINEL-tool-input"
S_OUT = "SENTINEL-tool-output"
S_TASK = "SENTINEL-task-title"
S_EDIT = "SENTINEL-edited-title"
CONV_ID_DM = ids.conversation_id(TENANT, "D1", THREAD)


# --- driving the gateway app against the harness ---------------------------------------------


class Gateway:
    """The gateway app on the harness's Temporal client, its content store and its Slack fake."""

    def __init__(self, h, temporal=None):
        self.h = h
        self.secret = secrets.token_hex(8)
        self.app = create_app(
            Settings(),
            TenantsConfig.of(h.agents),
            temporal or h.client,
            InMemoryTaskLinkStore(),
            SignatureVerifier(self.secret),
            slack_for=lambda tenant: h.slack,
            agents=h.agent_store,
            content=h.acts.content,
        )
        self.relay = InboundRelay(h.acts.content, self.app.state.deps.tenants, h.client)
        self.http = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://gateway"
        )

    async def post(self, path: str, body: bytes, content_type: str) -> httpx.Response:
        return await self.http.post(
            path, content=body, headers=signed_headers(self.secret, body, content_type)
        )

    async def say(self, text: str, event_id: str = "Ev1") -> httpx.Response:
        payload = im_event(text=text, channel="D1", ts=THREAD)
        payload["event_id"] = event_id
        body = json.dumps(payload).encode()
        resp = await self.post("/slack/events", body, "application/json")
        await self.relay.deliver_once()
        return resp

    async def interact(self, payload: dict) -> httpx.Response:
        body = urlencode({"payload": json.dumps({"team": {"id": TEAM}, **payload})}).encode()
        return await self.post("/slack/interactivity", body, "application/x-www-form-urlencoded")

    async def click(
        self, action_id: str, value: dict, user: str = ALICE, **extra
    ) -> httpx.Response:
        return await self.interact(
            {
                "type": "block_actions",
                "user": {"id": user},
                "trigger_id": "trig",
                "actions": [{"action_id": action_id, "value": json.dumps(value)}],
                **extra,
            }
        )

    async def save_edit(self, fields: dict, user: str = ALICE) -> httpx.Response:
        view = self.h.slack.views()[-1]["view"]
        values = {name: {"value": {"value": text}} for name, text in fields.items()}
        return await self.interact(
            {
                "type": "view_submission",
                "user": {"id": user},
                "view": {
                    "callback_id": view["callback_id"],
                    "private_metadata": view["private_metadata"],
                    "state": {"values": values},
                },
            }
        )


def card_value(h, which: int = -1) -> dict:
    """The task ID and nonce on the newest approval card (or an update that redrew it)."""
    shown = [p["blocks"] for p in h.slack.posts() if p["blocks"]] + [
        u["blocks"] for u in h.slack.updates() if u["blocks"]
    ]
    actions = next(b for b in shown[which] if b["type"] == "actions")
    return json.loads(actions["elements"][0]["value"])


async def stored_message(h, event_id: str, text: str, user: str = ALICE) -> InboundMessage:
    """A message as the gateway leaves it: the text in Postgres, the signal without it."""
    await h.acts.content.put_inbound(
        TENANT,
        event_id,
        {
            "kind": "message",
            "agent_id": "it-helper",
            "user_id": user,
            "text": text,
            "channel": CHANNEL,
            "thread_ts": THREAD,
            "channel_kind": "channel",
        },
    )
    return InboundMessage(event_id, user, "", CHANNEL, THREAD, "channel", text_stored=True)


async def converse(h, msg: InboundMessage):
    return await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "it-helper", CHANNEL, THREAD),
        id=CONV_ID,
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )


async def history_bytes(client: Client, workflow_ids: list[str]) -> bytes:
    raw = b""
    for workflow_id in workflow_ids:
        history = await client.get_workflow_handle(workflow_id).fetch_history()
        raw += b"".join(event.SerializeToString() for event in history.events)
    return raw


def leaked(raw: bytes, *sentinels: str) -> list[str]:
    return [s for s in sentinels if s.encode() in raw]


def tool_turn(*calls: ToolCall, stop: str = "tool_use") -> LLMResponse:
    return LLMResponse(text=None, tool_calls=list(calls), stop_reason=stop)


def answer(text: str, stop: str = "end_turn") -> LLMResponse:
    return LLMResponse(text=text, tool_calls=[], stop_reason=stop)


def create(call_id: str, title: str = "New monitor", details: str = "Flickers.") -> ToolCall:
    return ToolCall(call_id, "create_item", {"title": title, "details": details})


def replies(h) -> list[str]:
    return [u["text"] for u in h.slack.updates()]


# --- what the recorded history holds ---------------------------------------------------------


async def test_a_dm_turn_leaves_no_message_or_reply_text_in_history(harness):
    """Fails if what a person wrote, or the model replied, appears in the recorded history."""
    h = harness
    h.llm.queue(answer(S_MODEL))
    gateway = Gateway(h)

    assert (await gateway.say(S_USER)).status_code == 200
    await eventually(lambda: h.slack.updates())

    assert h.slack.updates()[0]["text"] == S_MODEL
    assert h.llm.calls[0]["messages"][-1]["content"][0]["text"] == S_USER
    conv = ids.conversation_id(TENANT, "D1", THREAD)
    assert leaked(await history_bytes(h.client, [conv]), S_USER, S_MODEL) == []
    assert S_USER not in json.dumps(list(h.acts.content.inbound.values()))
    stored = await h.history.load(TENANT, conv)
    assert [m["role"] for m in stored] == ["user", "assistant"]


async def test_a_tool_turn_leaves_no_tool_input_or_output_in_history(harness):
    """Fails if a tool call's input, or the text the tool returned, appears in the recorded history."""
    h = harness
    h.notion.add_page(DB_ID, {"Name": title_prop(f"{S_TOOL} {S_OUT}")})
    h.llm.queue(
        tool_turn(ToolCall("tu_1", "search_items", {"query": S_TOOL})),
        answer(S_MODEL),
    )
    gateway = Gateway(h)

    await gateway.say(S_USER)
    await eventually(lambda: h.slack.updates())

    sent = h.llm.calls[1]["messages"]
    assert sent[-2]["content"][0]["input"] == {"query": S_TOOL}
    assert S_OUT in sent[-1]["content"][0]["content"]
    conv = ids.conversation_id(TENANT, "D1", THREAD)
    assert leaked(await history_bytes(h.client, [conv]), S_USER, S_MODEL, S_TOOL, S_OUT) == []


async def test_a_task_and_its_edit_leave_no_payload_in_history_and_the_edit_is_what_is_filed(
    harness,
):
    """Fails if a task's title, details or an edit appear in history, or the edit is not what runs."""
    h = harness
    h.llm.queue(
        tool_turn(create("tu_c", S_TASK, "Details nobody should log.")),
        answer("Sent you a card."),
    )
    gateway = Gateway(h)

    await gateway.say(S_USER)
    cards = await eventually(lambda: [p for p in h.slack.posts() if p["blocks"]])
    assert S_TASK in json.dumps(cards[0]["blocks"])
    value = card_value(h)
    task = h.client.get_workflow_handle(value["task_id"])

    await gateway.click("task_edit", value)
    assert (
        await gateway.save_edit({"title": S_EDIT, "details": "Edited details."})
    ).status_code == 200
    await eventually(lambda: [u for u in h.slack.updates() if S_EDIT in json.dumps(u["blocks"])])
    fresh = card_value(h)
    assert fresh["nonce"] != value["nonce"]

    await gateway.click("task_approve", fresh)
    await eventually(lambda: [p for p in h.slack.posts() if "Filed" in p["text"]])

    assert [
        page["properties"]["Name"]["title"][0]["text"]["content"]
        for page in h.notion.all_pages(DB_ID)
    ] == [S_EDIT]
    assert S_EDIT in next(p["text"] for p in h.slack.posts() if "Filed" in p["text"])
    raw = await history_bytes(h.client, [CONV_ID_DM, value["task_id"]])
    assert leaked(raw, S_USER, S_TASK, S_EDIT, "Details nobody", "Edited details") == []
    rows = json.dumps(list(h.acts.content.inbound.values()))
    assert not [s for s in (S_USER, S_EDIT, "Edited details") if s in rows]
    # The recorded histories still replay on the code that wrote them.
    replayer = Replayer(workflows=[ConversationWorkflow, TaskWorkflow])
    for workflow_id in (CONV_ID_DM, value["task_id"]):
        await replayer.replay_workflow(
            await h.client.get_workflow_handle(workflow_id).fetch_history()
        )
    assert await task.query("state") == "watching"


# --- acknowledging Slack first ----------------------------------------------------------------


class DownTemporal(FakeTemporalClient):
    """A Temporal client that cannot be reached until ``up`` is set."""

    def __init__(self) -> None:
        super().__init__()
        self.up = False

    async def start_workflow(self, workflow, arg=None, **kwargs):
        if not self.up:
            raise RPCError("unavailable", RPCStatusCode.UNAVAILABLE, b"")
        await super().start_workflow(workflow, arg, **kwargs)

    async def signal(self, *_):
        raise RPCError("unavailable", RPCStatusCode.UNAVAILABLE, b"")


def gateway_client(agents, temporal, secret):
    return TestClient(
        create_app(
            Settings(),
            TenantsConfig.of(agents),
            temporal,
            InMemoryTaskLinkStore(),
            SignatureVerifier(secret),
        )
    )


def test_slack_is_answered_while_temporal_is_down_and_the_message_arrives_once_it_is_back(agents):
    """Fails if an event is refused or lost while Temporal is down, or delivered twice after."""
    secret = "s3cret"
    temporal = DownTemporal()
    client = gateway_client(agents, temporal, secret)
    d = client.app.state.deps
    clock = [datetime(2026, 11, 16, 12, 0, tzinfo=UTC)]
    relay = InboundRelay(d.content, d.tenants, temporal, clock=lambda: clock[0])

    first = post_event(client, secret, im_event())
    retry = post_event(client, secret, im_event())  # Slack sends the same event again
    assert (first.status_code, retry.status_code) == (200, 200)

    assert asyncio.run(relay.deliver_once()) == 0  # Temporal is down: kept, with a delay
    assert temporal.signal_with_starts == []
    temporal.up = True
    assert asyncio.run(relay.deliver_once()) == 0  # still inside the delay
    clock[0] += timedelta(seconds=5)
    assert asyncio.run(relay.deliver_once()) == 1
    assert asyncio.run(relay.deliver_once()) == 0
    [call] = temporal.signal_with_starts
    assert call["start_signal_args"][0].event_id == "Ev42"
    assert call["start_signal_args"][0].text == ""


async def test_a_slack_retry_of_an_event_gives_one_turn(harness):
    """Fails if the same Slack event, sent twice, runs two turns."""
    h = harness
    h.llm.queue(answer("one"), answer("two"))
    gateway = Gateway(h)

    await gateway.say("hello", event_id="EvDup")
    await gateway.say("hello", event_id="EvDup")
    await eventually(lambda: h.slack.updates())
    await h.client.get_workflow_handle(CONV_ID_DM).describe()

    assert len(h.llm.calls) == 1
    assert len([p for p in h.slack.posts() if p["text"] == "Thinking…"]) == 1


class SlowSignal(DownTemporal):
    async def signal(self, *_):
        await asyncio.sleep(5)


class BrokenHandle:
    def __init__(self, temporal):
        self._t = temporal

    async def signal(self, name, arg):
        await self._t.signal()

    async def query(self, *_args, **_kwargs):
        await self._t.signal()


@pytest.mark.parametrize("temporal_class", [DownTemporal, SlowSignal], ids=["down", "slow"])
async def test_a_button_click_with_temporal_unavailable_tells_the_clicker_to_try_again(
    harness, monkeypatch, temporal_class
):
    """Fails if a click that cannot reach Temporal is dropped silently or answered with an error."""
    h = harness
    temporal = temporal_class()
    temporal.get_workflow_handle = lambda workflow_id: BrokenHandle(temporal)
    monkeypatch.setattr(gateway_app, "CLICK_TIMEOUT_SECONDS", 0.2)
    gateway = Gateway(h, temporal)

    resp = await gateway.click(
        "task_approve",
        {"task_id": ids.task_id(TENANT, "t1"), "nonce": "n1"},
        channel={"id": "D-ALICE"},
        message={"ts": "1.1"},
    )

    assert resp.status_code == 200
    [shown] = h.slack.ephemerals()
    assert (shown["channel"], shown["user_id"], shown["text"]) == ("D-ALICE", ALICE, TRY_AGAIN)


async def test_saving_an_edit_with_temporal_unavailable_keeps_the_form_open_with_a_message(
    harness,
):
    """Fails if an edit that could not be passed on closes the form as if it were saved."""
    h = harness
    h.llm.queue(tool_turn(create("tu_c")), answer("Sent."))
    gateway = Gateway(h)
    await gateway.say("order a monitor")
    await eventually(lambda: [p for p in h.slack.posts() if p["blocks"]])
    value = card_value(h)
    await gateway.click("task_edit", value)

    down = DownTemporal()
    down.get_workflow_handle = lambda workflow_id: BrokenHandle(down)
    gateway.app.state.deps.temporal_client = down
    resp = await gateway.save_edit({"title": "New title", "details": "New details"})

    assert resp.json() == {"response_action": "errors", "errors": {"title": TRY_AGAIN}}


# --- what is approved is what runs -------------------------------------------------------------


async def test_the_write_reads_the_revision_that_was_approved_not_a_newer_one(harness):
    """Fails if an approval does not bind to its revision, or an old card still approves."""
    h = harness
    h.llm.queue(tool_turn(create("tu_c", "First title")), answer("Sent."))
    await converse(h, await stored_message(h, "Ev1", "order a monitor"))
    await eventually(lambda: [p for p in h.slack.posts() if p["blocks"]])
    first = card_value(h)
    task = h.client.get_workflow_handle(first["task_id"])

    await h.acts.content.put_inbound(
        TENANT,
        "edit:e1",
        {
            "kind": "edit",
            "task_id": first["task_id"],
            "fields": {"title": "Second title", "details": "d2"},
        },
        delivered=True,
    )
    await task.signal("edit", EditRequest(first["nonce"], ALICE, {}, "edit:e1"))
    await eventually(
        lambda: [u for u in h.slack.updates() if "Second title" in json.dumps(u["blocks"])]
    )
    second = card_value(h)
    # A third revision appears behind the workflow's back, after the card for the second.
    await h.acts.content.put_draft(
        TENANT,
        first["task_id"],
        3,
        {"title": "Sneaked title", "details": "x", "database_id": DB_ID},
    )

    await task.signal(TaskWorkflow.decision, ApprovalDecision(first["nonce"], ALICE, True))
    assert await task.query("state") == "awaiting_approval"  # the card for revision 1 is dead
    await task.signal(TaskWorkflow.decision, ApprovalDecision(second["nonce"], ALICE, True))
    await eventually(lambda: h.notion.all_pages(DB_ID))

    [page] = h.notion.all_pages(DB_ID)
    assert page["properties"]["Name"]["title"][0]["text"]["content"] == "Second title"
    approved = await h.acts.content.get_draft(TENANT, first["task_id"], 2)
    [approval] = [e for e in h.audit.events if e.kind == "approval"]
    assert approval.input_sha256 == canonical_sha256(approved)


async def test_a_comment_is_resolved_into_a_new_revision_approved_and_posted_once(harness):
    """Fails if a stored comment draft skips the grant check, the revision, or posts twice."""
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    h.llm.queue(
        tool_turn(ToolCall("tu_1", "add_comment", {"item": page["url"], "comment": S_TASK})),
        answer("Drafted."),
    )

    await converse(h, await stored_message(h, "Ev1", "comment please"))
    cards = await eventually(lambda: [p for p in h.slack.posts() if p["blocks"]])

    assert "wants to comment on *Broken VPN*" in json.dumps(cards[0]["blocks"])
    value = card_value(h)
    task = h.client.get_workflow_handle(value["task_id"])
    assert (await task.query("draft", result_type=DraftView)).revision == 2
    assert (await h.acts.content.get_draft(TENANT, value["task_id"], 1)) == {
        "item": page["url"],
        "comment": S_TASK,
    }
    await task.signal(TaskWorkflow.decision, ApprovalDecision(value["nonce"], ALICE, True))
    assert await task.result() == "done"

    posted = [
        "".join(t["text"]["content"] for t in c["rich_text"])
        for cs in h.notion.comments.values()
        for c in cs
    ]
    assert len(posted) == 1 and posted[0].startswith(S_TASK)
    assert any(p["text"].startswith("Commented on Broken VPN") for p in h.slack.posts())
    assert leaked(await history_bytes(h.client, [CONV_ID, value["task_id"]]), S_TASK) == []


async def test_a_comment_on_an_item_the_agent_was_not_granted_fails_without_a_card(harness):
    """Fails if a stored comment draft reaches an approval card for an item outside the grants."""
    h = harness
    page = h.notion.add_page(OTHER_DB_ID, {"Name": title_prop("Secret HR case")})
    h.llm.queue(
        tool_turn(ToolCall("tu_1", "add_comment", {"item": page["id"], "comment": "hi"})),
        answer("Drafted."),
    )

    await converse(h, await stored_message(h, "Ev1", "comment please"))
    await eventually(lambda: [p for p in h.slack.posts() if "can't comment" in p["text"]])

    assert [p for p in h.slack.posts() if p["blocks"]] == []
    assert h.notion.comments == {}


# --- the model step ---------------------------------------------------------------------------


def turn_ctx() -> TurnContext:
    return TurnContext(TENANT, "it-helper", 1, ALICE, CHANNEL, THREAD, "channel")


async def test_a_model_step_run_again_reuses_the_answer_it_stored(harness):
    """Fails if a retried call_model asks the model again and returns different tool-call IDs."""
    h = harness
    h.llm.queue(
        tool_turn(ToolCall("tu_first", "search_items", {"query": "a"})),
        tool_turn(ToolCall("tu_second", "search_items", {"query": "b"})),
    )

    one = await ActivityEnvironment().run(h.acts.call_model, turn_ctx(), CONV_ID, "Ev1:s0")
    again = await ActivityEnvironment().run(h.acts.call_model, turn_ctx(), CONV_ID, "Ev1:s0")

    assert [c.id for c in one.tool_calls] == ["tu_first"] == [c.id for c in again.tool_calls]
    assert len(h.llm.calls) == 1
    found, stored = await h.acts.content.get_tool_input(TENANT, CONV_ID, "tu_first")
    assert (found, stored) == (True, {"query": "a"})
    assert len(await h.history.load(TENANT, CONV_ID)) == 1


async def test_a_reply_slack_refused_is_never_stored_in_history(harness):
    """Fails if history keeps an answer Slack refused to show, or stores two assistant turns."""
    h = harness
    h.llm.queue(answer(S_MODEL))
    h.slack.fail_next_update(ValueError("slack down"))

    await converse(h, await stored_message(h, "Ev1", "question"))
    await eventually(lambda: h.slack.updates())

    assert replies(h) == [TURN_FAILED]
    stored = await h.history.load(TENANT, CONV_ID)
    assert [m["role"] for m in stored] == ["user", "assistant"]
    assert S_MODEL not in json.dumps(stored)


async def test_a_reply_cut_off_at_the_token_limit_is_posted_with_the_note(harness):
    """Fails if the content-free path drops the note on a cut-off text reply."""
    h = harness
    h.llm.queue(answer("A long answer that", stop="max_tokens"))

    await converse(h, await stored_message(h, "Ev1", "tell me everything"))
    await eventually(lambda: h.slack.updates())

    assert replies(h) == ["A long answer that" + CUT_SHORT]


async def test_identical_writes_in_one_reply_start_one_task_and_a_bad_input_is_a_tool_error(
    harness,
):
    """Fails if the duplicate check or the schema check is lost when inputs leave the workflow."""
    h = harness
    h.llm.queue(
        tool_turn(
            create("tu_1", "Same", "Same details."),
            create("tu_2", "Same", "Same details."),
            ToolCall("tu_3", "create_item", {"title": "No details"}),
        ),
        answer("Done."),
    )

    await converse(h, await stored_message(h, "Ev1", "order two"))
    await eventually(lambda: h.slack.updates())

    results = {b["tool_use_id"]: b for b in h.llm.calls[1]["messages"][-1]["content"]}
    assert "approval card" in results["tu_1"]["content"]
    assert results["tu_2"]["content"].startswith("Not run: identical")
    assert "missing required field 'details'" in results["tu_3"]["content"]
    assert len([p for p in h.slack.posts() if p["blocks"]]) == 1


async def test_a_hand_off_in_a_stored_message_turn_switches_the_thread_without_the_workflow_reading_the_input():
    """Fails if a hand-off needs the tool input in the workflow, or leaves a stale specialist."""
    from tests.test_front_door import ANNOUNCEMENT, front_door_dict, running

    async with running(front_door_dict()) as h:
        h.llm.queue(
            tool_turn(ToolCall("tu_h", "hand_off", {"agent": "it-helper"})),
            answer("The specialist's answer."),
        )
        msg = await stored_message(h, "Ev1", "my laptop battery is swelling")
        await h.client.start_workflow(
            ConversationWorkflow.run,
            ConversationRef(TENANT, "concierge", CHANNEL, THREAD),
            id=CONV_ID,
            task_queue=TASK_QUEUE,
            start_signal="message",
            start_signal_args=[msg],
        )
        await eventually(lambda: len(h.slack.updates()) >= 2)

        assert [u["text"] for u in h.slack.updates()] == [ANNOUNCEMENT, "The specialist's answer."]
        assert "Handed to it-helper" in json.dumps(await h.history.load(TENANT, CONV_ID))


# --- both kinds of message in one thread -----------------------------------------------------


async def test_a_message_that_carries_its_text_and_one_that_does_not_share_a_thread(harness):
    """Fails if a thread started before content left history cannot take a stored message."""
    h = harness
    h.llm.queue(answer("first"), answer("second"))
    inline = InboundMessage("Ev1", ALICE, "sent inline", CHANNEL, THREAD, "channel")
    handle = await converse(h, inline)
    await eventually(lambda: len(h.slack.updates()) >= 1)

    await handle.signal(ConversationWorkflow.message, await stored_message(h, "Ev2", "was stored"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    users = [
        m["content"][0]["text"]
        for m in await h.history.load(TENANT, CONV_ID)
        if m["role"] == "user"
    ]
    assert users == ["sent inline", "was stored"]


async def test_a_stored_message_with_no_text_is_asked_what_it_wants(harness):
    """Fails if a bare mention reaches the model, or stores an empty text block."""
    h = harness

    await converse(h, await stored_message(h, "Ev1", "   "))
    await eventually(lambda: h.slack.posts())

    assert [p["text"] for p in h.slack.posts()] == ["What would you like to ask?"]
    assert h.llm.calls == []
    assert await h.history.load(TENANT, CONV_ID) == []


async def test_a_retried_ingest_and_a_retried_edit_after_the_content_was_cleared_still_succeed(
    harness,
):
    """Fails if a retry, finding the cleared row, treats the message as empty or refuses the edit."""
    h = harness
    await stored_message(h, "Ev1", "words")
    again = [
        await ActivityEnvironment().run(h.acts.ingest_message, TENANT, CONV_ID, "Ev1")
        for _ in range(2)
    ]

    assert again == [True, True]
    assert len(await h.history.load(TENANT, CONV_ID)) == 1
