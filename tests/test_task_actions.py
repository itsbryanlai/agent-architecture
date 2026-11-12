"""Comments on items and the Edit button on the approval card."""

import asyncio
import json
import secrets
import uuid
from urllib.parse import urlencode

import pytest
from fastapi.testclient import TestClient
from slack_sdk.signature import SignatureVerifier

from agentos import ids
from agentos.config import TASK_QUEUE, Settings
from agentos.gateway.app import create_app
from agentos.models import (
    ApprovalDecision,
    ConversationRef,
    DraftView,
    EditRequest,
    InboundMessage,
    LLMResponse,
    TaskSpec,
    ToolCall,
    TurnContext,
)
from agentos.slack_client import FakeSlack
from agentos.stores import InMemoryTaskLinkStore
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import MAX_EDITS, TaskWorkflow
from tests.conftest import ALICE, BOB, DB_ID, OTHER_DB_ID, TENANT, eventually, title_prop
from tests.test_gateway import FakeTemporalClient, signed_headers

NONCE = "nonce-1"


def ctx() -> TurnContext:
    return TurnContext(
        tenant_id=TENANT,
        agent_id="it-helper",
        agent_version=1,
        requester_user_id=ALICE,
        channel="C_GENERAL",
        thread_ts="1700000000.000100",
        channel_kind="channel",
    )


def comment_spec(item: str, comment: str = "Any update on this?") -> TaskSpec:
    return TaskSpec(ctx(), "add_comment", {"item": item, "comment": comment}, NONCE)


def create_spec() -> TaskSpec:
    payload = {"title": "New monitor", "details": "Mine flickers.", "database_id": DB_ID}
    return TaskSpec(ctx(), "create_item", payload, NONCE)


async def start(h, spec: TaskSpec):
    return await h.client.start_workflow(
        TaskWorkflow.run,
        spec,
        id=ids.task_id(TENANT, str(uuid.uuid4())),
        task_queue=TASK_QUEUE,
    )


async def wait_state(handle, expected: str):
    async def check():
        return await handle.query(TaskWorkflow.state) == expected

    await eventually(check, timeout=20)


async def draft(handle) -> DraftView:
    return await handle.query(TaskWorkflow.draft)


def cards(h):
    return [p for p in h.slack.posts() if p["blocks"]]


def thread_posts(h, needle: str):
    return [p for p in h.slack.posts() if p["thread_ts"] and needle in p["text"]]


def all_comment_text(h) -> list[str]:
    out = []
    for comments in h.notion.comments.values():
        for c in comments:
            out.append("".join(t["text"]["content"] for t in c["rich_text"]))
    return out


# --- Comment on items -----------------------------------------------------------


async def test_add_comment_offered_to_model(harness):
    h = harness
    h.llm.queue(LLMResponse(text="Hi", tool_calls=[], stop_reason="end_turn"))
    msg = InboundMessage("Ev1", ALICE, "hello", "C_GENERAL", "1700000000.000100", "channel")
    await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "it-helper", "C_GENERAL", "1700000000.000100"),
        id=ids.conversation_id(TENANT, "C_GENERAL", "1700000000.000100"),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )
    await eventually(lambda: h.llm.calls)

    tool = next(t for t in h.llm.calls[0]["tools"] if t["name"] == "add_comment")
    assert set(tool["input_schema"]["properties"]) == {"item", "comment"}
    assert tool["input_schema"]["required"] == ["item", "comment"]


async def test_add_comment_tool_call_starts_task(harness):
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[
                ToolCall("tu_1", "add_comment", {"item": page["url"], "comment": "Still broken"})
            ],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Drafted.", tool_calls=[], stop_reason="end_turn"),
    )
    msg = InboundMessage(
        "Ev2", ALICE, "comment please", "C_GENERAL", "1700000000.000100", "channel"
    )
    await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "it-helper", "C_GENERAL", "1700000000.000100"),
        id=ids.conversation_id(TENANT, "C_GENERAL", "1700000000.000100"),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )
    await eventually(lambda: cards(h))

    result = h.llm.calls[1]["messages"][-1]["content"][0]
    assert "approval card" in result["content"]
    assert h.notion.comments == {}


async def test_comment_on_ungranted_item_fails_without_card(harness):
    h = harness
    page = h.notion.add_page(OTHER_DB_ID, {"Name": title_prop("Secret HR case")})
    handle = await start(h, comment_spec(page["id"]))

    assert await handle.result() == "failed"

    assert cards(h) == []
    assert len(thread_posts(h, "can't comment")) == 1
    assert h.notion.count("create_comment") == 0
    assert all_comment_text(h) == []


async def test_comment_card_shows_item_and_comment(harness):
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    handle = await start(h, comment_spec(page["url"], "Please prioritise this."))
    await wait_state(handle, "awaiting_approval")

    card = cards(h)[0]
    assert card["channel"] == f"D-{ALICE}"
    text = json.dumps(card["blocks"])
    assert "wants to comment on *Broken VPN*" in text
    assert "Please prioritise this." in text
    actions = next(b for b in card["blocks"] if b["type"] == "actions")
    assert [e["action_id"] for e in actions["elements"]] == [
        "task_approve",
        "task_edit",
        "task_cancel",
    ]
    assert h.notion.count("create_comment") == 0


async def test_comment_runs_once_after_lost_response(harness):
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    handle = await start(h, comment_spec(page["id"], "Please prioritise this."))
    await wait_state(handle, "awaiting_approval")
    h.notion.fail_next_comment_after_write()

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))

    assert await handle.result() == "done"
    texts = all_comment_text(h)
    assert len(texts) == 1
    assert texts[0].startswith("Please prioritise this.")
    assert "ref " in texts[0] and "IT helper" in texts[0]
    assert len(thread_posts(h, "Commented on Broken VPN")) == 1
    assert [u["text"] for u in h.slack.updates()][-1] == "Commented: Broken VPN"


async def test_comment_cancel_posts_nothing(harness):
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    handle = await start(h, comment_spec(page["id"]))
    await wait_state(handle, "awaiting_approval")

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, False))

    assert await handle.result() == "cancelled"
    assert h.notion.count("create_comment") == 0
    assert all_comment_text(h) == []


# --- Edit on the card ---------------------------------------------------------------


class QueryableHandle:
    def __init__(self, client, workflow_id):
        self._client, self.workflow_id = client, workflow_id

    async def query(self, name, result_type=None):
        assert name == "draft"
        return self._client.draft

    async def signal(self, name, arg):
        self._client.signals.append((self.workflow_id, name, arg))


class QueryableTemporal(FakeTemporalClient):
    def __init__(self, draft: DraftView):
        super().__init__()
        self.draft = draft

    def get_workflow_handle(self, workflow_id):
        return QueryableHandle(self, workflow_id)


TASK_ID = ids.task_id(TENANT, "abc")


def gateway(agents, draft: DraftView):
    sec = secrets.token_hex(16)
    temporal = QueryableTemporal(draft)
    slack = FakeSlack()
    app = create_app(
        Settings(),
        agents,
        temporal,
        InMemoryTaskLinkStore(),
        SignatureVerifier(sec),
        slack=slack,
    )
    return TestClient(app), sec, temporal, slack


def post_interaction(client, sec, payload: dict):
    body = urlencode({"payload": json.dumps(payload)}).encode()
    headers = signed_headers(sec, body, "application/x-www-form-urlencoded")
    return client.post("/slack/interactivity", content=body, headers=headers)


def an_edit_draft(editable: bool = True) -> DraftView:
    return DraftView(
        tool_name="create_item",
        fields={"title": "New monitor", "details": "Mine flickers."},
        nonce="n-current",
        requester_user_id=ALICE,
        editable=editable,
    )


def edit_click(clicker: str, nonce: str = "n-current") -> dict:
    return {
        "type": "block_actions",
        "user": {"id": clicker},
        "trigger_id": "trig-1",
        "actions": [
            {
                "action_id": "task_edit",
                "value": json.dumps({"task_id": TASK_ID, "nonce": nonce}),
            }
        ],
    }


def test_edit_click_opens_prefilled_modal(agents):
    client, sec, _, slack = gateway(agents, an_edit_draft())

    assert post_interaction(client, sec, edit_click(ALICE)).status_code == 200

    [opened] = slack.views()
    assert opened["trigger_id"] == "trig-1"
    view = opened["view"]
    assert view["callback_id"] == "task_edit"
    assert json.loads(view["private_metadata"]) == {"task_id": TASK_ID, "nonce": "n-current"}
    inputs = {b["block_id"]: b["element"] for b in view["blocks"]}
    assert inputs["title"]["initial_value"] == "New monitor"
    assert inputs["title"]["max_length"] == 150
    assert inputs["details"]["initial_value"] == "Mine flickers."
    assert inputs["details"]["max_length"] == 3000


def test_edit_click_by_someone_else_is_ignored(agents):
    client, sec, _, slack = gateway(agents, an_edit_draft())

    assert post_interaction(client, sec, edit_click(BOB)).status_code == 200
    # An old card (stale nonce) and a draft that is no longer editable open nothing either.
    assert post_interaction(client, sec, edit_click(ALICE, nonce="n-old")).status_code == 200
    client2, sec2, _, slack2 = gateway(agents, an_edit_draft(editable=False))
    assert post_interaction(client2, sec2, edit_click(ALICE)).status_code == 200

    assert slack.views() == []
    assert slack2.views() == []


def submission(values: dict, user: str = ALICE) -> dict:
    return {
        "type": "view_submission",
        "user": {"id": user},
        "view": {
            "callback_id": "task_edit",
            "private_metadata": json.dumps({"task_id": TASK_ID, "nonce": "n-current"}),
            "state": {
                "values": {
                    k: {"value": {"type": "plain_text_input", "value": v}}
                    for k, v in values.items()
                }
            },
        },
    }


def test_empty_edit_returns_modal_errors(agents):
    client, sec, temporal, _ = gateway(agents, an_edit_draft())

    resp = post_interaction(client, sec, submission({"title": "   ", "details": "ok"}))

    assert resp.status_code == 200
    body = resp.json()
    assert body["response_action"] == "errors"
    assert list(body["errors"]) == ["title"]
    assert temporal.signals == []


def test_valid_edit_submission_signals_and_closes_modal(agents):
    client, sec, temporal, _ = gateway(agents, an_edit_draft())

    resp = post_interaction(client, sec, submission({"title": " Better ", "details": "Fine"}))

    assert resp.status_code == 200
    assert resp.content == b""
    assert temporal.signals == [
        (TASK_ID, "edit", EditRequest("n-current", ALICE, {"title": "Better", "details": "Fine"}))
    ]


async def test_edit_replaces_draft_and_requires_new_approval(harness):
    h = harness
    handle = await start(h, create_spec())
    await wait_state(handle, "awaiting_approval")
    before = await draft(handle)
    assert before.editable and before.nonce == NONCE

    await handle.signal(
        TaskWorkflow.edit, EditRequest(NONCE, ALICE, {"title": "Better monitor", "details": "New."})
    )
    after = await eventually(lambda: _changed(handle))

    assert after.nonce != NONCE
    assert after.fields == {"title": "Better monitor", "details": "New."}
    # The card was redrawn in place with the new nonce and the edit note.
    refreshed = await eventually(lambda: h.slack.updates())
    text = json.dumps(refreshed[-1]["blocks"], ensure_ascii=False)
    assert "Better monitor" in text and "Edited — review and approve again" in text
    assert after.nonce in text and NONCE not in text

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))
    await asyncio.sleep(0.5)
    assert await handle.query(TaskWorkflow.state) == "awaiting_approval"
    assert h.notion.count("create_page") == 0

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(after.nonce, ALICE, True))
    await wait_state(handle, "watching")
    [page] = h.notion.all_pages(DB_ID)
    assert page["properties"]["Name"]["title"][0]["plain_text"] == "Better monitor"


async def _changed(handle):
    d = await draft(handle)
    return d if d.nonce != NONCE else None


async def test_bad_edits_are_ignored(harness):
    h = harness
    handle = await start(h, create_spec())
    await wait_state(handle, "awaiting_approval")

    await handle.signal(TaskWorkflow.edit, EditRequest(NONCE, BOB, {"title": "x", "details": "y"}))
    await handle.signal(
        TaskWorkflow.edit, EditRequest("old", ALICE, {"title": "x", "details": "y"})
    )
    await handle.signal(TaskWorkflow.edit, EditRequest(NONCE, ALICE, {"title": "x"}))
    await handle.signal(
        TaskWorkflow.edit, EditRequest(NONCE, ALICE, {"title": " ", "details": "y"})
    )
    await handle.signal(
        TaskWorkflow.edit, EditRequest(NONCE, ALICE, {"title": "t" * 151, "details": "y"})
    )
    await asyncio.sleep(0.5)

    d = await draft(handle)
    assert d.nonce == NONCE and d.fields["title"] == "New monitor"


async def test_edit_limit(harness):
    h = harness
    handle = await start(h, create_spec())
    await wait_state(handle, "awaiting_approval")

    nonce = NONCE
    for i in range(MAX_EDITS):
        await handle.signal(
            TaskWorkflow.edit, EditRequest(nonce, ALICE, {"title": f"T{i}", "details": "d"})
        )
        nonce = (await eventually(lambda n=nonce: _nonce_not(handle, n))).nonce

    last = await draft(handle)
    assert last.fields["title"] == f"T{MAX_EDITS - 1}"
    assert last.editable is False

    await handle.signal(
        TaskWorkflow.edit, EditRequest(nonce, ALICE, {"title": "too many", "details": "d"})
    )
    await asyncio.sleep(0.5)
    after = await draft(handle)
    assert after.nonce == nonce and after.fields["title"] == f"T{MAX_EDITS - 1}"
    assert after.editable is False


async def _nonce_not(handle, nonce):
    d = await draft(handle)
    return d if d.nonce != nonce else None


@pytest.fixture(autouse=True)
def _quiet_workflow_logs(caplog):
    caplog.set_level("ERROR", logger="temporalio.workflow")
