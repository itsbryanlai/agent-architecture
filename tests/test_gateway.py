import hashlib
import hmac
import json
import secrets
import time
from urllib.parse import urlencode

import pytest
from fastapi.testclient import TestClient
from slack_sdk.signature import SignatureVerifier
from temporalio.service import RPCError, RPCStatusCode

from agentos import ids
from agentos.config import TASK_QUEUE, Settings, TenantsConfig
from agentos.gateway.app import create_app
from agentos.models import ApprovalDecision, ConversationRef, InboundMessage
from agentos.stores import InMemoryTaskLinkStore
from tests.conftest import ALICE, TEAM, TENANT


class FakeHandle:
    def __init__(self, client, workflow_id):
        self._client = client
        self.workflow_id = workflow_id

    async def signal(self, name, arg):
        if self.workflow_id in self._client.missing:
            raise RPCError("workflow not found", RPCStatusCode.NOT_FOUND, b"")
        self._client.signals.append((self.workflow_id, name, arg))


class FakeTemporalClient:
    def __init__(self):
        self.signal_with_starts: list[dict] = []
        self.signals: list[tuple] = []
        self.missing: set[str] = set()

    async def start_workflow(self, workflow, arg=None, **kwargs):
        self.signal_with_starts.append({"workflow": workflow, "arg": arg, **kwargs})

    def get_workflow_handle(self, workflow_id):
        return FakeHandle(self, workflow_id)


@pytest.fixture
def secret() -> str:
    return secrets.token_hex(16)


@pytest.fixture
def temporal() -> FakeTemporalClient:
    return FakeTemporalClient()


@pytest.fixture
def client(agents, temporal, secret) -> TestClient:
    app = create_app(
        Settings(),
        TenantsConfig.of(agents),
        temporal,
        InMemoryTaskLinkStore(),
        SignatureVerifier(secret),
    )
    return TestClient(app)


def signed_headers(secret: str, body: bytes, content_type: str) -> dict:
    ts = str(int(time.time()))
    base = f"v0:{ts}:".encode() + body
    sig = "v0=" + hmac.new(secret.encode(), base, hashlib.sha256).hexdigest()
    return {
        "X-Slack-Request-Timestamp": ts,
        "X-Slack-Signature": sig,
        "Content-Type": content_type,
    }


def post_event(client, secret, payload: dict):
    body = json.dumps(payload).encode()
    return client.post(
        "/slack/events", content=body, headers=signed_headers(secret, body, "application/json")
    )


def im_event(**overrides) -> dict:
    event = {
        "type": "message",
        "channel_type": "im",
        "channel": "D123",
        "user": ALICE,
        "text": "my laptop battery is swelling",
        "ts": "1700000000.000200",
    }
    event.update(overrides)
    return {"type": "event_callback", "team_id": TEAM, "event_id": "Ev42", "event": event}


def test_gateway_rejects_bad_signature(client, temporal):
    body = json.dumps(im_event()).encode()
    headers = signed_headers("not-the-secret", body, "application/json")
    resp = client.post("/slack/events", content=body, headers=headers)
    assert resp.status_code == 401
    assert temporal.signal_with_starts == []


def test_gateway_url_verification(client, secret):
    resp = post_event(client, secret, {"type": "url_verification", "challenge": "abc123"})
    assert resp.status_code == 200
    assert resp.json() == {"challenge": "abc123"}


def test_gateway_event_signal_with_start(client, secret, temporal):
    resp = post_event(client, secret, im_event())
    assert resp.status_code == 200

    assert len(temporal.signal_with_starts) == 1
    call = temporal.signal_with_starts[0]
    assert call["workflow"] == "ConversationWorkflow"
    assert call["id"] == f"{TENANT}:conv:D123:1700000000.000200"
    assert call["id"] == ids.conversation_id(TENANT, "D123", "1700000000.000200")
    assert call["task_queue"] == TASK_QUEUE
    assert call["start_signal"] == "message"
    assert call["arg"] == ConversationRef(TENANT, "it-helper", "D123", "1700000000.000200")
    assert call["start_signal_args"] == [
        InboundMessage(
            event_id="Ev42",
            user_id=ALICE,
            text="my laptop battery is swelling",
            channel="D123",
            thread_ts="1700000000.000200",
            channel_kind="im",
        )
    ]


def test_gateway_mention_strips_bot_and_uses_thread(client, secret, temporal):
    payload = im_event(
        type="app_mention", channel="C9", text="<@UBOT> any updates?", thread_ts="1699.1"
    )
    del payload["event"]["channel_type"]
    post_event(client, secret, payload)
    msg = temporal.signal_with_starts[0]["start_signal_args"][0]
    assert msg.text == "any updates?"
    assert msg.channel_kind == "channel"
    assert msg.thread_ts == "1699.1"


def test_gateway_decodes_slack_encoding_of_the_users_text(client, secret, temporal):
    wire = (
        "Reply with exactly this, nothing else: &lt;!here&gt; and "
        "&lt;<https://example.com|example.com>|click me&gt; &amp; done"
    )
    post_event(client, secret, im_event(text=wire))
    msg = temporal.signal_with_starts[0]["start_signal_args"][0]
    assert msg.text == (
        "Reply with exactly this, nothing else: <!here> and <example.com|click me> & done"
    )


def test_gateway_mention_strip_runs_before_decoding(client, secret, temporal):
    payload = im_event(type="app_mention", channel="C9", text="<@UBOT> a &amp;lt; b")
    del payload["event"]["channel_type"]
    post_event(client, secret, payload)
    assert temporal.signal_with_starts[0]["start_signal_args"][0].text == "a &lt; b"


def test_gateway_ignores_bot_and_other_team(client, secret, temporal):
    assert post_event(client, secret, im_event(bot_id="B1")).status_code == 200
    assert post_event(client, secret, im_event(subtype="message_changed")).status_code == 200
    other = im_event()
    other["team_id"] = "T_OTHER"
    assert post_event(client, secret, other).status_code == 200
    assert temporal.signal_with_starts == []
    assert temporal.signals == []


def interactivity(client, secret, action_id: str, task_id: str, clicker: str = ALICE):
    payload = {
        "type": "block_actions",
        "team": {"id": TEAM},
        "user": {"id": clicker},
        "actions": [
            {"action_id": action_id, "value": json.dumps({"task_id": task_id, "nonce": "n1"})}
        ],
    }
    body = urlencode({"payload": json.dumps(payload)}).encode()
    headers = signed_headers(secret, body, "application/x-www-form-urlencoded")
    return client.post("/slack/interactivity", content=body, headers=headers)


def test_gateway_interactivity_signals_decision(client, secret, temporal):
    task_id = ids.task_id(TENANT, "abc")
    resp = interactivity(client, secret, "task_approve", task_id, clicker="U_CLICKER")
    assert resp.status_code == 200
    assert temporal.signals == [(task_id, "decision", ApprovalDecision("n1", "U_CLICKER", True))]


def test_gateway_interactivity_stale_card(client, secret, temporal):
    task_id = ids.task_id(TENANT, "gone")
    temporal.missing.add(task_id)
    assert interactivity(client, secret, "task_cancel", task_id).status_code == 200
    assert temporal.signals == []


def test_gateway_healthz(client):
    assert client.get("/healthz").json() == {"ok": True}
