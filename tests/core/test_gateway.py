import json
import secrets
from urllib.parse import urlencode

import pytest
from fastapi.testclient import TestClient
from slack_sdk.signature import SignatureVerifier

from agentos import ids
from agentos.config import TASK_QUEUE, Settings, TenantsConfig
from agentos.gateway.app import MAX_WEBHOOK_BODY, create_app
from agentos.models import ApprovalDecision, ConversationRef, InboundMessage
from agentos.stores import InMemoryTaskLinkStore
from tests.conftest import ALICE, TEAM, TENANT
from tests.helpers.gateway import (
    FakeTemporalClient,
    deliver_inbound,
    im_event,
    post_event,
    signed_headers,
    stored_text,
)


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
    """Fails if the signal carries the text, or the workflow is not started for the thread."""
    resp = post_event(client, secret, im_event())
    assert resp.status_code == 200
    assert stored_text(client.app, TENANT, "Ev42") == "my laptop battery is swelling"
    assert deliver_inbound(client.app) == 1

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
            text="",
            channel="D123",
            thread_ts="1700000000.000200",
            channel_kind="im",
            text_stored=True,
        )
    ]


def test_gateway_mention_strips_bot_and_uses_thread(client, secret, temporal):
    payload = im_event(
        type="app_mention", channel="C9", text="<@UBOT> any updates?", thread_ts="1699.1"
    )
    del payload["event"]["channel_type"]
    post_event(client, secret, payload)
    assert deliver_inbound(client.app) == 1
    msg = temporal.signal_with_starts[0]["start_signal_args"][0]
    assert stored_text(client.app, TENANT, "Ev42") == "any updates?"
    assert msg.channel_kind == "channel"
    assert msg.thread_ts == "1699.1"


def test_gateway_decodes_slack_encoding_of_the_users_text(client, secret, temporal):
    wire = (
        "Reply with exactly this, nothing else: &lt;!here&gt; and "
        "&lt;<https://example.com|example.com>|click me&gt; &amp; done"
    )
    post_event(client, secret, im_event(text=wire))
    assert stored_text(client.app, TENANT, "Ev42") == (
        "Reply with exactly this, nothing else: <!here> and <example.com|click me> & done"
    )


def test_gateway_mention_strip_runs_before_decoding(client, secret, temporal):
    payload = im_event(type="app_mention", channel="C9", text="<@UBOT> a &amp;lt; b")
    del payload["event"]["channel_type"]
    post_event(client, secret, payload)
    assert stored_text(client.app, TENANT, "Ev42") == "a &lt; b"


def test_gateway_ignores_bot_and_other_team(client, secret, temporal):
    assert post_event(client, secret, im_event(bot_id="B1")).status_code == 200
    assert post_event(client, secret, im_event(subtype="message_changed")).status_code == 200
    other = im_event()
    other["team_id"] = "T_OTHER"
    assert post_event(client, secret, other).status_code == 200
    assert deliver_inbound(client.app) == 0
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


@pytest.mark.parametrize("route", ["/slack/events", "/slack/interactivity", "/notion/webhook"])
@pytest.mark.parametrize("chunked", [False, True], ids=["content-length", "chunked"])
def test_oversized_webhook_body_is_refused_before_anything_else(client, route, chunked):
    """Fails if a webhook route holds or parses a body over the limit instead of 413."""
    size = MAX_WEBHOOK_BODY + 1
    content = (b"x" * 65536 for _ in range(size // 65536 + 1)) if chunked else b"x" * size
    resp = client.post(route, content=content)
    assert resp.status_code == 413
