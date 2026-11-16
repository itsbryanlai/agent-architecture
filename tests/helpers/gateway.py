"""Gateway helpers: a fake Temporal client, signed Slack requests, and running the relay."""

import asyncio
import hashlib
import hmac
import json
import time

from temporalio.service import RPCError, RPCStatusCode

from agentos.gateway.relay import InboundRelay
from tests.conftest import ALICE, TEAM


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


def deliver_inbound(app) -> int:
    """Run the relay once over the app's stored events; return how many it delivered."""
    d = app.state.deps
    return asyncio.run(InboundRelay(d.content, d.tenants, d.temporal_client).deliver_once())


def stored_text(app, tenant: str, event_id: str) -> str:
    """The text the gateway kept for an event."""
    d = app.state.deps
    return asyncio.run(d.content.get_inbound(tenant, event_id))["text"]
