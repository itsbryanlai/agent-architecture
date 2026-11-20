import json
import secrets
from dataclasses import replace
from datetime import UTC, datetime
from urllib.parse import urlencode

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from slack_sdk.signature import SignatureVerifier

from agentos import ids
from agentos.audit import InMemoryAuditStore
from agentos.config import TASK_QUEUE, Settings, TenantsConfig
from agentos.connectors.google_iam import FakeGoogleAccounts, GoogleError
from agentos.credentials import NOTION, SLACK_BOT, InMemoryCredentialStore, TenantCredentials
from agentos.drive_connections import InMemoryDriveConnectionStore
from agentos.gateway.app import MAX_WEBHOOK_BODY, create_app
from agentos.models import ApprovalDecision, ConversationRef, InboundMessage
from agentos.slack_client import FakeSlack
from agentos.slack_install import installed_tenant_secrets
from agentos.stores import InMemoryTaskLinkStore
from agentos.tenant_store import InMemoryTenantStore, TenantDirectory
from agentos.unit_of_work import InMemoryUnitOfWork
from tests.conftest import ALICE, TEAM, TENANT
from tests.helpers.gateway import (
    FakeTemporalClient,
    deliver_inbound,
    im_event,
    post_event,
    signed_headers,
    stored_text,
)
from tests.helpers.keys import RefreshableKeyStore


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


async def test_gateway_event_signal_with_start(client, secret, temporal):
    """Fails if the signal carries the text, or the workflow is not started for the thread."""
    resp = post_event(client, secret, im_event())
    assert resp.status_code == 200
    assert await stored_text(client.app, TENANT, "Ev42") == "my laptop battery is swelling"
    assert await deliver_inbound(client.app) == 1

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


async def test_gateway_mention_strips_bot_and_uses_thread(client, secret, temporal):
    payload = im_event(
        type="app_mention", channel="C9", text="<@UBOT> any updates?", thread_ts="1699.1"
    )
    del payload["event"]["channel_type"]
    post_event(client, secret, payload)
    assert await deliver_inbound(client.app) == 1
    msg = temporal.signal_with_starts[0]["start_signal_args"][0]
    assert await stored_text(client.app, TENANT, "Ev42") == "any updates?"
    assert msg.channel_kind == "channel"
    assert msg.thread_ts == "1699.1"


async def test_gateway_decodes_slack_encoding_of_the_users_text(client, secret, temporal):
    wire = (
        "Reply with exactly this, nothing else: &lt;!here&gt; and "
        "&lt;<https://example.com|example.com>|click me&gt; &amp; done"
    )
    post_event(client, secret, im_event(text=wire))
    assert await stored_text(client.app, TENANT, "Ev42") == (
        "Reply with exactly this, nothing else: <!here> and <example.com|click me> & done"
    )


async def test_gateway_mention_strip_runs_before_decoding(client, secret, temporal):
    payload = im_event(type="app_mention", channel="C9", text="<@UBOT> a &amp;lt; b")
    del payload["event"]["channel_type"]
    post_event(client, secret, payload)
    assert await stored_text(client.app, TENANT, "Ev42") == "a &lt; b"


async def test_gateway_ignores_bot_and_other_team(client, secret, temporal):
    assert post_event(client, secret, im_event(bot_id="B1")).status_code == 200
    assert post_event(client, secret, im_event(subtype="message_changed")).status_code == 200
    other = im_event()
    other["team_id"] = "T_OTHER"
    assert post_event(client, secret, other).status_code == 200
    assert await deliver_inbound(client.app) == 0
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


@pytest.mark.parametrize("app_name", ["This app", "Acme Helper"], ids=["fallback", "named"])
async def test_a_tenant_with_no_agent_gets_one_fixed_not_set_up_reply(
    agents, temporal, secret, app_name
):
    """Fails if a message in a tenant with no default agent starts a workflow, or is answered
    with anything but the fixed not-set-up text naming the app, once per event."""
    fresh = replace(
        agents,
        tenant_id="newco",
        slack_team_id="T_NEWCO",
        agents={},
        default_agent=None,
        channels={},
        secrets=installed_tenant_secrets("newco"),
    )
    slack = FakeSlack()

    async def slack_for(tenant_id):
        return slack

    app = create_app(
        Settings(console_url="https://console.example.com"),
        TenantsConfig.of(agents, fresh),
        temporal,
        InMemoryTaskLinkStore(),
        SignatureVerifier(secret),
        slack_for=slack_for,
        app_name=app_name,
    )
    client = TestClient(app)
    event = im_event()
    event["team_id"], event["event_id"] = "T_NEWCO", "EvNew"
    assert post_event(client, secret, event).status_code == 200
    # A Slack retry of the same event posts nothing.
    assert post_event(client, secret, event).status_code == 200
    # The sentinel: a message in the set-up tenant still reaches its workflow.
    assert post_event(client, secret, im_event()).status_code == 200
    assert await deliver_inbound(app) == 1

    assert len(slack.posts()) == 1
    post = slack.posts()[0]
    assert post["text"] == (
        f"{app_name} is installed but not set up yet. "
        "An admin can finish setup at https://console.example.com."
    )
    assert (post["channel"], post["thread_ts"]) == ("D123", "1700000000.000200")
    assert [c["arg"].tenant_id for c in temporal.signal_with_starts] == [TENANT]


# --- uninstall --------------------------------------------------------------------

UNINSTALL_TOKEN = "xoxb-made-up-uninstall"
REINSTALL_TOKEN = "xoxb-made-up-reinstall"
NOTION_TOKEN = "ntn-made-up-kept"


@pytest_asyncio.fixture
async def uninstall_gateway(agents, temporal, secret):
    """A gateway with the uninstall flow's stores, a tenant holding both credentials
    and a connected Drive account."""
    store = InMemoryTenantStore(agents)
    credential_store = InMemoryCredentialStore()
    key = secrets.token_bytes(32)
    keys = RefreshableKeyStore({(TENANT, 1): key})
    await keys.refresh()
    credentials = TenantCredentials(credential_store, keys)
    await credentials.store_secret(TENANT, SLACK_BOT, UNINSTALL_TOKEN, 1, key)
    await credentials.store_secret(TENANT, NOTION, NOTION_TOKEN, 1, key)
    drive = InMemoryDriveConnectionStore()
    google = FakeGoogleAccounts()
    account_id = ids.drive_account_id(TENANT)
    await google.ensure_account(account_id)
    await drive.connect(TENANT, google.account_email(account_id), datetime(2026, 1, 1, tzinfo=UTC))
    audit = InMemoryAuditStore()
    app = create_app(
        Settings(),
        TenantDirectory(store),
        temporal,
        InMemoryTaskLinkStore(),
        SignatureVerifier(secret),
        tenant_store=store,
        credentials=credentials,
        audit=audit,
        uow=InMemoryUnitOfWork(store, credential_store, audit, drive),
        drive=drive,
        google=google,
    )
    return TestClient(app), store, credential_store, audit, credentials, drive, google


@pytest.mark.parametrize(
    "case",
    [
        "app_uninstalled",
        "tokens_revoked bot",
        "repeat and out of order",
        "late retry after reinstall",
        "unknown team",
        "oauth only",
        "drive delete fails",
    ],
    ids=[
        "app-uninstalled",
        "tokens-revoked",
        "repeat",
        "late-retry-after-reinstall",
        "unknown-team",
        "oauth-only",
        "drive-delete-fails",
    ],
)
async def test_uninstall_events(uninstall_gateway, secret, temporal, case):
    """Fails if an uninstall event leaves the tenant enabled, its Slack token stored, its
    Notion token deleted or its Drive account and row kept, a repeat or out-of-order event
    audits again, a late retry of a processed event disables a reinstalled tenant or deletes
    its fresh token, an unknown team or a user-token-only revocation is acted on, or a
    refused Google delete blocks the uninstall."""
    client, store, credential_store, audit, credentials, drive, google = uninstall_gateway
    uninstall = {"type": "app_uninstalled"}
    revoked_bot = {"type": "tokens_revoked", "tokens": {"oauth": [], "bot": [UNINSTALL_TOKEN]}}
    revoked_oauth = {
        "type": "tokens_revoked",
        "tokens": {"oauth": ["xoxp-made-up-user"], "bot": []},
    }
    payloads = {
        "app_uninstalled": [(TEAM, "EvU1", uninstall)],
        "tokens_revoked bot": [(TEAM, "EvU1", revoked_bot)],
        "repeat and out of order": [(TEAM, "EvU1", uninstall), (TEAM, "EvU2", revoked_bot)],
        "late retry after reinstall": [(TEAM, "EvU1", uninstall)],
        "unknown team": [("T_OTHER", "EvU1", uninstall)],
        "oauth only": [(TEAM, "EvU1", revoked_oauth)],
        "drive delete fails": [(TEAM, "EvU1", uninstall)],
    }[case]
    if case == "drive delete fails":
        google.fail_next_delete(GoogleError("unavailable"))
    for team, event_id, event in payloads:
        payload = {"type": "event_callback", "team_id": team, "event_id": event_id, "event": event}
        assert post_event(client, secret, payload).status_code == 200
    if case == "late retry after reinstall":
        # A reinstall re-enables the tenant and stores a fresh token; then Slack delivers
        # the same uninstall event again, late.
        await store.set_enabled(TENANT, True)
        await credentials.store_with_current_key(TENANT, SLACK_BOT, REINSTALL_TOKEN)
        payload = {
            "type": "event_callback",
            "team_id": TEAM,
            "event_id": "EvU1",
            "event": uninstall,
        }
        assert post_event(client, secret, payload).status_code == 200
    # The settle: a relay pass finds nothing to deliver, and no workflow was started.
    assert await deliver_inbound(client.app) == 0
    assert temporal.signal_with_starts == []

    ref = await store.ref_for_slack_team(TEAM)
    if case in ("unknown team", "oauth only"):
        assert ref.enabled is True
        assert await credential_store.get(TENANT, SLACK_BOT) is not None
        assert audit.events == []
        assert await drive.get(TENANT) is not None
        assert google.accounts == {
            ids.drive_account_id(TENANT): google.account_email(ids.drive_account_id(TENANT))
        }
        return
    if case == "late retry after reinstall":
        assert ref.enabled is True
        row = await store.load(TENANT)
        assert await credentials.slack_bot_token(row.config) == REINSTALL_TOKEN
        assert await credential_store.get(TENANT, NOTION) is not None
        assert [(e.kind, e.tool) for e in audit.events] == [("uninstall", "app_uninstalled")]
        assert await drive.get(TENANT) is None
        assert google.accounts == {}
        return
    assert ref.enabled is False
    assert await credential_store.get(TENANT, SLACK_BOT) is None
    assert await credential_store.get(TENANT, NOTION) is not None
    assert await drive.get(TENANT) is None
    if case == "drive delete fails":
        # The uninstall stands; the account is left at Google, logged, and a reconnect
        # finds it again by its deterministic ID.
        assert google.accounts == {
            ids.drive_account_id(TENANT): google.account_email(ids.drive_account_id(TENANT))
        }
    else:
        assert google.accounts == {}
    assert len(audit.events) == 1
    recorded = audit.events[0]
    assert (recorded.kind, recorded.tool, recorded.target, recorded.identity) == (
        "uninstall",
        "tokens_revoked" if case == "tokens_revoked bot" else "app_uninstalled",
        TEAM,
        "system:uninstall",
    )
    assert recorded.agent_id is None
    assert UNINSTALL_TOKEN not in repr(recorded)
