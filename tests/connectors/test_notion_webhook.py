"""The Notion webhook accepts only signed events and never logs the token."""

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import stat
from datetime import UTC, datetime

import httpx
import pytest
from fastapi.testclient import TestClient
from slack_sdk.signature import SignatureVerifier

from agentos.agent_store import seeded_agent_store
from agentos.audit import InMemoryAuditStore, event_json
from agentos.config import Settings, TenantsConfig
from agentos.crypto_keys import EnvKeyStore, key_var_name
from agentos.gateway.app import create_app
from agentos.gateway.notion_signature import verify_notion_signature
from agentos.notion_webhook import (
    InMemoryWebhookTokenStore,
    WebhookTokens,
    build_parser,
    execute,
)
from agentos.payload_codec import decrypt_for_tenant, encrypt_for_tenant
from agentos.stores import InMemoryTaskLinkStore
from tests.conftest import TENANT
from tests.helpers.gateway import FakeTemporalClient

TOKEN = "secret_verification_token_abc123"
PAGE_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
TASK_ID = f"{TENANT}:task:t1"
NOW = datetime(2026, 11, 19, 12, 0, tzinfo=UTC)


def sign(body: bytes, token: str = TOKEN) -> str:
    return "sha256=" + hmac.new(token.encode(), body, hashlib.sha256).hexdigest()


def page_event(event_type: str = "page.properties_updated", page_id: str = PAGE_ID) -> dict:
    return {"id": "evt-1", "type": event_type, "entity": {"id": page_id, "type": "page"}}


@pytest.fixture
def temporal() -> FakeTemporalClient:
    return FakeTemporalClient()


@pytest.fixture
def links() -> InMemoryTaskLinkStore:
    store = InMemoryTaskLinkStore()
    store.links[(TENANT, PAGE_ID.replace("-", ""))] = TASK_ID
    return store


def make_client(
    agents, temporal, links, tmp_path, monkeypatch, token: str | None = TOKEN
) -> TestClient:
    # The secret is read from the environment variable the tenant's config names.
    if token is None:
        monkeypatch.delenv("NOTION_WEBHOOK_VERIFICATION_TOKEN", raising=False)
    else:
        monkeypatch.setenv("NOTION_WEBHOOK_VERIFICATION_TOKEN", token)
    settings = Settings(notion_token_dir=str(tmp_path / "sub"))
    return TestClient(
        create_app(
            settings,
            TenantsConfig.of(agents),
            temporal,
            links,
            SignatureVerifier("x"),
            agents=seeded_agent_store(agents),
        )
    )


def saved_path(tmp_path):
    return tmp_path / "sub" / f"notion-webhook-verification-token-{TENANT}"


def post(client, payload: dict, signature: str | None):
    body = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if signature is not None:
        headers["X-Notion-Signature"] = signature
    return client.post("/notion/webhook", content=body, headers=headers)


def test_handshake_writes_token_file_and_does_not_log_it(
    agents, temporal, links, tmp_path, monkeypatch, caplog
):
    client = make_client(agents, temporal, links, tmp_path, monkeypatch, token=None)
    with caplog.at_level(logging.DEBUG):
        resp = post(client, {"verification_token": TOKEN}, None)
    path = saved_path(tmp_path)
    assert resp.status_code == 200
    assert path.read_text() == TOKEN
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(path.parent).st_mode) == 0o700
    assert str(path) in caplog.text
    assert TOKEN not in caplog.text


def test_handshake_with_signature_header_writes_token_file(
    agents, temporal, links, tmp_path, monkeypatch
):
    client = make_client(agents, temporal, links, tmp_path, monkeypatch, token=None)
    body = json.dumps({"verification_token": TOKEN}).encode()
    resp = post(client, {"verification_token": TOKEN}, sign(body))
    assert resp.status_code == 200
    assert (saved_path(tmp_path)).read_text() == TOKEN


def test_event_with_type_and_token_field_is_not_a_handshake(
    agents, temporal, links, tmp_path, monkeypatch
):
    client = make_client(agents, temporal, links, tmp_path, monkeypatch, token=None)
    payload = {**page_event(), "verification_token": "x"}
    assert post(client, payload, None).status_code == 503
    assert not (saved_path(tmp_path)).exists()


def test_second_handshake_is_refused_and_the_file_unchanged(
    agents, temporal, links, tmp_path, monkeypatch, caplog
):
    """Fails if a second handshake replaces the token file the operator has not deleted."""
    client = make_client(agents, temporal, links, tmp_path, monkeypatch, token=None)
    post(client, {"verification_token": "original-token"}, None)
    with caplog.at_level(logging.DEBUG):
        resp = post(client, {"verification_token": "replacement-token"}, None)
    assert resp.status_code == 409
    assert (saved_path(tmp_path)).read_text() == "original-token"
    assert "replacement-token" not in caplog.text
    assert TENANT in caplog.text


def test_handshake_is_accepted_again_once_the_operator_deletes_the_file(
    agents, temporal, links, tmp_path, monkeypatch
):
    """Fails if deleting the file does not reopen the handshake."""
    client = make_client(agents, temporal, links, tmp_path, monkeypatch, token=None)
    post(client, {"verification_token": "original-token"}, None)
    saved_path(tmp_path).unlink()
    assert post(client, {"verification_token": "replacement-token"}, None).status_code == 200
    assert saved_path(tmp_path).read_text() == "replacement-token"


@pytest.mark.parametrize(
    "payload",
    [
        {"verification_token": "t", "extra": "x"},
        {"verification_token": ""},
        {"verification_token": "t" * 1025},
    ],
    ids=["extra-key", "empty", "too-long"],
)
def test_handshake_with_another_shape_is_refused(
    agents, temporal, links, tmp_path, monkeypatch, payload
):
    """Fails if the handshake accepts a body that is not exactly one sane token."""
    client = make_client(agents, temporal, links, tmp_path, monkeypatch, token=None)
    resp = post(client, payload, None)
    assert resp.status_code == 400
    assert not saved_path(tmp_path).exists()


def test_handshake_refuses_a_symlink_at_the_token_path(
    agents, temporal, links, tmp_path, monkeypatch
):
    """Fails if the handshake writes through a symlink planted at the token path."""
    client = make_client(agents, temporal, links, tmp_path, monkeypatch, token=None)
    target = tmp_path / "victim"
    target.write_text("keep")
    path = saved_path(tmp_path)
    path.parent.mkdir()
    path.symlink_to(target)
    resp = post(client, {"verification_token": "t"}, None)
    assert resp.status_code == 409
    assert target.read_text() == "keep"


def test_handshake_refused_when_token_configured(
    agents, temporal, links, tmp_path, monkeypatch, caplog
):
    client = make_client(agents, temporal, links, tmp_path, monkeypatch)
    path = saved_path(tmp_path)
    path.parent.mkdir()
    path.write_text("known-content")
    with caplog.at_level(logging.DEBUG):
        resp = post(client, {"verification_token": "attacker-value"}, None)
    assert resp.status_code == 409
    assert path.read_text() == "known-content"
    assert "attacker-value" not in caplog.text
    assert TOKEN not in caplog.text
    assert "handshake refused" in caplog.text


def test_event_rejected_when_token_not_configured(agents, temporal, links, tmp_path, monkeypatch):
    client = make_client(agents, temporal, links, tmp_path, monkeypatch, token=None)
    body = json.dumps(page_event()).encode()
    assert post(client, page_event(), sign(body)).status_code == 503
    assert temporal.signals == []


@pytest.mark.parametrize("event_type", ["page.properties_updated", "page.content_updated"])
@pytest.mark.parametrize("signature", [sign(b"other body"), None], ids=["wrong body", "missing"])
def test_event_with_bad_signature_rejected(
    agents, temporal, links, tmp_path, monkeypatch, caplog, event_type, signature
):
    """Fails if an event with a missing or wrong signature is accepted, signals anything, or logs the token."""
    client = make_client(agents, temporal, links, tmp_path, monkeypatch)
    with caplog.at_level(logging.DEBUG):
        resp = post(client, page_event(event_type), signature)
    assert resp.status_code == 401
    assert temporal.signals == []
    assert temporal.signal_with_starts == []
    assert TOKEN not in caplog.text


def test_signed_event_signals_task(agents, temporal, links, tmp_path, monkeypatch):
    client = make_client(agents, temporal, links, tmp_path, monkeypatch)
    body = json.dumps(page_event()).encode()
    resp = post(client, page_event(), sign(body))
    assert resp.status_code == 200
    assert len(temporal.signals) == 1
    workflow_id, name, arg = temporal.signals[0]
    assert (workflow_id, name) == (TASK_ID, "external_update")
    assert arg.source == "notion_webhook"


def test_verify_signature_unit():
    body = b'{"a": 1}'
    good = sign(body)
    assert verify_notion_signature(body, good, TOKEN)
    assert not verify_notion_signature(body, sign(body, "wrong"), TOKEN)
    assert not verify_notion_signature(b'{"a": 2}', good, TOKEN)
    assert not verify_notion_signature(body, good.removeprefix("sha256="), TOKEN)
    assert not verify_notion_signature(body, None, TOKEN)
    assert not verify_notion_signature(body, "", TOKEN)


@pytest.mark.parametrize("header", [None, "", "no-prefix", "sha1=abcd", "sha256=bad"])
def test_the_signature_is_computed_even_for_a_header_that_cannot_match(header):
    """Fails if a missing or mis-prefixed header returns before the HMAC, so its timing differs."""
    calls = []

    def counting_mac(key, msg, digest):
        calls.append(key)
        return hmac.new(key, msg, digest)

    assert not verify_notion_signature(b"{}", header, TOKEN, mac=counting_mac)
    assert len(calls) == 1


# --- the token in the database ---------------------------------------------------------


def make_db_app(agents, temporal, links, tmp_path, monkeypatch):
    """A gateway whose webhook tokens live in the (fake) database, encrypted under a generated key."""
    monkeypatch.delenv("NOTION_WEBHOOK_VERIFICATION_TOKEN", raising=False)
    store = InMemoryWebhookTokenStore()
    keys = EnvKeyStore(
        {key_var_name(TENANT, 1): base64.b64encode(secrets.token_bytes(32)).decode()}
    )
    settings = Settings(notion_token_dir=str(tmp_path / "sub"))
    app = create_app(
        settings,
        TenantsConfig.of(agents),
        temporal,
        links,
        SignatureVerifier("x"),
        agents=seeded_agent_store(agents),
        webhook_tokens=WebhookTokens(store, keys),
    )
    return app, store, keys


def db_client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def apost(client, payload: dict, signature: str | None):
    body = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if signature is not None:
        headers["X-Notion-Signature"] = signature
    return await client.post("/notion/webhook", content=body, headers=headers)


async def test_handshake_stores_a_pending_token_once(
    agents, temporal, links, tmp_path, monkeypatch, caplog
):
    """Fails if a second handshake replaces the pending token, or the token reaches a log or a file."""
    app, store, keys = make_db_app(agents, temporal, links, tmp_path, monkeypatch)
    async with db_client(app) as client:
        with caplog.at_level(logging.DEBUG):
            assert (await apost(client, {"verification_token": TOKEN}, None)).status_code == 200
            assert (
                await apost(client, {"verification_token": "replacement-token"}, None)
            ).status_code == 409
    row = store.snapshot()[TENANT]
    assert row.state == "pending"
    assert decrypt_for_tenant(keys, TENANT, row.ciphertext, row.key_version) == TOKEN.encode()
    assert TOKEN.encode() not in row.ciphertext
    assert TOKEN not in caplog.text
    assert "replacement-token" not in caplog.text
    assert TENANT in caplog.text
    assert not (tmp_path / "sub").exists()


@pytest.mark.parametrize(
    ("confirmed", "expected"), [(False, 503), (True, 200)], ids=["pending", "confirmed"]
)
async def test_an_event_is_verified_only_against_the_confirmed_token(
    agents, temporal, links, tmp_path, monkeypatch, confirmed, expected
):
    """Fails if an event signed with the pending token is accepted, or one signed with the confirmed token is refused."""
    app, store, _keys = make_db_app(agents, temporal, links, tmp_path, monkeypatch)
    async with db_client(app) as client:
        await apost(client, {"verification_token": TOKEN}, None)
        if confirmed:
            await store.confirm(TENANT, NOW)
        body = json.dumps(page_event()).encode()
        resp = await apost(client, page_event(), sign(body))
    assert resp.status_code == expected
    assert len(temporal.signals) == (1 if confirmed else 0)


async def test_show_confirm_and_clear_manage_the_token_and_audit_the_operator(monkeypatch, capsys):
    """Fails if show prints a confirmed token, confirm leaves it pending, clear keeps it, or an audit row names the token or misses the operator."""
    store = InMemoryWebhookTokenStore()
    audit = InMemoryAuditStore()
    keys = EnvKeyStore(
        {key_var_name(TENANT, 1): base64.b64encode(secrets.token_bytes(32)).decode()}
    )
    blob, version = encrypt_for_tenant(keys, TENANT, TOKEN.encode())
    await store.put_pending(TENANT, blob, version)
    monkeypatch.setattr("agentos.notion_webhook.operator_identity", lambda: "operator:ops")

    def args(command):
        return build_parser().parse_args(["--tenant", TENANT, command])

    assert await execute(args("show"), store, audit, keys, request_id="inv-1") == 0
    assert capsys.readouterr().out.strip() == TOKEN
    assert await execute(args("confirm"), store, audit, keys, request_id="inv-1") == 0
    assert (await store.get(TENANT)).state == "active"
    assert await execute(args("show"), store, audit, keys, request_id="inv-1") == 1
    assert TOKEN not in capsys.readouterr().out
    assert await execute(args("clear"), store, audit, keys, request_id="inv-1") == 0
    assert await store.get(TENANT) is None
    assert await execute(args("clear"), store, audit, keys, request_id="inv-1") == 1

    rows = [(e.kind, e.tool, e.identity, e.requester_user_id) for e in audit.events]
    assert rows == [
        ("webhook_token", "show", "operator:ops", "operator:ops"),
        ("webhook_token", "confirm", "operator:ops", "operator:ops"),
        ("webhook_token", "clear", "operator:ops", "operator:ops"),
    ]
    for event in audit.events:
        assert TOKEN not in event_json(event)
