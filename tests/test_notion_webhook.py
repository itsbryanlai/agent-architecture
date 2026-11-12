"""The Notion webhook accepts only signed events and never logs the token."""

import hashlib
import hmac
import json
import logging
import os
import stat

import pytest
from fastapi.testclient import TestClient
from slack_sdk.signature import SignatureVerifier

from agentos.config import Settings, TenantsConfig
from agentos.gateway.app import create_app
from agentos.gateway.notion_signature import verify_notion_signature
from agentos.stores import InMemoryTaskLinkStore
from tests.conftest import TENANT
from tests.test_gateway import FakeTemporalClient

TOKEN = "secret_verification_token_abc123"
PAGE_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
TASK_ID = f"{TENANT}:task:t1"


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
        create_app(settings, TenantsConfig.of(agents), temporal, links, SignatureVerifier("x"))
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


def test_handshake_overwrites_an_existing_file(agents, temporal, links, tmp_path, monkeypatch):
    client = make_client(agents, temporal, links, tmp_path, monkeypatch, token=None)
    post(client, {"verification_token": "old"}, None)
    post(client, {"verification_token": "new"}, None)
    assert (saved_path(tmp_path)).read_text() == "new"


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


def test_event_with_bad_signature_rejected(agents, temporal, links, tmp_path, monkeypatch, caplog):
    client = make_client(agents, temporal, links, tmp_path, monkeypatch)
    with caplog.at_level(logging.DEBUG):
        resp = post(client, page_event(), sign(b"other body"))
    assert resp.status_code == 401
    assert temporal.signals == []
    assert TOKEN not in caplog.text


def test_event_without_signature_rejected(agents, temporal, links, tmp_path, monkeypatch):
    client = make_client(agents, temporal, links, tmp_path, monkeypatch)
    assert post(client, page_event(), None).status_code == 401
    assert temporal.signals == []


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
