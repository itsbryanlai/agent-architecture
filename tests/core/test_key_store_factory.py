"""Every process builds its key store through the one factory, so they always agree."""

from __future__ import annotations

import base64
import secrets

import pytest
from fastapi.testclient import TestClient
from temporalio.api.common.v1 import Payload
from temporalio.converter import WorkflowSerializationContext

import agentos.gateway.app as gateway_app
from agentos import crypto_keys, ids, temporal_client, worker
from agentos.config import Settings, TenantsConfig
from agentos.crypto_keys import create_key, key_var_name
from agentos.gateway.app import _real_lifespan, create_app
from agentos.temporal_client import build_temporal_client


class FakePool:
    def __init__(self, *args, **kwargs):
        pass

    async def open(self):
        pass

    async def close(self):
        pass


class FakeRelay:
    def __init__(self, *args, **kwargs):
        pass

    def wake(self):
        pass

    async def run(self):
        pass


class FakeSync:
    def __init__(self, *args, **kwargs):
        pass

    async def run(self, seconds):
        pass


class FakeWorker:
    def __init__(self, *args, **kwargs):
        pass

    async def run(self):
        pass


async def noop(*args, **kwargs):
    return None


async def empty_tenants(*args, **kwargs):
    return TenantsConfig({})


async def fake_client(settings):
    return object()


@pytest.mark.parametrize("key_source", ["env", "files"])
async def test_every_process_builds_its_key_store_through_the_factory(
    key_source, tmp_path, monkeypatch
):
    """Fails if the worker, the gateway or the Temporal client builds a key store of its own instead of the shared factory."""
    if key_source == "env":
        monkeypatch.setenv(
            key_var_name("acme", 1), base64.b64encode(secrets.token_bytes(32)).decode()
        )
        settings = Settings(
            slack_signing_secret="x", database_url="postgresql://unused", key_source="env"
        )
    else:
        create_key(tmp_path / "keys", "acme")
        settings = Settings(
            slack_signing_secret="x",
            database_url="postgresql://unused",
            key_dir=str(tmp_path / "keys"),
        )

    calls: list[tuple[str, Settings]] = []
    real_factory = crypto_keys.build_key_store

    def spy(label):
        def factory(settings):
            calls.append((label, settings))
            return real_factory(settings)

        return factory

    # The Temporal client: the converter it connects with must encrypt under the
    # factory's store, for the key source the settings name.
    captured = {}

    async def fake_connect(address, **kwargs):
        captured["converter"] = kwargs["data_converter"]
        return object()

    monkeypatch.setattr(temporal_client.Client, "connect", fake_connect)
    monkeypatch.setattr(temporal_client, "build_key_store", spy("temporal_client"))
    await build_temporal_client(settings)
    codec = captured["converter"].payload_codec.with_context(
        WorkflowSerializationContext(
            namespace="default", workflow_id=ids.conversation_id("acme", "C1", "1.0")
        )
    )
    (encrypted,) = await codec.encode([Payload(metadata={"encoding": b"json/plain"}, data=b'"x"')])
    assert encrypted.metadata["encryption-key-id"] == b"acme/v1"

    # The gateway, through its real start-up lifespan.
    monkeypatch.setattr("agentos.config.load_settings", lambda: settings)
    monkeypatch.setattr("psycopg_pool.AsyncConnectionPool", FakePool)
    monkeypatch.setattr("agentos.schema_version.require_current_schema", noop)
    monkeypatch.setattr("agentos.tenant_store.startup_tenants", empty_tenants)
    monkeypatch.setattr(gateway_app, "startup_agent_failure", noop)
    monkeypatch.setattr("agentos.gateway.relay.InboundRelay", FakeRelay)
    monkeypatch.setattr(temporal_client, "build_temporal_client", fake_client)
    monkeypatch.setattr(crypto_keys, "build_key_store", spy("gateway"))
    app = create_app(None, None, None, None, None, lifespan=_real_lifespan)
    with TestClient(app):
        assert app.state.deps.webhook_tokens is not None

    # The worker, through its real start-up.
    monkeypatch.setattr(worker, "load_settings", lambda: settings)
    monkeypatch.setattr(worker, "AsyncConnectionPool", FakePool)
    monkeypatch.setattr(worker, "require_current_schema", noop)
    monkeypatch.setattr(worker, "startup_tenants", empty_tenants)
    monkeypatch.setattr(worker, "startup_agent_failure", noop)
    monkeypatch.setattr(worker, "published_tenants", empty_tenants)
    monkeypatch.setattr(worker, "build_llm", lambda settings: object())
    monkeypatch.setattr(worker, "startup_price_failure", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker, "build_temporal_client", fake_client)
    monkeypatch.setattr(worker, "RegistrySync", FakeSync)
    monkeypatch.setattr(worker, "Worker", FakeWorker)
    monkeypatch.setattr(worker, "build_key_store", spy("worker"))
    await worker.main()

    assert [label for label, _ in calls] == ["temporal_client", "gateway", "worker"]
    assert all(s is settings for _, s in calls)
