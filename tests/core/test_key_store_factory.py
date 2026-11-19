"""Every process builds its key store through the one factory, so they always agree."""

from __future__ import annotations

import base64
import os
import secrets
from types import SimpleNamespace

import pytest
from temporalio.api.common.v1 import Payload
from temporalio.converter import WorkflowSerializationContext

import agentos.gateway.app as gateway_app
from agentos import ids, temporal_client, worker
from agentos.config import Settings
from agentos.crypto_keys import key_var_name
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


async def acme_tenant(*args, **kwargs):
    # Only the tenant IDs matter here: the key check runs before anything reads a config.
    return SimpleNamespace(tenants={"acme": None})


async def fake_client(settings, keys=None, pool=None):
    return object()


ENV_SETTINGS = Settings(
    slack_signing_secret="x", database_url="postgresql://unused", key_source="env"
)
DB_SETTINGS = Settings(
    slack_signing_secret="x", database_url="postgresql://unused", key_source="db"
)


async def test_the_temporal_client_encrypts_with_the_key_variable(monkeypatch):
    """Fails if the Temporal client's converter ignores AGENTOS_KEY_SOURCE=env and looks for a key file instead of the key variable."""
    monkeypatch.setenv(key_var_name("acme", 1), base64.b64encode(secrets.token_bytes(32)).decode())
    captured = {}

    async def fake_connect(address, **kwargs):
        captured["converter"] = kwargs["data_converter"]
        return object()

    monkeypatch.setattr(temporal_client.Client, "connect", fake_connect)
    await build_temporal_client(ENV_SETTINGS)
    codec = captured["converter"].payload_codec.with_context(
        WorkflowSerializationContext(
            namespace="default", workflow_id=ids.conversation_id("acme", "C1", "1.0")
        )
    )
    (encrypted,) = await codec.encode([Payload(metadata={"encoding": b"json/plain"}, data=b'"x"')])
    assert encrypted.metadata["encryption-key-id"] == b"acme/v1"


async def start_gateway(monkeypatch, settings):
    monkeypatch.setattr("agentos.config.load_settings", lambda: settings)
    monkeypatch.setattr("psycopg_pool.AsyncConnectionPool", FakePool)
    monkeypatch.setattr("agentos.schema_version.require_current_schema", noop)
    monkeypatch.setattr("agentos.tenant_store.startup_tenants", acme_tenant)
    monkeypatch.setattr(gateway_app, "startup_agent_failure", noop)
    monkeypatch.setattr("agentos.gateway.relay.InboundRelay", FakeRelay)
    monkeypatch.setattr(temporal_client, "build_temporal_client", fake_client)
    app = create_app(None, None, None, None, None, lifespan=_real_lifespan)
    async with _real_lifespan(app):
        pass


async def start_worker(monkeypatch, settings):
    monkeypatch.setattr(worker, "load_settings", lambda: settings)
    monkeypatch.setattr(worker, "AsyncConnectionPool", FakePool)
    monkeypatch.setattr(worker, "require_current_schema", noop)
    monkeypatch.setattr(worker, "startup_tenants", acme_tenant)
    monkeypatch.setattr(worker, "build_llm", lambda settings: object())
    monkeypatch.setattr(worker, "startup_price_failure", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker, "build_temporal_client", fake_client)
    await worker.main()


@pytest.mark.parametrize("start", [start_gateway, start_worker], ids=["gateway", "worker"])
@pytest.mark.parametrize(
    ("settings", "named"),
    [(ENV_SETTINGS, key_var_name("acme", 1)), (DB_SETTINGS, "AGENTOS_MASTER_KEY_V1")],
    ids=["env", "db without a master"],
)
async def test_start_up_reads_the_key_source_the_settings_name(start, settings, named, monkeypatch):
    """Fails if the gateway or the worker checks tenant keys in a key directory when the settings name key variables, or starts in db mode with no master key variable."""
    for name in list(os.environ):
        if name.startswith("AGENTOS_MASTER_KEY_V"):
            monkeypatch.delenv(name)
    with pytest.raises(SystemExit) as refused:
        await start(monkeypatch, settings)
    assert named in str(refused.value)
