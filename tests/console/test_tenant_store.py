"""Tenants in the database: the import, the narrow cross-tenant reads, and serving
a tenant added, changed or disabled without a restart (gateway, worker, console)."""

import logging
import secrets as token_source
from dataclasses import replace

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient
from slack_sdk.signature import SignatureVerifier
from temporalio.exceptions import ApplicationError

from agentos.activities import AgentActivities
from agentos.agent_store import InMemoryAgentStore
from agentos.audit import InMemoryAuditStore
from agentos.config import (
    ConfigError,
    Settings,
    TenantsConfig,
    load_tenants_config,
    parse_agents_config,
)
from agentos.connectors.notion import FakeNotionAPI
from agentos.control import ControlPlane
from agentos.control_api import create_app as create_control_app
from agentos.credentials import SLACK_BOT, InMemoryCredentialStore, TenantCredentials
from agentos.gateway.app import create_app
from agentos.gateway.relay import InboundRelay
from agentos.grant_checker import NotionGrantChecker
from agentos.members import InMemoryMemberStore
from agentos.models import TurnContext
from agentos.payload_codec import encrypt_with_tenant_key
from agentos.slack_client import FakeSlack
from agentos.slack_signin import SlackSession
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenant_store import (
    UNKNOWN_TENANT_TTL_SECONDS,
    InMemoryTenantStore,
    StaticTenants,
    TenantDirectory,
    import_tenant,
    startup_tenants,
)
from agentos.tenants import RegistrySync, TenantRegistry, UnknownTenant, build_registry, runtime_for
from tests.conftest import (
    ALICE,
    GLOBEX,
    GLOBEX_TEAM,
    TEAM,
    TENANT,
    two_tenant_dicts,
)
from tests.helpers.console_harness import ROOT, ConsoleHarness, using
from tests.helpers.control_api import BASE, JSON, StandIn
from tests.helpers.gateway import FakeTemporalClient, im_event, post_event
from tests.helpers.keys import CountingKeyStore


@pytest.fixture
def secret() -> str:
    return "made-up-signing-secret"


class Clock:
    """A clock the test moves, standing in for ``time.monotonic``."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def acme():
    return parse_agents_config(two_tenant_dicts()[0])


def globex():
    return parse_agents_config(two_tenant_dicts()[1])


def write_tenants(directory, *raws: dict):
    for raw in raws:
        (directory / f"{raw['tenant_id']}.yaml").write_text(yaml.safe_dump(raw))


# --- The one-time import ------------------------------------------------------


async def test_import_creates_a_row_for_each_yaml_tenant(tmp_path):
    write_tenants(tmp_path, *two_tenant_dicts())
    yaml_tenants = load_tenants_config(tmp_path)
    store = InMemoryTenantStore()

    results = [await import_tenant(store, c) for c in yaml_tenants.tenants.values()]

    assert results == [True, True]
    row = await store.load(GLOBEX)
    assert row.enabled is True
    assert row.config.slack_team_id == GLOBEX_TEAM
    assert row.config.default_agent == "it-helper"
    assert row.config.secrets.notion_token_env == "NOTION_TOKEN_GLOBEX"
    assert row.config.agents == {}


async def test_import_twice_changes_nothing_and_keeps_settings_changed_since():
    store = InMemoryTenantStore()
    assert await import_tenant(store, acme()) is True
    changed = replace(acme(), channels={}, admins=("U_NEW_ADMIN",))
    store.replace_config(changed)

    assert await import_tenant(store, acme()) is False

    kept = (await store.load(TENANT)).config
    assert kept.admins == ("U_NEW_ADMIN",)


async def test_import_refuses_a_tenant_that_shares_a_workspace_with_a_stored_one():
    store = InMemoryTenantStore()
    await import_tenant(store, acme())
    clash = replace(globex(), slack_team_id=TEAM)

    with pytest.raises(ConfigError, match="slack_team_id"):
        await import_tenant(store, clash)

    assert [r.tenant_id for r in await store.list_refs()] == [TENANT]


async def test_import_refuses_a_tenant_that_shares_a_secret_variable_with_a_stored_one():
    store = InMemoryTenantStore()
    await import_tenant(store, acme())
    raw = two_tenant_dicts()[1]
    raw["secrets"]["notion_token_env"] = "NOTION_TOKEN_ACME"

    with pytest.raises(ConfigError, match="NOTION_TOKEN_ACME"):
        await import_tenant(store, parse_agents_config(raw))


# --- The live directory -------------------------------------------------------


class CountingStore(InMemoryTenantStore):
    def __init__(self, *configs):
        super().__init__(*configs)
        self.lists = 0
        self.loads = 0
        self.fail = False

    async def list_refs(self):
        self.lists += 1
        if self.fail:
            raise ConnectionError("database is down")
        return await super().list_refs()

    async def load(self, tenant_id):
        self.loads += 1
        return await super().load(tenant_id)


async def test_the_directory_serves_from_a_cache_and_rereads_only_changed_tenants():
    store, clock = CountingStore(acme(), globex()), Clock()
    directory = TenantDirectory(store, ttl_seconds=15, clock=clock)

    await directory.snapshot()
    await directory.snapshot()
    assert (store.lists, store.loads) == (1, 2)

    clock.advance(16)
    store.replace_config(replace(acme(), admins=("U_NEW",)))
    snapshot = await directory.snapshot()

    assert (store.lists, store.loads) == (2, 3)
    assert snapshot.tenants[TENANT].admins == ("U_NEW",)


async def test_a_tenant_added_after_start_is_found_by_its_slack_team_at_once():
    store, clock = InMemoryTenantStore(acme()), Clock()
    directory = TenantDirectory(store, clock=clock)
    assert await directory.for_slack_team(GLOBEX_TEAM) is None
    clock.advance(2)

    await import_tenant(store, globex())

    assert await directory.for_slack_team(GLOBEX_TEAM) == GLOBEX
    assert (await directory.get(GLOBEX)).slack_team_id == GLOBEX_TEAM


async def test_a_disabled_tenant_is_unknown_to_every_lookup():
    store, clock = InMemoryTenantStore(acme(), globex()), Clock()
    directory = TenantDirectory(store, ttl_seconds=15, clock=clock)
    assert await directory.for_slack_team(GLOBEX_TEAM) == GLOBEX

    await store.set_enabled(GLOBEX, False)
    clock.advance(16)

    assert await directory.for_slack_team(GLOBEX_TEAM) is None
    assert await directory.get(GLOBEX) is None
    assert await directory.only_tenant() == TENANT
    # A tenant that was disabled before the directory ever saw it is refused too.
    fresh = TenantDirectory(store, clock=Clock())
    assert await fresh.for_slack_team(GLOBEX_TEAM) is None


async def test_a_failed_refresh_keeps_serving_the_last_good_set(caplog):
    store, clock = CountingStore(acme()), Clock()
    directory = TenantDirectory(store, ttl_seconds=15, clock=clock)
    await directory.snapshot()
    store.fail = True
    clock.advance(16)

    with caplog.at_level(logging.ERROR, logger="agentos.tenants"):
        snapshot = await directory.snapshot()

    assert list(snapshot.tenants) == [TENANT]
    assert "database is down" in caplog.text
    with pytest.raises(ConnectionError):
        await TenantDirectory(store, clock=Clock()).refresh()


async def test_two_stored_tenants_sharing_a_secret_variable_are_refused_at_start():
    raw = two_tenant_dicts()[1]
    raw["secrets"]["notion_token_env"] = "NOTION_TOKEN_ACME"
    store = InMemoryTenantStore(acme(), parse_agents_config(raw))

    with pytest.raises(SystemExit, match="NOTION_TOKEN_ACME"):
        await startup_tenants(TenantDirectory(store))


@pytest.mark.parametrize(
    "case", ["an empty table", "every tenant disabled"], ids=["empty", "all-disabled"]
)
async def test_start_up_with_no_enabled_tenant_serves_none(case, secret, caplog):
    """Fails if a process refuses to start with no enabled tenant, instead of serving none
    until the next refresh picks one up."""
    store = InMemoryTenantStore() if case == "an empty table" else InMemoryTenantStore(acme())
    if case == "every tenant disabled":
        await store.set_enabled(TENANT, False)
    directory = TenantDirectory(store)

    with caplog.at_level(logging.INFO, logger="agentos.tenants"):
        tenants = await startup_tenants(directory)

    assert tenants.tenants == {}
    assert caplog.text.count("no enabled tenants") == 1
    registry = await build_registry(
        tenants, Settings(), lambda t: FakeSlack(), lambda t: FakeNotionAPI()
    )
    assert registry.tenants() == []
    client, temporal = gateway(directory, secret)
    assert post_event(client, secret, event_for(TEAM, "Ev1")).status_code == 200
    assert await delivered(client) == 0
    assert temporal.signal_with_starts == []


async def test_start_up_returns_the_enabled_tenants():
    store = InMemoryTenantStore(acme(), globex())
    await store.set_enabled(GLOBEX, False)

    tenants = await startup_tenants(TenantDirectory(store))

    assert list(tenants.tenants) == [TENANT]


async def test_a_fixed_set_of_tenants_answers_like_the_directory():
    source = StaticTenants(TenantsConfig.of(acme(), globex()))

    assert await source.for_slack_team(TEAM) == TENANT
    assert await source.for_slack_team("T_NOBODY") is None
    assert (await source.get(GLOBEX)).tenant_id == GLOBEX
    assert await source.only_tenant() is None


# --- Gateway ------------------------------------------------------------------


def gateway(directory, secret):
    temporal = FakeTemporalClient()
    app = create_app(
        Settings(), directory, temporal, InMemoryTaskLinkStore(), SignatureVerifier(secret)
    )
    return TestClient(app), temporal


async def delivered(client) -> int:
    """Let the gateway's relay hand what it stored to Temporal."""
    d = client.app.state.deps
    return await InboundRelay(d.content, d.tenants, d.temporal_client).deliver_once()


def event_for(team: str, event_id: str) -> dict:
    payload = im_event()
    payload["team_id"] = team
    payload["event_id"] = event_id
    return payload


async def test_the_gateway_serves_a_tenant_added_after_start_without_a_restart(secret):
    store, clock = InMemoryTenantStore(acme()), Clock()
    client, temporal = gateway(TenantDirectory(store, clock=clock), secret)
    post_event(client, secret, event_for(GLOBEX_TEAM, "Ev1"))
    await delivered(client)
    assert temporal.signal_with_starts == []

    clock.advance(2)
    await import_tenant(store, globex())
    post_event(client, secret, event_for(GLOBEX_TEAM, "Ev2"))
    await delivered(client)

    assert [c["arg"].tenant_id for c in temporal.signal_with_starts] == [GLOBEX]
    assert temporal.signal_with_starts[0]["id"].startswith(f"{GLOBEX}:")


async def test_the_gateway_answers_nothing_for_a_tenant_disabled_while_running(secret):
    store, clock = InMemoryTenantStore(acme(), globex()), Clock()
    client, temporal = gateway(TenantDirectory(store, ttl_seconds=15, clock=clock), secret)
    post_event(client, secret, event_for(GLOBEX_TEAM, "Ev1"))
    await delivered(client)
    assert len(temporal.signal_with_starts) == 1

    await store.set_enabled(GLOBEX, False)
    clock.advance(16)
    response = post_event(client, secret, event_for(GLOBEX_TEAM, "Ev2"))

    assert response.status_code == 200
    await delivered(client)
    assert len(temporal.signal_with_starts) == 1
    assert post_event(client, secret, event_for(TEAM, "Ev3")).status_code == 200
    await delivered(client)
    assert len(temporal.signal_with_starts) == 2


async def test_the_notion_webhook_of_a_disabled_tenant_is_not_found(secret):
    store, clock = InMemoryTenantStore(acme(), globex()), Clock()
    client, _ = gateway(TenantDirectory(store, ttl_seconds=15, clock=clock), secret)
    await store.set_enabled(GLOBEX, False)
    clock.advance(16)

    assert client.post(f"/notion/webhook/{GLOBEX}", content=b"{}").status_code == 404


# --- Worker -------------------------------------------------------------------


def fake_factories():
    built = {"slack": [], "notion": []}

    def slack(token: str):
        built["slack"].append(token)
        return FakeSlack()

    def notion(token: str):
        built["notion"].append(token)
        return FakeNotionAPI()

    return built, slack, notion


def set_secrets(monkeypatch, *suffixes: str):
    for suffix in suffixes:
        monkeypatch.setenv(f"SLACK_BOT_TOKEN_{suffix}", f"slack-{suffix}")
        monkeypatch.setenv(f"NOTION_TOKEN_{suffix}", f"notion-{suffix}")


async def start_worker_registry(monkeypatch, store, clock):
    set_secrets(monkeypatch, "ACME")
    directory = TenantDirectory(store, ttl_seconds=5, clock=clock)
    built, slack, notion = fake_factories()
    registry = await build_registry(await startup_tenants(directory), Settings(), slack, notion)
    return registry, RegistrySync(directory, registry, slack, notion), built


async def test_the_worker_serves_a_tenant_added_later_with_its_own_clients(monkeypatch):
    store, clock = InMemoryTenantStore(acme()), Clock()
    registry, sync, built = await start_worker_registry(monkeypatch, store, clock)
    with pytest.raises(UnknownTenant):
        registry.get(GLOBEX)

    set_secrets(monkeypatch, "GLOBEX")
    await import_tenant(store, globex())
    clock.advance(6)

    assert await sync.sync() == [GLOBEX]
    assert registry.tenants() == [TENANT, GLOBEX]
    assert built["slack"] == ["slack-ACME", "slack-GLOBEX"]
    assert registry.get(GLOBEX).config.slack_team_id == GLOBEX_TEAM
    assert await sync.sync() == []


async def test_the_worker_drops_a_tenant_that_is_disabled(monkeypatch):
    store, clock = InMemoryTenantStore(acme(), globex()), Clock()
    set_secrets(monkeypatch, "GLOBEX")
    registry, sync, _ = await start_worker_registry(monkeypatch, store, clock)
    assert registry.tenants() == [TENANT, GLOBEX]

    await store.set_enabled(GLOBEX, False)
    clock.advance(6)
    await sync.sync()

    with pytest.raises(UnknownTenant):
        registry.get(GLOBEX)
    assert registry.tenants() == [TENANT]


async def test_a_sync_that_drops_a_tenant_forgets_its_cached_token():
    """Fails if a tenant the sync drops keeps its decrypted token in the resolver cache."""
    store, clock = InMemoryTenantStore(globex()), Clock()
    directory = TenantDirectory(store, ttl_seconds=5, clock=clock)
    key = token_source.token_bytes(32)
    keys = CountingKeyStore({(GLOBEX, 1): key})
    await keys.refresh()
    credential_store = InMemoryCredentialStore()
    await credential_store.put(
        GLOBEX, SLACK_BOT, encrypt_with_tenant_key(GLOBEX, key, b"xoxb-made-up"), 1
    )
    credentials = TenantCredentials(credential_store, keys)
    config = globex()
    token = await credentials.slack_bot_token(config)
    registry = TenantRegistry({GLOBEX: runtime_for(config, FakeSlack(), slack_token=token)})
    _built, slack, notion = fake_factories()
    sync = RegistrySync(
        directory,
        registry,
        slack,
        notion,
        slack_token=credentials.slack_bot_token,
        on_drop=credentials.forget,
    )
    clock.advance(6)
    await sync.sync()
    assert keys.reads == 1  # the sync's own resolve was served from the cache

    await store.set_enabled(GLOBEX, False)
    clock.advance(6)
    await sync.sync()

    assert registry.tenants() == []
    assert await credentials.slack_bot_token(config) == token
    assert keys.reads == 2  # forgotten: the still-stored row was decrypted again


async def test_changed_settings_reach_the_worker_without_new_clients(monkeypatch):
    store, clock = InMemoryTenantStore(acme()), Clock()
    registry, sync, built = await start_worker_registry(monkeypatch, store, clock)

    store.replace_config(replace(acme(), channels={"C9": "it-helper"}))
    clock.advance(6)
    await sync.sync()

    assert registry.get(TENANT).config.channels == {"C9": "it-helper"}
    assert built["slack"] == ["slack-ACME"]


async def test_a_new_tenant_with_an_unset_variable_waits_and_is_reported_once(monkeypatch, caplog):
    store, clock = InMemoryTenantStore(acme()), Clock()
    registry, sync, _ = await start_worker_registry(monkeypatch, store, clock)
    monkeypatch.delenv("SLACK_BOT_TOKEN_GLOBEX", raising=False)
    monkeypatch.delenv("NOTION_TOKEN_GLOBEX", raising=False)
    await import_tenant(store, globex())

    with caplog.at_level(logging.ERROR, logger="agentos.tenants"):
        for _ in range(3):
            clock.advance(6)
            assert await sync.sync() == []

    assert caplog.text.count("SLACK_BOT_TOKEN_GLOBEX") == 1
    assert "tenant_id=globex" in caplog.text
    assert registry.tenants() == [TENANT]

    set_secrets(monkeypatch, "GLOBEX")
    clock.advance(6)
    assert await sync.sync() == [GLOBEX]


async def test_a_new_tenant_the_start_up_check_refuses_is_not_served(monkeypatch):
    store, clock = InMemoryTenantStore(acme()), Clock()
    registry, _, _ = await start_worker_registry(monkeypatch, store, clock)
    directory = TenantDirectory(store, ttl_seconds=5, clock=clock)
    set_secrets(monkeypatch, "GLOBEX")
    built, slack, notion = fake_factories()
    seen = []

    async def admit(config):
        seen.append(config.tenant_id)
        return "no usable encryption key"

    sync = RegistrySync(directory, registry, slack, notion, admit=admit)
    await import_tenant(store, globex())
    clock.advance(6)

    assert await sync.sync() == []
    assert seen == [GLOBEX]
    assert registry.tenants() == [TENANT]
    assert built["slack"] == []


async def test_an_activity_for_a_tenant_that_appears_on_the_next_refresh_succeeds(monkeypatch):
    """Fails if work for a tenant installed seconds ago fails for good, instead of waiting
    for the directory refresh that admits it and then running."""
    set_secrets(monkeypatch, "ACME")
    store, clock = InMemoryTenantStore(acme()), Clock()
    directory = TenantDirectory(store, ttl_seconds=5, clock=clock)
    _, slack, notion = fake_factories()
    registry = await build_registry(await startup_tenants(directory), Settings(), slack, notion)
    sync = RegistrySync(directory, registry, slack, notion)
    acts = AgentActivities(
        settings=Settings(),
        registry=registry,
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=None,
        tool_outputs=InMemoryToolOutputStore(),
        audit=InMemoryAuditStore(),
        agents=InMemoryAgentStore(),
    )
    ctx = TurnContext(
        tenant_id=GLOBEX,
        agent_id="it-helper",
        agent_version=1,
        requester_user_id=ALICE,
        channel="C1",
        thread_ts="1.1",
        channel_kind="channel",
    )

    with pytest.raises(ApplicationError) as caught:
        await acts.post_placeholder(ctx, "key")
    assert caught.value.non_retryable is False

    set_secrets(monkeypatch, "GLOBEX")
    await import_tenant(store, globex())
    clock.advance(6)
    assert await sync.sync() == [GLOBEX]

    assert await acts.post_placeholder(ctx, "key") == "1700000000.000001"


# --- Console and control API ---------------------------------------------------


async def test_the_console_signs_in_a_tenant_added_after_start_and_refuses_it_once_disabled():
    h = ConsoleHarness(single_tenant=True)
    store, clock = InMemoryTenantStore(h.tenants.tenants[TENANT]), Clock()
    directory = TenantDirectory(store, ttl_seconds=15, clock=clock)
    await directory.refresh()
    h.plane = ControlPlane(
        h.agents,
        h.members,
        h.audit,
        directory,
        NotionGrantChecker(lambda tenant_id: h.notion),
        usage=h.usage,
        budgets=h.budgets,
        clock=h.clock,
        pauses=h.pauses,
    )
    h.app = create_control_app(
        h.plane, directory, SlackSession(), sessions=h.sessions, console=h.console
    )

    async with using(h.client()) as client:
        refused = await h.sign_in_as(client, ROOT, GLOBEX_TEAM)
        assert refused.status_code == 403

    clock.advance(2)
    raw = two_tenant_dicts()[1]
    raw["admins"] = [ROOT]
    await import_tenant(store, parse_agents_config(raw))

    client = await h.signed_in(ROOT, GLOBEX_TEAM)
    async with using(client):
        page = await client.get(f"/ui/{GLOBEX}/agents")
        assert page.status_code == 200

        await store.set_enabled(GLOBEX, False)
        clock.advance(16)

        assert (await client.get(f"/ui/{GLOBEX}/agents")).status_code == 403
        assert (await h.sign_in_as(h.client(), ROOT, GLOBEX_TEAM)).status_code == 403
    still = await h.signed_in(ROOT, TEAM)
    async with using(still):
        assert (await still.get(f"/ui/{TENANT}/agents")).status_code == 200


async def test_the_control_api_treats_a_disabled_tenant_as_unknown():
    store, clock = InMemoryTenantStore(acme(), globex()), Clock()
    directory = TenantDirectory(store, ttl_seconds=15, clock=clock)
    plane = ControlPlane(
        InMemoryAgentStore(),
        InMemoryMemberStore(),
        InMemoryAuditStore(),
        directory,
        NotionGrantChecker(lambda tenant_id: FakeNotionAPI()),
    )
    app = create_control_app(plane, directory, StandIn(ROOT))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url=BASE, headers=JSON) as client:
        assert (await client.get(f"/tenants/{GLOBEX}/agents")).status_code != 404

        await store.set_enabled(GLOBEX, False)
        clock.advance(16)

        assert (await client.get(f"/tenants/{GLOBEX}/agents")).status_code == 404
        assert (await client.get(f"/tenants/{TENANT}/agents")).status_code != 404


async def test_a_tenant_id_found_unknown_is_not_looked_up_again_for_a_short_while():
    """Fails if every request for an unknown tenant ID reads the database, or if the answer is
    remembered past its time-to-live or for a tenant that does exist."""
    store, clock = CountingStore(acme()), Clock()
    directory = TenantDirectory(store, ttl_seconds=1000, clock=clock)
    await directory.snapshot()
    before = store.lists

    assert await directory.get("no-such-tenant") is None
    assert store.lists == before + 1
    for _ in range(5):
        assert await directory.get("no-such-tenant") is None
    assert store.lists == before + 1

    assert (await directory.get(TENANT)).tenant_id == TENANT
    assert store.lists == before + 1

    clock.advance(UNKNOWN_TENANT_TTL_SECONDS + 1)
    assert await directory.get("no-such-tenant") is None
    assert store.lists == before + 2
