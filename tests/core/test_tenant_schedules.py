"""Every served tenant's nightly schedules, ensured by the worker with no operator step."""

import logging
import secrets as token_source

from agentos import ids
from agentos.agent_store import seeded_agent_store
from agentos.config import Settings, agent_definition, parse_agents_config
from agentos.connectors.notion import FakeNotionAPI
from agentos.credentials import InMemoryCredentialStore, TenantCredentials
from agentos.schedules import refresh_schedules_once
from agentos.slack_client import FakeSlack
from agentos.tenant_store import (
    InMemoryTenantStore,
    TenantDirectory,
    import_tenant,
    startup_tenants,
)
from agentos.worker import build_tenant_clients
from tests.conftest import GLOBEX, ROOT_A, ROOT_B, TENANT, knowledge_agents_dict, two_tenant_dicts
from tests.helpers.keys import RefreshableKeyStore
from tests.helpers.knowledge import FakeScheduleClient

ROOT_C = "cccccccc-0000-0000-0000-000000000001"


class Clock:
    """A clock the test moves, standing in for ``time.monotonic``."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def acme():
    raw = two_tenant_dicts()[0]
    raw["agents"] = knowledge_agents_dict()["agents"]  # it-helper reads A, hr-helper reads B
    return parse_agents_config(raw)


def globex():
    raw = two_tenant_dicts()[1]
    raw["agents"]["it-helper"]["grants"]["notion"]["pages"] = [{"id": ROOT_B, "name": "HR wiki"}]
    return parse_agents_config(raw)


async def start_worker(monkeypatch, store, clock):
    """The worker's start-up tenant wiring over fakes, as production runs it."""
    for suffix in ("ACME", "GLOBEX"):
        monkeypatch.setenv(f"SLACK_BOT_TOKEN_{suffix}", f"slack-{suffix}")
        monkeypatch.setenv(f"NOTION_TOKEN_{suffix}", f"notion-{suffix}")
    directory = TenantDirectory(store, ttl_seconds=5, clock=clock)
    keys = RefreshableKeyStore(
        {(TENANT, 1): token_source.token_bytes(32), (GLOBEX, 1): token_source.token_bytes(32)}
    )
    await keys.refresh()
    credentials = TenantCredentials(InMemoryCredentialStore(), keys)
    registry, sync = await build_tenant_clients(
        await startup_tenants(directory),
        directory,
        Settings(),
        credentials,
        keys,
        logging.getLogger("test"),
        slack_factory=lambda token: FakeSlack(),
        notion_factory=lambda token: FakeNotionAPI(),
    )
    return registry, sync


class FailingScheduleClient(FakeScheduleClient):
    """Creates naming one tenant fail, as a Temporal outage would; the flag is the injection point."""

    def __init__(self, fail_tenant=None):
        super().__init__()
        self.fail_tenant = fail_tenant

    async def create_schedule(self, schedule_id, schedule):
        if self.fail_tenant is not None and schedule_id.startswith(f"{self.fail_tenant}:"):
            raise ConnectionError("temporal is down")
        return await super().create_schedule(schedule_id, schedule)


async def test_a_newly_admitted_tenant_gets_every_schedule_kind_once(monkeypatch):
    """Fails if a tenant the sync admits misses a schedule kind, gets a duplicate, or has
    its schedules re-applied on an unchanged pass."""
    store, clock = InMemoryTenantStore(acme()), Clock()
    registry, sync = await start_worker(monkeypatch, store, clock)
    agents = seeded_agent_store(acme(), globex())
    client = FakeScheduleClient()
    ensured: dict = {}
    await refresh_schedules_once(client, registry, agents, ensured)

    await import_tenant(store, globex())
    clock.advance(6)
    assert await sync.sync() == [GLOBEX]
    await refresh_schedules_once(client, registry, agents, ensured)

    assert sorted(e for e in client.log if e[1].startswith(f"{GLOBEX}:")) == sorted(
        [
            ("create", ids.retention_schedule_id(GLOBEX)),
            ("create", ids.creator_access_schedule_id(GLOBEX)),
            ("create", ids.index_schedule_id(GLOBEX, ids.knowledge_scope(ROOT_B))),
        ]
    )
    before = list(client.log)
    await refresh_schedules_once(client, registry, agents, ensured)
    assert client.log == before


async def test_schedule_calls_follow_the_granted_scopes_only(monkeypatch):
    """Fails if a new page grant gets no index schedule on the next pass, a publish that
    changes no grant re-applies schedules, or an unchanged tenant causes a schedule call."""
    store, clock = InMemoryTenantStore(acme()), Clock()
    registry, _sync = await start_worker(monkeypatch, store, clock)
    agents = seeded_agent_store(acme())
    client = FakeScheduleClient()
    ensured: dict = {}
    await refresh_schedules_once(client, registry, agents, ensured)
    before = list(client.log)

    renamed = agent_definition(acme().agents["plain"])
    renamed["name"] = "Renamed helper"
    agents.set_published_unchecked(TENANT, "plain", renamed)
    await refresh_schedules_once(client, registry, agents, ensured)
    assert client.log == before

    granted = agent_definition(acme().agents["plain"])
    granted["grants"]["notion"]["pages"] = [{"id": ROOT_C, "name": "Ops wiki"}]
    agents.set_published_unchecked(TENANT, "plain", granted)
    await refresh_schedules_once(client, registry, agents, ensured)
    delta = client.log[len(before) :]
    assert ("create", ids.index_schedule_id(TENANT, ids.knowledge_scope(ROOT_C))) in delta
    # The pass re-applies index schedules only: retention and creator-access stand.
    assert all(
        op != "delete" and i.startswith(ids.index_schedule_id(TENANT, "")) for op, i in delta
    )

    after = list(client.log)
    await refresh_schedules_once(client, registry, agents, ensured)
    assert client.log == after


async def test_one_tenants_failure_stops_neither_the_other_nor_the_next_pass(monkeypatch, caplog):
    """Fails if a tenant whose schedule apply raises skips another tenant's schedules,
    stops the pass, is not logged with its ID, or is not retried once the failure clears."""
    store, clock = InMemoryTenantStore(acme(), globex()), Clock()
    registry, _sync = await start_worker(monkeypatch, store, clock)
    agents = seeded_agent_store(acme(), globex())
    client = FailingScheduleClient(fail_tenant=TENANT)
    ensured: dict = {}

    with caplog.at_level(logging.ERROR, logger="agentos.schedules"):
        await refresh_schedules_once(client, registry, agents, ensured)

    assert f"tenant_id={TENANT}" in caplog.text
    assert "ConnectionError" in caplog.text
    assert sorted(client.schedules) == sorted(
        [
            ids.retention_schedule_id(GLOBEX),
            ids.creator_access_schedule_id(GLOBEX),
            ids.index_schedule_id(GLOBEX, ids.knowledge_scope(ROOT_B)),
        ]
    )

    client.fail_tenant = None
    await refresh_schedules_once(client, registry, agents, ensured)

    assert sorted(i for i in client.schedules if i.startswith(f"{TENANT}:")) == sorted(
        [
            ids.retention_schedule_id(TENANT),
            ids.creator_access_schedule_id(TENANT),
            ids.index_schedule_id(TENANT, ids.knowledge_scope(ROOT_A)),
            ids.index_schedule_id(TENANT, ids.knowledge_scope(ROOT_B)),
        ]
    )


async def test_start_up_ensures_every_served_tenants_schedules(monkeypatch):
    """Fails if a worker start-up leaves a served tenant without its nightly schedules."""
    store, clock = InMemoryTenantStore(acme(), globex()), Clock()
    registry, _sync = await start_worker(monkeypatch, store, clock)
    agents = seeded_agent_store(acme(), globex())
    client = FakeScheduleClient()

    await refresh_schedules_once(client, registry, agents, {})

    assert sorted(client.schedules) == sorted(
        [
            ids.retention_schedule_id(TENANT),
            ids.creator_access_schedule_id(TENANT),
            ids.index_schedule_id(TENANT, ids.knowledge_scope(ROOT_A)),
            ids.index_schedule_id(TENANT, ids.knowledge_scope(ROOT_B)),
            ids.retention_schedule_id(GLOBEX),
            ids.creator_access_schedule_id(GLOBEX),
            ids.index_schedule_id(GLOBEX, ids.knowledge_scope(ROOT_B)),
        ]
    )
    assert all(op == "create" for op, _ in client.log)


async def test_a_reinstalled_tenant_is_re_ensured(monkeypatch):
    """Fails if a disabled tenant's schedules are touched, or a reinstall skips the
    re-ensure because the tenant's scopes did not change while it was away."""
    store, clock = InMemoryTenantStore(acme(), globex()), Clock()
    registry, sync = await start_worker(monkeypatch, store, clock)
    agents = seeded_agent_store(acme(), globex())
    client = FakeScheduleClient()
    ensured: dict = {}
    await refresh_schedules_once(client, registry, agents, ensured)
    before = list(client.log)

    await store.set_enabled(GLOBEX, False)
    clock.advance(6)
    await sync.sync()
    await refresh_schedules_once(client, registry, agents, ensured)
    assert client.log == before

    await store.set_enabled(GLOBEX, True)
    clock.advance(6)
    assert await sync.sync() == [GLOBEX]
    await refresh_schedules_once(client, registry, agents, ensured)

    assert sorted(e for e in client.log[len(before) :] if e[1].startswith(f"{GLOBEX}:")) == sorted(
        [
            ("update", ids.retention_schedule_id(GLOBEX)),
            ("update", ids.creator_access_schedule_id(GLOBEX)),
            ("update", ids.index_schedule_id(GLOBEX, ids.knowledge_scope(ROOT_B))),
        ]
    )
