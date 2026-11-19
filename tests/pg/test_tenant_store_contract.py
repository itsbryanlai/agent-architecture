"""The tenant store, one contract run against the in-memory twin and the Postgres store."""

from __future__ import annotations

import dataclasses

import pytest

from agentos.config import ConfigError, parse_agents_config
from agentos.tenant_store import InMemoryTenantStore, PostgresTenantStore
from tests.conftest import agents_dict


@pytest.fixture
def store(store_pool):
    return InMemoryTenantStore() if store_pool is None else PostgresTenantStore(store_pool)


def tenant_config(tenant: str, **over):
    config = parse_agents_config(agents_dict())
    return dataclasses.replace(
        config,
        tenant_id=tenant,
        slack_team_id=f"T-{tenant}",
        notion_workspace_id=f"w-{tenant}",
        channels={"C1": "it-helper"},
        admins=("U1",),
        secrets=dataclasses.replace(config.secrets, slack_bot_token_env="MY_SLACK"),
        **over,
    )


async def test_a_tenant_is_inserted_once_and_a_shared_slack_workspace_is_refused(
    store, tenant: str, other_tenant: str
):
    """Fails if a tenant is stored twice, or a second tenant may claim the same Slack workspace."""
    mine = tenant_config(tenant)
    assert await store.insert(mine) is True
    assert await store.insert(mine) is False

    clash = dataclasses.replace(mine, tenant_id=other_tenant, notion_workspace_id=None)
    with pytest.raises(ConfigError, match="slack_team_id|already uses"):
        await store.insert(clash)

    assert await store.load(other_tenant) is None


async def test_a_stored_tenant_loads_with_its_settings_and_secret_names_only(
    store, tenant: str, monkeypatch
):
    """Fails if a loaded tenant loses its channels, admins or workspace, or carries a secret's value."""
    monkeypatch.setenv("MY_SLACK", "made-up-value-123")
    await store.insert(tenant_config(tenant))

    row = await store.load(tenant)

    assert (row.config.channels, row.config.admins) == ({"C1": "it-helper"}, ("U1",))
    assert row.config.notion_workspace_id == f"w-{tenant}" and row.enabled is True
    assert row.config.secrets.slack_bot_token_env == "MY_SLACK"
    assert "made-up-value-123" not in repr(row)


async def test_routing_changes_load_back_and_move_the_change_time(store, tenant: str):
    """Fails if a saved default agent or channel map does not load back, or the directory
    cannot notice the change."""
    await store.insert(tenant_config(tenant))
    before = await store.load(tenant)

    await store.set_routing(tenant, None, {"C2": "it-helper"})

    after = await store.load(tenant)
    assert (after.config.default_agent, after.config.channels) == (None, {"C2": "it-helper"})
    assert after.updated_at > before.updated_at


@pytest.mark.parametrize("clash", [False, True], ids=["own workspace", "another tenant's"])
async def test_the_notion_workspace_is_stored_cleared_and_not_shared(
    store, tenant: str, other_tenant: str, clash: bool
):
    """Fails if a connected workspace does not load back with its name, a clear leaves it
    behind, or two tenants may share one Notion workspace."""
    await store.insert(tenant_config(tenant))
    await store.insert(tenant_config(other_tenant))

    if clash:
        with pytest.raises(ConfigError, match="already uses"):
            await store.set_notion_workspace(tenant, f"w-{other_tenant}", "Other HQ")
        assert (await store.load(tenant)).config.notion_workspace_id == f"w-{tenant}"
        return

    await store.set_notion_workspace(tenant, "w-new", "New HQ")
    row = await store.load(tenant)
    assert (row.config.notion_workspace_id, row.config.notion_workspace_name) == (
        "w-new",
        "New HQ",
    )

    await store.set_notion_workspace(tenant, None, None)
    row = await store.load(tenant)
    assert (row.config.notion_workspace_id, row.config.notion_workspace_name) == (None, None)


async def test_disabling_a_tenant_is_seen_by_the_lookups_by_slack_team_and_by_list(
    store, tenant: str
):
    """Fails if a disabled tenant is not reported disabled, or a lookup misses a stored tenant."""
    await store.insert(tenant_config(tenant))
    before = await store.load(tenant)

    await store.set_enabled(tenant, False)

    after = await store.load(tenant)
    assert after.enabled is False and after.updated_at > before.updated_at
    ref = await store.ref_for_slack_team(f"T-{tenant}")
    assert (ref.tenant_id, ref.enabled) == (tenant, False)
    assert await store.ref_for_slack_team("T-unknown") is None
    assert tenant in {r.tenant_id for r in await store.list_refs()}
