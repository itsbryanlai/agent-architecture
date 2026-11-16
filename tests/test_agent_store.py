"""Agents in the database: lifecycle, what a turn loads, front-door rules, import, startup."""

import copy

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from agentos.activities import AgentActivities
from agentos.agent_store import (
    InMemoryAgentStore,
    published_tenants,
    seeded_agent_store,
    startup_agent_failure,
)
from agentos.audit import InMemoryAuditStore
from agentos.config import (
    ConfigError,
    Settings,
    TenantsConfig,
    agent_definition,
    parse_agent,
    parse_agents_config,
)
from agentos.models import TaskSpec, TurnContext
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for
from tests.conftest import ALICE, DB_ID, TENANT, agents_dict, make_fake_notion
from tests.helpers.front_door import front_door_dict

ADMIN = "U_ADMIN"
EDITOR = "U_EDITOR"
NONCE = "n-1"


def helper_block() -> dict:
    block = copy.deepcopy(agents_dict()["agents"]["it-helper"])
    del block["version"]
    return block


async def draft_helper(store: InMemoryAgentStore, tenant: str = TENANT):
    return await store.create_agent(tenant, "it-helper", helper_block(), EDITOR)


async def make_published(store, agent_id="it-helper", definition=None, tenant=TENANT):
    record = await store.create_agent(tenant, agent_id, definition or helper_block(), EDITOR)
    await store.submit(tenant, agent_id, record.version, EDITOR)
    return await store.publish(tenant, agent_id, record.version, ADMIN)


async def republish(store, agent_id, change, tenant=TENANT):
    """Draft from the published version, apply ``change`` to its definition, publish."""
    draft = await store.create_draft(tenant, agent_id, EDITOR)
    definition = copy.deepcopy(draft.definition)
    change(definition)
    await store.replace_draft(tenant, agent_id, draft.version, definition)
    await store.submit(tenant, agent_id, draft.version, EDITOR)
    return await store.publish(tenant, agent_id, draft.version, ADMIN)


# --- lifecycle -------------------------------------------------------------------


async def test_a_definition_is_validated_like_a_yaml_block():
    store = InMemoryAgentStore()
    broken = helper_block()
    del broken["system_prompt"]
    with pytest.raises(ConfigError, match="system_prompt"):
        await store.create_agent(TENANT, "it-helper", broken, EDITOR)
    await draft_helper(store)
    with pytest.raises(ConfigError, match="system_prompt"):
        await store.replace_draft(TENANT, "it-helper", 1, broken)
    assert await store.versions(TENANT, "ghost") == []


def test_definition_round_trips_through_the_yaml_parser():
    config = parse_agents_config(front_door_dict())
    for agent in config.agents.values():
        again = parse_agent(agent.agent_id, agent_definition(agent), agent.version)
        assert again == agent


# --- what a turn loads -----------------------------------------------------------


def make_acts(store, config):
    return AgentActivities(
        settings=Settings(),
        registry=TenantRegistry(
            {config.tenant_id: runtime_for(config, FakeSlack(), make_fake_notion())}
        ),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=None,
        tool_outputs=InMemoryToolOutputStore(),
        audit=InMemoryAuditStore(),
        agents=store,
    )


async def test_load_agent_returns_the_newest_published_version_only():
    config = parse_agents_config(agents_dict())
    store = seeded_agent_store(config)
    acts = make_acts(store, config)
    env = ActivityEnvironment()
    assert (await env.run(acts.load_agent, TENANT, "it-helper")).system_prompt == (
        "You help employees with IT requests."
    )

    draft = await store.create_draft(TENANT, "it-helper", EDITOR)
    edited = copy.deepcopy(draft.definition)
    edited["system_prompt"] = "New prompt."
    await store.replace_draft(TENANT, "it-helper", draft.version, edited)
    assert (await env.run(acts.load_agent, TENANT, "it-helper")).system_prompt == (
        "You help employees with IT requests."
    )
    await store.submit(TENANT, "it-helper", draft.version, EDITOR)
    assert (await env.run(acts.load_agent, TENANT, "it-helper")).version == 1

    await store.publish(TENANT, "it-helper", draft.version, ADMIN)
    loaded = await env.run(acts.load_agent, TENANT, "it-helper")
    assert (loaded.version, loaded.system_prompt) == (2, "New prompt.")


async def test_agent_without_a_published_version_is_unknown_and_not_retried():
    config = parse_agents_config(agents_dict())
    store = seeded_agent_store(config)
    await add_draft_only(store)
    acts = make_acts(store, config)
    env = ActivityEnvironment()

    with pytest.raises(ApplicationError, match="unknown agent 'drafty'") as err:
        await env.run(acts.load_agent, TENANT, "drafty")
    assert err.value.non_retryable
    with pytest.raises(ApplicationError, match="unknown agent 'nobody'") as err:
        await env.run(acts.load_agent, TENANT, "nobody")
    assert err.value.non_retryable


async def add_draft_only(store):
    await store.create_agent(TENANT, "drafty", helper_block(), EDITOR)


def create_spec(title: str) -> TaskSpec:
    ctx = TurnContext(TENANT, "it-helper", 1, ALICE, "C1", "1700000000.000100", "channel")
    payload = {"title": title, "details": "d", "database_id": DB_ID}
    return TaskSpec(ctx, "create_item", payload, NONCE)


async def test_narrowed_grant_applies_to_the_next_call_without_a_restart():
    config = parse_agents_config(agents_dict())
    store = seeded_agent_store(config)
    notion = make_fake_notion()
    slack = FakeSlack(emails={ALICE: "alice@example.com"})
    acts = AgentActivities(
        settings=Settings(),
        registry=TenantRegistry({TENANT: runtime_for(config, slack, notion)}),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=None,
        tool_outputs=InMemoryToolOutputStore(),
        audit=(audit := InMemoryAuditStore()),
        agents=store,
    )
    env = ActivityEnvironment()

    first = await env.run(acts.create_notion_item, create_spec("Monitor"), "task-1")
    assert first.title == "Monitor"

    # The write below was approved before the grant is removed.
    approved = create_spec("Approved earlier")
    await republish(store, "it-helper", lambda d: d["grants"]["notion"].update(databases=[]))

    with pytest.raises(ApplicationError) as err:
        await env.run(acts.create_notion_item, approved, "task-2")
    assert err.value.non_retryable
    assert "not been granted" in str(err.value)
    assert [p["id"] for p in notion.all_pages(DB_ID)] == [first.page_id]
    assert [(e.kind, e.outcome) for e in audit.events][-1] == ("write", "error")


async def test_restored_grant_applies_again_on_the_next_call():
    config = parse_agents_config(agents_dict())
    store = seeded_agent_store(config)
    notion = make_fake_notion()
    acts = AgentActivities(
        settings=Settings(),
        registry=TenantRegistry(
            {TENANT: runtime_for(config, FakeSlack(emails={ALICE: "a@example.com"}), notion)}
        ),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=None,
        tool_outputs=InMemoryToolOutputStore(),
        audit=InMemoryAuditStore(),
        agents=store,
    )
    env = ActivityEnvironment()
    original = copy.deepcopy(agent_definition(config.agents["it-helper"]))
    await republish(store, "it-helper", lambda d: d["grants"]["notion"].update(databases=[]))
    with pytest.raises(ApplicationError):
        await env.run(acts.create_notion_item, create_spec("A"), "task-1")

    await republish(store, "it-helper", lambda d: d.update(original))

    item = await env.run(acts.create_notion_item, create_spec("B"), "task-2")
    assert item.title == "B"


# --- front-door rules on the published set ---------------------------------------


async def front_door_store():
    config = parse_agents_config(front_door_dict())
    return seeded_agent_store(config), config


async def test_publish_that_breaks_the_front_door_rules_is_refused():
    store, _ = await front_door_store()

    # it-helper would become a front door while concierge lists it as a specialist.
    draft = await store.create_draft(TENANT, "it-helper", EDITOR)
    broken = copy.deepcopy(draft.definition)
    broken["specialists"] = ["unlisted"]
    await store.replace_draft(TENANT, "it-helper", draft.version, broken)
    await store.submit(TENANT, "it-helper", draft.version, EDITOR)

    with pytest.raises(ConfigError, match="cannot publish agent 'it-helper'.*front door"):
        await store.publish(TENANT, "it-helper", draft.version, ADMIN)

    assert (await store.get_version(TENANT, "it-helper", draft.version)).status == "pending"
    assert (await store.published(TENANT, "it-helper")).version == 1
    assert (await store.published(TENANT, "it-helper")).specialists == []


async def test_publish_naming_a_specialist_that_is_not_published_is_refused():
    store, _ = await front_door_store()
    door = helper_block()
    door["specialists"] = ["nobody"]
    await store.create_agent(TENANT, "new-door", door, EDITOR)
    await store.submit(TENANT, "new-door", 1, EDITOR)

    with pytest.raises(ConfigError, match="specialist 'nobody'.*not a defined agent"):
        await store.publish(TENANT, "new-door", 1, ADMIN)

    assert await store.published(TENANT, "new-door") is None


async def test_publish_dropping_a_specialists_description_is_refused():
    store, _ = await front_door_store()
    draft = await store.create_draft(TENANT, "it-helper", EDITOR)
    blank = copy.deepcopy(draft.definition)
    blank["description"] = ""
    await store.replace_draft(TENANT, "it-helper", draft.version, blank)
    await store.submit(TENANT, "it-helper", draft.version, EDITOR)

    with pytest.raises(ConfigError, match="description"):
        await store.publish(TENANT, "it-helper", draft.version, ADMIN)


# --- startup ---------------------------------------------------------------------


async def test_startup_refuses_a_default_agent_or_channel_agent_with_no_published_version():
    raw = agents_dict()
    raw["channels"] = {"C_HELP": "it-helper"}
    config = parse_agents_config(raw)
    tenants = TenantsConfig.of(config)

    assert await startup_agent_failure(seeded_agent_store(config), tenants) is None

    empty = InMemoryAgentStore()
    failure = await startup_agent_failure(empty, tenants)
    assert "tenant 'acme': default_agent names agent 'it-helper'" in failure
    assert "tenant 'acme': channel 'C_HELP' names agent 'it-helper'" in failure

    draft_only = InMemoryAgentStore()
    await draft_only.create_agent(TENANT, "it-helper", helper_block(), EDITOR)
    assert "default_agent names agent 'it-helper'" in await startup_agent_failure(
        draft_only, tenants
    )


async def test_published_tenants_carry_the_published_agents_and_the_yaml_settings():
    """Fails if the worker's tenant view shows a YAML agent instead of the published one, or loses the YAML settings."""
    raw = agents_dict()
    raw["channels"] = {"C_HELP": "it-helper"}
    config = parse_agents_config(raw)
    store = seeded_agent_store(config)
    await republish(store, "it-helper", lambda d: d.update(name="Renamed"))

    view = (await published_tenants(store, TenantsConfig.of(config))).tenants[TENANT]

    assert view.agents["it-helper"].name == "Renamed"
    assert (view.default_agent, view.channels, view.slack_team_id) == (
        "it-helper",
        {"C_HELP": "it-helper"},
        config.slack_team_id,
    )
    assert config.agents["it-helper"].name == "IT helper"
