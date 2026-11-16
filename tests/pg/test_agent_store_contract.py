"""The agent store's version lifecycle and import, one contract run against the in-memory
twin and the Postgres store. A behaviour that differs between them fails here."""

from __future__ import annotations

import copy
from dataclasses import replace

import pytest

from agentos import ids
from agentos.agent_store import AgentStoreError, InMemoryAgentStore, PostgresAgentStore
from agentos.audit import InMemoryAuditStore, PostgresAuditStore, canonical_sha256
from agentos.config import ConfigError, agent_definition, parse_agent, parse_agents_config
from tests.conftest import agents_dict

EDITOR = "U_EDITOR"
ADMIN = "U_ADMIN"
IMPORT = "import:yaml"


def helper_block() -> dict:
    block = copy.deepcopy(agents_dict()["agents"]["it-helper"])
    del block["version"]
    return block


def front_door_agents() -> dict:
    """``concierge`` is a front door for ``it-helper``; ``unlisted`` is neither."""
    raw = agents_dict()
    helper = raw["agents"]["it-helper"]
    helper["description"] = "Laptops, accounts, VPN, access and other IT requests."
    concierge = copy.deepcopy(helper)
    concierge.update(
        name="Concierge",
        description="General questions about the company.",
        system_prompt="You answer general questions.",
        grants={},
        specialists=["it-helper"],
    )
    raw["agents"]["concierge"] = concierge
    raw["agents"]["unlisted"] = {**copy.deepcopy(helper), "name": "Unlisted"}
    raw["default_agent"] = "concierge"
    return parse_agents_config(raw).agents


class World:
    def __init__(self, store, audit, tenant: str):
        self.store, self.audit, self.tenant = store, audit, tenant


@pytest.fixture
def world(store_pool, tenant: str) -> World:
    if store_pool is None:
        audit = InMemoryAuditStore()
        return World(InMemoryAgentStore(audit=audit), audit, tenant)
    pool = store_pool
    return World(PostgresAgentStore(pool), PostgresAuditStore(pool), tenant)


async def make_published(w: World, name: str | None = None):
    record = await w.store.create_agent(w.tenant, "it-helper", helper_block(), EDITOR)
    await w.store.submit(w.tenant, "it-helper", record.version, EDITOR)
    return await w.store.publish(w.tenant, "it-helper", record.version, ADMIN)


async def republish(w: World, name: str):
    draft = await w.store.create_draft(w.tenant, "it-helper", EDITOR)
    edited = copy.deepcopy(draft.definition)
    edited["name"] = name
    await w.store.replace_draft(w.tenant, "it-helper", draft.version, edited)
    await w.store.submit(w.tenant, "it-helper", draft.version, EDITOR)
    return await w.store.publish(w.tenant, "it-helper", draft.version, ADMIN)


async def test_create_submit_publish_records_who_and_when(world: World):
    """Fails if a version does not record who created, submitted and approved it, or is live before approval."""
    s, t = world.store, world.tenant
    draft = await s.create_agent(t, "it-helper", helper_block(), EDITOR)
    assert (draft.version, draft.status, draft.created_by) == (1, "draft", EDITOR)
    assert await s.published(t, "it-helper") is None

    pending = await s.submit(t, "it-helper", 1, EDITOR)
    assert (pending.status, pending.submitted_by) == ("pending", EDITOR)
    assert pending.submitted_at is not None
    assert await s.published(t, "it-helper") is None

    done = await s.publish(t, "it-helper", 1, ADMIN)
    assert (done.status, done.approved_by) == ("published", ADMIN)
    assert done.published_at is not None
    live = await s.published(t, "it-helper")
    assert (live.agent_id, live.version, live.name) == ("it-helper", 1, "IT helper")


async def test_second_publish_supersedes_the_first_and_only_one_is_published(world: World):
    """Fails if a second publish leaves two published versions or changes the superseded one."""
    await make_published(world)

    second = await republish(world, "IT helper 2")

    assert second.version == 2
    versions = await world.store.versions(world.tenant, "it-helper")
    assert [(v.version, v.status) for v in versions] == [(2, "published"), (1, "superseded")]
    assert (await world.store.published(world.tenant, "it-helper")).name == "IT helper 2"
    first = await world.store.get_version(world.tenant, "it-helper", 1)
    assert first.definition["name"] == "IT helper"


async def test_new_draft_copies_the_published_version_or_a_given_one(world: World):
    """Fails if a new draft copies a version other than the published one, or the one asked for."""
    await make_published(world)
    await republish(world, "Second")

    from_published = await world.store.create_draft(world.tenant, "it-helper", EDITOR)
    from_first = await world.store.create_draft(world.tenant, "it-helper", EDITOR, from_version=1)

    assert (from_published.version, from_published.definition["name"]) == (3, "Second")
    assert (from_first.version, from_first.definition["name"]) == (4, "IT helper")
    assert from_published.status == from_first.status == "draft"


async def test_withdraw_returns_a_pending_version_to_draft_and_it_can_be_edited_again(
    world: World,
):
    """Fails if a withdrawn version stays pending or cannot be edited again."""
    s, t = world.store, world.tenant
    await s.create_agent(t, "it-helper", helper_block(), EDITOR)
    await s.submit(t, "it-helper", 1, EDITOR)

    assert (await s.withdraw(t, "it-helper", 1)).status == "draft"
    again = await s.replace_draft(t, "it-helper", 1, {**helper_block(), "name": "Edited"})
    assert again.definition["name"] == "Edited"


async def test_illegal_moves_and_edits_are_refused(world: World):
    """Fails if a version moves out of order or an edit reaches a pending or published version."""
    s, t = world.store, world.tenant
    block = helper_block()
    await s.create_agent(t, "it-helper", block, EDITOR)

    with pytest.raises(AgentStoreError):  # a draft is not published without approval
        await s.publish(t, "it-helper", 1, ADMIN)
    with pytest.raises(AgentStoreError):  # only a pending version is withdrawn
        await s.withdraw(t, "it-helper", 1)
    await s.submit(t, "it-helper", 1, EDITOR)
    with pytest.raises(AgentStoreError):  # submitted twice
        await s.submit(t, "it-helper", 1, EDITOR)
    with pytest.raises(AgentStoreError):  # a pending version is not edited
        await s.replace_draft(t, "it-helper", 1, block)
    await s.publish(t, "it-helper", 1, ADMIN)
    with pytest.raises(AgentStoreError):  # a published version is not edited
        await s.replace_draft(t, "it-helper", 1, block)
    with pytest.raises(AgentStoreError):  # published twice
        await s.publish(t, "it-helper", 1, ADMIN)
    with pytest.raises(AgentStoreError):  # a version that does not exist
        await s.submit(t, "it-helper", 2, EDITOR)
    with pytest.raises(AgentStoreError):  # the agent exists
        await s.create_agent(t, "it-helper", block, EDITOR)
    with pytest.raises(AgentStoreError):  # a source version that does not exist
        await s.create_draft(t, "it-helper", EDITOR, from_version=9)
    assert (await s.get_version(t, "it-helper", 1)).definition == block


async def test_a_superseded_version_cannot_change(world: World):
    """Fails if a superseded version can be withdrawn or edited."""
    await make_published(world)
    await republish(world, "Second")

    with pytest.raises(AgentStoreError):
        await world.store.withdraw(world.tenant, "it-helper", 1)
    with pytest.raises(AgentStoreError):
        await world.store.replace_draft(world.tenant, "it-helper", 1, helper_block())


async def test_publishing_a_version_older_than_the_published_one_is_refused(world: World):
    """Fails if an older pending version can replace a newer published one."""
    s, t = world.store, world.tenant
    await make_published(world)
    for _ in range(2):
        draft = await s.create_draft(t, "it-helper", EDITOR)
        await s.submit(t, "it-helper", draft.version, EDITOR)
    await s.publish(t, "it-helper", 3, ADMIN)

    with pytest.raises(AgentStoreError, match="older than the published version 3"):
        await s.publish(t, "it-helper", 2, ADMIN)

    assert [(v.version, v.status) for v in await s.versions(t, "it-helper")] == [
        (3, "published"),
        (2, "pending"),
        (1, "superseded"),
    ]


async def test_a_given_version_field_is_ignored_because_the_store_assigns_it(world: World):
    """Fails if a version number in the submitted definition is honoured or stored."""
    block = {**helper_block(), "version": 7}
    record = await world.store.create_agent(world.tenant, "it-helper", block, EDITOR)
    assert record.version == 1
    assert "version" not in record.definition


async def test_tenants_are_kept_apart(world: World, other_tenant: str):
    """Fails if a tenant sees, or is blocked by, another tenant's agents."""
    await make_published(world)

    assert await world.store.published(other_tenant, "it-helper") is None
    assert await world.store.published_all(other_tenant) == {}
    config = parse_agents_config(agents_dict())
    await world.store.import_agents(other_tenant, config.agents)
    assert (await world.store.published(other_tenant, "it-helper")).version == 1


async def test_import_publishes_version_one_and_a_second_run_skips_and_names_each_agent(
    world: World,
):
    """Fails if an import does not publish each agent as version 1 by the importer, or a rerun changes anything."""
    agents = front_door_agents()
    s, t = world.store, world.tenant

    first = await s.import_agents(t, agents)
    assert sorted(first.imported) == ["concierge", "it-helper", "unlisted"]
    assert first.skipped == []
    for agent_id in first.imported:
        [record] = await s.versions(t, agent_id)
        assert (record.version, record.status) == (1, "published")
        assert (record.created_by, record.approved_by) == (IMPORT, IMPORT)

    second = await s.import_agents(t, agents)
    assert second.imported == []
    assert sorted(second.skipped) == ["concierge", "it-helper", "unlisted"]
    assert len(await s.versions(t, "it-helper")) == 1
    published = await s.published_all(t)
    assert set(published) == set(agents)
    for agent_id, agent in agents.items():
        assert published[agent_id] == parse_agent(agent_id, agent_definition(agent), 1)


async def test_import_skips_an_existing_agent_and_changes_nothing_in_it(world: World):
    """Fails if an import overwrites an agent that already exists."""
    await make_published(world)
    await republish(world, "Edited in the database")

    result = await world.store.import_agents(
        world.tenant, parse_agents_config(agents_dict()).agents
    )

    assert result.skipped == ["it-helper"]
    assert (await world.store.published(world.tenant, "it-helper")).name == (
        "Edited in the database"
    )


async def test_import_that_breaks_the_front_door_rules_is_refused_whole(world: World):
    """Fails if an import that names a specialist it does not define publishes any agent."""
    agents = dict(front_door_agents())
    del agents["it-helper"]

    with pytest.raises(ConfigError, match="specialist 'it-helper'.*not a defined agent"):
        await world.store.import_agents(world.tenant, agents)

    assert await world.store.published_all(world.tenant) == {}
    assert await world.audit.list(world.tenant) == []


async def test_import_checks_the_new_agents_against_those_already_published(world: World):
    """Fails if a front door is refused although its specialist was published by an earlier import."""
    agents = front_door_agents()
    await world.store.import_agents(world.tenant, {"it-helper": agents["it-helper"]})

    await world.store.import_agents(world.tenant, {"concierge": agents["concierge"]})

    assert set(await world.store.published_all(world.tenant)) == {"it-helper", "concierge"}


async def test_import_audits_each_imported_agent_once_and_a_rerun_writes_nothing(world: World):
    """Fails if an import audits an agent twice or with content, or a rerun writes an audit row."""
    agents = front_door_agents()
    s, t = world.store, world.tenant

    await s.import_agents(t, agents)

    events = await world.audit.list(t)
    assert sorted(e.agent_id for e in events) == ["concierge", "it-helper", "unlisted"]
    for event in events:
        assert (event.kind, event.agent_version, event.outcome) == ("agent_import", 1, "ok")
        assert (event.requester_user_id, event.approver_user_id) == (IMPORT, IMPORT)
        assert event.identity == f"user:{IMPORT}"
        assert event.tenant_id == t
        assert event.event_key == ids.audit_import_key(t, event.agent_id)
        [record] = await s.versions(t, event.agent_id)
        assert event.input_sha256 == canonical_sha256(record.definition)
        assert "system_prompt" not in repr(event)

    await s.import_agents(t, agents)

    assert len(await world.audit.list(t)) == 3


async def test_the_import_refuses_an_agent_id_outside_the_pattern(world: World):
    """Fails if the one-time import writes an agent whose ID the console cannot route."""
    good = parse_agent("it-helper", helper_block(), 1)
    with pytest.raises(ConfigError, match="agent ID"):
        await world.store.import_agents(world.tenant, {"a/b": replace(good, agent_id="a/b")})
    assert await world.store.list_agents(world.tenant) == []
