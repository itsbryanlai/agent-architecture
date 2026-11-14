"""Agents in the database: lifecycle, what a turn loads, front-door rules, import, startup."""

import copy
from pathlib import Path

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_store import (
    ALLOWED_MOVES,
    AgentStoreError,
    InMemoryAgentStore,
    published_tenants,
    seeded_agent_store,
    startup_agent_failure,
)
from agentos.audit import InMemoryAuditStore, canonical_sha256
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
from tests.test_front_door import front_door_dict

ROOT = Path(__file__).resolve().parent.parent
MIGRATION = (ROOT / "scripts" / "migrations" / "006_agents.sql").read_text()
INIT_DB = (ROOT / "scripts" / "init_db.sql").read_text()

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


async def test_create_submit_publish_records_who_and_when():
    store = InMemoryAgentStore()
    draft = await store.create_agent(TENANT, "it-helper", helper_block(), EDITOR)
    assert (draft.version, draft.status, draft.created_by) == (1, "draft", EDITOR)
    assert await store.published(TENANT, "it-helper") is None

    pending = await store.submit(TENANT, "it-helper", 1, EDITOR)
    assert (pending.status, pending.submitted_by) == ("pending", EDITOR)
    assert pending.submitted_at is not None
    assert await store.published(TENANT, "it-helper") is None

    done = await store.publish(TENANT, "it-helper", 1, ADMIN)
    assert (done.status, done.approved_by) == ("published", ADMIN)
    assert done.published_at is not None
    live = await store.published(TENANT, "it-helper")
    assert (live.agent_id, live.version, live.name) == ("it-helper", 1, "IT helper")


async def test_second_publish_supersedes_the_first_and_only_one_is_published():
    store = InMemoryAgentStore()
    await make_published(store)

    second = await republish(store, "it-helper", lambda d: d.update(name="IT helper 2"))

    assert second.version == 2
    versions = await store.versions(TENANT, "it-helper")
    assert [(v.version, v.status) for v in versions] == [(2, "published"), (1, "superseded")]
    assert [v.status for v in versions].count("published") == 1
    assert (await store.published(TENANT, "it-helper")).name == "IT helper 2"
    assert (await store.get_version(TENANT, "it-helper", 1)).definition["name"] == "IT helper"


async def test_new_draft_copies_the_published_version_or_a_given_one():
    store = InMemoryAgentStore()
    await make_published(store)
    await republish(store, "it-helper", lambda d: d.update(name="Second"))

    from_published = await store.create_draft(TENANT, "it-helper", EDITOR)
    from_first = await store.create_draft(TENANT, "it-helper", EDITOR, from_version=1)

    assert (from_published.version, from_published.definition["name"]) == (3, "Second")
    assert (from_first.version, from_first.definition["name"]) == (4, "IT helper")
    assert from_published.status == from_first.status == "draft"


async def test_withdraw_returns_a_pending_version_to_draft_and_it_can_be_edited_again():
    store = InMemoryAgentStore()
    await draft_helper(store)
    await store.submit(TENANT, "it-helper", 1, EDITOR)

    back = await store.withdraw(TENANT, "it-helper", 1)
    assert back.status == "draft"
    edited = copy.deepcopy(helper_block())
    edited["name"] = "Edited"
    again = await store.replace_draft(TENANT, "it-helper", 1, edited)
    assert again.definition["name"] == "Edited"


async def test_illegal_moves_and_edits_are_refused():
    store = InMemoryAgentStore()
    await draft_helper(store)
    block = helper_block()

    with pytest.raises(AgentStoreError):  # a draft is not published without approval
        await store.publish(TENANT, "it-helper", 1, ADMIN)
    with pytest.raises(AgentStoreError):  # only a pending version is withdrawn
        await store.withdraw(TENANT, "it-helper", 1)
    await store.submit(TENANT, "it-helper", 1, EDITOR)
    with pytest.raises(AgentStoreError):  # submitted twice
        await store.submit(TENANT, "it-helper", 1, EDITOR)
    with pytest.raises(AgentStoreError):  # a pending version is not edited
        await store.replace_draft(TENANT, "it-helper", 1, block)
    await store.publish(TENANT, "it-helper", 1, ADMIN)
    with pytest.raises(AgentStoreError):  # a published version is not edited
        await store.replace_draft(TENANT, "it-helper", 1, block)
    with pytest.raises(AgentStoreError):  # published twice
        await store.publish(TENANT, "it-helper", 1, ADMIN)
    with pytest.raises(AgentStoreError):
        await store.submit(TENANT, "it-helper", 2, EDITOR)
    with pytest.raises(AgentStoreError):
        await store.create_agent(TENANT, "it-helper", block, EDITOR)
    with pytest.raises(AgentStoreError):
        await store.create_draft(TENANT, "it-helper", EDITOR, from_version=9)
    assert (await store.get_version(TENANT, "it-helper", 1)).definition == block


async def test_superseded_version_cannot_change():
    store = InMemoryAgentStore()
    await make_published(store)
    await republish(store, "it-helper", lambda d: d.update(name="Second"))

    with pytest.raises(AgentStoreError):
        await store.withdraw(TENANT, "it-helper", 1)
    with pytest.raises(AgentStoreError):
        await store.replace_draft(TENANT, "it-helper", 1, helper_block())


def test_allowed_moves_are_the_four_the_trigger_allows():
    assert ALLOWED_MOVES == {
        ("draft", "pending"),
        ("pending", "draft"),
        ("pending", "published"),
        ("published", "superseded"),
    }


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


async def test_a_given_version_field_is_ignored_because_the_store_assigns_it():
    store = InMemoryAgentStore()
    block = {**helper_block(), "version": 7}
    record = await store.create_agent(TENANT, "it-helper", block, EDITOR)
    assert record.version == 1
    assert "version" not in record.definition


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


# --- one-time import -------------------------------------------------------------


async def test_import_publishes_version_one_and_a_second_run_skips_and_names_each_agent():
    config = parse_agents_config(front_door_dict())
    store = InMemoryAgentStore()

    first = await store.import_agents(TENANT, config.agents)
    assert sorted(first.imported) == ["concierge", "it-helper", "unlisted"]
    assert first.skipped == []
    for agent_id in first.imported:
        [record] = await store.versions(TENANT, agent_id)
        assert (record.version, record.status) == (1, "published")
        assert (record.created_by, record.approved_by) == ("import:yaml", "import:yaml")

    second = await store.import_agents(TENANT, config.agents)
    assert second.imported == []
    assert sorted(second.skipped) == ["concierge", "it-helper", "unlisted"]
    assert len(await store.versions(TENANT, "it-helper")) == 1

    published = await store.published_all(TENANT)
    assert set(published) == set(config.agents)
    for agent_id, agent in config.agents.items():
        assert published[agent_id] == replace_version(agent, 1)


def replace_version(agent, version):
    return parse_agent(agent.agent_id, agent_definition(agent), version)


async def test_import_skips_an_existing_agent_and_changes_nothing_in_it():
    config = parse_agents_config(agents_dict())
    store = InMemoryAgentStore()
    await make_published(store)
    await republish(store, "it-helper", lambda d: d.update(name="Edited in the database"))

    result = await store.import_agents(TENANT, config.agents)

    assert result.skipped == ["it-helper"]
    assert (await store.published(TENANT, "it-helper")).name == "Edited in the database"


async def test_import_that_breaks_the_front_door_rules_is_refused_whole():
    config = parse_agents_config(front_door_dict())
    agents = dict(config.agents)
    del agents["it-helper"]
    store = InMemoryAgentStore()

    with pytest.raises(ConfigError, match="specialist 'it-helper'.*not a defined agent"):
        await store.import_agents(TENANT, agents)

    assert await store.published_all(TENANT) == {}


async def test_import_checks_the_new_agents_against_those_already_published():
    config = parse_agents_config(front_door_dict())
    store = InMemoryAgentStore()
    await store.import_agents(TENANT, {"it-helper": config.agents["it-helper"]})

    await store.import_agents(TENANT, {"concierge": config.agents["concierge"]})

    assert set(await store.published_all(TENANT)) == {"it-helper", "concierge"}


async def test_import_audits_each_imported_agent_once_and_a_rerun_writes_nothing():
    config = parse_agents_config(front_door_dict())
    audit = InMemoryAuditStore()
    store = InMemoryAgentStore(audit=audit)

    await store.import_agents(TENANT, config.agents)

    assert sorted(e.agent_id for e in audit.events) == ["concierge", "it-helper", "unlisted"]
    for event in audit.events:
        assert (event.kind, event.agent_version, event.outcome) == ("agent_import", 1, "ok")
        assert (event.requester_user_id, event.approver_user_id) == ("import:yaml", "import:yaml")
        assert event.identity == "user:import:yaml"
        assert event.tenant_id == TENANT
        assert event.event_key == ids.audit_import_key(TENANT, event.agent_id)
        [record] = await store.versions(TENANT, event.agent_id)
        assert event.input_sha256 == canonical_sha256(record.definition)
        assert "system_prompt" not in repr(event)

    await store.import_agents(TENANT, config.agents)

    assert len(audit.events) == 3


async def test_a_refused_import_audits_nothing():
    config = parse_agents_config(front_door_dict())
    agents = dict(config.agents)
    del agents["it-helper"]
    audit = InMemoryAuditStore()

    with pytest.raises(ConfigError):
        await InMemoryAgentStore(audit=audit).import_agents(TENANT, agents)

    assert audit.events == []


async def test_tenants_are_kept_apart():
    one = parse_agents_config(agents_dict())
    store = InMemoryAgentStore()
    await store.import_agents("acme", one.agents)

    assert await store.published("globex", "it-helper") is None
    assert await store.published_all("globex") == {}
    await store.import_agents("globex", one.agents)
    assert (await store.published("globex", "it-helper")).version == 1


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


# --- migration text --------------------------------------------------------------


@pytest.mark.parametrize("sql", [MIGRATION, INIT_DB], ids=["migration", "init_db"])
def test_sql_has_row_level_security_grants_trigger_and_one_published_index(sql):
    for table in ("agents", "agent_versions"):
        assert f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY" in sql
        assert f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY" in sql
        assert f"CREATE POLICY tenant_isolation ON {table}" in sql
    assert "USING (tenant_id = current_setting('app.tenant_id', true))" in sql
    assert "WITH CHECK (tenant_id = current_setting('app.tenant_id', true))" in sql

    assert "REVOKE ALL ON agents, agent_versions FROM agentos_app" in sql
    assert "GRANT SELECT, INSERT ON agents TO agentos_app" in sql
    assert "GRANT SELECT, INSERT, UPDATE ON agent_versions TO agentos_app" in sql
    agents_part = sql.split("REVOKE ALL ON agents")[1].split("CREATE TABLE IF NOT EXISTS members")[
        0
    ]
    assert "DELETE ON" not in agents_part

    assert "PRIMARY KEY (tenant_id, agent_id)" in sql
    assert "PRIMARY KEY (tenant_id, agent_id, version)" in sql
    assert (
        "CREATE UNIQUE INDEX IF NOT EXISTS agent_versions_one_published\n"
        "  ON agent_versions (tenant_id, agent_id) WHERE status = 'published'"
    ) in sql

    assert "CREATE TRIGGER agent_versions_guard" in sql
    assert "BEFORE UPDATE OR DELETE ON agent_versions" in sql
    assert "only a draft can be deleted" in sql
    assert "the definition changes only while a version is a draft" in sql
    assert "OLD.status = 'draft'   AND NEW.status = 'pending'" in sql
    assert "OLD.status = 'pending' AND NEW.status IN ('draft', 'published')" in sql
    assert "OLD.status = 'published' AND NEW.status = 'superseded'" in sql
    assert "CHECK (status IN ('draft', 'pending', 'published', 'superseded'))" in " ".join(
        sql.split()
    )


def test_init_db_matches_the_migration_for_agents():
    body = MIGRATION.split("\n\n", 1)[1]
    assert body in INIT_DB
