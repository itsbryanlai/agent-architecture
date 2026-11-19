from dataclasses import replace

import httpx
import psycopg
import pytest
from notion_client.errors import RequestTimeoutError

from agentos.agent_store import InMemoryAgentStore
from agentos.check_grants import check_published
from agentos.config import ConfigError, NotionDatabaseGrant, TenantsConfig, parse_agents_config
from agentos.connectors.notion import NotionConnector, check_grant
from agentos.grant_checker import GrantCheckUnavailable, NotionGrantChecker, is_transient
from tests.conftest import DB_ID, TENANT, agents_dict, default_schema, make_fake_notion
from tests.helpers.agents import helper_block, make_published

NAMES_ONLY_IDS = {
    "title_prop_id": None,
    "status_prop_id": None,
    "requested_by_prop_id": None,
    "request_id_prop_id": None,
}


def grant_from(agents) -> NotionDatabaseGrant:
    return agents.agents["it-helper"].grants.notion_databases[0]


def names_only(agents) -> NotionDatabaseGrant:
    return replace(grant_from(agents), **NAMES_ONLY_IDS)


def schema_with(**changes) -> dict:
    """The default schema with some property names replaced (old name -> new)."""
    schema = default_schema()
    return {changes.get(name, name): prop for name, prop in schema.items()}


async def test_check_resolves_ids_by_name(agents):
    """Fails if a names-only grant reports a problem or does not resolve all four property ids."""
    api = make_fake_notion()
    check = await check_grant(api, names_only(agents), require_ids=False)
    assert check.problems == []
    assert check.resolved == {
        "title": "title",
        "status": "stat1",
        "requested_by": "reqby1",
        "request_id": "reqid1",
    }


async def test_check_trailing_space_is_problem_with_hint(agents):
    api = make_fake_notion()
    api.set_schema(DB_ID, schema_with(**{"Agent request ID": "Agent request ID "}))
    check = await check_grant(api, names_only(agents), require_ids=False)
    assert len(check.problems) == 1
    assert "request_id" in check.problems[0]
    assert "'Agent request ID '" in check.problems[0]
    assert "trailing space" in check.problems[0]


async def test_check_wrong_type_is_problem(agents):
    api = make_fake_notion()
    schema = default_schema()
    schema["Agent request ID"] = {"id": "reqid1", "type": "people"}
    api.set_schema(DB_ID, schema)
    check = await check_grant(api, grant_from(agents), require_ids=True)
    assert len(check.problems) == 1
    assert "request_id" in check.problems[0]
    assert "rich_text" in check.problems[0]
    assert "people" in check.problems[0]


async def test_check_missing_done_value(agents):
    api = make_fake_notion()
    schema = default_schema()
    schema["Status"]["options"] = ["To do", "Doing"]
    api.set_schema(DB_ID, schema)
    check = await check_grant(api, grant_from(agents), require_ids=True)
    assert len(check.problems) == 1
    assert "Done" in check.problems[0]


async def test_check_renamed_property_by_id_is_note(agents):
    api = make_fake_notion()
    api.set_schema(DB_ID, schema_with(Status="Stage"))
    check = await check_grant(api, grant_from(agents), require_ids=True)
    assert check.problems == []
    assert check.resolved["status"] == "stat1"
    assert len(check.notes) == 1
    assert "'Status'" in check.notes[0] and "'Stage'" in check.notes[0]


async def test_connector_uses_ids_in_filters_and_create(agents):
    api = make_fake_notion()
    # Names differ from the config; only IDs can work.
    api.set_schema(DB_ID, schema_with(Name="Title", **{"Agent request ID": "Key"}))
    conn = NotionConnector(api, [grant_from(agents)])

    await conn.find_item(DB_ID, "acme:task:1:create")
    await conn.create_item(DB_ID, "Laptop", "Details", "acme:task:1:create")

    query = next(c for n, c in api.calls if n == "query_database")
    assert query["filter"]["property"] == "reqid1"
    create = next(c for n, c in api.calls if n == "create_page")
    assert set(create["properties"]) == {"title", "reqid1"}


async def test_to_item_reads_by_id_after_rename(agents):
    api = make_fake_notion()
    api.set_schema(DB_ID, schema_with(Name="Title", Status="Stage"))
    page = api.add_page(DB_ID, {"title": {"title": [{"text": {"content": "Monitor"}}]}})
    api.set_status(page["id"], "stat1", "Done")
    conn = NotionConnector(api, [grant_from(agents)])

    item = await conn.get_item(page["id"])

    assert item.title == "Monitor"
    assert item.status == "Done"


def test_config_parses_property_ids(agents):
    grant = grant_from(agents)
    assert grant.title_prop_id == "title"
    assert grant.status_prop_id == "stat1"
    assert grant.requested_by_prop_id == "reqby1"
    assert grant.request_id_prop_id == "reqid1"

    raw = agents_dict()
    db = raw["agents"]["it-helper"]["grants"]["notion"]["databases"][0]
    db["property_ids"]["bogus"] = "x"
    with pytest.raises(ConfigError) as exc:
        parse_agents_config(raw)
    assert "it-helper" in str(exc.value) and "bogus" in str(exc.value)

    db["property_ids"] = ["not", "a", "dict"]
    with pytest.raises(ConfigError):
        parse_agents_config(raw)

    del db["property_ids"]
    parsed = parse_agents_config(raw)
    assert parsed.agents["it-helper"].grants.notion_databases[0].title_prop_id is None


def test_connector_rejects_grant_without_ids(agents):
    with pytest.raises(ConfigError):
        NotionConnector(make_fake_notion(), [names_only(agents)])


# --- what counts as Notion being unavailable ------------------------------------------


class Status(Exception):
    def __init__(self, status):
        super().__init__("notion error")
        self.status = status


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError("refused"),
        httpx.ReadTimeout("slow"),
        RequestTimeoutError(),
        TimeoutError("slow"),
        ConnectionResetError("reset"),
        Status(429),
        Status(500),
        Status(503),
    ],
)
def test_network_errors_rate_limits_and_server_errors_are_transient(error):
    assert is_transient(error)


@pytest.mark.parametrize(
    "error",
    [
        Status(400),
        Status(403),
        Status(404),
        RuntimeError("a bug"),
        ValueError("bad value"),
        KeyError("missing"),
        ConfigError("tenant 'acme': environment variable X is not set"),
    ],
)
def test_anything_else_is_not_transient(error):
    assert not is_transient(error)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ConfigError("tenant 'acme': environment variable NOTION_TOKEN is not set"), ConfigError),
        (ConnectionError("database is down"), GrantCheckUnavailable),
        (psycopg.OperationalError("connection failed"), GrantCheckUnavailable),
        (RuntimeError("a bug"), RuntimeError),
    ],
    ids=["setup-error", "store-down", "driver-outage", "bug"],
)
async def test_a_failure_resolving_the_client_keeps_setup_errors_and_maps_outages(
    agents, error, expected
):
    """Fails if a setup error resolving the Notion client is reported as an outage, a store
    outage surfaces as itself instead of the unavailable answer, or a bug is hidden."""

    def resolve(tenant_id):
        raise error

    with pytest.raises(expected) as err:
        await NotionGrantChecker(resolve).check("acme", agents.agents["it-helper"])
    if expected is GrantCheckUnavailable:
        assert err.value.__cause__ is error


async def test_an_unexpected_error_while_checking_propagates_as_itself(agents):
    class Broken:
        async def retrieve_schema(self, database_id):
            raise RuntimeError("a bug")

    with pytest.raises(RuntimeError, match="a bug"):
        await NotionGrantChecker(lambda t: Broken()).check("acme", agents.agents["it-helper"])


async def test_a_transient_error_is_unavailable_and_keeps_its_cause(agents):
    class Down:
        async def retrieve_schema(self, database_id):
            raise httpx.ConnectError("refused")

    with pytest.raises(GrantCheckUnavailable) as err:
        await NotionGrantChecker(lambda t: Down()).check("acme", agents.agents["it-helper"])
    assert isinstance(err.value.__cause__, httpx.ConnectError)


@pytest.mark.parametrize("code", [403, 404])
async def test_a_database_notion_will_not_show_is_a_grant_problem(agents, code):
    class Hidden:
        async def retrieve_schema(self, database_id):
            raise Status(code)

    [report] = await NotionGrantChecker(lambda t: Hidden()).check(
        "acme", agents.agents["it-helper"]
    )

    assert len(report.problems) == 1 and "IT requests" in report.problems[0]
    assert "Status" in report.problems[0]


@pytest.mark.parametrize(
    ("label", "live_title", "warns"),
    [
        ("IT Helpdesk Guidebook", "Acme Corp IT Helpdesk Guidebook", False),
        ("Acme Corp IT Helpdesk Guidebook", "IT Helpdesk Guidebook", False),
        ("Acme Corp — IT Helpdesk", "acme corp it helpdesk", False),
        ("IT Helpdesk Guidebook", "HR Policy Handbook", True),
        ("HR", "Shrink requests", True),
        ("IT", "Edit log", True),
        ("---", "IT Helpdesk Guidebook", True),
    ],
)
async def test_a_label_contained_in_the_live_title_is_not_flagged(agents, label, live_title, warns):
    """Fails if a label contained in the live title is flagged, or a real mismatch stops warning."""
    api = make_fake_notion()
    api.set_database_info(DB_ID, live_title)
    agent = agents.agents["it-helper"]
    grant = replace(agent.grants.notion_databases[0], name=label)
    version = replace(agent, grants=replace(agent.grants, notion_databases=[grant]))

    [report] = await NotionGrantChecker(lambda t: api).check(TENANT, version)

    assert report.label_differs is warns
    assert report.live_title == live_title


@pytest.mark.parametrize(
    ("container_title", "data_source_title"),
    [("New database", "IT requests"), ("IT requests", None)],
    ids=["container differs", "no data source"],
)
async def test_a_single_data_sources_title_is_the_live_title(
    agents, container_title, data_source_title
):
    """Fails if a database whose container title differs from its single data source's title
    shows the container's title instead of the name people see in Notion."""
    api = make_fake_notion()
    api.set_database_info(DB_ID, container_title, data_source_title=data_source_title)

    [report] = await NotionGrantChecker(lambda t: api).check(TENANT, agents.agents["it-helper"])

    assert report.live_title == "IT requests"
    assert api.count("retrieve_data_source") == (1 if data_source_title is not None else 0)


async def test_the_check_command_reads_published_definitions_from_the_database_not_the_yaml(
    agents, capsys
):
    """Fails if the command checks the YAML file's agents instead of the ones the worker serves
    from the database, or skips a published agent whose grant has a problem."""
    store = InMemoryAgentStore()
    definition = helper_block()
    await make_published(store, "db-helper", definition)
    assert "db-helper" not in agents.agents  # the YAML file's tenant has only it-helper
    api = make_fake_notion()
    broken = default_schema()
    broken["Agent request ID"] = {"id": "reqid1", "type": "people"}
    api.set_schema(DB_ID, broken)

    code = await check_published(TenantsConfig.of(agents), store, lambda config: api)

    out = capsys.readouterr().out
    assert code == 1
    assert "agent 'db-helper'" in out and "PROBLEM" in out
    assert "it-helper" not in out


async def test_the_check_command_prints_property_ids_for_a_draft_before_it_is_published(
    agents, capsys
):
    """Fails if the property IDs cannot be had before publishing (the worker refuses a published
    agent without them), or the command picks the published version over a newer draft."""
    store = InMemoryAgentStore()
    published = helper_block()
    published["grants"]["notion"]["databases"][0]["name"] = "Old name"
    await make_published(store, "db-helper", published)
    draft = helper_block()
    del draft["grants"]["notion"]["databases"][0]["property_ids"]
    await store.create_draft(TENANT, "db-helper", "U1")
    await store.replace_draft(TENANT, "db-helper", 2, draft)
    tenants = TenantsConfig.of(agents)

    code = await check_published(
        tenants, store, lambda config: make_fake_notion(), tenant_id=TENANT, agent_id="db-helper"
    )

    out = capsys.readouterr().out
    assert code == 0
    assert "version 2" in out and "grant 'IT requests'" in out
    assert 'status: "stat1"' in out


async def test_the_check_command_resolves_the_client_through_an_async_resolver(agents, capsys):
    """Fails if the command cannot build the tenant's Notion client through a credential
    resolver that reads the database (an async notion_for)."""
    store = InMemoryAgentStore()
    await make_published(store, "db-helper", helper_block())
    api = make_fake_notion()

    async def notion_for(config):
        return api

    code = await check_published(TenantsConfig.of(agents), store, notion_for)

    assert code == 0
    assert "agent 'db-helper'" in capsys.readouterr().out
