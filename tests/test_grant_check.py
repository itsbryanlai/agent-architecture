import logging
from dataclasses import replace

import httpx
import pytest
from notion_client.errors import RequestTimeoutError

from agentos.config import ConfigError, NotionDatabaseGrant, parse_agents_config
from agentos.connectors.notion import NotionConnector, check_grant
from agentos.grant_checker import GrantCheckUnavailable, NotionGrantChecker, is_transient
from agentos.worker import check_grants_at_startup
from tests.conftest import DB_ID, agents_dict, default_schema, make_fake_notion

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

    await conn.create_item(DB_ID, "Laptop", "Details", None, "acme:task:1:create")

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


async def test_worker_refuses_to_start_on_problems(agents):
    api = make_fake_notion()
    schema = default_schema()
    schema["Agent request ID"] = {"id": "reqid1", "type": "people"}
    api.set_schema(DB_ID, schema)
    failure = await check_grants_at_startup(api, agents, logging.getLogger("test"))
    assert failure is not None
    assert "will not start" in failure


async def test_worker_starts_when_grants_are_clean(agents):
    api = make_fake_notion()
    assert await check_grants_at_startup(api, agents, logging.getLogger("test")) is None


async def test_worker_refuses_to_start_without_ids(agents):
    api = make_fake_notion()
    raw = agents_dict()
    del raw["agents"]["it-helper"]["grants"]["notion"]["databases"][0]["property_ids"]
    failure = await check_grants_at_startup(
        api, parse_agents_config(raw), logging.getLogger("test")
    )
    assert failure is not None
    assert "agentos.check_grants" in failure


async def test_check_command_prints_ids_for_names_only_config(agents):
    api = make_fake_notion()
    check = await check_grant(api, names_only(agents), require_ids=False)
    assert check.problems == []
    assert set(check.resolved) == {"title", "status", "requested_by", "request_id"}


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


async def test_a_failure_building_the_client_is_not_a_notion_outage(agents):
    def no_client(tenant_id):
        raise ConfigError("tenant 'acme': environment variable NOTION_TOKEN is not set")

    checker = NotionGrantChecker(no_client)

    with pytest.raises(ConfigError, match="NOTION_TOKEN"):
        await checker.check("acme", agents.agents["it-helper"])


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
