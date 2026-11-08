import pytest

from agentos.connectors.notion import GrantDenied, NotionConnector, UnknownRequester
from tests.conftest import (
    ALICE_NOTION,
    BOB_NOTION,
    DB_ID,
    OTHER_DB_ID,
    make_fake_notion,
    title_prop,
)


def connector(api, agents):
    return NotionConnector(api, agents.agents["it-helper"].grants.notion_databases)


async def test_grant_denied_makes_no_api_call(agents):
    api = make_fake_notion()
    page = api.add_page(OTHER_DB_ID, {"Name": title_prop("Secret HR case")})
    conn = connector(api, agents)

    with pytest.raises(GrantDenied):
        await conn.get_item(page["id"])

    assert [name for name, _ in api.calls] == ["retrieve_page"]
    assert api.count("query_database") == 0
    assert api.count("create_page") == 0

    # A direct call naming an ungranted database is refused before any request.
    api.calls.clear()
    with pytest.raises(GrantDenied):
        await conn.create_item(OTHER_DB_ID, "x", "y", None, "acme:task:1:create")
    assert api.calls == []


async def test_create_item_is_idempotent_after_lost_response(agents):
    api = make_fake_notion()
    conn = connector(api, agents)
    key = "acme:task:abc:create"

    api.fail_next_create_after_write()
    with pytest.raises(TimeoutError):
        await conn.create_item(DB_ID, "Laptop battery swelling", "It bulges.", None, key)

    item = await conn.create_item(DB_ID, "Laptop battery swelling", "It bulges.", None, key)

    pages = api.all_pages(DB_ID)
    assert len(pages) == 1
    assert item.page_id == pages[0]["id"]
    assert item.title == "Laptop battery swelling"
    assert api.count("create_page") == 1


async def test_my_items_pins_requester(agents):
    api = make_fake_notion()
    conn = connector(api, agents)

    tools = conn.tools_for_llm()
    my_items_tool = next(t for t in tools if t["name"] == "my_items")
    assert my_items_tool["input_schema"].get("properties") == {}
    assert "required" not in my_items_tool["input_schema"]
    assert "get_item" not in [t["name"] for t in tools]

    api.add_page(
        DB_ID,
        {"Name": title_prop("Alice monitor"), "Requested by": {"people": [{"id": ALICE_NOTION}]}},
    )
    api.add_page(
        DB_ID,
        {"Name": title_prop("Bob keyboard"), "Requested by": {"people": [{"id": BOB_NOTION}]}},
    )

    items = await conn.my_items("alice@EXAMPLE.com")
    assert [i.title for i in items] == ["Alice monitor"]


async def test_my_items_unknown_requester(agents):
    conn = connector(make_fake_notion(), agents)
    with pytest.raises(UnknownRequester):
        await conn.my_items("nobody@example.com")
