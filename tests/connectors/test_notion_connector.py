import pytest

from agentos.connectors.notion import (
    ConnectorError,
    GrantDenied,
    NotionConnector,
    NotionUserCache,
    UnknownItem,
    UnknownRequester,
)
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


async def test_create_in_an_ungranted_database_sends_nothing(agents):
    """Fails if a create naming an ungranted database reaches Notion before the grant check."""
    api = make_fake_notion()
    conn = connector(api, agents)

    with pytest.raises(GrantDenied):
        await conn.create_item(OTHER_DB_ID, "x", "y", "acme:task:1:create")

    assert api.calls == []


@pytest.mark.parametrize("method", ["get_item", "resolve_item"])
async def test_an_ungranted_page_and_a_missing_page_get_the_same_error(agents, method):
    """Fails if the error tells whether an ungranted page exists."""
    api = make_fake_notion()
    ungranted = api.add_page(OTHER_DB_ID, {"Name": title_prop("Secret HR case")})
    conn = connector(api, agents)

    with pytest.raises(UnknownItem) as denied:
        await getattr(conn, method)(ungranted["id"])
    with pytest.raises(UnknownItem) as missing:
        await getattr(conn, method)("00000000-0000-0000-0000-000000000000")

    assert str(denied.value) == str(missing.value)
    assert api.count("query_database") == 0


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


async def test_a_requester_is_looked_up_once_and_a_miss_is_remembered(agents):
    """Fails if a lookup pages the Notion user list again for a person already looked up."""
    api = make_fake_notion()
    users = NotionUserCache()

    def fresh():
        return NotionConnector(api, agents.agents["it-helper"].grants.notion_databases, users=users)

    assert await fresh()._notion_user_id("alice@example.com") == ALICE_NOTION
    assert await fresh()._notion_user_id("alice@example.com") == ALICE_NOTION
    assert await fresh()._notion_user_id("nobody@example.com") is None
    assert await fresh()._notion_user_id("nobody@example.com") is None

    assert api.count("list_users") == 1


async def test_a_cached_requester_is_looked_up_again_after_the_ttl(agents):
    """Fails if a cached answer never expires, so a new Notion user is never found."""
    api = make_fake_notion()
    now = [0.0]
    users = NotionUserCache(ttl_seconds=60, clock=lambda: now[0])

    assert await users.user_id(api, "new@example.com") is None
    api.users.append({"object": "user", "id": "notion-new", "person": {"email": "new@example.com"}})
    now[0] = 61
    assert await users.user_id(api, "new@example.com") == "notion-new"


@pytest.mark.parametrize(
    ("mention", "text_len", "fits"),
    [(None, 1900, True), (BOB_NOTION, 1899, True), (BOB_NOTION, 1900, False)],
    ids=["plain-at-limit", "mention-under-limit", "mention-space-overflows"],
)
async def test_the_comment_body_check_counts_the_mentions_leading_space(
    agents, mention, text_len, fits
):
    """Fails if a comment at Notion's limit is sent when a real mention's leading space pushes it over."""
    api = make_fake_notion()
    page = api.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    conn = connector(api, agents)
    name = "a" * 26  # the longest signature name: text plus signature is text plus 100

    if fits:
        await conn.create_comment(
            page["id"], "c" * text_len, "acme:task:1:comment", name, mention_user_id=mention
        )
        [comment] = api.comments[page["id"].replace("-", "")]
        assert len(comment["rich_text"][-1]["text"]["content"]) <= 2000
    else:
        with pytest.raises(ConnectorError):
            await conn.create_comment(
                page["id"], "c" * text_len, "acme:task:1:comment", name, mention_user_id=mention
            )
        assert api.count("create_comment") == 0
