"""The connector interface's promises, through the registry: Notion now, Drive joins later."""

import httpx
import pytest
from notion_client.errors import APIResponseError

from agentos.activities import _is_client_error
from agentos.connectors.notion import normalize_id
from agentos.connectors.registry import connector_for
from agentos.slack_client import FakeSlack
from agentos.tenants import runtime_for
from tests.conftest import ALICE, DB_ID, make_fake_notion, title_prop


@pytest.fixture(params=["notion"])
def connector(request, agents):
    """The registered connector and the fake API it acts on; Drive joins the params."""
    api = make_fake_notion()
    runtime = runtime_for(agents, FakeSlack(emails={ALICE: "alice@example.com"}), api)
    return connector_for(request.param, runtime, agents.agents["it-helper"]), api


@pytest.mark.parametrize(
    ("kind", "fail_kind"),
    [("create_item", "create"), ("add_comment", "comment")],
    ids=["item", "comment"],
)
async def test_the_kind_lookup_finds_a_write_whose_response_was_lost(connector, kind, fail_kind):
    """Fails if find_write cannot find a prior write after its response was lost."""
    conn, api = connector
    idem_key = "acme:task-1:write"
    if kind == "create_item":
        target = DB_ID
        payload = {"title": "New monitor", "details": "Mine flickers."}
    else:
        target = api.add_page(DB_ID, {"Name": title_prop("Broken VPN")})["id"]
        payload = {"comment": "Any update?"}
    api.fail_after_write(fail_kind)

    with pytest.raises(TimeoutError):
        await conn.write(kind, target, payload, idem_key, agent_name="IT helper")

    found = await conn.find_write(kind, target, idem_key)
    assert found is not None
    if kind == "create_item":
        pages = api.all_pages(DB_ID)
        assert len(pages) == 1
        assert found.page_id == pages[0]["id"]
    else:
        comments = api.comments[normalize_id(target)]
        assert len(comments) == 1
        assert found == comments[0]["id"]


def api_error(code: str, status: int, retry_after: str | None = None) -> APIResponseError:
    headers = httpx.Headers({"retry-after": retry_after} if retry_after else {})
    return APIResponseError(code, status, f"notion said {code}", headers, "", None, None)


@pytest.mark.parametrize(
    ("code", "status", "header", "final", "gone", "delay"),
    [
        ("unauthorized", 401, None, True, False, None),
        ("object_not_found", 404, None, True, True, None),
        ("rate_limited", 429, "7", False, False, 7.0),
        ("conflict_error", 409, None, False, False, None),
    ],
    ids=["final", "gone", "retry-after", "transient"],
)
def test_api_errors_map_through_the_registered_error_classes(
    connector, code, status, header, final, gone, delay
):
    """Fails if the connector's error classes misjudge final, gone, retry-after or transient errors."""
    conn, _ = connector
    err = api_error(code, status, header)

    assert conn.errors.is_final(err) is final
    assert conn.errors.is_gone(err) is gone
    assert _is_client_error(err) is final
    failure = conn.errors.retry_failure(err)
    if delay is None:
        assert failure is None
    else:
        assert failure.next_retry_delay.total_seconds() == delay
