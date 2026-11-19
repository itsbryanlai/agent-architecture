"""RealNotionAPI over a mocked HTTP transport, with the Notion SDK's own error types."""

import json

import httpx
import pytest
from notion_client.errors import APIResponseError, RequestTimeoutError

from agentos.connectors.notion import RealNotionAPI

DB = "11111111-2222-3333-4444-555555555555"
PAGE = {"object": "page", "id": "page-1", "url": "https://www.notion.so/page1", "properties": {}}


def api_with(handler):
    """A client whose only network is ``handler``; ``requests`` lists what it was sent."""
    requests: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.startswith("/v1/databases/"):
            return httpx.Response(200, json={"data_sources": [{"id": "ds-1"}]})
        return handler(request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(record))
    return RealNotionAPI("test-token", http_client=http), requests


def page_posts(requests: list[httpx.Request]) -> list[httpx.Request]:
    return [r for r in requests if r.method == "POST" and r.url.path == "/v1/pages"]


def error_body(code: str, message: str) -> dict:
    return {"object": "error", "code": code, "message": message}


def slow(request: httpx.Request) -> httpx.Response:
    raise httpx.ReadTimeout("no answer", request=request)


@pytest.mark.parametrize(
    ("reply", "expected_error", "status"),
    [
        (
            httpx.Response(
                429, headers={"retry-after": "0"}, json=error_body("rate_limited", "slow down")
            ),
            APIResponseError,
            429,
        ),
        (
            httpx.Response(409, json=error_body("conflict_error", "try again")),
            APIResponseError,
            409,
        ),
        (slow, RequestTimeoutError, None),
    ],
)
async def test_a_failed_create_is_sent_once_and_the_sdks_own_error_comes_back(
    reply, expected_error, status
):
    """Fails if the SDK retries a page create on its own, so Temporal is not the only retrier."""
    handler = reply if callable(reply) else (lambda request: reply)
    api, requests = api_with(handler)

    with pytest.raises(expected_error) as err:
        await api.create_page(DB, {}, [])

    assert len(page_posts(requests)) == 1
    assert getattr(err.value, "status", None) == status


async def test_a_successful_create_returns_the_page_and_sends_the_body():
    """Fails if the page body or the data source parent is not what the connector passed."""
    api, requests = api_with(lambda request: httpx.Response(200, json=PAGE))
    children = [{"object": "block", "type": "paragraph", "paragraph": {"rich_text": []}}]

    page = await api.create_page(DB, {"title": {"title": []}}, children)

    assert page["id"] == "page-1"
    sent = json.loads(page_posts(requests)[0].content)
    assert sent["parent"] == {"type": "data_source_id", "data_source_id": "ds-1"}
    assert sent["children"] == children


@pytest.mark.parametrize(
    ("reply", "found"),
    [
        (
            httpx.Response(
                200,
                json={
                    "object": "data_source",
                    "id": "ds-1",
                    "title": [{"plain_text": "IT requests"}],
                },
            ),
            True,
        ),
        (
            httpx.Response(404, json=error_body("object_not_found", "Could not find data source")),
            False,
        ),
    ],
    ids=["success", "not shared"],
)
async def test_retrieve_data_source_reads_the_data_source_object(reply, found):
    """Fails if retrieve_data_source does not read /v1/data_sources/{id}, or a Notion refusal
    comes back as anything but the SDK's own error with its status."""
    api, requests = api_with(lambda request: reply)

    if found:
        source = await api.retrieve_data_source("ds-1")
        assert source["title"][0]["plain_text"] == "IT requests"
    else:
        with pytest.raises(APIResponseError) as err:
            await api.retrieve_data_source("ds-1")
        assert err.value.status == 404
    assert [r.url.path for r in requests] == ["/v1/data_sources/ds-1"]


@pytest.mark.parametrize(
    ("kind", "status", "code"),
    [
        ("database", 404, "object_not_found"),
        ("page", 403, "restricted_resource"),
    ],
    ids=["database-gone", "page-unshared"],
)
async def test_a_creator_read_surfaces_notions_refusal_with_its_status(kind, status, code):
    """Fails if the nightly creator read's 403/404 comes back as anything but the SDK's own
    error with its status, so the check cannot tell a lost object from a transient failure."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status, json=error_body(code, "the creator cannot see it"))

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    api = RealNotionAPI("creator-token", http_client=http)
    object_id = DB if kind == "database" else "page-1"
    read = api.retrieve_database if kind == "database" else api.retrieve_page

    with pytest.raises(APIResponseError) as err:
        await read(object_id)

    assert err.value.status == status
    assert requests[0].headers["authorization"] == "Bearer creator-token"
    assert [r.url.path for r in requests] == [
        f"/v1/databases/{DB}" if kind == "database" else "/v1/pages/page-1"
    ]
