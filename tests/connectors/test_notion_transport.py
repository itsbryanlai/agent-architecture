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
