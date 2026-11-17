"""Slack 429s: Retry-After reaches Temporal, and a long workspace read waits out limits."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from aiohttp import web
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from agentos.activities import AgentActivities
from agentos.agent_store import seeded_agent_store
from agentos.audit import InMemoryAuditStore
from agentos.config import Settings, parse_agents_config
from agentos.llm import ScriptedLLMClient
from agentos.models import TurnContext
from agentos.slack_client import (
    FakeSlack,
    RealSlackPoster,
    SlackHistoryUnavailable,
    SlackLookupError,
    post_metadata,
)
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for
from tests.conftest import ALICE, TENANT, make_fake_notion
from tests.helpers.audience import THREAD, tenant_dict


class FakeSlackServer:
    """A local Slack Web API that answers each request from a scripted queue.

    An item is ``(status, retry_after, body)``; the last item repeats.
    """

    def __init__(self, script: list[tuple[int, str | None, dict]]):
        self.script = list(script)
        self.requests: list[str] = []
        self.sent: list[dict] = []  # what each request carried, in the body or the query

    async def handle(self, request: web.Request) -> web.Response:
        self.requests.append(request.match_info["method"])
        body = await request.json() if request.content_type == "application/json" else {}
        self.sent.append({**dict(await request.post()), **body, **request.query})
        status, retry_after, body = self.script[0] if len(self.script) == 1 else self.script.pop(0)
        headers = {"Retry-After": retry_after} if retry_after is not None else {}
        return web.json_response(body, status=status, headers=headers)


@pytest.fixture
async def serve():
    runners: list[web.AppRunner] = []

    async def start(script) -> tuple[FakeSlackServer, str]:
        server = FakeSlackServer(script)
        app = web.Application()
        app.router.add_route("*", "/api/{method}", server.handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        runners.append(runner)
        port = site._server.sockets[0].getsockname()[1]
        return server, f"http://127.0.0.1:{port}/api/"

    yield start
    for runner in runners:
        await runner.cleanup()


@pytest.fixture
def no_waiting(monkeypatch):
    """Rate-limit waits are recorded, not slept."""
    waits: list[float] = []
    real_sleep = asyncio.sleep

    async def record(seconds, *args):
        waits.append(seconds)
        await real_sleep(0)

    monkeypatch.setattr("slack_sdk.http_retry.builtin_async_handlers.asyncio.sleep", record)
    return waits


RATE_LIMITED = (429, "7", {"ok": False, "error": "ratelimited"})


async def test_lookup_429_carries_retry_after_and_is_not_retried_in_process(serve, no_waiting):
    """Fails if a single lookup sleeps through a 429 itself or loses Slack's Retry-After."""
    server, url = await serve([RATE_LIMITED])
    poster = RealSlackPoster("xoxb-test", TENANT, base_url=url)

    with pytest.raises(SlackLookupError) as err:
        await poster.user_info("U1")

    assert (err.value.transient, err.value.retry_after) == (True, 7.0)
    assert server.requests == ["users.info"]
    assert no_waiting == []


async def test_activity_hands_retry_after_to_temporal():
    """Fails if a rate-limited audience check retries on Temporal's backoff, not Slack's Retry-After."""
    config = parse_agents_config(tenant_dict({"dm": "everyone"}))
    slack = FakeSlack()
    acts = AgentActivities(
        settings=Settings(),
        registry=TenantRegistry({TENANT: runtime_for(config, slack, make_fake_notion())}),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=ScriptedLLMClient(),
        tool_outputs=InMemoryToolOutputStore(),
        audit=InMemoryAuditStore(),
        agents=seeded_agent_store(config),
    )
    slack.fail_lookups(SlackLookupError("ratelimited", transient=True, retry_after=7.0))

    with pytest.raises(ApplicationError) as failed:
        await ActivityEnvironment().run(
            acts.check_audience, TurnContext(TENANT, "it-helper", 1, ALICE, "D1", THREAD, "im")
        )

    assert failed.value.next_retry_delay == timedelta(seconds=7)
    assert not failed.value.non_retryable


async def test_workspace_read_waits_out_rate_limits_and_finishes(serve, no_waiting):
    """Fails if a 429 on a middle page of the workspace read fails the whole read."""

    def page(ids: list[str], cursor: str) -> dict:
        return {
            "ok": True,
            "members": [{"id": i, "team_id": "T1"} for i in ids],
            "response_metadata": {"next_cursor": cursor},
        }

    _, url = await serve(
        [
            (200, None, page(["U1", "U2"], "c2")),
            RATE_LIMITED,
            RATE_LIMITED,
            (200, None, page(["U3"], "")),
        ]
    )
    poster = RealSlackPoster("xoxb-test", TENANT, base_url=url)

    users = await poster.list_users()

    assert [u.id for u in users] == ["U1", "U2", "U3"]
    assert len(no_waiting) == 2 and all(7 <= w < 8 for w in no_waiting)


async def test_post_waits_out_one_rate_limit(serve, no_waiting):
    """Fails if an interactive post gives up on its first 429."""
    server, url = await serve([RATE_LIMITED, (200, None, {"ok": True, "ts": "1.2"})])
    poster = RealSlackPoster("xoxb-test", TENANT, base_url=url)

    assert await poster.post_message("C1", "hi") == "1.2"
    assert server.requests == ["chat.postMessage", "chat.postMessage"]


@pytest.mark.parametrize("thread_ts", ["1.1", None], ids=["thread post", "top-level post"])
async def test_a_post_is_marked_and_looked_up_with_metadata_over_the_wire(serve, thread_ts):
    """Fails if the client stops sending the marker with a post, or reads history without asking for metadata."""
    marker = post_metadata("m1")
    found = {"ts": "5.5", "metadata": marker}
    server, url = await serve(
        [
            (200, None, {"ok": True, "ts": "5.5"}),
            (200, None, {"ok": True, "messages": [{"ts": "4.4"}, found]}),
        ]
    )
    poster = RealSlackPoster("xoxb-test", TENANT, base_url=url)

    await poster.post_message("C1", "hi", thread_ts=thread_ts, metadata=marker)
    ts = await poster.find_post("C1", thread_ts, "m1", since=1700000000.0)

    assert server.sent[0]["metadata"] == marker
    assert server.requests[1] == ("conversations.replies" if thread_ts else "conversations.history")
    assert str(server.sent[1]["include_all_metadata"]).lower() in ("1", "true")
    assert ts == "5.5"


async def test_a_history_read_refused_for_a_missing_scope_names_the_scope(serve):
    """Fails if Slack's missing_scope answer to a history read is not reported with the scope it names."""
    _, url = await serve(
        [(200, None, {"ok": False, "error": "missing_scope", "needed": "channels:history"})]
    )
    poster = RealSlackPoster("xoxb-test", TENANT, base_url=url)

    with pytest.raises(SlackHistoryUnavailable) as refused:
        await poster.find_post("C1", None, "m1", since=1700000000.0)

    assert (refused.value.code, refused.value.scope) == ("missing_scope", "channels:history")
