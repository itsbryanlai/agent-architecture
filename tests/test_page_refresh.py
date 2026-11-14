"""Signed Notion page events refresh single pages in the knowledge index."""

import base64
import hashlib
import hmac
import json

import pytest
from fastapi.testclient import TestClient
from slack_sdk.signature import SignatureVerifier

from agentos import ids
from agentos.agent_store import seeded_agent_store
from agentos.config import TASK_QUEUE, Settings, TenantsConfig, parse_agents_config
from agentos.gateway.app import create_app
from agentos.knowledge.embedder import FakeEmbedder
from agentos.stores import InMemoryTaskLinkStore
from agentos.workflows.page_refresh import PageRefreshWorkflow
from tests.conftest import (
    PAGE_A1,
    PAGE_A2,
    ROOT_A,
    ROOT_B,
    TENANT,
    agents_dict,
    knowledge_agents_dict,
)
from tests.conftest import text_block as tb
from tests.test_gateway import FakeTemporalClient
from tests.test_indexing import SCOPE_A, run_index

TOKEN = "refresh-test-token"


@pytest.fixture(autouse=True)
def webhook_secret(monkeypatch):
    """The gateway reads the tenant's webhook secret from its named variable."""
    monkeypatch.setenv("NOTION_WEBHOOK_VERIFICATION_TOKEN", TOKEN)


NEW_PAGE = "aaaaaaaa-0000-0000-0000-000000000099"
PLAIN = lambda page_id: page_id.replace("-", "")


def signed_post(client: TestClient, event_type: str, page_id: str):
    payload = {"id": "evt", "type": event_type, "entity": {"id": page_id, "type": "page"}}
    body = json.dumps(payload).encode()
    signature = "sha256=" + hmac.new(TOKEN.encode(), body, hashlib.sha256).hexdigest()
    return client.post(
        "/notion/webhook",
        content=body,
        headers={"Content-Type": "application/json", "X-Notion-Signature": signature},
    )


def _payload_data(node):
    """Every base64 payload ``data`` value found anywhere in a history."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "data" and isinstance(value, str):
                yield value
            else:
                yield from _payload_data(value)
    elif isinstance(node, list):
        for item in node:
            yield from _payload_data(item)


def gateway(raw_agents: dict, temporal: FakeTemporalClient, links=None) -> TestClient:
    settings = Settings()
    app = create_app(
        settings,
        TenantsConfig.of(parse_agents_config(raw_agents)),
        temporal,
        links or InMemoryTaskLinkStore(),
        SignatureVerifier("x"),
        agents=seeded_agent_store(parse_agents_config(raw_agents)),
    )
    return TestClient(app)


async def refresh(h, page_id: str, signals: int = 1) -> int:
    """Run one PageRefreshWorkflow, signalled ``signals`` times, to completion."""
    handle = await h.client.start_workflow(
        PageRefreshWorkflow.run,
        args=[TENANT, PLAIN(page_id)],
        id=ids.page_refresh_id(TENANT, page_id),
        task_queue=TASK_QUEUE,
        start_signal="changed",
    )
    for _ in range(signals - 1):
        await handle.signal(PageRefreshWorkflow.changed)
    return await handle.result()


def test_signed_page_event_starts_refresh_workflow():
    temporal = FakeTemporalClient()
    resp = signed_post(gateway(knowledge_agents_dict(), temporal), "page.content_updated", PAGE_A1)
    assert resp.status_code == 200
    assert len(temporal.signal_with_starts) == 1
    started = temporal.signal_with_starts[0]
    assert started["workflow"] == "PageRefreshWorkflow"
    assert started["id"] == ids.page_refresh_id(TENANT, PAGE_A1)
    assert started["id"] == f"{TENANT}:page-refresh:{PLAIN(PAGE_A1)}"
    assert started["args"] == [TENANT, PLAIN(PAGE_A1)]
    assert started["start_signal"] == "changed"
    assert started["task_queue"] == TASK_QUEUE


def test_no_refresh_without_page_grants():
    temporal = FakeTemporalClient()
    links = InMemoryTaskLinkStore()
    links.links[(TENANT, PLAIN(PAGE_A1))] = f"{TENANT}:task:t1"
    client = gateway(agents_dict(), temporal, links)
    assert signed_post(client, "page.properties_updated", PAGE_A1).status_code == 200
    assert temporal.signal_with_starts == []
    assert [(wf, name) for wf, name, _ in temporal.signals] == [
        (f"{TENANT}:task:t1", "external_update")
    ]


def test_unsigned_page_event_starts_nothing():
    temporal = FakeTemporalClient()
    client = gateway(knowledge_agents_dict(), temporal)
    body = json.dumps(
        {"type": "page.content_updated", "entity": {"id": PAGE_A1, "type": "page"}}
    ).encode()
    resp = client.post("/notion/webhook", content=body)
    assert resp.status_code == 401
    assert temporal.signal_with_starts == []


async def test_burst_of_events_runs_one_refresh(kharness):
    h = kharness
    await run_index(h)
    h.embedder.texts_embedded = 0
    h.notion.set_blocks(PAGE_A2, [tb("paragraph", "Burst edit about firmware.")])

    refreshes = await refresh(h, PAGE_A2, signals=3)

    assert refreshes == 1
    # One page was re-embedded once: its single chunk.
    assert h.embedder.texts_embedded == 1


async def test_refresh_reembeds_edited_page(kharness):
    h = kharness
    await run_index(h)
    before = h.embedder.texts_embedded
    h.notion.set_blocks(PAGE_A2, [tb("paragraph", "Chargers must be certified.")])

    await refresh(h, PAGE_A2)

    assert h.embedder.texts_embedded > before
    texts = [t for t, _ in h.knowledge.chunks[(TENANT, SCOPE_A, PLAIN(PAGE_A2))]]
    assert any("Chargers must be certified" in t for t in texts)
    assert not any("Swollen batteries" in t for t in texts)
    query = (await FakeEmbedder().embed(["chargers certified"]))[0]
    hits = await h.knowledge.search(TENANT, [SCOPE_A], query, 1)
    assert hits[0].page_id == PLAIN(PAGE_A2)


async def test_refresh_removes_page_moved_out_of_root(kharness):
    h = kharness
    await run_index(h)
    h.notion.move_page(PAGE_A2, {"type": "workspace", "workspace": True})

    await refresh(h, PAGE_A2)

    assert (TENANT, SCOPE_A, PLAIN(PAGE_A2)) not in h.knowledge.pages
    assert (TENANT, SCOPE_A, PLAIN(PAGE_A1)) in h.knowledge.pages


async def test_refresh_removes_trashed_page(kharness):
    h = kharness
    await run_index(h)
    h.notion.trash_page(PAGE_A2)

    await refresh(h, PAGE_A2)

    assert (TENANT, SCOPE_A, PLAIN(PAGE_A2)) not in h.knowledge.pages
    assert (TENANT, SCOPE_A, PLAIN(PAGE_A2)) not in h.knowledge.chunks


async def test_refresh_indexes_new_page_under_root(kharness):
    h = kharness
    await run_index(h)
    h.notion.add_free_page(
        NEW_PAGE, "Fresh page", parent={"type": "page_id", "page_id": PLAIN(ROOT_A)}
    )
    h.notion.set_blocks(NEW_PAGE, [tb("paragraph", "Brand new guidance.")])

    await refresh(h, NEW_PAGE)

    assert (TENANT, SCOPE_A, PLAIN(NEW_PAGE)) in h.knowledge.pages
    # It is not stored under the other granted root.
    scope_b = ids.knowledge_scope(ROOT_B)
    assert (TENANT, scope_b, PLAIN(NEW_PAGE)) not in h.knowledge.pages


async def test_refresh_history_has_no_page_text(kharness):
    h = kharness
    await run_index(h)
    h.notion.set_blocks(PAGE_A2, [tb("paragraph", "Zebra-striped secret wording.")])

    await refresh(h, PAGE_A2)

    handle = h.client.get_workflow_handle(ids.page_refresh_id(TENANT, PAGE_A2))
    history = json.loads((await handle.fetch_history()).to_json())
    payloads = "\n".join(base64.b64decode(d).decode() for d in _payload_data(history))
    assert "Zebra-striped" not in payloads
    assert "Batteries" not in payloads
    assert PLAIN(PAGE_A2) in payloads
    assert SCOPE_A in payloads
