"""Knowledge grants, ids, the store and the embedder."""

import logging

import pytest

from agentos import ids
from agentos.config import AgentsConfig, ConfigError, Settings, parse_agents_config
from agentos.connectors.notion import check_page_grant
from agentos.knowledge.embedder import BATCH_SIZE, FakeEmbedder, OpenAICompatibleEmbedder
from agentos.knowledge.store import InMemoryKnowledgeStore
from agentos.worker import build_embedder, check_grants_at_startup
from tests.conftest import agents_dict, make_fake_notion

ROOT = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
ROOT_PLAIN = "aaaaaaaabbbbccccddddeeeeeeeeeeee"


def with_pages(pages) -> AgentsConfig:
    raw = agents_dict()
    raw["agents"]["it-helper"]["grants"]["notion"]["pages"] = pages
    return parse_agents_config(raw)


def test_page_grant_parses():
    cfg = with_pages([{"id": ROOT, "name": "IT wiki"}])
    grant = cfg.agents["it-helper"].grants.notion_pages[0]
    assert (grant.id, grant.name) == (ROOT, "IT wiki")
    assert len(cfg.agents["it-helper"].grants.notion_databases) == 1

    with pytest.raises(ConfigError, match="it-helper"):
        with_pages([{"id": ROOT}])


def test_knowledge_scope_and_ids():
    assert ids.knowledge_scope(ROOT) == ids.knowledge_scope(ROOT_PLAIN.upper())
    assert ids.knowledge_scope(ROOT) == f"notion:page:{ROOT_PLAIN}"
    scope = ids.knowledge_scope(ROOT)
    assert ids.index_workflow_id("acme", scope).startswith("acme:")
    assert ids.index_schedule_id("acme", scope).startswith("acme:")
    assert ids.index_workflow_id("acme", scope) != ids.index_schedule_id("acme", scope)


async def seeded_store():
    emb = FakeEmbedder()
    store = InMemoryKnowledgeStore()
    rows = [
        ("acme", "s1", "p1", "reset vpn password"),
        ("acme", "s2", "p2", "reset vpn password"),
        ("other", "s1", "p3", "reset vpn password"),
    ]
    for tenant, scope, page, text in rows:
        [vec] = await emb.embed([text])
        await store.replace_page(tenant, scope, page, f"T {page}", f"u/{page}", "h", [(text, vec)])
    return emb, store


async def test_search_filters_by_scope_and_tenant():
    emb, store = await seeded_store()
    [q] = await emb.embed(["reset vpn password"])

    hits = await store.search("acme", ["s1"], q, 10)

    assert [(h.scope, h.page_id) for h in hits] == [("s1", "p1")]
    both = await store.search("acme", ["s1", "s2"], q, 10)
    assert {h.page_id for h in both} == {"p1", "p2"}
    assert await store.page_meta("acme", ["s1"], ["p1", "p2", "p3"]) == {"p1": ("T p1", "u/p1")}


async def test_search_empty_scopes_returns_nothing():
    emb, store = await seeded_store()
    [q] = await emb.embed(["reset vpn password"])
    assert await store.search("acme", [], q, 10) == []


async def test_replace_page_replaces_chunks():
    emb = FakeEmbedder()
    store = InMemoryKnowledgeStore()
    vecs = await emb.embed(["one", "two", "three"])
    await store.replace_page(
        "acme", "s", "p", "T", "u", "h1", list(zip(["one", "two", "three"], vecs))
    )
    await store.replace_page("acme", "s", "p", "T", "u", "h2", [("one", vecs[0])])

    hits = await store.search("acme", ["s"], vecs[0], 10)
    assert [h.text for h in hits] == ["one"]
    assert await store.page_hashes("acme", "s") == {"p": "h2"}
    assert await store.delete_pages("acme", "s", ["p", "missing"]) == 1
    assert await store.search("acme", ["s"], vecs[0], 10) == []


async def test_fake_embedder_ranks_shared_words_closer():
    emb = FakeEmbedder()
    q, close, far = await emb.embed(
        ["reset VPN password", "how to reset your VPN password", "parental leave policy"]
    )

    def dot(a, b):
        return sum(x * y for x, y in zip(a, b))

    assert len(q) == 1024
    assert dot(q, close) > dot(q, far)


class FakeEmbeddingsAPI:
    def __init__(self, bad_dimensions: bool = False):
        self.calls: list[list[str]] = []
        self.bad = bad_dimensions

    async def create(self, *, model, input, dimensions, encoding_format):
        self.calls.append(list(input))
        assert encoding_format == "float"
        size = dimensions - 1 if self.bad else dimensions
        data = [type("D", (), {"index": i, "embedding": [0.1] * size})() for i in range(len(input))]
        return type("R", (), {"data": data})()


class FakeOpenAI:
    def __init__(self, bad_dimensions: bool = False):
        self.embeddings = FakeEmbeddingsAPI(bad_dimensions)


async def test_qwen_embedder_batches_and_checks_dimensions():
    client = FakeOpenAI()
    emb = OpenAICompatibleEmbedder("k", "http://x", "m", 8, client=client)
    out = await emb.embed([f"t{i}" for i in range(23)])

    assert len(out) == 23
    assert [len(c) for c in client.embeddings.calls] == [10, 10, 3]
    assert max(len(c) for c in client.embeddings.calls) <= BATCH_SIZE

    bad = OpenAICompatibleEmbedder("k", "http://x", "m", 8, client=FakeOpenAI(bad_dimensions=True))
    with pytest.raises(ValueError):
        await bad.embed(["a"])


def test_build_embedder_only_when_pages_granted():
    assert build_embedder(Settings(), with_pages([])) is None

    cfg = with_pages([{"id": ROOT, "name": "IT wiki"}])
    with pytest.raises(RuntimeError, match="QWEN_API_KEY"):
        build_embedder(Settings(), cfg)
    embedder = build_embedder(
        Settings(qwen_api_key="k", qwen_base_url="http://x", embed_model="text-embedding-v4"), cfg
    )
    assert (embedder.model, embedder.dimensions) == ("text-embedding-v4", 1024)


async def test_grant_check_flags_trashed_page(caplog):
    api = make_fake_notion()
    api.add_free_page(ROOT, "IT wiki")
    cfg = with_pages([{"id": ROOT, "name": "IT wiki"}])
    grant = cfg.agents["it-helper"].grants.notion_pages[0]

    assert await check_page_grant(api, grant) == []
    assert await check_grants_at_startup(api, cfg, logging.getLogger("t")) is None

    api.add_free_page(ROOT, "IT wiki", in_trash=True)
    problems = await check_page_grant(api, grant)
    assert len(problems) == 1 and "IT wiki" in problems[0]
    assert await check_grants_at_startup(api, cfg, logging.getLogger("t"))
