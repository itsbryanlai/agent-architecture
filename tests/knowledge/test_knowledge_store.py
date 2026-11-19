"""Knowledge grants, ids, the store and the embedder."""

import pytest

from agentos import ids
from agentos.config import (
    AgentsConfig,
    ConfigError,
    parse_agents_config,
)
from agentos.connectors.notion import check_page_grant
from agentos.grant_checker import NotionGrantChecker
from agentos.knowledge.embedder import BATCH_SIZE, OpenAICompatibleEmbedder
from tests.conftest import TENANT, agents_dict, make_fake_notion

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


async def test_grant_check_flags_trashed_page():
    """Fails if a trashed granted page passes the per-agent check the worker's health runs."""
    api = make_fake_notion()
    api.add_free_page(ROOT, "IT wiki")
    cfg = with_pages([{"id": ROOT, "name": "IT wiki"}])
    agent = cfg.agents["it-helper"]
    grant = agent.grants.notion_pages[0]
    checker = NotionGrantChecker(lambda _t: api)

    assert await check_page_grant(api, grant) == []
    assert all(report.ok for report in await checker.check(TENANT, agent))

    api.add_free_page(ROOT, "IT wiki", in_trash=True)
    problems = await check_page_grant(api, grant)
    assert len(problems) == 1 and "IT wiki" in problems[0]
    reports = await checker.check(TENANT, agent)
    assert [p for r in reports for p in r.problems if "IT wiki" in p]
