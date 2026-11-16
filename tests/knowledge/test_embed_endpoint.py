"""The configurable embedding endpoint, and how indexing treats embedder errors."""

import json

import httpx
import pytest

from agentos.agent_store import seeded_agent_store
from agentos.config import AgentsConfig, Settings, TenantsConfig, load_settings, parse_agents_config
from agentos.connectors.notion import FakeNotionAPI
from agentos.ids import knowledge_scope
from agentos.knowledge.embedder import FakeEmbedder, OpenAICompatibleEmbedder
from agentos.knowledge.indexing import FAILED, IndexActivities
from agentos.knowledge.store import InMemoryKnowledgeStore
from agentos.slack_client import FakeSlack
from agentos.tenants import TenantRegistry, runtime_for
from agentos.worker import build_embedder
from tests.conftest import agents_dict, text_block

ROOT = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def with_pages() -> AgentsConfig:
    raw = agents_dict()
    raw["agents"]["it-helper"]["grants"]["notion"]["pages"] = [{"id": ROOT, "name": "IT wiki"}]
    return parse_agents_config(raw)


async def embed_one(settings: Settings) -> httpx.Request:
    """Build the embedder from ``settings``, embed one text, and return the request it sent."""
    sent: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        body = {
            "object": "list",
            "model": "m",
            "data": [{"object": "embedding", "index": 0, "embedding": [0.0] * 1024}],
            "usage": {"prompt_tokens": 1, "total_tokens": 1},
        }
        return httpx.Response(200, json=body)

    http = httpx.AsyncClient(transport=httpx.MockTransport(answer))
    embedder = build_embedder(settings, TenantsConfig.of(with_pages()), http_client=http)
    assert isinstance(embedder, OpenAICompatibleEmbedder)
    await embedder.embed(["a page"])
    [request] = sent
    return request


@pytest.mark.parametrize(
    ("settings", "url", "model"),
    [
        (
            Settings(embed_base_url="http://localhost:11434/v1", embed_model="bge-m3"),
            "http://localhost:11434/v1/embeddings",
            "bge-m3",
        ),
        (
            Settings(qwen_api_key="k", qwen_base_url="http://qwen.test/v1"),
            "http://qwen.test/v1/embeddings",
            None,
        ),
    ],
)
async def test_the_embedder_calls_the_configured_endpoint(settings, url, model):
    """Fails if the embedder calls another endpoint than AGENTOS_EMBED_BASE_URL, or the Qwen one when that is unset."""
    request = await embed_one(settings)
    assert str(request.url) == url
    if model is not None:
        assert json.loads(request.content)["model"] == model


def test_no_embedder_is_built_without_page_grants_or_without_the_qwen_key():
    """Fails if an embedder is built with no page grants, or without the Qwen key when no endpoint is set."""
    no_pages = TenantsConfig.of(parse_agents_config(agents_dict()))
    assert build_embedder(Settings(), no_pages) is None

    with pytest.raises(RuntimeError, match="QWEN_API_KEY"):
        build_embedder(Settings(), TenantsConfig.of(with_pages()))


def test_embed_api_key_hidden_from_repr(monkeypatch):
    monkeypatch.setenv("EMBED_API_KEY", "super-secret-embed-key")
    monkeypatch.setenv("AGENTOS_EMBED_BASE_URL", "http://localhost:11434/v1")
    settings = load_settings()

    assert settings.embed_api_key == "super-secret-embed-key"
    assert "super-secret-embed-key" not in repr(settings)


class FailingEmbedder(FakeEmbedder):
    def __init__(self, status_code: int):
        super().__init__()
        self.status_code = status_code

    async def embed(self, texts):
        err = RuntimeError("embedder said no")
        err.status_code = self.status_code
        raise err


def activities_for(embedder) -> IndexActivities:
    api = FakeNotionAPI()
    api.add_page_tree(ROOT, {"title": "Guide", "blocks": [text_block("paragraph", "Some text.")]})
    config = with_pages()
    registry = TenantRegistry({config.tenant_id: runtime_for(config, FakeSlack(), api)})
    return IndexActivities(registry, InMemoryKnowledgeStore(), embedder, seeded_agent_store(config))


async def test_sync_page_client_error_marks_the_page_failed():
    """Fails if a page the embedder refuses for good raises, stopping the run, or is stored."""
    acts = activities_for(FailingEmbedder(404))

    result = await acts.sync_page(with_pages().tenant_id, knowledge_scope(ROOT), ROOT)

    assert result.status == FAILED
    assert acts.store.pages == {}


async def test_sync_page_server_error_is_retried():
    acts = activities_for(FailingEmbedder(500))
    with pytest.raises(RuntimeError, match="embedder said no"):
        await acts.sync_page(with_pages().tenant_id, knowledge_scope(ROOT), ROOT)
