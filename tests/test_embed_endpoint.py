"""The configurable embedding endpoint, and how indexing treats embedder errors."""

import pytest
from temporalio.exceptions import ApplicationError

from agentos.config import AgentsConfig, Settings, TenantsConfig, load_settings, parse_agents_config
from agentos.connectors.notion import FakeNotionAPI
from agentos.knowledge.embedder import FakeEmbedder, OpenAICompatibleEmbedder
from agentos.knowledge.indexing import IndexActivities
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


def test_embedder_uses_own_endpoint_when_set():
    settings = Settings(embed_base_url="http://localhost:11434/v1", embed_model="bge-m3")
    embedder = build_embedder(settings, TenantsConfig.of(with_pages()))

    assert isinstance(embedder, OpenAICompatibleEmbedder)
    assert str(embedder._client.base_url).startswith("http://localhost:11434/v1")
    assert (embedder.model, embedder.dimensions) == ("bge-m3", 1024)


def test_embedder_falls_back_to_qwen_settings():
    cfg = TenantsConfig.of(with_pages())
    with pytest.raises(RuntimeError, match="QWEN_API_KEY"):
        build_embedder(Settings(), cfg)

    embedder = build_embedder(Settings(qwen_api_key="k", qwen_base_url="http://qwen.test/v1"), cfg)
    assert str(embedder._client.base_url).startswith("http://qwen.test/v1")


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
    raw = agents_dict()
    raw["tenant_id"] = "t1"
    config = parse_agents_config(raw)
    registry = TenantRegistry({"t1": runtime_for(config, FakeSlack(), api)})
    return IndexActivities(registry, InMemoryKnowledgeStore(), embedder)


async def test_sync_page_client_error_is_non_retryable():
    acts = activities_for(FailingEmbedder(404))
    with pytest.raises(ApplicationError) as caught:
        await acts.sync_page("t1", "notion:page:x", ROOT)
    assert caught.value.non_retryable is True


async def test_sync_page_server_error_is_retried():
    acts = activities_for(FailingEmbedder(500))
    with pytest.raises(RuntimeError, match="embedder said no"):
        await acts.sync_page("t1", "notion:page:x", ROOT)
