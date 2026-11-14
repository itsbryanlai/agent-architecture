"""A revoked grant stops indexing and its stored rows are deleted."""

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError

from agentos import ids, index_knowledge
from agentos.agent_store import seeded_agent_store
from agentos.config import (
    TASK_QUEUE,
    Settings,
    TenantsConfig,
    parse_agents_config,
)
from agentos.index_knowledge import apply_schedules
from agentos.knowledge.embedder import FakeEmbedder
from agentos.knowledge.indexing import IndexActivities
from agentos.knowledge.store import InMemoryKnowledgeStore
from agentos.tenants import TenantRegistry, runtime_for
from agentos.workflows.indexing import IndexingWorkflow
from tests.conftest import ROOT_A, ROOT_B, TENANT, knowledge_agents_dict, make_fake_notion
from tests.test_knowledge_search import FakeScheduleClient

SCOPE_A = ids.knowledge_scope(ROOT_A)
SCOPE_B = ids.knowledge_scope(ROOT_B)
ROOT_A_PLAIN = ROOT_A.replace("-", "")


def config_without_grants():
    raw = knowledge_agents_dict()
    for agent in raw["agents"].values():
        agent["grants"]["notion"]["pages"] = []
    return parse_agents_config(raw)


def config_with_only_b():
    raw = knowledge_agents_dict()
    raw["agents"]["it-helper"]["grants"]["notion"]["pages"] = []
    return parse_agents_config(raw)


def activities(cfg, notion=None):
    store = InMemoryKnowledgeStore()
    notion = notion or make_fake_notion()
    registry = TenantRegistry({TENANT: runtime_for(cfg, None, notion)})
    return IndexActivities(registry, store, FakeEmbedder(), seeded_agent_store(cfg)), store, notion


async def seed(store, scope, page_id="p1"):
    await store.replace_page(TENANT, scope, page_id, "T", "u", "h", [("text", [1.0, 0.0])])


async def test_sync_page_refuses_a_scope_no_longer_granted():
    acts, store, notion = activities(config_with_only_b())
    notion.add_page_tree(ROOT_A, {"title": "IT wiki", "blocks": []})
    calls_before = len(notion.calls)

    with pytest.raises(ApplicationError) as err:
        await acts.sync_page(TENANT, SCOPE_A, ROOT_A_PLAIN)

    assert err.value.non_retryable
    assert len(notion.calls) == calls_before
    assert store.pages == {}


async def test_prune_scope_refuses_a_scope_no_longer_granted():
    acts, store, _ = activities(config_with_only_b())
    await seed(store, SCOPE_A)

    with pytest.raises(ApplicationError) as err:
        await acts.prune_scope(TENANT, SCOPE_A, [])

    assert err.value.non_retryable


async def test_leftover_schedule_run_reads_and_stores_nothing(kharness):
    h = kharness
    # The grant behind this scope was revoked: the worker's config has no such root.
    revoked = ids.knowledge_scope("c" * 32)
    calls_before = len(h.notion.calls)

    with pytest.raises(WorkflowFailureError):
        await h.client.execute_workflow(
            IndexingWorkflow.run,
            args=[TENANT, revoked, "c" * 32],
            id=ids.index_workflow_id(TENANT, revoked),
            task_queue=TASK_QUEUE,
        )

    assert len(h.notion.calls) == calls_before
    assert h.knowledge.pages == {}


async def test_store_deletes_one_scope_and_lists_scopes():
    store = InMemoryKnowledgeStore()
    await seed(store, SCOPE_A, "p1")
    await seed(store, SCOPE_A, "p2")
    await seed(store, SCOPE_B, "p3")
    await store.replace_page("globex", SCOPE_A, "p1", "T", "u", "h", [("t", [1.0])])

    assert sorted(await store.scopes(TENANT)) == [SCOPE_A, SCOPE_B]
    assert await store.delete_scope(TENANT, SCOPE_A) == 2

    assert await store.scopes(TENANT) == [SCOPE_B]
    assert ("globex", SCOPE_A, "p1") in store.pages
    assert ("globex", SCOPE_A, "p1") in store.chunks
    assert not [k for k in store.chunks if k[:2] == (TENANT, SCOPE_A)]


async def test_apply_schedules_deletes_rows_of_ungranted_scopes():
    store = InMemoryKnowledgeStore()
    await seed(store, SCOPE_A)
    await seed(store, SCOPE_B)
    stale = ids.index_schedule_id(TENANT, SCOPE_A)
    keep = ids.index_schedule_id(TENANT, SCOPE_B)
    client = FakeScheduleClient([stale, keep])

    plan = await apply_schedules(client, config_with_only_b(), store)

    assert plan.delete == [stale]
    assert await store.scopes(TENANT) == [SCOPE_B]


async def test_schedule_command_with_no_grants_left_deletes_schedules_and_rows(monkeypatch):
    cfg = config_without_grants()
    store = InMemoryKnowledgeStore()
    await seed(store, SCOPE_A)
    client = FakeScheduleClient([ids.index_schedule_id(TENANT, SCOPE_A)])

    async def build_client(_settings):
        return client

    class OpenStore:
        def __init__(self, opened):
            self.opened = opened

        async def __aenter__(self):
            return self.opened

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(index_knowledge, "load_settings", lambda: Settings())
    monkeypatch.setattr(index_knowledge, "load_tenants_config", lambda _path: TenantsConfig.of(cfg))
    monkeypatch.setattr(index_knowledge, "build_temporal_client", build_client)
    monkeypatch.setattr(index_knowledge, "open_store", lambda _settings: OpenStore(store))
    monkeypatch.setattr(
        index_knowledge,
        "open_agent_store",
        lambda _settings: OpenStore(seeded_agent_store(cfg)),
    )

    await index_knowledge.run(wait=False, schedule=True)

    assert client.schedules == {}
    assert store.pages == {}
    assert store.chunks == {}
