"""A test-run harness: fakes, indexed pages, a draft with cases, and the run workflow.

The run workflow polls the test's own queue here (the scope redirects the main queue's
name); production polls ``TEST_TASK_QUEUE``, which changes no workflow behaviour.
"""

from __future__ import annotations

import copy
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from temporalio.client import WorkflowHandle
from temporalio.worker import Worker

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_store import InMemoryAgentStore, seeded_agent_store
from agentos.audit import InMemoryAuditStore, canonical_sha256
from agentos.budgets import BudgetGuard, InMemoryBudgetStore
from agentos.config import DEFAULT_MODEL, TASK_QUEUE, Settings, parse_agents_config
from agentos.knowledge.embedder import FakeEmbedder
from agentos.knowledge.indexing import IndexActivities
from agentos.knowledge.store import InMemoryKnowledgeStore
from agentos.llm import ScriptedLLMClient
from agentos.models import LLMResponse, ToolCall, Usage
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.switches import InMemorySwitchStore
from agentos.tenants import TenantRegistry, runtime_for
from agentos.test_sets import RUNNING, InMemoryTestSetStore, TestCase, TestRun, case_set_sha256
from agentos.usage import InMemoryUsageStore, PriceEntry, Prices, UsageRecorder
from agentos.workflows.indexing import IndexingWorkflow
from agentos.workflows.test_run import TestRunWorkflow
from tests.conftest import (
    ALICE,
    ROOT_A,
    ROOT_B,
    TENANT,
    a_tree,
    b_tree,
    knowledge_agents_dict,
    make_fake_notion,
)
from tests.helpers.temporal import TemporalScope

# The cap test prices its own model: $1 per million input tokens, so a scripted
# call's token count is its cost in microdollars. The shipped default model is priced
# at zero, so a response that reports no model of its own is priced, not unknown.
PRICE_MODEL = "run-model"


def run_prices() -> Prices:
    return Prices(
        [
            PriceEntry(
                PRICE_MODEL, date(2026, 1, 1), Decimal(1), Decimal(0), Decimal(0), Decimal(0)
            ),
            PriceEntry(
                DEFAULT_MODEL, date(2026, 1, 1), Decimal(0), Decimal(0), Decimal(0), Decimal(0)
            ),
        ]
    )


def answer(text: str, usage: Usage | None = None) -> LLMResponse:
    return LLMResponse(text=text, tool_calls=[], stop_reason="end_turn", usage=usage or Usage())


def refusal(usage: Usage | None = None) -> LLMResponse:
    return LLMResponse(
        text="I cannot help with that.",
        tool_calls=[],
        stop_reason="refusal",
        usage=usage or Usage(),
    )


def tool_turn(*calls: ToolCall) -> LLMResponse:
    return LLMResponse(text=None, tool_calls=list(calls), stop_reason="tool_use")


def case(
    question: str, expect: str, target: str = "", agent: str = "it-helper", version: int = 2
) -> TestCase:
    return TestCase(
        TENANT,
        agent,
        version,
        ids.test_case_id(TENANT, uuid.uuid4().hex),
        question,
        expect,
        target,
        created_by=ALICE,
    )


@dataclass
class RunHarness:
    wf_env: TemporalScope
    llm: ScriptedLLMClient
    slack: FakeSlack
    notion: object
    acts: AgentActivities
    agents: InMemoryAgentStore
    sets: InMemoryTestSetStore
    usage: InMemoryUsageStore

    @property
    def client(self):
        return self.wf_env.client


@asynccontextmanager
async def run_harness(wf_env: TemporalScope) -> AsyncIterator[RunHarness]:
    cfg = parse_agents_config(knowledge_agents_dict())
    notion = make_fake_notion()
    notion.add_page_tree(ROOT_A, a_tree())
    notion.add_page_tree(ROOT_B, b_tree())
    llm = ScriptedLLMClient()
    slack = FakeSlack(emails={ALICE: "alice@example.com"})
    agents = seeded_agent_store(cfg)
    sets = InMemoryTestSetStore()
    usage = InMemoryUsageStore()
    budgets = InMemoryBudgetStore()
    knowledge, embedder = InMemoryKnowledgeStore(), FakeEmbedder()
    switches = InMemorySwitchStore()
    guard = BudgetGuard(budgets, usage, ttl_seconds=0)
    acts = AgentActivities(
        settings=Settings(),
        registry=TenantRegistry({TENANT: runtime_for(cfg, slack, notion)}),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=llm,
        tool_outputs=InMemoryToolOutputStore(),
        audit=InMemoryAuditStore(),
        agents=agents,
        knowledge=knowledge,
        embedder=embedder,
        switches=switches,
        usage=UsageRecorder(usage, run_prices(), listener=guard),
        budgets=guard,
        test_sets=sets,
    )
    index = IndexActivities(
        acts.registry,
        knowledge,
        embedder,
        agents,
        switches=switches,
        budgets=guard,
    )
    worker = Worker(
        wf_env.client,
        task_queue=wf_env.task_queue,
        workflows=[TestRunWorkflow, IndexingWorkflow],
        activities=acts.all() + index.all(),
        interceptors=[wf_env.children],
    )
    async with worker:
        for root in (ROOT_A, ROOT_B):
            scope = ids.knowledge_scope(root)
            await wf_env.client.execute_workflow(
                IndexingWorkflow.run,
                args=[TENANT, scope, root.replace("-", "")],
                id=ids.index_workflow_id(TENANT, scope),
                task_queue=TASK_QUEUE,
            )
        yield RunHarness(wf_env, llm, slack, notion, acts, agents, sets, usage)


async def make_draft(h: RunHarness, agent: str = "it-helper", change=None) -> int:
    """A new draft of the published agent, with ``change`` applied to its definition."""
    draft = await h.agents.create_draft(TENANT, agent, ALICE)
    if change is not None:
        definition = copy.deepcopy(draft.definition)
        change(definition)
        await h.agents.replace_draft(TENANT, agent, draft.version, definition)
    return draft.version


async def start_run(
    h: RunHarness,
    cases: list[TestCase],
    agent: str = "it-helper",
    version: int = 2,
    started_by: str = ALICE,
) -> tuple[TestRun, WorkflowHandle]:
    """Store the cases and the run's row and start the workflow; return both, not waiting.

    A test that changes the world mid-run (an uninstall) holds the handle; one that
    only wants the verdict awaits it, as ``run_cases`` does.
    """
    for c in cases:
        await h.sets.add_case(TENANT, c)
    record = await h.agents.get_version(TENANT, agent, version)
    run = TestRun(
        TENANT,
        ids.test_run_id(TENANT, uuid.uuid4().hex),
        agent,
        version,
        RUNNING,
        started_by,
        canonical_sha256(record.definition),
        case_set_sha256(await h.sets.cases(TENANT, agent, version)),
    )
    await h.sets.start_run(TENANT, run)
    handle = await h.client.start_workflow(
        TestRunWorkflow.run,
        args=[TENANT, run.run_id],
        id=run.run_id,
        task_queue=TASK_QUEUE,
    )
    return run, handle


async def run_cases(
    h: RunHarness,
    cases: list[TestCase],
    agent: str = "it-helper",
    version: int = 2,
    started_by: str = ALICE,
) -> TestRun:
    """Store the cases, start the run workflow and wait for it; return the finished row."""
    run, handle = await start_run(h, cases, agent, version, started_by)
    await handle.result()
    return await h.sets.get_run(TENANT, run.run_id)
