"""The README's restart demo, automated: kill the worker mid-write, start another, get one item.

This test starts its own Temporal server and workers (a worker restart cannot share the
harness's), and it needs the time-skipping server.
"""

import uuid
from datetime import UTC, datetime, timedelta

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_store import seeded_agent_store
from agentos.audit import InMemoryAuditStore
from agentos.config import TASK_QUEUE, Settings, parse_agents_config
from agentos.llm import ScriptedLLMClient
from agentos.models import ApprovalDecision, TaskSpec, TurnContext
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryExternalWriteStore,
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, BOB, DB_ID, TENANT, agents_dict, eventually, make_fake_notion

NONCE = "nonce-restart"


def new_worker(env, agents, stores, slack, notion) -> Worker:
    """A worker process: fresh activities over the stores that outlive it."""
    acts = AgentActivities(
        settings=Settings(),
        registry=TenantRegistry({TENANT: runtime_for(agents, slack, notion)}),
        history=InMemoryHistoryStore(),
        idem=stores["idem"],
        links=stores["links"],
        llm=ScriptedLLMClient(),
        tool_outputs=InMemoryToolOutputStore(),
        audit=stores["audit"],
        agents=seeded_agent_store(agents),
        external_writes=stores["writes"],
    )
    return Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[ConversationWorkflow, TaskWorkflow],
        activities=acts.all(),
        # No sticky queue: a task for a stopped worker must not wait for a sticky timeout.
        max_cached_workflows=0,
    )


async def test_a_worker_killed_after_notion_created_the_item_leaves_one_item_and_one_filed():
    """Fails if a restart after the Notion write returned files the item a second time."""
    agents = parse_agents_config(agents_dict())
    slack = FakeSlack(emails={ALICE: "alice@example.com", BOB: "bob@example.com"})
    notion = make_fake_notion()
    skew = {"by": timedelta(0)}  # moves the store's clock, to run out the dead worker's lease
    stores = {
        "idem": InMemoryIdempotencyStore(),
        "links": InMemoryTaskLinkStore(),
        "audit": InMemoryAuditStore(),
        "writes": InMemoryExternalWriteStore(clock=lambda: datetime.now(UTC) + skew["by"]),
    }
    ctx = TurnContext(TENANT, "it-helper", 1, ALICE, "C_GENERAL", "1700000000.000100", "channel")
    spec = TaskSpec(
        ctx,
        "create_item",
        {"title": "New monitor", "details": "Mine flickers.", "database_id": DB_ID},
        NONCE,
    )
    # Own Temporal server, not the session's: the test kills and restarts the worker and must own the server's history.
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        first = new_worker(env, agents, stores, slack, notion)
        hang = notion.hang_after_write()
        async with first:
            handle = await env.client.start_workflow(
                TaskWorkflow.run,
                spec,
                id=ids.task_id(TENANT, str(uuid.uuid4())),
                task_queue=TASK_QUEUE,
            )

            async def awaiting():
                return await handle.query(TaskWorkflow.state) == "awaiting_approval"

            await eventually(awaiting, timeout=20)
            await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))
            await hang.reached.wait()
            # The worker stops with the page created in Notion and the activity unfinished.
        assert len(notion.all_pages(DB_ID)) == 1
        skew["by"] = timedelta(minutes=5)

        async with new_worker(env, agents, stores, slack, notion):

            async def watching():
                return await handle.query(TaskWorkflow.state) == "watching"

            await env.sleep(timedelta(minutes=5))
            await eventually(watching, timeout=30)

        assert len(notion.all_pages(DB_ID)) == 1
        filed = [p for p in slack.posts() if p["thread_ts"] and "Filed: New monitor" in p["text"]]
        assert len(filed) == 1
    finally:
        hang.release()
        await env.shutdown()
