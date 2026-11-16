"""Helpers that start a task workflow and drive it through approval."""

import uuid

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.models import ApprovalDecision, TaskSpec, TurnContext
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, DB_ID, TENANT, eventually

NONCE = "nonce-123"


def spec() -> TaskSpec:
    ctx = TurnContext(
        tenant_id=TENANT,
        agent_id="it-helper",
        agent_version=1,
        requester_user_id=ALICE,
        channel="C_GENERAL",
        thread_ts="1700000000.000100",
        channel_kind="channel",
    )
    return TaskSpec(
        ctx=ctx,
        tool_name="create_item",
        payload={"title": "New monitor", "details": "Mine flickers.", "database_id": DB_ID},
        approval_nonce=NONCE,
    )


async def start_task(h, task_spec: TaskSpec | None = None):
    handle = await h.client.start_workflow(
        TaskWorkflow.run,
        task_spec or spec(),
        id=ids.task_id(TENANT, str(uuid.uuid4())),
        task_queue=TASK_QUEUE,
    )
    await wait_state(handle, "awaiting_approval")
    return handle


async def wait_state(handle, expected: str):
    async def check():
        return await handle.query(TaskWorkflow.state) == expected

    await eventually(check, timeout=20)


def thread_posts(h, needle: str):
    return [p for p in h.slack.posts() if p["thread_ts"] and needle in p["text"]]


async def approve(h, handle):
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))
