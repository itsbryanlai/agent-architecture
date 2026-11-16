"""RetentionWorkflow: one tenant, a batch per activity call, until nothing more is due."""

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.retention import InMemoryRetentionStore, PurgeCounts, RetentionActivities
from agentos.workflows.retention import RetentionWorkflow


async def test_batches_repeat_until_one_comes_back_not_full():
    """Fails if a night's purge stops after the first full batch, or never stops."""
    store = InMemoryRetentionStore(
        [
            PurgeCounts(idempotency=500, more=True),
            PurgeCounts(idempotency=500, sessions=3, more=True),
            PurgeCounts(idempotency=7),
            PurgeCounts(idempotency=999),  # never reached
        ]
    )
    async with (
        await WorkflowEnvironment.start_time_skipping() as env,
        Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[RetentionWorkflow],
            activities=RetentionActivities(store).all(),
        ),
    ):
        total = await env.client.execute_workflow(
            RetentionWorkflow.run,
            "acme",
            id=ids.retention_workflow_id("acme"),
            task_queue=TASK_QUEUE,
        )

    assert (total.idempotency, total.sessions) == (1007, 3)
    assert store.calls == ["acme"] * 3
