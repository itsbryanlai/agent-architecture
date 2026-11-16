"""RetentionWorkflow: one tenant, a batch per activity call, until nothing more is due."""

from temporalio.worker import Worker

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.retention import InMemoryRetentionStore, PurgeCounts, RetentionActivities
from agentos.workflows.retention import RetentionWorkflow


async def test_batches_repeat_until_one_comes_back_not_full(temporal_scope):
    """Fails if a night's purge stops after the first full batch, or never stops."""
    store = InMemoryRetentionStore(
        [
            PurgeCounts(idempotency=500, more=True),
            PurgeCounts(idempotency=500, sessions=3, more=True),
            PurgeCounts(idempotency=7),
            PurgeCounts(idempotency=999),  # never reached
        ]
    )
    async with Worker(
        temporal_scope.client,
        task_queue=temporal_scope.task_queue,
        workflows=[RetentionWorkflow],
        activities=RetentionActivities(store).all(),
    ):
        total = await temporal_scope.client.execute_workflow(
            RetentionWorkflow.run,
            "acme",
            id=ids.retention_workflow_id("acme"),
            task_queue=TASK_QUEUE,
        )

    assert (total.idempotency, total.sessions) == (1007, 3)
    assert store.calls == ["acme"] * 3
