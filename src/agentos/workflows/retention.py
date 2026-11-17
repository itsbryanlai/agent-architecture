"""RetentionWorkflow: purge one tenant's expired rows, a batch per activity call.

Started nightly by a Temporal schedule (see ``agentos.retention``), under the workflow ID
``<tenant>:retention``. History carries the tenant ID and row counts only. Workflow code is
deterministic: no I/O, no clock.
"""

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agentos.retention import MAX_ROUNDS, PurgeCounts, RetentionActivities
    from agentos.workflows import ACTIVITY_TIMEOUT, RETRY


@workflow.defn(name="RetentionWorkflow")
class RetentionWorkflow:
    @workflow.run
    async def run(self, tenant_id: str) -> PurgeCounts:
        total = PurgeCounts()
        for _ in range(MAX_ROUNDS):
            counts = await workflow.execute_activity_method(
                RetentionActivities.purge_retention_batch,
                tenant_id,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
            total.add(counts)
            if not counts.more:
                break
        total.more = False
        return total
