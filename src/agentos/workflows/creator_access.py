"""CreatorAccessWorkflow: one tenant's nightly check that its creators still see their grants.

Started nightly by a Temporal schedule (see ``agentos.creator_access``), under the workflow ID
``<tenant>:creator-access``. History carries the tenant ID and the pass's counts only. Workflow
code is deterministic: no I/O, no clock.
"""

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agentos.creator_access import CreatorAccessActivities, CreatorAccessCounts
    from agentos.workflows import RETRY

# One pass reads every picked grant of every published agent, then re-checks the grants
# themselves; each request is limited to REQUEST_TIMEOUT_MS and the tenant's Notion
# limiter, so five minutes covers a large tenant without parking the queue.
CREATOR_ACCESS_TIMEOUT = timedelta(minutes=5)


@workflow.defn(name="CreatorAccessWorkflow")
class CreatorAccessWorkflow:
    @workflow.run
    async def run(self, tenant_id: str) -> CreatorAccessCounts:
        return await workflow.execute_activity_method(
            CreatorAccessActivities.check_creator_access,
            tenant_id,
            start_to_close_timeout=CREATOR_ACCESS_TIMEOUT,
            retry_policy=RETRY,
        )
