"""CreatorAccessWorkflow: one tenant's nightly check that its creators still see their grants.

Started nightly by a Temporal schedule (see ``agentos.creator_access``), under the workflow ID
``<tenant>:creator-access``. The workflow lists the tenant's published agents, runs one
activity per agent, and ends with the combined health check. History carries the tenant ID,
the agent IDs and the pass's counts only. Workflow code is deterministic: no I/O, no clock.
"""

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agentos.creator_access import CreatorAccessActivities, CreatorAccessCounts
    from agentos.workflows import ACTIVITY_TIMEOUT, RETRY

# One agent's pass reads its picked grants — at most one picker listing (200 objects) —
# through the tenant's Notion limiter (3 requests/second): about 67 seconds of pacing
# alone, and each read is a cheap metadata GET. Two minutes covers the reads with headroom;
# a read itself is capped at REQUEST_TIMEOUT_MS (10 s), and the heartbeat cancels an attempt
# that stops making progress instead of parking the queue beside its retry.
CREATOR_ACCESS_AGENT_TIMEOUT = timedelta(minutes=2)
# As the write activity's: one read is capped at REQUEST_TIMEOUT_MS, so a live attempt
# always heartbeats between reads well within this.
CREATOR_ACCESS_HEARTBEAT_TIMEOUT = timedelta(seconds=25)
# The combined health step re-checks every published agent's grants (schema reads, page
# retrieves), each request limited to REQUEST_TIMEOUT_MS and the tenant's limiter; five
# minutes covers a large tenant without parking the queue.
CREATOR_ACCESS_HEALTH_TIMEOUT = timedelta(minutes=5)


@workflow.defn(name="CreatorAccessWorkflow")
class CreatorAccessWorkflow:
    @workflow.run
    async def run(self, tenant_id: str) -> CreatorAccessCounts:
        agent_ids = await workflow.execute_activity_method(
            CreatorAccessActivities.creator_access_agents,
            tenant_id,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        total = CreatorAccessCounts()
        for agent_id in agent_ids:
            counts = await workflow.execute_activity_method(
                CreatorAccessActivities.check_creator_access_agent,
                args=[tenant_id, agent_id],
                start_to_close_timeout=CREATOR_ACCESS_AGENT_TIMEOUT,
                heartbeat_timeout=CREATOR_ACCESS_HEARTBEAT_TIMEOUT,
                retry_policy=RETRY,
            )
            total.add(counts)
        await workflow.execute_activity_method(
            CreatorAccessActivities.creator_access_health,
            tenant_id,
            start_to_close_timeout=CREATOR_ACCESS_HEALTH_TIMEOUT,
            retry_policy=RETRY,
        )
        return total
