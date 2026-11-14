"""AudienceWatchWorkflow: act on one Slack event that may change who reads an agent's
answers in a channel.

Started by the gateway with a workflow ID built from the tenant and the Slack event ID,
and never reused, so Slack delivering the same event again starts nothing. It runs one
activity that decides and applies the pause. If that activity fails (the joiner could not
be looked up), the agents that serve the channel are paused anyway: nothing is allowed
because the answer could not be had. History carries IDs only. Workflow code is
deterministic: no I/O.
"""

from temporalio import workflow
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from agentos.audience_watch import AudienceWatchActivities
    from agentos.models import AudienceEvent
    from agentos.workflows import ACTIVITY_TIMEOUT, RETRY


@workflow.defn(name="AudienceWatchWorkflow")
class AudienceWatchWorkflow:
    @workflow.run
    async def run(self, event: AudienceEvent) -> list[str]:
        try:
            return await workflow.execute_activity_method(
                AudienceWatchActivities.watch_audience_event,
                event,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
        except ActivityError:
            return await workflow.execute_activity_method(
                AudienceWatchActivities.pause_unverified,
                event,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
