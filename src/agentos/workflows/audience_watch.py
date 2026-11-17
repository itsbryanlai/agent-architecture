"""AudienceWatchWorkflow: act on one Slack event saying a channel was shared with another
organisation.

Started by the gateway with a workflow ID built from the tenant and the Slack event ID,
and never reused, so Slack delivering the same event again starts nothing. It runs one
activity that pauses the agents answering in the channel; the activity is retried until it
succeeds. History carries IDs only. Workflow code is deterministic: no I/O.
"""

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agentos.audience_watch import AudienceWatchActivities
    from agentos.models import AudienceEvent
    from agentos.workflows import ACTIVITY_TIMEOUT, RETRY


@workflow.defn(name="AudienceWatchWorkflow")
class AudienceWatchWorkflow:
    @workflow.run
    async def run(self, event: AudienceEvent) -> list[str]:
        return await workflow.execute_activity_method(
            AudienceWatchActivities.watch_audience_event,
            event,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
