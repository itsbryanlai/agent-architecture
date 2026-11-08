"""TaskWorkflow: one unit of real work, from approval card to follow-up.

Rules this workflow keeps:

* Writes never run without approval. Only the requester can approve, and the
  approval nonce on the click must match the one on the card. Anything else is
  ignored and the workflow keeps waiting.
* What was approved is exactly what runs: the payload in ``TaskSpec`` was frozen
  before the card was posted and is never changed here.
* Exactly once. The item is created with the idempotency key
  ``{task_id}:create``; the connector looks for that key before creating, so a
  retried activity finds the item it already made.
* History stays small: after creation the workflow keeps only the item's page
  ID, title, URL and last status.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from agentos.activities import AgentActivities
    from agentos.config import AgentVersion
    from agentos.connectors.notion import normalize_id
    from agentos.models import ApprovalDecision, ExternalUpdate, NotionItem, TaskSpec
    from agentos.workflows import ACTIVITY_TIMEOUT, RETRY

STATES = ("awaiting_approval", "executing", "watching", "done", "cancelled", "expired", "failed")


@workflow.defn(name="TaskWorkflow")
class TaskWorkflow:
    @workflow.init
    def __init__(self, spec: TaskSpec) -> None:
        self._spec = spec
        self._state = "drafted"
        self._decision: ApprovalDecision | None = None
        self._page_key: str | None = None
        self._poked = False

    @workflow.query(name="state")
    def state(self) -> str:
        return self._state

    @workflow.signal(name="decision")
    def decision(self, d: ApprovalDecision) -> None:
        if self._decision is not None:
            return
        # Only the requester may decide, and only with the nonce from the card.
        # The channel a click came from proves nothing, so it is not checked.
        if d.nonce != self._spec.approval_nonce:
            workflow.logger.warning("ignored decision: nonce does not match")
            return
        if d.user_id != self._spec.ctx.requester_user_id:
            workflow.logger.warning("ignored decision: clicker is not the requester")
            return
        self._decision = d

    @workflow.signal(name="external_update")
    def external_update(self, u: ExternalUpdate) -> None:
        if self._page_key is not None and normalize_id(u.page_id) == self._page_key:
            self._poked = True

    @workflow.run
    async def run(self, spec: TaskSpec) -> str:
        task_id = workflow.info().workflow_id
        ctx = spec.ctx
        title = spec.payload["title"]

        agent: AgentVersion = await self._act(AgentActivities.load_agent, ctx.agent_id)
        card_ts: str = await self._act(AgentActivities.post_approval_card, spec, task_id)
        self._state = "awaiting_approval"

        # --- wait for a valid decision, with one reminder, until expiry ---------
        expires = timedelta(hours=agent.approval.expires_after_hours)
        remind = timedelta(hours=agent.approval.reminder_after_hours)
        started = workflow.now()
        reminded = remind >= expires
        while self._decision is None:
            elapsed = workflow.now() - started
            if elapsed >= expires:
                break
            next_stop = expires if reminded else remind
            try:
                await workflow.wait_condition(
                    lambda: self._decision is not None, timeout=next_stop - elapsed
                )
            except TimeoutError:
                if not reminded and workflow.now() - started >= remind:
                    reminded = True
                    await self._act(
                        AgentActivities.post_in_thread,
                        ctx,
                        f"Reminder: <@{ctx.requester_user_id}>, the request '{title}' is "
                        "waiting for your approval in your DMs.",
                        f"{task_id}:reminder",
                    )

        if self._decision is None:
            await self._act(AgentActivities.update_card, spec, task_id, card_ts, "Expired")
            await self._act(
                AgentActivities.post_in_thread,
                ctx,
                "The request expired without approval.",
                f"{task_id}:expired",
            )
            self._state = "expired"
            return self._state

        if not self._decision.approved:
            await self._act(AgentActivities.update_card, spec, task_id, card_ts, "Cancelled")
            self._state = "cancelled"
            return self._state

        # --- execute exactly once ------------------------------------------------
        self._state = "executing"
        try:
            item: NotionItem = await self._act(AgentActivities.create_notion_item, spec, task_id)
        except ActivityError as err:
            reason = _reason(err)
            await self._act(
                AgentActivities.update_card, spec, task_id, card_ts, f"Failed: {reason}"
            )
            await self._act(
                AgentActivities.post_in_thread,
                ctx,
                f"I couldn't file '{title}': {reason}",
                f"{task_id}:failed",
            )
            self._state = "failed"
            return self._state

        await self._act(AgentActivities.update_card, spec, task_id, card_ts, "Submitted")
        await self._act(
            AgentActivities.post_in_thread,
            ctx,
            f"Filed: {item.title} ({item.url}). I'll tell you here when it's done.",
            f"{task_id}:filed",
        )

        # --- watch until done ----------------------------------------------------
        self._state = "watching"
        self._page_key = normalize_id(item.page_id)
        done_values = _done_values(agent, spec.payload["database_id"])
        poll = timedelta(minutes=agent.follow_up.poll_interval_minutes)
        stale_after = timedelta(days=agent.follow_up.stale_after_days)
        max_watch = timedelta(days=agent.follow_up.max_watch_days)
        watch_start = workflow.now()
        last_status = item.status
        last_change = watch_start
        stale_sent = False

        while True:
            elapsed = workflow.now() - watch_start
            if elapsed >= max_watch:
                await self._act(
                    AgentActivities.post_in_thread,
                    ctx,
                    f"I've stopped watching '{item.title}'.",
                    f"{task_id}:stopped",
                )
                self._state = "done"
                return self._state
            try:
                await workflow.wait_condition(
                    lambda: self._poked, timeout=min(poll, max_watch - elapsed)
                )
            except TimeoutError:
                pass
            self._poked = False

            current: NotionItem = await self._act(
                AgentActivities.get_notion_item, spec, item.page_id
            )
            if current.status in done_values:
                await self._act(
                    AgentActivities.post_in_thread,
                    ctx,
                    f"Your request '{current.title}' was marked {current.status}.",
                    f"{task_id}:done",
                )
                self._state = "done"
                return self._state
            now = workflow.now()
            if current.status != last_status:
                last_status = current.status
                last_change = now
            elif not stale_sent and now - last_change >= stale_after:
                stale_sent = True
                await self._act(
                    AgentActivities.post_in_thread,
                    ctx,
                    f"No movement on '{current.title}' in {agent.follow_up.stale_after_days} days.",
                    f"{task_id}:stale",
                )
            item = NotionItem(
                page_id=item.page_id, url=item.url, title=current.title, status=current.status
            )

    async def _act(self, method, *args):
        return await workflow.execute_activity_method(
            method,
            args=list(args),
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )


def _reason(err: ActivityError) -> str:
    cause = err.cause
    if isinstance(cause, ApplicationError):
        return cause.message
    return str(cause or err)


def _done_values(agent: AgentVersion, database_id: str) -> list[str]:
    for grant in agent.grants.notion_databases:
        if normalize_id(grant.id) == normalize_id(database_id):
            return list(grant.done_values)
    return []
