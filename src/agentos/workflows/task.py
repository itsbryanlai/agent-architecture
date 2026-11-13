"""TaskWorkflow: one unit of real work, from approval card to follow-up.

Rules this workflow keeps:

* Writes never run without approval. Only the requester can approve, and the
  approval nonce on the click must match the one on the card. Anything else is
  ignored and the workflow keeps waiting.
* What was approved is exactly what runs: the payload in ``TaskSpec`` is frozen
  before the card is posted. The only thing that changes it is an edit by the
  requester, which replaces the draft, issues a new nonce (so a click on an
  older version of the card no longer matches) and needs a fresh approval. At
  most ``MAX_EDITS`` edits are accepted per task, and the expiry and reminder
  window starts again from each edit.
* A comment may only target a page in a database the agent was granted. That
  is checked before the card is posted and again before the comment is made.
* Exactly once. The item is created with the idempotency key
  ``{task_id}:create`` (a comment with ``{task_id}:comment``); the connector
  looks for that key before writing, so a retried activity finds what it
  already made.
* Logs carry IDs only, never the comment, the title or the details.
* History stays small: after creation the workflow keeps only the item's page
  ID, title, URL and last status.
"""

from __future__ import annotations

import dataclasses
from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from agentos.activities import AgentActivities
    from agentos.config import AgentVersion
    from agentos.connectors.notion import normalize_id
    from agentos.models import (
        EDITABLE_FIELDS,
        ApprovalDecision,
        DraftView,
        EditRequest,
        ExternalUpdate,
        NotionItem,
        TaskSpec,
    )
    from agentos.slack_format import escape_slack
    from agentos.workflows import ACTIVITY_TIMEOUT, RETRY

MAX_EDITS = 5

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
        self._edits = 0
        self._refresh_due = False

    @workflow.query(name="draft")
    def draft(self) -> DraftView:
        spec = self._spec
        return DraftView(
            tool_name=spec.tool_name,
            fields={k: spec.payload[k] for k in EDITABLE_FIELDS.get(spec.tool_name, {})},
            nonce=spec.approval_nonce,
            requester_user_id=spec.ctx.requester_user_id,
            editable=self._can_edit(),
        )

    def _can_edit(self) -> bool:
        return (
            self._state == "awaiting_approval"
            and self._decision is None
            and self._edits < MAX_EDITS
        )

    @workflow.signal(name="edit")
    def edit(self, e: EditRequest) -> None:
        """Replace the draft with the requester's edit and issue a new nonce."""
        spec = self._spec
        if not self._can_edit():
            workflow.logger.warning("ignored edit: the draft can no longer be edited")
            return
        if e.nonce != spec.approval_nonce:
            workflow.logger.warning("ignored edit: nonce does not match")
            return
        if e.user_id != spec.ctx.requester_user_id:
            workflow.logger.warning("ignored edit: sender is not the requester")
            return
        limits = EDITABLE_FIELDS.get(spec.tool_name, {})
        fields = e.fields if isinstance(e.fields, dict) else {}
        if set(fields) != set(limits):
            workflow.logger.warning("ignored edit: fields are not the editable ones")
            return
        clean: dict = {}
        for key, value in fields.items():
            text = value.strip() if isinstance(value, str) else ""
            if not text or len(text) > limits[key]:
                workflow.logger.warning("ignored edit: invalid value for a field")
                return
            clean[key] = text
        self._spec = dataclasses.replace(
            spec,
            payload={**spec.payload, **clean},
            approval_nonce=str(workflow.uuid4()),
        )
        self._edits += 1
        self._refresh_due = True

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

        agent: AgentVersion = await self._act(
            AgentActivities.load_agent, ctx.tenant_id, ctx.agent_id
        )
        if spec.tool_name == "add_comment":
            # Resolve the item and check its database is granted before any card.
            try:
                payload: dict = await self._act(AgentActivities.prepare_comment, spec)
            except ActivityError:
                await self._act(
                    AgentActivities.post_in_thread,
                    ctx,
                    "I can't comment on that item: I couldn't find it, or it isn't in a "
                    "database I have access to.",
                    f"{task_id}:failed",
                )
                self._state = "failed"
                return self._state
            self._spec = dataclasses.replace(spec, payload=payload)
        card_ts: str = await self._act(AgentActivities.post_approval_card, self._spec, task_id)
        self._state = "awaiting_approval"

        # --- wait for a valid decision, with one reminder, until expiry ---------
        expires = timedelta(hours=agent.approval.expires_after_hours)
        remind = timedelta(hours=agent.approval.reminder_after_hours)
        started = workflow.now()
        reminded = remind >= expires
        reminder_key = f"{task_id}:reminder"
        while self._decision is None:
            if self._refresh_due:
                # An edit was accepted: redraw the card with the new nonce and
                # restart the expiry and reminder window from now.
                self._refresh_due = False
                await self._act(AgentActivities.refresh_card, self._spec, task_id, card_ts)
                started = workflow.now()
                reminded = remind >= expires
                reminder_key = f"{task_id}:reminder:{self._edits}"
                continue
            elapsed = workflow.now() - started
            if elapsed >= expires:
                break
            next_stop = expires if reminded else remind
            try:
                await workflow.wait_condition(
                    lambda: self._decision is not None or self._refresh_due,
                    timeout=next_stop - elapsed,
                )
            except TimeoutError:
                if not reminded and workflow.now() - started >= remind:
                    reminded = True
                    await self._act(
                        AgentActivities.post_in_thread,
                        ctx,
                        f"Reminder: <@{ctx.requester_user_id}>, the request "
                        f"'{escape_slack(_subject(self._spec))}' is waiting for your approval in your DMs.",
                        reminder_key,
                    )

        spec = self._spec
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

        if spec.tool_name == "add_comment":
            return await self._run_comment(spec, task_id, card_ts)

        title = spec.payload["title"]

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
                f"I couldn't file '{escape_slack(title)}': {escape_slack(reason)}",
                f"{task_id}:failed",
            )
            self._state = "failed"
            return self._state

        await self._act(AgentActivities.update_card, spec, task_id, card_ts, "Submitted")
        await self._act(
            AgentActivities.post_in_thread,
            ctx,
            f"Filed: {escape_slack(item.title)} ({item.url}). I'll tell you here when it's done.",
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
                    f"I've stopped watching '{escape_slack(item.title)}'.",
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
                    f"Your request '{escape_slack(current.title)}' was marked "
                    f"{escape_slack(current.status or '')}.",
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
                    f"No movement on '{escape_slack(current.title)}' in "
                    f"{agent.follow_up.stale_after_days} days.",
                    f"{task_id}:stale",
                )
            item = NotionItem(
                page_id=item.page_id, url=item.url, title=current.title, status=current.status
            )

    async def _run_comment(self, spec: TaskSpec, task_id: str, card_ts: str) -> str:
        """Post the approved comment once. There is nothing to watch afterwards."""
        ctx = spec.ctx
        self._state = "executing"
        title = spec.payload["item_title"]
        try:
            await self._act(AgentActivities.add_notion_comment, spec, task_id)
        except ActivityError as err:
            reason = _reason(err)
            await self._act(
                AgentActivities.update_card, spec, task_id, card_ts, f"Failed: {reason}"
            )
            await self._act(
                AgentActivities.post_in_thread,
                ctx,
                f"I couldn't comment on '{escape_slack(title)}': {escape_slack(reason)}",
                f"{task_id}:failed",
            )
            self._state = "failed"
            return self._state
        await self._act(AgentActivities.update_card, spec, task_id, card_ts, "Commented")
        await self._act(
            AgentActivities.post_in_thread,
            ctx,
            f"Commented on {escape_slack(title)} ({spec.payload['item_url']}).",
            f"{task_id}:commented",
        )
        self._state = "done"
        return self._state

    async def _act(self, method, *args):
        return await workflow.execute_activity_method(
            method,
            args=list(args),
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )


def _subject(spec: TaskSpec) -> str:
    if spec.tool_name == "add_comment":
        return spec.payload["item_title"]
    return spec.payload["title"]


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
