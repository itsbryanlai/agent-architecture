"""TaskWorkflow: one unit of real work, from approval card to follow-up.

Rules this workflow keeps:

* Writes never run without approval. Only the requester can approve, and the
  approval nonce on the click must match the one on the card. Anything else is
  ignored and the workflow keeps waiting.
* What was approved is exactly what runs: the payload is frozen before the card is posted,
  as revision 1 of the task's draft in Postgres (``task_drafts``); the spec carries the
  reference (``draft_ref``, ``revision``) and the nonce, never the text. The only thing that
  changes it is an edit by the requester, which stores the next revision, issues a new nonce
  (so a click on an older version of the card no longer matches) and needs a fresh approval.
  An approval binds to one (revision, nonce): the write activities read that revision, and
  a decision that arrives while an edit is being stored is ignored. At most ``MAX_EDITS``
  edits are accepted per task, and the expiry and reminder window starts again from each
  edit. A task recorded before drafts were stored carries its payload in the spec and runs
  the way it did then.
* A comment may only target a page in a database the agent was granted. That
  is checked before the card is posted and again before the comment is made.
* Exactly once, by the write activities and their lease in Postgres (see
  ``agentos.activities``): one attempt at a time writes, and a retry returns what the
  earlier attempt stored or finds it by its key (``{task_id}:create``, a comment
  ``{task_id}:comment``). This workflow adds the last step. When the activity's retries
  run out, a failure may still hide a write that happened, so the task looks for the
  item a few times, with timers between, before it speaks. If it is found, the task
  carries on as filed. If it is not, the task says it could not confirm and points to
  the tracker. It never says nothing was filed after retries ran out.
* A long watch continues as new, carrying a ``WatchState`` of IDs, times and flags, and a burst
  of pokes is answered at most once per ``POKE_GAP``.
* An uninstalled tenant ends the task cleanly: the runtime reports it disabled (a
  non-retryable refusal), and the task stops with no Slack post — the token is deleted —
  recording its final state for ``my_tasks``. A tenant not loaded yet stays retryable.
  Behind the ``tenant-disabled`` marker; older histories replay without it.
* Logs carry IDs only, never the comment, the title or the details.
* History stays small and free of content: after creation the workflow keeps only the item's
  page ID and last status. The lines posted in the thread that name the title, the comment
  or a link are written by the ``post_task_notice`` activity from the stored draft.
"""

from __future__ import annotations

import dataclasses
import re
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from agentos import ids, task_notices
    from agentos.activities import AgentActivities
    from agentos.agent_health import agent_unavailable_text
    from agentos.config import AgentVersion
    from agentos.connectors.notion import normalize_id
    from agentos.models import (
        EDITABLE_FIELDS,
        ApprovalDecision,
        DraftOutcome,
        DraftView,
        EditRequest,
        ExternalUpdate,
        NotionItem,
        NudgeDone,
        NudgePrepared,
        NudgeRequest,
        TaskSpec,
        WatchState,
        check_field,
    )
    from agentos.switches import agent_off_card_text, agent_off_text
    from agentos.workflows import (
        ACTIVITY_TIMEOUT,
        POKE_GAP,
        RECONCILE_INTERVAL,
        RECONCILE_TRIES,
        RETRY,
        TENANT_DISABLED_PATCH,
        WATCH_HISTORY_BOUND,
        WRITE_HEARTBEAT_TIMEOUT,
        WRITE_TIMEOUT,
        is_tenant_disabled,
    )

MAX_EDITS = 5

# Histories recorded before the reconcile step ran the write with no heartbeat and
# reported every exhausted retry as a failure. They replay on that path.
RECONCILE_PATCH = "reconcile-on-exhaustion"
# Histories recorded before the pre-write check never ask it, so they replay without it.
PRE_WRITE_PATCH = "pre-write-audience"
# Histories recorded before a task's payload was stored in Postgres carry it in the spec.
DRAFT_REFS_PATCH = "draft-refs"
# Histories recorded before it never continue as new and answer every poke at once.
CONTINUE_AS_NEW_PATCH = "task-continue-as-new"
# Histories recorded before a closed approval was audited close it with no audit row.
PATCH_APPROVAL_CLOSED = "approval-closed-audit"
# Histories recorded before agent health existed never see an unavailable agent; they
# replay without its clean stop. A watch is not covered: an unavailable agent's poll
# fails retryably, so the watch keeps polling with no workflow change.
AGENT_UNAVAILABLE_PATCH = "agent-unavailable"
# Histories recorded before the task store existed never record their state; they
# replay without the record activity, and those tasks simply have no row.
TASK_RECORD_PATCH = "task-record"
# Histories recorded before the nudge existed never offer one: their stale notice is
# the plain line, their watch never wakes for a nudge, and they carry no nudge state.
NUDGE_PATCH = "nudge"
UNCHECKED_CARD_TEXT = "I couldn't confirm you can still make this request; nothing was changed."
# One look per call: this workflow owns the retries, with a timer between.
RECONCILE_RETRY = RetryPolicy(maximum_attempts=1)

STATES = ("awaiting_approval", "executing", "watching", "done", "cancelled", "expired", "failed")


@workflow.defn(name="TaskWorkflow")
class TaskWorkflow:
    @workflow.init
    def __init__(self, spec: TaskSpec) -> None:
        self._spec = spec
        # A task whose payload is a stored draft: the spec names the revision, not the text.
        self._refs = bool(spec.draft_ref)
        # An edit being stored; while it is, a decision or a second edit is ignored.
        self._edit_pending: EditRequest | None = None
        self._state = "drafted"
        self._decision: ApprovalDecision | None = None
        self._page_key: str | None = None
        self._poked = False
        self._edits = 0
        self._refresh_due = False
        # The nudge offer's nonce, the nudge task in flight and a click or report the
        # watch loop has not applied yet survive a hop in the WatchState; the loop
        # applies them, never here.
        resume = spec.resume
        self._nudge_nonce = resume.nudge_nonce if resume is not None else ""
        self._nudge_child = resume.nudge_child if resume is not None else ""
        self._nudge_requested = resume.nudge_requested if resume is not None else False
        self._nudge_done = resume.nudge_done if resume is not None else False
        if spec.resume is not None:
            self._state = "watching"

    @workflow.query(name="draft")
    def draft(self) -> DraftView:
        spec = self._spec
        return DraftView(
            tool_name=spec.tool_name,
            fields={}
            if self._refs
            else {k: spec.payload[k] for k in EDITABLE_FIELDS.get(spec.tool_name, {})},
            nonce=spec.approval_nonce,
            requester_user_id=spec.ctx.requester_user_id,
            editable=self._can_edit(),
            revision=spec.revision,
        )

    def _can_edit(self) -> bool:
        return (
            self._state == "awaiting_approval"
            and self._decision is None
            and self._edit_pending is None
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
        if self._refs or e.edit_id:
            if self._refs and e.edit_id:
                # The values are in ``inbound_events``; ``apply_edit`` checks and stores them.
                self._edit_pending = e
            else:
                workflow.logger.warning("ignored edit: it does not match how this draft is kept")
            return
        limits = EDITABLE_FIELDS.get(spec.tool_name, {})
        fields = e.fields if isinstance(e.fields, dict) else {}
        if set(fields) != set(limits):
            workflow.logger.warning("ignored edit: fields are not the editable ones")
            return
        clean: dict = {}
        for key, value in fields.items():
            text = value.strip() if isinstance(value, str) else ""
            if not text or check_field(spec.tool_name, key, text):
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
        if self._edit_pending is not None:
            workflow.logger.warning("ignored decision: an edit is being saved")
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

    @workflow.signal(name="nudge")
    def nudge(self, r: NudgeRequest) -> None:
        """A click on the stale notice's nudge button: only the requester, only the
        live offer's nonce, only while watching with no nudge already in flight.

        An accepted click spends the offer at once: the nonce is cleared here, so a
        second click on the same button is ignored even while the nudge is drafted.
        """
        if self._state != "watching":
            workflow.logger.warning("ignored nudge click: the task is not watching")
            return
        if not self._nudge_nonce or r.nonce != self._nudge_nonce:
            workflow.logger.warning("ignored nudge click: nonce does not match the offer")
            return
        if r.user_id != self._spec.ctx.requester_user_id:
            workflow.logger.warning("ignored nudge click: clicker is not the requester")
            return
        if self._nudge_child:
            workflow.logger.warning("ignored nudge click: a nudge is already in flight")
            return
        self._nudge_requested = True
        self._nudge_nonce = ""

    @workflow.signal(name="nudge_done")
    def nudge_done(self, d: NudgeDone) -> None:
        """The nudge task reports its comment landed; the watch restarts the stale clock."""
        if self._nudge_child and d.child_id == self._nudge_child:
            self._nudge_done = True

    @workflow.run
    async def run(self, spec: TaskSpec) -> str:
        if self._refs:
            workflow.patched(DRAFT_REFS_PATCH)
        try:
            return await self._run(spec)
        except ActivityError as err:
            # Before approval nothing has been written, so a terminal failure
            # ends the task with one line. After it, the failure paths below
            # decide what to say, and anything else is raised.
            if self._state not in ("drafted", "awaiting_approval"):
                raise
            task_id = workflow.info().workflow_id
            if workflow.patched(TENANT_DISABLED_PATCH) and is_tenant_disabled(err):
                return await self._stop_for_tenant_disabled(spec, task_id)
            if workflow.patched(AGENT_UNAVAILABLE_PATCH) and _is_agent_unavailable(err):
                # The activity's message is the fixed unavailable reply, built from the
                # agent's name: a clean stop, not a failure. Nothing was written.
                await self._act(
                    AgentActivities.post_in_thread, spec.ctx, _reason(err), f"{task_id}:failed"
                )
                self._state = "cancelled"
                await self._record()
                return self._state
            await self._notice(task_notices.CARRY_ON_FAILED, f"{task_id}:failed", task_id)
            self._state = "failed"
            await self._record()
            return self._state

    async def _run(self, spec: TaskSpec) -> str:
        task_id = workflow.info().workflow_id
        ctx = spec.ctx

        agent: AgentVersion = await self._act(
            AgentActivities.load_agent, ctx.tenant_id, ctx.agent_id
        )
        if spec.resume is not None:
            # A continue-as-new of a watching task: approval and the write are long done.
            resume = spec.resume
            database_id = spec.database_id if self._refs else spec.payload["database_id"]
            item = NotionItem(page_id=resume.page_id, url="", title="", status=resume.last_status)
            return await self._watch(task_id, agent, item, database_id, resume)
        if spec.tool_name == "add_comment":
            # Resolve the item and check its database is granted before any card.
            try:
                if self._refs:
                    resolved: DraftOutcome = await self._act(
                        AgentActivities.prepare_stored_comment, spec
                    )
                else:
                    payload: dict = await self._act(AgentActivities.prepare_comment, spec)
            except ActivityError as err:
                if _is_agent_off(err):
                    await self._act(
                        AgentActivities.post_in_thread,
                        ctx,
                        agent_off_text(agent.name),
                        f"{task_id}:failed",
                    )
                    self._state = "cancelled"
                    await self._record()
                    return self._state
                if workflow.patched(AGENT_UNAVAILABLE_PATCH) and _is_agent_unavailable(err):
                    await self._act(
                        AgentActivities.post_in_thread,
                        ctx,
                        agent_unavailable_text(agent.name),
                        f"{task_id}:failed",
                    )
                    self._state = "cancelled"
                    await self._record()
                    return self._state
                await self._act(
                    AgentActivities.post_in_thread,
                    ctx,
                    "I can't comment on that item: I couldn't find it, or it isn't in a "
                    "database I have access to.",
                    f"{task_id}:failed",
                )
                self._state = "failed"
                await self._record()
                return self._state
            if self._refs:
                self._spec = dataclasses.replace(
                    spec, revision=resolved.revision, database_id=resolved.database_id
                )
            else:
                self._spec = dataclasses.replace(spec, payload=payload)
        card_ts: str = await self._act(AgentActivities.post_approval_card, self._spec, task_id)
        self._state = "awaiting_approval"
        await self._record()

        # --- wait for a valid decision, with one reminder, until expiry ---------
        expires = timedelta(hours=agent.approval.expires_after_hours)
        remind = timedelta(hours=agent.approval.reminder_after_hours)
        started = workflow.now()
        reminded = remind >= expires
        reminder_key = f"{task_id}:reminder"
        while self._decision is None:
            if self._edit_pending is not None:
                await self._store_edit(self._edit_pending)
                continue
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
                    lambda: (
                        self._decision is not None
                        or self._refresh_due
                        or self._edit_pending is not None
                    ),
                    timeout=next_stop - elapsed,
                )
            except TimeoutError:
                if not reminded and workflow.now() - started >= remind:
                    reminded = True
                    await self._notice(task_notices.REMINDER, reminder_key, task_id)

        spec = self._spec
        if self._decision is None:
            if workflow.patched(PATCH_APPROVAL_CLOSED):
                await self._try(AgentActivities.record_approval_closed, spec, task_id, "expired")
            await self._try(AgentActivities.update_card, spec, task_id, card_ts, "Expired")
            await self._try(
                AgentActivities.post_in_thread,
                ctx,
                "The request expired without approval.",
                f"{task_id}:expired",
            )
            self._state = "expired"
            await self._record()
            return self._state

        if not self._decision.approved:
            if workflow.patched(PATCH_APPROVAL_CLOSED):
                await self._try(AgentActivities.record_approval_closed, spec, task_id, "declined")
            await self._try(AgentActivities.update_card, spec, task_id, card_ts, "Cancelled")
            self._state = "cancelled"
            await self._record()
            return self._state

        if workflow.patched(PRE_WRITE_PATCH):
            # Pending cards do not outlive what let them run: an agent turned off, a channel
            # paused or an audience changed since the card was shown stops the write here.
            try:
                stopped = await self._act(AgentActivities.check_before_write, spec, task_id)
            except ActivityError as err:
                if workflow.patched(TENANT_DISABLED_PATCH) and is_tenant_disabled(err):
                    return await self._stop_for_tenant_disabled(spec, task_id)
                # The check could not be completed: fail closed, take the buttons off.
                stopped = UNCHECKED_CARD_TEXT
            if stopped is not None:
                await self._try(AgentActivities.update_card_line, spec, task_id, card_ts, stopped)
                self._state = "cancelled"
                await self._record()
                return self._state

        if spec.tool_name == "add_comment":
            return await self._run_comment(spec, task_id, card_ts, agent.name)
        if spec.tool_name == "nudge":
            return await self._run_nudge(spec, task_id, card_ts, agent.name)

        title = "" if self._refs else spec.payload["title"]
        database_id = spec.database_id if self._refs else spec.payload["database_id"]

        # --- execute exactly once ------------------------------------------------
        self._state = "executing"
        await self._record()
        try:
            item: NotionItem = await self._act_write(
                AgentActivities.create_notion_item, spec, task_id
            )
        except ActivityError as err:
            if workflow.patched(TENANT_DISABLED_PATCH) and is_tenant_disabled(err):
                return await self._stop_for_tenant_disabled(spec, task_id)
            if _is_agent_off(err):
                return await self._stop_for_agent_off(spec, task_id, card_ts, agent.name)
            if workflow.patched(AGENT_UNAVAILABLE_PATCH) and _is_agent_unavailable(err):
                return await self._stop_for_agent_unavailable(spec, task_id, card_ts, agent.name)
            found = None
            if workflow.patched(RECONCILE_PATCH) and not _is_permanent(err):
                found = await self._reconcile(AgentActivities.reconcile_notion_item, spec, task_id)
                if found is None:
                    tracker = f"https://www.notion.so/{normalize_id(database_id)}"
                    await self._try(
                        AgentActivities.update_card, spec, task_id, card_ts, "Not confirmed"
                    )
                    await self._try(
                        AgentActivities.post_in_thread,
                        ctx,
                        f"I couldn't confirm whether it was filed; please check {tracker}",
                        f"{task_id}:failed",
                    )
                    self._state = "failed"
                    await self._record()
                    return self._state
            if found is None:
                reason = _reason(err)
                await self._try(
                    AgentActivities.update_card, spec, task_id, card_ts, f"Failed: {reason}"
                )
                await self._notice(
                    task_notices.FAILED_CREATE,
                    f"{task_id}:failed",
                    task_id,
                    subject=title,
                    reason=reason,
                )
                self._state = "failed"
                await self._record()
                return self._state
            item = found

        await self._try(AgentActivities.update_card, spec, task_id, card_ts, "Submitted")
        await self._notice(
            task_notices.FILED, f"{task_id}:filed", task_id, subject=item.title, url=item.url
        )

        # --- watch until done ----------------------------------------------------
        now = workflow.now()
        state = WatchState(
            page_id=item.page_id,
            last_status=item.status,
            last_change=now,
            stale_sent=False,
            deadline=now + timedelta(days=agent.follow_up.max_watch_days),
        )
        return await self._watch(task_id, agent, item, database_id, state)

    async def _watch(
        self,
        task_id: str,
        agent: AgentVersion,
        item: NotionItem,
        database_id: str,
        state: WatchState,
    ) -> str:
        """Poll the item until it is done, the watch runs out or the agent is turned off.

        A long watch continues as new, carrying ``state``, so no run's history grows without
        bound. Pokes are answered at most once per ``POKE_GAP``.
        """
        ctx = self._spec.ctx
        self._state = "watching"
        await self._record(state.last_status or "")
        self._page_key = normalize_id(item.page_id)
        self._poked = state.poked
        done_values = _done_values(agent, database_id)
        poll = timedelta(minutes=agent.follow_up.poll_interval_minutes)
        stale_after = timedelta(days=agent.follow_up.stale_after_days)
        deadline = state.deadline
        last_status = state.last_status
        last_change = state.last_change
        stale_sent = state.stale_sent
        nudges_used = state.nudges_used
        # Histories recorded before this patch neither hop nor space out pokes.
        hop = workflow.patched(CONTINUE_AS_NEW_PATCH)
        # Histories recorded before the nudge never offer one and never wake for one.
        nudge_on = workflow.patched(NUDGE_PATCH)
        last_poll = None

        while True:
            remaining = deadline - workflow.now()
            if remaining <= timedelta(0):
                await self._notice(
                    task_notices.STOPPED_WATCH, f"{task_id}:stopped", task_id, subject=item.title
                )
                self._state = "done"
                await self._record(last_status or "")
                return self._state
            if hop and _hop_due():
                # Signals that arrive after this point make Temporal refuse the hop and run
                # their handlers first, so a poke is never lost; one already received is carried.
                await workflow.wait_condition(workflow.all_handlers_finished)
                resume = WatchState(
                    page_id=item.page_id,
                    last_status=last_status,
                    last_change=last_change,
                    stale_sent=stale_sent,
                    deadline=deadline,
                    poked=self._poked,
                    nudges_used=nudges_used,
                    nudge_nonce=self._nudge_nonce,
                    nudge_child=self._nudge_child,
                    nudge_requested=self._nudge_requested,
                    nudge_done=self._nudge_done,
                )
                workflow.continue_as_new(dataclasses.replace(self._spec, resume=resume))
            woken_by_poke = True
            try:
                await workflow.wait_condition(
                    lambda: (
                        self._poked or (nudge_on and (self._nudge_requested or self._nudge_done))
                    ),
                    timeout=min(poll, remaining),
                )
                if nudge_on:
                    # A nudge click or report is not a poke: it neither spaces out the
                    # next poll nor asks for a fresh-status report.
                    woken_by_poke = self._poked
            except TimeoutError:
                woken_by_poke = False
            if hop and woken_by_poke and last_poll is not None:
                wait = POKE_GAP - (workflow.now() - last_poll)
                if wait > timedelta(0):
                    await workflow.sleep(wait)
            self._poked = False
            last_poll = workflow.now()

            if nudge_on:
                if self._nudge_done:
                    # The nudge ran: the stale clock restarts from now, and a later
                    # stale period may offer another, up to max_nudges. The spent
                    # offer's button goes first, under the key its notice was posted.
                    self._nudge_done = False
                    self._nudge_child = ""
                    self._nudge_nonce = ""
                    await self._try(
                        AgentActivities.clear_stale_offer,
                        self._spec,
                        task_id,
                        f"{task_id}:stale:{nudges_used}",
                        agent.follow_up.stale_after_days,
                    )
                    nudges_used += 1
                    last_change = workflow.now()
                    stale_sent = False
                if self._nudge_requested:
                    self._nudge_requested = False
                    await self._try(
                        AgentActivities.clear_stale_offer,
                        self._spec,
                        task_id,
                        f"{task_id}:stale:{nudges_used}",
                        agent.follow_up.stale_after_days,
                    )
                    await self._start_nudge(
                        task_id, item, database_id, nudges_used, agent.follow_up.stale_after_days
                    )

            try:
                current: NotionItem = await self._act(
                    AgentActivities.watch_notion_item
                    if self._refs
                    else AgentActivities.get_notion_item,
                    ctx.tenant_id,
                    ctx.agent_id,
                    item.page_id,
                )
            except ActivityError as err:
                if workflow.patched(TENANT_DISABLED_PATCH) and is_tenant_disabled(err):
                    # The app was uninstalled: the watch ends here, with no post.
                    return await self._stop_for_tenant_disabled(
                        self._spec, task_id, last_status or ""
                    )
                if _is_agent_off(err):
                    # An admin turned the agent off: the watch ends here, with no post.
                    self._state = "cancelled"
                    await self._record(last_status or "")
                    return self._state
                if _is_permanent(err):
                    await self._notice(
                        task_notices.STOPPED_UNREADABLE,
                        f"{task_id}:stopped",
                        task_id,
                        subject=item.title,
                    )
                    self._state = "done"
                    await self._record(last_status or "")
                    return self._state
                workflow.logger.warning("watch poll failed after retries; still watching")
                continue
            if current.status in done_values:
                await self._notice(
                    task_notices.DONE,
                    f"{task_id}:done",
                    task_id,
                    subject=current.title,
                    status=current.status or "",
                )
                self._state = "done"
                await self._record(current.status or "")
                return self._state
            now = workflow.now()
            if current.status != last_status:
                last_status = current.status
                last_change = now
                await self._record(current.status or "")
            else:
                if woken_by_poke:
                    # A poke asked for a fresh look: report the check even when nothing
                    # moved, so a my_tasks wait in the thread sees the refresh land.
                    await self._record(current.status or "")
                if not stale_sent and now - last_change >= stale_after:
                    stale_sent = True
                    if nudge_on:
                        # One offer per stale period: the button lives on this notice
                        # alone, and only a nudge that runs starts the next period.
                        nonce = ""
                        if agent.follow_up.nudge and nudges_used < agent.follow_up.max_nudges:
                            offer = False
                            try:
                                offer = await self._act(
                                    AgentActivities.nudge_offer, self._spec, task_id, item.page_id
                                )
                            except ActivityError as err:
                                error_type, slack_code = _failure_cause(err)
                                workflow.logger.warning(
                                    "activity nudge_offer failed after retries; continuing "
                                    "tenant_id=%s task_id=%s error=%s slack_code=%s",
                                    ctx.tenant_id,
                                    task_id,
                                    error_type,
                                    slack_code,
                                )
                            if offer:
                                nonce = str(workflow.uuid4())
                                self._nudge_nonce = nonce
                        await self._try(
                            AgentActivities.post_stale_notice,
                            self._spec,
                            task_id,
                            f"{task_id}:stale:{nudges_used}",
                            agent.follow_up.stale_after_days,
                            nonce,
                        )
                    else:
                        await self._notice(
                            task_notices.STALE,
                            f"{task_id}:stale",
                            task_id,
                            subject=current.title,
                            days=agent.follow_up.stale_after_days,
                        )
            item = NotionItem(
                page_id=item.page_id, url=item.url, title=current.title, status=current.status
            )

    async def _stop_for_tenant_disabled(
        self, spec: TaskSpec, task_id: str, status: str = ""
    ) -> str:
        """The tenant's app was uninstalled: end cleanly with no Slack post.

        The token is deleted, so no card redraw or thread line can land; the task's
        final state is still recorded for ``my_tasks``. A write in flight when the
        uninstall landed cannot be confirmed either way (the runtime is gone); the
        task says nothing rather than claim it was not filed.
        """
        workflow.logger.info(
            "task ended: tenant uninstalled tenant_id=%s task_id=%s",
            spec.ctx.tenant_id,
            task_id,
        )
        self._state = "cancelled"
        await self._record(status)
        return self._state

    async def _stop_for_agent_off(
        self, spec: TaskSpec, task_id: str, card_ts: str, agent_name: str
    ) -> str:
        """The agent was turned off after the approval and before the write: nothing was written."""
        await self._try(
            AgentActivities.update_card_line,
            spec,
            task_id,
            card_ts,
            agent_off_card_text(agent_name),
        )
        self._state = "cancelled"
        await self._record()
        return self._state

    async def _stop_for_agent_unavailable(
        self, spec: TaskSpec, task_id: str, card_ts: str, agent_name: str
    ) -> str:
        """The agent's grants broke after the approval and before the write: nothing was written."""
        await self._try(
            AgentActivities.update_card_line,
            spec,
            task_id,
            card_ts,
            agent_unavailable_text(agent_name),
        )
        self._state = "cancelled"
        await self._record()
        return self._state

    async def _run_comment(
        self, spec: TaskSpec, task_id: str, card_ts: str, agent_name: str
    ) -> str:
        """Post the approved comment once. There is nothing to watch afterwards."""
        self._state = "executing"
        await self._record()
        payload = {} if self._refs else spec.payload
        title, item_url = payload.get("item_title", ""), payload.get("item_url", "")
        try:
            await self._act_write(AgentActivities.add_notion_comment, spec, task_id)
        except ActivityError as err:
            if workflow.patched(TENANT_DISABLED_PATCH) and is_tenant_disabled(err):
                return await self._stop_for_tenant_disabled(spec, task_id)
            if _is_agent_off(err):
                return await self._stop_for_agent_off(spec, task_id, card_ts, agent_name)
            if workflow.patched(AGENT_UNAVAILABLE_PATCH) and _is_agent_unavailable(err):
                return await self._stop_for_agent_unavailable(spec, task_id, card_ts, agent_name)
            if workflow.patched(RECONCILE_PATCH) and not _is_permanent(err):
                found = await self._reconcile(
                    AgentActivities.reconcile_notion_comment, spec, task_id
                )
                if found is None:
                    await self._try(
                        AgentActivities.update_card, spec, task_id, card_ts, "Not confirmed"
                    )
                    await self._notice(
                        task_notices.NOT_CONFIRMED_COMMENT,
                        f"{task_id}:failed",
                        task_id,
                        url=item_url,
                    )
                    self._state = "failed"
                    await self._record()
                    return self._state
            else:
                reason = _reason(err)
                await self._try(
                    AgentActivities.update_card, spec, task_id, card_ts, f"Failed: {reason}"
                )
                await self._notice(
                    task_notices.FAILED_COMMENT,
                    f"{task_id}:failed",
                    task_id,
                    subject=title,
                    reason=reason,
                )
                self._state = "failed"
                await self._record()
                return self._state
        await self._try(AgentActivities.update_card, spec, task_id, card_ts, "Commented")
        await self._notice(
            task_notices.COMMENTED,
            f"{task_id}:commented",
            task_id,
            subject=title,
            url=item_url,
        )
        self._state = "done"
        await self._record()
        return self._state

    async def _start_nudge(
        self, task_id: str, item: NotionItem, database_id: str, nudges_used: int, stale_days: int
    ) -> None:
        """Draft the nudge and start its task: one approval card for both effects.

        The watch keeps watching while the card waits. The nudge task reports back
        when its comment lands; only then does the stale clock restart. A nudge that
        is declined, expires or fails reports nothing: the period's offer is spent.
        """
        ctx = self._spec.ctx
        child_id = ids.task_id(ctx.tenant_id, str(workflow.uuid4()))
        try:
            prepared: NudgePrepared = await self._act(
                AgentActivities.prepare_nudge, self._spec, task_id, item.page_id, child_id
            )
        except ActivityError as err:
            error_type, slack_code = _failure_cause(err)
            workflow.logger.warning(
                "activity prepare_nudge failed after retries; continuing "
                "tenant_id=%s task_id=%s error=%s slack_code=%s",
                ctx.tenant_id,
                task_id,
                error_type,
                slack_code,
            )
            self._nudge_nonce = ""
            await self._try(
                AgentActivities.clear_stale_offer,
                self._spec,
                task_id,
                f"{task_id}:stale:{nudges_used}",
                stale_days,
            )
            await self._notice(
                task_notices.NUDGE_PREPARE_FAILED,
                f"{task_id}:nudge-failed:{nudges_used}",
                task_id,
                subject=item.title,
            )
            return
        self._nudge_nonce = ""
        if prepared.refused:
            await self._notice(
                task_notices.NUDGE_SELF,
                f"{task_id}:nudge-self:{nudges_used}",
                task_id,
                subject=item.title,
            )
            return
        spec = TaskSpec(
            ctx=ctx,
            tool_name="nudge",
            payload={},
            approval_nonce=str(workflow.uuid4()),
            draft_ref=child_id,
            revision=1,
            database_id=database_id,
            parent_id=task_id,
            nudge_dm=prepared.dm,
        )
        await workflow.start_child_workflow(
            TaskWorkflow.run, spec, id=child_id, parent_close_policy=ParentClosePolicy.ABANDON
        )
        self._nudge_child = child_id

    async def _run_nudge(self, spec: TaskSpec, task_id: str, card_ts: str, agent_name: str) -> str:
        """Run the approved nudge: the comment first, then the DM, each exactly once.

        A DM failure after the comment reports "commented, couldn't DM" and never
        retries the comment. The watch that started this task is told the comment
        landed, so its stale clock restarts from the nudge.
        """
        self._state = "executing"
        await self._record()
        payload = {} if self._refs else spec.payload
        title, item_url = payload.get("item_title", ""), payload.get("item_url", "")
        try:
            await self._act_write(AgentActivities.nudge_comment, spec, task_id)
        except ActivityError as err:
            if workflow.patched(TENANT_DISABLED_PATCH) and is_tenant_disabled(err):
                return await self._stop_for_tenant_disabled(spec, task_id)
            if _is_agent_off(err):
                return await self._stop_for_agent_off(spec, task_id, card_ts, agent_name)
            if workflow.patched(AGENT_UNAVAILABLE_PATCH) and _is_agent_unavailable(err):
                return await self._stop_for_agent_unavailable(spec, task_id, card_ts, agent_name)
            if workflow.patched(RECONCILE_PATCH) and not _is_permanent(err):
                found = await self._reconcile(
                    AgentActivities.reconcile_notion_comment, spec, task_id
                )
                if found is None:
                    await self._try(
                        AgentActivities.update_card, spec, task_id, card_ts, "Not confirmed"
                    )
                    await self._notice(
                        task_notices.NOT_CONFIRMED_COMMENT,
                        f"{task_id}:failed",
                        task_id,
                        url=item_url,
                    )
                    self._state = "failed"
                    await self._record()
                    return self._state
            else:
                reason = _reason(err)
                await self._try(
                    AgentActivities.update_card, spec, task_id, card_ts, f"Failed: {reason}"
                )
                await self._notice(
                    task_notices.FAILED_COMMENT,
                    f"{task_id}:failed",
                    task_id,
                    subject=title,
                    reason=reason,
                )
                self._state = "failed"
                await self._record()
                return self._state
        # The comment landed. The DM runs once; whatever happens to it, the comment
        # is never attempted again. A nudge with no Slack match has no DM to run.
        dm_ts = ""
        if spec.nudge_dm:
            try:
                dm_ts = await self._act_write(AgentActivities.nudge_dm, spec, task_id)
            except ActivityError as err:
                if workflow.patched(TENANT_DISABLED_PATCH) and is_tenant_disabled(err):
                    return await self._stop_for_tenant_disabled(spec, task_id)
                await self._try(
                    AgentActivities.update_card, spec, task_id, card_ts, "Commented, DM failed"
                )
                await self._notice(
                    task_notices.NUDGE_NO_DM, f"{task_id}:nudge-no-dm", task_id, subject=title
                )
                await self._report_nudge_done(spec, task_id)
                self._state = "done"
                await self._record()
                return self._state
        await self._try(
            AgentActivities.update_card, spec, task_id, card_ts, "Nudged" if dm_ts else "Commented"
        )
        await self._notice(
            task_notices.NUDGED if dm_ts else task_notices.NUDGE_COMMENTED,
            f"{task_id}:nudged",
            task_id,
            subject=title,
        )
        await self._report_nudge_done(spec, task_id)
        self._state = "done"
        await self._record()
        return self._state

    async def _report_nudge_done(self, spec: TaskSpec, task_id: str) -> None:
        """Tell the watch the nudge's comment landed, so it restarts the stale clock.

        Best-effort: the watch may have ended while the card waited (the item is done
        or the watch ran out); the nudge still ran, and the report is then dropped.
        """
        if not spec.parent_id:
            return
        try:
            await workflow.get_external_workflow_handle(spec.parent_id).signal(
                "nudge_done", NudgeDone(task_id)
            )
        except Exception:  # noqa: BLE001 - the report is best-effort; the nudge already ran
            workflow.logger.warning(
                "nudge report not delivered tenant_id=%s task_id=%s parent_id=%s",
                spec.ctx.tenant_id,
                task_id,
                spec.parent_id,
            )

    async def _store_edit(self, e: EditRequest) -> None:
        """Store the saved edit as the next revision of the draft, then redraw the card.

        The new revision and its nonce replace the old only once the activity has stored
        it, and a decision is ignored until then (see ``decision``).
        """
        try:
            applied: bool = await self._act(AgentActivities.apply_edit, self._spec, e.edit_id)
        except ActivityError:
            applied = False
        self._edit_pending = None
        if not applied:
            workflow.logger.warning("ignored edit: the stored values were not accepted")
            return
        self._spec = dataclasses.replace(
            self._spec,
            revision=self._spec.revision + 1,
            approval_nonce=str(workflow.uuid4()),
        )
        self._edits += 1
        self._refresh_due = True

    async def _notice(
        self,
        kind: str,
        idem_key: str,
        task_id: str,
        *,
        subject: str = "",
        reason: str = "",
        url: str = "",
        status: str = "",
        days: int = 0,
    ) -> None:
        """Post one of the task's thread lines; a failure to post does not end the task.

        A task with a stored draft names only the kind: ``post_task_notice`` reads the title
        or comment it mentions. A task that carries its payload writes the line here, from
        ``subject`` and ``url``.
        """
        ctx = self._spec.ctx
        if self._refs:
            await self._try(
                AgentActivities.post_task_notice,
                self._spec,
                task_id,
                kind,
                idem_key,
                reason,
                status,
                days,
            )
            return
        text = task_notices.notice_text(
            kind,
            subject=subject or _subject(self._spec),
            reason=reason,
            url=url,
            status=status,
            requester=ctx.requester_user_id,
            days=days,
        )
        await self._try(AgentActivities.post_in_thread, ctx, text, idem_key)

    async def _record(self, status: str = "") -> None:
        """Record the task's state in the platform's store, for the asker's ``my_tasks``.

        Best-effort: a failed record never ends the task. The activity reads the item's
        title and link from the stored draft or the write's result; the workflow carries
        only its state, the status and the time. Histories recorded before the store
        existed never record (the patch marker is absent).
        """
        if not workflow.patched(TASK_RECORD_PATCH):
            return
        await self._try(
            AgentActivities.record_task_state,
            self._spec,
            workflow.info().workflow_id,
            self._state,
            status,
            workflow.now(),
        )

    async def _try(self, method, *args) -> None:
        """Run an activity whose failure must not end the task (a card redraw, a notice)."""
        try:
            await self._act(method, *args)
        except ActivityError as err:
            error_type, slack_code = _failure_cause(err)
            workflow.logger.warning(
                "activity %s failed after retries; continuing tenant_id=%s task_id=%s error=%s slack_code=%s",
                method.__name__,
                self._spec.ctx.tenant_id,
                workflow.info().workflow_id,
                error_type,
                slack_code,
            )

    async def _act_write(self, method, *args):
        """Run an approved write. New histories heartbeat it, so a stuck attempt is cancelled."""
        if not workflow.patched(RECONCILE_PATCH):
            return await self._act(method, *args)
        return await workflow.execute_activity_method(
            method,
            args=list(args),
            start_to_close_timeout=WRITE_TIMEOUT,
            heartbeat_timeout=WRITE_HEARTBEAT_TIMEOUT,
            retry_policy=RETRY,
        )

    async def _reconcile(self, method, spec: TaskSpec, task_id: str):
        """Look for a write whose retries ran out: a few tries, a timer between them.

        Returns what the activity found, or None when no look found it. A look that fails
        counts as not found: the answer is then "could not confirm", never "not filed".
        """
        for attempt in range(RECONCILE_TRIES):
            if attempt:
                await workflow.sleep(RECONCILE_INTERVAL)
            try:
                found = await workflow.execute_activity_method(
                    method,
                    args=[spec, task_id],
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=RECONCILE_RETRY,
                )
            except ActivityError:
                continue
            if found is not None:
                return found
        return None

    async def _act(self, method, *args):
        return await workflow.execute_activity_method(
            method,
            args=list(args),
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )


def _hop_due() -> bool:
    """True when this run's history is long enough that the watch should start a new run."""
    info = workflow.info()
    return info.is_continue_as_new_suggested() or info.get_current_history_length() >= (
        WATCH_HISTORY_BOUND
    )


def _subject(spec: TaskSpec) -> str:
    """The title a task that carries its payload is about (empty for one with a stored draft)."""
    return task_notices.subject(spec.tool_name, spec.payload)


def _is_permanent(err: ActivityError) -> bool:
    """True when a retry cannot help: the activity said so, rather than running out of tries."""
    cause = err.cause
    return isinstance(cause, ApplicationError) and cause.non_retryable


def _is_agent_off(err: ActivityError) -> bool:
    """True when the activity refused because an admin turned the agent off."""
    cause = err.cause
    return isinstance(cause, ApplicationError) and cause.type == "AgentOff"


def _is_agent_unavailable(err: ActivityError) -> bool:
    """True when the activity refused because the agent's grant check failed.

    A watch never matches this: its poll fails retryably, so the watch keeps polling
    and resumes when a check passes. The draft, card and write paths stop clean.
    """
    cause = err.cause
    return isinstance(cause, ApplicationError) and cause.type == "AgentUnavailable"


def _reason(err: ActivityError) -> str:
    cause = err.cause
    if isinstance(cause, ApplicationError):
        return cause.message
    return str(cause or err)


def _failure_cause(err: ActivityError) -> tuple[str, str]:
    """The failure's exception type and, for a Slack API failure, Slack's error code.

    The innermost application error in the cause chain names the type. A Slack code is
    read out of its message, which is never logged itself: it can quote card or payload
    text. Only names, IDs and codes reach the log.
    """
    app: ApplicationError | None = None
    cause: BaseException | None = err.cause
    while cause is not None:
        if isinstance(cause, ApplicationError):
            app = cause
        cause = cause.__cause__
    if app is None:
        return type(err.cause or err).__name__, ""
    error_type = app.type or type(app).__name__
    code = ""
    if error_type == "SlackApiError":
        match = re.search(r"'error': '([a-z_]+)'", app.message)
        code = match.group(1) if match else ""
    return error_type, code


def _done_values(agent: AgentVersion, database_id: str) -> list[str]:
    for grant in agent.grants.notion_databases:
        if normalize_id(grant.id) == normalize_id(database_id):
            return list(grant.done_values)
    return []
