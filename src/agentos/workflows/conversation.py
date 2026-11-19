"""ConversationWorkflow: one per Slack thread, running the agent's turn loop.

Rules this workflow keeps:

* History stays small and free of content. Chat history lives in Postgres; the
  workflow carries the conversation reference, an inbox of pending messages (IDs, no
  text) and the recently seen event IDs. What a person wrote, what the model replied,
  each tool call's input and a task's payload are stored by the activities that handle
  them, and the workflow carries only references and values derived from them (whether a
  message had text, a tool call's name and an input hash, a draft revision). Each turn
  reloads history inside an activity. Tool output text is stored by the activity that ran
  the tool, and the workflow carries only references to it.
* The legacy path. A message signalled before content left history carries its text, and
  its turn is run the way it was then (``call_llm`` and the activities that take content as
  arguments), so a history recorded then replays. A message from the gateway sets
  ``text_stored`` and runs the content-free path, behind the ``content-refs`` marker. A
  turn is entirely one or the other.
* Links are never written by the model. The model cites sources by number;
  the workflow carries only the numbers and page IDs (citations), and the
  reply activity builds the Sources line from stored page metadata.
  Citation numbers are unique for the life of the thread (they carry over
  turns and continue-as-new), so a number cited from an earlier turn still
  means the same page. An answer that cites none of the passages its turn's
  searches returned still shows where it searched: the workflow passes the
  turn's own citations to the reply activity (behind the ``searched-footer``
  marker), which names those pages in a Searched line instead.
* Identity comes from the runtime. The requester is the Slack user who sent the
  message, put into ``TurnContext`` here, never anything the LLM said.
* Writes never run here. A call to a write tool (the connector manifest's ``risk`` says
  which tools those are, and ``call_llm`` marks each call with it) starts a separate
  TaskWorkflow, which asks the requester to approve before anything is created.
  The task is abandoned (not cancelled) when this conversation closes, because
  the work outlives the conversation. One reply may start at most the agent's
  ``max_writes_per_turn`` tasks; the other write calls, and a write repeated in the
  same reply, get an error result. Input that does not fit a tool's schema gets one too.
* A reply the model cut off at its token limit is posted with a note when it is text,
  and runs no tool when it has tool calls: each gets an error result asking for less.
* A reply is stored in history only after it was shown (``finalize_model_reply`` does both,
  in that order), so a Slack failure never leaves history claiming the user saw an answer.
* A front door hands a thread to one of its configured specialists with the
  ``hand_off`` tool. Nothing outside the workflow runs for it. The specialist
  answers the same message, announces itself, and owns the rest of the thread:
  the thread's agent is workflow state (``self.ref``) and carries over
  continue-as-new, and it is also stored per thread, so a workflow that closed after a
  week of silence and starts again keeps the specialist. A specialist cannot hand off, and other tool calls in the
  hand-off response are not run.
* An asker outside the thread's agent's audience is told so and nothing else runs: a
  turn starts by checking the asker and the place against the agent (behind a workflow
  version marker), a refused turn stores no history and calls no model or tool, and a
  check that cannot be completed refuses too. A hand-off is checked again before the
  thread is switched, and a refused one leaves the thread with the front door.
* A failed turn is reported in the thread and never ends the conversation: the
  placeholder is replaced with a short error, history is left valid, and the
  workflow goes on to wait for the next message.
* While a tool step runs long, the placeholder shows that step's label, fixed
  runtime text built from the agent's grants and config, never the model's words
  or a tool's input. Labels are throttled in workflow time and awaited inside the
  step, and a failed edit is dropped. A dropped edit may still reach Slack after
  the reply, so once one is dropped the turn's final text is posted as a new
  message in the thread instead of editing the placeholder.
* Workflow code is deterministic: no I/O, ``workflow.uuid4()`` for IDs.
"""

import asyncio
import json
from collections import deque
from collections.abc import Awaitable
from datetime import datetime, timedelta
from typing import TypeVar

from temporalio import workflow
from temporalio.converter import value_to_type
from temporalio.exceptions import ActivityError, ChildWorkflowError, WorkflowAlreadyStartedError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from agentos import ids
    from agentos.activities import HAND_OFF_TOOL, AgentActivities
    from agentos.audience import (
        UNAVAILABLE,
        AudienceDecision,
        decision_text,
    )
    from agentos.config import AgentVersion
    from agentos.knowledge.search import SEARCH_RESULTS
    from agentos.models import (
        CUT_SHORT,
        EDITABLE_FIELDS,
        HAND_OFF_ANNOUNCEMENT,
        NO_ANSWER,
        OUT_OF_STEPS,
        READ_KIND,
        TURN_FAILED,
        Citation,
        ConversationRef,
        DraftOutcome,
        InboundMessage,
        LLMResponse,
        StepResult,
        TaskSpec,
        ToolCall,
        ToolCallRef,
        ToolResult,
        TurnContext,
        check_field,
    )
    from agentos.slack_format import escape_slack
    from agentos.workflows import (
        ACTIVITY_TIMEOUT,
        LLM_TIMEOUT,
        PROGRESS_RETRY,
        PROGRESS_TIMEOUT,
        RETRY,
    )
    from agentos.workflows.task import TaskWorkflow

IDLE_TIMEOUT = timedelta(days=7)
SEEN_LIMIT = 500
MAX_CITATIONS = 200
TOOL_NOT_COMPLETED = "The tool call did not complete."
STEP_LIMIT_REACHED = "step limit reached"
HAND_OFF_ACCEPTED = "Handed to {agent}. They will answer now."
HAND_OFF_REFUSED = "{agent} is not one of this agent's specialists."
HAND_OFF_EXTRA = "Only one hand-off is allowed per turn."
HAND_OFF_NOT_RUN = "Not run: the conversation was handed to a specialist."
HAND_OFF_UNAVAILABLE = (
    "{agent} is not available to this user here, so the thread was not handed over."
)
HAND_OFF_PREFIX = "h1:"
MAX_TOKENS_STOP = "max_tokens"
CUT_OFF = "Your reply was cut off. Try again with less."
DUPLICATE_WRITE = "Not run: identical to an earlier call in this reply."
WRITE_LIMIT = (
    "Not started: at most {limit} writes per reply. Tell the user what is left and ask "
    "whether to go on."
)
# Tools this workflow ran before the connector manifests said what a call is.
LEGACY_READ_TOOLS = ("search_items", "my_items", "search_knowledge")
# workflow.patched IDs: each guards a change to the commands a turn issues, so
# a conversation that is replayed past an older turn still matches its history.
PATCH_PARTIAL_TOOL_RESULTS = "close-only-failed-tool-calls"
PATCH_STEP_LIMIT = "no-tools-on-last-step"
PATCH_EARLY_FAILURE_NOTICE = "early-failure-notice"
PATCH_AUDIENCE = "audience-check"
PATCH_EMPTY_MESSAGE = "empty-message-reply"
PATCH_CHILD_FAILURE = "recover-from-child-failure"
PATCH_TOOL_INPUT = "tool-input-errors"
PATCH_WRITE_LIMIT = "write-limit-per-turn"
PATCH_MANIFEST_KINDS = "manifest-tool-kinds"
PATCH_CUT_OFF = "cut-off-replies"
PATCH_STORE_AFTER_SHOWN = "store-reply-after-shown"
PATCH_THREAD_AGENT = "thread-agent-row"
# A turn whose message text is in ``inbound_events``: the content-free commands (see the module doc).
PATCH_CONTENT_REFS = "content-refs"
PATCH_DISCARD_INBOUND = "discard-unconsumed-inbound"
PATCH_CONTIGUOUS_MARKERS = "contiguous-citation-markers"
PATCH_NOTICE_AUDIT = "notice-audit"
PATCH_HANDOFF_REFUSAL_AGENT = "handoff-refusal-specialist"
PATCH_PROGRESS = "progress"
PATCH_PROGRESS_DROPPED = "progress-dropped"
PATCH_SEARCHED_FOOTER = "searched-footer"
EMPTY_MESSAGE_REPLY = "What would you like to ask?"

_T = TypeVar("_T")

# Progress on the placeholder: a step must run this long before its label shows,
# and at most one label lands per this interval across the turn's steps. Both are
# workflow time, never real time.
PROGRESS_AFTER = timedelta(seconds=2)
PROGRESS_GAP = timedelta(seconds=3)
# The labels are fixed runtime text. Titles come from the agent's grants or what
# the runtime already knows, never from the model or a tool's input.
SEARCHING_SOURCES = "Searching the knowledge sources…"
READING_NOTION = "Reading Notion…"
DRAFTING_REQUEST = "Drafting the request…"
HANDING_OVER = "Handing over to {agent}…"


def progress_label(
    name: str,
    kind: str,
    *,
    source_titles: list[str] | None = None,
    agent_name: str = "",
) -> str | None:
    """The placeholder text while ``name`` runs, or None for a tool with no label.

    ``source_titles`` are the agent's granted knowledge pages, ``agent_name`` the specialist a
    hand-off names. Each is escaped for Slack mrkdwn here, once, at the point of use. A Notion
    read shows the fixed "Reading Notion…": the runtime never knows the page title before the
    read returns, and the label must not wait on it.
    """
    if name == "search_knowledge":
        titles = source_titles or []
        if len(titles) == 1:
            return f"Searching {escape_slack(titles[0])}…"
        return SEARCHING_SOURCES
    if name == HAND_OFF_TOOL:
        return HANDING_OVER.format(agent=escape_slack(agent_name))
    if kind == READ_KIND:
        return READING_NOTION
    if kind in EDITABLE_FIELDS:
        return DRAFTING_REQUEST
    return None


class _TurnState:
    """What a turn has done so far, so a failure can be cleaned up."""

    def __init__(self) -> None:
        self.ctx: TurnContext | None = None
        self.placeholder_ts: str | None = None
        self.pending_tool_ids: list[str] = []
        self.event_id: str = ""
        # True once the final reply is in history; recovery then stores no
        # second assistant turn.
        self.reply_stored = False
        # True once the thread has been handed to a specialist in this turn.
        self.handed_off = False
        # True for a turn run on the content-free path: it needs no marker of an older change.
        self.refs = False
        # True once ingest_message returned: it consumed the inbound row either way.
        self.ingested = False
        # When the turn last showed a progress label; the throttle reads it.
        self.last_progress: datetime | None = None
        # True once a progress edit on this placeholder was dropped: a late attempt may still
        # land on it after the reply, so the final text is posted as a new message instead.
        self.progress_dropped = False
        # The citations this turn's searches kept, so the reply can name the pages it
        # searched even when the answer cites none of them. References only, like citations.
        self.searched: list[Citation] = []


@workflow.defn(name="ConversationWorkflow")
class ConversationWorkflow:
    def __init__(self) -> None:
        self.inbox: deque[InboundMessage] = deque()
        self.seen: deque[str] = deque(maxlen=SEEN_LIMIT)
        # Citation numbers are unique for the life of the thread, so a number
        # the model cites from an earlier turn still means the same page. Each
        # search numbers its kept passages from the next free marker, so the
        # numbers run 1..n with no gaps (PATCH_CONTIGUOUS_MARKERS); histories
        # recorded before that patch reserved a block of SEARCH_RESULTS per
        # search instead. Only references are kept (number, scope, page ID),
        # the latest MAX_CITATIONS of them.
        self.next_marker = 1
        self.citations: list[Citation] = []
        # The thread's agent. It starts as the agent the workflow was started
        # with and changes once, when a front door hands the thread over.
        self.ref: ConversationRef | None = None

    @workflow.signal(name="message")
    def message(self, m: InboundMessage) -> None:
        # Slack retries deliveries; the event ID makes each message count once.
        if m.event_id in self.seen:
            return
        self.seen.append(m.event_id)
        self.inbox.append(m)

    @workflow.run
    async def run(
        self,
        ref: ConversationRef,
        inbox: list[InboundMessage] | None = None,
        seen: list[str] | None = None,
        next_marker: int = 1,
        citations: list[Citation] | None = None,
    ) -> None:
        # Temporal applies these type hints only when every parameter was sent.
        # The gateway starts the workflow with ``ref`` alone, so convert here.
        ref = _typed(ConversationRef, ref)
        if not workflow.info().continued_run_id and workflow.patched(PATCH_THREAD_AGENT):
            # A thread handed to a specialist stays with it when this workflow starts again
            # after a week of silence. A continue-as-new already carries the thread's agent.
            ref = await self._thread_agent(ref)
        self.ref = ref
        inbox = _typed(list[InboundMessage], inbox) if inbox else None
        seen = _typed(list[str], seen) if seen else None
        citations = _typed(list[Citation], citations) if citations else None
        self.next_marker = max(self.next_marker, next_marker)
        if citations:
            self.citations = (citations + self.citations)[-MAX_CITATIONS:]

        # State carried over a continue-as-new comes first; anything signalled
        # since then was appended after it.
        if seen:
            carried = deque(seen, maxlen=SEEN_LIMIT)
            for event_id in self.seen:
                if event_id not in carried:
                    carried.append(event_id)
            self.seen = carried
        if inbox:
            self.inbox = deque(list(inbox) + list(self.inbox))

        while True:
            try:
                await workflow.wait_condition(lambda: bool(self.inbox), timeout=IDLE_TIMEOUT)
            except TimeoutError:
                # Idle for a week: close. Tasks keep running, and a new message
                # starts this workflow again with signal-with-start.
                return
            m = self.inbox.popleft()
            await self._turn(m)

            if workflow.info().is_continue_as_new_suggested():
                await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(
                    args=[
                        self.ref,
                        list(self.inbox),
                        list(self.seen),
                        self.next_marker,
                        list(self.citations),
                    ]
                )

    async def _turn(self, m: InboundMessage) -> None:
        """Run one turn. A failed activity is reported, never raised."""
        ref = self._ref()
        conv_id = ids.conversation_id(ref.tenant_id, ref.channel, ref.thread_ts)
        state = _TurnState()
        try:
            await self._run_turn(m, conv_id, state)
        except ActivityError as err:
            await self._fail_turn(err, ref, m, conv_id, state)
        except (ChildWorkflowError, WorkflowAlreadyStartedError) as err:
            # A task that could not be started is a failed turn like a failed activity.
            if not (state.refs or workflow.patched(PATCH_CHILD_FAILURE)):
                raise
            await self._fail_turn(err, ref, m, conv_id, state)
        if state.refs and not state.ingested and workflow.patched(PATCH_DISCARD_INBOUND):
            try:
                await workflow.execute_activity_method(
                    AgentActivities.discard_message,
                    args=[ref.tenant_id, m.event_id],
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=RETRY,
                )
            except ActivityError as err:
                self._log_failure(ref, err, "discard inbound")

    async def _fail_turn(
        self,
        err: Exception,
        ref: ConversationRef,
        m: InboundMessage,
        conv_id: str,
        state: "_TurnState",
    ) -> None:
        # A hand-off during the turn changes the thread's agent, so log
        # and recover as whichever agent was running.
        self._log_failure(self._ref(), err)
        if state.ctx is None or state.placeholder_ts is None:
            # No placeholder to replace, so the user would get no reply at
            # all: post the failure notice as a new message instead.
            if state.refs or workflow.patched(PATCH_EARLY_FAILURE_NOTICE):
                await self._notify_early_failure(ref, m)
            return
        await self._recover(conv_id, state)

    async def _thread_agent(self, ref: ConversationRef) -> ConversationRef:
        """The thread's own agent: its specialist when it was handed to one, else ``ref``'s."""
        try:
            agent_id: str = await workflow.execute_activity_method(
                AgentActivities.thread_agent,
                args=[ref.tenant_id, ref.agent_id, ref.channel, ref.thread_ts],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
        except ActivityError as err:
            self._log_failure(ref, err, "thread agent")
            return ref
        return ConversationRef(ref.tenant_id, agent_id, ref.channel, ref.thread_ts)

    def _ref(self) -> ConversationRef:
        assert self.ref is not None
        return self.ref

    async def _notify_early_failure(self, ref: ConversationRef, m: InboundMessage) -> None:
        # The agent may be gone from the config, so there may be no agent
        # version; the activity only needs the tenant, channel and thread.
        ctx = TurnContext(
            tenant_id=ref.tenant_id,
            agent_id=ref.agent_id,
            agent_version=0,
            requester_user_id=m.user_id,
            channel=ref.channel,
            thread_ts=ref.thread_ts,
            channel_kind=m.channel_kind,
        )
        try:
            if workflow.patched(PATCH_NOTICE_AUDIT):
                await workflow.execute_activity_method(
                    AgentActivities.post_notice_in_thread,
                    args=[ctx, TURN_FAILED, f"{m.event_id}:err", "failure"],
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=RETRY,
                )
            else:
                await workflow.execute_activity_method(
                    AgentActivities.post_in_thread,
                    args=[ctx, TURN_FAILED, f"{m.event_id}:err"],
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=RETRY,
                )
        except ActivityError as err:
            self._log_failure(ref, err, "failure notice")

    def _log_failure(self, ref: ConversationRef, err: Exception, step: str = "turn") -> None:
        # IDs and the activity type only; never message text.
        workflow.logger.warning(
            "%s failed tenant_id=%s agent_id=%s workflow_id=%s activity=%s",
            step,
            ref.tenant_id,
            ref.agent_id,
            workflow.info().workflow_id,
            err.activity_type if isinstance(err, ActivityError) else type(err).__name__,
        )

    async def _recover(self, conv_id: str, state: "_TurnState") -> None:
        """Leave valid history and tell the user. Each step is best effort."""
        ref = self._ref()
        ctx = state.ctx
        placeholder_ts = state.placeholder_ts
        assert ctx is not None and placeholder_ts is not None
        try:
            if state.pending_tool_ids:
                # Every tool_use needs a matching tool_result, or each later
                # model call in this thread would be rejected.
                await self._append(
                    ctx,
                    conv_id,
                    f"{state.event_id}:recover:results",
                    "user",
                    [
                        {
                            "type": "tool_result",
                            "tool_use_id": tool_use_id,
                            "content": TOOL_NOT_COMPLETED,
                            "is_error": True,
                        }
                        for tool_use_id in state.pending_tool_ids
                    ],
                )
        except ActivityError as err:
            self._log_failure(ref, err, "recovery")
        if not state.reply_stored:
            try:
                await self._append(
                    ctx,
                    conv_id,
                    f"{state.event_id}:recover:assistant",
                    "assistant",
                    [{"type": "text", "text": TURN_FAILED}],
                )
            except ActivityError as err:
                self._log_failure(ref, err, "recovery")
        try:
            if workflow.patched(PATCH_NOTICE_AUDIT):
                await self._finalize(
                    state,
                    AgentActivities.finalize_notice,
                    AgentActivities.post_final_notice,
                    [ctx, placeholder_ts, TURN_FAILED, "failure"],
                )
            else:
                await self._finalize(
                    state,
                    AgentActivities.finalize_reply,
                    AgentActivities.post_final_reply,
                    [ctx, placeholder_ts, TURN_FAILED, [], False],
                )
        except ActivityError as err:
            self._log_failure(ref, err, "recovery")

    async def _run_turn(self, m: InboundMessage, conv_id: str, state: "_TurnState") -> None:
        ref = self._ref()
        # A message whose text the gateway kept in Postgres runs the content-free path. The
        # marker is asked only then, so a history recorded before it replays without it.
        state.refs = m.text_stored and workflow.patched(PATCH_CONTENT_REFS)
        refs = state.refs
        agent: AgentVersion = await self._load_agent(ref.tenant_id, ref.agent_id)
        ctx = self._turn_context(ref, agent, m)
        state.event_id = m.event_id
        if (refs or workflow.patched(PATCH_AUDIENCE)) and not await self._audience_allows(
            agent, ctx, m
        ):
            return
        if refs:
            has_text: bool = await workflow.execute_activity_method(
                AgentActivities.ingest_message,
                args=[ctx.tenant_id, conv_id, m.event_id],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
            state.ingested = True
            if not has_text:
                await self._reply_empty_message(ctx, m)
                return
        else:
            if not m.text.strip() and workflow.patched(PATCH_EMPTY_MESSAGE):
                # A bare mention: nothing to answer, and an empty text block stored in
                # history would be rejected by the model on every later call.
                await self._reply_empty_message(ctx, m)
                return
            await self._append(
                ctx, conv_id, f"{m.event_id}:user", "user", [{"type": "text", "text": m.text}]
            )
        placeholder_ts: str = await workflow.execute_activity_method(
            AgentActivities.post_placeholder,
            args=[ctx, f"{m.event_id}:ph"],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        state.ctx = ctx
        state.placeholder_ts = placeholder_ts

        # Keys of the specialist's appends carry a prefix, so they never
        # collide with the front door's for the same message.
        key_prefix = ""
        step = 0
        while step < agent.max_steps:
            key = f"{m.event_id}:{key_prefix}s{step}"
            calls: list[ToolCall] | list[ToolCallRef]
            if refs:
                outcome: StepResult = await workflow.execute_activity_method(
                    AgentActivities.call_model,
                    args=[ctx, conv_id, key],
                    start_to_close_timeout=LLM_TIMEOUT,
                    retry_policy=RETRY,
                )
                calls, stop_reason = outcome.tool_calls, outcome.stop_reason
            else:
                resp: LLMResponse = await workflow.execute_activity_method(
                    AgentActivities.call_llm,
                    args=[ctx, conv_id],
                    start_to_close_timeout=LLM_TIMEOUT,
                    retry_policy=RETRY,
                )
                calls, stop_reason = resp.tool_calls, resp.stop_reason
            # The model stopped at its token limit: text may end mid-sentence and a tool
            # call's input may be missing its end.
            cut_off = stop_reason == MAX_TOKENS_STOP and (refs or workflow.patched(PATCH_CUT_OFF))
            if not calls:
                if refs:
                    await self._reply_stored(
                        ctx, state, placeholder_ts, key, cut_off and outcome.has_text
                    )
                    return
                text = resp.text or NO_ANSWER
                content = list(resp.content) or [{"type": "text", "text": text}]
                if not any(b.get("type") == "text" for b in content):
                    content.append({"type": "text", "text": text})
                shown = text + CUT_SHORT if cut_off and resp.text else text
                await self._reply(
                    ctx, conv_id, state, placeholder_ts, f"{key}:assistant", content, shown, True
                )
                return

            if not refs:
                await self._append(
                    ctx, conv_id, f"{key}:assistant", "assistant", _assistant_blocks(resp)
                )
            # From here until the results are stored, history has tool_use
            # blocks with no tool_result; a failure must close them.
            state.pending_tool_ids = [call.id for call in calls]
            # One hand-off per turn, whatever the loaded agent lists. A cut-off reply
            # runs nothing, a hand-off included.
            hand_off = None if state.handed_off or cut_off else _accepted_hand_off(agent, calls)
            unavailable: ToolResult | None = None
            if hand_off is not None and (refs or workflow.patched(PATCH_AUDIENCE)):
                unavailable = await self._unreachable_hand_off(
                    ctx, m, hand_off, _hand_off_target(agent, hand_off) or ""
                )
                if unavailable is not None:
                    hand_off = None
            # A hand-off starts nothing outside the workflow and the specialist
            # has its own step budget, so the last step may still hand off.
            if (
                step == agent.max_steps - 1
                and hand_off is None
                and (refs or workflow.patched(PATCH_STEP_LIMIT))
            ):
                # No step is left to use the results, so nothing runs: a write
                # tool started now would act with nobody to report it.
                await workflow.execute_activity_method(
                    AgentActivities.append_tool_results,
                    args=[
                        ctx.tenant_id,
                        conv_id,
                        f"{key}:results",
                        [ToolResult(call.id, STEP_LIMIT_REACHED, is_error=True) for call in calls],
                    ],
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=RETRY,
                )
                state.pending_tool_ids = []
                await self._reply(
                    ctx,
                    conv_id,
                    state,
                    placeholder_ts,
                    f"{key}:out-of-steps",
                    [{"type": "text", "text": OUT_OF_STEPS}],
                    OUT_OF_STEPS,
                    False,
                )
                return
            if cut_off:
                await workflow.execute_activity_method(
                    AgentActivities.append_tool_results,
                    args=[
                        ctx.tenant_id,
                        conv_id,
                        f"{key}:results",
                        [ToolResult(call.id, CUT_OFF, is_error=True) for call in calls],
                    ],
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=RETRY,
                )
                state.pending_tool_ids = []
                step += 1
                continue
            if hand_off is not None:
                # Another call beside the hand-off is not run: a write would
                # start as the front door while the user is told a specialist
                # took over. The specialist is loaded and announced before the
                # results are stored, so a failure there leaves the thread with
                # the front door and history that never claims a hand-off.
                target = _hand_off_target(agent, hand_off) or ""
                results = [
                    ToolResult(call.id, HAND_OFF_ACCEPTED.format(agent=target))
                    if call is hand_off
                    else ToolResult(call.id, HAND_OFF_NOT_RUN, is_error=True)
                    for call in calls
                ]
                agent, ctx, placeholder_ts = await self._hand_off(m, state, target)
                await workflow.execute_activity_method(
                    AgentActivities.append_tool_results,
                    args=[ctx.tenant_id, conv_id, f"{key}:results", results],
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=RETRY,
                )
                state.pending_tool_ids = []
                key_prefix = HAND_OFF_PREFIX
                step = 0
                continue
            refused = self._refused_calls(agent, calls)
            if workflow.patched(PATCH_CONTIGUOUS_MARKERS):
                outcomes = await self._run_calls(
                    ctx, state, agent, conv_id, calls, refused, unavailable
                )
            else:
                first_markers: list[int] = []
                for call in calls:
                    first_markers.append(self.next_marker)
                    if call.name == "search_knowledge":
                        self.next_marker += SEARCH_RESULTS
                outcomes = await asyncio.gather(
                    *[
                        self._dispatch(ctx, state, agent, conv_id, call, first_marker, unavailable)
                        if i not in refused
                        else _resolved(refused[i])
                        for i, (call, first_marker) in enumerate(zip(calls, first_markers))
                    ],
                    return_exceptions=True,
                )
            failed = [o for o in outcomes if isinstance(o, BaseException)]
            if failed:
                if refs or workflow.patched(PATCH_PARTIAL_TOOL_RESULTS):
                    # A call that succeeded may already have had an effect (a
                    # write task started, a card sent): keep its result and
                    # close only the calls that failed.
                    await workflow.execute_activity_method(
                        AgentActivities.append_tool_results,
                        args=[
                            ctx.tenant_id,
                            conv_id,
                            f"{key}:results",
                            [
                                ToolResult(call.id, TOOL_NOT_COMPLETED, is_error=True)
                                if isinstance(outcome, BaseException)
                                else outcome
                                for call, outcome in zip(calls, outcomes)
                            ],
                        ],
                        start_to_close_timeout=ACTIVITY_TIMEOUT,
                        retry_policy=RETRY,
                    )
                    state.pending_tool_ids = []
                raise failed[0]
            results: list[ToolResult] = list(outcomes)  # type: ignore[arg-type]
            for result in results:
                self.citations.extend(result.citations)
                state.searched.extend(result.citations)
            del self.citations[:-MAX_CITATIONS]
            await workflow.execute_activity_method(
                AgentActivities.append_tool_results,
                args=[ctx.tenant_id, conv_id, f"{key}:results", results],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
            state.pending_tool_ids = []
            step += 1

        if workflow.patched(PATCH_NOTICE_AUDIT):
            await self._finalize(
                state,
                AgentActivities.finalize_notice,
                AgentActivities.post_final_notice,
                [ctx, placeholder_ts, OUT_OF_STEPS, "step_limit"],
            )
        else:
            await self._finalize(
                state,
                AgentActivities.finalize_reply,
                AgentActivities.post_final_reply,
                [ctx, placeholder_ts, OUT_OF_STEPS, [], False],
            )

    async def _finalize(self, state: "_TurnState", activity, post_activity, args: list) -> None:
        """Show the turn's final text. ``args[1]`` is the placeholder ts in every finalize
        activity. When a progress edit was dropped, a late attempt may still land on the
        placeholder, so the text is posted as a new message (``post_activity``, once under a key
        derived from the placeholder) instead of edited onto it."""
        if state.progress_dropped:
            activity = post_activity
            args = [*args, ids.slack_final_post_key(args[1])]
        await workflow.execute_activity_method(
            activity, args=args, start_to_close_timeout=ACTIVITY_TIMEOUT, retry_policy=RETRY
        )

    async def _reply(
        self,
        ctx: TurnContext,
        conv_id: str,
        state: "_TurnState",
        placeholder_ts: str,
        step_key: str,
        content: list[dict],
        shown: str,
        model_answer: bool,
    ) -> None:
        """Show the turn's final reply, and keep it in history once it has been shown.

        ``content`` is what history keeps and ``shown`` what the user reads (it may carry a
        note history does not). Stored first, history would claim the user got an answer
        Slack then refused to post; a reply that was not shown is not stored, and recovery
        stores the failure notice in its place.
        """
        citations = list(self.citations) if model_answer else []
        if not (state.refs or workflow.patched(PATCH_STORE_AFTER_SHOWN)):
            await self._append(ctx, conv_id, step_key, "assistant", content)
            state.reply_stored = True
        if not model_answer and workflow.patched(PATCH_NOTICE_AUDIT):
            await self._finalize(
                state,
                AgentActivities.finalize_notice,
                AgentActivities.post_final_notice,
                [ctx, placeholder_ts, shown, "step_limit"],
            )
        elif workflow.patched(PATCH_SEARCHED_FOOTER):
            await self._finalize(
                state,
                AgentActivities.finalize_searched_reply,
                AgentActivities.post_final_searched_reply,
                [
                    ctx,
                    placeholder_ts,
                    shown,
                    citations,
                    model_answer,
                    state.searched if model_answer else [],
                ],
            )
        else:
            await self._finalize(
                state,
                AgentActivities.finalize_reply,
                AgentActivities.post_final_reply,
                [ctx, placeholder_ts, shown, citations, model_answer],
            )
        if not state.reply_stored:
            await self._append(ctx, conv_id, step_key, "assistant", content)
            state.reply_stored = True

    async def _reply_empty_message(self, ctx: TurnContext, m: InboundMessage) -> None:
        """Answer a message with no text: ask what the person wants. Nothing is stored."""
        key = ids.empty_message_key(m.event_id, ctx.agent_id)
        if workflow.patched(PATCH_NOTICE_AUDIT):
            await workflow.execute_activity_method(
                AgentActivities.post_notice_in_thread,
                args=[ctx, EMPTY_MESSAGE_REPLY, key, "empty_message"],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
        else:
            await workflow.execute_activity_method(
                AgentActivities.post_in_thread,
                args=[ctx, EMPTY_MESSAGE_REPLY, key],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )

    async def _reply_stored(
        self,
        ctx: TurnContext,
        state: "_TurnState",
        placeholder_ts: str,
        key: str,
        cut_short: bool,
    ) -> None:
        """Show the model's final reply, which ``call_model`` stored under ``key``.

        The activity posts it and then keeps it in history, so a reply that was not shown is
        not stored; recovery stores the failure notice in its place.
        """
        if workflow.patched(PATCH_SEARCHED_FOOTER):
            await self._finalize(
                state,
                AgentActivities.finalize_searched_model_reply,
                AgentActivities.post_final_searched_model_reply,
                [ctx, placeholder_ts, key, list(self.citations), cut_short, state.searched],
            )
        else:
            await self._finalize(
                state,
                AgentActivities.finalize_model_reply,
                AgentActivities.post_final_model_reply,
                [ctx, placeholder_ts, key, list(self.citations), cut_short],
            )
        state.reply_stored = True

    def _kind(self, call: ToolCall | ToolCallRef) -> str:
        """What the call is: "read", the kind of task a write becomes, or "" for no tool.

        The ``call_llm`` activity reads it from the connector manifests. A call recorded before
        it did is told by name, as this workflow did then. A call without its input is
        always marked.
        """
        if isinstance(call, ToolCallRef) or workflow.patched(PATCH_MANIFEST_KINDS):
            return call.kind
        if call.name in LEGACY_READ_TOOLS:
            return READ_KIND
        return call.name if call.name in EDITABLE_FIELDS else ""

    def _refused_calls(
        self, agent: AgentVersion, calls: list[ToolCall] | list[ToolCallRef]
    ) -> dict[int, ToolResult]:
        """The calls that must not run, by position, each with the error the model reads.

        Input that does not fit the tool's schema. A write the model already made in this
        reply (the same tool and input). A write beyond the agent's ``max_writes_per_turn``:
        each is a card in the requester's DMs that waits three days for an answer.
        """
        refused: dict[int, ToolResult] = {}
        for i, call in enumerate(calls):
            if call.problem and (
                isinstance(call, ToolCallRef) or workflow.patched(PATCH_TOOL_INPUT)
            ):
                refused[i] = ToolResult(call.id, call.problem, is_error=True)
        writes = [
            i for i, c in enumerate(calls) if i not in refused and self._kind(c) in EDITABLE_FIELDS
        ]
        refs = bool(calls) and isinstance(calls[0], ToolCallRef)
        if not writes or not (refs or workflow.patched(PATCH_WRITE_LIMIT)):
            return refused
        seen: set[tuple[str, str]] = set()
        started = 0
        for i in writes:
            call = calls[i]
            made = (call.name, _input_key(call))
            if made in seen:
                refused[i] = ToolResult(call.id, DUPLICATE_WRITE, is_error=True)
                continue
            seen.add(made)
            if started >= agent.max_writes_per_turn:
                refused[i] = ToolResult(
                    call.id, WRITE_LIMIT.format(limit=agent.max_writes_per_turn), is_error=True
                )
            else:
                started += 1
        return refused

    async def _audience_allows(
        self, agent: AgentVersion, ctx: TurnContext, m: InboundMessage
    ) -> bool:
        """Whether the asker may use the thread's agent here; otherwise say so and stop the turn.

        The turn then stores nothing and runs no model call or tool. A check that cannot
        be completed refuses too, with its own reply.
        """
        try:
            decision: AudienceDecision = await workflow.execute_activity_method(
                AgentActivities.check_audience,
                args=[ctx],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
        except ActivityError as err:
            self._log_failure(self._ref(), err, "audience check")
            decision = AudienceDecision(False, UNAVAILABLE)
        if decision.allowed:
            return True
        await workflow.execute_activity_method(
            AgentActivities.record_audience_refusal,
            args=[ctx, m.event_id, decision.reason, None],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        text = decision_text(agent.name, decision)
        await workflow.execute_activity_method(
            AgentActivities.post_in_thread,
            args=[ctx, text, ids.audience_refusal_key(m.event_id, ctx.agent_id)],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        return False

    async def _unreachable_hand_off(
        self,
        ctx: TurnContext,
        m: InboundMessage,
        call: ToolCall | ToolCallRef,
        target: str,
    ) -> ToolResult | None:
        """The tool error for a hand-off to a specialist the asker may not use, or None.

        The model is only offered specialists the asker can reach, but a hand-off is
        checked again here before the thread is switched. A refused hand-off leaves the
        thread with the front door; the model is told the specialist is unavailable.
        """
        specialist = await self._load_agent(ctx.tenant_id, target)
        specialist_ctx = self._turn_context(
            ConversationRef(ctx.tenant_id, target, ctx.channel, ctx.thread_ts), specialist, m
        )
        try:
            decision: AudienceDecision = await workflow.execute_activity_method(
                AgentActivities.check_audience,
                args=[specialist_ctx],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
        except ActivityError as err:
            self._log_failure(self._ref(), err, "audience check")
            decision = AudienceDecision(False, UNAVAILABLE)
        if decision.allowed:
            return None
        if workflow.patched(PATCH_HANDOFF_REFUSAL_AGENT):
            refusal_ctx, refusal_target = specialist_ctx, ctx.agent_id
        else:
            refusal_ctx, refusal_target = ctx, target
        await workflow.execute_activity_method(
            AgentActivities.record_audience_refusal,
            args=[refusal_ctx, m.event_id, decision.reason, refusal_target],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        return ToolResult(call.id, HAND_OFF_UNAVAILABLE.format(agent=target), is_error=True)

    async def _load_agent(self, tenant_id: str, agent_id: str) -> AgentVersion:
        return await workflow.execute_activity_method(
            AgentActivities.load_agent,
            args=[tenant_id, agent_id],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )

    @staticmethod
    def _turn_context(ref: ConversationRef, agent: AgentVersion, m: InboundMessage) -> TurnContext:
        return TurnContext(
            tenant_id=ref.tenant_id,
            agent_id=ref.agent_id,
            agent_version=agent.version,
            requester_user_id=m.user_id,
            channel=ref.channel,
            thread_ts=ref.thread_ts,
            channel_kind=m.channel_kind,
        )

    async def _hand_off(
        self, m: InboundMessage, state: "_TurnState", target: str
    ) -> tuple[AgentVersion, TurnContext, str]:
        """Give the thread to the specialist the front door chose.

        The front door's placeholder becomes the announcement and the
        specialist gets a placeholder of its own below it, so the thread reads
        message, announcement, answer. Nothing is switched until the specialist
        is loaded, the announcement is shown and its placeholder is posted, so
        a failure before then leaves the thread with the front door.
        """
        ref = self._ref()
        assert state.ctx is not None and state.placeholder_ts is not None
        specialist = await self._load_agent(ref.tenant_id, target)
        new_ref = ConversationRef(ref.tenant_id, target, ref.channel, ref.thread_ts)
        ctx = self._turn_context(new_ref, specialist, m)
        # Audited before anything is shown, so a failure to record leaves the
        # thread with the front door and nothing announced. While the record runs
        # long, the placeholder says who is taking over; the announcement below
        # replaces that label, never the other way round.
        await self._with_progress(
            state.ctx,
            state,
            progress_label(HAND_OFF_TOOL, "", agent_name=specialist.name),
            workflow.execute_activity_method(
                AgentActivities.record_hand_off,
                args=[state.ctx, target, m.event_id],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            ),
        )
        announcement = HAND_OFF_ANNOUNCEMENT.format(agent=specialist.name)
        if workflow.patched(PATCH_NOTICE_AUDIT):
            await self._finalize(
                state,
                AgentActivities.finalize_notice,
                AgentActivities.post_final_notice,
                [state.ctx, state.placeholder_ts, announcement, "hand_off"],
            )
        else:
            await self._finalize(
                state,
                AgentActivities.finalize_reply,
                AgentActivities.post_final_reply,
                [state.ctx, state.placeholder_ts, announcement, [], False],
            )
        placeholder_ts: str = await workflow.execute_activity_method(
            AgentActivities.post_placeholder,
            args=[ctx, f"{m.event_id}:{HAND_OFF_PREFIX}ph"],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        if state.refs or workflow.patched(PATCH_THREAD_AGENT):
            # So the thread keeps its specialist when this workflow closes and starts again.
            # The thread already shows the hand-off, so a failure to store it is logged only.
            try:
                await workflow.execute_activity_method(
                    AgentActivities.remember_thread_agent,
                    args=[ref.tenant_id, ref.channel, ref.thread_ts, target],
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=RETRY,
                )
            except ActivityError as err:
                self._log_failure(ref, err, "thread agent")
        self.ref = new_ref
        state.ctx = ctx
        state.placeholder_ts = placeholder_ts
        # The specialist's placeholder is new: no progress edit was dropped on it, so its
        # final text may edit it even if the front door's placeholder was left unsafe.
        state.progress_dropped = False
        state.searched = []
        state.handed_off = True
        return specialist, ctx, placeholder_ts

    async def _append(
        self, ctx: TurnContext, conv_id: str, step_key: str, role: str, content: list
    ) -> int:
        return await workflow.execute_activity_method(
            AgentActivities.append_history,
            args=[ctx.tenant_id, conv_id, step_key, role, content],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )

    async def _run_calls(
        self,
        ctx: TurnContext,
        state: "_TurnState",
        agent: AgentVersion,
        conv_id: str,
        calls: list[ToolCall] | list[ToolCallRef],
        refused: dict[int, ToolResult],
        unavailable: ToolResult | None,
    ) -> list[ToolResult | BaseException]:
        """The calls' outcomes in call order, with searches numbered by what they keep.

        Searches run one at a time in call order, each numbered from the thread's next
        free marker, which advances by the citations the search kept. The other calls
        run in parallel. A failed call is an exception in its own position, as
        ``asyncio.gather(return_exceptions=True)`` returns it.
        """
        outcomes: dict[int, ToolResult | BaseException] = {}
        parallel: list[int] = []
        for i, call in enumerate(calls):
            if i in refused:
                outcomes[i] = refused[i]
            elif call.name == "search_knowledge":
                outcome = (
                    await asyncio.gather(
                        self._dispatch(
                            ctx, state, agent, conv_id, call, self.next_marker, unavailable
                        ),
                        return_exceptions=True,
                    )
                )[0]
                outcomes[i] = outcome
                if isinstance(outcome, ToolResult):
                    self.next_marker += len(outcome.citations)
            else:
                parallel.append(i)
        rest = await asyncio.gather(
            *[
                self._dispatch(ctx, state, agent, conv_id, calls[i], self.next_marker, unavailable)
                for i in parallel
            ],
            return_exceptions=True,
        )
        outcomes.update(zip(parallel, rest))
        return [outcomes[i] for i in range(len(calls))]

    async def _dispatch(
        self,
        ctx: TurnContext,
        state: "_TurnState",
        agent: AgentVersion,
        conv_id: str,
        call: ToolCall | ToolCallRef,
        first_marker: int,
        unavailable: ToolResult | None = None,
    ) -> ToolResult:
        if unavailable is not None and call.id == unavailable.tool_use_id:
            return unavailable
        if call.name == HAND_OFF_TOOL:
            # An accepted hand-off never reaches here; this one is refused.
            if _hand_off_target(agent, call) is None:
                return ToolResult(
                    call.id, HAND_OFF_REFUSED.format(agent=_target_text(call)), is_error=True
                )
            return ToolResult(call.id, HAND_OFF_EXTRA, is_error=True)
        kind = self._kind(call)
        label = progress_label(
            call.name, kind, source_titles=[g.name for g in agent.grants.notion_pages]
        )
        return await self._with_progress(
            ctx, state, label, self._run_call(ctx, agent, conv_id, call, kind, first_marker)
        )

    async def _run_call(
        self,
        ctx: TurnContext,
        agent: AgentVersion,
        conv_id: str,
        call: ToolCall | ToolCallRef,
        kind: str,
        first_marker: int,
    ) -> ToolResult:
        if kind == READ_KIND:
            return await workflow.execute_activity_method(
                AgentActivities.run_stored_read_tool
                if isinstance(call, ToolCallRef)
                else AgentActivities.run_read_tool,
                args=[ctx, conv_id, call, first_marker],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
        if kind in EDITABLE_FIELDS:
            # A write never runs here: it becomes a task that asks the requester first.
            if isinstance(call, ToolCallRef):
                return await self._start_stored_task(ctx, conv_id, call, kind)
            return await self._start_task(ctx, agent, call, kind)
        # Never raise on a tool the LLM invented; tell it instead.
        return ToolResult(call.id, f"Unknown tool '{call.name}'.", is_error=True)

    async def _with_progress(
        self,
        ctx: TurnContext,
        state: "_TurnState",
        label: str | None,
        coro: Awaitable[_T],
    ) -> _T:
        """Run one tool step; when it runs long, show its label on the placeholder.

        The label shows only once the step has run ``PROGRESS_AFTER``, and at most one
        edit lands per ``PROGRESS_GAP`` across the turn's steps. The edit is awaited
        here, before the step's result returns, so a landed edit never comes after the
        final reply; a failure is dropped. A dropped edit may still land late, which is
        why the turn then posts its final text as a new message. A step that finishes in
        time shows nothing.
        """
        if not workflow.patched(PATCH_PROGRESS) or label is None:
            return await coro
        task = asyncio.ensure_future(coro)
        started = workflow.now()
        try:
            try:
                await workflow.wait_condition(lambda: task.done(), timeout=PROGRESS_AFTER)
            except TimeoutError:
                while not task.done():
                    eligible = started + PROGRESS_AFTER
                    if state.last_progress is not None:
                        eligible = max(eligible, state.last_progress + PROGRESS_GAP)
                    now = workflow.now()
                    if now >= eligible:
                        # Claim the throttle slot before the edit, not after: parallel steps
                        # see it taken and wait their turn, and a dropped edit still uses its
                        # slot, so no other step edits the placeholder in its place.
                        state.last_progress = now
                        assert state.placeholder_ts is not None
                        await self._progress_update(ctx, state, state.placeholder_ts, label)
                        break
                    try:
                        await workflow.wait_condition(lambda: task.done(), timeout=eligible - now)
                    except TimeoutError:
                        pass
            return await task
        finally:
            # A cancelled wrapper must not leave the wrapped step running on beside it.
            if not task.done():
                task.cancel()

    async def _progress_update(
        self, ctx: TurnContext, state: "_TurnState", placeholder_ts: str, label: str
    ) -> None:
        try:
            await workflow.execute_activity_method(
                AgentActivities.update_progress,
                args=[ctx, placeholder_ts, label],
                start_to_close_timeout=PROGRESS_TIMEOUT,
                retry_policy=PROGRESS_RETRY,
            )
        except ActivityError as err:
            self._log_failure(self._ref(), err, "progress")
            # The edit was dropped, but a late attempt may still reach Slack after the reply.
            # The placeholder is no longer safe to edit, so the final text is posted as a new
            # message. A history recorded before this change replays with the marker absent and
            # keeps editing the placeholder, as it did then.
            if workflow.patched(PATCH_PROGRESS_DROPPED):
                state.progress_dropped = True

    async def _start_task(
        self, ctx: TurnContext, agent: AgentVersion, call: ToolCall, kind: str
    ) -> ToolResult:
        if kind == "add_comment":
            item = call.input.get("item")
            comment = call.input.get("comment")
            if not isinstance(item, str) or not item.strip():
                return ToolResult(call.id, f"{kind} needs a non-empty 'item'.", is_error=True)
            if not isinstance(comment, str) or not comment.strip():
                return ToolResult(call.id, f"{kind} needs a non-empty 'comment'.", is_error=True)
            if error := check_field(kind, "comment", comment.strip()):
                return ToolResult(call.id, error, is_error=True)
            # The task resolves the item and checks the grant before any card.
            payload = {"item": item.strip(), "comment": comment.strip()}
            drafted = _drafted_text(kind, ctx)
        else:
            title = call.input.get("title")
            details = call.input.get("details")
            if not isinstance(title, str) or not title.strip():
                return ToolResult(call.id, f"{kind} needs a non-empty 'title'.", is_error=True)
            if not isinstance(details, str) or not details.strip():
                return ToolResult(call.id, f"{kind} needs a non-empty 'details'.", is_error=True)
            if not agent.grants.notion_databases:
                return ToolResult(
                    call.id, "This agent has no Notion database to create items in.", is_error=True
                )
            for name, value in (("title", title), ("details", details)):
                if error := check_field(kind, name, value.strip()):
                    return ToolResult(call.id, error, is_error=True)
            # The payload is reduced to exactly the fields that will be created, and
            # frozen here: the approval card shows it and the task runs it as is.
            payload = {
                "title": title.strip(),
                "details": details.strip(),
                "database_id": agent.grants.notion_databases[0].id,
            }
            drafted = _drafted_text(kind, ctx)
        spec = TaskSpec(
            ctx=ctx,
            tool_name=kind,
            payload=payload,
            approval_nonce=str(workflow.uuid4()),
        )
        await workflow.start_child_workflow(
            TaskWorkflow.run,
            spec,
            id=ids.task_id(ctx.tenant_id, str(workflow.uuid4())),
            parent_close_policy=ParentClosePolicy.ABANDON,
        )
        return ToolResult(call.id, drafted)

    async def _start_stored_task(
        self, ctx: TurnContext, conv_id: str, call: ToolCallRef, kind: str
    ) -> ToolResult:
        """Start the task for a write call whose input is stored: the draft is made by an activity.

        The task starts with a reference to revision 1 of its draft and the approval nonce; the
        title, details or comment never pass through the workflow.
        """
        task_id = ids.task_id(ctx.tenant_id, str(workflow.uuid4()))
        outcome: DraftOutcome = await workflow.execute_activity_method(
            AgentActivities.draft_task,
            args=[ctx, conv_id, call, task_id],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        if outcome.error:
            return ToolResult(call.id, outcome.error, is_error=True)
        spec = TaskSpec(
            ctx=ctx,
            tool_name=kind,
            payload={},
            approval_nonce=str(workflow.uuid4()),
            draft_ref=task_id,
            revision=outcome.revision,
            database_id=outcome.database_id,
        )
        await workflow.start_child_workflow(
            TaskWorkflow.run,
            spec,
            id=task_id,
            parent_close_policy=ParentClosePolicy.ABANDON,
        )
        return ToolResult(call.id, _drafted_text(kind, ctx))


def _drafted_text(kind: str, ctx: TurnContext) -> str:
    """What the model is told after a write became a task waiting for approval."""
    if kind == "add_comment":
        return (
            f"Drafted the comment and sent <@{ctx.requester_user_id}> an approval card. "
            "Nothing is posted until they click Submit."
        )
    return (
        f"Drafted the request and sent <@{ctx.requester_user_id}> an approval card. "
        "Nothing is filed until they click Submit."
    )


def _input_key(call: ToolCall | ToolCallRef) -> str:
    """What makes two calls the same call: the input itself, or its hash when it is not carried."""
    if isinstance(call, ToolCallRef):
        return call.input_sha256
    return json.dumps(call.input, sort_keys=True, default=str)


async def _resolved(result: ToolResult) -> ToolResult:
    """A call's result that needs no command, in place of running the call."""
    return result


def _typed(tp, value):
    """Convert plain dicts to ``tp``; leave a value that already has the type alone.

    A continue-as-new sends every parameter, so Temporal has already applied
    the type hints and the values are dataclasses, not dicts.
    """
    try:
        return value_to_type(tp, value)
    except TypeError:
        return value


def _named_target(call: ToolCall | ToolCallRef) -> object:
    """The agent a ``hand_off`` call names: from its input, or from the value derived from it."""
    if isinstance(call, ToolCallRef):
        return call.target or None
    return call.input.get("agent") if isinstance(call.input, dict) else None


def _hand_off_target(agent: AgentVersion, call: ToolCall | ToolCallRef) -> str | None:
    """The specialist a ``hand_off`` call names, if this agent may hand to it."""
    target = _named_target(call)
    if call.name == HAND_OFF_TOOL and isinstance(target, str) and target in agent.specialists:
        return target
    return None


def _accepted_hand_off(
    agent: AgentVersion, calls: list[ToolCall] | list[ToolCallRef]
) -> ToolCall | ToolCallRef | None:
    """The first valid ``hand_off`` in a response; any later one is refused."""
    return next((c for c in calls if _hand_off_target(agent, c) is not None), None)


def _target_text(call: ToolCall | ToolCallRef) -> str:
    return str(_named_target(call))


def _assistant_blocks(resp: LLMResponse) -> list[dict]:
    """The assistant turn to store: the model's own blocks when available."""
    if resp.content:
        return list(resp.content)
    blocks: list[dict] = []
    if resp.text:
        blocks.append({"type": "text", "text": resp.text})
    for call in resp.tool_calls:
        blocks.append({"type": "tool_use", "id": call.id, "name": call.name, "input": call.input})
    return blocks
