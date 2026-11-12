"""ConversationWorkflow: one per Slack thread, running the agent's turn loop.

Rules this workflow keeps:

* History stays small and free of content. Chat history lives in Postgres; the
  workflow carries the conversation reference, an inbox of pending messages and
  the recently seen event IDs. Each turn reloads history inside an activity.
  Tool output text is stored by the activity that ran the tool, and the
  workflow carries only references to it; the append activity puts the text
  into history.
* Links are never written by the model. The model cites sources by number;
  the workflow carries only the numbers and page IDs (citations), and the
  reply activity builds the Sources line from stored page metadata.
  Citation numbers are unique for the life of the thread (they carry over
  turns and continue-as-new), so a number cited from an earlier turn still
  means the same page.
* Identity comes from the runtime. The requester is the Slack user who sent the
  message, put into ``TurnContext`` here, never anything the LLM said.
* Writes never run here. A ``create_item`` or ``add_comment`` call from the LLM starts a separate
  TaskWorkflow, which asks the requester to approve before anything is created.
  The task is abandoned (not cancelled) when this conversation closes, because
  the work outlives the conversation.
* A failed turn is reported in the thread and never ends the conversation: the
  placeholder is replaced with a short error, history is left valid, and the
  workflow goes on to wait for the next message.
* Workflow code is deterministic: no I/O, ``workflow.uuid4()`` for IDs.
"""

import asyncio
from collections import deque
from datetime import timedelta

from temporalio import workflow
from temporalio.converter import value_to_type
from temporalio.exceptions import ActivityError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from agentos import ids
    from agentos.activities import READ_TOOLS, AgentActivities
    from agentos.config import AgentVersion
    from agentos.knowledge.search import SEARCH_RESULTS
    from agentos.models import (
        Citation,
        ConversationRef,
        InboundMessage,
        LLMResponse,
        TaskSpec,
        ToolCall,
        ToolResult,
        TurnContext,
    )
    from agentos.workflows import ACTIVITY_TIMEOUT, LLM_TIMEOUT, RETRY
    from agentos.workflows.task import TaskWorkflow

IDLE_TIMEOUT = timedelta(days=7)
SEEN_LIMIT = 500
MAX_CITATIONS = 200
OUT_OF_STEPS = "I couldn't finish that. Try rephrasing."
NO_ANSWER = "Sorry, I couldn't come up with an answer to that."
TURN_FAILED = "Sorry, something went wrong and I couldn't answer that. Please try again."
TOOL_NOT_COMPLETED = "The tool call did not complete."


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


@workflow.defn(name="ConversationWorkflow")
class ConversationWorkflow:
    def __init__(self) -> None:
        self.inbox: deque[InboundMessage] = deque()
        self.seen: deque[str] = deque(maxlen=SEEN_LIMIT)
        # Citation numbers are unique for the life of the thread, so a number
        # the model cites from an earlier turn still means the same page. Each
        # search_knowledge call gets its own block of SEARCH_RESULTS numbers
        # before any call starts, so parallel searches never collide. Only
        # references are kept (number, scope, page ID), the latest
        # MAX_CITATIONS of them.
        self.next_marker = 1
        self.citations: list[Citation] = []

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
            await self._turn(ref, m)

            if workflow.info().is_continue_as_new_suggested():
                await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(
                    args=[
                        ref,
                        list(self.inbox),
                        list(self.seen),
                        self.next_marker,
                        list(self.citations),
                    ]
                )

    async def _turn(self, ref: ConversationRef, m: InboundMessage) -> None:
        """Run one turn. A failed activity is reported, never raised."""
        conv_id = ids.conversation_id(ref.tenant_id, ref.channel, ref.thread_ts)
        state = _TurnState()
        try:
            await self._run_turn(ref, m, conv_id, state)
        except ActivityError as err:
            self._log_failure(ref, err)
            if state.ctx is None or state.placeholder_ts is None:
                # Nothing was posted yet, so there is nothing to replace.
                return
            await self._recover(ref, conv_id, state)

    def _log_failure(self, ref: ConversationRef, err: ActivityError, step: str = "turn") -> None:
        # IDs and the activity type only; never message text.
        workflow.logger.warning(
            "%s failed tenant_id=%s agent_id=%s workflow_id=%s activity=%s",
            step,
            ref.tenant_id,
            ref.agent_id,
            workflow.info().workflow_id,
            err.activity_type,
        )

    async def _recover(self, ref: ConversationRef, conv_id: str, state: "_TurnState") -> None:
        """Leave valid history and tell the user. Each step is best effort."""
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
            await workflow.execute_activity_method(
                AgentActivities.finalize_reply,
                args=[ctx, placeholder_ts, TURN_FAILED, []],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
        except ActivityError as err:
            self._log_failure(ref, err, "recovery")

    async def _run_turn(
        self, ref: ConversationRef, m: InboundMessage, conv_id: str, state: "_TurnState"
    ) -> None:
        agent: AgentVersion = await workflow.execute_activity_method(
            AgentActivities.load_agent,
            ref.agent_id,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        ctx = TurnContext(
            tenant_id=ref.tenant_id,
            agent_id=ref.agent_id,
            agent_version=agent.version,
            requester_user_id=m.user_id,
            channel=ref.channel,
            thread_ts=ref.thread_ts,
            channel_kind=m.channel_kind,
        )
        state.event_id = m.event_id
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

        for step in range(agent.max_steps):
            resp: LLMResponse = await workflow.execute_activity_method(
                AgentActivities.call_llm,
                args=[ctx, conv_id],
                start_to_close_timeout=LLM_TIMEOUT,
                retry_policy=RETRY,
            )
            if not resp.tool_calls:
                text = resp.text or NO_ANSWER
                content = list(resp.content) or [{"type": "text", "text": text}]
                if not any(b.get("type") == "text" for b in content):
                    content.append({"type": "text", "text": text})
                await self._append(
                    ctx, conv_id, f"{m.event_id}:s{step}:assistant", "assistant", content
                )
                state.reply_stored = True
                await workflow.execute_activity_method(
                    AgentActivities.finalize_reply,
                    args=[ctx, placeholder_ts, text, list(self.citations)],
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=RETRY,
                )
                return

            await self._append(
                ctx,
                conv_id,
                f"{m.event_id}:s{step}:assistant",
                "assistant",
                _assistant_blocks(resp),
            )
            # From here until the results are stored, history has tool_use
            # blocks with no tool_result; a failure must close them.
            state.pending_tool_ids = [call.id for call in resp.tool_calls]
            first_markers: list[int] = []
            for call in resp.tool_calls:
                first_markers.append(self.next_marker)
                if call.name == "search_knowledge":
                    self.next_marker += SEARCH_RESULTS
            outcomes = await asyncio.gather(
                *[
                    self._dispatch(ctx, agent, conv_id, call, first_marker)
                    for call, first_marker in zip(resp.tool_calls, first_markers)
                ],
                return_exceptions=True,
            )
            for outcome in outcomes:
                if isinstance(outcome, BaseException):
                    raise outcome
            results: list[ToolResult] = list(outcomes)  # type: ignore[arg-type]
            for result in results:
                self.citations.extend(result.citations)
            del self.citations[:-MAX_CITATIONS]
            await workflow.execute_activity_method(
                AgentActivities.append_tool_results,
                args=[ctx.tenant_id, conv_id, f"{m.event_id}:s{step}:results", results],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
            state.pending_tool_ids = []

        await workflow.execute_activity_method(
            AgentActivities.finalize_reply,
            args=[ctx, placeholder_ts, OUT_OF_STEPS, []],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )

    async def _append(
        self, ctx: TurnContext, conv_id: str, step_key: str, role: str, content: list
    ) -> int:
        return await workflow.execute_activity_method(
            AgentActivities.append_history,
            args=[ctx.tenant_id, conv_id, step_key, role, content],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )

    async def _dispatch(
        self,
        ctx: TurnContext,
        agent: AgentVersion,
        conv_id: str,
        call: ToolCall,
        first_marker: int,
    ) -> ToolResult:
        if call.name in READ_TOOLS:
            return await workflow.execute_activity_method(
                AgentActivities.run_read_tool,
                args=[ctx, conv_id, call, first_marker],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
        if call.name in ("create_item", "add_comment"):
            return await self._start_task(ctx, agent, call)
        # Never raise on a tool the LLM invented; tell it instead.
        return ToolResult(call.id, f"Unknown tool '{call.name}'.", is_error=True)

    async def _start_task(
        self, ctx: TurnContext, agent: AgentVersion, call: ToolCall
    ) -> ToolResult:
        if call.name == "add_comment":
            item = call.input.get("item")
            comment = call.input.get("comment")
            if not isinstance(item, str) or not item.strip():
                return ToolResult(call.id, "add_comment needs a non-empty 'item'.", is_error=True)
            if not isinstance(comment, str) or not comment.strip():
                return ToolResult(
                    call.id, "add_comment needs a non-empty 'comment'.", is_error=True
                )
            # The task resolves the item and checks the grant before any card.
            payload = {"item": item.strip(), "comment": comment.strip()}
            drafted = (
                f"Drafted the comment and sent <@{ctx.requester_user_id}> an approval card. "
                "Nothing is posted until they click Submit."
            )
        else:
            title = call.input.get("title")
            details = call.input.get("details")
            if not isinstance(title, str) or not title.strip():
                return ToolResult(call.id, "create_item needs a non-empty 'title'.", is_error=True)
            if not isinstance(details, str) or not details.strip():
                return ToolResult(
                    call.id, "create_item needs a non-empty 'details'.", is_error=True
                )
            if not agent.grants.notion_databases:
                return ToolResult(
                    call.id, "This agent has no Notion database to create items in.", is_error=True
                )
            # The payload is reduced to exactly the fields that will be created, and
            # frozen here: the approval card shows it and the task runs it as is.
            payload = {
                "title": title.strip(),
                "details": details.strip(),
                "database_id": agent.grants.notion_databases[0].id,
            }
            drafted = (
                f"Drafted the request and sent <@{ctx.requester_user_id}> an approval card. "
                "Nothing is filed until they click Submit."
            )
        spec = TaskSpec(
            ctx=ctx,
            tool_name=call.name,
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


def _typed(tp, value):
    """Convert plain dicts to ``tp``; leave a value that already has the type alone.

    A continue-as-new sends every parameter, so Temporal has already applied
    the type hints and the values are dataclasses, not dicts.
    """
    try:
        return value_to_type(tp, value)
    except TypeError:
        return value


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
