"""ConversationWorkflow: one per Slack thread, running the agent's turn loop.

Rules this workflow keeps:

* History stays small and free of content. Chat history lives in Postgres; the
  workflow carries the conversation reference, an inbox of pending messages and
  the recently seen event IDs. Each turn reloads history inside an activity.
* Identity comes from the runtime. The requester is the Slack user who sent the
  message, put into ``TurnContext`` here, never anything the LLM said.
* Writes never run here. A ``create_item`` call from the LLM starts a separate
  TaskWorkflow, which asks the requester to approve before anything is created.
  The task is abandoned (not cancelled) when this conversation closes, because
  the work outlives the conversation.
* Workflow code is deterministic: no I/O, ``workflow.uuid4()`` for IDs.
"""

import asyncio
from collections import deque
from datetime import timedelta

from temporalio import workflow
from temporalio.converter import value_to_type
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from agentos import ids
    from agentos.activities import READ_TOOLS, AgentActivities
    from agentos.config import AgentVersion
    from agentos.models import (
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
OUT_OF_STEPS = "I couldn't finish that. Try rephrasing."
NO_ANSWER = "Sorry, I couldn't come up with an answer to that."


@workflow.defn(name="ConversationWorkflow")
class ConversationWorkflow:
    def __init__(self) -> None:
        self.inbox: deque[InboundMessage] = deque()
        self.seen: deque[str] = deque(maxlen=SEEN_LIMIT)

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
    ) -> None:
        # Temporal applies these type hints only when every parameter was sent.
        # The gateway starts the workflow with ``ref`` alone, so convert here.
        ref = value_to_type(ConversationRef, ref)
        inbox = value_to_type(list[InboundMessage], inbox) if inbox else None
        seen = value_to_type(list[str], seen) if seen else None

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
                workflow.continue_as_new(args=[ref, list(self.inbox), list(self.seen)])

    async def _turn(self, ref: ConversationRef, m: InboundMessage) -> None:
        conv_id = ids.conversation_id(ref.tenant_id, ref.channel, ref.thread_ts)
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
        await self._append(ctx, conv_id, "user", [{"type": "text", "text": m.text}])
        placeholder_ts: str = await workflow.execute_activity_method(
            AgentActivities.post_placeholder,
            args=[ctx, f"{m.event_id}:ph"],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )

        for _ in range(agent.max_steps):
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
                await self._append(ctx, conv_id, "assistant", content)
                await workflow.execute_activity_method(
                    AgentActivities.finalize_reply,
                    args=[ctx, placeholder_ts, text],
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=RETRY,
                )
                return

            await self._append(ctx, conv_id, "assistant", _assistant_blocks(resp))
            results = await asyncio.gather(
                *[self._dispatch(ctx, agent, call) for call in resp.tool_calls]
            )
            await self._append(
                ctx,
                conv_id,
                "user",
                [
                    {
                        "type": "tool_result",
                        "tool_use_id": r.tool_use_id,
                        "content": r.content,
                        "is_error": r.is_error,
                    }
                    for r in results
                ],
            )

        await workflow.execute_activity_method(
            AgentActivities.finalize_reply,
            args=[ctx, placeholder_ts, OUT_OF_STEPS],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )

    async def _append(self, ctx: TurnContext, conv_id: str, role: str, content: list) -> int:
        return await workflow.execute_activity_method(
            AgentActivities.append_history,
            args=[ctx.tenant_id, conv_id, role, content],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )

    async def _dispatch(self, ctx: TurnContext, agent: AgentVersion, call: ToolCall) -> ToolResult:
        if call.name in READ_TOOLS:
            return await workflow.execute_activity_method(
                AgentActivities.run_read_tool,
                args=[ctx, call],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
        if call.name == "create_item":
            return await self._start_task(ctx, agent, call)
        # Never raise on a tool the LLM invented; tell it instead.
        return ToolResult(call.id, f"Unknown tool '{call.name}'.", is_error=True)

    async def _start_task(
        self, ctx: TurnContext, agent: AgentVersion, call: ToolCall
    ) -> ToolResult:
        title = call.input.get("title")
        details = call.input.get("details")
        if not isinstance(title, str) or not title.strip():
            return ToolResult(call.id, "create_item needs a non-empty 'title'.", is_error=True)
        if not isinstance(details, str) or not details.strip():
            return ToolResult(call.id, "create_item needs a non-empty 'details'.", is_error=True)
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
        return ToolResult(
            call.id,
            f"Drafted the request and sent <@{ctx.requester_user_id}> an approval card. "
            "Nothing is filed until they click Submit.",
        )


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
