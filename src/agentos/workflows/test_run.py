"""TestRunWorkflow: one run of a draft version's test cases, on its own task queue.

Each case runs as a turn against the **draft** definition — the same ``call_model``
and read-tool path a real turn uses, pinned to the draft version, with the draft's
grants — in a conversation of its own. The runner (the agent's creator or an admin,
checked by the control plane before this workflow starts) is the turn's requester;
the turn runs as their direct message, so a front door's specialists are offered as
they would be to them. The draft's own audience is not checked: its editors may test
it whatever audience it sets.

Rules this workflow keeps:

* Content stays out of history. The workflow carries the run's reference, each case's
  reference and expectation (kind and target ID), citations and drafted-action
  references. The question is appended to the case's conversation by an activity that
  reads it from the case row; the final answer is read by the activity that judges it.
* Writes never run. A call to a write tool is recorded (its task kind and target
  database, resolved with the draft's grants) and stops the case: no card is posted,
  no task starts, nothing is written. A hand-off is recorded and stops the case too;
  the specialist does not answer.
* Nothing is posted to Slack: no placeholder, no progress, no reply.
* The run's spend counts in the tenant's usage (``call_model`` records it) and is
  capped: a check after every model step stops the run at the cap, and the cases not
  run are reported. Unknown spend stops it too (D-21). One call may take the run past
  the cap by its own cost, the bound the monthly budgets keep.
* A failed case is recorded as an error and the run goes on, as a failed turn never
  ends a conversation. The run always finishes its row, so the console never waits on
  a run that ended.
* Workflow code is deterministic: no I/O, no clocks of its own.
"""

from temporalio import workflow
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from agentos import ids
    from agentos.activities import HAND_OFF_TOOL, AgentActivities
    from agentos.models import (
        EDITABLE_FIELDS,
        READ_KIND,
        Citation,
        StepResult,
        ToolCallRef,
        ToolResult,
        TurnContext,
    )
    from agentos.test_sets import (
        CAP_REACHED,
        CASE_ERROR,
        ERROR,
        NOT_RUN,
        SPEND_UNKNOWN,
        CaseRef,
        CaseResult,
        DraftedWrite,
        RunSpend,
        TestRunRef,
    )
    from agentos.workflows import ACTIVITY_TIMEOUT, LLM_TIMEOUT, RETRY

# Fixed runtime texts the case's history keeps in place of a tool's effect.
TEST_WRITE_DRAFTED = "Test run: the write was drafted and not run."
TEST_HAND_OFF = "Test run: the hand-off was recorded; the specialist does not answer."
TEST_NOT_RUN = "Test run: not run; the case stopped at the drafted action."


class _RunStopped(Exception):
    """The cap (or unknown spend) stopped the run mid-case; the reason is a fixed code."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@workflow.defn(name="TestRunWorkflow")
class TestRunWorkflow:
    @workflow.run
    async def run(self, tenant_id: str, run_id: str) -> str:
        ref: TestRunRef = await workflow.execute_activity_method(
            AgentActivities.load_test_run,
            args=[tenant_id, run_id],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        results: list[CaseResult] = []
        conv_ids: list[str] = []
        stopped: str | None = None
        for case in ref.cases:
            if stopped is not None:
                results.append(CaseResult(case.case_id, NOT_RUN, stopped))
                continue
            conv_id = ids.test_conversation_id(run_id, case.case_id)
            spend = await self._spend(tenant_id, conv_ids)
            if spend.reached:
                stopped = self._stop_reason(spend)
                results.append(CaseResult(case.case_id, NOT_RUN, stopped))
                continue
            conv_ids.append(conv_id)
            ctx = TurnContext(
                tenant_id=tenant_id,
                agent_id=ref.agent_id,
                agent_version=ref.version,
                requester_user_id=ref.started_by,
                channel=run_id,
                thread_ts=case.case_id,
                channel_kind="im",
            )
            try:
                results.append(
                    await self._case(tenant_id, run_id, ref, case, ctx, conv_id, conv_ids)
                )
            except _RunStopped as err:
                stopped = err.reason
                results.append(CaseResult(case.case_id, NOT_RUN, stopped))
            except ActivityError as err:
                workflow.logger.warning(
                    "test case failed tenant_id=%s agent_id=%s workflow_id=%s activity=%s",
                    tenant_id,
                    ref.agent_id,
                    workflow.info().workflow_id,
                    err.activity_type,
                )
                results.append(CaseResult(case.case_id, ERROR, CASE_ERROR))
        spend = await self._spend(tenant_id, conv_ids)
        await workflow.execute_activity_method(
            AgentActivities.finish_test_run,
            args=[tenant_id, run_id, results, spend.cost],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        return run_id

    @staticmethod
    def _stop_reason(spend: RunSpend) -> str:
        return SPEND_UNKNOWN if spend.unpriced > 0 else CAP_REACHED

    async def _spend(self, tenant_id: str, conv_ids: list[str]) -> RunSpend:
        return await workflow.execute_activity_method(
            AgentActivities.test_run_spend,
            args=[tenant_id, list(conv_ids)],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )

    async def _case(
        self,
        tenant_id: str,
        run_id: str,
        ref: TestRunRef,
        case: CaseRef,
        ctx: TurnContext,
        conv_id: str,
        conv_ids: list[str],
    ) -> CaseResult:
        """One case as a turn: model steps and reads until a final reply, a drafted
        write, a hand-off, the step limit, or the cap. Then the judge reads the result."""
        await workflow.execute_activity_method(
            AgentActivities.start_test_case,
            args=[tenant_id, conv_id, run_id, case.case_id],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        citations: list[Citation] = []
        next_marker = 1
        drafted: DraftedWrite | None = None
        hand_off: str | None = None
        final_key: str | None = None
        for step in range(ref.max_steps):
            key = f"{case.case_id}:s{step}"
            outcome: StepResult = await workflow.execute_activity_method(
                AgentActivities.call_model,
                args=[ctx, conv_id, key],
                start_to_close_timeout=LLM_TIMEOUT,
                retry_policy=RETRY,
            )
            spend = await self._spend(tenant_id, conv_ids)
            if spend.reached:
                raise _RunStopped(self._stop_reason(spend))
            if not outcome.tool_calls:
                final_key = key
                break
            results: list[ToolResult] = []
            stop = False
            for call in outcome.tool_calls:
                if stop:
                    results.append(ToolResult(call.id, TEST_NOT_RUN, is_error=True))
                    continue
                results.append(await self._call(ctx, conv_id, call, citations, next_marker))
                if call.problem:
                    continue
                if call.name == HAND_OFF_TOOL:
                    hand_off = call.target
                    stop = True
                elif call.kind in EDITABLE_FIELDS:
                    drafted = await workflow.execute_activity_method(
                        AgentActivities.record_test_write,
                        args=[ctx, conv_id, call],
                        start_to_close_timeout=ACTIVITY_TIMEOUT,
                        retry_policy=RETRY,
                    )
                    stop = True
                elif call.kind == READ_KIND and call.name == "search_knowledge":
                    next_marker += len(results[-1].citations)
            await workflow.execute_activity_method(
                AgentActivities.append_tool_results,
                args=[tenant_id, conv_id, f"{key}:results", results],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
            if stop:
                break
        return await workflow.execute_activity_method(
            AgentActivities.judge_test_case,
            args=[ctx, conv_id, final_key, case, citations, drafted, hand_off],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )

    async def _call(
        self,
        ctx: TurnContext,
        conv_id: str,
        call: ToolCallRef,
        citations: list[Citation],
        next_marker: int,
    ) -> ToolResult:
        """One tool call of a case: a read runs with the draft's grants; a write or a
        hand-off is recorded as fixed text and stops the case; anything else is told."""
        if call.problem:
            return ToolResult(call.id, call.problem, is_error=True)
        if call.name == HAND_OFF_TOOL:
            return ToolResult(call.id, TEST_HAND_OFF)
        if call.kind in EDITABLE_FIELDS:
            return ToolResult(call.id, TEST_WRITE_DRAFTED)
        if call.kind == READ_KIND:
            result: ToolResult = await workflow.execute_activity_method(
                AgentActivities.run_test_read_tool,
                args=[ctx, conv_id, call, next_marker],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY,
            )
            citations.extend(result.citations)
            return result
        return ToolResult(call.id, f"Unknown tool '{call.name}'.", is_error=True)
