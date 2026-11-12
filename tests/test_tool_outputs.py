"""Tool output text stays out of Temporal workflow history."""

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.llm import ScriptedLLMClient
from agentos.models import LLMResponse, ToolCall, ToolResult
from tests.conftest import DB_ID, TENANT, eventually, title_prop
from tests.test_conversation_workflow import CHANNEL, THREAD, inbound, start_conversation
from tests.test_llm_errors import make_activities

DISTINCTIVE = "Zebra-crossing laptop recall"


async def run_search_turn(h):
    h.notion.add_page(DB_ID, {"Name": title_prop(DISTINCTIVE)})
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[ToolCall(id="tu_1", name="search_items", input={"query": "zebra"})],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Found it", tool_calls=[], stop_reason="end_turn"),
    )
    handle = await start_conversation(h, inbound("Ev1", "any zebra requests?"))
    await eventually(lambda: h.slack.updates())
    return handle


async def test_read_tool_output_not_in_workflow_history(harness):
    h = harness
    handle = await run_search_turn(h)

    history = await handle.fetch_history()
    needle = DISTINCTIVE.encode()
    carrying = [e for e in history.events if needle in e.SerializeToString()]
    assert carrying == []
    # The check is meaningful: the text exists in the stored chat history.
    stored = await h.history.load(TENANT, ids.conversation_id(TENANT, CHANNEL, THREAD))
    assert DISTINCTIVE in stored[2]["content"][0]["content"]


async def test_tool_results_resolved_into_history(harness):
    h = harness
    await run_search_turn(h)

    stored = await h.history.load(TENANT, ids.conversation_id(TENANT, CHANNEL, THREAD))
    block = stored[2]["content"][0]
    assert block["type"] == "tool_result"
    assert block["tool_use_id"] == "tu_1"
    assert block["is_error"] is False
    assert block["content"].startswith(f"- {DISTINCTIVE} (status: none) https://")
    # The model received the same full text on its second call.
    assert DISTINCTIVE in h.llm.calls[1]["messages"][2]["content"][0]["content"]


async def test_append_tool_results_is_idempotent(agents):
    acts = make_activities(agents, ScriptedLLMClient())
    await acts.tool_outputs.put(TENANT, "c1", "tu_1", "stored text", False)
    results = [ToolResult("tu_1", stored=True), ToolResult("tu_2", "inline text", True)]
    env = ActivityEnvironment()

    first = await env.run(acts.append_tool_results, TENANT, "c1", "Ev1:s0:results", results)
    again = await env.run(acts.append_tool_results, TENANT, "c1", "Ev1:s0:results", results)

    assert first == again == 1
    stored = await acts.history.load(TENANT, "c1")
    assert len(stored) == 1
    assert stored[0]["content"] == [
        {"type": "tool_result", "tool_use_id": "tu_1", "content": "stored text", "is_error": False},
        {"type": "tool_result", "tool_use_id": "tu_2", "content": "inline text", "is_error": True},
    ]


async def test_missing_stored_output_is_non_retryable(agents):
    acts = make_activities(agents, ScriptedLLMClient())
    with pytest.raises(ApplicationError) as exc:
        await ActivityEnvironment().run(
            acts.append_tool_results,
            TENANT,
            "c1",
            "Ev1:s0:results",
            [ToolResult("tu_9", stored=True)],
        )
    assert exc.value.non_retryable is True
    assert exc.value.type == "ValueError"
    assert await acts.history.load(TENANT, "c1") == []
