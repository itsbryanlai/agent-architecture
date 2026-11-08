import asyncio
import json

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.models import ConversationRef, InboundMessage, LLMResponse, ToolCall
from agentos.workflows.conversation import ConversationWorkflow
from tests.conftest import ALICE, DB_ID, TENANT, eventually, title_prop

CHANNEL = "C_GENERAL"
THREAD = "1700000000.000100"


def inbound(event_id: str, text: str = "hello") -> InboundMessage:
    return InboundMessage(
        event_id=event_id,
        user_id=ALICE,
        text=text,
        channel=CHANNEL,
        thread_ts=THREAD,
        channel_kind="channel",
    )


async def start_conversation(h, msg: InboundMessage):
    return await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "it-helper", CHANNEL, THREAD),
        id=ids.conversation_id(TENANT, CHANNEL, THREAD),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )


def placeholders(h):
    return [p for p in h.slack.posts() if p["text"] == "Thinking…"]


async def test_text_reply_and_duplicate_event_dropped(harness):
    h = harness
    h.llm.queue(LLMResponse(text="Hi there", tool_calls=[], stop_reason="end_turn"))

    handle = await start_conversation(h, inbound("Ev1"))
    await handle.signal(ConversationWorkflow.message, inbound("Ev1"))

    await eventually(lambda: h.slack.updates())
    await asyncio.sleep(0.5)  # give a duplicate turn time to show up if it existed

    assert len(placeholders(h)) == 1
    assert len(h.slack.updates()) == 1
    update = h.slack.updates()[0]
    assert update["text"] == "Hi there"
    assert update["ts"] == placeholders(h)[0]["ts"]
    assert len(h.llm.calls) == 1


async def test_read_tool_round_trip(harness):
    h = harness
    h.notion.add_page(DB_ID, {"Name": title_prop("Laptop battery swelling")})
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[ToolCall(id="tu_1", name="search_items", input={"query": "laptop"})],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Found one request.", tool_calls=[], stop_reason="end_turn"),
    )

    await start_conversation(h, inbound("Ev2", "any laptop requests?"))
    await eventually(lambda: h.slack.updates())

    assert len(h.llm.calls) == 2
    second = h.llm.calls[1]["messages"]
    assert second[-2]["role"] == "assistant"
    assert second[-2]["content"][0]["type"] == "tool_use"
    last = second[-1]
    assert last["role"] == "user"
    result = last["content"][0]
    assert result["type"] == "tool_result"
    assert result["tool_use_id"] == "tu_1"
    assert "Laptop battery swelling" in result["content"]
    assert h.slack.updates()[0]["text"] == "Found one request."


async def test_create_item_starts_task_and_dms_card(harness):
    h = harness
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[
                ToolCall(
                    id="tu_c",
                    name="create_item",
                    input={"title": "New monitor", "details": "Mine flickers."},
                )
            ],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Sent you an approval card.", tool_calls=[], stop_reason="end_turn"),
    )

    await start_conversation(h, inbound("Ev3", "please order me a monitor"))
    await eventually(lambda: h.slack.updates())

    cards = await eventually(lambda: [p for p in h.slack.posts() if p["blocks"]])
    card = cards[0]
    assert card["channel"] == f"D-{ALICE}"  # the requester's DM
    assert card["channel"] != CHANNEL
    assert card["thread_ts"] is None

    actions = next(b for b in card["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    assert value["task_id"].startswith(f"{TENANT}:task:")

    task = h.client.get_workflow_handle(value["task_id"])
    await eventually(lambda: _state_is(task, "awaiting_approval"))

    tool_result = h.llm.calls[1]["messages"][-1]["content"][0]
    assert tool_result["tool_use_id"] == "tu_c"
    assert "Nothing is filed until they click Submit" in tool_result["content"]
    assert h.notion.count("create_page") == 0


async def _state_is(handle, expected: str) -> bool:
    return await handle.query("state") == expected
