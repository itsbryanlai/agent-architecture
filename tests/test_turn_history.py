"""History sent to the model is repaired and bounded on every turn."""

from agentos import ids
from agentos.history_view import HISTORY_BUDGET_CHARS, prepare_for_model
from agentos.models import LLMResponse
from agentos.workflows.conversation import ConversationWorkflow
from tests.conftest import TENANT, eventually
from tests.test_conversation_workflow import CHANNEL, THREAD, inbound, start_conversation

CONV_ID = ids.conversation_id(TENANT, CHANNEL, THREAD)


def is_text_user(message: dict) -> bool:
    return message["role"] == "user" and any(
        isinstance(b, dict) and b.get("type") == "text" for b in message["content"]
    )


def assert_tool_pairs_valid(messages: list[dict]) -> None:
    """Every tool_use is answered by the very next message, and no result is orphaned."""
    for i, m in enumerate(messages):
        uses = [
            b["id"] for b in m["content"] if isinstance(b, dict) and b.get("type") == "tool_use"
        ]
        if uses:
            assert i + 1 < len(messages), "tool_use is the last message"
            nxt = messages[i + 1]
            assert nxt["role"] == "user"
            answered = [
                b["tool_use_id"]
                for b in nxt["content"]
                if isinstance(b, dict) and b.get("type") == "tool_result"
            ]
            assert sorted(answered) == sorted(uses)
        results = [
            b["tool_use_id"]
            for b in m["content"]
            if isinstance(b, dict) and b.get("type") == "tool_result"
        ]
        if results:
            assert i > 0
            prev_uses = [
                b["id"]
                for b in messages[i - 1]["content"]
                if isinstance(b, dict) and b.get("type") == "tool_use"
            ]
            assert sorted(results) == sorted(prev_uses)


async def test_unanswered_tool_use_is_closed_when_history_is_loaded(harness):
    h = harness
    # The state a failed recovery leaves: a tool_use with no tool_result,
    # followed by the failure notice.
    await h.history.append(TENANT, CONV_ID, "e0:user", "user", [{"type": "text", "text": "old"}])
    await h.history.append(
        TENANT,
        CONV_ID,
        "e0:s0:assistant",
        "assistant",
        [{"type": "tool_use", "id": "tu_lost", "name": "search_items", "input": {}}],
    )
    await h.history.append(
        TENANT, CONV_ID, "e0:recover:assistant", "assistant", [{"type": "text", "text": "failed"}]
    )
    h.llm.queue(LLMResponse(text="Fine now", tool_calls=[], stop_reason="end_turn"))

    await start_conversation(h, inbound("Ev1", "next question"))
    await eventually(lambda: h.slack.updates())

    sent = h.llm.calls[0]["messages"]
    assert_tool_pairs_valid(sent)
    closing = next(
        b
        for m in sent
        for b in m["content"]
        if isinstance(b, dict) and b.get("type") == "tool_result"
    )
    assert closing["tool_use_id"] == "tu_lost"
    assert closing["is_error"] is True
    assert h.slack.updates()[0]["text"] == "Fine now"
    # Stored history is not rewritten by the repair.
    stored = await h.history.load(TENANT, CONV_ID)
    assert not any(
        isinstance(b, dict) and b.get("type") == "tool_result" for m in stored for b in m["content"]
    )


async def test_history_sent_to_the_model_is_bounded(harness):
    h = harness
    chunk = "x" * (HISTORY_BUDGET_CHARS // 8)
    turns = 20
    for n in range(turns):
        await h.history.append(
            TENANT, CONV_ID, f"e{n}:user", "user", [{"type": "text", "text": f"q{n} {chunk}"}]
        )
        await h.history.append(
            TENANT,
            CONV_ID,
            f"e{n}:s0:assistant",
            "assistant",
            [{"type": "tool_use", "id": f"tu_{n}", "name": "search_items", "input": {}}],
        )
        await h.history.append(
            TENANT,
            CONV_ID,
            f"e{n}:s0:results",
            "user",
            [{"type": "tool_result", "tool_use_id": f"tu_{n}", "content": chunk}],
        )
        await h.history.append(
            TENANT, CONV_ID, f"e{n}:s1:assistant", "assistant", [{"type": "text", "text": "ok"}]
        )
    h.llm.queue(LLMResponse(text="Answer", tool_calls=[], stop_reason="end_turn"))

    await start_conversation(h, inbound("EvLast", "newest question"))
    await eventually(lambda: h.slack.updates())

    sent = h.llm.calls[0]["messages"]
    assert is_text_user(sent[0])
    assert_tool_pairs_valid(sent)
    assert sent[-1]["content"][0]["text"] == "newest question"
    assert "q0 " not in str(sent)
    assert "q19 " in str(sent)
    assert len(str(sent)) <= HISTORY_BUDGET_CHARS * 1.1
    stored = await h.history.load(TENANT, CONV_ID)
    assert len(stored) == turns * 4 + 2  # the stored thread stays complete


async def test_newest_message_is_kept_when_it_alone_exceeds_the_budget(harness):
    h = harness
    h.llm.queue(LLMResponse(text="Answer", tool_calls=[], stop_reason="end_turn"))
    await start_conversation(h, inbound("Ev1", "y" * (HISTORY_BUDGET_CHARS * 2)))
    await eventually(lambda: h.slack.updates())
    assert len(h.llm.calls[0]["messages"]) == 1


async def test_later_message_still_answered(harness):
    h = harness
    h.llm.queue(
        LLMResponse(text="one", tool_calls=[], stop_reason="end_turn"),
        LLMResponse(text="two", tool_calls=[], stop_reason="end_turn"),
    )
    handle = await start_conversation(h, inbound("Ev1"))
    await eventually(lambda: len(h.slack.updates()) >= 1)
    await handle.signal(ConversationWorkflow.message, inbound("Ev2", "again"))
    await eventually(lambda: len(h.slack.updates()) >= 2)
    assert_tool_pairs_valid(h.llm.calls[1]["messages"])


def test_result_that_answers_no_tool_use_is_dropped():
    twice = [
        {"role": "user", "content": [{"type": "text", "text": "q"}]},
        {
            "role": "assistant",
            "content": [{"type": "tool_use", "id": "a", "name": "t", "input": {}}],
        },
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "a", "content": "1"}]},
        # A second copy, as a retried recovery could leave.
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "a", "content": "2"}]},
        {"role": "assistant", "content": [{"type": "text", "text": "done"}]},
    ]
    prepared = prepare_for_model(twice)
    assert_tool_pairs_valid(prepared)
    assert [m["role"] for m in prepared] == ["user", "assistant", "user", "assistant"]
    assert prepared[2]["content"][0]["content"] == "1"
