"""History sent to the model is repaired and bounded on every turn."""

import pytest

from agentos import ids
from agentos.history_view import HISTORY_BUDGET_CHARS, prepare_for_model
from agentos.models import LLMResponse
from agentos.workflows.conversation import EMPTY_MESSAGE_REPLY, ConversationWorkflow
from tests.conftest import TENANT, eventually
from tests.helpers.conversation import CHANNEL, THREAD, inbound, start_conversation
from tests.helpers.turns import assert_tool_pairs_valid

CONV_ID = ids.conversation_id(TENANT, CHANNEL, THREAD)


def is_text_user(message: dict) -> bool:
    return message["role"] == "user" and any(
        isinstance(b, dict) and b.get("type") == "text" for b in message["content"]
    )


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
    """Fails if a long thread is sent to the model whole, or the stored thread loses messages."""
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

    async def reply_stored() -> bool:
        # The reply is stored after it was shown, so the update comes first.
        return len(await h.history.load(TENANT, CONV_ID)) == turns * 4 + 2

    await eventually(reply_stored)  # the stored thread stays complete


async def test_newest_message_is_kept_when_it_alone_exceeds_the_budget(harness):
    h = harness
    h.llm.queue(LLMResponse(text="Answer", tool_calls=[], stop_reason="end_turn"))
    await start_conversation(h, inbound("Ev1", "y" * (HISTORY_BUDGET_CHARS * 2)))
    await eventually(lambda: h.slack.updates())
    assert len(h.llm.calls[0]["messages"]) == 1


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


def test_window_never_starts_on_a_message_carrying_tool_results():
    # An unanswered tool_use followed by the next turn's question: the repair
    # puts the missing result and the question in one user message. Cutting
    # in front of that message would leave its result with no tool_use.
    big = "x" * (HISTORY_BUDGET_CHARS // 2)
    messages = [
        {"role": "user", "content": [{"type": "text", "text": "q0 " + big}]},
        {
            "role": "assistant",
            "content": [{"type": "tool_use", "id": "t1", "name": "search_items", "input": {}}],
        },
        {"role": "user", "content": [{"type": "text", "text": "q1 " + big}]},
        {"role": "assistant", "content": [{"type": "text", "text": "a1"}]},
        {"role": "user", "content": [{"type": "text", "text": "q2"}]},
    ]
    sent = prepare_for_model(messages)
    assert_tool_pairs_valid(sent)
    assert sent[-1]["content"][0]["text"] == "q2"


@pytest.mark.parametrize("text", ["", "   \n"], ids=["bare-mention", "whitespace-dm"])
async def test_message_without_text_gets_a_fixed_reply_and_poisons_nothing(harness, text):
    """Fails if a bare mention is stored or sent to the model, or the thread stops answering."""
    h = harness
    h.llm.queue(LLMResponse(text="Real answer", tool_calls=[], stop_reason="end_turn"))
    handle = await start_conversation(h, inbound("Ev1", text))
    await eventually(lambda: [p for p in h.slack.posts() if p["text"] == EMPTY_MESSAGE_REPLY])
    await handle.signal(ConversationWorkflow.message, inbound("Ev2", "a real question"))
    await eventually(lambda: h.slack.updates())

    assert h.slack.updates()[0]["text"] == "Real answer"
    assert len(h.llm.calls) == 1
    assert [m["content"][0]["text"] for m in h.llm.calls[0]["messages"]] == ["a real question"]
    stored = await h.history.load(TENANT, CONV_ID)
    assert all(b["text"].strip() for m in stored for b in m["content"])


async def test_empty_text_block_already_stored_is_not_sent(harness):
    """Fails if a row stored before bare mentions were refused still reaches the model."""
    h = harness
    await h.history.append(TENANT, CONV_ID, "old:user", "user", [{"type": "text", "text": ""}])
    await h.history.append(
        TENANT, CONV_ID, "old:recover", "assistant", [{"type": "text", "text": "Turn failed"}]
    )
    h.llm.queue(LLMResponse(text="Answer", tool_calls=[], stop_reason="end_turn"))
    await start_conversation(h, inbound("Ev1", "hello"))
    await eventually(lambda: h.slack.updates())

    sent = h.llm.calls[0]["messages"]
    assert [(m["role"], m["content"][0]["text"]) for m in sent] == [("user", "hello")]
