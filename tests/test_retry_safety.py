"""Retried history appends and Slack posts: no duplicate history, visible duplicate posts."""

import itertools
import logging

import pytest
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.llm import ScriptedLLMClient
from agentos.models import LLMResponse, ToolCall
from agentos.stores import InMemoryHistoryStore
from agentos.workflows.conversation import TURN_FAILED, ConversationWorkflow
from tests.conftest import TENANT, eventually
from tests.test_conversation_workflow import (
    CHANNEL,
    THREAD,
    inbound,
    placeholders,
    start_conversation,
)
from tests.test_llm_errors import make_activities


def text_of(message):
    return [b["text"] for b in message["content"] if b.get("type") == "text"]


async def test_history_append_is_idempotent_per_step_key():
    store = InMemoryHistoryStore()
    first = await store.append(TENANT, "c1", "Ev1:user", "user", [{"type": "text", "text": "hi"}])
    again = await store.append(TENANT, "c1", "Ev1:user", "user", [{"type": "text", "text": "hi"}])
    other = await store.append(TENANT, "c1", "Ev2:user", "user", [{"type": "text", "text": "hi"}])

    assert first == again == 1
    assert other == 2
    assert len(await store.load(TENANT, "c1")) == 2


async def test_append_history_rejects_empty_step_key(agents):
    acts = make_activities(agents, ScriptedLLMClient())
    with pytest.raises(ValueError):
        await ActivityEnvironment().run(
            acts.append_history, TENANT, "c1", "", "user", [{"type": "text", "text": "hi"}]
        )


async def test_turn_step_keys_do_not_collide(harness):
    h = harness
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[ToolCall(id="tu_1", name="search_items", input={"query": "laptop"})],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Done", tool_calls=[], stop_reason="end_turn"),
    )

    await start_conversation(h, inbound("Ev1", "any laptop requests?"))
    await eventually(lambda: h.slack.updates())

    conv_id = ids.conversation_id(TENANT, CHANNEL, THREAD)
    stored = await h.history.load(TENANT, conv_id)
    assert [m["role"] for m in stored] == ["user", "assistant", "user", "assistant"]
    assert stored[1]["content"][0]["type"] == "tool_use"
    assert stored[2]["content"][0]["type"] == "tool_result"
    assert stored[3]["content"][0]["type"] == "text"

    keys = h.history.step_keys[(TENANT, conv_id)]
    assert keys == {
        "Ev1:user": 1,
        "Ev1:s0:assistant": 2,
        "Ev1:s0:results": 3,
        "Ev1:s1:assistant": 4,
    }


async def test_retried_append_does_not_duplicate_history(harness):
    h = harness
    h.llm.queue(LLMResponse(text="Hi there", tool_calls=[], stop_reason="end_turn"))
    h.history.fail_next_append_after_write()

    await start_conversation(h, inbound("Ev1", "hello"))
    await eventually(lambda: h.slack.updates())

    assert h.slack.updates()[0]["text"] == "Hi there"
    conv_id = ids.conversation_id(TENANT, CHANNEL, THREAD)
    stored = await h.history.load(TENANT, conv_id)
    assert [m["role"] for m in stored] == ["user", "assistant"]
    assert [text_of(m) for m in stored if m["role"] == "user"] == [["hello"]]


async def test_two_messages_same_text_both_stored(harness):
    h = harness
    h.llm.queue(
        LLMResponse(text="one", tool_calls=[], stop_reason="end_turn"),
        LLMResponse(text="two", tool_calls=[], stop_reason="end_turn"),
    )

    handle = await start_conversation(h, inbound("Ev1", "same"))
    await eventually(lambda: len(h.slack.updates()) >= 1)

    await handle.signal(ConversationWorkflow.message, inbound("Ev2", "same"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    conv_id = ids.conversation_id(TENANT, CHANNEL, THREAD)
    stored = await h.history.load(TENANT, conv_id)
    users = [text_of(m) for m in stored if m["role"] == "user"]
    assert users == [["same"], ["same"]]


async def test_failed_finalize_after_reply_stored_no_double_assistant(harness):
    h = harness
    h.llm.queue(LLMResponse(text="The answer", tool_calls=[], stop_reason="end_turn"))
    h.slack.fail_next_update(ValueError("slack down"))

    await start_conversation(h, inbound("Ev1", "question"))
    await eventually(lambda: h.slack.updates())

    assert h.slack.updates()[0]["text"] == TURN_FAILED
    assert h.slack.updates()[0]["ts"] == placeholders(h)[0]["ts"]

    conv_id = ids.conversation_id(TENANT, CHANNEL, THREAD)
    stored = await h.history.load(TENANT, conv_id)
    roles = [m["role"] for m in stored]
    assert roles == ["user", "assistant"]
    assert all(a != b for a, b in itertools.pairwise(roles))
    assert text_of(stored[1]) == ["The answer"]
    assert not any(TURN_FAILED in text_of(m) for m in stored)


async def test_post_once_logs_possible_duplicate(agents, caplog):
    acts = make_activities(agents, ScriptedLLMClient())
    await acts.idem.put_if_absent(TENANT, "k1:pending", {"channel": "C1"})

    with caplog.at_level(logging.WARNING, logger="agentos.activities"):
        ts = await acts._post_once(TENANT, "k1", "C1", "secret message text", "1.1")

    assert len(acts.slack.posts()) == 1
    assert (await acts.idem.get(TENANT, "k1"))["ts"] == ts
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "possible duplicate Slack post" in warnings[0]
    assert "k1" in warnings[0]
    assert "secret message text" not in caplog.text


async def test_post_once_returns_recorded_ts_without_posting(agents):
    acts = make_activities(agents, ScriptedLLMClient())
    await acts.idem.put_if_absent(TENANT, "k1", {"channel": "C1", "ts": "9.9"})

    ts = await acts._post_once(TENANT, "k1", "C1", "text", None)

    assert ts == "9.9"
    assert acts.slack.posts() == []
