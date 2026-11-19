"""Progress on the placeholder: fixed labels, throttled in workflow time, answer last."""

from datetime import timedelta

import pytest

from agentos.models import LLMResponse, ToolCall
from agentos.workflows import PROGRESS_TIMEOUT
from agentos.workflows.conversation import (
    PROGRESS_AFTER,
    ConversationWorkflow,
    progress_label,
)
from tests.conftest import eventually
from tests.helpers.conversation import THREAD, inbound, placeholders, start_conversation


def search_call(call_id: str = "tu_s") -> ToolCall:
    return ToolCall(id=call_id, name="search_items", input={"query": "laptop"})


def tool_step(call: ToolCall) -> LLMResponse:
    return LLMResponse(text=None, tool_calls=[call], stop_reason="tool_use")


def answer(text: str) -> LLMResponse:
    return LLMResponse(text=text, tool_calls=[], stop_reason="end_turn")


@pytest.mark.parametrize(
    ("name", "kind", "kwargs", "expected"),
    [
        ("search_knowledge", "read", {"source_titles": ["IT wiki"]}, "Searching IT wiki…"),
        (
            "search_knowledge",
            "read",
            {"source_titles": ["IT & <wiki>"]},
            "Searching IT &amp; &lt;wiki&gt;…",
        ),
        (
            "search_knowledge",
            "read",
            {"source_titles": ["IT wiki", "HR wiki"]},
            "Searching the knowledge sources…",
        ),
        ("search_knowledge", "read", {"source_titles": []}, "Searching the knowledge sources…"),
        ("search_items", "read", {}, "Reading Notion…"),
        ("my_items", "read", {}, "Reading Notion…"),
        ("create_item", "create_item", {}, "Drafting the request…"),
        ("add_comment", "add_comment", {}, "Drafting the request…"),
        ("hand_off", "", {"agent_name": "IT helper"}, "Handing over to IT helper…"),
        ("hand_off", "", {"agent_name": "A & <B>"}, "Handing over to A &amp; &lt;B&gt;…"),
        ("invented_tool", "", {}, None),
    ],
)
def test_progress_label(name, kind, kwargs, expected):
    """Fails if a tool's label is not the fixed runtime text, escaped, with the right fallback."""
    assert progress_label(name, kind, **kwargs) == expected


async def test_slow_steps_show_throttled_progress(harness):
    """Fails if slow steps show missing or extra progress edits, or the gap throttle is gone."""
    h = harness
    h.llm.queue(
        tool_step(search_call("tu_1")),
        tool_step(search_call("tu_2")),
        tool_step(search_call("tu_3")),
        answer("Found three."),
    )
    first = h.notion.hang_next_read()
    second = h.notion.hang_next_read()
    third = h.notion.hang_next_read()

    await start_conversation(h, inbound("EvP1", "look for laptops"))
    await first.reached.wait()
    await h.wf_env.sleep(PROGRESS_AFTER)
    await eventually(lambda: [u for u in h.slack.updates() if u["text"] == "Reading Notion…"])
    first.release()

    # Step two runs past its two-second mark but finishes inside the three-second
    # gap from step one's edit, so its label is throttled away entirely.
    await second.reached.wait()
    await h.wf_env.sleep(PROGRESS_AFTER + timedelta(seconds=0.5))
    second.release()

    # Step three outlives the gap, so its label lands.
    await third.reached.wait()
    await h.wf_env.sleep(PROGRESS_AFTER + timedelta(seconds=0.5))
    await eventually(
        lambda: len([u for u in h.slack.updates() if u["text"] == "Reading Notion…"]) == 2
    )
    third.release()

    # Settle point: the final reply, then count the placeholder's edits.
    await eventually(lambda: [u for u in h.slack.updates() if u["text"] == "Found three."])
    ph = placeholders(h)[0]["ts"]
    assert [u["text"] for u in h.slack.updates() if u["ts"] == ph] == [
        "Reading Notion…",
        "Reading Notion…",
        "Found three.",
    ]


async def test_fast_turn_shows_no_progress(harness):
    """Fails if a turn whose steps finish quickly shows a progress label."""
    h = harness
    h.llm.queue(tool_step(search_call()), answer("Found one."), answer("Sentinel."))

    handle = await start_conversation(h, inbound("EvP2", "look for laptops"))
    await eventually(lambda: [u for u in h.slack.updates() if u["text"] == "Found one."])
    # Settle point: a later message and its reply; a late label would show after it.
    await handle.signal(ConversationWorkflow.message, inbound("EvP2b", "again"))
    await eventually(lambda: [u for u in h.slack.updates() if u["text"] == "Sentinel."])

    ph = placeholders(h)[0]["ts"]
    assert [u["text"] for u in h.slack.updates() if u["ts"] == ph] == ["Found one."]


async def test_final_reply_wins_over_a_late_progress_edit(harness):
    """Fails if the reply posts before an in-flight progress edit completes, so the edit could land after it."""
    h = harness
    h.llm.queue(tool_step(search_call()), answer("The answer."), answer("Sentinel."))
    read = h.notion.hang_next_read()
    edit = h.slack.hang_next_update()

    handle = await start_conversation(h, inbound("EvP3", "look for laptops"))
    await read.reached.wait()
    await h.wf_env.sleep(PROGRESS_AFTER)
    await edit.reached.wait()  # the progress edit is in flight
    read.release()  # the step finishes while the edit is still open

    # The awaited edit blocks the answer. Advance well past the point where a
    # fire-and-forget edit would have let it post (the steps after the edit are instant);
    # the edit is still held, so the answer must not be there yet.
    await h.wf_env.sleep(PROGRESS_AFTER)
    assert not [u for u in h.slack.updates() if u["text"] == "The answer."]

    edit.release()  # the edit lands, then the answer
    await eventually(lambda: [u for u in h.slack.updates() if u["text"] == "The answer."])
    # Settle point: a later message and its reply; then the placeholder's last word.
    await handle.signal(ConversationWorkflow.message, inbound("EvP3b", "again"))
    await eventually(lambda: [u for u in h.slack.updates() if u["text"] == "Sentinel."])

    ph = placeholders(h)[0]["ts"]
    assert [u["text"] for u in h.slack.updates() if u["ts"] == ph] == [
        "Reading Notion…",
        "The answer.",
    ]


async def test_failed_progress_edit_is_dropped(harness):
    """Fails if a refused progress edit fails the turn, or its answer is still written onto the placeholder."""
    h = harness
    h.llm.queue(tool_step(search_call()), answer("Still answered."), answer("Sentinel."))
    read = h.notion.hang_next_read()
    h.slack.fail_next_update(ValueError("slack down"), times=2)

    handle = await start_conversation(h, inbound("EvP4", "look for laptops"))
    await read.reached.wait()
    # Both attempts of the one edit fail, the second past the retry gap.
    await h.wf_env.sleep(PROGRESS_AFTER + timedelta(seconds=2))
    read.release()

    # The turn still answers, as a new message; the refused edit never landed.
    await eventually(lambda: [p for p in h.slack.posts() if p["text"] == "Still answered."])
    # Settle point: a later message and its reply, then assert no edit landed.
    await handle.signal(ConversationWorkflow.message, inbound("EvP4b", "again"))
    await eventually(lambda: [u for u in h.slack.updates() if u["text"] == "Sentinel."])

    ph = placeholders(h)[0]["ts"]
    assert [u for u in h.slack.updates() if u["ts"] == ph] == []


async def test_a_dropped_progress_edit_makes_the_answer_a_new_message(harness):
    """Fails if a progress edit that times out on both attempts still lets the finalize edit the placeholder."""
    h = harness
    h.llm.queue(tool_step(search_call()), answer("The answer."), answer("Sentinel."))
    read = h.notion.hang_next_read()
    first_edit = h.slack.hang_next_update()

    handle = await start_conversation(h, inbound("EvP5", "look for laptops"))
    await read.reached.wait()
    await h.wf_env.sleep(PROGRESS_AFTER)
    await first_edit.reached.wait()  # the first attempt is held open

    # The first attempt times out; arm the retry before it starts, then let it time out too,
    # so the one edit is dropped after both attempts.
    await h.wf_env.sleep(PROGRESS_TIMEOUT + timedelta(seconds=0.5))
    retry_edit = h.slack.hang_next_update()
    await h.wf_env.sleep(PROGRESS_TIMEOUT + timedelta(seconds=1))
    read.release()

    # The answer arrives as a new message in the thread, not an edit of the placeholder.
    await eventually(lambda: [p for p in h.slack.posts() if p["text"] == "The answer."])
    # Settle point: a later message and its reply; then the placeholder's edits.
    await handle.signal(ConversationWorkflow.message, inbound("EvP5b", "again"))
    await eventually(lambda: [u for u in h.slack.updates() if u["text"] == "Sentinel."])

    ph = placeholders(h)[0]["ts"]
    assert [u for u in h.slack.updates() if u["ts"] == ph] == []
    answer_posts = [p for p in h.slack.posts() if p["text"] == "The answer."]
    assert len(answer_posts) == 1
    assert answer_posts[0]["thread_ts"] == THREAD
    first_edit.release()
    retry_edit.release()
