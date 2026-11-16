"""What a turn may do: the pinned version, bad tool input, write limits, cut-off replies, and the
thread's agent after a long silence."""

import copy
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from temporalio.client import WorkflowExecutionStatus
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Replayer

from agentos import ids
from agentos.audience import refusal_text
from agentos.connectors import manifest
from agentos.llm import QwenLLMClient, ScriptedLLMClient
from agentos.models import LLMResponse, TaskSpec, ToolCall, TurnContext
from agentos.workflows.conversation import (
    IDLE_TIMEOUT,
    TURN_FAILED,
    ConversationWorkflow,
)
from agentos.workflows.task import TaskWorkflow
from tests.child_starts import ChildStarts
from tests.conftest import ALICE, DB_ID, TENANT, agents_dict, eventually
from tests.test_agent_store import republish
from tests.test_audience import CHAN, OWNER, converse, say, tenant_dict
from tests.test_audience import running as running_audience
from tests.test_conversation_workflow import (
    CHANNEL,
    THREAD,
    inbound,
    start_conversation,
)
from tests.test_front_door import (
    answer,
    front_door_dict,
    hand_off_to,
    tool_turn,
    with_item_rules,
)
from tests.test_front_door import converse as converse_front_door
from tests.test_front_door import message as front_door_message
from tests.test_front_door import running as running_front_door
from tests.test_llm import FakeOpenAI, _response, _tool_call
from tests.test_turn_failure import tool_result_ids, tool_use_ids

CONV_ID = ids.conversation_id(TENANT, CHANNEL, THREAD)


def create(call_id: str, title: str = "New monitor", details: str = "Flickers.") -> ToolCall:
    return ToolCall(id=call_id, name="create_item", input={"title": title, "details": details})


def cards(h) -> list[dict]:
    return [p for p in h.slack.posts() if p["blocks"]]


def results_by_id(messages) -> dict[str, dict]:
    return {
        b["tool_use_id"]: b
        for m in messages
        for b in m["content"]
        if isinstance(b, dict) and b.get("type") == "tool_result"
    }


# --- the version a turn started with ------------------------------------------------------------


class PublishesOnFirstCall(ScriptedLLMClient):
    """Publishes a new version of the agent while the first model call is in flight."""

    def __init__(self, store):
        super().__init__()
        self.store = store

    async def complete(self, system, messages, tools):
        first = not self.calls
        response = await super().complete(system, messages, tools)
        if first:
            await republish(self.store, "it-helper", lambda d: d.update(system_prompt="NEW PROMPT"))
        return response


async def test_a_version_published_mid_turn_does_not_change_the_prompt_of_later_steps(harness):
    """Fails if a step of a turn runs another version's prompt than the one the turn started with."""
    h = harness
    h.acts.llm = PublishesOnFirstCall(h.agent_store)
    h.acts.llm.queue(
        tool_turn(ToolCall("tu_s", "search_items", {"query": "laptop"})), answer("Done.")
    )

    await start_conversation(h, inbound("Ev1"))
    await eventually(lambda: h.slack.updates())

    first, second = (call["system"] for call in h.acts.llm.calls)
    assert "You help employees with IT requests." in first
    assert second == first


# --- a failure that is not an activity's ---------------------------------------------------------


async def test_a_task_that_cannot_start_ends_the_turn_not_the_conversation():
    """Fails if a child workflow that does not start ends the conversation or leaves "Thinking…"."""
    async with running_front_door(agents_dict(), [ChildStarts(refuse=True)]) as h:
        h.llm.queue(tool_turn(create("tu_1")), answer("Back again."))

        handle = await converse_front_door(h, "it-helper", front_door_message("Ev1"))
        await eventually(lambda: h.slack.updates())
        assert h.slack.updates()[0]["text"] == TURN_FAILED
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
        stored = await h.history.load(TENANT, CONV_ID)
        assert tool_use_ids(stored) == tool_result_ids(stored) == ["tu_1"]

        await handle.signal(ConversationWorkflow.message, front_door_message("Ev2", "there?"))
        await eventually(lambda: len(h.slack.updates()) >= 2)
        assert h.slack.updates()[1]["text"] == "Back again."


@pytest.mark.parametrize(
    ("call", "problem"),
    [
        (ToolCall("tu_1", "search_items", {}), "missing required field 'query'"),
        (ToolCall("tu_1", "search_items", {"query": "x", "bogus": 1}), "unexpected field 'bogus'"),
        (ToolCall("tu_1", "search_items", {"query": 5}), "'query' must be a string"),
        (
            ToolCall("tu_1", "create_item", {"title": ["a"], "details": "d"}),
            "'title' must be a string",
        ),
    ],
    ids=["missing", "extra", "wrong type", "write with wrong type"],
)
async def test_malformed_tool_input_is_a_tool_error_and_the_turn_goes_on(harness, call, problem):
    """Fails if input that does not fit the tool's schema reaches the tool, or ends the turn."""
    h = harness
    h.llm.queue(tool_turn(call), answer("Sorry about that."))

    await start_conversation(h, inbound("Ev1"))
    await eventually(lambda: h.slack.updates())

    result = results_by_id(h.llm.calls[1]["messages"])["tu_1"]
    assert result["is_error"] is True
    assert result["content"] == f"Invalid input for {call.name}: {problem}."
    assert h.slack.updates()[0]["text"] == "Sorry about that."
    assert cards(h) == []


# --- how many writes one reply may start ---------------------------------------------------------


async def test_writes_beyond_the_limit_get_an_error_and_start_nothing():
    """Fails if one reply starts more than two approval cards, or the extra calls get no result."""
    starts = ChildStarts()
    async with running_front_door(agents_dict(), [starts]) as h:
        calls = [create(f"tu_{n}", title=f"Item {n}") for n in range(5)]
        h.llm.queue(tool_turn(*calls), answer("Two are drafted."))

        await converse_front_door(h, "it-helper", front_door_message("Ev1"))
        await eventually(lambda: h.slack.updates())

        assert len(starts.started) == 2
        results = results_by_id(h.llm.calls[1]["messages"])
        errors = [results[f"tu_{n}"]["is_error"] for n in range(5)]
        assert errors == [False, False, True, True, True]
        assert "at most 2 writes" in results["tu_4"]["content"]


async def test_identical_writes_in_one_reply_start_one_task():
    """Fails if the same write twice in one reply sends two approval cards."""
    starts = ChildStarts()
    async with running_front_door(agents_dict(), [starts]) as h:
        h.llm.queue(tool_turn(create("tu_a"), create("tu_b")), answer("Drafted."))

        await converse_front_door(h, "it-helper", front_door_message("Ev1"))
        await eventually(lambda: h.slack.updates())

        assert len(starts.started) == 1
        results = results_by_id(h.llm.calls[1]["messages"])
        assert results["tu_a"]["is_error"] is False
        assert results["tu_b"] == {
            "type": "tool_result",
            "tool_use_id": "tu_b",
            "content": "Not run: identical to an earlier call in this reply.",
            "is_error": True,
        }


async def test_an_agent_sets_how_many_writes_one_reply_may_start():
    """Fails if max_writes_per_turn in the definition is ignored."""
    raw = agents_dict()
    raw["agents"]["it-helper"]["max_writes_per_turn"] = 1
    starts = ChildStarts()
    async with running_front_door(raw, [starts]) as h:
        h.llm.queue(tool_turn(create("tu_1", "One"), create("tu_2", "Two")), answer("Drafted."))

        await converse_front_door(h, "it-helper", front_door_message("Ev1"))
        await eventually(lambda: h.slack.updates())

        assert len(starts.started) == 1


# --- what the manifest says is a write -----------------------------------------------------------


async def test_a_write_the_manifest_declares_needs_approval_whatever_it_is_called(
    harness, monkeypatch
):
    """Fails if only tools named create_item or add_comment are treated as writes."""
    h = harness
    create_spec = manifest.TOOLS["create_item"]
    ticket = manifest.ToolSpec(
        name="file_ticket",
        risk=manifest.RiskClass.WRITE_IRREVERSIBLE,
        description="File a ticket.",
        input_schema=create_spec.input_schema,
        idempotency=create_spec.idempotency,
    )
    monkeypatch.setitem(manifest.TOOLS, "file_ticket", ticket)
    call = ToolCall("tu_t", "file_ticket", {"title": "Printer", "details": "Jams."})
    h.llm.queue(tool_turn(call), answer("Drafted."))

    await start_conversation(h, inbound("Ev1", "file a ticket"))
    await eventually(lambda: h.slack.updates())

    (card,) = await eventually(lambda: cards(h))
    assert card["channel"] == f"D-{ALICE}"
    assert h.notion.all_pages(DB_ID) == [], "nothing is created before the requester approves"
    assert "approval card" in results_by_id(h.llm.calls[1]["messages"])["tu_t"]["content"]


async def test_an_approved_write_the_manifests_no_longer_describe_is_refused_and_audited(
    harness, monkeypatch
):
    """Fails if a write is sent although no manifest declares the idempotency method it relies on."""
    h = harness
    changed = replace(manifest.TOOLS["create_item"], idempotency="something_else")
    monkeypatch.setitem(manifest.TOOLS, "create_item", changed)
    ctx = TurnContext(TENANT, "it-helper", 1, ALICE, CHANNEL, THREAD, "channel")
    spec = TaskSpec(ctx, "create_item", {"title": "T", "details": "d", "database_id": DB_ID}, "n")

    with pytest.raises(ApplicationError) as err:
        await ActivityEnvironment().run(h.acts.create_notion_item, spec, "task-1")

    assert err.value.non_retryable
    assert h.notion.all_pages(DB_ID) == []
    assert [(e.kind, e.outcome) for e in h.audit.events][-1] == ("write", "error")


async def test_a_read_tool_never_starts_a_task():
    """Fails if a read tool call is answered with an approval card."""
    starts = ChildStarts()
    async with running_front_door(agents_dict(), [starts]) as h:
        h.llm.queue(
            tool_turn(ToolCall("tu_r", "search_items", {"query": "create"})), answer("None.")
        )

        await converse_front_door(h, "it-helper", front_door_message("Ev1"))
        await eventually(lambda: h.slack.updates())

        assert starts.started == []
        assert cards(h) == []


# --- a reply that was cut off --------------------------------------------------------------------


async def test_a_text_reply_cut_off_at_the_token_limit_says_so(harness):
    """Fails if a reply that stopped at max_tokens is posted as if it were complete."""
    h = harness
    h.llm.queue(LLMResponse(text="Step one, then", tool_calls=[], stop_reason="max_tokens"))

    await start_conversation(h, inbound("Ev1"))
    await eventually(lambda: h.slack.updates())

    assert h.slack.updates()[0]["text"] == "Step one, then\n\n(reply cut short)"
    stored = await h.history.load(TENANT, CONV_ID)
    assert stored[-1] == {
        "role": "assistant",
        "content": [{"type": "text", "text": "Step one, then"}],
    }


async def test_tool_calls_in_a_cut_off_reply_are_not_run():
    """Fails if a tool call from a reply that stopped at max_tokens is dispatched."""
    starts = ChildStarts()
    async with running_front_door(agents_dict(), [starts]) as h:
        h.llm.queue(
            LLMResponse(
                text=None,
                tool_calls=[create("tu_c"), ToolCall("tu_s", "search_items", {"query": "x"})],
                stop_reason="max_tokens",
            ),
            answer("Shorter."),
        )

        await converse_front_door(h, "it-helper", front_door_message("Ev1"))
        await eventually(lambda: h.slack.updates())

        assert starts.started == []
        assert not [e for e in h.audit.events if e.kind == "tool_call"]
        results = results_by_id(h.llm.calls[1]["messages"])
        cut_off = "Your reply was cut off. Try again with less."
        assert (results["tu_c"]["content"], results["tu_c"]["is_error"]) == (cut_off, True)
        assert (results["tu_s"]["content"], results["tu_s"]["is_error"]) == (cut_off, True)
        assert h.slack.updates()[0]["text"] == "Shorter."


# --- the thread's specialist ---------------------------------------------------------------------


async def test_a_thread_idle_for_over_a_week_keeps_its_specialist():
    """Fails if a thread handed to a specialist goes back to the front door after the workflow
    closed for being idle."""
    async with running_front_door(front_door_dict()) as h:
        h.llm.queue(tool_turn(hand_off_to("it-helper")), answer("Ask IT."))
        first = await converse_front_door(h, "concierge", front_door_message("Ev1"))
        await eventually(lambda: len(h.slack.updates()) >= 2)

        await first.result()
        assert (await first.describe()).status == WorkflowExecutionStatus.COMPLETED
        await h.wf_env.sleep(IDLE_TIMEOUT + timedelta(days=1))

        h.llm.queue(answer("Still IT here."))
        await converse_front_door(h, "concierge", front_door_message("Ev2", "and another thing"))
        await eventually(lambda: len(h.slack.updates()) >= 3)

        assert h.llm.calls[-1]["system"] == with_item_rules("You help employees with IT requests.")
        assert h.slack.updates()[-1]["text"] == "Still IT here."
        assert len([u for u in h.slack.updates() if "picking this up" in u["text"]]) == 1


async def test_a_thread_with_a_hand_off_limited_writes_and_a_cut_off_reply_replays():
    """Fails if the turn loop issues commands in an order that a replay of its own history rejects."""
    async with running_front_door(front_door_dict()) as h:
        h.llm.queue(
            tool_turn(hand_off_to("it-helper")),
            tool_turn(create("tu_1", "One"), create("tu_2", "Two"), create("tu_3", "Three")),
            LLMResponse(text="Drafted two, and", tool_calls=[], stop_reason="max_tokens"),
        )
        handle = await converse_front_door(h, "concierge", front_door_message("Ev1"))
        await eventually(lambda: len(h.slack.updates()) >= 2)
        await eventually(lambda: h.slack.updates()[-1]["text"].endswith("(reply cut short)"))

        history = await handle.fetch_history()

    await Replayer(workflows=[ConversationWorkflow, TaskWorkflow]).replay_workflow(history)


# --- the owner is told once a day ----------------------------------------------------------------


async def test_the_owner_is_mentioned_once_a_day_for_the_same_asker_in_the_same_place():
    """Fails if a person who keeps asking an agent they may not use pages its owner every time."""
    raw = tenant_dict({"channels": [CHAN], "dm": "everyone"})
    today = [datetime(2026, 11, 16, 9, 0, tzinfo=UTC)]
    async with running_audience(raw) as h:
        h.acts.clock = lambda: today[0]
        handle = await converse(h, "it-helper", say("Ev1", ALICE, "C_OTHER", "channel"))
        await eventually(lambda: len(h.slack.posts()) == 1)
        await handle.signal(ConversationWorkflow.message, say("Ev2", ALICE, "C_OTHER", "channel"))
        await eventually(lambda: len(h.slack.posts()) == 2)
        today[0] += timedelta(days=1)
        await handle.signal(ConversationWorkflow.message, say("Ev3", ALICE, "C_OTHER", "channel"))
        await eventually(lambda: len(h.slack.posts()) == 3)

        texts = [p["text"] for p in h.slack.posts()]
        assert texts[0] == refusal_text("IT helper", OWNER)
        assert OWNER not in texts[1] and "isn't available to you here" in texts[1]
        assert texts[2] == texts[0]


# --- tool-call IDs from a server that reuses them ------------------------------------------------


class QueuedOpenAI(FakeOpenAI):
    """A fake OpenAI client that answers each call with the next queued completion."""

    def __init__(self, *responses):
        super().__init__()
        self._queue = list(responses)

    async def _create(self, **kwargs):
        self.calls.append(copy.deepcopy(kwargs))
        return self._queue.pop(0)


async def test_two_steps_whose_server_gave_the_same_call_id_both_reach_the_model(harness):
    """Fails if the second result of a reused tool-call ID is dropped and the model sees the first."""
    h = harness
    h.notion.add_page(DB_ID, {"Name": {"title": [{"text": {"content": "Laptop battery"}}]}})
    h.notion.add_page(DB_ID, {"Name": {"title": [{"text": {"content": "Monitor flicker"}}]}})
    search = lambda q: _tool_call("call_0", "search_items", f'{{"query": "{q}"}}')
    server = QueuedOpenAI(
        _response(tool_calls=[search("laptop")], finish_reason="tool_calls"),
        _response(tool_calls=[search("monitor")], finish_reason="tool_calls"),
        _response(content="Found both."),
    )
    h.acts.llm = QwenLLMClient("unused", "http://fake.invalid", "qwen-test", client=server)

    await start_conversation(h, inbound("Ev1", "laptop and monitor requests?"))
    await eventually(lambda: h.slack.updates())

    assert h.slack.updates()[0]["text"] == "Found both."
    sent = server.calls[2]["messages"]
    tool_messages = [m for m in sent if m["role"] == "tool"]
    assert [m["tool_call_id"] for m in tool_messages] == ["call_0", "call_0"]
    assert "Laptop battery" in tool_messages[0]["content"]
    assert "Monitor flicker" in tool_messages[1]["content"]
