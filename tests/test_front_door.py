"""A front door hands a thread to a specialist, and where Slack messages land."""

import copy
import json
import logging
from contextlib import asynccontextmanager

import pytest
from fastapi.testclient import TestClient
from slack_sdk.signature import SignatureVerifier
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_store import seeded_agent_store
from agentos.config import TASK_QUEUE, ConfigError, Settings, TenantsConfig, parse_agents_config
from agentos.gateway.app import create_app
from agentos.llm import ScriptedLLMClient
from agentos.models import ConversationRef, InboundMessage, LLMResponse, ToolCall
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for
from agentos.workflows.conversation import (
    OUT_OF_STEPS,
    TURN_FAILED,
    ConversationWorkflow,
)
from agentos.workflows.task import TaskWorkflow
from tests.conftest import (
    ALICE,
    BOB,
    TENANT,
    Harness,
    agents_dict,
    eventually,
    make_fake_notion,
)
from tests.test_gateway import FakeTemporalClient, im_event, post_event
from tests.test_llm_errors import StatusError

CHANNEL = "C_GENERAL"
THREAD = "1700000000.000100"
CONV_ID = ids.conversation_id(TENANT, CHANNEL, THREAD)
ANNOUNCEMENT = "IT helper here, picking this up."
NOT_RUN = "Not run: the conversation was handed to a specialist."


def front_door_dict(concierge_steps: int = 4) -> dict:
    """``concierge`` is a front door for ``it-helper``; ``it-helper`` has the Notion grant."""
    raw = agents_dict()
    helper = raw["agents"]["it-helper"]
    helper["description"] = "Laptops, accounts, VPN, access and other IT requests."
    concierge = copy.deepcopy(helper)
    concierge["name"] = "Concierge"
    concierge["description"] = "General questions about the company."
    concierge["system_prompt"] = "You answer general questions."
    concierge["max_steps"] = concierge_steps
    concierge["grants"] = {}
    concierge["specialists"] = ["it-helper"]
    raw["agents"]["concierge"] = concierge
    raw["agents"]["unlisted"] = {**copy.deepcopy(helper), "name": "Unlisted"}
    raw["default_agent"] = "concierge"
    return raw


@asynccontextmanager
async def running(raw: dict):
    config = parse_agents_config(raw)
    wf_env = await WorkflowEnvironment.start_time_skipping()
    h = Harness(
        wf_env=wf_env,
        agents=config,
        llm=ScriptedLLMClient(),
        slack=FakeSlack(emails={ALICE: "alice@example.com", BOB: "bob@example.com"}),
        notion=make_fake_notion(),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        tool_outputs=InMemoryToolOutputStore(),
        agent_store=seeded_agent_store(config),
    )
    acts = AgentActivities(
        settings=Settings(),
        registry=TenantRegistry({TENANT: runtime_for(config, h.slack, h.notion)}),
        history=h.history,
        idem=h.idem,
        links=h.links,
        llm=h.llm,
        tool_outputs=h.tool_outputs,
        audit=h.audit,
        agents=h.agent_store,
    )
    worker = Worker(
        wf_env.client,
        task_queue=TASK_QUEUE,
        workflows=[ConversationWorkflow, TaskWorkflow],
        activities=acts.all(),
    )
    async with worker:
        try:
            yield h
        finally:
            await wf_env.shutdown()


@pytest.fixture
async def fd():
    async with running(front_door_dict()) as h:
        yield h


@pytest.fixture
async def fd_one_step():
    async with running(front_door_dict(concierge_steps=1)) as h:
        yield h


def message(event_id: str, text: str = "my laptop battery is swelling") -> InboundMessage:
    return InboundMessage(event_id, ALICE, text, CHANNEL, THREAD, "channel")


async def converse(h, agent_id: str, msg: InboundMessage):
    return await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, agent_id, CHANNEL, THREAD),
        id=CONV_ID,
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )


def hand_off_to(agent: str, call_id: str = "tu_h") -> ToolCall:
    return ToolCall(id=call_id, name="hand_off", input={"agent": agent})


def tool_turn(*calls: ToolCall) -> LLMResponse:
    return LLMResponse(text=None, tool_calls=list(calls), stop_reason="tool_use")


def answer(text: str) -> LLMResponse:
    return LLMResponse(text=text, tool_calls=[], stop_reason="end_turn")


def tool_names(call: dict) -> list[str]:
    return [t["name"] for t in call["tools"]]


def announcements(h) -> list[dict]:
    return [p for p in h.slack.posts() if "picking this up" in p["text"]]


# --- config ----------------------------------------------------------------


def test_config_without_front_door_keys_parses_unchanged():
    config = parse_agents_config(agents_dict())
    helper = config.agents["it-helper"]
    assert helper.description == ""
    assert helper.specialists == []
    assert config.channels == {}


def test_config_front_door_parses():
    raw = front_door_dict()
    raw["channels"] = {"C0123456789": "it-helper"}
    config = parse_agents_config(raw)
    assert config.agents["concierge"].specialists == ["it-helper"]
    assert config.agents["it-helper"].description.startswith("Laptops")
    assert config.channels == {"C0123456789": "it-helper"}


def _undefined_specialist(raw):
    raw["agents"]["concierge"]["specialists"] = ["ghost"]


def _own_specialist(raw):
    raw["agents"]["concierge"]["specialists"] = ["concierge"]


def _no_description(raw):
    raw["agents"]["it-helper"]["description"] = " "


def _nested_front_door(raw):
    raw["agents"]["it-helper"]["specialists"] = ["unlisted"]
    raw["agents"]["unlisted"]["description"] = "Something."


def _duplicate_specialist(raw):
    raw["agents"]["concierge"]["specialists"] = ["it-helper", "it-helper"]


def _undefined_channel_agent(raw):
    raw["channels"] = {"C0123456789": "ghost"}


@pytest.mark.parametrize(
    "mutate, expected",
    [
        (_undefined_specialist, ["agent 'concierge'", "'ghost'", "not a defined agent"]),
        (_own_specialist, ["agent 'concierge'", "its own specialist"]),
        (_no_description, ["agent 'concierge'", "'it-helper'", "non-empty 'description'"]),
        (_nested_front_door, ["agent 'concierge'", "specialist 'it-helper'", "front door"]),
        (_duplicate_specialist, ["agent 'concierge'", "'it-helper'", "more than once"]),
        (_undefined_channel_agent, ["channel 'C0123456789'", "'ghost'", "not defined"]),
    ],
)
def test_config_front_door_validation(mutate, expected):
    raw = front_door_dict()
    mutate(raw)
    with pytest.raises(ConfigError) as err:
        parse_agents_config(raw)
    for part in expected:
        assert part in str(err.value)


# --- the hand-off tool -------------------------------------------------------


async def test_hand_off_tool_offered_only_to_front_door(fd):
    h = fd
    h.llm.queue(answer("front"), answer("specialist"))

    await converse(h, "concierge", message("Ev1"))
    await eventually(lambda: len(h.slack.updates()) >= 1)
    await h.client.get_workflow_handle(CONV_ID).signal(ConversationWorkflow.message, message("Ev2"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    front_call, second_call = h.llm.calls
    assert tool_names(front_call).count("hand_off") == 1
    definition = next(t for t in front_call["tools"] if t["name"] == "hand_off")
    assert definition["input_schema"] == {
        "type": "object",
        "properties": {"agent": {"type": "string", "enum": ["it-helper"]}},
        "required": ["agent"],
    }
    assert "- it-helper: IT helper. Laptops, accounts, VPN" in definition["description"]
    # No hand-off happened, so the second message still goes to the front door.
    assert tool_names(second_call) == tool_names(front_call)


async def test_specialist_is_offered_no_hand_off(fd):
    h = fd
    h.llm.queue(answer("ok"))

    await converse(h, "it-helper", message("Ev1"))
    await eventually(lambda: h.slack.updates())

    assert "hand_off" not in tool_names(h.llm.calls[0])


async def test_hand_off_specialist_answers_same_message(fd):
    h = fd
    h.llm.queue(tool_turn(hand_off_to("it-helper")), answer("Report it to IT."))

    await converse(h, "concierge", message("Ev1"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    # The front door's placeholder becomes the announcement, and the specialist
    # answers in a placeholder of its own below it.
    placeholders = [p for p in h.slack.posts() if p["text"] == "Thinking…"]
    assert len(placeholders) == 2
    assert all(p["thread_ts"] == THREAD for p in placeholders)
    assert [u["text"] for u in h.slack.updates()] == [ANNOUNCEMENT, "Report it to IT."]
    assert [u["ts"] for u in h.slack.updates()] == [p["ts"] for p in placeholders]
    assert announcements(h) == []
    assert h.llm.calls[0]["system"] == "You answer general questions."
    assert h.llm.calls[1]["system"] == "You help employees with IT requests."
    # The specialist sees the hand-off in history, but is not offered the tool.
    assert "hand_off" not in tool_names(h.llm.calls[1])
    result = h.llm.calls[1]["messages"][-1]["content"][0]
    assert result["tool_use_id"] == "tu_h"
    assert result["content"] == "Handed to it-helper. They will answer now."


async def test_next_message_goes_to_specialist(fd):
    h = fd
    h.llm.queue(tool_turn(hand_off_to("it-helper")), answer("First."), answer("Second."))

    await converse(h, "concierge", message("Ev1"))
    await eventually(lambda: len(h.slack.updates()) >= 2)
    await h.client.get_workflow_handle(CONV_ID).signal(
        ConversationWorkflow.message, message("Ev2", "and my VPN?")
    )
    await eventually(lambda: len(h.slack.updates()) >= 3)

    third = h.llm.calls[2]
    assert third["system"] == "You help employees with IT requests."
    assert "hand_off" not in tool_names(third)
    assert [u["text"] for u in h.slack.updates()] == [ANNOUNCEMENT, "First.", "Second."]


async def test_hand_off_to_unlisted_agent_refused(fd):
    h = fd
    h.llm.queue(tool_turn(hand_off_to("unlisted")), answer("I will handle it myself."))

    await converse(h, "concierge", message("Ev1"))
    await eventually(lambda: h.slack.updates())

    result = h.llm.calls[1]["messages"][-1]["content"][0]
    assert result["is_error"] is True
    assert result["content"] == "unlisted is not one of this agent's specialists."
    assert h.llm.calls[1]["system"] == "You answer general questions."
    assert announcements(h) == []
    assert h.slack.updates()[0]["text"] == "I will handle it myself."


async def test_second_hand_off_in_one_response_refused(fd):
    h = fd
    h.llm.queue(
        tool_turn(
            hand_off_to("unlisted", "tu_bad"),
            hand_off_to("it-helper", "tu_a"),
            hand_off_to("it-helper", "tu_b"),
        ),
        answer("Done."),
    )

    await converse(h, "concierge", message("Ev1"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    results = {b["tool_use_id"]: b for b in h.llm.calls[1]["messages"][-1]["content"]}
    assert results["tu_bad"]["is_error"] is True
    assert "is_error" not in results["tu_a"] or results["tu_a"]["is_error"] is False
    for call_id in ("tu_bad", "tu_b"):
        assert results[call_id]["is_error"] is True
        assert results[call_id]["content"] == NOT_RUN
    assert [u["text"] for u in h.slack.updates()] == [ANNOUNCEMENT, "Done."]
    assert h.llm.calls[1]["system"] == "You help employees with IT requests."


async def test_hand_off_step_keys_unique(fd):
    h = fd
    h.llm.queue(tool_turn(hand_off_to("it-helper")), answer("Report it to IT."))

    await converse(h, "concierge", message("Ev1", "battery"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    stored = await h.history.load(TENANT, CONV_ID)
    assert [m["role"] for m in stored] == ["user", "assistant", "user", "assistant"]
    assert stored[0]["content"] == [{"type": "text", "text": "battery"}]
    assert stored[1]["content"][0]["name"] == "hand_off"
    assert stored[2]["content"][0]["tool_use_id"] == "tu_h"
    assert stored[3]["content"] == [{"type": "text", "text": "Report it to IT."}]
    assert h.history.step_keys[(TENANT, CONV_ID)] == {
        "Ev1:user": 1,
        "Ev1:s0:assistant": 2,
        "Ev1:s0:results": 3,
        "Ev1:h1:s0:assistant": 4,
    }


async def test_hand_off_announcement_survives_a_slack_retry(fd):
    h = fd
    h.llm.queue(tool_turn(hand_off_to("it-helper")), answer("Report it to IT."))
    update_message = h.slack.update_message
    failures = []

    async def flaky(channel, ts, text, blocks=None):
        if "picking this up" in text and not failures:
            failures.append(text)
            raise RuntimeError("slack is down")
        return await update_message(channel, ts, text, blocks=blocks)

    h.slack.update_message = flaky

    await converse(h, "concierge", message("Ev1"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    assert len(failures) == 1
    assert [u["text"] for u in h.slack.updates()] == [ANNOUNCEMENT, "Report it to IT."]
    assert len([p for p in h.slack.posts() if p["text"] == "Thinking…"]) == 2


async def test_hand_off_on_last_step_still_hands_off(fd_one_step):
    h = fd_one_step
    h.llm.queue(tool_turn(hand_off_to("it-helper")), answer("Report it to IT."))

    await converse(h, "concierge", message("Ev1"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    assert [u["text"] for u in h.slack.updates()] == [ANNOUNCEMENT, "Report it to IT."]
    results = h.llm.calls[1]["messages"][-1]["content"]
    assert results[0]["content"] == "Handed to it-helper. They will answer now."


async def test_invalid_hand_off_on_last_step_runs_the_step_limit(fd_one_step):
    h = fd_one_step
    h.llm.queue(tool_turn(hand_off_to("unlisted")))

    await converse(h, "concierge", message("Ev1"))
    await eventually(lambda: h.slack.updates())

    assert h.slack.updates()[0]["text"] == OUT_OF_STEPS
    assert announcements(h) == []
    assert len(h.llm.calls) == 1


async def test_specialist_survives_continue_as_new(fd):
    h = fd
    h.llm.queue(answer("As the specialist."))

    # A continue-as-new passes the thread's current ref, here the specialist's.
    await converse(h, "it-helper", message("Ev1"))
    await eventually(lambda: h.slack.updates())

    assert h.llm.calls[0]["system"] == "You help employees with IT requests."
    assert "hand_off" not in tool_names(h.llm.calls[0])
    assert [u["text"] for u in h.slack.updates()] == ["As the specialist."]


async def test_specialist_failure_recovers_as_specialist(fd, caplog):
    h = fd
    caplog.set_level(logging.INFO)
    h.llm.queue(
        tool_turn(hand_off_to("it-helper")),
        StatusError(401),
        answer("Back again."),
    )

    await converse(h, "concierge", message("Ev1"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    placeholder = [p for p in h.slack.posts() if p["text"] == "Thinking…"]
    assert [u["text"] for u in h.slack.updates()] == [ANNOUNCEMENT, TURN_FAILED]
    assert h.slack.updates()[1]["ts"] == placeholder[1]["ts"]
    finalized = [
        r.getMessage() for r in caplog.records if "activity=finalize_reply" in r.getMessage()
    ]
    # The first finalize is the announcement, by the front door; the failure
    # notice is the second, and it is sent as the specialist.
    assert len(finalized) == 2
    assert "agent_id=concierge" in finalized[0]
    assert "agent_id=it-helper" in finalized[1]

    await h.client.get_workflow_handle(CONV_ID).signal(
        ConversationWorkflow.message, message("Ev2", "are you there?")
    )
    await eventually(lambda: len(h.slack.updates()) >= 3)

    assert h.llm.calls[2]["system"] == "You help employees with IT requests."
    assert h.slack.updates()[2]["text"] == "Back again."


async def test_task_after_hand_off_carries_specialist(fd):
    h = fd
    create = ToolCall(
        id="tu_c",
        name="create_item",
        input={"title": "New monitor", "details": "Mine flickers."},
    )
    h.llm.queue(
        tool_turn(hand_off_to("it-helper")),
        tool_turn(create),
        answer("Sent you an approval card."),
    )

    await converse(h, "concierge", message("Ev1", "please order me a monitor"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    card = (await eventually(lambda: [p for p in h.slack.posts() if p["blocks"]]))[0]
    actions = next(b for b in card["blocks"] if b["type"] == "actions")
    task_id = json.loads(actions["elements"][0]["value"])["task_id"]
    history = await h.client.get_workflow_handle(task_id).fetch_history()
    started = next(
        e.workflow_execution_started_event_attributes
        for e in history.events
        if e.HasField("workflow_execution_started_event_attributes")
    )
    spec = json.loads(started.input.payloads[0].data)
    assert spec["ctx"]["agent_id"] == "it-helper"
    assert spec["ctx"]["requester_user_id"] == ALICE


async def test_specialist_load_failure_keeps_the_thread_with_the_front_door(fd, caplog):
    h = fd
    caplog.set_level(logging.INFO)
    h.llm.queue(tool_turn(hand_off_to("it-helper")), answer("Still me."))
    complete = h.llm.complete

    async def complete_then_drop_specialist(system, messages, tools):
        response = await complete(system, messages, tools)
        # The specialist disappears from the config after the front door chose it.
        h.agent_store.set_published_unchecked(TENANT, "it-helper", None)
        return response

    h.llm.complete = complete_then_drop_specialist

    await converse(h, "concierge", message("Ev1"))
    await eventually(lambda: h.slack.updates())

    assert [u["text"] for u in h.slack.updates()] == [TURN_FAILED]
    assert len([p for p in h.slack.posts() if p["text"] == "Thinking…"]) == 1
    stored = await h.history.load(TENANT, CONV_ID)
    assert [m["role"] for m in stored] == ["user", "assistant", "user", "assistant"]
    closed = stored[2]["content"][0]
    assert closed["tool_use_id"] == "tu_h"
    assert closed["content"] == "The tool call did not complete."
    assert closed["is_error"] is True
    assert all(
        "agent_id=concierge" in r.getMessage()
        for r in caplog.records
        if "turn failed" in r.getMessage()
    )

    h.agent_store.set_published_unchecked(
        TENANT, "it-helper", front_door_dict()["agents"]["it-helper"]
    )
    await h.client.get_workflow_handle(CONV_ID).signal(
        ConversationWorkflow.message, message("Ev2", "hello?")
    )
    await eventually(lambda: len(h.slack.updates()) >= 2)

    assert h.llm.calls[-1]["system"] == "You answer general questions."


async def test_other_calls_beside_a_hand_off_are_not_run(fd):
    h = fd
    create = ToolCall(
        id="tu_c", name="create_item", input={"title": "Monitor", "details": "Flickers."}
    )
    read = ToolCall(id="tu_r", name="search_items", input={"query": "vpn"})
    h.llm.queue(tool_turn(create, hand_off_to("it-helper"), read), answer("Report it to IT."))

    await converse(h, "concierge", message("Ev1"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    assert [p for p in h.slack.posts() if p["blocks"]] == []
    results = {b["tool_use_id"]: b for b in h.llm.calls[1]["messages"][-1]["content"]}
    for call_id in ("tu_c", "tu_r"):
        assert results[call_id]["is_error"] is True
        assert results[call_id]["content"] == NOT_RUN
    assert results["tu_h"]["content"] == "Handed to it-helper. They will answer now."
    assert [u["text"] for u in h.slack.updates()] == [ANNOUNCEMENT, "Report it to IT."]


async def test_second_hand_off_in_a_turn_refused_even_if_specialist_lists_specialists():
    raw = front_door_dict()
    # Config validation rejects this, so build the config and add it afterwards.
    config_raw = copy.deepcopy(raw)
    async with running(config_raw) as h:
        listing = copy.deepcopy(raw["agents"]["it-helper"])
        listing["specialists"] = ["unlisted"]
        h.agent_store.set_published_unchecked(TENANT, "it-helper", listing)
        h.llm.queue(
            tool_turn(hand_off_to("it-helper")),
            tool_turn(hand_off_to("unlisted", "tu_h2")),
            answer("Staying here."),
        )

        await converse(h, "concierge", message("Ev1"))
        await eventually(lambda: len(h.slack.updates()) >= 2)

        result = h.llm.calls[2]["messages"][-1]["content"][0]
        assert result["tool_use_id"] == "tu_h2"
        assert result["is_error"] is True
        assert result["content"] == "Only one hand-off is allowed per turn."
        assert h.llm.calls[2]["system"] == "You help employees with IT requests."
        assert [u["text"] for u in h.slack.updates()] == [ANNOUNCEMENT, "Staying here."]


async def test_specialist_out_of_steps_uses_its_own_keys_and_placeholder():
    raw = front_door_dict()
    raw["agents"]["it-helper"]["max_steps"] = 1
    async with running(raw) as h:
        h.llm.queue(
            tool_turn(hand_off_to("it-helper")),
            tool_turn(ToolCall(id="tu_r", name="search_items", input={"query": "vpn"})),
        )

        await converse(h, "concierge", message("Ev1"))
        await eventually(lambda: len(h.slack.updates()) >= 2)

        placeholders = [p for p in h.slack.posts() if p["text"] == "Thinking…"]
        assert [u["text"] for u in h.slack.updates()] == [ANNOUNCEMENT, OUT_OF_STEPS]
        assert h.slack.updates()[1]["ts"] == placeholders[1]["ts"]
        keys = h.history.step_keys[(TENANT, CONV_ID)]
        assert "Ev1:h1:s0:out-of-steps" in keys
        assert "Ev1:h1:s0:results" in keys


# --- where messages land -------------------------------------------------------


def post_mention(client, secret, channel: str, ts: str = "1700000000.000300"):
    return post_event(
        client,
        secret,
        {
            "type": "event_callback",
            "team_id": "T0TEST",
            "event_id": f"Ev-{channel}-{ts}",
            "event": {
                "type": "app_mention",
                "user": ALICE,
                "text": "<@UBOT> hello",
                "channel": channel,
                "ts": ts,
            },
        },
    )


def gateway(secret):
    raw = front_door_dict()
    raw["channels"] = {"C_MAPPED": "it-helper"}
    temporal = FakeTemporalClient()
    app = create_app(
        Settings(),
        TenantsConfig.of(parse_agents_config(raw)),
        temporal,
        InMemoryTaskLinkStore(),
        SignatureVerifier(secret),
    )
    return TestClient(app), temporal


def test_mention_in_mapped_channel_starts_mapped_agent():
    secret = "s3cret"
    client, temporal = gateway(secret)

    post_mention(client, secret, "C_MAPPED")
    post_mention(client, secret, "C_OTHER")
    post_event(client, secret, im_event())

    started = [call["arg"].agent_id for call in temporal.signal_with_starts]
    assert started == ["it-helper", "concierge", "concierge"]
