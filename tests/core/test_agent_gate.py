"""One gate in front of everything an agent does: agent off, a pause, an audience change."""

import ast
import inspect
import textwrap
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.activities import AgentActivities
from agentos.audience import decision_text
from agentos.config import TASK_QUEUE
from agentos.models import ApprovalDecision, TaskSpec, ToolCall, TurnContext
from agentos.slack_client import SlackLookupError, SlackUser
from agentos.switches import (
    AGENT,
    ALL_AGENTS,
    CONNECTOR,
    NOTION,
    WRITES,
    Effect,
    InMemorySwitchStore,
    Switch,
    SwitchState,
)
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, DB_ID, TEAM, TENANT, eventually

NONCE = "nonce-1"
CHANNEL = "C_GENERAL"
NOW = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)
OFF_ON_CARD = "IT helper is turned off by an admin. Nothing was changed."
REFUSED_ON_CARD = "IT helper can no longer act for you here; nothing was changed."


def ctx() -> TurnContext:
    return TurnContext(
        tenant_id=TENANT,
        agent_id="it-helper",
        agent_version=1,
        requester_user_id=ALICE,
        channel=CHANNEL,
        thread_ts="1700000000.000100",
        channel_kind="channel",
    )


def create_spec() -> TaskSpec:
    payload = {"title": "New monitor", "details": "Mine flickers.", "database_id": DB_ID}
    return TaskSpec(ctx(), "create_item", payload, NONCE)


async def turn_off(h, scope=AGENT, target="it-helper"):
    assert await h.switches.switch_off(TENANT, scope, target, "U_ADMIN", NOW)


async def start_task(h):
    handle = await h.client.start_workflow(
        TaskWorkflow.run,
        create_spec(),
        id=ids.task_id(TENANT, str(uuid.uuid4())),
        task_queue=TASK_QUEUE,
    )

    async def waiting():
        return await handle.query(TaskWorkflow.state) == "awaiting_approval"

    await eventually(waiting, timeout=20)
    return handle


async def submit(handle):
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))


def thread_posts(h, needle: str):
    return [p for p in h.slack.posts() if p["thread_ts"] and needle in p["text"]]


def nothing_written(h):
    assert h.notion.count("create_page") == 0 and h.notion.all_pages(DB_ID) == []


# --- agent off ----------------------------------------------------------------------------


async def test_agent_off_then_submit_writes_nothing_and_the_card_says_so(harness):
    """Fails if a write the requester approved still runs after an admin turned the agent off."""
    h = harness
    handle = await start_task(h)
    await turn_off(h)

    await submit(handle)

    assert await handle.result() == "cancelled"
    nothing_written(h)
    assert h.slack.updates()[-1]["text"] == OFF_ON_CARD
    assert thread_posts(h, "Filed") == []


class FlipsAfterReads(InMemorySwitchStore):
    """Shows no switch for the first reads, then the agent switch: a flip between two steps."""

    def __init__(self, quiet_reads: int):
        super().__init__()
        self.quiet_reads = quiet_reads
        self.reads = 0

    async def list(self, tenant_id):
        self.reads += 1
        if self.reads > self.quiet_reads and not await super().list(tenant_id):
            await self.switch_off(tenant_id, AGENT, "it-helper", "U_ADMIN", NOW)
        return await super().list(tenant_id)


async def test_agent_off_between_the_last_check_and_the_write_still_writes_nothing(harness):
    """Fails if the write activity trusts the check made just before it and does not gate itself."""
    h = harness
    handle = await start_task(h)
    h.acts.switches = FlipsAfterReads(quiet_reads=1)  # the pre-write check reads once

    await submit(handle)

    assert await handle.result() == "cancelled"
    nothing_written(h)
    assert h.slack.updates()[-1]["text"] == OFF_ON_CARD


@pytest.mark.parametrize("tool", ["create_item", "add_comment"])
async def test_a_write_activity_for_an_agent_that_is_off_calls_notion_not_at_all(harness, tool):
    """Fails if either write activity reaches the connector, or retries, for an agent that is off."""
    h = harness
    h.notion.add_page(DB_ID, {"Name": {"title": [{"text": {"content": "Broken VPN"}}]}})
    page = h.notion.all_pages(DB_ID)[0]
    if tool == "create_item":
        spec, run = create_spec(), h.acts.create_notion_item
    else:
        payload = {
            "page_id": page["id"],
            "database_id": DB_ID,
            "item_title": "Broken VPN",
            "item_url": page["url"],
            "comment": "Still broken",
        }
        spec, run = TaskSpec(ctx(), "add_comment", payload, NONCE), h.acts.add_notion_comment
    await turn_off(h)
    pages_before = len(h.notion.all_pages(DB_ID))

    with pytest.raises(ApplicationError) as err:
        await ActivityEnvironment().run(run, spec, "acme:task:1")

    assert err.value.non_retryable and err.value.type == "AgentOff"
    assert len(h.notion.all_pages(DB_ID)) == pages_before
    assert h.notion.count("create_page") == 0 and h.notion.count("create_comment") == 0


async def test_agent_off_during_a_watch_stops_the_polling_with_no_post(harness):
    """Fails if a watch keeps polling, or posts "Done" or "stopped", for an agent that is off."""
    h = harness
    handle = await start_task(h)
    await submit(handle)

    async def watching():
        return await handle.query(TaskWorkflow.state) == "watching"

    await eventually(watching, timeout=20)
    page_id = h.notion.all_pages(DB_ID)[0]["id"]
    await turn_off(h)
    reads_before = h.notion.count("retrieve_page")
    h.notion.set_status(page_id, "Status", "Done")

    await h.wf_env.sleep(timedelta(minutes=16))

    assert await handle.result() == "cancelled"
    assert h.notion.count("retrieve_page") == reads_before
    assert thread_posts(h, "was marked Done") == [] and thread_posts(h, "stopped watching") == []


async def test_a_read_tool_for_an_agent_that_is_off_is_a_tool_error_and_no_connector_call(harness):
    """Fails if a read dispatched before the switch still reaches Notion for an agent that is off."""
    h = harness
    await turn_off(h)
    calls_before = len(h.notion.calls)

    result = await ActivityEnvironment().run(
        h.acts.run_read_tool,
        ctx(),
        f"{TENANT}:conv:C:1",
        ToolCall("tu_1", "search_items", {"query": "monitor"}),
        1,
    )

    assert result.is_error and result.stored
    assert len(h.notion.calls) == calls_before
    stored, is_error = (await h.tool_outputs.get_many(TENANT, f"{TENANT}:conv:C:1", ["tu_1"]))[
        "tu_1"
    ]
    assert is_error and stored == "IT helper is turned off by an admin."


# --- the pre-write check: a pause, an audience change ------------------------------------------


async def pause_channel(h):
    await h.acts.pauses.open_pause(TENANT, "it-helper", CHANNEL, "channel_shared", NOW)


async def deactivate_requester(h):
    h.slack.users[ALICE] = SlackUser(ALICE, TEAM, deleted=True)


@pytest.mark.parametrize("change", [pause_channel, deactivate_requester])
async def test_a_pause_or_a_lost_audience_after_the_card_refuses_submit(harness, change):
    """Fails if Submit writes after the channel was paused or the requester left the audience."""
    h = harness
    handle = await start_task(h)
    await change(h)

    await submit(handle)

    assert await handle.result() == "cancelled"
    nothing_written(h)
    assert h.slack.updates()[-1]["text"] == REFUSED_ON_CARD
    assert thread_posts(h, "Filed") == []


async def test_submit_still_writes_when_nothing_changed_after_the_card(harness):
    """Fails if the pre-write check refuses a requester who is still in the audience."""
    h = harness
    handle = await start_task(h)

    await submit(handle)

    async def watching():
        return await handle.query(TaskWorkflow.state) == "watching"

    await eventually(watching, timeout=20)
    assert len(h.notion.all_pages(DB_ID)) == 1


async def test_a_comment_task_for_an_agent_that_is_off_says_so_before_any_card(harness):
    """Fails if a comment task for an agent that is off says "couldn't find it" or posts a card."""
    h = harness
    await turn_off(h)
    spec = TaskSpec(ctx(), "add_comment", {"item": "Broken VPN", "comment": "Still broken"}, NONCE)

    handle = await h.client.start_workflow(
        TaskWorkflow.run,
        spec,
        id=ids.task_id(TENANT, str(uuid.uuid4())),
        task_queue=TASK_QUEUE,
    )

    assert await handle.result() == "cancelled"
    [told] = thread_posts(h, "turned off")
    assert told["text"] == "IT helper is turned off by an admin."
    assert not [p for p in h.slack.posts() if p["blocks"]]


async def test_a_turn_or_hand_off_to_an_agent_that_is_off_is_refused_with_the_fixed_reply(harness):
    """Fails if the audience check admits an agent an admin turned off (the hand-off race)."""
    h = harness
    await turn_off(h)

    decision = await ActivityEnvironment().run(h.acts.check_audience, ctx())

    assert not decision.allowed
    assert decision_text("IT helper", decision) == "IT helper is turned off by an admin."


async def test_a_pre_write_check_that_cannot_finish_fails_closed_and_clears_the_buttons(harness):
    """Fails if Submit writes, or leaves its buttons, when the audience could not be checked."""
    h = harness
    handle = await start_task(h)
    h.slack.fail_lookups(SlackLookupError("slack_down", transient=False))

    await submit(handle)

    assert await handle.result() == "cancelled"
    nothing_written(h)
    last = h.slack.updates()[-1]
    assert (
        last["text"] == "I couldn't confirm you can still make this request; nothing was changed."
    )
    assert all(b["type"] == "section" for b in last["blocks"])


# --- a draft_ref refusal replaces the card in place ---------------------------------------------


async def start_draft_task(h):
    task_id = ids.task_id(TENANT, str(uuid.uuid4()))
    payload = {"title": "New monitor", "details": "Mine flickers.", "database_id": DB_ID}
    await h.acts.content.put_draft(TENANT, task_id, 1, payload)
    handle = await h.client.start_workflow(
        TaskWorkflow.run,
        TaskSpec(ctx(), "create_item", {}, NONCE, draft_ref=task_id, revision=1),
        id=task_id,
        task_queue=TASK_QUEUE,
    )

    async def waiting():
        return await handle.query(TaskWorkflow.state) == "awaiting_approval"

    await eventually(waiting, timeout=20)
    return handle


def assert_card_replaced_in_place(h, text: str):
    [card] = [p for p in h.slack.posts() if p["blocks"]]
    last = h.slack.updates()[-1]
    assert (last["channel"], last["ts"], last["text"]) == (card["channel"], card["ts"], text)
    assert all(b["type"] == "section" for b in last["blocks"])
    assert [p for p in h.slack.posts() if p["channel"] == card["channel"]] == [card]


async def test_a_draft_ref_task_refused_for_agent_off_replaces_the_card_with_no_buttons(harness):
    """Fails if a draft_ref refusal for agent-off keeps the card's buttons or posts under the card."""
    h = harness
    handle = await start_draft_task(h)
    await turn_off(h)

    await submit(handle)

    assert await handle.result() == "cancelled"
    nothing_written(h)
    assert_card_replaced_in_place(h, OFF_ON_CARD)


@pytest.mark.parametrize("change", [pause_channel, deactivate_requester])
async def test_a_draft_ref_task_refused_for_a_pause_or_lost_audience_replaces_the_card(
    harness, change
):
    """Fails if a draft_ref refusal for a pause or lost audience keeps the buttons or posts under the card."""
    h = harness
    handle = await start_draft_task(h)
    await change(h)

    await submit(handle)

    assert await handle.result() == "cancelled"
    nothing_written(h)
    assert_card_replaced_in_place(h, REFUSED_ON_CARD)


# --- which switches stop which effect -----------------------------------------------------------


def state(*rows: tuple[str, str]) -> SwitchState:
    return SwitchState(TENANT, tuple(Switch(TENANT, s, t, "U_ADMIN", NOW) for s, t in rows))


@pytest.mark.parametrize(
    "off, stopped",
    [
        ((AGENT, "it-helper"), {Effect.MODEL, Effect.READ, Effect.WRITE, Effect.POLL}),
        ((AGENT, "other"), set()),
        ((ALL_AGENTS, ""), {Effect.MODEL, Effect.READ, Effect.WRITE, Effect.POLL}),
        ((WRITES, ""), {Effect.WRITE}),
        ((CONNECTOR, NOTION), {Effect.READ, Effect.WRITE, Effect.POLL}),
    ],
)
def test_each_switch_stops_exactly_the_effects_it_covers(off, stopped):
    """Fails if a switch stops an effect outside its scope, or leaves one inside it running."""
    refused = {e for e in Effect if state(off).refusal("it-helper", e) is not None}

    assert refused == stopped


# --- every activity goes through the gate -------------------------------------------------------

# Activities that do not act for an agent, each with the reason it is not gated.
NOT_GATED = {
    "load_agent": "reads the agent's definition; the workflow needs it to say the agent is off",
    "append_history": "stores a message in the thread's history; no model, connector or post",
    "append_tool_results": "moves stored tool output into history; no connector call",
    "post_placeholder": "posts 'Thinking'; the fixed off notice replaces it, so it must still run",
    "finalize_reply": "posts the reply or the fixed off notice that call_llm returned",
    "finalize_notice": "shows a fixed non-answer notice and records its audit row; no agent effect",
    "post_in_thread": "posts a fixed notice (reminder, refusal, failure); no agent effect",
    "post_notice_in_thread": "posts a fixed notice and records its audit row; no agent effect",
    "update_card_line": "must still run to tell the requester the agent is off or stopped",
    "update_card": "must still run to tell the requester the agent is off",
    "record_hand_off": "audit row of a hand-off that already happened",
    "record_audience_refusal": "audit row of a refusal",
    "record_approval_closed": "audit row of an approval that closed without a write",
    "thread_agent": "reads which agent owns a thread; the workflow needs it to start a turn",
    "remember_thread_agent": "stores which agent a thread was handed to; no model or connector",
    "ingest_message": "moves the text a person wrote from the inbound event into history",
    "discard_message": "clears the inbound event's text when a turn ended before ingest; no model, connector or post",
    "finalize_model_reply": "posts the reply or the fixed off notice that call_model stored",
    "apply_edit": "stores the requester's saved edit as the next draft revision; no model or connector",
    "post_task_notice": "posts a fixed line about a task in its thread; no agent effect",
}


def _is_gate_call(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Await)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr.startswith("_gate")
    )


def _activity_names() -> dict[str, ast.AsyncFunctionDef]:
    tree = ast.parse(textwrap.dedent(inspect.getsource(AgentActivities)))
    found = {}
    for fn in ast.walk(tree):
        if isinstance(fn, ast.AsyncFunctionDef):
            for deco in fn.decorator_list:
                if isinstance(deco, ast.Call) and "defn" in ast.unparse(deco.func):
                    found[deco.keywords[0].value.value] = fn
    return found


def test_every_activity_that_acts_for_an_agent_starts_at_the_gate():
    """Fails if an activity method is added that neither starts at the gate nor is listed as not
    acting for an agent. This is the one test that reads source, because the rule is about code
    shape: a new activity is a new way to act, and a switch must cover it from its first line.
    """
    activities = _activity_names()
    assert set(NOT_GATED) <= set(activities), "NOT_GATED names an activity that does not exist"
    ungated = []
    for name, fn in activities.items():
        if name in NOT_GATED:
            continue
        awaits = sorted(
            (n for n in ast.walk(fn) if isinstance(n, ast.Await)),
            key=lambda n: (n.lineno, n.col_offset),
        )
        if not awaits or not _is_gate_call(awaits[0]):
            ungated.append(name)
    assert ungated == []
