"""Services and actions are opt-in per agent (D-52): what the model is offered, and what a
call outside the chosen actions does."""

import pytest
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.config import LEGACY_ACTIONS
from agentos.connectors.manifest import TOOLS
from agentos.models import LLMResponse, ToolCall, TurnContext
from tests.conftest import ALICE, TENANT, eventually
from tests.helpers.agents import republish
from tests.helpers.conversation import start_stored_conversation
from tests.helpers.drive import DRIVE_SHEET, sheet_grant_block
from tests.helpers.tasks import approve, start_task

CHANNEL = "C_GENERAL"
THREAD = "1700000000.000100"
CONV = ids.conversation_id(TENANT, CHANNEL, THREAD)
# What a version with no actions list is offered by the Drive-granted test agent: the legacy
# actions whose grant it holds (a Notion database, a Drive item), and the task list.
LEGACY = {
    name for name in LEGACY_ACTIONS if TOOLS[name].needs in ("notion_database", "drive_item")
} | {"my_tasks"}
NOT_SET_UP = "This agent isn't set up to {}."


async def publish(h, actions, with_sheet=False):
    """Republish it-helper with ``actions`` (None: the key left out) and maybe a Sheet grant."""

    def change(definition):
        if actions is not None:
            definition["actions"] = actions
        if with_sheet:
            definition["grants"].setdefault("drive", {}).update(sheets=[sheet_grant_block()])

    record = await republish(h.agent_store, "it-helper", change)
    return TurnContext(TENANT, "it-helper", record.version, ALICE, CHANNEL, THREAD, "channel")


@pytest.mark.parametrize(
    ("actions", "sheet", "offered"),
    [
        (None, False, LEGACY),
        ([], False, set()),
        (["append_row"], False, set()),
        (["append_row"], True, {"append_row", "my_tasks"}),
        (["search_items", "my_items"], False, {"search_items", "my_items"}),
    ],
    ids=["legacy", "none ticked", "write without its grant", "write with its grant", "reads only"],
)
async def test_a_tool_is_offered_only_when_its_action_is_chosen_and_its_grant_is_there(
    dharness, actions, sheet, offered
):
    """Fails if the model is offered an action its creator did not choose or one whose grant is
    missing, a chosen action is withheld, or the task list is offered to an agent with no write
    action; a version with no actions list must offer what its grants allow."""
    h = dharness
    ctx = await publish(h, actions, with_sheet=sheet)
    h.llm.queue(LLMResponse(text="Hi", tool_calls=[], stop_reason="end_turn"))

    await ActivityEnvironment().run(h.acts.call_llm, ctx, CONV)

    # Knowledge search follows the page and Drive grants, not the chosen actions.
    names = {t["name"] for t in h.llm.calls[0]["tools"]} - {"search_knowledge"}
    assert names == offered


@pytest.mark.parametrize(
    ("chosen", "problem", "queried"),
    [
        (["create_item"], NOT_SET_UP.format("search Notion items"), False),
        (["search_items"], "", True),
    ],
    ids=["unchosen read", "chosen read"],
)
async def test_a_read_outside_the_chosen_actions_is_refused_before_any_connector(
    dharness, chosen, problem, queried
):
    """Fails if a model call to a read action outside the chosen ones reaches Notion, or the
    model is not told the agent is not set up for it, or a chosen read is refused."""
    h = dharness
    ctx = await publish(h, chosen)
    call = ToolCall("tu_1", "search_items", {"query": "vpn"})
    h.llm.queue(LLMResponse(text=None, tool_calls=[call], stop_reason="tool_use"))
    step = await ActivityEnvironment().run(h.acts.call_model, ctx, CONV, "Ev1:s0")

    await ActivityEnvironment().run(h.acts.run_read_tool, ctx, CONV, call, 1)

    [ref] = step.tool_calls
    text, is_error = (await h.tool_outputs.get_many(TENANT, CONV, ["tu_1"]))["tu_1"]
    asked_notion = any(name == "query_database" for name, _ in h.notion.calls)
    assert (ref.problem, is_error, asked_notion) == (problem, bool(problem), queried)
    assert text.startswith(problem)


@pytest.mark.parametrize(
    ("tool", "tool_input", "chosen", "result", "cards"),
    [
        (
            "create_item",
            {"title": "VPN", "details": "broken"},
            ["search_items"],
            NOT_SET_UP.format("create Notion items"),
            0,
        ),
        (
            "append_row",
            {"sheet": DRIVE_SHEET, "row": {"name": "bob"}},
            ["create_item"],
            NOT_SET_UP.format("add rows to a Sheet"),
            0,
        ),
        (
            "create_item",
            {"title": "VPN", "details": "broken"},
            ["create_item"],
            "Nothing is filed until they click Submit",
            1,
        ),
    ],
    ids=["unchosen create", "unchosen drive write", "chosen create"],
)
async def test_a_write_outside_the_chosen_actions_starts_no_task(
    dharness, tool, tool_input, chosen, result, cards
):
    """Fails if a model call to a write action outside the chosen ones starts a task or posts
    an approval card, or the model is not told the agent is not set up for it, or a chosen
    write is refused."""
    h = dharness
    await publish(h, chosen)
    h.llm.queue(
        LLMResponse(
            text=None, tool_calls=[ToolCall("tu_w", tool, tool_input)], stop_reason="tool_use"
        ),
        LLMResponse(text="Done.", tool_calls=[], stop_reason="end_turn"),
    )

    await start_stored_conversation(h, "Ev-w", "do it")
    await eventually(lambda: h.slack.updates())  # the turn's reply is the settle point

    [reply] = h.llm.calls[1]["messages"][-1]["content"]
    assert result in reply["content"]
    assert len([p for p in h.slack.posts() if p["blocks"]]) == cards


async def test_an_approved_write_stops_when_a_new_version_dropped_its_action(harness):
    """Fails if a write the requester approved still runs after the agent was republished
    without that action, or the card does not say the agent is no longer set up for it."""
    h = harness
    handle = await start_task(h)
    await republish(h.agent_store, "it-helper", lambda d: d.update(actions=["search_items"]))

    await approve(h, handle)

    assert await handle.result() == "cancelled"
    assert h.notion.count("create_page") == 0
    assert h.slack.updates()[-1]["text"] == NOT_SET_UP.format("create Notion items")
