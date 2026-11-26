"""Services and actions are opt-in per agent (D-52): what the model is offered, and what a
call outside the chosen actions does."""

import pytest
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.models import LLMResponse, ToolCall, TurnContext
from tests.conftest import ALICE, TENANT
from tests.helpers.agents import republish
from tests.helpers.drive import DRIVE_SHEET, sheet_grant_block

CHANNEL = "C_GENERAL"
THREAD = "1700000000.000100"
CONV = ids.conversation_id(TENANT, CHANNEL, THREAD)
# What a version with no actions list is offered by the Drive-granted test agent.
LEGACY = {"search_items", "my_items", "create_item", "add_comment", "comment_on_file", "my_tasks"}


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


NOT_SET_UP = "This agent isn't set up to {}."


@pytest.mark.parametrize(
    ("tool", "chosen", "problem"),
    [
        ("search_items", ["create_item"], NOT_SET_UP.format("search Notion items")),
        ("search_items", ["search_items"], ""),
        ("create_item", ["search_items"], NOT_SET_UP.format("create Notion items")),
        ("create_item", ["create_item"], ""),
        ("append_row", ["create_item"], NOT_SET_UP.format("add rows to a Sheet")),
    ],
    ids=["unchosen read", "chosen read", "unchosen write", "chosen write", "unchosen drive write"],
)
async def test_a_call_to_an_action_the_agent_was_not_given_is_refused_before_any_connector(
    dharness, tool, chosen, problem
):
    """Fails if a model call to an action outside the chosen ones reaches Notion or becomes a
    task, or the model is not told the agent is not set up for it, or a chosen action is refused."""
    h = dharness
    ctx = await publish(h, chosen)
    inputs = {
        "search_items": {"query": "vpn"},
        "create_item": {"title": "VPN", "details": "broken"},
        "append_row": {"sheet": DRIVE_SHEET, "row": {"name": "bob"}},
    }
    call = ToolCall("tu_1", tool, inputs[tool])
    h.llm.queue(LLMResponse(text=None, tool_calls=[call], stop_reason="tool_use"))

    step = await ActivityEnvironment().run(h.acts.call_model, ctx, CONV, "Ev1:s0")

    [ref] = step.tool_calls
    assert ref.problem == problem
    if tool == "search_items":
        await ActivityEnvironment().run(h.acts.run_read_tool, ctx, CONV, call, 1)
        text, is_error = (await h.tool_outputs.get_many(TENANT, CONV, ["tu_1"]))["tu_1"]
        queried = any(name == "query_database" for name, _ in h.notion.calls)
        assert (is_error, text if is_error else "", queried) == (
            bool(problem),
            problem,
            not problem,
        )
