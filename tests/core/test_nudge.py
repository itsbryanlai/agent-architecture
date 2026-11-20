"""The nudge: a stale item's offer, one approval card, a comment and a DM, each once."""

import json
from datetime import UTC, datetime, timedelta

import pytest
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowExecutionStatus

from agentos import ids
from agentos.models import ApprovalDecision, EditRequest, ExternalUpdate, NudgeRequest
from agentos.slack_client import SlackUser
from agentos.workflows.task import TaskWorkflow
from tests.conftest import (
    ALICE,
    ALICE_NOTION,
    BOB,
    BOB_NOTION,
    DB_ID,
    TEAM,
    TENANT,
    default_schema,
    eventually,
)
from tests.helpers.agents import republish
from tests.helpers.replay_support import suggest_continue_as_new_after
from tests.helpers.slack import slack_error
from tests.helpers.tasks import approve, start_task, thread_posts, wait_state

ASSIGNEE_SCHEMA = {**default_schema(), "Assignee": {"id": "asg1", "type": "people"}}
CAROL_NOTION = "notion-user-carol"


def with_assignee(definition: dict) -> None:
    db = definition["grants"]["notion"]["databases"][0]
    db["properties"]["assignee"] = "Assignee"
    db["property_ids"]["assignee"] = "asg1"


@pytest.fixture
async def nh(harness):
    """A harness whose agent maps the assignee property, whose Notion has it, and
    whose requester has a display name."""
    h = harness
    h.notion.set_schema(DB_ID, ASSIGNEE_SCHEMA)
    h.slack.users[ALICE] = SlackUser(ALICE, TEAM, display_name="Alice")
    await republish(h.agent_store, "it-helper", with_assignee)
    return h


def stale_posts(h):
    return [p for p in h.slack.posts() if p["thread_ts"] and "No movement" in p["text"]]


def offer_value(post):
    for block in post["blocks"] or []:
        if block["type"] == "actions":
            return json.loads(block["elements"][0]["value"])
    return None


def nudge_cards(h):
    return [
        p for p in h.slack.posts() if p["blocks"] and "wants to nudge" in json.dumps(p["blocks"])
    ]


def dm_posts(h):
    return [p for p in h.slack.posts() if p["channel"] == f"D-{BOB}"]


def comments(h, page_id):
    return h.notion.comments.get(page_id.replace("-", ""), [])


def comment_text(comment) -> str:
    return "".join(t.get("text", {}).get("content", "") for t in comment["rich_text"])


async def stale_item(h, assignee=BOB_NOTION):
    """A filed, watching item gone stale; its assignee set before the stale poll."""
    handle = await start_task(h)
    await approve(h, handle)
    await wait_state(handle, "watching")
    page = h.notion.all_pages(DB_ID)[0]
    if assignee is not None:
        h.notion.set_people(page["id"], "Assignee", [assignee])
    await h.wf_env.sleep(timedelta(days=4))
    await eventually(lambda: stale_posts(h))
    return handle, page


async def click_nudge(h, handle):
    value = offer_value(stale_posts(h)[-1])
    await handle.signal(TaskWorkflow.nudge, NudgeRequest(value["nonce"], ALICE))
    return await await_nudge_card(h)


async def await_nudge_card(h):
    cards = await eventually(lambda: nudge_cards(h))
    actions = next(b for b in cards[-1]["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    child = h.client.get_workflow_handle(value["task_id"])
    await wait_state(child, "awaiting_approval")
    return child, value["nonce"]


# --- the offer and its fallbacks -------------------------------------------------


@pytest.mark.parametrize(
    "mapped,assignee,policy,offered",
    [
        (False, None, {}, True),
        (True, None, {}, True),
        (True, CAROL_NOTION, {}, True),
        (True, ALICE_NOTION, {}, False),
        (True, BOB_NOTION, {"nudge": False}, False),
        (True, BOB_NOTION, {"max_nudges": 0}, False),
    ],
    ids=["no-property", "empty", "not-in-slack", "assignee-is-requester", "nudge-off", "max-zero"],
)
async def test_the_stale_offer_follows_the_fallbacks(harness, mapped, assignee, policy, offered):
    """Fails if the stale notice offers a nudge against a decided fallback, or hides a valid offer."""
    h = harness
    if mapped:
        h.notion.set_schema(DB_ID, ASSIGNEE_SCHEMA)

        def change(definition):
            with_assignee(definition)
            definition["follow_up"].update(policy)

        await republish(h.agent_store, "it-helper", change)
    if assignee == CAROL_NOTION:
        h.notion.users.append(
            {
                "object": "user",
                "id": CAROL_NOTION,
                "type": "person",
                "name": "Carol",
                "person": {"email": "carol@example.com"},
            }
        )
    _, _ = await stale_item(h, assignee)

    assert (offer_value(stale_posts(h)[-1]) is not None) == offered


async def test_only_the_requester_on_the_live_nonce_starts_the_nudge(nh):
    """Fails if another user's click or a stale nonce spends the offer or starts a nudge."""
    h = nh
    handle, page = await stale_item(h)
    value = offer_value(stale_posts(h)[-1])
    children_before = len(h.wf_env.children.started)
    polls = h.notion.count("retrieve_page")

    await handle.signal(TaskWorkflow.nudge, NudgeRequest(value["nonce"], BOB))
    await handle.signal(TaskWorkflow.nudge, NudgeRequest("not-the-offer", ALICE))
    # Settle: the poke's poll runs after the clicks; a click that started a nudge
    # would have started its child before that poll.
    await handle.signal(
        TaskWorkflow.external_update, ExternalUpdate(page_id=page["id"], source="notion_webhook")
    )
    await eventually(lambda: h.notion.count("retrieve_page") > polls)
    assert len(h.wf_env.children.started) == children_before

    await handle.signal(TaskWorkflow.nudge, NudgeRequest(value["nonce"], ALICE))
    await await_nudge_card(h)
    assert len(nudge_cards(h)) == 1


async def test_a_click_that_finds_the_assignee_is_the_requester_says_so(nh):
    """Fails if a nudge whose assignee became the requester after the offer starts a card or says nothing."""
    h = nh
    handle, page = await stale_item(h)
    h.notion.set_people(page["id"], "Assignee", [ALICE_NOTION])
    value = offer_value(stale_posts(h)[-1])

    await handle.signal(TaskWorkflow.nudge, NudgeRequest(value["nonce"], ALICE))

    # The line is the settle: it posts after the click was prepared and refused.
    await eventually(lambda: thread_posts(h, "is you, so there's nobody to nudge"))
    assert len(thread_posts(h, "is you, so there's nobody to nudge")) == 1
    assert nudge_cards(h) == []


async def test_a_nudge_that_cannot_be_prepared_says_so_and_keeps_watching(nh):
    """Fails if a prepare that fails for good starts a card, says nothing, or ends the watch early."""
    h = nh
    handle, page = await stale_item(h)
    h.notion.trash_page(page["id"])
    value = offer_value(stale_posts(h)[-1])

    await handle.signal(TaskWorkflow.nudge, NudgeRequest(value["nonce"], ALICE))

    await eventually(lambda: thread_posts(h, "I couldn't prepare the nudge"))
    assert nudge_cards(h) == []
    # Settle: the watch's next poll finds the item gone and stops with its own line.
    await eventually(lambda: thread_posts(h, "I've stopped watching"))
    assert len(thread_posts(h, "I couldn't prepare the nudge")) == 1
    assert await handle.result() == "done"


async def test_a_broken_assignee_mapping_disables_only_the_nudge_mention(nh):
    """Fails if a broken assignee mapping stops the nudge or still mentions anyone."""
    h = nh
    # The property the grant maps is gone from the database; the agent stays published.
    h.notion.set_schema(DB_ID, default_schema())
    handle, page = await stale_item(h, assignee=None)

    child, nonce = await click_nudge(h, handle)
    await child.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, True))

    assert await child.result() == "done"
    [comment] = comments(h, page["id"])
    assert comment_text(comment).startswith("Any update on this?")
    assert all("mention" not in t for t in comment["rich_text"])
    assert dm_posts(h) == []


# --- the card's preview is what runs ----------------------------------------------


@pytest.mark.parametrize("mode", ["dm", "plain mention", "comment only"])
async def test_what_the_nudge_card_previewed_is_what_ran(nh, mode):
    """Fails if the comment or DM differs from the card's preview, or a fallback effect runs."""
    h = nh
    assignee = BOB_NOTION
    if mode == "plain mention":
        h.notion.users.append(
            {
                "object": "user",
                "id": CAROL_NOTION,
                "type": "person",
                "name": "Carol",
                "person": {"email": "carol@example.com"},
            }
        )
        assignee = CAROL_NOTION
    elif mode == "comment only":
        assignee = None
    handle, page = await stale_item(h, assignee)
    child, nonce = await click_nudge(h, handle)
    card_text = json.dumps(nudge_cards(h)[-1]["blocks"], ensure_ascii=False)
    assert "Any update on this?" in card_text

    await child.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, True))
    assert await child.result() == "done"

    [comment] = comments(h, page["id"])
    text = comment_text(comment)
    assert "ref " in text
    events = [(e.kind, e.tool, e.outcome) for e in h.audit.events if e.task_id == child.id]
    if mode == "dm":
        assert "@Bob Any update on this?" in card_text
        assert "Alice asked me to check on 'New monitor'" in card_text
        assert "*Direct message to the assignee*" in card_text
        assert comment["rich_text"][0]["mention"]["user"]["id"] == BOB_NOTION
        assert text.strip().startswith("Any update on this?")
        [dm] = dm_posts(h)
        assert dm["text"] == (
            f"Alice asked me to check on 'New monitor'\n"
            f"<{page['url']}|New monitor>\n\nAny update on this?"
        )
        assert events == [
            ("approval", "nudge", "ok"),
            ("write", "nudge_comment", "ok"),
            ("write", "nudge_dm", "ok"),
        ]
    elif mode == "plain mention":
        assert "@Carol Any update on this?" in card_text
        assert "Direct message" not in card_text
        assert text.startswith("@Carol Any update on this?")
        assert all("mention" not in t for t in comment["rich_text"])
        assert dm_posts(h) == []
        assert events == [("approval", "nudge", "ok"), ("write", "nudge_comment", "ok")]
    else:
        assert "Direct message" not in card_text
        assert text.startswith("Any update on this?")
        assert all("mention" not in t for t in comment["rich_text"])
        assert dm_posts(h) == []
        assert events == [("approval", "nudge", "ok"), ("write", "nudge_comment", "ok")]


async def test_an_edited_nudge_runs_the_edited_text_in_both_effects(nh):
    """Fails if an edited nudge posts or sends the text the card showed before the edit."""
    h = nh
    handle, page = await stale_item(h)
    child, nonce = await click_nudge(h, handle)

    edit_id = ids.edit_event_id("nudge-edit")
    await h.acts.content.put_inbound(
        TENANT,
        edit_id,
        {"kind": "edit", "task_id": child.id, "fields": {"comment": "Please prioritise this."}},
        delivered=True,
    )
    await child.signal(TaskWorkflow.edit, EditRequest(nonce, ALICE, {}, edit_id))

    async def new_nonce():
        view = await child.query(TaskWorkflow.draft)
        return view.nonce if view.nonce != nonce else None

    fresh = await eventually(new_nonce)
    await child.signal(TaskWorkflow.decision, ApprovalDecision(fresh, ALICE, True))
    assert await child.result() == "done"

    [comment] = comments(h, page["id"])
    assert comment_text(comment).strip().startswith("Please prioritise this.")
    [dm] = dm_posts(h)
    assert dm["text"].endswith("\n\nPlease prioritise this.")


# --- gates and failures ------------------------------------------------------------


async def test_agent_off_before_the_nudge_writes_cancels_both(nh):
    """Fails if an approved nudge still comments or DMs after an admin turned the agent off."""
    h = nh
    handle, _ = await stale_item(h)
    child, nonce = await click_nudge(h, handle)
    assert await h.switches.switch_off(TENANT, "agent", "it-helper", "U_ADMIN", datetime.now(UTC))

    await child.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, True))

    assert await child.result() == "cancelled"
    assert h.notion.count("create_comment") == 0
    assert dm_posts(h) == []
    assert h.slack.updates()[-1]["text"] == (
        "IT helper is turned off by an admin. Nothing was changed."
    )


async def test_a_failed_dm_reports_commented_and_never_recomments(nh):
    """Fails if a DM failure after the comment retries the comment, reports nothing, or goes unaudited."""
    h = nh
    handle, page = await stale_item(h)
    child, nonce = await click_nudge(h, handle)
    h.slack.fail_next_post(slack_error("channel_not_found"), times=None, channel=f"D-{BOB}")

    await child.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, True))

    assert await child.result() == "done"
    assert len(comments(h, page["id"])) == 1
    assert dm_posts(h) == []
    assert len(thread_posts(h, "but I couldn't DM the assignee")) == 1
    events = [(e.kind, e.tool, e.outcome) for e in h.audit.events if e.task_id == child.id]
    assert events == [
        ("approval", "nudge", "ok"),
        ("write", "nudge_comment", "ok"),
        ("write", "nudge_dm", "error"),
    ]


async def test_a_permanent_dm_refusal_is_not_retried(nh):
    """Fails if a DM Slack refuses for good is attempted again instead of ending the nudge."""
    h = nh
    handle, page = await stale_item(h)
    child, nonce = await click_nudge(h, handle)
    # Only the first attempt is refused: a retry would send the DM.
    h.slack.fail_next_post(slack_error("account_inactive"), times=1, channel=f"D-{BOB}")

    await child.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, True))

    assert await child.result() == "done"
    assert len(comments(h, page["id"])) == 1
    assert dm_posts(h) == []
    assert len(thread_posts(h, "but I couldn't DM the assignee")) == 1


# --- a spent offer and a hop -------------------------------------------------------


async def test_a_spent_offer_drops_its_button(nh):
    """Fails if the stale notice keeps its nudge button after the offer was spent."""
    h = nh
    handle, _ = await stale_item(h)
    notice = stale_posts(h)[-1]
    child, nonce = await click_nudge(h, handle)

    # The card is the settle: the click was applied, and the button cleared, before it.
    def cleared():
        return [u for u in h.slack.updates() if u["ts"] == notice["ts"]]

    assert cleared() and all(
        u["text"] == notice["text"] and all(b["type"] != "actions" for b in u["blocks"])
        for u in cleared()
    )

    await child.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, True))
    assert await child.result() == "done"
    await eventually(lambda: thread_posts(h, "Nudged the assignee"))
    assert cleared() and all(
        u["text"] == notice["text"] and all(b["type"] != "actions" for b in u["blocks"])
        for u in cleared()
    )


async def test_two_clicks_on_one_offer_start_one_nudge(nh):
    """Fails if a second click on the same offer, while the first nudge is drafted, starts another."""
    h = nh
    handle, page = await stale_item(h)
    value = offer_value(stale_posts(h)[-1])
    await handle.signal(TaskWorkflow.nudge, NudgeRequest(value["nonce"], ALICE))
    # Hold the draft's first read open: the second click lands while the nudge is
    # being prepared, before the child exists.
    read = h.notion.hang_next_read("retrieve_page")
    await read.reached.wait()
    await handle.signal(TaskWorkflow.nudge, NudgeRequest(value["nonce"], ALICE))
    read.release()

    child, nonce = await await_nudge_card(h)
    polls = h.notion.count("retrieve_page")
    await child.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, True))
    assert await child.result() == "done"
    # Settle: applying the nudge report always polls, and a second draft would have
    # started (and posted its card) before that poll.
    await eventually(lambda: h.notion.count("retrieve_page") > polls, timeout=30)
    assert len(nudge_cards(h)) == 1
    assert len(comments(h, page["id"])) == 1


async def _nudge_report_handled(handle) -> bool:
    """The watch's history shows the nudge_done signal and a workflow task after it."""
    events = (await handle.fetch_history()).events
    for i, event in enumerate(events):
        if (
            event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
            and event.workflow_execution_signaled_event_attributes.signal_name == "nudge_done"
        ):
            return any(
                e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
                for e in events[i + 1 :]
            )
    return False


async def test_a_nudge_report_in_flight_survives_a_hop(nh):
    """Fails if a watch that continues as new drops a nudge report it has not applied yet."""
    h = nh
    handle, page = await stale_item(h)
    child, nonce = await click_nudge(h, handle)

    # Hold the watch's next poll open and let the nudge run while it hangs: the report
    # lands before the poll returns, so the hop at the top of the next loop must carry
    # the report the loop has not applied yet.
    read = h.notion.hang_next_read("retrieve_page")
    await handle.signal(
        TaskWorkflow.external_update, ExternalUpdate(page_id=page["id"], source="notion_webhook")
    )
    await read.reached.wait()
    # The run that is watching now (the stale period may already have hopped once).
    hanging_run = (await handle.describe()).run_id
    bound = len((await handle.fetch_history()).events)
    with suggest_continue_as_new_after(bound):
        await child.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, True))
        assert await child.result() == "done"
        await eventually(lambda: _nudge_report_handled(handle))
        read.release()

        async def continued():
            run = await h.client.get_workflow_handle(handle.id, run_id=hanging_run).describe()
            return run.status == WorkflowExecutionStatus.CONTINUED_AS_NEW

        await eventually(continued)
        # The new run applies the report: the clock restarts from the nudge.
        await h.wf_env.sleep(timedelta(days=2))
        assert len(stale_posts(h)) == 1
        await h.wf_env.sleep(timedelta(days=2))
        await eventually(lambda: len(stale_posts(h)) == 2)
        assert offer_value(stale_posts(h)[-1]) is not None

        # The later offer works: a second nudge runs end to end.
        value = offer_value(stale_posts(h)[-1])
        await handle.signal(TaskWorkflow.nudge, NudgeRequest(value["nonce"], ALICE))
        await eventually(lambda: len(nudge_cards(h)) == 2)
        second, second_nonce = await await_nudge_card(h)
        await second.signal(TaskWorkflow.decision, ApprovalDecision(second_nonce, ALICE, True))
        assert await second.result() == "done"

    assert len(comments(h, page["id"])) == 2
    assert len(dm_posts(h)) == 2


# --- the stale clock ---------------------------------------------------------------


async def test_the_stale_clock_restarts_from_the_nudge(nh):
    """Fails if a nudge does not restart the stale clock, or one stale period offers twice."""
    h = nh
    handle, _ = await stale_item(h)

    await h.wf_env.sleep(timedelta(days=4))
    assert len(stale_posts(h)) == 1  # one offer per stale period

    child, nonce = await click_nudge(h, handle)
    await child.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, True))
    assert await child.result() == "done"

    await h.wf_env.sleep(timedelta(days=2))
    assert len(stale_posts(h)) == 1  # restarted from the nudge: not stale yet
    await h.wf_env.sleep(timedelta(days=2))
    await eventually(lambda: len(stale_posts(h)) == 2)
    assert offer_value(stale_posts(h)[-1]) is not None  # the next period offers again


async def test_no_offer_after_max_nudges_ran(nh):
    """Fails if a task offers a nudge after it already ran max_nudges of them."""
    h = nh
    await republish(h.agent_store, "it-helper", lambda d: d["follow_up"].update(max_nudges=1))
    handle, _ = await stale_item(h)
    child, nonce = await click_nudge(h, handle)
    await child.signal(TaskWorkflow.decision, ApprovalDecision(nonce, ALICE, True))
    assert await child.result() == "done"

    await h.wf_env.sleep(timedelta(days=4))
    await eventually(lambda: len(stale_posts(h)) == 2)
    assert offer_value(stale_posts(h)[-1]) is None


@pytest.fixture(autouse=True)
def _quiet_workflow_logs(caplog):
    caplog.set_level("ERROR", logger="temporalio.workflow")
