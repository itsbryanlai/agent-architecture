"""An approved write happens once, whatever the attempts, delays and failures around it:
a Notion item or comment, a nudge's two effects, a Drive comment."""

import asyncio
import dataclasses
import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from slack_sdk.errors import SlackApiError
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.connectors.google_iam import DriveSection, GoogleError
from agentos.connectors.notion import comment_ref
from agentos.models import ApprovalDecision, TaskSpec, TurnContext
from agentos.slack_client import SlackUser
from agentos.stores import InMemoryExternalWriteStore
from agentos.workflows.task import TaskWorkflow
from tests.conftest import (
    ALICE,
    ALICE_NOTION,
    BOB,
    BOB_NOTION,
    DB_ID,
    TEAM,
    TENANT,
    eventually,
    title_prop,
)
from tests.helpers.agents import republish
from tests.helpers.drive import (
    DRIVE_DOC,
    DRIVE_SHEET,
    DRIVE_TEMPLATE,
    SHEET_COLUMNS,
    SHEET_TAB,
    TEMPLATE_BLANKS,
    TEMPLATE_FOLDER,
    add_roster_sheet,
    add_template_tree,
    sheet_grant_block,
    template_grant_block,
)
from tests.helpers.slack import slack_error

NONCE = "nonce-eo"
DETAILS = "Mine flickers."


def create_spec() -> TaskSpec:
    ctx = TurnContext(TENANT, "it-helper", 1, ALICE, "C_GENERAL", "1700000000.000100", "channel")
    payload = {"title": "New monitor", "details": DETAILS, "database_id": DB_ID}
    return TaskSpec(ctx, "create_item", payload, NONCE)


def comment_spec(page_id: str) -> TaskSpec:
    ctx = TurnContext(TENANT, "it-helper", 1, ALICE, "C_GENERAL", "1700000000.000100", "channel")
    payload = {"page_id": page_id, "comment": "Any update?", "item_title": "Broken VPN"}
    return TaskSpec(ctx, "add_comment", payload, NONCE)


async def run(activity, spec: TaskSpec, task_id: str):
    return await ActivityEnvironment().run(activity, spec, task_id)


async def start_task(h) -> tuple[object, str]:
    task_id = ids.task_id(TENANT, str(uuid.uuid4()))
    handle = await h.client.start_workflow(
        TaskWorkflow.run, create_spec(), id=task_id, task_queue=TASK_QUEUE
    )

    await wait_state(handle, "awaiting_approval")
    return handle, task_id


async def wait_state(handle, expected: str):
    async def reached():
        return await handle.query(TaskWorkflow.state) == expected

    await eventually(reached, timeout=20)


async def approve(handle) -> None:
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))


def thread_posts(h, needle: str):
    return [p for p in h.slack.posts() if p["thread_ts"] and needle in p["text"]]


def break_method(obj, name: str, error: Exception, times: int | None = None):
    """Make ``obj.name`` raise ``error`` ``times`` times (always when None)."""
    original = getattr(obj, name)
    left = {"n": times}

    async def wrapper(*args, **kwargs):
        if left["n"] is None or left["n"] > 0:
            if left["n"] is not None:
                left["n"] -= 1
            raise error
        return await original(*args, **kwargs)

    setattr(obj, name, wrapper)


# --- one attempt at a time -----------------------------------------------------------


async def test_a_second_attempt_beside_a_running_one_writes_nothing(harness):
    """Fails if an attempt starting while another is mid-write creates a second item."""
    h = harness
    task_id = ids.task_id(TENANT, "overlap")
    hang = h.notion.hang_after_write()
    first = asyncio.create_task(run(h.acts.create_notion_item, create_spec(), task_id))
    await hang.reached.wait()

    with pytest.raises(ApplicationError) as busy:
        await run(h.acts.create_notion_item, create_spec(), task_id)
    assert busy.value.type == "WriteInProgress" and not busy.value.non_retryable
    assert h.notion.count("create_page") == 1

    hang.release()
    item = await first
    calls_before = len(h.notion.calls)
    again = await run(h.acts.create_notion_item, create_spec(), task_id)

    assert again == item
    assert len(h.notion.all_pages(DB_ID)) == 1
    assert len(h.notion.calls) == calls_before


async def test_a_retry_waits_out_a_search_that_lags_the_write(harness):
    """Fails if a retry that cannot see the page yet writes it again."""
    h = harness
    task_id = ids.task_id(TENANT, "lag")
    h.notion.delay_visibility(2)
    h.notion.fail_after_write()
    with pytest.raises(TimeoutError):
        await run(h.acts.create_notion_item, create_spec(), task_id)

    item = await run(h.acts.create_notion_item, create_spec(), task_id)

    assert len(h.notion.all_pages(DB_ID)) == 1
    assert item.page_id == h.notion.all_pages(DB_ID)[0]["id"]


async def test_an_attempt_whose_lease_was_taken_over_writes_nothing(harness):
    """Fails if an attempt that lost its lease during the marker query still creates the page."""
    h = harness
    skew = {"by": timedelta(0)}
    h.acts.writes = InMemoryExternalWriteStore(clock=lambda: datetime.now(UTC) + skew["by"])
    task_id = ids.task_id(TENANT, "stale")
    reached, go = asyncio.Event(), asyncio.Event()
    original = h.notion.query_database

    async def slow_first_query(*args, **kwargs):
        found = await original(*args, **kwargs)
        h.notion.query_database = original  # only attempt A is slow
        reached.set()
        await go.wait()
        return found

    h.notion.query_database = slow_first_query
    stale = asyncio.create_task(run(h.acts.create_notion_item, create_spec(), task_id))
    await reached.wait()
    skew["by"] = timedelta(minutes=5)  # A's lease has run out
    hang = h.notion.hang_after_write()
    taker = asyncio.create_task(run(h.acts.create_notion_item, create_spec(), task_id))
    await hang.reached.wait()  # B holds the lease and has created the page

    go.set()
    with pytest.raises(ApplicationError) as lost:
        await stale
    hang.release()
    item = await taker

    assert lost.value.type == "WriteInProgress"
    assert len(h.notion.all_pages(DB_ID)) == 1
    assert h.notion.count("create_page") == 1
    assert await run(h.acts.create_notion_item, create_spec(), task_id) == item


# --- what lands in Notion is what the card showed ---------------------------------------


@pytest.mark.parametrize("matched", [True, False])
async def test_the_page_is_what_the_card_showed(harness, matched):
    """Fails if the page body or requester differs from the line the card showed."""
    h = harness
    if not matched:
        h.slack.emails[ALICE] = "stranger@example.net"
        h.slack.users[ALICE] = SlackUser(ALICE, TEAM, display_name="Alice Stranger")
    handle, _ = await start_task(h)
    card = json.dumps(next(p for p in h.slack.posts() if p["blocks"])["blocks"])
    line = "Requested by Alice Stranger via IT helper"

    await approve(handle)
    await wait_state(handle, "watching")

    page = h.notion.all_pages(DB_ID)[0]
    title = "".join(t["text"]["content"] for t in page["properties"]["Name"]["title"])
    people = page["properties"].get("Requested by", {}).get("people", [])
    assert title == "New monitor"
    if matched:
        assert line not in card and "Added to the page" not in card
        assert h.notion.read_body(page["id"]) == [DETAILS]
        assert people == [{"id": ALICE_NOTION}]
    else:
        assert line in card
        assert h.notion.read_body(page["id"]) == [DETAILS, line]
        assert people == []


async def test_requesters_are_looked_up_once_across_tasks(harness):
    """Fails if each task pages the Notion user list for the same requester again."""
    h = harness
    await start_task(h)
    await start_task(h)

    assert h.notion.count("list_users") == 1


# --- retries run out ----------------------------------------------------------------------


async def test_when_retries_run_out_after_the_page_exists_the_task_says_filed(harness):
    """Fails if a task whose write succeeded but whose bookkeeping kept failing says it failed."""
    h = harness
    handle, _ = await start_task(h)
    break_method(h.external_writes, "complete", TimeoutError("database blip"))

    await approve(handle)
    await h.wf_env.sleep(timedelta(minutes=2))  # past the retries and the reconcile timers
    await wait_state(handle, "watching")

    assert len(h.notion.all_pages(DB_ID)) == 1
    assert len(thread_posts(h, "Filed: New monitor")) == 1
    assert thread_posts(h, "couldn't") == []


async def test_when_retries_run_out_and_nothing_can_be_seen_the_task_says_it_cannot_confirm(
    harness,
):
    """Fails if an exhausted write is reported as not filed, or the tracker is not named."""
    h = harness
    handle, _ = await start_task(h)
    break_method(h.notion, "create_page", TimeoutError("notion is slow"))

    await approve(handle)
    assert await handle.result() == "failed"

    texts = [p["text"] for p in h.slack.posts() if p["thread_ts"]]
    tracker = f"https://www.notion.so/{DB_ID.replace('-', '')}"
    assert f"I couldn't confirm whether it was filed; please check {tracker}" in texts
    assert not any("nothing was filed" in t.lower() for t in texts)


# --- comments -----------------------------------------------------------------------------


@pytest.mark.parametrize("which", ["another_author", "short_reference"])
async def test_only_the_integrations_own_comment_with_the_full_reference_counts(harness, which):
    """Fails if someone else's comment, or a prefix of the reference, stops the comment."""
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    task_id = ids.task_id(TENANT, "comment")
    ref = comment_ref(f"{task_id}:comment")
    if which == "another_author":
        h.notion.add_comment_by(page["id"], f"quoting {ref}", "someone-else")
    else:
        h.notion.add_comment_by(page["id"], f"quoting {ref[:12]}", h.notion.bot_id)

    comment_id = await run(h.acts.add_notion_comment, comment_spec(page["id"]), task_id)

    mine = [c for c in h.notion.comments[page["id"].replace("-", "")] if c["id"] == comment_id]
    assert len(h.notion.comments[page["id"].replace("-", "")]) == 2
    assert ref in mine[0]["rich_text"][0]["text"]["content"]


async def test_a_comment_whose_response_was_lost_is_found_by_the_retry(harness):
    """Fails if the retry of a comment whose response was lost posts it a second time."""
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    task_id = ids.task_id(TENANT, "comment-lost")
    h.notion.delay_visibility(1)
    h.notion.fail_after_write("comment")
    with pytest.raises(TimeoutError):
        await run(h.acts.add_notion_comment, comment_spec(page["id"]), task_id)

    await run(h.acts.add_notion_comment, comment_spec(page["id"]), task_id)

    assert len(h.notion.comments[page["id"].replace("-", "")]) == 1


async def test_a_second_comment_attempt_beside_a_running_one_writes_nothing(harness):
    """Fails if a comment attempt starting while another is mid-write posts a second comment."""
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    task_id = ids.task_id(TENANT, "comment-overlap")
    hang = h.notion.hang_after_write("comment")
    first = asyncio.create_task(run(h.acts.add_notion_comment, comment_spec(page["id"]), task_id))
    await hang.reached.wait()

    with pytest.raises(ApplicationError) as busy:
        await run(h.acts.add_notion_comment, comment_spec(page["id"]), task_id)
    assert busy.value.type == "WriteInProgress" and not busy.value.non_retryable
    hang.release()
    comment_id = await first

    assert await run(h.acts.add_notion_comment, comment_spec(page["id"]), task_id) == comment_id
    assert len(h.notion.comments[page["id"].replace("-", "")]) == 1


# --- a comment under the approval policy (no card) -------------------------------------


def policy_comment_spec(page_id: str) -> TaskSpec:
    return dataclasses.replace(comment_spec(page_id), under_policy=True)


async def test_a_policy_comment_whose_response_was_lost_is_found_by_the_retry(harness):
    """Fails if the retry of a policy-allowed comment whose response was lost posts it a second time."""
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    task_id = ids.task_id(TENANT, "policy-comment-lost")
    h.notion.delay_visibility(1)
    h.notion.fail_after_write("comment")
    with pytest.raises(TimeoutError):
        await run(h.acts.add_notion_comment, policy_comment_spec(page["id"]), task_id)

    await run(h.acts.add_notion_comment, policy_comment_spec(page["id"]), task_id)

    assert len(h.notion.comments[page["id"].replace("-", "")]) == 1


async def test_a_second_policy_comment_attempt_beside_a_running_one_writes_nothing(harness):
    """Fails if a policy-allowed comment attempt starting while another is mid-write posts a second comment."""
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    task_id = ids.task_id(TENANT, "policy-comment-overlap")
    hang = h.notion.hang_after_write("comment")
    first = asyncio.create_task(
        run(h.acts.add_notion_comment, policy_comment_spec(page["id"]), task_id)
    )
    await hang.reached.wait()

    with pytest.raises(ApplicationError) as busy:
        await run(h.acts.add_notion_comment, policy_comment_spec(page["id"]), task_id)
    assert busy.value.type == "WriteInProgress" and not busy.value.non_retryable
    hang.release()
    comment_id = await first

    assert (
        await run(h.acts.add_notion_comment, policy_comment_spec(page["id"]), task_id) == comment_id
    )
    assert len(h.notion.comments[page["id"].replace("-", "")]) == 1


# --- the nudge's two effects ---------------------------------------------------------


def nudge_spec(page_id: str) -> TaskSpec:
    ctx = TurnContext(TENANT, "it-helper", 1, ALICE, "C_GENERAL", "1700000000.000100", "channel")
    payload = {
        "page_id": page_id,
        "database_id": DB_ID,
        "item_title": "Broken VPN",
        "item_url": "https://www.notion.so/brokenvpn",
        "comment": "Any update on this?",
        "assignee_name": "Bob",
        "assignee_notion_id": BOB_NOTION,
        "assignee_slack_id": BOB,
        "requester_name": "Alice",
    }
    return TaskSpec(ctx, "nudge", payload, NONCE)


def dm_posts(h) -> list[dict]:
    return [p for p in h.slack.posts() if p["channel"] == f"D-{BOB}"]


async def test_a_nudge_comment_whose_response_was_lost_is_found_by_the_retry(harness):
    """Fails if the retry of a nudge comment whose response was lost posts it a second time."""
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    task_id = ids.task_id(TENANT, "nudge-comment-lost")
    h.notion.delay_visibility(1)
    h.notion.fail_after_write("comment")
    with pytest.raises(TimeoutError):
        await run(h.acts.nudge_comment, nudge_spec(page["id"]), task_id)

    await run(h.acts.nudge_comment, nudge_spec(page["id"]), task_id)

    assert len(h.notion.comments[page["id"].replace("-", "")]) == 1


async def test_a_second_nudge_comment_attempt_beside_a_running_one_writes_nothing(harness):
    """Fails if a nudge comment attempt starting while another is mid-write posts a second comment."""
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop("Broken VPN")})
    task_id = ids.task_id(TENANT, "nudge-comment-overlap")
    hang = h.notion.hang_after_write("comment")
    first = asyncio.create_task(run(h.acts.nudge_comment, nudge_spec(page["id"]), task_id))
    await hang.reached.wait()

    with pytest.raises(ApplicationError) as busy:
        await run(h.acts.nudge_comment, nudge_spec(page["id"]), task_id)
    assert busy.value.type == "WriteInProgress" and not busy.value.non_retryable
    hang.release()
    comment_id = await first

    assert await run(h.acts.nudge_comment, nudge_spec(page["id"]), task_id) == comment_id
    assert len(h.notion.comments[page["id"].replace("-", "")]) == 1


async def test_a_nudge_dm_whose_response_was_lost_is_found_by_the_retry(harness):
    """Fails if the retry of a nudge DM whose response was lost sends it a second time."""
    h = harness
    task_id = ids.task_id(TENANT, "nudge-dm-lost")
    h.slack.fail_after_post()
    with pytest.raises(TimeoutError):
        await run(h.acts.nudge_dm, nudge_spec("page-1"), task_id)

    ts = await run(h.acts.nudge_dm, nudge_spec("page-1"), task_id)

    [dm] = dm_posts(h)
    assert dm["ts"] == ts


async def test_a_second_nudge_dm_attempt_beside_a_running_one_writes_nothing(harness):
    """Fails if a nudge DM attempt starting while another is mid-send sends a second DM."""
    h = harness
    task_id = ids.task_id(TENANT, "nudge-dm-overlap")
    hang = h.slack.hang_after_post()
    first = asyncio.create_task(run(h.acts.nudge_dm, nudge_spec("page-1"), task_id))
    await hang.reached.wait()

    with pytest.raises(ApplicationError) as busy:
        await run(h.acts.nudge_dm, nudge_spec("page-1"), task_id)
    assert busy.value.type == "WriteInProgress" and not busy.value.non_retryable
    hang.release()
    ts = await first

    assert await run(h.acts.nudge_dm, nudge_spec("page-1"), task_id) == ts
    assert len(dm_posts(h)) == 1


@pytest.mark.parametrize(
    ("stage", "code", "refused"),
    [
        ("post", "channel_not_found", True),
        ("post", "account_inactive", True),
        ("open_dm", "user_not_found", True),
        ("post", "internal_error", False),
    ],
    ids=["gone-channel", "inactive-account", "gone-user", "transient"],
)
async def test_a_slack_refusal_of_the_dm_is_audited_and_only_a_transient_one_is_retried(
    harness, stage, code, refused
):
    """Fails if a permanent Slack refusal of the DM is retryable or unaudited, or a transient one ends the nudge."""
    h = harness
    task_id = ids.task_id(TENANT, "nudge-dm-refused")
    error = slack_error(code, "https://slack.com/api/conversations.open")
    if stage == "open_dm":
        h.slack.fail_next_open_dm(error, times=1)
    else:
        h.slack.fail_next_post(error, times=1, channel=f"D-{BOB}")

    if refused:
        with pytest.raises(ApplicationError) as err:
            await run(h.acts.nudge_dm, nudge_spec("page-1"), task_id)
        assert err.value.non_retryable
        assert dm_posts(h) == []
        outcome = "error"
    else:
        # The first attempt fails retryably; the next attempt sends the DM.
        with pytest.raises(SlackApiError):
            await run(h.acts.nudge_dm, nudge_spec("page-1"), task_id)
        assert await run(h.acts.nudge_dm, nudge_spec("page-1"), task_id)
        assert len(dm_posts(h)) == 1
        outcome = "ok"

    events = [(e.kind, e.tool, e.outcome) for e in h.audit.events if e.task_id == task_id]
    assert events == [("approval", "nudge", "ok"), ("write", "nudge_dm", outcome)]


# --- a Drive comment (D-47) -----------------------------------------------------------


def drive_comment_spec() -> TaskSpec:
    ctx = TurnContext(TENANT, "it-helper", 1, ALICE, "C_GENERAL", "1700000000.000100", "channel")
    payload = {
        "page_id": DRIVE_DOC,
        "comment": "Any update?",
        "item_title": "Guide",
        "item_url": f"https://drive.google.com/file/d/{DRIVE_DOC}",
    }
    return TaskSpec(ctx, "add_comment", payload, NONCE, tool="comment_on_file")


async def test_a_drive_comment_whose_response_was_lost_is_found_by_the_retry(dharness):
    """Fails if the retry of a Drive comment whose response was lost posts it a second time."""
    h = dharness
    task_id = ids.task_id(TENANT, "drive-comment-lost")
    h.google.fail_after_write("comment")
    with pytest.raises(GoogleError):
        await run(h.acts.add_notion_comment, drive_comment_spec(), task_id)

    await run(h.acts.add_notion_comment, drive_comment_spec(), task_id)

    assert len(h.google.drive_comments[DRIVE_DOC]) == 1


async def test_a_second_drive_comment_attempt_beside_a_running_one_writes_nothing(dharness):
    """Fails if a Drive comment attempt starting while another is mid-write posts a second comment."""
    h = dharness
    task_id = ids.task_id(TENANT, "drive-comment-overlap")
    hang = h.google.hang_after_write("comment")
    first = asyncio.create_task(run(h.acts.add_notion_comment, drive_comment_spec(), task_id))
    await hang.reached.wait()

    with pytest.raises(ApplicationError) as busy:
        await run(h.acts.add_notion_comment, drive_comment_spec(), task_id)
    assert busy.value.type == "WriteInProgress" and not busy.value.non_retryable
    hang.release()
    comment_id = await first

    assert await run(h.acts.add_notion_comment, drive_comment_spec(), task_id) == comment_id
    assert len(h.google.drive_comments[DRIVE_DOC]) == 1


# --- a Drive sheet append (D-48) -------------------------------------------------------


async def grant_append_sheet(h):
    """Publish it-helper with the Roster sheet grant and serve its Q1 tab from the fake."""
    await republish(
        h.agent_store,
        "it-helper",
        lambda d: d["grants"].setdefault("drive", {}).update(sheets=[sheet_grant_block()]),
    )
    add_roster_sheet(h.google)


def append_spec() -> TaskSpec:
    ctx = TurnContext(TENANT, "it-helper", 1, ALICE, "C_GENERAL", "1700000000.000100", "channel")
    payload = {
        "page_id": DRIVE_SHEET,
        "sheet_id": DRIVE_SHEET,
        "tab": SHEET_TAB,
        "columns": list(SHEET_COLUMNS),
        "row": {"name": "bob", "role": "people"},
        "item_title": "Roster",
        "item_url": f"https://drive.google.com/file/d/{DRIVE_SHEET}",
    }
    return TaskSpec(ctx, "append_row", payload, NONCE, tool="append_row")


async def test_a_drive_append_whose_response_was_lost_is_found_by_the_retry(dharness):
    """Fails if the retry of a Drive append whose response was lost appends the row a second time."""
    h = dharness
    await grant_append_sheet(h)
    task_id = ids.task_id(TENANT, "drive-append-lost")
    h.google.fail_after_write("append")
    with pytest.raises(GoogleError):
        await run(h.acts.append_sheet_row, append_spec(), task_id)

    await run(h.acts.append_sheet_row, append_spec(), task_id)

    rows = h.google.sheets[DRIVE_SHEET][SHEET_TAB]["rows"]
    assert len(rows) == 3 and rows[-1] == ["bob", "people"]
    assert len(h.google.sheet_metadata[DRIVE_SHEET]) == 1


async def test_a_second_drive_append_attempt_beside_a_running_one_writes_nothing(dharness):
    """Fails if a Drive append attempt starting while another is mid-write appends a second row."""
    h = dharness
    await grant_append_sheet(h)
    task_id = ids.task_id(TENANT, "drive-append-overlap")
    hang = h.google.hang_after_write("append")
    first = asyncio.create_task(run(h.acts.append_sheet_row, append_spec(), task_id))
    await hang.reached.wait()

    with pytest.raises(ApplicationError) as busy:
        await run(h.acts.append_sheet_row, append_spec(), task_id)
    assert busy.value.type == "WriteInProgress" and not busy.value.non_retryable
    hang.release()
    metadata_id = await first

    assert await run(h.acts.append_sheet_row, append_spec(), task_id) == metadata_id
    rows = h.google.sheets[DRIVE_SHEET][SHEET_TAB]["rows"]
    assert len(rows) == 3
    assert len(h.google.sheet_metadata[DRIVE_SHEET]) == 1


async def test_a_drive_append_whose_marker_lags_is_found_by_the_retry_looking_again(dharness):
    """Fails if a retry whose metadata search lags the append appends the row a second time,
    instead of looking again until the marker is visible."""
    h = dharness
    h.acts.visibility_interval = 0
    await grant_append_sheet(h)
    task_id = ids.task_id(TENANT, "drive-append-lag")
    h.google.delay_visibility(2)
    h.google.fail_after_write("append")
    with pytest.raises(GoogleError):
        await run(h.acts.append_sheet_row, append_spec(), task_id)

    await run(h.acts.append_sheet_row, append_spec(), task_id)

    rows = h.google.sheets[DRIVE_SHEET][SHEET_TAB]["rows"]
    assert len(rows) == 3 and rows[-1] == ["bob", "people"]
    assert len(h.google.sheet_metadata[DRIVE_SHEET]) == 1


# --- a Drive create-from-template (D-49) -----------------------------------------------


async def grant_create_template(h):
    """Publish it-helper with the offer-template grant and serve the template tree."""
    await republish(
        h.agent_store,
        "it-helper",
        lambda d: d["grants"].setdefault("drive", {}).update(templates=[template_grant_block()]),
    )
    add_template_tree(h.google)


def create_doc_spec() -> TaskSpec:
    ctx = TurnContext(TENANT, "it-helper", 1, ALICE, "C_GENERAL", "1700000000.000100", "channel")
    payload = {
        "page_id": DRIVE_TEMPLATE,
        "template_id": DRIVE_TEMPLATE,
        "folder_id": TEMPLATE_FOLDER,
        "blanks": list(TEMPLATE_BLANKS),
        "title": "Offer for Bob",
        "values": {"name": "Bob", "role": "engineer"},
        "item_title": "Offer for Bob",
        "item_url": f"https://drive.google.com/drive/folders/{TEMPLATE_FOLDER}",
    }
    return TaskSpec(ctx, "create_doc", payload, NONCE, tool="create_doc_from_template")


async def test_a_drive_create_whose_copy_response_was_lost_fills_the_found_copy(dharness):
    """Fails if the retry of a create whose copy response was lost copies a second Doc, or
    leaves the copy an earlier attempt made unfilled."""
    h = dharness
    await grant_create_template(h)
    task_id = ids.task_id(TENANT, "drive-create-lost")
    h.google.fail_after_write("copy")
    with pytest.raises(GoogleError):
        await run(h.acts.create_drive_doc, create_doc_spec(), task_id)

    await run(h.acts.create_drive_doc, create_doc_spec(), task_id)

    [new_id] = h.google.drive_children[TEMPLATE_FOLDER]
    assert h.google.drive_content[new_id][0].text == "Dear Bob,\nYou are hired as engineer."
    assert len(h.google.copy_calls) == 1


async def test_a_second_drive_create_attempt_beside_a_running_one_writes_nothing(dharness):
    """Fails if a create attempt starting while another is mid-copy copies a second Doc."""
    h = dharness
    await grant_create_template(h)
    task_id = ids.task_id(TENANT, "drive-create-overlap")
    hang = h.google.hang_after_write("copy")
    first = asyncio.create_task(run(h.acts.create_drive_doc, create_doc_spec(), task_id))
    await hang.reached.wait()

    with pytest.raises(ApplicationError) as busy:
        await run(h.acts.create_drive_doc, create_doc_spec(), task_id)
    assert busy.value.type == "WriteInProgress" and not busy.value.non_retryable
    hang.release()
    item = await first

    assert (await run(h.acts.create_drive_doc, create_doc_spec(), task_id)).page_id == item.page_id
    assert len(h.google.drive_children[TEMPLATE_FOLDER]) == 1


async def test_a_drive_create_whose_marker_lags_is_found_by_the_retry_looking_again(dharness):
    """Fails if a retry whose appProperty search lags the copy copies a second Doc, instead
    of looking again until the marker is visible."""
    h = dharness
    h.acts.visibility_interval = 0
    await grant_create_template(h)
    task_id = ids.task_id(TENANT, "drive-create-lag")
    h.google.delay_visibility(2)
    h.google.fail_after_write("copy")
    with pytest.raises(GoogleError):
        await run(h.acts.create_drive_doc, create_doc_spec(), task_id)

    await run(h.acts.create_drive_doc, create_doc_spec(), task_id)

    [new_id] = h.google.drive_children[TEMPLATE_FOLDER]
    assert h.google.drive_content[new_id][0].text == "Dear Bob,\nYou are hired as engineer."
    assert len(h.google.copy_calls) == 1


@pytest.mark.parametrize(
    "template_text",
    [
        "Dear {{name}},\nYou are hired as {{role}}.",
        "Dear {{ name }},\nYou are hired as {{ role }}.",
    ],
    ids=["canonical", "spaced"],
)
async def test_a_drive_create_whose_fill_response_was_lost_is_found_filled_by_the_retry(
    dharness, template_text
):
    """Fails if the retry of a create whose fill response was lost fills the copy a second
    time or copies a second Doc, or a spaced blank is left unfilled so the filled copy
    reads as not done."""
    h = dharness
    await grant_create_template(h)
    h.google.set_drive_content(DRIVE_TEMPLATE, [DriveSection("", template_text)])
    task_id = ids.task_id(TENANT, "drive-create-fill-lost")
    h.google.fail_after_write("fill")
    with pytest.raises(GoogleError):
        await run(h.acts.create_drive_doc, create_doc_spec(), task_id)

    await run(h.acts.create_drive_doc, create_doc_spec(), task_id)

    [new_id] = h.google.drive_children[TEMPLATE_FOLDER]
    assert h.google.drive_content[new_id][0].text == "Dear Bob,\nYou are hired as engineer."
    assert len(h.google.copy_calls) == 1
    assert len(h.google.fill_calls) == 1
