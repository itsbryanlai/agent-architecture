"""What an admin sees of a grant before publishing, and the Notion picker.

Fakes only: the in-memory plane, FakeNotionAPI and an httpx MockTransport for Notion's OAuth
and search endpoints.
"""

import copy
import json
from dataclasses import replace
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from agentos.access_diff import WideningNotAcknowledged, diff_access
from agentos.audience import EffectiveAudience
from agentos.config import DriveGrant, NotionPageGrant
from agentos.connectors.notion import normalize_id
from agentos.control_api import create_app
from agentos.creator_access import (
    CANNOT_VERIFY,
    LOST,
    VERIFIED,
    AccessResult,
    InMemoryCreatorAccessStore,
)
from agentos.grant_picks import GrantNotPicked
from agentos.notion_picker import HttpNotionOAuth, Picker, PickerConfig
from tests.conftest import DB_ID, TENANT, agents_dict, parse_agents_config
from tests.helpers.agents import helper_block
from tests.helpers.console_harness import (
    OTHER,
    OWNER,
    ROOT,
    ConsoleHarness,
    csrf_token,
    post,
    using,
)
from tests.helpers.control_api import JSON
from tests.helpers.drive import (
    DRIVE_FOLDER,
    DRIVE_SHEET,
    DRIVE_TEMPLATE,
    TEMPLATE_FOLDER,
    TEMPLATE_PLAIN_FOLDER,
)

API = f"/tenants/{TENANT}"
VERSION = f"/ui/{TENANT}/agents/it-helper/versions/1"
PAGE = "cccccccc-0000-0000-0000-000000000001"
USER_TOKEN = "ntn_user_token_for_the_picker_test"


class Admin:
    def __init__(self, user: str):
        self.user = user

    async def actor(self, request) -> str:
        return self.user


@pytest.fixture
async def h():
    harness = ConsoleHarness()
    await harness.seed_members()
    return harness


def api_client(h: ConsoleHarness, user: str) -> httpx.AsyncClient:
    app = create_app(h.plane, h.tenants, Admin(user))
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8100", headers=JSON
    )


async def pending(h, definition=None, version_agent="it-helper"):
    await h.plane.add_agent(TENANT, OWNER, version_agent, definition or helper_block())
    await h.plane.submit_version(TENANT, OWNER, version_agent, 1)


async def publish_first(h):
    await pending(h)
    sha = (await h.agents.get_version(TENANT, "it-helper", 1)).definition_sha256
    await h.plane.publish_version(
        TENANT, ROOT, "it-helper", 1, reviewed_sha256=sha, acknowledge_widening=True
    )


async def second_version(h, change):
    """Version 2 of it-helper with ``change`` applied to the definition, submitted."""
    draft = await h.plane.new_draft(TENANT, OWNER, "it-helper")
    definition = copy.deepcopy(draft.definition)
    change(definition)
    await h.plane.edit_draft(TENANT, OWNER, "it-helper", draft.version, definition)
    await h.plane.submit_version(TENANT, OWNER, "it-helper", draft.version)
    return draft.version


def add_page_grant(definition):
    definition["grants"]["notion"]["pages"] = [{"id": PAGE, "name": "Handbook"}]


async def hash_of(h, version: int) -> str:
    return (await h.agents.get_version(TENANT, "it-helper", version)).definition_sha256


# --- the live title and the label ----------------------------------------------------


@pytest.mark.parametrize(
    ("live_title", "warns"), [("HR salaries", True), ("  it   REQUESTS ", False)]
)
async def test_a_label_that_differs_from_the_live_title_is_flagged(h, live_title, warns):
    """Fails if the admin is not told that the creator's label names something other than
    what the ID points at, on the version page or in the API, or is warned about a mere
    difference in case and spacing."""
    h.notion.set_database_info(DB_ID, live_title, {"type": "workspace"})
    await pending(h)
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(VERSION)).text
    async with api_client(h, ROOT) as api:
        review = (await api.get(f"{API}/agents/it-helper/versions/1/review")).json()
    [grant] = review["grants"]
    assert grant["live_title"] == live_title and grant["parent"] == "workspace"
    assert grant["label_differs"] is warns
    assert ("does not match the live title" in page) is warns
    assert live_title.strip() in page.replace("&#39;", "'")


async def test_a_page_grant_shows_its_parent(h):
    """Fails if the first ancestor of a granted page is not named to the admin."""
    h.notion.add_free_page("dddddddd-0000-0000-0000-000000000001", "Company wiki")
    h.notion.add_free_page(
        PAGE, "Handbook", {"type": "page_id", "page_id": "dddddddd-0000-0000-0000-000000000001"}
    )
    definition = helper_block()
    add_page_grant(definition)
    await pending(h, definition)
    async with api_client(h, ROOT) as api:
        review = (await api.get(f"{API}/agents/it-helper/versions/1/review")).json()
    page_grant = next(g for g in review["grants"] if g["kind"] == "page")
    assert page_grant["live_title"] == "Handbook"
    assert page_grant["parent"] == "page 'Company wiki'"


# --- the diff and the widening box ---------------------------------------------------

BASE = EffectiveAudience(True, ("C111",), False, (), False)


@pytest.mark.parametrize(
    ("grant_change", "audience", "widens"),
    [
        ("add_page", BASE, True),
        ("add_drive_folder", BASE, True),
        ("add_drive_sheet", BASE, True),
        ("sheet_changed_tab", BASE, True),
        ("add_drive_template", BASE, True),
        ("template_changed_folder", BASE, True),
        ("other_properties", BASE, True),
        ("none", replace(BASE, channels=("C111", "C222")), True),
        ("none", replace(BASE, dm_everyone=True), True),
        ("none", replace(BASE, dm_user_groups=("S123",)), True),
        ("none", replace(BASE, guests=True), True),
        ("none", replace(BASE, any_unmapped_channel=True), True),
        ("remove_database", BASE, False),
        ("relabel", BASE, False),
        ("none", replace(BASE, channels=()), False),
        ("none", BASE, False),
    ],
)
def test_which_changes_widen_access(grant_change, audience, widens):
    """Fails if a new grant (a Drive folder, sheet or template included), other properties
    (a sheet's tab or a template's folder included), a new channel, wider direct messages or
    guests do not count as widening, or if a removal, a relabel or no change does."""
    old = parse_agents_config(agents_dict()).agents["it-helper"]
    new = copy.deepcopy(old)
    if grant_change == "add_page":
        new.grants.notion_pages.append(NotionPageGrant(PAGE, "Handbook"))
    elif grant_change == "add_drive_folder":
        new.grants.drive.append(DriveGrant(DRIVE_FOLDER, "Team folder", "folder"))
    elif grant_change == "add_drive_sheet":
        new.grants.drive.append(
            DriveGrant(DRIVE_SHEET, "Roster", "sheet", tab="Q1", columns=("name", "role"))
        )
    elif grant_change == "sheet_changed_tab":
        old.grants.drive.append(
            DriveGrant(DRIVE_SHEET, "Roster", "sheet", tab="Q1", columns=("name", "role"))
        )
        new.grants.drive.append(
            DriveGrant(DRIVE_SHEET, "Roster", "sheet", tab="Q2", columns=("name", "role"))
        )
    elif grant_change == "add_drive_template":
        new.grants.drive.append(
            DriveGrant(
                DRIVE_TEMPLATE,
                "Offer template",
                "template",
                folder_id=TEMPLATE_FOLDER,
                blanks=("name", "role"),
            )
        )
    elif grant_change == "template_changed_folder":
        old.grants.drive.append(
            DriveGrant(
                DRIVE_TEMPLATE,
                "Offer template",
                "template",
                folder_id=TEMPLATE_FOLDER,
                blanks=("name", "role"),
            )
        )
        new.grants.drive.append(
            DriveGrant(
                DRIVE_TEMPLATE,
                "Offer template",
                "template",
                folder_id=TEMPLATE_PLAIN_FOLDER,
                blanks=("name", "role"),
            )
        )
    elif grant_change == "other_properties":
        new.grants.notion_databases[0].status_prop_id = "other-status"
    elif grant_change == "remove_database":
        new.grants.notion_databases.clear()
    elif grant_change == "relabel":
        new.grants.notion_databases[0].name = "Tickets"
    assert diff_access(old, new, BASE, audience).widens is widens


async def test_a_first_publish_needs_the_acknowledgement(h):
    """Fails if a version with nothing published before it, which adds every grant and the whole
    audience, publishes without the acknowledgement, or its page does not list them as added."""
    await pending(h)
    sha = await hash_of(h, 1)
    with pytest.raises(WideningNotAcknowledged, match="IT requests"):
        await h.plane.publish_version(
            TENANT, ROOT, "it-helper", 1, reviewed_sha256=sha, reviewed_published="none"
        )
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(VERSION)).text
    assert "This version widens access" in page and "added: database grant" in page
    await h.plane.publish_version(
        TENANT, ROOT, "it-helper", 1, reviewed_sha256=sha, acknowledge_widening=True
    )
    assert (await h.agents.get_version(TENANT, "it-helper", 1)).status == "published"


async def test_a_widening_publish_needs_the_acknowledgement(h):
    """Fails if a version that adds a grant publishes without the approver's
    acknowledgement (plane, API or console), is refused with it, or leaves no audit event."""
    await publish_first(h)
    h.notion.add_free_page(PAGE, "Handbook")
    version = await second_version(h, add_page_grant)
    sha = await hash_of(h, version)
    bound = {"definition_sha256": sha, "reviewed_published": "1"}

    with pytest.raises(WideningNotAcknowledged, match="Handbook"):
        await h.plane.publish_version(TENANT, ROOT, "it-helper", version, reviewed_sha256=sha)
    async with api_client(h, ROOT) as api:
        url = f"{API}/agents/it-helper/versions/{version}/publish"
        refused = await api.post(url, json=bound)
        assert refused.status_code == 409 and "widens access" in refused.json()["error"]
    c = await h.signed_in(ROOT)
    async with using(c):
        path = f"/ui/{TENANT}/agents/it-helper/versions/{version}"
        page = (await c.get(path)).text
        assert "This version widens access" in page
        assert 'name="reviewed_published" value="1"' in page
        csrf = await csrf_token(c)
        assert (await post(c, f"{path}/publish", csrf, **bound)).status_code == 409
        allowed = await post(c, f"{path}/publish", csrf, widening="yes", **bound)
        assert allowed.status_code == 303
    assert (await h.agents.get_version(TENANT, "it-helper", version)).status == "published"
    assert "widening_confirm" in [e.kind for e in h.audit.events]


async def test_the_acknowledgement_names_the_version_it_was_reviewed_against(h):
    """Fails if an acknowledgement given against one published version is accepted after another
    version was published, when the diff the approver saw no longer describes the change."""
    await publish_first(h)
    h.notion.add_free_page(PAGE, "Handbook")
    second = await second_version(h, add_page_grant)
    third = await second_version(h, lambda d: d.update(max_steps=3))
    await h.plane.publish_version(
        TENANT,
        ROOT,
        "it-helper",
        second,
        reviewed_sha256=await hash_of(h, second),
        acknowledge_widening=True,
        reviewed_published="1",
    )

    async with api_client(h, ROOT) as api:
        stale = await api.post(
            f"{API}/agents/it-helper/versions/{third}/publish",
            json={
                "definition_sha256": await hash_of(h, third),
                "acknowledge_widening": True,
                "reviewed_published": "1",
            },
        )
    assert stale.status_code == 409 and "changed since you opened it" in stale.text
    assert (await h.agents.get_version(TENANT, "it-helper", third)).status == "pending"


async def test_a_narrowing_version_shows_no_banner_and_publishes_freely(h):
    """Fails if removing a grant raises the widening banner or demands the acknowledgement."""
    await publish_first(h)
    version = await second_version(h, lambda d: d["grants"]["notion"].update(databases=[]))
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(f"/ui/{TENANT}/agents/it-helper/versions/{version}")).text
    assert "widens access" not in page and "removed: database grant" in page
    await h.plane.publish_version(
        TENANT, ROOT, "it-helper", version, reviewed_sha256=await hash_of(h, version)
    )


@pytest.mark.parametrize(
    ("audience", "warns"),
    [
        ({"channels": ["C0REQUESTS"]}, True),
        ({"channels": ["C0REQUESTS"], "dm": "everyone"}, False),
    ],
    ids=["omits dm", "dm everyone"],
)
async def test_a_draft_closing_direct_messages_warns_plainly(h, audience, warns):
    """Fails if a draft whose audience omits dm does not say plainly that direct messages
    will be closed, on the version page and in the diff the publish confirmation shows,
    or a draft keeping dm open warns."""
    await publish_first(h)
    version = await second_version(h, lambda d: d.update(audience=audience))
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(f"/ui/{TENANT}/agents/it-helper/versions/{version}")).text
    warning = "Direct messages: nobody (add <code>dm: everyone</code> to keep them open)"
    assert (warning in page) is warns
    diff_line = "changed: direct messages: nobody (add dm: everyone to keep them open)"
    assert (diff_line in page) is warns


# --- check-grants detail -------------------------------------------------------------


async def test_a_creator_gets_ok_or_a_category_and_an_admin_gets_the_detail(h):
    """Fails if a creator calling check-grants learns property names, types or titles, or if
    the admin loses that detail."""
    h.notion.set_database_info(DB_ID, "HR salaries")
    schema = h.notion.schemas[DB_ID.replace("-", "")]
    schema["Agent request ID"]["type"] = "people"
    await pending(h)
    url = f"{API}/agents/it-helper/versions/1/check-grants"
    async with api_client(h, OWNER) as api:
        creator = (await api.post(url)).json()
    async with api_client(h, ROOT) as api:
        admin = (await api.post(url)).json()
    [seen] = creator["grants"]
    assert creator["ok"] is False and seen["ok"] is False and seen["reason"] == "mapping_mismatch"
    text = json.dumps(creator)
    assert "Agent request ID" not in text and "people" not in text and "HR salaries" not in text
    assert "request_id" in admin["grants"][0]["problems"][0]
    assert admin["grants"][0]["live_title"] == "HR salaries"


async def test_a_creator_is_told_why_a_page_is_unreachable_and_nothing_more(h):
    """Fails if a creator is given Notion's error type, rather than a category, for a page the
    integration cannot see."""
    definition = helper_block()
    add_page_grant(definition)
    await pending(h, definition)  # PAGE is not in Notion
    async with api_client(h, OWNER) as api:
        body = (await api.post(f"{API}/agents/it-helper/versions/1/check-grants")).json()
    page = next(g for g in body["grants"] if g["kind"] == "page")
    assert page["reason"] == "not_shared_with_integration" and "problems" not in page
    assert "FakeNotFound" not in json.dumps(body)


# --- the picker ----------------------------------------------------------------------


def notion_transport(seen: dict):
    """Notion's OAuth token and search endpoints: the token buys a listing of the creator's."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/oauth/token":
            seen["code"] = json.loads(request.content)["code"]
            return httpx.Response(200, json={"access_token": USER_TOKEN})
        if request.url.path == "/v1/search":
            seen["auth"] = request.headers["authorization"]
            if request.headers["authorization"] != f"Bearer {USER_TOKEN}":
                return httpx.Response(200, json={"results": [{"object": "page", "id": "x"}]})
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "object": "database",
                            "id": DB_ID,
                            "title": [{"plain_text": "IT requests"}],
                        },
                        {
                            "object": "page",
                            "id": PAGE,
                            "properties": {
                                "title": {"type": "title", "title": [{"plain_text": "Handbook"}]}
                            },
                        },
                    ],
                    "has_more": False,
                },
            )
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def make_picker(seen: dict) -> Picker:
    config = PickerConfig("notion-client", "notion-secret")
    return Picker(config, HttpNotionOAuth(config, notion_transport(seen)))


@pytest.fixture
async def hp():
    seen: dict = {}
    harness = ConsoleHarness(picker=make_picker(seen), require_grant_picker=True)
    await harness.seed_members()
    harness.seen = seen
    return harness


async def start_flow(c) -> str:
    response = await post(c, f"{VERSION}/pick/start", await csrf_token(c))
    assert response.status_code == 303
    location = response.headers["location"]
    assert location.startswith("https://api.notion.com/v1/oauth/authorize")
    return parse_qs(urlsplit(location).query)["state"][0]


async def test_a_notion_sign_in_with_the_wrong_state_is_refused(hp):
    """Fails if the Notion callback accepts a state this session never began, or one that
    another signed-in user began."""
    await hp.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    owner = await hp.signed_in(OWNER)
    other = await hp.signed_in(OTHER)
    async with using(owner), using(other):
        state = await start_flow(owner)
        forged = await owner.get("/auth/notion/callback", params={"code": "c", "state": "nope"})
        stolen = await other.get("/auth/notion/callback", params={"code": "c", "state": state})
        assert forged.status_code == 400 and stolen.status_code == 400
        assert "code" not in hp.seen  # Notion was never asked
        # The state was spent by the failed use: even the owner cannot reuse it.
        spent = await owner.get("/auth/notion/callback", params={"code": "c", "state": state})
        assert spent.status_code == 400


async def test_the_picker_lists_what_the_creators_token_sees_and_stores_no_token(hp):
    """Fails if the list comes from the tenant integration instead of the creator's token, if
    an object not in that list can be picked, or if the creator's token is kept anywhere."""
    definition = helper_block()
    add_page_grant(definition)
    await hp.plane.add_agent(TENANT, OWNER, "it-helper", definition)
    c = await hp.signed_in(OWNER)
    async with using(c):
        state = await start_flow(c)
        back = await c.get("/auth/notion/callback", params={"code": "abc", "state": state})
        assert back.status_code == 303 and back.headers["location"].endswith("/pick")
        assert hp.seen["auth"] == f"Bearer {USER_TOKEN}" and hp.seen["code"] == "abc"
        listing = (await c.get(back.headers["location"])).text
        assert "IT requests" in listing and "Handbook" in listing
        # Position 5 is not in the list; only positions 0 and 1 exist.
        chosen = await post(
            c,
            f"{VERSION}/pick/choose",
            await csrf_token(c),
            pick_0="yes",
            pick_1="yes",
            pick_5="yes",
        )
        assert chosen.status_code == 303
    picks = await hp.plane.version_review(TENANT, OWNER, "it-helper", 1)
    assert sorted(p.picked_by for p in picks.picks.values()) == [OWNER, OWNER]
    # Settle point: the flow is over. The token is in no store, session or audit row.
    holders = [hp.picker_state(), hp.plane._picks._rows, hp.audit.events, hp.sessions._rows]
    assert USER_TOKEN not in repr(holders)


async def test_a_grant_not_picked_cannot_be_submitted_when_the_picker_is_required(hp):
    """Fails if a creator can submit a new grant they did not choose from their Notion, or
    cannot once they have."""
    await hp.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    with pytest.raises(GrantNotPicked, match="IT requests"):
        await hp.plane.submit_version(TENANT, OWNER, "it-helper", 1)
    from agentos.grant_picks import PickedObject

    await hp.plane.record_grant_picks(
        TENANT, OWNER, "it-helper", 1, [PickedObject(DB_ID.upper(), "database")]
    )
    await hp.plane.submit_version(TENANT, OWNER, "it-helper", 1)


@pytest.mark.parametrize(
    ("status", "cell"),
    [
        (VERIFIED, "verified 2026-11-14 12:00 UTC"),
        (LOST, '<span class="error">lost</span>'),
        (CANNOT_VERIFY, "cannot verify (no consent)"),
        (None, '<span class="muted">not checked yet</span>'),
    ],
    ids=["verified", "lost", "cannot-verify", "not-checked"],
)
async def test_the_version_page_shows_the_creators_access_per_grant(status, cell):
    """Fails if the version page hides the nightly verdict on a grant, or shows the wrong one."""
    access = InMemoryCreatorAccessStore()
    harness = ConsoleHarness(picker=make_picker({}), creator_access=access)
    await harness.seed_members()
    await pending(harness)
    if status is not None:
        await access.record(
            TENANT,
            "it-helper",
            [AccessResult(normalize_id(DB_ID), status)],
            harness.clock(),
            {normalize_id(DB_ID)},
        )
    c = await harness.signed_in(ROOT)
    async with using(c):
        page = (await c.get(VERSION)).text
    assert cell in page
