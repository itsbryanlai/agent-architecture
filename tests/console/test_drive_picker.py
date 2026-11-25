"""The Drive grant picker on the version page: the workspace's account, one click to grant."""

from __future__ import annotations

import pytest

from agentos import ids
from agentos.connectors.google_iam import (
    MIME_DOC,
    MIME_SHEET,
    DriveItem,
    DriveSection,
    FakeGoogleAccounts,
)
from agentos.drive_connections import InMemoryDriveConnectionStore
from tests.conftest import TENANT
from tests.helpers.agents import helper_block
from tests.helpers.console_harness import ROOT, ConsoleHarness, csrf_in, post, using
from tests.helpers.drive import (
    DRIVE_DOC,
    DRIVE_FOLDER,
    DRIVE_SHARED,
    DRIVE_SHARED_DOC,
    DRIVE_SHARED_NESTED,
    DRIVE_SHARED_SUB,
    DRIVE_SHEET,
    DRIVE_TEMPLATE,
    TEMPLATE_DRIVE,
    TEMPLATE_FOLDER,
    TEMPLATE_PLAIN_FOLDER,
    add_template_tree,
)

VERSION = f"/ui/{TENANT}/agents/it-helper/versions/1"
NESTED_SHEET = "drive-nested-sheet-0001"


def tenant_email() -> str:
    return f"{ids.drive_account_id(TENANT)}@test-project.iam.gserviceaccount.com"


async def picker_harness(connected: bool = True):
    drive = InMemoryDriveConnectionStore()
    google = FakeGoogleAccounts()
    h = ConsoleHarness(drive=drive, google=google)
    await h.seed_members()
    if connected:
        await h.plane.connect_drive(TENANT, ROOT)
        google.shared[tenant_email()] = [
            DriveItem(
                DRIVE_FOLDER,
                "Team folder",
                f"https://drive.google.com/drive/folders/{DRIVE_FOLDER}",
                "folder",
            ),
            DriveItem(DRIVE_DOC, "Guide", f"https://drive.google.com/file/d/{DRIVE_DOC}", "file"),
            DriveItem(
                DRIVE_SHARED,
                "Team drive",
                f"https://drive.google.com/drive/folders/{DRIVE_SHARED}",
                "shared_drive",
            ),
            DriveItem(
                DRIVE_SHEET, "Roster", f"https://drive.google.com/file/d/{DRIVE_SHEET}", "sheet"
            ),
        ]
        google.add_drive_file(DRIVE_SHEET, "Roster", MIME_SHEET, DRIVE_FOLDER, [])
        google.add_sheet_tab(DRIVE_SHEET, "Q1", ["name", "role"])
        google.add_sheet_tab(DRIVE_SHEET, "Q2", ["name", "role"])
    await h.agents.create_agent(TENANT, "it-helper", helper_block(), ROOT)
    return h, google


@pytest.mark.parametrize("case", ["connected", "not connected", "not set up"])
async def test_the_version_page_drive_section(case):
    """Fails if the section offers the picker when Drive is not connected, hides the
    Settings link when it is not connected, or shows a picker on a deployment without Drive."""
    if case == "not set up":
        h = ConsoleHarness()
        await h.agents.create_agent(TENANT, "it-helper", helper_block(), ROOT)
    else:
        h, _ = await picker_harness(connected=case == "connected")
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(VERSION)).text
    assert "Google Drive" in page
    if case == "connected":
        assert "Choose from Google Drive" in page
        assert "available to every agent creator" in page
    elif case == "not connected":
        assert "Connect it in Settings" in page
        assert "Choose from Google Drive" not in page
    else:
        assert "not set up on this deployment" in page
        assert "Choose from Google Drive" not in page


async def test_the_picker_lists_the_account_items_and_adds_a_grant_in_one_click():
    """Fails if the picker does not list what the workspace's account can see, marks a
    shared drive as an ordinary folder, or a chosen item is not added to the draft as a
    Drive grant of the right kind."""
    h, _ = await picker_harness()
    c = await h.signed_in(ROOT)
    async with using(c):
        pick = (await c.get(VERSION + "/drive/pick")).text
        assert "Team folder" in pick and "Guide" in pick and "Team drive" in pick
        assert "shared_drive" in pick
        resp = await post(
            c,
            VERSION + "/drive/pick/choose",
            csrf_in(pick),
            pick=[DRIVE_FOLDER, DRIVE_SHARED],
            request_id="r1",
        )
        assert resp.status_code == 303
        page = (await c.get(VERSION)).text
        assert DRIVE_FOLDER in page
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    assert [(g.id, g.kind) for g in record.agent().grants.drive] == [
        (DRIVE_FOLDER, "folder"),
        (DRIVE_SHARED, "shared_drive"),
    ]


async def test_picking_a_sheet_asks_for_its_tab_and_adds_the_sheet_grant():
    """Fails if the picker does not list a shared Sheet, grants it without asking for its
    tab, or the added sheet grant does not carry the chosen tab."""
    h, _ = await picker_harness()
    c = await h.signed_in(ROOT)
    async with using(c):
        pick = (await c.get(VERSION + "/drive/pick")).text
        assert "Roster" in pick and "sheet" in pick
        resp = await post(
            c, VERSION + "/drive/pick/choose", csrf_in(pick), pick=DRIVE_SHEET, request_id="r1"
        )
        assert resp.status_code == 303
        tabs_url = resp.headers["location"]
        assert "/drive/pick/tabs" in tabs_url and DRIVE_SHEET in tabs_url
        tabs_page = (await c.get(tabs_url)).text
        assert "Q1" in tabs_page and "Q2" in tabs_page
        resp = await post(
            c,
            VERSION + "/drive/pick/tabs",
            csrf_in(tabs_page),
            sheet_id_0=DRIVE_SHEET,
            tab_0="1",
            request_id="r2",
        )
        assert resp.status_code == 303
        page = (await c.get(VERSION)).text
        assert DRIVE_SHEET in page and "Q2" in page
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    assert [(g.id, g.kind, g.tab) for g in record.agent().grants.drive] == [
        (DRIVE_SHEET, "sheet", "Q2")
    ]


async def test_removing_a_drive_grant():
    """Fails if removing a Drive grant leaves it in the draft."""
    h, _ = await picker_harness()
    await h.plane.add_drive_grants(
        TENANT,
        ROOT,
        "it-helper",
        1,
        [DriveItem(DRIVE_FOLDER, "Team folder", "https://x", "folder")],
    )
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(VERSION)).text
        assert DRIVE_FOLDER in page
        resp = await post(
            c, VERSION + "/drive/remove", csrf_in(page), grant_id=DRIVE_FOLDER, request_id="r1"
        )
        assert resp.status_code == 303
        page = (await c.get(VERSION)).text
        assert "no Google Drive grants" in page
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    assert record.agent().grants.drive == []


@pytest.mark.parametrize(
    ("browse", "choice", "folder_id"),
    [
        (TEMPLATE_DRIVE, {"dest": TEMPLATE_FOLDER}, TEMPLATE_FOLDER),
        (TEMPLATE_FOLDER, {"here": "yes"}, TEMPLATE_FOLDER),
        (TEMPLATE_DRIVE, {"here": "yes"}, None),
        ("", {"dest": TEMPLATE_DRIVE}, None),
        ("", {"dest": TEMPLATE_PLAIN_FOLDER}, None),
    ],
    ids=[
        "folder-from-the-list",
        "this-folder-in-a-shared-drive",
        "shared-drive-root-as-this-folder",
        "shared-drive-root-from-the-list",
        "my-drive-folder",
    ],
)
async def test_the_template_picker_takes_a_nested_template_then_a_shared_drive_folder(
    browse, choice, folder_id
):
    """Fails if the template picker does not browse into a shared drive for the template, offers
    "Put new Docs in this folder" anywhere but a folder inside a shared drive, or takes a
    shared drive's root or a My Drive folder as the destination."""
    h, google = await picker_harness()
    add_template_tree(google)
    google.shared[tenant_email()] = [
        DriveItem(TEMPLATE_DRIVE, "HR shared drive", "https://x", "shared_drive"),
        DriveItem(TEMPLATE_PLAIN_FOLDER, "My folder", "https://x", "folder"),
    ]
    c = await h.signed_in(ROOT)
    async with using(c):
        top = (await c.get(VERSION + "/drive/pick/template")).text
        assert f"?folder={TEMPLATE_DRIVE}" in top
        step_one = (await c.get(VERSION + f"/drive/pick/template?folder={TEMPLATE_DRIVE}")).text
        assert "Offer template" in step_one
        resp = await post(
            c,
            VERSION + "/drive/pick/template",
            csrf_in(step_one),
            template=DRIVE_TEMPLATE,
            folder=TEMPLATE_DRIVE,
            request_id="r1",
        )
        assert resp.status_code == 303
        query = f"&folder={browse}" if browse else ""
        step_two = (await c.get(resp.headers["location"] + query)).text
        assert "a folder inside a shared drive" in step_two
        assert ("Put new Docs in this folder" in step_two) == (browse == TEMPLATE_FOLDER)
        resp = await post(
            c,
            VERSION + "/drive/pick/template",
            csrf_in(step_two),
            template=DRIVE_TEMPLATE,
            folder=browse,
            request_id="r2",
            **choice,
        )
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    if folder_id:
        assert resp.status_code == 303
        assert [(g.id, g.kind, g.folder_id) for g in record.agent().grants.drive] == [
            (DRIVE_TEMPLATE, "template", folder_id)
        ]
    else:
        assert resp.status_code == 409
        assert "Pick a destination folder inside a shared drive" in resp.text
        assert record.agent().grants.drive == []


@pytest.mark.parametrize(
    "case",
    ["from list", "typed", "already granted", "nested"],
    ids=["list", "typed", "already-granted", "nested"],
)
async def test_the_section_picker_asks_for_the_heading_and_adds_the_section_grant(case):
    """Fails if the section picker grants a Doc without asking for its heading, does not offer
    the Doc's headings from one read (or a typed name), adds the wrong heading, or accepts a
    Doc already granted as a read file instead of showing a plain message."""
    h, google = await picker_harness()
    nested = case == "nested"
    # A nested Doc is reached by opening the shared folder: the form carries where it was.
    where = {"folder": DRIVE_FOLDER} if nested else {}
    google.shared[tenant_email()] = (
        []
        if nested
        else [
            DriveItem(
                DRIVE_DOC,
                "Guide",
                f"https://drive.google.com/file/d/{DRIVE_DOC}",
                "file",
                mime_type=MIME_DOC,
            )
        ]
    )
    google.add_drive_folder(DRIVE_FOLDER, "Team folder")
    google.add_drive_doc(
        DRIVE_DOC,
        "Guide",
        [
            {"text": "Status\n", "style": "HEADING_1"},
            {"text": "Old.\n", "style": "NORMAL_TEXT"},
            {"text": "Other\n", "style": "HEADING_1"},
        ],
        DRIVE_FOLDER,
    )
    if case == "already granted":
        await h.plane.add_drive_grants(
            TENANT, ROOT, "it-helper", 1, [DriveItem(DRIVE_DOC, "Guide", "https://x", "file")]
        )
    c = await h.signed_in(ROOT)
    async with using(c):
        query = f"?folder={DRIVE_FOLDER}" if nested else ""
        pick = (await c.get(VERSION + "/drive/pick/section" + query)).text
        assert "Guide" in pick
        resp = await post(
            c,
            VERSION + "/drive/pick/section",
            csrf_in(pick),
            doc=DRIVE_DOC,
            request_id="r1",
            **where,
        )
        if case == "already granted":
            assert resp.status_code == 409
            assert "already granted" in resp.text
            record = await h.agents.get_version(TENANT, "it-helper", 1)
            assert [(g.id, g.kind) for g in record.agent().grants.drive] == [(DRIVE_DOC, "file")]
            return
        assert resp.status_code == 200
        heading_page = resp.text
        assert "Status" in heading_page and "Other" in heading_page
        if case in ("from list", "nested"):
            resp = await post(
                c,
                VERSION + "/drive/pick/section",
                csrf_in(heading_page),
                doc=DRIVE_DOC,
                heading_pos="0",
                request_id="r2",
                **where,
            )
            expected_heading = "Status"
        else:
            resp = await post(
                c,
                VERSION + "/drive/pick/section",
                csrf_in(heading_page),
                doc=DRIVE_DOC,
                heading_text="Overview",
                request_id="r2",
                **where,
            )
            expected_heading = "Overview"
        assert resp.status_code == 303
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    assert [(g.id, g.kind, g.heading, g.doc_id) for g in record.agent().grants.drive] == [
        (DRIVE_DOC, "section", expected_heading, DRIVE_DOC)
    ]


def add_shared_tree(google: FakeGoogleAccounts) -> None:
    """The shared drive's contents as Drive holds them: a folder and a Doc at its root, a Doc
    and a Sheet inside the folder."""
    google.add_drive_folder(DRIVE_SHARED, "Team drive", drive_id=DRIVE_SHARED)
    google.add_drive_folder(DRIVE_SHARED_SUB, "Policies", parent=DRIVE_SHARED)
    google.add_drive_file(DRIVE_SHARED_DOC, "Handbook", MIME_DOC, DRIVE_SHARED, [])
    google.add_drive_file(
        DRIVE_SHARED_NESTED, "Leave policy", MIME_DOC, DRIVE_SHARED_SUB, [DriveSection("", "x")]
    )
    google.add_drive_file(NESTED_SHEET, "Nested roster", MIME_SHEET, DRIVE_SHARED_SUB, [])
    google.add_sheet_tab(NESTED_SHEET, "Staff", ["name"])


@pytest.mark.parametrize(
    ("folder", "listed", "granted", "up"),
    [
        (DRIVE_SHARED, ["Policies", "Handbook"], DRIVE_SHARED_DOC, ""),
        (
            DRIVE_SHARED_SUB,
            ["Leave policy", "Nested roster"],
            DRIVE_SHARED_NESTED,
            f"?folder={DRIVE_SHARED}",
        ),
    ],
    ids=["shared-drive-root", "folder"],
)
async def test_the_picker_opens_a_shared_drive_and_a_folder_and_grants_what_is_inside(
    folder, listed, granted, up
):
    """Fails if opening a shared drive or a folder lists anything but its own children, the
    page does not say where it is or link Up to the parent (Top for a drive's root), or
    ticking a nested Doc does not add a read grant."""
    h, google = await picker_harness()
    add_shared_tree(google)
    c = await h.signed_in(ROOT)
    async with using(c):
        top = (await c.get(VERSION + "/drive/pick")).text
        assert f"/drive/pick?folder={DRIVE_SHARED}" in top
        page = (await c.get(VERSION + f"/drive/pick?folder={folder}")).text
        assert all(name in page for name in listed)
        assert "Top" in page and "open in Drive" in page
        assert f'href="{VERSION}/drive/pick{up}">Up</a>' in page
        assert "Team folder" not in page
        resp = await post(
            c,
            VERSION + "/drive/pick/choose",
            csrf_in(page),
            folder=folder,
            pick=granted,
            request_id="r1",
        )
        assert resp.status_code == 303
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    assert [(g.id, g.kind) for g in record.agent().grants.drive] == [(granted, "file")]


async def test_an_item_added_to_a_folder_after_the_page_loaded_changes_no_tick():
    """Fails if a tick is taken by its position in the list, so an item added to the folder
    between the page and the post moves the grant to a different item."""
    h, google = await picker_harness()
    add_shared_tree(google)
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(VERSION + f"/drive/pick?folder={DRIVE_SHARED}")).text
        # A new folder sorts first, ahead of "Policies" and "Handbook".
        google.add_drive_folder("drive-archive-0001", "Archive", parent=DRIVE_SHARED)
        resp = await post(
            c,
            VERSION + "/drive/pick/choose",
            csrf_in(page),
            folder=DRIVE_SHARED,
            pick=DRIVE_SHARED_DOC,
            request_id="r1",
        )
        assert resp.status_code == 303
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    assert [(g.id, g.kind) for g in record.agent().grants.drive] == [(DRIVE_SHARED_DOC, "file")]


@pytest.mark.parametrize(
    ("sheet", "folder", "granted"),
    [(NESTED_SHEET, DRIVE_SHARED_SUB, True), (DRIVE_SHEET, DRIVE_SHARED_SUB, False)],
    ids=["in-the-posted-folder", "not-in-the-posted-folder"],
)
async def test_a_nested_sheet_asks_for_its_tab_and_only_an_id_in_the_posted_folder_is_taken(
    sheet, folder, granted
):
    """Fails if a Sheet inside a folder skips the tab step or is not granted with its tab,
    or the tab step accepts a Sheet ID that is not in the posted folder's listing."""
    h, google = await picker_harness()
    add_shared_tree(google)
    c = await h.signed_in(ROOT)
    async with using(c):
        pick = (await c.get(VERSION + f"/drive/pick?folder={folder}")).text
        resp = await post(
            c,
            VERSION + "/drive/pick/choose",
            csrf_in(pick),
            folder=folder,
            pick=NESTED_SHEET,
            request_id="r1",
        )
        tabs_url = resp.headers["location"]
        assert f"sheet={NESTED_SHEET}" in tabs_url and f"folder={folder}" in tabs_url
        tabs_page = (await c.get(tabs_url)).text
        assert "Staff" in tabs_page
        resp = await post(
            c,
            VERSION + "/drive/pick/tabs",
            csrf_in(tabs_page),
            folder=folder,
            sheet_id_0=sheet,
            tab_0="0",
            request_id="r2",
        )
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    if granted:
        assert resp.status_code == 303
        assert [(g.id, g.kind, g.tab) for g in record.agent().grants.drive] == [
            (NESTED_SHEET, "sheet", "Staff")
        ]
    else:
        assert resp.status_code == 409
        assert record.agent().grants.drive == []


async def test_a_folder_the_account_cannot_see_shows_google_drive_unavailable():
    """Fails if a folder Google refuses shows a stack trace or an empty list instead of the
    Google Drive unavailable page with the plain words about the account's access."""
    h, google = await picker_harness()
    add_shared_tree(google)
    google.drive_refused.add(DRIVE_SHARED_SUB)
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(VERSION + f"/drive/pick?folder={DRIVE_SHARED_SUB}")).text
    assert "Google Drive unavailable" in page
    assert "sharing with its address was removed" in page


@pytest.mark.parametrize(
    ("method", "path", "fields"),
    [
        ("GET", "/drive/pick?folder=../about", {}),
        ("GET", "/drive/pick/template?folder=x/permissions", {}),
        ("GET", "/drive/pick/template?template=short", {}),
        ("POST", "/drive/pick/choose", {"pick": "x' in parents or 'y"}),
        ("POST", "/drive/pick/choose", {"folder": "short"}),
        ("POST", "/drive/pick/template", {"template": DRIVE_TEMPLATE, "dest": "../about"}),
        ("POST", "/drive/pick/template", {"template": ""}),
        ("POST", "/drive/pick/section", {"doc": "x/permissions"}),
        ("POST", "/drive/pick/tabs", {"sheet_id_0": "x' in parents or 'y"}),
    ],
)
async def test_a_crafted_drive_id_is_refused_before_google_is_asked(method, path, fields):
    """Fails if an ID that is not shaped like a Drive ID (a path, a query fragment, too short,
    or empty where one is required) reaches a Google call instead of a Choose again page."""
    h, google = await picker_harness()
    add_template_tree(google)
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(VERSION + "/drive/pick")).text
        if method == "GET":
            resp = await c.get(VERSION + path)
        else:
            resp = await post(
                c, VERSION + path, csrf_in(page), request_id="r1", **{"folder": "", **fields}
            )
    assert "Choose again" in resp.text
    assert "That is not an item from the Drive list" in resp.text
    assert google.get_calls == [] and google.list_calls == []
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    assert record.agent().grants.drive == []
