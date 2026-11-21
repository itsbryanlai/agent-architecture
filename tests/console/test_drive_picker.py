"""The Drive grant picker on the version page: the workspace's account, one click to grant."""

from __future__ import annotations

import pytest

from agentos import ids
from agentos.connectors.google_iam import MIME_DOC, MIME_SHEET, DriveItem, FakeGoogleAccounts
from agentos.drive_connections import InMemoryDriveConnectionStore
from tests.conftest import TENANT
from tests.helpers.agents import helper_block
from tests.helpers.console_harness import ROOT, ConsoleHarness, csrf_in, post, using
from tests.helpers.drive import (
    DRIVE_DOC,
    DRIVE_FOLDER,
    DRIVE_SHARED,
    DRIVE_SHEET,
    TEMPLATE_DRIVE,
    TEMPLATE_FOLDER,
)

VERSION = f"/ui/{TENANT}/agents/it-helper/versions/1"


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
            pick_0="yes",
            pick_2="yes",
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
            c, VERSION + "/drive/pick/choose", csrf_in(pick), pick_3="yes", request_id="r1"
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
    ("folder", "accepted"),
    [(TEMPLATE_FOLDER, True), (DRIVE_FOLDER, False)],
    ids=["shared-drive-folder", "plain-folder"],
)
async def test_the_template_picker_offers_only_shared_drive_folders(folder, accepted):
    """Fails if the template picker offers a folder outside a shared drive as a destination
    or refuses one inside a shared drive."""
    h, google = await picker_harness()
    google.shared[tenant_email()] = [
        DriveItem(
            DRIVE_DOC,
            "Guide",
            f"https://drive.google.com/file/d/{DRIVE_DOC}",
            "file",
            mime_type=MIME_DOC,
        ),
        DriveItem(
            TEMPLATE_FOLDER,
            "Offer letters",
            f"https://drive.google.com/drive/folders/{TEMPLATE_FOLDER}",
            "folder",
            drive_id=TEMPLATE_DRIVE,
        ),
        DriveItem(
            DRIVE_FOLDER,
            "Team folder",
            f"https://drive.google.com/drive/folders/{DRIVE_FOLDER}",
            "folder",
        ),
    ]
    c = await h.signed_in(ROOT)
    async with using(c):
        pick = (await c.get(VERSION + "/drive/pick/template")).text
        assert "Offer letters" in pick
        assert "Team folder" not in pick
        resp = await post(
            c,
            VERSION + "/drive/pick/template",
            csrf_in(pick),
            template=DRIVE_DOC,
            folder=folder,
            request_id="r1",
        )
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    if accepted:
        assert resp.status_code == 303
        assert [(g.id, g.kind, g.folder_id) for g in record.agent().grants.drive] == [
            (DRIVE_DOC, "template", TEMPLATE_FOLDER)
        ]
    else:
        assert resp.status_code == 409
        assert record.agent().grants.drive == []


@pytest.mark.parametrize(
    "case", ["from list", "typed", "already granted"], ids=["list", "typed", "already-granted"]
)
async def test_the_section_picker_asks_for_the_heading_and_adds_the_section_grant(case):
    """Fails if the section picker grants a Doc without asking for its heading, does not offer
    the Doc's headings from one read (or a typed name), adds the wrong heading, or accepts a
    Doc already granted as a read file instead of showing a plain message."""
    h, google = await picker_harness()
    google.shared[tenant_email()] = [
        DriveItem(
            DRIVE_DOC,
            "Guide",
            f"https://drive.google.com/file/d/{DRIVE_DOC}",
            "file",
            mime_type=MIME_DOC,
        )
    ]
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
        pick = (await c.get(VERSION + "/drive/pick/section")).text
        assert "Guide" in pick
        resp = await post(
            c, VERSION + "/drive/pick/section", csrf_in(pick), doc=DRIVE_DOC, request_id="r1"
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
        if case == "from list":
            resp = await post(
                c,
                VERSION + "/drive/pick/section",
                csrf_in(heading_page),
                doc=DRIVE_DOC,
                heading_pos="0",
                request_id="r2",
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
            )
            expected_heading = "Overview"
        assert resp.status_code == 303
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    assert [(g.id, g.kind, g.heading, g.doc_id) for g in record.agent().grants.drive] == [
        (DRIVE_DOC, "section", expected_heading, DRIVE_DOC)
    ]
