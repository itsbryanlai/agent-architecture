"""The Drive grant picker on the version page: the workspace's account, one click to grant."""

from __future__ import annotations

import pytest

from agentos import ids
from agentos.connectors.google_iam import DriveItem, FakeGoogleAccounts
from agentos.drive_connections import InMemoryDriveConnectionStore
from tests.conftest import TENANT
from tests.helpers.agents import helper_block
from tests.helpers.console_harness import ROOT, ConsoleHarness, csrf_in, post, using
from tests.helpers.drive import DRIVE_DOC, DRIVE_FOLDER, DRIVE_SHARED

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
        ]
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
