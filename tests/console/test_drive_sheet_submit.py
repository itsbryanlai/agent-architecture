"""Submitting a version with a Drive sheet grant reads the tab's header once and stores it
as the grant's columns, so the admin reviews the columns the agent will write (D-48)."""

import pytest

from agentos import ids
from agentos.config import ConfigError
from agentos.connectors.google_iam import MIME_SHEET, FakeGoogleAccounts
from agentos.drive_connections import InMemoryDriveConnectionStore
from tests.conftest import TENANT
from tests.helpers.agents import helper_block
from tests.helpers.console_harness import OWNER, ROOT, ConsoleHarness
from tests.helpers.drive import DRIVE_SHEET, SHEET_TAB

FOLDER = "sheet-folder-0001"


def sheet_harness():
    drive = InMemoryDriveConnectionStore()
    google = FakeGoogleAccounts()
    return ConsoleHarness(drive=drive, google=google), drive, google


def sheet_definition() -> dict:
    """A draft naming the Roster's Q1 tab, with no columns: the creator names none."""
    definition = helper_block()
    definition["grants"]["drive"] = {
        "sheets": [{"id": DRIVE_SHEET, "name": "Roster", "tab": SHEET_TAB}]
    }
    return definition


def serve_sheet(google: FakeGoogleAccounts, header: list[str]) -> None:
    google.add_drive_folder(FOLDER, "Team folder")
    google.add_drive_file(DRIVE_SHEET, "Roster", MIME_SHEET, FOLDER, [])
    google.add_sheet_tab(DRIVE_SHEET, SHEET_TAB, header, [["alice", "engineer"]])


@pytest.mark.parametrize("case", ["reads and stores", "no header row", "not connected"])
async def test_submit_reads_the_sheets_header_once_and_stores_the_columns(case):
    """Fails if submit does not read the tab's header and store it as the grant's columns,
    reads it more than once, or accepts a tab with no header row or an unconnected Drive."""
    h, _drive, google = sheet_harness()
    await h.seed_members()
    if case != "not connected":
        await h.plane.connect_drive(TENANT, ROOT)
    serve_sheet(google, ["name", "role"] if case != "no header row" else [])
    await h.plane.add_agent(TENANT, OWNER, "it-helper", sheet_definition())

    if case == "reads and stores":
        await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
        record = await h.agents.get_version(TENANT, "it-helper", 1)
        [sheet] = record.agent().grants.drive
        assert sheet.columns == ("name", "role")
        assert record.definition["grants"]["drive"]["sheets"][0]["columns"] == ["name", "role"]
        email = f"{ids.drive_account_id(TENANT)}@test-project.iam.gserviceaccount.com"
        assert google.sheet_read_calls == [(email, DRIVE_SHEET)]
    else:
        with pytest.raises(ConfigError) as err:
            await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
        assert ("no header row" if case == "no header row" else "not connected") in str(err.value)
        record = await h.agents.get_version(TENANT, "it-helper", 1)
        assert record.status == "draft"
        assert record.agent().grants.drive[0].columns == ()
