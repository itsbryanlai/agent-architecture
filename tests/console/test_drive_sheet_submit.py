"""Submitting a version with a Drive sheet grant reads the tab's header once and stores it
as the grant's columns, so the admin reviews the columns the agent will write (D-48)."""

import asyncio
import copy

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
    google.add_sheet_tab(DRIVE_SHEET, SHEET_TAB, header, [["alice", "engineer"]] if header else [])


@pytest.mark.parametrize(
    "case",
    [
        "reads and stores",
        "no header row",
        "not connected",
        "duplicate columns",
        "edit while reading",
    ],
    ids=["stores", "no-header", "not-connected", "duplicates", "concurrent-edit"],
)
async def test_submit_reads_the_sheets_header_once_and_stores_the_columns(case):
    """Fails if submit does not read the tab's header once and store it as the grant's
    columns, accepts a tab with no header row, a duplicate column name or an unconnected
    Drive, or drops an edit made while the header read was in flight."""
    h, _drive, google = sheet_harness()
    await h.seed_members()
    if case != "not connected":
        await h.plane.connect_drive(TENANT, ROOT)
    header = (
        []
        if case == "no header row"
        else ["name", "name"]
        if case == "duplicate columns"
        else ["name", "role"]
    )
    serve_sheet(google, header)
    await h.plane.add_agent(TENANT, OWNER, "it-helper", sheet_definition())

    if case == "edit while reading":
        hang = google.hang_next_sheet_read()
        submit = asyncio.create_task(h.plane.submit_version(TENANT, OWNER, "it-helper", 1))
        await hang.reached.wait()
        record = await h.agents.get_version(TENANT, "it-helper", 1)
        definition = copy.deepcopy(record.definition)
        definition["system_prompt"] = "Edited while the header read was in flight."
        await h.plane.edit_draft(TENANT, OWNER, "it-helper", 1, definition)
        hang.release()
        await submit
        record = await h.agents.get_version(TENANT, "it-helper", 1)
        assert record.status == "pending"
        assert record.definition["system_prompt"] == "Edited while the header read was in flight."
        assert record.agent().grants.drive[0].columns == ("name", "role")
    elif case == "reads and stores":
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
        expected = {
            "no header row": "no header row",
            "not connected": "not connected",
            "duplicate columns": "duplicate column name",
        }[case]
        assert expected in str(err.value)
        record = await h.agents.get_version(TENANT, "it-helper", 1)
        assert record.status == "draft"
        assert record.agent().grants.drive[0].columns == ()


@pytest.mark.parametrize(
    ("grid", "expected"),
    [
        ([["name", "role"], ["alice", "eng"]], (("name", "role"), 1, "A")),
        (
            [["Q1 roster"], [], ["", "", "name", "role", "team"], ["", "", "alice"]],
            (("name", "role", "team"), 3, "C"),
        ),
        ([["", "name", "", "role"]], "has an empty column; give every column a name"),
        ([["only title"], [], ["", "one"]], "has no header row"),
        ([[] for _ in range(20)] + [["name", "role"]], "has no header row"),
    ],
    ids=["row-one", "title-above-column-c", "inner-blank", "single-cells", "beyond-row-20"],
)
async def test_submit_finds_the_header_wherever_it_starts_and_records_where(grid, expected):
    """Fails if submit misses a header below a title or right of column A, accepts a header
    with an empty column, looks past the first 20 rows, or forgets where the header starts."""
    h, _drive, google = sheet_harness()
    await h.seed_members()
    await h.plane.connect_drive(TENANT, ROOT)
    serve_sheet(google, [])
    google.set_sheet_rows(DRIVE_SHEET, SHEET_TAB, grid)
    await h.plane.add_agent(TENANT, OWNER, "it-helper", sheet_definition())

    if isinstance(expected, str):
        with pytest.raises(ConfigError) as err:
            await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
        assert expected in str(err.value)
        return
    await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    [sheet] = record.agent().grants.drive
    assert (sheet.columns, sheet.header_row, sheet.first_column) == expected
