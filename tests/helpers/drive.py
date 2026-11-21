"""Helpers that build a fake Drive tree and run the Drive indexing workflow."""

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.connectors.google_iam import (
    MIME_DOC,
    MIME_SHEET,
    MIME_SLIDES,
    DriveSection,
    FakeGoogleAccounts,
)
from agentos.workflows.drive_indexing import DriveIndexingWorkflow

DRIVE_FOLDER = "drive-folder-0001"
DRIVE_DOC = "drive-doc-0001"
DRIVE_SHEET = "drive-sheet-0001"
DRIVE_SLIDES = "drive-slides-0001"
DRIVE_PDF = "drive-pdf-0001"
DRIVE_SUBFOLDER = "drive-subfolder-0001"
DRIVE_NESTED_DOC = "drive-nested-doc-0001"

DRIVE_SCOPE = ids.drive_scope(DRIVE_FOLDER)

DRIVE_SHARED = "drive-shared-0001"
DRIVE_SHARED_DOC = "drive-shared-doc-0001"
DRIVE_SHARED_SUB = "drive-shared-sub-0001"
DRIVE_SHARED_NESTED = "drive-shared-nested-0001"

SHARED_DRIVE_SCOPE = ids.drive_scope(DRIVE_SHARED)


def drive_account_email(google: FakeGoogleAccounts) -> str:
    """The tenant's account email, with the account registered in the fake."""
    from tests.conftest import TENANT

    account_id = ids.drive_account_id(TENANT)
    email = google.account_email(account_id)
    google.accounts[account_id] = email
    return email


def make_drive_google() -> FakeGoogleAccounts:
    """A folder with a Doc, a two-tab Sheet, a Slides, a non-exportable PDF, and a
    sub-folder holding one more Doc."""
    google = FakeGoogleAccounts()
    drive_account_email(google)
    google.add_drive_folder(DRIVE_FOLDER, "Team folder")
    google.add_drive_file(
        DRIVE_DOC,
        "Guide",
        MIME_DOC,
        DRIVE_FOLDER,
        [DriveSection("", "Reset your VPN password at the portal.")],
    )
    google.add_drive_file(
        DRIVE_SHEET,
        "Roster",
        MIME_SHEET,
        DRIVE_FOLDER,
        [
            DriveSection("Q1", "name,role\nalice,engineer\n"),
            DriveSection("Q2", "name,role\nbob,people\n"),
        ],
    )
    google.add_drive_file(
        DRIVE_SLIDES, "Deck", MIME_SLIDES, DRIVE_FOLDER, [DriveSection("", "Welcome to the team.")]
    )
    google.add_drive_file(DRIVE_PDF, "Scan", "application/pdf", DRIVE_FOLDER, [])
    google.add_drive_folder(DRIVE_SUBFOLDER, "Policies", parent=DRIVE_FOLDER)
    google.add_drive_file(
        DRIVE_NESTED_DOC,
        "Laptops",
        MIME_DOC,
        DRIVE_SUBFOLDER,
        [DriveSection("", "Laptops are replaced every three years.")],
    )
    return google


def drive_agents_dict() -> dict:
    """``it-helper`` reads the Drive folder (and keeps its Notion database grant)."""
    from tests.conftest import agents_dict

    raw = agents_dict()
    raw["agents"]["it-helper"]["grants"]["drive"] = {
        "folders": [{"id": DRIVE_FOLDER, "name": "Team folder"}]
    }
    return raw


def drive_grants_dict() -> dict:
    """A definition with every Drive grant kind, for the parse/serialise round trip."""
    raw = drive_agents_dict()
    raw["agents"]["it-helper"]["grants"]["drive"]["files"] = [{"id": DRIVE_DOC, "name": "Guide"}]
    raw["agents"]["it-helper"]["grants"]["drive"]["shared_drives"] = [
        {"id": DRIVE_SHARED, "name": "Team drive"}
    ]
    raw["agents"]["it-helper"]["grants"]["drive"]["sheets"] = [sheet_grant_block()]
    return raw


# The Roster sheet's append target: its tab and the header row a published definition froze.
SHEET_TAB = "Q1"
SHEET_COLUMNS = ["name", "role"]


def sheet_grant_block() -> dict:
    """A Drive sheet grant naming the Roster's Q1 tab and its published columns."""
    return {
        "id": DRIVE_SHEET,
        "name": "Roster",
        "tab": SHEET_TAB,
        "columns": list(SHEET_COLUMNS),
    }


def add_roster_sheet(google: FakeGoogleAccounts) -> int:
    """The Roster's Q1 tab as Google would serve it: a header row and one data row, so an
    append lands at row index 2. Returns the tab's numeric sheet ID."""
    return google.add_sheet_tab(DRIVE_SHEET, SHEET_TAB, SHEET_COLUMNS, [["alice", "engineer"]])


def add_shared_drive_tree(google: FakeGoogleAccounts) -> None:
    """A shared drive with a Doc at its root and a sub-folder holding one more Doc."""
    google.add_drive_folder(DRIVE_SHARED, "Team drive")
    google.add_drive_file(
        DRIVE_SHARED_DOC,
        "Shared guide",
        MIME_DOC,
        DRIVE_SHARED,
        [DriveSection("", "Shared drive text.")],
    )
    google.add_drive_folder(DRIVE_SHARED_SUB, "Policies", parent=DRIVE_SHARED)
    google.add_drive_file(
        DRIVE_SHARED_NESTED,
        "Shared policy",
        MIME_DOC,
        DRIVE_SHARED_SUB,
        [DriveSection("", "Nested shared text.")],
    )


def add_bulk_children(
    google: FakeGoogleAccounts, parent: str, count: int, prefix: str
) -> list[str]:
    """``count`` non-exportable files under ``parent``: the walk skips and counts them,
    so a wide tree costs discovery only, never an export or an embedding."""
    file_ids = [f"{prefix}-{i:04d}" for i in range(count)]
    for file_id in file_ids:
        google.add_drive_file(file_id, f"Bulk {file_id}", "application/pdf", parent, [])
    return file_ids


async def run_drive_index(h, scope=DRIVE_SCOPE, root=DRIVE_FOLDER, suffix="", confirm_gone=False):
    """One DriveIndexingWorkflow run, with its own workflow ID as each nightly run has."""
    from tests.conftest import TENANT

    return await h.client.execute_workflow(
        DriveIndexingWorkflow.run,
        args=[
            TENANT,
            scope,
            root,
            None,
            None,
            None,
            0,
            0,
            0,
            0,
            0,
            None,
            False,
            True,
            False,
            None,
            None,
            confirm_gone,
        ],
        id=ids.drive_index_workflow_id(TENANT, scope) + suffix,
        task_queue=TASK_QUEUE,
    )
