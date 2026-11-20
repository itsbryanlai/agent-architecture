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
    """A definition with both Drive grant kinds, for the parse/serialise round trip."""
    raw = drive_agents_dict()
    raw["agents"]["it-helper"]["grants"]["drive"]["files"] = [{"id": DRIVE_DOC, "name": "Guide"}]
    return raw


async def run_drive_index(h, scope=DRIVE_SCOPE, root=DRIVE_FOLDER, suffix=""):
    from tests.conftest import TENANT

    return await h.client.execute_workflow(
        DriveIndexingWorkflow.run,
        args=[TENANT, scope, root],
        id=ids.drive_index_workflow_id(TENANT, scope) + suffix,
        task_queue=TASK_QUEUE,
    )
