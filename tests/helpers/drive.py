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
    raw["agents"]["it-helper"]["grants"]["drive"]["templates"] = [template_grant_block()]
    raw["agents"]["it-helper"]["grants"]["drive"]["sections"] = [section_grant_block()]
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


# The create-from-template target (D-49): a template Doc with two blanks, and a destination
# folder inside a shared drive. A plain folder outside any shared drive is the refusal case.
DRIVE_TEMPLATE = "drive-template-0001"
TEMPLATE_DRIVE = "drive-tmpl-shared-0001"
TEMPLATE_FOLDER = "drive-tmpl-folder-0001"
TEMPLATE_PLAIN_FOLDER = "drive-tmpl-plain-0001"
TEMPLATE_BLANKS = ["name", "role"]
TEMPLATE_TEXT = "Dear {{name}},\nYou are hired as {{role}}."


def template_grant_block() -> dict:
    """A Drive template grant naming the offer template, its folder, its published blanks
    and the shared drive the submit read for the folder."""
    return {
        "id": DRIVE_TEMPLATE,
        "name": "Offer template",
        "folder_id": TEMPLATE_FOLDER,
        "blanks": list(TEMPLATE_BLANKS),
        "drive_id": TEMPLATE_DRIVE,
    }


def add_template_tree(google: FakeGoogleAccounts) -> None:
    """A shared drive holding the destination folder and the template Doc with its blanks.
    The folder inherits the drive's driveId, as Google reports it; the plain folder sits in
    My Drive (no driveId) for the publish refusal."""
    google.add_drive_folder(TEMPLATE_DRIVE, "HR shared drive", drive_id=TEMPLATE_DRIVE)
    google.add_drive_folder(TEMPLATE_FOLDER, "Offer letters", parent=TEMPLATE_DRIVE)
    google.add_drive_file(
        DRIVE_TEMPLATE,
        "Offer template",
        MIME_DOC,
        TEMPLATE_DRIVE,
        [DriveSection("", TEMPLATE_TEXT)],
    )
    google.add_drive_folder(TEMPLATE_PLAIN_FOLDER, "My folder")


# The section-replace target (D-50): a granted Doc with a "Status" heading whose section
# spans a nested heading up to the next same-level heading ("Other"), which is kept.
DRIVE_SECTION = "drive-section-0001"
SECTION_HEADING = "Status"
SECTION_DOC_NAME = "VPN guide"
SECTION_PARAGRAPHS = [
    {"text": "Status\n", "style": "HEADING_1"},
    {"text": "The VPN is down.\n", "style": "NORMAL_TEXT"},
    {"text": "Steps\n", "style": "HEADING_2"},
    {"text": "Restart the router.\n", "style": "NORMAL_TEXT"},
    {"text": "Other\n", "style": "HEADING_1"},
    {"text": "Keep this.\n", "style": "NORMAL_TEXT"},
]
# The section's current text, as the draft freezes it (the paragraphs under the heading,
# trimmed): the card's "before".
SECTION_BEFORE = "The VPN is down.\nSteps\nRestart the router."


def section_grant_block() -> dict:
    """A Drive section grant naming the VPN guide's Status heading. The grant's ID is the
    Doc's file ID (granted as for reads); doc_id names the same Doc for the write."""
    return {
        "id": DRIVE_SECTION,
        "name": SECTION_DOC_NAME,
        "doc_id": DRIVE_SECTION,
        "heading": SECTION_HEADING,
    }


def add_section_doc(google: FakeGoogleAccounts, parent: str = DRIVE_FOLDER) -> None:
    """The VPN guide Doc as Google would serve it: a Drive file (indexed like any Doc) plus
    its structural model, revision 1."""
    google.add_drive_doc(
        DRIVE_SECTION, SECTION_DOC_NAME, [dict(p) for p in SECTION_PARAGRAPHS], parent
    )


def add_shared_drive_tree(google: FakeGoogleAccounts) -> None:
    """A shared drive with a Doc at its root and a sub-folder holding one more Doc."""
    google.add_drive_folder(DRIVE_SHARED, "Team drive", drive_id=DRIVE_SHARED)
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
