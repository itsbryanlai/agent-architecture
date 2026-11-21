"""The Drive connector's grant boundary: a reference resolves only to a granted, live file
under a granted root, and the comment marker counts only the account's own comments."""

import pytest

from agentos.config import DriveGrant
from agentos.connectors.base import GrantDenied, UnknownItem
from agentos.connectors.drive import DriveConnector, DriveReader
from agentos.connectors.google_iam import MIME_DOC, DriveSection, FakeGoogleAccounts, GoogleError
from agentos.connectors.notion import comment_ref
from tests.helpers.drive import DRIVE_SHARED, DRIVE_SHARED_DOC, add_shared_drive_tree

FILE = "drive-file-0001"
FOLDER = "team-folder-0001"
ELSEWHERE = "elsewhere-folder-0001"
EMAIL = "agent@x.iam.gserviceaccount.com"


def make(granted: bool, kind: str = "file"):
    google = FakeGoogleAccounts()
    if kind == "shared_drive":
        add_shared_drive_tree(google)
        # A shared drive's root is not a file: files.get of it answers 404, as Google does.
        google.fail_get_for(DRIVE_SHARED, GoogleError("not_found", status=404))
        target, name = DRIVE_SHARED, "Team drive"
    else:
        google.add_drive_folder(FOLDER, "Team folder")
        google.add_drive_file(FILE, "Guide", MIME_DOC, FOLDER, [DriveSection("", "text")])
        target, name = (FILE, "Guide") if kind == "file" else (FOLDER, "Team folder")
    grants = [DriveGrant(target, name, kind)] if granted else []
    conn = DriveConnector(DriveReader(google, EMAIL), grants)
    return conn, google


@pytest.mark.parametrize(
    ("case", "resolves"),
    [
        ("granted file", True),
        ("ungranted file", False),
        ("missing file", False),
        ("trashed file", False),
        ("file in granted folder", True),
        ("file in granted shared drive", True),
        ("file moved out of granted folder", False),
        ("granted folder itself", False),
    ],
)
async def test_resolve_item_resolves_only_a_granted_live_file(case, resolves):
    """Fails if resolve_item returns a file outside the grants (including one moved out of
    a granted folder), a folder or drive root, fetches a granted shared drive's root (not a
    file) instead of matching it by ID, or reveals whether an ungranted, missing or trashed
    file exists instead of the same UnknownItem."""
    kind = "file"
    if case in (
        "file in granted folder",
        "file moved out of granted folder",
        "granted folder itself",
    ):
        kind = "folder"
    elif case == "file in granted shared drive":
        kind = "shared_drive"
    conn, google = make(granted=case != "ungranted file", kind=kind)
    ref, expected = FILE, (FILE, "Guide")
    if case == "file in granted shared drive":
        ref, expected = DRIVE_SHARED_DOC, (DRIVE_SHARED_DOC, "Shared guide")
    elif case == "missing file":
        ref = "drive-file-9999"
    elif case == "trashed file":
        google.trash_drive_file(FILE)
    elif case == "file moved out of granted folder":
        google.add_drive_folder(ELSEWHERE, "Elsewhere")
        google.move_drive_child(FOLDER, ELSEWHERE, FILE)
    elif case == "granted folder itself":
        ref = FOLDER
    if resolves:
        item = await conn.resolve_item(ref)
        assert (item.page_id, item.title) == expected
    else:
        with pytest.raises(UnknownItem):
            await conn.resolve_item(ref)


@pytest.mark.parametrize("which", ["another_author", "short_reference"])
async def test_find_write_counts_only_the_accounts_own_comment_with_the_full_reference(which):
    """Fails if someone else's comment, or a prefix of the reference, stops the comment."""
    conn, google = make(granted=True)
    idem_key = "acme:task:comment-1:comment"
    ref = comment_ref(idem_key)
    if which == "another_author":
        google.add_drive_comment(FILE, f"quoting {ref}", mine=False)
    else:
        google.add_drive_comment(FILE, f"quoting {ref[:12]}")

    assert await conn.find_write("add_comment", FILE, idem_key) is None
    made = await conn.write(
        "add_comment", FILE, {"comment": "Any update?"}, idem_key, agent_name="IT helper"
    )

    comments = google.drive_comments[FILE]
    assert len(comments) == 2
    assert comments[1]["id"] == made
    assert comments[1]["content"].startswith("Any update?")
    assert ref in comments[1]["content"]


@pytest.mark.parametrize(
    ("kind", "case", "ref", "expected"),
    [
        ("sheet", "unique name", "Roster", "drive-sheet-0001"),
        ("sheet", "shared name", "Roster", None),
        ("sheet", "ID of one of two", "drive-sheet-0002", "drive-sheet-0002"),
        ("template", "unique name", "Offer template", "drive-template-0001"),
        ("template", "shared name", "Offer template", None),
    ],
    ids=[
        "sheet-unique-name",
        "sheet-shared-name",
        "sheet-by-id",
        "template-unique-name",
        "template-shared-name",
    ],
)
def test_a_name_two_grants_share_is_refused_so_the_model_uses_the_id(kind, case, ref, expected):
    """Fails if sheet_for or template_for picks one of two granted Sheets or templates that
    share a name instead of refusing so the model uses the ID, or a unique name or an ID
    does not resolve."""
    google = FakeGoogleAccounts()
    if kind == "sheet":
        grants = [DriveGrant("drive-sheet-0001", "Roster", "sheet", tab="Q1")]
        if case != "unique name":
            grants.append(DriveGrant("drive-sheet-0002", "Roster", "sheet", tab="Q1"))
    else:
        grants = [
            DriveGrant(
                "drive-template-0001",
                "Offer template",
                "template",
                folder_id=FOLDER,
                blanks=("name",),
            )
        ]
        if case != "unique name":
            grants.append(
                DriveGrant(
                    "drive-template-0002",
                    "Offer template",
                    "template",
                    folder_id=FOLDER,
                    blanks=("name",),
                )
            )
    conn = DriveConnector(DriveReader(google, EMAIL), grants)
    lookup = conn.sheet_for if kind == "sheet" else conn.template_for

    if expected is None:
        with pytest.raises(GrantDenied) as err:
            lookup(ref)
        assert "ID" in str(err.value)
    else:
        assert lookup(ref).id == expected
