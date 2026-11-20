"""The Drive connector's grant boundary: a reference resolves only to a granted, live file."""

import pytest

from agentos.config import DriveGrant
from agentos.connectors.base import UnknownItem
from agentos.connectors.drive import DriveConnector, DriveReader
from agentos.connectors.google_iam import MIME_DOC, DriveSection, FakeGoogleAccounts

FILE = "drive-file-0001"


def make(granted: bool):
    google = FakeGoogleAccounts()
    google.add_drive_file(FILE, "Guide", MIME_DOC, "folder-1", [DriveSection("", "text")])
    grants = [DriveGrant(FILE, "Guide", "file")] if granted else []
    conn = DriveConnector(DriveReader(google, "agent@x.iam.gserviceaccount.com"), grants)
    return conn, google


@pytest.mark.parametrize(
    ("case", "resolves"),
    [
        ("granted file", True),
        ("ungranted file", False),
        ("missing file", False),
        ("trashed file", False),
    ],
)
async def test_resolve_item_resolves_only_a_granted_live_file(case, resolves):
    """Fails if resolve_item returns a file outside the grants, or reveals whether an
    ungranted, missing or trashed file exists instead of the same UnknownItem."""
    conn, google = make(granted=case != "ungranted file")
    if case == "missing file":
        ref = "drive-file-9999"
    elif case == "trashed file":
        google.trash_drive_file(FILE)
        ref = FILE
    else:
        ref = FILE
    if resolves:
        item = await conn.resolve_item(ref)
        assert (item.page_id, item.title) == (FILE, "Guide")
    else:
        with pytest.raises(UnknownItem):
            await conn.resolve_item(ref)
