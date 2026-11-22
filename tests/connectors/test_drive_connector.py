"""The Drive connector's grant boundary: a reference resolves only to a granted, live file
under a granted root, and the comment marker counts only the account's own comments."""

import pytest

from agentos.config import DriveGrant
from agentos.connectors.base import ConnectorError, GrantDenied, UnknownItem
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
        ("section", "unique name", "VPN guide", "drive-section-0001"),
        ("section", "shared name", "VPN guide", None),
        ("section", "file grant by ID", "drive-file-0001", None),
    ],
    ids=[
        "sheet-unique-name",
        "sheet-shared-name",
        "sheet-by-id",
        "template-unique-name",
        "template-shared-name",
        "section-unique-name",
        "section-shared-name",
        "section-file-grant-by-id",
    ],
)
def test_a_name_two_grants_share_is_refused_so_the_model_uses_the_id(kind, case, ref, expected):
    """Fails if sheet_for, template_for or section_for picks one of two granted Sheets,
    templates or sections that share a name instead of refusing so the model uses the ID, or
    a unique name or an ID does not resolve, or section_for accepts a read-only file grant by
    ID instead of refusing it (only a section grant replaces a section)."""
    google = FakeGoogleAccounts()
    if kind == "sheet":
        grants = [DriveGrant("drive-sheet-0001", "Roster", "sheet", tab="Q1")]
        if case != "unique name":
            grants.append(DriveGrant("drive-sheet-0002", "Roster", "sheet", tab="Q1"))
    elif kind == "template":
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
    elif case == "file grant by ID":
        # A read-only file grant is not a section grant: section_for refuses it by ID.
        grants = [DriveGrant("drive-file-0001", "Guide", "file")]
    else:
        grants = [
            DriveGrant(
                "drive-section-0001",
                "VPN guide",
                "section",
                doc_id="drive-section-0001",
                heading="Status",
            )
        ]
        if case != "unique name":
            grants.append(
                DriveGrant(
                    "drive-section-0002",
                    "VPN guide",
                    "section",
                    doc_id="drive-section-0002",
                    heading="Status",
                )
            )
    conn = DriveConnector(DriveReader(google, EMAIL), grants)
    lookup = {"sheet": conn.sheet_for, "template": conn.template_for, "section": conn.section_for}[
        kind
    ]

    if expected is None:
        with pytest.raises(GrantDenied) as err:
            lookup(ref)
        assert "ID" in str(err.value)
    else:
        assert lookup(ref).id == expected


# --- the section write sequence and the plain-paragraph refusals (D-50) ---------------------

SECTION_DOC = "drive-section-0001"


def section_conn(paragraphs: list[dict]):
    """A connector granted the VPN guide's Status section, and the Doc as Google serves it."""
    google = FakeGoogleAccounts()
    google.add_drive_folder(FOLDER, "Team folder")
    google.add_drive_doc(SECTION_DOC, "VPN guide", paragraphs, FOLDER)
    grant = DriveGrant(SECTION_DOC, "VPN guide", "section", doc_id=SECTION_DOC, heading="Status")
    return DriveConnector(DriveReader(google, EMAIL), [grant]), google, grant


def _p(text: str, style: str = "NORMAL_TEXT") -> dict:
    return {"text": text, "style": style}


@pytest.mark.parametrize(
    "case",
    [
        "last section",
        "empty section",
        "heading last",
        "lower-level heading",
        "style stays NORMAL_TEXT",
        "table refusal",
        "image refusal",
    ],
)
async def test_the_section_write_inserts_first_and_keeps_the_text_plain(case):
    """Fails if the replace does not insert the new text first and delete the shifted old
    range keeping the heading and the body's final newline (a section ending at the body end,
    an empty section, a heading that is the Doc's last paragraph, a section spanning a
    lower-level heading), if the new paragraphs do not stay NORMAL_TEXT, or if a section
    holding a table or an inline image is accepted instead of refused as not plain paragraphs."""
    cases = {
        "last section": (
            [_p("Status\n", "HEADING_1"), _p("Old text.\n")],
            "New text.",
            [("Status\n", "HEADING_1"), ("New text.\n", "NORMAL_TEXT")],
        ),
        "empty section": (
            [_p("Status\n", "HEADING_1"), _p("Other\n", "HEADING_1"), _p("Keep.\n")],
            "New text.",
            [
                ("Status\n", "HEADING_1"),
                ("New text.\n", "NORMAL_TEXT"),
                ("Other\n", "HEADING_1"),
                ("Keep.\n", "NORMAL_TEXT"),
            ],
        ),
        "heading last": (
            [_p("Status\n", "HEADING_1")],
            "New text.",
            [("Status\n", "HEADING_1"), ("New text.\n", "NORMAL_TEXT")],
        ),
        "lower-level heading": (
            [
                _p("Status\n", "HEADING_1"),
                _p("The VPN is down.\n"),
                _p("Steps\n", "HEADING_2"),
                _p("Restart the router.\n"),
                _p("Other\n", "HEADING_1"),
                _p("Keep this.\n"),
            ],
            "The VPN is back up.",
            [
                ("Status\n", "HEADING_1"),
                ("The VPN is back up.\n", "NORMAL_TEXT"),
                ("Other\n", "HEADING_1"),
                ("Keep this.\n", "NORMAL_TEXT"),
            ],
        ),
        "style stays NORMAL_TEXT": (
            [_p("Status\n", "HEADING_1"), _p("Old.\n"), _p("Other\n", "HEADING_1")],
            "Line one.\nLine two.",
            [
                ("Status\n", "HEADING_1"),
                ("Line one.\n", "NORMAL_TEXT"),
                ("Line two.\n", "NORMAL_TEXT"),
                ("Other\n", "HEADING_1"),
            ],
        ),
        "table refusal": (
            [
                _p("Status\n", "HEADING_1"),
                _p("Intro.\n"),
                {"kind": "table", "size": 5},
                _p("More.\n"),
                _p("Other\n", "HEADING_1"),
            ],
            "New text.",
            None,
        ),
        "image refusal": (
            [
                _p("Status\n", "HEADING_1"),
                {"text": "Caption.\n", "style": "NORMAL_TEXT", "image": True},
                _p("Other\n", "HEADING_1"),
            ],
            "New text.",
            None,
        ),
    }
    paragraphs, new_text, expected = cases[case]
    conn, google, grant = section_conn(paragraphs)

    if expected is None:
        with pytest.raises(ConnectorError) as err:
            await conn.read_section(grant)
        assert "plain paragraphs" in str(err.value)
        assert google.doc_replace_calls == []
        return

    section = await conn.read_section(grant)
    payload = {
        "heading": "Status",
        "text": new_text,
        "revision_id": section["revision_id"],
        "content_start": section["content_start"],
        "content_end": section["content_end"],
    }
    assert await conn.replace_section(SECTION_DOC, payload, "idem") == SECTION_DOC
    assert [(p["text"], p["style"]) for p in google.docs[SECTION_DOC]["paragraphs"]] == expected
    assert len(google.doc_replace_calls) == 1


async def test_a_moved_doc_whose_section_already_holds_the_approved_text_is_done():
    """Fails if a replace whose revision lock refuses (the Doc moved since the read) but whose
    section already holds the approved text writes again or refuses, instead of reading the
    section and returning done with nothing written."""
    conn, google, grant = section_conn(
        [_p("Status\n", "HEADING_1"), _p("Old text.\n"), _p("Other\n", "HEADING_1"), _p("Keep.\n")]
    )
    section = await conn.read_section(grant)
    # An earlier attempt landed: the section holds the approved text and the revision moved on.
    google.docs[SECTION_DOC]["paragraphs"][1]["text"] = "New text.\n"
    google.set_doc_revision(SECTION_DOC, "moved-9")
    payload = {
        "heading": "Status",
        "text": "New text.",
        "revision_id": section["revision_id"],
        "content_start": section["content_start"],
        "content_end": section["content_end"],
    }

    assert await conn.replace_section(SECTION_DOC, payload, "idem") == SECTION_DOC
    assert google.docs[SECTION_DOC]["revision_id"] == "moved-9"
    assert google.docs[SECTION_DOC]["paragraphs"][1]["text"] == "New text.\n"
    assert len(google.doc_replace_calls) == 1
