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


def section_conn(paragraphs: list[dict], heading: str = "Status", marker: str = ""):
    """A connector granted the VPN guide's Status section, and the Doc as Google serves it."""
    google = FakeGoogleAccounts()
    google.add_drive_folder(FOLDER, "Team folder")
    google.add_drive_doc(SECTION_DOC, "VPN guide", paragraphs, FOLDER)
    grant = DriveGrant(
        SECTION_DOC, "VPN guide", "section", doc_id=SECTION_DOC, heading=heading, marker=marker
    )
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
        "image refusal",
        "inherited bold",
    ],
)
async def test_the_section_write_inserts_first_and_keeps_the_text_plain(case):
    """Fails if the replace does not insert the new text first and delete the shifted old
    range keeping the heading and the body's final newline (a section ending at the body end,
    an empty section, a heading that is the Doc's last paragraph, a section spanning a
    lower-level heading), if the new paragraphs do not stay NORMAL_TEXT, or if a section
    holding an inline image is accepted instead of refused as not editable."""
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
        "inherited bold": (
            [_bold("Status\n"), {"text": "\n", "style": "NORMAL_TEXT", "bold": True}, _bold("N\n")],
            "New text.",
            [("Status\n", "NORMAL_TEXT"), ("New text.\n", "NORMAL_TEXT"), ("N\n", "NORMAL_TEXT")],
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
    marker = "NORMAL_TEXT:bold" if case == "inherited bold" else ""
    conn, google, grant = section_conn(paragraphs, marker=marker)

    if expected is None:
        with pytest.raises(ConnectorError) as err:
            await conn.read_section(grant)
        assert "text, lines and tables" in str(err.value)
        assert google.doc_replace_calls == []
        return

    section = await conn.read_section(grant)
    payload = {
        "heading": "Status",
        "text": new_text,
        "revision_id": section["revision_id"],
    }
    assert await conn.replace_section(SECTION_DOC, payload, "idem") == SECTION_DOC
    assert [(p["text"], p["style"]) for p in google.docs[SECTION_DOC]["paragraphs"]] == expected
    assert len(google.doc_replace_calls) == 1
    # The new lines never take the marker's look, so a retry still finds the section as written.
    assert await conn.find_replaced_section(SECTION_DOC, payload) == SECTION_DOC


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
    }

    assert await conn.replace_section(SECTION_DOC, payload, "idem") == SECTION_DOC
    assert google.docs[SECTION_DOC]["revision_id"] == "moved-9"
    assert google.docs[SECTION_DOC]["paragraphs"][1]["text"] == "New text.\n"
    assert google.doc_replace_calls == []


def _bold(text: str) -> dict:
    return {"text": text, "style": "NORMAL_TEXT", "bold": True}


def _rule() -> dict:
    return {"text": "\n", "style": "NORMAL_TEXT", "rule": True}


def _table(*rows: list[str]) -> dict:
    return {"kind": "table", "rows": [list(row) for row in rows]}


def _shape(google) -> list:
    """The Doc's body as a reader sees it: text lines, ``---`` for a line, a table's rows."""
    return [
        p["rows"] if p.get("kind") == "table" else "---" if p.get("rule") else p["text"]
        for p in google.docs[SECTION_DOC]["paragraphs"]
    ]


@pytest.mark.parametrize(
    ("paragraphs", "heading", "marker", "outcome"),
    [
        (
            [_p("Status\n", "HEADING_1"), _p("A.\n"), _p("Other\n", "HEADING_1"), _p("B.\n")],
            "Status",
            "HEADING_1",
            "A.",
        ),
        (
            [_bold("Status\n"), _p("A.\n"), _p("Big\n", "HEADING_1"), _bold("Next\n"), _p("B.\n")],
            "Status",
            "NORMAL_TEXT:bold",
            "A.",
        ),
        (
            [_bold("Status\n"), _p("TBD\n"), _p("A.\n"), _bold("Next\n"), _p("B.\n")],
            "Status",
            "NORMAL_TEXT:bold",
            "TBD\nA.",
        ),
        (
            [
                _bold("Status\n"),
                _p("A.\n"),
                {"text": "Sub\n", "style": "NORMAL_TEXT", "bold": True, "underline": True},
                _p("B.\n"),
            ],
            "Status",
            "NORMAL_TEXT:bold",
            "A.",
        ),
        (
            [
                {"text": "Status\n", "style": "NORMAL_TEXT", "underline": True},
                _p("A.\n"),
                {"text": "Next\n", "style": "NORMAL_TEXT", "underline": True},
            ],
            "Status",
            "NORMAL_TEXT:underline",
            "A.",
        ),
        ([_p("STATUS\n"), _p("a line\n")], "STATUS", "NORMAL_TEXT:caps", "changed style"),
        ([_p("Status\n"), _p("A.\n")], "Status", "", "can't find the heading"),
        ([_bold("Status\n"), _p("A.\n")], "Status", "", "can't find the heading"),
        ([_bold("Status\n"), _p("A.\n")], "Status", "HEADING_1", "changed style"),
    ],
    ids=[
        "heading",
        "bold",
        "caps content line",
        "superset ends",
        "underline",
        "caps alone",
        "plain line",
        "legacy grant",
        "restyled",
    ],
)
async def test_a_section_starts_at_the_marker_style_the_grant_recorded(
    paragraphs, heading, marker, outcome
):
    """Fails if a section starts at a line that is not in the style recorded at submit (a plain
    line, a capitals-only line, a bold line under a heading-only legacy grant), runs past a
    heading, a line in the marker's style or one with more styles, ends at a capitals content
    line like TBD, or a line restyled since publishing is silently treated as missing."""
    conn, _, grant = section_conn(paragraphs, heading, marker)

    if outcome in ("can't find the heading", "changed style"):
        with pytest.raises(ConnectorError, match=outcome):
            await conn.read_section(grant)
    else:
        assert (await conn.read_section(grant))["before"] == outcome


@pytest.mark.parametrize(
    ("body", "text", "after"),
    [
        (
            "line",
            "New intro.\n---\nNew outro.",
            ["Status\n", "New intro.\n", "---", "New outro.\n", "Other\n"],
        ),
        ("line", "---\nOnly outro.", ["Status\n", "---", "Only outro.\n", "Other\n"]),
        ("line", "New intro.\n---\n---\nNew outro.", "Keep the 1 horizontal line in 'Status'"),
        ("line", "Just text.", "Keep the 1 horizontal line in 'Status'"),
        ("none", "Intro.\n---\nOutro.", "'Status' has no horizontal lines"),
        ("line", "Intro.\nStatus\n---\nOutro.", "reads 'Status'"),
    ],
    ids=[
        "both parts",
        "first part emptied",
        "a line added",
        "the line dropped",
        "a line where none is",
        "the marker's own text",
    ],
)
async def test_a_section_keeps_its_horizontal_lines_and_replaces_the_text_between(
    body, text, after
):
    """Fails if a write recreates, moves or drops a horizontal line, writes text that does not
    carry the section's lines (or carries a --- line a section without lines cannot have, or a
    line that reads like the marker) before the card, or leaves a part between two lines
    unwritten."""
    paragraphs = [
        _p("Status\n", "HEADING_1"),
        _p("Intro.\n"),
        *([_rule(), _p("Outro.\n")] if body == "line" else []),
        _p("Other\n", "HEADING_1"),
    ]
    conn, google, grant = section_conn(paragraphs, marker="HEADING_1")

    if isinstance(after, str):
        with pytest.raises(ConnectorError, match=after):
            await conn.read_section(grant, text)
        assert google.doc_replace_calls == []
        return

    section = await conn.read_section(grant, text)
    assert section["before"] == "Intro.\n---\nOutro."
    payload = {"heading": "Status", "text": text, "revision_id": section["revision_id"]}
    await conn.replace_section(SECTION_DOC, payload, "idem")
    assert _shape(google) == after


@pytest.mark.parametrize(
    ("body", "text", "outcome"),
    [
        (
            "table",
            "Intro.\n| Item | Qty |\n| Pencils | 12 |\nDone.",
            ["Status\n", "Intro.\n", [["Item", "Qty"], ["Pencils", "12"]], "Done.\n", "Other\n"],
        ),
        (
            "table",
            "Better intro.\nSecond line.\n| Item | Qty |\n| Pens | 2 |\nAfter.",
            [
                "Status\n",
                "Better intro.\n",
                "Second line.\n",
                [["Item", "Qty"], ["Pens", "2"]],
                "After.\n",
                "Other\n",
            ],
        ),
        ("table", "Intro.\n| Item | Qty |\n| Pens | 2 |\n| Ink | 1 |\nAfter.", "2 rows and 2"),
        ("table", "Intro.\n| Item | Qty |\nAfter.", "2 rows and 2"),
        (
            "image cell",
            "Intro.\n| Item | Qty |\n| Pens | 2 |\nAfter.",
            "a table cell with an image",
        ),
        ("no table", "Intro.\n| Item | Qty |\nAfter.", "'Status' has no table"),
    ],
    ids=[
        "cells changed",
        "text before the table changed",
        "row added",
        "row removed",
        "image in a cell",
        "a table where none is",
    ],
)
async def test_a_table_is_edited_cell_by_cell_and_never_reshaped(body, text, outcome):
    """Fails if a table's cells are written at shifted positions, the text before a table is
    written over the newline Google refuses to delete, a table that gains or loses a row, or one
    holding an image, is written, a pipe row is accepted where the section has no table, or the
    refusal does not say what to keep."""
    table = _table(["Item", "Qty"], ["Pens", "2"])
    if body == "image cell":
        table["image_cells"] = [(1, 0)]
    paragraphs = [
        _p("Status\n", "HEADING_1"),
        _p("Intro.\n"),
        *([table] if body != "no table" else []),
        _p("After.\n"),
        _p("Other\n", "HEADING_1"),
    ]
    conn, google, grant = section_conn(paragraphs, marker="HEADING_1")

    if isinstance(outcome, str):
        with pytest.raises(ConnectorError, match=outcome):
            await conn.read_section(grant, text)
        assert google.doc_replace_calls == []
        return

    section = await conn.read_section(grant, text)
    assert section["before"] == "Intro.\n| Item | Qty |\n| Pens | 2 |\nAfter."
    payload = {"heading": "Status", "text": text, "revision_id": section["revision_id"]}
    await conn.replace_section(SECTION_DOC, payload, "idem")
    assert _shape(google) == outcome
