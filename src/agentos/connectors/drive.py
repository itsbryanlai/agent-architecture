"""The Google Drive connector: the folders and files a tenant was granted, read and written.

Drive calls run as the tenant's own service account (the address its files are shared
with), through the deployment's ``GoogleAccounts`` minting a short-lived token. A
``DriveReader`` binds that account's email to the deployment client, so every call acts
with the tenant's access, never another tenant's and never the asker's.

Knowledge search reads the indexed Drive scopes; the model's first Drive tool is
``comment_on_file`` (D-47), a comment task like Notion's ``add_comment``: the comment
ends with the key's reference, and the marker lookup scans the file's comments for it
before a retry posts again. The second is ``append_row`` (D-48), an append task on a
granted Sheet: the appended row carries developer metadata holding the key's hash, the
marker lookup searches for it before a retry appends again, and the row and its marker
land in one atomic batchUpdate. The write refuses when the draft's columns no longer match
the live grant's (a republish since the draft) or the tab's header no longer matches the
columns the definition froze at publish. The third is ``create_doc_from_template`` (D-49),
a create task on a granted template: the copied Doc carries an appProperty holding the key's
hash, the marker lookup searches the destination folder for it before a retry copies again,
and the copy carries its marker in the same request while the fill replaces every raw blank
form with a sentinel and each sentinel with its value in one batchUpdate, so values land
literally and a retry fills only what is left; a template with no blanks needs no fill. A
value or title carrying a blank's form or the sentinel prefix is refused at draft and edit,
so the passes never splice a value. The write refuses when the draft's blanks or folder no
longer match the live grant's, or the destination folder's live shared drive is not the one
the definition recorded at submit. The fourth is ``replace_section`` (D-50), a replace task on
a granted Doc section: the draft reads the heading's current text and the Doc's revisionId,
and the write replaces the content under the heading (keeping the heading) in one Docs
batchUpdate locked to that revision by ``writeControl.requiredRevisionId``. The revision is
the idempotency lock — a retry after a lost response, or a Doc someone else moved, gets a
revision mismatch, then the section's text is compared with the approved text: equal means the
earlier attempt landed, different refuses with nothing written. The write refuses when the
draft's heading no longer matches the live grant's, or the heading is gone or duplicated.
``resolve_item`` walks a file's parents up to
a granted root — the same walk the search re-check makes — so a file inside a granted
folder or shared drive resolves, and one moved out of it does not; the granted root is
matched by ID and never fetched, as a shared drive's root is not a file. A template grant
is copy-only: the template itself does not resolve as a comment target through it. The connector
brings the Drive error table so a failure is classified like every other connector's.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass

from agentos.config import DRIVE_ID, DriveGrant, Grants
from agentos.connectors import drive_errors
from agentos.connectors.base import MAX_WALK_DEPTH, ConnectorError, GrantDenied, UnknownItem
from agentos.connectors.google_iam import (
    MIME_DOC,
    MIME_FOLDER,
    DocEdit,
    DocParagraph,
    DocRead,
    DocTable,
    DriveComment,
    DriveFile,
    DriveSection,
    GoogleAccounts,
    GoogleError,
    SheetMetadata,
    SheetTab,
    utf16_len,
)
from agentos.connectors.manifest import MANIFEST_DIR, Manifest, ToolSpec, load_manifest
from agentos.connectors.notion import comment_ref
from agentos.models import SIGNATURE_NAME_MAX, NotionItem

# A Drive file ID in a link: /file/d/<id>, /document/d/<id>, /spreadsheets/d/<id>,
# /presentation/d/<id>, or an open?id=<id> query. IDs are letters, digits, - and _.
_LINK_ID = re.compile(r"/(?:file|document|spreadsheets|presentation)/d/([A-Za-z0-9_-]+)")
_QUERY_ID = re.compile(r"[?&]id=([A-Za-z0-9_-]+)")

# The one answer for a reference that names no granted, live file — never whether it exists.
_UNKNOWN_FILE = "I couldn't find a Google Drive file in that link or ID."

# The developer metadata key every appended row carries (D-48). Its value is the write's
# idempotency key hash, so a retry finds the row an earlier attempt appended.
SHEET_METADATA_KEY = "agentos-write"
# The refusal when the live header no longer matches the columns the definition froze.
HEADER_CHANGED_TEXT = (
    "The sheet's columns changed since this agent was published; ask the creator to republish"
)
# The appProperty key every Doc copied from a template carries (D-49). Its value is the
# write's idempotency key hash, so a retry finds the copy an earlier attempt made.
DOC_APP_PROPERTY_KEY = "agentos_write"
# The refusal when the live grant's blanks or folder no longer match the draft's, or the
# destination folder's live shared drive is not the one the definition recorded.
DOC_SHAPE_CHANGED_TEXT = (
    "The template's blanks or folder changed since this agent was published; "
    "ask the creator to republish"
)
# The refusal when the destination folder sits in no shared drive: the same plain words
# the publish refuses with, so a folder moved to My Drive reads the same everywhere.
SHARED_DRIVE_FOLDER_TEXT = (
    "New Docs can only be created in a shared drive folder; pick a folder inside a shared drive."
)
# The refusal for a value or title carrying the fill's markup (D-49): the sentinel prefix
# or a raw form of one of the template's blanks. Such a value would be spliced by the
# fill's passes or read as an unfilled blank, so the draft and the edit refuse it.
FILL_MARKUP_TEXT = "A value can't contain a template blank like {{name}}; write it another way."
# The refusal when the live grant's heading no longer matches the draft's (a republish
# between the draft and the approval), as the append's and the create's shape refusals (D-50).
SECTION_SHAPE_CHANGED_TEXT = (
    "The section's heading changed since this agent was published; ask the creator to republish"
)
# The refusal when the Doc moved since the read revision and the section's text is not the
# approved text: nothing is written, the requester is told to draft again (D-50).
DOC_MOVED_TEXT = "The document changed since you approved; I'll draft it again"
# The longest current section text a replace drafts: a longer section is refused at draft so
# the card never previews a body it cannot show whole (D-50).
MAX_SECTION_BEFORE = 5000
# The refusal for a section holding something the replace cannot write: an inline image, a
# page or section break, or a table cell of several lines. Text, horizontal lines and table
# cells are written; the rest is left to the owner (D-50).
SECTION_NOT_PLAIN_TEXT = (
    "I can only replace a section made of text, lines and tables; "
    "'{doc}' has an image or other content under '{heading}' that I can't edit"
)
# The refusal for a section holding a paragraph that reads like a divider or a table row: the
# model's view of a section marks lines with --- and tables with pipe rows, so such text would
# make the view ambiguous. Refused at submit and at read, never guessed at (D-50).
SECTION_AMBIGUOUS_TEXT = (
    "'{doc}' has a line that reads like a divider or table row under '{heading}'"
)
# The refusal for new text that does not keep the section's horizontal lines: the Docs API
# cannot insert one, so each existing line stays and the model writes --- where it is (D-50).
SECTION_RULES_TEXT = "Keep the {count} horizontal {lines} in '{heading}': write --- where {each} is"
# The refusal for new text that does not keep a table's rows and columns (D-50).
SECTION_TABLE_TEXT = "Keep the table in '{heading}' at {rows} rows and {columns} columns"
# The refusal for new text under a heading that sits right before a table with no line of
# text between them: Google refuses text at a table's first index (D-50).
SECTION_BEFORE_TABLE_TEXT = (
    "I can't add text right above the table in '{heading}'; ask the owner to add a line there"
)
# The refusal for a section whose current text is longer than the draft bound (D-50).
SECTION_TOO_LONG_TEXT = (
    "The section under '{heading}' in '{doc}' is too long to replace "
    "(over {limit} characters); pick a smaller section"
)
# The refusal for new text carrying a character Google Docs strips (a control character or a
# private-use code point): it would not land literally, so the draft and the edit refuse it.
SECTION_CONTROL_CHAR_TEXT = (
    "The new text has a character Google Docs removes (a control or private-use character); "
    "write it without that character."
)
# Characters Google Docs strips from inserted text: U+0000-U+0008 and U+000C-U+001F (newline
# U+000A and tab U+0009 are kept), and the private-use area U+E000-U+F8FF.
_STRIPPED_CHAR = re.compile("[\x00-\x08\x0c-\x1f\ue000-\uf8ff]")
# A template blank: ``{{field name}}``. The name is the inner text, trimmed; duplicate
# names collapse to one. The fill replaces every raw form found for a name, spaced or not.
_BLANK = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")


def heading_missing_text(heading: str, doc_name: str) -> str:
    """The plain words for a heading the Doc no longer has (renamed or removed): the
    creator updates the agent (D-50)."""
    return (
        f"I can't find the heading '{heading}' in {doc_name}; ask the creator to update the agent"
    )


def heading_duplicated_text(heading: str, doc_name: str) -> str:
    """The plain words for a heading the Doc has more than once: the section is ambiguous,
    so the creator updates the agent (D-50)."""
    return (
        f"The heading '{heading}' appears more than once in {doc_name}; "
        "ask the creator to update the agent"
    )


def heading_style_changed_text(heading: str, doc_name: str) -> str:
    """The plain words for a marker line that is still in the Doc but no longer looks as it
    did when the agent was published (its heading style, bold, underline or capitals changed):
    the creator republishes (D-50)."""
    return (
        f"The line '{heading}' in {doc_name} changed style since this agent was published; "
        "ask the creator to republish"
    )


def heading_level(style: str) -> int | None:
    """A paragraph style's heading level: ``HEADING_1``..``HEADING_6`` as 1..6, ``TITLE``
    and ``SUBTITLE`` as 0 (the highest), and None for a non-heading (``NORMAL_TEXT``). A
    section ends at the next heading of the same or a higher level (a smaller number)."""
    if style in ("TITLE", "SUBTITLE"):
        return 0
    if style.startswith("HEADING_"):
        try:
            return int(style[len("HEADING_") :])
        except ValueError:
            return None
    return None


def marker_signature(p: DocParagraph) -> str | None:
    """The style signature of a line that can mark a section, or None when the line cannot:
    a heading style is its own signature (``HEADING_2``); a normal line is a marker when its
    whole text is bold, underlined or in capitals, and its signature names which
    (``NORMAL_TEXT:bold+caps``). The grant freezes it at submit, so a line that later changes
    style no longer marks the section (D-50)."""
    if p.rule:
        return None
    if heading_level(p.style) is not None:
        return p.style
    flags = [
        name for name, on in (("bold", p.bold), ("underline", p.underline), ("caps", p.caps)) if on
    ]
    return "NORMAL_TEXT:" + "+".join(flags) if flags else None


def heading_matches(
    paragraphs: tuple[DocParagraph, ...], heading: str, marker: str | None = None
) -> list[int]:
    """The indices of the marker lines whose text is ``heading`` (trimmed): a section needs
    exactly one (D-50). ``marker`` is the grant's recorded style signature: None accepts a line
    of any marker style (the submit-time look), a signature only that style, and "" (a grant
    published before signatures) only a heading style."""
    name = heading.strip()
    found: list[int] = []
    for i, p in enumerate(paragraphs):
        signature = marker_signature(p)
        if signature is None or p.text.strip() != name:
            continue
        if marker == "" and heading_level(p.style) is None:
            continue
        if marker and signature != marker:
            continue
        found.append(i)
    return found


def locate_marker(doc: DocRead, heading: str, marker: str) -> int:
    """The index of the one line that marks the section, or a ``ConnectorError`` in plain
    words: the heading missing, more than once, or still there in another style (D-50)."""
    matches = heading_matches(doc.paragraphs, heading, marker)
    if not matches:
        if marker and heading_matches(doc.paragraphs, heading):
            raise ConnectorError(heading_style_changed_text(heading, doc.title))
        raise ConnectorError(heading_missing_text(heading, doc.title))
    if len(matches) > 1:
        raise ConnectorError(heading_duplicated_text(heading, doc.title))
    return matches[0]


RULE = "rule"
TABLE = "table"
# The line a horizontal line is written as in the section text the model reads and writes.
RULE_LINE = "---"


@dataclass(frozen=True)
class SectionPart:
    """A run of text paragraphs of a section between its horizontal lines and tables: the
    UTF-16 range ``[start, end)`` they fill and their text (each paragraph ends in a newline)."""

    start: int
    end: int
    text: str


@dataclass(frozen=True)
class SectionSep:
    """A horizontal line or a table of a section, kept in place by a write (D-50)."""

    kind: str
    start: int
    end: int
    table: DocTable | None = None


@dataclass(frozen=True)
class SectionLayout:
    """A section as the write sees it: the content range under the marker line and its parts
    and separators in order, ``len(parts) == len(seps) + 1``. A write replaces each part's
    text and each table cell's text in place and never touches a separator's structure (D-50)."""

    start: int
    end: int
    parts: tuple[SectionPart, ...]
    seps: tuple[SectionSep, ...]


def section_layout(doc: DocRead, marker_index: int) -> SectionLayout:
    """The layout of the section under the marker line at ``marker_index`` (D-50): the content
    starts at the line's end and runs to the next line of the same signature, or for a heading
    to the next heading of the same or a higher level, or to the body's end. Horizontal lines
    and tables inside it split the text into parts."""
    paragraphs = doc.paragraphs
    marker = paragraphs[marker_index]
    signature = marker_signature(marker)
    level = heading_level(marker.style)
    start = marker.end_index
    end = paragraphs[-1].end_index
    boundary = len(paragraphs)
    for j in range(marker_index + 1, len(paragraphs)):
        p = paragraphs[j]
        nxt = heading_level(p.style)
        stops = (
            (nxt is not None and nxt <= level)
            if level is not None
            else (marker_signature(p) == signature)
        )
        if stops:
            end, boundary = p.start_index, j
            break
    elements: list[tuple[int, DocParagraph | DocTable]] = [
        (p.start_index, p) for p in paragraphs[marker_index + 1 : boundary]
    ]
    elements += [
        (t.start_index, t) for t in doc.tables if t.start_index >= start and t.end_index <= end
    ]
    elements.sort(key=lambda e: e[0])
    parts: list[SectionPart] = []
    seps: list[SectionSep] = []
    part_start, text = start, ""
    for at, element in elements:
        if isinstance(element, DocTable):
            sep = SectionSep(TABLE, element.start_index, element.end_index, element)
        elif element.rule:
            sep = SectionSep(RULE, element.start_index, element.end_index)
        else:
            text += element.text
            continue
        parts.append(SectionPart(part_start, at, text))
        seps.append(sep)
        part_start, text = sep.end, ""
    parts.append(SectionPart(part_start, end, text))
    return SectionLayout(start, end, tuple(parts), tuple(seps))


def _escape_cell(text: str) -> str:
    return text.replace("|", "\\|")


def _table_lines(rows: list[list[str]] | tuple[tuple[str, ...], ...]) -> str:
    """A table as pipe rows, one line per row, as the model reads and writes it (D-50)."""
    return "".join("| " + " | ".join(_escape_cell(c) for c in row) + " |\n" for row in rows)


def _sep_text(sep: SectionSep) -> str:
    if sep.kind == RULE:
        return RULE_LINE + "\n"
    assert sep.table is not None
    return _table_lines([[cell.text.strip() for cell in row] for row in sep.table.rows])


def section_view(layout: SectionLayout) -> str:
    """The section's text as the model reads and writes it, trimmed: its paragraphs, each
    horizontal line as a line holding exactly ``---``, each table as pipe rows
    (``| a | b |``) (D-50)."""
    chunks: list[str] = []
    for i, part in enumerate(layout.parts):
        chunks.append(part.text)
        if i < len(layout.seps):
            chunks.append(_sep_text(layout.seps[i]))
    return "".join(chunks).strip()


def section_bounds(doc: DocRead, marker_index: int) -> tuple[int, int, str]:
    """The content range of the section under the marker line at ``marker_index`` (its UTF-16
    start and end) and its text as ``section_view`` gives it (D-50)."""
    layout = section_layout(doc, marker_index)
    return layout.start, layout.end, section_view(layout)


def section_plain_error(doc: DocRead, heading: str, marker_index: int) -> str | None:
    """The refusal for a section holding something the replace cannot write, or None: an
    inline image or any other element that is not text, a horizontal line or a table, a
    table cell of several lines, or a paragraph that reads like a divider (``---``) or a table
    row (starts with ``|``), which would make the model's view ambiguous (D-50). Such a section is refused at submit, at draft and at
    the write."""
    layout = section_layout(doc, marker_index)
    refused = any(fs < layout.end and fe > layout.start for fs, fe in doc.foreign) or any(
        not cell.plain or len(row) != len(sep.table.rows[0])
        for sep in layout.seps
        if sep.table is not None
        for row in sep.table.rows
        for cell in row
    )
    if refused:
        return SECTION_NOT_PLAIN_TEXT.format(doc=doc.title, heading=heading)
    for part in layout.parts:
        for line in part.text.split("\n"):
            if line.strip() == RULE_LINE or line.strip().startswith("|"):
                return SECTION_AMBIGUOUS_TEXT.format(doc=doc.title, heading=heading)
    return None


_PIPE_SPLIT = re.compile(r"(?<!\\)\|")


def _is_table_row(line: str) -> bool:
    line = line.strip()
    return len(line) >= 2 and line.startswith("|") and line.endswith("|")


def _table_row(line: str) -> tuple[str, ...]:
    return tuple(cell.strip().replace("\\|", "|") for cell in _PIPE_SPLIT.split(line.strip()[1:-1]))


@dataclass(frozen=True)
class SectionText:
    """New section text split along an existing layout: the text of each part (each line ends
    in a newline) and the horizontal lines and tables (as rows of cell text) between them."""

    parts: tuple[str, ...]
    seps: tuple[tuple[str, tuple[tuple[str, ...], ...]], ...]


def parse_section_text(text: str, layout: SectionLayout, heading: str) -> SectionText:
    """The new text split at its ``---`` lines and pipe-row tables, checked to keep the
    section's horizontal lines and tables as they are: the same lines in the same places, the
    same tables at the same rows and columns. A section without lines (or tables) takes a
    ``---`` line (or a pipe row) as ordinary text. A text that does not keep them is refused in
    plain words, before the card (D-50)."""
    has_rules = any(sep.kind == RULE for sep in layout.seps)
    has_tables = any(sep.kind == TABLE for sep in layout.seps)
    lines = text.split("\n")
    parts: list[list[str]] = [[]]
    seps: list[tuple[str, tuple[tuple[str, ...], ...]]] = []
    i = 0
    while i < len(lines):
        if has_rules and lines[i].strip() == RULE_LINE:
            seps.append((RULE, ()))
            parts.append([])
            i += 1
        elif has_tables and _is_table_row(lines[i]):
            rows: list[tuple[str, ...]] = []
            while i < len(lines) and _is_table_row(lines[i]):
                rows.append(_table_row(lines[i]))
                i += 1
            seps.append((TABLE, tuple(rows)))
            parts.append([])
        else:
            parts[-1].append(lines[i])
            i += 1
    parsed = SectionText(tuple("".join(f"{ln}\n" for ln in part) for part in parts), tuple(seps))
    _check_section_text(parsed, layout, heading)
    return parsed


def _check_section_text(parsed: SectionText, layout: SectionLayout, heading: str) -> None:
    old_kinds = [sep.kind for sep in layout.seps]
    new_kinds = [kind for kind, _ in parsed.seps]
    rules = old_kinds.count(RULE)
    old_tables = [sep.table for sep in layout.seps if sep.table is not None]
    if old_kinds != new_kinds and (
        new_kinds.count(RULE) != rules or new_kinds.count(TABLE) == len(old_tables)
    ):
        raise ConnectorError(
            SECTION_RULES_TEXT.format(
                count=rules,
                lines="line" if rules == 1 else "lines",
                heading=heading,
                each="it" if rules == 1 else "each one",
            )
        )
    new_tables = [rows for kind, rows in parsed.seps if kind == TABLE]
    shapes_match = len(new_tables) == len(old_tables) and all(
        len(rows) == len(old.rows) and all(len(row) == len(old.rows[0]) for row in rows)
        for rows, old in zip(new_tables, old_tables)
    )
    if not shapes_match:
        first = old_tables[0]
        for rows, old in zip(new_tables, old_tables):
            first = old
            if len(rows) != len(old.rows) or any(len(row) != len(old.rows[0]) for row in rows):
                break
        raise ConnectorError(
            SECTION_TABLE_TEXT.format(
                heading=heading, rows=len(first.rows), columns=len(first.rows[0])
            )
        )


def section_card_text(text: str) -> str:
    """Section text as the approval card shows it: each ``---`` line as a ``———`` row and
    each run of pipe rows as a small grid of ``│``-separated cells (D-50)."""
    shown: list[str] = []
    for line in text.split("\n"):
        if line.strip() == RULE_LINE:
            shown.append("———")
        elif _is_table_row(line):
            shown.append("│ " + " │ ".join(_table_row(line)) + " │")
        else:
            shown.append(line)
    return "\n".join(shown)


def section_replacement_view(layout: SectionLayout, text: str, heading: str) -> str | None:
    """What ``section_view`` reads once ``text`` is written over the section, or None when
    ``text`` does not fit the layout. The write's look-before-write and its reconcile compare
    it with the live view: equal means an earlier attempt landed (D-50)."""
    try:
        parsed = parse_section_text(text, layout, heading)
    except ConnectorError:
        return None
    chunks: list[str] = []
    for i, part in enumerate(parsed.parts):
        chunks.append(part)
        if i < len(parsed.seps):
            kind, rows = parsed.seps[i]
            chunks.append(RULE_LINE + "\n" if kind == RULE else _table_lines(rows))
    return "".join(chunks).strip()


def section_control_char_error(text: str) -> str | None:
    """The refusal for new text carrying a character Google Docs strips (a control character
    other than newline and tab, or a private-use code point), or None. Such a character would
    not land literally, so the draft and the edit refuse it (D-50)."""
    return SECTION_CONTROL_CHAR_TEXT if _STRIPPED_CHAR.search(text) else None


def section_write_plan(
    doc: DocRead, start: int, end: int, text: str
) -> tuple[int, str, int, int, int, int]:
    """The Docs batchUpdate plan that replaces the section ``[start, end)`` with ``text`` and
    keeps the heading, as real Google accepts it (D-50): ``(insert_index, insert_text,
    delete_start, delete_end, style_start, style_end)``.

    The new text is inserted FIRST at the start of the section's first content paragraph, so
    the new paragraphs take that paragraph's NORMAL style; the old range is then deleted at
    its shifted position (``+ utf16_len(insert_text)``). An empty section has no content
    paragraph to inherit from, so the inserted range is normalised to NORMAL_TEXT and its
    bullets cleared (``style_start``/``style_end``); a section that ends at the body end keeps
    the body's final newline (delete up to ``end - 1``, insert without a trailing newline); a
    heading that is the Doc's last paragraph inserts before that final newline, with a leading
    newline to start the new paragraph. ``style_start == style_end`` means no normalisation.
    """
    body_end = doc.paragraphs[-1].end_index
    if start == end and start == body_end:
        # The heading is the Doc's last paragraph: insert before the body's final newline.
        insert_index = body_end - 1
        insert_text = "\n" + text
        length = utf16_len(insert_text)
        return insert_index, insert_text, 0, 0, insert_index + 1, insert_index + length
    if start == end:
        # An empty section before another heading: insert, then normalise the inserted range.
        insert_text = text + "\n"
        length = utf16_len(insert_text)
        return start, insert_text, 0, 0, start, start + length
    if end == body_end:
        # The section ends at the body end: keep the final newline, insert without a trailing one.
        insert_text = text
        length = utf16_len(insert_text)
        return start, insert_text, start + length, end - 1 + length, 0, 0
    insert_text = text + "\n"
    length = utf16_len(insert_text)
    return start, insert_text, start + length, end + length, 0, 0


def section_edits(doc: DocRead, layout: SectionLayout, text: str, heading: str) -> list[DocEdit]:
    """The edits that write ``text`` over the section, last in the Doc first so every index
    still holds (D-50): each part whose text changed is replaced between the lines and tables
    around it (``section_write_plan``), each table cell whose text changed is deleted and
    refilled in place. Unchanged parts and cells are not touched. Refuses, in plain words,
    text that does not keep the section's lines and tables, or that adds text right above a
    table (Google refuses text at a table's first index)."""
    parsed = parse_section_text(text, layout, heading)
    body_end = doc.paragraphs[-1].end_index
    found: list[tuple[int, DocEdit]] = []
    for i, (part, new) in enumerate(zip(layout.parts, parsed.parts)):
        if new == part.text:
            continue
        if not new:
            end = part.end - 1 if part.end == body_end else part.end
            if end > part.start:
                found.append((part.start, (0, "", part.start, end, 0, 0)))
            continue
        if part.start == part.end and i < len(layout.seps) and layout.seps[i].kind == TABLE:
            raise ConnectorError(SECTION_BEFORE_TABLE_TEXT.format(heading=heading))
        found.append((part.start, section_write_plan(doc, part.start, part.end, new[:-1])))
    for sep, (_, rows) in zip(
        (sep for sep in layout.seps if sep.table is not None),
        (entry for entry in parsed.seps if entry[0] == TABLE),
    ):
        for old_row, new_row in zip(sep.table.rows, rows):
            for cell, new_text in zip(old_row, new_row):
                if new_text == cell.text.strip():
                    continue
                shift = utf16_len(new_text)
                found.append(
                    (
                        cell.start_index,
                        (
                            cell.start_index,
                            new_text,
                            cell.start_index + shift,
                            cell.end_index + shift,
                            0,
                            0,
                        ),
                    )
                )
    found.sort(key=lambda entry: entry[0], reverse=True)
    return [edit for _, edit in found]


def sheet_marker(idem_key: str) -> str:
    """The developer metadata value marking an appended row: the whole digest of its key."""
    return hashlib.sha256(idem_key.encode()).hexdigest()


def doc_marker(idem_key: str) -> str:
    """The appProperty value marking a copied Doc: the whole digest of its key."""
    return hashlib.sha256(idem_key.encode()).hexdigest()


def template_blank_names(text: str) -> tuple[str, ...]:
    """The distinct ``{{field name}}`` blank names in a template's text, in first-seen order."""
    names: list[str] = []
    for match in _BLANK.finditer(text):
        name = match.group(1).strip()
        if name and name not in names:
            names.append(name)
    return tuple(names)


def template_blank_forms(text: str) -> dict[str, tuple[str, ...]]:
    """Every raw ``{{field name}}`` form per trimmed blank name, in first-seen order: the
    fill replaces each form the text actually writes (``{{name}}``, ``{{ name }}``, ...),
    so detection and fill agree (D-49)."""
    forms: dict[str, list[str]] = {}
    for match in _BLANK.finditer(text):
        name = match.group(1).strip()
        if not name:
            continue
        found = forms.setdefault(name, [])
        if match.group(0) not in found:
            found.append(match.group(0))
    return {name: tuple(raw) for name, raw in forms.items()}


# The prefix every fill sentinel starts with. A value carrying it is refused, so the
# value pass only ever touches what the form pass marked.
FILL_SENTINEL_PREFIX = "⟦agentos-fill:"


def fill_sentinel(name: str) -> str:
    """The unique sentinel a blank's forms are replaced with before the value pass: a hash
    of the blank's name in mathematical brackets, a string no template writes, so it does
    not occur in the template's text and the value pass only touches what the form pass
    marked (D-49)."""
    return FILL_SENTINEL_PREFIX + hashlib.sha256(name.encode()).hexdigest() + "⟧"


def fill_markup_error(text: str, blanks: Iterable[str]) -> str | None:
    """The refusal for a value or title carrying the fill's markup, or None: the sentinel
    prefix, or any raw form of one of the template's blanks (``{{name}}``, ``{{ name }}``,
    ...). The draft and the edit check it so a value lands literally and a filled copy
    never reads as unfilled (D-49)."""
    if FILL_SENTINEL_PREFIX in text:
        return FILL_MARKUP_TEXT
    for blank in blanks:
        if re.search(r"\{\{\s*" + re.escape(blank) + r"\s*\}\}", text):
            return FILL_MARKUP_TEXT
    return None


def drive_file_id(ref: str) -> str | None:
    """The file ID a Drive link or bare ID names, or None."""
    ref = (ref or "").strip()
    for pattern in (_LINK_ID, _QUERY_ID):
        match = pattern.search(ref)
        if match:
            return match.group(1)
    return ref if DRIVE_ID.fullmatch(ref) else None


class DriveReader:
    """The deployment's Drive client bound to one tenant's account email.

    Every read carries that account's minted token, so it sees only what was shared with
    the tenant's address. The email is the connection row's, read fresh by the caller.
    """

    def __init__(self, google: GoogleAccounts, email: str):
        self._google = google
        self._email = email

    async def list_children(self, folder_id: str) -> list[DriveFile]:
        return await self._google.list_children(self._email, folder_id)

    async def get_file(self, file_id: str) -> DriveFile:
        return await self._google.get_file(self._email, file_id)

    async def export_file(self, file_id: str, mime_type: str) -> list[DriveSection]:
        return await self._google.export_file(self._email, file_id, mime_type)

    async def get_drive(self, drive_id: str) -> DriveFile:
        return await self._google.get_drive(self._email, drive_id)

    async def list_drive_children(self, drive_id: str) -> list[DriveFile]:
        return await self._google.list_drive_children(self._email, drive_id)

    async def list_comments(self, file_id: str) -> list[DriveComment]:
        return await self._google.list_comments(self._email, file_id)

    async def create_comment(self, file_id: str, content: str) -> str:
        return await self._google.create_comment(self._email, file_id, content)

    async def read_sheet(self, file_id: str, tab: str, header_only: bool = False) -> SheetTab:
        return await self._google.read_sheet(self._email, file_id, tab, header_only)

    async def search_sheet_metadata(
        self, file_id: str, key: str, value: str
    ) -> list[SheetMetadata]:
        return await self._google.search_sheet_metadata(self._email, file_id, key, value)

    async def append_sheet_row(
        self,
        file_id: str,
        sheet_id: int,
        row: list[str | int | float],
        row_index: int,
        key: str,
        value: str,
    ) -> str:
        return await self._google.append_sheet_row(
            self._email, file_id, sheet_id, row, row_index, key, value
        )

    async def copy_file(
        self, template_id: str, folder_id: str, title: str, key: str, value: str
    ) -> DriveFile:
        return await self._google.copy_file(self._email, template_id, folder_id, title, key, value)

    async def search_app_properties(
        self, folder_id: str, drive_id: str, key: str, value: str
    ) -> list[DriveFile]:
        return await self._google.search_app_properties(
            self._email, folder_id, drive_id, key, value
        )

    async def fill_doc(self, file_id: str, replacements: list[tuple[str, str]]) -> None:
        return await self._google.fill_doc(self._email, file_id, replacements)

    async def read_doc(self, file_id: str) -> DocRead:
        return await self._google.read_doc(self._email, file_id)

    async def replace_doc_section(
        self, file_id: str, required_revision_id: str, edits: list[DocEdit]
    ) -> str:
        return await self._google.replace_doc_section(
            self._email, file_id, required_revision_id, edits
        )


def _held_kinds(grants: Iterable[DriveGrant]) -> set[str]:
    kinds: set[str] = set()
    for grant in grants:
        kinds.add(grant.kind)
        kinds.add("drive_item")
    return kinds


def grant_kinds(grants: Grants) -> set[str]:
    """The kinds of grant held for Drive, in the words of a manifest's ``needs``. The
    connector offers a tool by this, and the console's hint reads it too."""
    return _held_kinds(grants.drive)


class DriveConnector:
    """Drive limited to the grants of one agent version: the comment tool, and the
    grant boundary every resolve and write goes through."""

    errors = drive_errors

    def __init__(
        self,
        reader: DriveReader | None,
        grants: list[DriveGrant],
        manifest: Manifest | None = None,
    ):
        self._reader = reader
        self._grants = {g.id: g for g in grants}
        self.manifest = manifest or load_manifest(MANIFEST_DIR / "drive.yaml")

    @property
    def name(self) -> str:
        return self.manifest.connector

    @property
    def grants(self) -> list[DriveGrant]:
        return list(self._grants.values())

    def tools_for_llm(self) -> list[dict]:
        held = _held_kinds(self._grants.values())
        sheets = [g for g in self._grants.values() if g.kind == "sheet"]
        templates = [g for g in self._grants.values() if g.kind == "template"]
        sections = [g for g in self._grants.values() if g.kind == "section"]
        tools = []
        for spec in self.manifest.tools.values():
            if not spec.offer_to_llm or (spec.needs and spec.needs not in held):
                # A tool is offered only to an agent holding the kind of grant its manifest
                # names in ``needs``: the console's hint reads the same ``grant_kinds``.
                continue
            if spec.name == "append_row":
                # Its description names the granted sheets and their columns (D-48).
                tools.append(self._append_row_definition(spec, sheets))
            elif spec.name == "create_doc_from_template":
                # Its description names the granted templates and their blanks (D-49).
                tools.append(self._create_doc_definition(spec, templates))
            elif spec.name == "replace_section":
                # Its description names the granted sections and their headings (D-50).
                tools.append(self._replace_section_definition(spec, sections))
            else:
                tools.append(spec.llm_definition())
        return tools

    @staticmethod
    def _append_row_definition(spec: ToolSpec, sheets: list[DriveGrant]) -> dict:
        listing = "\n".join(
            f'- "{g.name}" (id {g.id}, tab "{g.tab}"): columns '
            + (", ".join(g.columns) if g.columns else "(none recorded)")
            for g in sheets
        )
        definition = spec.llm_definition()
        definition["description"] = f"{spec.description}\nGranted sheets:\n{listing}"
        return definition

    @staticmethod
    def _create_doc_definition(spec: ToolSpec, templates: list[DriveGrant]) -> dict:
        listing = "\n".join(
            f'- "{g.name}" (id {g.id}): blanks '
            + (", ".join(g.blanks) if g.blanks else "(none recorded)")
            for g in templates
        )
        definition = spec.llm_definition()
        definition["description"] = f"{spec.description}\nGranted templates:\n{listing}"
        return definition

    @staticmethod
    def _replace_section_definition(spec: ToolSpec, sections: list[DriveGrant]) -> dict:
        listing = "\n".join(f'- "{g.name}" (id {g.id}, heading "{g.heading}")' for g in sections)
        definition = spec.llm_definition()
        definition["description"] = f"{spec.description}\nGranted sections:\n{listing}"
        return definition

    def grant_for(self, target_id: str) -> DriveGrant:
        grant = self._grants.get(target_id or "")
        if grant is None:
            raise GrantDenied(
                "This agent has not been granted access to that Drive folder or file."
            )
        return grant

    def sheet_for(self, ref: str) -> DriveGrant:
        """The granted Sheet a name or ID names, or ``GrantDenied``. Only a sheet grant
        appends; a folder, file or shared drive grant is not a Sheet to append to. A name
        two granted sheets share is refused: the model must use the ID, never a guess."""
        ref = (ref or "").strip()
        matches = [g for g in self._grants.values() if g.kind == "sheet" and ref in (g.id, g.name)]
        if len(matches) == 1:
            return matches[0]
        if matches:
            raise GrantDenied(f"More than one granted Sheet is named '{ref}'; use the sheet's ID.")
        raise GrantDenied(
            "This agent has not been granted a Sheet by that name or ID to append to."
        )

    def template_for(self, ref: str) -> DriveGrant:
        """The granted template a name or ID names, or ``GrantDenied``. Only a template grant
        copies a Doc; a name two granted templates share is refused: the model must use the ID."""
        ref = (ref or "").strip()
        matches = [
            g for g in self._grants.values() if g.kind == "template" and ref in (g.id, g.name)
        ]
        if len(matches) == 1:
            return matches[0]
        if matches:
            raise GrantDenied(
                f"More than one granted template is named '{ref}'; use the template's ID."
            )
        raise GrantDenied("This agent has not been granted a template by that name or ID to copy.")

    def section_for(self, ref: str) -> DriveGrant:
        """The granted section a name or ID names, or ``GrantDenied``. Only a section grant
        replaces a section; a name two granted sections share is refused: the model must use
        the ID (D-50)."""
        ref = (ref or "").strip()
        matches = [
            g for g in self._grants.values() if g.kind == "section" and ref in (g.id, g.name)
        ]
        if len(matches) == 1:
            return matches[0]
        if matches:
            raise GrantDenied(
                f"More than one granted section is named '{ref}'; use the section's ID."
            )
        raise GrantDenied(
            "This agent has not been granted a section by that name or ID to replace."
        )

    async def check_doc_shape(self, template_id: str, blanks: list[str], folder_id: str) -> None:
        """The write's shape check without the write (D-49), run before reconcile looks for
        the marker: the approved blanks and folder must still be the live grant's, and the
        folder's live shared drive the recorded one. A republish between draft and approval,
        or a folder moved out of its shared drive, refuses here."""
        grant = self._grants.get(template_id)
        if grant is None or grant.kind != "template":
            raise GrantDenied("This agent has not been granted that template to copy.")
        if tuple(blanks) != grant.blanks or folder_id != grant.folder_id:
            raise ConnectorError(DOC_SHAPE_CHANGED_TEXT)
        self._check_shared_drive(grant, await self._live_file(folder_id))

    def _check_shared_drive(self, grant: DriveGrant, folder: DriveFile) -> None:
        """The destination folder as it is live: it must sit in the shared drive the
        definition recorded at submit. A folder moved to My Drive refuses with the
        publish's plain words; one moved to another shared drive refuses as a changed
        shape."""
        if not folder.drive_id:
            raise ConnectorError(SHARED_DRIVE_FOLDER_TEXT)
        if folder.drive_id != grant.drive_id:
            raise ConnectorError(DOC_SHAPE_CHANGED_TEXT)

    async def resolve_doc_folder(self, template_id: str, folder_id: str) -> DriveFile:
        """The destination folder of a granted template, for the card's name and link: the
        template grant is checked first, the folder must be the grant's, and it is read live
        (a folder gone, trashed or not a folder raises ``UnknownItem``). Its shared drive
        must still be the recorded one, so a folder moved since the publish refuses before
        any card."""
        grant = self.template_for(template_id)
        if grant.folder_id != folder_id:
            raise ConnectorError(DOC_SHAPE_CHANGED_TEXT)
        try:
            folder = await self._reader.get_file(folder_id)
        except Exception as err:
            if drive_errors.is_gone(err):
                raise UnknownItem(_UNKNOWN_FILE) from err
            raise
        if folder.mime_type != MIME_FOLDER or folder.trashed:
            raise UnknownItem(_UNKNOWN_FILE)
        self._check_shared_drive(grant, folder)
        return folder

    async def read_checked_sheet(self, grant: DriveGrant, *, header_only: bool = False) -> SheetTab:
        """The tab as it is now, refusing when its header no longer matches the columns the
        definition froze at publish (D-48). The write reads this just before appending; a
        draft reads only row 1 (``header_only``), the header alone."""
        sheet = await self._reader.read_sheet(grant.id, grant.tab, header_only=header_only)
        if tuple(sheet.header) != grant.columns:
            raise ConnectorError(HEADER_CHANGED_TEXT)
        return sheet

    async def check_append_shape(self, file_id: str, columns: list[str]) -> None:
        """The write's shape check without the write (D-48), run before reconcile looks for
        the marker: the approved columns must still be the live grant's, and the tab's live
        header must still match them. A republish between draft and approval refuses here."""
        grant = self._grants.get(file_id)
        if grant is None or grant.kind != "sheet":
            raise GrantDenied("This agent has not been granted that Sheet to append to.")
        if tuple(columns) != grant.columns:
            raise ConnectorError(HEADER_CHANGED_TEXT)
        await self.read_checked_sheet(grant, header_only=True)

    async def read(
        self, tool: str, tool_input: dict, requester_email: str | None = None
    ) -> list[NotionItem]:
        raise ConnectorError(f"Unknown tool '{tool}'.")

    async def _live_file(self, file_id: str) -> DriveFile:
        """The file as it is now: gone, unreadable or trashed reads as ``UnknownItem``."""
        try:
            node = await self._reader.get_file(file_id)
        except Exception as err:
            if drive_errors.is_gone(err):
                raise UnknownItem(_UNKNOWN_FILE) from err
            raise
        if node.trashed:
            raise UnknownItem(_UNKNOWN_FILE)
        return node

    async def resolve_item(self, item_ref: str) -> NotionItem:
        """The granted file a link or ID names: a directly granted file, or one under a
        granted folder or shared drive.

        The file itself is fetched (trashed and folder checks), then its parents are
        walked up to a granted root, the same walk the search re-check makes (first
        parent, bounded depth): a file moved out of the granted folder does not resolve.
        A granted root is matched by ID and never fetched — a shared drive's root is
        not a file, so ``files.get`` of it would answer 404. A folder or a shared
        drive's root is not a file to comment on. A template grant is copy-only: the
        template itself is not a comment target through it. A file that is not granted,
        not found, not shared or trashed raises the same ``UnknownItem``, so the answer
        never reveals whether an ungranted file exists.
        """
        file_id = drive_file_id(item_ref)
        if file_id is None or self._reader is None:
            raise UnknownItem(_UNKNOWN_FILE)
        file = await self._live_file(file_id)
        if file.mime_type == MIME_FOLDER:
            raise UnknownItem(_UNKNOWN_FILE)
        item = NotionItem(page_id=file.id, url=file.web_view_link, title=file.name, status=None)
        grant = self._grants.get(file_id)
        if grant is not None and grant.kind != "template":
            return item
        node = file
        for _ in range(MAX_WALK_DEPTH):
            if not node.parents:
                raise UnknownItem(_UNKNOWN_FILE)
            parent = node.parents[0]
            if parent in self._grants:
                return item
            node = await self._live_file(parent)
        raise UnknownItem(_UNKNOWN_FILE)

    async def assignee_user_id(self, item_id: str) -> str | None:
        return None

    async def find_comment(self, file_id: str, idem_key: str) -> str | None:
        """The ID of the comment an earlier attempt made under ``idem_key``, if visible.

        Only comments the tenant's own account wrote count, and the whole reference must
        match: anyone else's comment that quotes the reference proves nothing.
        """
        ref = comment_ref(idem_key)
        for comment in await self._reader.list_comments(file_id):
            if comment.mine and ref in comment.content:
                return comment.id
        return None

    async def create_comment(self, file_id: str, text: str, idem_key: str, agent_name: str) -> str:
        """Add the comment as the tenant's account, ending with the agent's name and the
        key's reference, so a retry finds it the same way Notion's comment does."""
        body = f"{text}\n\n— {agent_name[:SIGNATURE_NAME_MAX]}, {comment_ref(idem_key)}"
        return await self._reader.create_comment(file_id, body)

    async def find_appended_row(self, file_id: str, idem_key: str) -> str | None:
        """The metadata ID of the row an earlier attempt appended under ``idem_key``, if
        visible: the developer metadata search is the append write's marker lookup (D-48)."""
        found = await self._reader.search_sheet_metadata(
            file_id, SHEET_METADATA_KEY, sheet_marker(idem_key)
        )
        return str(found[0].metadata_id) if found else None

    async def append_row(self, file_id: str, payload: dict, idem_key: str) -> str:
        """Append the approved row as the tenant's account, marking it with the key's hash.

        The approved shape is the written shape: the draft's columns must still be the live
        grant's (a republish since the draft refuses), and the tab's header, read just before
        the append, must still match them. The row is built from the draft's columns — a text
        cell lands literally, a model's JSON number as a number — and lands with its marker
        in one atomic batchUpdate, so a lost response never leaves an unmarked row.
        """
        grant = self._grants.get(file_id)
        if grant is None or grant.kind != "sheet":
            raise GrantDenied("This agent has not been granted that Sheet to append to.")
        columns = payload["columns"]
        if tuple(columns) != grant.columns:
            raise ConnectorError(HEADER_CHANGED_TEXT)
        sheet = await self.read_checked_sheet(grant)
        row = [payload["row"].get(column, "") for column in columns]
        return await self._reader.append_sheet_row(
            file_id,
            sheet.sheet_id,
            row,
            sheet.data_rows,
            SHEET_METADATA_KEY,
            sheet_marker(idem_key),
        )

    async def _find_copy(self, grant: DriveGrant, idem_key: str) -> DriveFile | None:
        """The Doc an earlier attempt copied under ``idem_key``, if visible: the appProperty
        search in the destination folder is the create write's marker lookup (D-49). The
        folder is read for its shared drive, which the drive-scoped search asks for."""
        folder = await self._reader.get_file(grant.folder_id)
        found = await self._reader.search_app_properties(
            grant.folder_id, folder.drive_id, DOC_APP_PROPERTY_KEY, doc_marker(idem_key)
        )
        return found[0] if found else None

    async def find_created_doc(self, template_id: str, idem_key: str) -> NotionItem | None:
        """The copied Doc an earlier attempt made under ``idem_key``, if it is visible AND
        filled. A copy still carrying a blank's form or fill sentinel reads as not done, so
        the write runs and fills what is left (a lost response between the copy and the
        fill, or between the fill's two passes)."""
        grant = self._grants.get(template_id)
        if grant is None or grant.kind != "template":
            raise GrantDenied("This agent has not been granted that template to copy.")
        copy = await self._find_copy(grant, idem_key)
        if copy is None:
            return None
        sections = await self._reader.export_file(copy.id, MIME_DOC)
        text = "\n".join(section.text for section in sections)
        if any(fill_sentinel(blank) in text for blank in grant.blanks):
            return None
        if set(template_blank_names(text)) & set(grant.blanks):
            return None
        return NotionItem(page_id=copy.id, url=copy.web_view_link, title=copy.name, status=None)

    async def create_doc(self, template_id: str, payload: dict, idem_key: str) -> NotionItem:
        """Copy the granted template into its shared-drive folder as the approved title,
        marking the copy with the key's hash, then fill every blank literally.

        The approved shape is the written shape: the draft's blanks and folder must still be
        the live grant's, and the folder's live shared drive the recorded one (a republish
        or a moved folder since the draft refuses). The copy carries its marker in the same
        request. The fill reads the copy's text for every raw form of each blank and replaces
        them in two passes in one batchUpdate: each form to the blank's sentinel, then each
        sentinel to its value. The draft and the edit refuse a value carrying a blank's form
        or the sentinel prefix, so the passes never splice a value and a retry fills only
        what is left. A template with no blanks needs no fill: the copy is the finished Doc.
        """
        await self.check_doc_shape(template_id, payload["blanks"], payload["folder_id"])
        grant = self._grants[template_id]
        copy = await self._find_copy(grant, idem_key)
        if copy is None:
            copy = await self._reader.copy_file(
                template_id,
                grant.folder_id,
                payload["title"],
                DOC_APP_PROPERTY_KEY,
                doc_marker(idem_key),
            )
        sections = await self._reader.export_file(copy.id, MIME_DOC)
        forms = template_blank_forms("\n".join(section.text for section in sections))
        replacements: list[tuple[str, str]] = []
        for blank in payload["blanks"]:
            for form in forms.get(blank, ()):
                replacements.append((form, fill_sentinel(blank)))
        for blank in payload["blanks"]:
            replacements.append((fill_sentinel(blank), payload["values"].get(blank, "")))
        if replacements:
            await self._reader.fill_doc(copy.id, replacements)
        return NotionItem(page_id=copy.id, url=copy.web_view_link, title=copy.name, status=None)

    async def read_section(self, grant: DriveGrant, text: str | None = None) -> dict:
        """The section's current text and the Doc's read revision, for the draft and the card
        (D-50): a ``documents.get`` of the granted Doc, the marker line found exactly once in
        its recorded style, the text under it (the card's "before", with each horizontal line
        as ``---`` and each table as pipe rows) and the content range the write replaces. The
        section must hold only text, lines and tables and its current text be within the draft
        bound; with ``text``, the new text must keep the section's lines and tables. Anything
        else refuses here in plain words before any card. The revisionId is the lock the write
        names; the before text is frozen into the draft so what was approved is what runs. The
        link is the Doc's webViewLink."""
        doc = await self._reader.read_doc(grant.doc_id)
        index = locate_marker(doc, grant.heading, grant.marker)
        if error := section_plain_error(doc, grant.heading, index):
            raise ConnectorError(error)
        layout = section_layout(doc, index)
        before = section_view(layout)
        if len(before) > MAX_SECTION_BEFORE:
            raise ConnectorError(
                SECTION_TOO_LONG_TEXT.format(
                    heading=grant.heading, doc=doc.title, limit=MAX_SECTION_BEFORE
                )
            )
        if text is not None:
            section_edits(doc, layout, text, grant.heading)
        file = await self._reader.get_file(grant.doc_id)
        return {
            "revision_id": doc.revision_id,
            "before": before,
            "content_start": layout.start,
            "content_end": layout.end,
            "item_title": doc.title,
            "item_url": file.web_view_link or f"https://drive.google.com/file/d/{grant.doc_id}",
        }

    async def check_section_shape(
        self, doc_id: str, heading: str, marker: str | None = None
    ) -> DocRead:
        """The write's shape check without the write (D-50), run before the write and before
        reconcile looks: the approved heading and marker style must still be the live grant's
        (a republish since the draft refuses), the Doc must still have that line exactly once
        in that style (a renamed, removed, duplicated or restyled line refuses in plain
        words), and the section must hold only text, lines and tables (an image refuses).
        ``marker`` is None for a draft made before styles were recorded. Returns the Doc read
        for the check."""
        grant = self._grants.get(doc_id)
        if grant is None or grant.kind != "section":
            raise GrantDenied("This agent has not been granted that Doc section to replace.")
        if grant.heading != heading or (marker is not None and grant.marker != marker):
            raise ConnectorError(SECTION_SHAPE_CHANGED_TEXT)
        doc = await self._reader.read_doc(doc_id)
        index = locate_marker(doc, heading, grant.marker)
        if error := section_plain_error(doc, heading, index):
            raise ConnectorError(error)
        return doc

    async def find_replaced_section(self, doc_id: str, payload: dict) -> str | None:
        """Whether the section already holds the approved text (D-50): the revision write's
        look-before-write and reconcile lookup. The shape check runs first, as reconcile's
        does (a republish, a heading gone, duplicated or restyled, or an image refuses). The
        Doc is read and the section's current text compared with the approved text as it would
        read once written (lines and tables included) — equal means an earlier attempt landed
        (the revision is the lock, so no marker is needed); different means not seen."""
        grant = self._grants.get(doc_id)
        if grant is None or grant.kind != "section":
            return None
        doc = await self.check_section_shape(doc_id, payload["heading"], payload.get("marker"))
        layout = section_layout(doc, locate_marker(doc, payload["heading"], grant.marker))
        wanted = section_replacement_view(layout, payload["text"], payload["heading"])
        return doc_id if wanted is not None and section_view(layout) == wanted else None

    async def replace_section(self, doc_id: str, payload: dict, idem_key: str) -> str:
        """Replace the approved section's content as the tenant's account, keeping the marker
        line, its horizontal lines and its tables' shape (D-50). The approved shape is the
        written shape: the draft's heading and style must still be the live grant's, the Doc
        must still have the line exactly once, and the section must hold only text, lines and
        tables. The batchUpdate carries ``writeControl.requiredRevisionId`` = the stored read
        revision and, last part of the Doc first, replaces each part between the lines and each
        changed table cell in place (``section_edits``), so the new paragraphs keep the NORMAL
        style and the body's final newline is never deleted. The revision is the lock: a retry
        after a lost response, or a Doc someone else moved, gets a revision mismatch; the Doc
        is then read and the section's text compared with the approved text — equal means the
        earlier attempt landed (done), different refuses with nothing written."""
        grant = self._grants[doc_id]
        doc = await self.check_section_shape(doc_id, payload["heading"], payload.get("marker"))
        layout = section_layout(doc, locate_marker(doc, payload["heading"], grant.marker))
        edits = section_edits(doc, layout, payload["text"], payload["heading"])
        if not edits:
            return doc_id
        try:
            return await self._reader.replace_doc_section(doc_id, payload["revision_id"], edits)
        except GoogleError as err:
            if err.code != "revision_mismatch":
                raise
            doc = await self._reader.read_doc(doc_id)
            matches = heading_matches(doc.paragraphs, payload["heading"], grant.marker)
            if len(matches) == 1:
                layout = section_layout(doc, matches[0])
                wanted = section_replacement_view(layout, payload["text"], payload["heading"])
                if wanted is not None and section_view(layout) == wanted:
                    return doc_id
            raise ConnectorError(DOC_MOVED_TEXT) from err

    async def find_write(self, kind: str, target: str, idem_key: str) -> NotionItem | str | None:
        # replace_section is not here: its look carries the approved payload (the heading and
        # the new text), so it uses find_replaced_section directly, not this idem-key lookup.
        if kind == "add_comment":
            return await self.find_comment(target, idem_key)
        if kind == "append_row":
            return await self.find_appended_row(target, idem_key)
        if kind == "create_doc":
            return await self.find_created_doc(target, idem_key)
        raise ConnectorError(f"Unknown write kind '{kind}'.")

    async def write(
        self,
        kind: str,
        target: str,
        payload: dict,
        idem_key: str,
        *,
        agent_name: str,
        requester: dict | None = None,
    ) -> NotionItem | str:
        if kind == "add_comment":
            return await self.create_comment(target, payload["comment"], idem_key, agent_name)
        if kind == "append_row":
            return await self.append_row(target, payload, idem_key)
        if kind == "create_doc":
            return await self.create_doc(target, payload, idem_key)
        if kind == "replace_section":
            return await self.replace_section(target, payload, idem_key)
        raise ConnectorError(f"Unknown write kind '{kind}'.")

    def bulk_priority(self) -> AbstractContextManager[None]:
        return nullcontext()
