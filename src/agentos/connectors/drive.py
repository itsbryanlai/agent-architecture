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

from agentos.config import DRIVE_ID, DriveGrant, Grants
from agentos.connectors import drive_errors
from agentos.connectors.base import MAX_WALK_DEPTH, ConnectorError, GrantDenied, UnknownItem
from agentos.connectors.google_iam import (
    MIME_DOC,
    MIME_FOLDER,
    DocParagraph,
    DocRead,
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
# The refusal for a section that is not plain paragraphs: a table, an inline image or any
# other non-paragraph element under the heading. The replace only writes plain text (D-50).
SECTION_NOT_PLAIN_TEXT = (
    "I can only replace a section made of plain paragraphs; "
    "'{doc}' has a table or image under '{heading}'"
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


def heading_matches(paragraphs: tuple[DocParagraph, ...], heading: str) -> list[int]:
    """The indices of the heading paragraphs whose text is ``heading`` (trimmed): a section
    needs exactly one (D-50)."""
    name = heading.strip()
    return [
        i
        for i, p in enumerate(paragraphs)
        if heading_level(p.style) is not None and p.text.strip() == name
    ]


def section_bounds(
    paragraphs: tuple[DocParagraph, ...], heading_index: int
) -> tuple[int, int, str]:
    """The content range of the section under the heading at ``heading_index``: the UTF-16
    start (the heading's end) and end (the next heading of the same or higher level, or the
    body's end), and the text under the heading, trimmed. The heading itself is kept (D-50)."""
    level = heading_level(paragraphs[heading_index].style)
    start = paragraphs[heading_index].end_index
    end = start
    parts: list[str] = []
    for p in paragraphs[heading_index + 1 :]:
        nxt = heading_level(p.style)
        if nxt is not None and level is not None and nxt <= level:
            break
        end = p.end_index
        parts.append(p.text)
    return start, end, "".join(parts).strip()


def section_span(paragraphs: tuple[DocParagraph, ...], heading_index: int) -> tuple[int, int]:
    """The full index span under the heading at ``heading_index``: from the heading's end to
    the next heading of the same or higher level's start (or the body's end). Unlike the
    content range, it includes any non-paragraph element between the last content paragraph
    and the next heading, so a trailing table or image is refused too (D-50)."""
    level = heading_level(paragraphs[heading_index].style)
    start = paragraphs[heading_index].end_index
    for p in paragraphs[heading_index + 1 :]:
        nxt = heading_level(p.style)
        if nxt is not None and level is not None and nxt <= level:
            return start, p.start_index
    return start, paragraphs[-1].end_index


def section_plain_error(doc: DocRead, heading: str, heading_index: int) -> str | None:
    """The refusal for a section that is not plain paragraphs, or None: a table, an inline
    image or any other non-paragraph element whose index range falls inside the section's
    full span under the heading. The replace writes plain text only, so such a section is
    refused at submit, at draft and at the write (D-50)."""
    start, end = section_span(doc.paragraphs, heading_index)
    if any(fs < end and fe > start for fs, fe in doc.foreign):
        return SECTION_NOT_PLAIN_TEXT.format(doc=doc.title, heading=heading)
    return None


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
        self,
        file_id: str,
        required_revision_id: str,
        insert_index: int,
        insert_text: str,
        delete_start: int,
        delete_end: int,
        style_start: int = 0,
        style_end: int = 0,
    ) -> str:
        return await self._google.replace_doc_section(
            self._email,
            file_id,
            required_revision_id,
            insert_index,
            insert_text,
            delete_start,
            delete_end,
            style_start,
            style_end,
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

    async def read_section(self, grant: DriveGrant) -> dict:
        """The section's current text and the Doc's read revision, for the draft and the card
        (D-50): a ``documents.get`` of the granted Doc, the heading found exactly once, the
        text under it (the card's "before") and the content range the write replaces. The
        section must be plain paragraphs and its current text within the draft bound, so a
        table, an image or an over-long section refuses here in plain words before any card.
        The revisionId is the lock the write names; the range and the before text are frozen
        into the draft so what was approved is what runs. The link is the Doc's webViewLink."""
        doc = await self._reader.read_doc(grant.doc_id)
        matches = heading_matches(doc.paragraphs, grant.heading)
        if not matches:
            raise ConnectorError(heading_missing_text(grant.heading, doc.title))
        if len(matches) > 1:
            raise ConnectorError(heading_duplicated_text(grant.heading, doc.title))
        start, end, before = section_bounds(doc.paragraphs, matches[0])
        if error := section_plain_error(doc, grant.heading, matches[0]):
            raise ConnectorError(error)
        if len(before) > MAX_SECTION_BEFORE:
            raise ConnectorError(
                SECTION_TOO_LONG_TEXT.format(
                    heading=grant.heading, doc=doc.title, limit=MAX_SECTION_BEFORE
                )
            )
        file = await self._reader.get_file(grant.doc_id)
        return {
            "revision_id": doc.revision_id,
            "before": before,
            "content_start": start,
            "content_end": end,
            "item_title": doc.title,
            "item_url": file.web_view_link or f"https://drive.google.com/file/d/{grant.doc_id}",
        }

    async def check_section_shape(self, doc_id: str, heading: str) -> DocRead:
        """The write's shape check without the write (D-50), run before the write and before
        reconcile looks: the approved heading must still be the live grant's (a republish
        since the draft refuses), the Doc must still have that heading exactly once (a
        renamed, removed or duplicated heading refuses in plain words), and the section must
        be plain paragraphs (a table or image refuses). Returns the Doc read for the check."""
        grant = self._grants.get(doc_id)
        if grant is None or grant.kind != "section":
            raise GrantDenied("This agent has not been granted that Doc section to replace.")
        if grant.heading != heading:
            raise ConnectorError(SECTION_SHAPE_CHANGED_TEXT)
        doc = await self._reader.read_doc(doc_id)
        matches = heading_matches(doc.paragraphs, heading)
        if not matches:
            raise ConnectorError(heading_missing_text(heading, doc.title))
        if len(matches) > 1:
            raise ConnectorError(heading_duplicated_text(heading, doc.title))
        if error := section_plain_error(doc, heading, matches[0]):
            raise ConnectorError(error)
        return doc

    async def find_replaced_section(self, doc_id: str, payload: dict) -> str | None:
        """Whether the section already holds the approved text (D-50): the revision write's
        look-before-write and reconcile lookup. The shape check runs first, as reconcile's
        does (a republish, a heading gone or duplicated, or a table or image refuses). The Doc
        is read and the section's current text compared with the approved text — equal means
        an earlier attempt landed (the revision is the lock, so no marker is needed);
        different means not seen."""
        grant = self._grants.get(doc_id)
        if grant is None or grant.kind != "section":
            return None
        doc = await self.check_section_shape(doc_id, payload["heading"])
        matches = heading_matches(doc.paragraphs, payload["heading"])
        if len(matches) != 1:
            return None
        _, _, before = section_bounds(doc.paragraphs, matches[0])
        return doc_id if before == payload["text"].strip() else None

    async def replace_section(self, doc_id: str, payload: dict, idem_key: str) -> str:
        """Replace the approved section's content as the tenant's account, keeping the heading
        (D-50). The approved shape is the written shape: the draft's heading must still be the
        live grant's, the Doc must still have it exactly once, and the section must be plain
        paragraphs. The batchUpdate carries ``writeControl.requiredRevisionId`` = the stored
        read revision and inserts the new text first at the section's first content paragraph,
        then deletes the shifted old range (``section_write_plan``), so the new paragraphs keep
        the NORMAL style and the body's final newline is never deleted. The revision is the
        lock: a retry after a lost response, or a Doc someone else moved, gets a revision
        mismatch; the Doc is then read and the section's text compared with the approved text —
        equal means the earlier attempt landed (done), different refuses with nothing written."""
        doc = await self.check_section_shape(doc_id, payload["heading"])
        plan = section_write_plan(
            doc, payload["content_start"], payload["content_end"], payload["text"]
        )
        try:
            return await self._reader.replace_doc_section(doc_id, payload["revision_id"], *plan)
        except GoogleError as err:
            if err.code != "revision_mismatch":
                raise
            doc = await self._reader.read_doc(doc_id)
            matches = heading_matches(doc.paragraphs, payload["heading"])
            if len(matches) == 1:
                _, _, before = section_bounds(doc.paragraphs, matches[0])
                if before == payload["text"].strip():
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
