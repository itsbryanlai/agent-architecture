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
columns the definition froze at publish. ``resolve_item`` walks a file's parents up to
a granted root — the same walk the search re-check makes — so a file inside a granted
folder or shared drive resolves, and one moved out of it does not; the granted root is
matched by ID and never fetched, as a shared drive's root is not a file. The connector
brings the Drive error table so a failure is classified like every other connector's.
"""

from __future__ import annotations

import hashlib
import re
from contextlib import AbstractContextManager, nullcontext

from agentos.config import DRIVE_ID, DriveGrant
from agentos.connectors import drive_errors
from agentos.connectors.base import MAX_WALK_DEPTH, ConnectorError, GrantDenied, UnknownItem
from agentos.connectors.google_iam import (
    MIME_FOLDER,
    DriveComment,
    DriveFile,
    DriveSection,
    GoogleAccounts,
    SheetMetadata,
    SheetTab,
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


def sheet_marker(idem_key: str) -> str:
    """The developer metadata value marking an appended row: the whole digest of its key."""
    return hashlib.sha256(idem_key.encode()).hexdigest()


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
        sheets = [g for g in self._grants.values() if g.kind == "sheet"]
        tools = []
        for spec in self.manifest.tools.values():
            if not spec.offer_to_llm:
                continue
            if spec.name == "append_row":
                # The append tool is offered only to an agent granted a Sheet, and its
                # description names the granted sheets and their columns (D-48).
                if sheets:
                    tools.append(self._append_row_definition(spec, sheets))
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
        drive's root is not a file to comment on. A file that is not granted, not
        found, not shared or trashed raises the same ``UnknownItem``, so the answer
        never reveals whether an ungranted file exists.
        """
        file_id = drive_file_id(item_ref)
        if file_id is None or self._reader is None:
            raise UnknownItem(_UNKNOWN_FILE)
        file = await self._live_file(file_id)
        if file.mime_type == MIME_FOLDER:
            raise UnknownItem(_UNKNOWN_FILE)
        item = NotionItem(page_id=file.id, url=file.web_view_link, title=file.name, status=None)
        if file_id in self._grants:
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

    async def find_write(self, kind: str, target: str, idem_key: str) -> NotionItem | str | None:
        if kind == "add_comment":
            return await self.find_comment(target, idem_key)
        if kind == "append_row":
            return await self.find_appended_row(target, idem_key)
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
        raise ConnectorError(f"Unknown write kind '{kind}'.")

    def bulk_priority(self) -> AbstractContextManager[None]:
        return nullcontext()
