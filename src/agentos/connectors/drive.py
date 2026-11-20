"""The Google Drive connector: the folders and files a tenant was granted, read and written.

Drive calls run as the tenant's own service account (the address its files are shared
with), through the deployment's ``GoogleAccounts`` minting a short-lived token. A
``DriveReader`` binds that account's email to the deployment client, so every call acts
with the tenant's access, never another tenant's and never the asker's.

Knowledge search reads the indexed Drive scopes; the model's first Drive tool is
``comment_on_file`` (D-47), a comment task like Notion's ``add_comment``: the comment
ends with the key's reference, and the marker lookup scans the file's comments for it
before a retry posts again. ``resolve_item`` walks a file's parents up to a granted
root — the same walk the search re-check makes — so a file inside a granted folder or
shared drive resolves, and one moved out of it does not. The connector brings the Drive
error table so a failure is classified like every other connector's.
"""

from __future__ import annotations

import re
from contextlib import AbstractContextManager, nullcontext

from agentos.config import DRIVE_ID, DriveGrant
from agentos.connectors import drive_errors
from agentos.connectors.base import ConnectorError, GrantDenied, UnknownItem
from agentos.connectors.google_iam import (
    MIME_FOLDER,
    DriveComment,
    DriveFile,
    DriveSection,
    GoogleAccounts,
)
from agentos.connectors.manifest import MANIFEST_DIR, Manifest, load_manifest
from agentos.connectors.notion import comment_ref
from agentos.knowledge.search import MAX_WALK_DEPTH
from agentos.models import SIGNATURE_NAME_MAX, NotionItem

# A Drive file ID in a link: /file/d/<id>, /document/d/<id>, /spreadsheets/d/<id>,
# /presentation/d/<id>, or an open?id=<id> query. IDs are letters, digits, - and _.
_LINK_ID = re.compile(r"/(?:file|document|spreadsheets|presentation)/d/([A-Za-z0-9_-]+)")
_QUERY_ID = re.compile(r"[?&]id=([A-Za-z0-9_-]+)")

# The one answer for a reference that names no granted, live file — never whether it exists.
_UNKNOWN_FILE = "I couldn't find a Google Drive file in that link or ID."


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
        return [spec.llm_definition() for spec in self.manifest.tools.values() if spec.offer_to_llm]

    def grant_for(self, target_id: str) -> DriveGrant:
        grant = self._grants.get(target_id or "")
        if grant is None:
            raise GrantDenied(
                "This agent has not been granted access to that Drive folder or file."
            )
        return grant

    async def read(
        self, tool: str, tool_input: dict, requester_email: str | None = None
    ) -> list[NotionItem]:
        raise ConnectorError(f"Unknown tool '{tool}'.")

    async def resolve_item(self, item_ref: str) -> NotionItem:
        """The granted file a link or ID names: a directly granted file, or one under a
        granted folder or shared drive.

        The file's parents are walked up to a granted root, the same walk the search
        re-check makes (first parent, bounded depth): a file moved out of the granted
        folder does not resolve. A folder or a shared drive's root is not a file to
        comment on. A file that is not granted, not found, not shared or trashed raises
        the same ``UnknownItem``, so the answer never reveals whether an ungranted file
        exists.
        """
        file_id = drive_file_id(item_ref)
        if file_id is None or self._reader is None:
            raise UnknownItem(_UNKNOWN_FILE)
        file: DriveFile | None = None
        current = file_id
        for _ in range(MAX_WALK_DEPTH):
            try:
                node = await self._reader.get_file(current)
            except Exception as err:
                if drive_errors.is_gone(err):
                    raise UnknownItem(_UNKNOWN_FILE) from err
                raise
            if node.trashed:
                raise UnknownItem(_UNKNOWN_FILE)
            if file is None:
                file = node
            if current in self._grants:
                if file.mime_type == MIME_FOLDER:
                    raise UnknownItem(_UNKNOWN_FILE)
                return NotionItem(
                    page_id=file.id, url=file.web_view_link, title=file.name, status=None
                )
            if not node.parents:
                raise UnknownItem(_UNKNOWN_FILE)
            current = node.parents[0]
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

    async def find_write(self, kind: str, target: str, idem_key: str) -> NotionItem | str | None:
        if kind == "add_comment":
            return await self.find_comment(target, idem_key)
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
        raise ConnectorError(f"Unknown write kind '{kind}'.")

    def bulk_priority(self) -> AbstractContextManager[None]:
        return nullcontext()
