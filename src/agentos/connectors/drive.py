"""The Google Drive connector: read access to the folders and files a tenant was granted.

Drive reads run as the tenant's own service account (the address its files are shared
with), through the deployment's ``GoogleAccounts`` minting a short-lived token. A
``DriveReader`` binds that account's email to the deployment client, so every call acts
with the tenant's access, never another tenant's and never the asker's.

For now the connector offers the model no tools (``tools_for_llm`` is empty): knowledge
search reads the indexed Drive scopes, and the writes arrive with their own tickets. The
connector holds the grants, resolves a granted file's reference, and brings the Drive
error table so a read's failures are classified like every other connector's.
"""

from __future__ import annotations

import re
from contextlib import AbstractContextManager, nullcontext

from agentos.config import DRIVE_ID, DriveGrant
from agentos.connectors import drive_errors
from agentos.connectors.base import ConnectorError, GrantDenied, UnknownItem
from agentos.connectors.google_iam import DriveFile, DriveSection, GoogleAccounts
from agentos.connectors.manifest import MANIFEST_DIR, Manifest, load_manifest
from agentos.models import NotionItem

# A Drive file ID in a link: /file/d/<id>, /document/d/<id>, /spreadsheets/d/<id>,
# /presentation/d/<id>, or an open?id=<id> query. IDs are letters, digits, - and _.
_LINK_ID = re.compile(r"/(?:file|document|spreadsheets|presentation)/d/([A-Za-z0-9_-]+)")
_QUERY_ID = re.compile(r"[?&]id=([A-Za-z0-9_-]+)")


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


class DriveConnector:
    """Drive limited to the grants of one agent version. No model-callable tools yet."""

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
        return []

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
        """The granted file a link or ID names.

        Only a directly granted file resolves for now; a file nested in a granted folder
        is resolved by the write tickets that need it. A file that is not granted, not
        found or not shared raises the same ``UnknownItem``, so the answer never reveals
        whether an ungranted file exists.
        """
        file_id = drive_file_id(item_ref)
        if file_id is None:
            raise UnknownItem("I couldn't find a Google Drive file in that link or ID.")
        grant = self._grants.get(file_id)
        if grant is None or grant.kind != "file" or self._reader is None:
            raise UnknownItem("I couldn't find a Google Drive file in that link or ID.")
        try:
            file = await self._reader.get_file(file_id)
        except Exception as err:
            if drive_errors.is_gone(err):
                raise UnknownItem(
                    "I couldn't find a Google Drive file in that link or ID."
                ) from err
            raise
        if file.trashed:
            raise UnknownItem("I couldn't find a Google Drive file in that link or ID.")
        return NotionItem(page_id=file.id, url=file.web_view_link, title=file.name, status=None)

    async def assignee_user_id(self, item_id: str) -> str | None:
        return None

    async def find_write(self, kind: str, target: str, idem_key: str) -> NotionItem | str | None:
        raise ConnectorError("Google Drive writes are not available yet.")

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
        raise ConnectorError("Google Drive writes are not available yet.")

    def bulk_priority(self) -> AbstractContextManager[None]:
        return nullcontext()
