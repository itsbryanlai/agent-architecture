"""The Drive re-check before a stored hit may reach the model, and the scope dispatcher.

A Drive hit is confirmed live with the tenant's own Drive account: ``files.get`` with the
file's parents, walked up until it reaches the scope's granted root (kept) or a node that
is gone, trashed, has no readable parent, or sits outside the root (dropped). A file moved
out of the granted folder since the last walk therefore never reaches the model, as
``PageChecker`` keeps a Notion page still under its root. Lookups are cached for the one
search, so a folder shared by many hits is read once.
"""

from __future__ import annotations

from agentos import ids
from agentos.connectors import drive_errors
from agentos.connectors.drive import DriveReader
from agentos.connectors.google_iam import DriveFile
from agentos.knowledge.search import MAX_WALK_DEPTH, PageChecker
from agentos.knowledge.store import ChunkHit


class DriveChecker:
    """One instance per search: file lookups are cached for that call only."""

    def __init__(self, reader: DriveReader | None):
        self._reader = reader
        self._cache: dict[str, DriveFile | None] = {}

    async def _file(self, file_id: str) -> DriveFile | None:
        """The live file, or None when it is gone, unreadable or trashed. A transient
        error re-raises so the activity retries, as Notion's re-check does."""
        if file_id not in self._cache:
            try:
                file = await self._reader.get_file(file_id)
            except Exception as err:
                if drive_errors.is_gone(err):
                    self._cache[file_id] = None
                    return None
                raise
            self._cache[file_id] = None if file.trashed else file
        return self._cache[file_id]

    async def confirm(self, hit: ChunkHit, root_page_id: str = "") -> bool:
        if self._reader is None:
            return False
        current = hit.page_id
        for _ in range(MAX_WALK_DEPTH):
            file = await self._file(current)
            if file is None:
                return False
            if current == root_page_id:
                return True
            if not file.parents:
                return False
            current = file.parents[0]
        return False


class ScopeChecker:
    """Dispatches a hit to its connector's checker by scope: Drive scopes to the Drive
    re-check, everything else to Notion's. A Drive hit with no reader is dropped."""

    def __init__(self, notion: PageChecker, drive: DriveChecker | None):
        self._notion = notion
        self._drive = drive

    async def confirm(self, hit: ChunkHit, root_page_id: str) -> bool:
        if ids.is_drive_scope(hit.scope):
            return await self._drive.confirm(hit, root_page_id) if self._drive else False
        return await self._notion.confirm(hit, root_page_id)
