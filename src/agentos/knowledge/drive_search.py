"""The Drive re-check before a stored hit may reach the model, and the scope dispatcher.

A Drive hit is confirmed live with the tenant's own Drive account: ``files.get``. A file
that answers 404 or 403 (gone or no longer shared), or is trashed, is dropped, as
``PageChecker`` drops a Notion page. Drive has no parent-chain move check: the nightly
walk removes a file that left a granted folder, and the live read catches a file that is
gone or unreadable now.
"""

from __future__ import annotations

from agentos.connectors import drive_errors
from agentos.connectors.drive import DriveReader
from agentos.knowledge.search import PageChecker
from agentos.knowledge.store import ChunkHit


class DriveChecker:
    """One instance per search: file lookups are cached for that call only."""

    def __init__(self, reader: DriveReader | None):
        self._reader = reader
        self._cache: dict[str, bool] = {}

    async def confirm(self, hit: ChunkHit, root_page_id: str = "") -> bool:
        if self._reader is None:
            return False
        if hit.page_id not in self._cache:
            try:
                file = await self._reader.get_file(hit.page_id)
            except Exception as err:
                if drive_errors.is_gone(err):
                    self._cache[hit.page_id] = False
                    return False
                raise
            self._cache[hit.page_id] = not file.trashed
        return self._cache[hit.page_id]


class ScopeChecker:
    """Dispatches a hit to its connector's checker by scope: Drive scopes to the Drive
    re-check, everything else to Notion's. A Drive hit with no reader is dropped."""

    def __init__(self, notion: PageChecker, drive: DriveChecker | None):
        self._notion = notion
        self._drive = drive

    async def confirm(self, hit: ChunkHit, root_page_id: str) -> bool:
        if hit.scope.startswith("drive:"):
            return await self._drive.confirm(hit, root_page_id) if self._drive else False
        return await self._notion.confirm(hit, root_page_id)
