"""Which granted Notion objects were chosen from the creator's own Notion account.

Notion has no "act as this user" for the tenant's integration, so the console cannot ask
Notion whether a creator may see a database. The practical form is a picker: the creator
signs in to Notion (see ``notion_picker``), the console lists what *their* token can
search, and only an object picked from that list counts as one the creator may grant. A
pick is recorded here per agent: the Notion ID, its kind, who chose it and when. Never a
title, page text or the creator's token.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.connectors.notion import normalize_id
from agentos.stores import joined_transaction


class GrantNotPicked(Exception):
    """A submit or test run was refused because a new grant was not chosen from the
    creator's Notion."""


@dataclass(frozen=True)
class PickedObject:
    """An object the creator's Notion token lists. ``title`` is for the picker page only."""

    id: str
    kind: str  # "database" or "page"
    title: str = ""


@dataclass(frozen=True)
class PickRecord:
    tenant_id: str
    agent_id: str
    notion_id: str
    kind: str
    picked_by: str
    picked_at: datetime


class GrantPickStore(Protocol):
    async def record(
        self,
        tenant_id: str,
        agent_id: str,
        objects: list[PickedObject],
        picked_by: str,
        at: datetime,
        conn: Any = None,
    ) -> None:
        """Remember that ``picked_by`` chose these objects for the agent. Choosing one again
        refreshes who and when."""
        ...

    async def for_agent(
        self, tenant_id: str, agent_id: str, conn: Any = None
    ) -> dict[str, PickRecord]:
        """The agent's picks by normalised Notion ID."""
        ...


class InMemoryGrantPickStore:
    def __init__(self) -> None:
        self._rows: dict[tuple[str, str, str], PickRecord] = {}

    async def record(self, tenant_id, agent_id, objects, picked_by, at, conn=None) -> None:
        for obj in objects:
            key = normalize_id(obj.id)
            self._rows[(tenant_id, agent_id, key)] = PickRecord(
                tenant_id, agent_id, key, obj.kind, picked_by, at
            )

    async def for_agent(self, tenant_id, agent_id, conn=None) -> dict[str, PickRecord]:
        return {
            key[2]: row
            for key, row in self._rows.items()
            if key[0] == tenant_id and key[1] == agent_id
        }

    def snapshot(self) -> dict:
        return copy.copy(self._rows)

    def restore(self, state: dict) -> None:
        self._rows = state


class PostgresGrantPickStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def record(self, tenant_id, agent_id, objects, picked_by, at, conn=None) -> None:
        async with joined_transaction(self._pool, tenant_id, conn) as joined:
            for obj in objects:
                await joined.execute(
                    """
                    INSERT INTO grant_picks
                      (tenant_id, agent_id, notion_id, kind, picked_by, picked_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tenant_id, agent_id, notion_id)
                    DO UPDATE SET kind = EXCLUDED.kind, picked_by = EXCLUDED.picked_by,
                                  picked_at = EXCLUDED.picked_at
                    """,
                    (tenant_id, agent_id, normalize_id(obj.id), obj.kind, picked_by, at),
                )

    async def for_agent(self, tenant_id, agent_id, conn=None) -> dict[str, PickRecord]:
        async with joined_transaction(self._pool, tenant_id, conn) as joined:
            cur = await joined.execute(
                """
                SELECT tenant_id, agent_id, notion_id, kind, picked_by, picked_at
                FROM grant_picks WHERE tenant_id = %s AND agent_id = %s
                """,
                (tenant_id, agent_id),
            )
            rows = await cur.fetchall()
        return {row[2]: PickRecord(*row) for row in rows}
