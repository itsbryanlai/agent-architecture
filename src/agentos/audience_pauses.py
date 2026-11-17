"""Paused channels: where an agent has stopped answering until its owner looks again.

An answer posted in a channel is read by everyone in it, including guests and people
from other Slack workspaces who cannot ask the agent themselves. When such people
appear in a channel an agent serves, the agent is paused there. A pause is one row per
(agent, channel); it stays in the table, marked resumed, when someone confirms the
channel again, so the history is kept. At most one pause per agent and channel is
active at a time.

A pause holds IDs and a fixed reason code, never chat or document text.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.stores import joined_transaction, tenant_transaction

# Why a channel was paused. These are the only values stored or shown.
CHANNEL_SHARED = "channel_shared"

REASON_TEXT = {
    CHANNEL_SHARED: "the channel was shared with another organisation",
}


class NotPaused(Exception):
    """The agent is not paused in that channel."""


@dataclass(frozen=True)
class PauseRecord:
    tenant_id: str
    agent_id: str
    channel: str
    reason: str
    paused_at: datetime
    resumed_by: str | None = None
    resumed_at: datetime | None = None


class PauseStore(Protocol):
    async def get_active(
        self, tenant_id: str, agent_id: str, channel: str, conn: Any = None
    ) -> PauseRecord | None:
        """The agent's active pause in the channel, or None.

        ``conn`` is the connection of a unit of work (``unit_of_work.py``), here and below.
        """
        ...

    async def list_active(self, tenant_id: str, agent_id: str | None = None) -> list[PauseRecord]:
        """Active pauses of the tenant, or of one agent, oldest first."""
        ...

    async def open_pause(
        self, tenant_id: str, agent_id: str, channel: str, reason: str, at: datetime
    ) -> PauseRecord:
        """Pause the agent in the channel; returns the active pause, the existing one if any."""
        ...

    async def close_pause(
        self, tenant_id: str, agent_id: str, channel: str, by: str, at: datetime, conn: Any = None
    ) -> PauseRecord | None:
        """Mark the active pause resumed; None when there is none."""
        ...


_COLUMNS = "tenant_id, agent_id, channel, reason, paused_at, resumed_by, resumed_at"


class PostgresPauseStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def get_active(
        self, tenant_id: str, agent_id: str, channel: str, conn: Any = None
    ) -> PauseRecord | None:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            cur = await db.execute(
                f"SELECT {_COLUMNS} FROM audience_pauses "
                "WHERE tenant_id = %s AND agent_id = %s AND channel = %s AND resumed_at IS NULL",
                (tenant_id, agent_id, channel),
            )
            row = await cur.fetchone()
        return None if row is None else PauseRecord(*row)

    async def list_active(self, tenant_id: str, agent_id: str | None = None) -> list[PauseRecord]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT {_COLUMNS} FROM audience_pauses "
                "WHERE tenant_id = %s AND resumed_at IS NULL AND (%s::text IS NULL OR agent_id = %s) "
                "ORDER BY paused_at, agent_id, channel",
                (tenant_id, agent_id, agent_id),
            )
            return [PauseRecord(*row) for row in await cur.fetchall()]

    async def open_pause(
        self, tenant_id: str, agent_id: str, channel: str, reason: str, at: datetime
    ) -> PauseRecord:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                "INSERT INTO audience_pauses (tenant_id, agent_id, channel, reason, paused_at) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (tenant_id, agent_id, channel) WHERE resumed_at IS NULL DO NOTHING",
                (tenant_id, agent_id, channel, reason, at),
            )
            cur = await conn.execute(
                f"SELECT {_COLUMNS} FROM audience_pauses "
                "WHERE tenant_id = %s AND agent_id = %s AND channel = %s AND resumed_at IS NULL",
                (tenant_id, agent_id, channel),
            )
            row = await cur.fetchone()
        return PauseRecord(*row)

    async def close_pause(
        self, tenant_id: str, agent_id: str, channel: str, by: str, at: datetime, conn: Any = None
    ) -> PauseRecord | None:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            cur = await db.execute(
                "UPDATE audience_pauses SET resumed_by = %s, resumed_at = %s "
                "WHERE tenant_id = %s AND agent_id = %s AND channel = %s AND resumed_at IS NULL "
                f"RETURNING {_COLUMNS}",
                (by, at, tenant_id, agent_id, channel),
            )
            row = await cur.fetchone()
        return None if row is None else PauseRecord(*row)


class InMemoryPauseStore:
    def __init__(self) -> None:
        self._rows: list[PauseRecord] = []

    def snapshot(self) -> list[PauseRecord]:
        """The rows as they are now, for ``InMemoryUnitOfWork``."""
        return list(self._rows)

    def restore(self, state: list[PauseRecord]) -> None:
        self._rows = list(state)

    @property
    def history(self) -> list[PauseRecord]:
        """Every pause that has been resumed, in the order they were opened."""
        return [r for r in self._rows if r.resumed_at is not None]

    def _active(self, tenant_id: str, agent_id: str, channel: str) -> int | None:
        for i, r in enumerate(self._rows):
            same = (r.tenant_id, r.agent_id, r.channel) == (tenant_id, agent_id, channel)
            if same and r.resumed_at is None:
                return i
        return None

    async def get_active(
        self, tenant_id: str, agent_id: str, channel: str, conn: Any = None
    ) -> PauseRecord | None:
        i = self._active(tenant_id, agent_id, channel)
        return None if i is None else self._rows[i]

    async def list_active(self, tenant_id: str, agent_id: str | None = None) -> list[PauseRecord]:
        return sorted(
            (
                r
                for r in self._rows
                if r.tenant_id == tenant_id
                and r.resumed_at is None
                and (agent_id is None or r.agent_id == agent_id)
            ),
            key=lambda r: (r.paused_at, r.agent_id, r.channel),
        )

    async def open_pause(
        self, tenant_id: str, agent_id: str, channel: str, reason: str, at: datetime
    ) -> PauseRecord:
        i = self._active(tenant_id, agent_id, channel)
        if i is not None:
            return self._rows[i]
        record = PauseRecord(tenant_id, agent_id, channel, reason, at)
        self._rows.append(record)
        return record

    async def close_pause(
        self, tenant_id: str, agent_id: str, channel: str, by: str, at: datetime, conn: Any = None
    ) -> PauseRecord | None:
        i = self._active(tenant_id, agent_id, channel)
        if i is None:
            return None
        self._rows[i] = replace(self._rows[i], resumed_by=by, resumed_at=at)
        return self._rows[i]
