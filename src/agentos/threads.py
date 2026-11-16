"""Per-thread and per-refusal facts that outlive a conversation workflow.

``ThreadStore`` holds which agent a thread was handed to. A conversation workflow closes
after a week of silence and a new message starts it again with the agent the channel maps
to, so the specialist a front door chose would be forgotten if it lived only in workflow
state. ``OwnerPingStore`` holds the UTC day an agent's owner was last mentioned in a refusal
to one asker in one place, so the owner is mentioned once a day for each.

Both hold IDs and a date, never message text. Every method takes ``tenant_id`` and the
tables are under row-level security, like every other store.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.stores import tenant_transaction


class ThreadStore(Protocol):
    async def agent_for(self, tenant_id: str, thread_key: str) -> str | None:
        """The agent the thread was handed to, or None when it never was."""
        ...

    async def set_agent(self, tenant_id: str, thread_key: str, agent_id: str, at: datetime) -> None:
        """Record the thread's agent. A repeat, or a later hand-off, replaces it."""
        ...


class OwnerPingStore(Protocol):
    async def claim(
        self, tenant_id: str, agent_id: str, channel: str, asker_user_id: str, day: date
    ) -> bool:
        """True once for each ``day`` (and per agent, channel and asker): the owner may be
        mentioned now. False when the owner was already mentioned that day or a later one."""
        ...


class InMemoryThreadStore:
    def __init__(self) -> None:
        self._agents: dict[tuple[str, str], str] = {}

    async def agent_for(self, tenant_id: str, thread_key: str) -> str | None:
        return self._agents.get((tenant_id, thread_key))

    async def set_agent(self, tenant_id: str, thread_key: str, agent_id: str, at: datetime) -> None:
        self._agents[(tenant_id, thread_key)] = agent_id


class InMemoryOwnerPingStore:
    def __init__(self) -> None:
        self._days: dict[tuple[str, str, str, str], date] = {}

    async def claim(
        self, tenant_id: str, agent_id: str, channel: str, asker_user_id: str, day: date
    ) -> bool:
        key = (tenant_id, agent_id, channel, asker_user_id)
        last = self._days.get(key)
        if last is not None and last >= day:
            return False
        self._days[key] = day
        return True


class PostgresThreadStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def agent_for(self, tenant_id: str, thread_key: str) -> str | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                "SELECT agent_id FROM threads WHERE tenant_id = %s AND thread_key = %s",
                (tenant_id, thread_key),
            )
            row = await cur.fetchone()
        return None if row is None else row[0]

    async def set_agent(self, tenant_id: str, thread_key: str, agent_id: str, at: datetime) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                """
                INSERT INTO threads (tenant_id, thread_key, agent_id, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (tenant_id, thread_key)
                DO UPDATE SET agent_id = EXCLUDED.agent_id, updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, thread_key, agent_id, at),
            )


class PostgresOwnerPingStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def claim(
        self, tenant_id: str, agent_id: str, channel: str, asker_user_id: str, day: date
    ) -> bool:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                INSERT INTO owner_pings (tenant_id, agent_id, channel, asker_user_id, notified_on)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, agent_id, channel, asker_user_id)
                DO UPDATE SET notified_on = EXCLUDED.notified_on
                WHERE owner_pings.notified_on < EXCLUDED.notified_on
                RETURNING 1
                """,
                (tenant_id, agent_id, channel, asker_user_id, day),
            )
            return await cur.fetchone() is not None
