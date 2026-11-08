"""Persistent state that lives outside Temporal.

Chat history is kept here rather than in workflow history, so Temporal only
carries a conversation ID and small values and never document or chat content.

Every method takes ``tenant_id`` explicitly and every table is keyed by it
first. There is no global tenant state.
"""

from __future__ import annotations

import copy
from typing import Protocol

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool


class HistoryStore(Protocol):
    async def append(
        self, tenant_id: str, conversation_id: str, role: str, content: list[dict]
    ) -> int:
        """Append one message and return its sequence number."""
        ...

    async def load(self, tenant_id: str, conversation_id: str) -> list[dict]:
        """Return the conversation as Anthropic-format messages, oldest first."""
        ...


class IdempotencyStore(Protocol):
    async def get(self, tenant_id: str, key: str) -> dict | None: ...

    async def put_if_absent(self, tenant_id: str, key: str, value: dict) -> dict:
        """Store ``value`` unless the key exists; return whichever value is stored."""
        ...


class TaskLinkStore(Protocol):
    async def link(self, tenant_id: str, page_id: str, task_id: str) -> None: ...

    async def task_for_page(self, tenant_id: str, page_id: str) -> str | None: ...


# --- Postgres -----------------------------------------------------------------


class PostgresHistoryStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def append(
        self, tenant_id: str, conversation_id: str, role: str, content: list[dict]
    ) -> int:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                INSERT INTO conversation_messages (tenant_id, conversation_id, seq, role, content)
                SELECT %s, %s, COALESCE(MAX(seq), 0) + 1, %s, %s
                FROM conversation_messages
                WHERE tenant_id = %s AND conversation_id = %s
                RETURNING seq
                """,
                (tenant_id, conversation_id, role, Jsonb(content), tenant_id, conversation_id),
            )
            row = await cur.fetchone()
            return int(row[0])

    async def load(self, tenant_id: str, conversation_id: str) -> list[dict]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT role, content FROM conversation_messages
                WHERE tenant_id = %s AND conversation_id = %s
                ORDER BY seq
                """,
                (tenant_id, conversation_id),
            )
            rows = await cur.fetchall()
            return [{"role": role, "content": content} for role, content in rows]


class PostgresIdempotencyStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def get(self, tenant_id: str, key: str) -> dict | None:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT value FROM idempotency_records WHERE tenant_id = %s AND idem_key = %s",
                (tenant_id, key),
            )
            row = await cur.fetchone()
            return row[0] if row else None

    async def put_if_absent(self, tenant_id: str, key: str, value: dict) -> dict:
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO idempotency_records (tenant_id, idem_key, value)
                VALUES (%s, %s, %s)
                ON CONFLICT (tenant_id, idem_key) DO NOTHING
                """,
                (tenant_id, key, Jsonb(value)),
            )
            cur = await conn.execute(
                "SELECT value FROM idempotency_records WHERE tenant_id = %s AND idem_key = %s",
                (tenant_id, key),
            )
            row = await cur.fetchone()
            return row[0]


class PostgresTaskLinkStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def link(self, tenant_id: str, page_id: str, task_id: str) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO task_links (tenant_id, page_id, task_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (tenant_id, page_id) DO NOTHING
                """,
                (tenant_id, page_id, task_id),
            )

    async def task_for_page(self, tenant_id: str, page_id: str) -> str | None:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT task_id FROM task_links WHERE tenant_id = %s AND page_id = %s",
                (tenant_id, page_id),
            )
            row = await cur.fetchone()
            return row[0] if row else None


# --- In memory (tests) --------------------------------------------------------


class InMemoryHistoryStore:
    def __init__(self) -> None:
        self.messages: dict[tuple[str, str], list[dict]] = {}

    async def append(
        self, tenant_id: str, conversation_id: str, role: str, content: list[dict]
    ) -> int:
        msgs = self.messages.setdefault((tenant_id, conversation_id), [])
        msgs.append({"role": role, "content": copy.deepcopy(content)})
        return len(msgs)

    async def load(self, tenant_id: str, conversation_id: str) -> list[dict]:
        return copy.deepcopy(self.messages.get((tenant_id, conversation_id), []))


class InMemoryIdempotencyStore:
    def __init__(self) -> None:
        self.records: dict[tuple[str, str], dict] = {}

    async def get(self, tenant_id: str, key: str) -> dict | None:
        value = self.records.get((tenant_id, key))
        return copy.deepcopy(value) if value is not None else None

    async def put_if_absent(self, tenant_id: str, key: str, value: dict) -> dict:
        stored = self.records.setdefault((tenant_id, key), copy.deepcopy(value))
        return copy.deepcopy(stored)


class InMemoryTaskLinkStore:
    def __init__(self) -> None:
        self.links: dict[tuple[str, str], str] = {}

    async def link(self, tenant_id: str, page_id: str, task_id: str) -> None:
        self.links.setdefault((tenant_id, page_id), task_id)

    async def task_for_page(self, tenant_id: str, page_id: str) -> str | None:
        return self.links.get((tenant_id, page_id))
