"""Persistent state that lives outside Temporal.

Chat history is kept here rather than in workflow history, so Temporal only
carries a conversation ID and small values and never document or chat content.

Every method takes ``tenant_id`` explicitly and every table is keyed by it
first. There is no global tenant state.

A history append is named by a step key, so a retried append never stores a
message twice.

Tool output text is kept here too (``ToolOutputStore``): workflow history
carries only the reference, never the text a tool read.
"""

from __future__ import annotations

import copy
from typing import Protocol

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool


class HistoryStore(Protocol):
    async def append(
        self, tenant_id: str, conversation_id: str, step_key: str, role: str, content: list[dict]
    ) -> int:
        """Append one message under step_key and return its seq.

        A repeat with the same (tenant_id, conversation_id, step_key) stores nothing
        and returns the seq of the message already stored.
        """
        ...

    async def load(self, tenant_id: str, conversation_id: str) -> list[dict]:
        """Return the conversation as Anthropic-format messages, oldest first."""
        ...


class IdempotencyStore(Protocol):
    async def get(self, tenant_id: str, key: str) -> dict | None: ...

    async def put_if_absent(self, tenant_id: str, key: str, value: dict) -> dict:
        """Store ``value`` unless the key exists; return whichever value is stored."""
        ...


class ToolOutputStore(Protocol):
    async def put(
        self, tenant_id: str, conversation_id: str, tool_use_id: str, content: str, is_error: bool
    ) -> None:
        """Store a tool's output text. The first write for a tool_use_id wins."""
        ...

    async def get_many(
        self, tenant_id: str, conversation_id: str, tool_use_ids: list[str]
    ) -> dict[str, tuple[str, bool]]:
        """Return ``{tool_use_id: (content, is_error)}`` for those that are stored."""
        ...


class TaskLinkStore(Protocol):
    async def link(self, tenant_id: str, page_id: str, task_id: str) -> None: ...

    async def task_for_page(self, tenant_id: str, page_id: str) -> str | None: ...


# --- Postgres -----------------------------------------------------------------


class PostgresHistoryStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def append(
        self, tenant_id: str, conversation_id: str, step_key: str, role: str, content: list[dict]
    ) -> int:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                INSERT INTO conversation_messages (tenant_id, conversation_id, seq, step_key, role, content)
                SELECT %s, %s, COALESCE(MAX(seq), 0) + 1, %s, %s, %s
                FROM conversation_messages
                WHERE tenant_id = %s AND conversation_id = %s
                ON CONFLICT (tenant_id, conversation_id, step_key) DO NOTHING
                RETURNING seq
                """,
                (
                    tenant_id,
                    conversation_id,
                    step_key,
                    role,
                    Jsonb(content),
                    tenant_id,
                    conversation_id,
                ),
            )
            row = await cur.fetchone()
            if row is None:
                # The step was already stored by an earlier attempt.
                cur = await conn.execute(
                    """
                    SELECT seq FROM conversation_messages
                    WHERE tenant_id = %s AND conversation_id = %s AND step_key = %s
                    """,
                    (tenant_id, conversation_id, step_key),
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


class PostgresToolOutputStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def put(
        self, tenant_id: str, conversation_id: str, tool_use_id: str, content: str, is_error: bool
    ) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO tool_outputs (tenant_id, conversation_id, tool_use_id, content, is_error)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, conversation_id, tool_use_id) DO NOTHING
                """,
                (tenant_id, conversation_id, tool_use_id, content, is_error),
            )

    async def get_many(
        self, tenant_id: str, conversation_id: str, tool_use_ids: list[str]
    ) -> dict[str, tuple[str, bool]]:
        if not tool_use_ids:
            return {}
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT tool_use_id, content, is_error FROM tool_outputs
                WHERE tenant_id = %s AND conversation_id = %s AND tool_use_id = ANY(%s)
                """,
                (tenant_id, conversation_id, list(tool_use_ids)),
            )
            rows = await cur.fetchall()
            return {tid: (content, is_error) for tid, content, is_error in rows}


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
        self.step_keys: dict[tuple[str, str], dict[str, int]] = {}
        self._fail_next_append = False

    # Test hooks.

    def fail_next_append_after_write(self) -> None:
        """Make the next append store the message, then raise a retryable error once."""
        self._fail_next_append = True

    async def append(
        self, tenant_id: str, conversation_id: str, step_key: str, role: str, content: list[dict]
    ) -> int:
        key = (tenant_id, conversation_id)
        keys = self.step_keys.setdefault(key, {})
        seq = keys.get(step_key)
        if seq is None:
            msgs = self.messages.setdefault(key, [])
            msgs.append({"role": role, "content": copy.deepcopy(content)})
            seq = keys[step_key] = len(msgs)
        if self._fail_next_append:
            self._fail_next_append = False
            raise RuntimeError("simulated lost result after the message was stored")
        return seq

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


class InMemoryToolOutputStore:
    def __init__(self) -> None:
        self.outputs: dict[tuple[str, str, str], tuple[str, bool]] = {}

    async def put(
        self, tenant_id: str, conversation_id: str, tool_use_id: str, content: str, is_error: bool
    ) -> None:
        self.outputs.setdefault((tenant_id, conversation_id, tool_use_id), (content, is_error))

    async def get_many(
        self, tenant_id: str, conversation_id: str, tool_use_ids: list[str]
    ) -> dict[str, tuple[str, bool]]:
        found = {}
        for tid in tool_use_ids:
            value = self.outputs.get((tenant_id, conversation_id, tid))
            if value is not None:
                found[tid] = value
        return found


class InMemoryTaskLinkStore:
    def __init__(self) -> None:
        self.links: dict[tuple[str, str], str] = {}

    async def link(self, tenant_id: str, page_id: str, task_id: str) -> None:
        self.links.setdefault((tenant_id, page_id), task_id)

    async def task_for_page(self, tenant_id: str, page_id: str) -> str | None:
        return self.links.get((tenant_id, page_id))
