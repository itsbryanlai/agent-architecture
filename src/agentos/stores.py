"""Persistent state that lives outside Temporal.

Chat history is kept here rather than in workflow history, so Temporal only
carries a conversation ID and small values and never document or chat content.

Every method takes ``tenant_id`` explicitly and every table is keyed by it
first. There is no global tenant state.

The database enforces tenant separation with row-level security: every Postgres
call runs in a transaction that first sets ``app.tenant_id``, and the app
connects as a role that cannot bypass the policies. The explicit ``tenant_id``
conditions in the SQL stay as a second line of defense.

A history append is named by a step key, so a retried append never stores a
message twice.

Tool output text is kept here too (``ToolOutputStore``): workflow history
carries only the reference, never the text a tool read.
"""

from __future__ import annotations

import copy
import uuid
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from agentos.history_view import HISTORY_BUDGET_CHARS, char_len, window


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

    async def load_window(
        self, tenant_id: str, conversation_id: str, budget: int = HISTORY_BUDGET_CHARS
    ) -> list[dict]:
        """The messages ``history_view.window`` would keep of the whole conversation.

        Only the rows of the window are read; the cut is made in front of a user message with
        text, from the sizes stored on the rows.
        """
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


@asynccontextmanager
async def tenant_transaction(pool: AsyncConnectionPool, tenant_id: str):
    """A connection in a transaction scoped to one tenant by row-level security."""
    if not tenant_id:
        raise ValueError("tenant_id is required")
    async with pool.connection() as conn, conn.transaction():
        await conn.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant_id,))
        yield conn


@asynccontextmanager
async def joined_transaction(pool: AsyncConnectionPool, tenant_id: str, conn=None):
    """``conn`` when a unit of work passes one, else a transaction of this call's own.

    A store method that a unit of work (``unit_of_work.py``) may include takes an optional
    ``conn`` and opens its transaction through this, so one method serves both the single
    change and the change that commits together with its audit event.
    """
    if conn is not None:
        yield conn
        return
    async with tenant_transaction(pool, tenant_id) as own:
        yield own


class PostgresHistoryStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def append(
        self, tenant_id: str, conversation_id: str, step_key: str, role: str, content: list[dict]
    ) -> int:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                INSERT INTO conversation_messages
                  (tenant_id, conversation_id, seq, step_key, role, content, char_len)
                SELECT %s, %s, COALESCE(MAX(seq), 0) + 1, %s, %s, %s, %s
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
                    char_len({"role": role, "content": content}),
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
        async with tenant_transaction(self._pool, tenant_id) as conn:
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

    async def load_window(
        self, tenant_id: str, conversation_id: str, budget: int = HISTORY_BUDGET_CHARS
    ) -> list[dict]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                _WINDOW_START, {"t": tenant_id, "c": conversation_id, "budget": budget}
            )
            (start,) = await cur.fetchone()
            cur = await conn.execute(
                """
                SELECT role, content FROM conversation_messages
                WHERE tenant_id = %s AND conversation_id = %s AND seq >= %s
                ORDER BY seq
                """,
                (tenant_id, conversation_id, start),
            )
            rows = await cur.fetchall()
            return [{"role": role, "content": content} for role, content in rows]


# A user message with text and no tool result: the only place the window may be cut. The same
# test as ``history_view._starts_a_turn``, which also ignores text blocks with no visible text.
_STARTS_A_TURN = """
  m.role = 'user' AND jsonb_typeof(m.content) = 'array'
  AND EXISTS (SELECT 1 FROM jsonb_array_elements(m.content) b
              WHERE b ->> 'type' = 'text' AND b ->> 'text' ~ '\\S')
  AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(m.content) b
                  WHERE b ->> 'type' = 'tool_result')
"""

# The window start, from the sizes alone: the whole thread when it fits the budget; otherwise
# the earliest turn start whose rows from there on fit; otherwise the newest turn start; and
# the whole thread when it has none. Only seq and char_len are read for rows outside the window.
_WINDOW_START = f"""
WITH sizes AS (
  SELECT seq, SUM(char_len) OVER (ORDER BY seq DESC) AS kept
  FROM conversation_messages
  WHERE tenant_id = %(t)s AND conversation_id = %(c)s
)
SELECT CASE WHEN (SELECT COALESCE(MAX(kept), 0) FROM sizes) <= %(budget)s THEN 0 ELSE COALESCE(
  (SELECT MIN(m.seq) FROM conversation_messages m JOIN sizes s ON s.seq = m.seq
   WHERE m.tenant_id = %(t)s AND m.conversation_id = %(c)s AND s.kept <= %(budget)s
     AND {_STARTS_A_TURN}),
  (SELECT MAX(m.seq) FROM conversation_messages m
   WHERE m.tenant_id = %(t)s AND m.conversation_id = %(c)s AND {_STARTS_A_TURN}),
  0) END
"""


class PostgresIdempotencyStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def get(self, tenant_id: str, key: str) -> dict | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                "SELECT value FROM idempotency_records WHERE tenant_id = %s AND idem_key = %s",
                (tenant_id, key),
            )
            row = await cur.fetchone()
            return row[0] if row else None

    async def put_if_absent(self, tenant_id: str, key: str, value: dict) -> dict:
        async with tenant_transaction(self._pool, tenant_id) as conn:
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


@dataclass(frozen=True)
class WriteClaim:
    """The outcome of ``ExternalWriteStore.claim``.

    ``claimed``: the caller holds the lease, named by ``token``, and may write. ``issued``
    is true when an earlier attempt got as far as sending the write, so its result may
    exist in the target system even though none was stored. ``done``: the write finished
    and ``result_ref`` is its stored result. ``busy``: another attempt holds a live lease;
    the caller must write nothing and try again later.
    """

    status: Literal["claimed", "done", "busy"]
    issued: bool = False
    result_ref: str | None = None
    token: str = ""


class ExternalWriteStore(Protocol):
    """Who may write to an external system for one key, and what the write returned.

    One row per (tenant, key). A write goes ``claimed`` (a lease is held) to
    ``issued`` (the write was sent) to ``done`` (its result is stored). Every claim
    mints a new token. A step taken with a token that is no longer the row's current
    one (the lease ran out and another attempt took over) changes nothing. The store
    holds only keys and a small result reference, never content.
    """

    async def claim(self, tenant_id: str, idem_key: str, lease_seconds: float) -> WriteClaim:
        """Take the key, or take it over once its lease has expired, in one step."""
        ...

    async def mark_issued(
        self, tenant_id: str, idem_key: str, token: str, lease_seconds: float
    ) -> bool:
        """Record that the write is about to be sent, and extend the lease.

        False when ``token`` no longer holds the lease: the caller must not write.
        """
        ...

    async def complete(
        self, tenant_id: str, idem_key: str, token: str, result_ref: str
    ) -> str | None:
        """Store the result for the holder of ``token``; the first one stored wins.

        Returns the stored result, or None when ``token`` does not hold the lease and
        nothing is stored yet.
        """
        ...

    async def release(self, tenant_id: str, idem_key: str, token: str) -> None:
        """End the lease now if ``token`` still holds it, keeping what is known."""
        ...

    async def result(self, tenant_id: str, idem_key: str) -> str | None:
        """The stored result, or None while the write is not done."""
        ...


class PostgresExternalWriteStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def claim(self, tenant_id: str, idem_key: str, lease_seconds: float) -> WriteClaim:
        token = uuid.uuid4().hex
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                INSERT INTO external_writes (tenant_id, idem_key, state, lease_until, lease_token)
                VALUES (%s, %s, 'claimed', now() + make_interval(secs => %s), %s)
                ON CONFLICT (tenant_id, idem_key) DO UPDATE
                  SET lease_until = EXCLUDED.lease_until, lease_token = EXCLUDED.lease_token,
                      updated_at = now()
                  WHERE external_writes.state <> 'done' AND external_writes.lease_until <= now()
                RETURNING state
                """,
                (tenant_id, idem_key, lease_seconds, token),
            )
            row = await cur.fetchone()
            if row is not None:
                return WriteClaim("claimed", issued=row[0] == "issued", token=token)
            cur = await conn.execute(
                "SELECT state, result_ref FROM external_writes WHERE tenant_id = %s AND idem_key = %s",
                (tenant_id, idem_key),
            )
            state, result_ref = await cur.fetchone()
            if state == "done":
                return WriteClaim("done", result_ref=result_ref)
            return WriteClaim("busy")

    async def mark_issued(
        self, tenant_id: str, idem_key: str, token: str, lease_seconds: float
    ) -> bool:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                UPDATE external_writes
                SET state = 'issued', lease_until = now() + make_interval(secs => %s),
                    updated_at = now()
                WHERE tenant_id = %s AND idem_key = %s AND lease_token = %s
                  AND state <> 'done' AND lease_until > now()
                """,
                (lease_seconds, tenant_id, idem_key, token),
            )
            return cur.rowcount == 1

    async def complete(
        self, tenant_id: str, idem_key: str, token: str, result_ref: str
    ) -> str | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                """
                UPDATE external_writes
                SET state = 'done', result_ref = %s, lease_until = now(), updated_at = now()
                WHERE tenant_id = %s AND idem_key = %s AND lease_token = %s AND state <> 'done'
                """,
                (result_ref, tenant_id, idem_key, token),
            )
            cur = await conn.execute(
                "SELECT result_ref FROM external_writes WHERE tenant_id = %s AND idem_key = %s",
                (tenant_id, idem_key),
            )
            row = await cur.fetchone()
            return row[0] if row else None

    async def release(self, tenant_id: str, idem_key: str, token: str) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                """
                UPDATE external_writes SET lease_until = now(), updated_at = now()
                WHERE tenant_id = %s AND idem_key = %s AND lease_token = %s AND state <> 'done'
                """,
                (tenant_id, idem_key, token),
            )

    async def result(self, tenant_id: str, idem_key: str) -> str | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                SELECT result_ref FROM external_writes
                WHERE tenant_id = %s AND idem_key = %s AND state = 'done'
                """,
                (tenant_id, idem_key),
            )
            row = await cur.fetchone()
            return row[0] if row else None


class PostgresToolOutputStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def put(
        self, tenant_id: str, conversation_id: str, tool_use_id: str, content: str, is_error: bool
    ) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
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
        async with tenant_transaction(self._pool, tenant_id) as conn:
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
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                """
                INSERT INTO task_links (tenant_id, page_id, task_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (tenant_id, page_id) DO NOTHING
                """,
                (tenant_id, page_id, task_id),
            )

    async def task_for_page(self, tenant_id: str, page_id: str) -> str | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
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

    async def load_window(
        self, tenant_id: str, conversation_id: str, budget: int = HISTORY_BUDGET_CHARS
    ) -> list[dict]:
        return window(await self.load(tenant_id, conversation_id), budget)


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


class InMemoryExternalWriteStore:
    """For tests. ``clock`` lets a test move time to expire a lease."""

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or (lambda: datetime.now(UTC))
        self.rows: dict[tuple[str, str], dict] = {}

    async def claim(self, tenant_id: str, idem_key: str, lease_seconds: float) -> WriteClaim:
        now = self._clock()
        token = uuid.uuid4().hex
        row = self.rows.get((tenant_id, idem_key))
        if row is None:
            self.rows[(tenant_id, idem_key)] = {
                "state": "claimed",
                "result_ref": None,
                "lease_until": now + timedelta(seconds=lease_seconds),
                "token": token,
            }
            return WriteClaim("claimed", token=token)
        if row["state"] == "done":
            return WriteClaim("done", result_ref=row["result_ref"])
        if row["lease_until"] > now:
            return WriteClaim("busy")
        row["lease_until"] = now + timedelta(seconds=lease_seconds)
        row["token"] = token
        return WriteClaim("claimed", issued=row["state"] == "issued", token=token)

    def _holds(self, tenant_id: str, idem_key: str, token: str) -> dict | None:
        row = self.rows.get((tenant_id, idem_key))
        return row if row and row["token"] == token and row["state"] != "done" else None

    async def mark_issued(
        self, tenant_id: str, idem_key: str, token: str, lease_seconds: float
    ) -> bool:
        row = self._holds(tenant_id, idem_key, token)
        now = self._clock()
        if row is None or row["lease_until"] <= now:
            return False
        row["state"] = "issued"
        row["lease_until"] = now + timedelta(seconds=lease_seconds)
        return True

    async def complete(
        self, tenant_id: str, idem_key: str, token: str, result_ref: str
    ) -> str | None:
        row = self._holds(tenant_id, idem_key, token)
        if row is not None:
            row.update(state="done", result_ref=result_ref, lease_until=self._clock())
        stored = self.rows.get((tenant_id, idem_key))
        return stored["result_ref"] if stored else None

    async def release(self, tenant_id: str, idem_key: str, token: str) -> None:
        row = self._holds(tenant_id, idem_key, token)
        if row is not None:
            row["lease_until"] = self._clock()

    async def result(self, tenant_id: str, idem_key: str) -> str | None:
        row = self.rows.get((tenant_id, idem_key))
        return row["result_ref"] if row and row["state"] == "done" else None


class InMemoryTaskLinkStore:
    def __init__(self) -> None:
        self.links: dict[tuple[str, str], str] = {}

    async def link(self, tenant_id: str, page_id: str, task_id: str) -> None:
        self.links.setdefault((tenant_id, page_id), task_id)

    async def task_for_page(self, tenant_id: str, page_id: str) -> str | None:
        return self.links.get((tenant_id, page_id))
