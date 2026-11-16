"""Where chat and document content lives while Temporal history carries only references.

A signal, a workflow argument and an activity argument hold IDs and small values. The text
people wrote, the text the model returned, the input of each tool call and the payload of a
task are kept here, by those IDs, in Postgres (``scripts/migrations/022_content_refs.sql``):

* ``inbound_events``: a Slack event the gateway has acknowledged, until the conversation
  workflow has taken it; also the values of a saved Edit form.
* ``model_steps``: what the model returned for one step of a turn, written before anything
  else is done with it, so a retried model call reads it instead of asking again. A final
  reply stays here until the activity that shows it has stored it in history.
* ``tool_inputs``: the input of each tool call, by tool-use ID.
* ``task_drafts``: the frozen payload of a task, one row per revision. A revision is never
  changed: an edit adds the next one.

Every method takes ``tenant_id`` and runs under the tenant's row-level security, like the
other stores.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from agentos.stores import tenant_transaction


@dataclass(frozen=True)
class StoredStep:
    """One step of a turn as the model returned it."""

    stop_reason: str
    content: list[dict]  # the assistant content blocks
    reply_text: str | None = None  # what a final reply shows; None when the step called tools


@dataclass(frozen=True)
class InboundRow:
    event_id: str
    payload: dict
    attempts: int
    next_attempt_at: datetime


class ContentStore(Protocol):
    async def put_inbound(
        self, tenant_id: str, event_id: str, payload: dict, delivered: bool = False
    ) -> bool:
        """Store an inbound event. False when the event ID is already stored (a Slack retry)."""
        ...

    async def get_inbound(self, tenant_id: str, event_id: str) -> dict | None: ...

    async def undelivered(self, tenant_id: str, limit: int) -> list[InboundRow]:
        """The events not yet delivered to their workflow, oldest first."""
        ...

    async def consume_inbound(
        self, tenant_id: str, event_id: str, stub: dict, now: datetime
    ) -> None:
        """Replace an event's payload by ``stub`` (no content) once its content was taken."""
        ...

    async def mark_delivered(self, tenant_id: str, event_id: str, now: datetime) -> None: ...

    async def delay_inbound(
        self, tenant_id: str, event_id: str, attempts: int, retry_at: datetime
    ) -> None:
        """Record a failed delivery: the number of attempts so far and when to try again."""
        ...

    async def get_step(
        self, tenant_id: str, conversation_id: str, step_key: str
    ) -> StoredStep | None: ...

    async def put_step(
        self, tenant_id: str, conversation_id: str, step_key: str, step: StoredStep
    ) -> StoredStep:
        """Store ``step`` unless the key exists; return whichever step is stored."""
        ...

    async def put_tool_input(
        self, tenant_id: str, conversation_id: str, tool_use_id: str, tool_input: object
    ) -> None:
        """Store a tool call's input. The first write for a tool_use_id wins."""
        ...

    async def get_tool_input(
        self, tenant_id: str, conversation_id: str, tool_use_id: str
    ) -> tuple[bool, object]:
        """``(found, input)`` for the tool call; ``found`` is False when none is stored."""
        ...

    async def put_draft(self, tenant_id: str, task_id: str, revision: int, payload: dict) -> None:
        """Store a revision of a task's payload. The first write for a revision wins."""
        ...

    async def get_draft(self, tenant_id: str, task_id: str, revision: int) -> dict | None: ...


class PostgresContentStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def put_inbound(
        self, tenant_id: str, event_id: str, payload: dict, delivered: bool = False
    ) -> bool:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                INSERT INTO inbound_events (tenant_id, event_id, payload, delivered_at)
                VALUES (%s, %s, %s, CASE WHEN %s THEN now() END)
                ON CONFLICT (tenant_id, event_id) DO NOTHING
                """,
                (tenant_id, event_id, Jsonb(payload), delivered),
            )
            return cur.rowcount == 1

    async def get_inbound(self, tenant_id: str, event_id: str) -> dict | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                "SELECT payload FROM inbound_events WHERE tenant_id = %s AND event_id = %s",
                (tenant_id, event_id),
            )
            row = await cur.fetchone()
            return row[0] if row else None

    async def undelivered(self, tenant_id: str, limit: int) -> list[InboundRow]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                SELECT event_id, payload, attempts, next_attempt_at FROM inbound_events
                WHERE tenant_id = %s AND delivered_at IS NULL
                ORDER BY received_at, event_id LIMIT %s
                """,
                (tenant_id, limit),
            )
            return [InboundRow(*row) for row in await cur.fetchall()]

    async def consume_inbound(
        self, tenant_id: str, event_id: str, stub: dict, now: datetime
    ) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                """
                UPDATE inbound_events SET payload = %s, consumed_at = %s
                WHERE tenant_id = %s AND event_id = %s
                """,
                (Jsonb(stub), now, tenant_id, event_id),
            )

    async def mark_delivered(self, tenant_id: str, event_id: str, now: datetime) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                """
                UPDATE inbound_events SET delivered_at = %s
                WHERE tenant_id = %s AND event_id = %s AND delivered_at IS NULL
                """,
                (now, tenant_id, event_id),
            )

    async def delay_inbound(
        self, tenant_id: str, event_id: str, attempts: int, retry_at: datetime
    ) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                """
                UPDATE inbound_events SET attempts = %s, next_attempt_at = %s
                WHERE tenant_id = %s AND event_id = %s AND delivered_at IS NULL
                """,
                (attempts, retry_at, tenant_id, event_id),
            )

    async def get_step(
        self, tenant_id: str, conversation_id: str, step_key: str
    ) -> StoredStep | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            return await _read_step(conn, tenant_id, conversation_id, step_key)

    async def put_step(
        self, tenant_id: str, conversation_id: str, step_key: str, step: StoredStep
    ) -> StoredStep:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                """
                INSERT INTO model_steps
                  (tenant_id, conversation_id, step_key, stop_reason, content, reply_text)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, conversation_id, step_key) DO NOTHING
                """,
                (
                    tenant_id,
                    conversation_id,
                    step_key,
                    step.stop_reason,
                    Jsonb(step.content),
                    step.reply_text,
                ),
            )
            stored = await _read_step(conn, tenant_id, conversation_id, step_key)
            assert stored is not None
            return stored

    async def put_tool_input(
        self, tenant_id: str, conversation_id: str, tool_use_id: str, tool_input: object
    ) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                """
                INSERT INTO tool_inputs (tenant_id, conversation_id, tool_use_id, input)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (tenant_id, conversation_id, tool_use_id) DO NOTHING
                """,
                (tenant_id, conversation_id, tool_use_id, Jsonb(tool_input)),
            )

    async def get_tool_input(
        self, tenant_id: str, conversation_id: str, tool_use_id: str
    ) -> tuple[bool, object]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                SELECT input FROM tool_inputs
                WHERE tenant_id = %s AND conversation_id = %s AND tool_use_id = %s
                """,
                (tenant_id, conversation_id, tool_use_id),
            )
            row = await cur.fetchone()
            return (True, row[0]) if row else (False, None)

    async def put_draft(self, tenant_id: str, task_id: str, revision: int, payload: dict) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                """
                INSERT INTO task_drafts (tenant_id, task_id, revision, payload)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (tenant_id, task_id, revision) DO NOTHING
                """,
                (tenant_id, task_id, revision, Jsonb(payload)),
            )

    async def get_draft(self, tenant_id: str, task_id: str, revision: int) -> dict | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                SELECT payload FROM task_drafts
                WHERE tenant_id = %s AND task_id = %s AND revision = %s
                """,
                (tenant_id, task_id, revision),
            )
            row = await cur.fetchone()
            return row[0] if row else None


async def _read_step(conn, tenant_id: str, conversation_id: str, step_key: str):
    cur = await conn.execute(
        """
        SELECT stop_reason, content, reply_text FROM model_steps
        WHERE tenant_id = %s AND conversation_id = %s AND step_key = %s
        """,
        (tenant_id, conversation_id, step_key),
    )
    row = await cur.fetchone()
    return StoredStep(row[0], row[1], row[2]) if row else None


class InMemoryContentStore:
    """For tests. ``inbound`` and the other dicts are open to assertions."""

    def __init__(self) -> None:
        self.inbound: dict[tuple[str, str], dict] = {}
        self.delivered: dict[tuple[str, str], datetime] = {}
        self.attempts: dict[tuple[str, str], tuple[int, datetime]] = {}
        self.order: list[tuple[str, str]] = []
        self.steps: dict[tuple[str, str, str], StoredStep] = {}
        self.tool_inputs: dict[tuple[str, str, str], object] = {}
        self.drafts: dict[tuple[str, str, int], dict] = {}

    async def put_inbound(
        self, tenant_id: str, event_id: str, payload: dict, delivered: bool = False
    ) -> bool:
        key = (tenant_id, event_id)
        if key in self.inbound:
            return False
        self.inbound[key] = copy.deepcopy(payload)
        self.order.append(key)
        if delivered:
            self.delivered[key] = datetime.now(UTC)
        return True

    async def get_inbound(self, tenant_id: str, event_id: str) -> dict | None:
        found = self.inbound.get((tenant_id, event_id))
        return copy.deepcopy(found) if found is not None else None

    async def undelivered(self, tenant_id: str, limit: int) -> list[InboundRow]:
        epoch = datetime.min.replace(tzinfo=UTC)
        rows = []
        for key in self.order:
            if key[0] != tenant_id or key in self.delivered:
                continue
            attempts, retry_at = self.attempts.get(key, (0, epoch))
            rows.append(InboundRow(key[1], copy.deepcopy(self.inbound[key]), attempts, retry_at))
        return rows[:limit]

    async def consume_inbound(
        self, tenant_id: str, event_id: str, stub: dict, now: datetime
    ) -> None:
        self.inbound[(tenant_id, event_id)] = copy.deepcopy(stub)

    async def mark_delivered(self, tenant_id: str, event_id: str, now: datetime) -> None:
        self.delivered.setdefault((tenant_id, event_id), now)

    async def delay_inbound(
        self, tenant_id: str, event_id: str, attempts: int, retry_at: datetime
    ) -> None:
        self.attempts[(tenant_id, event_id)] = (attempts, retry_at)

    async def get_step(
        self, tenant_id: str, conversation_id: str, step_key: str
    ) -> StoredStep | None:
        return self.steps.get((tenant_id, conversation_id, step_key))

    async def put_step(
        self, tenant_id: str, conversation_id: str, step_key: str, step: StoredStep
    ) -> StoredStep:
        return self.steps.setdefault((tenant_id, conversation_id, step_key), step)

    async def put_tool_input(
        self, tenant_id: str, conversation_id: str, tool_use_id: str, tool_input: object
    ) -> None:
        self.tool_inputs.setdefault(
            (tenant_id, conversation_id, tool_use_id), copy.deepcopy(tool_input)
        )

    async def get_tool_input(
        self, tenant_id: str, conversation_id: str, tool_use_id: str
    ) -> tuple[bool, object]:
        key = (tenant_id, conversation_id, tool_use_id)
        if key not in self.tool_inputs:
            return False, None
        return True, copy.deepcopy(self.tool_inputs[key])

    async def put_draft(self, tenant_id: str, task_id: str, revision: int, payload: dict) -> None:
        self.drafts.setdefault((tenant_id, task_id, revision), copy.deepcopy(payload))

    async def get_draft(self, tenant_id: str, task_id: str, revision: int) -> dict | None:
        found = self.drafts.get((tenant_id, task_id, revision))
        return copy.deepcopy(found) if found is not None else None


def backoff(attempts: int, now: datetime, base: float = 1.0, cap: float = 300.0) -> datetime:
    """When to try a delivery again after ``attempts`` failures: doubling, capped."""
    return now + timedelta(seconds=min(cap, base * 2 ** max(0, attempts - 1)))
