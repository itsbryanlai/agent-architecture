"""The audit log: an append-only, per-tenant record of what agents did.

It answers who asked, which agent and version acted, whose access was used,
which tool ran, what it read or changed, and who approved. An event holds IDs
and SHA-256 hashes only, never chat text, document text or a tool's input or
output: the hash of the canonical JSON proves what was approved or returned
without keeping it.

Events are written by the activities that do the work, under an event key
built in ``ids.py``, so an activity that retries records its event once. The
database refuses UPDATE and DELETE on the table for every role; retention is
not enforced yet.

List a tenant's events, newest first, with::

    uv run python -m agentos.audit --tenant <id> --limit 50
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import hashlib
import json
import sys
from datetime import UTC, datetime
from typing import Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.models import AuditEvent
from agentos.stores import tenant_transaction

MAX_LIMIT = 1000

_COLUMNS = (
    "tenant_id, event_key, at, kind, agent_id, agent_version, requester_user_id, identity, "
    "conversation_id, task_id, tool, target, input_sha256, output_sha256, approver_user_id, "
    "outcome"
)
_COLUMN_NAMES = [c.strip() for c in _COLUMNS.split(",")]
_READ_COLUMNS = _COLUMNS + ", seq"
_READ_COLUMN_NAMES = _COLUMN_NAMES + ["seq"]


def canonical_sha256(value: object) -> str:
    """SHA-256 of the canonical JSON of ``value`` (sorted keys, no spaces, UTF-8)."""
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def agent_identity(agent_id: str) -> str:
    """The identity an agent's tool calls run as: its own, never the requester's."""
    return f"agent:{agent_id}"


class AuditStore(Protocol):
    async def record(self, event: AuditEvent) -> None:
        """Append ``event``. A repeat with the same (tenant_id, event_key) stores nothing."""
        ...

    async def list(
        self,
        tenant_id: str,
        limit: int = 50,
        before: datetime | None = None,
        before_seq: int | None = None,
    ) -> list[AuditEvent]:
        """A tenant's events, newest first.

        ``before`` keeps only events older than that time. ``before_seq`` keeps
        only events recorded before the one with that ``seq``, so a reader can
        page through the log without skipping or repeating an event.
        """
        ...


def _check_limit(limit: int) -> int:
    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_LIMIT}")
    return limit


class PostgresAuditStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def record(self, event: AuditEvent) -> None:
        async with tenant_transaction(self._pool, event.tenant_id) as conn:
            await conn.execute(
                """
                INSERT INTO audit_events (
                  tenant_id, event_key, kind, agent_id, agent_version, requester_user_id,
                  identity, conversation_id, task_id, tool, target, input_sha256,
                  output_sha256, approver_user_id, outcome
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, event_key) DO NOTHING
                """,
                (
                    event.tenant_id,
                    event.event_key,
                    event.kind,
                    event.agent_id,
                    event.agent_version,
                    event.requester_user_id,
                    event.identity,
                    event.conversation_id,
                    event.task_id,
                    event.tool,
                    event.target,
                    event.input_sha256,
                    event.output_sha256,
                    event.approver_user_id,
                    event.outcome,
                ),
            )

    async def list(
        self,
        tenant_id: str,
        limit: int = 50,
        before: datetime | None = None,
        before_seq: int | None = None,
    ) -> list[AuditEvent]:
        _check_limit(limit)
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"""
                SELECT {_READ_COLUMNS} FROM audit_events
                WHERE tenant_id = %s
                  AND (%s::timestamptz IS NULL OR at < %s)
                  AND (%s::bigint IS NULL OR seq < %s)
                ORDER BY seq DESC
                LIMIT %s
                """,
                (tenant_id, before, before, before_seq, before_seq, limit),
            )
            rows = await cur.fetchall()
        return [AuditEvent(**dict(zip(_READ_COLUMN_NAMES, row))) for row in rows]


class InMemoryAuditStore:
    def __init__(self) -> None:
        self.events: list[AuditEvent] = []  # in the order recorded
        self._keys: set[tuple[str, str]] = set()
        self._next_seq = 1
        self._fail_next_record = 0

    # Test hook.

    def fail_next_records(self, count: int = 1) -> None:
        """Make the next ``count`` record calls raise a retryable error, storing nothing."""
        self._fail_next_record = count

    async def record(self, event: AuditEvent) -> None:
        if self._fail_next_record:
            self._fail_next_record -= 1
            raise RuntimeError("simulated audit store outage")
        key = (event.tenant_id, event.event_key)
        if key in self._keys:
            return
        self._keys.add(key)
        self.events.append(dataclasses.replace(event, at=datetime.now(UTC), seq=self._next_seq))
        self._next_seq += 1

    async def list(
        self,
        tenant_id: str,
        limit: int = 50,
        before: datetime | None = None,
        before_seq: int | None = None,
    ) -> list[AuditEvent]:
        _check_limit(limit)
        mine = [
            e
            for e in self.events
            if e.tenant_id == tenant_id
            and (before is None or (e.at and e.at < before))
            and (before_seq is None or (e.seq is not None and e.seq < before_seq))
        ]
        return list(reversed(mine))[:limit]


def event_json(event: AuditEvent) -> str:
    row = dataclasses.asdict(event)
    row["at"] = event.at.isoformat() if event.at else None
    return json.dumps(row, sort_keys=True)


async def run(tenant: str, limit: int, before: datetime | None) -> int:
    from agentos.config import load_settings

    settings = load_settings()
    pool = AsyncConnectionPool(settings.database_url, open=False)
    await pool.open()
    try:
        for event in await PostgresAuditStore(pool).list(tenant, limit, before):
            print(event_json(event))
    finally:
        await pool.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Print a tenant's audit events as JSON lines.")
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument(
        "--before", type=datetime.fromisoformat, help="only events older than this ISO timestamp"
    )
    args = parser.parse_args()
    try:
        _check_limit(args.limit)
    except ValueError as err:
        parser.error(str(err))
    sys.exit(asyncio.run(run(args.tenant, args.limit, args.before)))


if __name__ == "__main__":
    main()
