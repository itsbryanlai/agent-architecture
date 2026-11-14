"""Web console sessions: one row per signed-in browser, kept server-side.

The browser holds a random 256-bit session ID. Only its SHA-256 is stored, so a
copy of the table cannot be used to sign in. A session names the Slack user who
signed in and the tenant their workspace maps to, and carries the CSRF secret
that every form of that session must repeat. It never changes after it is
created, and it is refused once ``expires_at`` has passed.

Every lookup is scoped to one tenant by row-level security, so the caller names
the tenant it expects; a session of another tenant is simply not found.
"""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.stores import tenant_transaction

SESSION_LIFETIME = timedelta(hours=8)


def session_hash(token: str) -> str:
    """What is stored for a session ID: its SHA-256, in hex."""
    return hashlib.sha256(token.encode()).hexdigest()


@dataclass(frozen=True)
class SessionRecord:
    tenant_id: str
    user_id: str
    csrf_secret: str
    created_at: datetime
    expires_at: datetime


@dataclass(frozen=True)
class IssuedSession:
    """A new session and the session ID to give the browser. The ID is not stored."""

    token: str
    record: SessionRecord


class SessionStore(Protocol):
    async def create(
        self, tenant_id: str, user_id: str, now: datetime, lifetime: timedelta = SESSION_LIFETIME
    ) -> IssuedSession:
        """Store a new session and drop the tenant's expired ones."""
        ...

    async def get(self, tenant_id: str, token: str, now: datetime) -> SessionRecord | None:
        """The live session for this ID in this tenant, or None (unknown or expired)."""
        ...

    async def delete(self, tenant_id: str, token: str) -> None:
        """Sign out. Deleting a session that is not there is not an error."""
        ...


def _new_session(tenant_id: str, user_id: str, now: datetime, lifetime: timedelta) -> IssuedSession:
    record = SessionRecord(
        tenant_id=tenant_id,
        user_id=user_id,
        csrf_secret=secrets.token_urlsafe(32),
        created_at=now,
        expires_at=now + lifetime,
    )
    return IssuedSession(secrets.token_urlsafe(32), record)


class PostgresSessionStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def create(
        self, tenant_id: str, user_id: str, now: datetime, lifetime: timedelta = SESSION_LIFETIME
    ) -> IssuedSession:
        issued = _new_session(tenant_id, user_id, now, lifetime)
        record = issued.record
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                "DELETE FROM sessions WHERE tenant_id = %s AND expires_at <= %s",
                (tenant_id, now),
            )
            await conn.execute(
                """
                INSERT INTO sessions
                  (tenant_id, session_hash, user_id, csrf_secret, created_at, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    tenant_id,
                    session_hash(issued.token),
                    user_id,
                    record.csrf_secret,
                    record.created_at,
                    record.expires_at,
                ),
            )
        return issued

    async def get(self, tenant_id: str, token: str, now: datetime) -> SessionRecord | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                SELECT tenant_id, user_id, csrf_secret, created_at, expires_at
                FROM sessions
                WHERE tenant_id = %s AND session_hash = %s AND expires_at > %s
                """,
                (tenant_id, session_hash(token), now),
            )
            row = await cur.fetchone()
        return None if row is None else SessionRecord(*row)

    async def delete(self, tenant_id: str, token: str) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                "DELETE FROM sessions WHERE tenant_id = %s AND session_hash = %s",
                (tenant_id, session_hash(token)),
            )


class InMemorySessionStore:
    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], SessionRecord] = {}

    async def create(
        self, tenant_id: str, user_id: str, now: datetime, lifetime: timedelta = SESSION_LIFETIME
    ) -> IssuedSession:
        issued = _new_session(tenant_id, user_id, now, lifetime)
        self._rows = {
            key: row
            for key, row in self._rows.items()
            if not (key[0] == tenant_id and row.expires_at <= now)
        }
        self._rows[(tenant_id, session_hash(issued.token))] = issued.record
        return issued

    async def get(self, tenant_id: str, token: str, now: datetime) -> SessionRecord | None:
        row = self._rows.get((tenant_id, session_hash(token)))
        return row if row is not None and row.expires_at > now else None

    async def delete(self, tenant_id: str, token: str) -> None:
        self._rows.pop((tenant_id, session_hash(token)), None)
