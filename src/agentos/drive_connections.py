"""The tenant's Google Drive connection (D-45): one service account per tenant.

The row is the connection's identity and last check, not a credential: the account's
email and status are not secret, and no key is stored per tenant. The deployment's
service account (a deployment secret in the settings) creates the tenant's account and
mints short-lived tokens for it; this table holds which account belongs to the tenant,
whether the last check of what it can see succeeded, and when.

The console writes the row in the same unit of work as its ``connector`` audit event
(``drive_connect`` / ``drive_disconnect``), under the tenant lock that also covers the
Google call. The settings page reads the row and lists what the account can see without
writing anything: the live check's outcome is shown, not stored. ``record_check`` is for
a later background check of the connection. Disconnect and uninstall delete the row.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.stores import joined_transaction

CONNECTED = "connected"
ERROR = "error"


@dataclass(frozen=True)
class DriveConnection:
    """One tenant's Drive account and its last check. ``last_error`` is a short code
    (``google_iam.PROBLEM_TEXT`` words it), never a message from Google."""

    tenant_id: str
    service_account_email: str
    status: str
    last_error: str | None
    connected_at: datetime
    checked_at: datetime | None = None


class DriveConnectionStore(Protocol):
    async def get(self, tenant_id: str, conn: Any = None) -> DriveConnection | None:
        """The tenant's connection, or None when Drive was never connected. With
        ``conn`` the read joins the unit of work's transaction."""
        ...

    async def connect(self, tenant_id: str, email: str, at: datetime, conn: Any = None) -> None:
        """Store the tenant's account as connected, replacing any earlier row.

        A reconnect keeps the original ``connected_at`` and clears the last error.
        With ``conn`` (a unit of work's connection) the row commits with that
        transaction, alongside its audit event.
        """
        ...

    async def record_check(
        self, tenant_id: str, status: str, last_error: str | None, at: datetime, conn: Any = None
    ) -> None:
        """A background check's outcome and time (a later Drive-read poll writes it).
        A tenant with no row is unchanged."""
        ...

    async def delete(self, tenant_id: str, conn: Any = None) -> bool:
        """Remove the tenant's connection. False when there was none."""
        ...


class InMemoryDriveConnectionStore:
    def __init__(self) -> None:
        self._rows: dict[str, DriveConnection] = {}

    def snapshot(self) -> dict[str, DriveConnection]:
        """The rows as they are now, for ``InMemoryUnitOfWork``."""
        return dict(self._rows)

    def restore(self, state: dict[str, DriveConnection]) -> None:
        self._rows = dict(state)

    async def get(self, tenant_id: str, conn: Any = None) -> DriveConnection | None:
        return self._rows.get(tenant_id)

    async def connect(self, tenant_id: str, email: str, at: datetime, conn: Any = None) -> None:
        earlier = self._rows.get(tenant_id)
        self._rows[tenant_id] = DriveConnection(
            tenant_id=tenant_id,
            service_account_email=email,
            status=CONNECTED,
            last_error=None,
            connected_at=earlier.connected_at if earlier is not None else at,
        )

    async def record_check(
        self, tenant_id: str, status: str, last_error: str | None, at: datetime, conn: Any = None
    ) -> None:
        row = self._rows.get(tenant_id)
        if row is not None:
            self._rows[tenant_id] = replace(
                row, status=status, last_error=last_error, checked_at=at
            )

    async def delete(self, tenant_id: str, conn: Any = None) -> bool:
        return self._rows.pop(tenant_id, None) is not None


_COLUMNS = "tenant_id, service_account_email, status, last_error, connected_at, checked_at"


class PostgresDriveConnectionStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def get(self, tenant_id: str, conn: Any = None) -> DriveConnection | None:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            cur = await db.execute(
                f"SELECT {_COLUMNS} FROM drive_connections WHERE tenant_id = %s",
                (tenant_id,),
            )
            row = await cur.fetchone()
        return None if row is None else DriveConnection(*row)

    async def connect(self, tenant_id: str, email: str, at: datetime, conn: Any = None) -> None:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            await db.execute(
                """
                INSERT INTO drive_connections
                  (tenant_id, service_account_email, status, last_error, connected_at)
                VALUES (%s, %s, 'connected', NULL, %s)
                ON CONFLICT (tenant_id) DO UPDATE SET
                  service_account_email = EXCLUDED.service_account_email,
                  status = 'connected', last_error = NULL, checked_at = NULL
                """,
                (tenant_id, email, at),
            )

    async def record_check(
        self, tenant_id: str, status: str, last_error: str | None, at: datetime, conn: Any = None
    ) -> None:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            await db.execute(
                "UPDATE drive_connections SET status = %s, last_error = %s, checked_at = %s "
                "WHERE tenant_id = %s",
                (status, last_error, at, tenant_id),
            )

    async def delete(self, tenant_id: str, conn: Any = None) -> bool:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            cur = await db.execute(
                "DELETE FROM drive_connections WHERE tenant_id = %s RETURNING tenant_id",
                (tenant_id,),
            )
            return await cur.fetchone() is not None
