"""The tenant's Google Drive connection (D-45, D-53): the account the agent reads Drive as.

The row is the connection's identity and last check, not a credential: the account's
email and status are not secret. By default (``kind`` ``service_account``) the account is a
service account the deployment creates for the tenant and mints short-lived tokens for; no
key is stored per tenant. An admin may instead connect the agent's own Google user in the
customer's Workspace (``kind`` ``user``): the email is that user's address (the column keeps
its name), and the user's refresh token lives encrypted in the tenant's credentials, not
here. This table holds which account belongs to the tenant, whether the last check of what
it can see succeeded, and when.

The console writes the row in the same unit of work as its ``connector`` audit event
(``drive_connect`` / ``drive_disconnect``), under the tenant lock that also covers the
Google call. The settings page reads the row and lists what the account can see without
writing anything: the live check's outcome is shown, not stored. ``record_check`` is for
a later background check of the connection. Disconnect and uninstall delete the row.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.config import ConfigError
from agentos.connectors.google_iam import GoogleError
from agentos.stores import joined_transaction

if TYPE_CHECKING:
    from agentos.credentials import TenantCredentials

CONNECTED = "connected"
ERROR = "error"
SERVICE_ACCOUNT = "service_account"
USER = "user"


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
    kind: str = SERVICE_ACCOUNT


class DriveConnectionStore(Protocol):
    async def get(self, tenant_id: str, conn: Any = None) -> DriveConnection | None:
        """The tenant's connection, or None when Drive was never connected. With
        ``conn`` the read joins the unit of work's transaction."""
        ...

    async def connect(
        self,
        tenant_id: str,
        email: str,
        at: datetime,
        conn: Any = None,
        kind: str = SERVICE_ACCOUNT,
    ) -> None:
        """Store the tenant's account as connected, replacing any earlier row.

        A reconnect keeps the original ``connected_at`` and clears the last error.
        ``kind`` is ``service_account`` or ``user`` (the agent's own Google user). With
        ``conn`` (a unit of work's connection) the row commits with that transaction,
        alongside its audit event.
        """
        ...

    async def tenant_for_user_email(self, email: str) -> str | None:
        """The tenant whose ``user`` connection names this address, or None. The one read
        across tenants: a Drive call knows only the account's email, and the lookup
        returns the tenant ID and nothing else."""
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

    async def connect(
        self,
        tenant_id: str,
        email: str,
        at: datetime,
        conn: Any = None,
        kind: str = SERVICE_ACCOUNT,
    ) -> None:
        earlier = self._rows.get(tenant_id)
        self._rows[tenant_id] = DriveConnection(
            tenant_id=tenant_id,
            service_account_email=email,
            status=CONNECTED,
            last_error=None,
            connected_at=earlier.connected_at if earlier is not None else at,
            kind=kind,
        )

    async def tenant_for_user_email(self, email: str) -> str | None:
        wanted = email.lower()
        for row in self._rows.values():
            if row.kind == USER and row.service_account_email.lower() == wanted:
                return row.tenant_id
        return None

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


_COLUMNS = "tenant_id, service_account_email, status, last_error, connected_at, checked_at, kind"


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

    async def connect(
        self,
        tenant_id: str,
        email: str,
        at: datetime,
        conn: Any = None,
        kind: str = SERVICE_ACCOUNT,
    ) -> None:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            await db.execute(
                """
                INSERT INTO drive_connections
                  (tenant_id, service_account_email, status, last_error, connected_at, kind)
                VALUES (%s, %s, 'connected', NULL, %s, %s)
                ON CONFLICT (tenant_id) DO UPDATE SET
                  service_account_email = EXCLUDED.service_account_email,
                  status = 'connected', last_error = NULL, checked_at = NULL,
                  kind = EXCLUDED.kind
                """,
                (tenant_id, email, at, kind),
            )

    async def tenant_for_user_email(self, email: str) -> str | None:
        # The directory function reads across tenants (row-level security would hide the
        # row without a tenant) and returns the tenant ID only; no tenant is set here.
        async with self._pool.connection() as db:
            cur = await db.execute("SELECT drive_user_tenant(%s)", (email,))
            row = await cur.fetchone()
        return None if row is None else row[0]

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


def user_refresh_tokens(
    drive: DriveConnectionStore, credentials: TenantCredentials
) -> Callable[[str], Awaitable[str | None]]:
    """The lookup ``RealGoogleAccounts.use_user_tokens`` takes: an agent user's address to
    the refresh token its tenant stored, or None for an address with no user connection or
    a connection with no stored token (the caller refuses both). The tenant comes from the
    address, never from the caller."""

    async def refresh_token_for(email: str) -> str | None:
        tenant_id = await drive.tenant_for_user_email(email)
        if tenant_id is None:
            return None
        try:
            return await credentials.google_user_token(tenant_id)
        except ConfigError as err:
            # The stored sign-in cannot be decrypted now (a key not loaded yet): not a
            # lost sign-in, so not a request to sign in again.
            raise GoogleError("unavailable") from err

    return refresh_token_for
