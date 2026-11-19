"""Per-tenant connector credentials, encrypted in the database with the tenant's payload key.

One row per tenant and kind (``slack_bot`` now; a later ticket adds ``notion``). The
value is stored as nonce || AES-256-GCM under the tenant's key version, with the tenant
ID as associated data — the same cipher the Temporal payloads and the Notion webhook
token use — so a credential stored for one tenant does not decrypt for another. The
credential itself never appears in a log, an error or a ``repr``.

``TenantCredentials.slack_bot_token`` is the one resolver every process reads a tenant's
Slack bot token through: the credential row first, and when no row exists the environment
variable the tenant's settings name (a local single-tenant run keeps working unchanged).
A key the store has not loaded triggers one refresh before refusing, so a tenant installed
seconds ago is served without a restart. A row that cannot be decrypted is a ``ConfigError``
naming the tenant and the key version, never a value: the caller reports it and tries again
on its next pass, rather than silently falling back to another token.

A reinstall replaces the row (``put`` upserts); nothing here deletes a credential.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from cryptography.exceptions import InvalidTag
from psycopg_pool import AsyncConnectionPool

from agentos.config import AgentsConfig, ConfigError
from agentos.crypto_keys import KeyStore, TenantKeyError
from agentos.payload_codec import decrypt_for_tenant, encrypt_with_tenant_key
from agentos.stores import joined_transaction, tenant_transaction
from agentos.tenants import require_tenant_secret

SLACK_BOT = "slack_bot"


@dataclass(frozen=True)
class Credential:
    """One tenant's stored credential: ciphertext, never the value itself."""

    tenant_id: str
    kind: str
    ciphertext: bytes
    key_version: int
    updated_at: datetime | None = None


class CredentialStore(Protocol):
    async def get(self, tenant_id: str, kind: str) -> Credential | None:
        """The tenant's row for this kind, or None."""
        ...

    async def put(
        self, tenant_id: str, kind: str, ciphertext: bytes, key_version: int, conn: Any = None
    ) -> None:
        """Store the credential, replacing any row the tenant already has for this kind.

        ``conn`` is the connection of a unit of work (``unit_of_work.py``): the call then
        joins that transaction instead of opening its own.
        """
        ...


class InMemoryCredentialStore:
    """The same behaviour as Postgres, minus the database: one row per tenant and kind."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], Credential] = {}
        self._tick = datetime(2026, 1, 1, tzinfo=UTC)

    def snapshot(self) -> dict[tuple[str, str], Credential]:
        """The rows as they are now, for ``InMemoryUnitOfWork``."""
        return dict(self._rows)

    def restore(self, state: dict[tuple[str, str], Credential]) -> None:
        self._rows = dict(state)

    async def get(self, tenant_id: str, kind: str) -> Credential | None:
        return self._rows.get((tenant_id, kind))

    async def put(
        self, tenant_id: str, kind: str, ciphertext: bytes, key_version: int, conn: Any = None
    ) -> None:
        self._tick += timedelta(microseconds=1)
        self._rows[(tenant_id, kind)] = Credential(
            tenant_id, kind, ciphertext, key_version, self._tick
        )


_COLUMNS = "tenant_id, kind, ciphertext, key_version, updated_at"


class PostgresCredentialStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def get(self, tenant_id: str, kind: str) -> Credential | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT {_COLUMNS} FROM tenant_credentials WHERE tenant_id = %s AND kind = %s",
                (tenant_id, kind),
            )
            row = await cur.fetchone()
        return None if row is None else Credential(*row)

    async def put(
        self, tenant_id: str, kind: str, ciphertext: bytes, key_version: int, conn: Any = None
    ) -> None:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            await db.execute(
                "INSERT INTO tenant_credentials (tenant_id, kind, ciphertext, key_version) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (tenant_id, kind) DO UPDATE SET "
                "ciphertext = EXCLUDED.ciphertext, key_version = EXCLUDED.key_version, "
                "updated_at = now()",
                (tenant_id, kind, ciphertext, key_version),
            )


class TenantCredentials:
    """Store a tenant's credential encrypted, and resolve its Slack bot token.

    The row is read afresh on every resolve; only the decryption is cached, keyed by the
    exact ciphertext and version it came from, so a replaced credential is never served
    from the cache.
    """

    def __init__(self, store: CredentialStore, keys: KeyStore):
        self._store = store
        self._keys = keys
        self._cache: dict[tuple[str, str], tuple[tuple[int, bytes], str]] = {}

    async def store_secret(
        self,
        tenant_id: str,
        kind: str,
        value: str,
        key_version: int,
        key: bytes,
        conn: Any = None,
    ) -> None:
        """Encrypt ``value`` under the key the caller holds and store it for the tenant.

        The install passes the key it just created inside the same unit of work, so the
        credential commits with the tenant row and the audit event. The value is never
        logged.
        """
        blob = encrypt_with_tenant_key(tenant_id, key, value.encode())
        await self._store.put(tenant_id, kind, blob, key_version, conn=conn)

    async def slack_bot_token(self, config: AgentsConfig) -> str:
        """The tenant's Slack bot token: the credential row first, else its env variable.

        Raises ``ConfigError`` naming the variable (no row, unset variable) or the tenant
        and key version (a row that cannot be decrypted) — never a value.
        """
        row = await self._store.get(config.tenant_id, SLACK_BOT)
        if row is None:
            return require_tenant_secret(config, "slack_bot_token_env")
        source = (row.key_version, row.ciphertext)
        entry = self._cache.get((config.tenant_id, SLACK_BOT))
        if entry is not None and entry[0] == source:
            return entry[1]
        token = await self._decrypt(config.tenant_id, SLACK_BOT, row)
        self._cache[(config.tenant_id, SLACK_BOT)] = (source, token)
        return token

    async def _decrypt(self, tenant_id: str, kind: str, row: Credential) -> str:
        """Decrypt with the tenant's key, reloading the keys once when it is missing."""
        try:
            return decrypt_for_tenant(
                self._keys, tenant_id, row.ciphertext, row.key_version
            ).decode()
        except TenantKeyError:
            # A tenant installed since the last key refresh: reload once, then retry.
            await self._keys.refresh()
        except InvalidTag:
            raise self._undecryptable(tenant_id, kind, row.key_version) from None
        try:
            return decrypt_for_tenant(
                self._keys, tenant_id, row.ciphertext, row.key_version
            ).decode()
        except (TenantKeyError, InvalidTag):
            raise self._undecryptable(tenant_id, kind, row.key_version) from None

    @staticmethod
    def _undecryptable(tenant_id: str, kind: str, version: int) -> ConfigError:
        return ConfigError(
            f"tenant '{tenant_id}': the stored {kind} credential cannot be decrypted with "
            f"key version {version}; restore the key or reinstall the app"
        )
