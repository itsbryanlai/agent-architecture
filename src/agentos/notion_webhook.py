"""The Notion webhook verification token, encrypted per tenant in the database.

The subscription handshake stores the token as ``pending``, encrypted with the
tenant's payload key (the codec's AES-256-GCM cipher, the tenant ID as
associated data). One row per tenant: the insert is ``ON CONFLICT DO NOTHING``,
so a second handshake cannot replace a token that is pending or active. The
host operator finishes the subscription with::

    uv run python -m agentos.notion_webhook show --tenant <id>
    uv run python -m agentos.notion_webhook confirm --tenant <id>
    uv run python -m agentos.notion_webhook clear --tenant <id>

``show`` prints the pending token once, to paste into Notion's verify box;
``confirm`` makes it active, the only state that accepts events; ``clear``
removes it so a new handshake is accepted. Each is audited as
``operator:<login of the OS account running it>`` (see ``agentos.switches``),
and the audit row never holds the token. A pending token never accepts an
event, and an unauthenticated POST can never replace an active one.

The gateway reads the active token afresh for every event and caches only the
decryption, keyed by the exact ciphertext it decrypted: a cleared or replaced
token is never served from the cache.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import logging
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from cryptography.exceptions import InvalidTag
from psycopg_pool import AsyncConnectionPool

from agentos import ids
from agentos.audit import AuditStore
from agentos.config import TENANT_ID_PATTERN, load_settings
from agentos.crypto_keys import KeyStore, TenantKeyError, build_key_store
from agentos.models import AuditEvent
from agentos.payload_codec import decrypt_for_tenant, encrypt_for_tenant
from agentos.stores import joined_transaction, tenant_transaction
from agentos.switches import operator_identity
from agentos.unit_of_work import InMemoryUnitOfWork, PostgresUnitOfWork, UnitOfWork
from agentos.usage import utcnow

log = logging.getLogger("agentos.notion_webhook")

PENDING = "pending"
ACTIVE = "active"


@dataclass(frozen=True)
class WebhookToken:
    """One tenant's verification token: ciphertext, never the token itself."""

    tenant_id: str
    state: str
    ciphertext: bytes
    key_version: int
    created_at: datetime | None = None
    confirmed_at: datetime | None = None


class WebhookTokenStore(Protocol):
    async def get(self, tenant_id: str) -> WebhookToken | None:
        """The tenant's token row, or None."""
        ...

    async def put_pending(
        self, tenant_id: str, ciphertext: bytes, key_version: int, conn: Any = None
    ) -> bool:
        """Store a pending token. False when the tenant already has one (pending or active).

        ``conn`` is the connection of a unit of work (``unit_of_work.py``), here and below.
        """
        ...

    async def confirm(self, tenant_id: str, at: datetime, conn: Any = None) -> bool:
        """Make the pending token active. False when there is no pending token."""
        ...

    async def clear(self, tenant_id: str, conn: Any = None) -> bool:
        """Remove the token so a new handshake is accepted. False when there was none."""
        ...


class InMemoryWebhookTokenStore:
    def __init__(self) -> None:
        self._rows: dict[str, WebhookToken] = {}

    def snapshot(self) -> dict[str, WebhookToken]:
        """The rows as they are now, for ``InMemoryUnitOfWork``."""
        return dict(self._rows)

    def restore(self, state: dict[str, WebhookToken]) -> None:
        self._rows = dict(state)

    async def get(self, tenant_id: str) -> WebhookToken | None:
        return self._rows.get(tenant_id)

    async def put_pending(
        self, tenant_id: str, ciphertext: bytes, key_version: int, conn: Any = None
    ) -> bool:
        if tenant_id in self._rows:
            return False
        self._rows[tenant_id] = WebhookToken(tenant_id, PENDING, ciphertext, key_version)
        return True

    async def confirm(self, tenant_id: str, at: datetime, conn: Any = None) -> bool:
        row = self._rows.get(tenant_id)
        if row is None or row.state != PENDING:
            return False
        self._rows[tenant_id] = dataclasses.replace(row, state=ACTIVE, confirmed_at=at)
        return True

    async def clear(self, tenant_id: str, conn: Any = None) -> bool:
        return self._rows.pop(tenant_id, None) is not None


_COLUMNS = "tenant_id, state, ciphertext, key_version, created_at, confirmed_at"


class PostgresWebhookTokenStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def get(self, tenant_id: str) -> WebhookToken | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT {_COLUMNS} FROM notion_webhook_tokens WHERE tenant_id = %s",
                (tenant_id,),
            )
            row = await cur.fetchone()
        return None if row is None else WebhookToken(*row)

    async def put_pending(
        self, tenant_id: str, ciphertext: bytes, key_version: int, conn: Any = None
    ) -> bool:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            cur = await db.execute(
                "INSERT INTO notion_webhook_tokens (tenant_id, state, ciphertext, key_version) "
                "VALUES (%s, 'pending', %s, %s) "
                "ON CONFLICT (tenant_id) DO NOTHING RETURNING tenant_id",
                (tenant_id, ciphertext, key_version),
            )
            return await cur.fetchone() is not None

    async def confirm(self, tenant_id: str, at: datetime, conn: Any = None) -> bool:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            cur = await db.execute(
                "UPDATE notion_webhook_tokens SET state = 'active', confirmed_at = %s "
                "WHERE tenant_id = %s AND state = 'pending' RETURNING tenant_id",
                (at, tenant_id),
            )
            return await cur.fetchone() is not None

    async def clear(self, tenant_id: str, conn: Any = None) -> bool:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            cur = await db.execute(
                "DELETE FROM notion_webhook_tokens WHERE tenant_id = %s RETURNING tenant_id",
                (tenant_id,),
            )
            return await cur.fetchone() is not None


class WebhookTokens:
    """The gateway's side: store a handshake's token pending, read the active one.

    The row is read afresh for every event; only the decryption is cached, keyed
    by the exact ciphertext and version it came from, so a token that was
    cleared, replaced or re-confirmed is never served from the cache.
    """

    def __init__(self, store: WebhookTokenStore, keys: KeyStore):
        self._store = store
        self._keys = keys
        self._cache: dict[str, tuple[tuple[int, bytes], str]] = {}

    async def accept_handshake(self, tenant_id: str, token: str) -> bool:
        """Encrypt the token under the tenant's newest key and store it pending.

        False when the tenant already has a pending or active token: nothing is
        replaced. Raises ``TenantKeyError`` when the tenant has no usable key.
        """
        try:
            blob, version = encrypt_for_tenant(self._keys, tenant_id, token.encode())
        except TenantKeyError:
            # A tenant installed since the last key refresh: reload once, then retry.
            await self._keys.refresh()
            blob, version = encrypt_for_tenant(self._keys, tenant_id, token.encode())
        return await self._store.put_pending(tenant_id, blob, version)

    async def active(self, tenant_id: str) -> str | None:
        """The tenant's confirmed token, decrypted in memory, or None."""
        row = await self._store.get(tenant_id)
        if row is None or row.state != ACTIVE:
            return None
        source = (row.key_version, row.ciphertext)
        entry = self._cache.get(tenant_id)
        if entry is None or entry[0] != source:
            try:
                token = await self._decrypt(tenant_id, row.ciphertext, row.key_version)
            except (TenantKeyError, InvalidTag):
                # A key version that is gone or holds another value: refuse events
                # cleanly (the caller answers 503) instead of failing each one.
                log.warning(
                    "tenant_id=%s Notion webhook token cannot be decrypted with key "
                    "version %s; events are refused until the key is restored or the "
                    "token is cleared and confirmed again",
                    tenant_id,
                    row.key_version,
                )
                return None
            self._cache[tenant_id] = (source, token)
            return token
        return entry[1]

    async def _decrypt(self, tenant_id: str, blob: bytes, version: int) -> str:
        """Decrypt with the tenant's key, reloading the keys once when it is missing."""
        try:
            return decrypt_for_tenant(self._keys, tenant_id, blob, version).decode()
        except TenantKeyError:
            await self._keys.refresh()
            return decrypt_for_tenant(self._keys, tenant_id, blob, version).decode()


# --- the command line -----------------------------------------------------------------


def _event(tenant_id: str, command: str, identity: str, invocation: str) -> AuditEvent:
    return AuditEvent(
        tenant_id=tenant_id,
        event_key=ids.audit_control_key(tenant_id, "webhook_token", f"{command}:{invocation}"),
        kind="webhook_token",
        agent_id=None,
        agent_version=None,
        requester_user_id=identity,
        identity=identity,
        tool=command,
    )


async def execute(
    args: argparse.Namespace,
    store: WebhookTokenStore,
    audit: AuditStore,
    keys: KeyStore,
    uow: UnitOfWork | None = None,
    request_id: str | None = None,
) -> int:
    """Run one parsed command against the given stores. Reads no tenant list.

    ``uow`` is the unit of work the change and its audit event commit in; the
    default is the in-memory one over ``store`` and ``audit``. A refused command
    (nothing to show, confirm or clear) changes nothing and records nothing.
    """
    if not TENANT_ID_PATTERN.fullmatch(args.tenant):
        print(f"'{args.tenant}' is not a valid tenant ID")
        return 2
    identity = operator_identity()
    invocation = request_id or uuid.uuid4().hex
    unit = InMemoryUnitOfWork(store, audit) if uow is None else uow
    try:
        if args.command == "show":
            row = await store.get(args.tenant)
            if row is None:
                print(f"no webhook token for tenant '{args.tenant}'; a handshake stores one")
                return 1
            if row.state != PENDING:
                print(
                    f"the webhook token of tenant '{args.tenant}' is already confirmed; "
                    "clear it and re-subscribe for a new one"
                )
                return 1
            try:
                token = decrypt_for_tenant(
                    keys, args.tenant, row.ciphertext, row.key_version
                ).decode()
            except InvalidTag:
                print(
                    f"refused: the webhook token of tenant '{args.tenant}' cannot be decrypted "
                    f"with key version {row.key_version}; clear it and re-subscribe"
                )
                return 1
            async with unit.begin(args.tenant) as conn:
                await audit.record(_event(args.tenant, "show", identity, invocation), conn=conn)
            print(token)
            return 0
        async with unit.begin(args.tenant) as conn:
            if args.command == "confirm":
                changed = await store.confirm(args.tenant, utcnow(), conn=conn)
            else:
                changed = await store.clear(args.tenant, conn=conn)
            if changed:
                await audit.record(
                    _event(args.tenant, args.command, identity, invocation), conn=conn
                )
        if not changed:
            print(f"nothing to {args.command} for tenant '{args.tenant}'")
            return 1
        print(f"{args.command}: done for tenant '{args.tenant}'")
        return 0
    except TenantKeyError as err:
        print(f"refused: {err}")
        return 1


async def run(args: argparse.Namespace) -> int:
    from agentos.audit import PostgresAuditStore
    from agentos.schema_version import require_current_schema

    settings = load_settings()
    pool = AsyncConnectionPool(settings.require("database_url"), open=False, **settings.pool_args())
    await pool.open()
    try:
        await require_current_schema(pool)
        return await execute(
            args,
            PostgresWebhookTokenStore(pool),
            PostgresAuditStore(pool),
            await build_key_store(settings, pool),
            PostgresUnitOfWork(pool),
        )
    finally:
        await pool.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Show, confirm or clear a tenant's Notion webhook verification token."
    )
    parser.add_argument("--tenant", required=True, help="the tenant whose token to manage")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("show", help="print the pending token once, to paste into Notion")
    commands.add_parser("confirm", help="make the pending token active (events are accepted)")
    commands.add_parser("clear", help="remove the token so a new handshake is accepted")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    sys.exit(asyncio.run(run(build_parser().parse_args())))


if __name__ == "__main__":
    main()
