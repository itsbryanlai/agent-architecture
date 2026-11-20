"""The tenant's approval policy per low-risk action (D-43).

An admin may turn requester approval off for a low-risk action — today a Notion
comment on an item the requester filed. The policy is one row per tenant and
action: ``requester_approval`` (the default) or ``none``. An action with no row,
like an action no manifest marks low-risk, reads as the default, so a tenant
that never touches the setting keeps asking for approval on every write.

The console writes a row in the same unit of work as its ``policy_change`` audit
event; the task workflow reads it once per comment, just before the write. Only
the manifest's ``low_risk`` flag says which actions may be relaxed at all; this
store holds the tenant's choice for them.
"""

from __future__ import annotations

from typing import Any, Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.stores import joined_transaction, tenant_transaction

REQUESTER_APPROVAL = "requester_approval"
NO_APPROVAL = "none"
POLICIES = (REQUESTER_APPROVAL, NO_APPROVAL)


class ApprovalPolicyStore(Protocol):
    async def get(self, tenant_id: str, action: str) -> str:
        """The tenant's policy for ``action``; the default for one with no row."""
        ...

    async def set(self, tenant_id: str, action: str, policy: str, conn: Any = None) -> None:
        """Store the tenant's policy for ``action``, replacing any earlier one.

        With ``conn`` (a unit of work's connection) the change commits with that
        transaction, alongside its audit event.
        """
        ...


class InMemoryApprovalPolicyStore:
    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], str] = {}

    def snapshot(self) -> dict[tuple[str, str], str]:
        """The policies as they are now, for ``InMemoryUnitOfWork``."""
        return dict(self._rows)

    def restore(self, state: dict[tuple[str, str], str]) -> None:
        self._rows = dict(state)

    async def get(self, tenant_id: str, action: str) -> str:
        return self._rows.get((tenant_id, action), REQUESTER_APPROVAL)

    async def set(self, tenant_id: str, action: str, policy: str, conn: Any = None) -> None:
        self._rows[(tenant_id, action)] = policy


class PostgresApprovalPolicyStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def get(self, tenant_id: str, action: str) -> str:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                "SELECT policy FROM approval_policies WHERE tenant_id = %s AND action = %s",
                (tenant_id, action),
            )
            row = await cur.fetchone()
        return REQUESTER_APPROVAL if row is None else row[0]

    async def set(self, tenant_id: str, action: str, policy: str, conn: Any = None) -> None:
        async with joined_transaction(self._pool, tenant_id, conn) as joined:
            await joined.execute(
                """
                INSERT INTO approval_policies (tenant_id, action, policy)
                VALUES (%s, %s, %s)
                ON CONFLICT (tenant_id, action) DO UPDATE SET
                  policy = EXCLUDED.policy, updated_at = now()
                """,
                (tenant_id, action, policy),
            )
