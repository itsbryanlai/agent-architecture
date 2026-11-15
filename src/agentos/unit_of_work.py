"""One transaction for a change and its audit event.

The control plane makes every change inside ``uow.begin(tenant_id)``. Postgres opens one
tenant transaction, takes the tenant's advisory lock and yields its connection; the store
methods and the audit store are given that connection, so the change and its event commit
together or not at all. A failed audit insert therefore leaves no change behind, and the
retry is not refused as "already done".

The lock is the one ``PostgresAgentStore`` takes for a publish, so submit, withdraw, edit
and publish of one tenant never interleave, and a count that decides a change (the last
admin) is taken under it.

``InMemoryUnitOfWork`` is the same contract for tests: one asyncio lock per tenant and, when
the unit raises, every in-memory store it covers is put back as it was before. It is not
used by the Postgres stores, which have no ``snapshot``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.stores import tenant_transaction

TENANT_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))"


class UnitOfWork(Protocol):
    def begin(self, tenant_id: str) -> AbstractAsyncContextManager[Any]:
        """Hold the tenant's lock and yield the connection the unit's calls join.

        The connection is ``None`` for the in-memory form. Leaving the block with an
        exception undoes every change made inside it.
        """
        ...


class PostgresUnitOfWork:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    @asynccontextmanager
    async def begin(self, tenant_id: str) -> AsyncIterator[Any]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(TENANT_LOCK_SQL, (tenant_id,))
            yield conn


class InMemoryUnitOfWork:
    """Tenant lock and rollback for the in-memory stores, given as the stores it covers."""

    def __init__(self, *stores: Any) -> None:
        for store in stores:
            if not (hasattr(store, "snapshot") and hasattr(store, "restore")):
                raise TypeError(
                    f"{type(store).__name__} cannot be rolled back; give the control plane "
                    "a PostgresUnitOfWork for stores that live in Postgres"
                )
        self._stores = stores
        self._locks: dict[str, asyncio.Lock] = {}

    @asynccontextmanager
    async def begin(self, tenant_id: str) -> AsyncIterator[None]:
        lock = self._locks.setdefault(tenant_id, asyncio.Lock())
        async with lock:
            before = [(store, store.snapshot()) for store in self._stores]
            try:
                yield None
            except BaseException:
                for store, state in before:
                    store.restore(state)
                raise
