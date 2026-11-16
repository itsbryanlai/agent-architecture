"""Row-level security, store side: no store call runs without a tenant.

The policies, grants and triggers, and the tenant being set first by every store
method, run against a real Postgres in tests/pg.
"""

from contextlib import asynccontextmanager

import pytest

from agentos.stores import tenant_transaction


class RecordingPool:
    """Counts the connections taken; no query ever reaches it."""

    def __init__(self):
        self.taken = 0

    @asynccontextmanager
    async def connection(self):
        self.taken += 1
        yield None


async def test_tenant_transaction_rejects_empty_tenant():
    """Fails if a transaction opens, or a connection is taken, for an empty or missing tenant ID."""
    pool = RecordingPool()
    for bad in ("", None):
        with pytest.raises(ValueError):
            async with tenant_transaction(pool, bad):
                pass
    assert pool.taken == 0
