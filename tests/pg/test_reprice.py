"""The reprice SQL: adjustments written by the owner, counted by totals, refused to the app role."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from psycopg import errors
from psycopg_pool import AsyncConnectionPool

from agentos.audit import PostgresAuditStore
from agentos.stores import tenant_transaction
from agentos.unit_of_work import PostgresUnitOfWork
from agentos.usage import Adjustment, PostgresUsageStore, PriceEntry, Prices, UsageEvent
from agentos.usage_reprice import reprice
from tests.pg.conftest import TestDatabase

AT = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)


@pytest.fixture
async def owner_pool(db: TestDatabase):
    async with AsyncConnectionPool(db.owner_url, open=False) as pool:
        yield pool


async def test_reprice_writes_an_adjustment_and_an_audit_event_the_app_role_then_totals(
    pool, owner_pool, tenant: str
):
    """Fails if the owner's reprice is not counted by the app role's totals, or leaves no audit event."""
    app = PostgresUsageStore(pool)
    await app.record(
        UsageEvent(tenant, "k1", "llm", "a", 1, "m-2026", 1_000_000, 0, 0, 0, None, "c", AT)
    )
    prices = Prices(
        [PriceEntry("m", date(2026, 1, 1), Decimal(3), Decimal(0), Decimal(0), Decimal(0))]
    )
    owner = PostgresUsageStore(owner_pool)

    dry = await reprice(
        owner,
        PostgresAuditStore(owner_pool),
        prices,
        tenant,
        "2026-11",
        write=False,
        identity="operator:t",
        uow=PostgresUnitOfWork(owner_pool),
    )
    assert len(dry.adjustments) == 1 and dry.stored == 0
    [before] = await app.totals(tenant, AT.replace(day=1), AT.replace(month=12, day=1))
    assert before.unpriced_calls == 1

    done = await reprice(
        owner,
        PostgresAuditStore(owner_pool),
        prices,
        tenant,
        "2026-11",
        write=True,
        identity="operator:t",
        uow=PostgresUnitOfWork(owner_pool),
    )

    assert done.stored == 1
    [after] = await app.totals(tenant, AT.replace(day=1), AT.replace(month=12, day=1))
    assert (after.cost, after.unpriced_calls) == (Decimal(3), 0)
    events = await PostgresAuditStore(pool).list(tenant)
    assert [(e.kind, e.agent_id) for e in events] == [("usage_repriced", None)]


async def test_the_app_role_cannot_write_an_adjustment(pool, tenant: str):
    """Fails if the app role can price a usage row itself."""
    app = PostgresUsageStore(pool)
    await app.record(UsageEvent(tenant, "k1", "embedding", None, None, "m", cost=None, at=AT))

    with pytest.raises(errors.InsufficientPrivilege):
        await app.adjust(tenant, [Adjustment("k1", Decimal(1), "m", date(2026, 1, 1))])


async def test_a_row_that_already_has_a_cost_or_an_adjustment_is_not_adjusted_again(
    pool, owner_pool, tenant: str
):
    """Fails if an adjustment can be added to a priced row or twice to one row."""
    app, owner = PostgresUsageStore(pool), PostgresUsageStore(owner_pool)
    await app.record(UsageEvent(tenant, "p", "embedding", None, None, "m", cost=Decimal(1), at=AT))
    await app.record(UsageEvent(tenant, "u", "embedding", None, None, "m", cost=None, at=AT))
    rows = [Adjustment(k, Decimal(2), "m", date(2026, 1, 1)) for k in ("p", "u")]

    assert await owner.adjust(tenant, rows) == 1
    assert await owner.adjust(tenant, rows) == 0
    async with tenant_transaction(pool, tenant) as conn:
        count = (await (await conn.execute("SELECT count(*) FROM usage_adjustments")).fetchone())[0]
    assert count == 1
