"""The monthly usage rollup the budget check reads equals the aggregate of the rows it summarises."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from psycopg_pool import AsyncConnectionPool

from agentos.usage import (
    Adjustment,
    InMemoryUsageStore,
    PostgresUsageStore,
    UsageEvent,
)
from tests.pg.conftest import MIGRATIONS, Databases, TestDatabase

NOV = datetime(2026, 11, 1, tzinfo=UTC)
DEC = datetime(2026, 12, 1, tzinfo=UTC)
AT = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)


def event(tenant: str, key: str, cost: str | None, at: datetime = AT, **fields) -> UsageEvent:
    base = {
        "tenant_id": tenant,
        "usage_key": key,
        "kind": "llm",
        "agent_id": "aa",
        "agent_version": 1,
        "model": "m",
        "input_tokens": 10,
        "output_tokens": 5,
        "cost": None if cost is None else Decimal(cost),
        "at": at,
    }
    return UsageEvent(**(base | fields))


def embedding(tenant: str, key: str, cost: str | None, at: datetime = AT) -> UsageEvent:
    return event(tenant, key, cost, at, kind="embedding", agent_id=None, agent_version=None)


@pytest.fixture
def usage(store_pool):
    return InMemoryUsageStore() if store_pool is None else PostgresUsageStore(store_pool)


async def aggregate(usage, tenant: str, start: datetime, end: datetime) -> dict:
    """Month figures per (kind, agent), from the rows themselves (the GROUP BY)."""
    figures: dict = {}
    for g in await usage.totals(tenant, start, end):
        calls, cost, unpriced = figures.get((g.kind, g.agent_id), (0, Decimal(0), 0))
        figures[(g.kind, g.agent_id)] = (
            calls + g.calls,
            cost + g.cost,
            unpriced + g.unpriced_calls,
        )
    return figures


async def rolled_up(usage, tenant: str, start: datetime) -> dict:
    return {
        (g.kind, g.agent_id): (g.calls, g.cost, g.unpriced_calls)
        for g in await usage.month_totals(tenant, start)
    }


async def test_the_rollup_equals_the_aggregate_after_inserts_and_repeats(usage, tenant: str):
    """Fails if a repeated usage key is counted twice, a month or agent is mixed with another, or an unpriced call is lost."""
    await usage.record(event(tenant, "k1", "0.50000000"))
    await usage.record(event(tenant, "k1", "9.00000000"))  # a retry of the same call
    await usage.record(event(tenant, "k2", "0.25000000"))
    await usage.record(event(tenant, "k3", None))
    await usage.record(event(tenant, "k4", "1.00000000", agent_id="bb"))
    await usage.record(embedding(tenant, "k5", "0.10000000"))
    await usage.record(event(tenant, "k6", "7.00000000", at=AT + timedelta(days=30)))

    rollup = await rolled_up(usage, tenant, NOV)

    assert rollup == await aggregate(usage, tenant, NOV, DEC)
    assert rollup[("llm", "aa")] == (3, Decimal("0.75"), 1)
    assert rolled_up_cost(await rolled_up(usage, tenant, DEC)) == Decimal(7)


def rolled_up_cost(figures: dict) -> Decimal:
    return sum((cost for _, cost, _ in figures.values()), Decimal(0))


async def test_the_rollup_of_one_tenant_holds_nothing_of_another(
    usage, tenant: str, other_tenant: str
):
    """Fails if a tenant's rollup includes a call of another tenant."""
    await usage.record(event(tenant, "k1", "1"))
    await usage.record(event(other_tenant, "k1", "2"))

    assert await rolled_up(usage, tenant, NOV) == {("llm", "aa"): (1, Decimal(1), 0)}


@pytest.fixture
async def owner_pool(db: TestDatabase):
    async with AsyncConnectionPool(db.owner_url, open=False) as pool:
        yield pool


async def test_an_adjustment_moves_the_call_from_unpriced_into_the_cost_once(
    pool, owner_pool, tenant: str
):
    """Fails if pricing a call later leaves it counted as unpriced, adds its cost twice, or loses the rest of the month."""
    app, owner = PostgresUsageStore(pool), PostgresUsageStore(owner_pool)
    await app.record(event(tenant, "priced", "1.00000000"))
    await app.record(event(tenant, "late", None))
    await app.record(embedding(tenant, "other", None))
    rows = [Adjustment("late", Decimal("2.5"), "m", date(2026, 1, 1))]

    assert await owner.adjust(tenant, rows) == 1
    assert await owner.adjust(tenant, rows) == 0

    rollup = await rolled_up(app, tenant, NOV)
    assert rollup == await aggregate(app, tenant, NOV, DEC)
    assert rollup == {
        ("llm", "aa"): (2, Decimal("3.5"), 0),
        ("embedding", None): (1, Decimal(0), 1),
    }


async def test_the_backfill_builds_the_rollup_from_rows_stored_before_it_and_can_run_again(
    databases: Databases,
):
    """Fails if the migration's backfill miscounts existing rows, ignores adjustments, or changes the rollup when run twice."""
    fresh = databases.create(init=True)
    tenant = "t-backfill"
    with fresh.owner() as conn:
        for key, kind, agent, cost in [
            ("a", "llm", "aa", "1.5"),
            ("b", "llm", "aa", None),
            ("c", "llm", "aa", None),
            ("d", "embedding", None, "0.1"),
        ]:
            conn.execute(
                "INSERT INTO usage_events (tenant_id, usage_key, kind, agent_id, agent_version,"
                " model, cost_usd, at) VALUES (%s, %s, %s, %s, %s, 'm', %s, %s)",
                (tenant, key, kind, agent, None if agent is None else 1, cost, AT),
            )
        conn.execute(
            "INSERT INTO usage_adjustments (tenant_id, usage_key, cost_usd, price_model,"
            " price_effective_from) VALUES (%s, 'b', 2, 'm', '2026-01-01')",
            (tenant,),
        )
        conn.execute("TRUNCATE usage_month_totals")  # as before the migration existed

    migration = MIGRATIONS / "026_usage_month_totals.sql"
    fresh.execute_file(migration)
    fresh.execute_file(migration)

    async with fresh.pool() as pool:
        store = PostgresUsageStore(pool)
        assert await rolled_up(store, tenant, NOV) == await aggregate(store, tenant, NOV, DEC)
        assert await rolled_up(store, tenant, NOV) == {
            ("llm", "aa"): (3, Decimal("3.5"), 1),
            ("embedding", None): (1, Decimal("0.1"), 0),
        }
