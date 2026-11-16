"""Shared test helpers."""

from datetime import UTC, datetime
from decimal import Decimal

from agentos.usage import UsageEvent
from tests.conftest import TENANT

D = Decimal


NOV = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)


def usage_row(key, agent="it-helper", cost="1", kind="llm", at=NOV, tenant=TENANT):
    return UsageEvent(
        tenant_id=tenant,
        usage_key=key,
        kind=kind,
        agent_id=None if kind == "embedding" else agent,
        agent_version=None if kind == "embedding" else 1,
        model="m",
        cost=None if cost is None else D(cost),
        at=at,
    )
