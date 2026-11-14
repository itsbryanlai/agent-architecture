"""Monthly budgets: a dollar limit per tenant and per agent, and the stop that enforces it.

A budget is a positive amount in US dollars for the UTC calendar month. A tenant has at
most one tenant-wide budget, which counts every model call and every embedding, and at
most one budget per agent, which counts that agent's model calls over all its versions.
No budget means no limit.

``BudgetGuard`` is asked by ``call_llm`` before the provider is called. When the month's
spend has reached a budget the activity answers with a fixed text and calls no model, so
nothing is billed and no usage row is written. The spend comes from the usage rows, read
at most once per ``ttl_seconds`` per tenant in each process. A turn that starts inside
that window does not see rows written since the read, so the overspend a budget can allow
is what the tenant spends in one window per worker process, plus the calls already in
flight. A raised budget takes up to one window to lift a stop for the same reason.

A cost that is null (a model with no price) cannot be added to a dollar amount. Those rows
are counted in ``Spend.unpriced_calls`` so a page can say the budget cannot see them; they
are never summed as zero silently. Spend and unpriced counts are also kept per cost type
(``usage.KIND_LABELS``), so a budget line shows model and embedding spend side by side.

Indexing asks ``BudgetGuard.tenant_stopped`` before each embedding call. It is the same
cached read as the turn check, so a page can be embedded up to one window per worker process
after the budget was reached, plus the pages already in flight. Embedding costs are small
next to that overspend.

If the budgets or the spend cannot be read, the check raises. The activity fails and
Temporal retries it, so a read failure never lets a call through unchecked.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from psycopg_pool import AsyncConnectionPool

from agentos import usage as usage_module
from agentos.models import LLMResponse
from agentos.stores import tenant_transaction
from agentos.usage import LLM, UsageGroup, UsageStore, month_bounds, utcnow

log = logging.getLogger("agentos.budgets")

STOP_REASON = "budget_reached"
AGENT = "agent"
TENANT = "tenant"
WARN_PERCENT = 80
CENTS = Decimal("0.01")
MAX_AMOUNT = Decimal("9999999999.99")
DEFAULT_TTL_SECONDS = 60
AGENT_KINDS = (LLM,)  # the cost types an agent budget counts

_AMOUNT = re.compile(r"\d{1,10}(\.\d{1,2})?")
_BAD_AMOUNT = (
    "a budget must be a positive amount in US dollars, with at most two decimals "
    f"and no more than {MAX_AMOUNT:,}"
)


def stop_text(agent_name: str) -> str:
    return f"{agent_name} has reached its monthly budget. An admin can raise it in the console."


class BudgetError(ValueError):
    """The change names a budget that cannot be set or does not exist."""


class InvalidBudget(BudgetError):
    """The amount is not a positive sum of dollars and cents."""


class BudgetNotFound(BudgetError):
    """No budget is set for that scope."""


def parse_amount(value: Any) -> Decimal:
    """A budget amount from a form or a JSON body. Anything unclear is refused, never rounded."""
    if isinstance(value, bool) or not isinstance(value, str | int | Decimal):
        raise InvalidBudget(_BAD_AMOUNT)
    if isinstance(value, str):
        text = value.strip()
        if not _AMOUNT.fullmatch(text):
            raise InvalidBudget(_BAD_AMOUNT)
        value = Decimal(text)
    try:
        amount = Decimal(value)
        if not amount.is_finite() or amount != amount.quantize(CENTS):
            raise InvalidBudget(_BAD_AMOUNT)
    except InvalidOperation as err:
        raise InvalidBudget(_BAD_AMOUNT) from err
    amount = amount.quantize(CENTS)
    if not 0 < amount <= MAX_AMOUNT:
        raise InvalidBudget(_BAD_AMOUNT)
    return amount


# --- rows -----------------------------------------------------------------------------


@dataclass(frozen=True)
class Budget:
    tenant_id: str
    agent_id: str | None  # None is the tenant-wide budget
    amount: Decimal
    updated_by: str
    updated_at: datetime | None = None


class BudgetStore(Protocol):
    async def list(self, tenant_id: str) -> list[Budget]:
        """Every budget of the tenant."""
        ...

    async def set_budget(
        self, tenant_id: str, agent_id: str | None, amount: Decimal, updated_by: str
    ) -> Budget:
        """Set or replace the budget of one scope (an agent, or the tenant when None)."""
        ...

    async def remove_budget(self, tenant_id: str, agent_id: str | None) -> None:
        """Remove a budget, which lifts the limit. ``BudgetNotFound`` when none is set."""
        ...


class InMemoryBudgetStore:
    def __init__(self) -> None:
        self._rows: dict[tuple[str, str | None], Budget] = {}

    async def list(self, tenant_id: str) -> list[Budget]:
        return sorted(
            (b for (t, _), b in self._rows.items() if t == tenant_id),
            key=lambda b: (b.agent_id is not None, b.agent_id or ""),
        )

    async def set_budget(
        self, tenant_id: str, agent_id: str | None, amount: Decimal, updated_by: str
    ) -> Budget:
        row = Budget(tenant_id, agent_id, amount, updated_by, utcnow())
        self._rows[(tenant_id, agent_id)] = row
        return row

    async def remove_budget(self, tenant_id: str, agent_id: str | None) -> None:
        if self._rows.pop((tenant_id, agent_id), None) is None:
            raise _not_found(agent_id)


def _not_found(agent_id: str | None) -> BudgetNotFound:
    scope = "the organisation" if agent_id is None else f"agent '{agent_id}'"
    return BudgetNotFound(f"no budget is set for {scope}")


_COLUMNS = "tenant_id, agent_id, amount_usd, updated_by, updated_at"


class PostgresBudgetStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def list(self, tenant_id: str) -> list[Budget]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT {_COLUMNS} FROM budgets WHERE tenant_id = %s", (tenant_id,)
            )
            return [Budget(*row) for row in await cur.fetchall()]

    async def set_budget(
        self, tenant_id: str, agent_id: str | None, amount: Decimal, updated_by: str
    ) -> Budget:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"""
                INSERT INTO budgets (tenant_id, agent_id, amount_usd, updated_by)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (tenant_id, (COALESCE(agent_id, ''))) DO UPDATE
                SET amount_usd = EXCLUDED.amount_usd,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = now()
                RETURNING {_COLUMNS}
                """,
                (tenant_id, agent_id, amount, updated_by),
            )
            row = await cur.fetchone()
        return Budget(*row)

    async def remove_budget(self, tenant_id: str, agent_id: str | None) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                DELETE FROM budgets
                WHERE tenant_id = %s AND COALESCE(agent_id, '') = %s
                RETURNING tenant_id
                """,
                (tenant_id, agent_id or ""),
            )
            if await cur.fetchone() is None:
                raise _not_found(agent_id)


# --- spend and levels -----------------------------------------------------------------


@dataclass
class KindSpend:
    """One cost type's part of a scope's spend."""

    kind: str
    cost: Decimal = Decimal(0)
    unpriced_calls: int = 0

    @property
    def label(self) -> str:
        return usage_module.KIND_LABELS.get(self.kind, self.kind)

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "label": self.label,
            "spend": format(self.cost.normalize(), "f"),
            "unpriced_calls": self.unpriced_calls,
        }


@dataclass
class Spend:
    """What was spent in a scope: the priced rows' dollars, and how many rows had no price."""

    cost: Decimal = Decimal(0)
    unpriced_calls: int = 0
    by_kind: dict[str, KindSpend] = field(default_factory=dict)

    def add(self, group: UsageGroup) -> None:
        self.cost += group.cost
        self.unpriced_calls += group.unpriced_calls
        part = self.by_kind.setdefault(group.kind, KindSpend(group.kind))
        part.cost += group.cost
        part.unpriced_calls += group.unpriced_calls

    def kinds(self, only: tuple[str, ...] | None = None) -> list[KindSpend]:
        """The split by cost type, in the order of ``usage.KIND_LABELS``, then any other kind seen.

        ``only`` limits it to the cost types the scope counts (an agent counts model calls only).
        """
        order = list(usage_module.KIND_LABELS) + [
            k for k in self.by_kind if k not in usage_module.KIND_LABELS
        ]
        return [
            KindSpend(
                k,
                self.by_kind[k].cost if k in self.by_kind else Decimal(0),
                self.by_kind[k].unpriced_calls if k in self.by_kind else 0,
            )
            for k in order
            if only is None or k in only
        ]


@dataclass
class MonthSpend:
    tenant: Spend = field(default_factory=Spend)  # every model call and every embedding
    agents: dict[str, Spend] = field(default_factory=dict)  # model calls, all versions

    def of(self, agent_id: str) -> Spend:
        return self.agents.get(agent_id, Spend())


def month_spend(groups: list[UsageGroup]) -> MonthSpend:
    spend = MonthSpend()
    for g in groups:
        spend.tenant.add(g)
        if g.kind in AGENT_KINDS and g.agent_id is not None:
            spend.agents.setdefault(g.agent_id, Spend()).add(g)
    return spend


NONE, OK, WARNING, STOPPED = "none", "ok", "warning", "stopped"


def level_of(spend: Decimal, amount: Decimal | None) -> tuple[int | None, str]:
    """The whole percent of the budget spent, and the level: warning from 80%, stopped at 100%."""
    if amount is None:
        return None, NONE
    percent = int(spend * 100 // amount)
    if spend >= amount:
        return percent, STOPPED
    if spend * 100 >= amount * WARN_PERCENT:
        return percent, WARNING
    return percent, OK


@dataclass
class BudgetLine:
    """One scope's budget against this month's spend. ``agent_id`` None is the tenant."""

    agent_id: str | None
    amount: Decimal | None
    spend: Decimal
    unpriced_calls: int
    percent: int | None
    level: str
    updated_by: str | None = None
    updated_at: datetime | None = None
    kinds: list[KindSpend] = field(default_factory=list)  # spend and unpriced calls by cost type

    @classmethod
    def of(
        cls,
        agent_id: str | None,
        budget: Budget | None,
        spend: Spend,
        kinds: tuple[str, ...] | None = None,
    ) -> BudgetLine:
        """``kinds`` limits the split to the cost types the scope counts; None means all."""
        amount = None if budget is None else budget.amount
        percent, level = level_of(spend.cost, amount)
        return cls(
            agent_id,
            amount,
            spend.cost,
            spend.unpriced_calls,
            percent,
            level,
            None if budget is None else budget.updated_by,
            None if budget is None else budget.updated_at,
            spend.kinds(kinds),
        )

    @property
    def cannot_see_unpriced(self) -> bool:
        """A budget is set and some of the month's calls have no price, so it cannot count them."""
        return self.amount is not None and self.unpriced_calls > 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "amount": None if self.amount is None else format(self.amount, "f"),
            "spend": format(self.spend.normalize(), "f"),
            "percent": self.percent,
            "level": self.level,
            "unpriced_calls": self.unpriced_calls,
            "kinds": [k.as_dict() for k in self.kinds],
            "updated_by": self.updated_by,
            "updated_at": None if self.updated_at is None else self.updated_at.isoformat(),
        }


@dataclass
class BudgetsView:
    month: str
    tenant: BudgetLine | None  # admins only
    agents: list[BudgetLine]

    def line_of(self, agent_id: str) -> BudgetLine | None:
        return next((x for x in self.agents if x.agent_id == agent_id), None)

    def as_dict(self) -> dict[str, Any]:
        return {
            "month": self.month,
            "tenant": None if self.tenant is None else self.tenant.as_dict(),
            "agents": [x.as_dict() for x in self.agents],
        }


# --- the stop -------------------------------------------------------------------------


@dataclass
class _Cached:
    fetched_at: datetime
    month_start: datetime
    budgets: dict[str | None, Decimal]
    spend: MonthSpend


class BudgetGuard:
    """Says whether an agent's turn must stop before a model call. See the module text."""

    def __init__(
        self,
        budgets: BudgetStore,
        usage: UsageStore,
        clock: Callable[[], datetime] = utcnow,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ):
        self.budgets = budgets
        self.usage = usage
        self.clock = clock
        self.ttl_seconds = ttl_seconds
        self._cache: dict[str, _Cached] = {}

    def forget(self) -> None:
        self._cache.clear()

    async def stopped_by(self, tenant_id: str, agent_id: str) -> str | None:
        """``"agent"`` or ``"tenant"`` when that budget is reached, else None.

        The agent's budget is named first when both are reached.
        """
        cached = await self._read(tenant_id)
        limit = cached.budgets.get(agent_id)
        if limit is not None and cached.spend.of(agent_id).cost >= limit:
            return AGENT
        limit = cached.budgets.get(None)
        if limit is not None and cached.spend.tenant.cost >= limit:
            return TENANT
        return None

    async def tenant_stopped(self, tenant_id: str) -> bool:
        """True when the tenant-wide budget is reached. Agent budgets never count here."""
        cached = await self._read(tenant_id)
        limit = cached.budgets.get(None)
        return limit is not None and cached.spend.tenant.cost >= limit

    async def stop_response(
        self, tenant_id: str, agent_id: str, agent_name: str
    ) -> LLMResponse | None:
        """The fixed answer when a budget is reached, else None. Logs ids only."""
        reached = await self.stopped_by(tenant_id, agent_id)
        if reached is None:
            return None
        log.info(
            "turn stopped by budget tenant_id=%s agent_id=%s budget=%s",
            tenant_id,
            agent_id,
            reached,
        )
        return LLMResponse(text=stop_text(agent_name), tool_calls=[], stop_reason=STOP_REASON)

    async def _read(self, tenant_id: str) -> _Cached:
        now = self.clock()
        start, end, _ = month_bounds(None, now)
        cached = self._cache.get(tenant_id)
        if (
            cached is not None
            and cached.month_start == start
            and 0 <= (now - cached.fetched_at).total_seconds() < self.ttl_seconds
        ):
            return cached
        limits = {b.agent_id: b.amount for b in await self.budgets.list(tenant_id)}
        spend = MonthSpend()
        if limits:
            spend = month_spend(await self.usage.totals(tenant_id, start, end))
        cached = self._cache[tenant_id] = _Cached(now, start, limits, spend)
        return cached
