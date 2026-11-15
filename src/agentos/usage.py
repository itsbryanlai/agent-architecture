"""Usage and cost: what each model call and each embedding used, and what it cost.

One row per model call or per embedded page. A row names the tenant, the agent and
the agent version that made the call (none for an embedding), the model the provider
reported, the token counts, the cost, the conversation (for tracing) and the time. It
never names the person who asked and never holds chat or document text, so nothing
here can be grouped, filtered or exported by requester.

A call is priced when it is recorded, with the price entry in effect that day, and the
cost is stored on the row. A later price change never rewrites a stored cost. A model
with no price entry is recorded with its tokens and a null cost, which the page shows
as "no price". A budget treats a month with such a row as spending it cannot measure and
stops (``budgets.py``). ``usage_adjustments`` is how a row recorded without a price gets
one later: ``python -m agentos.usage reprice`` appends one adjustment per row it can now
price, and totals count a row as priced once it has one. The row itself never changes.

A row is keyed by the activity attempt that made the call (``ids.usage_call_key``), so
each attempt the provider answered is counted once. The activity records the row after
the provider has answered. A failed attempt records nothing. If the result of an
answered attempt is lost and Temporal retries, the retry calls the provider again and
is billed again, so it records its own row. Only a repeat of the same key (the same
attempt recorded twice) stores nothing. The app role needs INSERT and SELECT only.

Recording never fails the turn, and never drops a paid call quietly. ``UsageRecorder``
tries the insert a few times, a short wait apart. If every try fails it logs at error
level the tenant, the agent, the model and the token counts (never content) and tells
its listener (the budget guard) the call is unmetered, so the guard treats that scope
as spending it cannot measure for its cache window. The row itself is lost: the logged
counts are the record, and the reprice command cannot recover it.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

import yaml
from psycopg_pool import AsyncConnectionPool

from agentos.config import ConfigError
from agentos.models import Usage
from agentos.stores import joined_transaction, tenant_transaction

log = logging.getLogger("agentos.usage")

LLM = "llm"
EMBEDDING = "embedding"
# The cost types, in display order, each with the label people read. A new cost type is one
# entry here: budgets split spend by this list wherever they show it.
KIND_LABELS = {LLM: "Model", EMBEDDING: "Embedding"}

PER_MILLION = Decimal(1_000_000)
COST_PLACES = Decimal("0.00000001")
NO_TEAM = "No team"


def utcnow() -> datetime:
    return datetime.now(UTC)


# --- prices ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PriceEntry:
    """US dollars per million tokens of one model, from ``effective_from`` (UTC) on.

    ``aliases`` are other exact names the provider may report for the same model.
    """

    model: str
    effective_from: date
    input: Decimal
    output: Decimal
    cache_read: Decimal
    cache_write: Decimal
    aliases: tuple[str, ...] = ()


class Prices:
    def __init__(self, entries: list[PriceEntry]):
        self.entries = list(entries)
        self._models = {e.model for e in self.entries}
        self._aliases: dict[str, str] = {}
        for e in self.entries:
            for alias in e.aliases:
                self._aliases.setdefault(alias, e.model)

    def resolve(self, name: str) -> str | None:
        """The listed model a reported name stands for, or None.

        An exact model name wins, then an alias, then the longest listed model name that
        the reported name starts with, followed by ``-`` (a dated suffix).
        """
        if name in self._models:
            return name
        if name in self._aliases:
            return self._aliases[name]
        prefixed = [m for m in self._models if name.startswith(m + "-")]
        return max(prefixed, key=len, default=None)

    def entry_for(self, model: str, at: datetime) -> PriceEntry | None:
        """The latest entry of the model that started on or before the day of ``at``."""
        listed = self.resolve(model)
        if listed is None:
            return None
        day = at.astimezone(UTC).date()
        found = [e for e in self.entries if e.model == listed and e.effective_from <= day]
        return max(found, key=lambda e: e.effective_from, default=None)

    def cost(self, model: str, at: datetime, usage: Usage) -> Decimal | None:
        """The cost in US dollars, or None when no entry is in effect for the model."""
        return self.cost_of(
            model,
            at,
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_tokens,
            usage.cache_write_tokens,
        )

    def cost_of(
        self,
        model: str,
        at: datetime,
        input_tokens: int | None,
        output_tokens: int | None = 0,
        cache_read_tokens: int | None = 0,
        cache_write_tokens: int | None = 0,
    ) -> Decimal | None:
        """The cost, or None when there is no entry or a count is unknown where a price is not 0.

        A count the provider did not report costs nothing only when the price of that kind is 0.
        """
        entry = self.entry_for(model, at)
        if entry is None:
            return None
        total = Decimal(0)
        for count, rate in (
            (input_tokens, entry.input),
            (output_tokens, entry.output),
            (cache_read_tokens, entry.cache_read),
            (cache_write_tokens, entry.cache_write),
        ):
            if count is None:
                if rate:
                    return None
                continue
            total += count * rate
        return (total / PER_MILLION).quantize(COST_PLACES)


_PRICE_FIELDS = ("model", "effective_from", "input", "output", "cache_read", "cache_write")
_OPTIONAL_PRICE_FIELDS = ("aliases",)


def _price_number(where: str, name: str, value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ConfigError(f"{where}: '{name}' must be a number of dollars per million tokens")
    number = Decimal(str(value))
    if not number.is_finite() or number < 0:
        raise ConfigError(f"{where}: '{name}' must be zero or more")
    return number


def _price_entry(position: int, raw: Any) -> PriceEntry:
    where = f"prices: entry {position}"
    if not isinstance(raw, dict):
        raise ConfigError(f"{where} must be a mapping")
    for key in raw:
        if key not in _PRICE_FIELDS + _OPTIONAL_PRICE_FIELDS:
            raise ConfigError(f"{where} has an unknown field '{key}'")
    for key in _PRICE_FIELDS:
        if raw.get(key) in (None, ""):
            raise ConfigError(f"{where}: missing required field '{key}'")
    model = raw["model"]
    if not isinstance(model, str) or not model.strip():
        raise ConfigError(f"{where}: 'model' must be a model name")
    start = raw["effective_from"]
    if isinstance(start, str):
        try:
            start = date.fromisoformat(start)
        except ValueError:
            start = None
    if not isinstance(start, date) or isinstance(start, datetime):
        raise ConfigError(f"{where}: 'effective_from' must be a date such as 2026-01-31")
    numbers = {k: _price_number(where, k, raw[k]) for k in _PRICE_FIELDS[2:]}
    aliases = raw.get("aliases") or []
    if not isinstance(aliases, list) or not all(isinstance(a, str) and a.strip() for a in aliases):
        raise ConfigError(f"{where}: 'aliases' must be a list of model names")
    return PriceEntry(model.strip(), start, **numbers, aliases=tuple(a.strip() for a in aliases))


def parse_prices(raw: Any) -> Prices:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict) or set(raw) - {"prices"}:
        raise ConfigError("prices: expected a mapping with one key, 'prices'")
    listed = raw.get("prices") or []
    if not isinstance(listed, list):
        raise ConfigError("prices: 'prices' must be a list of entries")
    entries = [_price_entry(n, item) for n, item in enumerate(listed, start=1)]
    seen: set[tuple[str, date]] = set()
    for entry in entries:
        key = (entry.model, entry.effective_from)
        if key in seen:
            raise ConfigError(
                f"prices: model '{entry.model}' is listed more than once for {entry.effective_from}"
            )
        seen.add(key)
    models = {e.model for e in entries}
    claimed: dict[str, str] = {}
    for entry in entries:
        for alias in entry.aliases:
            if alias in models:
                raise ConfigError(f"prices: alias '{alias}' is also listed as a model")
            if claimed.setdefault(alias, entry.model) != entry.model:
                raise ConfigError(
                    f"prices: alias '{alias}' belongs to both '{claimed[alias]}' and '{entry.model}'"
                )
    return Prices(entries)


def load_prices(path: str | Path) -> Prices:
    try:
        with open(path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except (OSError, yaml.YAMLError) as err:
        raise ConfigError(f"cannot read the prices file {path}: {type(err).__name__}") from err
    try:
        return parse_prices(raw)
    except ConfigError as err:
        raise ConfigError(f"{path}: {err}") from err


# --- rows -----------------------------------------------------------------------------


@dataclass
class UsageEvent:
    tenant_id: str
    usage_key: str
    kind: str
    agent_id: str | None
    agent_version: int | None
    model: str
    # None means the provider did not report the count (an embedding endpoint can omit it).
    input_tokens: int | None = 0
    output_tokens: int | None = 0
    cache_read_tokens: int | None = 0
    cache_write_tokens: int | None = 0
    cost: Decimal | None = None
    conversation_id: str | None = None
    at: datetime | None = None


@dataclass
class UsageGroup:
    """The rows of one tenant and month that share a kind, agent, version and model."""

    kind: str
    agent_id: str | None
    agent_version: int | None
    model: str
    calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    cost: Decimal = Decimal(0)  # the sum of the priced rows only
    unpriced_calls: int = 0


@dataclass(frozen=True)
class Adjustment:
    """The cost an unpriced row got later: appended once per row, never changed."""

    usage_key: str
    cost: Decimal
    price_model: str  # the price entry's model name
    price_effective_from: date


class UsageStore(Protocol):
    async def record(self, event: UsageEvent) -> None:
        """Append ``event``. A repeat of the same (tenant_id, usage_key) stores nothing."""
        ...

    async def unpriced(self, tenant_id: str, start: datetime, end: datetime) -> list[UsageEvent]:
        """The tenant's rows in the window that have no cost and no adjustment."""
        ...

    async def adjust(self, tenant_id: str, rows: list[Adjustment], conn: Any = None) -> int:
        """Append adjustments for rows with no cost. A row already adjusted is skipped.

        Returns how many were stored. ``conn`` joins a unit of work (``unit_of_work.py``).
        """
        ...

    async def totals(self, tenant_id: str, start: datetime, end: datetime) -> list[UsageGroup]:
        """The tenant's rows from ``start`` up to but not including ``end``, grouped."""
        ...


class InMemoryUsageStore:
    def __init__(self) -> None:
        self.events: list[UsageEvent] = []
        self._keys: set[tuple[str, str]] = set()
        self.adjustments: dict[tuple[str, str], Adjustment] = {}

    def snapshot(self) -> tuple[list[UsageEvent], set[tuple[str, str]], dict]:
        """The rows as they are now, for ``InMemoryUnitOfWork``."""
        return list(self.events), set(self._keys), dict(self.adjustments)

    def restore(self, state: tuple[list[UsageEvent], set[tuple[str, str]], dict]) -> None:
        self.events, self._keys, self.adjustments = list(state[0]), set(state[1]), dict(state[2])

    async def record(self, event: UsageEvent) -> None:
        key = (event.tenant_id, event.usage_key)
        if key in self._keys:
            return
        self._keys.add(key)
        self.events.append(event)

    async def unpriced(self, tenant_id: str, start: datetime, end: datetime) -> list[UsageEvent]:
        return [
            e
            for e in self.events
            if e.tenant_id == tenant_id
            and e.at is not None
            and start <= e.at < end
            and e.cost is None
            and (tenant_id, e.usage_key) not in self.adjustments
        ]

    async def adjust(self, tenant_id: str, rows: list[Adjustment], conn: Any = None) -> int:
        stored = 0
        for row in rows:
            key = (tenant_id, row.usage_key)
            event = next((e for e in self.events if (e.tenant_id, e.usage_key) == key), None)
            if event is None or event.cost is not None or key in self.adjustments:
                continue
            self.adjustments[key] = row
            stored += 1
        return stored

    async def totals(self, tenant_id: str, start: datetime, end: datetime) -> list[UsageGroup]:
        groups: dict[tuple, UsageGroup] = {}
        for e in self.events:
            if e.tenant_id != tenant_id or e.at is None or not start <= e.at < end:
                continue
            key = (e.kind, e.agent_id, e.agent_version, e.model)
            g = groups.setdefault(key, UsageGroup(*key))
            g.calls += 1
            g.input_tokens += e.input_tokens or 0
            g.output_tokens += e.output_tokens or 0
            g.cache_read_tokens += e.cache_read_tokens or 0
            g.cache_write_tokens += e.cache_write_tokens or 0
            adjustment = self.adjustments.get((tenant_id, e.usage_key))
            if e.cost is None and adjustment is None:
                g.unpriced_calls += 1
            else:
                g.cost += (e.cost or Decimal(0)) + (
                    adjustment.cost if adjustment is not None else Decimal(0)
                )
        return list(groups.values())


class PostgresUsageStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def record(self, event: UsageEvent) -> None:
        async with tenant_transaction(self._pool, event.tenant_id) as conn:
            await conn.execute(
                """
                INSERT INTO usage_events (
                  tenant_id, usage_key, kind, agent_id, agent_version, model,
                  input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
                  cost_usd, conversation_id, at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, now()))
                ON CONFLICT (tenant_id, usage_key) DO NOTHING
                """,
                (
                    event.tenant_id,
                    event.usage_key,
                    event.kind,
                    event.agent_id,
                    event.agent_version,
                    event.model,
                    event.input_tokens,
                    event.output_tokens,
                    event.cache_read_tokens,
                    event.cache_write_tokens,
                    event.cost,
                    event.conversation_id,
                    event.at,
                ),
            )

    async def unpriced(self, tenant_id: str, start: datetime, end: datetime) -> list[UsageEvent]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                SELECT e.usage_key, e.kind, e.agent_id, e.agent_version, e.model,
                       e.input_tokens, e.output_tokens, e.cache_read_tokens, e.cache_write_tokens,
                       e.conversation_id, e.at
                FROM usage_events e
                WHERE e.tenant_id = %s AND e.at >= %s AND e.at < %s AND e.cost_usd IS NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM usage_adjustments a
                    WHERE a.tenant_id = e.tenant_id AND a.usage_key = e.usage_key)
                ORDER BY e.at, e.usage_key
                """,
                (tenant_id, start, end),
            )
            rows = await cur.fetchall()
        return [
            UsageEvent(tenant_id, key, kind, agent, version, model, *counts, None, conv, at)
            for (key, kind, agent, version, model, *counts, conv, at) in rows
        ]

    async def adjust(self, tenant_id: str, rows: list[Adjustment], conn: Any = None) -> int:
        """Needs INSERT on usage_adjustments, which the app role does not have: owner-run only."""
        stored = 0
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            for row in rows:
                cur = await db.execute(
                    """
                    INSERT INTO usage_adjustments
                      (tenant_id, usage_key, cost_usd, price_model, price_effective_from)
                    SELECT tenant_id, usage_key, %s, %s, %s FROM usage_events
                    WHERE tenant_id = %s AND usage_key = %s AND cost_usd IS NULL
                    ON CONFLICT (tenant_id, usage_key) DO NOTHING
                    """,
                    (row.cost, row.price_model, row.price_effective_from, tenant_id, row.usage_key),
                )
                stored += cur.rowcount
        return stored

    async def totals(self, tenant_id: str, start: datetime, end: datetime) -> list[UsageGroup]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                SELECT e.kind, e.agent_id, e.agent_version, e.model, count(*),
                       COALESCE(sum(e.input_tokens), 0), COALESCE(sum(e.output_tokens), 0),
                       COALESCE(sum(e.cache_read_tokens), 0),
                       COALESCE(sum(e.cache_write_tokens), 0),
                       COALESCE(sum(COALESCE(e.cost_usd, 0) + COALESCE(a.cost_usd, 0)), 0),
                       count(*) FILTER (WHERE e.cost_usd IS NULL AND a.usage_key IS NULL)
                FROM usage_events e
                LEFT JOIN usage_adjustments a
                  ON a.tenant_id = e.tenant_id AND a.usage_key = e.usage_key
                WHERE e.tenant_id = %s AND e.at >= %s AND e.at < %s
                GROUP BY e.kind, e.agent_id, e.agent_version, e.model
                """,
                (tenant_id, start, end),
            )
            rows = await cur.fetchall()
        return [
            UsageGroup(
                kind,
                agent_id,
                version,
                model,
                int(calls),
                int(inp),
                int(out),
                int(cache_read),
                int(cache_write),
                Decimal(cost),
                int(unpriced),
            )
            for (
                kind,
                agent_id,
                version,
                model,
                calls,
                inp,
                out,
                cache_read,
                cache_write,
                cost,
                unpriced,
            ) in rows
        ]


# --- recording ------------------------------------------------------------------------


@dataclass(frozen=True)
class Recorded:
    """What recording a call came to: its cost (None when unpriced) and whether the row is stored."""

    cost: Decimal | None
    stored: bool


class SpendListener(Protocol):
    """Told of every recorded call, so a cached spend figure can follow the row at once."""

    def note_recorded(self, event: UsageEvent, stored: bool) -> None: ...


RECORD_ATTEMPTS = 3
RECORD_WAITS = (0.2, 0.5)  # seconds before the second and third try


class UsageRecorder:
    """Prices a call with the entry in effect now and stores it; never raises.

    The insert is tried ``RECORD_ATTEMPTS`` times; a failure after the last is logged at
    error level with the counts, and ``listener`` is told the row was not stored.
    """

    def __init__(
        self,
        store: UsageStore,
        prices: Prices,
        clock: Callable[[], datetime] = utcnow,
        listener: SpendListener | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self.store = store
        self.prices = prices
        self.clock = clock
        self.listener = listener
        self.sleep = sleep

    async def llm_call(
        self,
        *,
        tenant_id: str,
        agent_id: str,
        agent_version: int,
        conversation_id: str,
        key: str,
        usage: Usage,
        fallback_model: str,
    ) -> Recorded:
        at = self.clock()
        model = usage.model or fallback_model
        return await self._store(
            UsageEvent(
                tenant_id=tenant_id,
                usage_key=key,
                kind=LLM,
                agent_id=agent_id,
                agent_version=agent_version,
                model=model,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cache_read_tokens=usage.cache_read_tokens,
                cache_write_tokens=usage.cache_write_tokens,
                cost=self.prices.cost(model, at, usage),
                conversation_id=conversation_id,
                at=at,
            )
        )

    async def embedding(
        self, *, tenant_id: str, key: str, model: str, billed: int | None
    ) -> Recorded:
        """Embedding input is billed against the tenant, not an agent: a scope can be shared.

        A count the endpoint did not report costs 0 when the model's input price is 0, and is
        unpriced otherwise.
        """
        at = self.clock()
        return await self._store(
            UsageEvent(
                tenant_id=tenant_id,
                usage_key=key,
                kind=EMBEDDING,
                agent_id=None,
                agent_version=None,
                model=model,
                input_tokens=billed,
                cost=self.prices.cost_of(model, at, billed),
                at=at,
            )
        )

    async def _store(self, event: UsageEvent) -> Recorded:
        stored = False
        for attempt in range(1, RECORD_ATTEMPTS + 1):
            try:
                await self.store.record(event)
                stored = True
                break
            except Exception as err:  # noqa: BLE001 - a usage row must never fail the work it measures
                if attempt < RECORD_ATTEMPTS:
                    log.warning(
                        "usage insert failed, retrying tenant_id=%s agent_id=%s attempt=%d "
                        "error_type=%s",
                        event.tenant_id,
                        event.agent_id or "-",
                        attempt,
                        type(err).__name__,
                    )
                    await self.sleep(RECORD_WAITS[min(attempt, len(RECORD_WAITS)) - 1])
                    continue
                log.error(
                    "usage not recorded tenant_id=%s agent_id=%s model=%s input_tokens=%s "
                    "output_tokens=%s cache_read_tokens=%s cache_write_tokens=%s error_type=%s",
                    event.tenant_id,
                    event.agent_id or "-",
                    event.model,
                    event.input_tokens,
                    event.output_tokens,
                    event.cache_read_tokens,
                    event.cache_write_tokens,
                    type(err).__name__,
                )
        if self.listener is not None:
            try:
                self.listener.note_recorded(event, stored)
            except Exception as err:  # noqa: BLE001 - the figure is a cache; the turn goes on
                log.warning("spend listener failed error_type=%s", type(err).__name__)
        return Recorded(event.cost, stored)


# --- the report -----------------------------------------------------------------------

_MONTH = re.compile(r"(\d{4})-(\d{2})")


def month_bounds(month: str | None, now: datetime) -> tuple[datetime, datetime, str]:
    """The UTC calendar month as ``(start, end, "YYYY-MM")``; ``None`` is the month of ``now``."""
    if month is None:
        year, number = now.astimezone(UTC).year, now.astimezone(UTC).month
    else:
        found = _MONTH.fullmatch(month)
        year, number = (int(found[1]), int(found[2])) if found else (0, 0)
        if not (1 <= year <= 9998 and 1 <= number <= 12):
            raise ConfigError("month must look like 2026-11")
    start = datetime(year, number, 1, tzinfo=UTC)
    end = datetime(year + (number == 12), number % 12 + 1, 1, tzinfo=UTC)
    return start, end, f"{year:04d}-{number:02d}"


@dataclass
class Totals:
    calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    cost: Decimal = Decimal(0)  # priced calls only; see unpriced_calls
    unpriced_calls: int = 0

    def add(self, group: UsageGroup) -> None:
        self.calls += group.calls
        self.input_tokens += group.input_tokens
        self.output_tokens += group.output_tokens
        self.cache_read_tokens += group.cache_read_tokens
        self.cache_write_tokens += group.cache_write_tokens
        self.cost += group.cost
        self.unpriced_calls += group.unpriced_calls

    @property
    def tokens(self) -> int:
        return (
            self.input_tokens
            + self.output_tokens
            + self.cache_read_tokens
            + self.cache_write_tokens
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "calls": self.calls,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_read_tokens": self.cache_read_tokens,
            "cache_write_tokens": self.cache_write_tokens,
            "cost_usd": format(self.cost.normalize(), "f"),
            "unpriced_calls": self.unpriced_calls,
        }


@dataclass
class Line:
    """One row of a table: an agent, a team (None is no team) or a model."""

    name: str | None
    totals: Totals
    team: str | None = None  # by agent only


@dataclass
class UsageReport:
    month: str
    total: Totals
    by_agent: list[Line] = field(default_factory=list)
    by_team: list[Line] = field(default_factory=list)
    by_model: list[Line] = field(default_factory=list)
    embedding: Totals | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "month": self.month,
            "total": self.total.as_dict(),
            "by_agent": [
                {"agent_id": x.name, "team": x.team, **x.totals.as_dict()} for x in self.by_agent
            ],
            "by_team": [{"team": x.name, **x.totals.as_dict()} for x in self.by_team],
            "by_model": [{"model": x.name, **x.totals.as_dict()} for x in self.by_model],
            "embedding": None if self.embedding is None else self.embedding.as_dict(),
        }

    def csv_text(self) -> str:
        """The aggregates as CSV, one row per table line. Never the stored rows."""
        out = io.StringIO()
        writer = csv.writer(out, lineterminator="\r\n")
        writer.writerow(
            ["section", "name", "team", "calls", "input_tokens", "output_tokens"]
            + ["cache_read_tokens", "cache_write_tokens", "cost_usd", "unpriced_calls"]
        )

        def row(section: str, name: str | None, team: str | None, totals: Totals) -> None:
            numbers = totals.as_dict()
            writer.writerow(
                [section, _cell(name), _cell(team), *(numbers[k] for k in numbers)],
            )

        for line in self.by_agent:
            row("agent", line.name, line.team, line.totals)
        for line in self.by_team:
            row("team", NO_TEAM if line.name is None else line.name, None, line.totals)
        for line in self.by_model:
            row("model", line.name, None, line.totals)
        if self.embedding is not None:
            row("embedding", None, None, self.embedding)
        row("total", None, None, self.total)
        return out.getvalue()


def _cell(value: str | None) -> str:
    """Text for a spreadsheet cell: one that starts like a formula is made plain text."""
    text = value or ""
    return "'" + text if text[:1] in ("=", "+", "-", "@", "\t", "\r") else text


def _ordered(lines: dict[Any, Line]) -> list[Line]:
    return sorted(lines.values(), key=lambda x: (-x.totals.tokens, x.name is None, x.name or ""))


def build_report(
    month: str,
    groups: list[UsageGroup],
    teams: dict[tuple[str, int], str | None],
) -> UsageReport:
    """Aggregate groups by agent, team and model. ``teams`` maps (agent, version) to a team."""
    report = UsageReport(month=month, total=Totals())
    agents: dict[str, Line] = {}
    newest: dict[str, int] = {}
    by_team: dict[str | None, Line] = {}
    models: dict[str, Line] = {}
    for g in groups:
        report.total.add(g)
        if g.kind == EMBEDDING:
            report.embedding = report.embedding or Totals()
            report.embedding.add(g)
            continue
        team = teams.get((g.agent_id, g.agent_version))
        agent = agents.setdefault(g.agent_id, Line(g.agent_id, Totals()))
        if g.agent_version >= newest.get(g.agent_id, 0):
            # An agent's row shows the team of its newest version that made calls.
            newest[g.agent_id] = g.agent_version
            agent.team = team
        agent.totals.add(g)
        by_team.setdefault(team, Line(team, Totals())).totals.add(g)
        models.setdefault(g.model, Line(g.model, Totals())).totals.add(g)
    report.by_agent = _ordered(agents)
    report.by_team = _ordered(by_team)
    report.by_model = _ordered(models)
    return report


if __name__ == "__main__":
    from agentos.usage_reprice import main

    raise SystemExit(main())
