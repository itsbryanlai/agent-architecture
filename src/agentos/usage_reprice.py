"""Price the usage rows of a past month that were recorded without a price.

Run it as the database owner, after fixing ``config/prices.yaml``::

    export AGENTOS_OWNER_DATABASE_URL=postgresql://<owner>:<password>@localhost:55432/agentos
    uv run python -m agentos.usage reprice --tenant acme --month 2026-11          # dry run
    uv run python -m agentos.usage reprice --tenant acme --month 2026-11 --yes    # write

A row recorded for a model with no price keeps a null cost, and the app role cannot update
it. This appends one ``usage_adjustments`` row for each such row that the price file can now
price, with the price in effect on the day of the call, and records one ``usage_repriced``
audit event for the run. Totals and budgets count a row as priced once it has an adjustment.
The default is a dry run that writes nothing. A row it cannot price (no entry for its model
or day, or a token count the provider never reported where the price is not 0) is listed and
left as it is. The name of the URL variable is all that is ever printed about the connection.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from psycopg_pool import AsyncConnectionPool

from agentos import ids
from agentos.audit import AuditStore
from agentos.config import DEFAULT_PRICES_PATH, TENANT_ID_PATTERN, ConfigError, load_settings
from agentos.models import AuditEvent
from agentos.switches import operator_identity
from agentos.unit_of_work import InMemoryUnitOfWork, UnitOfWork
from agentos.usage import Adjustment, Prices, UsageStore, load_prices, month_bounds, utcnow

OWNER_URL_VAR = "AGENTOS_OWNER_DATABASE_URL"
AUDIT_KIND = "usage_repriced"


@dataclass
class RepriceResult:
    month: str
    adjustments: list[Adjustment] = field(default_factory=list)  # rows that can now be priced
    stored: int = 0  # how many of them were written (0 in a dry run)
    unpriceable: Counter = field(default_factory=Counter)  # model -> rows still without a cost

    @property
    def total(self) -> Decimal:
        return sum((a.cost for a in self.adjustments), Decimal(0))


async def reprice(
    usage: UsageStore,
    audit: AuditStore,
    prices: Prices,
    tenant_id: str,
    month: str,
    *,
    write: bool,
    identity: str,
    now: datetime | None = None,
    uow: UnitOfWork | None = None,
) -> RepriceResult:
    """Work out the adjustments for the month's unpriced rows; store them only when ``write``.

    The adjustments and the audit event commit in one transaction of ``uow``. Without one
    the in-memory stores are covered; stores in Postgres need a ``PostgresUnitOfWork``.
    """
    start, end, label = month_bounds(month, now or utcnow())
    result = RepriceResult(label)
    for row in await usage.unpriced(tenant_id, start, end):
        entry = prices.entry_for(row.model, row.at)
        cost = prices.cost_of(
            row.model,
            row.at,
            row.input_tokens,
            row.output_tokens,
            row.cache_read_tokens,
            row.cache_write_tokens,
        )
        if entry is None or cost is None:
            result.unpriceable[row.model] += 1
            continue
        result.adjustments.append(
            Adjustment(row.usage_key, cost, entry.model, entry.effective_from)
        )
    if not write or not result.adjustments:
        return result
    unit = InMemoryUnitOfWork(usage, audit) if uow is None else uow
    async with unit.begin(tenant_id) as conn:
        result.stored = await usage.adjust(tenant_id, result.adjustments, conn=conn)
        await audit.record(
            AuditEvent(
                tenant_id=tenant_id,
                event_key=ids.audit_control_key(
                    tenant_id, AUDIT_KIND, f"{label}:{uuid.uuid4().hex}"
                ),
                kind=AUDIT_KIND,
                agent_id=None,
                agent_version=None,
                requester_user_id=identity,
                identity=identity,
                tool="reprice",
                target=f"{label} rows={result.stored}",
            ),
            conn=conn,
        )
    return result


def report(result: RepriceResult, write: bool) -> list[str]:
    """The lines the command prints. Models and counts only."""
    lines = []
    by_model = Counter(a.price_model for a in result.adjustments)
    for model, count in sorted(by_model.items()):
        lines.append(f"{model}: {count} row(s) can be priced")
    for model, count in sorted(result.unpriceable.items()):
        lines.append(f"{model}: {count} row(s) still cannot be priced (no price entry in effect)")
    if not result.adjustments:
        lines.append(f"{result.month}: no rows to price")
    elif write:
        lines.append(f"{result.month}: priced {result.stored} row(s), ${result.total:f} in all")
    else:
        lines.append(
            f"{result.month}: would price {len(result.adjustments)} row(s), "
            f"${result.total:f} in all. Nothing written; add --yes to write."
        )
    return lines


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m agentos.usage",
        description="Price usage rows that were recorded without a price. Run as the owner role.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    sub = commands.add_parser("reprice", help="price a month's unpriced rows (dry run by default)")
    sub.add_argument("--tenant", required=True)
    sub.add_argument("--month", required=True, help="the UTC month, such as 2026-11")
    sub.add_argument("--yes", action="store_true", help="write the adjustments")
    sub.add_argument(
        "--prices",
        default=os.environ.get("AGENTOS_PRICES") or DEFAULT_PRICES_PATH,
        help="the prices file (default: AGENTOS_PRICES or config/prices.yaml)",
    )
    sub.add_argument(
        "--url-var",
        default=OWNER_URL_VAR,
        help=f"the variable holding the owner's database URL (default {OWNER_URL_VAR})",
    )
    return parser


async def run(args: argparse.Namespace) -> int:
    from agentos.audit import PostgresAuditStore
    from agentos.unit_of_work import PostgresUnitOfWork
    from agentos.usage import PostgresUsageStore

    if not TENANT_ID_PATTERN.fullmatch(args.tenant):
        print(f"'{args.tenant}' is not a valid tenant ID")
        return 2
    url = os.environ.get(args.url_var)
    if not url:
        print(f"{args.url_var} is not set. Export the owner role's database URL in this shell.")
        return 2
    try:
        prices = load_prices(args.prices)
        month_bounds(args.month, utcnow())
    except ConfigError as err:
        print(f"refused: {err}")
        return 2
    pool = AsyncConnectionPool(url, open=False, **load_settings().pool_args())
    await pool.open()
    try:
        result = await reprice(
            PostgresUsageStore(pool),
            PostgresAuditStore(pool),
            prices,
            args.tenant,
            args.month,
            write=args.yes,
            identity=operator_identity(),
            uow=PostgresUnitOfWork(pool),
        )
    finally:
        await pool.close()
    print("\n".join(report(result, args.yes)))
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(run(build_parser().parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
