"""Retention: delete a tenant's expired rows, in batches, on a nightly schedule.

    uv run python -m agentos.retention --tenant acme      # purge one tenant now
    uv run python -m agentos.retention                    # ... every tenant
    uv run python -m agentos.retention --schedule         # nightly runs at 04:00 UTC
    uv run python -m agentos.retention set --tenant acme --messages-days 180   # as the owner role

Settings live on the tenant (``tenants.retention``, whole days per kind of data); an empty
object means the defaults below. What is purged:

* conversation messages, per conversation: only a conversation whose newest message is older
  than the limit, so a thread is never cut in the middle;
* tool outputs, tool inputs and model steps (what the model replied, kept until it was shown):
  older than the tool-output limit, in a conversation with no newer message;
* inbound events already delivered to their workflow, and the saved Edit forms among them, older
  than the idempotency limit (an event not yet delivered is never purged);
* the drafts of a task whose newest revision is older than the tool-output limit;
* threads (the specialist a thread was handed to) idle for longer than the messages limit;
* mentions of an agent's owner (``owner_pings``) older than two days, which is all they are read for;
* idempotency records older than the limit;
* sessions, a number of days after they expired (a session is purged at login too).

Never purged: audit and usage rows (append-only by design, and the app role has no DELETE on
them), ``external_writes``, ``task_links`` and agent definitions. A limit is never below the longest task life a definition may hold: the
largest approval window and the largest watch that validation allows (``config.py``), plus a
day. ``set`` refuses a lower value, and the job raises a stored lower value to it, so a row a
live approval or follow-up watch still reads is never deleted, whatever version an agent is
on. The job logs counts only.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import sys
import uuid
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from psycopg_pool import AsyncConnectionPool
from temporalio import activity
from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleCalendarSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleUpdate,
)

from agentos import ids
from agentos.config import (
    KEY_SOURCE_DB,
    MAX_APPROVAL_EXPIRES_HOURS,
    MAX_WATCH_DAYS,
    TASK_QUEUE,
    TENANT_ID_PATTERN,
    ConfigError,
    load_settings,
)
from agentos.stores import tenant_transaction
from agentos.temporal_client import build_temporal_client
from agentos.tenant_store import load_stored_tenants

log = logging.getLogger("agentos.retention")

OWNER_URL_VAR = "AGENTOS_OWNER_DATABASE_URL"
AUDIT_KIND = "retention_set"

# Rows deleted per statement, and the most batches one workflow run makes.
BATCH_SIZE = 500
MAX_ROUNDS = 400

# Days, per kind of data. Messages are kept a year because people reopen old threads; tool
# output text and idempotency records only matter while a task or retry can still read them,
# so three months; a session is purged the day after it expired.
DEFAULT_DAYS = {
    "messages_days": 365,
    "tool_outputs_days": 90,
    "idempotency_days": 90,
    "sessions_days": 1,
}
# An owner is mentioned in a refusal once a day for each person and place; a row is read only for that.
OWNER_PING_DAYS = 2
# Kinds a live task may still need are held to the tenant's longest task life.
TASK_BOUND = ("messages_days", "tool_outputs_days", "idempotency_days")
# The longest a task can live: the largest approval window and watch a definition may hold, and a day.
MIN_TASK_DAYS = math.ceil(MAX_APPROVAL_EXPIRES_HOURS / 24) + MAX_WATCH_DAYS + 1


@dataclass(frozen=True)
class RetentionPolicy:
    messages_days: int
    tool_outputs_days: int
    idempotency_days: int
    sessions_days: int

    def days(self, kind: str) -> int:
        return getattr(self, kind)


def parse_policy(raw: dict | None, longest_task_days: int = MIN_TASK_DAYS) -> RetentionPolicy:
    """The tenant's settings over the defaults, with task-bound kinds raised to the task life."""
    raw = raw or {}
    unknown = sorted(set(raw) - set(DEFAULT_DAYS))
    if unknown:
        raise ConfigError(f"unknown retention setting(s): {', '.join(unknown)}")
    values: dict[str, int] = {}
    for kind, default in DEFAULT_DAYS.items():
        value = raw.get(kind, default)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ConfigError(f"retention {kind} must be a whole number of days; got {value!r}")
        values[kind] = max(value, longest_task_days) if kind in TASK_BOUND else value
    return RetentionPolicy(**values)


def check_new_limits(changes: dict[str, int]) -> dict[str, int]:
    """The limits an owner may store: known kinds, whole days, and not below the task life."""
    if not changes:
        raise ConfigError("name at least one limit to set")
    for kind, value in changes.items():
        parse_policy({kind: value}, longest_task_days=0)  # known kind, whole days
        if kind in TASK_BOUND and value < MIN_TASK_DAYS:
            raise ConfigError(
                f"{kind} cannot be below {MIN_TASK_DAYS} days: a task can run that long "
                "(the longest approval window and watch a definition may hold, and a day)"
            )
    return dict(changes)


async def set_limits(
    pool: AsyncConnectionPool,
    tenant_id: str,
    changes: dict[str, int],
    identity: str,
    request_id: str | None = None,
) -> None:
    """Store limits on the tenant and audit it, in one transaction."""
    from agentos.audit import PostgresAuditStore
    from agentos.models import AuditEvent
    from agentos.unit_of_work import PostgresUnitOfWork

    changes = check_new_limits(changes)
    audit = PostgresAuditStore(pool)
    async with PostgresUnitOfWork(pool).begin(tenant_id) as conn:
        cur = await conn.execute(
            "UPDATE tenants SET retention = retention || %s::jsonb WHERE tenant_id = %s",
            (json.dumps(changes), tenant_id),
        )
        if cur.rowcount != 1:
            raise ConfigError(f"unknown tenant '{tenant_id}'")
        await audit.record(
            AuditEvent(
                tenant_id=tenant_id,
                event_key=ids.audit_control_key(
                    tenant_id, AUDIT_KIND, request_id or uuid.uuid4().hex
                ),
                kind=AUDIT_KIND,
                agent_id=None,
                agent_version=None,
                requester_user_id=identity,
                identity=identity,
                tool="retention",
                target=",".join(f"{k}={v}" for k, v in sorted(changes.items())),
            ),
            conn=conn,
        )


@dataclass
class PurgeCounts:
    """Rows deleted by one batch round, per kind. ``more`` is true when a kind hit the batch size."""

    messages: int = 0
    tool_outputs: int = 0
    idempotency: int = 0
    sessions: int = 0
    more: bool = False
    tool_inputs: int = 0
    model_steps: int = 0
    inbound_events: int = 0
    task_drafts: int = 0
    threads: int = 0
    owner_pings: int = 0

    def add(self, other: PurgeCounts) -> None:
        self.messages += other.messages
        self.tool_outputs += other.tool_outputs
        self.idempotency += other.idempotency
        self.sessions += other.sessions
        self.tool_inputs += other.tool_inputs
        self.model_steps += other.model_steps
        self.inbound_events += other.inbound_events
        self.task_drafts += other.task_drafts
        self.threads += other.threads
        self.owner_pings += other.owner_pings

    def kinds(self) -> dict[str, int]:
        """The rows deleted, per kind, for a log line or the command line."""
        return {k: v for k, v in vars(self).items() if k != "more"}


class RetentionStore(Protocol):
    async def policy(self, tenant_id: str) -> RetentionPolicy:
        """The tenant's effective policy. Raises ``ValueError`` for an unknown tenant."""
        ...

    async def purge_batch(
        self, tenant_id: str, policy: RetentionPolicy, now: datetime, limit: int = BATCH_SIZE
    ) -> PurgeCounts: ...


class PostgresRetentionStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def policy(self, tenant_id: str) -> RetentionPolicy:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                "SELECT retention FROM tenants WHERE tenant_id = %s", (tenant_id,)
            )
            row = await cur.fetchone()
            if row is None:
                raise ValueError(f"unknown tenant {tenant_id!r}")
        return parse_policy(row[0])

    async def purge_batch(
        self, tenant_id: str, policy: RetentionPolicy, now: datetime, limit: int = BATCH_SIZE
    ) -> PurgeCounts:
        def cutoff(kind: str) -> datetime:
            return now - timedelta(days=policy.days(kind))

        counts = PurgeCounts()
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                DELETE FROM tool_outputs o
                WHERE (o.tenant_id, o.conversation_id, o.tool_use_id) IN (
                  SELECT t.tenant_id, t.conversation_id, t.tool_use_id FROM tool_outputs t
                  WHERE t.tenant_id = %(t)s AND t.created_at < %(cutoff)s
                    AND NOT EXISTS (
                      SELECT 1 FROM conversation_messages m
                      WHERE m.tenant_id = t.tenant_id AND m.conversation_id = t.conversation_id
                        AND m.created_at >= %(cutoff)s)
                  LIMIT %(limit)s)
                """,
                {"t": tenant_id, "cutoff": cutoff("tool_outputs_days"), "limit": limit},
            )
            counts.tool_outputs = cur.rowcount
            # What the model said and was asked for in a conversation is purged with that
            # conversation's tool outputs: nothing reads it once the conversation is idle.
            for table, key, attr in (
                ("tool_inputs", "tool_use_id", "tool_inputs"),
                ("model_steps", "step_key", "model_steps"),
            ):
                cur = await conn.execute(
                    f"""
                    DELETE FROM {table} o
                    WHERE (o.tenant_id, o.conversation_id, o.{key}) IN (
                      SELECT t.tenant_id, t.conversation_id, t.{key} FROM {table} t
                      WHERE t.tenant_id = %(t)s AND t.created_at < %(cutoff)s
                        AND NOT EXISTS (
                          SELECT 1 FROM conversation_messages m
                          WHERE m.tenant_id = t.tenant_id
                            AND m.conversation_id = t.conversation_id
                            AND m.created_at >= %(cutoff)s)
                      LIMIT %(limit)s)
                    """,
                    {"t": tenant_id, "cutoff": cutoff("tool_outputs_days"), "limit": limit},
                )
                setattr(counts, attr, cur.rowcount)
            cur = await conn.execute(
                """
                DELETE FROM task_drafts
                WHERE tenant_id = %(t)s AND task_id IN (
                  SELECT task_id FROM task_drafts WHERE tenant_id = %(t)s
                  GROUP BY task_id HAVING MAX(created_at) < %(cutoff)s LIMIT %(limit)s)
                """,
                {"t": tenant_id, "cutoff": cutoff("tool_outputs_days"), "limit": limit},
            )
            counts.task_drafts = cur.rowcount
            cur = await conn.execute(
                """
                DELETE FROM inbound_events
                WHERE (tenant_id, event_id) IN (
                  SELECT tenant_id, event_id FROM inbound_events
                  WHERE tenant_id = %(t)s AND delivered_at < %(cutoff)s LIMIT %(limit)s)
                """,
                {"t": tenant_id, "cutoff": cutoff("idempotency_days"), "limit": limit},
            )
            counts.inbound_events = cur.rowcount
            cur = await conn.execute(
                """
                DELETE FROM threads
                WHERE (tenant_id, thread_key) IN (
                  SELECT tenant_id, thread_key FROM threads
                  WHERE tenant_id = %(t)s AND updated_at < %(cutoff)s LIMIT %(limit)s)
                """,
                {"t": tenant_id, "cutoff": cutoff("messages_days"), "limit": limit},
            )
            counts.threads = cur.rowcount
            cur = await conn.execute(
                """
                DELETE FROM owner_pings
                WHERE (tenant_id, agent_id, channel, asker_user_id) IN (
                  SELECT tenant_id, agent_id, channel, asker_user_id FROM owner_pings
                  WHERE tenant_id = %(t)s AND notified_on < %(day)s LIMIT %(limit)s)
                """,
                {
                    "t": tenant_id,
                    "day": (now - timedelta(days=OWNER_PING_DAYS)).date(),
                    "limit": limit,
                },
            )
            counts.owner_pings = cur.rowcount
            cur = await conn.execute(
                """
                DELETE FROM conversation_messages
                WHERE tenant_id = %(t)s AND conversation_id IN (
                  SELECT conversation_id FROM conversation_messages WHERE tenant_id = %(t)s
                  GROUP BY conversation_id HAVING MAX(created_at) < %(cutoff)s
                  LIMIT %(limit)s)
                """,
                {"t": tenant_id, "cutoff": cutoff("messages_days"), "limit": limit},
            )
            counts.messages = cur.rowcount
            cur = await conn.execute(
                """
                DELETE FROM idempotency_records
                WHERE (tenant_id, idem_key) IN (
                  SELECT tenant_id, idem_key FROM idempotency_records
                  WHERE tenant_id = %(t)s AND created_at < %(cutoff)s LIMIT %(limit)s)
                """,
                {"t": tenant_id, "cutoff": cutoff("idempotency_days"), "limit": limit},
            )
            counts.idempotency = cur.rowcount
            cur = await conn.execute(
                """
                DELETE FROM sessions
                WHERE (tenant_id, session_hash) IN (
                  SELECT tenant_id, session_hash FROM sessions
                  WHERE tenant_id = %(t)s AND expires_at < %(cutoff)s LIMIT %(limit)s)
                """,
                {"t": tenant_id, "cutoff": cutoff("sessions_days"), "limit": limit},
            )
            counts.sessions = cur.rowcount
        # A kind that deleted a full batch may have more due. Messages go by whole conversations,
        # so one batch may delete more rows than ``limit``.
        counts.more = max(v for k, v in counts.kinds().items()) >= limit
        return counts


class InMemoryRetentionStore:
    """A scripted store for workflow tests: each call to ``purge_batch`` pops the next counts."""

    def __init__(self, rounds: list[PurgeCounts], policy: RetentionPolicy | None = None):
        self.rounds = list(rounds)
        self._policy = policy or parse_policy({})
        self.calls: list[str] = []

    async def policy(self, tenant_id: str) -> RetentionPolicy:
        return self._policy

    async def purge_batch(
        self, tenant_id: str, policy: RetentionPolicy, now: datetime, limit: int = BATCH_SIZE
    ) -> PurgeCounts:
        self.calls.append(tenant_id)
        return self.rounds.pop(0) if self.rounds else PurgeCounts()


def _utcnow() -> datetime:
    return datetime.now(UTC)


class RetentionActivities:
    def __init__(self, store: RetentionStore, clock=_utcnow):
        self._store = store
        self._clock = clock

    @activity.defn(name="purge_retention_batch")
    async def purge_retention_batch(self, tenant_id: str) -> PurgeCounts:
        policy = await self._store.policy(tenant_id)
        counts = await self._store.purge_batch(tenant_id, policy, self._clock())
        # Counts only; never a row's content.
        log.info(
            "tenant_id=%s retention %s",
            tenant_id,
            " ".join(f"{kind}={n}" for kind, n in counts.kinds().items()),
        )
        return counts

    def all(self) -> list:
        return [self.purge_retention_batch]


async def purge_all(
    store: RetentionStore, tenant_id: str, clock=_utcnow, max_rounds: int = MAX_ROUNDS
) -> PurgeCounts:
    """Purge one tenant until nothing more is due, for the command line."""
    total = PurgeCounts()
    policy = await store.policy(tenant_id)
    for _ in range(max_rounds):
        round_ = await store.purge_batch(tenant_id, policy, clock())
        total.add(round_)
        if not round_.more:
            break
    return total


def retention_schedule(tenant_id: str) -> Schedule:
    from agentos.workflows.retention import RetentionWorkflow

    return Schedule(
        action=ScheduleActionStartWorkflow(
            RetentionWorkflow.run,
            args=[tenant_id],
            id=ids.retention_workflow_id(tenant_id),
            task_queue=TASK_QUEUE,
        ),
        # Every day at 04:00 UTC, after the 03:00 indexing; a run still going is skipped.
        spec=ScheduleSpec(
            calendars=[ScheduleCalendarSpec(hour=[ScheduleRange(4)], minute=[ScheduleRange(0)])]
        ),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
    )


async def apply_schedule(client: Client, tenant_id: str) -> str:
    """Create or update the tenant's nightly retention schedule; returns what was done."""
    schedule_id = ids.retention_schedule_id(tenant_id)
    schedule = retention_schedule(tenant_id)
    try:
        await client.create_schedule(schedule_id, schedule)
        return "created"
    except ScheduleAlreadyRunningError:
        await client.get_schedule_handle(schedule_id).update(
            lambda _input: ScheduleUpdate(schedule=schedule)
        )
        return "updated"


async def run(tenant: str | None, schedule: bool, clock=_utcnow) -> None:
    settings = load_settings()
    tenants = await load_stored_tenants(settings.require("database_url"))
    if tenant is not None and tenant not in tenants.tenants:
        raise SystemExit(f"unknown tenant '{tenant}'; configured: {', '.join(tenants.tenants)}")
    selected = [tenant] if tenant else list(tenants.tenants)
    if schedule:
        async with AsyncExitStack() as stack:
            pool = None
            if settings.key_source == KEY_SOURCE_DB:
                pool = await stack.enter_async_context(
                    AsyncConnectionPool(
                        settings.require("database_url"), open=False, **settings.pool_args()
                    )
                )
            client = await build_temporal_client(settings, pool=pool)
            for tenant_id in selected:
                print(
                    f"{await apply_schedule(client, tenant_id)} schedule "
                    f"{ids.retention_schedule_id(tenant_id)}"
                )
        return
    pool = AsyncConnectionPool(settings.require("database_url"), open=False, **settings.pool_args())
    await pool.open()
    try:
        store = PostgresRetentionStore(pool)
        for tenant_id in selected:
            total = await purge_all(store, tenant_id, clock)
            print(
                f"tenant_id={tenant_id} "
                + " ".join(f"{kind}={n}" for kind, n in total.kinds().items())
            )
    finally:
        await pool.close()


def build_set_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m agentos.retention set",
        description=(
            "Set a tenant's retention limits, in whole days. Run as the owner role. "
            "A limit below the longest task life is refused. The change is audited."
        ),
    )
    parser.add_argument("--tenant", required=True)
    for kind in DEFAULT_DAYS:
        parser.add_argument(f"--{kind.replace('_', '-')}", type=int, dest=kind)
    parser.add_argument(
        "--url-var",
        default=OWNER_URL_VAR,
        help=f"the variable holding the owner's database URL (default {OWNER_URL_VAR})",
    )
    return parser


async def run_set(args: argparse.Namespace) -> int:
    from agentos.switches import operator_identity

    if not TENANT_ID_PATTERN.fullmatch(args.tenant):
        print(f"'{args.tenant}' is not a valid tenant ID")
        return 2
    url = os.environ.get(args.url_var)
    if not url:
        print(f"{args.url_var} is not set. Export the owner role's database URL in this shell.")
        return 2
    changes = {k: getattr(args, k) for k in DEFAULT_DAYS if getattr(args, k) is not None}
    pool = AsyncConnectionPool(url, open=False, **load_settings().pool_args())
    await pool.open()
    try:
        await set_limits(pool, args.tenant, changes, operator_identity(), uuid.uuid4().hex)
    except ConfigError as err:
        print(f"refused: {err}")
        return 2
    finally:
        await pool.close()
    shown = " ".join(f"{k}={v}" for k, v in sorted(changes.items()))
    print(f"tenant_id={args.tenant} set {shown}")
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["set"]:
        return asyncio.run(run_set(build_set_parser().parse_args(argv[1:])))
    parser = argparse.ArgumentParser(
        description=(
            "Delete a tenant's expired conversation messages, stored content, idempotency "
            "records, idle threads and sessions. Audit and usage rows are kept. "
            "To change a tenant's limits: python -m agentos.retention set --help"
        )
    )
    parser.add_argument("--tenant", help="only this tenant (default: every stored tenant)")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="create or update the nightly (04:00 UTC) retention schedule instead of purging now",
    )
    args = parser.parse_args(argv)
    asyncio.run(run(args.tenant, args.schedule))
    return 0


if __name__ == "__main__":
    sys.exit(main())
