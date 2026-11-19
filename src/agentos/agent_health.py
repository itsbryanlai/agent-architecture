"""Agent health: a broken grant disables one agent, never the worker.

A published agent whose Notion grant check fails (a database or page that is
missing or not shared, a property mapping that does not match the live schema,
a missing property ID) is marked **unavailable** with the reason, and the worker
starts and serves every other agent and tenant. A message routed to an
unavailable agent gets one fixed reply, with no model call and no Notion call;
a write waiting for approval is refused at Submit with the same reason. This is
a health state, not an admin's intent: the kill switches (``switches.py``) stay
separate, and an admin turning an agent off is not a health transition.

Health lives in ``agent_health``, one row per tenant and agent, written by the
worker's check and read by the gate on every step. The check runs at start-up,
on the directory refresh for agents that are due (an unavailable agent at most
once every few minutes; a newly published version at once), and a passing check
marks the agent available again. A transient Notion failure (network error,
rate limit, server error) is not a grant problem: it changes no health, and the
next check tries again.

Each transition — to unavailable, or back to available — is audited as
``agent_health`` and told to people once: the agent's creator and the tenant's
admins when it goes unavailable, the creator when it recovers. The audit row
and the row commit in one transaction; the direct messages carry an idempotency
key named by the check that made the transition, so a repeated check tells
nobody twice. A reason names the grant and the problem, never document content.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from psycopg_pool import AsyncConnectionPool

from agentos import ids
from agentos.agent_store import AgentStore
from agentos.audit import AuditStore, canonical_sha256
from agentos.config import AgentVersion
from agentos.grant_checker import GrantCheckUnavailable, NotionGrantChecker
from agentos.members import ADMIN, MemberStore
from agentos.models import AuditEvent
from agentos.slack_format import escape_slack
from agentos.stores import IdempotencyStore, joined_transaction, tenant_transaction
from agentos.tenants import TenantRegistry
from agentos.unit_of_work import InMemoryUnitOfWork, UnitOfWork
from agentos.usage import utcnow

log = logging.getLogger("agentos.agent_health")

AVAILABLE = "available"
UNAVAILABLE = "unavailable"

IDENTITY = "system:agent-health"

# An unavailable agent is re-checked at most this often, however often the directory
# refresh runs. A newly published version is checked at once, without waiting.
HEALTH_RECHECK_SECONDS = 300.0


def agent_unavailable_text(agent_name: str) -> str:
    """The one fixed reply of an unavailable agent, for a message or a refused Submit."""
    return (
        f"{agent_name} is unavailable right now: an admin needs to fix its access. "
        "Nothing was changed."
    )


class AgentUnavailable(Exception):
    """The agent's grant check failed: it may not act until a check passes."""

    def __init__(self, agent_name: str):
        super().__init__(agent_unavailable_text(agent_name))


@dataclass(frozen=True)
class AgentHealth:
    """One agent's health as the worker's last check found it.

    ``checked_at`` is when that check ran; it names the transition's audit event
    and notices, and bounds the next re-check of an unavailable agent.
    """

    tenant_id: str
    agent_id: str
    agent_version: int
    status: str
    reason: str | None
    checked_at: datetime


class HealthStore(Protocol):
    async def get(self, tenant_id: str, agent_id: str) -> AgentHealth | None:
        """The agent's health row, or None when no check has recorded one."""
        ...

    async def list(self, tenant_id: str) -> list[AgentHealth]:
        """Every health row of the tenant, by agent ID."""
        ...

    async def record(self, health: AgentHealth, conn: Any = None) -> bool:
        """Store the check's result. True when the status changed (a transition).

        A first check that finds the agent available is not a transition. The
        version, reason and checked_at are stored whether or not the status changed.
        ``conn`` is the connection of a unit of work, so the row and its audit event
        commit together.
        """
        ...


class InMemoryAgentHealthStore:
    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], AgentHealth] = {}

    def snapshot(self) -> dict[tuple[str, str], AgentHealth]:
        """The rows as they are now, for ``InMemoryUnitOfWork``."""
        return dict(self._rows)

    def restore(self, state: dict[tuple[str, str], AgentHealth]) -> None:
        self._rows = dict(state)

    async def get(self, tenant_id: str, agent_id: str) -> AgentHealth | None:
        return self._rows.get((tenant_id, agent_id))

    async def list(self, tenant_id: str) -> list[AgentHealth]:
        return sorted(
            (h for (t, _), h in self._rows.items() if t == tenant_id),
            key=lambda h: h.agent_id,
        )

    async def record(self, health: AgentHealth, conn: Any = None) -> bool:
        key = (health.tenant_id, health.agent_id)
        old = self._rows.get(key)
        self._rows[key] = health
        return (old.status if old is not None else AVAILABLE) != health.status


_COLUMNS = "tenant_id, agent_id, agent_version, status, reason, checked_at"


class PostgresAgentHealthStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def get(self, tenant_id: str, agent_id: str) -> AgentHealth | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT {_COLUMNS} FROM agent_health WHERE tenant_id = %s AND agent_id = %s",
                (tenant_id, agent_id),
            )
            row = await cur.fetchone()
        return None if row is None else AgentHealth(*row)

    async def list(self, tenant_id: str) -> list[AgentHealth]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT {_COLUMNS} FROM agent_health WHERE tenant_id = %s ORDER BY agent_id",
                (tenant_id,),
            )
            return [AgentHealth(*row) for row in await cur.fetchall()]

    async def record(self, health: AgentHealth, conn: Any = None) -> bool:
        async with joined_transaction(self._pool, health.tenant_id, conn) as db:
            cur = await db.execute(
                """
                WITH prev AS (
                  SELECT status FROM agent_health WHERE tenant_id = %s AND agent_id = %s
                )
                INSERT INTO agent_health (tenant_id, agent_id, agent_version, status, reason,
                                          checked_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, agent_id) DO UPDATE SET
                  agent_version = EXCLUDED.agent_version,
                  status = EXCLUDED.status,
                  reason = EXCLUDED.reason,
                  checked_at = EXCLUDED.checked_at
                RETURNING (SELECT status FROM prev)
                """,
                (
                    health.tenant_id,
                    health.agent_id,
                    health.tenant_id,
                    health.agent_id,
                    health.agent_version,
                    health.status,
                    health.reason,
                    health.checked_at,
                ),
            )
            row = await cur.fetchone()
        old = row[0] if row is not None else None
        return (old or AVAILABLE) != health.status


def unavailable_reason(reports) -> str:
    """One line naming each failing grant and its problems, never document content."""
    return "; ".join(
        f"grant '{report.grant}': {problem}" for report in reports for problem in report.problems
    )


def unavailable_notice_text(agent_name: str, reason: str) -> str:
    """What the creator and admins are told when an agent goes unavailable."""
    return (
        f"{escape_slack(agent_name)} is unavailable right now: {reason}. "
        "An admin needs to fix its access; the agent answers again once a check passes."
    )


def recovered_notice_text(agent_name: str) -> str:
    """What the creator is told when an agent's access checks pass again."""
    return f"{escape_slack(agent_name)} is available again: its Notion access checks passed."


def _is_user_id(value: str) -> bool:
    """A Slack user ID, so an imported agent's placeholder creator is not messaged."""
    return len(value) > 2 and value[0] in "UW" and value.replace("_", "").isalnum()


class AgentHealthChecker:
    """Checks a tenant's published agents' grants and records each agent's health.

    The worker runs this at start-up for every served tenant, and on the directory
    refresh for the agents that are due. A transient Notion failure leaves health
    as it was; only a grant problem makes an agent unavailable, and only a passing
    check makes it available again.
    """

    def __init__(
        self,
        registry: TenantRegistry,
        agents: AgentStore,
        health: HealthStore,
        audit: AuditStore,
        idem: IdempotencyStore,
        members: MemberStore,
        uow: UnitOfWork | None = None,
        clock: Callable[[], datetime] = utcnow,
    ):
        self._registry = registry
        self._agents = agents
        self._health = health
        self._audit = audit
        self._idem = idem
        self._members = members
        self._uow = uow if uow is not None else InMemoryUnitOfWork(health, audit)
        self._clock = clock

    async def check_tenant(self, tenant_id: str, agents: dict[str, AgentVersion]) -> None:
        """Check each given published agent of the tenant and record its health."""
        runtime = self._registry.get(tenant_id)
        checker = NotionGrantChecker(lambda _t: runtime.notion_api)
        for agent in agents.values():
            await self._check_one(tenant_id, runtime, checker, agent)

    async def _check_one(self, tenant_id, runtime, checker, agent: AgentVersion) -> None:
        grants = agent.grants.notion_databases or agent.grants.notion_pages
        if runtime.notion_api is None and grants:
            reason = "the tenant has no Notion token; connect Notion or remove the grants"
            await self._record(tenant_id, runtime, agent, UNAVAILABLE, reason)
            return
        try:
            reports = await checker.check(tenant_id, agent)
        except GrantCheckUnavailable as err:
            # Notion could not be read: no statement about the grants, no change.
            log.warning(
                "tenant_id=%s agent_id=%s health not checked: %s",
                tenant_id,
                agent.agent_id,
                type(err.__cause__).__name__ if err.__cause__ else type(err).__name__,
            )
            return
        for report in reports:
            for note in report.notes:
                log.warning(
                    "tenant_id=%s agent_id=%s grant=%s: %s",
                    tenant_id,
                    agent.agent_id,
                    report.grant,
                    note,
                )
        if any(report.problems for report in reports):
            await self._record(tenant_id, runtime, agent, UNAVAILABLE, unavailable_reason(reports))
        else:
            await self._record(tenant_id, runtime, agent, AVAILABLE, None)

    async def _record(self, tenant_id, runtime, agent: AgentVersion, status, reason) -> None:
        row = AgentHealth(tenant_id, agent.agent_id, agent.version, status, reason, self._clock())
        async with self._uow.begin(tenant_id) as conn:
            changed = await self._health.record(row, conn=conn)
            if changed:
                await self._audit.record(
                    AuditEvent(
                        tenant_id=tenant_id,
                        event_key=ids.audit_agent_health_key(
                            tenant_id, agent.agent_id, row.checked_at.isoformat()
                        ),
                        kind="agent_health",
                        agent_id=agent.agent_id,
                        agent_version=agent.version,
                        requester_user_id=IDENTITY,
                        identity=IDENTITY,
                        tool=status,
                        output_sha256=canonical_sha256(reason) if reason is not None else None,
                    ),
                    conn=conn,
                )
        if not changed:
            return
        log.info(
            "agent health tenant_id=%s agent_id=%s status=%s", tenant_id, agent.agent_id, status
        )
        if status == UNAVAILABLE:
            people = await self._recipients(tenant_id, runtime.config.admins, agent.agent_id)
            text = unavailable_notice_text(agent.name, reason)
        else:
            record = await self._agents.get_agent(tenant_id, agent.agent_id)
            people = (
                [record.created_by] if record is not None and _is_user_id(record.created_by) else []
            )
            text = recovered_notice_text(agent.name)
        await self._notify(tenant_id, runtime, agent.agent_id, row.checked_at, people, text)

    async def _recipients(self, tenant_id: str, config_admins, agent_id: str) -> list[str]:
        """The agent's creator and the tenant's admins, each once."""
        people: list[str] = []
        record = await self._agents.get_agent(tenant_id, agent_id)
        if record is not None:
            people.append(record.created_by)
        people.extend(config_admins)
        people.extend(
            m.user_id for m in await self._members.list_members(tenant_id) if m.role == ADMIN
        )
        return list(dict.fromkeys(p for p in people if _is_user_id(p)))

    async def _notify(self, tenant_id, runtime, agent_id, checked_at, people, text) -> None:
        for person in people:
            key = ids.agent_health_notice_key(tenant_id, agent_id, checked_at.isoformat(), person)
            if await self._idem.get(tenant_id, key) is not None:
                continue
            try:
                channel = await runtime.slack.open_dm(person)
                ts = await runtime.slack.post_message(channel, text)
                await self._idem.put_if_absent(tenant_id, key, {"ts": ts})
            except Exception as err:  # noqa: BLE001 - the health state stands even if a DM fails
                log.warning(
                    "tenant_id=%s agent_id=%s health notice not sent error_type=%s",
                    tenant_id,
                    agent_id,
                    type(err).__name__,
                )


def health_due(
    row: AgentHealth | None, agent: AgentVersion, now: datetime, recheck_after: float
) -> bool:
    """Whether the refresh should check this agent now.

    An agent no check has recorded is checked once. A new published version is
    checked at once. An unavailable agent is re-checked after ``recheck_after``
    seconds; an available one is left alone until its version changes.
    """
    if row is None:
        return True
    if row.agent_version != agent.version:
        return True
    return row.status == UNAVAILABLE and (now - row.checked_at).total_seconds() >= recheck_after


async def refresh_health_once(
    checker: AgentHealthChecker,
    registry: TenantRegistry,
    agents: AgentStore,
    health: HealthStore,
    now: datetime,
    recheck_after: float = HEALTH_RECHECK_SECONDS,
) -> None:
    """One refresh pass: check the agents of every served tenant that are due."""
    for tenant_id in registry.tenants():
        published = await agents.published_all(tenant_id)
        rows = {h.agent_id: h for h in await health.list(tenant_id)}
        due = {
            agent_id: agent
            for agent_id, agent in published.items()
            if health_due(rows.get(agent_id), agent, now, recheck_after)
        }
        if due:
            await checker.check_tenant(tenant_id, due)


async def run_health_refresh(
    checker: AgentHealthChecker,
    registry: TenantRegistry,
    agents: AgentStore,
    health: HealthStore,
    interval_seconds: float,
    clock: Callable[[], datetime] = utcnow,
) -> None:
    """Refresh forever on the directory cadence. A failed pass is logged, not fatal."""
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            await refresh_health_once(checker, registry, agents, health, clock())
        except Exception as err:  # noqa: BLE001 - the loop must outlive one failed pass
            log.error("agent health refresh failed: %s", type(err).__name__)
