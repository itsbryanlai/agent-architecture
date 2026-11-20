"""The nightly check that a creator can still see what they granted an agent.

A creator who consented at the picker keeps their Notion sign-in, encrypted per tenant
and per creator (``tenant_credentials``, kind ``notion_creator:<Slack user ID>``). Once a
night, one schedule per tenant runs ``CreatorAccessWorkflow``: it lists the tenant's
published agents, runs one activity per agent — for each granted Notion object its
creator picked, one cheap read with that creator's stored token, through the tenant's
Notion rate limiter, heartbeating between reads — and ends with the combined health
check. The verdict is stored per object in ``creator_access``:

* ``verified`` — the creator's token can still read the object;
* ``lost`` — Notion refused the creator (403 or 404): the object is gone or unshared;
* ``cannot_verify`` — there is no token to check with: the creator never consented, or
  their token is dead (a 401).

A ``lost`` verdict is sticky: only a successful read (``verified``) replaces it. A
``cannot_verify`` never lifts a ``lost`` — the row stays ``lost`` and the pass counts the
object as unchanged — and never suspends on its own. A transient failure (network error,
rate limit, server error) and any other unexpected error are no verdict: the object's row
stays as it was, and the next pass tries again.

The health check combines both sources: an agent is available only when its grants pass
*and* no granted object is ``lost``. A lost object marks the agent unavailable with the
reason "its creator can no longer see '<grant>'", through the same record path as a grant
problem (audit, best-effort DMs, fixed reply, refused Submit); a later pass where both
sources pass recovers it. Neither source can recover an agent the other still fails, so
the two never flap each other. Recovery from a ``lost`` verdict waits for the next nightly
pass: the worker's five-minute re-check re-evaluates health but does not re-read the
creator's access.

``cannot_verify`` never suspends: without consent the picker works as it always did, and
the console shows the grant as unverifiable. Revoking consent deletes the token; the next
pass writes ``cannot_verify`` again. A consented pick replaces the stored token; unticking
the consent box on a re-pick revokes it. Dropping a creator from the tenant, or moving
them to a role that does not create, deletes it in the same unit of work as the member
change (``control.py``). A disabled (uninstalled) or unknown tenant's pass ends cleanly
with zero counts, before any read. Each creator's Notion client is closed after the pass.

Run ``uv run python -m agentos.creator_access --schedule`` to create or update one nightly
schedule (05:00 UTC, after indexing and retention) per stored tenant; without ``--schedule``
the command starts one pass per tenant on the worker and prints its counts.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

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
from temporalio.common import WorkflowIDConflictPolicy

from agentos import ids
from agentos.agent_store import AgentStore
from agentos.config import KEY_SOURCE_DB, TASK_QUEUE, AgentVersion, load_settings
from agentos.connectors.notion import NotionAPI, RealNotionAPI, is_unreachable, normalize_id
from agentos.connectors.notion_limiter import NotionLimiter
from agentos.credentials import TenantCredentials
from agentos.grant_picks import GrantPickStore
from agentos.stores import joined_transaction
from agentos.temporal_client import build_temporal_client
from agentos.tenant_store import load_stored_tenants
from agentos.tenants import TenantDisabled, UnknownTenant
from agentos.usage import utcnow

if TYPE_CHECKING:
    from agentos.agent_health import AgentHealthChecker
    from agentos.tenants import TenantRegistry

log = logging.getLogger("agentos.creator_access")

VERIFIED = "verified"
LOST = "lost"
CANNOT_VERIFY = "cannot_verify"


@dataclass(frozen=True)
class AccessResult:
    """One object's verdict from one pass. ``notion_id`` is normalised."""

    notion_id: str
    status: str


@dataclass(frozen=True)
class AccessRow:
    """One object's last verdict, and when the pass that wrote it ran."""

    tenant_id: str
    agent_id: str
    notion_id: str
    status: str
    checked_at: datetime


@dataclass
class CreatorAccessCounts:
    """One pass's totals: small values for the workflow history, never content."""

    verified: int = 0
    lost: int = 0
    cannot_verify: int = 0
    unchanged: int = 0

    def add(self, other: CreatorAccessCounts) -> None:
        self.verified += other.verified
        self.lost += other.lost
        self.cannot_verify += other.cannot_verify
        self.unchanged += other.unchanged


class CreatorAccessStore(Protocol):
    async def record(
        self,
        tenant_id: str,
        agent_id: str,
        results: list[AccessResult],
        at: datetime,
        keep: set[str],
        conn: Any = None,
    ) -> None:
        """Store the pass's verdicts and delete the agent's rows for objects not in ``keep``.

        One transaction: an object whose read gave no verdict keeps its old row (its ID is
        in ``keep``), and an object no longer granted leaves no row behind.
        """
        ...

    async def for_agent(
        self, tenant_id: str, agent_id: str, conn: Any = None
    ) -> dict[str, AccessRow]:
        """The agent's verdicts by normalised Notion ID."""
        ...


class InMemoryCreatorAccessStore:
    def __init__(self) -> None:
        self._rows: dict[tuple[str, str, str], AccessRow] = {}

    def snapshot(self) -> dict[tuple[str, str, str], AccessRow]:
        """The rows as they are now, for ``InMemoryUnitOfWork``."""
        return dict(self._rows)

    def restore(self, state: dict[tuple[str, str, str], AccessRow]) -> None:
        self._rows = dict(state)

    async def record(self, tenant_id, agent_id, results, at, keep, conn=None) -> None:
        for key in [k for k in self._rows if k[0] == tenant_id and k[1] == agent_id]:
            if key[2] not in keep:
                del self._rows[key]
        for result in results:
            self._rows[(tenant_id, agent_id, result.notion_id)] = AccessRow(
                tenant_id, agent_id, result.notion_id, result.status, at
            )

    async def for_agent(self, tenant_id, agent_id, conn=None) -> dict[str, AccessRow]:
        return {
            key[2]: row
            for key, row in self._rows.items()
            if key[0] == tenant_id and key[1] == agent_id
        }


class PostgresCreatorAccessStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def record(self, tenant_id, agent_id, results, at, keep, conn=None) -> None:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            for result in results:
                await db.execute(
                    """
                    INSERT INTO creator_access (tenant_id, agent_id, notion_id, status, checked_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (tenant_id, agent_id, notion_id)
                    DO UPDATE SET status = EXCLUDED.status, checked_at = EXCLUDED.checked_at
                    """,
                    (tenant_id, agent_id, result.notion_id, result.status, at),
                )
            await db.execute(
                "DELETE FROM creator_access WHERE tenant_id = %s AND agent_id = %s "
                "AND notion_id <> ALL(%s)",
                (tenant_id, agent_id, sorted(keep)),
            )

    async def for_agent(self, tenant_id, agent_id, conn=None) -> dict[str, AccessRow]:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            cur = await db.execute(
                "SELECT tenant_id, agent_id, notion_id, status, checked_at "
                "FROM creator_access WHERE tenant_id = %s AND agent_id = %s",
                (tenant_id, agent_id),
            )
            rows = await cur.fetchall()
        return {row[2]: AccessRow(*row) for row in rows}


def _beat() -> None:
    """Tell Temporal the attempt is alive. A no-op outside an activity (a direct test call)."""
    try:
        activity.heartbeat()
    except RuntimeError:
        pass


def _sticky_lost(prior: dict[str, AccessRow], notion_id: str) -> bool:
    """Whether the object's lost verdict stands: only a verified read replaces it."""
    row = prior.get(notion_id)
    return row is not None and row.status == LOST


class CreatorAccessChecker:
    """One tenant's pass: read each picked grant with its picker's stored sign-in.

    ``api_for(tenant_id, user_id)`` returns that creator's Notion client, or None when
    they have no stored sign-in. One agent's or creator's failure never skips the rest:
    it is logged with IDs and the error type, and the pass goes on. The clients the pass
    resolved are closed when it ends.
    """

    def __init__(
        self,
        agents: AgentStore,
        picks: GrantPickStore,
        access: CreatorAccessStore,
        api_for: Callable[[str, str], Awaitable[NotionAPI | None]],
        clock: Callable[[], datetime] = utcnow,
    ):
        self._agents = agents
        self._picks = picks
        self._access = access
        self._api_for = api_for
        self._clock = clock

    async def check_tenant(self, tenant_id: str) -> CreatorAccessCounts:
        counts = CreatorAccessCounts()
        published = await self._agents.published_all(tenant_id)
        await self._pass(tenant_id, published.values(), counts)
        return counts

    async def check_agent(self, tenant_id: str, agent_id: str) -> CreatorAccessCounts:
        """One agent's pass, for the workflow's per-agent activity. An agent unpublished
        since the workflow listed it is no verdict."""
        counts = CreatorAccessCounts()
        agent = await self._agents.published(tenant_id, agent_id)
        if agent is not None:
            await self._pass(tenant_id, [agent], counts)
        return counts

    async def _pass(self, tenant_id: str, agents, counts: CreatorAccessCounts) -> None:
        clients: list[NotionAPI] = []
        try:
            for agent in agents:
                try:
                    await self._check_one(tenant_id, agent, counts, clients)
                except Exception as err:  # noqa: BLE001 - one agent never stops the pass
                    log.error(
                        "tenant_id=%s agent_id=%s creator access check failed error_type=%s",
                        tenant_id,
                        agent.agent_id,
                        type(err).__name__,
                    )
        finally:
            for api in clients:
                close = getattr(api, "aclose", None)
                if close is None:
                    continue
                try:
                    await close()
                except Exception as err:  # noqa: BLE001 - a close never fails the pass
                    log.warning(
                        "tenant_id=%s creator client not closed error_type=%s",
                        tenant_id,
                        type(err).__name__,
                    )

    async def _check_one(
        self,
        tenant_id: str,
        agent: AgentVersion,
        counts: CreatorAccessCounts,
        clients: list[NotionAPI],
    ) -> None:
        granted = {
            normalize_id(g.id) for g in (*agent.grants.notion_databases, *agent.grants.notion_pages)
        }
        picks = await self._picks.for_agent(tenant_id, agent.agent_id)
        wanted = {nid: pick for nid, pick in picks.items() if nid in granted}
        by_creator: dict[str, list[str]] = {}
        for nid, pick in wanted.items():
            by_creator.setdefault(pick.picked_by, []).append(nid)
        prior = await self._access.for_agent(tenant_id, agent.agent_id)
        results: list[AccessResult] = []
        for creator, nids in by_creator.items():
            try:
                api = await self._api_for(tenant_id, creator)
            except Exception as err:  # noqa: BLE001 - one creator never stops the pass
                log.warning(
                    "tenant_id=%s creator=%s sign-in not resolved error_type=%s",
                    tenant_id,
                    creator,
                    type(err).__name__,
                )
                counts.unchanged += len(nids)
                continue
            if api is None:
                for nid in nids:
                    if _sticky_lost(prior, nid):
                        counts.unchanged += 1
                    else:
                        results.append(AccessResult(nid, CANNOT_VERIFY))
                        counts.cannot_verify += 1
                continue
            if api not in clients:
                clients.append(api)
            for nid in nids:
                _beat()
                try:
                    if wanted[nid].kind == "database":
                        await api.retrieve_database(nid)
                    else:
                        await api.retrieve_page(nid)
                except Exception as err:  # noqa: BLE001 - one object never stops the pass
                    if is_unreachable(err):
                        results.append(AccessResult(nid, LOST))
                        counts.lost += 1
                    elif getattr(err, "status", None) == 401:
                        # A dead token verifies nothing; a lost verdict stands until a
                        # verified read, so it is no new verdict here either.
                        if _sticky_lost(prior, nid):
                            counts.unchanged += 1
                        else:
                            results.append(AccessResult(nid, CANNOT_VERIFY))
                            counts.cannot_verify += 1
                    else:
                        # Transient or unexpected: no verdict, the row stays as it was.
                        log.warning(
                            "tenant_id=%s agent_id=%s notion_id=%s creator read failed "
                            "error_type=%s",
                            tenant_id,
                            agent.agent_id,
                            nid,
                            type(err).__name__,
                        )
                        counts.unchanged += 1
                    continue
                results.append(AccessResult(nid, VERIFIED))
                counts.verified += 1
        await self._access.record(tenant_id, agent.agent_id, results, self._clock(), set(wanted))


class CreatorAccessActivities:
    """The nightly activities: list the tenant's agents, check each one, then the health
    check that combines both sources. The workflow runs them in that order.

    ``client_for(token, tenant_id)`` builds the Notion client a creator's token reads
    through; the default is a real client sharing the tenant's rate limiter, closed after
    the pass. A test (the replay rig) supplies a fake so no pass reaches Notion.

    A disabled (uninstalled) or unknown tenant's activities end cleanly with zero counts
    before any read: its schedule fires into an empty pass, never a retry.
    """

    def __init__(
        self,
        registry: TenantRegistry,
        agents: AgentStore,
        picks: GrantPickStore,
        credentials: TenantCredentials,
        access: CreatorAccessStore,
        health_checker: AgentHealthChecker,
        clock: Callable[[], datetime] = utcnow,
        client_for: Callable[[str, str], NotionAPI] | None = None,
    ):
        self._registry = registry
        self._agents = agents
        self._health = health_checker

        async def api_for(tenant_id: str, user_id: str) -> NotionAPI | None:
            token = await credentials.creator_notion_token(tenant_id, user_id)
            if token is None:
                return None
            if client_for is not None:
                return client_for(token, tenant_id)
            runtime = registry.get(tenant_id)
            limiter = getattr(runtime.notion_api, "limiter", None) or NotionLimiter()
            return RealNotionAPI(token, limiter=limiter)

        self._checker = CreatorAccessChecker(agents, picks, access, api_for, clock)

    def _served(self, tenant_id: str) -> bool:
        """Whether the tenant is loaded and enabled; a disabled or unknown one is not."""
        try:
            self._registry.get(tenant_id)
        except (TenantDisabled, UnknownTenant):
            log.info(
                "tenant_id=%s not served; creator access pass ends with zero counts", tenant_id
            )
            return False
        return True

    @activity.defn(name="creator_access_agents")
    async def creator_access_agents(self, tenant_id: str) -> list[str]:
        """The tenant's published agent IDs, sorted: the workflow's per-agent loop. IDs
        only, never a definition or a grant's content."""
        if not self._served(tenant_id):
            return []
        return sorted(await self._agents.published_all(tenant_id))

    @activity.defn(name="check_creator_access_agent")
    async def check_creator_access_agent(
        self, tenant_id: str, agent_id: str
    ) -> CreatorAccessCounts:
        if not self._served(tenant_id):
            return CreatorAccessCounts()
        counts = await self._checker.check_agent(tenant_id, agent_id)
        # Counts only; never a grant's content or a token.
        log.info(
            "tenant_id=%s agent_id=%s creator access verified=%d lost=%d cannot_verify=%d "
            "unchanged=%d",
            tenant_id,
            agent_id,
            counts.verified,
            counts.lost,
            counts.cannot_verify,
            counts.unchanged,
        )
        return counts

    @activity.defn(name="creator_access_health")
    async def creator_access_health(self, tenant_id: str) -> None:
        """The combined health check over every published agent, after the access pass."""
        if not self._served(tenant_id):
            return
        await self._health.check_tenant(tenant_id, await self._agents.published_all(tenant_id))

    def all(self) -> list:
        return [
            self.creator_access_agents,
            self.check_creator_access_agent,
            self.creator_access_health,
        ]


def creator_access_schedule(tenant_id: str) -> Schedule:
    from agentos.workflows.creator_access import CreatorAccessWorkflow

    return Schedule(
        action=ScheduleActionStartWorkflow(
            CreatorAccessWorkflow.run,
            args=[tenant_id],
            id=ids.creator_access_workflow_id(tenant_id),
            task_queue=TASK_QUEUE,
        ),
        # Every day at 05:00 UTC, after the 03:00 indexing and 04:00 retention; a run
        # still going when the next is due is skipped.
        spec=ScheduleSpec(
            calendars=[ScheduleCalendarSpec(hour=[ScheduleRange(5)], minute=[ScheduleRange(0)])]
        ),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
    )


async def apply_schedule(client: Client, tenant_id: str) -> str:
    """Create or update the tenant's nightly creator-access schedule; returns what was done."""
    schedule_id = ids.creator_access_schedule_id(tenant_id)
    schedule = creator_access_schedule(tenant_id)
    try:
        await client.create_schedule(schedule_id, schedule)
        return "created"
    except ScheduleAlreadyRunningError:
        await client.get_schedule_handle(schedule_id).update(
            lambda _input: ScheduleUpdate(schedule=schedule)
        )
        return "updated"


async def run(tenant: str | None, schedule: bool) -> None:
    settings = load_settings()
    tenants = await load_stored_tenants(settings.require("database_url"))
    if tenant is not None and tenant not in tenants.tenants:
        raise SystemExit(f"unknown tenant '{tenant}'; stored: {', '.join(tenants.tenants)}")
    selected = [tenant] if tenant else list(tenants.tenants)
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
            if schedule:
                print(
                    f"{await apply_schedule(client, tenant_id)} schedule "
                    f"{ids.creator_access_schedule_id(tenant_id)}"
                )
                continue
            from agentos.workflows.creator_access import CreatorAccessWorkflow

            handle = await client.start_workflow(
                CreatorAccessWorkflow.run,
                tenant_id,
                id=ids.creator_access_workflow_id(tenant_id),
                task_queue=TASK_QUEUE,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            )
            counts = await handle.result()
            print(
                f"tenant_id={tenant_id} verified={counts.verified} lost={counts.lost} "
                f"cannot_verify={counts.cannot_verify} unchanged={counts.unchanged}"
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Check that each published agent's creator can still read every Notion object "
            "they picked for it, with the sign-in they consented to keep. A lost object "
            "suspends the agent through agent health; no consent only marks it cannot verify."
        )
    )
    parser.add_argument("--tenant", help="only this tenant (default: every stored tenant)")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="create or update the nightly (05:00 UTC) schedule instead of running a pass now",
    )
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    asyncio.run(run(args.tenant, args.schedule))
    return 0


if __name__ == "__main__":
    sys.exit(main())
