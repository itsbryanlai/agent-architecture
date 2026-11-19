"""Index each agent's granted Notion pages, now or on a nightly schedule.

    uv run python -m agentos.index_knowledge             # start one run per scope
    uv run python -m agentos.index_knowledge --wait      # ... and wait for each summary
    uv run python -m agentos.index_knowledge --schedule  # nightly runs at 03:00 UTC
    uv run python -m agentos.index_knowledge --tenant acme  # only one tenant
    uv run python -m agentos.index_knowledge --confirm-gone # delete the index of a root page
                                                            # that is gone, without waiting

Needs a running worker (``uv run python -m agentos.worker``), which does the
reading and embedding with its own Notion token and Qwen key. This command
only asks Temporal to start the work and never reads a token itself.

Indexing calls the embeddings endpoint, so it spends embedding credit; pages
whose content has not changed are not embedded again.

One run starts per distinct granted scope of each tenant: two agents of the
same tenant granted the same root page share one run. A run that is already
going is not started twice. Schedules are planned and deleted per tenant, by
that tenant's ID prefix, so one tenant's command never touches another's.

A scope whose root page is gone or unshared is not deleted by one run: the owner is told, and
the index is deleted when three nightly runs in a row find the root gone. ``--confirm-gone``
is the admin's way to delete it now. It changes nothing for a scope whose root page is back.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field

from psycopg_pool import AsyncConnectionPool
from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleCalendarSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleUpdate,
    WorkflowHandle,
)
from temporalio.common import WorkflowIDConflictPolicy

from agentos import ids
from agentos.agent_store import AgentStore, PostgresAgentStore, with_published_agents
from agentos.config import (
    KEY_SOURCE_DB,
    TASK_QUEUE,
    AgentsConfig,
    Settings,
    load_settings,
)
from agentos.knowledge.store import KnowledgeStore, PostgresKnowledgeStore
from agentos.temporal_client import build_temporal_client
from agentos.tenant_store import load_stored_tenants
from agentos.tenants import granted_roots
from agentos.workflows.indexing import IndexingWorkflow

__all__ = ["apply_schedules", "granted_roots", "plan_schedules", "start_indexing"]


async def start_indexing(
    client: Client, agents: AgentsConfig, confirm_gone: bool = False
) -> list[tuple[str, WorkflowHandle]]:
    """Start one IndexingWorkflow per distinct scope; reuse a run already going.

    With ``confirm_gone``, a run that finds its scope's root page gone deletes the scope's
    index at once instead of waiting for repeated runs. A run already going is reused as it is.
    """
    started = []
    for scope, root in granted_roots(agents).items():
        workflow_id = ids.index_workflow_id(agents.tenant_id, scope)
        handle = await client.start_workflow(
            IndexingWorkflow.run,
            args=[agents.tenant_id, scope, root, *_FRESH_RUN, confirm_gone],
            id=workflow_id,
            task_queue=TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        started.append((workflow_id, handle))
    return started


# The walk state of a run that has only begun: nothing seen, no start time, no parents.
_FRESH_RUN = (None, None, None, 0, 0, None, None, 0)


@dataclass
class SchedulePlan:
    """Which nightly schedules to create, update and delete (by schedule ID)."""

    create: list[str] = field(default_factory=list)
    update: list[str] = field(default_factory=list)
    delete: list[str] = field(default_factory=list)
    purged: list[str] = field(default_factory=list)  # scopes whose stored rows were deleted


def plan_schedules(agents: AgentsConfig, existing_ids: list[str]) -> SchedulePlan:
    """One schedule per granted scope; stale ones of this tenant are deleted.

    ``existing_ids`` is every schedule ID that exists. Only IDs with this
    tenant's index-schedule prefix are considered, so other schedules are
    never touched.
    """
    prefix = ids.index_schedule_id(agents.tenant_id, "")
    wanted = [ids.index_schedule_id(agents.tenant_id, scope) for scope in granted_roots(agents)]
    ours = {i for i in existing_ids if i.startswith(prefix)}
    return SchedulePlan(
        create=[i for i in wanted if i not in ours],
        update=[i for i in wanted if i in ours],
        delete=sorted(ours - set(wanted)),
    )


def _schedule(agents: AgentsConfig, scope: str, root: str) -> Schedule:
    return Schedule(
        action=ScheduleActionStartWorkflow(
            IndexingWorkflow.run,
            args=[agents.tenant_id, scope, root],
            id=ids.index_workflow_id(agents.tenant_id, scope),
            task_queue=TASK_QUEUE,
        ),
        # Every day at 03:00 UTC; a run still going when the next is due is skipped.
        spec=ScheduleSpec(
            calendars=[ScheduleCalendarSpec(hour=[ScheduleRange(3)], minute=[ScheduleRange(0)])]
        ),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
    )


async def apply_schedules(
    client: Client, agents: AgentsConfig, store: KnowledgeStore | None = None
) -> SchedulePlan:
    """Create or update one nightly schedule per granted scope and delete stale ones.

    With a ``store``, the stored pages of every scope of this tenant that is
    no longer granted are deleted too, so a revoked grant leaves no rows.
    """
    existing = [s.id async for s in await client.list_schedules()]
    plan = plan_schedules(agents, existing)
    for scope, root in granted_roots(agents).items():
        schedule_id = ids.index_schedule_id(agents.tenant_id, scope)
        schedule = _schedule(agents, scope, root)
        if schedule_id in plan.create:
            await client.create_schedule(schedule_id, schedule)
        elif schedule_id in plan.update:
            await client.get_schedule_handle(schedule_id).update(
                lambda _input, schedule=schedule: ScheduleUpdate(schedule=schedule)
            )
    for schedule_id in plan.delete:
        await client.get_schedule_handle(schedule_id).delete()
    if store is not None:
        granted = granted_roots(agents)
        for scope in await store.scopes(agents.tenant_id):
            if scope not in granted:
                await store.delete_scope(agents.tenant_id, scope)
                plan.purged.append(scope)
    return plan


@asynccontextmanager
async def open_store(settings: Settings) -> AsyncIterator[KnowledgeStore]:
    """The knowledge store, over the same database the worker uses."""
    pool = AsyncConnectionPool(settings.require("database_url"), open=False, **settings.pool_args())
    await pool.open()
    try:
        yield PostgresKnowledgeStore(pool)
    finally:
        await pool.close()


@asynccontextmanager
async def open_agent_store(settings: Settings) -> AsyncIterator[AgentStore]:
    """The agent store, over the same database the worker uses."""
    pool = AsyncConnectionPool(settings.require("database_url"), open=False, **settings.pool_args())
    await pool.open()
    try:
        yield PostgresAgentStore(pool)
    finally:
        await pool.close()


async def run(
    wait: bool, schedule: bool, tenant: str | None = None, confirm_gone: bool = False
) -> None:
    settings = load_settings()
    tenants = await load_stored_tenants(settings.require("database_url"))
    if tenant is not None and tenant not in tenants.tenants:
        raise SystemExit(f"unknown tenant '{tenant}'; configured: {', '.join(tenants.tenants)}")
    selected = [tenants.tenants[tenant]] if tenant else list(tenants.tenants.values())
    # Grants come from each tenant's published agents, not from the YAML file.
    async with open_agent_store(settings) as agent_store:
        selected = [await with_published_agents(agent_store, config) for config in selected]
    empty = [config.tenant_id for config in selected if not config.agents]
    if empty:
        # An empty published set would read as "every grant revoked" and delete
        # the tenant's schedules and stored pages.
        raise SystemExit(
            f"tenant(s) {', '.join(empty)} have no published agents. "
            "Run `uv run python -m agentos.agent_store import` first."
        )
    if not schedule and not any(granted_roots(a) for a in selected):
        print("No published agent has a Notion page grant, so there is nothing to index.")
        return
    async with AsyncExitStack() as stack:
        pool = None
        if settings.key_source == KEY_SOURCE_DB:
            pool = await stack.enter_async_context(
                AsyncConnectionPool(
                    settings.require("database_url"), open=False, **settings.pool_args()
                )
            )
        client = await build_temporal_client(settings, pool=pool)
        if schedule:
            # Every selected tenant is planned, including one with no grants left,
            # so its stale schedules and stored pages are deleted.
            async with open_store(settings) as store:
                for agents in selected:
                    plan = await apply_schedules(client, agents, store)
                    for label, items in (
                        ("created schedule", plan.create),
                        ("updated schedule", plan.update),
                        ("deleted schedule", plan.delete),
                        ("deleted stored pages of", plan.purged),
                    ):
                        for item in items:
                            print(f"{label} {item}")
            return
        started = []
        for agents in selected:
            started += await start_indexing(client, agents, confirm_gone)
        for workflow_id, _ in started:
            print(f"started {workflow_id}")
        if wait:
            for workflow_id, handle in started:
                summary = await handle.result()
                print(
                    f"{workflow_id}: pages={summary.pages} changed={summary.changed} "
                    f"skipped={summary.skipped} failed={summary.failed} pruned={summary.pruned}"
                    + (" root-gone" if summary.root_gone else "")
                )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Index the Notion pages granted to agents. Needs a running worker. "
            "Indexing spends embedding credit; unchanged pages are not embedded again."
        )
    )
    parser.add_argument("--wait", action="store_true", help="wait for each run and print a summary")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="create or update one nightly (03:00 UTC) schedule per scope and delete stale ones",
    )
    parser.add_argument("--tenant", help="only this tenant (default: every configured tenant)")
    parser.add_argument(
        "--confirm-gone",
        action="store_true",
        help=(
            "start runs that delete the index of a scope whose root page is gone, "
            "without waiting for three nightly runs"
        ),
    )
    args = parser.parse_args()
    asyncio.run(run(args.wait, args.schedule, args.tenant, args.confirm_gone))


if __name__ == "__main__":
    main()
