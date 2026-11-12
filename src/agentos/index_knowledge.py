"""Index each agent's granted Notion pages, now or on a nightly schedule.

    uv run python -m agentos.index_knowledge             # start one run per scope
    uv run python -m agentos.index_knowledge --wait      # ... and wait for each summary
    uv run python -m agentos.index_knowledge --schedule  # nightly runs at 03:00 UTC
    uv run python -m agentos.index_knowledge --tenant acme  # only one tenant

Needs a running worker (``uv run python -m agentos.worker``), which does the
reading and embedding with its own Notion token and Qwen key. This command
only asks Temporal to start the work and never reads a token itself.

Indexing calls the embeddings endpoint, so it spends embedding credit; pages
whose content has not changed are not embedded again.

One run starts per distinct granted scope of each tenant: two agents of the
same tenant granted the same root page share one run. A run that is already
going is not started twice. Schedules are planned and deleted per tenant, by
that tenant's ID prefix, so one tenant's command never touches another's.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field

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
from agentos.config import TASK_QUEUE, AgentsConfig, load_settings, load_tenants_config
from agentos.temporal_client import build_temporal_client
from agentos.tenants import granted_roots
from agentos.workflows.indexing import IndexingWorkflow

__all__ = ["apply_schedules", "granted_roots", "plan_schedules", "start_indexing"]


async def start_indexing(client: Client, agents: AgentsConfig) -> list[tuple[str, WorkflowHandle]]:
    """Start one IndexingWorkflow per distinct scope; reuse a run already going."""
    started = []
    for scope, root in granted_roots(agents).items():
        workflow_id = ids.index_workflow_id(agents.tenant_id, scope)
        handle = await client.start_workflow(
            IndexingWorkflow.run,
            args=[agents.tenant_id, scope, root],
            id=workflow_id,
            task_queue=TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        started.append((workflow_id, handle))
    return started


@dataclass
class SchedulePlan:
    """Which nightly schedules to create, update and delete (by schedule ID)."""

    create: list[str] = field(default_factory=list)
    update: list[str] = field(default_factory=list)
    delete: list[str] = field(default_factory=list)


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


async def apply_schedules(client: Client, agents: AgentsConfig) -> SchedulePlan:
    """Create or update one nightly schedule per granted scope and delete stale ones."""
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
    return plan


async def run(wait: bool, schedule: bool, tenant: str | None = None) -> None:
    settings = load_settings()
    tenants = load_tenants_config(settings.config_path)
    if tenant is not None and tenant not in tenants.tenants:
        raise SystemExit(f"unknown tenant '{tenant}'; configured: {', '.join(tenants.tenants)}")
    selected = [tenants.tenants[tenant]] if tenant else list(tenants.tenants.values())
    if not any(granted_roots(a) for a in selected):
        print("No agent has a Notion page grant, so there is nothing to index.")
        return
    client = await build_temporal_client(settings)
    if schedule:
        # Every selected tenant is planned, including one with no grants left
        # while another has some, so its stale schedules are deleted.
        for agents in selected:
            plan = await apply_schedules(client, agents)
            for label, items in (
                ("created", plan.create),
                ("updated", plan.update),
                ("deleted", plan.delete),
            ):
                for schedule_id in items:
                    print(f"{label} schedule {schedule_id}")
        return
    started = []
    for agents in selected:
        started += await start_indexing(client, agents)
    for workflow_id, _ in started:
        print(f"started {workflow_id}")
    if wait:
        for workflow_id, handle in started:
            summary = await handle.result()
            print(
                f"{workflow_id}: pages={summary.pages} changed={summary.changed} "
                f"skipped={summary.skipped} pruned={summary.pruned}"
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
    args = parser.parse_args()
    asyncio.run(run(args.wait, args.schedule, args.tenant))


if __name__ == "__main__":
    main()
