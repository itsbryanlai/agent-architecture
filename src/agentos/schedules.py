"""Every served tenant's nightly schedules, ensured with no operator step.

The worker ensures each tenant's retention (04:00 UTC) and creator-access (05:00 UTC)
schedules and one index schedule (03:00 UTC) per granted scope: at start-up for every
served tenant, and on the directory cadence for a tenant the sync just admitted, a
reinstall brought back, or a published page grant changed. Each apply is create-or-update
(the modules' own helpers), so a pass that runs twice changes nothing the second time,
and a pass that stopped half-way converges on the next one.

A tenant's failure is logged with its ID and retried on the next pass; it never stops
the worker, another tenant, or serving the tenant. An unchanged tenant causes no
schedule call: the pass compares a fingerprint of its granted scopes. A disabled
(uninstalled) tenant's schedules are left as they are — its runs already end cleanly
(D-39) — and a reinstall re-ensures. The ``--schedule`` commands of the three modules
remain as a manual repair.
"""

from __future__ import annotations

import asyncio
import logging

from temporalio.client import Client

from agentos.agent_store import AgentStore, with_published_agents
from agentos.config import AgentsConfig
from agentos.creator_access import apply_schedule as apply_creator_access_schedule
from agentos.index_knowledge import apply_schedules as apply_index_schedules
from agentos.retention import apply_schedule as apply_retention_schedule
from agentos.tenants import TenantRegistry, granted_roots

log = logging.getLogger("agentos.schedules")


def scope_fingerprint(agents: AgentsConfig) -> tuple[tuple[str, str], ...]:
    """The tenant's granted scopes and their roots, ordered.

    Equal fingerprints mean the tenant's index schedules stand; a publish that changes
    no page grant (a prompt, an audience) causes no schedule call.
    """
    return tuple(sorted(granted_roots(agents).items()))


async def ensure_tenant_schedules(client: Client, agents: AgentsConfig) -> None:
    """Apply one tenant's nightly schedules: retention, creator-access, index per scope.

    ``agents`` carries the tenant's published set (``with_published_agents``). Every
    apply is idempotent (create-or-update, stale index schedules deleted), so this
    converges to the tenant's grants however often it runs. Stored pages are not
    purged here; ``index_knowledge --schedule`` remains the repair that does.
    """
    await apply_retention_schedule(client, agents.tenant_id)
    await apply_creator_access_schedule(client, agents.tenant_id)
    await apply_index_schedules(client, agents)


async def refresh_schedules_once(
    client: Client,
    registry: TenantRegistry,
    agents: AgentStore,
    ensured: dict[str, tuple[tuple[str, str], ...]],
) -> None:
    """One pass over the served tenants: ensure what is new or changed, skip the rest.

    A tenant with no recorded fingerprint (start-up, just admitted, reinstalled) gets
    all three schedule kinds; one whose granted scopes changed gets its index schedules
    re-applied. ``ensured`` records each tenant's fingerprint only after its apply
    succeeded, so a failed tenant is tried again on the next pass. A tenant no longer
    served loses its record, so a reinstall re-ensures. One tenant's failure is logged
    with its ID and never skips the rest.
    """
    served = registry.tenants()
    for tenant_id in list(ensured):
        if tenant_id not in served:
            del ensured[tenant_id]
    for tenant_id in served:
        try:
            config = await with_published_agents(agents, registry.get(tenant_id).config)
            fingerprint = scope_fingerprint(config)
            if ensured.get(tenant_id) == fingerprint:
                continue
            if tenant_id in ensured:
                await apply_index_schedules(client, config)
            else:
                await ensure_tenant_schedules(client, config)
            ensured[tenant_id] = fingerprint
            log.info("tenant_id=%s nightly schedules ensured", tenant_id)
        except Exception as err:  # noqa: BLE001 - one tenant never stops the pass
            log.error(
                "tenant_id=%s schedules not ensured error_type=%s", tenant_id, type(err).__name__
            )


async def run_schedule_refresh(
    client: Client,
    registry: TenantRegistry,
    agents: AgentStore,
    ensured: dict[str, tuple[tuple[str, str], ...]],
    interval_seconds: float,
) -> None:
    """Ensure schedules forever on the directory cadence. A failed pass is logged, not fatal."""
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            await refresh_schedules_once(client, registry, agents, ensured)
        except Exception as err:  # noqa: BLE001 - the loop must outlive one failed pass
            log.error("schedule refresh failed: %s", type(err).__name__)
