"""Check each Notion grant against Notion and print each database's property IDs.

Run ``uv run python -m agentos.check_grants`` to check every enabled tenant's *published*
agents as they are stored in the database, which is what the worker serves. To get the
property IDs of a version before it is published, name it:
``--tenant T --agent A [--version N]`` (default: the agent's newest draft or pending version,
else its published one). The worker will not start on a published agent without property IDs,
so run this on the draft, paste the ``property_ids`` block into the definition, then publish.
It reads only the granted databases' schemas, with that tenant's own Notion token (its
encrypted credential row first, else the variable named in its ``secrets`` block,
``NOTION_TOKEN`` by default); it never changes a database or a definition. Page grants are
checked by reading the page: it must be readable and not trashed or archived. Exits 1 if
any grant has a problem, otherwise 0.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import sys
from collections.abc import Callable

from psycopg_pool import AsyncConnectionPool

from agentos.agent_store import PENDING, AgentStore, PostgresAgentStore
from agentos.config import AgentsConfig, AgentVersion, ConfigError, TenantsConfig, load_settings
from agentos.connectors.notion import RealNotionAPI, check_grant, check_page_grant
from agentos.credentials import PostgresCredentialStore, TenantCredentials
from agentos.crypto_keys import build_key_store
from agentos.tenant_store import PostgresTenantStore, TenantDirectory, startup_tenants
from agentos.tenants import require_tenant_secret

ROLES = ("title", "status", "requested_by", "request_id", "assignee")


async def _version_to_check(
    agents: AgentStore, tenant_id: str, agent_id: str, version: int | None
) -> AgentVersion:
    """The named version; else the newest draft or pending one; else the published one."""
    records = await agents.versions(tenant_id, agent_id)
    if version is not None:
        record = next((r for r in records if r.version == version), None)
    else:
        working = [r for r in records if r.status in ("draft", PENDING)]
        record = max(working, key=lambda r: r.version) if working else None
        if record is None:
            published = await agents.published(tenant_id, agent_id)
            return published if published is not None else _missing(agent_id, None)
    if record is None:
        _missing(agent_id, version)
    return record.agent()


def _missing(agent_id: str, version: int | None) -> AgentVersion:
    what = "has no versions" if version is None else f"has no version {version}"
    raise ConfigError(f"agent '{agent_id}' {what}")


async def check_published(
    tenants: TenantsConfig,
    agents: AgentStore,
    notion_for: Callable[[AgentsConfig], object],
    tenant_id: str | None = None,
    agent_id: str | None = None,
    version: int | None = None,
) -> int:
    """Check agents from ``agents``; print the result; return 0 or 1.

    ``notion_for`` builds a tenant's Notion client; it may be async (the credential
    resolver reads the database). Without ``agent_id``, every published agent of each
    tenant (or of ``tenant_id``). With it, one version of that agent (see the module
    docstring); ``tenant_id`` is then required unless there is a single tenant.
    """
    failed = False
    configs = list(tenants.tenants.values())
    if tenant_id is not None:
        configs = [c for c in configs if c.tenant_id == tenant_id]
        if not configs:
            print(f"PROBLEM  tenant '{tenant_id}' is not an enabled tenant")
            return 1
    if agent_id is not None and len(configs) != 1:
        print("PROBLEM  name the tenant with --tenant")
        return 1
    for config in configs:
        print(f"tenant '{config.tenant_id}':")
        try:
            api = notion_for(config)
            if inspect.isawaitable(api):
                api = await api
            if agent_id is None:
                chosen = list((await agents.published_all(config.tenant_id)).values())
            else:
                chosen = [await _version_to_check(agents, config.tenant_id, agent_id, version)]
        except ConfigError as err:
            print(f"  PROBLEM  {err}")
            print()
            failed = True
            continue
        for agent in chosen:
            if agent_id is not None:
                print(f"agent '{agent.agent_id}' version {agent.version}:")
            for grant in agent.grants.notion_databases:
                print(f"agent '{agent.agent_id}', grant '{grant.name}':")
                check = await check_grant(api, grant, require_ids=False)
                for problem in check.problems:
                    print(f"  PROBLEM  {problem}")
                for note in check.notes:
                    print(f"  note     {note}")
                failed = failed or bool(check.problems)
                if check.resolved:
                    print("  property_ids:")
                    for role in ROLES:
                        if role in check.resolved:
                            print(f'    {role}: "{check.resolved[role]}"')
                print()
            for page_grant in agent.grants.notion_pages:
                print(f"agent '{agent.agent_id}', page grant '{page_grant.name}':")
                problems = await check_page_grant(api, page_grant)
                for problem in problems:
                    print(f"  PROBLEM  {problem}")
                failed = failed or bool(problems)
                print()
    print("Problems found; fix them in Notion or the definition." if failed else "All grants OK.")
    return 1 if failed else 0


async def run(args: argparse.Namespace) -> int:
    settings = load_settings()
    pool = AsyncConnectionPool(settings.require("database_url"), open=False)
    await pool.open()
    try:
        tenants = await startup_tenants(TenantDirectory(PostgresTenantStore(pool)))
        keys = await build_key_store(settings, pool)
        credentials = TenantCredentials(PostgresCredentialStore(pool), keys)

        async def notion_for(config: AgentsConfig) -> RealNotionAPI:
            # The same resolution the worker and the console use: the credential row
            # first, else the variable; a tenant with neither names the variable.
            token = await credentials.notion_token(config)
            if token is None:
                token = require_tenant_secret(config, "notion_token_env")
            return RealNotionAPI(token)

        return await check_published(
            tenants,
            PostgresAgentStore(pool),
            notion_for,
            tenant_id=args.tenant,
            agent_id=args.agent,
            version=args.version,
        )
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Check Notion grants and print property IDs.")
    parser.add_argument("--tenant", help="check only this tenant")
    parser.add_argument("--agent", help="check one agent's draft, pending or chosen version")
    parser.add_argument(
        "--version", type=int, help="the version of --agent (default: newest draft)"
    )
    args = parser.parse_args()
    if args.version is not None and args.agent is None:
        parser.error("--version needs --agent")
    sys.exit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
