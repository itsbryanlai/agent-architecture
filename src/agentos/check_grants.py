"""Check each Notion grant against Notion and print each database's property IDs.

Run ``uv run python -m agentos.check_grants`` after writing the property names
in the config. It checks every configured tenant. It reads only the granted
databases' schemas, with that tenant's own Notion token (the variable named in
its ``secrets`` block, ``NOTION_TOKEN`` by default); it never changes a database. Page grants are checked
by reading the page: it must be readable and not trashed or archived. For each grant it
prints any problems and notes, then the ``property_ids`` block to paste into
the config. Exits 1 if any grant has a problem, otherwise 0.
"""

from __future__ import annotations

import asyncio
import sys

from agentos.config import ConfigError, load_settings, load_tenants_config
from agentos.connectors.notion import RealNotionAPI, check_grant, check_page_grant
from agentos.tenants import require_tenant_secret

ROLES = ("title", "status", "requested_by", "request_id")


async def run() -> int:
    settings = load_settings()
    tenants = load_tenants_config(settings.config_path)
    failed = False
    for config in tenants.tenants.values():
        print(f"tenant '{config.tenant_id}':")
        try:
            api = RealNotionAPI(require_tenant_secret(config, "notion_token_env"))
        except ConfigError as err:
            print(f"  PROBLEM  {err}")
            print()
            failed = True
            continue
        for agent in config.agents.values():
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
    print("Problems found; fix them in Notion or the config." if failed else "All grants OK.")
    return 1 if failed else 0


def main() -> None:
    sys.exit(asyncio.run(run()))


if __name__ == "__main__":
    main()
