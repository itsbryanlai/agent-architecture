"""Check each Notion grant against the live database and print its property IDs.

Run ``uv run python -m agentos.check_grants`` after writing the property names
in the config. It reads only the granted databases' schemas, with the agent's
own token (``NOTION_TOKEN``); it never changes a database. For each grant it
prints any problems and notes, then the ``property_ids`` block to paste into
the config. Exits 1 if any grant has a problem, otherwise 0.
"""

from __future__ import annotations

import asyncio
import sys

from agentos.config import load_agents_config, load_settings
from agentos.connectors.notion import RealNotionAPI, check_grant

ROLES = ("title", "status", "requested_by", "request_id")


async def run() -> int:
    settings = load_settings()
    agents = load_agents_config(settings.config_path)
    api = RealNotionAPI(settings.require("notion_token"))
    failed = False
    for agent in agents.agents.values():
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
    print("Problems found; fix them in Notion or the config." if failed else "All grants OK.")
    return 1 if failed else 0


def main() -> None:
    sys.exit(asyncio.run(run()))


if __name__ == "__main__":
    main()
