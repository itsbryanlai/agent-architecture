"""Check a version's Notion database grants before it is published.

A grant names a Notion database and maps the agent's roles (title, status,
requested by, request ID) to that database's properties. A mapping that does
not match the live database makes the worker refuse to start, so the control
plane checks every database grant of a version when an admin publishes it, and
on request for an editor, and refuses a publish when any grant has a problem.

The check only reads the schemas of the granted databases. It uses the tenant's
own Notion client, because agents run with tenant-level credentials.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from agentos.config import AgentVersion
from agentos.connectors.notion import NotionAPI, check_grant


class GrantCheckUnavailable(Exception):
    """Notion could not be reached or read, so the grants were not checked."""


class GrantProblems(Exception):
    """A publish was refused because some grant has a problem."""

    def __init__(self, reports: list[GrantReport]):
        self.reports = reports
        problems = sum(len(r.problems) for r in reports)
        super().__init__(f"{problems} grant problem(s); fix them in Notion or the definition")


@dataclass
class GrantReport:
    """The result for one database grant: what is wrong and what is worth knowing."""

    grant: str
    database_id: str
    problems: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


class GrantChecker(Protocol):
    async def check(self, tenant_id: str, agent: AgentVersion) -> list[GrantReport]:
        """One report per Notion database grant of ``agent``, in grant order."""
        ...


class NotionGrantChecker:
    def __init__(self, api_for: Callable[[str], NotionAPI]):
        self._api_for = api_for

    async def check(self, tenant_id: str, agent: AgentVersion) -> list[GrantReport]:
        grants = agent.grants.notion_databases
        if not grants:
            return []
        try:
            api = self._api_for(tenant_id)
            reports = []
            for grant in grants:
                result = await check_grant(api, grant, require_ids=True)
                reports.append(
                    GrantReport(grant.name, grant.id, list(result.problems), list(result.notes))
                )
        except Exception as err:
            raise GrantCheckUnavailable(
                f"could not read the granted databases from Notion ({type(err).__name__})"
            ) from err
        return reports
