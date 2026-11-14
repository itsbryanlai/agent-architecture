"""Check a version's Notion database and page grants before it is published.

A grant names a Notion database and maps the agent's roles (title, status,
requested by, request ID) to that database's properties. A mapping that does
not match the live database makes the worker refuse to start, so the control
plane checks every database grant of a version when an admin publishes it, and
on request for an editor, and refuses a publish when any grant has a problem.

The check only reads: the schemas of the granted databases and the granted pages.
A page grant must be readable and not trashed. A transient Notion failure (network
error, rate limit, server error) is not a grant problem: it raises
``GrantCheckUnavailable``, so a publish is refused for now but not judged. It uses the tenant's
own Notion client, because agents run with tenant-level credentials.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from agentos.config import AgentVersion
from agentos.connectors.notion import NotionAPI, check_grant, check_page_grant, is_unreachable


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
    """The result for one grant: what is wrong and what is worth knowing.

    ``kind`` is ``"database"`` or ``"page"``; ``database_id`` holds the Notion ID of
    either.
    """

    grant: str
    database_id: str
    kind: str = "database"
    problems: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


class GrantChecker(Protocol):
    async def check(self, tenant_id: str, agent: AgentVersion) -> list[GrantReport]:
        """One report per Notion database grant of ``agent``, in grant order."""
        ...


def is_transient(err: Exception) -> bool:
    """A failure that may pass: a network error, a rate limit or a server error.

    A 403 or 404 (the page is gone or not shared) and other client errors are
    permanent: they are problems with the grant, not with Notion.
    """
    if is_unreachable(err):
        return False
    status = getattr(err, "status", None)
    return not isinstance(status, int) or status == 429 or status >= 500


class _ReadErrors:
    """Wraps a Notion client and remembers the errors ``retrieve_page`` raised.

    ``check_page_grant`` turns every failure into a problem string. The checker
    needs to tell a transient failure from a real problem, so it keeps the error.
    """

    def __init__(self, api: NotionAPI):
        self._api = api
        self.errors: list[Exception] = []

    async def retrieve_page(self, page_id: str) -> dict:
        try:
            return await self._api.retrieve_page(page_id)
        except Exception as err:
            self.errors.append(err)
            raise


class NotionGrantChecker:
    def __init__(self, api_for: Callable[[str], NotionAPI]):
        self._api_for = api_for

    async def check(self, tenant_id: str, agent: AgentVersion) -> list[GrantReport]:
        grants = agent.grants.notion_databases
        pages = agent.grants.notion_pages
        if not grants and not pages:
            return []
        try:
            api = self._api_for(tenant_id)
            reports = []
            for grant in grants:
                result = await check_grant(api, grant, require_ids=True)
                reports.append(
                    GrantReport(
                        grant.name,
                        grant.id,
                        problems=list(result.problems),
                        notes=list(result.notes),
                    )
                )
            for page_grant in pages:
                seen = _ReadErrors(api)
                problems = await check_page_grant(seen, page_grant)
                if any(is_transient(err) for err in seen.errors):
                    raise seen.errors[0]
                reports.append(GrantReport(page_grant.name, page_grant.id, "page", problems))
        except Exception as err:
            raise GrantCheckUnavailable(
                f"could not read the granted databases and pages from Notion ({type(err).__name__})"
            ) from err
        return reports
