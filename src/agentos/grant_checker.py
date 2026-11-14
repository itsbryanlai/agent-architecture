"""Check a version's Notion database and page grants before it is published.

A grant names a Notion database and maps the agent's roles (title, status,
requested by, request ID) to that database's properties. A mapping that does
not match the live database makes the worker refuse to start, so the control
plane checks every database grant of a version when an admin publishes it, and
on request for an editor, and refuses a publish when any grant has a problem.

The check only reads: the schemas of the granted databases and the granted pages.
A page grant must be readable and not trashed. A transient Notion failure (network
error, rate limit, server error) is not a grant problem: it raises
``GrantCheckUnavailable``, so a publish is refused for now but not judged. Any other
failure (a tenant without its Notion key, a bug) is not a statement about Notion and
propagates as itself. It uses the tenant's own Notion client, because agents run with
tenant-level credentials.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

import httpx
from notion_client.errors import RequestTimeoutError

from agentos.config import AgentVersion
from agentos.connectors.notion import NotionAPI, check_grant, check_page_grant

# What the Notion client raises when the request never got an answer: its own
# timeout, any httpx transport failure, and the OS-level connection errors and
# timeouts (not every OSError: a file or permission error is a bug, not an outage).
TRANSPORT_ERRORS = (httpx.TransportError, RequestTimeoutError, ConnectionError, TimeoutError)


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

    Only these say anything about Notion. A 403 or 404 (the page is gone or not
    shared) and other client errors are permanent: they are problems with the
    grant. Anything else (a missing key, a bug) is no statement about Notion and
    is not transient.
    """
    if isinstance(err, TRANSPORT_ERRORS):
        return True
    status = getattr(err, "status", None)
    return isinstance(status, int) and (status == 429 or status >= 500)


def _is_grant_problem(err: Exception) -> bool:
    """A refusal from Notion itself (403, 404, other 4xx): the grant is at fault."""
    return isinstance(getattr(err, "status", None), int) and not is_transient(err)


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
        # Outside the try: a missing key is a setup error, not a Notion outage.
        api = self._api_for(tenant_id)
        try:
            return await self._reports(api, grants, pages)
        except Exception as err:
            if is_transient(err):
                raise GrantCheckUnavailable(
                    "could not read the granted databases and pages from Notion "
                    f"({type(err).__name__})"
                ) from err
            raise

    @staticmethod
    async def _reports(api: NotionAPI, grants, pages) -> list[GrantReport]:
        reports = []
        for grant in grants:
            try:
                result = await check_grant(api, grant, require_ids=True)
            except Exception as err:
                if not _is_grant_problem(err):
                    raise
                what = f"database '{grant.name}' cannot be read with this token"
                reports.append(
                    GrantReport(grant.name, grant.id, problems=[f"{what} ({type(err).__name__})"])
                )
                continue
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
            for err in seen.errors:
                if not _is_grant_problem(err):
                    raise err
            reports.append(GrantReport(page_grant.name, page_grant.id, "page", problems))
        return reports
