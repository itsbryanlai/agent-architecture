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
from dataclasses import dataclass, field, replace
from typing import Protocol

import httpx
from notion_client.errors import RequestTimeoutError

from agentos.config import AgentVersion
from agentos.connectors.notion import (
    LiveObject,
    NotionAPI,
    check_grant,
    check_page_grant,
    describe_object,
)

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
    # A short category when something is wrong (see ``REASONS``); None when the grant is fine.
    reason: str | None = None
    # What Notion says the object is called and where it sits, now. The creator's label is
    # ``grant``; ``label_differs`` is set when the two do not contain one another once normalised.
    live_title: str | None = None
    parent: str | None = None
    label_differs: bool = False
    # False on a report cut down for a creator (see ``without_detail``).
    detail: bool = True

    @property
    def ok(self) -> bool:
        return not self.problems and self.reason is None

    def without_detail(self) -> GrantReport:
        """The report a creator sees: ok or a problem category, no schema, text or titles."""
        return GrantReport(
            self.grant, self.database_id, self.kind, reason=self.reason, detail=False
        )

    def as_dict(self) -> dict:
        """The JSON form: everything for an admin, ok and a category for anyone else."""
        base = {"grant": self.grant, "database_id": self.database_id, "kind": self.kind}
        base["ok"] = self.ok
        base["reason"] = self.reason
        if not self.detail:
            return base
        return base | {
            "problems": self.problems,
            "notes": self.notes,
            "live_title": self.live_title,
            "parent": self.parent,
            "label_differs": self.label_differs,
        }


# Why a grant has a problem, as far as a non-admin may be told.
NOT_SHARED = "not_shared_with_integration"
NOT_FOUND = "not_found"
WRONG_TYPE = "wrong_type"
MAPPING = "mapping_mismatch"
UNREADABLE = "unreadable"
REASONS = (NOT_SHARED, NOT_FOUND, WRONG_TYPE, MAPPING, UNREADABLE)


def _reason_of(err: Exception) -> str:
    status = getattr(err, "status", None)
    if status in (403, 404):
        return NOT_SHARED
    if status == 400:
        return WRONG_TYPE
    return UNREADABLE


def _normalised_name(name: str) -> str:
    kept = "".join(ch if ch.isalnum() else " " for ch in name.casefold())
    return " ".join(kept.split())


def _same_name(a: str, b: str) -> bool:
    left, right = _normalised_name(a), _normalised_name(b)
    return left in right or right in left


def _with_live(report: GrantReport, live: LiveObject) -> GrantReport:
    differs = live.title is not None and not _same_name(report.grant, live.title)
    return replace(report, live_title=live.title, parent=live.parent, label_differs=differs)


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
                    GrantReport(
                        grant.name,
                        grant.id,
                        problems=[f"{what} ({type(err).__name__})"],
                        reason=_reason_of(err),
                    )
                )
                continue
            report = GrantReport(
                grant.name,
                grant.id,
                problems=list(result.problems),
                notes=list(result.notes),
                reason=MAPPING if result.problems else None,
            )
            reports.append(_with_live(report, await describe_object(api, "database", grant.id)))
        for page_grant in pages:
            seen = _ReadErrors(api)
            problems = await check_page_grant(seen, page_grant)
            for err in seen.errors:
                if not _is_grant_problem(err):
                    raise err
            reason = None
            if problems:
                reason = _reason_of(seen.errors[0]) if seen.errors else NOT_FOUND
            report = GrantReport(page_grant.name, page_grant.id, "page", problems, reason=reason)
            if not seen.errors:
                report = _with_live(report, await describe_object(api, "page", page_grant.id))
            reports.append(report)
        return reports
