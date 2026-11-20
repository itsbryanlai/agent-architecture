"""The control-plane API: agents, versions, members, usage, budgets, switches and the audit log, as JSON.

This is a separate app and process from the gateway. The gateway is reachable
from the internet; this API is not, and must not be. Without sign-in with Slack
configured it listens on the loopback address only and the entry point refuses any
other bind. Never point a tunnel at its port in that mode. Start it with::

    AGENTOS_CONTROL_ACTOR=<your Slack user ID> uv run python -m agentos.control_api

With ``SLACK_CLIENT_ID``, ``SLACK_CLIENT_SECRET`` and ``AGENTOS_CONSOLE_URL`` set
the same process also serves the web console (``web_ui.py``), requires a Slack
sign-in for everything, and may listen beyond loopback.

Who is acting. The acting Slack user ID comes from an ``IdentityProvider``, which
the process owns; nothing in a request (header, query or body) can set or change
it. There are two: ``LocalOperator``, a single user ID fixed when the process
starts, and ``SlackSession``, the user of the request's sign-in session. A process
uses exactly one of them.

What the actor may do is decided only by ``ControlPlane``, from the actor's role
in the tenant named in the path. This module adds no rule of its own, calls no
store, and only turns the control plane's answers and refusals into HTTP.

This module does not use ``from __future__ import annotations``: FastAPI reads the
route parameters' type aliases at definition time.
"""

import argparse
import asyncio
import hashlib
import logging
import re
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, Any, Protocol
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg import ProgrammingError
from psycopg.conninfo import conninfo_to_dict
from pydantic import BaseModel, ConfigDict

from agentos.agent_store import AgentNotFound
from agentos.config import ConfigError, Settings, TenantsConfig, load_settings
from agentos.control import ControlPlane
from agentos.control_errors import STATUS_BY_ERROR
from agentos.grant_checker import GrantProblems
from agentos.notion_picker import (
    NOTION_CALLBACK_PATH,
    HttpNotionOAuth,
    Picker,
    picker_from_settings,
)
from agentos.sessions import SessionStore
from agentos.slack_install import (
    INSTALL_CALLBACK_PATH,
    INSTALL_PATH,
    HttpSlackOAuth,
    Installer,
    install_from_settings,
)
from agentos.slack_signin import (
    CALLBACK_PATH,
    Console,
    HttpSlackOidc,
    SlackSession,
    sign_in_from_settings,
)
from agentos.tenant_store import TenantSource, as_source
from agentos.web_ui import CONSOLE_FORM_PREFIXES, install_web_ui

log = logging.getLogger("agentos.control_api")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8100
LOOPBACK_HOSTS = ("127.0.0.1", "::1", "localhost")
ACTOR_VARIABLE = "AGENTOS_CONTROL_ACTOR"
MAX_AUDIT_LIMIT = 200
CONTENT_SECURITY_POLICY = (
    "default-src 'self'; style-src 'self' 'unsafe-inline'; form-action 'self'; "
    "frame-ancestors 'none'"
)
_SLACK_USER_ID = re.compile(r"[UW][A-Z0-9]{2,}")


# --- who is acting ------------------------------------------------------------------


class IdentityProvider(Protocol):
    async def actor(self, request: Request) -> str:
        """The acting Slack user ID for this request, decided by the runtime."""
        ...


class LocalOperator:
    """The person running this process: one Slack user ID, fixed at start."""

    def __init__(self, user_id: str):
        if not _SLACK_USER_ID.fullmatch(user_id):
            raise ValueError(f"{ACTOR_VARIABLE} must be a Slack user ID such as U0123ABCDEF")
        self._user_id = user_id

    async def actor(self, request: Request) -> str:
        return self._user_id


def operator_from_environment(environ: Mapping[str, str]) -> LocalOperator:
    """The operator named by ``AGENTOS_CONTROL_ACTOR``; refuse to start without one."""
    user_id = environ.get(ACTOR_VARIABLE, "").strip()
    if not user_id:
        raise SystemExit(
            f"{ACTOR_VARIABLE} is not set. Set it to your Slack user ID: "
            "the control API acts as that user and as nobody else."
        )
    try:
        return LocalOperator(user_id)
    except ValueError as err:
        raise SystemExit(str(err)) from err


def check_bind_host(host: str, sign_in_configured: bool = False) -> None:
    """Refuse any host but loopback, unless sign-in with Slack is configured.

    Without sign-in every request acts as the fixed operator, so the API must not
    be reachable from outside. With it, every request needs a session.
    """
    if sign_in_configured:
        return
    if host not in LOOPBACK_HOSTS:
        raise SystemExit(
            f"refusing to listen on '{host}': the control API binds only to "
            f"{', '.join(LOOPBACK_HOSTS)} until sign-in with Slack is configured "
            "(SLACK_CLIENT_ID, SLACK_CLIENT_SECRET and AGENTOS_CONSOLE_URL)"
        )


# Passwords that ship in the repository for local development.
DEVELOPMENT_DATABASE_PASSWORDS = frozenset({"agentos_app", "agentos"})


def check_database_for_bind(host: str, database_url: str) -> None:
    """Refuse a non-loopback bind while the database password is empty or a development default.

    A console reachable beyond this machine must not sit in front of a database
    that anyone who has read the repository can open.
    """
    if host in LOOPBACK_HOSTS:
        return
    try:
        password = conninfo_to_dict(database_url).get("password") or ""
    except ProgrammingError:
        password = ""
    if not password or password in DEVELOPMENT_DATABASE_PASSWORDS:
        raise SystemExit(
            f"refusing to listen on '{host}': the database password in AGENTOS_DATABASE_URL "
            "is empty or a development default. Set a password of your own in "
            "AGENTOS_DATABASE_URL before listening beyond loopback."
        )


# --- bodies -------------------------------------------------------------------------


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NewAgent(_Body):
    agent_id: str
    definition: dict[str, Any]


class NewDraft(_Body):
    from_version: int | None = None


class Definition(_Body):
    definition: dict[str, Any]


class AudienceCounts(_Body):
    total: int
    guests: int
    external: int


class Publish(BaseModel):
    """The publish body. ``definition_sha256`` is the hash of the version the approver was
    shown (the ``definition_sha256`` of the version record they read); a publish without
    it is refused, because it would publish whatever is pending when the request lands.
    Other fields it does not know are ignored, and who acts never comes from a body. A
    misspelt ``confirm_audience`` is therefore not a confirmation, and the publish is
    refused."""

    model_config = ConfigDict(extra="ignore")
    definition_sha256: str
    confirm_audience: AudienceCounts | None = None
    # The approver has seen that the version widens access (see ``access_diff``).
    acknowledge_widening: bool = False
    # The published version number the approver's diff was computed against, or "none"
    # (``published`` in the review). A publish without it is refused.
    reviewed_published: str


class BudgetAmount(_Body):
    amount: Decimal


class NewMember(_Body):
    user_id: str
    role: str


class MemberRole(_Body):
    role: str


# --- the app ------------------------------------------------------------------------


def _error(status: int, message: str, **extra: Any) -> JSONResponse:
    return JSONResponse({"error": message, **extra}, status_code=status)


@dataclass
class ControlDeps:
    plane: ControlPlane | None
    tenants: TenantSource | None
    sessions: SessionStore | None = None
    console: Console | None = None
    picker: Picker | None = None
    install: Installer | None = None


def create_app(
    plane: ControlPlane | None,
    tenants: TenantsConfig | TenantSource | None,
    identity: IdentityProvider,
    lifespan: Any = None,
    port: int = DEFAULT_PORT,
    sessions: SessionStore | None = None,
    console: Console | None = None,
    picker: Picker | None = None,
    install: Installer | None = None,
) -> FastAPI:
    """Build the API. Tests pass fakes; the real process fills ``deps`` on startup.

    With a ``console`` the app also serves the web pages, and ``identity`` should be
    ``SlackSession()``. The Host and Origin allowlists then include the console URL.
    """
    deps = ControlDeps(plane, as_source(tenants), sessions, console, picker, install)
    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.deps = deps
    allowed_hosts = {f"{host}:{port}" for host in ("127.0.0.1", "localhost", "[::1]")}
    allowed_origins = {f"http://{host}" for host in allowed_hosts}
    if console is not None:
        allowed_hosts.add(console.sign_in.host)
        allowed_origins.add(console.sign_in.origin)

    def form_origin_ok(headers: Mapping[str, str]) -> bool:
        """A form post comes from one of our pages: its Origin, or else its Referer."""
        origin = headers.get("origin")
        if origin is not None:
            return origin in allowed_origins
        referer = urlsplit(headers.get("referer", ""))
        return f"{referer.scheme}://{referer.netloc}" in allowed_origins

    def is_console_page_navigation(request: Request) -> bool:
        """A top-level browser navigation (not a frame, fetch or embed) to a console page."""
        headers = request.headers
        path = request.url.path
        return (
            console is not None
            and request.method in ("GET", "HEAD")
            and headers.get("sec-fetch-mode") == "navigate"
            and headers.get("sec-fetch-dest") == "document"
            and (path in ("/", "/login", INSTALL_PATH) or path.startswith("/ui/"))
        )

    @app.middleware("http")
    async def local_browser_guard(request: Request, call_next):
        """Keep a web page in the operator's browser from driving this API.

        The API acts as a fixed operator for anything that reaches the port, so it
        refuses what a browser could send on a page's behalf: a foreign Host (DNS
        rebinding), a foreign Origin or a cross-site fetch (CSRF), and a write that is
        not JSON (a form or no-cors post is a simple request; JSON forces a preflight,
        which fails because no CORS is offered). Refusals say nothing about the request.
        A console form post is the one other write: it must come from our own origin
        (the route then checks its CSRF token against the session).

        A cross-site request is one exception besides the Slack callback: a top-level
        page navigation to a console page (the redirect after Slack sign-in, a link from
        Slack or an email). The browser labels a redirect chain that passed through
        another site as cross-site, so refusing it makes the console unreachable. It is
        safe because every console GET only reads, the page cannot be framed or read by
        the other site, and the Lax session cookie is already sent on such navigations.
        Writes, the JSON API and every cross-site request without a console stay refused.
        """
        if request.url.path == "/healthz":
            return await call_next(request)
        headers = request.headers
        if headers.get("host") not in allowed_hosts:
            return _error(421, "host not allowed")
        origin = headers.get("origin")
        if origin is not None and origin not in allowed_origins:
            return _error(403, "origin not allowed")
        # Slack sends the browser back to the callback from another site; the callback
        # is protected by its state cookie instead.
        arrives_from_slack = console is not None and request.url.path in (
            CALLBACK_PATH,
            NOTION_CALLBACK_PATH,
            INSTALL_CALLBACK_PATH,
        )
        if (
            headers.get("sec-fetch-site") in ("cross-site", "same-site")
            and not arrives_from_slack
            and not is_console_page_navigation(request)
        ):
            return _error(403, "cross-site requests are not allowed")
        if request.method in ("POST", "PUT", "PATCH", "DELETE"):
            media_type = headers.get("content-type", "").split(";")[0].strip().lower()
            is_form = (
                console is not None
                and media_type == "application/x-www-form-urlencoded"
                and request.url.path.startswith(CONSOLE_FORM_PREFIXES)
            )
            if is_form:
                if not form_origin_ok(headers):
                    return _error(403, "origin not allowed")
            elif media_type != "application/json":
                return _error(415, "writes must be sent as application/json")
        return await call_next(request)

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        """On every response, refusals included (this middleware is the outermost)."""
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = CONTENT_SECURITY_POLICY
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Cache-Control"] = "no-store"
        return response

    def refusal_handler(status: int):
        async def handle(request: Request, err: Exception) -> JSONResponse:
            if isinstance(err, GrantProblems):
                return _error(status, str(err), grants=jsonable_encoder(err.reports))
            return _error(status, str(err))

        return handle

    for error_type, status in STATUS_BY_ERROR.items():
        app.add_exception_handler(error_type, refusal_handler(status))

    @app.exception_handler(RequestValidationError)
    async def bad_request(request: Request, err: RequestValidationError) -> JSONResponse:
        # Messages and locations only: the default response would echo the input.
        problems = [f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in err.errors()]
        return _error(422, "invalid request", problems=problems)

    @app.exception_handler(Exception)
    async def unexpected(request: Request, err: Exception) -> JSONResponse:
        log.error("control API error: %s", type(err).__name__)
        return _error(500, "internal error")

    async def acting(request: Request) -> str:
        """Who is asking. Runs before anything about the path's tenant is looked up, so a
        caller with no session cannot tell a real tenant from an unknown one."""
        return await identity.actor(request)

    Actor = Annotated[str, Depends(acting)]

    async def known_tenant(tenant_id: str, actor: Actor) -> str:
        if deps.tenants is None or await deps.tenants.get(tenant_id) is None:
            raise HTTPException(404, f"unknown tenant '{tenant_id}'")
        return tenant_id

    Tenant = Annotated[str, Depends(known_tenant)]

    async def request_id(
        request: Request,
        actor: Actor,
        idempotency_key: Annotated[str | None, Header(max_length=200)] = None,
    ) -> str | None:
        """An audit nonce for a retried write.

        The same key on the same actor, path and body is one request; a key reused for
        anything else is a different request and is audited as one.
        """
        if not idempotency_key:
            return None
        body = hashlib.sha256(await request.body()).hexdigest()
        scope = f"{actor}\n{request.method} {request.url.path}\n{body}\n{idempotency_key}"
        return hashlib.sha256(scope.encode()).hexdigest()

    RequestId = Annotated[str | None, Depends(request_id)]

    def out(value: Any, status: int = 200) -> JSONResponse:
        return JSONResponse(jsonable_encoder(value), status_code=status)

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True}

    @app.get("/tenants/{tenant_id}/me")
    async def me(tenant_id: Tenant, actor: Actor) -> JSONResponse:
        return out({"user_id": actor, "role": await deps.plane.role_of(tenant_id, actor)})

    # --- agents ---

    @app.get("/tenants/{tenant_id}/agents")
    async def list_agents(tenant_id: Tenant, actor: Actor) -> JSONResponse:
        return out(await deps.plane.list_agents(tenant_id, actor))

    @app.post("/tenants/{tenant_id}/agents")
    async def add_agent(
        tenant_id: Tenant, actor: Actor, rid: RequestId, body: NewAgent
    ) -> JSONResponse:
        record = await deps.plane.add_agent(tenant_id, actor, body.agent_id, body.definition, rid)
        return out(record, 201)

    @app.get("/tenants/{tenant_id}/agents/{agent_id}/versions")
    async def versions(tenant_id: Tenant, actor: Actor, agent_id: str) -> JSONResponse:
        found = await deps.plane.versions(tenant_id, actor, agent_id)
        if not found:
            raise AgentNotFound(f"agent '{agent_id}' does not exist")
        return out(found)

    @app.get("/tenants/{tenant_id}/agents/{agent_id}/versions/{version}")
    async def get_version(
        tenant_id: Tenant, actor: Actor, agent_id: str, version: int
    ) -> JSONResponse:
        record = await deps.plane.get_version(tenant_id, actor, agent_id, version)
        if record is None:
            raise AgentNotFound(f"agent '{agent_id}' has no version {version}")
        return out(record)

    @app.post("/tenants/{tenant_id}/agents/{agent_id}/drafts")
    async def new_draft(
        tenant_id: Tenant, actor: Actor, rid: RequestId, agent_id: str, body: NewDraft | None = None
    ) -> JSONResponse:
        from_version = None if body is None else body.from_version
        return out(await deps.plane.new_draft(tenant_id, actor, agent_id, from_version, rid), 201)

    @app.put("/tenants/{tenant_id}/agents/{agent_id}/versions/{version}")
    async def edit_draft(
        tenant_id: Tenant,
        actor: Actor,
        rid: RequestId,
        agent_id: str,
        version: int,
        body: Definition,
    ) -> JSONResponse:
        return out(
            await deps.plane.edit_draft(tenant_id, actor, agent_id, version, body.definition, rid)
        )

    @app.post("/tenants/{tenant_id}/agents/{agent_id}/versions/{version}/submit")
    async def submit(
        tenant_id: Tenant, actor: Actor, rid: RequestId, agent_id: str, version: int
    ) -> JSONResponse:
        return out(await deps.plane.submit_version(tenant_id, actor, agent_id, version, rid))

    @app.post("/tenants/{tenant_id}/agents/{agent_id}/versions/{version}/withdraw")
    async def withdraw(
        tenant_id: Tenant, actor: Actor, rid: RequestId, agent_id: str, version: int
    ) -> JSONResponse:
        return out(await deps.plane.withdraw_version(tenant_id, actor, agent_id, version, rid))

    @app.post("/tenants/{tenant_id}/agents/{agent_id}/versions/{version}/reject")
    async def reject(
        tenant_id: Tenant, actor: Actor, rid: RequestId, agent_id: str, version: int
    ) -> JSONResponse:
        return out(await deps.plane.reject_version(tenant_id, actor, agent_id, version, rid))

    @app.post("/tenants/{tenant_id}/agents/{agent_id}/versions/{version}/publish")
    async def publish(
        tenant_id: Tenant,
        actor: Actor,
        rid: RequestId,
        agent_id: str,
        version: int,
        body: Publish,
    ) -> JSONResponse:
        confirm = body.confirm_audience
        return out(
            await deps.plane.publish_version(
                tenant_id,
                actor,
                agent_id,
                version,
                rid,
                None if confirm is None else confirm.model_dump(),
                reviewed_sha256=body.definition_sha256,
                acknowledge_widening=body.acknowledge_widening is True,
                reviewed_published=body.reviewed_published,
            )
        )

    @app.post("/tenants/{tenant_id}/agents/{agent_id}/versions/{version}/check-grants")
    async def check_grants(
        tenant_id: Tenant, actor: Actor, agent_id: str, version: int
    ) -> JSONResponse:
        reports = await deps.plane.check_version_grants(tenant_id, actor, agent_id, version)
        return out({"ok": all(r.ok for r in reports), "grants": [r.as_dict() for r in reports]})

    @app.get("/tenants/{tenant_id}/agents/{agent_id}/versions/{version}/review")
    async def review(tenant_id: Tenant, actor: Actor, agent_id: str, version: int) -> JSONResponse:
        """What a draft or pending version changes about access, for its editor or an admin.

        Only an admin gets the grants with their live titles and parents; the diff and the
        picks (who chose which object from their own Notion) go to either."""
        found = await deps.plane.version_review(tenant_id, actor, agent_id, version)
        return out(
            {
                "grants": None if found.reports is None else [r.as_dict() for r in found.reports],
                "live_problem": found.live_problem,
                "widens": None if found.diff is None else found.diff.widens,
                "published": None if found.diff is None else found.diff.published,
                "changes": None
                if found.diff is None
                else [
                    {"kind": c.kind, "what": c.what, "widens": c.widens} for c in found.diff.changes
                ],
                "picks": {
                    nid: {"picked_by": p.picked_by, "picked_at": p.picked_at}
                    for nid, p in found.picks.items()
                },
            }
        )

    # --- members ---

    @app.get("/tenants/{tenant_id}/members")
    async def list_members(tenant_id: Tenant, actor: Actor) -> JSONResponse:
        return out(await deps.plane.list_members(tenant_id, actor))

    @app.post("/tenants/{tenant_id}/members")
    async def add_member(
        tenant_id: Tenant, actor: Actor, rid: RequestId, body: NewMember
    ) -> JSONResponse:
        await deps.plane.invite_member(tenant_id, actor, body.user_id, body.role, rid)
        return out({"user_id": body.user_id, "role": body.role}, 201)

    @app.patch("/tenants/{tenant_id}/members/{user_id}")
    async def change_role(
        tenant_id: Tenant, actor: Actor, rid: RequestId, user_id: str, body: MemberRole
    ) -> JSONResponse:
        await deps.plane.change_role(tenant_id, actor, user_id, body.role, rid)
        return out({"user_id": user_id, "role": body.role})

    @app.delete("/tenants/{tenant_id}/members/{user_id}")
    async def drop_member(
        tenant_id: Tenant, actor: Actor, rid: RequestId, user_id: str
    ) -> JSONResponse:
        await deps.plane.drop_member(tenant_id, actor, user_id, rid)
        return out({"user_id": user_id, "removed": True})

    # --- usage ---

    @app.get("/tenants/{tenant_id}/usage")
    async def usage(
        tenant_id: Tenant, actor: Actor, month: Annotated[str | None, Query()] = None
    ) -> JSONResponse:
        """Usage and cost for ``month`` (YYYY-MM, default the current UTC month)."""
        return out((await deps.plane.usage_report(tenant_id, actor, month)).as_dict())

    # --- budgets ---

    @app.get("/tenants/{tenant_id}/budgets")
    async def budgets(tenant_id: Tenant, actor: Actor) -> JSONResponse:
        """Budgets against the current UTC month's spend: the tenant line (admins) and agents."""
        return out((await deps.plane.budgets(tenant_id, actor)).as_dict())

    @app.put("/tenants/{tenant_id}/budget")
    async def set_tenant_budget(
        tenant_id: Tenant, actor: Actor, rid: RequestId, body: BudgetAmount
    ) -> JSONResponse:
        row = await deps.plane.set_budget(tenant_id, actor, None, body.amount, rid)
        return out({"agent_id": None, "amount": format(row.amount, "f")})

    @app.delete("/tenants/{tenant_id}/budget")
    async def remove_tenant_budget(tenant_id: Tenant, actor: Actor, rid: RequestId) -> JSONResponse:
        await deps.plane.remove_budget(tenant_id, actor, None, rid)
        return out({"agent_id": None, "removed": True})

    @app.put("/tenants/{tenant_id}/agents/{agent_id}/budget")
    async def set_agent_budget(
        tenant_id: Tenant, actor: Actor, rid: RequestId, agent_id: str, body: BudgetAmount
    ) -> JSONResponse:
        row = await deps.plane.set_budget(tenant_id, actor, agent_id, body.amount, rid)
        return out({"agent_id": agent_id, "amount": format(row.amount, "f")})

    @app.delete("/tenants/{tenant_id}/agents/{agent_id}/budget")
    async def remove_agent_budget(
        tenant_id: Tenant, actor: Actor, rid: RequestId, agent_id: str
    ) -> JSONResponse:
        await deps.plane.remove_budget(tenant_id, actor, agent_id, rid)
        return out({"agent_id": agent_id, "removed": True})

    # --- kill switches ---

    @app.get("/tenants/{tenant_id}/switches")
    async def switches(tenant_id: Tenant, actor: Actor) -> JSONResponse:
        """Every switch and its state: all of them for an admin, a creator's agents only."""
        return out((await deps.plane.switches(tenant_id, actor)).as_dict())

    async def change_switch(
        tenant_id: str, actor: str, rid: str | None, scope: str, target: str | None, off: bool
    ) -> JSONResponse:
        change = deps.plane.turn_off if off else deps.plane.turn_on
        changed = await change(tenant_id, actor, scope, target, rid)
        return out({"scope": scope, "target": target, "off": off, "changed": changed})

    @app.put("/tenants/{tenant_id}/switches/all-agents")
    async def turn_off_all_agents(tenant_id: Tenant, actor: Actor, rid: RequestId):
        return await change_switch(tenant_id, actor, rid, "all_agents", None, True)

    @app.delete("/tenants/{tenant_id}/switches/all-agents")
    async def turn_on_all_agents(tenant_id: Tenant, actor: Actor, rid: RequestId):
        return await change_switch(tenant_id, actor, rid, "all_agents", None, False)

    @app.put("/tenants/{tenant_id}/switches/writes")
    async def turn_off_writes(tenant_id: Tenant, actor: Actor, rid: RequestId):
        return await change_switch(tenant_id, actor, rid, "writes", None, True)

    @app.delete("/tenants/{tenant_id}/switches/writes")
    async def turn_on_writes(tenant_id: Tenant, actor: Actor, rid: RequestId):
        return await change_switch(tenant_id, actor, rid, "writes", None, False)

    @app.put("/tenants/{tenant_id}/switches/agents/{agent_id}")
    async def turn_off_agent(tenant_id: Tenant, actor: Actor, rid: RequestId, agent_id: str):
        return await change_switch(tenant_id, actor, rid, "agent", agent_id, True)

    @app.delete("/tenants/{tenant_id}/switches/agents/{agent_id}")
    async def turn_on_agent(tenant_id: Tenant, actor: Actor, rid: RequestId, agent_id: str):
        return await change_switch(tenant_id, actor, rid, "agent", agent_id, False)

    @app.put("/tenants/{tenant_id}/switches/connectors/{connector}")
    async def turn_off_connector(tenant_id: Tenant, actor: Actor, rid: RequestId, connector: str):
        return await change_switch(tenant_id, actor, rid, "connector", connector, True)

    @app.delete("/tenants/{tenant_id}/switches/connectors/{connector}")
    async def turn_on_connector(tenant_id: Tenant, actor: Actor, rid: RequestId, connector: str):
        return await change_switch(tenant_id, actor, rid, "connector", connector, False)

    # --- audit ---

    @app.get("/tenants/{tenant_id}/audit")
    async def audit(
        tenant_id: Tenant,
        actor: Actor,
        limit: Annotated[int, Query(ge=1)] = 50,
        before: Annotated[int | None, Query(ge=0)] = None,
    ) -> JSONResponse:
        """Newest first. ``before`` is a ``seq``: the next page is the events under it."""
        limit = min(limit, MAX_AUDIT_LIMIT)
        events = await deps.plane.audit_log(tenant_id, actor, limit, before_seq=before)
        next_before = events[-1].seq if len(events) == limit and events else None
        return out({"events": events, "next_before": next_before})

    if console is not None:
        install_web_ui(app)

    return app


# --- the real process ---------------------------------------------------------------


@asynccontextmanager
async def _real_lifespan(app: FastAPI):
    """Build the real dependencies when the server starts, not at import."""
    from psycopg_pool import AsyncConnectionPool

    from agentos.agent_feedback import PostgresAgentFeedbackStore
    from agentos.agent_health import PostgresAgentHealthStore
    from agentos.agent_store import PostgresAgentStore
    from agentos.audience import SlackAudienceInspector
    from agentos.audience_pauses import PostgresPauseStore
    from agentos.audit import PostgresAuditStore
    from agentos.budgets import PostgresBudgetStore
    from agentos.config import KEY_SOURCE_DB
    from agentos.connectors.notion import RealNotionAPI
    from agentos.creator_access import PostgresCreatorAccessStore
    from agentos.credentials import PostgresCredentialStore, TenantCredentials
    from agentos.crypto_keys import (
        TenantKeyError,
        build_key_store,
        master_keys_from_env,
        start_key_refresh,
        tenant_key_material,
    )
    from agentos.grant_checker import NotionGrantChecker
    from agentos.grant_picks import PostgresGrantPickStore
    from agentos.members import PostgresMemberStore
    from agentos.notion_webhook import PostgresWebhookTokenStore, WebhookTokens
    from agentos.schema_version import require_current_schema
    from agentos.sessions import PostgresSessionStore
    from agentos.slack_client import RealSlackPoster
    from agentos.switches import PostgresSwitchStore
    from agentos.tenant_store import PostgresTenantStore, TenantDirectory, startup_tenants
    from agentos.tenants import require_tenant_secret
    from agentos.unit_of_work import PostgresUnitOfWork
    from agentos.usage import PostgresUsageStore

    logging.basicConfig(level=logging.INFO)
    settings = load_settings()
    pool = AsyncConnectionPool(settings.require("database_url"), open=False, **settings.pool_args())
    await pool.open()
    key_refresh: asyncio.Task | None = None
    try:
        await require_current_schema(pool)
        tenant_store = PostgresTenantStore(pool)
        directory = TenantDirectory(tenant_store)
        # A tenant with no Notion token is served Slack-only; its grant checks fail at
        # publish time with a message naming the variable, not at start-up.
        await startup_tenants(directory)
        try:
            keys = await build_key_store(settings, pool)
        except TenantKeyError as err:
            raise SystemExit(str(err)) from err
        # The install creates a tenant's key while the console runs; the periodic
        # refresh loads it (and every other new key) without a restart, as in the gateway.
        key_refresh = start_key_refresh(keys)
        credentials = TenantCredentials(PostgresCredentialStore(pool), keys)
        webhook_tokens = WebhookTokens(PostgresWebhookTokenStore(pool), keys)
        apis: dict[str, tuple[str, RealNotionAPI]] = {}

        async def notion_for(tenant_id: str) -> RealNotionAPI:
            # Built on first use and rebuilt when the credential changes (Connect Notion,
            # a reconnect), as the Slack client is; a tenant with no token fails its own
            # publish with a message naming the variable, not the whole console.
            config = directory.latest().get(tenant_id)
            token = await credentials.notion_token(config) or require_tenant_secret(
                config, "notion_token_env"
            )
            entry = apis.get(tenant_id)
            if entry is None or entry[0] != token:
                entry = (token, RealNotionAPI(token))
                apis[tenant_id] = entry
            return entry[1]

        slack_clients: dict[str, tuple[str, RealSlackPoster]] = {}

        async def slack_for(tenant_id: str) -> RealSlackPoster:
            # Built on first use and rebuilt when the credential changes, so a tenant
            # whose Slack token is unavailable fails its own publish with a message
            # naming the cause, not the whole console.
            config = directory.latest().get(tenant_id)
            token = await credentials.slack_bot_token(config)
            entry = slack_clients.get(tenant_id)
            if entry is None or entry[0] != token:
                entry = (token, RealSlackPoster(token, tenant_id=tenant_id))
                slack_clients[tenant_id] = entry
            return entry[1]

        def team_for(tenant_id: str) -> str:
            return directory.latest().get(tenant_id).slack_team_id

        app.state.deps.sessions = PostgresSessionStore(pool)
        app.state.deps.plane = ControlPlane(
            PostgresAgentStore(pool),
            PostgresMemberStore(pool),
            PostgresAuditStore(pool),
            directory,
            NotionGrantChecker(notion_for),
            SlackAudienceInspector(slack_for, team_for),
            usage=PostgresUsageStore(pool),
            budgets=PostgresBudgetStore(pool),
            pauses=PostgresPauseStore(pool),
            switches=PostgresSwitchStore(pool),
            unit_of_work=PostgresUnitOfWork(pool),
            grant_picks=PostgresGrantPickStore(pool),
            require_grant_picker=settings.require_grant_picker,
            credentials=credentials,
            tenant_store=tenant_store,
            webhook_tokens=webhook_tokens,
            slack_for=slack_for,
            health=PostgresAgentHealthStore(pool),
            creator_access=PostgresCreatorAccessStore(pool),
            feedback_reads=PostgresAgentFeedbackStore(pool),
        )
        app.state.deps.tenants = directory
        if app.state.deps.console is not None:
            try:
                install_config = install_from_settings(settings)
            except ConfigError as err:
                raise SystemExit(str(err)) from err
            if install_config is not None:
                if settings.key_source != KEY_SOURCE_DB:
                    log.info("Slack install not offered: AGENTOS_KEY_SOURCE is not 'db'")
                else:
                    masters = master_keys_from_env()
                    app.state.deps.install = Installer(
                        install_config,
                        HttpSlackOAuth(install_config.client_id, install_config.client_secret),
                        PostgresTenantStore(pool),
                        credentials,
                        PostgresAuditStore(pool),
                        PostgresUnitOfWork(pool),
                        ensure_key=lambda tenant_id, conn: tenant_key_material(
                            pool, masters, tenant_id, conn=conn
                        ),
                    )
        yield
    finally:
        if key_refresh is not None:
            key_refresh.cancel()
            await asyncio.gather(key_refresh, return_exceptions=True)
        await pool.close()


def build_process_app(
    host: str, port: int, settings: Settings, environ: Mapping[str, str]
) -> FastAPI:
    """The app for this process, or ``SystemExit`` when it may not start as configured.

    With sign-in configured it is the web console behind ``SlackSession`` and may bind
    any host; the fixed operator is not read. Without it, the loopback-only API acts
    as the operator named in the environment.
    """
    try:
        sign_in = sign_in_from_settings(settings)
    except ConfigError as err:
        raise SystemExit(str(err)) from err
    check_bind_host(host, sign_in_configured=sign_in is not None)
    check_database_for_bind(host, settings.database_url)
    if sign_in is None:
        operator = operator_from_environment(environ)
        return create_app(None, None, operator, lifespan=_real_lifespan, port=port)
    slack = HttpSlackOidc(sign_in.client_id, sign_in.client_secret)
    try:
        picker_config = picker_from_settings(
            settings.notion_oauth_client_id, settings.notion_oauth_client_secret
        )
    except ConfigError as err:
        raise SystemExit(str(err)) from err
    if settings.require_grant_picker and picker_config is None:
        raise SystemExit(
            "AGENTOS_REQUIRE_GRANT_PICKER is set but the picker is not configured: set "
            "NOTION_OAUTH_CLIENT_ID and NOTION_OAUTH_CLIENT_SECRET"
        )
    picker = (
        None if picker_config is None else Picker(picker_config, HttpNotionOAuth(picker_config))
    )
    return create_app(
        None,
        None,
        SlackSession(),
        lifespan=_real_lifespan,
        port=port,
        console=Console(sign_in, slack),
        picker=picker,
    )


def main() -> None:
    import os

    import uvicorn

    parser = argparse.ArgumentParser(
        description="Run the control-plane API and web console. Loopback only, unless "
        "sign-in with Slack is configured."
    )
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()
    settings = load_settings()
    app = build_process_app(args.host, args.port, settings, os.environ)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
