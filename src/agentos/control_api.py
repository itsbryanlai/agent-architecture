"""The control-plane API: agents, versions, members and the audit log, as JSON.

This is a separate app and process from the gateway. The gateway is reachable
from the internet; this API is not, and must not be. It listens on the loopback
address only and the entry point refuses any other bind. Never point a tunnel at
its port. Start it with::

    AGENTOS_CONTROL_ACTOR=<your Slack user ID> uv run python -m agentos.control_api

Who is acting. The acting Slack user ID comes from an ``IdentityProvider``, which
the process owns; nothing in a request (header, query or body) can set or change
it. The one provider here is ``LocalOperator``: a single user ID fixed when the
process starts. A Slack sign-in session will replace it.

What the actor may do is decided only by ``ControlPlane``, from the actor's role
in the tenant named in the path. This module adds no rule of its own, calls no
store, and only turns the control plane's answers and refusals into HTTP.

This module does not use ``from __future__ import annotations``: FastAPI reads the
route parameters' type aliases at definition time.
"""

import argparse
import hashlib
import logging
import re
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Any, Protocol

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from agentos.agent_store import AgentNotFound, AgentStoreError
from agentos.config import ConfigError, TenantsConfig
from agentos.control import ControlPlane, Forbidden
from agentos.grant_checker import GrantCheckUnavailable, GrantProblems
from agentos.members import InvalidMember, MemberError, MemberNotFound

log = logging.getLogger("agentos.control_api")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8100
LOOPBACK_HOSTS = ("127.0.0.1", "::1", "localhost")
ACTOR_VARIABLE = "AGENTOS_CONTROL_ACTOR"
MAX_AUDIT_LIMIT = 200
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


def check_bind_host(host: str) -> None:
    """Refuse any host but loopback: this API must never be reachable from outside."""
    if host not in LOOPBACK_HOSTS:
        raise SystemExit(
            f"refusing to listen on '{host}': the control API binds only to "
            f"{', '.join(LOOPBACK_HOSTS)} until sign-in with Slack exists"
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
    tenants: TenantsConfig | None


def create_app(
    plane: ControlPlane | None,
    tenants: TenantsConfig | None,
    identity: IdentityProvider,
    lifespan: Any = None,
    port: int = DEFAULT_PORT,
) -> FastAPI:
    """Build the API. Tests pass fakes; the real process fills ``deps`` on startup."""
    deps = ControlDeps(plane, tenants)
    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.deps = deps
    allowed_hosts = {f"{host}:{port}" for host in ("127.0.0.1", "localhost", "[::1]")}
    allowed_origins = {f"http://{host}" for host in allowed_hosts}

    @app.middleware("http")
    async def local_browser_guard(request: Request, call_next):
        """Keep a web page in the operator's browser from driving this API.

        The API acts as a fixed operator for anything that reaches the port, so it
        refuses what a browser could send on a page's behalf: a foreign Host (DNS
        rebinding), a foreign Origin or a cross-site fetch (CSRF), and a write that is
        not JSON (a form or no-cors post is a simple request; JSON forces a preflight,
        which fails because no CORS is offered). Refusals say nothing about the request.
        """
        if request.url.path == "/healthz":
            return await call_next(request)
        headers = request.headers
        if headers.get("host") not in allowed_hosts:
            return _error(421, "host not allowed")
        origin = headers.get("origin")
        if origin is not None and origin not in allowed_origins:
            return _error(403, "origin not allowed")
        if headers.get("sec-fetch-site") in ("cross-site", "same-site"):
            return _error(403, "cross-site requests are not allowed")
        if request.method in ("POST", "PUT", "PATCH", "DELETE"):
            media_type = headers.get("content-type", "").split(";")[0].strip().lower()
            if media_type != "application/json":
                return _error(415, "writes must be sent as application/json")
        return await call_next(request)

    @app.exception_handler(Forbidden)
    async def forbidden(request: Request, err: Forbidden) -> JSONResponse:
        return _error(403, str(err))

    @app.exception_handler(AgentNotFound)
    async def agent_not_found(request: Request, err: AgentNotFound) -> JSONResponse:
        return _error(404, str(err))

    @app.exception_handler(MemberNotFound)
    async def member_not_found(request: Request, err: MemberNotFound) -> JSONResponse:
        return _error(404, str(err))

    @app.exception_handler(InvalidMember)
    async def invalid_member(request: Request, err: InvalidMember) -> JSONResponse:
        return _error(422, str(err))

    @app.exception_handler(AgentStoreError)
    async def wrong_state(request: Request, err: AgentStoreError) -> JSONResponse:
        return _error(409, str(err))

    @app.exception_handler(MemberError)
    async def member_conflict(request: Request, err: MemberError) -> JSONResponse:
        return _error(409, str(err))

    @app.exception_handler(ConfigError)
    async def invalid_definition(request: Request, err: ConfigError) -> JSONResponse:
        return _error(422, str(err))

    @app.exception_handler(GrantProblems)
    async def grant_problems(request: Request, err: GrantProblems) -> JSONResponse:
        return _error(409, str(err), grants=jsonable_encoder(err.reports))

    @app.exception_handler(GrantCheckUnavailable)
    async def grants_unreadable(request: Request, err: GrantCheckUnavailable) -> JSONResponse:
        return _error(502, str(err))

    @app.exception_handler(RequestValidationError)
    async def bad_request(request: Request, err: RequestValidationError) -> JSONResponse:
        # Messages and locations only: the default response would echo the input.
        problems = [f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in err.errors()]
        return _error(422, "invalid request", problems=problems)

    @app.exception_handler(Exception)
    async def unexpected(request: Request, err: Exception) -> JSONResponse:
        log.error("control API error: %s", type(err).__name__)
        return _error(500, "internal error")

    def known_tenant(tenant_id: str) -> str:
        if deps.tenants is None or deps.tenants.get(tenant_id) is None:
            raise HTTPException(404, f"unknown tenant '{tenant_id}'")
        return tenant_id

    Tenant = Annotated[str, Depends(known_tenant)]

    async def acting(request: Request, tenant_id: Tenant) -> str:
        return await identity.actor(request)

    Actor = Annotated[str, Depends(acting)]

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
        tenant_id: Tenant, actor: Actor, rid: RequestId, agent_id: str, version: int
    ) -> JSONResponse:
        return out(await deps.plane.publish_version(tenant_id, actor, agent_id, version, rid))

    @app.post("/tenants/{tenant_id}/agents/{agent_id}/versions/{version}/check-grants")
    async def check_grants(
        tenant_id: Tenant, actor: Actor, agent_id: str, version: int
    ) -> JSONResponse:
        reports = await deps.plane.check_version_grants(tenant_id, actor, agent_id, version)
        return out({"ok": not any(r.problems for r in reports), "grants": reports})

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

    return app


# --- the real process ---------------------------------------------------------------


@asynccontextmanager
async def _real_lifespan(app: FastAPI):
    """Build the real dependencies when the server starts, not at import."""
    from psycopg_pool import AsyncConnectionPool

    from agentos.agent_store import PostgresAgentStore
    from agentos.audit import PostgresAuditStore
    from agentos.config import load_settings, load_tenants_config
    from agentos.connectors.notion import RealNotionAPI
    from agentos.grant_checker import NotionGrantChecker
    from agentos.members import PostgresMemberStore
    from agentos.tenants import require_tenant_secret

    logging.basicConfig(level=logging.INFO)
    settings = load_settings()
    tenants = load_tenants_config(settings.config_path)
    pool = AsyncConnectionPool(settings.database_url, open=False)
    await pool.open()
    try:
        apis: dict[str, RealNotionAPI] = {}

        def notion_for(tenant_id: str) -> RealNotionAPI:
            if tenant_id not in apis:
                config = tenants.get(tenant_id)
                apis[tenant_id] = RealNotionAPI(require_tenant_secret(config, "notion_token_env"))
            return apis[tenant_id]

        app.state.deps.plane = ControlPlane(
            PostgresAgentStore(pool),
            PostgresMemberStore(pool),
            PostgresAuditStore(pool),
            tenants,
            NotionGrantChecker(notion_for),
        )
        app.state.deps.tenants = tenants
        yield
    finally:
        await pool.close()


def main() -> None:
    import os

    import uvicorn

    parser = argparse.ArgumentParser(description="Run the control-plane API on this machine only.")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()
    check_bind_host(args.host)
    operator = operator_from_environment(os.environ)
    app = create_app(None, None, operator, lifespan=_real_lifespan, port=args.port)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
