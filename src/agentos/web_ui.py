"""The web console: server-rendered pages over the control plane, behind Slack sign-in.

Every page action calls the same ``ControlPlane`` method as the JSON API, with the
signed-in user as the actor, so the two share one role layer. This module decides
nothing about who may do what: it hides controls the role cannot use and shows the
control plane's refusal, with the message and status the API maps (``control_errors``),
when a forged request gets past that.

Requests. A page needs a live session whose tenant is the tenant in the path. A user
who has lost every role sees a "no access" page. A form post is accepted only with
the session's CSRF token (the process-wide Origin check is in ``control_api``). The
form body is parsed here, so the app needs no multipart dependency.

Pages are Jinja2 templates with autoescape on: a definition holding markup is shown
as text.
"""

from __future__ import annotations

import hmac
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote

import yaml
from fastapi import FastAPI
from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse, Response

from agentos.agent_store import PENDING, AgentNotFound
from agentos.config import ConfigError
from agentos.control import ControlPlane
from agentos.control_errors import REFUSALS, status_of
from agentos.grant_checker import GrantProblems
from agentos.members import ADMIN, CREATOR, ROLES
from agentos.sessions import SESSION_LIFETIME
from agentos.slack_signin import (
    CALLBACK_PATH,
    LOGIN_COOKIE,
    LOGIN_COOKIE_PATH,
    LOGIN_COOKIE_SECONDS,
    SESSION_COOKIE,
    SlackSignInError,
    authorize_url,
    identity_from_token_response,
    login_cookie_value,
    new_login_values,
    read_login_cookie,
    read_session_cookie,
    session_cookie_value,
)

log = logging.getLogger("agentos.web_ui")

CONSOLE_FORM_PREFIXES = ("/ui/", "/logout")
MAX_FORM_BYTES = 256 * 1024
AUDIT_PAGE = 50

SKELETON = """\
name: My agent
description: What this agent is for.
system_prompt: |
  You are a helpful agent.
max_steps: 8
grants:
  notion:
    databases: []
    pages: []
"""

# What a page may announce after a change. A page is chosen by a fixed key in the
# redirect, never by text in the URL.
NOTICES = {
    "created": "Agent created as a draft.",
    "draft": "New draft created.",
    "saved": "Draft saved.",
    "submitted": "Submitted for approval.",
    "withdrawn": "Withdrawn; the version is a draft again.",
    "rejected": "Rejected; the version is a draft again.",
    "published": "Published.",
    "member-added": "Member added.",
    "member-role": "Role changed.",
    "member-removed": "Member removed.",
}

_TEMPLATES = Path(__file__).parent / "templates"
_env = Environment(
    loader=FileSystemLoader(_TEMPLATES),
    autoescape=select_autoescape(default=True, default_for_string=True),
    undefined=StrictUndefined,
)


def _stamp(value: datetime | None) -> str:
    return "" if value is None else value.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


_env.filters["stamp"] = _stamp


# --- definitions as YAML text ----------------------------------------------------------


class _Dumper(yaml.SafeDumper):
    pass


def _str_representer(dumper: yaml.SafeDumper, data: str) -> yaml.ScalarNode:
    style = "|" if "\n" in data else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)


_Dumper.add_representer(str, _str_representer)


def definition_text(definition: dict[str, Any]) -> str:
    return yaml.dump(
        definition, Dumper=_Dumper, sort_keys=False, allow_unicode=True, default_flow_style=False
    )


def parse_definition(text: str) -> dict[str, Any]:
    """The definition a textarea holds. A problem is a ``ConfigError``, like the store's own."""
    try:
        parsed = yaml.safe_load(text)
    except yaml.YAMLError as err:
        mark = getattr(err, "problem_mark", None)
        where = f" at line {mark.line + 1}" if mark is not None else ""
        problem = getattr(err, "problem", None) or "syntax error"
        raise ConfigError(f"the definition is not valid YAML{where}: {problem}") from err
    if not isinstance(parsed, dict):
        raise ConfigError("the definition must be a YAML mapping of fields")
    return parsed


# --- one request -----------------------------------------------------------------------


@dataclass
class Ctx:
    """Who is asking, in which tenant, and what the form (if any) carried."""

    tenant_id: str
    user_id: str
    role: str
    csrf: str
    plane: ControlPlane
    config_admins: tuple[str, ...]
    now: datetime
    form: dict[str, str] = field(default_factory=dict)

    @property
    def is_admin(self) -> bool:
        return self.role == ADMIN

    def get(self, name: str) -> str:
        return self.form.get(name, "")


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url, status_code=303)


def _page(template: str, status: int = 200, **values: Any) -> HTMLResponse:
    values.setdefault("ctx", None)
    values.setdefault("notice", None)
    return HTMLResponse(_env.get_template(template).render(**values), status_code=status)


def _message(title: str, text: str, status: int, ctx: Ctx | None = None, back: str | None = None):
    return _page("message.html", status, title=title, text=text, back=back, ctx=ctx)


def _cookie_options(secure: bool) -> dict[str, Any]:
    return {"httponly": True, "secure": secure, "samesite": "lax"}


class WebUI:
    def __init__(self, app: FastAPI):
        self.app = app
        app.add_route("/", self.index, methods=["GET"])
        app.add_route("/login", self.login, methods=["GET"])
        app.add_route(CALLBACK_PATH, self.callback, methods=["GET"])
        app.add_route("/logout", self.logout, methods=["POST"])
        base = "/ui/{tenant_id}"
        agent = base + "/agents/{agent_id}"
        version = agent + "/versions/{version:int}"
        self.route("GET", base, self.tenant_home)
        self.route("GET", base + "/agents", self.agents)
        self.route("POST", base + "/agents", self.add_agent)
        self.route("GET", agent, self.agent)
        self.route("POST", agent + "/drafts", self.new_draft)
        self.route("GET", version, self.version)
        self.route("POST", version + "/edit", self.edit_draft)
        self.route("POST", version + "/check-grants", self.check_grants)
        self.route("POST", version + "/{action}", self.move_version)
        self.route("GET", base + "/members", self.members)
        self.route("POST", base + "/members", self.add_member)
        self.route("POST", base + "/members/{user_id}/role", self.change_role)
        self.route("POST", base + "/members/{user_id}/remove", self.drop_member)
        self.route("GET", base + "/audit", self.audit)

    # --- plumbing ---------------------------------------------------------------------

    @property
    def deps(self):
        return self.app.state.deps

    def route(self, method: str, path: str, handler: Callable[..., Awaitable[Response]]) -> None:
        """Register a tenant page. The handler runs only for a signed-in user of the
        path's tenant; a post also needs the session's CSRF token."""

        async def endpoint(request: Request) -> Response:
            params = dict(request.path_params)
            auth = await self.authenticate(request, params.pop("tenant_id"), post=method == "POST")
            if isinstance(auth, Response):
                return auth
            try:
                return await handler(request, auth, **params)
            except Exception as err:  # noqa: BLE001 - shown as a page; unknown ones say nothing
                return await self.failure(request, auth, err)

        self.app.add_route(path, endpoint, methods=[method])

    async def session_of(self, request: Request):
        """The live session behind the cookie, or None."""
        parsed = read_session_cookie(request.cookies.get(SESSION_COOKIE))
        deps = self.deps
        if parsed is None or deps.sessions is None:
            return None
        tenant_id, token = parsed
        return await deps.sessions.get(tenant_id, token, deps.console.clock())

    async def authenticate(self, request: Request, tenant_id: str, post: bool) -> Ctx | Response:
        session = await self.session_of(request)
        if session is None:
            return _redirect("/login")
        if session.tenant_id != tenant_id:
            return _message(
                "Wrong organisation", "This session belongs to another organisation.", 403
            )
        deps = self.deps
        role = await deps.plane.role_of(tenant_id, session.user_id)
        if role is None:
            return self.no_access()
        config = deps.tenants.get(tenant_id)
        ctx = Ctx(
            tenant_id=tenant_id,
            user_id=session.user_id,
            role=role,
            csrf=session.csrf_secret,
            plane=deps.plane,
            config_admins=() if config is None else config.admins,
            now=deps.console.clock(),
        )
        if post:
            form = await self.read_form(request, session.csrf_secret, ctx)
            if isinstance(form, Response):
                return form
            ctx.form = form
        return ctx

    async def read_form(
        self, request: Request, csrf: str, ctx: Ctx | None = None
    ) -> dict[str, str] | Response:
        """The form's fields, once its CSRF token matches the session's; else a refusal."""
        declared = request.headers.get("content-length", "0")
        if not declared.isdigit() or int(declared) > MAX_FORM_BYTES:
            return _message("Too large", "That form is too large.", 413, ctx)
        body = await request.body()
        if len(body) > MAX_FORM_BYTES:
            return _message("Too large", "That form is too large.", 413, ctx)
        try:
            parsed = parse_qs(body.decode(), keep_blank_values=True, max_num_fields=50)
        except ValueError:
            return _message("Bad request", "That form could not be read.", 400, ctx)
        fields = {name: values[0] for name, values in parsed.items()}
        token = fields.pop("csrf", "")
        if not token or not _same(token, csrf):
            return _message(
                "Form expired",
                "That form is not valid any more. Go back, reload the page and try again.",
                403,
                ctx,
            )
        return fields

    @staticmethod
    def no_access() -> Response:
        return _message(
            "No access",
            "Your Slack account has no role in this console. Ask an admin to add you.",
            403,
        )

    async def failure(
        self,
        request: Request,
        ctx: Ctx,
        err: Exception,
        back: str | None = None,
        rerender: Callable[[int, str], Awaitable[Response]] | None = None,
    ) -> Response:
        """A refusal as a page: the control plane's message with the status the API maps."""
        status = status_of(err)
        if status is None:
            log.error("console error: %s", type(err).__name__)
            return _message("Something went wrong", "Internal error.", 500, ctx, back)
        if rerender is not None:
            return await rerender(status, str(err))
        return _message(_TITLES.get(status, "Refused"), str(err), status, ctx, back)

    def base(self, ctx: Ctx) -> str:
        return f"/ui/{quote(ctx.tenant_id, safe='')}"

    def notice(self, request: Request) -> str | None:
        return NOTICES.get(request.query_params.get("done", ""))

    # --- sign-in ----------------------------------------------------------------------

    async def index(self, request: Request) -> Response:
        session = await self.session_of(request)
        if session is None:
            return _redirect("/login")
        return _redirect(f"/ui/{quote(session.tenant_id, safe='')}/agents")

    async def login(self, request: Request) -> Response:
        sign_in = self.deps.console.sign_in
        tenants = self.deps.tenants
        only = None if tenants is None else tenants.only_tenant()
        team = None if only is None else tenants.get(only).slack_team_id
        state, nonce = new_login_values()
        response = _redirect(authorize_url(sign_in, state, nonce, team))
        response.set_cookie(
            LOGIN_COOKIE,
            login_cookie_value(state, nonce),
            max_age=LOGIN_COOKIE_SECONDS,
            path=LOGIN_COOKIE_PATH,
            **_cookie_options(sign_in.secure_cookies),
        )
        return response

    async def callback(self, request: Request) -> Response:
        console = self.deps.console
        sign_in = console.sign_in

        def done(response: Response) -> Response:
            response.delete_cookie(LOGIN_COOKIE, path=LOGIN_COOKIE_PATH)
            return response

        def refused(text: str, status: int = 400) -> Response:
            return done(_message("Sign-in failed", text, status, back="/login"))

        login = read_login_cookie(request.cookies.get(LOGIN_COOKIE))
        query = request.query_params
        state, code = query.get("state", ""), query.get("code", "")
        if login is None or not state or not _same(state, login[0]):
            return refused("Sign-in could not be verified. Start again.")
        if "error" in query or not code or len(code) > 1024:
            return refused("Slack did not complete the sign-in.")
        try:
            tokens = await console.slack.exchange_code(code, sign_in.redirect_uri)
            identity = identity_from_token_response(tokens, sign_in, login[1], console.clock())
        except SlackSignInError as err:
            log.warning("sign-in refused: %s", err)
            return refused("Sign-in could not be completed. Start again.", 502)
        tenant_id = self.deps.tenants.by_slack_team(identity.team_id)
        if tenant_id is None:
            return refused("This console is not set up for your Slack workspace.", 403)
        if await self.deps.plane.role_of(tenant_id, identity.user_id) is None:
            return done(self.no_access())
        issued = await self.deps.sessions.create(tenant_id, identity.user_id, console.clock())
        response = _redirect(f"/ui/{quote(tenant_id, safe='')}/agents")
        response.set_cookie(
            SESSION_COOKIE,
            session_cookie_value(tenant_id, issued.token),
            max_age=int(SESSION_LIFETIME.total_seconds()),
            path="/",
            **_cookie_options(sign_in.secure_cookies),
        )
        return done(response)

    async def logout(self, request: Request) -> Response:
        """Delete the session row and the cookie. A post with a bad token changes nothing."""
        session = await self.session_of(request)
        response = _message("Signed out", "You are signed out.", 200, back="/login")
        if session is not None:
            form = await self.read_form(request, session.csrf_secret)
            if isinstance(form, Response):
                return form
            _, token = read_session_cookie(request.cookies.get(SESSION_COOKIE))
            await self.deps.sessions.delete(session.tenant_id, token)
        response.delete_cookie(SESSION_COOKIE, path="/")
        return response

    # --- pages ------------------------------------------------------------------------

    async def tenant_home(self, request: Request, ctx: Ctx) -> Response:
        return _redirect(self.base(ctx) + "/agents")

    async def agents(self, request: Request, ctx: Ctx) -> Response:
        return await self.agents_page(request, ctx, notice=self.notice(request))

    async def agents_page(
        self,
        request: Request,
        ctx: Ctx,
        status: int = 200,
        error: str | None = None,
        notice: str | None = None,
    ) -> Response:
        agents = await ctx.plane.list_agents(ctx.tenant_id, ctx.user_id)
        return _page(
            "agents.html",
            status,
            ctx=ctx,
            agents=agents,
            error=error,
            notice=notice,
            new_id=ctx.get("agent_id"),
            new_text=ctx.get("definition") or SKELETON,
            base=self.base(ctx),
        )

    async def add_agent(self, request: Request, ctx: Ctx) -> Response:
        try:
            definition = parse_definition(ctx.get("definition"))
            record = await ctx.plane.add_agent(
                ctx.tenant_id, ctx.user_id, ctx.get("agent_id").strip(), definition
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.agents_page(request, ctx, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        return _redirect(self.version_url(ctx, record.agent_id, record.version, "created"))

    def version_url(self, ctx: Ctx, agent_id: str, version: int, done: str | None = None) -> str:
        url = f"{self.base(ctx)}/agents/{quote(agent_id, safe='')}/versions/{version}"
        return url if done is None else f"{url}?done={done}"

    async def agent(self, request: Request, ctx: Ctx, agent_id: str) -> Response:
        versions = await ctx.plane.versions(ctx.tenant_id, ctx.user_id, agent_id)
        if not versions:
            raise AgentNotFound(f"agent '{agent_id}' does not exist")
        owner = await self.owner_of(ctx, agent_id)
        return _page(
            "agent.html",
            ctx=ctx,
            agent_id=agent_id,
            versions=versions,
            can_edit=self.can_edit(ctx, owner),
            notice=self.notice(request),
            base=self.base(ctx),
        )

    async def owner_of(self, ctx: Ctx, agent_id: str) -> str | None:
        agents = await ctx.plane.list_agents(ctx.tenant_id, ctx.user_id)
        return next((a.created_by for a in agents if a.agent_id == agent_id), None)

    @staticmethod
    def can_edit(ctx: Ctx, owner: str | None) -> bool:
        """Whether to show editing controls: an admin, or the creator of this agent.

        This only decides what is shown; ``ControlPlane`` decides what is allowed."""
        return ctx.role == ADMIN or (ctx.role == CREATOR and owner == ctx.user_id)

    async def new_draft(self, request: Request, ctx: Ctx, agent_id: str) -> Response:
        raw = ctx.get("from_version").strip()
        from_version = int(raw) if raw.isdigit() else None
        record = await ctx.plane.new_draft(ctx.tenant_id, ctx.user_id, agent_id, from_version)
        return _redirect(self.version_url(ctx, agent_id, record.version, "draft"))

    async def version(self, request: Request, ctx: Ctx, agent_id: str, version: int) -> Response:
        return await self.version_page(request, ctx, agent_id, version, notice=self.notice(request))

    async def version_page(
        self,
        request: Request,
        ctx: Ctx,
        agent_id: str,
        version: int,
        status: int = 200,
        error: str | None = None,
        notice: str | None = None,
        text: str | None = None,
        reports: list | None = None,
    ) -> Response:
        record = await ctx.plane.get_version(ctx.tenant_id, ctx.user_id, agent_id, version)
        if record is None:
            raise AgentNotFound(f"agent '{agent_id}' has no version {version}")
        editor = self.can_edit(ctx, await self.owner_of(ctx, agent_id))
        return _page(
            "version.html",
            status,
            ctx=ctx,
            record=record,
            definition=text if text is not None else definition_text(record.definition),
            editor=editor,
            is_draft=record.status == "draft",
            is_pending=record.status == PENDING,
            error=error,
            notice=notice,
            reports=reports,
            grants_ok=None if reports is None else not any(r.problems for r in reports),
            base=self.base(ctx),
            agent_url=f"{self.base(ctx)}/agents/{quote(agent_id, safe='')}",
            version_url=self.version_url(ctx, agent_id, version),
        )

    async def edit_draft(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        text = ctx.get("definition")
        try:
            definition = parse_definition(text)
            await ctx.plane.edit_draft(ctx.tenant_id, ctx.user_id, agent_id, version, definition)
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.version_page(
                    request, ctx, agent_id, version, status, message, text=text
                )

            return await self.failure(request, ctx, err, rerender=again)
        return _redirect(self.version_url(ctx, agent_id, version, "saved"))

    async def check_grants(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        reports = await ctx.plane.check_version_grants(
            ctx.tenant_id, ctx.user_id, agent_id, version
        )
        return await self.version_page(request, ctx, agent_id, version, reports=reports)

    async def move_version(self, request: Request, ctx: Ctx, agent_id: str, version: int, action):
        plane = ctx.plane
        moves = {
            "submit": (plane.submit_version, "submitted"),
            "withdraw": (plane.withdraw_version, "withdrawn"),
            "reject": (plane.reject_version, "rejected"),
            "publish": (plane.publish_version, "published"),
        }
        if action not in moves:
            return _message("Not found", "There is no such action.", 404, ctx)
        call, done = moves[action]
        try:
            await call(ctx.tenant_id, ctx.user_id, agent_id, version)
        except GrantProblems as err:
            return await self.version_page(
                request, ctx, agent_id, version, 409, str(err), reports=err.reports
            )
        return _redirect(self.version_url(ctx, agent_id, version, done))

    async def members(self, request: Request, ctx: Ctx) -> Response:
        rows = await ctx.plane.list_members(ctx.tenant_id, ctx.user_id)
        return _page(
            "members.html",
            ctx=ctx,
            members=rows,
            roles=ROLES,
            notice=self.notice(request),
            base=self.base(ctx),
        )

    async def add_member(self, request: Request, ctx: Ctx) -> Response:
        await ctx.plane.invite_member(
            ctx.tenant_id, ctx.user_id, ctx.get("user_id").strip(), ctx.get("role")
        )
        return _redirect(self.base(ctx) + "/members?done=member-added")

    async def change_role(self, request: Request, ctx: Ctx, user_id: str) -> Response:
        await ctx.plane.change_role(ctx.tenant_id, ctx.user_id, user_id, ctx.get("role"))
        return _redirect(self.base(ctx) + "/members?done=member-role")

    async def drop_member(self, request: Request, ctx: Ctx, user_id: str) -> Response:
        await ctx.plane.drop_member(ctx.tenant_id, ctx.user_id, user_id)
        return _redirect(self.base(ctx) + "/members?done=member-removed")

    async def audit(self, request: Request, ctx: Ctx) -> Response:
        raw = request.query_params.get("before", "")
        before = int(raw) if raw.isdigit() else None
        events = await ctx.plane.audit_log(
            ctx.tenant_id, ctx.user_id, AUDIT_PAGE, before_seq=before
        )
        older = events[-1].seq if len(events) == AUDIT_PAGE else None
        return _page("audit.html", ctx=ctx, events=events, older=older, base=self.base(ctx))


_TITLES = {
    403: "Not allowed",
    404: "Not found",
    409: "Not possible now",
    422: "Invalid",
    502: "Notion could not be reached",
}


def _same(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode(), b.encode())


def install_web_ui(app: FastAPI) -> None:
    WebUI(app)
