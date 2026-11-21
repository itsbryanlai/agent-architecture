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

import hashlib
import hmac
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlencode

import yaml
from fastapi import FastAPI
from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse, Response

from agentos import ids
from agentos.access_diff import WideningNotAcknowledged
from agentos.agent_feedback import FEEDBACK_DAYS
from agentos.agent_store import PENDING, AgentNotFound, VersionChanged
from agentos.audience import AudienceError, AudienceNotConfirmed
from agentos.audience_pauses import REASON_TEXT
from agentos.config import ConfigError
from agentos.connectors.google_iam import MIME_DOC, DriveItem, GoogleError
from agentos.connectors.notion import normalize_id
from agentos.control import ControlPlane, Forbidden, Principal
from agentos.control_errors import REFUSALS, page_status, status_of
from agentos.crypto_keys import TenantKeyError
from agentos.grant_checker import GrantCheckUnavailable, GrantProblems
from agentos.grant_picks import GrantNotPicked
from agentos.members import ADMIN, CREATOR, ROLES
from agentos.notion_picker import (
    NOTION_CALLBACK_PATH,
    Flow,
    NotionPickerError,
)
from agentos.notion_picker import (
    authorize_url as notion_authorize_url,
)
from agentos.sessions import SESSION_LIFETIME, session_hash
from agentos.slack_install import (
    INSTALL_CALLBACK_PATH,
    INSTALL_COOKIE,
    INSTALL_COOKIE_SECONDS,
    INSTALL_PATH,
    SlackInstallError,
    grant_from_response,
    new_install_state,
    read_install_cookie,
)
from agentos.slack_install import (
    authorize_url as install_authorize_url,
)
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
from agentos.test_sets import EXPECT_KINDS, EXPECT_LABELS
from agentos.test_sets import REASON_TEXT as CASE_REASON_TEXT
from agentos.usage import NO_TEAM

log = logging.getLogger("agentos.web_ui")

CONSOLE_FORM_PREFIXES = ("/ui/", "/logout")
MAX_FORM_BYTES = 256 * 1024
AUDIT_PAGE = 50
# Turning these off stops a whole organisation, so the form asks for a confirmation.
CONFIRMED_SWITCHES = ("all_agents", "writes")

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
    "picked": "Recorded. The objects you chose can now be granted in the definition.",
    "consent-revoked": "Your Notion sign-in is no longer kept. The nightly check will show "
    "your grants as cannot verify.",
    "resumed": "Resumed; the agent answers in that channel again.",
    "member-added": "Member added.",
    "member-role": "Role changed.",
    "member-removed": "Member removed.",
    "budget-set": "Budget saved.",
    "budget-removed": "Budget removed.",
    "switch-off": "Turned off. It takes effect on the next step.",
    "switch-on": "Turned back on.",
    "routing-saved": "Routing saved. Every process picks it up on its next refresh.",
    "approval-policy-saved": "Approval policy saved. The next task reads it.",
    "notion-connected": "Notion connected.",
    "notion-disconnected": "Notion disconnected.",
    "drive-connected": "Google Drive connected. Share folders and files with the address on this page.",
    "drive-disconnected": "Google Drive disconnected. The organisation's account was deleted at Google.",
    "drive-picked": "Google Drive grants added to the draft.",
    "drive-removed": "Google Drive grant removed from the draft.",
    "webhook-confirmed": "Webhook token confirmed. Signed events are accepted.",
    "webhook-cleared": "Webhook token cleared. A new handshake is accepted.",
    "case-added": "Test case added to the draft. Choose its expected behaviour.",
    "case-expect": "Expected behaviour saved.",
    "case-removed": "Test case removed.",
    "test-run": "Test run started. Its results appear on this page when it finishes.",
}

# What the version page shows after a refused publish. Like NOTICES, the redirect
# carries a fixed key, never the error text.
REFUSED = {
    "version-changed": "The version changed since you opened it; review it again and publish.",
}

_TEMPLATES = Path(__file__).parent / "templates"
_env = Environment(
    loader=FileSystemLoader(_TEMPLATES),
    autoescape=select_autoescape(default=True, default_for_string=True),
    undefined=StrictUndefined,
)


def _stamp(value: datetime | None) -> str:
    return "" if value is None else value.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def _usd(value: Decimal) -> str:
    return f"${value:,.4f}"


def _usd2(value: Decimal) -> str:
    return f"${value:,.2f}"


_env.filters["stamp"] = _stamp
# A one-time token for a form that must not repeat, such as a new draft.
_env.globals["new_token"] = lambda: uuid.uuid4().hex
_env.globals["pause_reason"] = lambda code: REASON_TEXT.get(code, code)
_env.filters["usd"] = _usd
_env.filters["usd2"] = _usd2


def recent_months(now: datetime, count: int = 12) -> list[str]:
    """The current UTC month and the ones before it, newest first, as YYYY-MM."""
    now = now.astimezone(UTC)
    index = now.year * 12 + now.month - 1
    return [f"{(i // 12):04d}-{(i % 12) + 1:02d}" for i in range(index, index - count, -1)]


def _feedback_days(raw: str) -> int:
    """The feedback window the query asked for: one of the fixed choices, else the default."""
    return int(raw) if raw.isdigit() and int(raw) in FEEDBACK_DAYS else FEEDBACK_DAYS[0]


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


def grant_rows(agent, review, creator_access=None) -> list[dict[str, Any]]:
    """The version's grants as table rows: the creator's label and ID, who picked each from
    Notion, the nightly creator-access verdict, and (for an admin, on a draft or pending
    version) the live title and parent."""
    live = {}
    if review is not None and review.reports:
        live = {normalize_id(r.database_id): r for r in review.reports}
    picks = {} if review is None else review.picks
    access = {} if creator_access is None else creator_access
    rows = []
    for kind, grant in (
        *(("database", g) for g in agent.grants.notion_databases),
        *(("page", g) for g in agent.grants.notion_pages),
    ):
        key = normalize_id(grant.id)
        rows.append(
            {
                "label": grant.name,
                "kind": kind,
                "id": grant.id,
                "pick": picks.get(key),
                "live": live.get(key),
                "access": access.get(key),
            }
        )
    return rows


# --- one request -----------------------------------------------------------------------


@dataclass
class Ctx:
    """Who is asking, in which tenant, and what the form (if any) carried."""

    tenant_id: str
    user_id: str
    role: str
    principal: Principal
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
        self.route("GET", agent + "/feedback", self.feedback)
        self.route("POST", agent + "/channels/{channel}/resume", self.resume_channel)
        self.route("POST", agent + "/drafts", self.new_draft)
        self.route("GET", version, self.version)
        self.route("POST", version + "/edit", self.edit_draft)
        self.route("POST", version + "/check-grants", self.check_grants)
        self.route("POST", version + "/pick/start", self.pick_start)
        self.route("GET", version + "/pick", self.pick_page)
        self.route("POST", version + "/pick/choose", self.pick_choose)
        self.route("POST", version + "/pick/revoke", self.pick_revoke)
        self.route("GET", version + "/drive/pick", self.drive_pick_page)
        self.route("POST", version + "/drive/pick/choose", self.drive_pick_choose)
        self.route("GET", version + "/drive/pick/tabs", self.drive_tab_page)
        self.route("POST", version + "/drive/pick/tabs", self.drive_tab_choose)
        self.route("GET", version + "/drive/pick/template", self.drive_template_page)
        self.route("POST", version + "/drive/pick/template", self.drive_template_choose)
        self.route("POST", version + "/drive/remove", self.drive_remove)
        self.route("POST", version + "/test-cases", self.add_test_case)
        self.route("POST", version + "/test-cases/{case_id}/expect", self.set_case_expect)
        self.route("POST", version + "/test-cases/{case_id}/remove", self.remove_test_case)
        self.route("POST", version + "/test-run", self.run_tests)
        self.route("POST", agent + "/test-cases", self.case_from_feedback)
        app.add_route(NOTION_CALLBACK_PATH, self.notion_callback, methods=["GET"])
        self.route("POST", version + "/{action}", self.move_version)
        self.route("GET", base + "/members", self.members)
        self.route("POST", base + "/members", self.add_member)
        self.route("POST", base + "/members/{user_id}/role", self.change_role)
        self.route("POST", base + "/members/{user_id}/remove", self.drop_member)
        self.route("GET", base + "/audit", self.audit)
        self.route("GET", base + "/usage", self.usage)
        self.route("GET", base + "/usage.csv", self.usage_csv)
        self.route("POST", base + "/budgets", self.set_budget)
        self.route("POST", base + "/budgets/remove", self.remove_budget)
        self.route("GET", base + "/switches", self.switches)
        self.route("POST", base + "/switches/off", self.turn_off)
        self.route("POST", base + "/switches/on", self.turn_on)
        self.route("GET", base + "/settings", self.settings)
        self.route("POST", base + "/settings/routing", self.set_routing)
        self.route("POST", base + "/settings/approvals", self.set_approval_policy)
        self.route("POST", base + "/settings/notion/start", self.notion_connect_start)
        self.route("POST", base + "/settings/notion/disconnect", self.notion_disconnect)
        self.route("POST", base + "/settings/drive/connect", self.drive_connect)
        self.route("POST", base + "/settings/drive/disconnect", self.drive_disconnect)
        self.route("POST", base + "/settings/webhook/reveal", self.webhook_reveal)
        self.route("POST", base + "/settings/webhook/confirm", self.webhook_confirm)
        self.route("POST", base + "/settings/webhook/clear", self.webhook_clear)
        app.add_route("/static/console.js", self.console_js, methods=["GET"])
        if self.deps.console is not None:
            # Public pages: the install runs before any tenant (or session) exists.
            app.add_route(INSTALL_PATH, self.install_page, methods=["GET"])
            app.add_route(INSTALL_CALLBACK_PATH, self.install_callback, methods=["GET"])

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
        config = await deps.tenants.get(tenant_id)
        if config is None:
            # Unknown or disabled: a session of a disabled tenant stops working.
            return _message(
                "Not available", "This organisation is not available.", 403, back="/login"
            )
        principal = await deps.plane.principal(tenant_id, session.user_id)
        if principal.role is None:
            return self.no_access()
        ctx = Ctx(
            tenant_id=tenant_id,
            user_id=session.user_id,
            role=principal.role,
            principal=principal,
            csrf=session.csrf_secret,
            plane=deps.plane,
            config_admins=config.admins,
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
        message: str | None = None,
    ) -> Response:
        """A refusal as a page: the control plane's message with the status the API maps.

        ``message`` overrides the text when the console words a refusal itself.
        """
        status = status_of(err)
        status = None if status is None else page_status(status)
        if status is None:
            log.error("console error: %s", type(err).__name__)
            return _message("Something went wrong", "Internal error.", 500, ctx, back)
        text = str(err) if message is None else message
        if rerender is not None:
            try:
                return await rerender(status, text)
            except AgentNotFound:
                # The page to show again is one this person may not read.
                pass
        return _message(_TITLES.get(status, "Refused"), text, status, ctx, back)

    def base(self, ctx: Ctx) -> str:
        return f"/ui/{quote(ctx.tenant_id, safe='')}"

    def notice(self, request: Request) -> str | None:
        return NOTICES.get(request.query_params.get("done", ""))

    def refusal(self, request: Request) -> str | None:
        return REFUSED.get(request.query_params.get("refused", ""))

    # --- sign-in ----------------------------------------------------------------------

    async def index(self, request: Request) -> Response:
        session = await self.session_of(request)
        if session is None:
            return _redirect("/login")
        return _redirect(f"/ui/{quote(session.tenant_id, safe='')}/agents")

    async def login(self, request: Request) -> Response:
        sign_in = self.deps.console.sign_in
        tenants = self.deps.tenants
        only = None if tenants is None else await tenants.only_tenant()
        team = None if only is None else (await tenants.get(only)).slack_team_id
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
            identity = await identity_from_token_response(
                tokens, sign_in, login[1], console.clock(), console.slack
            )
        except SlackSignInError as err:
            log.warning("sign-in refused: %s", err)
            return refused("Sign-in could not be completed. Start again.", 503)
        tenant_id = await self.deps.tenants.for_slack_team(identity.team_id)
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

    # --- the Slack install --------------------------------------------------------------

    async def install_page(self, request: Request) -> Response:
        """The public "Add to Slack" page. The state cookie binds the flow to this browser."""
        install = self.deps.install
        if install is None:
            return _message("Not available", "Installing is not enabled on this console.", 404)
        config = install.config
        if not config.allowed_teams:
            return _message(
                "Install not enabled",
                "Installing is not enabled on this console yet. "
                "Ask the platform operator to enable it for your workspace.",
                200,
            )
        state = new_install_state()
        response = _page("install.html", authorize_url=install_authorize_url(config, state))
        response.set_cookie(
            INSTALL_COOKIE,
            state,
            max_age=INSTALL_COOKIE_SECONDS,
            path=INSTALL_PATH,
            **_cookie_options(config.secure_cookies),
        )
        return response

    async def install_callback(self, request: Request) -> Response:
        """Slack sends the browser back here; one unit of work creates the tenant."""
        install = self.deps.install
        if install is None:
            return _message("Not available", "Installing is not enabled on this console.", 404)
        config = install.config

        def done(response: Response) -> Response:
            response.delete_cookie(INSTALL_COOKIE, path=INSTALL_PATH)
            return response

        def refused(text: str, status: int = 400) -> Response:
            return done(_message("Install failed", text, status, back=INSTALL_PATH))

        query = request.query_params
        state, code = query.get("state", ""), query.get("code", "")
        cookie = read_install_cookie(request.cookies.get(INSTALL_COOKIE))
        if cookie is None or not state or not _same(state, cookie):
            return refused("The install could not be verified. Start again.")
        if "error" in query or not code or len(code) > 1024:
            return refused("Slack did not complete the install.")
        team_id = ""

        def log_refused(err: Exception) -> None:
            if team_id:
                log.warning(
                    "install refused tenant_id=%s: %s", ids.tenant_for_slack_team(team_id), err
                )
            else:
                log.warning("install refused: %s", err)

        try:
            body = await install.oauth.exchange_code(code, config.redirect_uri)
            team_id, bot_token, installer = grant_from_response(body)
            result = await install.install(team_id, bot_token, installer)
        except SlackInstallError as err:
            log_refused(err)
            return refused(str(err), err.status)
        except ConfigError as err:
            log_refused(err)
            return refused(str(err), 409)
        except TenantKeyError as err:
            log_refused(err)
            return refused(str(err), 503)
        if result.created:
            text = (
                f"The app is installed for workspace {result.team_id}. Sign in to finish the setup."
            )
        else:
            text = (
                f"The app was reinstalled for workspace {result.team_id}; "
                "its bot token was replaced."
            )
        return done(
            _message("Installed", text, 200, back=f"/ui/{quote(result.tenant_id, safe='')}/agents")
        )

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
        agents = await ctx.plane.list_agents(ctx.tenant_id, ctx.principal)
        return _page(
            "agents.html",
            status,
            ctx=ctx,
            agents=agents,
            health=await ctx.plane.agent_health(ctx.tenant_id, ctx.principal),
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
                ctx.tenant_id,
                ctx.user_id,
                ctx.get("agent_id").strip(),
                definition,
                request_id=_form_request_id(ctx),
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
        return await self.agent_page(request, ctx, agent_id, notice=self.notice(request))

    async def agent_page(
        self,
        request: Request,
        ctx: Ctx,
        agent_id: str,
        status: int = 200,
        error: str | None = None,
        notice: str | None = None,
    ) -> Response:
        versions = await ctx.plane.versions(ctx.tenant_id, ctx.principal, agent_id)
        if not versions:
            raise AgentNotFound(f"agent '{agent_id}' does not exist")
        owner = await self.owner_of(ctx, agent_id)
        budgets = await ctx.plane.budgets(ctx.tenant_id, ctx.principal)
        switches = await ctx.plane.switches(ctx.tenant_id, ctx.principal)
        can_edit = self.can_edit(ctx, owner)
        return _page(
            "agent.html",
            status,
            ctx=ctx,
            agent_id=agent_id,
            budget=budgets.line_of(agent_id),
            stopped_by=switches.stopped_by.get(agent_id),
            writes_off=switches.writes.off,
            versions=versions,
            can_edit=can_edit,
            paused=await self.paused_of(ctx, agent_id) if can_edit else [],
            error=error,
            notice=notice,
            base=self.base(ctx),
            agent_url=f"{self.base(ctx)}/agents/{quote(agent_id, safe='')}",
        )

    @staticmethod
    async def paused_of(ctx: Ctx, agent_id: str):
        return await ctx.plane.paused_channels(ctx.tenant_id, ctx.principal, agent_id)

    async def feedback(self, request: Request, ctx: Ctx, agent_id: str) -> Response:
        """The agent's feedback page: its creator and admins, never who asked or voted."""
        days = _feedback_days(request.query_params.get("days", ""))
        view = await ctx.plane.agent_feedback(ctx.tenant_id, ctx.principal, agent_id, days)
        return _page(
            "feedback.html",
            ctx=ctx,
            view=view,
            agent_id=agent_id,
            days=days,
            days_choices=FEEDBACK_DAYS,
            base=self.base(ctx),
            agent_url=f"{self.base(ctx)}/agents/{quote(agent_id, safe='')}",
        )

    async def resume_channel(self, request: Request, ctx: Ctx, agent_id: str, channel: str):
        try:
            await ctx.plane.resume_channel(
                ctx.tenant_id,
                ctx.user_id,
                agent_id,
                channel,
                confirm_audience=_audience_confirmation(ctx),
                request_id=_form_request_id(ctx),
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.agent_page(request, ctx, agent_id, status, message)

            return await self.failure(
                request, ctx, err, rerender=again, message=_console_refusal(err)
            )
        return _redirect(f"{self.base(ctx)}/agents/{quote(agent_id, safe='')}?done=resumed")

    async def owner_of(self, ctx: Ctx, agent_id: str) -> str | None:
        if ctx.role == CREATOR:
            # A creator reads only their own agents, so the principal already says.
            return ctx.user_id if agent_id in ctx.principal.owned_agent_ids else None
        agents = await ctx.plane.list_agents(ctx.tenant_id, ctx.principal)
        return next((a.created_by for a in agents if a.agent_id == agent_id), None)

    @staticmethod
    def can_edit(ctx: Ctx, owner: str | None) -> bool:
        """Whether to show editing controls: an admin, or the creator of this agent.

        This only decides what is shown; ``ControlPlane`` decides what is allowed."""
        return ctx.role == ADMIN or (ctx.role == CREATOR and owner == ctx.user_id)

    async def new_draft(self, request: Request, ctx: Ctx, agent_id: str) -> Response:
        raw = ctx.get("from_version").strip()
        from_version = int(raw) if raw.isdigit() else None
        record = await ctx.plane.new_draft(
            ctx.tenant_id, ctx.user_id, agent_id, from_version, _form_request_id(ctx)
        )
        return _redirect(self.version_url(ctx, agent_id, record.version, "draft"))

    async def version(self, request: Request, ctx: Ctx, agent_id: str, version: int) -> Response:
        return await self.version_page(
            request,
            ctx,
            agent_id,
            version,
            notice=self.notice(request),
            error=self.refusal(request),
        )

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
        record = await ctx.plane.get_version(ctx.tenant_id, ctx.principal, agent_id, version)
        if record is None:
            raise AgentNotFound(f"agent '{agent_id}' has no version {version}")
        editor = self.can_edit(ctx, await self.owner_of(ctx, agent_id))
        audience = await ctx.plane.audience_view(
            ctx.tenant_id,
            ctx.principal,
            agent_id,
            version,
            count=(editor or ctx.is_admin) and record.status in ("draft", PENDING),
        )
        review = (
            await ctx.plane.version_review(ctx.tenant_id, ctx.principal, agent_id, version)
            if editor or ctx.is_admin
            else None
        )
        picker = self.deps.picker
        may_see_access = picker is not None and (editor or ctx.is_admin)
        creator_access = (
            await ctx.plane.creator_access_view(ctx.tenant_id, ctx.principal, agent_id)
            if may_see_access
            else {}
        )
        creator_consented = (
            await ctx.plane.creator_consent(ctx.tenant_id, ctx.principal)
            if may_see_access
            else False
        )
        cases = await ctx.plane.test_cases(ctx.tenant_id, ctx.principal, agent_id, version)
        run_review = await ctx.plane.test_run_review(
            ctx.tenant_id, ctx.principal, agent_id, version
        )
        drive_configured = drive_connected = False
        if editor or ctx.is_admin:
            drive_configured, drive_connected = await ctx.plane.drive_connection_state(
                ctx.tenant_id, ctx.principal
            )
        return _page(
            "version.html",
            status,
            ctx=ctx,
            record=record,
            audience=audience,
            review=review,
            grant_rows=grant_rows(record.agent(), review, creator_access),
            drive_grants=record.agent().grants.drive,
            drive_configured=drive_configured,
            drive_connected=drive_connected,
            widens=bool(review and review.diff and review.diff.widens),
            picker_enabled=picker is not None,
            creator_consented=creator_consented,
            paused=await self.paused_of(ctx, agent_id) if editor or ctx.is_admin else [],
            definition=text if text is not None else definition_text(record.definition),
            editor=editor,
            is_draft=record.status == "draft",
            is_pending=record.status == PENDING,
            error=error,
            notice=notice,
            reports=reports,
            grants_ok=None if reports is None else all(r.ok for r in reports),
            cases=cases,
            run_review=run_review,
            expect_kinds=EXPECT_KINDS,
            expect_labels=EXPECT_LABELS,
            case_reason=CASE_REASON_TEXT,
            base=self.base(ctx),
            agent_url=f"{self.base(ctx)}/agents/{quote(agent_id, safe='')}",
            version_url=self.version_url(ctx, agent_id, version),
        )

    async def edit_draft(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        text = ctx.get("definition")
        try:
            definition = parse_definition(text)
            await ctx.plane.edit_draft(
                ctx.tenant_id,
                ctx.user_id,
                agent_id,
                version,
                definition,
                request_id=_form_request_id(ctx),
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.version_page(
                    request, ctx, agent_id, version, status, message, text=text
                )

            return await self.failure(request, ctx, err, rerender=again)
        return _redirect(self.version_url(ctx, agent_id, version, "saved"))

    # --- test sets --------------------------------------------------------------------

    async def add_test_case(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        try:
            await ctx.plane.add_test_case(
                ctx.tenant_id,
                ctx.user_id,
                agent_id,
                version,
                ctx.get("question"),
                ctx.get("expect"),
                ctx.get("target"),
                request_id=_form_request_id(ctx),
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.version_page(request, ctx, agent_id, version, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        return _redirect(self.version_url(ctx, agent_id, version, "case-added"))

    async def set_case_expect(
        self, request: Request, ctx: Ctx, agent_id: str, version: int, case_id: str
    ):
        try:
            await ctx.plane.set_case_expect(
                ctx.tenant_id,
                ctx.user_id,
                agent_id,
                version,
                case_id,
                ctx.get("expect"),
                ctx.get("target"),
                request_id=_form_request_id(ctx),
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.version_page(request, ctx, agent_id, version, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        return _redirect(self.version_url(ctx, agent_id, version, "case-expect"))

    async def remove_test_case(
        self, request: Request, ctx: Ctx, agent_id: str, version: int, case_id: str
    ):
        try:
            await ctx.plane.remove_test_case(
                ctx.tenant_id,
                ctx.user_id,
                agent_id,
                version,
                case_id,
                request_id=_form_request_id(ctx),
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.version_page(request, ctx, agent_id, version, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        return _redirect(self.version_url(ctx, agent_id, version, "case-removed"))

    async def run_tests(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        try:
            await ctx.plane.run_tests(
                ctx.tenant_id,
                ctx.user_id,
                agent_id,
                version,
                request_id=_form_request_id(ctx),
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.version_page(request, ctx, agent_id, version, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        return _redirect(self.version_url(ctx, agent_id, version, "test-run"))

    async def case_from_feedback(self, request: Request, ctx: Ctx, agent_id: str):
        """The feedback page's "Make a test case": the row's question onto the next draft."""
        raw = ctx.get("question_seq")
        try:
            draft = await ctx.plane.make_test_case_from_feedback(
                ctx.tenant_id,
                ctx.user_id,
                agent_id,
                ctx.get("conversation_id"),
                int(raw) if raw.isdigit() else 0,
                request_id=_form_request_id(ctx),
            )
        except REFUSALS as err:
            return await self.failure(
                request,
                ctx,
                err,
                back=f"{self.base(ctx)}/agents/{quote(agent_id, safe='')}/feedback",
            )
        return _redirect(self.version_url(ctx, agent_id, draft.version, "case-added"))

    async def notion_down(
        self, request: Request, ctx: Ctx, agent_id: str, version: int
    ) -> Response:
        """The version page with a fixed message; 503 because a proxy hides a 502."""
        return await self.version_page(
            request, ctx, agent_id, version, page_status(502), NOTION_DOWN
        )

    async def check_grants(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        try:
            reports = await ctx.plane.check_version_grants(
                ctx.tenant_id, ctx.principal, agent_id, version
            )
        except GrantCheckUnavailable:
            return await self.notion_down(request, ctx, agent_id, version)
        return await self.version_page(request, ctx, agent_id, version, reports=reports)

    # --- Choose from Notion ---------------------------------------------------------

    def picker_flow(self, request: Request, ctx: Ctx, agent_id: str, version: int) -> Flow:
        parsed = read_session_cookie(request.cookies.get(SESSION_COOKIE))
        token = parsed[1] if parsed else ""
        return Flow(ctx.tenant_id, ctx.user_id, session_hash(token), agent_id, version)

    async def pick_start(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        picker = self.deps.picker
        if picker is None:
            return _message("Not available", "Choosing from Notion is not set up.", 404, ctx)
        await ctx.plane.require_agent_editor(ctx.tenant_id, ctx.principal, agent_id)
        flow = replace(
            self.picker_flow(request, ctx, agent_id, version),
            consent=ctx.get("consent") == "yes",
        )
        state = picker.sessions.begin(flow, ctx.now)
        return _redirect(notion_authorize_url(picker.config, self.deps.console.sign_in, state))

    async def notion_callback(self, request: Request) -> Response:
        """Notion sends the browser back here. The state must be one this session began."""
        picker = self.deps.picker
        session = await self.session_of(request)
        if picker is None or session is None:
            return _redirect("/login")
        now = self.deps.console.clock()
        parsed = read_session_cookie(request.cookies.get(SESSION_COOKIE))
        flow = picker.sessions.finish(request.query_params.get("state", ""), now)
        mine = (
            flow is not None
            and parsed is not None
            and flow.session_hash == session_hash(parsed[1])
            and flow.tenant_id == session.tenant_id
            and flow.user_id == session.user_id
        )
        if not mine or flow is None:
            return _message("Could not sign in", "That Notion sign-in is not valid.", 400)
        if flow.purpose == "connect":
            return await self.notion_connect_callback(request, flow)
        back = self.version_url_for(flow)
        code = request.query_params.get("code", "")
        if request.query_params.get("error") or not code:
            return _redirect(back)
        redirect_uri = picker.config.redirect_uri(self.deps.console.sign_in)
        try:
            if flow.consent:
                objects, token = await picker.oauth.list_and_keep(code, redirect_uri)
            else:
                objects = await picker.oauth.list_visible(code, redirect_uri)
        except NotionPickerError as err:
            log.warning("notion picker failed: %s", err)
            return _message(
                "Notion unavailable",
                "Couldn't list your Notion pages just now. Try again.",
                page_status(502),
            )
        if flow.consent:
            try:
                await self.deps.plane.store_creator_consent(flow.tenant_id, flow.user_id, token)
            except Forbidden as err:
                return _message("Not allowed", str(err), 403, back=back)
            except ConfigError as err:
                return _message("Not possible now", str(err), 409, back=back)
            except TenantKeyError:
                log.warning("notion consent refused tenant_id=%s: key unavailable", flow.tenant_id)
                return _message(
                    "Not available",
                    "This organisation's encryption key is not available yet. Try again in a moment.",
                    503,
                    back=back,
                )
        else:
            # The consent box is a per-pick choice: completing a re-pick with it unticked
            # revokes the sign-in a former pick kept. Nothing stored revokes nothing, and
            # a console that cannot keep sign-ins has nothing to revoke.
            try:
                if await self.deps.plane.creator_consent(flow.tenant_id, flow.user_id):
                    await self.deps.plane.revoke_creator_consent(flow.tenant_id, flow.user_id)
            except Forbidden as err:
                return _message("Not allowed", str(err), 403, back=back)
            except ConfigError as err:
                return _message("Not possible now", str(err), 409, back=back)
        picker.sessions.store(flow, objects, now)
        return _redirect(back + "/pick")

    def version_url_for(self, flow: Flow) -> str:
        return (
            f"/ui/{flow.tenant_id}/agents/{quote(flow.agent_id, safe='')}/versions/{flow.version}"
        )

    async def pick_page(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        await ctx.plane.require_agent_editor(ctx.tenant_id, ctx.principal, agent_id)
        picker = self.deps.picker
        flow = self.picker_flow(request, ctx, agent_id, version)
        objects = None if picker is None else picker.sessions.listing(flow, ctx.now)
        return _page(
            "pick.html",
            ctx=ctx,
            objects=objects,
            agent_id=agent_id,
            version=version,
            base=self.base(ctx),
            version_url=self.version_url(ctx, agent_id, version),
        )

    async def pick_choose(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        picker = self.deps.picker
        flow = self.picker_flow(request, ctx, agent_id, version)
        objects = None if picker is None else picker.sessions.listing(flow, ctx.now)
        if objects is None:
            return _message(
                "Choose again",
                "Your list from Notion has expired. Choose from Notion again.",
                409,
                ctx,
                back=self.version_url(ctx, agent_id, version),
            )
        # Only a position in the listing the creator's own token produced can be chosen.
        chosen = [obj for i, obj in enumerate(objects) if ctx.get(f"pick_{i}") == "yes"]
        await ctx.plane.record_grant_picks(
            ctx.tenant_id, ctx.user_id, agent_id, version, chosen, _form_request_id(ctx)
        )
        picker.sessions.drop(flow)
        return _redirect(self.version_url(ctx, agent_id, version, "picked"))

    async def pick_revoke(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        """Stop keeping the signed-in creator's Notion sign-in; the token is deleted."""
        try:
            changed = await ctx.plane.revoke_creator_consent(
                ctx.tenant_id, ctx.user_id, request_id=_form_request_id(ctx)
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.version_page(request, ctx, agent_id, version, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        if not changed:
            return await self.version_page(
                request,
                ctx,
                agent_id,
                version,
                409,
                "Nothing is stored to revoke: you have not kept your Notion sign-in.",
            )
        return _redirect(self.version_url(ctx, agent_id, version, "consent-revoked"))

    # --- Choose from Google Drive -----------------------------------------------------
    # The workspace's Drive account, not a per-creator sign-in: the picker lists what the
    # account can see and a click grants it. The admin's approval at publish is the check.

    async def drive_pick_page(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        await ctx.plane.require_agent_editor(ctx.tenant_id, ctx.principal, agent_id)
        back = self.version_url(ctx, agent_id, version)
        try:
            listing = await ctx.plane.drive_picker_listing(ctx.tenant_id, ctx.principal)
        except GoogleError as err:
            return _message("Google Drive unavailable", str(err), page_status(502), ctx, back=back)
        return _page(
            "drive_pick.html",
            ctx=ctx,
            listing=listing,
            agent_id=agent_id,
            version=version,
            base=self.base(ctx),
            version_url=self.version_url(ctx, agent_id, version),
        )

    async def drive_pick_choose(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        await ctx.plane.require_agent_editor(ctx.tenant_id, ctx.principal, agent_id)
        back = self.version_url(ctx, agent_id, version)
        try:
            listing = await ctx.plane.drive_picker_listing(ctx.tenant_id, ctx.principal)
        except GoogleError as err:
            return _message("Google Drive unavailable", str(err), page_status(502), ctx, back=back)
        # Only a position in the listing the workspace's account produced can be chosen.
        chosen = [item for i, item in enumerate(listing.items) if ctx.get(f"pick_{i}") == "yes"]
        sheets = [item for item in chosen if item.kind == "sheet"]
        others = [item for item in chosen if item.kind != "sheet"]
        if others:
            await ctx.plane.add_drive_grants(
                ctx.tenant_id, ctx.user_id, agent_id, version, others, _form_request_id(ctx)
            )
        if not sheets:
            return _redirect(self.version_url(ctx, agent_id, version, "drive-picked"))
        # A Sheet is granted for one tab: the next step reads its tabs and asks which one.
        query = urlencode([("sheet", item.id) for item in sheets])
        return _redirect(f"{back}/drive/pick/tabs?{query}")

    async def drive_tab_page(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        """The tab step: each chosen Sheet with its live tab titles, one pick per Sheet."""
        await ctx.plane.require_agent_editor(ctx.tenant_id, ctx.principal, agent_id)
        back = self.version_url(ctx, agent_id, version)
        try:
            listing = await ctx.plane.drive_picker_listing(ctx.tenant_id, ctx.principal)
            wanted = request.query_params.getlist("sheet")
            sheets = []
            for item in listing.items:
                if item.kind != "sheet" or item.id not in wanted:
                    continue
                tabs = await ctx.plane.drive_sheet_tabs(ctx.tenant_id, ctx.principal, item.id)
                sheets.append({"item": item, "tabs": tabs})
        except GoogleError as err:
            return _message("Google Drive unavailable", str(err), page_status(502), ctx, back=back)
        if not sheets:
            return _message(
                "Choose again",
                "Pick a Sheet from the Drive list first.",
                409,
                ctx,
                back=back,
            )
        return _page(
            "drive_tabs.html",
            ctx=ctx,
            sheets=sheets,
            agent_id=agent_id,
            version=version,
            base=self.base(ctx),
            version_url=back,
        )

    async def drive_tab_choose(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        await ctx.plane.require_agent_editor(ctx.tenant_id, ctx.principal, agent_id)
        back = self.version_url(ctx, agent_id, version)
        try:
            listing = await ctx.plane.drive_picker_listing(ctx.tenant_id, ctx.principal)
        except GoogleError as err:
            return _message("Google Drive unavailable", str(err), page_status(502), ctx, back=back)
        by_id = {item.id: item for item in listing.items if item.kind == "sheet"}
        chosen: list[DriveItem] = []
        i = 0
        while sheet_id := ctx.get(f"sheet_id_{i}"):
            item = by_id.get(sheet_id)
            if item is None:
                return _message(
                    "Choose again",
                    "That Sheet is no longer in the workspace's Drive list. Choose from "
                    "Google Drive again.",
                    409,
                    ctx,
                    back=back,
                )
            try:
                tabs = await ctx.plane.drive_sheet_tabs(ctx.tenant_id, ctx.principal, sheet_id)
            except GoogleError as err:
                return _message(
                    "Google Drive unavailable", str(err), page_status(502), ctx, back=back
                )
            # Only a position in the tab list the account's read produced can be chosen.
            position = ctx.get(f"tab_{i}")
            if not position.isdigit() or int(position) >= len(tabs):
                return _message("Choose again", "Pick a tab for each Sheet.", 409, ctx, back=back)
            chosen.append(
                DriveItem(
                    id=item.id,
                    name=item.name,
                    link=item.link,
                    kind="sheet",
                    tab=tabs[int(position)],
                )
            )
            i += 1
        if chosen:
            await ctx.plane.add_drive_grants(
                ctx.tenant_id, ctx.user_id, agent_id, version, chosen, _form_request_id(ctx)
            )
        return _redirect(self.version_url(ctx, agent_id, version, "drive-picked"))

    async def drive_template_page(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        """The template step: pick a granted Doc as the template and a folder as the
        destination of the new Docs (D-49). Only Docs and folders the account sees are offered."""
        await ctx.plane.require_agent_editor(ctx.tenant_id, ctx.principal, agent_id)
        back = self.version_url(ctx, agent_id, version)
        try:
            listing = await ctx.plane.drive_picker_listing(ctx.tenant_id, ctx.principal)
        except GoogleError as err:
            return _message("Google Drive unavailable", str(err), page_status(502), ctx, back=back)
        templates = [item for item in listing.items if item.mime_type == MIME_DOC]
        folders = [item for item in listing.items if item.kind == "folder"]
        return _page(
            "drive_template.html",
            ctx=ctx,
            templates=templates,
            folders=folders,
            agent_id=agent_id,
            version=version,
            base=self.base(ctx),
            version_url=back,
        )

    async def drive_template_choose(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        await ctx.plane.require_agent_editor(ctx.tenant_id, ctx.principal, agent_id)
        back = self.version_url(ctx, agent_id, version)
        try:
            listing = await ctx.plane.drive_picker_listing(ctx.tenant_id, ctx.principal)
        except GoogleError as err:
            return _message("Google Drive unavailable", str(err), page_status(502), ctx, back=back)
        # Only a position in the listing the workspace's account produced can be chosen.
        template = next(
            (i for i in listing.items if i.id == ctx.get("template") and i.mime_type == MIME_DOC),
            None,
        )
        folder = next(
            (i for i in listing.items if i.id == ctx.get("folder") and i.kind == "folder"),
            None,
        )
        if template is None or folder is None:
            return _message(
                "Choose again",
                "Pick a template Doc and a destination folder from the Drive list.",
                409,
                ctx,
                back=back,
            )
        chosen = [
            DriveItem(
                id=template.id,
                name=template.name,
                link=template.link,
                kind="template",
                folder_id=folder.id,
            )
        ]
        await ctx.plane.add_drive_grants(
            ctx.tenant_id, ctx.user_id, agent_id, version, chosen, _form_request_id(ctx)
        )
        return _redirect(self.version_url(ctx, agent_id, version, "drive-picked"))

    async def drive_remove(self, request: Request, ctx: Ctx, agent_id: str, version: int):
        await ctx.plane.require_agent_editor(ctx.tenant_id, ctx.principal, agent_id)
        await ctx.plane.remove_drive_grant(
            ctx.tenant_id,
            ctx.user_id,
            agent_id,
            version,
            ctx.get("grant_id"),
            _form_request_id(ctx),
        )
        return _redirect(self.version_url(ctx, agent_id, version, "drive-removed"))

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
        extra = {}
        if action == "publish":
            extra["confirm_audience"] = _audience_confirmation(ctx)
            # The hash of the version the approver's page showed; the plane refuses a
            # different one. A form without it is refused as a changed version.
            extra["reviewed_sha256"] = ctx.get("definition_sha256").strip() or "none"
            extra["acknowledge_widening"] = ctx.get("widening") == "yes"
            extra["reviewed_published"] = ctx.get("reviewed_published").strip() or "unknown"
        try:
            await call(
                ctx.tenant_id,
                ctx.user_id,
                agent_id,
                version,
                request_id=_form_request_id(ctx),
                **extra,
            )
        except (AudienceError, WideningNotAcknowledged, GrantNotPicked) as err:
            status = page_status(status_of(err) or 409)
            return await self.version_page(
                request, ctx, agent_id, version, status, _console_refusal(err) or str(err)
            )
        except GrantProblems as err:
            return await self.version_page(
                request, ctx, agent_id, version, 409, str(err), reports=err.reports
            )
        except GrantCheckUnavailable:
            return await self.notion_down(request, ctx, agent_id, version)
        except VersionChanged:
            return _redirect(self.version_url(ctx, agent_id, version) + "?refused=version-changed")
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
            ctx.tenant_id,
            ctx.user_id,
            ctx.get("user_id").strip(),
            ctx.get("role"),
            request_id=_form_request_id(ctx),
        )
        return _redirect(self.base(ctx) + "/members?done=member-added")

    async def change_role(self, request: Request, ctx: Ctx, user_id: str) -> Response:
        await ctx.plane.change_role(
            ctx.tenant_id, ctx.user_id, user_id, ctx.get("role"), request_id=_form_request_id(ctx)
        )
        return _redirect(self.base(ctx) + "/members?done=member-role")

    async def drop_member(self, request: Request, ctx: Ctx, user_id: str) -> Response:
        await ctx.plane.drop_member(
            ctx.tenant_id, ctx.user_id, user_id, request_id=_form_request_id(ctx)
        )
        return _redirect(self.base(ctx) + "/members?done=member-removed")

    async def audit(self, request: Request, ctx: Ctx) -> Response:
        raw = request.query_params.get("before", "")
        before = int(raw) if raw.isdigit() else None
        events = await ctx.plane.audit_log(
            ctx.tenant_id, ctx.user_id, AUDIT_PAGE, before_seq=before
        )
        older = events[-1].seq if len(events) == AUDIT_PAGE else None
        return _page("audit.html", ctx=ctx, events=events, older=older, base=self.base(ctx))

    async def usage(self, request: Request, ctx: Ctx) -> Response:
        month = request.query_params.get("month") or None
        report = await ctx.plane.usage_report(ctx.tenant_id, ctx.principal, month)
        budgets = await ctx.plane.budgets(ctx.tenant_id, ctx.principal)
        return _page(
            "usage.html",
            ctx=ctx,
            report=report,
            budgets=budgets,
            notice=self.notice(request),
            months=recent_months(ctx.now),
            no_team=NO_TEAM,
            base=self.base(ctx),
        )

    async def set_budget(self, request: Request, ctx: Ctx) -> Response:
        """An empty agent field is the tenant-wide budget."""
        agent_id = ctx.get("agent_id").strip() or None
        await ctx.plane.set_budget(
            ctx.tenant_id,
            ctx.user_id,
            agent_id,
            ctx.get("amount"),
            request_id=_form_request_id(ctx),
        )
        return _redirect(self.base(ctx) + "/usage?done=budget-set")

    async def remove_budget(self, request: Request, ctx: Ctx) -> Response:
        agent_id = ctx.get("agent_id").strip() or None
        await ctx.plane.remove_budget(
            ctx.tenant_id, ctx.user_id, agent_id, request_id=_form_request_id(ctx)
        )
        return _redirect(self.base(ctx) + "/usage?done=budget-removed")

    async def switches(
        self, request: Request, ctx: Ctx, status: int = 200, error: str | None = None
    ) -> Response:
        """The kill switches, for admins. A creator reads an agent's state on its own page."""
        if not ctx.is_admin:
            raise Forbidden(f"'{ctx.user_id}' may not do this in tenant '{ctx.tenant_id}'")
        return _page(
            "switches.html",
            status,
            ctx=ctx,
            view=await ctx.plane.switches(ctx.tenant_id, ctx.principal),
            needs_confirm=CONFIRMED_SWITCHES,
            error=error,
            notice=self.notice(request),
            base=self.base(ctx),
        )

    async def turn_off(self, request: Request, ctx: Ctx) -> Response:
        """Turning off every agent or all writes also needs the confirm box ticked.

        Only an admin gets the confirm step; anyone else goes straight to the control
        plane, which refuses them."""
        scope = ctx.get("scope")
        if ctx.is_admin and scope in CONFIRMED_SWITCHES and ctx.get("confirm") != "yes":
            what = "every agent of the organisation" if scope == "all_agents" else "every write"
            return await self.switches(
                request, ctx, 422, f"Tick the box to confirm: this stops {what}."
            )
        await ctx.plane.turn_off(
            ctx.tenant_id,
            ctx.user_id,
            scope,
            ctx.get("target").strip(),
            request_id=_form_request_id(ctx),
        )
        return _redirect(self.base(ctx) + "/switches?done=switch-off")

    async def turn_on(self, request: Request, ctx: Ctx) -> Response:
        await ctx.plane.turn_on(
            ctx.tenant_id,
            ctx.user_id,
            ctx.get("scope"),
            ctx.get("target").strip(),
            request_id=_form_request_id(ctx),
        )
        return _redirect(self.base(ctx) + "/switches?done=switch-on")

    # --- tenant settings --------------------------------------------------------

    async def settings(self, request: Request, ctx: Ctx) -> Response:
        return await self.settings_page(request, ctx, notice=self.notice(request))

    async def settings_page(
        self,
        request: Request,
        ctx: Ctx,
        status: int = 200,
        error: str | None = None,
        notice: str | None = None,
        webhook_token: str | None = None,
    ) -> Response:
        """The admin settings page. ``webhook_token`` is shown once, after a reveal."""
        view = await ctx.plane.tenant_settings(ctx.tenant_id, ctx.principal)
        return _page(
            "settings.html",
            status,
            ctx=ctx,
            view=view,
            picker_enabled=self.deps.picker is not None,
            webhook_token=webhook_token,
            error=error,
            notice=notice,
            base=self.base(ctx),
        )

    async def set_routing(self, request: Request, ctx: Ctx) -> Response:
        """The routing form: a select per mapped channel, one row to add a mapping."""
        try:
            channels = {
                name[len("route_") :]: value
                for name, value in ctx.form.items()
                if name.startswith("route_") and value
            }
            new_channel = ctx.get("new_channel").strip()
            if new_channel:
                new_agent = ctx.get("new_agent").strip()
                if not new_agent:
                    raise ConfigError("pick an agent for the channel you are adding")
                if new_channel in channels:
                    raise ConfigError(
                        f"channel '{new_channel}' is already mapped; change it in the table"
                    )
                channels[new_channel] = new_agent
            await ctx.plane.set_tenant_routing(
                ctx.tenant_id,
                ctx.user_id,
                ctx.get("default_agent").strip() or None,
                channels,
                request_id=_form_request_id(ctx),
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.settings_page(request, ctx, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        return _redirect(self.base(ctx) + "/settings?done=routing-saved")

    async def set_approval_policy(self, request: Request, ctx: Ctx) -> Response:
        """One low-risk action's approval switch; the control plane checks the role."""
        try:
            await ctx.plane.set_approval_policy(
                ctx.tenant_id,
                ctx.user_id,
                ctx.get("action").strip(),
                ctx.get("policy").strip(),
                request_id=_form_request_id(ctx),
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.settings_page(request, ctx, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        return _redirect(self.base(ctx) + "/settings?done=approval-policy-saved")

    async def notion_connect_start(self, request: Request, ctx: Ctx) -> Response:
        """Begin Connect Notion: the picker's state mechanism, purpose ``connect``."""
        picker = self.deps.picker
        if picker is None:
            return _message(
                "Not available", "Connecting Notion is not set up on this console.", 404, ctx
            )
        await ctx.plane.require_settings_admin(ctx.tenant_id, ctx.principal)
        parsed = read_session_cookie(request.cookies.get(SESSION_COOKIE))
        token = parsed[1] if parsed else ""
        flow = Flow(ctx.tenant_id, ctx.user_id, session_hash(token), "", 0, purpose="connect")
        state = picker.sessions.begin(flow, ctx.now)
        return _redirect(
            notion_authorize_url(picker.config, self.deps.console.sign_in, state, owner="workspace")
        )

    async def notion_connect_callback(self, request: Request, flow: Flow) -> Response:
        """Notion sent the browser back from a workspace authorization."""
        back = f"/ui/{quote(flow.tenant_id, safe='')}/settings"
        query = request.query_params
        code = query.get("code", "")
        if query.get("error") or not code or len(code) > 1024:
            return _redirect(back)
        picker = self.deps.picker
        try:
            grant = await picker.oauth.connect_workspace(
                code, picker.config.redirect_uri(self.deps.console.sign_in)
            )
        except NotionPickerError as err:
            log.warning("notion connect failed tenant_id=%s: %s", flow.tenant_id, err)
            return _message(
                "Notion unavailable",
                "Couldn't connect Notion just now. Nothing was changed; try again.",
                page_status(502),
                back=back,
            )
        try:
            await self.deps.plane.connect_notion(flow.tenant_id, flow.user_id, grant)
        except Forbidden as err:
            return _message("Not allowed", str(err), 403, back=back)
        except ConfigError as err:
            return _message("Not possible now", str(err), 409, back=back)
        except TenantKeyError as err:
            log.warning("notion connect refused tenant_id=%s: %s", flow.tenant_id, err)
            return _message(
                "Not available",
                "This organisation's encryption key is not available yet. Try again in a moment.",
                503,
                back=back,
            )
        return _redirect(back + "?done=notion-connected")

    async def notion_disconnect(self, request: Request, ctx: Ctx) -> Response:
        try:
            changed = await ctx.plane.disconnect_notion(
                ctx.tenant_id, ctx.user_id, request_id=_form_request_id(ctx)
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.settings_page(request, ctx, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        if not changed:
            return await self.settings_page(
                request,
                ctx,
                409,
                "Nothing is stored to disconnect: Notion is not connected in the console. "
                "The environment variable, if set, is unchanged.",
            )
        return _redirect(self.base(ctx) + "/settings?done=notion-disconnected")

    async def drive_connect(self, request: Request, ctx: Ctx) -> Response:
        """Connect Google Drive: the control plane creates the tenant's account and checks
        the role; a Google failure shows on the settings page with nothing stored."""
        try:
            await ctx.plane.connect_drive(
                ctx.tenant_id, ctx.user_id, request_id=_form_request_id(ctx)
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.settings_page(request, ctx, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        return _redirect(self.base(ctx) + "/settings?done=drive-connected")

    async def drive_disconnect(self, request: Request, ctx: Ctx) -> Response:
        try:
            changed = await ctx.plane.disconnect_drive(
                ctx.tenant_id, ctx.user_id, request_id=_form_request_id(ctx)
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.settings_page(request, ctx, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        if not changed:
            return await self.settings_page(
                request, ctx, 409, "Nothing is stored to disconnect: Google Drive is not connected."
            )
        return _redirect(self.base(ctx) + "/settings?done=drive-disconnected")

    async def webhook_reveal(self, request: Request, ctx: Ctx) -> Response:
        try:
            token = await ctx.plane.reveal_webhook_token(
                ctx.tenant_id, ctx.user_id, request_id=_form_request_id(ctx)
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.settings_page(request, ctx, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        return await self.settings_page(request, ctx, webhook_token=token)

    async def webhook_confirm(self, request: Request, ctx: Ctx) -> Response:
        try:
            await ctx.plane.confirm_webhook_token(
                ctx.tenant_id, ctx.user_id, request_id=_form_request_id(ctx)
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.settings_page(request, ctx, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        return _redirect(self.base(ctx) + "/settings?done=webhook-confirmed")

    async def webhook_clear(self, request: Request, ctx: Ctx) -> Response:
        try:
            await ctx.plane.clear_webhook_token(
                ctx.tenant_id, ctx.user_id, request_id=_form_request_id(ctx)
            )
        except REFUSALS as err:

            async def again(status: int, message: str) -> Response:
                return await self.settings_page(request, ctx, status, message)

            return await self.failure(request, ctx, err, rerender=again)
        return _redirect(self.base(ctx) + "/settings?done=webhook-cleared")

    async def console_js(self, request: Request) -> Response:
        """The one script the pages use (the Copy button). Served from our own origin,
        which the Content-Security-Policy allows; the pages carry no inline script."""
        return Response(CONSOLE_JS, media_type="application/javascript; charset=utf-8")

    async def usage_csv(self, request: Request, ctx: Ctx) -> Response:
        """The page's aggregates as a download. It only reads, so it is a GET."""
        month = request.query_params.get("month") or None
        report = await ctx.plane.usage_report(ctx.tenant_id, ctx.principal, month)
        return Response(
            report.csv_text(),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="usage-{report.month}.csv"'},
        )


NOTION_DOWN = "Couldn't reach Notion just now; nothing was changed. Try again."

# The Copy button on the settings page: copy the named element's text, or select it
# when the clipboard is not available (plain http on a non-loopback host).
CONSOLE_JS = """\
document.addEventListener("click", function (event) {
  var button = event.target.closest("[data-copy]");
  if (!button) { return; }
  var source = document.getElementById(button.getAttribute("data-copy"));
  if (!source) { return; }
  if (navigator.clipboard && window.isSecureContext) {
    navigator.clipboard.writeText(source.textContent).then(function () {
      button.textContent = "Copied";
    });
  } else {
    var range = document.createRange();
    range.selectNodeContents(source);
    var selection = window.getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
  }
});
"""

_TITLES = {
    403: "Not allowed",
    404: "Not found",
    409: "Not possible now",
    422: "Invalid",
    503: "Notion could not be reached",
}


def _console_refusal(err: Exception) -> str | None:
    """The console's own sentence for a refused audience confirmation, or None.

    Rendered from the refusal's structured counts and place, never by rewriting the
    API's message; every other refusal keeps the control plane's text.
    """
    if not isinstance(err, AudienceNotConfirmed) or err.counts is None:
        return None
    c = err.counts
    people = f"{c['total']} people: {c['guests']} guests, {c['external']} from other workspaces"
    if err.place == "channel":
        box = "I confirm these people may see what this agent says here"
        action = "resume"
        who = "The people in this channel"
    else:
        box = "I confirm these people may see what this agent can access"
        action = "publish"
        who = "The people who can reach this agent"
    if err.changed:
        return (
            f"{who} changed since they were counted ({people}). "
            f"Review them, tick '{box}' and {action} again."
        )
    return f"Tick '{box}' ({people}) and {action} again."


def _audience_confirmation(ctx: Ctx) -> dict[str, int] | None:
    """The counts the approver ticked the box for, or None when the box is not ticked.

    A count that is not a plain number is treated as no confirmation.
    """
    if ctx.get("confirm") != "yes":
        return None
    counts = {name: ctx.get(name).strip() for name in ("total", "guests", "external")}
    if not all(value.isdigit() and len(value) < 9 for value in counts.values()):
        return None
    return {name: int(value) for name, value in counts.items()}


def _form_request_id(ctx: Ctx) -> str | None:
    """The audit nonce for a form that carries a one-time token, or None without one.

    The token is made when the page is rendered, so a double click or a resent form is one
    request. It is hashed with the signed-in user, so no token is another user's.
    """
    token = ctx.get("request_id").strip()
    if not (token.isalnum() and 16 <= len(token) <= 64):
        return None
    return hashlib.sha256(f"{ctx.user_id}\n{token}".encode()).hexdigest()


def _same(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode(), b.encode())


def install_web_ui(app: FastAPI) -> None:
    WebUI(app)
