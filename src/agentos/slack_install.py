"""The Slack OAuth install: "Add to Slack" creates a working tenant with no manual step.

The flow. ``GET /install/slack`` (a public console page) renders the button and sets a
single-use ``state`` cookie that lives ten minutes. The button sends the browser to
Slack's OAuth v2 authorize with exactly the bot scopes of ``slack/app-manifest.yaml``.
``GET /install/slack/callback`` checks the returned ``state`` against that cookie,
exchanges the code at ``oauth.v2.access`` with the app's client ID and secret, and reads
``team.id``, the bot ``access_token`` and ``authed_user.id`` from the response. The pilot
allowlist (``AGENTOS_INSTALL_ALLOWED_TEAMS``) decides whether the workspace may install;
unset refuses every install.

Then, in one unit of work: the tenant row (its ID built from the team ID in ``ids.py``,
stable so a reinstall finds it), the tenant's payload key, the bot token encrypted under
that key, the installer as the first admin (the tenant row's ``admins`` list, like a YAML
tenant's), and one ``install`` audit event that never holds the token. A reinstall of a
known team replaces the token (audited as a reinstall), re-enables the tenant if an
uninstall disabled it, and creates nothing else: the settings, admins, data and key stay
as they are.

The pages are rendered by ``web_ui.py``; this module owns the configuration, the exchange
with Slack and the unit of work. An error names what went wrong (a Slack error code, the
allowlist refusal, a conflicting tenant) and never a token.
"""

from __future__ import annotations

import re
import secrets
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import urlencode, urlsplit

import httpx
import yaml

from agentos import ids
from agentos.audit import AuditStore
from agentos.config import (
    DEFAULT_NOTION_TOKEN_ENV,
    DEFAULT_NOTION_WEBHOOK_TOKEN_ENV,
    DEFAULT_SLACK_BOT_TOKEN_ENV,
    TENANT_ID_PATTERN,
    AgentsConfig,
    ConfigError,
    Settings,
    TenantSecrets,
)
from agentos.credentials import SLACK_BOT, TenantCredentials
from agentos.models import AuditEvent
from agentos.slack_signin import parse_console_url
from agentos.tenant_store import TenantStore
from agentos.unit_of_work import UnitOfWork

AUTHORIZE_URL = "https://slack.com/api/oauth.v2.authorize"
TOKEN_URL = "https://slack.com/api/oauth.v2.access"
INSTALL_PATH = "/install/slack"
INSTALL_CALLBACK_PATH = "/install/slack/callback"
INSTALL_COOKIE = "agentos_install"
INSTALL_COOKIE_SECONDS = 600
DEFAULT_MANIFEST_PATH = "slack/app-manifest.yaml"

_STATE = re.compile(r"[A-Za-z0-9_-]{16,128}")


class SlackInstallError(Exception):
    """The install failed. The message names the cause for the page; never a token."""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class InstallConfig:
    client_id: str
    client_secret: str = field(repr=False)
    console_url: str  # the public base URL, with no trailing slash
    allowed_teams: frozenset[str] = frozenset()
    scopes: tuple[str, ...] = ()

    @property
    def redirect_uri(self) -> str:
        return f"{self.console_url}{INSTALL_CALLBACK_PATH}"

    @property
    def host(self) -> str:
        return urlsplit(self.console_url).netloc

    @property
    def origin(self) -> str:
        return self.console_url

    @property
    def secure_cookies(self) -> bool:
        return self.console_url.startswith("https://")


def bot_scopes_from_manifest(path: str) -> tuple[str, ...]:
    """The bot scopes the install requests: exactly the manifest's, in its order."""
    try:
        with open(path, encoding="utf-8") as f:
            manifest = yaml.safe_load(f)
    except OSError as err:
        raise ConfigError(f"cannot read the Slack app manifest at {path}") from err
    scopes = ((manifest or {}).get("oauth_config") or {}).get("scopes") or {}
    bot = scopes.get("bot")
    if not isinstance(bot, list) or not bot or not all(isinstance(s, str) and s for s in bot):
        raise ConfigError(f"{path}: oauth_config.scopes.bot must be a non-empty list of scopes")
    return tuple(bot)


def app_name_from_manifest(path: str = DEFAULT_MANIFEST_PATH) -> str:
    """The app's name for the not-set-up reply: ``display_information.name``, else "This app".

    The manifest is already in the image (the install reads its scopes); a missing file
    or name falls back rather than refusing, since the reply still names the console.
    """
    try:
        with open(path, encoding="utf-8") as f:
            manifest = yaml.safe_load(f)
    except OSError:
        return "This app"
    name = ((manifest or {}).get("display_information") or {}).get("name")
    return name if isinstance(name, str) and name.strip() else "This app"


def install_from_settings(
    settings: Settings, manifest_path: str = DEFAULT_MANIFEST_PATH
) -> InstallConfig | None:
    """The install configuration, None when sign-in is not set at all.

    The install shares sign-in's three settings (the app's client ID and secret and the
    console's public URL); setting only some of them is a ``ConfigError``, as for sign-in.
    The allowlist may be empty: every install is then refused with a clear page.
    """
    values = {
        "SLACK_CLIENT_ID": settings.slack_client_id,
        "SLACK_CLIENT_SECRET": settings.slack_client_secret,
        "AGENTOS_CONSOLE_URL": settings.console_url,
    }
    if not any(values.values()):
        return None
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise ConfigError(f"the install needs all of its settings; not set: {', '.join(missing)}")
    return InstallConfig(
        client_id=settings.slack_client_id or "",
        client_secret=settings.slack_client_secret or "",
        console_url=parse_console_url(settings.console_url or ""),
        allowed_teams=frozenset(settings.install_allowed_teams),
        scopes=bot_scopes_from_manifest(manifest_path),
    )


def new_install_state() -> str:
    """A fresh single-use ``state``: 256 random bits."""
    return secrets.token_urlsafe(32)


def read_install_cookie(value: str | None) -> str | None:
    if not value or not _STATE.fullmatch(value):
        return None
    return value


def authorize_url(config: InstallConfig, state: str) -> str:
    query = {
        "client_id": config.client_id,
        "scope": ",".join(config.scopes),
        "redirect_uri": config.redirect_uri,
        "state": state,
    }
    return f"{AUTHORIZE_URL}?{urlencode(query)}"


# --- talking to Slack -----------------------------------------------------------------


class SlackOAuth(Protocol):
    async def exchange_code(self, code: str, redirect_uri: str) -> dict[str, Any]:
        """Trade an authorization code for Slack's OAuth v2 response (the bot token).

        Raises ``SlackInstallError`` when Slack refuses the code or cannot be reached.
        """
        ...


class HttpSlackOAuth:
    """The real client: one POST to Slack's OAuth v2 token endpoint, over TLS."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self._client_id = client_id
        self._client_secret = client_secret
        self._transport = transport

    async def exchange_code(self, code: str, redirect_uri: str) -> dict[str, Any]:
        form = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "code": code,
            "redirect_uri": redirect_uri,
        }
        try:
            async with httpx.AsyncClient(timeout=10.0, transport=self._transport) as http:
                response = await http.post(TOKEN_URL, data=form)
            body = response.json()
        except (httpx.HTTPError, ValueError) as err:
            raise SlackInstallError(
                f"Slack could not be reached: {type(err).__name__}", 503
            ) from err
        if response.status_code != 200 or not isinstance(body, dict) or body.get("ok") is not True:
            error = body.get("error") if isinstance(body, dict) else None
            raise SlackInstallError(f"Slack refused the install: {str(error)[:60]}", 503)
        return body


def grant_from_response(body: dict[str, Any]) -> tuple[str, str, str]:
    """The (team ID, bot token, installer user ID) Slack's response names.

    Raises ``SlackInstallError`` when any of the three is missing or the token is not a
    bot token. The message names no value.
    """
    team = body.get("team")
    authed = body.get("authed_user")
    team_id = team.get("id") if isinstance(team, dict) else None
    token = body.get("access_token")
    user_id = authed.get("id") if isinstance(authed, dict) else None
    if (
        body.get("token_type") != "bot"
        or not isinstance(token, str)
        or not token
        or not isinstance(team_id, str)
        or not team_id
        or not isinstance(user_id, str)
        or not user_id
    ):
        raise SlackInstallError("Slack's response names no bot token, workspace or installer", 503)
    return team_id, token, user_id


# --- the install itself ---------------------------------------------------------------


def installed_tenant_secrets(tenant_id: str) -> TenantSecrets:
    """Per-tenant fallback variable names for an installed tenant.

    The credential row supplies the Slack token, so these variables are normally unset;
    they exist so the tenant shares no default variable name with another tenant (which
    ``TenantsConfig.of`` refuses). The naming follows the payload-key variables:
    upper-cased, ``-`` turned to ``_``.
    """
    suffix = tenant_id.upper().replace("-", "_")
    return TenantSecrets(
        slack_bot_token_env=f"{DEFAULT_SLACK_BOT_TOKEN_ENV}_{suffix}",
        notion_token_env=f"{DEFAULT_NOTION_TOKEN_ENV}_{suffix}",
        notion_webhook_token_env=f"{DEFAULT_NOTION_WEBHOOK_TOKEN_ENV}_{suffix}",
    )


@dataclass(frozen=True)
class InstallResult:
    tenant_id: str
    team_id: str
    created: bool  # False for a reinstall: only the token was replaced


class Installer:
    """Creates the tenant of an allowed workspace, or replaces its bot token.

    A tenant ID that already serves another workspace, and a workspace that already
    belongs to another tenant ID, are both refused with 409: an install never takes
    over another tenant's row.

    ``ensure_key`` returns the tenant's (key version, key bytes), creating version 1 when
    it has none; production passes ``crypto_keys.tenant_key_material`` over the database
    pool and master keys, reading on the unit of work's connection so the key created
    inside the transaction can encrypt the token before it commits.
    """

    def __init__(
        self,
        config: InstallConfig,
        oauth: SlackOAuth,
        tenants: TenantStore,
        credentials: TenantCredentials,
        audit: AuditStore,
        uow: UnitOfWork,
        ensure_key: Callable[[str, Any], Awaitable[tuple[int, bytes]]],
    ):
        self.config = config
        self.oauth = oauth
        self._tenants = tenants
        self._credentials = credentials
        self._audit = audit
        self._uow = uow
        self._ensure_key = ensure_key

    async def install(self, team_id: str, bot_token: str, installer: str) -> InstallResult:
        if not self.config.allowed_teams:
            raise SlackInstallError("installing is not enabled on this console yet", 403)
        if team_id not in self.config.allowed_teams:
            raise SlackInstallError(f"workspace '{team_id}' is not on the pilot allowlist", 403)
        tenant_id = ids.tenant_for_slack_team(team_id)
        if not TENANT_ID_PATTERN.fullmatch(tenant_id):
            raise SlackInstallError(f"Slack workspace '{team_id}' cannot become a tenant ID")
        config = AgentsConfig(
            tenant_id=tenant_id,
            slack_team_id=team_id,
            agents={},
            default_agent=None,
            notion_workspace_id=None,
            secrets=installed_tenant_secrets(tenant_id),
            channels={},
            admins=(installer,),
        )
        invocation = uuid.uuid4().hex
        async with self._uow.begin(tenant_id) as conn:
            try:
                created = await self._tenants.insert(config, conn=conn)
            except ConfigError as err:
                # The workspace already belongs to another tenant ID.
                raise SlackInstallError(str(err), 409) from err
            if not created:
                existing = await self._tenants.load(tenant_id)
                if existing is None or existing.config.slack_team_id != team_id:
                    raise SlackInstallError(
                        f"tenant '{tenant_id}' already serves another Slack workspace; "
                        "the install was refused",
                        409,
                    )
                if not existing.enabled:
                    # A reinstall after an uninstall resumes the tenant: its data,
                    # agents, settings and key stayed; only the enable flag and the
                    # bot token were gone.
                    await self._tenants.set_enabled(tenant_id, True, conn=conn)
            key_version, key = await self._ensure_key(tenant_id, conn)
            await self._credentials.store_secret(
                tenant_id, SLACK_BOT, bot_token, key_version, key, conn=conn
            )
            await self._audit.record(
                AuditEvent(
                    tenant_id=tenant_id,
                    event_key=ids.audit_control_key(tenant_id, "install", invocation),
                    kind="install",
                    agent_id=None,
                    agent_version=None,
                    requester_user_id=installer,
                    identity=f"user:{installer}",
                    tool="install" if created else "reinstall",
                    target=team_id,
                ),
                conn=conn,
            )
        return InstallResult(tenant_id, team_id, created)
