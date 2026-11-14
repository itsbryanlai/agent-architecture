"""Sign in with Slack (OpenID Connect) for the web console, and the session identity.

The flow. ``/login`` sends the browser to Slack with a random ``state`` and
``nonce``. Both are also set in a short-lived cookie that only this browser holds
and only the callback path receives, so no server-side state is needed before a
tenant is known (a session row is scoped to a tenant, and the tenant is not known
until Slack answers). The callback checks the returned ``state`` against that
cookie, exchanges the code for tokens at Slack's token endpoint over TLS with the
client secret, and reads the user and workspace from the ``id_token`` that endpoint
returned. The token is not signature-checked, which OpenID Connect Core 3.1.3.7
allows for an ID token received directly from the token endpoint over TLS; its
``iss``, ``aud``, ``exp`` and ``nonce`` are checked. No JWT library is used for
that reason: one would be a second trust path with nothing to verify against.

The workspace must map to a tenant, and the user must hold a role in it; only
then is a session stored. The session cookie is ``<tenant>.<session ID>``: the
tenant names the row-level-security scope for the lookup and is no claim, since
the stored row must name the same tenant.

``SlackSession`` is the ``IdentityProvider`` for a console process: the acting user
is the one the session row names, and the session's tenant must be the tenant in
the path.
"""

from __future__ import annotations

import base64
import binascii
import hmac
import json
import logging
import re
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import urlencode, urlsplit

import httpx
from fastapi import HTTPException, Request

from agentos.config import TENANT_ID_PATTERN, ConfigError, Settings

log = logging.getLogger("agentos.signin")

AUTHORIZE_URL = "https://slack.com/openid/connect/authorize"
TOKEN_URL = "https://slack.com/api/openid.connect.token"
ISSUER = "https://slack.com"
USER_CLAIM = "https://slack.com/user_id"
TEAM_CLAIM = "https://slack.com/team_id"

SESSION_COOKIE = "agentos_session"
LOGIN_COOKIE = "agentos_login"
LOGIN_COOKIE_PATH = "/auth/slack"
LOGIN_COOKIE_SECONDS = 600
CALLBACK_PATH = "/auth/slack/callback"

_LOOPBACK_NAMES = ("127.0.0.1", "localhost")
_TOKEN = re.compile(r"[A-Za-z0-9_-]{16,128}")


class SlackSignInError(Exception):
    """Sign-in failed. The message is for the log; the page says nothing of the cause."""


@dataclass(frozen=True)
class SignInConfig:
    client_id: str
    client_secret: str = field(repr=False)
    console_url: str  # the public base URL, with no trailing slash

    @property
    def redirect_uri(self) -> str:
        return f"{self.console_url}{CALLBACK_PATH}"

    @property
    def host(self) -> str:
        """The ``Host`` header value of this URL: host, and port when the URL has one."""
        return urlsplit(self.console_url).netloc

    @property
    def origin(self) -> str:
        return self.console_url

    @property
    def secure_cookies(self) -> bool:
        """Cookies are ``Secure`` on https. Only a loopback http URL turns that off."""
        return self.console_url.startswith("https://")


def _parse_console_url(raw: str) -> str:
    url = raw.strip()
    parts = urlsplit(url)
    if parts.scheme not in ("https", "http") or not parts.hostname:
        raise ConfigError("AGENTOS_CONSOLE_URL must be an https:// URL")
    if parts.path not in ("", "/") or parts.query or parts.fragment or parts.username:
        raise ConfigError("AGENTOS_CONSOLE_URL must be a base URL: scheme, host and port only")
    if parts.scheme == "http":
        try:
            port = parts.port
        except ValueError as err:
            raise ConfigError("AGENTOS_CONSOLE_URL has an invalid port") from err
        if parts.hostname not in _LOOPBACK_NAMES or port is None:
            raise ConfigError(
                "AGENTOS_CONSOLE_URL must be https://, except http://127.0.0.1:<port> "
                "or http://localhost:<port> for a local trial"
            )
    return f"{parts.scheme}://{parts.netloc}"


def sign_in_from_settings(settings: Settings) -> SignInConfig | None:
    """The sign-in configuration, None when it is not set at all.

    Setting only some of the three variables, or a URL that is not allowed, is a
    ``ConfigError``: a half-configured console must not start as a loopback-only
    one without saying so.
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
        raise ConfigError(f"sign-in needs all of its settings; not set: {', '.join(missing)}")
    return SignInConfig(
        client_id=settings.slack_client_id or "",
        client_secret=settings.slack_client_secret or "",
        console_url=_parse_console_url(settings.console_url or ""),
    )


# --- talking to Slack -----------------------------------------------------------------


class SlackOidc(Protocol):
    async def exchange_code(self, code: str, redirect_uri: str) -> dict[str, Any]:
        """Trade an authorization code for Slack's token response (with ``id_token``).

        Raises ``SlackSignInError`` when Slack refuses the code or cannot be reached.
        """
        ...


class HttpSlackOidc:
    """The real client: one POST to Slack's token endpoint, over TLS."""

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
            "grant_type": "authorization_code",
        }
        try:
            async with httpx.AsyncClient(timeout=10.0, transport=self._transport) as http:
                response = await http.post(TOKEN_URL, data=form)
            body = response.json()
        except (httpx.HTTPError, ValueError) as err:
            raise SlackSignInError(f"token request failed: {type(err).__name__}") from err
        if response.status_code != 200 or not isinstance(body, dict) or body.get("ok") is not True:
            error = body.get("error") if isinstance(body, dict) else None
            raise SlackSignInError(f"token endpoint refused the code: {str(error)[:60]}")
        return body


# --- the id_token ---------------------------------------------------------------------


@dataclass(frozen=True)
class SlackIdentity:
    user_id: str
    team_id: str


def _claims(id_token: object) -> dict[str, Any]:
    if not isinstance(id_token, str):
        raise SlackSignInError("no id_token in the token response")
    parts = id_token.split(".")
    if len(parts) != 3:
        raise SlackSignInError("id_token is not a JWT")
    try:
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(padded))
    except (binascii.Error, ValueError) as err:
        raise SlackSignInError("id_token payload is unreadable") from err
    if not isinstance(claims, dict):
        raise SlackSignInError("id_token payload is not an object")
    return claims


def identity_from_token_response(
    response: dict[str, Any], config: SignInConfig, nonce: str, now: datetime
) -> SlackIdentity:
    """The user and workspace the ID token names, after checking issuer, audience,
    expiry and nonce. See the module docstring for why the signature is not checked."""
    claims = _claims(response.get("id_token"))
    if claims.get("iss") != ISSUER:
        raise SlackSignInError("id_token issuer is not Slack")
    audience = claims.get("aud")
    audiences = audience if isinstance(audience, list) else [audience]
    if config.client_id not in audiences:
        raise SlackSignInError("id_token audience is not this app")
    expires = claims.get("exp")
    if isinstance(expires, bool) or not isinstance(expires, int | float):
        raise SlackSignInError("id_token has no expiry")
    if expires <= now.timestamp():
        raise SlackSignInError("id_token has expired")
    got_nonce = claims.get("nonce")
    if not isinstance(got_nonce, str) or not hmac.compare_digest(got_nonce, nonce):
        raise SlackSignInError("id_token nonce does not match")
    user_id, team_id = claims.get(USER_CLAIM), claims.get(TEAM_CLAIM)
    if not isinstance(user_id, str) or not user_id or not isinstance(team_id, str) or not team_id:
        raise SlackSignInError("id_token names no user or workspace")
    return SlackIdentity(user_id, team_id)


def authorize_url(config: SignInConfig, state: str, nonce: str, team_id: str | None) -> str:
    query = {
        "response_type": "code",
        "scope": "openid profile",
        "client_id": config.client_id,
        "redirect_uri": config.redirect_uri,
        "state": state,
        "nonce": nonce,
    }
    if team_id:
        query["team"] = team_id
    return f"{AUTHORIZE_URL}?{urlencode(query)}"


def new_login_values() -> tuple[str, str]:
    """A fresh ``state`` and ``nonce``: 256 random bits each."""
    return secrets.token_urlsafe(32), secrets.token_urlsafe(32)


def login_cookie_value(state: str, nonce: str) -> str:
    return f"{state}.{nonce}"


def read_login_cookie(value: str | None) -> tuple[str, str] | None:
    if not value:
        return None
    parts = value.split(".")
    if len(parts) != 2 or not all(_TOKEN.fullmatch(p) for p in parts):
        return None
    return parts[0], parts[1]


# --- the session cookie and the identity ----------------------------------------------


def session_cookie_value(tenant_id: str, token: str) -> str:
    return f"{tenant_id}.{token}"


def read_session_cookie(value: str | None) -> tuple[str, str] | None:
    """The (tenant, session ID) a cookie names, or None when it has the wrong shape."""
    if not value:
        return None
    tenant_id, _, token = value.partition(".")
    if not TENANT_ID_PATTERN.fullmatch(tenant_id) or not _TOKEN.fullmatch(token):
        return None
    return tenant_id, token


@dataclass
class Console:
    """What a process needs to serve the web console: the sign-in settings, the
    Slack client and the clock. Tests pass a fake client and a settable clock."""

    sign_in: SignInConfig
    slack: SlackOidc
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)


class SlackSession:
    """The identity of a console process: the user of the request's live session.

    Used by the JSON routes, which name the tenant in the path. A request with no
    live session is 401. A live session of another tenant is 403.
    """

    async def actor(self, request: Request) -> str:
        deps = request.app.state.deps
        parsed = read_session_cookie(request.cookies.get(SESSION_COOKIE))
        if parsed is None or deps.console is None or deps.sessions is None:
            raise HTTPException(401, "sign in required")
        tenant_id, token = parsed
        session = await deps.sessions.get(tenant_id, token, deps.console.clock())
        if session is None:
            raise HTTPException(401, "sign in required")
        if session.tenant_id != request.path_params.get("tenant_id"):
            raise HTTPException(403, "this session belongs to another tenant")
        return session.user_id
