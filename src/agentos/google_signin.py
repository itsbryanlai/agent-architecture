"""Sign in as the agent's Google account (D-53): connect Drive as a user in the customer's
Workspace instead of the deployment-created service account.

One OAuth client (``GOOGLE_OAUTH_CLIENT_ID`` / ``GOOGLE_OAUTH_CLIENT_SECRET``) serves every
tenant. The flow mirrors Connect Notion (``notion_picker``):

1. An admin presses "Sign in as the agent's Google account" on Settings. The console makes a
   random ``state`` bound to their console session and the tenant (single use, ten minutes,
   the picker's ``PickerSessions``) and sends the browser to Google, asking for offline access
   with the consent screen forced, so a refresh token is always returned.
2. Google sends the browser back to ``/auth/google/callback``. The callback needs the same
   console session and a ``state`` it issued, exchanges the code, and reads the ID token's
   claims. The token endpoint's answer over TLS is the trust anchor, as in Slack's sign-in
   of the console: the ID token's signature is not checked, but its issuer, audience, expiry,
   verified email and hosted domain (``hd``) are. An account with no ``hd`` is a personal
   Gmail, not the Workspace's agent user, and is refused.
3. The control plane stores the refresh token encrypted as the tenant's ``google_user``
   credential, with the connection row and its audit event. The token lives only in that
   request and the encrypted row; it is never logged.
"""

from __future__ import annotations

import base64
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol
from urllib.parse import urlencode

import httpx

from agentos.notion_picker import PickerSessions
from agentos.slack_signin import SignInConfig
from agentos.usage import utcnow

log = logging.getLogger("agentos.google_signin")

AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
REVOKE_URL = "https://oauth2.googleapis.com/revoke"
GOOGLE_CALLBACK_PATH = "/auth/google/callback"
SCOPES = ("openid", "email", "https://www.googleapis.com/auth/drive")
ISSUERS = ("https://accounts.google.com", "accounts.google.com")
NOT_WORKSPACE = (
    "Sign in with an account in your organisation's Google Workspace. Without Workspace, "
    "keep the agent's built-in Google address."
)


class GoogleSignInError(Exception):
    """The Google sign-in failed. The message is for the log, not the page."""


class NotWorkspaceAccount(GoogleSignInError):
    """The account has no hosted domain: a personal Google account."""


@dataclass(frozen=True)
class GoogleSignInConfig:
    client_id: str
    client_secret: str = field(repr=False)

    def redirect_uri(self, sign_in: SignInConfig) -> str:
        return f"{sign_in.console_url}{GOOGLE_CALLBACK_PATH}"


@dataclass(frozen=True)
class GoogleUserGrant:
    """What the sign-in returned: the agent user's address and its refresh token. The
    token stays out of ``repr``; it is stored encrypted and never logged."""

    email: str
    refresh_token: str = field(repr=False)


class GoogleOAuth(Protocol):
    async def connect_user(self, code: str, redirect_uri: str) -> GoogleUserGrant:
        """Trade the code for the signed-in user's refresh token and verified address.
        A refusal after the token was issued revokes it first, best effort."""
        ...

    async def revoke_user(self, refresh_token: str) -> None:
        """Revoke a refresh token at Google. Best effort: a refusal or a lost connection
        is logged by code and never raised."""
        ...


class HttpGoogleOAuth:
    def __init__(
        self,
        config: GoogleSignInConfig,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], datetime] = utcnow,
    ):
        self._config = config
        self._transport = transport
        self._clock = clock

    async def connect_user(self, code: str, redirect_uri: str) -> GoogleUserGrant:
        try:
            async with httpx.AsyncClient(timeout=15.0, transport=self._transport) as http:
                response = await http.post(
                    TOKEN_URL,
                    data={
                        "grant_type": "authorization_code",
                        "code": code,
                        "redirect_uri": redirect_uri,
                        "client_id": self._config.client_id,
                        "client_secret": self._config.client_secret,
                    },
                )
        except httpx.HTTPError as err:
            raise GoogleSignInError(f"Google request failed: {type(err).__name__}") from err
        try:
            body = response.json()
        except ValueError:
            body = None
        if response.status_code != 200 or not isinstance(body, dict):
            raise GoogleSignInError(f"token endpoint refused: {response.status_code}")
        refresh = body.get("refresh_token")
        if not isinstance(refresh, str) or not refresh:
            raise GoogleSignInError("token response carries no refresh token")
        try:
            claims = self._claims(body.get("id_token"))
            email = claims.get("email")
            if not isinstance(email, str) or not email or claims.get("email_verified") is not True:
                raise GoogleSignInError("ID token names no verified email")
            if not claims.get("hd"):
                raise NotWorkspaceAccount("the account has no hosted domain")
        except GoogleSignInError:
            # Google issued a sign-in this console will not keep: do not leave it valid.
            await self.revoke_user(refresh)
            raise
        return GoogleUserGrant(email=email, refresh_token=refresh)

    async def revoke_user(self, refresh_token: str) -> None:
        try:
            async with httpx.AsyncClient(timeout=15.0, transport=self._transport) as http:
                response = await http.post(REVOKE_URL, data={"token": refresh_token})
        except httpx.HTTPError as err:
            log.warning("google sign-in not revoked error_type=%s", type(err).__name__)
            return
        if response.status_code != 200:
            log.warning("google sign-in not revoked status=%s", response.status_code)

    def _claims(self, id_token: object) -> dict:
        """The ID token's claims, after the issuer, audience and expiry checks."""
        if not isinstance(id_token, str) or id_token.count(".") != 2:
            raise GoogleSignInError("token response carries no ID token")
        try:
            payload = id_token.split(".")[1]
            claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        except ValueError as err:
            raise GoogleSignInError("ID token is not readable") from err
        if not isinstance(claims, dict):
            raise GoogleSignInError("ID token is not readable")
        if claims.get("iss") not in ISSUERS:
            raise GoogleSignInError("ID token issuer is not Google")
        if claims.get("aud") != self._config.client_id:
            raise GoogleSignInError("ID token audience is not this client")
        exp = claims.get("exp")
        if not isinstance(exp, int | float) or exp <= self._clock().timestamp():
            raise GoogleSignInError("ID token is expired")
        return claims


def authorize_url(config: GoogleSignInConfig, sign_in: SignInConfig, state: str) -> str:
    query = {
        "client_id": config.client_id,
        "response_type": "code",
        "redirect_uri": config.redirect_uri(sign_in),
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    return f"{AUTHORIZE_URL}?{urlencode(query)}"


@dataclass
class GoogleSignIn:
    """What a console process needs to offer the sign-in as the agent's Google account."""

    config: GoogleSignInConfig
    oauth: GoogleOAuth
    sessions: PickerSessions = field(default_factory=PickerSessions)


def google_signin_from_settings(
    client_id: str | None, client_secret: str | None
) -> GoogleSignInConfig | None:
    """None when neither variable is set; a half-set pair is a configuration error."""
    if not client_id and not client_secret:
        return None
    if not client_id or not client_secret:
        from agentos.config import ConfigError

        raise ConfigError(
            "signing in as the agent's Google account needs both GOOGLE_OAUTH_CLIENT_ID "
            "and GOOGLE_OAUTH_CLIENT_SECRET"
        )
    return GoogleSignInConfig(client_id, client_secret)
