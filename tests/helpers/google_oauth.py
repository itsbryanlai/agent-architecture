"""Google's OAuth token endpoint over a mocked httpx transport, for the sign-in as the
agent's Google account."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from urllib.parse import parse_qs

import httpx

CLIENT_ID = "google-client-id"
AGENT_EMAIL = "it-helper@acme.test"
REFRESH_TOKEN = "1//refresh-token-for-the-test"
# The sign-in's clock, fixed: an ID token's expiry is read against it.
NOW = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)
FAR_FUTURE = int(datetime(2100, 1, 1, tzinfo=UTC).timestamp())


def id_token(claims: dict) -> str:
    """An unsigned JWT: the sign-in trusts the token endpoint's answer, not a signature."""

    def part(value: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()

    return f"{part({'alg': 'none'})}.{part(claims)}.sig"


def google_transport(seen: dict, **overrides) -> httpx.MockTransport:
    """The token endpoint's answer to a code. ``overrides`` change the ID token's claims;
    a claim set to None is left out."""

    def handler(request: httpx.Request) -> httpx.Response:
        form = {k: v[0] for k, v in parse_qs(request.content.decode()).items()}
        if request.url.path == "/revoke":
            seen.setdefault("revoked", []).append(form["token"])
            return httpx.Response(200, json={})
        seen["form"] = form
        claims = {
            "iss": "https://accounts.google.com",
            "aud": CLIENT_ID,
            "exp": FAR_FUTURE,
            "email": AGENT_EMAIL,
            "email_verified": True,
            "hd": "acme.test",
        }
        claims.update(overrides)
        claims = {k: v for k, v in claims.items() if v is not None}
        return httpx.Response(
            200,
            json={
                "access_token": "ya29.access",
                "expires_in": 3599,
                "refresh_token": f"{REFRESH_TOKEN}:{form['code']}",
                "id_token": id_token(claims),
            },
        )

    return httpx.MockTransport(handler)
