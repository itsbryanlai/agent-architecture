"""Choose from Notion: the creator's own Notion sign-in, used once to list what they can see.

A public Notion integration (``NOTION_OAUTH_CLIENT_ID`` / ``NOTION_OAUTH_CLIENT_SECRET``)
lets a creator sign in with their own Notion account and share pages and databases with it.
The flow:

1. The creator presses "Choose from Notion" on a draft. The console makes a random ``state``
   bound to their console session, the tenant, the agent and the version, and sends the
   browser to Notion.
2. Notion sends the browser back to ``/auth/notion/callback``. The callback needs the same
   console session and a ``state`` it issued (single use, ten minutes), exchanges the code for
   the creator's token, searches with that token, and drops the token. The token lives for
   the length of that request: it is a local variable, never stored, logged or put in a table.
3. What the search listed (IDs, kinds, titles) is kept in memory for the session so the
   creator can tick objects. Only an ID in that list can be picked, and a pick is recorded
   for the agent (``grant_picks``).

The listing is per process: with several console processes behind one address the callback
must reach the process that began the flow. The console runs as one process today.
"""

from __future__ import annotations

import base64
import logging
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Protocol
from urllib.parse import urlencode

import httpx

from agentos.grant_picks import PickedObject
from agentos.slack_signin import SignInConfig

log = logging.getLogger("agentos.notion_picker")

AUTHORIZE_URL = "https://api.notion.com/v1/oauth/authorize"
TOKEN_URL = "https://api.notion.com/v1/oauth/token"
SEARCH_URL = "https://api.notion.com/v1/search"
# Search results are databases and pages directly in this version; a later one returns data
# sources, whose IDs are not what a grant names.
NOTION_VERSION = "2022-06-28"
NOTION_CALLBACK_PATH = "/auth/notion/callback"
FLOW_LIFETIME = timedelta(minutes=10)
LISTING_LIFETIME = timedelta(minutes=30)
MAX_RESULTS = 200
MAX_PENDING = 500


class NotionPickerError(Exception):
    """The Notion sign-in or search failed. The message is for the log, not the page."""


@dataclass(frozen=True)
class PickerConfig:
    client_id: str
    client_secret: str = field(repr=False)

    def redirect_uri(self, sign_in: SignInConfig) -> str:
        return f"{sign_in.console_url}{NOTION_CALLBACK_PATH}"


class NotionOAuth(Protocol):
    async def list_visible(self, code: str, redirect_uri: str) -> list[PickedObject]:
        """Trade the code for the creator's token, list what it can search, discard it."""
        ...


class HttpNotionOAuth:
    def __init__(
        self,
        config: PickerConfig,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self._config = config
        self._transport = transport

    async def list_visible(self, code: str, redirect_uri: str) -> list[PickedObject]:
        basic = base64.b64encode(
            f"{self._config.client_id}:{self._config.client_secret}".encode()
        ).decode()
        try:
            async with httpx.AsyncClient(timeout=15.0, transport=self._transport) as http:
                token_response = await http.post(
                    TOKEN_URL,
                    headers={"Authorization": f"Basic {basic}", "Notion-Version": NOTION_VERSION},
                    json={
                        "grant_type": "authorization_code",
                        "code": code,
                        "redirect_uri": redirect_uri,
                    },
                )
                body = token_response.json()
                token = body.get("access_token") if isinstance(body, dict) else None
                if token_response.status_code != 200 or not isinstance(token, str) or not token:
                    raise NotionPickerError(f"token endpoint refused: {token_response.status_code}")
                return await self._search(http, token)
        except (httpx.HTTPError, ValueError) as err:
            raise NotionPickerError(f"Notion request failed: {type(err).__name__}") from err

    @staticmethod
    async def _search(http: httpx.AsyncClient, token: str) -> list[PickedObject]:
        found: list[PickedObject] = []
        cursor = None
        while len(found) < MAX_RESULTS:
            payload: dict = {"page_size": 100}
            if cursor:
                payload["start_cursor"] = cursor
            response = await http.post(
                SEARCH_URL,
                headers={"Authorization": f"Bearer {token}", "Notion-Version": NOTION_VERSION},
                json=payload,
            )
            if response.status_code != 200:
                raise NotionPickerError(f"search refused: {response.status_code}")
            body = response.json()
            for item in body.get("results", []):
                kind = item.get("object")
                if kind not in ("database", "page") or not item.get("id") or item.get("in_trash"):
                    continue
                found.append(PickedObject(item["id"], kind, _title_of(item)))
            if not body.get("has_more"):
                break
            cursor = body.get("next_cursor")
        return found[:MAX_RESULTS]


def _title_of(item: dict) -> str:
    if item.get("object") == "database":
        rich = item.get("title", [])
    else:
        rich = next(
            (
                p.get("title", [])
                for p in (item.get("properties") or {}).values()
                if p.get("type") == "title"
            ),
            [],
        )
    return "".join(r.get("plain_text", "") for r in rich)[:200]


def authorize_url(config: PickerConfig, sign_in: SignInConfig, state: str) -> str:
    query = {
        "client_id": config.client_id,
        "response_type": "code",
        "owner": "user",
        "redirect_uri": config.redirect_uri(sign_in),
        "state": state,
    }
    return f"{AUTHORIZE_URL}?{urlencode(query)}"


@dataclass(frozen=True)
class Flow:
    tenant_id: str
    user_id: str
    session_hash: str
    agent_id: str
    version: int


@dataclass
class _Listing:
    objects: list[PickedObject]
    expires: datetime


class PickerSessions:
    """In-memory state of picker flows and listings. Holds no Notion token."""

    def __init__(self) -> None:
        self._flows: dict[str, tuple[Flow, datetime]] = {}
        self._listings: dict[Flow, _Listing] = {}

    def begin(self, flow: Flow, now: datetime) -> str:
        self._expire(now)
        if len(self._flows) >= MAX_PENDING:
            self._flows.pop(next(iter(self._flows)))
        state = secrets.token_urlsafe(32)
        self._flows[state] = (flow, now + FLOW_LIFETIME)
        return state

    def finish(self, state: str, now: datetime) -> Flow | None:
        """The flow ``state`` began, once; None for an unknown, used or expired state."""
        self._expire(now)
        entry = self._flows.pop(state, None)
        return None if entry is None else entry[0]

    def store(self, flow: Flow, objects: list[PickedObject], now: datetime) -> None:
        self._listings[flow] = _Listing(objects, now + LISTING_LIFETIME)

    def listing(self, flow: Flow, now: datetime) -> list[PickedObject] | None:
        self._expire(now)
        found = self._listings.get(flow)
        return None if found is None else found.objects

    def drop(self, flow: Flow) -> None:
        self._listings.pop(flow, None)

    def _expire(self, now: datetime) -> None:
        self._flows = {k: v for k, v in self._flows.items() if v[1] > now}
        self._listings = {k: v for k, v in self._listings.items() if v.expires > now}


@dataclass
class Picker:
    """What a console process needs to offer the picker."""

    config: PickerConfig
    oauth: NotionOAuth
    sessions: PickerSessions = field(default_factory=PickerSessions)


def picker_from_settings(client_id: str | None, client_secret: str | None) -> PickerConfig | None:
    """None when neither variable is set; a half-set pair is a configuration error."""
    if not client_id and not client_secret:
        return None
    if not client_id or not client_secret:
        from agentos.config import ConfigError

        raise ConfigError(
            "the Notion picker needs both NOTION_OAUTH_CLIENT_ID and NOTION_OAUTH_CLIENT_SECRET"
        )
    return PickerConfig(client_id, client_secret)
