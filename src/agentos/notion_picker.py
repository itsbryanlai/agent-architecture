"""Choose from Notion: the creator's own Notion sign-in, used once to list what they can see.

A public Notion integration (``NOTION_OAUTH_CLIENT_ID`` / ``NOTION_OAUTH_CLIENT_SECRET``)
lets a creator sign in with their own Notion account and share pages and databases with it.
The flow:

1. The creator presses "Choose from Notion" on a draft. The console makes a random ``state``
   bound to their console session, the tenant, the agent and the version, and sends the
   browser to Notion.
2. Notion sends the browser back to ``/auth/notion/callback``. The callback needs the same
   console session and a ``state`` it issued (single use, ten minutes), exchanges the code for
   the creator's token and searches with that token. Without consent the token is dropped: it
   lives for the length of that request, a local variable, never stored, logged or put in a
   table. With the consent the creator ticked before signing in ("keep my Notion sign-in"),
   the control plane stores it encrypted per tenant and creator, and the nightly check
   (``creator_access``) reads with it; a consented pick replaces it, revoking deletes it.
   The box is a per-pick choice: completing a re-pick with it unticked revokes the sign-in
   a former pick kept.
3. What the search listed (IDs, kinds, titles) is kept in memory for the session so the
   creator can tick objects. Only an ID in that list can be picked, and a pick is recorded
   for the agent (``grant_picks``).

The listing is per process: with several console processes behind one address the callback
must reach the process that began the flow. The console runs as one process today.

Connect Notion (the admin settings page) reuses the same integration, state mechanism and
callback path with a ``purpose`` in the flow: the authorize URL asks for ``owner=workspace``
(a workspace admin authorizes the whole workspace, not picked pages), the callback exchanges
the code for the workspace bot token, validates it with one cheap call (``users/me``) and
hands it to ``ControlPlane.connect_notion``, which stores it encrypted as the tenant's
``notion`` credential. The token lives only in that request and the encrypted row; it is
never logged. Reconnecting replaces the row; Disconnect removes it.
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
USERS_ME_URL = "https://api.notion.com/v1/users/me"
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


@dataclass(frozen=True)
class WorkspaceGrant:
    """What Connect Notion's OAuth returned: the workspace bot token and its workspace.

    The token stays out of ``repr``; it is stored encrypted and never logged.
    """

    token: str = field(repr=False)
    workspace_id: str
    workspace_name: str


class NotionOAuth(Protocol):
    async def list_visible(self, code: str, redirect_uri: str) -> list[PickedObject]:
        """Trade the code for the creator's token, list what it can search, discard it."""
        ...

    async def list_and_keep(self, code: str, redirect_uri: str) -> tuple[list[PickedObject], str]:
        """Trade the code for the creator's token, list what it can search, return both.

        The caller stores the token only with the creator's consent; the token is never
        logged. Without consent the caller uses ``list_visible``, which discards it.
        """
        ...

    async def connect_workspace(self, code: str, redirect_uri: str) -> WorkspaceGrant:
        """Trade the code for the workspace bot token, validate it with one cheap call."""
        ...


class HttpNotionOAuth:
    def __init__(
        self,
        config: PickerConfig,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self._config = config
        self._transport = transport

    async def _exchange(self, http: httpx.AsyncClient, code: str, redirect_uri: str) -> dict:
        basic = base64.b64encode(
            f"{self._config.client_id}:{self._config.client_secret}".encode()
        ).decode()
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
        if token_response.status_code != 200 or not isinstance(body, dict):
            raise NotionPickerError(f"token endpoint refused: {token_response.status_code}")
        return body

    async def list_visible(self, code: str, redirect_uri: str) -> list[PickedObject]:
        objects, _token = await self._list(code, redirect_uri)
        return objects

    async def list_and_keep(self, code: str, redirect_uri: str) -> tuple[list[PickedObject], str]:
        return await self._list(code, redirect_uri)

    async def _list(self, code: str, redirect_uri: str) -> tuple[list[PickedObject], str]:
        try:
            async with httpx.AsyncClient(timeout=15.0, transport=self._transport) as http:
                body = await self._exchange(http, code, redirect_uri)
                token = body.get("access_token")
                if not isinstance(token, str) or not token:
                    raise NotionPickerError("token endpoint refused: no token")
                return await self._search(http, token), token
        except (httpx.HTTPError, ValueError) as err:
            raise NotionPickerError(f"Notion request failed: {type(err).__name__}") from err

    async def connect_workspace(self, code: str, redirect_uri: str) -> WorkspaceGrant:
        try:
            async with httpx.AsyncClient(timeout=15.0, transport=self._transport) as http:
                body = await self._exchange(http, code, redirect_uri)
                token = body.get("access_token")
                workspace_id = body.get("workspace_id")
                workspace_name = body.get("workspace_name")
                if not all(
                    isinstance(value, str) and value
                    for value in (token, workspace_id, workspace_name)
                ):
                    raise NotionPickerError("token response names no workspace bot token")
                # One cheap call proves the token works before anything is stored.
                response = await http.get(
                    USERS_ME_URL,
                    headers={"Authorization": f"Bearer {token}", "Notion-Version": NOTION_VERSION},
                )
                if response.status_code != 200:
                    raise NotionPickerError(f"validation refused: {response.status_code}")
                return WorkspaceGrant(token, workspace_id, workspace_name)
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


def authorize_url(
    config: PickerConfig, sign_in: SignInConfig, state: str, owner: str = "user"
) -> str:
    query = {
        "client_id": config.client_id,
        "response_type": "code",
        "owner": owner,
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
    # "pick" lists what the creator can see; "connect" authorizes the tenant's workspace.
    purpose: str = "pick"
    # The creator ticked "keep my Notion sign-in" before signing in. Not part of the
    # flow's identity: the listing is found again on the pick page either way.
    consent: bool = field(default=False, compare=False)


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
