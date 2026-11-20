"""Posting to Slack.

``SlackPoster`` is everything the activities need from Slack. The real
implementation wraps ``slack_sdk``'s async web client; ``FakeSlack`` records
calls for tests.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from agentos.connectors.notion import Hang

log = logging.getLogger(__name__)

# Slack API errors that pass by themselves: a rate limit, or a Slack-side failure.
TRANSIENT_SLACK_ERRORS = frozenset(
    {"ratelimited", "rate_limited", "internal_error", "fatal_error", "service_unavailable"}
)
PAGE_SIZE = 200
# A workspace or channel has at most this many entries read; more is a bug or an abuse.
MAX_PAGES = 500
# Rate-limit retries inside one call, each after Slack's Retry-After. A post or an
# update waits for at most a couple; a read of the whole workspace is many pages
# long and waits out as many limits as it meets.
INTERACTIVE_RATE_LIMIT_RETRIES = 2
BULK_RATE_LIMIT_RETRIES = 20
# A Retry-After above this is treated as this long.
MAX_RETRY_AFTER_SECONDS = 600.0
# Pages of history or replies one post lookup reads before giving up: it is a short window.
POST_LOOKUP_PAGES = 10
# Slack's answers to a history read that mean this app cannot see the channel: a missing scope,
# or a channel the bot is not a member of (it can post to a public one without joining).
HISTORY_REFUSED = {"missing_scope", "not_in_channel"}
# A bulk read logs its progress every this many pages.
PROGRESS_EVERY_PAGES = 10


class SlackLookupError(Exception):
    """A read of Slack's directory failed. ``transient`` says whether a retry may help.

    ``retry_after`` is the seconds Slack asked for in a ``Retry-After`` header, if it did.
    ``scope`` is the OAuth scope Slack says the call needed, when it named one.
    """

    def __init__(
        self,
        code: str,
        transient: bool = False,
        retry_after: float | None = None,
        scope: str | None = None,
    ):
        super().__init__(f"Slack lookup failed: {code}")
        self.code = code
        self.transient = transient
        self.retry_after = retry_after
        self.scope = scope


class SlackHistoryUnavailable(Exception):
    """Slack refused to show a channel's messages, so a post cannot be looked up.

    ``scope`` is the OAuth scope the app lacks, when Slack named one; ``code`` is Slack's
    error code (``missing_scope``, ``not_in_channel``). Neither carries a token or content.
    """

    def __init__(self, code: str, scope: str | None = None):
        super().__init__(f"Slack history unavailable: {code}")
        self.code = code
        self.scope = scope


# The message metadata event type that marks a post of this system.
POST_EVENT_TYPE = "agentos_post"


def post_metadata(marker: str) -> dict:
    """The message metadata that carries a post's marker (see ``ids.slack_post_marker``)."""
    return {"event_type": POST_EVENT_TYPE, "event_payload": {"key": marker}}


def _carries_marker(message: dict, marker: str) -> bool:
    meta = message.get("metadata") or {}
    return (
        meta.get("event_type") == POST_EVENT_TYPE
        and (meta.get("event_payload") or {}).get("key") == marker
    )


@dataclass(frozen=True)
class SlackUser:
    """What the audience rules need to know about one Slack user, from ``users.info``."""

    id: str
    team_id: str
    is_restricted: bool = False  # a multi-channel guest
    is_ultra_restricted: bool = False  # a single-channel guest
    deleted: bool = False
    is_bot: bool = False
    display_name: str = ""

    @property
    def is_guest(self) -> bool:
        return self.is_restricted or self.is_ultra_restricted


@dataclass(frozen=True)
class SlackChannel:
    """One channel the app is in, from ``conversations.list``: enough to pick it."""

    id: str
    name: str


class SlackPoster(Protocol):
    async def post_message(
        self,
        channel: str,
        text: str,
        thread_ts: str | None = None,
        blocks: list[dict] | None = None,
        metadata: dict | None = None,
    ) -> str:
        """Post a message and return its ts. ``metadata`` is Slack message metadata."""
        ...

    async def find_post(
        self, channel: str, thread_ts: str | None, marker: str, since: float
    ) -> str | None:
        """The ts of this app's post that carries ``marker`` (``post_metadata``), or None.

        Reads the thread's replies when ``thread_ts`` is set, otherwise the channel's history
        from ``since`` (epoch seconds). Needs the history scopes; raises
        ``SlackHistoryUnavailable`` when Slack refuses them.
        """
        ...

    async def update_message(
        self, channel: str, ts: str, text: str, blocks: list[dict] | None = None
    ) -> None: ...

    async def post_ephemeral(
        self, channel: str, user_id: str, text: str, thread_ts: str | None = None
    ) -> None:
        """Show ``text`` to one user in a channel; nobody else sees it."""
        ...

    async def open_dm(self, user_id: str) -> str:
        """Open (or reuse) a DM with the user and return its channel ID."""
        ...

    async def user_email(self, user_id: str) -> str | None:
        """Return the user's verified profile email (needs ``users:read.email``)."""
        ...

    async def user_by_email(self, email: str) -> str | None:
        """The Slack user ID for a profile email (``users.lookupByEmail``, needs
        ``users:read.email``), or None when no user has it. A failed lookup raises
        ``SlackLookupError``; only "no such user" is None."""
        ...

    async def open_view(self, trigger_id: str, view: dict) -> None:
        """Open a modal. The trigger ID from a click is valid for 3 seconds."""
        ...

    async def user_info(self, user_id: str) -> SlackUser:
        """The user's team and guest flags (needs ``users:read``)."""
        ...

    async def usergroup_members(self, group_id: str) -> list[str]:
        """User IDs in a user group (needs ``usergroups:read``)."""
        ...

    async def channel_members(self, channel_id: str) -> list[str]:
        """Every member of a channel, all pages (needs ``channels:read`` / ``groups:read``)."""
        ...

    async def list_users(self) -> list[SlackUser]:
        """Every user of the workspace, all pages (needs ``users:read``)."""
        ...

    async def list_channels(self) -> list[SlackChannel]:
        """The non-archived channels the app is in, all pages (needs ``channels:read``
        and ``groups:read``): public and private alike, but only ones the bot joined,
        so it can answer there. The console's routing picker offers these."""
        ...


class RealSlackPoster:
    """``slack_sdk`` behind the ``SlackPoster`` protocol.

    Three clients share the token. Posts, updates and DMs wait out a rate limit
    for a couple of retries. Single lookups do not wait: the error carries
    Slack's ``Retry-After`` so the activity can hand it to Temporal. Reading the
    whole workspace waits out as many limits as it meets, because failing at
    page 20 of 25 would only start over.
    """

    def __init__(self, token: str, tenant_id: str | None = None, base_url: str | None = None):
        from slack_sdk.http_retry.builtin_async_handlers import AsyncRateLimitErrorRetryHandler
        from slack_sdk.web.async_client import AsyncWebClient

        extra = {"base_url": base_url} if base_url else {}
        self._tenant_id = tenant_id
        self._client = AsyncWebClient(
            token=token,
            retry_handlers=[
                AsyncRateLimitErrorRetryHandler(max_retry_count=INTERACTIVE_RATE_LIMIT_RETRIES)
            ],
            **extra,
        )
        self._lookup_client = AsyncWebClient(token=token, retry_handlers=[], **extra)
        self._bulk_client = AsyncWebClient(
            token=token,
            retry_handlers=[
                AsyncRateLimitErrorRetryHandler(max_retry_count=BULK_RATE_LIMIT_RETRIES)
            ],
            **extra,
        )

    async def post_message(self, channel, text, thread_ts=None, blocks=None, metadata=None) -> str:
        kwargs: dict = {"channel": channel, "text": text}
        if thread_ts:
            kwargs["thread_ts"] = thread_ts
        if blocks is not None:
            kwargs["blocks"] = blocks
        if metadata is not None:
            kwargs["metadata"] = metadata
        resp = await self._client.chat_postMessage(**kwargs)
        return resp["ts"]

    async def find_post(self, channel, thread_ts, marker, since) -> str | None:
        cursor = None
        try:
            for _ in range(POST_LOOKUP_PAGES):
                if thread_ts:
                    resp = await self._lookup_client.conversations_replies(
                        channel=channel,
                        ts=thread_ts,
                        include_all_metadata=True,
                        limit=PAGE_SIZE,
                        cursor=cursor,
                    )
                else:
                    resp = await self._lookup_client.conversations_history(
                        channel=channel,
                        oldest=f"{since:.6f}",
                        include_all_metadata=True,
                        limit=PAGE_SIZE,
                        cursor=cursor,
                    )
                for message in resp.get("messages") or []:
                    if _carries_marker(message, marker):
                        return message["ts"]
                cursor = (resp.get("response_metadata") or {}).get("next_cursor") or None
                if not cursor:
                    return None
        except Exception as err:
            error = _lookup_error(err)
            if error.code in HISTORY_REFUSED:
                raise SlackHistoryUnavailable(error.code, _needed_scope(err)) from err
            raise error from err
        raise SlackLookupError("too_many_pages")

    async def update_message(self, channel, ts, text, blocks=None) -> None:
        # An empty blocks list removes the old blocks (e.g. the buttons).
        await self._client.chat_update(
            channel=channel, ts=ts, text=text, blocks=blocks if blocks is not None else []
        )

    async def post_ephemeral(self, channel, user_id, text, thread_ts=None) -> None:
        kwargs: dict = {"channel": channel, "user": user_id, "text": text}
        if thread_ts:
            kwargs["thread_ts"] = thread_ts
        await self._client.chat_postEphemeral(**kwargs)

    async def open_dm(self, user_id: str) -> str:
        resp = await self._client.conversations_open(users=user_id)
        return resp["channel"]["id"]

    async def user_email(self, user_id: str) -> str | None:
        resp = await self._client.users_info(user=user_id)
        return (resp.get("user") or {}).get("profile", {}).get("email")

    async def user_by_email(self, email: str) -> str | None:
        try:
            resp = await self._lookup_client.users_lookupByEmail(email=email)
        except Exception as err:
            error = _lookup_error(err)
            if error.code == "users_not_found":
                return None
            raise error from err
        return (resp.get("user") or {}).get("id") or None

    async def open_view(self, trigger_id: str, view: dict) -> None:
        await self._client.views_open(trigger_id=trigger_id, view=view)

    async def user_info(self, user_id: str) -> SlackUser:
        try:
            resp = await self._lookup_client.users_info(user=user_id)
        except Exception as err:
            raise _lookup_error(err) from err
        return _user_from(resp.get("user") or {})

    async def usergroup_members(self, group_id: str) -> list[str]:
        try:
            resp = await self._lookup_client.usergroups_users_list(usergroup=group_id)
        except Exception as err:
            raise _lookup_error(err) from err
        return list(resp.get("users") or [])

    async def channel_members(self, channel_id: str) -> list[str]:
        members: list[str] = []
        cursor = None
        try:
            for _ in range(MAX_PAGES):
                resp = await self._lookup_client.conversations_members(
                    channel=channel_id, limit=PAGE_SIZE, cursor=cursor
                )
                members.extend(resp.get("members") or [])
                cursor = (resp.get("response_metadata") or {}).get("next_cursor") or None
                if not cursor:
                    return members
        except Exception as err:
            raise _lookup_error(err) from err
        raise SlackLookupError("too_many_pages")

    async def list_users(self) -> list[SlackUser]:
        users: list[SlackUser] = []
        cursor = None
        try:
            for page in range(1, MAX_PAGES + 1):
                resp = await self._bulk_client.users_list(limit=PAGE_SIZE, cursor=cursor)
                users.extend(_user_from(raw) for raw in resp.get("members") or [])
                cursor = (resp.get("response_metadata") or {}).get("next_cursor") or None
                if not cursor:
                    return users
                if page % PROGRESS_EVERY_PAGES == 0:
                    log.info(
                        "slack users.list tenant_id=%s pages=%d users=%d",
                        self._tenant_id,
                        page,
                        len(users),
                    )
        except Exception as err:
            raise _lookup_error(err) from err
        raise SlackLookupError("too_many_pages")

    async def list_channels(self) -> list[SlackChannel]:
        channels: list[SlackChannel] = []
        cursor = None
        try:
            for page in range(1, MAX_PAGES + 1):
                resp = await self._bulk_client.conversations_list(
                    types="public_channel,private_channel",
                    exclude_archived=True,
                    limit=PAGE_SIZE,
                    cursor=cursor,
                )
                channels.extend(
                    SlackChannel(id=raw.get("id", ""), name=raw.get("name", ""))
                    for raw in resp.get("channels") or []
                    if raw.get("is_member")
                )
                cursor = (resp.get("response_metadata") or {}).get("next_cursor") or None
                if not cursor:
                    return channels
                if page % PROGRESS_EVERY_PAGES == 0:
                    log.info(
                        "slack conversations.list tenant_id=%s pages=%d channels=%d",
                        self._tenant_id,
                        page,
                        len(channels),
                    )
        except Exception as err:
            raise _lookup_error(err) from err
        raise SlackLookupError("too_many_pages")


def _needed_scope(err: Exception) -> str | None:
    """The scope Slack says a refused call needed, if the error response names one."""
    try:
        return str(err.response.get("needed") or "") or None  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 - an odd response just names no scope
        return None


def _user_from(raw: dict) -> SlackUser:
    return SlackUser(
        id=raw.get("id", ""),
        team_id=raw.get("team_id") or "",
        is_restricted=bool(raw.get("is_restricted")),
        is_ultra_restricted=bool(raw.get("is_ultra_restricted")),
        deleted=bool(raw.get("deleted")),
        is_bot=bool(raw.get("is_bot")),
        display_name=_display_name(raw),
    )


def _retry_after(response) -> float | None:
    """Seconds from a response's ``Retry-After`` header, capped, or None if absent or odd."""
    headers = getattr(response, "headers", None)
    if not headers:
        return None
    try:
        raw = next((v for k, v in headers.items() if str(k).lower() == "retry-after"), None)
        if isinstance(raw, (list, tuple)):
            raw = raw[0] if raw else None
        seconds = float(raw)
    except (TypeError, ValueError):
        return None
    if seconds < 0:
        return None
    return min(seconds, MAX_RETRY_AFTER_SECONDS)


def _display_name(raw: dict) -> str:
    profile = raw.get("profile") or {}
    return (
        profile.get("display_name") or profile.get("real_name") or raw.get("real_name") or ""
    ).strip()


def _lookup_error(err: Exception) -> SlackLookupError:
    """A Slack SDK failure as a ``SlackLookupError``; the error code only, never a token."""
    response = getattr(err, "response", None)
    code = ""
    status = None
    if response is not None:
        try:
            code = str(response.get("error") or "")
        except Exception:  # noqa: BLE001 - an odd response still becomes a lookup error
            code = ""
        status = getattr(response, "status_code", None)
    if not code:
        code = type(err).__name__
    retry_after = _retry_after(response)
    transient = (
        code in TRANSIENT_SLACK_ERRORS
        or (isinstance(status, int) and (status == 429 or status >= 500))
        or not hasattr(err, "response")
    )
    return SlackLookupError(
        code, transient=transient, retry_after=retry_after, scope=_needed_scope(err)
    )


def _check_blocks(blocks: list[dict] | None) -> None:
    """Refuse blocks as Slack does: a section's text must be 1..3000 characters.

    The fake keeps what production sends, so a splitter that emits an empty or over-long
    section fails the test that posts it, as it would fail against Slack.
    """
    if not blocks:
        return
    from slack_sdk.errors import SlackApiError
    from slack_sdk.web.slack_response import SlackResponse

    for block in blocks:
        if block.get("type") != "section":
            continue
        text = (block.get("text") or {}).get("text")
        if isinstance(text, str) and not 0 < len(text) <= 3000:
            raise SlackApiError(
                "The request to the Slack API failed.",
                SlackResponse(
                    client=None,
                    http_verb="POST",
                    api_url="https://slack.com/api/chat.postMessage",
                    req_args={},
                    data={"ok": False, "error": "invalid_blocks"},
                    headers={},
                    status_code=200,
                ),
            )


class FakeSlack:
    """Test fake. Every call is appended to ``calls`` as (method, kwargs).

    A user not listed in ``users`` is a full member of workspace ``team_id``.
    """

    def __init__(
        self,
        emails: dict[str, str] | None = None,
        users: dict[str, SlackUser] | None = None,
        groups: dict[str, list[str]] | None = None,
        channel_members: dict[str, list[str]] | None = None,
        team_id: str = "T0TEST",
        channels: dict[str, str] | None = None,
    ):
        self.calls: list[tuple[str, dict]] = []
        self.emails = dict(emails or {})
        self.users = dict(users or {})
        self.groups = {g: list(m) for g, m in (groups or {}).items()}
        self.members_of = {c: list(m) for c, m in (channel_members or {}).items()}
        self.channels = dict(channels or {})
        self.team_id = team_id
        self._lookup_error: Exception | None = None
        self._lookup_failures_left: int | None = None
        self._ts = 0
        self.messages: list[dict] = []
        self._history_refusal: SlackHistoryUnavailable | None = None
        self._fail_next_update: Exception | None = None
        self._update_failures_left: int | None = None
        self._hang_next_update: Hang | None = None
        # Injection points for the failures a post meets in production. See the methods.
        self._fail_after_post = 0
        self._hang_after_post: list[Hang] = []
        self._fail_next_post: Exception | None = None
        self._post_failures_left: int | None = None
        self._post_fail_channel: str | None = None

    def fail_lookups(self, error: Exception | None, times: int | None = None) -> None:
        """Make directory lookups raise ``error``: ``times`` of them, or all until set to None."""
        self._lookup_error = error
        self._lookup_failures_left = times

    def fail_next_update(self, error: Exception, times: int | None = 1) -> None:
        """Make update_message raise ``error``: the next ``times`` calls, or all when None."""
        self._fail_next_update = error
        self._update_failures_left = times

    def hang_next_update(self) -> Hang:
        """The next update_message waits until the returned ``Hang`` is released, as a
        slow Slack edit does. Other updates run normally."""
        from agentos.connectors.notion import Hang

        hang = Hang()
        self._hang_next_update = hang
        return hang

    def fail_after_post(self, times: int = 1) -> None:
        """The next ``times`` posts take effect and then raise a timeout, as when the
        response is lost."""
        self._fail_after_post = times

    def hang_after_post(self) -> Hang:
        """The next post takes effect, then waits until the returned ``Hang`` is
        released. Another post meanwhile runs normally: this is how two attempts
        overlap."""
        from agentos.connectors.notion import Hang

        hang = Hang()
        self._hang_after_post.append(hang)
        return hang

    def fail_next_post(
        self, error: Exception, times: int | None = 1, channel: str | None = None
    ) -> None:
        """Make post_message raise ``error`` before posting: the next ``times`` posts
        (all when None), only those to ``channel`` when one is named."""
        self._fail_next_post = error
        self._post_failures_left = times
        self._post_fail_channel = channel

    def _next_ts(self) -> str:
        self._ts += 1
        return f"1700000000.{self._ts:06d}"

    def refuse_history(self, error: SlackHistoryUnavailable | None) -> None:
        """Make ``find_post`` raise ``error``, as when the app lacks a history scope."""
        self._history_refusal = error

    async def post_message(self, channel, text, thread_ts=None, blocks=None, metadata=None) -> str:
        _check_blocks(blocks)
        if self._fail_next_post is not None and self._post_fail_channel in (None, channel):
            error = self._fail_next_post
            if self._post_failures_left is not None:
                self._post_failures_left -= 1
                if self._post_failures_left <= 0:
                    self._fail_next_post = None
            raise error
        ts = self._next_ts()
        sent = {
            "channel": channel,
            "text": text,
            "thread_ts": thread_ts,
            "blocks": blocks,
            "metadata": metadata,
            "ts": ts,
        }
        self.calls.append(("post_message", sent))
        self.messages.append(sent)
        if self._hang_after_post:
            await self._hang_after_post.pop(0).wait()
        if self._fail_after_post:
            self._fail_after_post -= 1
            raise TimeoutError("simulated lost response after the post took effect")
        return ts

    async def find_post(self, channel, thread_ts, marker, since) -> str | None:
        """The fake keeps every post, so ``since`` (a real-clock window) is not applied."""
        self.calls.append(
            ("find_post", {"channel": channel, "thread_ts": thread_ts, "marker": marker})
        )
        if self._history_refusal is not None:
            raise self._history_refusal
        for sent in self.messages:
            here = sent["channel"] == channel and (not thread_ts or sent["thread_ts"] == thread_ts)
            if here and _carries_marker(sent, marker):
                return sent["ts"]
        return None

    async def update_message(self, channel, ts, text, blocks=None) -> None:
        _check_blocks(blocks)
        if self._fail_next_update is not None:
            error = self._fail_next_update
            if self._update_failures_left is not None:
                self._update_failures_left -= 1
                if self._update_failures_left <= 0:
                    self._fail_next_update = None
            raise error
        hang, self._hang_next_update = self._hang_next_update, None
        if hang is not None:
            await hang.wait()
        self.calls.append(
            ("update_message", {"channel": channel, "ts": ts, "text": text, "blocks": blocks})
        )

    async def post_ephemeral(self, channel, user_id, text, thread_ts=None) -> None:
        self.calls.append(
            (
                "post_ephemeral",
                {"channel": channel, "user_id": user_id, "text": text, "thread_ts": thread_ts},
            )
        )

    async def open_dm(self, user_id: str) -> str:
        self.calls.append(("open_dm", {"user_id": user_id}))
        return f"D-{user_id}"

    async def user_email(self, user_id: str) -> str | None:
        self.calls.append(("user_email", {"user_id": user_id}))
        return self.emails.get(user_id)

    async def user_by_email(self, email: str) -> str | None:
        self.calls.append(("user_by_email", {"email": email}))
        self._check_lookup()
        key = email.strip().lower()
        for user_id, known in self.emails.items():
            if known.strip().lower() == key:
                return user_id
        return None

    async def open_view(self, trigger_id: str, view: dict) -> None:
        self.calls.append(("open_view", {"trigger_id": trigger_id, "view": view}))

    def _check_lookup(self) -> None:
        if self._lookup_error is None:
            return
        if self._lookup_failures_left is not None:
            self._lookup_failures_left -= 1
            if self._lookup_failures_left <= 0:
                error, self._lookup_error = self._lookup_error, None
                raise error
        raise self._lookup_error

    async def user_info(self, user_id: str) -> SlackUser:
        self.calls.append(("user_info", {"user_id": user_id}))
        self._check_lookup()
        return self.users.get(user_id) or SlackUser(user_id, self.team_id)

    async def usergroup_members(self, group_id: str) -> list[str]:
        self.calls.append(("usergroup_members", {"group_id": group_id}))
        self._check_lookup()
        return list(self.groups.get(group_id, []))

    async def channel_members(self, channel_id: str) -> list[str]:
        self.calls.append(("channel_members", {"channel_id": channel_id}))
        self._check_lookup()
        return list(self.members_of.get(channel_id, []))

    async def list_users(self) -> list[SlackUser]:
        self.calls.append(("list_users", {}))
        self._check_lookup()
        listed = {u.id: u for u in self.users.values()}
        for ids in [*self.members_of.values(), *self.groups.values()]:
            for user_id in ids:
                listed.setdefault(user_id, SlackUser(user_id, self.team_id))
        return list(listed.values())

    async def list_channels(self) -> list[SlackChannel]:
        self.calls.append(("list_channels", {}))
        self._check_lookup()
        return [SlackChannel(id=c, name=n) for c, n in self.channels.items()]

    def lookups(self) -> list[tuple[str, dict]]:
        """The directory reads made so far."""
        names = {
            "user_info",
            "user_by_email",
            "usergroup_members",
            "channel_members",
            "list_users",
            "list_channels",
        }
        return [(name, kw) for name, kw in self.calls if name in names]

    def ephemerals(self) -> list[dict]:
        return [kw for name, kw in self.calls if name == "post_ephemeral"]

    def views(self) -> list[dict]:
        return [kw for name, kw in self.calls if name == "open_view"]

    def posts(self) -> list[dict]:
        return [kw for name, kw in self.calls if name == "post_message"]

    def updates(self) -> list[dict]:
        return [kw for name, kw in self.calls if name == "update_message"]
