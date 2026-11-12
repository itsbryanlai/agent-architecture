"""Posting to Slack.

``SlackPoster`` is everything the activities need from Slack. The real
implementation wraps ``slack_sdk``'s async web client; ``FakeSlack`` records
calls for tests.
"""

from __future__ import annotations

from typing import Protocol


class SlackPoster(Protocol):
    async def post_message(
        self,
        channel: str,
        text: str,
        thread_ts: str | None = None,
        blocks: list[dict] | None = None,
    ) -> str:
        """Post a message and return its ts."""
        ...

    async def update_message(
        self, channel: str, ts: str, text: str, blocks: list[dict] | None = None
    ) -> None: ...

    async def open_dm(self, user_id: str) -> str:
        """Open (or reuse) a DM with the user and return its channel ID."""
        ...

    async def user_email(self, user_id: str) -> str | None:
        """Return the user's verified profile email (needs ``users:read.email``)."""
        ...

    async def open_view(self, trigger_id: str, view: dict) -> None:
        """Open a modal. The trigger ID from a click is valid for 3 seconds."""
        ...


class RealSlackPoster:
    def __init__(self, token: str):
        from slack_sdk.web.async_client import AsyncWebClient

        self._client = AsyncWebClient(token=token)

    async def post_message(self, channel, text, thread_ts=None, blocks=None) -> str:
        kwargs: dict = {"channel": channel, "text": text}
        if thread_ts:
            kwargs["thread_ts"] = thread_ts
        if blocks is not None:
            kwargs["blocks"] = blocks
        resp = await self._client.chat_postMessage(**kwargs)
        return resp["ts"]

    async def update_message(self, channel, ts, text, blocks=None) -> None:
        # An empty blocks list removes the old blocks (e.g. the buttons).
        await self._client.chat_update(
            channel=channel, ts=ts, text=text, blocks=blocks if blocks is not None else []
        )

    async def open_dm(self, user_id: str) -> str:
        resp = await self._client.conversations_open(users=user_id)
        return resp["channel"]["id"]

    async def user_email(self, user_id: str) -> str | None:
        resp = await self._client.users_info(user=user_id)
        return (resp.get("user") or {}).get("profile", {}).get("email")

    async def open_view(self, trigger_id: str, view: dict) -> None:
        await self._client.views_open(trigger_id=trigger_id, view=view)


class FakeSlack:
    """Test fake. Every call is appended to ``calls`` as (method, kwargs)."""

    def __init__(self, emails: dict[str, str] | None = None):
        self.calls: list[tuple[str, dict]] = []
        self.emails = dict(emails or {})
        self._ts = 0
        self._fail_next_update: Exception | None = None

    def fail_next_update(self, error: Exception) -> None:
        """Make the next update_message raise ``error`` once."""
        self._fail_next_update = error

    def _next_ts(self) -> str:
        self._ts += 1
        return f"1700000000.{self._ts:06d}"

    async def post_message(self, channel, text, thread_ts=None, blocks=None) -> str:
        ts = self._next_ts()
        self.calls.append(
            (
                "post_message",
                {
                    "channel": channel,
                    "text": text,
                    "thread_ts": thread_ts,
                    "blocks": blocks,
                    "ts": ts,
                },
            )
        )
        return ts

    async def update_message(self, channel, ts, text, blocks=None) -> None:
        if self._fail_next_update is not None:
            error, self._fail_next_update = self._fail_next_update, None
            raise error
        self.calls.append(
            ("update_message", {"channel": channel, "ts": ts, "text": text, "blocks": blocks})
        )

    async def open_dm(self, user_id: str) -> str:
        self.calls.append(("open_dm", {"user_id": user_id}))
        return f"D-{user_id}"

    async def user_email(self, user_id: str) -> str | None:
        self.calls.append(("user_email", {"user_id": user_id}))
        return self.emails.get(user_id)

    async def open_view(self, trigger_id: str, view: dict) -> None:
        self.calls.append(("open_view", {"trigger_id": trigger_id, "view": view}))

    def views(self) -> list[dict]:
        return [kw for name, kw in self.calls if name == "open_view"]

    def posts(self) -> list[dict]:
        return [kw for name, kw in self.calls if name == "post_message"]

    def updates(self) -> list[dict]:
        return [kw for name, kw in self.calls if name == "update_message"]
