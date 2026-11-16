"""A request-rate limit for one tenant's Notion integration.

Notion allows an integration an average of three requests per second (its published
request limit; the SDK bundled here does not state it). This is a token bucket sized
to that: ``rate`` tokens a second, at most ``burst`` saved up. It counts one worker
process; several workers share the integration's allowance, and a 429 from Notion is
still retried by Temporal after the wait Notion names.

Two priorities. Interactive requests (tool calls, writes, search re-checks) are the
default. Bulk requests (indexing, status polling) run inside ``bulk_priority()`` and
wait for as long as any interactive request is waiting.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar

NOTION_RATE_PER_SECOND = 3.0
NOTION_BURST = 3

_bulk: ContextVar[bool] = ContextVar("notion_bulk", default=False)


@contextmanager
def bulk_priority() -> Iterator[None]:
    """Notion requests made inside this block queue behind interactive ones."""
    token = _bulk.set(True)
    try:
        yield
    finally:
        _bulk.reset(token)


def is_bulk() -> bool:
    return _bulk.get()


class NotionLimiter:
    def __init__(
        self,
        rate: float = NOTION_RATE_PER_SECOND,
        burst: int = NOTION_BURST,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self._rate = rate
        self._burst = float(burst)
        self._clock = clock
        self._sleep = sleep
        self._tokens = float(burst)
        self._at = clock()
        self._interactive_waiting = 0

    def _take(self) -> float:
        """0 when a token was taken, else the seconds until one is there."""
        now = self._clock()
        self._tokens = min(self._burst, self._tokens + (now - self._at) * self._rate)
        self._at = now
        if self._tokens >= 1 - 1e-9:  # float rounding must not strand a waiter
            self._tokens = max(0.0, self._tokens - 1)
            return 0.0
        return (1 - self._tokens) / self._rate

    async def acquire(self, bulk: bool | None = None) -> None:
        """Wait for one request's worth of allowance. ``bulk`` defaults to the current
        ``bulk_priority()`` context."""
        bulk = is_bulk() if bulk is None else bulk
        if not bulk:
            self._interactive_waiting += 1
        try:
            while True:
                if bulk and self._interactive_waiting:
                    await self._sleep(1 / self._rate)
                    continue
                wait = self._take()
                if wait == 0:
                    return
                await self._sleep(wait)
        finally:
            if not bulk:
                self._interactive_waiting -= 1
