"""The relay: from the gateway's ``inbound_events`` to the conversation workflows.

The gateway answers Slack as soon as it has stored an event, so a slow or unreachable
Temporal never makes Slack wait or retry. This loop, which runs in the gateway process,
takes each stored message and signal-with-starts its thread's ``ConversationWorkflow`` with
the event ID and small metadata (never the text, which stays in Postgres), then marks the
row delivered.

Delivery is at least once: a crash between the signal and the mark delivers the message
again, and the workflow drops an event ID it has seen. A message that cannot be delivered
is retried with a growing delay (``backoff``), and the messages after it in the same thread
wait, so a thread's messages are never delivered out of order. Other threads go on.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.content_store import ContentStore, InboundRow, backoff
from agentos.models import ConversationRef, InboundMessage
from agentos.tenant_store import TenantSource

log = logging.getLogger("agentos.gateway.relay")

BATCH = 100
# The longest the loop sleeps when nothing woke it: a delivery that failed, or a message another
# gateway process stored, is found within this.
IDLE_SECONDS = 5.0


def _utcnow() -> datetime:
    return datetime.now(UTC)


class InboundRelay:
    def __init__(
        self,
        content: ContentStore,
        tenants: TenantSource,
        temporal_client: Any,
        clock: Callable[[], datetime] = _utcnow,
    ):
        self._content = content
        self._tenants = tenants
        self._client = temporal_client
        self._clock = clock
        self._wake = asyncio.Event()

    def wake(self) -> None:
        """Look for new messages now (the gateway calls this after storing one)."""
        self._wake.set()

    async def deliver_once(self) -> int:
        """Deliver every message that is due, oldest first; return how many were delivered."""
        delivered = 0
        for tenant in (await self._tenants.snapshot()).tenants:
            blocked: set[tuple[str, str]] = set()
            now = self._clock()
            for row in await self._content.undelivered(tenant, BATCH):
                thread = _thread(row)
                if thread in blocked or row.next_attempt_at > now:
                    blocked.add(thread)
                    continue
                if await self._deliver(tenant, row):
                    delivered += 1
                else:
                    blocked.add(thread)
        return delivered

    async def _deliver(self, tenant: str, row: InboundRow) -> bool:
        try:
            p = row.payload
            channel, thread_ts = p["channel"], p["thread_ts"]
            workflow_id = ids.conversation_id(tenant, channel, thread_ts)
            await self._client.start_workflow(
                "ConversationWorkflow",
                ConversationRef(tenant, p["agent_id"], channel, thread_ts),
                id=workflow_id,
                task_queue=TASK_QUEUE,
                start_signal="message",
                start_signal_args=[
                    InboundMessage(
                        event_id=row.event_id,
                        user_id=p["user_id"],
                        text="",
                        channel=channel,
                        thread_ts=thread_ts,
                        channel_kind=p["channel_kind"],
                        text_stored=True,
                    )
                ],
            )
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001 - any failure is retried later
            attempts = row.attempts + 1
            retry_at = backoff(attempts, self._clock())
            await self._content.delay_inbound(tenant, row.event_id, attempts, retry_at)
            log.warning(
                "tenant_id=%s event_id=%s delivery failed attempts=%d error_type=%s",
                tenant,
                row.event_id,
                attempts,
                type(err).__name__,
            )
            return False
        await self._content.mark_delivered(tenant, row.event_id, self._clock())
        log.info(
            "tenant_id=%s workflow_id=%s event_id=%s signalled message",
            tenant,
            workflow_id,
            row.event_id,
        )
        return True

    async def run(self) -> None:
        """Deliver until cancelled. A failure of the database or of Temporal is logged and waited out."""
        while True:
            try:
                delivered = await self.deliver_once()
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - the loop must outlive an outage
                log.warning("inbound relay pass failed error_type=%s", type(err).__name__)
                delivered = 0
            if delivered >= BATCH:
                continue
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=IDLE_SECONDS)
            except TimeoutError:
                pass


def _thread(row: InboundRow) -> tuple[str, str]:
    p = row.payload
    return (str(p.get("channel")), str(p.get("thread_ts")))
