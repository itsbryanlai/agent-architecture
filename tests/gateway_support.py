"""Helpers for tests that drive the gateway app and then let its relay deliver what it stored."""

from __future__ import annotations

import asyncio

from agentos.gateway.relay import InboundRelay


def deliver_inbound(app) -> int:
    """Run the relay once over the app's stored events; return how many it delivered."""
    d = app.state.deps
    return asyncio.run(InboundRelay(d.content, d.tenants, d.temporal_client).deliver_once())


def stored_text(app, tenant: str, event_id: str) -> str:
    """The text the gateway kept for an event."""
    d = app.state.deps
    return asyncio.run(d.content.get_inbound(tenant, event_id))["text"]
