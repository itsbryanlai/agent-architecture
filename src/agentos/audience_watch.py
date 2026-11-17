"""Watching audience changes: decide from a Slack event which agents to pause, and tell
the people accountable for them.

Rules kept here:

* The one trigger is a channel being shared with another organisation (Slack Connect).
  Who is added to a channel, guest or not, is the admins' responsibility and pauses
  nothing. The event needs no Slack lookup, so there is no case where it "could not be
  checked": a share is a share.
* The agents paused are the published agents whose effective audience names the channel,
  and only in that channel. Direct messages and other channels are untouched.
* Every effect happens once per pause, however often the activity runs: the pause row is
  opened once, its audit event and each notice have keys built from when the pause began.
* The owner (creator) of the agent and the tenant's admins get a direct message that
  names the agent, the channel and the reason. It carries no content.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime

from temporalio import activity

from agentos import ids
from agentos.agent_store import AgentStore
from agentos.audience import effective_audience
from agentos.audience_pauses import CHANNEL_SHARED, REASON_TEXT, PauseRecord, PauseStore
from agentos.audit import AuditStore
from agentos.config import AgentVersion
from agentos.members import ADMIN, MemberStore
from agentos.models import AudienceEvent, AuditEvent
from agentos.slack_format import escape_slack
from agentos.stores import IdempotencyStore
from agentos.tenants import TenantRegistry

log = logging.getLogger("agentos.audience_watch")

IDENTITY = "system:audience-watch"


def _utcnow() -> datetime:
    return datetime.now(UTC)


class AudienceWatchActivities:
    def __init__(
        self,
        registry: TenantRegistry,
        agents: AgentStore,
        members: MemberStore,
        pauses: PauseStore,
        audit: AuditStore,
        idem: IdempotencyStore,
        clock: Callable[[], datetime] = _utcnow,
    ):
        self._registry = registry
        self._agents = agents
        self._members = members
        self._pauses = pauses
        self._audit = audit
        self._idem = idem
        self._clock = clock

    def all(self) -> list:
        return [self.watch_audience_event]

    @activity.defn(name="watch_audience_event")
    async def watch_audience_event(self, event: AudienceEvent) -> list[str]:
        """Pause the agents that answer in the shared channel; return their IDs."""
        log.info(
            "activity=watch_audience_event tenant_id=%s channel=%s", event.tenant_id, event.channel
        )
        return await self._pause_agents(event, CHANNEL_SHARED)

    async def _pause_agents(self, event: AudienceEvent, reason: str) -> list[str]:
        tenant = event.tenant_id
        config = self._registry.get(tenant).config
        published = await self._agents.published_all(tenant)
        paused: list[str] = []
        for agent in published.values():
            if not effective_audience(agent, config, published).includes_channel(event.channel):
                continue
            pause = await self._pauses.open_pause(
                tenant, agent.agent_id, event.channel, reason, self._clock()
            )
            await self._audit_pause(event, agent, pause)
            await self._notify(config.admins, agent, pause)
            paused.append(agent.agent_id)
        return paused

    async def _audit_pause(
        self, event: AudienceEvent, agent: AgentVersion, pause: PauseRecord
    ) -> None:
        await self._audit.record(
            AuditEvent(
                tenant_id=event.tenant_id,
                event_key=ids.audit_audience_pause_key(
                    event.tenant_id, agent.agent_id, pause.channel, pause.paused_at.isoformat()
                ),
                kind="audience_pause",
                agent_id=agent.agent_id,
                agent_version=agent.version,
                requester_user_id=IDENTITY,
                identity=IDENTITY,
                tool=pause.reason,
                target=pause.channel,
            )
        )

    async def _recipients(self, tenant: str, config_admins: tuple[str, ...], agent_id: str):
        """The agent's creator and the tenant's admins, each once."""
        people: list[str] = []
        record = await self._agents.get_agent(tenant, agent_id)
        if record is not None:
            people.append(record.created_by)
        people.extend(config_admins)
        people.extend(
            m.user_id for m in await self._members.list_members(tenant) if m.role == ADMIN
        )
        return list(dict.fromkeys(p for p in people if _is_user_id(p)))

    async def _notify(
        self, config_admins: tuple[str, ...], agent: AgentVersion, pause: PauseRecord
    ) -> None:
        tenant = pause.tenant_id
        slack = self._registry.get(tenant).slack
        text = notice_text(agent.name, pause)
        for person in await self._recipients(tenant, config_admins, agent.agent_id):
            key = ids.audience_pause_notice_key(
                tenant, agent.agent_id, pause.channel, pause.paused_at.isoformat(), person
            )
            if await self._idem.get(tenant, key) is not None:
                continue
            channel = await slack.open_dm(person)
            ts = await slack.post_message(channel, text)
            await self._idem.put_if_absent(tenant, key, {"ts": ts})


def _is_user_id(value: str) -> bool:
    """A Slack user ID, so an imported agent's placeholder creator is not messaged."""
    return len(value) > 2 and value[0] in "UW" and value.replace("_", "").isalnum()


def notice_text(agent_name: str, pause: PauseRecord) -> str:
    """What the owner and admins are told. IDs and a fixed reason, never content."""
    return (
        f"{escape_slack(agent_name)} is paused in <#{pause.channel}> because "
        f"{REASON_TEXT[pause.reason]}. It will not answer there until the owner or an admin "
        "reviews who can see the channel and resumes it in the console."
    )
