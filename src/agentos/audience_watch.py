"""Watching audience changes: decide from a Slack event which agents to pause, and tell
the people accountable for them.

Rules kept here:

* Triggers are a guest or a person from another Slack workspace joining a channel, a
  channel being shared with another organisation, and a private channel being made
  public. An ordinary full member joining, or a bot, changes nothing.
* A joiner is looked up with a fresh ``users.info`` every time, never from the cache the
  turn checks use: a person made a guest a minute ago must not be missed.
* The agents paused are the published agents whose effective audience names the channel,
  and only in that channel. Direct messages and other channels are untouched.
* Every effect happens once per pause, however often the activity runs: the pause row is
  opened once, its audit event and each notice have keys built from when the pause began.
* The owner (creator) of the agent and the tenant's admins get a direct message that
  names the agent, the channel and the reason. It carries no content.
* A joiner who cannot be looked up pauses the agent too (``UNVERIFIED``): nothing is
  allowed because the answer could not be had.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime

from temporalio import activity
from temporalio.exceptions import ApplicationError

from agentos import ids
from agentos.agent_store import AgentStore
from agentos.audience import effective_audience
from agentos.audience_pauses import (
    CHANNEL_SHARED,
    EXTERNAL_JOINED,
    GUEST_JOINED,
    MADE_PUBLIC,
    REASON_TEXT,
    UNVERIFIED,
    PauseRecord,
    PauseStore,
)
from agentos.audit import AuditStore
from agentos.config import AgentVersion
from agentos.members import ADMIN, MemberStore
from agentos.models import JOINED, PUBLIC, SHARED, AudienceEvent, AuditEvent
from agentos.slack_client import SlackLookupError
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
        return [self.watch_audience_event, self.pause_unverified]

    @activity.defn(name="watch_audience_event")
    async def watch_audience_event(self, event: AudienceEvent) -> list[str]:
        """Pause the agents this event puts at risk; return their IDs.

        A failed lookup of the joiner is retried when transient and otherwise fails the
        activity, which the workflow answers with ``pause_unverified``.
        """
        log.info(
            "activity=watch_audience_event tenant_id=%s kind=%s channel=%s",
            event.tenant_id,
            event.kind,
            event.channel,
        )
        reason = await self._reason(event)
        if reason is None:
            return []
        return await self._pause_agents(event, reason)

    @activity.defn(name="pause_unverified")
    async def pause_unverified(self, event: AudienceEvent) -> list[str]:
        """Pause the agents a join may have put at risk, without looking the joiner up."""
        log.info(
            "activity=pause_unverified tenant_id=%s channel=%s", event.tenant_id, event.channel
        )
        return await self._pause_agents(event, UNVERIFIED)

    async def _reason(self, event: AudienceEvent) -> str | None:
        if event.kind == SHARED:
            return CHANNEL_SHARED
        if event.kind == PUBLIC:
            return MADE_PUBLIC
        if event.kind != JOINED or not event.user_id:
            raise ApplicationError(f"unknown audience event {event.kind!r}", non_retryable=True)
        runtime = self._registry.get(event.tenant_id)
        try:
            user = await runtime.slack.user_info(event.user_id)
        except SlackLookupError as err:
            log.warning(
                "joiner lookup failed tenant_id=%s channel=%s code=%s",
                event.tenant_id,
                event.channel,
                err.code,
            )
            if err.transient:
                raise
            raise ApplicationError(str(err), type="SlackLookupError", non_retryable=True) from err
        if user.is_bot or user.deleted:
            return None
        if user.team_id != runtime.config.slack_team_id:
            return EXTERNAL_JOINED
        if user.is_guest:
            return GUEST_JOINED
        return None

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
                requester_user_id=event.user_id or IDENTITY,
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
