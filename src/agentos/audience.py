"""Who may use an agent: the audience rules, the Slack lookups behind them and the
people counts an approver confirms at publish.

Rules kept here:

* Identity, place and agent come from the runtime: the Slack user ID of the sender,
  the channel and its kind from the event, the agent of the thread. Nothing here
  reads anything the model said.
* A channel message needs the channel in the audience. A direct message needs
  ``dm`` to be "everyone", or the sender to be in one of the listed user groups.
* A guest (single or multi-channel) is refused unless the audience sets
  ``guests``. A person from another Slack workspace (Slack Connect) and a
  deactivated user are always refused. "Everyone" is every full member of the
  tenant's own workspace.
* A definition with no audience gets the legacy default, so agents published before
  audiences existed keep working: every full member by direct message, the channels
  the tenant file routes to the agent (a default agent also answers in channels the
  file does not map, a front door's specialists in the front door's channels), and no
  guests.
* A lookup that fails raises ``SlackLookupError``; the caller refuses. Nothing is
  allowed because the answer could not be had. A successful lookup is cached for
  ``CACHE_TTL_SECONDS`` per tenant in the process: long enough that a busy channel
  costs a handful of Slack calls per user instead of one per message, and short
  enough (Slack rate limits are per minute) that a person removed from a user group,
  made a guest or deactivated is refused within five minutes.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol

from agentos.agent_health import UNAVAILABLE as HEALTH_UNAVAILABLE
from agentos.agent_health import HealthStore, agent_unavailable_text
from agentos.agent_store import AgentStore
from agentos.audience_pauses import PauseStore
from agentos.config import AgentsConfig, AgentVersion
from agentos.slack_client import SlackLookupError, SlackPoster, SlackUser
from agentos.slack_format import escape_slack
from agentos.switches import SwitchStore, agent_off_text, read_state
from agentos.tenants import TenantRegistry

log = logging.getLogger("agentos.audience")

CACHE_TTL_SECONDS = 300

# Why an asker was refused. These are the only values the audit log and the
# workflow see.
OK = "ok"
CHANNEL = "channel"  # the channel is not in the audience
DM = "dm"  # the audience does not include this person by direct message
GUEST = "guest"
EXTERNAL = "external"
DEACTIVATED = "deactivated"
UNAVAILABLE = "unavailable"  # the lookups failed, so the asker was refused
OFF = "off"  # an admin turned the agent off
PAUSED = "paused"  # the agent is paused in this channel until its owner confirms the audience
HEALTH = "health"  # the agent's grant check failed, so it is unavailable until a check passes


@dataclass
class AudienceDecision:
    """Whether the asker may use the agent here. Travels through Temporal."""

    allowed: bool
    reason: str = OK
    # The agent's creator, who the refusal tells the asker to ask. Set on a refusal.
    owner: str | None = None
    # Whether the reply mentions the owner. Off when the owner was already mentioned today
    # for this asker in this place; the reply then names the owner without a mention.
    notify_owner: bool = True


# --- the audience in force -------------------------------------------------------------


@dataclass(frozen=True)
class EffectiveAudience:
    """An agent's audience as the rules apply it, explicit or the legacy default.

    ``channels`` are the channels that can be named. A legacy default agent that the
    tenant file makes the default also answers in channels the file does not map
    (``any_unmapped_channel``); ``mapped`` are the channels the file does map.
    """

    explicit: bool
    channels: tuple[str, ...]
    dm_everyone: bool
    dm_user_groups: tuple[str, ...]
    guests: bool
    any_unmapped_channel: bool = False
    mapped: tuple[str, ...] = ()

    def includes_channel(self, channel: str) -> bool:
        if channel in self.channels:
            return True
        return self.any_unmapped_channel and channel not in self.mapped


def effective_audience(
    agent: AgentVersion, config: AgentsConfig, published: dict[str, AgentVersion]
) -> EffectiveAudience:
    """The audience in force for ``agent``. ``published`` is the tenant's published set."""
    if agent.audience is not None:
        a = agent.audience
        return EffectiveAudience(
            True, tuple(a.channels), a.dm_everyone, tuple(a.dm_user_groups), a.guests
        )

    def reaches(routed_id: str) -> bool:
        if routed_id == agent.agent_id:
            return True
        routed = published.get(routed_id)
        return routed is not None and agent.agent_id in routed.specialists

    channels = tuple(c for c, routed in config.channels.items() if reaches(routed))
    return EffectiveAudience(
        explicit=False,
        channels=channels,
        dm_everyone=True,
        dm_user_groups=(),
        guests=False,
        any_unmapped_channel=reaches(config.default_agent),
        mapped=tuple(config.channels),
    )


def person_refusal(user: SlackUser, team_id: str, guests: bool) -> str | None:
    """Why this person is not let in whatever the place, or None."""
    if user.deleted:
        return DEACTIVATED
    if user.team_id != team_id:
        return EXTERNAL
    if user.is_guest and not guests:
        return GUEST
    return None


# --- Slack lookups, cached -------------------------------------------------------------


class SlackDirectory:
    """One tenant's Slack directory reads, cached for a short time.

    A failed read raises and is not cached. ``clock`` is a monotonic seconds clock,
    set by tests.
    """

    def __init__(
        self,
        slack: SlackPoster,
        ttl: float = CACHE_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._slack = slack
        self._ttl = ttl
        self._clock = clock
        self._users: dict[str, tuple[float, SlackUser]] = {}
        self._groups: dict[str, tuple[float, frozenset[str]]] = {}

    async def user(self, user_id: str) -> SlackUser:
        hit = self._users.get(user_id)
        if hit is not None and self._clock() < hit[0]:
            return hit[1]
        user = await self._slack.user_info(user_id)
        self._users[user_id] = (self._clock() + self._ttl, user)
        return user

    async def group_members(self, group_id: str) -> frozenset[str]:
        hit = self._groups.get(group_id)
        if hit is not None and self._clock() < hit[0]:
            return hit[1]
        members = frozenset(await self._slack.usergroup_members(group_id))
        self._groups[group_id] = (self._clock() + self._ttl, members)
        return members


class AudienceChecker:
    """Decides whether an asker may use an agent in a place, for one process.

    Reads the tenant's Slack client from the registry and the published agents from
    the store; both are the same objects the activities use.
    """

    def __init__(
        self,
        registry: TenantRegistry,
        agents: AgentStore,
        ttl: float = CACHE_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        pauses: PauseStore | None = None,
        switches: SwitchStore | None = None,
        health: HealthStore | None = None,
    ):
        self._registry = registry
        self._agents = agents
        self._pauses = pauses
        self._switches = switches
        self._health = health
        self._ttl = ttl
        self._clock = clock
        self._directories: dict[str, SlackDirectory] = {}

    def directory(self, tenant_id: str) -> SlackDirectory:
        if tenant_id not in self._directories:
            slack = self._registry.get(tenant_id).slack
            self._directories[tenant_id] = SlackDirectory(slack, self._ttl, self._clock)
        return self._directories[tenant_id]

    async def decide(
        self,
        tenant_id: str,
        agent: AgentVersion,
        user_id: str,
        channel: str,
        channel_kind: str,
        published: dict[str, AgentVersion] | None = None,
    ) -> AudienceDecision:
        """May ``user_id`` use ``agent`` in ``channel``? Raises ``SlackLookupError``."""
        config = self._registry.get(tenant_id).config
        if agent.audience is None and published is None:
            published = await self._agents.published_all(tenant_id)
        audience = effective_audience(agent, config, published or {})
        in_channel = channel_kind != "im"
        if in_channel and not audience.includes_channel(channel):
            return AudienceDecision(False, CHANNEL)
        paused = in_channel and self._pauses is not None
        if paused and await self._pauses.get_active(tenant_id, agent.agent_id, channel):
            return AudienceDecision(False, PAUSED)
        if not in_channel and not (audience.dm_everyone or audience.dm_user_groups):
            return AudienceDecision(False, DM)
        directory = self.directory(tenant_id)
        user = await directory.user(user_id)
        if reason := person_refusal(user, config.slack_team_id, audience.guests):
            return AudienceDecision(False, reason)
        if not in_channel and not audience.dm_everyone:
            for group in audience.dm_user_groups:
                if user_id in await directory.group_members(group):
                    return AudienceDecision(True)
            return AudienceDecision(False, DM)
        return AudienceDecision(True)

    async def reachable(
        self,
        tenant_id: str,
        agents: list[AgentVersion],
        user_id: str,
        channel: str,
        channel_kind: str,
    ) -> list[AgentVersion]:
        """The agents among ``agents`` this asker may use here; a failed lookup keeps none.

        An agent an admin turned off, or one whose grant check failed, is never kept,
        so a thread is not offered to it.
        """
        published = await self._agents.published_all(tenant_id)
        state = None if self._switches is None else await read_state(self._switches, tenant_id)
        unavailable = (
            set()
            if self._health is None
            else {
                h.agent_id
                for h in await self._health.list(tenant_id)
                if h.status == HEALTH_UNAVAILABLE
            }
        )
        kept = []
        for agent in agents:
            if state is not None and state.agent_stopped_by(agent.agent_id) is not None:
                continue
            if agent.agent_id in unavailable:
                continue
            try:
                decision = await self.decide(
                    tenant_id, agent, user_id, channel, channel_kind, published
                )
            except SlackLookupError as err:
                log.warning(
                    "audience lookup failed tenant_id=%s agent_id=%s code=%s",
                    tenant_id,
                    agent.agent_id,
                    err.code,
                )
                continue
            if decision.allowed:
                kept.append(agent)
        return kept


# --- what the asker is told ------------------------------------------------------------

_USER_ID = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


def _mention(owner: str | None) -> str:
    """A mention of the owner, or a plain fallback when it is not a Slack user ID."""
    if owner and owner[0] in "UW" and len(owner) > 2 and set(owner) <= _USER_ID:
        return f"<@{owner}>"
    return "a workspace admin"


def refusal_text(agent_name: str, owner: str | None, notify_owner: bool = True) -> str:
    """The one fixed reply to an asker outside the audience. The name is escaped.

    With ``notify_owner`` off the owner is named without a mention, which would page them.
    """
    who = _mention(owner) if notify_owner or not owner else "the agent's owner"
    return f"{escape_slack(agent_name)} isn't available to you here. Ask {who} for access."


def unavailable_text(agent_name: str) -> str:
    """The reply when the lookups failed and the asker could not be checked."""
    return (
        f"I couldn't check whether {escape_slack(agent_name)} is available to you just now. "
        "Try again in a few minutes."
    )


def paused_text(agent_name: str) -> str:
    """The one fixed reply in a channel where the agent is paused. The name is escaped."""
    return (
        f"{escape_slack(agent_name)} is paused in this channel until its owner reviews "
        "who can see it."
    )


def no_longer_act_text(agent_name: str) -> str:
    """What a card says when a pause or a lost audience stopped an approved write."""
    return f"{agent_name} can no longer act for you here; nothing was changed."


def decision_text(agent_name: str, decision: AudienceDecision) -> str:
    """The fixed reply that tells the asker why the turn did not run."""
    if decision.reason == UNAVAILABLE:
        return unavailable_text(agent_name)
    if decision.reason == PAUSED:
        return paused_text(agent_name)
    if decision.reason == OFF:
        return agent_off_text(agent_name)
    if decision.reason == HEALTH:
        return agent_unavailable_text(agent_name)
    return refusal_text(agent_name, decision.owner, decision.notify_owner)


# --- refusals of the control plane -----------------------------------------------------


class AudienceError(Exception):
    """A publish was refused because of the version's audience."""


class ChannelOutsideAudience(AudienceError):
    """A channel the tenant file routes to the agent is not in its audience."""


class AudienceNotConfirmed(AudienceError):
    """The approver did not confirm the people who can reach the agent, or the people changed.

    The message is the API's machine-readable refusal. ``counts`` is the fresh count the
    confirmation must match, ``place`` is ``agent`` for a publish or ``channel`` for a
    resume, and ``changed`` says a confirmation was given but no longer matches; the
    console renders its own sentence from these, never by rewriting the message.
    """

    def __init__(
        self,
        message: str,
        *,
        counts: dict[str, int] | None = None,
        place: str = "agent",
        changed: bool = False,
    ):
        super().__init__(message)
        self.counts = counts
        self.place = place
        self.changed = changed


class AudienceCheckUnavailable(AudienceError):
    """Slack could not be read, so the people who can reach the agent were not counted."""


# --- the people an audience reaches, for the approver ---------------------------------


@dataclass
class PlaceCount:
    """People in one place: a channel ID, or "dm"."""

    place: str
    total: int
    guests: int
    external: int


@dataclass
class AudienceSummary:
    """Who can reach an agent, computed live from Slack.

    ``total`` counts every distinct person in the places: for a channel its members,
    for direct messages everyone (guests only when the audience lets them in) or the
    user groups' members. ``guests`` and ``external`` are how many of them are
    guests and Slack Connect people. A guest in a channel is counted though the rules
    refuse one unless guests are allowed, and so is an external person who is never
    admitted: the count is what the approver is shown, and it can only overstate.
    """

    places: list[PlaceCount] = field(default_factory=list)
    total: int = 0
    guests: int = 0
    external: int = 0

    def confirmation(self) -> dict[str, int]:
        return {"total": self.total, "guests": self.guests, "external": self.external}


class AudienceInspector(Protocol):
    async def summary(
        self, tenant_id: str, audience: EffectiveAudience, fresh: bool = True
    ) -> AudienceSummary:
        """Count the people who can reach an agent. Raises ``SlackLookupError``."""
        ...


class SlackAudienceInspector:
    """Counts an audience's people with the tenant's own Slack client.

    ``fresh=False`` may reuse the workspace list read in the last ``ttl`` seconds
    (the console page); a publish always reads again. Reading the workspace
    (``users.list``) is one call per 200 users and the heavy part for a large
    workspace; channel members come from ``conversations.members``.
    """

    def __init__(
        self,
        slack_for: Callable[[str], Awaitable[SlackPoster]],
        team_for: Callable[[str], str],
        ttl: float = CACHE_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._slack_for = slack_for
        self._team_for = team_for
        self._ttl = ttl
        self._clock = clock
        self._workspace: dict[str, tuple[float, dict[str, SlackUser]]] = {}

    async def _users(self, tenant_id: str, fresh: bool) -> dict[str, SlackUser]:
        hit = self._workspace.get(tenant_id)
        if not fresh and hit is not None and self._clock() < hit[0]:
            return hit[1]
        slack = await self._slack_for(tenant_id)
        listed = {u.id: u for u in await slack.list_users()}
        self._workspace[tenant_id] = (self._clock() + self._ttl, listed)
        return listed

    async def summary(
        self, tenant_id: str, audience: EffectiveAudience, fresh: bool = True
    ) -> AudienceSummary:
        slack = await self._slack_for(tenant_id)
        team = self._team_for(tenant_id)
        users: dict[str, SlackUser] | None = None
        everyone: dict[str, SlackUser] = {}

        async def known(user_id: str) -> SlackUser:
            nonlocal users
            if users is None:
                users = await self._users(tenant_id, fresh)
            return users.get(user_id) or await slack.user_info(user_id)

        def is_person(user: SlackUser) -> bool:
            return not user.deleted and not user.is_bot

        places: list[PlaceCount] = []
        reached: dict[str, SlackUser] = {}

        def count(place: str, people: list[SlackUser]) -> None:
            for person in people:
                reached[person.id] = person
            places.append(
                PlaceCount(
                    place,
                    len(people),
                    sum(1 for p in people if p.is_guest),
                    sum(1 for p in people if p.team_id != team),
                )
            )

        for channel in audience.channels:
            people = [await known(m) for m in await slack.channel_members(channel)]
            count(channel, [p for p in people if is_person(p)])

        if audience.dm_everyone or audience.dm_user_groups:
            if audience.dm_everyone:
                users = await self._users(tenant_id, fresh)
                everyone = {u.id: u for u in users.values() if is_person(u)}
            for group in audience.dm_user_groups:
                for member in await slack.usergroup_members(group):
                    person = await known(member)
                    if is_person(person):
                        everyone[person.id] = person
            people = [
                p
                for p in everyone.values()
                if p.team_id == team and (audience.guests or not p.is_guest)
            ]
            count("dm", people)

        return AudienceSummary(
            places,
            len(reached),
            sum(1 for p in reached.values() if p.is_guest),
            sum(1 for p in reached.values() if p.team_id != team),
        )
