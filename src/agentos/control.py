"""The control plane: who may change agents and members, and the record of it.

Every method takes the tenant and the ``actor``, a Slack user ID that the caller
got from a verified identity and never from a request body. A method raises
``Forbidden`` when the actor's role does not allow the action, and otherwise
makes the change and writes one audit event for it. This is the only module
besides the stores themselves that calls their write methods. No test checks
that; it holds by construction: the route handlers in ``control_api`` and ``web_ui``
are handed the ``ControlPlane`` and no write store, so they can only change state
through it. Two things change state without passing through here,
each on purpose: the host operator's command ``python -m agentos.switches``, which must
work with the console down and has no tenant file or role to check (it calls the same
``switches.set_switch`` in its own audited transaction, as ``operator:<login>``), and the
one-time imports of the YAML file (``agent_store`` and ``tenant_store``), which run
before any admin exists. Opening a pause is the Slack event handler's (``audience_watch``).

Roles. A tenant has admins and creators.

- An admin approves every publish (which also covers a wider grant: a grant
  reaches users only by being published), rejects pending versions, manages
  members and reads the audit log.
- A creator reads agents, creates agents, and drafts, edits, submits and
  withdraws the versions of an agent they created. An admin may do the same for
  any agent.
- An admin reads usage and cost for every agent; a creator reads it for the agents
  they created. Usage is never broken down by requester.
- An admin reads any agent's feedback page; a creator reads the feedback of the agents
  they created. The page shows counts, outcomes and votes, and a question's text only
  while retention keeps it — never who asked or who voted.
- An admin sets, raises and removes monthly budgets (one for the tenant, one per agent). An
  admin reads all of them; a creator reads the budgets of the agents they created, and
  never sets one. Every change is audited as ``budget_set`` with the new amount in
  ``target`` (``0`` and ``tool=remove`` for a removal).
- A user listed in the tenant's ``admins`` setting is always an admin. Nobody can
  remove or demote that user here, because it is configuration. A user with a
  ``members`` row has that row's role. Anyone else has no role.

An admin may publish a version they submitted themselves, so a tenant with one
admin can publish. The ``agent_publish`` event records the submitter as the
requester and the approver, so a self-approval can be seen.

A publish also checks the version's Notion database grants against the live
databases, with the tenant's Notion client, and is refused when any grant has a
problem. The editors of an agent and admins can run the same check by itself.

A caller may pass a ``request_id`` to a write. It is used in place of a random
value to build the audit event key, so a retry with the same ``request_id``
records one event. The store's own state check refuses a retried change, except for a new
draft, which has no state to refuse it: a retried ``new_draft`` finds the event its first
attempt recorded and returns that draft instead of creating a second one.

Approving is approving a content. A version carries the SHA-256 of the definition it was
submitted with (``definition_sha256``). ``publish_version`` takes the hash the approver was
shown (``reviewed_sha256``) and refuses with ``VersionChanged`` when the version carries
another, which a withdraw, an edit and a resubmit under the same number produces. The
``agent_publish`` event records that reviewed hash as its ``input_sha256``.

A publish also checks the version's audience. A channel the tenant file routes to the
agent must be in the audience, or the publish is refused. When the plane has a Slack
people counter, the publish needs the approver's confirmation of the people who can
reach the agent: ``confirm_audience`` must equal a fresh count (total, guests,
external), and an ``audience_confirm`` event records it with the publish. A plane
built without a counter does not ask: only tests do that.

Audience changes are watched (see ``audience_watch.py``): an agent paused in a channel
stays paused until ``resume_channel``, which the agent's creator or an admin calls with
the fresh count of the people in that channel, like a publish. Reading the paused
channels (``paused_channels``) is for admins and the agent's editors. Resuming is audited
as ``audience_resume``.

Kill switches (see ``switches.py``): an admin turns an agent, every agent, a connector
or all writes off and on; each change is audited as ``switch_off`` or ``switch_on``. An
admin reads every switch. A creator reads the state of the agents they created and the
tenant-wide switches that stop them, and never changes one.

Tenant settings (the admin settings page, ``tenant_settings``): an admin sets the default
agent and the channel routing, validated like the YAML import (a published agent, channels
inside the agent's audience), audited as ``tenant_settings``; connects the tenant's Notion
workspace (the OAuth workspace bot token stored encrypted as the ``notion`` credential,
audited as ``connector``, never the token) and disconnects it; and reveals, confirms or
clears the Notion webhook verification token exactly as the host command does, audited as
``webhook_token`` as the user instead of the operator. An admin also turns requester
approval off or on per low-risk action (``policy_change``): a comment on an item the
requester filed may then run without a card (D-43). An admin connects the tenant's
Google Drive (``connect_drive``): the deployment's Google service account creates one
service account per tenant, and the connection row and its ``connector`` event commit
together (``drive_connect``); ``disconnect_drive`` deletes the account at Google and the
row (D-45). No key is stored per tenant.

Grants. The grant check (``check_version_grants``) gives an admin the full report: the live
title and parent of each granted object, the creator's label and any mismatch, and the
problems. A creator gets ok or a problem category per grant and nothing else (no property
names or types, no titles). The version review (``version_review``) adds the diff against
the published version. A version that widens access is published only with
``acknowledge_widening``; the acknowledgement is audited as ``widening_confirm``. With the
picker required (``require_grant_picker``), submit refuses a grant that is new since the
published version and was not chosen from the creator's own Notion (``record_grant_picks``,
audited as ``grant_pick``). A creator who consents keeps their Notion sign-in
(``store_creator_consent``, encrypted per tenant and creator, audited as ``connector``);
``revoke_creator_consent`` deletes it, and dropping a member or moving a creator to a role
that does not create deletes it in the same unit of work as the member change.
``creator_access_view`` reads the nightly verdicts the worker's check wrote
(``creator_access``).

A refused action is not audited yet.

Every change and its audit event are one unit of work (``unit_of_work.py``): one tenant
transaction, taken under the tenant's lock, with the change and the event on the same
connection. If the audit insert fails, the change is rolled back and the error is raised,
so the retry is not refused as "already done" and records exactly one event. Reads that
decide a change (the count of admins before a demotion) are made inside the unit.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from cryptography.exceptions import InvalidTag

from agentos import ids
from agentos.access_diff import AccessDiff, WideningNotAcknowledged, diff_access
from agentos.agent_feedback import (
    FEEDBACK_DAYS,
    AgentFeedbackStore,
    AgentFeedbackView,
    empty_view,
)
from agentos.agent_health import AgentHealth, HealthStore
from agentos.agent_store import (
    DRAFT,
    PENDING,
    AgentNotFound,
    AgentRecord,
    AgentStore,
    AgentStoreError,
    AgentVersionRecord,
    version_changed,
)
from agentos.approval_policy import (
    POLICIES,
    ApprovalPolicyStore,
    InMemoryApprovalPolicyStore,
)
from agentos.audience import (
    AudienceCheckUnavailable,
    AudienceInspector,
    AudienceNotConfirmed,
    AudienceSummary,
    ChannelOutsideAudience,
    EffectiveAudience,
    effective_audience,
)
from agentos.audience_pauses import InMemoryPauseStore, NotPaused, PauseRecord, PauseStore
from agentos.audit import AuditStore, canonical_sha256
from agentos.budgets import (
    AGENT_KINDS,
    Budget,
    BudgetLine,
    BudgetStore,
    BudgetsView,
    InMemoryBudgetStore,
    month_spend,
    parse_amount,
)
from agentos.config import (
    CHANNEL_ID,
    AgentsConfig,
    AgentVersion,
    ConfigError,
    TenantsConfig,
    channel_audience_problems,
    read_secret,
)
from agentos.connectors.google_iam import DriveItem, GoogleAccounts, GoogleError, problem_text
from agentos.connectors.manifest import low_risk_actions
from agentos.connectors.notion import normalize_id
from agentos.creator_access import AccessRow, CreatorAccessStore, InMemoryCreatorAccessStore
from agentos.credentials import CREATOR_USER_ID_PATTERN, NOTION, TenantCredentials
from agentos.crypto_keys import TenantKeyError
from agentos.drive_connections import (
    CONNECTED,
    ERROR,
    DriveConnection,
    DriveConnectionStore,
)
from agentos.grant_checker import (
    GrantChecker,
    GrantCheckUnavailable,
    GrantProblems,
    GrantReport,
)
from agentos.grant_picks import (
    GrantNotPicked,
    GrantPickStore,
    InMemoryGrantPickStore,
    PickedObject,
    PickRecord,
)
from agentos.members import (
    ADMIN,
    CREATOR,
    ROLES,
    InvalidMember,
    MemberError,
    MemberNotFound,
    MemberRecord,
    MemberStore,
)
from agentos.models import AuditEvent
from agentos.notion_picker import WorkspaceGrant
from agentos.notion_webhook import PENDING as WEBHOOK_PENDING
from agentos.notion_webhook import WebhookTokenError, WebhookTokens
from agentos.slack_client import SlackChannel, SlackLookupError, SlackPoster
from agentos.switches import (
    InMemorySwitchStore,
    SwitchesView,
    SwitchStore,
    read_state,
    set_switch,
)
from agentos.tenant_store import TenantSource, TenantStore, as_source
from agentos.test_sets import (
    CITE,
    EXPECT_KINDS,
    FAILED,
    HAND_OFF,
    QUESTION_MAX,
    REFUSE,
    RUNNING,
    WRITE,
    InMemoryTestSetStore,
    TestCase,
    TestRun,
    TestSetError,
    TestSetStore,
    case_set_sha256,
)
from agentos.unit_of_work import InMemoryUnitOfWork, UnitOfWork
from agentos.usage import (
    InMemoryUsageStore,
    UsageReport,
    UsageStore,
    build_report,
    month_bounds,
    utcnow,
)


class Forbidden(Exception):
    """The actor's role does not allow the action."""


@dataclass(frozen=True)
class Principal:
    """Who is acting, resolved once per request by ``ControlPlane.principal``.

    ``role`` is None for a person with no role. ``owned_agent_ids`` holds the agents a creator
    made; an admin reads every agent, so it is empty for them. A plane call that takes an
    ``actor`` takes this in place of the user ID and then looks nothing up again.
    """

    tenant_id: str
    user_id: str
    role: str | None
    owned_agent_ids: frozenset[str] = frozenset()

    def sees(self, agent_id: str) -> bool:
        """Whether this person may read the agent: an admin any, a creator their own."""
        return self.role == ADMIN or agent_id in self.owned_agent_ids


Actor = str | Principal


@dataclass
class AudienceView:
    """What the console shows of a version's audience: the rules in force and who they reach.

    ``summary`` is None when the plane counts no people or Slack could not be read;
    ``problem`` then says why.
    """

    effective: EffectiveAudience
    summary: AudienceSummary | None = None
    problem: str | None = None


@dataclass
class PausedChannel:
    """A paused channel as the console shows it, with the people in the channel just now.

    ``summary`` is None when the plane counts no people or Slack could not be read;
    ``problem`` then says why.
    """

    pause: PauseRecord
    summary: AudienceSummary | None = None
    problem: str | None = None


@dataclass
class TenantSettingsView:
    """What the admin settings page shows: routing, the Notion connection, the webhook token.

    ``published`` are the agents a default or a channel may name. ``slack_channels`` is
    None when Slack could not be read (``slack_problem`` says why); the page still renders.
    ``notion_connected_by``/``at`` come from the tenant's newest ``connector`` connect
    event. ``notion_env_var`` names the environment variable in use when no credential
    is connected; None when Notion is connected or the tenant has no token at all.
    ``health`` holds the worker's last check of each published agent, by agent ID.
    ``approvals`` holds the tenant's approval policy for each low-risk action (D-43).
    ``drive_configured`` is False on a deployment without the Google settings; ``drive``
    is the tenant's connection row as stored (None when never connected), ``drive_items``
    what its account can see now (None when the live list failed or was not run),
    ``drive_more`` when more than the first 100 items are shared, and ``drive_problem``
    the plain words for the failed live check or the row's last error. The GET writes
    nothing (D-45).
    """

    default_agent: str | None
    channels: dict[str, str]
    published: list[AgentVersion]
    slack_channels: list[SlackChannel] | None = None
    slack_problem: str | None = None
    notion_connected: bool = False
    notion_workspace_id: str | None = None
    notion_workspace_name: str | None = None
    notion_connected_by: str | None = None
    notion_connected_at: datetime | None = None
    notion_env_var: str | None = None
    webhook_state: str | None = None
    # The tenant's health rows by agent ID; an agent with no row was never checked.
    health: dict[str, AgentHealth] = field(default_factory=dict)
    # The approval policy of each low-risk action, by action name.
    approvals: dict[str, str] = field(default_factory=dict)
    drive_configured: bool = False
    drive: DriveConnection | None = None
    drive_items: list[DriveItem] | None = None
    drive_more: bool = False
    drive_problem: str | None = None


def _slack_problem(err: Exception) -> str:
    """A reason fit to show: the Slack error code or the configuration message, no secrets."""
    if isinstance(err, SlackLookupError):
        scope = f" (needs {err.scope})" if err.scope else ""
        return f"Slack said '{err.code}'{scope}"
    return str(err)


def _no_config(agent: AgentVersion) -> EffectiveAudience:
    """A tenant with no file section has no routing, so only an explicit audience is known."""
    a = agent.audience
    if a is None:
        return EffectiveAudience(False, (), True, (), False)
    return EffectiveAudience(
        True, tuple(a.channels), a.dm_everyone, tuple(a.dm_user_groups), a.guests
    )


def _channel_only(pause: PauseRecord) -> EffectiveAudience:
    """An audience of one channel and no direct messages, to count the people in it."""
    return EffectiveAudience(True, (pause.channel,), False, (), True)


@dataclass
class VersionReview:
    """What an approver (or the creator) is shown about a version's access.

    ``reports`` are the live grant reports, only for an admin and only while the version is a
    draft or pending; ``live_problem`` says why there are none when Notion could not be read.
    ``diff`` compares with the published version. ``picks`` are the agent's picks by
    normalised Notion ID.
    """

    reports: list[GrantReport] | None = None
    live_problem: str | None = None
    diff: AccessDiff | None = None
    picks: dict[str, PickRecord] = field(default_factory=dict)


@dataclass
class TestRunReview:
    """A version's latest test run, and whether what it ran against changed since.

    ``stale`` compares the two hashes the run recorded with the version now: its
    definition and its case set (ids and expectations). An edit of either after the
    run makes the results old; a withdraw and resubmit alone changes neither.
    """

    run: TestRun | None
    stale: bool = False


def _all_grant_ids(agent: AgentVersion) -> dict[str, str]:
    """Normalised ID -> label of every Notion grant of a version."""
    ids_: dict[str, str] = {}
    for grant in (*agent.grants.notion_databases, *agent.grants.notion_pages):
        ids_[normalize_id(grant.id)] = grant.name
    return ids_


class ControlPlane:
    def __init__(
        self,
        agents: AgentStore,
        members: MemberStore,
        audit: AuditStore,
        tenants: TenantsConfig | TenantSource,
        grant_checker: GrantChecker,
        audience: AudienceInspector | None = None,
        usage: UsageStore | None = None,
        budgets: BudgetStore | None = None,
        clock: Callable[[], datetime] = utcnow,
        pauses: PauseStore | None = None,
        switches: SwitchStore | None = None,
        unit_of_work: UnitOfWork | None = None,
        grant_picks: GrantPickStore | None = None,
        require_grant_picker: bool = False,
        credentials: TenantCredentials | None = None,
        tenant_store: TenantStore | None = None,
        webhook_tokens: WebhookTokens | None = None,
        slack_for: Callable[[str], Awaitable[SlackPoster]] | None = None,
        health: HealthStore | None = None,
        creator_access: CreatorAccessStore | None = None,
        feedback_reads: AgentFeedbackStore | None = None,
        policies: ApprovalPolicyStore | None = None,
        test_sets: TestSetStore | None = None,
        start_test_run: Callable[[str, str], Awaitable[None]] | None = None,
        drive: DriveConnectionStore | None = None,
        google: GoogleAccounts | None = None,
    ):
        self._picks = InMemoryGrantPickStore() if grant_picks is None else grant_picks
        self._policies = InMemoryApprovalPolicyStore() if policies is None else policies
        self._test_sets = InMemoryTestSetStore() if test_sets is None else test_sets
        # Starts one run's workflow (tenant, run ID); idempotent. None leaves runs unavailable.
        self._start_test_run = start_test_run
        self._creator_access = (
            InMemoryCreatorAccessStore() if creator_access is None else creator_access
        )
        self._require_picker = require_grant_picker
        self._switches = InMemorySwitchStore() if switches is None else switches
        self._pauses = InMemoryPauseStore() if pauses is None else pauses
        self._audience = audience
        self._usage = InMemoryUsageStore() if usage is None else usage
        self._budgets = InMemoryBudgetStore() if budgets is None else budgets
        self._clock = clock
        self._agents = agents
        self._members = members
        self._audit = audit
        self._tenants = as_source(tenants)
        self._grant_checker = grant_checker
        # Tenant settings: None leaves the settings page's writes unconfigured.
        self._credentials = credentials
        self._tenant_store = tenant_store
        self._webhook_tokens = webhook_tokens
        self._slack_for = slack_for
        # Google Drive (D-45): either one None leaves Drive not set up on this console.
        self._drive = drive
        self._google = google
        # Agent health is read-only here: the worker's checks write it. None leaves the
        # console without health to show.
        self._health = health
        # The feedback page's reads are read-only here too: the worker and the gateway
        # write the rows. None leaves the page with empty sections.
        self._feedback_reads = feedback_reads
        # Stores in Postgres need a PostgresUnitOfWork; the default covers the in-memory ones.
        settings_stores = [
            s for s in (credentials, tenant_store, webhook_tokens, drive) if s is not None
        ]
        self._uow = (
            InMemoryUnitOfWork(
                agents,
                members,
                audit,
                self._budgets,
                self._switches,
                self._pauses,
                self._picks,
                self._policies,
                self._test_sets,
                *settings_stores,
            )
            if unit_of_work is None
            else unit_of_work
        )

    # --- roles ---------------------------------------------------------------

    async def _config_admins(self, tenant_id: str) -> tuple[str, ...]:
        config = await self._tenants.get(tenant_id)
        return () if config is None else config.admins

    async def role_of(self, tenant_id: str, user_id: str) -> str | None:
        """The user's role: a configured admin is an admin, else the members row, else None."""
        return await self._role(tenant_id, user_id, await self._config_admins(tenant_id))

    async def _role(
        self, tenant_id: str, user_id: str, config_admins: tuple[str, ...], conn: Any = None
    ) -> str | None:
        if user_id in config_admins:
            return ADMIN
        row = await self._members.get_member(tenant_id, user_id, conn=conn)
        return None if row is None else row.role

    async def principal(self, tenant_id: str, user_id: str) -> Principal:
        """The caller's role and, for a creator, their agents: the lookups a request needs, once."""
        role = await self.role_of(tenant_id, user_id)
        owned: frozenset[str] = frozenset()
        if role == CREATOR:
            owned = frozenset(
                a.agent_id
                for a in await self._agents.list_agents(tenant_id)
                if a.created_by == user_id
            )
        return Principal(tenant_id, user_id, role, owned)

    async def _principal(self, tenant_id: str, actor: Actor) -> Principal:
        if isinstance(actor, Principal):
            if actor.tenant_id != tenant_id:
                raise Forbidden(f"'{actor.user_id}' may not do this in tenant '{tenant_id}'")
            return actor
        return await self.principal(tenant_id, actor)

    async def _require(self, tenant_id: str, actor: Actor, *allowed: str) -> Principal:
        who = await self._principal(tenant_id, actor)
        if who.role not in allowed:
            raise Forbidden(f"'{who.user_id}' may not do this in tenant '{tenant_id}'")
        return who

    async def _require_role(self, tenant_id: str, actor: Actor, *allowed: str) -> str:
        who = await self._require(tenant_id, actor, *allowed)
        assert who.role is not None
        return who.role

    async def _require_editor(self, tenant_id: str, actor: Actor, agent_id: str) -> str:
        """An admin, or the creator of this agent. Returns the actor's role."""
        who = await self._require(tenant_id, actor, ADMIN, CREATOR)
        agent = await self._agents.get_agent(tenant_id, agent_id)
        if agent is None:
            raise AgentNotFound(f"agent '{agent_id}' does not exist")
        if who.role != ADMIN and agent.created_by != who.user_id:
            raise Forbidden(f"'{who.user_id}' did not create agent '{agent_id}'")
        assert who.role is not None
        return who.role

    async def require_agent_editor(self, tenant_id: str, actor: Actor, agent_id: str) -> None:
        """Raise ``Forbidden`` unless the actor is an admin or created this agent."""
        await self._require_editor(tenant_id, actor, agent_id)

    async def _admin_count(
        self, tenant_id: str, config_admins: tuple[str, ...], conn: Any = None
    ) -> int:
        rows = await self._members.list_members(tenant_id, conn=conn)
        admins = set(config_admins)
        admins.update(m.user_id for m in rows if m.role == ADMIN)
        return len(admins)

    # --- audit ---------------------------------------------------------------

    async def _record(
        self,
        kind: str,
        tenant_id: str,
        actor: str,
        *,
        version: AgentVersionRecord | None = None,
        agent_id: str | None = None,
        requester: str | None = None,
        approver: str | None = None,
        target: str | None = None,
        tool: str | None = None,
        request_id: str | None = None,
        conn: Any = None,
        input_sha256: str | None = None,
    ) -> None:
        """One event, on the unit of work's connection. A definition is hashed, never stored.

        ``input_sha256`` is the hash the version was submitted (and so reviewed) with, or the
        canonical hash of the definition while it is still a draft. A caller may name its
        own hash instead (a test case's content, never the text).
        """
        digest = input_sha256
        if digest is None and version is not None:
            digest = version.definition_sha256 or canonical_sha256(version.definition)
        await self._audit.record(
            AuditEvent(
                tenant_id=tenant_id,
                event_key=self._event_key(tenant_id, kind, request_id),
                kind=kind,
                agent_id=agent_id if version is None else version.agent_id,
                agent_version=None if version is None else version.version,
                requester_user_id=requester or actor,
                identity=f"user:{actor}",
                tool=tool,
                target=target,
                input_sha256=digest,
                approver_user_id=approver,
            ),
            conn=conn,
        )

    @staticmethod
    def _event_key(tenant_id: str, kind: str, request_id: str | None) -> str:
        return ids.audit_control_key(tenant_id, kind, request_id or uuid.uuid4().hex)

    # --- reads ---------------------------------------------------------------

    async def list_agents(self, tenant_id: str, actor: Actor) -> list[AgentRecord]:
        """Every agent for an admin; the agents they created for a creator."""
        who = await self._require(tenant_id, actor, ADMIN, CREATOR)
        return [a for a in await self._agents.list_agents(tenant_id) if who.sees(a.agent_id)]

    async def agent_health(self, tenant_id: str, actor: Actor) -> dict[str, AgentHealth]:
        """The health rows of the agents this person may read, by agent ID.

        An admin reads every agent's, a creator their own. The status is what the
        agents list badges; the reason is shown only on the admin settings page.
        A plane built without a health store has no rows.
        """
        who = await self._require(tenant_id, actor, ADMIN, CREATOR)
        if self._health is None:
            return {}
        return {h.agent_id: h for h in await self._health.list(tenant_id) if who.sees(h.agent_id)}

    async def agent_feedback(
        self, tenant_id: str, actor: Actor, agent_id: str, days: int = FEEDBACK_DAYS[0]
    ) -> AgentFeedbackView:
        """The agent's feedback page: outcomes, votes and questions of the last ``days``.

        An admin reads any agent's, a creator their own; another creator's agent is
        refused the same as one that does not exist. The view never names who asked
        or who voted, and shows a question's text only while retention keeps it.
        A plane built without a feedback store has empty sections.
        """
        who = await self._require(tenant_id, actor, ADMIN, CREATOR)
        agent = await self._agents.get_agent(tenant_id, agent_id)
        if agent is None or not who.sees(agent_id):
            raise AgentNotFound(f"agent '{agent_id}' does not exist")
        since = self._clock() - timedelta(days=days)
        if self._feedback_reads is None:
            return empty_view(agent_id, since)
        return await self._feedback_reads.read(tenant_id, agent_id, since)

    async def versions(
        self, tenant_id: str, actor: Actor, agent_id: str
    ) -> list[AgentVersionRecord]:
        """An agent's versions. A creator gets none for an agent that is not theirs, the same
        as for one that does not exist."""
        who = await self._require(tenant_id, actor, ADMIN, CREATOR)
        if not who.sees(agent_id):
            return []
        return await self._agents.versions(tenant_id, agent_id)

    async def get_version(
        self, tenant_id: str, actor: Actor, agent_id: str, version: int
    ) -> AgentVersionRecord | None:
        who = await self._require(tenant_id, actor, ADMIN, CREATOR)
        if not who.sees(agent_id):
            return None
        return await self._agents.get_version(tenant_id, agent_id, version)

    async def list_members(self, tenant_id: str, actor: str) -> list[MemberRecord]:
        """The member rows. A configured admin has no row; see ``role_of``."""
        await self._require_role(tenant_id, actor, ADMIN)
        return await self._members.list_members(tenant_id)

    async def audit_log(
        self,
        tenant_id: str,
        actor: str,
        limit: int = 50,
        before: datetime | None = None,
        before_seq: int | None = None,
    ) -> list[AuditEvent]:
        await self._require_role(tenant_id, actor, ADMIN)
        return await self._audit.list(tenant_id, limit, before, before_seq)

    async def usage_report(
        self, tenant_id: str, actor: Actor, month: str | None = None
    ) -> UsageReport:
        """Usage and cost for a UTC calendar month (``YYYY-MM``; default the current one).

        An admin sees every agent and the tenant's embedding usage. A creator sees
        only the agents they created, which is the agents they may edit. There is no
        per-person view: no row or total here is grouped by who asked.
        """
        who = await self._require(tenant_id, actor, ADMIN, CREATOR)
        start, end, label = month_bounds(month, self._clock())
        groups = await self._usage.totals(tenant_id, start, end)
        if who.role != ADMIN:
            groups = [g for g in groups if g.agent_id in who.owned_agent_ids]
        teams: dict[tuple[str, int], str | None] = {}
        for agent_id, version in {(g.agent_id, g.agent_version) for g in groups if g.agent_id}:
            record = await self._agents.get_version(tenant_id, agent_id, version)
            teams[(agent_id, version)] = None if record is None else (record.agent().team or None)
        return build_report(label, groups, teams)

    async def budgets(self, tenant_id: str, actor: Actor) -> BudgetsView:
        """Each agent's budget against the spend of the current UTC month.

        An admin gets the tenant-wide line and every agent; a creator the agents they
        created and no tenant line. An agent with no budget is listed with no limit.
        """
        who = await self._require(tenant_id, actor, ADMIN, CREATOR)
        start, end, label = month_bounds(None, self._clock())
        spend = month_spend(await self._usage.totals(tenant_id, start, end))
        rows = {b.agent_id: b for b in await self._budgets.list(tenant_id)}
        agents = [a for a in await self._agents.list_agents(tenant_id) if who.sees(a.agent_id)]
        lines = [
            BudgetLine.of(a.agent_id, rows.get(a.agent_id), spend.of(a.agent_id), AGENT_KINDS)
            for a in sorted(agents, key=lambda a: a.agent_id)
        ]
        tenant = BudgetLine.of(None, rows.get(None), spend.tenant) if who.role == ADMIN else None
        return BudgetsView(label, tenant, lines)

    async def switches(self, tenant_id: str, actor: Actor) -> SwitchesView:
        """Every switch and its state. An admin sees every agent; a creator their own."""
        who = await self._require(tenant_id, actor, ADMIN, CREATOR)
        agents = [a for a in await self._agents.list_agents(tenant_id) if who.sees(a.agent_id)]
        state = await read_state(self._switches, tenant_id)
        return SwitchesView.build(state, sorted(a.agent_id for a in agents))

    async def check_version_grants(
        self, tenant_id: str, actor: Actor, agent_id: str, version: int
    ) -> list[GrantReport]:
        """Check a version's Notion grants. Read-only; editors and admins.

        An admin gets the full report. Anyone else gets ok or a problem category per grant
        and nothing about the objects (schema, property names, titles).
        """
        role = await self._require_editor(tenant_id, actor, agent_id)
        record = await self._agents.get_version(tenant_id, agent_id, version)
        if record is None:
            raise AgentNotFound(f"agent '{agent_id}' has no version {version}")
        reports = await self._grant_checker.check(tenant_id, record.agent())
        return reports if role == ADMIN else [r.without_detail() for r in reports]

    async def version_review(
        self, tenant_id: str, actor: Actor, agent_id: str, version: int
    ) -> VersionReview:
        """The access review of a version for an editor or admin (see ``VersionReview``)."""
        role = await self._require_editor(tenant_id, actor, agent_id)
        record = await self._agents.get_version(tenant_id, agent_id, version)
        if record is None:
            raise AgentNotFound(f"agent '{agent_id}' has no version {version}")
        review = VersionReview(picks=await self._picks.for_agent(tenant_id, agent_id))
        if record.status not in ("draft", PENDING):
            return review
        review.diff = await self._access_diff(tenant_id, agent_id, record.agent())
        if role == ADMIN:
            try:
                review.reports = await self._grant_checker.check(tenant_id, record.agent())
            except GrantCheckUnavailable:
                review.live_problem = "Notion could not be read just now"
        return review

    async def _access_diff(self, tenant_id: str, agent_id: str, new: AgentVersion) -> AccessDiff:
        published = await self._agents.published(tenant_id, agent_id)
        old_audience = None if published is None else await self._effective(tenant_id, published)
        diff = diff_access(published, new, old_audience, await self._effective(tenant_id, new))
        diff.published = "none" if published is None else str(published.version)
        return diff

    async def audience_view(
        self, tenant_id: str, actor: Actor, agent_id: str, version: int, count: bool = True
    ) -> AudienceView:
        """The audience in force for a version and, when ``count``, the people it reaches.

        Any role that reads versions. Slack is read live; a failure is reported in ``problem`` and
        does not fail the page.
        """
        who = await self._require(tenant_id, actor, ADMIN, CREATOR)
        record = (
            await self._agents.get_version(tenant_id, agent_id, version)
            if who.sees(agent_id)
            else None
        )
        if record is None:
            raise AgentNotFound(f"agent '{agent_id}' has no version {version}")
        effective = await self._effective(tenant_id, record.agent())
        if not count or self._audience is None:
            return AudienceView(effective)
        try:
            return AudienceView(
                effective, await self._audience.summary(tenant_id, effective, False)
            )
        except (SlackLookupError, ConfigError) as err:
            return AudienceView(effective, problem=_slack_problem(err))

    async def paused_channels(
        self, tenant_id: str, actor: Actor, agent_id: str | None = None, count: bool = True
    ) -> list[PausedChannel]:
        """The channels where agents are paused, each with the people in it just now.

        An admin reads every agent's; a creator reads the agents they created. With
        ``agent_id`` the actor must be an admin or that agent's creator. Slack is read
        live (the workspace list may be a few minutes old); a failure is reported in
        ``problem`` and does not fail the read.
        """
        if agent_id is not None:
            await self._require_editor(tenant_id, actor, agent_id)
            pauses = await self._pauses.list_active(tenant_id, agent_id)
        else:
            who = await self._require(tenant_id, actor, ADMIN, CREATOR)
            pauses = [p for p in await self._pauses.list_active(tenant_id) if who.sees(p.agent_id)]
        views = []
        for pause in pauses:
            if not count or self._audience is None:
                views.append(PausedChannel(pause))
                continue
            try:
                summary = await self._audience.summary(tenant_id, _channel_only(pause), False)
            except (SlackLookupError, ConfigError) as err:
                views.append(PausedChannel(pause, problem=_slack_problem(err)))
            else:
                views.append(PausedChannel(pause, summary))
        return views

    async def resume_channel(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        channel: str,
        confirm_audience: dict[str, int] | None = None,
        request_id: str | None = None,
    ) -> PauseRecord:
        """End a pause. The agent's creator or an admin, who confirms who is in the channel.

        ``confirm_audience`` must equal a fresh count of the channel's people (total,
        guests, external), as a publish asks. A plane built without a counter does not
        ask: only tests do that.
        """
        await self._require_editor(tenant_id, actor, agent_id)
        pause = await self._pauses.get_active(tenant_id, agent_id, channel)
        if pause is None:
            raise NotPaused(f"agent '{agent_id}' is not paused in channel '{channel}'")
        counts = ""
        if self._audience is not None:
            summary = await self._confirmed_channel(tenant_id, pause, confirm_audience)
            c = summary.confirmation()
            counts = f" total={c['total']} guests={c['guests']} external={c['external']}"
        published = await self._agents.published(tenant_id, agent_id)
        version = (
            None
            if published is None
            else await self._agents.get_version(tenant_id, agent_id, published.version)
        )
        async with self._uow.begin(tenant_id) as conn:
            closed = await self._pauses.close_pause(
                tenant_id, agent_id, channel, actor, self._clock(), conn=conn
            )
            if closed is None:
                raise NotPaused(f"agent '{agent_id}' is not paused in channel '{channel}'")
            await self._record(
                "audience_resume",
                tenant_id,
                actor,
                version=version,
                agent_id=agent_id,
                approver=actor,
                target=f"{channel}{counts}",
                tool=pause.reason,
                request_id=request_id,
                conn=conn,
            )
        return closed

    async def _confirmed_channel(
        self, tenant_id: str, pause: PauseRecord, confirm: dict[str, int] | None
    ) -> AudienceSummary:
        """The fresh count of the paused channel, once the confirmation matches it."""
        assert self._audience is not None
        try:
            summary = await self._audience.summary(tenant_id, _channel_only(pause), True)
        except (SlackLookupError, ConfigError) as err:
            raise AudienceCheckUnavailable(
                f"could not count the people in this channel: {_slack_problem(err)}. "
                "Nothing was resumed; try again."
            ) from err
        now = summary.confirmation()
        said = (
            f"{now['total']} people are in this channel ({now['guests']} guests, "
            f"{now['external']} from other workspaces)"
        )
        if confirm is None:
            raise AudienceNotConfirmed(
                f"{said}. Confirm that they may see what this agent says here: send "
                f"confirm_audience {json.dumps(now)}.",
                counts=now,
                place="channel",
            )
        if confirm != now:
            raise AudienceNotConfirmed(
                f"The people in this channel changed since they were counted: {said}. "
                "Review them and confirm again.",
                counts=now,
                place="channel",
                changed=True,
            )
        return summary

    async def _effective(self, tenant_id: str, agent: AgentVersion) -> EffectiveAudience:
        config = await self._tenants.get(tenant_id)
        if config is None or agent.audience is not None:
            published: dict[str, AgentVersion] = {}
        else:
            published = await self._agents.published_all(tenant_id)
        return effective_audience(agent, config, published) if config else _no_config(agent)

    async def _check_mapped_channels(self, tenant_id: str, agent: AgentVersion) -> None:
        config = await self._tenants.get(tenant_id)
        if config is None:
            return
        if problems := channel_audience_problems(config.channels, {agent.agent_id: agent}):
            raise ChannelOutsideAudience(
                f"cannot publish agent '{agent.agent_id}': {problems[0]}. Add the channel to "
                "the audience, or take it out of the tenant file's channels."
            )

    async def _confirmed_audience(
        self, tenant_id: str, agent: AgentVersion, confirm: dict[str, int] | None
    ) -> AudienceSummary:
        """The fresh count, once the approver's confirmation matches it."""
        assert self._audience is not None
        effective = await self._effective(tenant_id, agent)
        try:
            summary = await self._audience.summary(tenant_id, effective, True)
        except (SlackLookupError, ConfigError) as err:
            raise AudienceCheckUnavailable(
                f"could not count the people who can reach this agent: {_slack_problem(err)}. "
                "Nothing was published; try again."
            ) from err
        now = summary.confirmation()
        said = (
            f"{now['total']} people can reach this agent ({now['guests']} guests, "
            f"{now['external']} from other workspaces)"
        )
        if confirm is None:
            raise AudienceNotConfirmed(
                f"{said}. Confirm that they may see what this agent can access: send "
                f"confirm_audience {json.dumps(now)}.",
                counts=now,
                place="agent",
            )
        if confirm != now:
            raise AudienceNotConfirmed(
                f"The people who can reach this agent changed since they were counted: {said}. "
                "Review them and confirm again.",
                counts=now,
                place="agent",
                changed=True,
            )
        return summary

    # --- agents --------------------------------------------------------------

    async def add_agent(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        definition: dict[str, Any],
        request_id: str | None = None,
    ) -> AgentVersionRecord:
        """Create an agent as version 1, a draft, with the actor as its creator."""
        await self._require_role(tenant_id, actor, ADMIN, CREATOR)
        async with self._uow.begin(tenant_id) as conn:
            record = await self._agents.create_agent(
                tenant_id, agent_id, definition, actor, conn=conn
            )
            await self._record(
                "agent_create", tenant_id, actor, version=record, request_id=request_id, conn=conn
            )
        return record

    async def new_draft(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        from_version: int | None = None,
        request_id: str | None = None,
    ) -> AgentVersionRecord:
        """A new draft copied from ``from_version``, or from the published version.

        With a ``request_id`` the call is idempotent: a retry finds the event the first
        attempt recorded and returns that draft, so it never makes a second one.
        """
        await self._require_editor(tenant_id, actor, agent_id)
        async with self._uow.begin(tenant_id) as conn:
            if request_id is not None:
                key = self._event_key(tenant_id, "agent_draft", request_id)
                seen = await self._audit.get(tenant_id, key, conn=conn)
                if seen is not None:
                    first = (
                        None
                        if seen.agent_id != agent_id or seen.agent_version is None
                        else await self._agents.get_version(
                            tenant_id, agent_id, seen.agent_version, conn=conn
                        )
                    )
                    if first is None:
                        raise AgentStoreError("this request ID was used for another change")
                    return first
            record = await self._agents.create_draft(
                tenant_id, agent_id, actor, from_version, conn=conn
            )
            # The new draft carries the source version's test cases, as it carries its
            # definition. create_draft found a source, so there is one to copy from.
            source = from_version
            if source is None:
                published = await self._agents.published(tenant_id, agent_id)
                source = None if published is None else published.version
            if source is not None:
                await self._test_sets.copy_cases(
                    tenant_id, agent_id, source, record.version, conn=conn
                )
            await self._record(
                "agent_draft", tenant_id, actor, version=record, request_id=request_id, conn=conn
            )
        return record

    async def edit_draft(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        version: int,
        definition: dict[str, Any],
        request_id: str | None = None,
    ) -> AgentVersionRecord:
        await self._require_editor(tenant_id, actor, agent_id)
        async with self._uow.begin(tenant_id) as conn:
            record = await self._agents.replace_draft(
                tenant_id, agent_id, version, definition, conn=conn
            )
            await self._record(
                "agent_edit", tenant_id, actor, version=record, request_id=request_id, conn=conn
            )
        return record

    async def submit_version(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        version: int,
        request_id: str | None = None,
    ) -> AgentVersionRecord:
        await self._require_editor(tenant_id, actor, agent_id)
        await self._require_picked(tenant_id, agent_id, version)
        async with self._uow.begin(tenant_id) as conn:
            record = await self._agents.submit(tenant_id, agent_id, version, actor, conn=conn)
            await self._record(
                "agent_submit", tenant_id, actor, version=record, request_id=request_id, conn=conn
            )
        return record

    async def _require_picked(
        self, tenant_id: str, agent_id: str, version: int, action: str = "submit again"
    ) -> None:
        """With the picker required, a grant new since the published version must have been
        chosen from the creator's own Notion. Grants an admin already approved are kept.

        Checked at submit and again at a test run: a grant picked after the last submit
        still needs its pick before a run reads with it. ``action`` names what to retry.
        """
        if not self._require_picker:
            return
        record = await self._agents.get_version(tenant_id, agent_id, version)
        if record is None:
            return  # the store refuses it with its own message
        published = await self._agents.published(tenant_id, agent_id)
        approved = set() if published is None else set(_all_grant_ids(published))
        picked = await self._picks.for_agent(tenant_id, agent_id)
        missing = {
            nid: label
            for nid, label in _all_grant_ids(record.agent()).items()
            if nid not in approved and nid not in picked
        }
        if missing:
            names = ", ".join(f"'{label}' ({nid})" for nid, label in missing.items())
            raise GrantNotPicked(
                f"not chosen from your Notion: {names}. Use 'Choose from Notion' on the "
                f"version page, then {action}."
            )

    async def record_grant_picks(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        version: int,
        objects: list[PickedObject],
        request_id: str | None = None,
    ) -> None:
        """Remember that the actor chose these objects from their own Notion for this agent.

        ``objects`` must come from a listing made with the actor's own Notion token
        (``notion_picker``); the plane trusts its caller for that, as for the actor.
        """
        await self._require_editor(tenant_id, actor, agent_id)
        record = await self._agents.get_version(tenant_id, agent_id, version)
        if record is None:
            raise AgentNotFound(f"agent '{agent_id}' has no version {version}")
        if not objects:
            return
        async with self._uow.begin(tenant_id) as conn:
            await self._picks.record(tenant_id, agent_id, objects, actor, self._clock(), conn=conn)
            await self._record(
                "grant_pick",
                tenant_id,
                actor,
                version=record,
                target=",".join(normalize_id(o.id) for o in objects)[:300],
                request_id=request_id,
                conn=conn,
            )

    async def withdraw_version(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        version: int,
        request_id: str | None = None,
    ) -> AgentVersionRecord:
        await self._require_editor(tenant_id, actor, agent_id)
        async with self._uow.begin(tenant_id) as conn:
            record = await self._agents.withdraw(tenant_id, agent_id, version, conn=conn)
            await self._record(
                "agent_withdraw",
                tenant_id,
                actor,
                version=record,
                request_id=request_id,
                conn=conn,
            )
        return record

    async def reject_version(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        version: int,
        request_id: str | None = None,
    ) -> AgentVersionRecord:
        """Send a pending version back to draft. Only an admin."""
        await self._require_role(tenant_id, actor, ADMIN)
        async with self._uow.begin(tenant_id) as conn:
            record = await self._agents.withdraw(tenant_id, agent_id, version, conn=conn)
            await self._record(
                "agent_reject", tenant_id, actor, version=record, request_id=request_id, conn=conn
            )
        return record

    async def publish_version(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        version: int,
        request_id: str | None = None,
        confirm_audience: dict[str, int] | None = None,
        reviewed_sha256: str | None = None,
        acknowledge_widening: bool = False,
        reviewed_published: str | None = None,
    ) -> AgentVersionRecord:
        """Publish a pending version. Only an admin, who may be its submitter.

        ``reviewed_sha256`` is the hash of the definition the admin was shown (the
        version's ``definition_sha256`` on the page or in the API record). It is refused
        with ``VersionChanged`` when the version carries another. The console and the API
        always pass it; without one, the version pending when the checks begin is the one
        published, and it must still be that one when the change is made.

        A version that widens access compared with the published one (see ``access_diff``)
        is refused with ``WideningNotAcknowledged`` unless ``acknowledge_widening`` is true.
        ``reviewed_published`` is the published version number (or ``"none"``) the approver's
        diff was computed against; another one is refused with ``VersionChanged``. The console
        and the API always pass it; without one the current published version is used.
        """
        await self._require_role(tenant_id, actor, ADMIN)
        pending = await self._agents.get_version(tenant_id, agent_id, version)
        confirmed: AudienceSummary | None = None
        widening: AccessDiff | None = None
        if pending is not None and pending.status == PENDING:
            if reviewed_sha256 is None:
                reviewed_sha256 = pending.definition_sha256
            elif pending.definition_sha256 != reviewed_sha256:
                raise version_changed(agent_id, version)
            # Any other status is refused by the store below, with its own reason.
            await self._check_mapped_channels(tenant_id, pending.agent())
            reports = await self._grant_checker.check(tenant_id, pending.agent())
            if not all(report.ok for report in reports):
                raise GrantProblems(reports)
            if self._audience is not None:
                confirmed = await self._confirmed_audience(
                    tenant_id, pending.agent(), confirm_audience
                )
            diff = await self._access_diff(tenant_id, agent_id, pending.agent())
            if reviewed_published is not None and reviewed_published != diff.published:
                raise version_changed(agent_id, version)
            if diff.widens:
                if not acknowledge_widening:
                    raise WideningNotAcknowledged(
                        "this version widens access: "
                        + "; ".join(c.text() for c in diff.changes if c.widens)
                        + ". Review it and publish again with acknowledge_widening."
                    )
                widening = diff
        async with self._uow.begin(tenant_id) as conn:
            record = await self._agents.publish(
                tenant_id, agent_id, version, actor, reviewed_sha256=reviewed_sha256, conn=conn
            )
            await self._record(
                "agent_publish",
                tenant_id,
                actor,
                version=record,
                requester=record.submitted_by,
                approver=actor,
                request_id=request_id,
                conn=conn,
            )
            if widening is not None:
                await self._record(
                    "widening_confirm",
                    tenant_id,
                    actor,
                    version=record,
                    requester=record.submitted_by,
                    approver=actor,
                    target=f"{sum(c.widens for c in widening.changes)} widening change(s)",
                    request_id=request_id,
                    conn=conn,
                )
            if confirmed is not None:
                counts = confirmed.confirmation()
                await self._record(
                    "audience_confirm",
                    tenant_id,
                    actor,
                    version=record,
                    requester=record.submitted_by,
                    approver=actor,
                    target=f"total={counts['total']} guests={counts['guests']} "
                    f"external={counts['external']}",
                    request_id=request_id,
                    conn=conn,
                )
        return record

    # --- test sets -------------------------------------------------------------

    async def test_cases(
        self, tenant_id: str, actor: Actor, agent_id: str, version: int
    ) -> list[TestCase]:
        """The version's test cases, in the order they were added. Editors and admins."""
        await self._require_editor(tenant_id, actor, agent_id)
        return await self._test_sets.cases(tenant_id, agent_id, version)

    async def add_test_case(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        version: int,
        question: str,
        expect: str = "",
        target: str = "",
        request_id: str | None = None,
    ) -> TestCase:
        """Add one case to a draft. The expectation may be left unset (the feedback
        page's "Make a test case" does), for the creator to choose on the version page."""
        await self._require_editor(tenant_id, actor, agent_id)
        record = await self._draft_for_cases(tenant_id, agent_id, version)
        text = question.strip()
        if not text or len(text) > QUESTION_MAX:
            raise TestSetError(f"a test question must be 1 to {QUESTION_MAX} characters")
        expect, target = self._validated_expectation(expect, target, record.agent())
        case = TestCase(
            tenant_id,
            agent_id,
            version,
            ids.test_case_id(tenant_id, uuid.uuid4().hex),
            text,
            expect,
            target,
            created_by=actor,
        )
        async with self._uow.begin(tenant_id) as conn:
            await self._test_sets.add_case(tenant_id, case, conn=conn)
            await self._record(
                "test_case",
                tenant_id,
                actor,
                version=record,
                tool="add",
                target=case.case_id,
                request_id=request_id,
                conn=conn,
                input_sha256=canonical_sha256(
                    {"question": text, "expect": expect, "target": target}
                ),
            )
        return case

    async def set_case_expect(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        version: int,
        case_id: str,
        expect: str,
        target: str,
        request_id: str | None = None,
    ) -> None:
        """Choose one case's expected behaviour: a kind and the object it names."""
        await self._require_editor(tenant_id, actor, agent_id)
        record = await self._draft_for_cases(tenant_id, agent_id, version)
        expect, target = self._validated_expectation(expect, target, record.agent())
        if not expect:
            raise TestSetError("choose an expected behaviour for the case")
        async with self._uow.begin(tenant_id) as conn:
            found = await self._test_sets.set_expect(
                tenant_id, agent_id, version, case_id, expect, target, conn=conn
            )
            if not found:
                raise AgentNotFound(f"agent '{agent_id}' version {version} has no such test case")
            await self._record(
                "test_case",
                tenant_id,
                actor,
                version=record,
                tool="expect",
                target=case_id,
                request_id=request_id,
                conn=conn,
                input_sha256=canonical_sha256({"expect": expect, "target": target}),
            )

    async def remove_test_case(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        version: int,
        case_id: str,
        request_id: str | None = None,
    ) -> None:
        """Remove one case from a draft."""
        await self._require_editor(tenant_id, actor, agent_id)
        record = await self._draft_for_cases(tenant_id, agent_id, version)
        async with self._uow.begin(tenant_id) as conn:
            found = await self._test_sets.remove_case(
                tenant_id, agent_id, version, case_id, conn=conn
            )
            if not found:
                raise AgentNotFound(f"agent '{agent_id}' version {version} has no such test case")
            await self._record(
                "test_case",
                tenant_id,
                actor,
                version=record,
                tool="remove",
                target=case_id,
                request_id=request_id,
                conn=conn,
                input_sha256=canonical_sha256({"case_id": case_id}),
            )

    async def run_tests(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        version: int,
        request_id: str | None = None,
    ) -> TestRun:
        """Run a draft's cases against the draft definition. The agent's creator or an admin.

        Every case needs an expectation first, and a grant new since the published version
        must have been picked, as at submit. One run of a version at a time: a second start
        while one is running is refused. With a ``request_id`` the call is idempotent: a
        retry finds the event the first attempt recorded and returns that run. The checks
        and the row's insert share the unit of work's tenant lock, so two starts never
        interleave. The row and its audit event commit before the workflow starts on its
        own task queue; a start that fails closes the row as failed, so the console never
        shows a run that is not coming.
        """
        await self._require_editor(tenant_id, actor, agent_id)
        record = await self._draft_for_cases(tenant_id, agent_id, version)
        async with self._uow.begin(tenant_id) as conn:
            if request_id is not None:
                key = self._event_key(tenant_id, "test_run", request_id)
                seen = await self._audit.get(tenant_id, key, conn=conn)
                if seen is not None:
                    first = (
                        None
                        if not seen.target
                        else await self._test_sets.get_run(tenant_id, seen.target)
                    )
                    if first is None or first.agent_id != agent_id or first.version != version:
                        raise TestSetError("this request ID was used for another change")
                    return first
            cases = await self._test_sets.cases(tenant_id, agent_id, version)
            if not cases:
                raise TestSetError("this draft has no test cases yet")
            unset = sum(not c.expect for c in cases)
            if unset:
                raise TestSetError(
                    f"{unset} test case(s) have no expected behaviour yet; "
                    "choose one for each before running"
                )
            await self._require_picked(tenant_id, agent_id, version, "run the tests again")
            if self._start_test_run is None:
                raise TestSetError("test runs are not available in this console")
            latest = await self._test_sets.latest_run(tenant_id, agent_id, version)
            if latest is not None and latest.status == RUNNING:
                raise TestSetError("A test run is already going on this version")
            run = TestRun(
                tenant_id,
                ids.test_run_id(tenant_id, uuid.uuid4().hex),
                agent_id,
                version,
                RUNNING,
                actor,
                canonical_sha256(record.definition),
                case_set_sha256(cases),
            )
            await self._test_sets.start_run(tenant_id, run, conn=conn)
            await self._record(
                "test_run",
                tenant_id,
                actor,
                version=record,
                target=run.run_id,
                request_id=request_id,
                conn=conn,
            )
        try:
            await self._start_test_run(tenant_id, run.run_id)
        except Exception as err:  # any start failure closes the row and is shown
            await self._test_sets.finish_run(tenant_id, run.run_id, [], Decimal(0), status=FAILED)
            raise TestSetError(
                f"the test run could not start ({type(err).__name__}); try again"
            ) from err
        return run

    async def test_run_review(
        self, tenant_id: str, actor: Actor, agent_id: str, version: int
    ) -> TestRunReview:
        """The version's latest run for the version page and the approving admin's view:
        the run itself, and whether the definition or the case set changed since it ran."""
        await self._require_editor(tenant_id, actor, agent_id)
        record = await self._agents.get_version(tenant_id, agent_id, version)
        if record is None:
            raise AgentNotFound(f"agent '{agent_id}' has no version {version}")
        run = await self._test_sets.latest_run(tenant_id, agent_id, version)
        stale = run is not None and (
            run.definition_sha256 != canonical_sha256(record.definition)
            or run.cases_sha256
            != case_set_sha256(await self._test_sets.cases(tenant_id, agent_id, version))
        )
        return TestRunReview(run, stale)

    async def make_test_case_from_feedback(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        conversation_id: str,
        question_seq: int,
        request_id: str | None = None,
    ) -> AgentVersionRecord:
        """Add a stored feedback question to the agent's next draft, expectation unset.

        The next draft is the agent's newest one; with none, a draft is made from the
        published version, carrying its cases forward. The question is read from stored
        history: a user message of a conversation an audit row ties to this agent. One
        retention has purged, another agent's conversation, or a message that is not the
        asker's is refused. Returns the draft the case landed on.
        """
        await self._require_editor(tenant_id, actor, agent_id)
        question = (
            None
            if self._feedback_reads is None
            else await self._feedback_reads.question_text(
                tenant_id, agent_id, conversation_id, question_seq
            )
        )
        if not question:
            raise TestSetError("that question is no longer stored, so it cannot become a test case")
        versions = await self._agents.versions(tenant_id, agent_id)
        draft = next((v for v in versions if v.status == DRAFT), None)
        if draft is None:
            draft = await self.new_draft(tenant_id, actor, agent_id, request_id=request_id)
        await self.add_test_case(
            tenant_id, actor, agent_id, draft.version, question, request_id=request_id
        )
        return draft

    async def _draft_for_cases(
        self, tenant_id: str, agent_id: str, version: int
    ) -> AgentVersionRecord:
        record = await self._agents.get_version(tenant_id, agent_id, version)
        if record is None:
            raise AgentNotFound(f"agent '{agent_id}' has no version {version}")
        if record.status != DRAFT:
            raise AgentStoreError(
                f"agent '{agent_id}' version {version} is {record.status}; "
                "test cases are written on a draft"
            )
        return record

    @staticmethod
    def _validated_expectation(expect: str, target: str, agent: AgentVersion) -> tuple[str, str]:
        """The case's expectation as stored: a known kind, and a target this draft grants
        (a page, a database) or lists (a specialist). Notion IDs are normalized."""
        expect, target = expect.strip(), target.strip()
        if expect and expect not in EXPECT_KINDS:
            raise TestSetError(f"unknown expected behaviour '{expect}'")
        if not expect or expect == REFUSE:
            return expect, ""
        if not target:
            raise TestSetError("choose the source, database or agent this case expects")
        if expect == CITE:
            granted = {normalize_id(g.id) for g in agent.grants.notion_pages}
            what = "a page granted to this draft"
        elif expect == WRITE:
            granted = {normalize_id(g.id) for g in agent.grants.notion_databases}
            what = "a database granted to this draft"
        else:
            granted = set(agent.specialists)
            what = "a specialist this draft lists"
        normalized = target if expect == HAND_OFF else normalize_id(target)
        if normalized not in granted:
            raise TestSetError(f"'{target}' is not {what}")
        return expect, normalized

    # --- budgets -------------------------------------------------------------

    async def set_budget(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str | None,
        amount: Any,
        request_id: str | None = None,
    ) -> Budget:
        """Set or raise the monthly budget of an agent, or of the tenant when ``agent_id`` is None.

        Only an admin. The amount is dollars and cents, refused rather than rounded.
        Setting the amount already in force changes nothing and records nothing.
        """
        await self._require_role(tenant_id, actor, ADMIN)
        amount = parse_amount(amount)
        if agent_id is not None and await self._agents.get_agent(tenant_id, agent_id) is None:
            raise AgentNotFound(f"agent '{agent_id}' does not exist")
        async with self._uow.begin(tenant_id) as conn:
            existing = next(
                (b for b in await self._budgets.list(tenant_id) if b.agent_id == agent_id), None
            )
            if existing is not None and existing.amount == amount:
                return existing
            budget = await self._budgets.set_budget(tenant_id, agent_id, amount, actor, conn=conn)
            await self._record(
                "budget_set",
                tenant_id,
                actor,
                agent_id=agent_id,
                target=format(budget.amount, "f"),
                tool="set",
                request_id=request_id,
                conn=conn,
            )
        return budget

    async def remove_budget(
        self, tenant_id: str, actor: str, agent_id: str | None, request_id: str | None = None
    ) -> None:
        """Lift a budget, so the scope has no limit. Only an admin."""
        await self._require_role(tenant_id, actor, ADMIN)
        async with self._uow.begin(tenant_id) as conn:
            await self._budgets.remove_budget(tenant_id, agent_id, conn=conn)
            await self._record(
                "budget_set",
                tenant_id,
                actor,
                agent_id=agent_id,
                target="0",
                tool="remove",
                request_id=request_id,
                conn=conn,
            )

    # --- kill switches -------------------------------------------------------

    async def turn_off(
        self,
        tenant_id: str,
        actor: str,
        scope: str,
        target: str | None = None,
        request_id: str | None = None,
    ) -> bool:
        """Turn a switch off. Only an admin. True when it was on; off already changes nothing.

        It takes effect on the next step of every worker, with no restart.
        """
        await self._require_role(tenant_id, actor, ADMIN)
        return await self._change_switch(tenant_id, actor, scope, target, True, request_id)

    async def turn_on(
        self,
        tenant_id: str,
        actor: str,
        scope: str,
        target: str | None = None,
        request_id: str | None = None,
    ) -> bool:
        """Turn a switch back on. Only an admin. True when it was off."""
        await self._require_role(tenant_id, actor, ADMIN)
        return await self._change_switch(tenant_id, actor, scope, target, False, request_id)

    async def _change_switch(
        self,
        tenant_id: str,
        actor: str,
        scope: str,
        target: str | None,
        off: bool,
        request_id: str | None,
    ) -> bool:
        return await set_switch(
            self._switches,
            self._audit,
            self._agents,
            tenant_id,
            scope,
            target,
            off=off,
            actor=actor,
            identity=f"user:{actor}",
            at=self._clock(),
            request_id=request_id,
            uow=self._uow,
        )

    # --- tenant settings -----------------------------------------------------

    async def require_settings_admin(self, tenant_id: str, actor: Actor) -> None:
        """Raise ``Forbidden`` unless the actor is an admin of the tenant."""
        await self._require_role(tenant_id, actor, ADMIN)

    async def tenant_settings(self, tenant_id: str, actor: Actor) -> TenantSettingsView:
        """The settings page's reads, for an admin. A Slack read failure is reported, not fatal.

        The tenant row is read fresh from the store, not from the directory cache: a
        routing change shows on the page at once, even inside the cache's TTL.
        """
        await self._require(tenant_id, actor, ADMIN)
        config = await self._fresh_config(tenant_id)
        if config is None:
            raise ConfigError(f"tenant '{tenant_id}' is not available")
        published = sorted(
            (await self._agents.published_all(tenant_id)).values(), key=lambda a: a.agent_id
        )
        slack_channels: list[SlackChannel] | None = None
        slack_problem: str | None = None
        if self._slack_for is not None:
            try:
                slack = await self._slack_for(tenant_id)
                slack_channels = await slack.list_channels()
            except (SlackLookupError, ConfigError) as err:
                slack_problem = _slack_problem(err)
        connected = False
        connected_by: str | None = None
        connected_at: datetime | None = None
        if self._credentials is not None:
            connected = await self._credentials.notion_connected(tenant_id)
            if connected:
                event = await self._audit.latest(tenant_id, "connector", "connect")
                if event is not None:
                    connected_by, connected_at = event.requester_user_id, event.at
        env_var = config.secrets.notion_token_env
        notion_env_var = None if connected or not read_secret(env_var) else env_var
        webhook_state = (
            None if self._webhook_tokens is None else await self._webhook_tokens.state(tenant_id)
        )
        health = (
            {}
            if self._health is None
            else {h.agent_id: h for h in await self._health.list(tenant_id)}
        )
        approvals = {
            action: await self._policies.get(tenant_id, action) for action in low_risk_actions()
        }
        drive_configured = self._drive is not None and self._google is not None
        drive_row = None if self._drive is None else await self._drive.get(tenant_id)
        drive_items: list[DriveItem] | None = None
        drive_more = False
        drive_problem: str | None = None
        if drive_row is not None and self._google is not None:
            # A read of Google for this view only: its outcome is shown, never stored.
            # The row's status is written by connect and disconnect; a later check
            # polls it.
            try:
                listing = await self._google.list_shared(drive_row.service_account_email)
            except GoogleError as err:
                drive_problem = str(err)
            else:
                drive_items = listing.items
                drive_more = listing.more
        elif drive_row is not None and drive_row.status == ERROR:
            drive_problem = problem_text(drive_row.last_error)
        return TenantSettingsView(
            default_agent=config.default_agent,
            channels=dict(config.channels),
            published=published,
            slack_channels=slack_channels,
            slack_problem=slack_problem,
            notion_connected=connected,
            notion_workspace_id=config.notion_workspace_id,
            notion_workspace_name=config.notion_workspace_name,
            notion_connected_by=connected_by,
            notion_connected_at=connected_at,
            notion_env_var=notion_env_var,
            webhook_state=webhook_state,
            health=health,
            approvals=approvals,
            drive_configured=drive_configured,
            drive=drive_row,
            drive_items=drive_items,
            drive_more=drive_more,
            drive_problem=drive_problem,
        )

    async def _fresh_config(self, tenant_id: str) -> AgentsConfig | None:
        """The tenant's stored settings as they are now, or None when unknown or disabled.

        With a store, the row is read directly, bypassing the directory's cache; a plane
        without one (a fixed tenant set) answers from its source, which holds no cache.
        """
        if self._tenant_store is None:
            return await self._tenants.get(tenant_id)
        row = await self._tenant_store.load(tenant_id)
        return row.config if row is not None and row.enabled else None

    async def set_tenant_routing(
        self,
        tenant_id: str,
        actor: str,
        default_agent: str | None,
        channels: dict[str, str],
        request_id: str | None = None,
    ) -> None:
        """Set the default agent and the channel map. Only an admin.

        Validated like the YAML import: the default and every mapped agent must have a
        published version, and a mapped channel must be inside its agent's audience.
        Setting what is already in force changes nothing and records nothing. The
        directory refresh picks the change up without a restart.
        """
        await self._require_role(tenant_id, actor, ADMIN)
        if self._tenant_store is None:
            raise ConfigError("tenant settings cannot be changed on this console")
        # Fresh, not the directory cache: the no-op comparison must see a change this
        # page made moments ago, or an unchanged resubmit records a second event.
        config = await self._fresh_config(tenant_id)
        if config is None:
            raise ConfigError(f"tenant '{tenant_id}' is not available")
        published = await self._agents.published_all(tenant_id)
        default_agent = default_agent or None
        if default_agent is not None and default_agent not in published:
            raise ConfigError(f"default agent '{default_agent}' has no published version")
        for channel, agent_id in channels.items():
            if not CHANNEL_ID.fullmatch(channel):
                raise ConfigError(f"'{channel}' is not a Slack channel ID")
            if agent_id not in published:
                raise ConfigError(
                    f"channel '{channel}': agent '{agent_id}' has no published version"
                )
        if problems := channel_audience_problems(channels, published):
            raise ConfigError(problems[0])
        if default_agent == config.default_agent and channels == config.channels:
            return
        mapping = ",".join(f"{c}->{a}" for c, a in sorted(channels.items()))
        async with self._uow.begin(tenant_id) as conn:
            await self._tenant_store.set_routing(tenant_id, default_agent, channels, conn=conn)
            await self._record(
                "tenant_settings",
                tenant_id,
                actor,
                tool="routing",
                target=f"default={default_agent or '-'}; {mapping}"[:300],
                request_id=request_id,
                conn=conn,
            )

    async def set_approval_policy(
        self,
        tenant_id: str,
        actor: str,
        action: str,
        policy: str,
        request_id: str | None = None,
    ) -> None:
        """Set the tenant's approval policy for one low-risk action. Only an admin.

        The action must be one the manifests mark low-risk (a comment action), and
        the policy one of the two the store holds. Setting what is already in force
        changes nothing and records nothing; a real change and its ``policy_change``
        event commit in one unit of work, with the admin's identity.
        """
        await self._require_role(tenant_id, actor, ADMIN)
        if action not in low_risk_actions():
            raise ConfigError(f"'{action}' is not a low-risk action")
        if policy not in POLICIES:
            raise ConfigError(f"'{policy}' is not an approval policy")
        if await self._policies.get(tenant_id, action) == policy:
            return
        async with self._uow.begin(tenant_id) as conn:
            await self._policies.set(tenant_id, action, policy, conn=conn)
            await self._record(
                "policy_change",
                tenant_id,
                actor,
                tool=action,
                target=policy,
                request_id=request_id,
                conn=conn,
            )

    async def connect_notion(
        self,
        tenant_id: str,
        actor: str,
        grant: WorkspaceGrant,
        request_id: str | None = None,
    ) -> None:
        """Store the workspace bot token ``grant`` carries, encrypted. Only an admin.

        The token, the workspace on the tenant row and the ``connector`` event commit in
        one unit of work; a workspace another tenant already uses refuses the whole
        change. A reconnect replaces the credential and the workspace, and records again.
        """
        await self._require_role(tenant_id, actor, ADMIN)
        if self._credentials is None or self._tenant_store is None:
            raise ConfigError("Notion cannot be connected on this console")
        async with self._uow.begin(tenant_id) as conn:
            await self._credentials.store_with_current_key(
                tenant_id, NOTION, grant.token, conn=conn
            )
            await self._tenant_store.set_notion_workspace(
                tenant_id, grant.workspace_id, grant.workspace_name, conn=conn
            )
            await self._record(
                "connector",
                tenant_id,
                actor,
                tool="connect",
                target=grant.workspace_id,
                request_id=request_id,
                conn=conn,
            )

    async def disconnect_notion(
        self, tenant_id: str, actor: str, request_id: str | None = None
    ) -> bool:
        """Remove the tenant's Notion credential and workspace. Only an admin.

        False, and nothing changes or is recorded, when Notion is not connected.
        """
        await self._require_role(tenant_id, actor, ADMIN)
        if self._credentials is None or self._tenant_store is None:
            raise ConfigError("Notion cannot be connected on this console")
        async with self._uow.begin(tenant_id) as conn:
            deleted = await self._credentials.delete(tenant_id, NOTION, conn=conn)
            if not deleted:
                return False
            await self._tenant_store.set_notion_workspace(tenant_id, None, None, conn=conn)
            await self._record(
                "connector",
                tenant_id,
                actor,
                tool="disconnect",
                request_id=request_id,
                conn=conn,
            )
        return True

    async def connect_drive(
        self, tenant_id: str, actor: str, request_id: str | None = None
    ) -> None:
        """Create (or find) the tenant's Google service account and store the connection.
        Only an admin.

        The account ID is deterministic from the tenant (``ids.drive_account_id``), so a
        second click, a retry after a lost create response and an overlapping attempt all
        find the same account. The tenant's unit-of-work lock is held across the Google
        call and the row write, so a connect and a disconnect never interleave: the row
        and its ``connector`` event (``drive_connect``, the account email as target)
        commit in one unit of work after the call succeeds. A tenant already connected to
        the same account changes nothing and records nothing; a reconnect after an error
        records again. No key is created or stored for the tenant.
        """
        await self._require_role(tenant_id, actor, ADMIN)
        if self._drive is None or self._google is None:
            raise ConfigError("Google Drive is not set up on this deployment")
        async with self._uow.begin(tenant_id) as conn:
            email = await self._google.ensure_account(ids.drive_account_id(tenant_id))
            # Under the tenant lock: an overlapping attempt that committed first makes
            # this one a no-op, so two clicks record one event.
            existing = await self._drive.get(tenant_id, conn=conn)
            if (
                existing is not None
                and existing.status == CONNECTED
                and existing.service_account_email == email
            ):
                return
            await self._drive.connect(tenant_id, email, self._clock(), conn=conn)
            await self._record(
                "connector",
                tenant_id,
                actor,
                tool="drive_connect",
                target=email,
                request_id=request_id,
                conn=conn,
            )

    async def disconnect_drive(
        self, tenant_id: str, actor: str, request_id: str | None = None
    ) -> bool:
        """Delete the tenant's Google service account and its connection row. Only an admin.

        False, and nothing is recorded, when Drive is not connected; the derived account
        is still deleted at Google, so an account left by a connect whose row write failed
        does not stay orphaned. The delete targets the row's stored email — a project move
        must not orphan the old account — and a 404 (already gone, as after a lost delete
        response) still clears the row. The tenant's unit-of-work lock is held across the
        Google call and the row write, so a connect and a disconnect never interleave; the
        row's deletion and the ``connector`` event (``drive_disconnect``, the account email
        as target) commit in one unit of work.
        """
        await self._require_role(tenant_id, actor, ADMIN)
        if self._drive is None or self._google is None:
            raise ConfigError("Google Drive is not set up on this deployment")
        async with self._uow.begin(tenant_id) as conn:
            row = await self._drive.get(tenant_id, conn=conn)
            email = (
                row.service_account_email
                if row is not None
                else self._google.account_email(ids.drive_account_id(tenant_id))
            )
            await self._google.delete_account(email)
            if row is None:
                return False
            await self._drive.delete(tenant_id, conn=conn)
            await self._record(
                "connector",
                tenant_id,
                actor,
                tool="drive_disconnect",
                target=row.service_account_email,
                request_id=request_id,
                conn=conn,
            )
        return True

    async def creator_consent(self, tenant_id: str, actor: Actor) -> bool:
        """Whether the actor's Notion sign-in is kept for the nightly check. Reads no token."""
        who = await self._require(tenant_id, actor, ADMIN, CREATOR)
        if self._credentials is None:
            return False
        return await self._credentials.creator_consent(tenant_id, who.user_id)

    async def store_creator_consent(
        self, tenant_id: str, actor: str, token: str, request_id: str | None = None
    ) -> None:
        """Store the actor's own Notion sign-in encrypted, with the consent they gave.

        A consented pick replaces any sign-in already stored for them. The token and the
        ``connector`` event commit in one unit of work; the token is never logged or audited.
        A user ID outside the credential kind's class is refused before anything is stored.
        """
        await self._require_role(tenant_id, actor, ADMIN, CREATOR)
        if not CREATOR_USER_ID_PATTERN.fullmatch(actor):
            raise InvalidMember(f"'{actor}' is not a Slack user ID a sign-in can be kept for")
        if self._credentials is None:
            raise ConfigError("the Notion sign-in cannot be kept on this console")
        async with self._uow.begin(tenant_id) as conn:
            await self._credentials.store_with_current_key(
                tenant_id, ids.notion_creator_kind(actor), token, conn=conn
            )
            await self._record(
                "connector",
                tenant_id,
                actor,
                tool="creator_consent",
                target=actor,
                request_id=request_id,
                conn=conn,
            )

    async def revoke_creator_consent(
        self, tenant_id: str, actor: str, request_id: str | None = None
    ) -> bool:
        """Delete the actor's kept Notion sign-in. False, and nothing recorded, when none is."""
        await self._require_role(tenant_id, actor, ADMIN, CREATOR)
        if self._credentials is None:
            raise ConfigError("the Notion sign-in cannot be kept on this console")
        async with self._uow.begin(tenant_id) as conn:
            deleted = await self._credentials.delete(
                tenant_id, ids.notion_creator_kind(actor), conn=conn
            )
            if not deleted:
                return False
            await self._record(
                "connector",
                tenant_id,
                actor,
                tool="creator_consent_revoked",
                request_id=request_id,
                conn=conn,
            )
        return True

    async def _revoke_creator_signin(
        self, tenant_id: str, actor: str, user_id: str, conn: Any, request_id: str | None = None
    ) -> None:
        """Delete a member's kept Notion sign-in in the caller's unit of work, audited as a
        consent revoke with the member as the target. A member who never consented leaves
        nothing to delete and nothing to record."""
        if self._credentials is None:
            return
        deleted = await self._credentials.delete(
            tenant_id, ids.notion_creator_kind(user_id), conn=conn
        )
        if deleted:
            await self._record(
                "connector",
                tenant_id,
                actor,
                tool="creator_consent_revoked",
                target=user_id,
                request_id=request_id,
                conn=conn,
            )

    async def creator_access_view(
        self, tenant_id: str, actor: Actor, agent_id: str
    ) -> dict[str, AccessRow]:
        """The nightly creator-access verdicts for an agent's grants. An editor or an admin."""
        await self._require_editor(tenant_id, actor, agent_id)
        return await self._creator_access.for_agent(tenant_id, agent_id)

    def _require_webhook_tokens(self) -> WebhookTokens:
        if self._webhook_tokens is None:
            raise ConfigError("the webhook token cannot be managed on this console")
        return self._webhook_tokens

    @staticmethod
    def _webhook_nonce(command: str, request_id: str | None) -> str:
        # The host command's key shape, so a console step and a command step read alike.
        return f"{command}:{request_id or uuid.uuid4().hex}"

    async def reveal_webhook_token(
        self, tenant_id: str, actor: str, request_id: str | None = None
    ) -> str:
        """The pending webhook token, shown once to an admin, audited as the CLI's ``show``.

        Refused when there is no pending token or it cannot be decrypted; a refusal
        records nothing. The token goes to the page only, never to a log or the audit.
        """
        await self._require_role(tenant_id, actor, ADMIN)
        tokens = self._require_webhook_tokens()
        state = await tokens.state(tenant_id)
        if state is None:
            raise WebhookTokenError(
                f"no webhook token for tenant '{tenant_id}'; a handshake stores one"
            )
        if state != WEBHOOK_PENDING:
            raise WebhookTokenError(
                f"the webhook token of tenant '{tenant_id}' is already confirmed; "
                "clear it and re-subscribe for a new one"
            )
        try:
            token = await tokens.pending(tenant_id)
        except InvalidTag:
            raise WebhookTokenError(
                f"the webhook token of tenant '{tenant_id}' cannot be decrypted; "
                "clear it and re-subscribe"
            ) from None
        except TenantKeyError as err:
            raise WebhookTokenError(str(err)) from None
        if token is None:
            raise WebhookTokenError(
                f"no webhook token for tenant '{tenant_id}'; a handshake stores one"
            )
        async with self._uow.begin(tenant_id) as conn:
            await self._record(
                "webhook_token",
                tenant_id,
                actor,
                tool="show",
                request_id=self._webhook_nonce("show", request_id),
                conn=conn,
            )
        return token

    async def confirm_webhook_token(
        self, tenant_id: str, actor: str, request_id: str | None = None
    ) -> None:
        """Make the pending webhook token active, audited as the CLI's ``confirm``.

        Refused, and nothing recorded, when there is no pending token.
        """
        await self._require_role(tenant_id, actor, ADMIN)
        tokens = self._require_webhook_tokens()
        async with self._uow.begin(tenant_id) as conn:
            changed = await tokens.confirm(tenant_id, self._clock(), conn=conn)
            if changed:
                await self._record(
                    "webhook_token",
                    tenant_id,
                    actor,
                    tool="confirm",
                    request_id=self._webhook_nonce("confirm", request_id),
                    conn=conn,
                )
        if not changed:
            raise WebhookTokenError(f"nothing to confirm for tenant '{tenant_id}'")

    async def clear_webhook_token(
        self, tenant_id: str, actor: str, request_id: str | None = None
    ) -> None:
        """Remove the webhook token so a new handshake is accepted, audited as ``clear``.

        Refused, and nothing recorded, when there is no token.
        """
        await self._require_role(tenant_id, actor, ADMIN)
        tokens = self._require_webhook_tokens()
        async with self._uow.begin(tenant_id) as conn:
            changed = await tokens.clear(tenant_id, conn=conn)
            if changed:
                await self._record(
                    "webhook_token",
                    tenant_id,
                    actor,
                    tool="clear",
                    request_id=self._webhook_nonce("clear", request_id),
                    conn=conn,
                )
        if not changed:
            raise WebhookTokenError(f"nothing to clear for tenant '{tenant_id}'")

    # --- members -------------------------------------------------------------

    async def invite_member(
        self, tenant_id: str, actor: str, user_id: str, role: str, request_id: str | None = None
    ) -> None:
        await self._require_role(tenant_id, actor, ADMIN)
        if user_id in await self._config_admins(tenant_id):
            raise MemberError(f"'{user_id}' is an admin set in the tenant file")
        async with self._uow.begin(tenant_id) as conn:
            await self._members.add_member(tenant_id, user_id, role, actor, conn=conn)
            await self._record(
                "member_add",
                tenant_id,
                actor,
                target=user_id,
                tool=f"role:{role}",
                request_id=request_id,
                conn=conn,
            )

    async def change_role(
        self, tenant_id: str, actor: str, user_id: str, role: str, request_id: str | None = None
    ) -> None:
        await self._require_role(tenant_id, actor, ADMIN)
        if role not in ROLES:
            raise InvalidMember(f"role must be one of {', '.join(ROLES)}")
        configured = await self._config_admins(tenant_id)
        async with self._uow.begin(tenant_id) as conn:
            # Under the tenant's lock: who may act, the count and the change are one decision.
            if await self._role(tenant_id, actor, configured, conn) != ADMIN:
                raise Forbidden(f"'{actor}' may not do this in tenant '{tenant_id}'")
            current = await self._protected_member(tenant_id, user_id, configured, conn)
            if current.role == role:
                raise MemberError(f"'{user_id}' already has the role {role}")
            if current.role == ADMIN and await self._admin_count(tenant_id, configured, conn) <= 1:
                raise Forbidden("the tenant's last admin cannot be demoted")
            await self._members.set_member_role(tenant_id, user_id, role, conn=conn)
            await self._record(
                "member_role",
                tenant_id,
                actor,
                target=user_id,
                tool=f"role:{role}",
                request_id=request_id,
                conn=conn,
            )

    async def drop_member(
        self, tenant_id: str, actor: str, user_id: str, request_id: str | None = None
    ) -> None:
        await self._require_role(tenant_id, actor, ADMIN)
        configured = await self._config_admins(tenant_id)
        async with self._uow.begin(tenant_id) as conn:
            if await self._role(tenant_id, actor, configured, conn) != ADMIN:
                raise Forbidden(f"'{actor}' may not do this in tenant '{tenant_id}'")
            current = await self._protected_member(tenant_id, user_id, configured, conn)
            if current.role == ADMIN and await self._admin_count(tenant_id, configured, conn) <= 1:
                raise Forbidden("the tenant's last admin cannot be removed")
            await self._members.remove_member(tenant_id, user_id, conn=conn)
            await self._record(
                "member_remove", tenant_id, actor, target=user_id, request_id=request_id, conn=conn
            )
            await self._revoke_creator_signin(tenant_id, actor, user_id, conn, request_id)

    async def _protected_member(
        self, tenant_id: str, user_id: str, configured: tuple[str, ...], conn: Any = None
    ) -> MemberRecord:
        """The member's row, after refusing a configured admin, who is configuration."""
        if user_id in configured:
            raise Forbidden(f"'{user_id}' is an admin set in the tenant file")
        row = await self._members.get_member(tenant_id, user_id, conn=conn)
        if row is None:
            raise MemberNotFound(f"'{user_id}' is not a member")
        return row
