"""The control plane: who may change agents and members, and the record of it.

Every method takes the tenant and the ``actor``, a Slack user ID that the caller
got from a verified identity and never from a request body. A method raises
``Forbidden`` when the actor's role does not allow the action, and otherwise
makes the change and writes one audit event for it. This is the only module
besides ``agent_store.py`` and ``members.py`` that calls the stores' write
methods; the write methods are named so that a test can find every call.

Roles. A tenant has admins and creators.

- An admin approves every publish (which also covers a wider grant: a grant
  reaches users only by being published), rejects pending versions, manages
  members and reads the audit log.
- A creator reads agents, creates agents, and drafts, edits, submits and
  withdraws the versions of an agent they created. An admin may do the same for
  any agent.
- An admin reads usage and cost for every agent; a creator reads it for the agents
  they created. Usage is never broken down by requester.
- An admin sets, raises and removes monthly budgets (one for the tenant, one per agent). An
  admin reads all of them; a creator reads the budgets of the agents they created, and
  never sets one. Every change is audited as ``budget_set`` with the new amount in
  ``target`` (``0`` and ``tool=remove`` for a removal).
- A user listed in the tenant YAML ``admins`` is always an admin. Nobody can
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
records one event. The store's own state check refuses the retried change.

A publish also checks the version's audience. A channel the tenant file routes to the
agent must be in the audience, or the publish is refused. When the plane has a Slack
people counter, the publish needs the approver's confirmation of the people who can
reach the agent: ``confirm_audience`` must equal a fresh count (total, guests,
external), and an ``audience_confirm`` event records it with the publish. A plane
built without a counter does not ask: only tests do that.

A refused action is not audited yet.

The audit event is recorded after the change, in its own transaction. If the
audit store fails, the change is kept and the error is raised.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from agentos import ids
from agentos.agent_store import (
    PENDING,
    AgentNotFound,
    AgentRecord,
    AgentStore,
    AgentVersionRecord,
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
from agentos.audit import AuditStore, canonical_sha256
from agentos.budgets import (
    Budget,
    BudgetLine,
    BudgetStore,
    BudgetsView,
    InMemoryBudgetStore,
    month_spend,
    parse_amount,
)
from agentos.config import AgentVersion, ConfigError, TenantsConfig, channel_audience_problems
from agentos.grant_checker import GrantChecker, GrantProblems, GrantReport
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
from agentos.slack_client import SlackLookupError
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


@dataclass
class AudienceView:
    """What the console shows of a version's audience: the rules in force and who they reach.

    ``summary`` is None when the plane counts no people or Slack could not be read;
    ``problem`` then says why.
    """

    effective: EffectiveAudience
    summary: AudienceSummary | None = None
    problem: str | None = None


def _slack_problem(err: Exception) -> str:
    """A reason fit to show: the Slack error code or the configuration message, no secrets."""
    if isinstance(err, SlackLookupError):
        return f"Slack said '{err.code}'"
    return str(err)


def _no_config(agent: AgentVersion) -> EffectiveAudience:
    """A tenant with no file section has no routing, so only an explicit audience is known."""
    a = agent.audience
    if a is None:
        return EffectiveAudience(False, (), True, (), False)
    return EffectiveAudience(
        True, tuple(a.channels), a.dm_everyone, tuple(a.dm_user_groups), a.guests
    )


class ControlPlane:
    def __init__(
        self,
        agents: AgentStore,
        members: MemberStore,
        audit: AuditStore,
        tenants: TenantsConfig,
        grant_checker: GrantChecker,
        audience: AudienceInspector | None = None,
        usage: UsageStore | None = None,
        budgets: BudgetStore | None = None,
        clock: Callable[[], datetime] = utcnow,
    ):
        self._audience = audience
        self._usage = InMemoryUsageStore() if usage is None else usage
        self._budgets = InMemoryBudgetStore() if budgets is None else budgets
        self._clock = clock
        self._agents = agents
        self._members = members
        self._audit = audit
        self._tenants = tenants
        self._grant_checker = grant_checker

    # --- roles ---------------------------------------------------------------

    def _config_admins(self, tenant_id: str) -> tuple[str, ...]:
        config = self._tenants.get(tenant_id)
        return () if config is None else config.admins

    async def role_of(self, tenant_id: str, user_id: str) -> str | None:
        """The user's role: a YAML admin is an admin, else the members row, else None."""
        if user_id in self._config_admins(tenant_id):
            return ADMIN
        row = await self._members.get_member(tenant_id, user_id)
        return None if row is None else row.role

    async def _require_role(self, tenant_id: str, actor: str, *allowed: str) -> str:
        role = await self.role_of(tenant_id, actor)
        if role not in allowed:
            raise Forbidden(f"'{actor}' may not do this in tenant '{tenant_id}'")
        return role

    async def _require_editor(self, tenant_id: str, actor: str, agent_id: str) -> None:
        """An admin, or the creator of this agent."""
        role = await self._require_role(tenant_id, actor, ADMIN, CREATOR)
        agent = await self._agents.get_agent(tenant_id, agent_id)
        if agent is None:
            raise AgentNotFound(f"agent '{agent_id}' does not exist")
        if role != ADMIN and agent.created_by != actor:
            raise Forbidden(f"'{actor}' did not create agent '{agent_id}'")

    async def _admin_count(self, tenant_id: str) -> int:
        rows = await self._members.list_members(tenant_id)
        admins = set(self._config_admins(tenant_id))
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
    ) -> None:
        """One event. A definition is hashed into ``input_sha256`` and never stored."""
        await self._audit.record(
            AuditEvent(
                tenant_id=tenant_id,
                event_key=ids.audit_control_key(tenant_id, kind, request_id or uuid.uuid4().hex),
                kind=kind,
                agent_id=agent_id if version is None else version.agent_id,
                agent_version=None if version is None else version.version,
                requester_user_id=requester or actor,
                identity=f"user:{actor}",
                tool=tool,
                target=target,
                input_sha256=None if version is None else canonical_sha256(version.definition),
                approver_user_id=approver,
            )
        )

    # --- reads ---------------------------------------------------------------

    async def list_agents(self, tenant_id: str, actor: str) -> list[AgentRecord]:
        await self._require_role(tenant_id, actor, ADMIN, CREATOR)
        return await self._agents.list_agents(tenant_id)

    async def versions(self, tenant_id: str, actor: str, agent_id: str) -> list[AgentVersionRecord]:
        await self._require_role(tenant_id, actor, ADMIN, CREATOR)
        return await self._agents.versions(tenant_id, agent_id)

    async def get_version(
        self, tenant_id: str, actor: str, agent_id: str, version: int
    ) -> AgentVersionRecord | None:
        await self._require_role(tenant_id, actor, ADMIN, CREATOR)
        return await self._agents.get_version(tenant_id, agent_id, version)

    async def list_members(self, tenant_id: str, actor: str) -> list[MemberRecord]:
        """The member rows. A YAML admin has no row; see ``role_of``."""
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
        self, tenant_id: str, actor: str, month: str | None = None
    ) -> UsageReport:
        """Usage and cost for a UTC calendar month (``YYYY-MM``; default the current one).

        An admin sees every agent and the tenant's embedding usage. A creator sees
        only the agents they created, which is the agents they may edit. There is no
        per-person view: no row or total here is grouped by who asked.
        """
        role = await self._require_role(tenant_id, actor, ADMIN, CREATOR)
        start, end, label = month_bounds(month, self._clock())
        groups = await self._usage.totals(tenant_id, start, end)
        if role != ADMIN:
            mine = {
                a.agent_id
                for a in await self._agents.list_agents(tenant_id)
                if a.created_by == actor
            }
            groups = [g for g in groups if g.agent_id in mine]
        teams: dict[tuple[str, int], str | None] = {}
        for agent_id, version in {(g.agent_id, g.agent_version) for g in groups if g.agent_id}:
            record = await self._agents.get_version(tenant_id, agent_id, version)
            teams[(agent_id, version)] = None if record is None else (record.agent().team or None)
        return build_report(label, groups, teams)

    async def budgets(self, tenant_id: str, actor: str) -> BudgetsView:
        """Each agent's budget against the spend of the current UTC month.

        An admin gets the tenant-wide line and every agent; a creator the agents they
        created and no tenant line. An agent with no budget is listed with no limit.
        """
        role = await self._require_role(tenant_id, actor, ADMIN, CREATOR)
        start, end, label = month_bounds(None, self._clock())
        spend = month_spend(await self._usage.totals(tenant_id, start, end))
        rows = {b.agent_id: b for b in await self._budgets.list(tenant_id)}
        agents = await self._agents.list_agents(tenant_id)
        if role != ADMIN:
            agents = [a for a in agents if a.created_by == actor]
        lines = [
            BudgetLine.of(a.agent_id, rows.get(a.agent_id), spend.of(a.agent_id))
            for a in sorted(agents, key=lambda a: a.agent_id)
        ]
        tenant = BudgetLine.of(None, rows.get(None), spend.tenant) if role == ADMIN else None
        return BudgetsView(label, tenant, lines)

    async def check_version_grants(
        self, tenant_id: str, actor: str, agent_id: str, version: int
    ) -> list[GrantReport]:
        """Check a version's Notion database grants. Read-only; editors and admins."""
        await self._require_editor(tenant_id, actor, agent_id)
        record = await self._agents.get_version(tenant_id, agent_id, version)
        if record is None:
            raise AgentNotFound(f"agent '{agent_id}' has no version {version}")
        return await self._grant_checker.check(tenant_id, record.agent())

    async def audience_view(
        self, tenant_id: str, actor: str, agent_id: str, version: int, count: bool = True
    ) -> AudienceView:
        """The audience in force for a version and, when ``count``, the people it reaches.

        Any role that reads versions. Slack is read live; a failure is reported in ``problem`` and
        does not fail the page.
        """
        await self._require_role(tenant_id, actor, ADMIN, CREATOR)
        record = await self._agents.get_version(tenant_id, agent_id, version)
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

    async def _effective(self, tenant_id: str, agent: AgentVersion) -> EffectiveAudience:
        config = self._tenants.get(tenant_id)
        if config is None or agent.audience is not None:
            published: dict[str, AgentVersion] = {}
        else:
            published = await self._agents.published_all(tenant_id)
        return effective_audience(agent, config, published) if config else _no_config(agent)

    def _check_mapped_channels(self, tenant_id: str, agent: AgentVersion) -> None:
        config = self._tenants.get(tenant_id)
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
                f"confirm_audience {json.dumps(now)}."
            )
        if confirm != now:
            raise AudienceNotConfirmed(
                f"The people who can reach this agent changed since they were counted: {said}. "
                "Review them and confirm again."
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
        record = await self._agents.create_agent(tenant_id, agent_id, definition, actor)
        await self._record("agent_create", tenant_id, actor, version=record, request_id=request_id)
        return record

    async def new_draft(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        from_version: int | None = None,
        request_id: str | None = None,
    ) -> AgentVersionRecord:
        """A new draft copied from ``from_version``, or from the published version."""
        await self._require_editor(tenant_id, actor, agent_id)
        record = await self._agents.create_draft(tenant_id, agent_id, actor, from_version)
        await self._record("agent_draft", tenant_id, actor, version=record, request_id=request_id)
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
        record = await self._agents.replace_draft(tenant_id, agent_id, version, definition)
        await self._record("agent_edit", tenant_id, actor, version=record, request_id=request_id)
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
        record = await self._agents.submit(tenant_id, agent_id, version, actor)
        await self._record("agent_submit", tenant_id, actor, version=record, request_id=request_id)
        return record

    async def withdraw_version(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        version: int,
        request_id: str | None = None,
    ) -> AgentVersionRecord:
        await self._require_editor(tenant_id, actor, agent_id)
        record = await self._agents.withdraw(tenant_id, agent_id, version)
        await self._record(
            "agent_withdraw", tenant_id, actor, version=record, request_id=request_id
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
        record = await self._agents.withdraw(tenant_id, agent_id, version)
        await self._record("agent_reject", tenant_id, actor, version=record, request_id=request_id)
        return record

    async def publish_version(
        self,
        tenant_id: str,
        actor: str,
        agent_id: str,
        version: int,
        request_id: str | None = None,
        confirm_audience: dict[str, int] | None = None,
    ) -> AgentVersionRecord:
        """Publish a pending version. Only an admin, who may be its submitter."""
        await self._require_role(tenant_id, actor, ADMIN)
        pending = await self._agents.get_version(tenant_id, agent_id, version)
        confirmed: AudienceSummary | None = None
        if pending is not None and pending.status == PENDING:
            # Any other status is refused by the store below, with its own reason.
            self._check_mapped_channels(tenant_id, pending.agent())
            reports = await self._grant_checker.check(tenant_id, pending.agent())
            if any(report.problems for report in reports):
                raise GrantProblems(reports)
            if self._audience is not None:
                confirmed = await self._confirmed_audience(
                    tenant_id, pending.agent(), confirm_audience
                )
        record = await self._agents.publish(tenant_id, agent_id, version, actor)
        await self._record(
            "agent_publish",
            tenant_id,
            actor,
            version=record,
            requester=record.submitted_by,
            approver=actor,
            request_id=request_id,
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
            )
        return record

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
        """
        await self._require_role(tenant_id, actor, ADMIN)
        amount = parse_amount(amount)
        if agent_id is not None and await self._agents.get_agent(tenant_id, agent_id) is None:
            raise AgentNotFound(f"agent '{agent_id}' does not exist")
        budget = await self._budgets.set_budget(tenant_id, agent_id, amount, actor)
        await self._record(
            "budget_set",
            tenant_id,
            actor,
            agent_id=agent_id,
            target=format(budget.amount, "f"),
            tool="set",
            request_id=request_id,
        )
        return budget

    async def remove_budget(
        self, tenant_id: str, actor: str, agent_id: str | None, request_id: str | None = None
    ) -> None:
        """Lift a budget, so the scope has no limit. Only an admin."""
        await self._require_role(tenant_id, actor, ADMIN)
        await self._budgets.remove_budget(tenant_id, agent_id)
        await self._record(
            "budget_set",
            tenant_id,
            actor,
            agent_id=agent_id,
            target="0",
            tool="remove",
            request_id=request_id,
        )

    # --- members -------------------------------------------------------------

    async def invite_member(
        self, tenant_id: str, actor: str, user_id: str, role: str, request_id: str | None = None
    ) -> None:
        await self._require_role(tenant_id, actor, ADMIN)
        if user_id in self._config_admins(tenant_id):
            raise MemberError(f"'{user_id}' is an admin set in the tenant file")
        await self._members.add_member(tenant_id, user_id, role, actor)
        await self._record(
            "member_add",
            tenant_id,
            actor,
            target=user_id,
            tool=f"role:{role}",
            request_id=request_id,
        )

    async def change_role(
        self, tenant_id: str, actor: str, user_id: str, role: str, request_id: str | None = None
    ) -> None:
        await self._require_role(tenant_id, actor, ADMIN)
        if role not in ROLES:
            raise InvalidMember(f"role must be one of {', '.join(ROLES)}")
        current = await self._protected_member(tenant_id, user_id)
        if current.role == role:
            raise MemberError(f"'{user_id}' already has the role {role}")
        if current.role == ADMIN and await self._admin_count(tenant_id) <= 1:
            raise Forbidden("the tenant's last admin cannot be demoted")
        await self._members.set_member_role(tenant_id, user_id, role)
        await self._record(
            "member_role",
            tenant_id,
            actor,
            target=user_id,
            tool=f"role:{role}",
            request_id=request_id,
        )

    async def drop_member(
        self, tenant_id: str, actor: str, user_id: str, request_id: str | None = None
    ) -> None:
        await self._require_role(tenant_id, actor, ADMIN)
        current = await self._protected_member(tenant_id, user_id)
        if current.role == ADMIN and await self._admin_count(tenant_id) <= 1:
            raise Forbidden("the tenant's last admin cannot be removed")
        await self._members.remove_member(tenant_id, user_id)
        await self._record("member_remove", tenant_id, actor, target=user_id, request_id=request_id)

    async def _protected_member(self, tenant_id: str, user_id: str) -> MemberRecord:
        """The member's row, after refusing a YAML admin, who is configuration."""
        if user_id in self._config_admins(tenant_id):
            raise Forbidden(f"'{user_id}' is an admin set in the tenant file")
        row = await self._members.get_member(tenant_id, user_id)
        if row is None:
            raise MemberNotFound(f"'{user_id}' is not a member")
        return row
