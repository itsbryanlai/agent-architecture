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

A refused action is not audited yet.

The audit event is recorded after the change, in its own transaction. If the
audit store fails, the change is kept and the error is raised.
"""

from __future__ import annotations

import uuid
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
from agentos.audit import AuditStore, canonical_sha256
from agentos.config import TenantsConfig
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


class Forbidden(Exception):
    """The actor's role does not allow the action."""


class ControlPlane:
    def __init__(
        self,
        agents: AgentStore,
        members: MemberStore,
        audit: AuditStore,
        tenants: TenantsConfig,
        grant_checker: GrantChecker,
    ):
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
                agent_id=None if version is None else version.agent_id,
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

    async def check_version_grants(
        self, tenant_id: str, actor: str, agent_id: str, version: int
    ) -> list[GrantReport]:
        """Check a version's Notion database grants. Read-only; editors and admins."""
        await self._require_editor(tenant_id, actor, agent_id)
        record = await self._agents.get_version(tenant_id, agent_id, version)
        if record is None:
            raise AgentNotFound(f"agent '{agent_id}' has no version {version}")
        return await self._grant_checker.check(tenant_id, record.agent())

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
    ) -> AgentVersionRecord:
        """Publish a pending version. Only an admin, who may be its submitter."""
        await self._require_role(tenant_id, actor, ADMIN)
        pending = await self._agents.get_version(tenant_id, agent_id, version)
        if pending is not None and pending.status == PENDING:
            # Any other status is refused by the store below, with its own reason.
            reports = await self._grant_checker.check(tenant_id, pending.agent())
            if any(report.problems for report in reports):
                raise GrantProblems(reports)
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
        return record

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
