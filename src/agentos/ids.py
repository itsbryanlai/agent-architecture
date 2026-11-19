"""Workflow and schedule IDs. These functions are the only place IDs are built.

Every ID starts with the tenant ID, so workflows from different tenants can
never collide and every ID says whose data it belongs to.
"""

import hashlib

from agentos.connectors.notion import normalize_id


def tenant_for_slack_team(team_id: str) -> str:
    """The stable tenant ID of a Slack workspace: its team ID, lower-cased, behind a prefix.

    One workspace always maps to the same tenant, so a reinstall finds the row the
    first install created. The caller checks the result against ``TENANT_ID_PATTERN``.
    """
    return f"t-{team_id.lower()}"


def conversation_id(tenant: str, channel: str, thread_ts: str) -> str:
    return f"{tenant}:conv:{channel}:{thread_ts}"


def thread_key(tenant: str, channel: str, thread_ts: str) -> str:
    """The key of a Slack thread in the ``threads`` table."""
    return f"{tenant}:thread:{channel}:{thread_ts}"


def task_id(tenant: str, uuid: str) -> str:
    return f"{tenant}:task:{uuid}"


def knowledge_scope(page_id: str) -> str:
    """The scope name for a granted root page: every page under it shares it."""
    return f"notion:page:{normalize_id(page_id)}"


def index_workflow_id(tenant: str, scope: str) -> str:
    return f"{tenant}:index:{scope}"


def index_schedule_id(tenant: str, scope: str) -> str:
    return f"{tenant}:index-schedule:{scope}"


def retention_workflow_id(tenant: str) -> str:
    return f"{tenant}:retention"


def retention_schedule_id(tenant: str) -> str:
    return f"{tenant}:retention-schedule"


def page_refresh_id(tenant: str, page_id: str) -> str:
    """One refresh workflow per page, so a burst of events lands in one run."""
    return f"{tenant}:page-refresh:{normalize_id(page_id)}"


# Audit event keys. Each names one thing that happened, so an activity that
# retries records it once. Every key starts with the tenant ID.


def audit_tool_call_key(conversation_id: str, tool_use_id: str) -> str:
    return f"{conversation_id}:audit:tool:{tool_use_id}"


def audit_answer_key(conversation_id: str, placeholder_ts: str) -> str:
    return f"{conversation_id}:audit:answer:{placeholder_ts}"


def audit_notice_key(conversation_id: str, tool: str, marker: str) -> str:
    """A non-answer final text: ``marker`` is the placeholder ts or the reply's idempotency key."""
    return f"{conversation_id}:audit:notice:{tool}:{marker}"


def audit_hand_off_key(tenant: str, event_id: str) -> str:
    return f"{tenant}:audit:hand-off:{event_id}"


def audit_approval_key(task: str, nonce: str) -> str:
    return f"{task}:audit:approval:{nonce}"


def audit_approval_closed_key(task: str, nonce: str) -> str:
    """An approval that closed without a write: the requester declined it, or it expired."""
    return f"{task}:audit:approval-closed:{nonce}"


def audit_write_key(idem_key: str, outcome: str) -> str:
    """``idem_key`` is the write's idempotency key, ``{task_id}:create`` or ``:comment``."""
    return f"{idem_key}:audit:write:{outcome}"


def audit_control_key(tenant: str, kind: str, nonce: str) -> str:
    """One agent-lifecycle or membership change made through the control plane."""
    return f"{tenant}:audit:control:{kind}:{nonce}"


def audit_import_key(tenant: str, agent_id: str) -> str:
    """The audit event for an agent the one-time YAML import inserted: one per agent."""
    return f"{tenant}:audit:import:{agent_id}"


def audit_audience_refused_key(tenant: str, event_id: str, agent_id: str, target: str = "") -> str:
    """An audience refusal, once each. ``agent_id`` is the refused agent; ``target``, when set,
    is the other party in a refused hand-off (the front door now, the specialist in pre-patch
    histories)."""
    return f"{tenant}:audit:audience-refused:{event_id}:{agent_id}:{target}"


def audience_refusal_key(event_id: str, agent_id: str) -> str:
    """The idempotency key of the reply that tells the asker an agent is not open to them."""
    return f"{event_id}:audience:{agent_id}"


def empty_message_key(event_id: str, agent_id: str) -> str:
    """The idempotency key of the reply that asks for a question after a message with no text."""
    return f"{event_id}:empty:{agent_id}"


# Usage row keys. Each names one attempt of one activity: a retry that calls the
# provider again is billed again, so it gets its own row, while the same attempt
# recorded twice stores one. Every key starts with the tenant ID.


def usage_call_key(
    tenant: str, workflow_id: str, run_id: str, activity_id: str, attempt: int
) -> str:
    """The model call made by one attempt of one activity of one workflow run."""
    return f"{tenant}:usage:{workflow_id}:{run_id}:{activity_id}:{attempt}"


def usage_embedding_key(
    tenant: str,
    workflow_id: str,
    run_id: str,
    activity_id: str,
    attempt: int,
    scope: str,
    page_id: str,
) -> str:
    """The embedding of one page by one attempt. One activity can sync several pages and scopes."""
    return f"{tenant}:usage:{workflow_id}:{run_id}:{activity_id}:{attempt}:{scope}:{page_id}"


# Audience watch. One workflow per Slack event, and keys that make each effect of a pause
# happen once: the audit event, and the notice to each person.


def audience_event_workflow_id(tenant: str, event_id: str) -> str:
    """The workflow that handles one Slack audience event: a repeat of the event is rejected."""
    return f"{tenant}:audience-event:{event_id}"


def audit_audience_pause_key(tenant: str, agent_id: str, channel: str, paused_at: str) -> str:
    """The audit event of one pause, named by when it began."""
    return f"{tenant}:audit:audience-pause:{agent_id}:{channel}:{paused_at}"


def audience_pause_notice_key(
    tenant: str, agent_id: str, channel: str, paused_at: str, recipient: str
) -> str:
    """The idempotency key of the direct message that tells one person about one pause."""
    return f"{tenant}:audience-pause-notice:{agent_id}:{channel}:{paused_at}:{recipient}"


def scope_gone_notice_key(tenant: str, scope: str, recipient: str, first_seen: str) -> str:
    """The idempotency key of the direct message that tells one person a scope's root page is gone."""
    return f"{tenant}:scope-gone-notice:{scope}:{first_seen}:{recipient}"


def edit_event_id(unique: str) -> str:
    """The ``inbound_events`` row that holds one saved Edit form (not a Slack event)."""
    return f"edit:{unique}"


# Slack posts. Slack has no idempotency key, so a post carries an opaque marker in its message
# metadata, and a retry that may have posted already looks the marker up before posting again.


def slack_post_marker(tenant: str, idem_key: str) -> str:
    """The opaque marker one Slack post carries: a hash of the tenant and the post's key, never content."""
    return hashlib.sha256(f"{tenant}:{idem_key}".encode()).hexdigest()[:32]


def slack_post_write_key(idem_key: str) -> str:
    """The ``external_writes`` key that makes one Slack post happen once."""
    return f"{idem_key}:slack-post"
