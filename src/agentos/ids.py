"""Workflow and schedule IDs. These functions are the only place IDs are built.

Every ID starts with the tenant ID, so workflows from different tenants can
never collide and every ID says whose data it belongs to.
"""

from agentos.connectors.notion import normalize_id


def conversation_id(tenant: str, channel: str, thread_ts: str) -> str:
    return f"{tenant}:conv:{channel}:{thread_ts}"


def task_id(tenant: str, uuid: str) -> str:
    return f"{tenant}:task:{uuid}"


def knowledge_scope(page_id: str) -> str:
    """The scope name for a granted root page: every page under it shares it."""
    return f"notion:page:{normalize_id(page_id)}"


def index_workflow_id(tenant: str, scope: str) -> str:
    return f"{tenant}:index:{scope}"


def index_schedule_id(tenant: str, scope: str) -> str:
    return f"{tenant}:index-schedule:{scope}"


def page_refresh_id(tenant: str, page_id: str) -> str:
    """One refresh workflow per page, so a burst of events lands in one run."""
    return f"{tenant}:page-refresh:{normalize_id(page_id)}"


# Audit event keys. Each names one thing that happened, so an activity that
# retries records it once. Every key starts with the tenant ID.


def audit_tool_call_key(conversation_id: str, tool_use_id: str) -> str:
    return f"{conversation_id}:audit:tool:{tool_use_id}"


def audit_answer_key(conversation_id: str, placeholder_ts: str) -> str:
    return f"{conversation_id}:audit:answer:{placeholder_ts}"


def audit_hand_off_key(tenant: str, event_id: str) -> str:
    return f"{tenant}:audit:hand-off:{event_id}"


def audit_approval_key(task: str, nonce: str) -> str:
    return f"{task}:audit:approval:{nonce}"


def audit_write_key(idem_key: str, outcome: str) -> str:
    """``idem_key`` is the write's idempotency key, ``{task_id}:create`` or ``:comment``."""
    return f"{idem_key}:audit:write:{outcome}"
