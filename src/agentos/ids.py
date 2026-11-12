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
