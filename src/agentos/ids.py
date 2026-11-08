"""Workflow IDs. These two functions are the only place IDs are built.

Every ID starts with the tenant ID, so workflows from different tenants can
never collide and every ID says whose data it belongs to.
"""


def conversation_id(tenant: str, channel: str, thread_ts: str) -> str:
    return f"{tenant}:conv:{channel}:{thread_ts}"


def task_id(tenant: str, uuid: str) -> str:
    return f"{tenant}:task:{uuid}"
