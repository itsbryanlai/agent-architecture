"""The platform's own record of each task: what was asked, where it stands.

A task's state lives in its workflow, but the model cannot query a workflow. The
``tasks`` table is the runtime's copy: the task workflow records each state change
through ``record_task_state`` (an upsert by task ID), and the ``my_tasks`` read tool
lists the asker's rows. The requester comes from the runtime, never the model, so a
person sees only their own requests.

A row holds short metadata only: the item's title and link, its last known status,
the thread it lives in and the page it watches (a reference for the poke). Never
conversation text or a comment. The title and link are read by the activity from the
stored draft or the write's stored result, so they never pass through workflow history.

``updated_at`` is when the task last reported: a state change, a status change, or a
poke-answered poll. A question asked in the task's own thread pokes the watch and waits
for that report to land, so the answer carries a fresh status; the wait is bounded and
best-effort, and the poke goes through the same ``external_update`` signal a webhook
uses, so the watch's ``POKE_GAP`` spacing still applies.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.knowledge.search import escape_attribute
from agentos.stores import tenant_transaction
from agentos.workflows import POKE_GAP

# The states a task is listed in. The workflow's own states map onto these: a task
# still drafting is about to ask for approval, an approved write is being filed, and
# an approval that expired closed without a write, as a declined one does.
AWAITING_APPROVAL = "awaiting_approval"
FILED = "filed"
WATCHING = "watching"
DONE = "done"
FAILED = "failed"
CANCELLED = "cancelled"

LISTED_STATES = (AWAITING_APPROVAL, FILED, WATCHING, DONE, FAILED, CANCELLED)
OPEN_STATES = (AWAITING_APPROVAL, FILED, WATCHING)

_WORKFLOW_TO_LISTED = {
    "drafted": AWAITING_APPROVAL,
    "awaiting_approval": AWAITING_APPROVAL,
    "executing": FILED,
    "watching": WATCHING,
    "done": DONE,
    "failed": FAILED,
    "cancelled": CANCELLED,
    "expired": CANCELLED,
}

# At most this many of the asker's tasks are listed, open ones first, then newest.
MAX_MY_TASKS = 10

# How long the in-thread wait for a poke-answered poll may take: the watch answers a
# poke at most POKE_GAP after its last poll, then the poll itself runs. The wait is
# best-effort; a task that does not report in time is listed as it was last recorded.
REFRESH_WAIT = POKE_GAP + timedelta(seconds=45)


def listed_state(workflow_state: str) -> str:
    """The state a task in ``workflow_state`` is listed in."""
    return _WORKFLOW_TO_LISTED[workflow_state]


@dataclass(frozen=True)
class TaskRecord:
    """One task as the runtime last recorded it. IDs and short metadata, never content
    beyond the item's title and link."""

    tenant_id: str
    task_id: str
    requester_user_id: str
    agent_id: str
    title: str
    url: str
    page_id: str
    state: str
    last_status: str
    updated_at: datetime
    channel: str
    thread_ts: str


class TaskStore(Protocol):
    async def record(self, record: TaskRecord) -> None:
        """Store the task's latest report. A repeat for the same task ID replaces the
        row; it never adds a second one."""
        ...

    async def get(self, tenant_id: str, task_id: str) -> TaskRecord | None: ...

    async def list_for_requester(
        self, tenant_id: str, requester_user_id: str, limit: int = MAX_MY_TASKS
    ) -> list[TaskRecord]:
        """The requester's tasks: open ones first, then newest, at most ``limit``."""
        ...

    async def watching_in_thread(
        self, tenant_id: str, channel: str, thread_ts: str, requester_user_id: str
    ) -> TaskRecord | None:
        """The requester's newest watching task in this thread, or None."""
        ...


class InMemoryTaskStore:
    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], TaskRecord] = {}

    async def record(self, record: TaskRecord) -> None:
        self._rows[(record.tenant_id, record.task_id)] = record

    async def get(self, tenant_id: str, task_id: str) -> TaskRecord | None:
        return self._rows.get((tenant_id, task_id))

    async def list_for_requester(
        self, tenant_id: str, requester_user_id: str, limit: int = MAX_MY_TASKS
    ) -> list[TaskRecord]:
        rows = [
            r
            for (t, _), r in self._rows.items()
            if t == tenant_id and r.requester_user_id == requester_user_id
        ]
        rows.sort(key=lambda r: (r.state in OPEN_STATES, r.updated_at), reverse=True)
        return rows[:limit]

    async def watching_in_thread(
        self, tenant_id: str, channel: str, thread_ts: str, requester_user_id: str
    ) -> TaskRecord | None:
        rows = [
            r
            for (t, _), r in self._rows.items()
            if t == tenant_id
            and r.channel == channel
            and r.thread_ts == thread_ts
            and r.requester_user_id == requester_user_id
            and r.state == WATCHING
        ]
        return max(rows, key=lambda r: r.updated_at, default=None)


_COLUMNS = (
    "tenant_id, task_id, requester_user_id, agent_id, title, url, page_id, "
    "state, last_status, updated_at, channel, thread_ts"
)


class PostgresTaskStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def record(self, record: TaskRecord) -> None:
        async with tenant_transaction(self._pool, record.tenant_id) as conn:
            await conn.execute(
                """
                INSERT INTO tasks (tenant_id, task_id, requester_user_id, agent_id, title, url,
                                   page_id, state, last_status, updated_at, channel, thread_ts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, task_id) DO UPDATE SET
                  requester_user_id = EXCLUDED.requester_user_id,
                  agent_id = EXCLUDED.agent_id,
                  title = EXCLUDED.title,
                  url = EXCLUDED.url,
                  page_id = EXCLUDED.page_id,
                  state = EXCLUDED.state,
                  last_status = EXCLUDED.last_status,
                  updated_at = EXCLUDED.updated_at,
                  channel = EXCLUDED.channel,
                  thread_ts = EXCLUDED.thread_ts
                """,
                (
                    record.tenant_id,
                    record.task_id,
                    record.requester_user_id,
                    record.agent_id,
                    record.title,
                    record.url,
                    record.page_id,
                    record.state,
                    record.last_status,
                    record.updated_at,
                    record.channel,
                    record.thread_ts,
                ),
            )

    async def get(self, tenant_id: str, task_id: str) -> TaskRecord | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT {_COLUMNS} FROM tasks WHERE tenant_id = %s AND task_id = %s",
                (tenant_id, task_id),
            )
            row = await cur.fetchone()
        return None if row is None else TaskRecord(*row)

    async def list_for_requester(
        self, tenant_id: str, requester_user_id: str, limit: int = MAX_MY_TASKS
    ) -> list[TaskRecord]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"""
                SELECT {_COLUMNS} FROM tasks
                WHERE tenant_id = %s AND requester_user_id = %s
                ORDER BY (state = ANY(%s)) DESC, updated_at DESC
                LIMIT %s
                """,
                (tenant_id, requester_user_id, list(OPEN_STATES), limit),
            )
            return [TaskRecord(*row) for row in await cur.fetchall()]

    async def watching_in_thread(
        self, tenant_id: str, channel: str, thread_ts: str, requester_user_id: str
    ) -> TaskRecord | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"""
                SELECT {_COLUMNS} FROM tasks
                WHERE tenant_id = %s AND channel = %s AND thread_ts = %s
                  AND requester_user_id = %s AND state = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (tenant_id, channel, thread_ts, requester_user_id, WATCHING),
            )
            row = await cur.fetchone()
        return None if row is None else TaskRecord(*row)


def format_task_list(records: list[TaskRecord], *, team_id: str) -> str:
    """The model-facing output of ``my_tasks``: one line per task, open ones first.

    Titles and statuses are text people wrote, so they are wrapped and escaped the way
    ``search_items`` output is. The thread link opens the channel the task's thread lives
    in (Slack's universal link; a thread permalink would need the workspace's domain).
    """
    if not records:
        return "No requests found for you."
    lines = []
    for r in records:
        thread = f"https://slack.com/app_redirect?team={team_id}&channel={r.channel}"
        updated = r.updated_at.strftime("%Y-%m-%d %H:%M UTC")
        lines.append(
            f'<item url="{escape_attribute(r.url)}" state="{escape_attribute(r.state)}" '
            f'status="{escape_attribute(r.last_status or "none")}" '
            f'updated="{escape_attribute(updated)}" thread="{escape_attribute(thread)}">'
            f"{escape_attribute(r.title)}</item>"
        )
    return "<items>\n" + "\n".join(lines) + "\n</items>"
