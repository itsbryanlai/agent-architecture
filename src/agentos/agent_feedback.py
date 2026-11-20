"""The creator feedback page's reads: one agent's outcomes, votes and questions.

Read-only, over the rows the worker and the gateway already write: the audit log
(``audit_events``), the answer votes (``answer_feedback``) and the stored history
(``conversation_messages``). No new data collection, no model call.

Sections, newest first, each one tenant- and agent-scoped query with a limit and a
count of every matching row in the window:

- unanswered: a ``search_knowledge`` call whose stored output hash is the fixed
  ``NO_MATCH`` text (the search found nothing within the cut-off), or an ``answer``
  whose hash is the fixed ``NO_ANSWER`` text. Both are detected from the hashes the
  audit row already holds, so they work even after retention purged the text.
- failed_turns: ``notice`` rows with tool ``failure`` or ``step_limit``.
- refused: ``tool_call`` rows with outcome ``error`` (a tool that failed or was
  refused by a switch or the agent's health), and ``audience_refused`` rows with
  tool ``health`` (the agent's grant check failed).
- approvals_closed: ``approval_closed`` rows (tool ``declined`` or ``expired``).
- writes_failed: ``write`` rows with outcome ``error``.
- votes: the 👎 rows of ``answer_feedback`` with their reasons, and the 👍/👎 totals
  of the window's stored votes.

Privacy: no query selects a requester, an identity, an approver or a voter, so the
page never shows who asked or who voted. A turn's question text is read from stored
history only while retention keeps it, and only for a row whose turn stored the
question: an answer, a tool call, a step-limit notice, or a failure notice recorded
after the agent loaded (version 0 marks the early failure posted before ingest).
An audience refusal happens before the question is stored, so its row carries no
question. Otherwise the row shows its count and date. No thread link is built.

Each question row carries the stored question's reference (conversation ID and
message seq) beside the agent version: the seam a later test-case action reads.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.audit import canonical_sha256
from agentos.knowledge.search import NO_MATCH
from agentos.models import NO_ANSWER
from agentos.stores import STARTS_A_TURN, tenant_transaction

# The windows the page offers, in days; the first is the default.
FEEDBACK_DAYS = (30, 90, 365)
SECTION_LIMIT = 50

# The fixed texts whose hashes mark an unanswered question in the audit row.
NO_MATCH_SHA256 = canonical_sha256(NO_MATCH)
NO_ANSWER_SHA256 = canonical_sha256(NO_ANSWER)

SECTION_TITLES = {
    "unanswered": "Questions the agent could not answer",
    "failed_turns": "Turns that failed or hit the step limit",
    "refused": "Refused tools and grants",
    "approvals_closed": "Declined or expired approvals",
    "writes_failed": "Writes that failed",
    "votes": "👎 votes",
}


@dataclass(frozen=True)
class FeedbackRow:
    """One row of a section: when, which version, what happened, and the question
    text with its stored reference while retention keeps it. Never a person."""

    at: datetime
    agent_version: int | None
    detail: str
    question: str | None = None
    conversation_id: str | None = None
    question_seq: int | None = None
    reason: str | None = None


@dataclass(frozen=True)
class FeedbackSection:
    key: str
    title: str
    total: int  # every matching row in the window, not only the page shown
    rows: list[FeedbackRow]


@dataclass(frozen=True)
class AgentFeedbackView:
    agent_id: str
    since: datetime
    sections: list[FeedbackSection]
    ups: int = 0
    downs: int = 0


def empty_view(agent_id: str, since: datetime) -> AgentFeedbackView:
    """The view of a plane built without a feedback store: every section empty."""
    return AgentFeedbackView(
        agent_id, since, [FeedbackSection(k, t, 0, []) for k, t in SECTION_TITLES.items()]
    )


class AgentFeedbackStore(Protocol):
    async def read(
        self, tenant_id: str, agent_id: str, since: datetime, limit: int = SECTION_LIMIT
    ) -> AgentFeedbackView:
        """The agent's sections and vote totals from ``since`` until now, newest first."""
        ...

    async def question_text(self, tenant_id: str, conversation_id: str, seq: int) -> str | None:
        """One stored message's text, or None when retention no longer keeps it.

        What "Make a test case" reads: the question behind a row's stored reference.
        """
        ...


def _detail(section: str, kind: str, tool: str | None) -> str:
    if section == "unanswered":
        return "no answer" if kind == "answer" else "no matching documents"
    if section == "refused" and kind == "audience_refused":
        return "grant check failed"
    return tool or ""


def _stores_question(kind: str, tool: str | None, agent_version: int | None) -> bool:
    """Whether the row's turn stored its question before the event was recorded.

    An audience refusal and an early failure notice (version 0) happen before ingest;
    an empty-message prompt has no question text at all.
    """
    return kind in ("answer", "tool_call") or (
        kind == "notice" and tool in ("step_limit", "failure") and (agent_version or 0) > 0
    )


# One predicate per audit section, in SQL and in Python, over the same rows.
_SECTION_PREDICATES = {
    "unanswered": (
        "(e.kind = 'tool_call' AND e.tool = 'search_knowledge'"
        " AND e.output_sha256 = %(no_match)s)"
        " OR (e.kind = 'answer' AND e.output_sha256 = %(no_answer)s)"
    ),
    "failed_turns": "e.kind = 'notice' AND e.tool IN ('failure', 'step_limit')",
    "refused": (
        "(e.kind = 'tool_call' AND e.outcome = 'error')"
        " OR (e.kind = 'audience_refused' AND e.tool = 'health')"
    ),
    "approvals_closed": "e.kind = 'approval_closed'",
    "writes_failed": "e.kind = 'write' AND e.outcome = 'error'",
}

# The turn's question: the newest turn-starting user message stored no later than the
# event. The turn stored it before the placeholder, the model call and the reply, and
# the next turn's question is stored only after this turn ends, so the newest one at
# or before the event is this turn's. The join runs only for rows whose turn stored a
# question; a purged message leaves the row with its count and date.
_QUESTION_JOIN = f"""
LEFT JOIN LATERAL (
  SELECT m.seq,
         (SELECT b ->> 'text' FROM jsonb_array_elements(m.content) b
          WHERE b ->> 'type' = 'text' LIMIT 1) AS question
  FROM conversation_messages m
  WHERE m.tenant_id = e.tenant_id AND m.conversation_id = e.conversation_id
    AND m.created_at <= e.at AND {STARTS_A_TURN}
  ORDER BY m.seq DESC
  LIMIT 1
) q ON e.kind IN ('answer', 'tool_call')
   OR (e.kind = 'notice' AND e.tool IN ('step_limit', 'failure') AND e.agent_version > 0)
"""

_AUDIT_SECTION_SQL = (
    "SELECT e.at, e.agent_version, e.kind, e.tool, e.conversation_id,"
    " q.question, q.seq AS question_seq, count(*) OVER () AS total"
    " FROM audit_events e"
    + _QUESTION_JOIN
    + " WHERE e.tenant_id = %(t)s AND e.agent_id = %(a)s AND e.at >= %(since)s"
    " AND ({predicate})"
    " ORDER BY e.at DESC, e.seq DESC LIMIT %(limit)s"
)

# The totals over every stored vote of the window, and the newest 👎 rows with their
# reasons. The aggregate always returns one row, so the totals survive an empty list.
_VOTES_SQL = """
SELECT t.ups, t.downs, d.reason, d.at, d.agent_version
FROM (
  SELECT count(*) FILTER (WHERE vote = 'up') AS ups,
         count(*) FILTER (WHERE vote = 'down') AS downs
  FROM answer_feedback
  WHERE tenant_id = %(t)s AND agent_id = %(a)s AND at >= %(since)s
) t
LEFT JOIN LATERAL (
  SELECT reason, at, agent_version
  FROM answer_feedback
  WHERE tenant_id = %(t)s AND agent_id = %(a)s AND at >= %(since)s AND vote = 'down'
  ORDER BY at DESC
  LIMIT %(limit)s
) d ON true
ORDER BY d.at DESC NULLS LAST
"""


class PostgresAgentFeedbackStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def read(
        self, tenant_id: str, agent_id: str, since: datetime, limit: int = SECTION_LIMIT
    ) -> AgentFeedbackView:
        params = {
            "t": tenant_id,
            "a": agent_id,
            "since": since,
            "limit": limit,
            "no_match": NO_MATCH_SHA256,
            "no_answer": NO_ANSWER_SHA256,
        }
        sections = []
        async with tenant_transaction(self._pool, tenant_id) as conn:
            for key, predicate in _SECTION_PREDICATES.items():
                cur = await conn.execute(_AUDIT_SECTION_SQL.format(predicate=predicate), params)
                rows = []
                total = 0
                for (
                    at,
                    version,
                    kind,
                    tool,
                    conversation_id,
                    question,
                    seq,
                    count,
                ) in await cur.fetchall():
                    total = count
                    stored = question is not None
                    rows.append(
                        FeedbackRow(
                            at=at,
                            agent_version=version,
                            detail=_detail(key, kind, tool),
                            question=question,
                            conversation_id=conversation_id if stored else None,
                            question_seq=seq if stored else None,
                        )
                    )
                sections.append(FeedbackSection(key, SECTION_TITLES[key], total, rows))
            cur = await conn.execute(_VOTES_SQL, params)
            ups = downs = 0
            rows = []
            for up_count, down_count, reason, at, version in await cur.fetchall():
                ups, downs = up_count, down_count
                if at is not None:
                    rows.append(FeedbackRow(at=at, agent_version=version, detail="", reason=reason))
            sections.append(FeedbackSection("votes", SECTION_TITLES["votes"], downs, rows))
        return AgentFeedbackView(agent_id, since, sections, ups, downs)

    async def question_text(self, tenant_id: str, conversation_id: str, seq: int) -> str | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                SELECT (SELECT b ->> 'text' FROM jsonb_array_elements(content) b
                        WHERE b ->> 'type' = 'text' LIMIT 1)
                FROM conversation_messages
                WHERE tenant_id = %s AND conversation_id = %s AND seq = %s
                """,
                (tenant_id, conversation_id, seq),
            )
            row = await cur.fetchone()
        return None if row is None else row[0]


def _is_question(message: dict) -> bool:
    """The Python twin of ``stores.STARTS_A_TURN``: a user message with visible text
    and no tool result."""
    content = message.get("content")
    if message.get("role") != "user" or not isinstance(content, list):
        return False
    blocks = [b for b in content if isinstance(b, dict)]
    return any(
        b.get("type") == "text" and isinstance(b.get("text"), str) and b["text"].strip()
        for b in blocks
    ) and not any(b.get("type") == "tool_result" for b in blocks)


class InMemoryAgentFeedbackStore:
    """The same reads over the in-memory twins: the audit events, the votes and the
    stored history, with the history's append times standing in for ``created_at``."""

    def __init__(self, audit, feedback, history):
        self._audit = audit
        self._feedback = feedback
        self._history = history

    async def read(
        self, tenant_id: str, agent_id: str, since: datetime, limit: int = SECTION_LIMIT
    ) -> AgentFeedbackView:
        events = [
            e
            for e in self._audit.events
            if e.tenant_id == tenant_id
            and e.agent_id == agent_id
            and e.at is not None
            and e.at >= since
        ]
        sections = []
        for key, matches in _SECTION_MATCHERS.items():
            matched = sorted(
                (e for e in events if matches(e)),
                key=lambda e: (e.at, e.seq or 0),
                reverse=True,
            )
            rows = [self._audit_row(key, e) for e in matched[:limit]]
            sections.append(FeedbackSection(key, SECTION_TITLES[key], len(matched), rows))
        votes = [
            v
            for v in self._feedback.snapshot().values()
            if v.tenant_id == tenant_id and v.agent_id == agent_id and v.at >= since
        ]
        ups = sum(v.vote == "up" for v in votes)
        downs = sum(v.vote == "down" for v in votes)
        down = sorted((v for v in votes if v.vote == "down"), key=lambda v: v.at, reverse=True)[
            :limit
        ]
        rows = [
            FeedbackRow(at=v.at, agent_version=v.agent_version, detail="", reason=v.reason)
            for v in down
        ]
        sections.append(FeedbackSection("votes", SECTION_TITLES["votes"], downs, rows))
        return AgentFeedbackView(agent_id, since, sections, ups, downs)

    async def question_text(self, tenant_id: str, conversation_id: str, seq: int) -> str | None:
        messages = self._history.messages.get((tenant_id, conversation_id), [])
        if not 1 <= seq <= len(messages):
            return None
        content = messages[seq - 1].get("content")
        if not isinstance(content, list):
            return None
        return next(
            (b["text"] for b in content if isinstance(b, dict) and b.get("type") == "text"),
            None,
        )

    def _audit_row(self, section: str, event) -> FeedbackRow:
        question, seq = None, None
        if _stores_question(event.kind, event.tool, event.agent_version):
            question, seq = self._question(event)
        stored = question is not None
        return FeedbackRow(
            at=event.at,
            agent_version=event.agent_version,
            detail=_detail(section, event.kind, event.tool),
            question=question,
            conversation_id=event.conversation_id if stored else None,
            question_seq=seq if stored else None,
        )

    def _question(self, event) -> tuple[str | None, int | None]:
        key = (event.tenant_id, event.conversation_id)
        messages = self._history.messages.get(key, [])
        times = self._history.times.get(key, [])
        for i in range(len(messages) - 1, -1, -1):
            if times[i] <= event.at and _is_question(messages[i]):
                text = next(b["text"] for b in messages[i]["content"] if b.get("type") == "text")
                return text, i + 1
        return None, None


_SECTION_MATCHERS = {
    "unanswered": lambda e: (
        (
            e.kind == "tool_call"
            and e.tool == "search_knowledge"
            and e.output_sha256 == NO_MATCH_SHA256
        )
        or (e.kind == "answer" and e.output_sha256 == NO_ANSWER_SHA256)
    ),
    "failed_turns": lambda e: e.kind == "notice" and e.tool in ("failure", "step_limit"),
    "refused": lambda e: (
        (e.kind == "tool_call" and e.outcome == "error")
        or (e.kind == "audience_refused" and e.tool == "health")
    ),
    "approvals_closed": lambda e: e.kind == "approval_closed",
    "writes_failed": lambda e: e.kind == "write" and e.outcome == "error",
}
