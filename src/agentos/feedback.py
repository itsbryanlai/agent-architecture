"""Answer feedback: one 👍/👎 per person per answer, changeable.

The worker posts the two buttons under every final answer (never under a notice, an
approval card or a fixed stop reply). A click is handled by the gateway: the voter is
the Slack user of the signed request, never a field of the payload; the answer is named
by the conversation ID and message ts the button carries, and the agent and version are
the ones that answer's button names. A 👎 opens a modal for an optional one-line reason
(at most ``REASON_MAX`` characters); submitting or dismissing keeps the vote. A second
click by the same person replaces their vote; another person's vote is separate.

Rows live in ``answer_feedback``, one per (tenant, conversation, answer ts, voter), and
follow the tenant's ``tool_outputs_days`` retention. The reason is the only free text
and is purged with the row. Each vote is audited as ``feedback`` (the vote and the agent,
never the reason text), and the voter is told ``THANKS`` ephemerally; nothing is posted
in the thread.

No model call, no connector call: feedback is the user's action, not the agent's, so
the agent-effect gate does not apply.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.stores import tenant_transaction

# The action IDs of the two buttons, and the vote each records.
FEEDBACK_ACTIONS = {"answer_feedback_up": "up", "answer_feedback_down": "down"}
# The modal a 👎 opens for the optional reason.
FEEDBACK_CALLBACK = "answer_feedback_reason"
REASON_MAX = 300
THANKS = "Thanks — noted."

# Slack renders a message's blocks instead of its text, so an answer with buttons carries
# its text in section blocks; one section holds at most this many characters.
SECTION_TEXT_MAX = 3000


def feedback_blocks(
    conversation_id: str, answer_ts: str, agent_id: str, agent_version: int
) -> list[dict]:
    """The 👍/👎 buttons of one answer. The value carries references only, never content."""
    value = json.dumps(
        {
            "conversation_id": conversation_id,
            "answer_ts": answer_ts,
            "agent_id": agent_id,
            "agent_version": agent_version,
        }
    )
    return [
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "action_id": action_id,
                    "text": {"type": "plain_text", "text": emoji, "emoji": True},
                    "value": value,
                }
                for action_id, emoji in (
                    ("answer_feedback_up", "👍"),
                    ("answer_feedback_down", "👎"),
                )
            ],
        }
    ]


def answer_blocks(
    text: str, conversation_id: str, answer_ts: str, agent_id: str, agent_version: int
) -> list[dict]:
    """The answer as Slack shows it with its buttons: the text in section blocks, then 👍/👎.

    The text is split at newlines into sections of at most ``SECTION_TEXT_MAX`` characters
    (a hard cut only when a single line is longer); the full text stays the message's
    fallback. A code fence longer than a section is split across two sections.
    """
    blocks = []
    rest = text
    while len(rest) > SECTION_TEXT_MAX:
        cut = rest.rfind("\n\n", 0, SECTION_TEXT_MAX + 1)
        if cut <= 0:
            cut = rest.rfind("\n", 0, SECTION_TEXT_MAX + 1)
        if cut <= 0:
            cut = SECTION_TEXT_MAX
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": rest[:cut]}})
        rest = rest[cut:].lstrip("\n")
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": rest}})
    return blocks + feedback_blocks(conversation_id, answer_ts, agent_id, agent_version)


@dataclass(frozen=True)
class AnswerFeedback:
    """One person's vote on one answer, as last recorded. ``at`` is when that vote was cast."""

    tenant_id: str
    conversation_id: str
    answer_ts: str
    voter: str
    agent_id: str
    agent_version: int
    vote: str  # "up" or "down"
    reason: str | None
    at: datetime


class FeedbackStore(Protocol):
    async def record_vote(self, feedback: AnswerFeedback) -> None:
        """Store the vote, replacing this voter's earlier vote on the same answer.

        The replacement clears any reason the earlier vote carried.
        """
        ...

    async def set_reason(
        self, tenant_id: str, conversation_id: str, answer_ts: str, voter: str, reason: str
    ) -> bool:
        """Store the reason on the voter's 👎. False when they have no vote on that answer,
        or their vote is now a 👍: a reason belongs to a 👎 only."""
        ...

    async def get(
        self, tenant_id: str, conversation_id: str, answer_ts: str, voter: str
    ) -> AnswerFeedback | None:
        """The voter's vote on that answer, or None."""
        ...


_COLUMNS = "tenant_id, conversation_id, answer_ts, voter, agent_id, agent_version, vote, reason, at"


class PostgresFeedbackStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def record_vote(self, feedback: AnswerFeedback) -> None:
        async with tenant_transaction(self._pool, feedback.tenant_id) as conn:
            await conn.execute(
                """
                INSERT INTO answer_feedback
                  (tenant_id, conversation_id, answer_ts, voter, agent_id, agent_version,
                   vote, reason, at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, conversation_id, answer_ts, voter) DO UPDATE SET
                  vote = EXCLUDED.vote, reason = EXCLUDED.reason, at = EXCLUDED.at
                """,
                (
                    feedback.tenant_id,
                    feedback.conversation_id,
                    feedback.answer_ts,
                    feedback.voter,
                    feedback.agent_id,
                    feedback.agent_version,
                    feedback.vote,
                    feedback.reason,
                    feedback.at,
                ),
            )

    async def set_reason(
        self, tenant_id: str, conversation_id: str, answer_ts: str, voter: str, reason: str
    ) -> bool:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                UPDATE answer_feedback SET reason = %s
                WHERE tenant_id = %s AND conversation_id = %s AND answer_ts = %s
                  AND voter = %s AND vote = 'down'
                """,
                (reason, tenant_id, conversation_id, answer_ts, voter),
            )
            return cur.rowcount == 1

    async def get(
        self, tenant_id: str, conversation_id: str, answer_ts: str, voter: str
    ) -> AnswerFeedback | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"""
                SELECT {_COLUMNS} FROM answer_feedback
                WHERE tenant_id = %s AND conversation_id = %s AND answer_ts = %s AND voter = %s
                """,
                (tenant_id, conversation_id, answer_ts, voter),
            )
            row = await cur.fetchone()
        return None if row is None else AnswerFeedback(*row)


class InMemoryFeedbackStore:
    def __init__(self) -> None:
        self._rows: dict[tuple[str, str, str, str], AnswerFeedback] = {}

    async def record_vote(self, feedback: AnswerFeedback) -> None:
        key = (feedback.tenant_id, feedback.conversation_id, feedback.answer_ts, feedback.voter)
        self._rows[key] = feedback

    async def set_reason(
        self, tenant_id: str, conversation_id: str, answer_ts: str, voter: str, reason: str
    ) -> bool:
        key = (tenant_id, conversation_id, answer_ts, voter)
        row = self._rows.get(key)
        if row is None or row.vote != "down":
            return False
        self._rows[key] = replace(row, reason=reason)
        return True

    async def get(
        self, tenant_id: str, conversation_id: str, answer_ts: str, voter: str
    ) -> AnswerFeedback | None:
        return self._rows.get((tenant_id, conversation_id, answer_ts, voter))
