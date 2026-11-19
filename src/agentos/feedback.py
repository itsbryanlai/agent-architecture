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
from typing import Any, Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.stores import joined_transaction, tenant_transaction

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


def _fence_lang_after(text: str, lang: str | None) -> str | None:
    """The code fence left open by ``text``, given ``lang`` open before it (None outside).

    A line starting with ``` opens a fence (its language tag is the rest of the line) or
    closes the open one, so each section can be rendered on its own.
    """
    for line in text.split("\n"):
        marker = line.strip()
        if marker.startswith("```"):
            lang = None if lang is not None else marker[3:].strip()
    return lang


def _section_texts(text: str) -> list[str]:
    """The answer's text as Slack section texts: each non-empty and at most
    ``SECTION_TEXT_MAX`` characters, cut at a blank line, else a newline, else hard (only a
    line longer than a section is cut mid-line). Only the matched delimiter is dropped at a
    cut, so a blank line that is not the delimiter stays in the text. A cut inside a code
    fence closes it and the next section reopens it with the same language tag.
    """
    pieces: list[str] = []
    rest = text
    lang: str | None = None
    while rest:
        prefix = f"```{lang}\n" if lang is not None else ""
        closer = "\n```" if _fence_lang_after(rest, lang) is not None else ""
        if len(prefix) + len(rest) + len(closer) <= SECTION_TEXT_MAX:
            pieces.append(prefix + rest + closer)
            break
        budget = SECTION_TEXT_MAX - len(prefix) - (len("\n```") if lang is not None else 0)
        cut = rest.rfind("\n\n", 0, budget + 1)
        delim = 2
        if cut <= 0:
            cut = rest.rfind("\n", 0, budget + 1)
            delim = 1
        if cut <= 0:
            cut = budget
            delim = 0
        piece = rest[:cut]
        end_lang = _fence_lang_after(piece, lang)
        if end_lang is not None and len(prefix) + len(piece) + len("\n```") > SECTION_TEXT_MAX:
            # No newline left room for the closing fence: cut short enough to carry it.
            cut = SECTION_TEXT_MAX - len(prefix) - len("\n```")
            piece = rest[:cut]
            delim = 0
            end_lang = _fence_lang_after(piece, lang)
        closer = "\n```" if end_lang is not None else ""
        pieces.append(prefix + piece + closer)
        rest = rest[cut + delim :]
        lang = end_lang
    return pieces


def answer_blocks(
    text: str, conversation_id: str, answer_ts: str, agent_id: str, agent_version: int
) -> list[dict]:
    """The answer as Slack shows it with its buttons: the text in section blocks, then 👍/👎.

    The text is split at newlines into sections of at most ``SECTION_TEXT_MAX`` characters
    (a hard cut only when a single line is longer); the full text stays the message's
    fallback. A code fence longer than a section is closed at the cut and reopened with the
    same language tag in the next section, so every section renders on its own.
    """
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": piece}}
        for piece in _section_texts(text)
    ]
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
    async def record_vote(self, feedback: AnswerFeedback, conn: Any = None) -> None:
        """Store the vote, replacing this voter's earlier vote on the same answer.

        The replacement clears any reason the earlier vote carried. With ``conn`` (a unit of
        work's connection) the vote commits with that transaction, alongside its audit event.
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

    async def record_vote(self, feedback: AnswerFeedback, conn: Any = None) -> None:
        async with joined_transaction(self._pool, feedback.tenant_id, conn) as joined:
            await joined.execute(
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

    def snapshot(self) -> dict[tuple[str, str, str, str], AnswerFeedback]:
        """The votes as they are now, for ``InMemoryUnitOfWork``."""
        return dict(self._rows)

    def restore(self, state: dict[tuple[str, str, str, str], AnswerFeedback]) -> None:
        self._rows = dict(state)

    async def record_vote(self, feedback: AnswerFeedback, conn: Any = None) -> None:
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
