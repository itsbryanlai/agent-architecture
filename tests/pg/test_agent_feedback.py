"""The feedback page's reads on the real database: one query per section, scoped to the
tenant and the agent, and question text that follows retention."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from psycopg.types.json import Jsonb

from agentos.agent_feedback import NO_ANSWER_SHA256, PostgresAgentFeedbackStore
from agentos.audit import canonical_sha256
from agentos.retention import PostgresRetentionStore, RetentionPolicy
from agentos.stores import tenant_transaction


async def seed_agent_rows(pool, tenant: str, agent: str, question: str) -> None:
    """One row per section for this tenant and agent, a stored question, and two votes.

    The answer that is a real answer and the tool call that succeeded match no section.
    """
    conv = f"conv-{agent}"
    async with tenant_transaction(pool, tenant) as conn:
        await conn.execute(
            "INSERT INTO conversation_messages (tenant_id, conversation_id, seq, role, content) "
            "VALUES (%s, %s, 1, 'user', %s)",
            (tenant, conv, Jsonb([{"type": "text", "text": question}])),
        )
        rows = [
            ("answer", None, "ok", NO_ANSWER_SHA256, conv),
            ("answer", None, "ok", canonical_sha256("a real answer"), conv),
            ("notice", "failure", "ok", None, conv),
            ("tool_call", "search_items", "error", None, conv),
            ("tool_call", "search_items", "ok", None, conv),
            ("audience_refused", "health", "ok", None, conv),
            ("approval_closed", "declined", "ok", None, None),
            ("write", "add_comment", "error", None, None),
        ]
        for i, (kind, tool, outcome, out, conversation) in enumerate(rows):
            await conn.execute(
                "INSERT INTO audit_events (tenant_id, event_key, kind, agent_id, agent_version, "
                "requester_user_id, identity, outcome, conversation_id, tool, output_sha256) "
                "VALUES (%s, %s, %s, %s, 1, 'u', %s, %s, %s, %s, %s)",
                (
                    tenant,
                    f"{tenant}:seed:{agent}:{i}",
                    kind,
                    agent,
                    f"agent:{agent}",
                    outcome,
                    conversation,
                    tool,
                    out,
                ),
            )
        for voter, ts, vote, reason in (
            ("v1", "1.1", "down", "the reason"),
            ("v2", "2.2", "up", None),
        ):
            await conn.execute(
                "INSERT INTO answer_feedback (tenant_id, conversation_id, answer_ts, voter, "
                "agent_id, agent_version, vote, reason) VALUES (%s, %s, %s, %s, %s, 1, %s, %s)",
                (tenant, conv, ts, voter, agent, vote, reason),
            )


async def test_the_feedback_queries_are_tenant_and_agent_scoped(pool, tenant, other_tenant):
    """Fails if a section or a vote total counts another tenant's or another agent's rows."""
    mine = f"the question of {tenant}/aa"
    await seed_agent_rows(pool, tenant, "aa", mine)
    await seed_agent_rows(pool, tenant, "bb", f"the question of {tenant}/bb")
    await seed_agent_rows(pool, other_tenant, "aa", f"the question of {other_tenant}/aa")
    store = PostgresAgentFeedbackStore(pool)
    view = await store.read(tenant, "aa", datetime.now(UTC) - timedelta(days=30))
    assert {s.key: s.total for s in view.sections} == {
        "unanswered": 1,
        "failed_turns": 1,
        "refused": 2,
        "approvals_closed": 1,
        "writes_failed": 1,
        "votes": 1,
    }
    assert (view.ups, view.downs) == (1, 1)
    unanswered = next(s for s in view.sections if s.key == "unanswered")
    assert unanswered.rows[0].question == mine
    assert (unanswered.rows[0].conversation_id, unanswered.rows[0].question_seq) == ("conv-aa", 1)
    votes = next(s for s in view.sections if s.key == "votes")
    assert votes.rows[0].reason == "the reason"
    for section in view.sections:
        for row in section.rows:
            assert row.question in (None, mine)


async def test_a_purged_question_leaves_the_count_and_date_not_the_text(pool, tenant):
    """Fails if the page shows a question text retention purged, or hides one it keeps."""
    now = datetime.now(UTC)
    old = now - timedelta(days=100)
    async with tenant_transaction(pool, tenant) as conn:
        for conv, text, at in (
            ("c-old", "the old question", old),
            ("c-new", "the recent question", now),
        ):
            await conn.execute(
                "INSERT INTO conversation_messages "
                "(tenant_id, conversation_id, seq, role, content, created_at) "
                "VALUES (%s, %s, 1, 'user', %s, %s)",
                (tenant, conv, Jsonb([{"type": "text", "text": text}]), at),
            )
            await conn.execute(
                "INSERT INTO audit_events (tenant_id, event_key, kind, agent_id, agent_version, "
                "requester_user_id, identity, outcome, conversation_id, output_sha256, at) "
                "VALUES (%s, %s, 'answer', 'aa', 1, 'u', 'agent:aa', 'ok', %s, %s, %s)",
                (tenant, f"{tenant}:{conv}", conv, NO_ANSWER_SHA256, at),
            )
    store = PostgresAgentFeedbackStore(pool)
    since = now - timedelta(days=365)
    view = await store.read(tenant, "aa", since)
    section = next(s for s in view.sections if s.key == "unanswered")
    assert section.total == 2
    assert {r.question for r in section.rows} == {"the old question", "the recent question"}
    # The nightly purge removes the idle old conversation; the audit rows are kept.
    counts = await PostgresRetentionStore(pool).purge_batch(
        tenant,
        RetentionPolicy(
            messages_days=68, tool_outputs_days=68, idempotency_days=68, sessions_days=1
        ),
        now,
    )
    assert counts.messages == 1
    view = await store.read(tenant, "aa", since)
    section = next(s for s in view.sections if s.key == "unanswered")
    assert section.total == 2
    assert {r.question for r in section.rows} == {None, "the recent question"}
    old_row = next(r for r in section.rows if r.question is None)
    assert (old_row.at, old_row.agent_version) == (old, 1)
