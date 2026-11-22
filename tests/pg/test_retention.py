"""Retention deletes, per tenant, only what has expired, and keeps what a live task reads."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from agentos import retention
from agentos.config import ConfigError, Settings
from agentos.retention import (
    MIN_TASK_DAYS,
    PostgresRetentionStore,
    build_set_parser,
    parse_policy,
    purge_all,
    run_set,
    set_limits,
)
from tests.pg.conftest import TestDatabase

NOW = datetime(2026, 11, 16, 4, 0, tzinfo=UTC)


def ago(days: float) -> datetime:
    return NOW - timedelta(days=days)


def seed(db: TestDatabase, tenant: str, retention: str = "{}") -> None:
    """Old and young rows of every kind a retention run reads, and of the kinds it never deletes."""
    with db.owner() as c:
        c.execute(
            "INSERT INTO tenants (tenant_id, slack_team_id, default_agent, retention) "
            "VALUES (%s, %s, 'aa', %s::jsonb)",
            (tenant, f"T-{tenant}", retention),
        )
        # An old thread, a thread with an old and a new message, and a new thread.
        for conv, seq, at in [("old", 1, 400), ("old", 2, 399), ("live", 1, 400), ("live", 2, 1)]:
            c.execute(
                "INSERT INTO conversation_messages "
                "(tenant_id, conversation_id, seq, role, content, created_at) "
                "VALUES (%s, %s, %s, 'user', '[]', %s)",
                (tenant, conv, seq, ago(at)),
            )
        for conv, use, at in [("old", "u1", 400), ("live", "u2", 400), ("live", "u3", 1)]:
            c.execute(
                "INSERT INTO tool_outputs "
                "(tenant_id, conversation_id, tool_use_id, content, is_error, created_at) "
                "VALUES (%s, %s, %s, 'x', false, %s)",
                (tenant, conv, use, ago(at)),
            )
        for conv, use, at in [("old", "u1", 400), ("live", "u2", 400), ("live", "u3", 1)]:
            c.execute(
                "INSERT INTO tool_inputs (tenant_id, conversation_id, tool_use_id, input, created_at) "
                "VALUES (%s, %s, %s, '{}', %s)",
                (tenant, conv, use, ago(at)),
            )
            c.execute(
                "INSERT INTO model_steps (tenant_id, conversation_id, step_key, stop_reason, content, "
                "created_at) VALUES (%s, %s, %s, 'end_turn', '[]', %s)",
                (tenant, conv, use, ago(at)),
            )
        # A task whose every revision is old, and a task edited a day ago.
        for task, revision, at in [
            ("done", 1, 400),
            ("done", 2, 399),
            ("open", 1, 400),
            ("open", 2, 1),
        ]:
            c.execute(
                "INSERT INTO task_drafts (tenant_id, task_id, revision, payload, created_at) "
                "VALUES (%s, %s, %s, '{}', %s)",
                (tenant, task, revision, ago(at)),
            )
        # Delivered long ago, delivered recently, never delivered, and a saved edit long delivered.
        for event, delivered in [
            ("old", 400),
            ("recent", 10),
            ("pending", None),
            ("edit:old", 400),
        ]:
            c.execute(
                "INSERT INTO inbound_events (tenant_id, event_id, payload, received_at, delivered_at) "
                "VALUES (%s, %s, '{}', %s, %s)",
                (tenant, event, ago(400), None if delivered is None else ago(delivered)),
            )
        for thread, at in [("idle", 400), ("active", 10)]:
            c.execute(
                "INSERT INTO threads (tenant_id, thread_key, agent_id, updated_at) "
                "VALUES (%s, %s, 'aa', %s)",
                (tenant, thread, ago(at)),
            )
        for voter, at in [("old", 400), ("young", 1)]:
            c.execute(
                "INSERT INTO answer_feedback "
                "(tenant_id, conversation_id, answer_ts, voter, agent_id, agent_version, "
                "vote, reason, at) VALUES (%s, 'c', 'a1', %s, 'aa', 1, 'down', 'why', %s)",
                (tenant, voter, ago(at)),
            )
        for asker, day in [
            ("long-ago", "2026-11-01"),
            ("yesterday", "2026-11-15"),
            ("today", "2026-11-16"),
        ]:
            c.execute(
                "INSERT INTO owner_pings (tenant_id, agent_id, channel, asker_user_id, notified_on) "
                "VALUES (%s, 'aa', 'C1', %s, %s)",
                (tenant, asker, day),
            )
        for key, at in [("old", 400), ("middle", 40), ("young", 10)]:
            c.execute(
                "INSERT INTO idempotency_records (tenant_id, idem_key, value, created_at) "
                "VALUES (%s, %s, '{}', %s)",
                (tenant, key, ago(at)),
            )
        for session, expired_days_ago in [("gone", 3), ("just-expired", 0.5), ("valid", -2)]:
            c.execute(
                "INSERT INTO sessions (tenant_id, session_hash, user_id, csrf_secret, expires_at) "
                "VALUES (%s, %s, 'u', 'c', %s)",
                (tenant, session, ago(expired_days_ago)),
            )
        c.execute(
            "INSERT INTO external_writes (tenant_id, idem_key, state, result_ref, lease_until, "
            "lease_token, created_at) VALUES (%s, 'w', 'done', 'r', %s, 't', %s)",
            (tenant, ago(400), ago(400)),
        )
        c.execute(
            "INSERT INTO task_links (tenant_id, page_id, task_id) VALUES (%s, 'p', 'k')", (tenant,)
        )
        c.execute(
            "INSERT INTO audit_events (tenant_id, event_key, kind, agent_id, agent_version, "
            "requester_user_id, identity, outcome, at) "
            "VALUES (%s, 'e', 'answer', 'aa', 1, 'u', 'agent:aa', 'ok', %s)",
            (tenant, ago(400)),
        )
        c.execute(
            "INSERT INTO usage_events (tenant_id, usage_key, kind, model, at) "
            "VALUES (%s, 'k', 'embedding', 'm', %s)",
            (tenant, ago(400)),
        )


def remaining(db: TestDatabase, tenant: str) -> dict[str, set]:
    queries = {
        "messages": "SELECT conversation_id || ':' || seq FROM conversation_messages",
        "tool_outputs": "SELECT tool_use_id FROM tool_outputs",
        "tool_inputs": "SELECT conversation_id || ':' || tool_use_id FROM tool_inputs",
        "model_steps": "SELECT conversation_id || ':' || step_key FROM model_steps",
        "task_drafts": "SELECT task_id || ':' || revision FROM task_drafts",
        "inbound_events": "SELECT event_id FROM inbound_events",
        "threads": "SELECT thread_key FROM threads",
        "owner_pings": "SELECT asker_user_id FROM owner_pings",
        "answer_feedback": "SELECT voter FROM answer_feedback",
        "idempotency": "SELECT idem_key FROM idempotency_records",
        "sessions": "SELECT session_hash FROM sessions",
        "external_writes": "SELECT idem_key FROM external_writes",
        "task_links": "SELECT page_id FROM task_links",
        "audit": "SELECT event_key FROM audit_events",
        "usage": "SELECT usage_key FROM usage_events",
    }
    with db.owner() as c:
        return {
            name: {r[0] for r in c.execute(f"{sql} WHERE tenant_id = %s", (tenant,)).fetchall()}
            for name, sql in queries.items()
        }


async def test_only_this_tenants_expired_rows_go_and_what_a_live_task_reads_stays(
    db: TestDatabase, pool, tenant: str, other_tenant: str
):
    """Fails if another tenant's rows, audit or usage rows, a live thread, or a row a task reads is deleted."""
    seed(db, tenant)
    seed(db, other_tenant)
    before_other = remaining(db, other_tenant)

    total = await purge_all(PostgresRetentionStore(pool), tenant, clock=lambda: NOW)

    after = remaining(db, tenant)
    assert after["messages"] == {"live:1", "live:2"}  # the thread with a new message is whole
    assert after["tool_outputs"] == {"u2", "u3"}  # in a live thread
    assert after["tool_inputs"] == after["model_steps"] == {"live:u2", "live:u3"}
    assert after["task_drafts"] == {"open:1", "open:2"}  # a task edited a day ago is whole
    assert after["inbound_events"] == {"recent", "pending"}  # delivered long ago, edits too, go
    assert after["threads"] == {"active"} and after["owner_pings"] == {"yesterday", "today"}
    assert after["answer_feedback"] == {"young"}  # the old vote and its reason go with the row
    assert after["idempotency"] == {"middle", "young"}  # younger than the longest task life
    assert after["sessions"] == {"just-expired", "valid"}
    assert after["external_writes"] == {"w"} and after["task_links"] == {"p"}
    assert after["audit"] == {"e"} and after["usage"] == {"k"}
    assert (total.messages, total.tool_outputs, total.idempotency, total.sessions) == (2, 1, 1, 1)
    assert (total.tool_inputs, total.model_steps, total.task_drafts, total.inbound_events) == (
        1,
        1,
        2,
        2,
    )
    assert (total.threads, total.owner_pings) == (1, 1)
    assert total.answer_feedback == 1
    assert remaining(db, other_tenant) == before_other


async def test_a_stored_limit_below_the_task_life_is_raised_to_it_at_run_time(
    db: TestDatabase, pool, tenant: str
):
    """Fails if a hand-edited short limit deletes a record an open approval or watch may use."""
    seed(db, tenant, retention='{"idempotency_days": 1, "messages_days": 1}')

    policy = await PostgresRetentionStore(pool).policy(tenant)

    assert policy.idempotency_days == policy.messages_days == MIN_TASK_DAYS
    await purge_all(PostgresRetentionStore(pool), tenant, clock=lambda: NOW)
    assert remaining(db, tenant)["idempotency"] == {"middle", "young"}


async def test_nothing_a_live_task_reads_is_purged_under_a_short_stored_limit(
    db: TestDatabase, pool, tenant: str
):
    """Fails if a hand-edited short limit deletes a draft, an event or a thread younger than a task lives."""
    short = '{"idempotency_days": 1, "messages_days": 1, "tool_outputs_days": 1}'
    seed(db, tenant, retention=short)
    with db.owner() as c:
        c.execute(
            "INSERT INTO task_drafts (tenant_id, task_id, revision, payload, created_at) "
            "VALUES (%s, 'mid', 1, '{}', %s)",
            (tenant, ago(40)),
        )
        c.execute(
            "INSERT INTO inbound_events (tenant_id, event_id, payload, delivered_at) "
            "VALUES (%s, 'mid', '{}', %s)",
            (tenant, ago(40)),
        )
        c.execute(
            "INSERT INTO threads (tenant_id, thread_key, agent_id, updated_at) "
            "VALUES (%s, 'mid', 'aa', %s)",
            (tenant, ago(40)),
        )

    await purge_all(PostgresRetentionStore(pool), tenant, clock=lambda: NOW)

    after = remaining(db, tenant)
    assert "mid:1" in after["task_drafts"] and "mid" in after["inbound_events"]
    assert "mid" in after["threads"]


async def test_set_stores_the_limit_and_audits_it_but_refuses_one_below_the_task_life(
    db: TestDatabase, pool, tenant: str
):
    """Fails if set accepts a limit under the task life, or changes limits with no audit event."""
    seed(db, tenant)

    with pytest.raises(ConfigError, match="idempotency_days"):
        await set_limits(pool, tenant, {"idempotency_days": MIN_TASK_DAYS - 1}, "operator:o")
    await set_limits(pool, tenant, {"messages_days": 200, "sessions_days": 0}, "operator:o")

    policy = await PostgresRetentionStore(pool).policy(tenant)
    assert (policy.messages_days, policy.sessions_days, policy.idempotency_days) == (200, 0, 90)
    with db.owner() as c:
        events = c.execute(
            "SELECT kind, agent_id, identity, target FROM audit_events "
            "WHERE tenant_id = %s AND kind = 'retention_set'",
            (tenant,),
        ).fetchall()
    assert events == [("retention_set", None, "operator:o", "messages_days=200,sessions_days=0")]


async def test_a_retried_set_records_one_event_and_a_new_invocation_records_again(
    db: TestDatabase, pool, tenant: str
):
    """Fails if a set retried with the same request ID records a second event, or a new ID does not record again."""
    seed(db, tenant)

    await set_limits(pool, tenant, {"messages_days": 200}, "operator:o", request_id="inv-1")
    await set_limits(pool, tenant, {"sessions_days": 2}, "operator:o", request_id="inv-1")
    await set_limits(pool, tenant, {"messages_days": 150}, "operator:o", request_id="inv-2")

    policy = await PostgresRetentionStore(pool).policy(tenant)
    assert (policy.messages_days, policy.sessions_days) == (150, 2)
    with db.owner() as c:
        events = c.execute(
            "SELECT kind FROM audit_events WHERE tenant_id = %s AND kind = 'retention_set'",
            (tenant,),
        ).fetchall()
    assert events == [("retention_set",), ("retention_set",)]


async def test_the_set_command_refuses_a_low_limit_and_stores_a_valid_one(
    db: TestDatabase, tenant: str, monkeypatch, capsys
):
    """Fails if the command line writes a refused limit, or reports success for it."""
    seed(db, tenant)
    monkeypatch.setenv("TEST_OWNER_URL", db.owner_url)
    argv = lambda days: [
        "--tenant",
        tenant,
        "--url-var",
        "TEST_OWNER_URL",
        "--tool-outputs-days",
        days,
    ]

    refused = await run_set(build_set_parser().parse_args(argv("3")))
    stored = await run_set(build_set_parser().parse_args(argv(str(MIN_TASK_DAYS))))

    assert (refused, stored) == (2, 0)
    out = capsys.readouterr().out
    assert "refused" in out and f"tool_outputs_days={MIN_TASK_DAYS}" in out
    with db.owner() as c:
        (limits,) = c.execute(
            "SELECT retention FROM tenants WHERE tenant_id = %s", (tenant,)
        ).fetchone()
    assert limits == {"tool_outputs_days": MIN_TASK_DAYS}


async def test_the_purge_command_deletes_the_tenants_expired_rows_and_prints_counts(
    db: TestDatabase, tenant: str, other_tenant: str, monkeypatch, capsys
):
    """Fails if the command line purges another tenant, prints row content, or leaves expired rows."""
    seed(db, tenant)
    seed(db, other_tenant)
    before_other = remaining(db, other_tenant)
    monkeypatch.setattr(retention, "load_settings", lambda: Settings(database_url=db.app_url))

    async def stored(_url):
        return SimpleNamespace(tenants={tenant: None, other_tenant: None})

    monkeypatch.setattr(retention, "load_stored_tenants", stored)

    await retention.run(tenant, schedule=False, clock=lambda: NOW)

    assert remaining(db, other_tenant) == before_other
    assert remaining(db, tenant)["idempotency"] == {"middle", "young"}
    assert capsys.readouterr().out.strip() == (
        f"tenant_id={tenant} messages=2 tool_outputs=1 idempotency=1 sessions=1 tool_inputs=1 "
        "model_steps=1 inbound_events=2 task_drafts=2 threads=1 owner_pings=1 answer_feedback=1"
    )


async def test_a_full_batch_is_followed_by_another_until_nothing_is_due(
    db: TestDatabase, pool, tenant: str
):
    """Fails if a purge stops after one batch while more expired rows remain."""
    with db.owner() as c:
        c.execute(
            "INSERT INTO tenants (tenant_id, slack_team_id, default_agent) VALUES (%s, %s, 'aa')",
            (tenant, f"T-{tenant}"),
        )
        for i in range(25):
            c.execute(
                "INSERT INTO idempotency_records (tenant_id, idem_key, value, created_at) "
                "VALUES (%s, %s, '{}', %s)",
                (tenant, f"k{i}", ago(400)),
            )
    store = PostgresRetentionStore(pool)
    policy = await store.policy(tenant)

    first = await store.purge_batch(tenant, policy, NOW, limit=10)
    assert (first.idempotency, first.more) == (10, True)
    rest = await purge_all(_Small(store), tenant, clock=lambda: NOW)

    assert rest.idempotency == 15
    assert remaining(db, tenant)["idempotency"] == set()


class _Small:
    """The store with a batch of 10, as a purge run would see it."""

    def __init__(self, store):
        self._store = store

    async def policy(self, tenant_id):
        return await self._store.policy(tenant_id)

    async def purge_batch(self, tenant_id, policy, now, limit=10):
        return await self._store.purge_batch(tenant_id, policy, now, limit=10)


@pytest.mark.parametrize(
    "raw",
    [{"messages_days": -1}, {"sessions_days": "7"}, {"messages_days": True}, {"audit_days": 3}],
    ids=["negative", "text", "boolean", "unknown kind"],
)
def test_a_malformed_retention_setting_is_refused(raw):
    """Fails if a malformed or unknown retention setting is accepted and acted on."""
    with pytest.raises(ConfigError):
        parse_policy(raw)
