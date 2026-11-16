"""Every Postgres store, through its public methods, against the real schema.

Each test drives one store far enough that every statement it holds runs once, including
its ON CONFLICT and paging clauses, and asserts what a caller would see.
"""

from __future__ import annotations

import copy
import dataclasses
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from psycopg import errors

from agentos.agent_store import AgentStoreError, PostgresAgentStore
from agentos.audience_pauses import PostgresPauseStore
from agentos.audit import PostgresAuditStore
from agentos.budgets import BudgetNotFound, PostgresBudgetStore
from agentos.config import ConfigError, parse_agents_config
from agentos.knowledge.store import PageSync, PostgresKnowledgeStore
from agentos.members import MemberError, MemberNotFound, PostgresMemberStore
from agentos.models import AuditEvent
from agentos.sessions import PostgresSessionStore
from agentos.stores import (
    PostgresExternalWriteStore,
    PostgresHistoryStore,
    PostgresIdempotencyStore,
    PostgresTaskLinkStore,
    PostgresToolOutputStore,
    tenant_transaction,
)
from agentos.switches import PostgresSwitchStore
from agentos.tenant_store import PostgresTenantStore
from agentos.usage import PostgresUsageStore, UsageEvent
from tests.conftest import agents_dict
from tests.pg.conftest import TestDatabase

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)


def unit_vector(position: int) -> list[float]:
    return [1.0 if i == position else 0.0 for i in range(1024)]


async def test_history_appends_once_per_step_and_loads_in_order(pool, tenant: str):
    """Fails if a repeated step is stored twice, or messages load out of order."""
    store = PostgresHistoryStore(pool)
    first = await store.append(tenant, "c", "s1", "user", [{"type": "text", "text": "hi"}])
    again = await store.append(tenant, "c", "s1", "user", [{"type": "text", "text": "hi"}])
    second = await store.append(tenant, "c", "s2", "assistant", [{"type": "text", "text": "yo"}])
    assert (first, again, second) == (1, 1, 2)
    assert [m["role"] for m in await store.load(tenant, "c")] == ["user", "assistant"]
    assert await store.load(tenant, "other") == []


async def test_idempotency_record_keeps_the_first_value(pool, tenant: str):
    """Fails if a second write replaces the value an earlier attempt stored."""
    store = PostgresIdempotencyStore(pool)
    assert await store.get(tenant, "k") is None
    assert await store.put_if_absent(tenant, "k", {"page": "one"}) == {"page": "one"}
    assert await store.put_if_absent(tenant, "k", {"page": "two"}) == {"page": "one"}
    assert await store.get(tenant, "k") == {"page": "one"}


async def test_tool_outputs_keep_the_first_text_and_read_back_by_id(pool, tenant: str):
    """Fails if a repeated put overwrites a tool output, or a read returns an ID it was not asked for."""
    store = PostgresToolOutputStore(pool)
    await store.put(tenant, "c", "u1", "first", False)
    await store.put(tenant, "c", "u1", "second", True)
    await store.put(tenant, "c", "u2", "boom", True)
    got = await store.get_many(tenant, "c", ["u1", "u2", "missing"])
    assert got == {"u1": ("first", False), "u2": ("boom", True)}
    assert await store.get_many(tenant, "c", []) == {}


async def test_task_link_keeps_the_first_task_for_a_page(pool, tenant: str):
    """Fails if a second link replaces the task a page was first linked to."""
    store = PostgresTaskLinkStore(pool)
    assert await store.task_for_page(tenant, "p") is None
    await store.link(tenant, "p", "task-1")
    await store.link(tenant, "p", "task-2")
    assert await store.task_for_page(tenant, "p") == "task-1"


async def test_knowledge_store_replaces_pages_and_searches_by_distance(
    pool, tenant: str, other_tenant: str
):
    """Fails if a re-index leaves old chunks, search ignores scope or tenant, or the nearest chunk is not first."""
    store = PostgresKnowledgeStore(pool)
    chunks = [("about apples", unit_vector(0)), ("about pears", unit_vector(1))]
    await store.replace_page(tenant, "s1", "p1", "Fruit", "https://n/p1", "h1", chunks)
    await store.replace_page(tenant, "s2", "p2", "Hidden", "https://n/p2", "h2", chunks[:1])
    await store.replace_page(other_tenant, "s1", "p3", "Theirs", "https://n/p3", "h3", chunks[:1])

    hits = await store.search(tenant, ["s1"], unit_vector(1), limit=5)
    assert [(h.page_id, h.text) for h in hits] == [("p1", "about pears"), ("p1", "about apples")]
    assert hits[0].distance == pytest.approx(0.0, abs=1e-6)
    assert (hits[0].title, hits[0].url) == ("Fruit", "https://n/p1")
    assert await store.search(tenant, [], unit_vector(1), limit=5) == []
    assert len(await store.search(tenant, ["s1", "s2"], unit_vector(0), limit=5)) == 3

    await store.replace_page(tenant, "s1", "p1", "Fruit v2", "https://n/p1", "h1b", chunks[:1])
    assert await store.page_hashes(tenant, "s1") == {"p1": "h1b"}
    assert [h.text for h in await store.search(tenant, ["s1"], unit_vector(1), limit=5)] == [
        "about apples"
    ]
    assert await store.page_meta(tenant, ["s1"], ["p1", "p2"]) == {
        "p1": ("Fruit v2", "https://n/p1")
    }
    assert sorted(await store.scopes(tenant)) == ["s1", "s2"]
    assert await store.delete_pages(tenant, "s1", ["p1", "nope"]) == 1
    assert await store.delete_scope(tenant, "s2") == 1
    assert await store.scopes(tenant) == []
    assert await store.scopes(other_tenant) == ["s1"]


async def test_knowledge_page_state_round_trips_and_touch_updates_it(pool, tenant: str):
    """Fails if the sync columns do not survive a store and read, an upsert keeps stale state, or a touch changes chunks."""
    store = PostgresKnowledgeStore(pool)
    sync = PageSync(
        "m1", "2026-01-01T00:00:00.000Z", "par", ["root", "mid"], ["c1"], ["d1"], ["r1"]
    )
    assert await store.page_state(tenant, "s", "p") is None

    await store.replace_page(tenant, "s", "p", "T", "u", "h1", [("a", unit_vector(0))], sync)
    state = await store.page_state(tenant, "s", "p")
    assert (state.content_hash, state.sync) == ("h1", sync)
    assert state.indexed_at.tzinfo is not None

    newer = dataclasses.replace(sync, embed_model="m2", ancestors=["root"], row_page_ids=[])
    await store.replace_page(tenant, "s", "p", "T", "u", "h2", [("a", unit_vector(0))], newer)
    assert (await store.page_state(tenant, "s", "p")).sync == newer

    latest = dataclasses.replace(newer, last_edited_time="2026-02-02T00:00:00.000Z")
    await store.touch_page(tenant, "s", "p", "h3", latest)
    await store.touch_page(tenant, "s", "missing", "h", latest)
    state = await store.page_state(tenant, "s", "p")
    assert (state.content_hash, state.sync) == ("h3", latest)
    assert await store.page_state(tenant, "s", "missing") is None
    assert len(await store.search(tenant, ["s"], unit_vector(0), limit=5)) == 1

    await store.replace_page(tenant, "s", "legacy", "T", "u", "h", [])
    assert await store.count_pages_without_model(tenant) == 1


async def test_knowledge_insert_stores_every_chunk_of_a_page_in_order(pool, tenant: str):
    """Fails if the one multi-row insert drops, reorders or duplicates chunks, or fails for a page without text."""
    store = PostgresKnowledgeStore(pool)
    chunks = [(f"chunk {n}", unit_vector(n)) for n in range(7)]
    await store.replace_page(tenant, "s", "p", "T", "u", "h", chunks)
    await store.replace_page(tenant, "s", "empty", "T", "u", "h", [])

    hits = await store.search(tenant, ["s"], unit_vector(0), limit=20)

    assert sorted((h.chunk_no, h.text) for h in hits) == [(n, f"chunk {n}") for n in range(7)]
    assert await store.page_state(tenant, "s", "empty") is not None


async def test_knowledge_search_filters_by_model_and_reports_distance_and_position(
    pool, tenant: str
):
    """Fails if a search returns chunks of another embedding model, loses a hit's stored position, or reports a wrong distance."""
    store = PostgresKnowledgeStore(pool)
    old = PageSync(embed_model="old", parent_ref="x", ancestors=["root"])
    new = PageSync(embed_model="new", parent_ref="y", ancestors=["root", "mid"])
    await store.replace_page(tenant, "s", "old-page", "Old", "u", "h", [("o", unit_vector(0))], old)
    await store.replace_page(tenant, "s", "new-page", "New", "u", "h", [("n", unit_vector(1))], new)

    only_new = await store.search(tenant, ["s"], unit_vector(0), limit=5, embed_model="new")
    everything = await store.search(tenant, ["s"], unit_vector(0), limit=5)

    assert [(h.page_id, h.parent_ref, h.ancestors) for h in only_new] == [
        ("new-page", "y", ["root", "mid"])
    ]
    assert only_new[0].distance == pytest.approx(1.0, abs=1e-6)
    assert [h.page_id for h in everything] == ["old-page", "new-page"]
    assert everything[0].distance == pytest.approx(0.0, abs=1e-6)
    assert await store.search(tenant, ["s"], unit_vector(0), limit=5, embed_model="none") == []


async def test_knowledge_delete_keeps_pages_stored_since_the_cutoff(pool, tenant: str):
    """Fails if a delete with indexed_before removes a page stored after that time, or keeps one stored before it."""
    store = PostgresKnowledgeStore(pool)
    for page in ("a", "b"):
        await store.replace_page(tenant, "s", page, "T", "u", "h", [])
    past = datetime.now(UTC) - timedelta(hours=1)
    future = datetime.now(UTC) + timedelta(hours=1)

    assert await store.delete_pages(tenant, "s", ["a", "b"], indexed_before=past) == 0
    assert await store.delete_pages(tenant, "s", ["a"], indexed_before=future) == 1
    assert await store.delete_pages(tenant, "s", ["b"]) == 1


async def test_scope_health_counts_each_run_once_and_clears(pool, tenant: str, other_tenant: str):
    """Fails if one run counts twice, a new run does not count, notification is lost, clearing misses, or tenants share a row."""
    store = PostgresKnowledgeStore(pool)

    first = await store.record_scope_gone(tenant, "s", "run-1")
    again = await store.record_scope_gone(tenant, "s", "run-1")
    second = await store.record_scope_gone(tenant, "s", "run-2")
    assert (first.consecutive_gone, again.consecutive_gone, second.consecutive_gone) == (1, 1, 2)
    assert second.notified_at is None
    await store.mark_scope_notified(tenant, "s")
    assert (await store.record_scope_gone(tenant, "s", "run-2")).notified_at is not None
    assert (await store.record_scope_gone(other_tenant, "s", "run-1")).consecutive_gone == 1

    await store.clear_scope_health(tenant, "s")

    assert (await store.record_scope_gone(tenant, "s", "run-3")).consecutive_gone == 1
    assert (await store.record_scope_gone(other_tenant, "s", "run-1")).consecutive_gone == 1


def audit_event(tenant: str, key: str, **fields) -> AuditEvent:
    base = {
        "tenant_id": tenant,
        "event_key": key,
        "kind": "answer",
        "agent_id": "aa",
        "agent_version": 1,
        "requester_user_id": "u",
        "identity": "agent:aa",
    }
    return AuditEvent(**(base | fields))


async def test_audit_log_records_once_and_pages_newest_first(pool, tenant: str):
    """Fails if a repeated event key stores a second row, or paging skips or repeats an event."""
    store = PostgresAuditStore(pool)
    for n in range(5):
        await store.record(audit_event(tenant, f"e{n}"))
    await store.record(audit_event(tenant, "e0"))
    await store.record(
        audit_event(tenant, "m", kind="member_add", agent_id=None, agent_version=None)
    )

    page1 = await store.list(tenant, limit=4)
    page2 = await store.list(tenant, limit=4, before_seq=page1[-1].seq)
    keys = [e.event_key for e in page1 + page2]
    assert keys == ["m", "e4", "e3", "e2", "e1", "e0"]
    assert page1[0].at is not None
    assert await store.list(tenant, before=datetime(2000, 1, 1, tzinfo=UTC)) == []
    assert len(await store.list(tenant, before=datetime.now(UTC) + timedelta(hours=1))) == 6


async def test_audit_log_refuses_a_kind_the_table_does_not_list(pool, tenant: str):
    """Fails if the foreign key lets an unknown kind in, or needs a privilege the app role lacks."""
    with pytest.raises(errors.ForeignKeyViolation):
        async with tenant_transaction(pool, tenant) as conn:
            await conn.execute(
                "INSERT INTO audit_events (tenant_id, event_key, kind, agent_id, agent_version, "
                "requester_user_id, identity, outcome) "
                "VALUES (%s, 'x', 'not_a_kind', 'aa', 1, 'u', 'agent:aa', 'ok')",
                (tenant,),
            )


async def test_usage_totals_group_a_month_and_count_unpriced_calls(pool, tenant: str):
    """Fails if a repeated usage key counts twice, the window is wrong, or an unpriced call is lost."""
    store = PostgresUsageStore(pool)

    def event(key: str, at: datetime, cost: str | None, **fields) -> UsageEvent:
        base = {
            "tenant_id": tenant,
            "usage_key": key,
            "kind": "llm",
            "agent_id": "aa",
            "agent_version": 1,
            "model": "m",
            "input_tokens": 10,
            "output_tokens": 5,
            "cost": None if cost is None else Decimal(cost),
            "at": at,
        }
        return UsageEvent(**(base | fields))

    await store.record(event("k1", NOW, "0.50000000"))
    await store.record(event("k1", NOW, "9.00000000"))
    await store.record(event("k2", NOW + timedelta(days=1), None))
    await store.record(event("k3", NOW + timedelta(days=40), "1.00000000"))
    await store.record(
        event("k4", NOW, "0.25000000", kind="embedding", agent_id=None, agent_version=None)
    )

    groups = await store.totals(tenant, NOW - timedelta(days=1), NOW + timedelta(days=30))
    by_kind = {g.kind: g for g in groups}
    llm = by_kind["llm"]
    assert (llm.calls, llm.input_tokens, llm.output_tokens) == (2, 20, 10)
    assert (llm.cost, llm.unpriced_calls) == (Decimal("0.5"), 1)
    assert by_kind["embedding"].cost == Decimal("0.25")
    assert await store.totals(tenant, NOW + timedelta(days=60), NOW + timedelta(days=90)) == []


async def test_budgets_upsert_one_row_per_scope_and_remove_by_scope(pool, tenant: str):
    """Fails if setting a budget twice makes two rows, or removing a missing budget passes silently."""
    store = PostgresBudgetStore(pool)
    first = await store.set_budget(tenant, None, Decimal("10.00"), "u1")
    again = await store.set_budget(tenant, None, Decimal("20.00"), "u2")
    await store.set_budget(tenant, "aa", Decimal("5.00"), "u1")
    rows = {b.agent_id: b for b in await store.list(tenant)}
    assert set(rows) == {None, "aa"}
    assert (rows[None].amount, rows[None].updated_by) == (Decimal("20.00"), "u2")
    assert first.updated_at <= again.updated_at
    await store.remove_budget(tenant, "aa")
    with pytest.raises(BudgetNotFound):
        await store.remove_budget(tenant, "aa")
    await store.remove_budget(tenant, None)
    assert await store.list(tenant) == []


async def test_switch_off_and_on_report_whether_anything_changed(pool, tenant: str):
    """Fails if turning a switch off twice reports a change, or the list is not ordered."""
    store = PostgresSwitchStore(pool)
    assert await store.switch_off(tenant, "writes", "", "u", NOW) is True
    assert await store.switch_off(tenant, "writes", "", "u2", NOW) is False
    assert await store.switch_off(tenant, "agent", "aa", "u", NOW) is True
    assert [(s.scope, s.target, s.turned_off_by) for s in await store.list(tenant)] == [
        ("agent", "aa", "u"),
        ("writes", "", "u"),
    ]
    assert await store.switch_on(tenant, "writes", "") is True
    assert await store.switch_on(tenant, "writes", "") is False
    with pytest.raises(errors.CheckViolation):
        await store.switch_off(tenant, "agent", "", "u", NOW)


async def test_pause_opens_once_and_a_resumed_channel_can_pause_again(pool, tenant: str):
    """Fails if a second open creates a second active pause, or a resumed one blocks the next."""
    store = PostgresPauseStore(pool)
    with pytest.raises(errors.CheckViolation):
        await store.open_pause(tenant, "aa", "C1", "someone_asked", NOW)
    first = await store.open_pause(tenant, "aa", "C1", "channel_shared", NOW)
    again = await store.open_pause(tenant, "aa", "C1", "channel_shared", NOW + timedelta(hours=1))
    assert again.paused_at == first.paused_at
    await store.open_pause(tenant, "bb", "C1", "channel_shared", NOW)
    assert (await store.get_active(tenant, "aa", "C1")).paused_at == first.paused_at
    assert [p.agent_id for p in await store.list_active(tenant)] == ["aa", "bb"]
    assert [p.agent_id for p in await store.list_active(tenant, "bb")] == ["bb"]
    closed = await store.close_pause(tenant, "aa", "C1", "admin", NOW + timedelta(hours=2))
    assert (closed.resumed_by, closed.resumed_at) == ("admin", NOW + timedelta(hours=2))
    assert await store.close_pause(tenant, "aa", "C1", "admin", NOW) is None
    assert await store.get_active(tenant, "aa", "C1") is None
    renewed = await store.open_pause(tenant, "aa", "C1", "channel_shared", NOW + timedelta(days=1))
    assert renewed.paused_at == NOW + timedelta(days=1)


async def test_members_are_added_once_and_changed_only_when_they_exist(pool, tenant: str):
    """Fails if a member can be added twice, or a change to a non-member passes silently."""
    store = PostgresMemberStore(pool)
    await store.add_member(tenant, "U2", "creator", "U1")
    await store.add_member(tenant, "U1", "admin", "U1")
    with pytest.raises(MemberError, match="already a member"):
        await store.add_member(tenant, "U1", "creator", "U2")
    await store.set_member_role(tenant, "U2", "admin")
    assert [(m.user_id, m.role) for m in await store.list_members(tenant)] == [
        ("U1", "admin"),
        ("U2", "admin"),
    ]
    assert (await store.get_member(tenant, "U2")).added_by == "U1"
    await store.remove_member(tenant, "U2")
    assert await store.get_member(tenant, "U2") is None
    for change in (
        store.remove_member(tenant, "U2"),
        store.set_member_role(tenant, "U2", "admin"),
    ):
        with pytest.raises(MemberNotFound):
            await change


async def test_sessions_expire_and_creating_one_clears_the_expired(
    db: TestDatabase, pool, tenant: str
):
    """Fails if an expired session is accepted, or expired rows pile up."""
    store = PostgresSessionStore(pool)
    old = await store.create(tenant, "U1", NOW, lifetime=timedelta(hours=1))
    assert (await store.get(tenant, old.token, NOW)).user_id == "U1"
    assert await store.get(tenant, old.token, NOW + timedelta(hours=2)) is None
    fresh = await store.create(tenant, "U2", NOW + timedelta(hours=2))
    with db.owner() as conn:
        rows = conn.execute("SELECT count(*) FROM sessions WHERE tenant_id = %s", (tenant,))
        assert rows.fetchone()[0] == 1
    assert await store.get(tenant, "wrong", NOW) is None
    await store.delete(tenant, fresh.token)
    assert await store.get(tenant, fresh.token, NOW + timedelta(hours=2)) is None


async def test_tenant_store_inserts_once_and_looks_up_through_the_functions(
    pool, tenant: str, other_tenant: str
):
    """Fails if a tenant is stored twice, a shared Slack workspace is accepted, or a lookup misses."""
    config = parse_agents_config(agents_dict())
    mine = dataclasses.replace(
        config,
        tenant_id=tenant,
        slack_team_id=f"T-{tenant}",
        notion_workspace_id=f"w-{tenant}",
        channels={"C1": "it-helper"},
        admins=("U1",),
    )
    store = PostgresTenantStore(pool)
    assert await store.insert(mine) is True
    assert await store.insert(mine) is False
    clash = dataclasses.replace(mine, tenant_id=other_tenant, notion_workspace_id=None)
    with pytest.raises(ConfigError, match="already uses"):
        await store.insert(clash)

    row = await store.load(tenant)
    assert (row.config.channels, row.config.admins) == ({"C1": "it-helper"}, ("U1",))
    assert row.config.notion_workspace_id == f"w-{tenant}" and row.enabled is True
    await store.set_enabled(tenant, False)
    after = await store.load(tenant)
    assert after.enabled is False and after.updated_at > row.updated_at
    ref = await store.ref_for_slack_team(f"T-{tenant}")
    assert (ref.tenant_id, ref.enabled) == (tenant, False)
    assert await store.ref_for_slack_team("T-unknown") is None
    assert tenant in {r.tenant_id for r in await store.list_refs()}
    assert await store.load(other_tenant) is None


def helper_block() -> dict:
    block = copy.deepcopy(agents_dict()["agents"]["it-helper"])
    del block["version"]
    return block


async def test_agent_versions_move_through_the_lifecycle_in_the_store(pool, tenant: str):
    """Fails if a store call skips a lifecycle step, publishes out of order, or leaves two published."""
    store = PostgresAgentStore(pool)
    v1 = await store.create_agent(tenant, "it-helper", helper_block(), "U1")
    assert (v1.version, v1.status) == (1, "draft")
    with pytest.raises(AgentStoreError, match="already exists"):
        await store.create_agent(tenant, "it-helper", helper_block(), "U1")
    edited = helper_block() | {"name": "IT helper 2"}
    assert (await store.replace_draft(tenant, "it-helper", 1, edited)).definition["name"] == (
        "IT helper 2"
    )
    assert (await store.submit(tenant, "it-helper", 1, "U1")).status == "pending"
    assert (await store.withdraw(tenant, "it-helper", 1)).status == "draft"
    await store.submit(tenant, "it-helper", 1, "U1")
    with pytest.raises(AgentStoreError, match="needs"):
        await store.replace_draft(tenant, "it-helper", 1, edited)
    published = await store.publish(tenant, "it-helper", 1, "U2")
    assert (published.status, published.approved_by) == ("published", "U2")

    v2 = await store.create_draft(tenant, "it-helper", "U1")
    assert (v2.version, v2.definition["name"]) == (2, "IT helper 2")
    await store.create_draft(tenant, "it-helper", "U1", from_version=1)
    await store.submit(tenant, "it-helper", 2, "U1")
    await store.submit(tenant, "it-helper", 3, "U1")
    await store.publish(tenant, "it-helper", 3, "U2")
    with pytest.raises(AgentStoreError, match="older than the published"):
        await store.publish(tenant, "it-helper", 2, "U2")
    assert [(r.version, r.status) for r in await store.versions(tenant, "it-helper")] == [
        (3, "published"),
        (2, "pending"),
        (1, "superseded"),
    ]
    assert (await store.published(tenant, "it-helper")).version == 3
    assert list(await store.published_all(tenant)) == ["it-helper"]
    assert (await store.get_version(tenant, "it-helper", 1)).status == "superseded"
    assert await store.get_version(tenant, "it-helper", 9) is None
    assert (await store.get_agent(tenant, "it-helper")).created_by == "U1"
    assert [a.agent_id for a in await store.list_agents(tenant)] == ["it-helper"]


async def test_import_inserts_new_agents_with_their_audit_record(pool, tenant: str):
    """Fails if an import overwrites an existing agent, or inserts one without its audit event."""
    config = parse_agents_config(agents_dict())
    store = PostgresAgentStore(pool)
    first = await store.import_agents(tenant, config.agents)
    again = await store.import_agents(tenant, config.agents)
    assert (first.imported, first.skipped) == (["it-helper"], [])
    assert (again.imported, again.skipped) == ([], ["it-helper"])
    assert (await store.published(tenant, "it-helper")).version == 1
    events = await PostgresAuditStore(pool).list(tenant)
    assert [(e.kind, e.agent_id) for e in events] == [("agent_import", "it-helper")]


async def test_external_writes_lease_busy_takeover_and_first_result_wins(pool, tenant: str):
    """Fails if a live lease can be claimed, a take-over forgets the write was issued, a
    second result replaces the first, or a done write can be claimed again."""
    store = PostgresExternalWriteStore(pool)
    first = await store.claim(tenant, "k", 60)
    assert (first.status, first.issued) == ("claimed", False)
    assert (await store.claim(tenant, "k", 60)).status == "busy"
    assert await store.mark_issued(tenant, "k", first.token, 60) is True
    await store.release(tenant, "k", first.token)
    second = await store.claim(tenant, "k", 60)
    assert (second.status, second.issued) == ("claimed", True)
    assert second.token != first.token
    assert await store.result(tenant, "k") is None
    assert await store.complete(tenant, "k", second.token, "ref-1") == "ref-1"
    assert await store.complete(tenant, "k", second.token, "ref-2") == "ref-1"
    done = await store.claim(tenant, "k", 60)
    assert (done.status, done.result_ref) == ("done", "ref-1")
    assert await store.result(tenant, "k") == "ref-1"


async def test_external_writes_ignore_a_token_whose_lease_was_taken_over(pool, tenant: str):
    """Fails if an attempt that lost its lease can end the new holder's lease, mark a
    write, or store a result."""
    store = PostgresExternalWriteStore(pool)
    stale = await store.claim(tenant, "k", 0)  # a lease that is already over
    holder = await store.claim(tenant, "k", 60)
    assert holder.status == "claimed"

    await store.release(tenant, "k", stale.token)
    assert await store.mark_issued(tenant, "k", stale.token, 60) is False
    assert await store.complete(tenant, "k", stale.token, "stale-ref") is None
    assert (await store.claim(tenant, "k", 60)).status == "busy"  # the holder's lease stands
    assert await store.mark_issued(tenant, "k", holder.token, 60) is True
    assert await store.complete(tenant, "k", holder.token, "ref") == "ref"


async def test_external_writes_are_per_tenant_and_a_done_result_never_changes(
    pool, tenant: str, other_tenant: str
):
    """Fails if another tenant sees or claims the row, or a done row's result is rewritten."""
    store = PostgresExternalWriteStore(pool)
    claim = await store.claim(tenant, "k", 60)
    await store.complete(tenant, "k", claim.token, "ref")

    assert await store.result(other_tenant, "k") is None
    assert (await store.claim(other_tenant, "k", 60)).status == "claimed"
    async with tenant_transaction(pool, tenant) as conn:
        cur = await conn.execute(
            "UPDATE external_writes SET result_ref = 'x' WHERE idem_key = 'k' "
            "AND lease_token = %s AND state <> 'done'",
            (claim.token,),
        )
        assert cur.rowcount == 0
    assert await store.result(tenant, "k") == "ref"


async def test_external_writes_refuse_delete_and_a_row_for_another_tenant(
    pool, tenant: str, other_tenant: str
):
    """Fails if the app role may delete a row or write one under a tenant it is not set to."""
    store = PostgresExternalWriteStore(pool)
    await store.claim(tenant, "k", 60)
    with pytest.raises(errors.InsufficientPrivilege):
        async with tenant_transaction(pool, tenant) as conn:
            await conn.execute("DELETE FROM external_writes")
    with pytest.raises(errors.InsufficientPrivilege):
        async with tenant_transaction(pool, tenant) as conn:
            await conn.execute(
                "INSERT INTO external_writes (tenant_id, idem_key, state, lease_until, lease_token) "
                "VALUES (%s, 'x', 'claimed', now(), 't')",
                (other_tenant,),
            )
