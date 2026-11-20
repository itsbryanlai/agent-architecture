"""The stores that keep what a conversation, a task or an index needs, one contract each,
run against the in-memory twin and the Postgres store."""

from __future__ import annotations

import dataclasses
from datetime import UTC, date, datetime, timedelta

import pytest

from agentos import ids
from agentos.content_store import InMemoryContentStore, PostgresContentStore, StoredStep
from agentos.knowledge.store import InMemoryKnowledgeStore, PageSync, PostgresKnowledgeStore
from agentos.stores import (
    InMemoryExternalWriteStore,
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
    PostgresExternalWriteStore,
    PostgresHistoryStore,
    PostgresIdempotencyStore,
    PostgresTaskLinkStore,
    PostgresToolOutputStore,
)

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)


def twin(store_pool, fake, real):
    return fake() if store_pool is None else real(store_pool)


def unit_vector(position: int) -> list[float]:
    return [1.0 if i == position else 0.0 for i in range(1024)]


from agentos.threads import (
    InMemoryOwnerPingStore,
    InMemoryThreadStore,
    PostgresOwnerPingStore,
    PostgresThreadStore,
)


@pytest.fixture
def external_writes(store_pool):
    return twin(store_pool, InMemoryExternalWriteStore, PostgresExternalWriteStore)


@pytest.fixture
def threads(store_pool):
    return twin(store_pool, InMemoryThreadStore, PostgresThreadStore)


@pytest.fixture
def owner_pings(store_pool):
    return twin(store_pool, InMemoryOwnerPingStore, PostgresOwnerPingStore)


@pytest.fixture
def history(store_pool):
    return twin(store_pool, InMemoryHistoryStore, PostgresHistoryStore)


@pytest.fixture
def idempotency(store_pool):
    return twin(store_pool, InMemoryIdempotencyStore, PostgresIdempotencyStore)


@pytest.fixture
def tool_outputs(store_pool):
    return twin(store_pool, InMemoryToolOutputStore, PostgresToolOutputStore)


@pytest.fixture
def content(store_pool):
    return twin(store_pool, InMemoryContentStore, PostgresContentStore)


@pytest.fixture
def task_links(store_pool):
    return twin(store_pool, InMemoryTaskLinkStore, PostgresTaskLinkStore)


@pytest.fixture
def knowledge(store_pool):
    return twin(store_pool, InMemoryKnowledgeStore, PostgresKnowledgeStore)


async def test_history_appends_once_per_step_and_loads_in_order(history, tenant: str):
    """Fails if a repeated step is stored twice, or messages load out of order."""
    first = await history.append(tenant, "c", "s1", "user", [{"type": "text", "text": "hi"}])
    again = await history.append(tenant, "c", "s1", "user", [{"type": "text", "text": "hi"}])
    second = await history.append(tenant, "c", "s2", "assistant", [{"type": "text", "text": "yo"}])
    assert (first, again, second) == (1, 1, 2)
    assert [m["role"] for m in await history.load(tenant, "c")] == ["user", "assistant"]
    assert await history.load(tenant, "other") == []


async def test_idempotency_record_keeps_the_first_value(idempotency, tenant: str):
    """Fails if a second write replaces the value an earlier attempt stored."""
    assert await idempotency.get(tenant, "k") is None
    assert await idempotency.put_if_absent(tenant, "k", {"page": "one"}) == {"page": "one"}
    assert await idempotency.put_if_absent(tenant, "k", {"page": "two"}) == {"page": "one"}
    assert await idempotency.get(tenant, "k") == {"page": "one"}


async def test_tool_outputs_keep_the_first_text_and_read_back_by_id(tool_outputs, tenant: str):
    """Fails if a repeated put overwrites a tool output, or a read returns an ID it was not asked for."""
    await tool_outputs.put(tenant, "c", "u1", "first", False)
    await tool_outputs.put(tenant, "c", "u1", "second", True)
    await tool_outputs.put(tenant, "c", "u2", "boom", True)
    got = await tool_outputs.get_many(tenant, "c", ["u1", "u2", "missing"])
    assert got == {"u1": ("first", False), "u2": ("boom", True)}
    assert await tool_outputs.get_many(tenant, "c", []) == {}


async def test_content_store_keeps_the_first_inbound_event_it_is_given_twice(content, tenant: str):
    """Fails if a retried inbound event replaces the stored one."""
    assert await content.put_inbound(tenant, "Ev1", {"text": "first"}) is True
    assert await content.put_inbound(tenant, "Ev1", {"text": "second"}) is False
    assert await content.get_inbound(tenant, "Ev1") == {"text": "first"}
    assert await content.get_inbound(tenant, "Ev2") is None


async def test_content_store_keeps_the_first_model_step_it_is_given_twice(content, tenant: str):
    """Fails if a retried model step replaces the stored one."""
    one = StoredStep("end_turn", [{"type": "text", "text": "a"}], "a")
    two = StoredStep("tool_use", [{"type": "tool_use", "id": "u", "name": "n", "input": {}}])
    assert await content.put_step(tenant, "c", "k", one) == one
    assert await content.put_step(tenant, "c", "k", two) == one
    assert await content.get_step(tenant, "c", "other") is None


async def test_content_store_keeps_the_first_tool_input_it_is_given_twice(content, tenant: str):
    """Fails if a retried tool call replaces the stored input."""
    await content.put_tool_input(tenant, "c", "u1", {"query": "first"})
    await content.put_tool_input(tenant, "c", "u1", {"query": "second"})
    assert await content.get_tool_input(tenant, "c", "u1") == (True, {"query": "first"})
    assert await content.get_tool_input(tenant, "c", "missing") == (False, None)


async def test_content_store_keeps_the_first_draft_of_a_revision_and_stores_the_next(
    content, tenant: str
):
    """Fails if a retry replaces a draft revision, or a new revision is not stored beside it."""
    await content.put_draft(tenant, "t", 1, {"title": "first"})
    await content.put_draft(tenant, "t", 1, {"title": "second"})
    await content.put_draft(tenant, "t", 2, {"title": "edited"})
    assert await content.get_draft(tenant, "t", 1) == {"title": "first"}
    assert await content.get_draft(tenant, "t", 2) == {"title": "edited"}
    assert await content.get_draft(tenant, "t", 3) is None


async def test_undelivered_events_come_oldest_first_and_leave_once_delivered(content, tenant: str):
    """Fails if the relay's queue is out of order, keeps a delivered event, or loses a delay."""
    for event in ("Ev1", "Ev2", "Ev3"):
        await content.put_inbound(tenant, event, {"n": event})
    await content.put_inbound(tenant, "edit:1", {"kind": "edit"}, delivered=True)

    await content.mark_delivered(tenant, "Ev1", NOW)
    await content.delay_inbound(tenant, "Ev2", 3, NOW + timedelta(minutes=1))

    rows = await content.undelivered(tenant, 10)
    assert [r.event_id for r in rows] == ["Ev2", "Ev3"]
    assert (rows[0].attempts, rows[0].next_attempt_at) == (3, NOW + timedelta(minutes=1))
    assert rows[1].attempts == 0


async def test_task_link_keeps_the_first_task_for_a_page(task_links, tenant: str):
    """Fails if a second link replaces the task a page was first linked to."""
    assert await task_links.task_for_page(tenant, "p") is None
    await task_links.link(tenant, "p", "task-1")
    await task_links.link(tenant, "p", "task-2")
    assert await task_links.task_for_page(tenant, "p") == "task-1"


async def seeded_fruit(knowledge, tenant: str, other_tenant: str):
    chunks = [("about apples", unit_vector(0)), ("about pears", unit_vector(1))]
    await knowledge.replace_page(tenant, "s1", "p1", "Fruit", "https://n/p1", "h1", chunks)
    await knowledge.replace_page(tenant, "s2", "p2", "Hidden", "https://n/p2", "h2", chunks[:1])
    await knowledge.replace_page(
        other_tenant, "s1", "p3", "Theirs", "https://n/p3", "h3", chunks[:1]
    )
    return chunks


@pytest.mark.parametrize(
    "scopes",
    [("s1", "s2"), (ids.drive_scope("folder-1"), ids.drive_scope("folder-2"))],
    ids=["notion", "drive"],
)
async def test_knowledge_search_is_scoped_to_the_tenant_and_ordered_by_distance(
    knowledge, tenant: str, other_tenant: str, scopes
):
    """Fails if search ignores scope or tenant (a Drive scope included), returns a scope
    the agent was not granted, or the nearest chunk is not first."""
    granted, other = scopes
    chunks = [("about apples", unit_vector(0)), ("about pears", unit_vector(1))]
    await knowledge.replace_page(tenant, granted, "p1", "Fruit", "https://n/p1", "h1", chunks)
    await knowledge.replace_page(tenant, other, "p2", "Hidden", "https://n/p2", "h2", chunks[:1])
    await knowledge.replace_page(
        other_tenant, granted, "p3", "Theirs", "https://n/p3", "h3", chunks[:1]
    )

    hits = await knowledge.search(tenant, [granted], unit_vector(1), limit=5)
    assert [(h.page_id, h.text) for h in hits] == [("p1", "about pears"), ("p1", "about apples")]
    assert hits[0].distance == pytest.approx(0.0, abs=1e-6)
    assert (hits[0].title, hits[0].url) == ("Fruit", "https://n/p1")
    assert await knowledge.search(tenant, [], unit_vector(1), limit=5) == []
    assert len(await knowledge.search(tenant, [granted, other], unit_vector(0), limit=5)) == 3
    assert [
        h.page_id for h in await knowledge.search(other_tenant, [granted], unit_vector(0), limit=5)
    ] == ["p3"]
    assert await knowledge.search(tenant, ["no-such-scope"], unit_vector(0), limit=5) == []


async def test_reindexing_a_page_replaces_its_chunks_and_details(
    knowledge, tenant: str, other_tenant: str
):
    """Fails if a re-index leaves old chunks, the old hash, or the old title behind."""
    chunks = await seeded_fruit(knowledge, tenant, other_tenant)

    await knowledge.replace_page(tenant, "s1", "p1", "Fruit v2", "https://n/p1", "h1b", chunks[:1])

    assert await knowledge.page_hashes(tenant, "s1") == {"p1": "h1b"}
    assert [h.text for h in await knowledge.search(tenant, ["s1"], unit_vector(1), limit=5)] == [
        "about apples"
    ]
    assert await knowledge.page_meta(tenant, ["s1"], ["p1", "p2"]) == {
        "p1": ("Fruit v2", "https://n/p1")
    }


async def test_deleting_pages_and_scopes_leaves_the_other_tenants_alone(
    knowledge, tenant: str, other_tenant: str
):
    """Fails if a delete misses its rows or reaches another tenant's scope."""
    await seeded_fruit(knowledge, tenant, other_tenant)

    assert sorted(await knowledge.scopes(tenant)) == ["s1", "s2"]
    assert await knowledge.delete_pages(tenant, "s1", ["p1", "nope"]) == 1
    assert await knowledge.delete_scope(tenant, "s2") == 1
    assert await knowledge.scopes(tenant) == []
    assert await knowledge.scopes(other_tenant) == ["s1"]


async def test_knowledge_page_state_round_trips_and_touch_updates_it(knowledge, tenant: str):
    """Fails if the sync columns do not survive a store and read, an upsert keeps stale state, or a touch changes chunks."""
    sync = PageSync(
        "m1", "2026-01-01T00:00:00.000Z", "par", ["root", "mid"], ["c1"], ["d1"], ["r1"]
    )
    assert await knowledge.page_state(tenant, "s", "p") is None

    await knowledge.replace_page(tenant, "s", "p", "T", "u", "h1", [("a", unit_vector(0))], sync)
    state = await knowledge.page_state(tenant, "s", "p")
    assert (state.content_hash, state.sync) == ("h1", sync)
    assert state.indexed_at.tzinfo is not None

    newer = dataclasses.replace(sync, embed_model="m2", ancestors=["root"], row_page_ids=[])
    await knowledge.replace_page(tenant, "s", "p", "T", "u", "h2", [("a", unit_vector(0))], newer)
    assert (await knowledge.page_state(tenant, "s", "p")).sync == newer

    latest = dataclasses.replace(newer, last_edited_time="2026-02-02T00:00:00.000Z")
    await knowledge.touch_page(tenant, "s", "p", "h3", latest)
    await knowledge.touch_page(tenant, "s", "missing", "h", latest)
    state = await knowledge.page_state(tenant, "s", "p")
    assert (state.content_hash, state.sync) == ("h3", latest)
    assert await knowledge.page_state(tenant, "s", "missing") is None
    assert len(await knowledge.search(tenant, ["s"], unit_vector(0), limit=5)) == 1

    await knowledge.replace_page(tenant, "s", "legacy", "T", "u", "h", [])
    assert await knowledge.count_pages_without_model(tenant) == 1


async def test_knowledge_insert_stores_every_chunk_of_a_page_in_order(knowledge, tenant: str):
    """Fails if the one multi-row insert drops, reorders or duplicates chunks, or fails for a page without text."""
    chunks = [(f"chunk {n}", unit_vector(n)) for n in range(7)]
    await knowledge.replace_page(tenant, "s", "p", "T", "u", "h", chunks)
    await knowledge.replace_page(tenant, "s", "empty", "T", "u", "h", [])

    hits = await knowledge.search(tenant, ["s"], unit_vector(0), limit=20)

    assert sorted((h.chunk_no, h.text) for h in hits) == [(n, f"chunk {n}") for n in range(7)]
    assert await knowledge.page_state(tenant, "s", "empty") is not None


async def test_knowledge_search_filters_by_model_and_reports_distance_and_position(
    knowledge, tenant: str
):
    """Fails if a search returns chunks of another embedding model, loses a hit's stored position, or reports a wrong distance."""
    old = PageSync(embed_model="old", parent_ref="x", ancestors=["root"])
    new = PageSync(embed_model="new", parent_ref="y", ancestors=["root", "mid"])
    await knowledge.replace_page(
        tenant, "s", "old-page", "Old", "u", "h", [("o", unit_vector(0))], old
    )
    await knowledge.replace_page(
        tenant, "s", "new-page", "New", "u", "h", [("n", unit_vector(1))], new
    )

    only_new = await knowledge.search(tenant, ["s"], unit_vector(0), limit=5, embed_model="new")
    everything = await knowledge.search(tenant, ["s"], unit_vector(0), limit=5)

    assert [(h.page_id, h.parent_ref, h.ancestors) for h in only_new] == [
        ("new-page", "y", ["root", "mid"])
    ]
    assert only_new[0].distance == pytest.approx(1.0, abs=1e-6)
    assert [h.page_id for h in everything] == ["old-page", "new-page"]
    assert everything[0].distance == pytest.approx(0.0, abs=1e-6)
    assert await knowledge.search(tenant, ["s"], unit_vector(0), limit=5, embed_model="none") == []


async def test_knowledge_delete_keeps_pages_stored_since_the_cutoff(knowledge, tenant: str):
    """Fails if a delete with indexed_before removes a page stored after that time, or keeps one stored before it."""
    for page in ("a", "b"):
        await knowledge.replace_page(tenant, "s", page, "T", "u", "h", [])
    past = datetime.now(UTC) - timedelta(hours=1)
    future = datetime.now(UTC) + timedelta(hours=1)

    assert await knowledge.delete_pages(tenant, "s", ["a", "b"], indexed_before=past) == 0
    assert await knowledge.delete_pages(tenant, "s", ["a"], indexed_before=future) == 1
    assert await knowledge.delete_pages(tenant, "s", ["b"]) == 1


async def test_external_writes_lease_busy_takeover_and_first_result_wins(
    external_writes, tenant: str
):
    """Fails if a live lease can be claimed, a take-over forgets the write was issued, a
    second result replaces the first, or a done write can be claimed again."""
    first = await external_writes.claim(tenant, "k", 60)
    assert (first.status, first.issued) == ("claimed", False)
    assert (await external_writes.claim(tenant, "k", 60)).status == "busy"
    assert await external_writes.mark_issued(tenant, "k", first.token, 60) is True
    await external_writes.release(tenant, "k", first.token)
    second = await external_writes.claim(tenant, "k", 60)
    assert (second.status, second.issued) == ("claimed", True)
    assert second.token != first.token
    assert await external_writes.result(tenant, "k") is None
    assert await external_writes.complete(tenant, "k", second.token, "ref-1") == "ref-1"
    assert await external_writes.complete(tenant, "k", second.token, "ref-2") == "ref-1"
    done = await external_writes.claim(tenant, "k", 60)
    assert (done.status, done.result_ref) == ("done", "ref-1")
    assert await external_writes.result(tenant, "k") == "ref-1"


async def test_external_writes_ignore_a_token_whose_lease_was_taken_over(
    external_writes, tenant: str
):
    """Fails if an attempt that lost its lease can end the new holder's lease, mark a
    write, or store a result."""
    stale = await external_writes.claim(tenant, "k", 0)  # a lease that is already over
    holder = await external_writes.claim(tenant, "k", 60)
    assert holder.status == "claimed"

    await external_writes.release(tenant, "k", stale.token)
    assert await external_writes.mark_issued(tenant, "k", stale.token, 60) is False
    assert await external_writes.complete(tenant, "k", stale.token, "stale-ref") is None
    assert (
        await external_writes.claim(tenant, "k", 60)
    ).status == "busy"  # the holder's lease stands
    assert await external_writes.mark_issued(tenant, "k", holder.token, 60) is True
    assert await external_writes.complete(tenant, "k", holder.token, "ref") == "ref"


async def test_a_thread_keeps_the_agent_it_was_last_handed_to(
    threads, tenant: str, other_tenant: str
):
    """Fails if a thread forgets its specialist, or one tenant reads another's thread."""
    assert await threads.agent_for(tenant, f"{tenant}:thread:C1:1.0") is None
    await threads.set_agent(tenant, f"{tenant}:thread:C1:1.0", "it-helper", NOW)
    await threads.set_agent(tenant, f"{tenant}:thread:C1:1.0", "hr-helper", NOW + timedelta(days=1))
    assert await threads.agent_for(tenant, f"{tenant}:thread:C1:1.0") == "hr-helper"
    assert await threads.agent_for(other_tenant, f"{tenant}:thread:C1:1.0") is None


async def test_the_owner_is_mentioned_once_a_day_per_agent_place_and_asker(
    owner_pings, tenant: str, other_tenant: str
):
    """Fails if a second refusal on the same UTC day mentions the owner again, or a new day, asker,
    place or tenant is held back by another's mention."""
    day = date(2026, 11, 16)
    assert await owner_pings.claim(tenant, "aa", "C1", "U1", day) is True
    assert await owner_pings.claim(tenant, "aa", "C1", "U1", day) is False
    assert await owner_pings.claim(tenant, "aa", "C1", "U1", day - timedelta(days=1)) is False
    assert await owner_pings.claim(tenant, "aa", "C1", "U2", day) is True
    assert await owner_pings.claim(tenant, "aa", "C2", "U1", day) is True
    assert await owner_pings.claim(tenant, "bb", "C1", "U1", day) is True
    assert await owner_pings.claim(other_tenant, "aa", "C1", "U1", day) is True
    assert await owner_pings.claim(tenant, "aa", "C1", "U1", day + timedelta(days=1)) is True


async def test_consuming_an_inbound_event_leaves_no_content_and_keeps_the_row(content, tenant: str):
    """Fails if an inbound row still holds text or form values once consumed, or loses its IDs."""
    await content.put_inbound(tenant, "Ev1", {"channel": "C", "text": "secret words"})
    await content.put_inbound(
        tenant, "edit:1", {"task_id": "t", "fields": {"title": "secret title"}}, delivered=True
    )
    for event, stub in (("Ev1", {"channel": "C"}), ("edit:1", {"task_id": "t"})):
        await content.consume_inbound(tenant, event, {**stub, "consumed": True}, NOW)

    assert await content.get_inbound(tenant, "Ev1") == {"channel": "C", "consumed": True}
    assert await content.get_inbound(tenant, "edit:1") == {"task_id": "t", "consumed": True}
    assert await content.put_inbound(tenant, "Ev1", {"text": "again"}) is False
