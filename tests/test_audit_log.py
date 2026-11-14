"""The audit log: one event per action, with IDs and hashes and no content."""

import asyncio
import dataclasses
import uuid
from pathlib import Path

import pytest

from agentos import ids
from agentos.audit import (
    InMemoryAuditStore,
    PostgresAuditStore,
    agent_identity,
    canonical_sha256,
    event_json,
)
from agentos.config import TASK_QUEUE
from agentos.connectors.notion import FakeNotFound
from agentos.models import (
    ApprovalDecision,
    AuditEvent,
    ConversationRef,
    InboundMessage,
    LLMResponse,
    TaskSpec,
    ToolCall,
    TurnContext,
)
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, DB_ID, TENANT, eventually, title_prop
from tests.test_front_door import front_door_dict, running
from tests.test_row_level_security import FakePool

ROOT = Path(__file__).resolve().parent.parent
MIGRATION = (ROOT / "scripts" / "migrations" / "005_audit_log.sql").read_text()
INIT_DB = (ROOT / "scripts" / "init_db.sql").read_text()

CHANNEL = "C_GENERAL"
THREAD = "1700000000.000100"
CONV_ID = ids.conversation_id(TENANT, CHANNEL, THREAD)
NONCE = "nonce-1"

# Strings the scripted conversation carries. None may appear in an event.
USER_TEXT = "ZZ-USER-TEXT where is the vpn doc"
QUERY = "ZZ-QUERY-vpn"
ANSWER = "ZZ-ANSWER-the vpn page is on the wiki"
TITLE = "ZZ-TITLE-new monitor"
DETAILS = "ZZ-DETAILS-mine flickers"
COMMENT = "ZZ-COMMENT-any update"
ITEM_TITLE = "ZZ-ITEM-vpn broken"
SECRETS = [USER_TEXT, QUERY, ANSWER, TITLE, DETAILS, COMMENT, ITEM_TITLE]


def tool_turn(*calls: ToolCall) -> LLMResponse:
    return LLMResponse(text=None, tool_calls=list(calls), stop_reason="tool_use")


def answer(text: str) -> LLMResponse:
    return LLMResponse(text=text, tool_calls=[], stop_reason="end_turn")


def inbound(event_id: str, text: str = USER_TEXT) -> InboundMessage:
    return InboundMessage(event_id, ALICE, text, CHANNEL, THREAD, "channel")


async def converse(h, agent_id: str, msg: InboundMessage):
    return await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, agent_id, CHANNEL, THREAD),
        id=CONV_ID,
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )


def ctx(agent_id: str = "it-helper") -> TurnContext:
    return TurnContext(TENANT, agent_id, 1, ALICE, CHANNEL, THREAD, "channel")


async def events(h, kind: str | None = None) -> list[AuditEvent]:
    found = await h.audit.list(TENANT, limit=100)
    return [e for e in found if kind is None or e.kind == kind]


def assert_actor(e: AuditEvent, agent_id: str = "it-helper") -> None:
    assert e.tenant_id == TENANT
    assert e.agent_id == agent_id
    assert e.agent_version == 1
    assert e.requester_user_id == ALICE
    assert e.identity == f"agent:{agent_id}"
    assert e.at is not None


# --- one event per action ------------------------------------------------------


async def test_read_tool_call_and_answer_each_record_one_event(harness):
    h = harness
    h.notion.add_page(DB_ID, {"Name": title_prop(ITEM_TITLE)})
    call = ToolCall("tu_1", "search_items", {"query": QUERY})
    h.llm.queue(tool_turn(call), answer(ANSWER))

    await converse(h, "it-helper", inbound("Ev1"))
    await eventually(lambda: h.slack.updates())

    [tool_event] = await events(h, "tool_call")
    assert_actor(tool_event)
    assert tool_event.tool == "search_items"
    assert tool_event.conversation_id == CONV_ID
    assert tool_event.outcome == "ok"
    assert tool_event.input_sha256 == canonical_sha256({"query": QUERY})
    assert tool_event.output_sha256 == canonical_sha256(
        (await h.tool_outputs.get_many(TENANT, CONV_ID, ["tu_1"]))["tu_1"][0]
    )
    assert tool_event.event_key == ids.audit_tool_call_key(CONV_ID, "tu_1")

    [answer_event] = await events(h, "answer")
    assert_actor(answer_event)
    assert answer_event.conversation_id == CONV_ID
    assert answer_event.output_sha256 == canonical_sha256(ANSWER)
    placeholder_ts = h.slack.updates()[0]["ts"]
    assert answer_event.event_key == ids.audit_answer_key(CONV_ID, placeholder_ts)
    assert len(await events(h)) == 2


async def test_failure_notice_is_not_an_answer(harness):
    h = harness
    h.llm.queue(RuntimeError("model down"))

    await converse(h, "it-helper", inbound("Ev1"))
    await eventually(lambda: h.slack.updates(), timeout=60)

    assert await events(h, "answer") == []


async def test_step_limit_notice_is_not_an_answer(harness):
    h = harness
    calls = [tool_turn(ToolCall(f"tu_{i}", "search_items", {"query": "x"})) for i in range(6)]
    h.llm.queue(*calls)

    await converse(h, "it-helper", inbound("Ev1"))
    await eventually(lambda: h.slack.updates(), timeout=30)

    assert await events(h, "answer") == []
    assert len(await events(h, "tool_call")) >= 1


async def test_write_after_approval_records_approval_then_write(harness):
    h = harness
    spec = TaskSpec(
        ctx(),
        "create_item",
        {"title": TITLE, "details": DETAILS, "database_id": DB_ID},
        NONCE,
    )
    task_id = ids.task_id(TENANT, str(uuid.uuid4()))
    handle = await h.client.start_workflow(
        TaskWorkflow.run, spec, id=task_id, task_queue=TASK_QUEUE
    )
    await eventually(lambda: h.slack.posts())
    assert await events(h) == []  # nothing is recorded before approval

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))
    await eventually(lambda: events(h, "write"))

    [approval] = await events(h, "approval")
    [write] = await events(h, "write")
    for e in (approval, write):
        assert_actor(e)
        assert e.task_id == task_id
        assert e.tool == "create_item"
        assert e.approver_user_id == ALICE
        assert e.input_sha256 == canonical_sha256(spec.payload)
    assert approval.event_key == ids.audit_approval_key(task_id, NONCE)
    assert approval.target == DB_ID
    assert write.event_key == ids.audit_write_key(f"{task_id}:create", "ok")
    page_id = h.notion.all_pages(DB_ID)[0]["id"]
    assert write.target == page_id
    assert write.output_sha256 == canonical_sha256({"page_id": page_id})
    assert write.outcome == "ok"
    assert approval.at <= write.at


async def test_comment_write_records_approval_and_write(harness):
    h = harness
    page = h.notion.add_page(DB_ID, {"Name": title_prop(ITEM_TITLE)})
    spec = TaskSpec(ctx(), "add_comment", {"item": page["url"], "comment": COMMENT}, NONCE)
    task_id = ids.task_id(TENANT, str(uuid.uuid4()))
    handle = await h.client.start_workflow(
        TaskWorkflow.run, spec, id=task_id, task_queue=TASK_QUEUE
    )
    await eventually(lambda: h.slack.posts())

    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))
    assert await handle.result() == "done"

    [approval] = await events(h, "approval")
    [write] = await events(h, "write")
    assert approval.tool == write.tool == "add_comment"
    assert approval.target == write.target == page["id"]
    assert write.approver_user_id == ALICE
    assert write.outcome == "ok"
    assert write.event_key == ids.audit_write_key(f"{task_id}:comment", "ok")


async def test_cancelled_task_records_nothing(harness):
    h = harness
    spec = TaskSpec(
        ctx(), "create_item", {"title": TITLE, "details": DETAILS, "database_id": DB_ID}, NONCE
    )
    handle = await h.client.start_workflow(
        TaskWorkflow.run, spec, id=ids.task_id(TENANT, "t1"), task_queue=TASK_QUEUE
    )
    await eventually(lambda: h.slack.posts())
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, False))
    assert await handle.result() == "cancelled"
    assert await events(h) == []


async def test_failed_write_records_error_outcome(harness):
    h = harness
    spec = TaskSpec(
        ctx(), "create_item", {"title": TITLE, "details": DETAILS, "database_id": DB_ID}, NONCE
    )

    # Notion refuses the create for good (a 404), so the activity fails without retries.
    async def refuse(*args, **kwargs):
        raise FakeNotFound("gone")

    h.notion.create_page = refuse
    task_id = ids.task_id(TENANT, "t-denied")
    handle = await h.client.start_workflow(
        TaskWorkflow.run, spec, id=task_id, task_queue=TASK_QUEUE
    )
    await eventually(lambda: h.slack.posts())
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))
    await eventually(lambda: events(h, "write"))

    [write] = await events(h, "write")
    assert write.outcome == "error"
    assert write.output_sha256 is None
    assert write.event_key == ids.audit_write_key(f"{task_id}:create", "error")
    assert len(await events(h, "approval")) == 1


async def test_hand_off_records_one_event_then_the_specialists_answer():
    async with running(front_door_dict()) as h:
        h.llm.queue(tool_turn(ToolCall("tu_h", "hand_off", {"agent": "it-helper"})), answer(ANSWER))

        await converse(h, "concierge", inbound("Ev1"))
        await eventually(lambda: len(h.slack.updates()) >= 2)

        [hand_off] = await events(h, "hand_off")
        assert_actor(hand_off, "concierge")
        assert hand_off.target == "it-helper"
        assert hand_off.conversation_id == CONV_ID
        assert hand_off.event_key == ids.audit_hand_off_key(TENANT, "Ev1")
        # The announcement is not an answer; the specialist's reply is, under its own ID.
        [answer_event] = await events(h, "answer")
        assert_actor(answer_event, "it-helper")
        assert answer_event.output_sha256 == canonical_sha256(ANSWER)
        assert len(await events(h)) == 2


# --- retries -------------------------------------------------------------------


async def test_activity_retry_records_once(harness):
    h = harness
    h.llm.queue(tool_turn(ToolCall("tu_1", "search_items", {"query": QUERY})), answer(ANSWER))
    h.audit.fail_next_records(2)  # the tool-call record fails, then the retry succeeds

    await converse(h, "it-helper", inbound("Ev1"))
    await eventually(lambda: h.slack.updates(), timeout=30)

    assert len(await events(h, "tool_call")) == 1
    assert len(await events(h, "answer")) == 1


async def test_an_audit_outage_fails_the_activity_instead_of_skipping_the_event(harness):
    h = harness
    h.llm.queue(answer(ANSWER))
    h.audit.fail_next_records(1)

    await converse(h, "it-helper", inbound("Ev1"))
    # The first answer record fails after the post; the retry posts again and records.
    await eventually(lambda: events(h, "answer"), timeout=30)

    assert len(await events(h, "answer")) == 1


async def test_same_event_key_stores_once():
    store = InMemoryAuditStore()
    event = AuditEvent(TENANT, "k1", "answer", "it-helper", 1, ALICE, agent_identity("it-helper"))

    await store.record(event)
    await store.record(dataclasses.replace(event, outcome="error"))

    [stored] = await store.list(TENANT)
    assert stored.outcome == "ok"


async def test_same_event_key_in_two_tenants_is_two_events():
    store = InMemoryAuditStore()
    a = AuditEvent("acme", "k1", "answer", "x", 1, ALICE, "agent:x")
    await store.record(a)
    await store.record(dataclasses.replace(a, tenant_id="globex"))
    assert len(await store.list("acme")) == len(await store.list("globex")) == 1


# --- no content ------------------------------------------------------------------


async def test_no_event_holds_message_or_tool_text(harness):
    h = harness
    h.notion.add_page(DB_ID, {"Name": title_prop(ITEM_TITLE)})
    h.llm.queue(tool_turn(ToolCall("tu_1", "search_items", {"query": QUERY})), answer(ANSWER))
    await converse(h, "it-helper", inbound("Ev1"))
    await eventually(lambda: h.slack.updates())

    spec = TaskSpec(
        ctx(), "create_item", {"title": TITLE, "details": DETAILS, "database_id": DB_ID}, NONCE
    )
    handle = await h.client.start_workflow(
        TaskWorkflow.run, spec, id=ids.task_id(TENANT, "t1"), task_queue=TASK_QUEUE
    )
    await eventually(lambda: [p for p in h.slack.posts() if p["blocks"]])
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))
    await eventually(lambda: events(h, "write"))

    found = await events(h)
    assert {e.kind for e in found} == {"answer", "tool_call", "approval", "write"}
    dump = "\n".join(event_json(e) for e in found)
    for secret in SECRETS:
        assert secret not in dump
    hashes = [e.input_sha256 for e in found] + [e.output_sha256 for e in found]
    assert all(x is None or len(x) == 64 for x in hashes)


def test_event_rejects_a_hash_that_is_not_a_hash():
    with pytest.raises(ValueError):
        AuditEvent(TENANT, "k", "answer", "a", 1, ALICE, "agent:a", output_sha256="some text")
    with pytest.raises(ValueError):
        AuditEvent(TENANT, "k", "chat", "a", 1, ALICE, "agent:a")
    with pytest.raises(ValueError):
        AuditEvent(TENANT, "k", "answer", "a", 1, ALICE, "agent:a", outcome="maybe")


def test_canonical_hash_ignores_key_order_and_spacing():
    assert canonical_sha256({"a": 1, "b": [1, 2]}) == canonical_sha256({"b": [1, 2], "a": 1})
    assert canonical_sha256({"a": 1}) != canonical_sha256({"a": 2})


# --- listing -----------------------------------------------------------------------


async def test_list_is_newest_first_per_tenant_with_limit_and_before():
    store = InMemoryAuditStore()
    for i in range(5):
        await store.record(AuditEvent("acme", f"k{i}", "answer", "x", 1, ALICE, "agent:x"))
        await asyncio.sleep(0.002)
    await store.record(AuditEvent("globex", "g0", "answer", "x", 1, ALICE, "agent:x"))

    listed = await store.list("acme", limit=3)
    assert [e.event_key for e in listed] == ["k4", "k3", "k2"]
    assert all(e.tenant_id == "acme" for e in await store.list("acme"))
    older = await store.list("acme", before=listed[-1].at)
    assert [e.event_key for e in older] == ["k1", "k0"]
    assert await store.list("initech") == []
    with pytest.raises(ValueError):
        await store.list("acme", limit=0)


# --- the SQL ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql", [MIGRATION, INIT_DB.split("CREATE TABLE IF NOT EXISTS audit_events")[1]]
)
def test_audit_sql_is_tenant_scoped_and_append_only(sql):
    assert "ENABLE ROW LEVEL SECURITY" in sql
    assert "FORCE ROW LEVEL SECURITY" in sql
    assert "CREATE POLICY tenant_isolation ON audit_events" in sql
    assert "current_setting('app.tenant_id', true)" in sql
    assert "GRANT INSERT, SELECT ON audit_events TO agentos_app" in sql
    assert "UPDATE" not in sql.split("GRANT INSERT, SELECT ON audit_events")[1].split(";")[0]
    assert "BEFORE UPDATE OR DELETE ON audit_events" in sql
    assert "BEFORE TRUNCATE ON audit_events" in sql
    assert "PRIMARY KEY (tenant_id, event_key)" in sql
    for kind in ("answer", "tool_call", "hand_off", "approval", "write"):
        assert f"'{kind}'" in sql


def test_audit_table_is_in_a_fresh_database_too():
    assert "CREATE TABLE IF NOT EXISTS audit_events" in INIT_DB
    assert "GRANT INSERT, SELECT ON audit_events TO agentos_app" in INIT_DB
    assert INIT_DB.count("audit_events") > 5


async def test_postgres_store_sets_the_tenant_before_each_call():
    pool = FakePool()
    store = PostgresAuditStore(pool)
    event = AuditEvent(TENANT, "k1", "answer", "x", 1, ALICE, "agent:x")

    await store.record(event)
    await store.list(TENANT, 10)

    assert pool.taken == 2
    transactions = [[e for e in pool.events if e[0] == "execute"][i] for i in (0, 2)]
    for first in transactions:
        assert first[1] == "SELECT set_config('app.tenant_id', %s, true)"
        assert first[2] == (TENANT,)
    insert = pool.events[2][1]
    assert "ON CONFLICT (tenant_id, event_key) DO NOTHING" in insert
    assert "UPDATE" not in insert and "DELETE" not in insert
