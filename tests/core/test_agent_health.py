"""Agent health: a broken grant disables one agent, never the worker.

Fakes only: scripted model, in-memory stores, fake Slack and Notion, injected
clocks, and Temporal's time-skipping test server for the turn and Submit paths.
"""

import copy
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from temporalio.worker import Worker

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_health import (
    AVAILABLE,
    UNAVAILABLE,
    AgentHealth,
    AgentHealthChecker,
    InMemoryAgentHealthStore,
    health_due,
    refresh_health_once,
)
from agentos.agent_store import InMemoryAgentStore, seeded_agent_store
from agentos.audit import InMemoryAuditStore
from agentos.config import TASK_QUEUE, Settings, parse_agents_config
from agentos.connectors.notion import ConnectorError
from agentos.llm import ScriptedLLMClient
from agentos.members import InMemoryMemberStore
from agentos.models import ConversationRef, InboundMessage, LLMResponse, Usage
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import (
    ALICE,
    DB_ID,
    OTHER_DB_ID,
    TENANT,
    agents_dict,
    default_schema,
    eventually,
    make_fake_notion,
)
from tests.helpers.agents import helper_block, make_published, republish
from tests.helpers.health import FlipsAfterReads
from tests.helpers.tasks import approve, start_task, thread_posts, wait_state

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
# Literal on purpose: the required text, not the production function's output.
FIXED_REPLY = (
    "IT helper is unavailable right now: an admin needs to fix its access. Nothing was changed."
)


def break_schema(notion, database_id=DB_ID) -> None:
    """The Railway failure: the granted database's mapping no longer matches."""
    schema = default_schema()
    schema["Agent request ID"] = {"id": "reqid1", "type": "people"}
    notion.set_schema(database_id, schema)


class Down:
    """Notion does not answer: a transient failure, not a grant problem."""

    async def retrieve_schema(self, database_id):
        raise httpx.ConnectError("refused")


class SchemaRaises:
    """retrieve_schema raises for one database; every other call goes to the real fake."""

    def __init__(self, inner, database_id, error):
        self._inner = inner
        self._database_id = database_id
        self._error = error

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def retrieve_schema(self, database_id):
        if database_id == self._database_id:
            raise self._error
        return await self._inner.retrieve_schema(database_id)


class Clock:
    def __init__(self, start: datetime):
        self.now = start

    def __call__(self) -> datetime:
        return self.now


class World:
    """One tenant's checker over fakes, with the stores a test reads back."""

    def __init__(self, notion, cfg=None, store=None, clock=None):
        self.cfg = cfg if cfg is not None else parse_agents_config(agents_dict())
        self.notion = notion
        self.slack = FakeSlack()
        self.health = InMemoryAgentHealthStore()
        self.audit = InMemoryAuditStore()
        self.idem = InMemoryIdempotencyStore()
        self.members = InMemoryMemberStore()
        self.agents = store if store is not None else seeded_agent_store(self.cfg)
        self.registry = TenantRegistry({TENANT: runtime_for(self.cfg, self.slack, self.notion)})
        self.checker = AgentHealthChecker(
            self.registry,
            self.agents,
            self.health,
            self.audit,
            self.idem,
            self.members,
            clock=clock if clock is not None else Clock(NOW),
        )

    async def check(self) -> None:
        await self.checker.check_tenant(TENANT, await self.agents.published_all(TENANT))

    def dms(self) -> dict[str, list[str]]:
        by_channel: dict[str, list[str]] = {}
        for name, kw in self.slack.calls:
            if name == "post_message" and kw["channel"].startswith("D-"):
                by_channel.setdefault(kw["channel"], []).append(kw["text"])
        return by_channel


async def published_store() -> InMemoryAgentStore:
    store = InMemoryAgentStore()
    await make_published(store, "it-helper", helper_block())
    return store


# --- start-up: per-agent health, never a process exit -----------------------------------------


@pytest.mark.parametrize(
    ("case", "status", "reason_part"),
    [
        ("clean", AVAILABLE, None),
        ("wrong_type", UNAVAILABLE, "IT requests"),
        ("missing_ids", UNAVAILABLE, "no property ID configured"),
        ("no_token", UNAVAILABLE, "no Notion token"),
        ("transient", None, None),
        ("connector_error", UNAVAILABLE, "no data source"),
        ("unexpected_error", None, None),
    ],
)
async def test_startup_records_health_per_agent_and_never_stops(case, status, reason_part):
    """Fails if a grant problem or a check error raises out of the start-up check, skips the
    other agent, or health is not what the grants say."""
    notion = make_fake_notion()
    cfg = parse_agents_config(agents_dict())
    if case == "wrong_type":
        break_schema(notion)
    elif case == "missing_ids":
        raw = agents_dict()
        del raw["agents"]["it-helper"]["grants"]["notion"]["databases"][0]["property_ids"]
        cfg = parse_agents_config(raw)
    elif case == "transient":
        notion = Down()
    elif case in ("connector_error", "unexpected_error"):
        # A second agent beside the failing one: its check must still run.
        cfg = two_agents_config()
        notion.set_schema(OTHER_DB_ID, default_schema())
        error = (
            ConnectorError("The granted database has no data source.")
            if case == "connector_error"
            else RuntimeError("a bug in the check")
        )
        notion = SchemaRaises(notion, DB_ID, error)
    w = World(notion=None if case == "no_token" else notion, cfg=cfg)

    await w.check()

    row = await w.health.get(TENANT, "it-helper")
    if status is None:
        assert row is None
        assert w.audit.events == []
    else:
        assert row.status == status
        assert (row.reason is None) == (reason_part is None)
        if reason_part is not None:
            assert reason_part in row.reason
    if case in ("connector_error", "unexpected_error"):
        assert (await w.health.get(TENANT, "good-helper")).status == AVAILABLE


def two_agents_config():
    """it-helper and good-helper, each granted its own database, both answering in C_GENERAL."""
    raw = agents_dict()
    raw["agents"]["it-helper"]["audience"] = {"channels": ["C_GENERAL"], "dm": "everyone"}
    good = copy.deepcopy(raw["agents"]["it-helper"])
    good["name"] = "Good helper"
    good["grants"]["notion"]["databases"][0]["id"] = OTHER_DB_ID
    good["grants"]["notion"]["databases"][0]["name"] = "Good requests"
    raw["agents"]["good-helper"] = good
    return parse_agents_config(raw)


def answer(text: str) -> LLMResponse:
    return LLMResponse(
        text=text, tool_calls=[], stop_reason="end_turn", usage=Usage(10, 5, 0, 0, "m")
    )


async def send(scope, agent: str, event_id: str, ts: str) -> None:
    await scope.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, agent, "C_GENERAL", ts),
        id=ids.conversation_id(TENANT, "C_GENERAL", ts),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[InboundMessage(event_id, ALICE, "hello", "C_GENERAL", ts, "channel")],
    )


async def test_a_broken_agent_beside_a_good_one_keeps_the_worker_serving(temporal_scope):
    """Fails if one broken agent stops the good agent's answer, or the broken one gets more than the fixed reply."""
    cfg = two_agents_config()
    notion = make_fake_notion()
    notion.set_schema(OTHER_DB_ID, default_schema())
    break_schema(notion)
    slack = FakeSlack(emails={ALICE: "alice@example.com"})
    llm = ScriptedLLMClient([answer("all good"), answer("sentinel")])
    health = InMemoryAgentHealthStore()
    audit = InMemoryAuditStore()
    idem = InMemoryIdempotencyStore()
    registry = TenantRegistry({TENANT: runtime_for(cfg, slack, notion)})
    agents = seeded_agent_store(cfg)
    checker = AgentHealthChecker(
        registry, agents, health, audit, idem, InMemoryMemberStore(), clock=Clock(NOW)
    )
    acts = AgentActivities(
        settings=Settings(),
        registry=registry,
        history=InMemoryHistoryStore(),
        idem=idem,
        links=InMemoryTaskLinkStore(),
        llm=llm,
        tool_outputs=InMemoryToolOutputStore(),
        audit=audit,
        agents=agents,
        health=health,
    )
    worker = Worker(
        temporal_scope.client,
        task_queue=temporal_scope.task_queue,
        workflows=[ConversationWorkflow, TaskWorkflow],
        activities=acts.all(),
        interceptors=[temporal_scope.children],
    )
    async with worker:
        await checker.check_tenant(TENANT, await agents.published_all(TENANT))
        assert (await health.get(TENANT, "it-helper")).status == UNAVAILABLE
        assert (await health.get(TENANT, "good-helper")).status == AVAILABLE
        calls_before = len(notion.calls)

        await send(temporal_scope, "good-helper", "Ev1", "1700000000.000100")
        await eventually(lambda: any("all good" in u["text"] for u in slack.updates()))
        await send(temporal_scope, "it-helper", "Ev2", "1700000000.000200")
        await eventually(lambda: any("unavailable right now" in p["text"] for p in slack.posts()))
        # Settle on the good agent before counting the broken one's posts.
        await send(temporal_scope, "good-helper", "Ev3", "1700000000.000300")
        await eventually(lambda: any("sentinel" in u["text"] for u in slack.updates()))

        assert [p["text"] for p in slack.posts() if "unavailable" in p["text"]] == [FIXED_REPLY]
        assert len(llm.calls) == 2
        assert len(notion.calls) == calls_before


# --- transitions: told once, audited once ------------------------------------------------------


async def test_a_transition_notifies_once_and_recovery_tells_the_creator():
    """Fails if a repeated failing check notifies again, or recovery is not audited and told to the creator."""
    notion = make_fake_notion()
    break_schema(notion)
    clock = Clock(NOW)
    cfg = replace(parse_agents_config(agents_dict()), admins=("U_ADMIN",))
    w = World(notion=notion, cfg=cfg, store=await published_store(), clock=clock)
    await w.members.add_member(TENANT, "U_MEMBER", "admin", "U_ADMIN")

    await w.check()
    clock.now = NOW + timedelta(minutes=1)
    await w.check()
    row = await w.health.get(TENANT, "it-helper")
    assert row.checked_at == clock.now  # the last check time moves, the status does not

    notion.set_schema(DB_ID, default_schema())
    clock.now = NOW + timedelta(minutes=2)
    await w.check()

    dms = w.dms()
    assert set(dms) == {"D-U_EDITOR", "D-U_ADMIN", "D-U_MEMBER"}
    assert len(dms["D-U_EDITOR"]) == 2
    assert len(dms["D-U_ADMIN"]) == 1 and len(dms["D-U_MEMBER"]) == 1
    assert "IT helper" in dms["D-U_ADMIN"][0] and "IT requests" in dms["D-U_ADMIN"][0]
    assert "available again" in dms["D-U_EDITOR"][1]
    assert [(e.kind, e.tool) for e in w.audit.events] == [
        ("agent_health", "unavailable"),
        ("agent_health", "available"),
    ]
    assert (await w.health.get(TENANT, "it-helper")).status == AVAILABLE


# --- a write waiting for approval --------------------------------------------------------------


async def test_submit_on_a_now_unavailable_agent_refuses_with_the_fixed_reason(harness):
    """Fails if an approved write runs, or the card says anything else, after the agent's grants broke."""
    h = harness
    handle = await start_task(h)
    await h.acts.health.record(
        AgentHealth(TENANT, "it-helper", 1, UNAVAILABLE, "grant 'IT requests': problem", NOW)
    )

    await approve(h, handle)

    assert await handle.result() == "cancelled"
    assert h.notion.count("create_page") == 0 and h.notion.all_pages(DB_ID) == []
    assert h.slack.updates()[-1]["text"] == FIXED_REPLY
    assert [e.kind for e in h.audit.events] == ["approval", "write"]
    assert h.audit.events[1].outcome == "error"


async def test_submit_after_health_flips_between_the_check_and_the_write_shows_the_clean_line(
    harness,
):
    """Fails if the write race shows "Failed: …" on the card instead of the fixed unavailable line."""
    h = harness
    handle = await start_task(h)
    h.acts.health = FlipsAfterReads(quiet_reads=1)  # the pre-write check reads once

    await approve(h, handle)

    assert await handle.result() == "cancelled"
    assert h.notion.count("create_page") == 0 and h.notion.all_pages(DB_ID) == []
    assert h.slack.updates()[-1]["text"] == FIXED_REPLY


async def test_a_watch_keeps_polling_while_unavailable_and_reports_the_change_after_recovery(
    harness,
):
    """Fails if a watch ends or posts a stopped line while the agent is unavailable, or misses
    the status change after recovery."""
    h = harness
    handle = await start_task(h)
    await approve(h, handle)
    await wait_state(handle, "watching")
    page_id = h.notion.all_pages(DB_ID)[0]["id"]
    await h.acts.health.record(
        AgentHealth(TENANT, "it-helper", 1, UNAVAILABLE, "grant 'IT requests': problem", NOW)
    )
    reads_before = h.notion.count("retrieve_page")
    h.notion.set_status(page_id, "Status", "Done")

    await h.wf_env.sleep(timedelta(minutes=16))

    assert h.notion.count("retrieve_page") == reads_before
    assert await handle.query(TaskWorkflow.state) == "watching"
    assert thread_posts(h, "was marked Done") == []
    assert thread_posts(h, "stopped watching") == []

    await h.acts.health.record(AgentHealth(TENANT, "it-helper", 1, AVAILABLE, None, NOW))
    await h.wf_env.sleep(timedelta(minutes=16))

    assert await handle.result() == "done"
    assert len(thread_posts(h, "was marked Done")) == 1


# --- recovery on the directory refresh, without a restart --------------------------------------


@pytest.mark.parametrize(
    ("row_status", "row_version", "agent_version", "elapsed_seconds", "due"),
    [
        (None, None, 1, 0, True),
        (AVAILABLE, 1, 1, 3600, False),
        (UNAVAILABLE, 1, 1, 60, False),
        (UNAVAILABLE, 1, 1, 300, True),
        (UNAVAILABLE, 1, 2, 60, True),
        (AVAILABLE, 1, 2, 60, True),
    ],
)
def test_the_refresh_checks_only_agents_that_are_due(
    row_status, row_version, agent_version, elapsed_seconds, due
):
    """Fails if the refresh re-checks an available agent, re-checks within the bound, or skips a new version."""
    agent = replace(parse_agents_config(agents_dict()).agents["it-helper"], version=agent_version)
    row = (
        None
        if row_status is None
        else AgentHealth(
            TENANT,
            "it-helper",
            row_version,
            row_status,
            "grant 'g': problem" if row_status == UNAVAILABLE else None,
            NOW,
        )
    )

    assert health_due(row, agent, NOW + timedelta(seconds=elapsed_seconds), 300.0) is due


async def test_the_refresh_restores_an_agent_without_a_restart():
    """Fails if recovery needs a restart, or the refresh re-checks an unavailable agent within the bound."""
    notion = make_fake_notion()
    break_schema(notion)
    clock = Clock(NOW)
    w = World(notion=notion, store=await published_store(), clock=clock)
    await w.check()
    assert (await w.health.get(TENANT, "it-helper")).status == UNAVAILABLE
    reads = notion.count("retrieve_schema")

    clock.now = NOW + timedelta(seconds=60)
    await refresh_health_once(w.checker, w.registry, w.agents, w.health, clock.now)
    assert notion.count("retrieve_schema") == reads

    notion.set_schema(DB_ID, default_schema())
    clock.now = NOW + timedelta(seconds=301)
    await refresh_health_once(w.checker, w.registry, w.agents, w.health, clock.now)

    assert (await w.health.get(TENANT, "it-helper")).status == AVAILABLE
    assert notion.count("retrieve_schema") > reads
    assert list(w.dms()) == ["D-U_EDITOR"]


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (ConnectorError("The granted database has no data source."), UNAVAILABLE),
        (RuntimeError("a bug in the check"), None),
    ],
    ids=["connector_error", "unexpected_error"],
)
async def test_the_refresh_checks_the_other_agents_when_one_check_raises(error, status):
    """Fails if one agent's check raising stops a later agent's recovery on the same refresh pass."""
    notion = make_fake_notion()
    notion.set_schema(OTHER_DB_ID, default_schema())
    break_schema(notion, OTHER_DB_ID)
    clock = Clock(NOW)
    w = World(notion=SchemaRaises(notion, DB_ID, error), cfg=two_agents_config(), clock=clock)
    await w.check()
    assert (await w.health.get(TENANT, "good-helper")).status == UNAVAILABLE

    notion.set_schema(OTHER_DB_ID, default_schema())
    clock.now = NOW + timedelta(seconds=301)
    await refresh_health_once(w.checker, w.registry, w.agents, w.health, clock.now)

    assert (await w.health.get(TENANT, "good-helper")).status == AVAILABLE
    row = await w.health.get(TENANT, "it-helper")
    if status is None:
        assert row is None
    else:
        assert row.status == status


async def test_a_new_published_version_is_checked_at_once():
    """Fails if a republished agent waits out the re-check bound while unavailable."""
    notion = make_fake_notion()
    break_schema(notion)
    clock = Clock(NOW)
    w = World(notion=notion, store=await published_store(), clock=clock)
    await w.check()
    assert (await w.health.get(TENANT, "it-helper")).status == UNAVAILABLE

    await republish(w.agents, "it-helper", lambda d: d.__setitem__("name", "IT helper v2"))
    notion.set_schema(DB_ID, default_schema())
    clock.now = NOW + timedelta(seconds=60)
    await refresh_health_once(w.checker, w.registry, w.agents, w.health, clock.now)

    row = await w.health.get(TENANT, "it-helper")
    assert (row.status, row.agent_version) == (AVAILABLE, 2)
