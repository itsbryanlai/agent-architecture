"""Audience changes are watched: a guest or outside person joining, a channel shared with
another organisation, or a private channel made public pauses the agents that answer there
until their owner or an admin confirms the new audience again."""

import copy
import json
import re
import secrets
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from slack_sdk.signature import SignatureVerifier
from temporalio.client import WorkflowHistory
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_store import InMemoryAgentStore
from agentos.audience import (
    AudienceChecker,
    AudienceNotConfirmed,
    SlackAudienceInspector,
    paused_text,
)
from agentos.audience_pauses import (
    CHANNEL_SHARED,
    EXTERNAL_JOINED,
    GUEST_JOINED,
    MADE_PUBLIC,
    UNVERIFIED,
    InMemoryPauseStore,
    NotPaused,
)
from agentos.audience_watch import AudienceWatchActivities
from agentos.audit import InMemoryAuditStore
from agentos.config import TASK_QUEUE, Settings, TenantsConfig, parse_agents_config
from agentos.control import ControlPlane, Forbidden
from agentos.gateway.app import create_app as create_gateway
from agentos.grant_checker import NotionGrantChecker
from agentos.llm import ScriptedLLMClient
from agentos.members import InMemoryMemberStore
from agentos.models import (
    AUDIT_KINDS,
    JOINED,
    PUBLIC,
    SHARED,
    AudienceEvent,
    ConversationRef,
    InboundMessage,
    LLMResponse,
)
from agentos.slack_client import FakeSlack, SlackLookupError, SlackUser
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for
from agentos.workflows.audience_watch import AudienceWatchWorkflow
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, TEAM, TENANT, agents_dict, eventually, make_fake_notion
from tests.console_harness import ROOT as CONSOLE_ADMIN
from tests.console_harness import ConsoleHarness, csrf_token, post, using
from tests.test_gateway import FakeTemporalClient, signed_headers

ROOT_DIR = Path(__file__).resolve().parent.parent
CHAN = "C0AAAA111"
OTHER_CHAN = "C0BBBB222"
THIRD_CHAN = "C0CCCC333"
ADMIN = "U_ROOT"
OWNER = "U_OWNER"
HR_OWNER = "U_HR_OWNER"
MEMBER_ADMIN = "U_ADMIN2"
BYSTANDER = "U_OTHER_CREATOR"
THREAD = "1700000000.000100"

GUEST = SlackUser("U_GUEST", TEAM, is_restricted=True)
SINGLE_GUEST = SlackUser("U_SINGLE", TEAM, is_ultra_restricted=True)
EXTERNAL = SlackUser("U_EXT", "T0OTHER")
BOT = SlackUser("U_BOT", TEAM, is_bot=True)
PEOPLE = {u.id: u for u in (GUEST, SINGLE_GUEST, EXTERNAL, BOT)}


def watch_raw() -> dict:
    raw = agents_dict()
    raw["admins"] = [ADMIN]
    helper = raw["agents"]["it-helper"]
    helper["description"] = "IT requests."
    helper["audience"] = {"channels": [CHAN, THIRD_CHAN], "dm": "everyone"}
    hr = copy.deepcopy(helper)
    hr["name"] = "HR helper"
    hr["audience"] = {"channels": [OTHER_CHAN, THIRD_CHAN]}
    raw["agents"]["hr-helper"] = hr
    return raw


class Clock:
    def __call__(self) -> float:
        return 1000.0


class World:
    """Stores, fakes and the activities and plane that share them."""

    def __init__(self, slack: FakeSlack | None = None):
        self.config = parse_agents_config(watch_raw())
        self.tenants = TenantsConfig.of(self.config)
        self.slack = slack or FakeSlack(users=dict(PEOPLE))
        self.slack.users.update(PEOPLE)
        self.slack.members_of.setdefault(CHAN, [ALICE, GUEST.id])
        self.slack.members_of.setdefault(OTHER_CHAN, [ALICE])
        self.slack.members_of.setdefault(THIRD_CHAN, [ALICE])
        self.store = InMemoryAgentStore.seeded(self.config, by=OWNER)
        hr = InMemoryAgentStore.seeded(self.config, by=HR_OWNER)
        self.store._agents[(TENANT, "hr-helper")] = hr._agents[(TENANT, "hr-helper")]
        self.members = InMemoryMemberStore()
        self.pauses = InMemoryPauseStore()
        self.audit = InMemoryAuditStore()
        self.idem = InMemoryIdempotencyStore()
        self.registry = TenantRegistry(
            {TENANT: runtime_for(self.config, self.slack, make_fake_notion())}
        )
        self.watch = AudienceWatchActivities(
            self.registry, self.store, self.members, self.pauses, self.audit, self.idem
        )
        inspector = SlackAudienceInspector(lambda t: self.slack, lambda t: TEAM, clock=Clock())
        self.plane = ControlPlane(
            self.store,
            self.members,
            self.audit,
            self.tenants,
            NotionGrantChecker(lambda t: make_fake_notion()),
            inspector,
            pauses=self.pauses,
        )
        self.checker = AudienceChecker(self.registry, self.store, pauses=self.pauses)

    async def seed_members(self) -> None:
        await self.members.add_member(TENANT, MEMBER_ADMIN, "admin", ADMIN)
        await self.members.add_member(TENANT, OWNER, "creator", ADMIN)
        await self.members.add_member(TENANT, BYSTANDER, "creator", ADMIN)

    async def event(self, kind: str, channel: str, user: str | None = None, id_: str = "Ev1"):
        ev = AudienceEvent(TENANT, id_, kind, channel, user)
        return await ActivityEnvironment().run(self.watch.watch_audience_event, ev)

    async def paused(self, agent_id: str | None = None):
        return await self.pauses.list_active(TENANT, agent_id)

    def dms(self) -> list[dict]:
        return self.slack.posts()


def joined(user: str):
    return (JOINED, CHAN, user)


# --- what pauses, and what does not -----------------------------------------------------


@pytest.mark.parametrize(
    "user, reason",
    [(GUEST.id, GUEST_JOINED), (SINGLE_GUEST.id, GUEST_JOINED), (EXTERNAL.id, EXTERNAL_JOINED)],
)
async def test_a_guest_or_outside_person_joining_pauses_the_agent_in_that_channel_only(
    user, reason
):
    w = World()
    paused = await w.event(JOINED, CHAN, user)
    assert paused == ["it-helper"]
    (pause,) = await w.paused()
    assert (pause.agent_id, pause.channel, pause.reason) == ("it-helper", CHAN, reason)
    assert await w.paused("hr-helper") == []


async def test_a_channel_two_agents_serve_pauses_both():
    w = World()
    assert sorted(await w.event(JOINED, THIRD_CHAN, GUEST.id)) == ["hr-helper", "it-helper"]
    assert {(p.agent_id, p.channel) for p in await w.paused()} == {
        ("it-helper", THIRD_CHAN),
        ("hr-helper", THIRD_CHAN),
    }


async def test_a_full_member_or_a_bot_joining_changes_nothing():
    w = World()
    assert await w.event(JOINED, CHAN, ALICE, "Ev1") == []
    assert await w.event(JOINED, CHAN, BOT.id, "Ev2") == []
    assert await w.paused() == []
    assert w.dms() == []
    assert [e for e in w.audit.events if e.kind == "audience_pause"] == []


async def test_a_channel_no_agent_serves_changes_nothing():
    w = World()
    assert await w.event(JOINED, "C0NOBODY99", GUEST.id) == []
    assert await w.event(SHARED, "C0NOBODY99") == []
    assert await w.paused() == []


async def test_sharing_a_channel_with_another_organisation_pauses_it():
    w = World()
    assert await w.event(SHARED, CHAN) == ["it-helper"]
    (pause,) = await w.paused()
    assert (pause.channel, pause.reason) == (CHAN, CHANNEL_SHARED)


async def test_a_private_channel_made_public_pauses_it():
    w = World()
    assert await w.event(PUBLIC, OTHER_CHAN) == ["hr-helper"]
    (pause,) = await w.paused()
    assert (pause.channel, pause.reason) == (OTHER_CHAN, MADE_PUBLIC)


async def test_the_joiner_is_looked_up_fresh_every_time():
    w = World()
    await w.event(JOINED, CHAN, ALICE, "Ev1")
    await w.event(JOINED, CHAN, ALICE, "Ev2")
    looked_up = [kw for name, kw in w.slack.calls if name == "user_info"]
    assert looked_up == [{"user_id": ALICE}, {"user_id": ALICE}]


async def test_a_legacy_default_agent_is_paused_in_a_channel_the_file_does_not_map():
    raw = agents_dict()
    raw["admins"] = [ADMIN]
    config = parse_agents_config(raw)
    w = World()
    w.config = config
    w.store = InMemoryAgentStore.seeded(config, by=OWNER)
    w.registry = TenantRegistry({TENANT: runtime_for(config, w.slack, make_fake_notion())})
    w.watch = AudienceWatchActivities(w.registry, w.store, w.members, w.pauses, w.audit, w.idem)
    assert await w.event(JOINED, "C0UNMAPPED1", GUEST.id) == ["it-helper"]


# --- idempotent per event; who is told --------------------------------------------------


async def test_the_same_event_twice_pauses_audits_and_tells_once():
    w = World()
    await w.seed_members()
    await w.event(JOINED, CHAN, GUEST.id, "Ev1")
    await w.event(JOINED, CHAN, GUEST.id, "Ev1")
    assert len(await w.paused()) == 1
    assert len([e for e in w.audit.events if e.kind == "audience_pause"]) == 1
    told = sorted(p["channel"] for p in w.dms())
    assert told == sorted({f"D-{u}" for u in (OWNER, ADMIN, MEMBER_ADMIN)})


async def test_a_second_trigger_while_paused_neither_repauses_nor_tells_again():
    w = World()
    await w.event(JOINED, CHAN, GUEST.id, "Ev1")
    sent = len(w.dms())
    await w.event(SHARED, CHAN, None, "Ev2")
    (pause,) = await w.paused()
    assert pause.reason == GUEST_JOINED
    assert len(w.dms()) == sent


async def test_the_owner_and_every_admin_are_told_once_and_nobody_else():
    w = World()
    await w.seed_members()
    await w.event(JOINED, CHAN, EXTERNAL.id)
    posts = w.dms()
    assert sorted(p["channel"] for p in posts) == sorted(
        f"D-{u}" for u in (OWNER, ADMIN, MEMBER_ADMIN)
    )
    for post_ in posts:
        assert f"<#{CHAN}>" in post_["text"] and "IT helper" in post_["text"]
        assert "outside" in post_["text"].lower() or "another workspace" in post_["text"].lower()
    assert f"D-{BYSTANDER}" not in {p["channel"] for p in posts}


async def test_the_notice_carries_the_channel_and_reason_and_no_content():
    w = World()
    await w.event(SHARED, CHAN)
    for post_ in w.dms():
        assert post_["thread_ts"] is None
        assert "shared" in post_["text"].lower()
        assert len(post_["text"]) < 400


async def test_a_new_pause_after_a_resume_tells_again():
    w = World()
    await w.event(JOINED, CHAN, GUEST.id, "Ev1")
    first = len(w.dms())
    await w.plane.resume_channel(TENANT, ADMIN, "it-helper", CHAN, COUNTS)
    await w.event(JOINED, CHAN, EXTERNAL.id, "Ev2")
    assert len(w.dms()) == 2 * first
    assert len([e for e in w.audit.events if e.kind == "audience_pause"]) == 2


async def test_pausing_is_audited_with_ids_and_the_reason_only():
    w = World()
    await w.event(JOINED, CHAN, GUEST.id)
    (event,) = [e for e in w.audit.events if e.kind == "audience_pause"]
    assert (event.agent_id, event.agent_version, event.tool, event.target) == (
        "it-helper",
        1,
        GUEST_JOINED,
        CHAN,
    )
    assert event.requester_user_id == GUEST.id
    assert event.event_key.startswith(f"{TENANT}:")
    assert event.input_sha256 is None and event.output_sha256 is None


# --- the durable workflow ----------------------------------------------------------------


@asynccontextmanager
async def running(w: World, llm: ScriptedLLMClient | None = None):
    wf_env = await WorkflowEnvironment.start_time_skipping()
    w.llm = llm or ScriptedLLMClient()
    w.history = InMemoryHistoryStore()
    acts = AgentActivities(
        settings=Settings(),
        registry=w.registry,
        history=w.history,
        idem=w.idem,
        links=InMemoryTaskLinkStore(),
        llm=w.llm,
        tool_outputs=InMemoryToolOutputStore(),
        audit=w.audit,
        agents=w.store,
        pauses=w.pauses,
    )
    worker = Worker(
        wf_env.client,
        task_queue=TASK_QUEUE,
        workflows=[ConversationWorkflow, TaskWorkflow, AudienceWatchWorkflow],
        activities=[*acts.all(), *w.watch.all()],
    )
    w.client = wf_env.client
    async with worker:
        try:
            yield w
        finally:
            await wf_env.shutdown()


async def start_watch(w: World, ev: AudienceEvent):
    return await w.client.start_workflow(
        AudienceWatchWorkflow.run,
        ev,
        id=ids.audience_event_workflow_id(TENANT, ev.event_id),
        task_queue=TASK_QUEUE,
        id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
    )


async def test_the_event_workflow_pauses_and_a_repeat_of_the_event_is_rejected():
    async with running(World()) as w:
        ev = AudienceEvent(TENANT, "Ev9", JOINED, CHAN, GUEST.id)
        handle = await start_watch(w, ev)
        assert await handle.result() == ["it-helper"]
        with pytest.raises(WorkflowAlreadyStartedError):
            await start_watch(w, ev)
        assert len(await w.paused()) == 1


async def test_the_workflow_id_names_the_tenant_and_the_event():
    assert ids.audience_event_workflow_id("acme", "Ev9") == "acme:audience-event:Ev9"


async def test_a_joiner_who_cannot_be_looked_up_pauses_the_agent_unverified():
    w = World()
    w.slack.fail_lookups(SlackLookupError("missing_scope"))
    async with running(w) as w:
        handle = await start_watch(w, AudienceEvent(TENANT, "Ev5", JOINED, CHAN, ALICE))
        assert await handle.result() == ["it-helper"]
        (pause,) = await w.paused()
    assert pause.reason == UNVERIFIED


# --- enforcement in the turn -------------------------------------------------------------


def say(event_id: str, user: str, channel: str, kind: str, thread: str = THREAD):
    return InboundMessage(event_id, user, "secret question", channel, thread, kind)


async def converse(w: World, agent_id: str, msg: InboundMessage):
    return await w.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, agent_id, msg.channel, msg.thread_ts),
        id=ids.conversation_id(TENANT, msg.channel, msg.thread_ts),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )


def answer(text: str) -> LLMResponse:
    return LLMResponse(text=text, tool_calls=[], stop_reason="end_turn")


def refusals(w: World):
    return [e for e in w.audit.events if e.kind == "audience_refused"]


PAUSED = "IT helper is paused in this channel until its owner reviews who can see it."


def test_the_paused_text_is_fixed_and_escapes_the_agent_name():
    assert paused_text("IT helper") == PAUSED
    assert paused_text("A <b> & C").startswith("A &lt;b&gt; &amp; C is paused")


async def test_a_paused_channel_refuses_with_the_paused_text_and_runs_nothing():
    async with running(World()) as w:
        await w.event(SHARED, CHAN)
        await converse(w, "it-helper", say("Ev1", ALICE, CHAN, "channel"))
        await eventually(lambda: [p for p in w.slack.posts() if p["channel"] == CHAN])
        (post_,) = [p for p in w.slack.posts() if p["channel"] == CHAN]
        assert post_["text"] == PAUSED
        assert w.llm.calls == []
        assert w.slack.updates() == []
        assert await w.history.load(TENANT, ids.conversation_id(TENANT, CHAN, THREAD)) == []
        (event,) = refusals(w)
        assert (event.tool, event.agent_id, event.requester_user_id) == (
            "paused",
            "it-helper",
            ALICE,
        )
        assert "secret question" not in repr(event)


async def test_other_channels_and_direct_messages_keep_answering_while_one_is_paused():
    llm = ScriptedLLMClient()
    llm.queue(answer("from dm"), answer("from hr"))
    async with running(World(), llm) as w:
        await w.event(JOINED, CHAN, GUEST.id)
        await converse(w, "it-helper", say("Ev1", ALICE, "D111", "im", "1700000000.000200"))
        await converse(
            w, "hr-helper", say("Ev2", ALICE, OTHER_CHAN, "channel", "1700000000.000300")
        )
        await eventually(lambda: len(w.slack.updates()) >= 2)
        assert {u["text"] for u in w.slack.updates()} == {"from dm", "from hr"}
        assert refusals(w) == []


async def test_a_thread_already_running_in_the_channel_is_paused_too():
    llm = ScriptedLLMClient()
    llm.queue(answer("first answer"))
    async with running(World(), llm) as w:
        handle = await converse(w, "it-helper", say("Ev1", ALICE, CHAN, "channel"))
        await eventually(lambda: w.slack.updates())
        await w.event(JOINED, CHAN, EXTERNAL.id)
        await handle.signal("message", say("Ev2", ALICE, CHAN, "channel"))
        await eventually(lambda: any(p["text"] == PAUSED for p in w.slack.posts()))
        assert len(w.llm.calls) == 1


async def test_a_resumed_channel_answers_again():
    llm = ScriptedLLMClient()
    llm.queue(answer("back again"))
    async with running(World(), llm) as w:
        await w.event(JOINED, CHAN, GUEST.id)
        await w.plane.resume_channel(TENANT, ADMIN, "it-helper", CHAN, COUNTS)
        await converse(w, "it-helper", say("Ev1", ALICE, CHAN, "channel"))
        await eventually(lambda: w.slack.updates())
        assert w.slack.updates()[0]["text"] == "back again"
        assert refusals(w) == []


async def test_a_paused_specialist_is_not_offered_and_not_reachable_in_that_channel():
    w = World()
    await w.event(JOINED, CHAN, GUEST.id)
    agent = await w.store.published(TENANT, "it-helper")
    decision = await w.checker.decide(TENANT, agent, ALICE, CHAN, "channel")
    assert (decision.allowed, decision.reason) == (False, "paused")
    dm = await w.checker.decide(TENANT, agent, ALICE, "D1", "im")
    assert dm.allowed
    other = await w.store.published(TENANT, "hr-helper")
    assert (await w.checker.decide(TENANT, other, ALICE, OTHER_CHAN, "channel")).allowed


async def test_a_thread_recorded_with_a_paused_refusal_replays():
    async with running(World()) as w:
        await w.event(SHARED, CHAN)
        handle = await converse(w, "it-helper", say("Ev1", ALICE, CHAN, "channel"))
        await eventually(lambda: w.slack.posts())
        history = await handle.fetch_history()
    await Replayer(workflows=[ConversationWorkflow, TaskWorkflow]).replay_workflow(
        WorkflowHistory.from_json(history.workflow_id, history.to_json())
    )


# --- resuming in the control plane -------------------------------------------------------


async def paused_world() -> World:
    w = World()
    await w.seed_members()
    await w.event(JOINED, CHAN, GUEST.id)
    return w


COUNTS = {"total": 2, "guests": 1, "external": 0}


async def test_the_owner_resumes_with_the_fresh_counts():
    w = await paused_world()
    record = await w.plane.resume_channel(TENANT, OWNER, "it-helper", CHAN, COUNTS)
    assert (record.resumed_by, record.resumed_at is not None) == (OWNER, True)
    assert await w.paused() == []
    (event,) = [e for e in w.audit.events if e.kind == "audience_resume"]
    assert (event.agent_id, event.agent_version, event.approver_user_id) == ("it-helper", 1, OWNER)
    assert event.target == f"{CHAN} total=2 guests=1 external=0"
    assert event.requester_user_id == OWNER


async def test_an_admin_resumes_any_agent():
    w = await paused_world()
    await w.plane.resume_channel(TENANT, MEMBER_ADMIN, "it-helper", CHAN, COUNTS)
    assert await w.paused() == []


async def test_another_creator_and_a_stranger_cannot_resume():
    w = await paused_world()
    for who in (BYSTANDER, "U_NOBODY", GUEST.id):
        with pytest.raises(Forbidden):
            await w.plane.resume_channel(TENANT, who, "it-helper", CHAN, COUNTS)
    assert len(await w.paused()) == 1
    assert [e for e in w.audit.events if e.kind == "audience_resume"] == []


async def test_resume_needs_the_confirmation_and_names_the_counts():
    w = await paused_world()
    with pytest.raises(AudienceNotConfirmed, match="2 people"):
        await w.plane.resume_channel(TENANT, OWNER, "it-helper", CHAN, None)
    assert len(await w.paused()) == 1


async def test_resume_with_stale_counts_is_refused():
    w = await paused_world()
    w.slack.members_of[CHAN] = [ALICE, GUEST.id, EXTERNAL.id]
    with pytest.raises(AudienceNotConfirmed, match="changed"):
        await w.plane.resume_channel(TENANT, OWNER, "it-helper", CHAN, COUNTS)
    assert len(await w.paused()) == 1
    await w.plane.resume_channel(
        TENANT, OWNER, "it-helper", CHAN, {"total": 3, "guests": 1, "external": 1}
    )
    assert await w.paused() == []


async def test_resuming_a_channel_that_is_not_paused_is_refused():
    w = await paused_world()
    with pytest.raises(NotPaused):
        await w.plane.resume_channel(TENANT, OWNER, "it-helper", OTHER_CHAN, COUNTS)
    with pytest.raises(NotPaused):
        await w.plane.resume_channel(TENANT, ADMIN, "hr-helper", CHAN, COUNTS)


async def test_resuming_keeps_the_history_of_the_pause():
    w = await paused_world()
    await w.plane.resume_channel(TENANT, ADMIN, "it-helper", CHAN, COUNTS)
    history = w.pauses.history
    assert [(p.agent_id, p.channel, p.resumed_by) for p in history] == [("it-helper", CHAN, ADMIN)]


async def test_paused_channels_are_read_by_admins_and_the_agents_editors_with_fresh_counts():
    w = await paused_world()
    await w.event(JOINED, OTHER_CHAN, EXTERNAL.id, "Ev2")
    everything = await w.plane.paused_channels(TENANT, ADMIN)
    assert {(v.pause.agent_id, v.pause.channel) for v in everything} == {
        ("it-helper", CHAN),
        ("hr-helper", OTHER_CHAN),
    }
    mine = await w.plane.paused_channels(TENANT, OWNER)
    assert [(v.pause.agent_id, v.pause.channel) for v in mine] == [("it-helper", CHAN)]
    view = mine[0]
    assert view.summary.confirmation() == COUNTS
    named = await w.plane.paused_channels(TENANT, OWNER, "it-helper")
    assert len(named) == 1
    with pytest.raises(Forbidden):
        await w.plane.paused_channels(TENANT, BYSTANDER, "it-helper")
    with pytest.raises(Forbidden):
        await w.plane.paused_channels(TENANT, "U_NOBODY")


async def test_a_page_read_may_not_have_counts_when_slack_cannot_be_read():
    w = await paused_world()
    w.slack.fail_lookups(SlackLookupError("missing_scope"))
    (view,) = await w.plane.paused_channels(TENANT, OWNER, "it-helper")
    assert view.summary is None and "missing_scope" in view.problem


# --- the console -------------------------------------------------------------------------


async def console_world():
    slack = FakeSlack(users=dict(PEOPLE), channel_members={CHAN: [ALICE, GUEST.id]})
    inspector = SlackAudienceInspector(lambda t: slack, lambda t: TEAM, clock=Clock())
    h = ConsoleHarness(audience=inspector)
    await h.seed_members()
    raw = agents_dict()
    helper = raw["agents"]["it-helper"]
    helper["audience"] = {"channels": [CHAN]}
    config = parse_agents_config(raw)
    seeded = InMemoryAgentStore.seeded(config, by="U_OWNER")
    h.agents._agents.update(seeded._agents)
    await h.pauses.open_pause(TENANT, "it-helper", CHAN, GUEST_JOINED, datetime.now(UTC))
    return h


AGENT_URL = f"/ui/{TENANT}/agents/it-helper"


async def test_the_agent_page_shows_the_paused_channel_the_reason_and_fresh_counts():
    h = await console_world()
    c = await h.signed_in(CONSOLE_ADMIN)
    async with using(c):
        page = (await c.get(AGENT_URL)).text
    assert "Paused channels" in page and CHAN in page
    assert "guest joined" in page.lower()
    assert "2 people" in page and "1 guest" in page
    assert 'name="confirm"' in page and f"{AGENT_URL}/channels/{CHAN}/resume" in page


async def test_the_version_page_shows_the_paused_channels_too():
    h = await console_world()
    c = await h.signed_in(CONSOLE_ADMIN)
    async with using(c):
        page = (await c.get(f"{AGENT_URL}/versions/1")).text
    assert "Paused channels" in page and CHAN in page


async def test_the_console_resumes_only_with_the_box_ticked_and_current_counts():
    h = await console_world()
    c = await h.signed_in("U_OWNER")
    async with using(c):
        token = await csrf_token(c)
        url = f"{AGENT_URL}/channels/{CHAN}/resume"
        unticked = await post(c, url, token)
        assert unticked.status_code == 409 and "confirm" in unticked.text
        stale = await post(c, url, token, confirm="yes", total="9", guests="1", external="0")
        assert stale.status_code == 409 and "changed" in stale.text
        assert len(await h.pauses.list_active(TENANT)) == 1
        done = await post(c, url, token, confirm="yes", total="2", guests="1", external="0")
        assert done.status_code == 303
    assert await h.pauses.list_active(TENANT) == []
    assert [e.kind for e in h.audit.events if e.kind == "audience_resume"] == ["audience_resume"]


async def test_another_creator_cannot_resume_from_the_console():
    h = await console_world()
    c = await h.signed_in("U_OTHER")
    async with using(c):
        token = await csrf_token(c)
        refused = await post(
            c,
            f"{AGENT_URL}/channels/{CHAN}/resume",
            token,
            confirm="yes",
            total="2",
            guests="1",
            external="0",
        )
        assert refused.status_code == 403
    assert len(await h.pauses.list_active(TENANT)) == 1


async def test_the_page_escapes_a_channel_id():
    h = await console_world()
    await h.pauses.open_pause(
        TENANT, "it-helper", "C<script>x</script>", GUEST_JOINED, datetime.now(UTC)
    )
    c = await h.signed_in(CONSOLE_ADMIN)
    async with using(c):
        page = (await c.get(AGENT_URL)).text
    assert "<script>x" not in page


# --- the gateway -------------------------------------------------------------------------


@pytest.fixture
def secret() -> str:
    return secrets.token_hex(16)


@pytest.fixture
def temporal() -> FakeTemporalClient:
    return FakeTemporalClient()


@pytest.fixture
def client(agents, temporal, secret) -> TestClient:
    from agentos.stores import InMemoryTaskLinkStore as Links

    app = create_gateway(
        Settings(), TenantsConfig.of(agents), temporal, Links(), SignatureVerifier(secret)
    )
    return TestClient(app)


def send(client, secret, event: dict, event_id: str = "EvW1", team: str = TEAM, sign=None):
    payload = {"type": "event_callback", "team_id": team, "event_id": event_id, "event": event}
    body = json.dumps(payload).encode()
    headers = signed_headers(sign or secret, body, "application/json")
    return client.post("/slack/events", content=body, headers=headers)


def watch_starts(temporal) -> list[dict]:
    return [c for c in temporal.signal_with_starts if c["workflow"] == "AudienceWatchWorkflow"]


def test_the_gateway_starts_a_watch_workflow_for_a_member_joining(client, secret, temporal):
    event = {"type": "member_joined_channel", "user": GUEST.id, "channel": CHAN, "team": TEAM}
    assert send(client, secret, event).status_code == 200
    (call,) = watch_starts(temporal)
    assert call["id"] == ids.audience_event_workflow_id(TENANT, "EvW1")
    assert call["task_queue"] == TASK_QUEUE
    assert call["id_reuse_policy"] == WorkflowIDReusePolicy.REJECT_DUPLICATE
    assert call["arg"] == AudienceEvent(TENANT, "EvW1", JOINED, CHAN, GUEST.id)


def test_the_gateway_starts_a_watch_workflow_for_a_shared_channel(client, secret, temporal):
    event = {"type": "channel_shared", "connected_team_id": "T0OTHER", "channel": CHAN}
    assert send(client, secret, event).status_code == 200
    (call,) = watch_starts(temporal)
    assert call["arg"] == AudienceEvent(TENANT, "EvW1", SHARED, CHAN, None)


def test_the_gateway_starts_a_watch_workflow_for_a_channel_made_public(client, secret, temporal):
    event = {
        "type": "message",
        "subtype": "channel_convert_to_public",
        "channel": CHAN,
        "user": ALICE,
        "ts": "1700000000.000500",
    }
    assert send(client, secret, event).status_code == 200
    (call,) = watch_starts(temporal)
    assert call["arg"] == AudienceEvent(TENANT, "EvW1", PUBLIC, CHAN, None)


def test_other_message_subtypes_and_events_still_start_nothing(client, secret, temporal):
    for event in (
        {"type": "message", "subtype": "channel_join", "channel": CHAN, "user": ALICE, "ts": "1.1"},
        {"type": "channel_unshared", "channel": CHAN},
        {"type": "member_left_channel", "user": ALICE, "channel": CHAN},
    ):
        assert send(client, secret, event).status_code == 200
    assert temporal.signal_with_starts == []


def test_the_gateway_rejects_a_bad_signature_for_a_watch_event(client, secret, temporal):
    event = {"type": "member_joined_channel", "user": GUEST.id, "channel": CHAN}
    assert send(client, secret, event, sign="not-the-secret").status_code == 401
    assert temporal.signal_with_starts == []


def test_a_workspace_that_is_not_a_tenant_is_ignored(client, secret, temporal):
    event = {"type": "member_joined_channel", "user": GUEST.id, "channel": CHAN}
    assert send(client, secret, event, team="T0STRANGER").status_code == 200
    assert temporal.signal_with_starts == []


def test_a_slack_retry_of_the_same_event_is_acknowledged_without_a_second_run(
    client, secret, temporal
):
    async def reject(workflow, arg=None, **kwargs):
        raise WorkflowAlreadyStartedError("x", "AudienceWatchWorkflow")

    temporal.start_workflow = reject
    event = {"type": "member_joined_channel", "user": GUEST.id, "channel": CHAN}
    assert send(client, secret, event).status_code == 200


# --- schema, ids, kinds ------------------------------------------------------------------

MIGRATION = (ROOT_DIR / "scripts" / "migrations" / "013_audience_watch.sql").read_text()
INIT_DB = (ROOT_DIR / "scripts" / "init_db.sql").read_text()

KINDS_SINCE_010 = [
    "answer", "tool_call", "hand_off", "approval", "write", "agent_create", "agent_draft",
    "agent_edit", "agent_import", "agent_submit", "agent_withdraw", "agent_reject",
    "agent_publish", "member_add", "member_remove", "member_role", "audience_refused",
    "audience_confirm", "budget_set", "audience_pause", "audience_resume",
]  # fmt: skip


def kinds_in(sql: str, after: str) -> list[str]:
    block = sql[sql.index(after) :]
    return re.findall(r"'(\w+)'", block[: block.index("))")])


@pytest.mark.parametrize(
    "sql, later",
    [(MIGRATION, []), (INIT_DB, ["switch_off", "switch_on"])],  # init_db is every migration
    ids=["migration", "init_db"],
)
def test_the_audit_check_keeps_every_kind_and_adds_pause_and_resume(sql, later):
    found = kinds_in(sql, "CHECK (kind IN (")
    assert sorted(found) == sorted(KINDS_SINCE_010 + later)


def test_the_code_can_write_the_new_kinds():
    assert "audience_pause" in AUDIT_KINDS and "audience_resume" in AUDIT_KINDS


def test_the_migration_does_not_touch_the_agent_required_rule():
    assert "audit_events_agent_required" not in MIGRATION


@pytest.mark.parametrize("sql", [MIGRATION, INIT_DB], ids=["migration", "init_db"])
def test_the_pause_table_is_tenant_scoped_forced_rls_and_never_deleted_by_the_app(sql):
    assert "CREATE TABLE IF NOT EXISTS audience_pauses" in sql
    assert "tenant_id" in sql and "resumed_by" in sql and "resumed_at" in sql
    assert "ALTER TABLE audience_pauses ENABLE ROW LEVEL SECURITY" in sql
    assert "ALTER TABLE audience_pauses FORCE ROW LEVEL SECURITY" in sql
    assert "CREATE POLICY tenant_isolation ON audience_pauses" in sql
    assert "USING (tenant_id = current_setting('app.tenant_id', true))" in sql
    assert "WITH CHECK (tenant_id = current_setting('app.tenant_id', true))" in sql
    assert "REVOKE ALL ON audience_pauses FROM agentos_app" in sql
    assert "GRANT SELECT, INSERT, UPDATE ON audience_pauses TO agentos_app" in sql
    assert "DELETE ON audience_pauses" not in sql
    assert "WHERE resumed_at IS NULL" in sql


def test_init_db_matches_the_migration_table_block():
    block = MIGRATION[MIGRATION.index("CREATE TABLE IF NOT EXISTS audience_pauses") :]
    block = block[: block.index("ALTER TABLE audit_events")].rstrip()
    assert block in INIT_DB


def test_the_audit_and_notice_keys_start_with_the_tenant():
    assert ids.audit_audience_pause_key("acme", "a", "C1", "t").startswith("acme:")
    assert ids.audience_pause_notice_key("acme", "a", "C1", "t", "U1").startswith("acme:")
    assert ids.audience_pause_notice_key("acme", "a", "C1", "t", "U1") != (
        ids.audience_pause_notice_key("acme", "a", "C1", "t", "U2")
    )


def test_nothing_committed_names_the_planning_folder():
    for name in ("audience_pauses.py", "audience_watch.py"):
        text = (ROOT_DIR / "src" / "agentos" / name).read_text()
        assert ".agents/planning" not in text and "AU-05" not in text
    assert ".agents/planning" not in MIGRATION and "AU-05" not in MIGRATION
