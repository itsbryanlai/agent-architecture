"""Agent audiences: who may use an agent, where, and the checks around publishing it."""

import copy
from pathlib import Path

import httpx
import pytest
from temporalio import workflow
from temporalio.client import WorkflowHistory
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Replayer

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_store import InMemoryAgentStore, seeded_agent_store
from agentos.audience import (
    AudienceChecker,
    AudienceCheckUnavailable,
    AudienceNotConfirmed,
    ChannelOutsideAudience,
    SlackAudienceInspector,
    effective_audience,
    refusal_text,
    unavailable_text,
)
from agentos.audit import InMemoryAuditStore
from agentos.config import (
    AgentVersion,
    Audience,
    ConfigError,
    Settings,
    TenantsConfig,
    agent_definition,
    parse_agent,
    parse_agents_config,
)
from agentos.control import ControlPlane
from agentos.control_api import create_app
from agentos.grant_checker import NotionGrantChecker
from agentos.llm import ScriptedLLMClient
from agentos.members import InMemoryMemberStore
from agentos.models import (
    LLMResponse,
    ToolCall,
    TurnContext,
)
from agentos.slack_client import FakeSlack, SlackLookupError, SlackUser
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for
from agentos.workflows.conversation import PATCH_HANDOFF_REFUSAL_AGENT, ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import (
    ALICE,
    BOB,
    TEAM,
    TENANT,
    agents_dict,
    eventually,
    make_fake_notion,
)
from tests.helpers.agents import republish
from tests.helpers.audience import CHAN, THREAD, converse, running, say, tenant_dict
from tests.helpers.console_harness import ROOT as CONSOLE_ADMIN
from tests.helpers.console_harness import ConsoleHarness, csrf_token, fake_slack_for, post, using
from tests.helpers.control_api import JSON, StandIn

ROOT = Path(__file__).resolve().parents[2]
OTHER_CHAN = "C0BBBB222"
GROUP = "S0GROUP01"


def helper_block(**audience) -> dict:
    block = copy.deepcopy(agents_dict()["agents"]["it-helper"])
    if audience:
        block["audience"] = audience
    return block


# --- parsing and the legacy default ---------------------------------------------------


def test_a_definition_without_audience_has_none_and_dumps_none():
    agent = parse_agent("it-helper", helper_block(), 1)
    assert agent.audience is None
    assert "audience" not in agent_definition(agent)


def test_the_documented_audience_parses():
    agent = parse_agent(
        "it-helper",
        helper_block(channels=[CHAN], dm={"user_groups": [GROUP]}, guests=True),
        1,
    )
    assert agent.audience == Audience(
        channels=[CHAN], dm_everyone=False, dm_user_groups=[GROUP], guests=True
    )


def test_dm_everyone_parses_and_guests_default_to_false():
    agent = parse_agent("it-helper", helper_block(channels=[CHAN], dm="everyone"), 1)
    assert agent.audience == Audience(channels=[CHAN], dm_everyone=True)
    assert agent.audience.guests is False


@pytest.mark.parametrize(
    "audience",
    [
        {"channels": [CHAN], "dm": "everyone", "guests": True},
        {"channels": [CHAN, OTHER_CHAN], "dm": {"user_groups": [GROUP]}},
        {"channels": [CHAN]},
        {"dm": "everyone"},
    ],
)
def test_an_audience_round_trips_through_the_definition(audience):
    agent = parse_agent("it-helper", helper_block(**audience), 3)
    assert parse_agent("it-helper", agent_definition(agent), 3) == agent


@pytest.mark.parametrize(
    "audience, message",
    [
        ({}, "reaches nobody"),
        (["C0AAAA111"], "audience"),
        ({"channels": []}, "reaches nobody"),
        ({"channels": "C0AAAA111"}, "channels"),
        ({"channels": ["general"]}, "Slack channel ID"),
        ({"channels": [CHAN, CHAN]}, "more than once"),
        ({"channels": [CHAN], "dm": "all"}, "dm"),
        ({"channels": [CHAN], "dm": {"user_groups": []}}, "user_groups"),
        ({"channels": [CHAN], "dm": {"user_groups": ["finance"]}}, "user group ID"),
        ({"channels": [CHAN], "dm": {"users": ["U1"]}}, "unknown"),
        ({"channels": [CHAN], "guests": "yes"}, "guests"),
        ({"channels": [CHAN], "everyone": True}, "unknown"),
    ],
)
def test_a_malformed_audience_is_refused(audience, message):
    """Fails if a definition whose audience is empty, not a mapping or malformed is accepted."""
    block = helper_block()
    block["audience"] = audience
    with pytest.raises(ConfigError, match=message):
        parse_agent("it-helper", block, 1)


def test_a_channel_mapped_to_an_agent_must_be_in_its_audience():
    raw = agents_dict()
    raw["agents"]["it-helper"]["audience"] = {"channels": [CHAN], "dm": "everyone"}
    raw["channels"] = {OTHER_CHAN: "it-helper"}
    with pytest.raises(ConfigError, match=rf"{OTHER_CHAN}.*it-helper.*audience"):
        parse_agents_config(raw)
    raw["channels"] = {CHAN: "it-helper"}
    assert parse_agents_config(raw).channels == {CHAN: "it-helper"}


def test_a_mapped_channel_of_an_agent_without_audience_is_fine():
    raw = agents_dict()
    raw["channels"] = {OTHER_CHAN: "it-helper"}
    assert parse_agents_config(raw).channels == {OTHER_CHAN: "it-helper"}


# --- the Slack lookups ----------------------------------------------------------------


# --- the rules ------------------------------------------------------------------------


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class World:
    def __init__(self, raw: dict, slack: FakeSlack | None = None):
        self.config = parse_agents_config(raw)
        self.slack = slack or FakeSlack()
        self.store = seeded_agent_store(self.config)
        self.clock = Clock()
        registry = TenantRegistry(
            {TENANT: runtime_for(self.config, self.slack, make_fake_notion())}
        )
        self.checker = AudienceChecker(registry, self.store, clock=self.clock)

    async def agent(self, agent_id: str = "it-helper") -> AgentVersion:
        return await self.store.published(TENANT, agent_id)

    async def decide(self, user: str, channel: str, kind: str, agent_id: str = "it-helper"):
        return await self.checker.decide(TENANT, await self.agent(agent_id), user, channel, kind)


GUEST = SlackUser("U_GUEST", TEAM, is_restricted=True)
SINGLE_GUEST = SlackUser("U_SINGLE", TEAM, is_ultra_restricted=True)
EXTERNAL = SlackUser("U_EXT", "T0OTHER")
GONE = SlackUser("U_GONE", TEAM, deleted=True)
ALL_ODD = {u.id: u for u in (GUEST, SINGLE_GUEST, EXTERNAL, GONE)}


async def test_a_channel_in_the_audience_admits_a_full_member_and_another_channel_does_not():
    w = World(tenant_dict({"channels": [CHAN]}))
    assert (await w.decide(ALICE, CHAN, "channel")).allowed
    refused = await w.decide(ALICE, OTHER_CHAN, "channel")
    assert (refused.allowed, refused.reason) == (False, "channel")


async def test_a_direct_message_needs_dm_in_the_audience():
    w = World(tenant_dict({"channels": [CHAN]}))
    refused = await w.decide(ALICE, "D1", "im")
    assert (refused.allowed, refused.reason) == (False, "dm")
    w = World(tenant_dict({"channels": [CHAN], "dm": "everyone"}))
    assert (await w.decide(ALICE, "D1", "im")).allowed


@pytest.mark.parametrize("kind, channel", [("im", "D1"), ("channel", CHAN)])
async def test_guests_are_refused_unless_the_audience_allows_them(kind, channel):
    w = World(tenant_dict({"channels": [CHAN], "dm": "everyone"}), FakeSlack(users=ALL_ODD))
    for guest in ("U_GUEST", "U_SINGLE"):
        refused = await w.decide(guest, channel, kind)
        assert (refused.allowed, refused.reason) == (False, "guest")
    w = World(
        tenant_dict({"channels": [CHAN], "dm": "everyone", "guests": True}),
        FakeSlack(users=ALL_ODD),
    )
    for guest in ("U_GUEST", "U_SINGLE"):
        assert (await w.decide(guest, channel, kind)).allowed


@pytest.mark.parametrize("kind, channel", [("im", "D1"), ("channel", CHAN)])
async def test_a_slack_connect_user_is_never_admitted_even_with_guests_allowed(kind, channel):
    w = World(
        tenant_dict({"channels": [CHAN], "dm": "everyone", "guests": True}),
        FakeSlack(users=ALL_ODD),
    )
    refused = await w.decide("U_EXT", channel, kind)
    assert (refused.allowed, refused.reason) == (False, "external")


async def test_a_deactivated_user_is_refused():
    w = World(tenant_dict({"channels": [CHAN], "dm": "everyone"}), FakeSlack(users=ALL_ODD))
    refused = await w.decide("U_GONE", CHAN, "channel")
    assert (refused.allowed, refused.reason) == (False, "deactivated")


async def test_a_user_group_admits_its_members_in_a_direct_message_only():
    slack = FakeSlack(groups={GROUP: [ALICE]}, users=ALL_ODD)
    w = World(tenant_dict({"channels": [CHAN], "dm": {"user_groups": [GROUP]}}), slack)
    assert (await w.decide(ALICE, "D1", "im")).allowed
    refused = await w.decide(BOB, "D1", "im")
    assert (refused.allowed, refused.reason) == (False, "dm")
    assert (await w.decide(BOB, CHAN, "channel")).allowed


async def test_a_guest_in_a_user_group_still_needs_guests_allowed():
    slack = FakeSlack(groups={GROUP: ["U_GUEST"]}, users=ALL_ODD)
    w = World(tenant_dict({"dm": {"user_groups": [GROUP]}}), slack)
    assert (await w.decide("U_GUEST", "D1", "im")).reason == "guest"
    w = World(tenant_dict({"dm": {"user_groups": [GROUP]}, "guests": True}), slack)
    assert (await w.decide("U_GUEST", "D1", "im")).allowed


# --- the legacy default ---------------------------------------------------------------


async def test_no_audience_admits_full_members_by_direct_message_and_refuses_guests():
    w = World(tenant_dict(), FakeSlack(users=ALL_ODD))
    assert (await w.decide(ALICE, "D1", "im")).allowed
    for odd, reason in (("U_GUEST", "guest"), ("U_EXT", "external"), ("U_GONE", "deactivated")):
        assert (await w.decide(odd, "D1", "im")).reason == reason


async def test_no_audience_answers_in_the_channels_the_tenant_file_maps_to_the_agent():
    raw = tenant_dict(channels={CHAN: "it-helper"}, front_door=True)
    w = World(raw)
    assert (await w.decide(ALICE, CHAN, "channel")).allowed
    # An unmapped channel goes to the default agent (the front door), not the helper
    # directly, but the helper is the door's specialist, so the thread may reach it.
    assert (await w.decide(ALICE, OTHER_CHAN, "channel", "concierge")).allowed
    assert (await w.decide(ALICE, OTHER_CHAN, "channel", "it-helper")).allowed


async def test_no_audience_agent_that_the_file_does_not_route_to_is_refused_in_channels():
    raw = tenant_dict(channels={CHAN: "it-helper"})
    raw["agents"]["other"] = copy.deepcopy(raw["agents"]["it-helper"])
    w = World(raw)
    refused = await w.decide(ALICE, CHAN, "channel", "other")
    assert (refused.allowed, refused.reason) == (False, "channel")


async def test_a_default_agent_without_audience_answers_in_channels_the_file_does_not_map():
    w = World(tenant_dict(channels={CHAN: "it-helper"}))
    assert (await w.decide(ALICE, OTHER_CHAN, "channel")).allowed


async def test_a_specialist_with_its_own_audience_does_not_inherit_the_front_door_channels():
    raw = tenant_dict(front_door=True, specialist_audience={"channels": [CHAN]})
    w = World(raw)
    assert (await w.decide(ALICE, CHAN, "channel", "it-helper")).allowed
    assert (await w.decide(ALICE, OTHER_CHAN, "channel", "it-helper")).reason == "channel"


async def test_the_effective_audience_names_what_the_legacy_default_means():
    raw = tenant_dict(channels={CHAN: "it-helper", OTHER_CHAN: "concierge"}, front_door=True)
    config = parse_agents_config(raw)
    store = seeded_agent_store(config)
    published = await store.published_all(TENANT)
    effective = effective_audience(published["it-helper"], config, published)
    assert effective.explicit is False
    assert effective.channels == (CHAN, OTHER_CHAN)  # its own, and its front door's
    assert effective.dm_everyone and not effective.guests
    assert effective.includes_channel(CHAN) and effective.includes_channel(OTHER_CHAN)
    # The front door is the default, so its specialist follows it into unmapped channels.
    assert effective.includes_channel("C0UNMAPPED")
    door = effective_audience(published["concierge"], config, published)
    # The default agent also answers in channels the file does not map.
    assert door.any_unmapped_channel and door.includes_channel("C0UNMAPPED")


async def test_an_explicit_audience_is_the_effective_audience():
    config = parse_agents_config(tenant_dict({"channels": [CHAN], "dm": {"user_groups": [GROUP]}}))
    store = seeded_agent_store(config)
    published = await store.published_all(TENANT)
    effective = effective_audience(published["it-helper"], config, published)
    assert effective.explicit is True
    assert effective.channels == (CHAN,) and effective.dm_user_groups == (GROUP,)
    assert not effective.dm_everyone and not effective.any_unmapped_channel


# --- lookups: cache and failure -------------------------------------------------------


async def test_user_and_group_lookups_are_cached_for_a_short_time():
    slack = FakeSlack(groups={GROUP: [ALICE]})
    w = World(tenant_dict({"dm": {"user_groups": [GROUP]}}), slack)
    for _ in range(3):
        assert (await w.decide(ALICE, "D1", "im")).allowed
    assert [n for n, _ in slack.lookups()] == ["user_info", "usergroup_members"]
    w.clock.now += 301
    assert (await w.decide(ALICE, "D1", "im")).allowed
    assert [n for n, _ in slack.lookups()].count("user_info") == 2


async def test_the_cache_is_per_tenant_and_a_removed_member_is_refused_after_the_ttl():
    slack = FakeSlack(groups={GROUP: [ALICE]})
    w = World(tenant_dict({"dm": {"user_groups": [GROUP]}}), slack)
    assert (await w.decide(ALICE, "D1", "im")).allowed
    slack.groups[GROUP] = []
    assert (await w.decide(ALICE, "D1", "im")).allowed  # still cached
    w.clock.now += 301
    assert (await w.decide(ALICE, "D1", "im")).reason == "dm"


async def test_a_failed_lookup_raises_and_is_not_cached():
    slack = FakeSlack()
    w = World(tenant_dict({"dm": "everyone"}), slack)
    slack.fail_lookups(SlackLookupError("ratelimited", transient=True))
    with pytest.raises(SlackLookupError):
        await w.decide(ALICE, "D1", "im")
    slack.fail_lookups(None)
    assert (await w.decide(ALICE, "D1", "im")).allowed


async def test_only_reachable_specialists_are_kept_and_a_failed_lookup_keeps_none():
    raw = tenant_dict(front_door=True, specialist_audience={"channels": [CHAN]})
    slack = FakeSlack(users=ALL_ODD)
    w = World(raw, slack)
    helper = await w.agent("it-helper")
    assert await w.checker.reachable(TENANT, [helper], ALICE, CHAN, "channel") == [helper]
    assert await w.checker.reachable(TENANT, [helper], ALICE, OTHER_CHAN, "channel") == []
    assert await w.checker.reachable(TENANT, [helper], "U_GUEST", CHAN, "channel") == []
    slack.fail_lookups(SlackLookupError("internal_error", transient=True))
    w.clock.now += 301
    assert await w.checker.reachable(TENANT, [helper], ALICE, CHAN, "channel") == []


# --- the turn-start check in the workflow --------------------------------------------


def reply(text: str) -> LLMResponse:
    return LLMResponse(text=text, tool_calls=[], stop_reason="end_turn")


def refusals(h) -> list[dict]:
    return [e for e in h.audit.events if e.kind == "audience_refused"]


async def test_a_refused_turn_posts_the_fixed_reply_and_runs_nothing_else():
    raw = tenant_dict({"channels": [CHAN], "dm": "everyone"})
    async with running(raw) as h:
        await converse(h, "it-helper", say("Ev1", ALICE, OTHER_CHAN, "channel", "secret question"))
        await eventually(lambda: h.slack.posts())

        (post,) = h.slack.posts()
        assert post["text"] == "IT helper isn't available to you here. Ask <@U_OWNER> for access."
        assert (post["channel"], post["thread_ts"]) == (OTHER_CHAN, THREAD)
        assert h.llm.calls == []
        assert h.slack.updates() == []
        conv = ids.conversation_id(TENANT, OTHER_CHAN, THREAD)
        assert await h.history.load(TENANT, conv) == []
        (event,) = refusals(h)
        assert (event.agent_id, event.agent_version, event.requester_user_id) == (
            "it-helper",
            1,
            ALICE,
        )
        assert (event.outcome, event.tool, event.target) == ("ok", "channel", None)
        assert "secret question" not in repr(event)
        assert event.input_sha256 is None and event.output_sha256 is None


async def test_a_refused_turn_calls_no_tool_even_if_the_model_would():
    raw = tenant_dict({"channels": [CHAN]})
    async with running(raw) as h:
        h.llm.queue(
            LLMResponse(
                text=None,
                tool_calls=[ToolCall("tu1", "search_items", {"query": "x"})],
                stop_reason="tool_use",
            )
        )
        await converse(h, "it-helper", say("Ev1", ALICE, "D1", "im"))
        await eventually(lambda: h.slack.posts())

        assert h.llm.calls == []
        assert [e.kind for e in h.audit.events] == ["audience_refused"]


async def test_a_refused_guest_is_told_and_the_next_message_is_checked_again():
    raw = tenant_dict({"channels": [CHAN], "dm": "everyone"})
    slack = FakeSlack(users=ALL_ODD)
    async with running(raw, slack) as h:
        h.llm.queue(reply("hello"))
        handle = await converse(h, "it-helper", say("Ev1", "U_GUEST", "D1", "im"))
        await eventually(lambda: h.slack.posts())
        assert h.llm.calls == []
        await handle.signal(ConversationWorkflow.message, say("Ev2", ALICE, "D1", "im"))
        await eventually(lambda: h.slack.updates())
        assert h.slack.updates()[0]["text"] == "hello"
        assert [e.tool for e in refusals(h)] == ["guest"]


async def test_an_allowed_turn_is_unchanged_and_audits_no_refusal():
    raw = tenant_dict({"channels": [CHAN], "dm": "everyone"})
    async with running(raw) as h:
        h.llm.queue(reply("Done"))
        await converse(h, "it-helper", say("Ev1", ALICE, CHAN, "channel"))
        await eventually(lambda: h.slack.updates())

        assert h.slack.posts()[0]["text"] == "Thinking…"
        assert h.slack.updates()[0]["text"] == "Done"
        assert len(h.llm.calls) == 1
        assert refusals(h) == []


async def test_a_transient_lookup_failure_is_retried_and_the_turn_goes_ahead():
    raw = tenant_dict({"dm": "everyone"})
    slack = FakeSlack()
    slack.fail_lookups(SlackLookupError("ratelimited", transient=True), times=1)
    async with running(raw, slack) as h:
        h.llm.queue(reply("Done"))
        await converse(h, "it-helper", say("Ev1", ALICE, "D1", "im"))
        await eventually(lambda: h.slack.updates())
        assert h.slack.updates()[0]["text"] == "Done"


async def test_a_lookup_that_cannot_be_had_refuses_the_turn_and_says_so():
    raw = tenant_dict({"dm": "everyone"})
    slack = FakeSlack()
    slack.fail_lookups(SlackLookupError("missing_scope", transient=False))
    async with running(raw, slack) as h:
        await converse(h, "it-helper", say("Ev1", ALICE, "D1", "im"))
        await eventually(lambda: h.slack.posts())

        (post,) = h.slack.posts()
        assert post["text"] == unavailable_text("IT helper")
        assert h.llm.calls == []
        (event,) = refusals(h)
        assert (event.outcome, event.tool) == ("error", "unavailable")


async def test_the_activity_retries_a_transient_failure_and_gives_up_on_a_permanent_one():
    config = parse_agents_config(tenant_dict({"dm": "everyone"}))
    slack = FakeSlack()
    acts = AgentActivities(
        settings=Settings(),
        registry=TenantRegistry({TENANT: runtime_for(config, slack, make_fake_notion())}),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=ScriptedLLMClient(),
        tool_outputs=InMemoryToolOutputStore(),
        audit=InMemoryAuditStore(),
        agents=seeded_agent_store(config),
    )
    ctx = TurnContext(TENANT, "it-helper", 1, ALICE, "D1", THREAD, "im")
    slack.fail_lookups(SlackLookupError("ratelimited", transient=True))
    with pytest.raises(SlackLookupError):
        await ActivityEnvironment().run(acts.check_audience, ctx)
    slack.fail_lookups(SlackLookupError("invalid_auth", transient=False))
    with pytest.raises(ApplicationError) as failed:
        await ActivityEnvironment().run(acts.check_audience, ctx)
    assert failed.value.non_retryable is True


def test_the_refusal_text_escapes_the_agent_name_and_falls_back_for_a_non_user_owner():
    assert refusal_text("A <b> & C", "U0123ABC") == (
        "A &lt;b&gt; &amp; C isn't available to you here. Ask <@U0123ABC> for access."
    )
    assert refusal_text("Helper", "import:yaml").endswith("Ask a workspace admin for access.")
    assert refusal_text("Helper", None).endswith("Ask a workspace admin for access.")
    assert refusal_text("Helper", "U1> <!channel").endswith("Ask a workspace admin for access.")


async def test_a_thread_recorded_with_the_audience_check_replays():
    raw = tenant_dict({"channels": [CHAN]})
    async with running(raw) as h:
        h.llm.queue(reply("Done"))
        handle = await converse(h, "it-helper", say("Ev1", ALICE, CHAN, "channel"))
        await eventually(lambda: h.slack.updates())
        history = await handle.fetch_history()
    assert "MARKER_RECORDED" in history.to_json()
    await Replayer(workflows=[ConversationWorkflow, TaskWorkflow]).replay_workflow(
        WorkflowHistory.from_json(history.workflow_id, history.to_json())
    )


# --- hand-off --------------------------------------------------------------------------


def front_door_raw(specialist_audience: dict | None) -> dict:
    raw = tenant_dict(front_door=True, specialist_audience=specialist_audience)
    raw["channels"] = {CHAN: "concierge"}
    return raw


def hand_off(call_id: str = "tu_h") -> LLMResponse:
    return LLMResponse(
        text=None,
        tool_calls=[ToolCall(call_id, "hand_off", {"agent": "it-helper"})],
        stop_reason="tool_use",
    )


def tool_names(call: dict) -> list[str]:
    return [t["name"] for t in call["tools"]]


async def test_the_hand_off_tool_lists_only_specialists_the_asker_can_reach():
    async with running(front_door_raw({"channels": [CHAN]})) as h:
        h.llm.queue(reply("a"))
        await converse(h, "concierge", say("Ev1", ALICE, CHAN, "channel"))
        await eventually(lambda: h.llm.calls)
        assert "hand_off" in tool_names(h.llm.calls[0])


async def test_no_hand_off_tool_when_no_specialist_is_reachable():
    # The helper is open in CHAN only; the front door is asked in a DM.
    raw = front_door_raw({"channels": [CHAN]})
    raw["channels"] = {}
    raw["agents"]["concierge"]["audience"] = {"dm": "everyone"}
    async with running(raw) as h:
        h.llm.queue(reply("a"))
        await converse(h, "concierge", say("Ev1", ALICE, "D1", "im"))
        await eventually(lambda: h.llm.calls)
        assert "hand_off" not in tool_names(h.llm.calls[0])


async def test_a_forged_hand_off_to_an_unreachable_specialist_is_refused_without_switching():
    """Fails if a refused hand-off names the front door instead of the specialist."""
    raw = front_door_raw({"channels": [CHAN]})
    raw["channels"] = {}
    raw["agents"]["concierge"]["audience"] = {"dm": "everyone"}
    async with running(raw) as h:
        specialist = await republish(
            h.agent_store, "it-helper", lambda definition: definition.update(description="IT v2.")
        )
        h.llm.queue(hand_off(), reply("I'll handle it myself"))
        handle = await converse(h, "concierge", say("Ev1", ALICE, "D1", "im"))
        await eventually(lambda: h.slack.updates())

        assert h.slack.updates()[-1]["text"] == "I'll handle it myself"
        assert not [u for u in h.slack.updates() if "picking this up" in u["text"]]
        second = h.llm.calls[1]["messages"][-1]["content"][0]
        assert second["is_error"] is True and "not available" in second["content"]
        assert [e.kind for e in h.audit.events].count("hand_off") == 0
        (event,) = refusals(h)
        assert (event.agent_id, event.agent_version, event.target, event.tool) == (
            "it-helper",
            specialist.version,
            "concierge",
            "dm",
        )
        # The thread stays with the front door for the next message too.
        h.llm.queue(reply("still me"))
        await handle.signal(ConversationWorkflow.message, say("Ev2", ALICE, "D1", "im"))
        await eventually(lambda: len(h.llm.calls) >= 3)
        assert h.llm.calls[2]["system"] == h.llm.calls[0]["system"]


async def test_a_pre_patch_refused_hand_off_still_names_the_front_door(monkeypatch):
    """Fails if the legacy refused hand-off stops naming the front door, target the specialist."""
    raw = front_door_raw({"channels": [CHAN]})
    raw["channels"] = {}
    raw["agents"]["concierge"]["audience"] = {"dm": "everyone"}
    real_patched = workflow.patched

    def legacy_patched(patch_id: str) -> bool:
        if patch_id == PATCH_HANDOFF_REFUSAL_AGENT:
            return False
        return real_patched(patch_id)

    monkeypatch.setattr(workflow, "patched", legacy_patched)
    async with running(raw) as h:
        h.llm.queue(hand_off(), reply("I'll handle it myself"))
        await converse(h, "concierge", say("Ev1", ALICE, "D1", "im"))
        await eventually(lambda: h.slack.updates())

        (event,) = refusals(h)
        assert (event.agent_id, event.target, event.tool) == ("concierge", "it-helper", "dm")


async def test_a_hand_off_to_a_reachable_specialist_still_switches():
    async with running(front_door_raw({"channels": [CHAN]})) as h:
        h.llm.queue(hand_off(), reply("Specialist answer"))
        await converse(h, "concierge", say("Ev1", ALICE, CHAN, "channel"))
        await eventually(lambda: len(h.slack.updates()) >= 2)
        assert "picking this up" in h.slack.updates()[0]["text"]
        assert h.slack.updates()[-1]["text"] == "Specialist answer"
        assert refusals(h) == []


# --- publishing: mapped channels, confirmation, the audit event -----------------------


ADMIN = "U_ROOT"
CREATOR = "U_CREATOR"
MEMBER_IDS = ["U_M1", "U_M2", "U_M3", "U_GUEST", "U_EXT"]


def publish_world(channels=None, slack=None):
    raw = agents_dict()
    raw["admins"] = [ADMIN]
    if channels:
        raw["channels"] = channels
    tenants = TenantsConfig.of(parse_agents_config(raw))
    slack = slack or FakeSlack(
        users={u.id: u for u in (GUEST, EXTERNAL)},
        channel_members={CHAN: ["U_M1", "U_M2", "U_GUEST", "U_EXT", "U_GONE"]},
        groups={GROUP: ["U_M1", "U_M3"]},
    )
    slack.users["U_GONE"] = GONE
    audit = InMemoryAuditStore()
    agents = InMemoryAgentStore()
    members = InMemoryMemberStore()
    inspector = SlackAudienceInspector(fake_slack_for(slack), lambda t: TEAM, clock=Clock())
    plane = ControlPlane(
        agents,
        members,
        audit,
        tenants,
        NotionGrantChecker(lambda t: make_fake_notion()),
        inspector,
    )
    return plane, agents, audit, slack, inspector


async def pending(plane, **audience):
    block = helper_block(**audience)
    block.pop("follow_up", None)
    await plane.add_agent(TENANT, ADMIN, "it-helper", block)
    await plane.submit_version(TENANT, ADMIN, "it-helper", 1)


async def test_publish_refuses_a_mapped_channel_outside_the_audience():
    plane, agents, audit, *_ = publish_world(channels={OTHER_CHAN: "it-helper"})
    await pending(plane, channels=[CHAN], dm="everyone")
    with pytest.raises(ChannelOutsideAudience, match=OTHER_CHAN):
        await plane.publish_version(TENANT, ADMIN, "it-helper", 1)
    assert (await agents.get_version(TENANT, "it-helper", 1)).status == "pending"
    assert audit.events[-1].kind == "agent_submit"


async def test_the_summary_counts_people_per_place_and_overall():
    *_, inspector = publish_world()
    config = parse_agents_config(tenant_dict({"channels": [CHAN], "dm": "everyone"}))
    store = seeded_agent_store(config)
    published = await store.published_all(TENANT)
    effective = effective_audience(published["it-helper"], config, published)
    summary = await inspector.summary(TENANT, effective)
    by_place = {p.place: (p.total, p.guests, p.external) for p in summary.places}
    # The channel: two members, a guest and an external person; the deactivated one is nobody.
    assert by_place[CHAN] == (4, 1, 1)
    # Direct messages: the active full members of the workspace. Guests are not "everyone",
    # and a Slack Connect person is never reached by direct message.
    assert by_place["dm"] == (3, 0, 0)
    # Distinct people over both places.
    assert summary.confirmation() == {"total": 5, "guests": 1, "external": 1}


async def test_the_summary_counts_a_user_group_by_its_members():
    *_, inspector = publish_world()
    config = parse_agents_config(tenant_dict({"dm": {"user_groups": [GROUP]}}))
    store = seeded_agent_store(config)
    published = await store.published_all(TENANT)
    effective = effective_audience(published["it-helper"], config, published)
    summary = await inspector.summary(TENANT, effective)
    assert [(p.place, p.total) for p in summary.places] == [("dm", 2)]


async def test_a_page_view_may_reuse_the_workspace_list_but_a_publish_reads_it_again():
    *_, slack, inspector = publish_world()
    config = parse_agents_config(tenant_dict({"dm": "everyone"}))
    store = seeded_agent_store(config)
    published = await store.published_all(TENANT)
    effective = effective_audience(published["it-helper"], config, published)
    await inspector.summary(TENANT, effective, fresh=False)
    await inspector.summary(TENANT, effective, fresh=False)
    assert [n for n, _ in slack.lookups()].count("list_users") == 1
    await inspector.summary(TENANT, effective, fresh=True)
    assert [n for n, _ in slack.lookups()].count("list_users") == 2


@pytest.mark.parametrize(
    "audience, counts",
    [({"channels": [CHAN]}, "4 people"), ({}, "people")],
    ids=["with an audience", "without an audience"],
)
async def test_publish_needs_the_confirmation_and_names_the_counts(audience, counts):
    """Fails if a version is published without the audience confirmation, with or without an audience set."""
    plane, agents, audit, *_ = publish_world()
    await pending(plane, **audience)
    with pytest.raises(AudienceNotConfirmed, match=counts) as err:
        await plane.publish_version(TENANT, ADMIN, "it-helper", 1)
    assert "confirm_audience" in str(err.value)
    assert (await agents.get_version(TENANT, "it-helper", 1)).status == "pending"
    assert "audience_confirm" not in [e.kind for e in audit.events]


async def test_publish_with_the_right_counts_publishes_and_audits_the_confirmation():
    plane, _agents, audit, *_ = publish_world()
    await pending(plane, channels=[CHAN])
    counts = {"total": 4, "guests": 1, "external": 1}
    record = await plane.publish_version(
        TENANT, ADMIN, "it-helper", 1, request_id="r1", confirm_audience=counts
    )
    assert record.status == "published"
    events = {e.kind: e for e in audit.events}
    confirm = events["audience_confirm"]
    assert (confirm.agent_id, confirm.agent_version) == ("it-helper", 1)
    assert (confirm.approver_user_id, confirm.requester_user_id) == (ADMIN, ADMIN)
    assert confirm.target == "total=4 guests=1 external=1"
    assert confirm.input_sha256 == events["agent_publish"].input_sha256


async def test_publish_with_stale_counts_is_refused():
    plane, agents, _audit, slack, _ = publish_world()
    await pending(plane, channels=[CHAN])
    slack.members_of[CHAN].append("U_M3")  # someone joined after the approver looked
    with pytest.raises(AudienceNotConfirmed, match="changed"):
        await plane.publish_version(
            TENANT, ADMIN, "it-helper", 1, confirm_audience={"total": 4, "guests": 1, "external": 1}
        )
    assert (await agents.get_version(TENANT, "it-helper", 1)).status == "pending"


async def test_publish_when_slack_cannot_be_read_is_refused_and_nothing_changes():
    plane, agents, _audit, slack, _ = publish_world()
    await pending(plane, channels=[CHAN])
    slack.fail_lookups(SlackLookupError("ratelimited", transient=True))
    with pytest.raises(AudienceCheckUnavailable, match="ratelimited"):
        await plane.publish_version(
            TENANT, ADMIN, "it-helper", 1, confirm_audience={"total": 4, "guests": 1, "external": 1}
        )
    assert (await agents.get_version(TENANT, "it-helper", 1)).status == "pending"


# --- the API and the console ---------------------------------------------------------


COUNTS = {"total": 4, "guests": 1, "external": 1}
API = f"/tenants/{TENANT}"


ACK = {"acknowledge_widening": True, "reviewed_published": "none"}
ACK_FORM = {"widening": "yes", "reviewed_published": "none"}


async def sha_of(agents, version: int = 1) -> str:
    """The hash an admin who opened the version just now was shown."""
    return (await agents.get_version(TENANT, "it-helper", version)).definition_sha256


def api_world(channels=None):
    plane, agents, audit, slack, _inspector = publish_world(channels)
    app = create_app(plane, plane._tenants, StandIn(ADMIN))
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    client = httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8100", headers=JSON)
    return plane, agents, audit, slack, client


async def test_api_publish_without_confirmation_is_409_and_with_it_is_200():
    plane, _agents, audit, _, client = api_world()
    await pending(plane, channels=[CHAN])
    sha = await sha_of(_agents)
    async with client as c:
        refused = await c.post(
            f"{API}/agents/it-helper/versions/1/publish",
            json={**ACK, "definition_sha256": await sha_of(_agents)},
        )
        assert refused.status_code == 409
        assert "confirm_audience" in refused.json()["error"]
        stale = await c.post(
            f"{API}/agents/it-helper/versions/1/publish",
            json={"confirm_audience": {**COUNTS, "total": 3}, **ACK, "definition_sha256": sha},
        )
        assert stale.status_code == 409 and "changed" in stale.json()["error"]
        done = await c.post(
            f"{API}/agents/it-helper/versions/1/publish",
            json={"confirm_audience": COUNTS, **ACK, "definition_sha256": sha},
        )
        assert done.status_code == 200 and done.json()["status"] == "published"
    assert "audience_confirm" in [e.kind for e in audit.events]


async def test_api_publish_of_a_channel_outside_the_audience_is_409():
    plane, agents, _, _, client = api_world(channels={OTHER_CHAN: "it-helper"})
    await pending(plane, channels=[CHAN])
    sha = await sha_of(agents)
    async with client as c:
        refused = await c.post(
            f"{API}/agents/it-helper/versions/1/publish",
            json={"confirm_audience": COUNTS, **ACK, "definition_sha256": sha},
        )
    assert refused.status_code == 409 and OTHER_CHAN in refused.json()["error"]


async def test_api_publish_is_502_when_slack_cannot_be_read():
    plane, agents, _, slack, client = api_world()
    await pending(plane, channels=[CHAN])
    slack.fail_lookups(SlackLookupError("internal_error", transient=True))
    async with client as c:
        refused = await c.post(
            f"{API}/agents/it-helper/versions/1/publish",
            json={"confirm_audience": COUNTS, **ACK, "definition_sha256": await sha_of(agents)},
        )
    assert refused.status_code == 502


def console_world(slack=None):
    slack = slack or FakeSlack(
        users={u.id: u for u in (GUEST, EXTERNAL)},
        channel_members={CHAN: ["U_M1", "U_M2", "U_GUEST", "U_EXT"]},
    )
    inspector = SlackAudienceInspector(fake_slack_for(slack), lambda t: TEAM, clock=Clock())
    return ConsoleHarness(audience=inspector), slack


async def console_pending(h, name="IT helper", **audience):
    block = helper_block(**audience)
    block["name"] = name
    await h.plane.add_agent(TENANT, CONSOLE_ADMIN, "it-helper", block)
    await h.plane.submit_version(TENANT, CONSOLE_ADMIN, "it-helper", 1)


VERSION_URL = f"/ui/{TENANT}/agents/it-helper/versions/1"


async def test_the_version_page_shows_the_audience_and_the_people_it_reaches():
    """Fails if the version page leaves out the audience, the people counts or the counts the confirm form posts."""
    h, _ = console_world()
    await console_pending(h, channels=[CHAN], dm="everyone")
    c = await h.signed_in(CONSOLE_ADMIN)
    async with using(c):
        page = (await c.get(VERSION_URL)).text
    assert "Audience" in page and CHAN in page
    assert "default (no audience set)" not in page
    assert "4 people" in page and "1 guest" in page and "1 from another workspace" in page
    assert 'name="confirm"' in page
    for field in ("guests", "external"):
        assert f'name="{field}" value="{COUNTS[field]}"' in page


async def test_a_definition_without_audience_is_marked_as_the_default():
    h, _ = console_world()
    h.tenants.tenants[TENANT].channels["C0MAPPED1"] = "it-helper"
    await console_pending(h)
    c = await h.signed_in(CONSOLE_ADMIN)
    async with using(c):
        page = (await c.get(VERSION_URL)).text
    assert "default (no audience set)" in page
    assert "C0MAPPED1" in page
    assert "Everyone" in page


async def test_the_audience_section_escapes_names_and_ids():
    h, _ = console_world()
    h.tenants.tenants[TENANT].channels["C<script>x</script>"] = "it-helper"
    await console_pending(h, name="<b>Helper</b>")
    c = await h.signed_in(CONSOLE_ADMIN)
    async with using(c):
        page = (await c.get(VERSION_URL)).text
    assert "<script>x" not in page and "<b>Helper</b>" not in page
    assert "C&lt;script&gt;x&lt;/script&gt;" in page


async def test_the_page_still_renders_when_slack_cannot_be_counted():
    slack = FakeSlack()
    slack.fail_lookups(SlackLookupError("missing_scope"))
    h, _ = console_world(slack)
    await console_pending(h, channels=[CHAN])
    c = await h.signed_in(CONSOLE_ADMIN)
    async with using(c):
        response = await c.get(VERSION_URL)
    assert response.status_code == 200
    assert "Could not count" in response.text and "missing_scope" in response.text
    assert 'name="confirm"' not in response.text


async def test_the_console_publishes_only_with_the_box_ticked_and_current_counts():
    h, _slack = console_world()
    await console_pending(h, channels=[CHAN])
    c = await h.signed_in(CONSOLE_ADMIN)
    async with using(c):
        token = await csrf_token(c)
        unticked = await post(
            c, f"{VERSION_URL}/publish", token, **ACK_FORM, definition_sha256=await sha_of(h.agents)
        )
        assert unticked.status_code == 409 and "confirm" in unticked.text
        stale = await post(
            c,
            f"{VERSION_URL}/publish",
            token,
            **ACK_FORM,
            definition_sha256=await sha_of(h.agents),
            confirm="yes",
            total="3",
            guests="1",
            external="1",
        )
        assert stale.status_code == 409 and "changed" in stale.text
        forged = await post(
            c,
            f"{VERSION_URL}/publish",
            token,
            **ACK_FORM,
            definition_sha256=await sha_of(h.agents),
            confirm="yes",
            total="x",
            guests="1",
            external="1",
        )
        assert forged.status_code == 409
        assert (await h.agents.get_version(TENANT, "it-helper", 1)).status == "pending"
        done = await post(
            c,
            f"{VERSION_URL}/publish",
            token,
            **ACK_FORM,
            definition_sha256=await sha_of(h.agents),
            confirm="yes",
            total="4",
            guests="1",
            external="1",
        )
        assert done.status_code == 303
    assert (await h.agents.get_version(TENANT, "it-helper", 1)).status == "published"
    confirm = next(e for e in h.audit.events if e.kind == "audience_confirm")
    assert confirm.approver_user_id == CONSOLE_ADMIN
    assert confirm.target == "total=4 guests=1 external=1"


async def test_the_console_shows_a_publish_refused_for_a_mapped_channel():
    h, _ = console_world()
    h.tenants.tenants[TENANT].channels[OTHER_CHAN] = "it-helper"
    await console_pending(h, channels=[CHAN])
    c = await h.signed_in(CONSOLE_ADMIN)
    async with using(c):
        token = await csrf_token(c)
        refused = await post(
            c,
            f"{VERSION_URL}/publish",
            token,
            **ACK_FORM,
            definition_sha256=await sha_of(h.agents),
            confirm="yes",
            total="4",
            guests="1",
            external="1",
        )
    assert refused.status_code == 409 and OTHER_CHAN in refused.text


@pytest.mark.parametrize("surface", ["console publish", "console resume", "api publish"])
async def test_a_missing_confirmation_reads_plainly_in_the_console_and_as_json_in_the_api(surface):
    """Fails if the console shows the API's confirm_audience wording instead of a sentence a
    person acts on, or the API answer loses its machine-readable form."""
    if surface == "api publish":
        plane, agents, _audit, _, client = api_world()
        await pending(plane, channels=[CHAN])
        async with client as c:
            refused = await c.post(
                f"{API}/agents/it-helper/versions/1/publish",
                json={**ACK, "definition_sha256": await sha_of(agents)},
            )
        assert refused.status_code == 409
        assert "confirm_audience" in refused.json()["error"]
        return
    h, _ = console_world()
    await console_pending(h, channels=[CHAN])
    if surface == "console resume":
        await h.pauses.open_pause(TENANT, "it-helper", CHAN, "channel_shared", h.clock())
        path = f"/ui/{TENANT}/agents/it-helper/channels/{CHAN}/resume"
        box = "I confirm these people may see what this agent says here"
    else:
        path = f"{VERSION_URL}/publish"
        box = "I confirm these people may see what this agent can access"
    c = await h.signed_in(CONSOLE_ADMIN)
    async with using(c):
        fields = (
            {}
            if surface == "console resume"
            else {
                **ACK_FORM,
                "definition_sha256": await sha_of(h.agents),
            }
        )
        refused = await post(c, path, await csrf_token(c), **fields)
    assert refused.status_code == 409
    text = refused.text.replace("&#39;", "'")
    assert f"Tick '{box}' (4 people: 1 guests, 1 from other workspaces)" in text
    assert "confirm_audience" not in text


# --- startup ----------------------------------------------------------------------------


async def test_startup_refuses_a_mapped_channel_outside_the_published_audience():
    from agentos.agent_store import startup_agent_failure

    raw = agents_dict()
    raw["channels"] = {OTHER_CHAN: "it-helper"}
    config = parse_agents_config(raw)
    store = seeded_agent_store(config)
    tenants = TenantsConfig.of(config)
    assert await startup_agent_failure(store, tenants) is None
    narrower = copy.deepcopy(agent_definition(config.agents["it-helper"]))
    narrower["audience"] = {"channels": [CHAN], "dm": "everyone"}
    store.set_published_unchecked(TENANT, "it-helper", narrower)
    failure = await startup_agent_failure(store, tenants)
    assert failure is not None
    assert f"tenant '{TENANT}'" in failure and OTHER_CHAN in failure and "it-helper" in failure
