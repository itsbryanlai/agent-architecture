"""Usage and cost: provider mapping, pricing, recording, aggregation and the usage page.

Fakes only: provider-shaped response objects, in-memory stores, a fake Slack sign-in.
"""

import copy
import csv
import dataclasses
import io
import logging
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos import usage as usage_module
from agentos.activities import AgentActivities
from agentos.agent_store import InMemoryAgentStore, seeded_agent_store
from agentos.audit import InMemoryAuditStore
from agentos.config import (
    ConfigError,
    Settings,
    TenantsConfig,
    agent_definition,
    load_settings,
    parse_agent,
    parse_agents_config,
)
from agentos.connectors.notion import FakeNotionAPI
from agentos.control import ControlPlane, Forbidden
from agentos.grant_checker import NotionGrantChecker
from agentos.knowledge.embedder import Embeddings, FakeEmbedder, OpenAICompatibleEmbedder
from agentos.knowledge.indexing import IndexActivities
from agentos.knowledge.store import InMemoryKnowledgeStore
from agentos.llm import ScriptedLLMClient, from_openai_response, to_llm_response
from agentos.members import InMemoryMemberStore
from agentos.models import LLMResponse, TurnContext, Usage
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for
from agentos.usage import (
    InMemoryUsageStore,
    PriceEntry,
    Prices,
    UsageEvent,
    UsageRecorder,
    build_report,
    load_prices,
    month_bounds,
    parse_prices,
)
from tests.conftest import ALICE, ROOT_A, TENANT, a_tree, agents_dict, make_fake_notion
from tests.console_harness import OTHER, OWNER, ROOT, ConsoleHarness, using
from tests.test_agent_store import helper_block

REPO = Path(__file__).resolve().parent.parent
NOV = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)
D = Decimal


def price(model="m", effective_from=date(2026, 1, 1), i="3", o="15", cr="0.3", cw="3.75"):
    return PriceEntry(model, effective_from, D(i), D(o), D(cr), D(cw))


class Clock:
    def __init__(self, now=NOV):
        self.now = now

    def __call__(self):
        return self.now


# --- provider usage ------------------------------------------------------------------


def anthropic_message(usage="default", model="claude-test"):
    if usage == "default":
        usage = SimpleNamespace(
            input_tokens=10,
            output_tokens=5,
            cache_read_input_tokens=3,
            cache_creation_input_tokens=2,
        )
    return SimpleNamespace(content=[], stop_reason="end_turn", model=model, usage=usage)


def test_the_anthropic_client_maps_the_four_token_counts_and_the_reported_model():
    resp = to_llm_response(anthropic_message())

    assert resp.usage == Usage(
        input_tokens=10,
        output_tokens=5,
        cache_read_tokens=3,
        cache_write_tokens=2,
        model="claude-test",
    )


def test_anthropic_cache_counts_the_provider_leaves_out_are_zero():
    usage = SimpleNamespace(
        input_tokens=7,
        output_tokens=1,
        cache_read_input_tokens=None,
        cache_creation_input_tokens=None,
    )

    assert to_llm_response(anthropic_message(usage)).usage == Usage(7, 1, 0, 0, "claude-test")


def test_a_response_with_no_usage_object_counts_zero_tokens():
    bare = SimpleNamespace(content=[], stop_reason="end_turn")

    assert to_llm_response(bare).usage == Usage(0, 0, 0, 0, None)


def openai_response(usage, model="qwen-test"):
    message = SimpleNamespace(role="assistant", content="ok", tool_calls=None)
    choice = SimpleNamespace(message=message, finish_reason="stop")
    return SimpleNamespace(choices=[choice], usage=usage, model=model)


def test_the_openai_client_splits_cached_prompt_tokens_out_of_the_input():
    usage = SimpleNamespace(
        prompt_tokens=100,
        completion_tokens=20,
        prompt_tokens_details=SimpleNamespace(cached_tokens=40),
    )

    resp = from_openai_response(openai_response(usage))

    assert resp.usage == Usage(60, 20, 40, 0, "qwen-test")


def test_the_openai_client_without_cache_details_counts_all_prompt_tokens_as_input():
    usage = SimpleNamespace(prompt_tokens=100, completion_tokens=20, prompt_tokens_details=None)

    assert from_openai_response(openai_response(usage)).usage == Usage(100, 20, 0, 0, "qwen-test")
    assert from_openai_response(openai_response(None)).usage == Usage(0, 0, 0, 0, "qwen-test")


def test_a_scripted_response_has_zero_usage_unless_the_test_sets_it():
    assert LLMResponse(text="x", tool_calls=[], stop_reason="end_turn").usage == Usage()
    set_usage = Usage(1, 2, 3, 4, "m")
    resp = LLMResponse(text="x", tool_calls=[], stop_reason="end_turn", usage=set_usage)
    llm = ScriptedLLMClient([resp])

    import asyncio

    assert asyncio.run(llm.complete("s", [], [])).usage == set_usage


async def test_the_embedder_sums_the_prompt_tokens_of_every_batch():
    calls = []

    async def create(**kwargs):
        calls.append(kwargs)
        data = [SimpleNamespace(index=i, embedding=[0.0, 1.0]) for i in range(len(kwargs["input"]))]
        return SimpleNamespace(data=data, usage=SimpleNamespace(prompt_tokens=11))

    client = SimpleNamespace(embeddings=SimpleNamespace(create=create))
    embedder = OpenAICompatibleEmbedder("k", "http://x.invalid", "bge", 2, client=client)

    vectors = await embedder.embed([f"t{i}" for i in range(23)])

    assert len(calls) == 3 and len(vectors) == 23
    assert isinstance(vectors, Embeddings) and vectors.billed == 33


async def test_embedding_tokens_are_unknown_when_the_endpoint_does_not_report_them():
    async def create(**kwargs):
        data = [SimpleNamespace(index=i, embedding=[0.0, 1.0]) for i in range(len(kwargs["input"]))]
        return SimpleNamespace(data=data)

    client = SimpleNamespace(embeddings=SimpleNamespace(create=create))
    embedder = OpenAICompatibleEmbedder("k", "http://x.invalid", "bge", 2, client=client)

    assert (await embedder.embed(["a"])).billed is None


# --- prices -----------------------------------------------------------------------------


def test_the_price_in_effect_on_the_day_decides_the_cost():
    prices = Prices(
        [
            price("m", date(2026, 1, 1), i="3", o="15"),
            price("m", date(2026, 10, 1), i="2", o="10"),
        ]
    )
    use = Usage(input_tokens=1_000_000, output_tokens=2_000_000, model="m")

    before = datetime(2025, 12, 31, 23, 59, tzinfo=UTC)
    first = datetime(2026, 9, 30, 23, 59, tzinfo=UTC)
    second = datetime(2026, 10, 1, 0, 0, tzinfo=UTC)
    assert prices.cost("m", before, use) is None
    assert prices.cost("m", first, use) == D("33")
    assert prices.cost("m", second, use) == D("22")


def test_all_four_token_kinds_are_priced_per_million():
    prices = Prices([price("m", i="3", o="15", cr="0.3", cw="3.75")])
    use = Usage(1_000, 1_000, 1_000, 1_000)

    assert prices.cost("m", NOV, use) == D("0.02205")


def test_a_model_with_no_price_costs_null_not_zero():
    prices = Prices([price("m")])

    assert prices.cost("other", NOV, Usage(5, 5)) is None
    assert Prices([]).cost("m", NOV, Usage(5, 5)) is None


def test_a_later_price_never_reaches_back_to_a_call_already_priced():
    store = InMemoryUsageStore()
    clock = Clock()
    recorder = UsageRecorder(store, Prices([price("m", i="3", o="15")]), clock)

    import asyncio

    asyncio.run(
        recorder.llm_call(
            tenant_id=TENANT,
            agent_id="a",
            agent_version=1,
            conversation_id="c",
            key="k1",
            usage=Usage(1_000_000, 0, 0, 0, "m"),
            fallback_model="m",
        )
    )
    cost_before = store.events[0].cost

    recorder.prices = Prices([price("m", i="99", o="99")])
    assert store.events[0].cost == cost_before == D("3")


def write_prices(tmp_path, entries):
    path = tmp_path / "prices.yaml"
    path.write_text(yaml.safe_dump({"prices": entries}))
    return path


def entry(**over):
    base = {
        "model": "m",
        "effective_from": date(2026, 1, 1),
        "input": 3,
        "output": 15,
        "cache_read": 0.3,
        "cache_write": 3.75,
    }
    base.update(over)
    return base


def test_prices_load_from_the_file_and_several_entries_per_model_are_allowed(tmp_path):
    path = write_prices(
        tmp_path, [entry(), entry(effective_from=date(2026, 6, 1), input=2), entry(model="n")]
    )

    prices = load_prices(path)

    assert prices.cost("m", NOV, Usage(input_tokens=1_000_000)) == D("2")
    assert prices.cost("n", NOV, Usage(input_tokens=1_000_000)) == D("3")


@pytest.mark.parametrize(
    "bad",
    [
        entry(model=""),
        entry(input=-1),
        entry(input="cheap"),
        entry(effective_from="soon"),
        {k: v for k, v in entry().items() if k != "cache_write"},
        {**entry(), "colour": "red"},
    ],
    ids=["no-model", "negative", "not-a-number", "bad-date", "missing-field", "unknown-field"],
)
def test_a_bad_price_entry_is_a_config_error_that_names_the_entry(tmp_path, bad):
    with pytest.raises(ConfigError, match="prices"):
        load_prices(write_prices(tmp_path, [bad]))


def test_two_entries_for_one_model_and_day_are_refused(tmp_path):
    with pytest.raises(ConfigError, match="more than once"):
        load_prices(write_prices(tmp_path, [entry(), entry(input=9)]))


def test_a_missing_prices_file_is_a_config_error(tmp_path):
    with pytest.raises(ConfigError, match="prices"):
        load_prices(tmp_path / "nope.yaml")


def test_the_shipped_prices_file_loads_and_invents_no_price():
    prices = load_prices(REPO / "config" / "prices.yaml")

    assert prices.entries == []
    assert parse_prices({"prices": []}).entries == []


def test_the_prices_path_is_a_setting_with_an_env_override(monkeypatch):
    monkeypatch.delenv("AGENTOS_PRICES", raising=False)
    assert load_settings().prices_path == "config/prices.yaml"
    assert Settings().prices_path == "config/prices.yaml"
    monkeypatch.setenv("AGENTOS_PRICES", "/etc/agentos/prices.yaml")
    assert load_settings().prices_path == "/etc/agentos/prices.yaml"


# --- keys -------------------------------------------------------------------------------


def test_usage_keys_start_with_the_tenant_and_name_the_activity_attempt():
    call = ids.usage_call_key("acme", "acme:conv:C1:1.1", "run-1", "5", 1)
    embed = ids.usage_embedding_key("acme", "acme:index:s", "run-1", "5", 1, "notion:page:x", "p1")

    assert call == "acme:usage:acme:conv:C1:1.1:run-1:5:1"
    assert embed.startswith("acme:usage:") and "notion:page:x" in embed and embed.endswith("p1")
    assert ids.usage_call_key("acme", "acme:conv:C1:1.1", "run-1", "6", 1) != call
    assert ids.usage_call_key("acme", "acme:conv:C1:1.1", "run-1", "5", 2) != call
    assert (
        ids.usage_embedding_key("acme", "acme:index:s", "run-1", "5", 2, "notion:page:x", "p1")
        != embed
    )
    assert ids.usage_call_key("globex", "w", "r", "1", 1) != ids.usage_call_key(
        "acme", "w", "r", "1", 1
    )


# --- recording from call_llm ------------------------------------------------------------


def make_activities(agents, llm, recorder, embedder=None):
    return AgentActivities(
        settings=Settings(model="configured-model"),
        registry=TenantRegistry(
            {agents.tenant_id: runtime_for(agents, FakeSlack(), FakeNotionAPI())}
        ),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=llm,
        tool_outputs=InMemoryToolOutputStore(),
        audit=InMemoryAuditStore(),
        agents=seeded_agent_store(agents),
        usage=recorder,
    )


def turn_ctx() -> TurnContext:
    return TurnContext(
        tenant_id=TENANT,
        agent_id="it-helper",
        agent_version=1,
        requester_user_id=ALICE,
        channel="C1",
        thread_ts="1.1",
        channel_kind="channel",
    )


def answer(usage=None) -> LLMResponse:
    return LLMResponse(
        text="done",
        tool_calls=[],
        stop_reason="end_turn",
        usage=usage or Usage(1000, 500, 200, 100, "m"),
    )


def recorder_with(store=None, entries=None, clock=None):
    store = InMemoryUsageStore() if store is None else store
    prices = Prices([price("m")] if entries is None else entries)
    return store, UsageRecorder(store, prices, clock or Clock())


async def test_call_llm_records_one_row_with_agent_version_model_tokens_and_cost(agents):
    store, recorder = recorder_with()
    acts = make_activities(agents, ScriptedLLMClient([answer()]), recorder)

    resp = await ActivityEnvironment().run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")

    assert resp.text == "done"
    [row] = store.events
    assert (row.tenant_id, row.kind, row.agent_id, row.agent_version) == (
        TENANT,
        "llm",
        "it-helper",
        1,
    )
    assert (row.model, row.conversation_id, row.at) == ("m", "acme:conv:C1:1.1", NOV)
    assert (row.input_tokens, row.output_tokens) == (1000, 500)
    assert (row.cache_read_tokens, row.cache_write_tokens) == (200, 100)
    assert row.cost == D("0.003") + D("0.0075") + D("0.00006") + D("0.000375")
    assert row.usage_key.startswith(f"{TENANT}:")


async def test_the_row_names_no_requester(agents):
    store, recorder = recorder_with()
    acts = make_activities(agents, ScriptedLLMClient([answer()]), recorder)

    await ActivityEnvironment().run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")

    fields = {f.name for f in dataclasses.fields(UsageEvent)}
    assert not {f for f in fields if "user" in f or "requester" in f}
    assert ALICE not in repr(store.events)


async def test_the_same_attempt_recorded_twice_counts_once(agents):
    store, recorder = recorder_with()
    llm = ScriptedLLMClient([answer(), answer()])
    acts = make_activities(agents, llm, recorder)
    env = ActivityEnvironment()

    await env.run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")
    await env.run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")

    assert len(llm.calls) == 2
    assert len(store.events) == 1


async def test_each_attempt_that_got_an_answer_is_its_own_row(agents):
    store, recorder = recorder_with()
    llm = ScriptedLLMClient([answer(), answer()])
    acts = make_activities(agents, llm, recorder)
    first, retry = ActivityEnvironment(), ActivityEnvironment()
    retry.info = replace(retry.info, attempt=first.info.attempt + 1)

    await first.run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")
    await retry.run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")

    assert len(llm.calls) == 2
    assert len(store.events) == 2
    assert len({e.usage_key for e in store.events}) == 2


async def test_two_calls_of_one_conversation_are_two_rows(agents):
    store, recorder = recorder_with()
    acts = make_activities(agents, ScriptedLLMClient([answer(), answer()]), recorder)
    first, second = ActivityEnvironment(), ActivityEnvironment()
    second.info = replace(second.info, activity_id="other-activity")

    await first.run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")
    await second.run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")

    assert len(store.events) == 2


async def test_a_call_to_a_model_with_no_price_records_tokens_and_null_cost(agents):
    store, recorder = recorder_with(entries=[])
    acts = make_activities(agents, ScriptedLLMClient([answer()]), recorder)

    await ActivityEnvironment().run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")

    [row] = store.events
    assert row.cost is None and row.input_tokens == 1000


async def test_a_response_without_a_model_name_is_recorded_under_the_configured_model(agents):
    store, recorder = recorder_with()
    resp = answer(Usage(5, 5))
    acts = make_activities(agents, ScriptedLLMClient([resp]), recorder)

    await ActivityEnvironment().run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")

    assert store.events[0].model == "configured-model"


class BrokenStore(InMemoryUsageStore):
    async def record(self, event):
        raise RuntimeError("usage database is down: SECRET-TEXT")


async def test_a_recording_failure_does_not_fail_the_turn_and_logs_no_content(agents, caplog):
    _, recorder = recorder_with(store=BrokenStore())
    acts = make_activities(agents, ScriptedLLMClient([answer()]), recorder)

    with caplog.at_level(logging.WARNING):
        resp = await ActivityEnvironment().run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")

    assert resp.text == "done"
    line = next(r.getMessage() for r in caplog.records if "usage" in r.getMessage())
    assert TENANT in line and "it-helper" in line and "RuntimeError" in line
    assert "SECRET-TEXT" not in line and "done" not in line


async def test_a_failed_provider_call_records_nothing(agents):
    store, recorder = recorder_with()
    acts = make_activities(agents, ScriptedLLMClient([RuntimeError("down")]), recorder)

    with pytest.raises(RuntimeError):
        await ActivityEnvironment().run(acts.call_llm, turn_ctx(), "acme:conv:C1:1.1")

    assert store.events == []


# --- embedding usage ----------------------------------------------------------------------


def index_activities(recorder, embedder):
    cfg = parse_agents_config(_page_agents())
    notion = make_fake_notion()
    notion.add_page_tree(ROOT_A, a_tree())
    registry = TenantRegistry({TENANT: runtime_for(cfg, FakeSlack(), notion)})
    return IndexActivities(
        registry, InMemoryKnowledgeStore(), embedder, seeded_agent_store(cfg), usage=recorder
    )


def _page_agents():
    raw = agents_dict()
    raw["agents"]["it-helper"]["grants"]["notion"]["pages"] = [{"id": ROOT_A, "name": "IT wiki"}]
    return raw


SCOPE = ids.knowledge_scope(ROOT_A)


async def test_indexing_records_embedding_tokens_against_the_tenant_with_no_agent():
    store, recorder = recorder_with(entries=[price("fake-embed", i="0.1", o="0", cr="0", cw="0")])
    embedder = FakeEmbedder()
    embedder.usage_per_text = 7
    acts = index_activities(recorder, embedder)

    result = await ActivityEnvironment().run(acts.sync_page, TENANT, SCOPE, ROOT_A)

    [row] = store.events
    assert (row.tenant_id, row.kind, row.agent_id, row.agent_version) == (
        TENANT,
        "embedding",
        None,
        None,
    )
    assert row.model == "fake-embed" and row.conversation_id is None
    assert row.input_tokens == 7 * result.chunks and row.output_tokens in (0, None)
    assert row.cost == D(7 * result.chunks) * D("0.1") / D(1_000_000)
    assert row.usage_key.startswith(f"{TENANT}:")


async def test_an_unchanged_page_embeds_nothing_and_records_nothing():
    store, recorder = recorder_with()
    acts = index_activities(recorder, FakeEmbedder())
    env = ActivityEnvironment()
    await env.run(acts.sync_page, TENANT, SCOPE, ROOT_A)
    rows = len(store.events)

    again = ActivityEnvironment()
    again.info = replace(again.info, activity_id="second-run")
    await again.run(acts.sync_page, TENANT, SCOPE, ROOT_A)

    assert rows == 1 and len(store.events) == 1


async def test_an_embedding_recorded_twice_by_the_same_attempt_counts_once():
    store, recorder = recorder_with()
    acts = index_activities(recorder, FakeEmbedder())
    env = ActivityEnvironment()

    await env.run(acts.sync_page, TENANT, SCOPE, ROOT_A)
    acts.store.pages.clear()
    acts.store.chunks.clear()
    await env.run(acts.sync_page, TENANT, SCOPE, ROOT_A)

    assert len(store.events) == 1


async def test_an_embedding_redone_by_a_later_attempt_is_its_own_row():
    store, recorder = recorder_with()
    acts = index_activities(recorder, FakeEmbedder())
    first, retry = ActivityEnvironment(), ActivityEnvironment()
    retry.info = replace(retry.info, attempt=first.info.attempt + 1)

    await first.run(acts.sync_page, TENANT, SCOPE, ROOT_A)
    acts.store.pages.clear()
    acts.store.chunks.clear()
    await retry.run(acts.sync_page, TENANT, SCOPE, ROOT_A)

    assert len(store.events) == 2


async def test_an_endpoint_that_reports_no_tokens_records_null_tokens_and_null_cost():
    store, recorder = recorder_with()
    embedder = FakeEmbedder()
    embedder.usage_per_text = None
    acts = index_activities(recorder, embedder)

    await ActivityEnvironment().run(acts.sync_page, TENANT, SCOPE, ROOT_A)

    [row] = store.events
    assert row.input_tokens is None and row.cost is None


# --- aggregation ------------------------------------------------------------------------


def event(
    key, agent="a1", version=1, model="m", at=NOV, cost="1", kind="llm", tokens=(10, 5, 0, 0)
):
    return UsageEvent(
        tenant_id=TENANT,
        usage_key=key,
        kind=kind,
        agent_id=None if kind == "embedding" else agent,
        agent_version=None if kind == "embedding" else version,
        model=model,
        input_tokens=tokens[0],
        output_tokens=tokens[1],
        cache_read_tokens=tokens[2],
        cache_write_tokens=tokens[3],
        cost=None if cost is None else D(cost),
        conversation_id=None,
        at=at,
    )


async def report_for(events, teams, month="2026-11", visible=None):
    store = InMemoryUsageStore()
    for e in events:
        await store.record(e)
    start, end, label = month_bounds(month, NOV)
    groups = await store.totals(TENANT, start, end)
    if visible is not None:
        groups = [g for g in groups if g.agent_id in visible]
    return build_report(label, groups, teams)


async def test_usage_is_grouped_by_agent_team_and_model_with_costs_summed():
    events = [
        event("1", "a1", model="m", cost="1"),
        event("2", "a1", model="m", cost="2"),
        event("3", "a2", model="n", cost="4"),
        event("4", "a3", model="m", cost="8"),
    ]
    teams = {("a1", 1): "IT", ("a2", 1): "IT", ("a3", 1): None}

    report = await report_for(events, teams)

    by_agent = {line.name: line for line in report.by_agent}
    assert by_agent["a1"].totals.calls == 2 and by_agent["a1"].totals.cost == D("3")
    assert by_agent["a1"].team == "IT" and by_agent["a3"].team is None
    by_team = {line.name: line.totals for line in report.by_team}
    assert by_team["IT"].cost == D("7") and by_team[None].cost == D("8")
    by_model = {line.name: line.totals for line in report.by_model}
    assert by_model["m"].cost == D("11") and by_model["n"].cost == D("4")
    assert report.total.cost == D("15") and report.total.calls == 4
    assert report.total.input_tokens == 40 and report.total.output_tokens == 20


async def test_a_team_follows_the_version_that_made_the_call():
    events = [event("1", "a1", version=1, cost="1"), event("2", "a1", version=2, cost="2")]
    teams = {("a1", 1): "IT", ("a1", 2): "HR"}

    report = await report_for(events, teams)

    assert {line.name: line.totals.cost for line in report.by_team} == {"IT": D("1"), "HR": D("2")}
    [agent] = report.by_agent
    assert agent.totals.cost == D("3")


async def test_an_unpriced_call_counts_tokens_but_shows_as_unpriced_not_zero_cost():
    events = [event("1", cost="1"), event("2", cost=None)]

    report = await report_for(events, {("a1", 1): None})

    assert report.total.cost == D("1") and report.total.unpriced_calls == 1
    assert report.total.input_tokens == 20
    [model] = report.by_model
    assert model.totals.unpriced_calls == 1


async def test_months_are_utc_calendar_months():
    events = [
        event("oct", at=datetime(2026, 10, 31, 23, 59, 59, tzinfo=UTC), cost="1"),
        event("nov-first", at=datetime(2026, 11, 1, 0, 0, tzinfo=UTC), cost="2"),
        event("nov-last", at=datetime(2026, 11, 30, 23, 59, 59, tzinfo=UTC), cost="4"),
        event("dec", at=datetime(2026, 12, 1, 0, 0, tzinfo=UTC), cost="8"),
    ]
    teams = {("a1", 1): None}

    assert (await report_for(events, teams, "2026-10")).total.cost == D("1")
    assert (await report_for(events, teams, "2026-11")).total.cost == D("6")
    assert (await report_for(events, teams, "2026-12")).total.cost == D("8")
    assert (await report_for(events, teams, "2027-01")).total.calls == 0


async def test_embedding_usage_is_its_own_line_and_not_in_the_agent_tables():
    events = [event("1", cost="1"), event("e", kind="embedding", model="bge", cost="0.5")]

    report = await report_for(events, {("a1", 1): None})

    assert report.embedding.calls == 1 and report.embedding.cost == D("0.5")
    assert [line.name for line in report.by_agent] == ["a1"]
    assert [line.name for line in report.by_model] == ["m"]
    assert report.total.cost == D("1.5")


async def test_an_embedding_row_with_null_tokens_sums_as_zero_and_is_unpriced():
    row = event("e", kind="embedding", model="bge", cost=None, tokens=(0, 0, 0, 0))
    row = replace(row, input_tokens=None, output_tokens=None)

    report = await report_for([row], {})

    assert report.embedding.input_tokens == 0 and report.embedding.unpriced_calls == 1


async def test_a_duplicate_key_does_not_add_a_second_row():
    store = InMemoryUsageStore()
    await store.record(event("same", cost="1"))
    await store.record(event("same", cost="9"))

    assert [e.cost for e in store.events] == [D("1")]


async def test_one_tenants_usage_is_not_in_another_tenants_totals():
    store = InMemoryUsageStore()
    await store.record(event("1", cost="1"))
    await store.record(replace(event("2", cost="2"), tenant_id="globex"))
    start, end, _ = month_bounds("2026-11", NOV)

    [group] = await store.totals(TENANT, start, end)

    assert group.cost == D("1")


def test_the_month_is_yyyy_mm_and_defaults_to_the_current_utc_month():
    assert month_bounds(None, NOV)[2] == "2026-11"
    start, end, label = month_bounds("2026-12", NOV)
    assert (start, end, label) == (
        datetime(2026, 12, 1, tzinfo=UTC),
        datetime(2027, 1, 1, tzinfo=UTC),
        "2026-12",
    )
    for bad in ("2026-13", "2026-1", "nov", "2026-11-01", "0000-01", "99999-01", ""):
        with pytest.raises(ConfigError, match="month"):
            month_bounds(bad, NOV)


# --- the control plane ----------------------------------------------------------------------


@dataclasses.dataclass
class Plane:
    plane: ControlPlane
    usage: InMemoryUsageStore
    agents: InMemoryAgentStore


async def make_plane() -> Plane:
    agents, members, usage = InMemoryAgentStore(), InMemoryMemberStore(), InMemoryUsageStore()
    await members.add_member(TENANT, OWNER, "creator", ROOT)
    await members.add_member(TENANT, OTHER, "creator", ROOT)
    raw = agents_dict()
    raw["admins"] = [ROOT]
    plane = ControlPlane(
        agents,
        members,
        InMemoryAuditStore(),
        TenantsConfig.of(parse_agents_config(raw)),
        NotionGrantChecker(lambda tenant_id: make_fake_notion()),
        usage=usage,
        clock=Clock(),
    )
    return Plane(plane, usage, agents)


def block(team=None):
    definition = helper_block()
    if team:
        definition["team"] = team
    return definition


async def seeded_plane() -> Plane:
    p = await make_plane()
    await p.plane.add_agent(TENANT, OWNER, "mine", block("IT"))
    await p.plane.add_agent(TENANT, OTHER, "theirs", block("HR"))
    await p.plane.add_agent(TENANT, OTHER, "teamless", block())
    for n, agent in enumerate(["mine", "theirs", "teamless"]):
        await p.usage.record(event(f"k{n}", agent, cost=str(2**n)))
    await p.usage.record(event("emb", kind="embedding", model="bge", cost="0.5"))
    return p


async def test_an_admin_sees_every_agent_and_the_embedding_line():
    p = await seeded_plane()

    report = await p.plane.usage_report(TENANT, ROOT, "2026-11")

    assert {line.name for line in report.by_agent} == {"mine", "theirs", "teamless"}
    assert {line.name for line in report.by_team} == {"IT", "HR", None}
    assert report.embedding is not None and report.total.cost == D("7.5")


async def test_a_creator_sees_only_the_agents_they_created_and_no_embedding_line():
    p = await seeded_plane()

    report = await p.plane.usage_report(TENANT, OWNER, "2026-11")

    assert [line.name for line in report.by_agent] == ["mine"]
    assert [line.name for line in report.by_team] == ["IT"]
    assert [line.name for line in report.by_model] == ["m"]
    assert report.embedding is None and report.total.cost == D("1")


async def test_someone_with_no_role_may_not_read_usage():
    p = await seeded_plane()

    with pytest.raises(Forbidden):
        await p.plane.usage_report(TENANT, "U_OUTSIDER", "2026-11")


async def test_the_month_defaults_to_the_clock_s_month():
    p = await seeded_plane()

    assert (await p.plane.usage_report(TENANT, ROOT)).month == "2026-11"


async def test_the_report_has_no_requester_and_no_per_person_field():
    p = await seeded_plane()
    report = await p.plane.usage_report(TENANT, ROOT, "2026-11")

    names = {
        f.name
        for cls in (usage_module.Totals, usage_module.Line, type(report))
        for f in dataclasses.fields(cls)
    }
    assert not {n for n in names if "user" in n or "requester" in n or "person" in n}
    assert "U_" not in str(report.as_dict())


# --- the JSON API --------------------------------------------------------------------------


@pytest.fixture
async def h():
    harness = ConsoleHarness()
    await harness.seed_members()
    harness.usage.events.clear()
    await harness.plane.add_agent(TENANT, OWNER, "mine", block("IT"))
    await harness.plane.add_agent(TENANT, OTHER, "theirs", block("=HR"))
    for n, agent in enumerate(["mine", "theirs"]):
        await harness.usage.record(event(f"k{n}", agent, cost=str(2**n)))
    await harness.usage.record(event("emb", kind="embedding", model="bge", cost="0.5"))
    await harness.usage.record(event("np", "mine", model="unknown", cost=None))
    return harness


JSON_HEADERS = {"Content-Type": "application/json"}


async def api_get(h, user, path):
    async with using(await h.signed_in(user)) as client:
        return await client.get(path)


async def get_after_losing_the_role(h, path):
    async with using(await h.signed_in(OTHER)) as client:
        await h.members.remove_member(TENANT, OTHER)
        return await client.get(path)


async def test_the_api_returns_the_same_aggregates_as_the_page_for_an_admin(h):
    response = await api_get(h, ROOT, f"/tenants/{TENANT}/usage?month=2026-11")

    assert response.status_code == 200
    body = response.json()
    assert body["month"] == "2026-11"
    assert {line["agent_id"] for line in body["by_agent"]} == {"mine", "theirs"}
    assert body["embedding"]["calls"] == 1
    assert body["total"]["unpriced_calls"] == 1
    assert body["total"]["cost_usd"] == "3.5"
    teams = {line["team"]: line for line in body["by_team"]}
    assert set(teams) == {"IT", "=HR"}


async def test_the_api_narrows_a_creator_to_their_agents(h):
    response = await api_get(h, OWNER, f"/tenants/{TENANT}/usage?month=2026-11")

    body = response.json()
    assert [line["agent_id"] for line in body["by_agent"]] == ["mine"]
    assert body["embedding"] is None


async def test_the_api_refuses_a_user_without_a_role_and_a_bad_month(h):
    no_role = await get_after_losing_the_role(h, f"/tenants/{TENANT}/usage")
    assert no_role.status_code == 403
    bad = await api_get(h, ROOT, f"/tenants/{TENANT}/usage?month=2026-13")
    assert bad.status_code == 422 and "month" in bad.json()["error"]


async def test_the_api_ignores_a_requester_filter_and_has_no_person_in_the_output(h):
    plain = await api_get(h, ROOT, f"/tenants/{TENANT}/usage?month=2026-11")
    filtered = await api_get(h, ROOT, f"/tenants/{TENANT}/usage?month=2026-11&user=U_ALICE")

    assert filtered.json() == plain.json()
    assert "U_ALICE" not in plain.text and "requester" not in plain.text


# --- the page and the CSV -------------------------------------------------------------------

UI = f"/ui/{TENANT}"


async def test_the_usage_page_shows_totals_tables_and_the_unpriced_note(h):
    response = await api_get(h, ROOT, f"{UI}/usage?month=2026-11")

    assert response.status_code == 200
    text = " ".join(response.text.split())
    for needle in ("By agent", "By team", "By model", "Embeddings", "mine", "theirs", "no price"):
        assert needle in text, needle
    assert "No team" not in text
    assert '<option value="2026-11" selected>' in response.text
    assert f"{UI}/usage.csv?month=2026-11" in response.text


async def test_the_page_for_a_creator_leaves_out_other_agents_and_embeddings(h):
    response = await api_get(h, OWNER, f"{UI}/usage?month=2026-11")

    assert response.status_code == 200
    assert "mine" in response.text and "theirs" not in response.text
    assert "Embeddings" not in response.text


async def test_a_user_with_no_role_gets_no_usage_page(h):
    response = await get_after_losing_the_role(h, f"{UI}/usage")

    assert response.status_code == 403


async def test_the_usage_link_is_in_the_navigation_for_admins_and_creators(h):
    for user in (ROOT, OWNER):
        response = await api_get(h, user, f"{UI}/agents")
        assert f'href="{UI}/usage"' in response.text


async def test_the_page_with_a_bad_month_says_so(h):
    response = await api_get(h, ROOT, f"{UI}/usage?month=nonsense")

    assert response.status_code == 422 and "month" in response.text


async def test_the_csv_is_the_aggregates_not_the_raw_rows(h):
    response = await api_get(h, ROOT, f"{UI}/usage.csv?month=2026-11")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment" in response.headers["content-disposition"]
    assert "usage-2026-11.csv" in response.headers["content-disposition"]
    rows = list(csv.reader(io.StringIO(response.text)))
    sections = {row[0] for row in rows if row}
    assert {"agent", "team", "model", "embedding", "total"} <= sections
    assert "k0" not in response.text and "usage_key" not in response.text
    assert len([r for r in rows if r and r[0] == "agent"]) == 2


async def test_the_csv_neutralises_a_team_name_a_spreadsheet_would_run(h):
    response = await api_get(h, ROOT, f"{UI}/usage.csv?month=2026-11")

    cells = [c for row in csv.reader(io.StringIO(response.text)) for c in row]
    assert "'=HR" in cells and "=HR" not in cells


async def test_the_csv_for_a_creator_has_only_their_agents(h):
    response = await api_get(h, OWNER, f"{UI}/usage.csv?month=2026-11")

    assert "mine" in response.text and "theirs" not in response.text
    assert "embedding" not in {row[0] for row in csv.reader(io.StringIO(response.text)) if row}


async def test_the_csv_download_by_browser_navigation_still_gets_the_security_headers(h):
    navigation = {
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Dest": "document",
    }
    async with using(await h.signed_in(ROOT)) as client:
        response = await client.get(f"{UI}/usage.csv?month=2026-11", headers=navigation)
        framed = await client.get(
            f"{UI}/usage.csv?month=2026-11", headers={**navigation, "Sec-Fetch-Dest": "iframe"}
        )

    assert response.status_code == 200
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert "default-src 'self'" in response.headers["content-security-policy"]
    assert response.headers["cache-control"] == "no-store"
    assert framed.status_code == 403
    assert framed.headers["x-frame-options"] == "DENY"


# --- team on the agent definition ----------------------------------------------------------


def parse(**over):
    raw = helper_block()
    raw.update(over)
    return parse_agent("it-helper", raw, 1)


def test_an_agent_without_a_team_has_an_empty_team():
    assert parse().team == ""
    assert parse(team=None).team == ""


def test_a_team_label_is_kept_trimmed_and_round_trips_through_the_definition():
    agent = parse(team="  IT  ")

    assert agent.team == "IT"
    assert agent_definition(agent)["team"] == "IT"
    assert parse_agent("it-helper", agent_definition(agent), agent.version) == agent
    assert "team" not in agent_definition(parse())


@pytest.mark.parametrize(
    "bad", [["IT"], 7, "", "   ", "x" * 65, "IT\nHR", "IT\x00"], ids=lambda v: repr(v)[:12]
)
def test_a_bad_team_label_is_a_config_error(bad):
    with pytest.raises(ConfigError, match="team"):
        parse(team=bad)


def test_a_team_in_a_definition_survives_the_agent_store():
    definition = copy.deepcopy(block("IT"))

    import asyncio

    store = InMemoryAgentStore()
    record = asyncio.run(store.create_agent(TENANT, "x", definition, "U1"))

    assert record.agent().team == "IT"
