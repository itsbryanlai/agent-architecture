"""The test-run workflow: the draft's definition, drafted writes, the cap, the verdicts.

Everything is a fake: the scripted LLM, fake Slack and Notion, in-memory stores, the
session's time-skipping Temporal server on this test's own queue.
"""

import copy
from decimal import Decimal

import pytest

from agentos.agent_health import agent_unavailable_text
from agentos.budgets import STOP_REASON, stop_text
from agentos.connectors.notion import normalize_id
from agentos.models import LLMResponse, ToolCall, Usage
from agentos.switches import SWITCHED_OFF, agent_off_text
from agentos.test_sets import (
    AGENT_OFF,
    AGENT_UNAVAILABLE,
    CAP_REACHED,
    CITE,
    DID_NOT_REFUSE,
    ERROR,
    FAIL,
    FAILED,
    HAND_OFF,
    NOT_RUN,
    PASS,
    REFUSE,
    SOURCE_NOT_CITED,
    STOPPED_BY_BUDGET,
    WRITE,
    WRONG_ACTION,
    WRONG_HAND_OFF,
)
from tests.conftest import ALICE, DB_ID, PAGE_A1, PAGE_B1, ROOT_B, TENANT, title_prop
from tests.helpers.agents import helper_block
from tests.helpers.test_runs import (
    PRICE_MODEL,
    answer,
    case,
    make_draft,
    refusal,
    run_cases,
    run_harness,
    start_run,
    tool_turn,
)


@pytest.fixture
async def rh(temporal_scope):
    async with run_harness(temporal_scope) as h:
        yield h


async def test_a_run_uses_the_drafts_definition_not_the_published_one(rh):
    """Fails if a run's model steps or reads use the published prompt or grants instead of the draft's."""

    def change(definition):
        definition["system_prompt"] = "DRAFT PROMPT: answer from the HR wiki."
        definition["grants"]["notion"]["pages"] = [{"id": ROOT_B, "name": "HR wiki"}]

    version = await make_draft(rh, change=change)
    rh.llm.queue(
        tool_turn(ToolCall("tu_s", "search_knowledge", {"query": "dental cover"})),
        answer("Dental cover starts after one month [1]."),
    )

    run = await run_cases(
        rh,
        [case("what dental cover do I get?", CITE, normalize_id(PAGE_B1), version=version)],
        version=version,
    )

    # The cited page is under the draft's grant only; the published grant reads the IT wiki.
    assert [r.outcome for r in run.results] == [PASS]
    system = rh.llm.calls[0]["system"]
    assert "DRAFT PROMPT: answer from the HR wiki." in system
    assert "You help employees with IT requests." not in system


async def test_a_write_case_stops_at_the_drafted_action_with_no_card_task_or_write(rh):
    """Fails if a drafted write runs, posts a card, starts a task, or its case continues past the draft."""
    await make_draft(rh)
    rh.llm.queue(
        tool_turn(
            ToolCall("tu_w", "create_item", {"title": "New monitor", "details": "Mine flickers."})
        ),
        refusal(),  # the sentinel case after it
        answer("Sent you a card."),  # never consumed: the write case stops at the draft
    )

    run = await run_cases(
        rh,
        [
            case("file a ticket for my monitor", WRITE, normalize_id(DB_ID)),
            case("something out of scope", REFUSE),
        ],
    )

    # Both cases finished (the settle point): now count what must not have happened.
    assert [(r.outcome, r.tool, r.target) for r in run.results] == [
        (PASS, "create_item", normalize_id(DB_ID)),
        (PASS, "", ""),
    ]
    assert rh.slack.posts() == []
    assert rh.slack.updates() == []
    assert [name for name, _ in rh.notion.calls if name in ("create_page", "create_comment")] == []
    assert await rh.acts.tasks.list_for_requester(TENANT, ALICE) == []
    assert len(rh.llm.calls) == 2


async def test_the_cap_stops_the_run_and_reports_the_cases_not_run(rh):
    """Fails if a run keeps going past its cap, or does not report the cases it did not run."""
    await make_draft(rh)
    rh.llm.queue(
        refusal(Usage(input_tokens=150_000, model=PRICE_MODEL)),  # $0.15
        refusal(Usage(input_tokens=100_000, model=PRICE_MODEL)),  # $0.10: the cap is reached
        refusal(Usage(input_tokens=100_000, model=PRICE_MODEL)),  # never called
    )

    run = await run_cases(rh, [case(f"question {n}", REFUSE) for n in range(3)])

    assert [(r.outcome, r.reason) for r in run.results] == [
        (PASS, ""),
        (NOT_RUN, CAP_REACHED),
        (NOT_RUN, CAP_REACHED),
    ]
    assert run.spend_usd == Decimal("0.25")
    assert len(rh.llm.calls) == 2


async def test_a_second_run_of_a_draft_reads_the_prompt_edited_since_the_first(rh):
    """Fails if a run of a draft uses the prompt an earlier run cached instead of the edited one."""
    version = await make_draft(rh)
    rh.llm.queue(answer("One."), answer("Two."), answer("Three."))
    await run_cases(rh, [case("q one", REFUSE)], version=version)
    assert "You help employees with IT requests." in rh.llm.calls[0]["system"]

    record = await rh.agents.get_version(TENANT, "it-helper", version)
    definition = copy.deepcopy(record.definition)
    definition["system_prompt"] = "EDITED DRAFT PROMPT."
    await rh.agents.replace_draft(TENANT, "it-helper", version, definition)
    await run_cases(rh, [case("q two", REFUSE)], version=version)

    # The second run reads both cases (the first run's and the new one); its first
    # model step must see the edited prompt, not the one the first run read.
    assert "EDITED DRAFT PROMPT." in rh.llm.calls[1]["system"]
    assert "You help employees with IT requests." not in rh.llm.calls[1]["system"]


async def test_a_drafted_comment_records_the_items_database_and_posts_nothing(rh):
    """Fails if a drafted add_comment does not record the item's own database, or posts the comment."""
    await make_draft(rh)
    page = rh.notion.add_page(DB_ID, {"Name": title_prop("Monitor request")})
    rh.llm.queue(
        tool_turn(ToolCall("tu_c", "add_comment", {"item": page["id"], "comment": "Still broken"})),
    )

    run = await run_cases(rh, [case("comment on my ticket", WRITE, normalize_id(DB_ID))])

    # The run finished (the settle point): now count what must not have happened.
    assert [(r.outcome, r.tool, r.target) for r in run.results] == [
        (PASS, "add_comment", normalize_id(DB_ID))
    ]
    assert [name for name, _ in rh.notion.calls if name == "create_comment"] == []
    assert rh.slack.posts() == []


async def test_a_disabled_tenants_run_closes_its_row_failed(rh):
    """Fails if a run whose tenant is uninstalled mid-case leaves its row running instead of failed."""
    version = await make_draft(rh)
    rh.llm.queue(
        tool_turn(ToolCall("tu_i", "search_items", {"query": "laptop"})),
        answer("Back online."),  # never consumed: the next model step is refused
    )
    hang = rh.notion.hang_next_read("query_database")
    run, handle = await start_run(rh, [case("any laptop requests?", REFUSE)], version=version)
    await hang.reached.wait()
    # The app is uninstalled while the case's read is in flight: the read finishes
    # (it holds the runtime it started with); the next model step is the one refused.
    rh.acts.registry.remove(TENANT)
    rh.acts.registry.set_disabled({TENANT})
    hang.release()

    assert await handle.result() == run.run_id  # the run ends cleanly, not in failure
    row = await rh.sets.get_run(TENANT, run.run_id)
    assert (row.status, row.results) == (FAILED, [])


@pytest.mark.parametrize(
    ("expect", "target", "responses", "outcome", "reason"),
    [
        pytest.param(
            CITE,
            normalize_id(PAGE_A1),
            [
                tool_turn(ToolCall("tu_s", "search_knowledge", {"query": "laptops replaced"})),
                answer("Laptops are replaced every three years [1]."),
            ],
            PASS,
            "",
            id="cite-pass",
        ),
        pytest.param(
            CITE,
            normalize_id(PAGE_A1),
            [
                tool_turn(ToolCall("tu_s", "search_knowledge", {"query": "laptops replaced"})),
                answer("Ask the IT team."),
            ],
            FAIL,
            SOURCE_NOT_CITED,
            id="cite-fail",
        ),
        pytest.param(
            WRITE,
            normalize_id(DB_ID),
            [
                tool_turn(
                    ToolCall(
                        "tu_w", "create_item", {"title": "New monitor", "details": "Flickers."}
                    )
                )
            ],
            PASS,
            "",
            id="write-pass",
        ),
        pytest.param(
            WRITE,
            normalize_id(DB_ID),
            [answer("No need to file anything.")],
            FAIL,
            WRONG_ACTION,
            id="write-fail",
        ),
        pytest.param(REFUSE, "", [refusal()], PASS, "", id="refuse-pass"),
        pytest.param(
            REFUSE, "", [answer("Sure, here is how.")], FAIL, DID_NOT_REFUSE, id="refuse-fail"
        ),
        pytest.param(
            REFUSE,
            "",
            [LLMResponse(text=stop_text("IT helper"), tool_calls=[], stop_reason=STOP_REASON)],
            ERROR,
            STOPPED_BY_BUDGET,
            id="budget-stop-is-an-error-not-a-refusal",
        ),
        pytest.param(
            REFUSE,
            "",
            [
                LLMResponse(
                    text=agent_off_text("IT helper"), tool_calls=[], stop_reason=SWITCHED_OFF
                )
            ],
            ERROR,
            AGENT_OFF,
            id="agent-off-is-an-error-not-a-refusal",
        ),
        pytest.param(
            REFUSE,
            "",
            [
                LLMResponse(
                    text=agent_unavailable_text("IT helper"),
                    tool_calls=[],
                    stop_reason=SWITCHED_OFF,
                )
            ],
            ERROR,
            AGENT_UNAVAILABLE,
            id="agent-unavailable-is-an-error-not-a-refusal",
        ),
        pytest.param(
            HAND_OFF,
            "hr-helper",
            [tool_turn(ToolCall("tu_h", "hand_off", {"agent": "hr-helper"}))],
            PASS,
            "",
            id="hand-off-pass",
        ),
        pytest.param(
            HAND_OFF,
            "hr-helper",
            [tool_turn(ToolCall("tu_h", "hand_off", {"agent": "plain"}))],
            FAIL,
            WRONG_HAND_OFF,
            id="hand-off-fail",
        ),
    ],
)
async def test_the_verdict_names_why_a_case_failed(rh, expect, target, responses, outcome, reason):
    """Fails if a case's verdict or reason does not match what its turn observably did."""
    agent, version = "it-helper", await make_draft(rh)
    if expect == HAND_OFF:
        block = helper_block()
        block["description"] = "Routes questions to the right team."
        block["specialists"] = ["hr-helper"]
        block["grants"] = {}
        record = await rh.agents.create_agent(TENANT, "front-door", block, ALICE)
        agent, version = "front-door", record.version
    rh.llm.queue(*responses)

    run = await run_cases(
        rh,
        [case("the question", expect, target, agent=agent, version=version)],
        agent=agent,
        version=version,
    )

    assert [(r.outcome, r.reason) for r in run.results] == [(outcome, reason)]
