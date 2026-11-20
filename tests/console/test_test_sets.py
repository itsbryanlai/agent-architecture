"""Creator test sets through the console: roles, validation, runs, the submit view,
and the feedback page's "Make a test case". Everything is in memory."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from agentos import ids
from agentos.agent_store import AgentStoreError, InMemoryAgentStore
from agentos.audit import InMemoryAuditStore, canonical_sha256
from agentos.connectors.notion import normalize_id
from agentos.control import ControlPlane, Forbidden
from agentos.grant_checker import NotionGrantChecker
from agentos.grant_picks import GrantNotPicked, PickedObject
from agentos.members import InMemoryMemberStore
from agentos.models import NO_ANSWER, AuditEvent
from agentos.test_sets import (
    CITE,
    DONE,
    FAIL,
    HAND_OFF,
    PASS,
    REFUSE,
    RUNNING,
    SOURCE_NOT_CITED,
    WRITE,
    CaseResult,
    InMemoryTestSetStore,
    TestRun,
    TestSetError,
    case_set_sha256,
)
from tests.conftest import DB_ID, ROOT_A, TENANT, make_fake_notion
from tests.helpers.agents import helper_block
from tests.helpers.console_harness import (
    ADMIN2,
    OTHER,
    OWNER,
    ROOT,
    ConsoleHarness,
    csrf_in,
    post,
    using,
)
from tests.helpers.control_plane_tenants import tenants

UI = f"/ui/{TENANT}"
VERSION = f"{UI}/agents/it-helper/versions/1"
FEEDBACK = f"{UI}/agents/it-helper/feedback"
OUTSIDER = "U_OUTSIDER"


class Starter:
    """Records the runs the plane asks to start; a fake for the Temporal starter."""

    def __init__(self):
        self.started: list[tuple[str, str]] = []

    async def __call__(self, tenant_id: str, run_id: str) -> None:
        self.started.append((tenant_id, run_id))


async def make_plane(starter=None, require_grant_picker=False):
    agents, members, audit = InMemoryAgentStore(), InMemoryMemberStore(), InMemoryAuditStore()
    await members.add_member(TENANT, ADMIN2, "admin", ROOT)
    await members.add_member(TENANT, OWNER, "creator", ROOT)
    await members.add_member(TENANT, OTHER, "creator", ROOT)
    sets = InMemoryTestSetStore()
    plane = ControlPlane(
        agents,
        members,
        audit,
        tenants(ROOT),
        NotionGrantChecker(lambda tenant_id: make_fake_notion()),
        test_sets=sets,
        start_test_run=starter,
        require_grant_picker=require_grant_picker,
    )
    return plane, agents, sets, audit


def granting_block() -> dict:
    """it-helper with a page grant and a specialist, so every expectation has a valid target."""
    block = helper_block()
    block["grants"]["notion"]["pages"] = [{"id": ROOT_A, "name": "IT wiki"}]
    block["specialists"] = ["hr-helper"]
    return block


@pytest.mark.parametrize(
    ("actor", "allowed"),
    [(OWNER, True), (OTHER, False), (ADMIN2, True), (OUTSIDER, False)],
    ids=["the creator", "another creator", "an admin", "no role"],
)
async def test_only_the_agents_creator_and_admins_may_change_cases_or_run_them(actor, allowed):
    """Fails if another creator or a person with no role can add a case or start a run."""
    starter = Starter()
    plane, _, _, _ = await make_plane(starter)
    await plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    if allowed:
        await plane.add_test_case(TENANT, actor, "it-helper", 1, "q", REFUSE)
        run = await plane.run_tests(TENANT, actor, "it-helper", 1)
        assert starter.started == [(TENANT, run.run_id)]
    else:
        with pytest.raises(Forbidden):
            await plane.add_test_case(TENANT, actor, "it-helper", 1, "q", REFUSE)
        await plane.add_test_case(TENANT, OWNER, "it-helper", 1, "q", REFUSE)
        with pytest.raises(Forbidden):
            await plane.run_tests(TENANT, actor, "it-helper", 1)
        assert starter.started == []


@pytest.mark.parametrize(
    ("question", "expect", "target", "accepted"),
    [
        ("", REFUSE, "", False),
        ("   ", REFUSE, "", False),
        ("q" * 2001, REFUSE, "", False),
        ("q" * 2000, REFUSE, "", True),
        ("q", "bogus", "", False),
        ("q", "", "", True),
        ("q", REFUSE, "ignored", True),
        ("q", CITE, "", False),
        ("q", CITE, ROOT_A, True),
        ("q", CITE, DB_ID, False),
        ("q", WRITE, DB_ID, True),
        ("q", WRITE, ROOT_A, False),
        ("q", HAND_OFF, "hr-helper", True),
        ("q", HAND_OFF, "plain", False),
    ],
    ids=[
        "empty question",
        "blank question",
        "too long",
        "at the limit",
        "unknown kind",
        "unset expectation",
        "refuse ignores target",
        "cite without target",
        "cite granted page",
        "cite a database",
        "write granted database",
        "write a page",
        "hand-off listed specialist",
        "hand-off unlisted agent",
    ],
)
async def test_a_case_needs_a_question_and_a_target_the_draft_grants(
    question, expect, target, accepted
):
    """Fails if a case accepts a bad question or kind, or a target this draft does not grant or list."""
    plane, _, sets, _ = await make_plane()
    await plane.add_agent(TENANT, OWNER, "it-helper", granting_block())
    if accepted:
        case = await plane.add_test_case(TENANT, OWNER, "it-helper", 1, question, expect, target)
        stored = (await sets.cases(TENANT, "it-helper", 1))[0]
        assert stored.case_id == case.case_id
        if expect == CITE:
            assert stored.target == normalize_id(ROOT_A)
        if expect == REFUSE:
            assert stored.target == ""
    else:
        with pytest.raises(TestSetError):
            await plane.add_test_case(TENANT, OWNER, "it-helper", 1, question, expect, target)
        assert await sets.cases(TENANT, "it-helper", 1) == []


@pytest.mark.parametrize("state", ["no cases", "unset", "pending", "no starter", "ready"])
async def test_a_run_needs_a_draft_with_chosen_cases(state):
    """Fails if a run starts without cases, with an unset expectation, on a non-draft, or with no starter."""
    starter = None if state == "no starter" else Starter()
    plane, _, _, audit = await make_plane(starter)
    await plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    if state != "no cases":
        await plane.add_test_case(
            TENANT, OWNER, "it-helper", 1, "q", "" if state == "unset" else REFUSE
        )
    if state == "pending":
        await plane.submit_version(TENANT, OWNER, "it-helper", 1)
    if state == "ready":
        run = await plane.run_tests(TENANT, OWNER, "it-helper", 1)
        assert run.status == RUNNING
        assert starter.started == [(TENANT, run.run_id)]
        assert [e.kind for e in audit.events] == ["agent_create", "test_case", "test_run"]
    else:
        # A non-draft is the store's own refusal; the rest are the test-set rules'.
        refusal = AgentStoreError if state == "pending" else TestSetError
        with pytest.raises(refusal):
            await plane.run_tests(TENANT, OWNER, "it-helper", 1)
        assert (starter.started if starter is not None else []) == []


@pytest.mark.parametrize("picked", [False, True], ids=["not picked", "picked"])
async def test_a_run_needs_a_new_grant_picked_as_a_submit_does(picked):
    """Fails if a run reads with a grant new since the published version that the creator did not choose from their Notion."""
    starter = Starter()
    plane, _, _, _ = await make_plane(starter, require_grant_picker=True)
    await plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    await plane.add_test_case(TENANT, OWNER, "it-helper", 1, "q", REFUSE)
    if picked:
        await plane.record_grant_picks(
            TENANT, OWNER, "it-helper", 1, [PickedObject(DB_ID, "database")]
        )
        run = await plane.run_tests(TENANT, OWNER, "it-helper", 1)
        assert starter.started == [(TENANT, run.run_id)]
    else:
        with pytest.raises(GrantNotPicked, match="run the tests again"):
            await plane.run_tests(TENANT, OWNER, "it-helper", 1)
        assert starter.started == []


@pytest.mark.parametrize(
    "retry", ["new click", "same request"], ids=["double click", "same request_id"]
)
async def test_one_run_of_a_version_at_a_time_and_a_retry_returns_the_same_run(retry):
    """Fails if a second start while a run is going makes another run, or a retry with the same request ID returns a different one."""
    starter = Starter()
    plane, _, sets, _ = await make_plane(starter)
    await plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    await plane.add_test_case(TENANT, OWNER, "it-helper", 1, "q", REFUSE)
    first = await plane.run_tests(TENANT, OWNER, "it-helper", 1, request_id="req-1")
    if retry == "new click":
        with pytest.raises(TestSetError, match="A test run is already going on this version"):
            await plane.run_tests(TENANT, OWNER, "it-helper", 1)
    else:
        again = await plane.run_tests(TENANT, OWNER, "it-helper", 1, request_id="req-1")
        assert again.run_id == first.run_id
    # The second call is over (the settle point): one workflow start, one run row.
    assert starter.started == [(TENANT, first.run_id)]
    assert (await sets.latest_run(TENANT, "it-helper", 1)).run_id == first.run_id


async def test_a_new_draft_carries_the_source_versions_cases():
    """Fails if a new draft does not carry the cases of the version it was copied from."""
    plane, _, sets, _ = await make_plane()
    await plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    await plane.add_test_case(TENANT, OWNER, "it-helper", 1, "q one", REFUSE)
    await plane.submit_version(TENANT, OWNER, "it-helper", 1)
    await plane.publish_version(TENANT, ROOT, "it-helper", 1)

    draft = await plane.new_draft(TENANT, OWNER, "it-helper")
    assert draft.version == 2
    assert [(c.question, c.expect) for c in await sets.cases(TENANT, "it-helper", 2)] == [
        ("q one", REFUSE)
    ]

    await plane.add_test_case(TENANT, OWNER, "it-helper", 2, "q two", REFUSE)
    older = await plane.new_draft(TENANT, OWNER, "it-helper", from_version=1)
    assert [c.question for c in await sets.cases(TENANT, "it-helper", older.version)] == ["q one"]


# --- the console pages ---------------------------------------------------------------


@pytest.fixture
async def h():
    harness = ConsoleHarness()
    await harness.seed_members()
    return harness


async def make_pending(h):
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)


async def seed_run(h, definition_sha256: str):
    run = TestRun(
        TENANT,
        ids.test_run_id(TENANT, "r1"),
        "it-helper",
        1,
        RUNNING,
        OWNER,
        definition_sha256,
        case_set_sha256(await h.test_sets.cases(TENANT, "it-helper", 1)),
    )
    await h.test_sets.start_run(TENANT, run)
    await h.test_sets.finish_run(
        TENANT,
        run.run_id,
        [CaseResult("k1", PASS), CaseResult("k2", FAIL, SOURCE_NOT_CITED)],
        Decimal("0.05"),
        status=DONE,
    )


@pytest.mark.parametrize("state", ["none", "fresh", "stale", "cases"])
async def test_the_submit_view_shows_the_latest_run_and_warns_when_missing_or_stale(h, state):
    """Fails if the approving admin's pending view hides the latest run, or misses the no-run or stale warning."""
    await make_pending(h)
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    if state != "none":
        await seed_run(h, record.definition_sha256)
    if state == "stale":
        await h.plane.withdraw_version(TENANT, OWNER, "it-helper", 1)
        block = helper_block()
        block["system_prompt"] = "Edited after the run."
        await h.plane.edit_draft(TENANT, OWNER, "it-helper", 1, block)
        await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
    if state == "cases":
        # The definition is untouched; only the case set changes after the run.
        await h.plane.withdraw_version(TENANT, OWNER, "it-helper", 1)
        await h.plane.add_test_case(TENANT, OWNER, "it-helper", 1, "a new question", REFUSE)
        await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
    c = await h.signed_in(ADMIN2)
    async with using(c):
        page = (await c.get(VERSION)).text
    if state == "none":
        assert "This version has no test run yet" in page
    else:
        assert "1 passed" in page and "1 failed" in page
        assert "the expected source was not cited" in page
        assert ("The draft or its test cases changed after its last test run" in page) is (
            state in ("stale", "cases")
        )


@pytest.mark.parametrize(
    ("draft", "tie", "role", "accepted"),
    [
        ("existing", "it-helper", "user", True),
        ("created", "it-helper", "user", True),
        ("created", "hr-helper", "user", False),
        ("created", "it-helper", "assistant", False),
    ],
    ids=["existing draft", "draft created", "another agent's conversation", "an assistant message"],
)
async def test_make_a_test_case_takes_only_this_agents_stored_question(
    h, draft, tie, role, accepted
):
    """Fails if the feedback button adds a question that is not this agent's stored user message, or does not add one that is."""
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
    await h.plane.publish_version(TENANT, ROOT, "it-helper", 1)
    if draft == "existing":
        await h.plane.new_draft(TENANT, OWNER, "it-helper")
    question = "how do I reset my password"
    await h.history.append(TENANT, "c1", "c1:msg", role, [{"type": "text", "text": question}])
    h.audit.events.append(
        AuditEvent(
            tenant_id=TENANT,
            event_key=f"{TENANT}:seed:1",
            kind="answer",
            agent_id=tie,
            agent_version=1,
            requester_user_id="U_ASKER",
            identity=f"agent:{tie}",
            conversation_id="c1",
            output_sha256=canonical_sha256(NO_ANSWER),
            at=datetime.now(UTC),
            seq=1,
        )
    )
    before = len(await h.plane.versions(TENANT, OWNER, "it-helper"))
    c = await h.signed_in(OWNER)
    async with using(c):
        page = (await c.get(FEEDBACK)).text
        if accepted:
            assert "Make a test case" in page
        response = await post(
            c,
            f"{UI}/agents/it-helper/test-cases",
            csrf_in(page),
            conversation_id="c1",
            question_seq="1",
        )
    after = len(await h.plane.versions(TENANT, OWNER, "it-helper"))
    if accepted:
        assert response.status_code == 303
        assert response.headers["location"].endswith("/versions/2?done=case-added")
        cases = await h.plane.test_cases(TENANT, OWNER, "it-helper", 2)
        assert [(x.question, x.expect) for x in cases] == [(question, "")]
        assert (after == before + 1) is (draft == "created")
    else:
        assert response.status_code == 409
        assert after == before  # no draft was made for a refused question
        assert await h.test_sets.cases(TENANT, "it-helper", 2) == []
