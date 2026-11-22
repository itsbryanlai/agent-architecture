"""The nightly check that a creator can still see what they granted, and its schedule.

Fakes only: in-memory stores, fake Slack and Notion, injected clocks.
"""

import secrets as token_source
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from notion_client.errors import APIResponseError
from temporalio.client import ScheduleAlreadyRunningError

from agentos import ids
from agentos.agent_health import (
    AVAILABLE,
    UNAVAILABLE,
    AgentHealthChecker,
    InMemoryAgentHealthStore,
)
from agentos.agent_store import InMemoryAgentStore
from agentos.audit import InMemoryAuditStore
from agentos.config import parse_agents_config
from agentos.connectors.notion import FakeNotFound, normalize_id
from agentos.control import ControlPlane
from agentos.creator_access import (
    CANNOT_VERIFY,
    LOST,
    VERIFIED,
    AccessResult,
    CreatorAccessChecker,
    InMemoryCreatorAccessStore,
    apply_schedule,
)
from agentos.credentials import InMemoryCredentialStore, TenantCredentials
from agentos.grant_checker import NotionGrantChecker
from agentos.grant_picks import InMemoryGrantPickStore, PickedObject
from agentos.members import InMemoryMemberStore
from agentos.slack_client import FakeSlack
from agentos.stores import InMemoryIdempotencyStore
from agentos.tenants import TenantRegistry, runtime_for
from tests.conftest import DB_ID, OTHER_DB_ID, TENANT, agents_dict, default_schema, make_fake_notion
from tests.helpers.agents import make_published
from tests.helpers.control_plane_tenants import tenants
from tests.helpers.keys import RefreshableKeyStore

NOW = datetime(2026, 11, 20, 5, 0, tzinfo=UTC)
CREATOR = "U_EDITOR"  # the agent's creator, who picked its grant and consented
LOST_REASON = "its creator can no longer see 'IT requests'"
GRANT_PROBLEM = "grant 'IT requests'"


class Clock:
    def __init__(self, start: datetime):
        self.now = start

    def __call__(self) -> datetime:
        return self.now


class CreatorReads:
    """The creator's own Notion client: a fake whose every read raises one error."""

    def __init__(self, inner, error):
        self._inner = inner
        self._error = error
        self.closed = 0

    async def retrieve_database(self, database_id):
        if self._error is not None:
            raise self._error
        return await self._inner.retrieve_database(database_id)

    async def retrieve_page(self, page_id):
        if self._error is not None:
            raise self._error
        return await self._inner.retrieve_page(page_id)

    async def aclose(self):
        self.closed += 1


def break_schema(notion) -> None:
    schema = default_schema()
    schema["Agent request ID"] = {"id": "reqid1", "type": "people"}
    notion.set_schema(DB_ID, schema)


class World:
    """One tenant's nightly pass and health check over fakes, with the stores a test reads."""

    def __init__(self, creator_notion, consent: bool, clock: Clock):
        self.cfg = parse_agents_config(agents_dict())
        self.clock = clock
        self.notion = make_fake_notion()
        self.creator_notion = creator_notion
        self.slack = FakeSlack()
        self.agents = InMemoryAgentStore()
        self.picks = InMemoryGrantPickStore()
        self.access = InMemoryCreatorAccessStore()
        self.health = InMemoryAgentHealthStore()
        self.audit = InMemoryAuditStore()
        self.idem = InMemoryIdempotencyStore()
        self.keys = RefreshableKeyStore({(TENANT, 1): token_source.token_bytes(32)})
        self.credentials = TenantCredentials(InMemoryCredentialStore(), self.keys)
        self.registry = TenantRegistry({TENANT: runtime_for(self.cfg, self.slack, self.notion)})
        self.checker = CreatorAccessChecker(
            self.agents, self.picks, self.access, self._api_for, clock=clock
        )
        self.health_checker = AgentHealthChecker(
            self.registry,
            self.agents,
            self.health,
            self.audit,
            self.idem,
            InMemoryMemberStore(),
            clock=clock,
            creator_access=self.access,
        )
        self._consent = consent

    async def seed(self) -> None:
        await self.keys.refresh()
        await make_published(self.agents)
        await self.picks.record(
            TENANT, "it-helper", [PickedObject(DB_ID, "database")], CREATOR, NOW
        )
        if self._consent:
            await self.credentials.store_with_current_key(
                TENANT, ids.notion_creator_kind(CREATOR), "ntn_creator_token_for_the_test"
            )

    async def _api_for(self, tenant_id: str, user_id: str):
        token = await self.credentials.creator_notion_token(tenant_id, user_id)
        return None if token is None else self.creator_notion

    async def nightly(self) -> None:
        await self.checker.check_tenant(TENANT)
        await self.health_checker.check_tenant(TENANT, await self.agents.published_all(TENANT))

    async def health_check(self) -> None:
        await self.health_checker.check_tenant(TENANT, await self.agents.published_all(TENANT))


async def world(creator_notion, consent: bool = True) -> World:
    w = World(creator_notion, consent, Clock(NOW))
    await w.seed()
    return w


# --- the nightly pass: one verdict per object, health through the same path ---------------


@pytest.mark.parametrize(
    ("case", "prior", "access_status", "stale_row", "health_status", "reason_parts"),
    [
        ("readable", None, VERIFIED, False, AVAILABLE, ()),
        ("gone", None, LOST, False, UNAVAILABLE, (LOST_REASON,)),
        ("transient", VERIFIED, VERIFIED, True, AVAILABLE, ()),
        ("no consent", None, CANNOT_VERIFY, False, AVAILABLE, ()),
        ("dead token", None, CANNOT_VERIFY, False, AVAILABLE, ()),
        ("lost then no consent", LOST, LOST, True, UNAVAILABLE, (LOST_REASON,)),
        ("lost then dead token", LOST, LOST, True, UNAVAILABLE, (LOST_REASON,)),
        ("lost then transient", LOST, LOST, True, UNAVAILABLE, (LOST_REASON,)),
        ("lost again", LOST, LOST, False, UNAVAILABLE, (LOST_REASON,)),
        ("both sources fail", None, LOST, False, UNAVAILABLE, (GRANT_PROBLEM, LOST_REASON)),
    ],
    ids=[
        "readable",
        "lost",
        "transient",
        "no-consent",
        "dead-token",
        "lost-then-revoked",
        "lost-then-dead-token",
        "lost-then-transient",
        "lost-again",
        "both-fail",
    ],
)
async def test_the_nightly_check_records_access_and_health(
    case, prior, access_status, stale_row, health_status, reason_parts
):
    """Fails if a readable object is not verified, a lost one does not suspend its agent with
    the grant named, a dead token (401) is not cannot_verify, a transient error or a
    cannot_verify lifts a lost verdict or refreshes its row, or both sources failing together
    do not join their reasons."""
    creator_notion = make_fake_notion()
    if case in ("gone", "lost again", "both sources fail"):
        creator_notion = CreatorReads(creator_notion, FakeNotFound(DB_ID))
    elif case in ("transient", "lost then transient"):
        creator_notion = CreatorReads(creator_notion, httpx.ConnectError("refused"))
    elif case in ("dead token", "lost then dead token"):
        creator_notion = CreatorReads(
            creator_notion,
            APIResponseError(
                "unauthorized", 401, "the token is no longer valid", httpx.Headers(), ""
            ),
        )
    w = await world(creator_notion, consent=case not in ("no consent", "lost then no consent"))
    if case == "both sources fail":
        break_schema(w.notion)
    key = normalize_id(DB_ID)
    if prior is not None:
        # A prior pass left this verdict; the new pass must treat it as the row to beat.
        await w.access.record(
            TENANT, "it-helper", [AccessResult(key, prior)], NOW - timedelta(days=1), {key}
        )

    await w.nightly()

    row = (await w.access.for_agent(TENANT, "it-helper")).get(key)
    assert row is not None and row.status == access_status
    assert row.checked_at == (NOW - timedelta(days=1) if stale_row else NOW)
    health = await w.health.get(TENANT, "it-helper")
    assert health.status == health_status
    assert (health.reason is None) == (not reason_parts)
    for part in reason_parts:
        assert part in health.reason


async def test_both_sources_must_pass_before_the_agent_is_available_again():
    """Fails if a passing grant check recovers an agent whose creator lost access, or a
    passing creator check recovers one whose grants fail."""
    inner = make_fake_notion()
    w = await world(inner)
    await w.nightly()
    assert (await w.health.get(TENANT, "it-helper")).status == AVAILABLE

    # The creator loses the object while the grants still pass.
    w.creator_notion = CreatorReads(inner, FakeNotFound(DB_ID))
    await w.nightly()
    row = await w.health.get(TENANT, "it-helper")
    assert (row.status, LOST_REASON in row.reason) == (UNAVAILABLE, True)

    # The worker's refresh re-checks the grants, which pass: the agent must stay down.
    await w.health_check()
    assert (await w.health.get(TENANT, "it-helper")).status == UNAVAILABLE

    # The creator can read it again and the grants pass: recovered.
    w.creator_notion = inner
    await w.nightly()
    assert (await w.health.get(TENANT, "it-helper")).status == AVAILABLE

    # Now the grants fail while the creator's access passes: down again, and back only
    # when the grants are fixed too.
    break_schema(w.notion)
    await w.nightly()
    assert (await w.health.get(TENANT, "it-helper")).status == UNAVAILABLE
    w.notion.set_schema(DB_ID, default_schema())
    await w.nightly()
    assert (await w.health.get(TENANT, "it-helper")).status == AVAILABLE


async def test_a_verdict_for_an_object_the_version_no_longer_grants_does_not_suspend():
    """Fails if a lost verdict left by an older version suspends an agent that no longer
    grants the object."""
    w = await world(make_fake_notion())
    stale = normalize_id(OTHER_DB_ID)
    await w.access.record(TENANT, "it-helper", [AccessResult(stale, LOST)], NOW, {stale})

    await w.nightly()

    assert (await w.health.get(TENANT, "it-helper")).status == AVAILABLE
    rows = await w.access.for_agent(TENANT, "it-helper")
    assert set(rows) == {normalize_id(DB_ID)}
    assert rows[normalize_id(DB_ID)].status == VERIFIED


async def test_dropping_a_creator_deletes_their_kept_sign_in():
    """Fails if dropping a consented creator keeps their sign-in, records no revoke, or leaves
    the nightly check verifying their picks."""
    w = await world(make_fake_notion())
    members = InMemoryMemberStore()
    await members.add_member(TENANT, CREATOR, "creator", "U_ADMIN")
    audit = InMemoryAuditStore()
    plane = ControlPlane(
        w.agents,
        members,
        audit,
        tenants("U_ADMIN"),
        NotionGrantChecker(lambda _tenant: w.notion),
        credentials=w.credentials,
        grant_picks=w.picks,
        creator_access=w.access,
    )
    assert await w.credentials.creator_consent(TENANT, CREATOR) is True

    await plane.drop_member(TENANT, "U_ADMIN", CREATOR)

    assert await w.credentials.creator_consent(TENANT, CREATOR) is False
    assert [(e.tool, e.target, e.identity) for e in audit.events if e.kind == "connector"] == [
        ("creator_consent_revoked", CREATOR, "user:U_ADMIN")
    ]
    await w.nightly()
    row = (await w.access.for_agent(TENANT, "it-helper"))[normalize_id(DB_ID)]
    assert row.status == CANNOT_VERIFY
    assert (await w.health.get(TENANT, "it-helper")).status == AVAILABLE


async def test_the_pass_closes_the_creator_client_it_resolved():
    """Fails if the nightly pass leaves the creator's Notion client open after the pass."""
    creator_notion = CreatorReads(make_fake_notion(), None)
    w = await world(creator_notion)

    await w.nightly()

    assert creator_notion.closed == 1


# --- the schedule: one per tenant, created once -------------------------------------------


class FakeScheduleClient:
    """Create fails when the schedule exists, as Temporal's does; update replaces it."""

    def __init__(self) -> None:
        self.schedules: dict = {}
        self.log: list[tuple[str, str]] = []

    async def create_schedule(self, schedule_id, schedule):
        if schedule_id in self.schedules:
            raise ScheduleAlreadyRunningError()
        self.schedules[schedule_id] = schedule
        self.log.append(("create", schedule_id))

    def get_schedule_handle(self, schedule_id):
        client = self

        class Handle:
            async def update(self, updater):
                client.schedules[schedule_id] = updater(None).schedule
                client.log.append(("update", schedule_id))

        return Handle()


async def test_scheduling_twice_leaves_one_schedule_per_tenant():
    """Fails if a second run duplicates the schedule, errors on it, or touches another tenant's."""
    client = FakeScheduleClient()

    first = await apply_schedule(client, TENANT)
    second = await apply_schedule(client, TENANT)
    await apply_schedule(client, "globex")

    assert (first, second) == ("created", "updated")
    assert sorted(client.schedules) == sorted(
        [ids.creator_access_schedule_id(TENANT), ids.creator_access_schedule_id("globex")]
    )
    assert client.log == [
        ("create", ids.creator_access_schedule_id(TENANT)),
        ("update", ids.creator_access_schedule_id(TENANT)),
        ("create", ids.creator_access_schedule_id("globex")),
    ]
    assert client.schedules[ids.creator_access_schedule_id(TENANT)].action.id == (
        ids.creator_access_workflow_id(TENANT)
    )
