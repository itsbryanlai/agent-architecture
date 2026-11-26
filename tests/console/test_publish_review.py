"""What an admin approves is what runs, and a change and its audit event are one commit.

Fakes only: in-memory stores, a fake Notion, a fake Slack sign-in. The SQL side (the trigger,
the column) is exercised in the Postgres tier.
"""

import asyncio
import re
from datetime import UTC, datetime

import httpx
import pytest

from agentos.agent_store import AgentStoreError, InMemoryAgentStore, VersionChanged
from agentos.audience_pauses import InMemoryPauseStore
from agentos.audit import InMemoryAuditStore
from agentos.budgets import InMemoryBudgetStore
from agentos.control import ControlPlane, Forbidden
from agentos.control_api import create_app
from agentos.grant_checker import NotionGrantChecker
from agentos.members import InMemoryMemberStore
from agentos.switches import InMemorySwitchStore, set_switch
from tests.conftest import TENANT, make_fake_notion
from tests.helpers.agents import helper_block
from tests.helpers.console_harness import (
    ADMIN2,
    OWNER,
    ROOT,
    ConsoleHarness,
    csrf_token,
    post,
    using,
)
from tests.helpers.control_api import JSON
from tests.helpers.control_plane_tenants import tenants

API = f"/tenants/{TENANT}"
VERSION = f"/ui/{TENANT}/agents/it-helper/versions/1"


class Admin:
    """The identity of the control API in these tests: whoever is set."""

    def __init__(self, user: str):
        self.user = user

    async def actor(self, request) -> str:
        return self.user


@pytest.fixture
async def h():
    harness = ConsoleHarness()
    await harness.seed_members()
    return harness


def api_client(h: ConsoleHarness, user: str = ROOT) -> httpx.AsyncClient:
    app = create_app(h.plane, h.tenants, Admin(user))
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8100", headers=JSON)


async def hash_of(h, version: int = 1) -> str:
    return (await h.agents.get_version(TENANT, "it-helper", version)).definition_sha256


async def status_of(h, version: int = 1) -> str:
    return (await h.agents.get_version(TENANT, "it-helper", version)).status


async def replace_pending(plane, **changes) -> None:
    """The creator withdraws version 1, edits it and submits the same number again."""
    await plane.withdraw_version(TENANT, OWNER, "it-helper", 1)
    edited = helper_block() | {"actions": []} | changes
    await plane.edit_draft(TENANT, OWNER, "it-helper", 1, edited)
    await plane.submit_version(TENANT, OWNER, "it-helper", 1)


# --- S1: the hash the admin saw ----------------------------------------------------------


async def publish_through(surface: str, h: ConsoleHarness, sha: str | None):
    """An admin's publish request from the API or from the version page's form."""
    if surface == "api":
        body = (
            {}
            if sha is None
            else {
                "definition_sha256": sha,
                "acknowledge_widening": True,
                "reviewed_published": "none",
            }
        )
        async with api_client(h) as c:
            return await c.post(f"{API}/agents/it-helper/versions/1/publish", json=body)
    c = await h.signed_in(ROOT)
    async with using(c):
        fields = (
            {}
            if sha is None
            else {"definition_sha256": sha, "widening": "yes", "reviewed_published": "none"}
        )
        return await post(c, f"{VERSION}/publish", await csrf_token(c), **fields)


@pytest.mark.parametrize("surface", ["api", "console"])
async def test_publish_names_the_version_the_admin_saw(h, surface):
    """Fails if a version withdrawn, edited and resubmitted under the same number is published
    by an admin who was shown the old content, or if the right hash is refused."""
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
    shown = await hash_of(h)
    await replace_pending(h.plane, max_steps=4)
    now = await hash_of(h)
    assert now != shown and await status_of(h) == "pending"

    stale = await publish_through(surface, h, shown)

    if surface == "console":
        assert stale.status_code == 303
        assert stale.headers["location"] == f"{VERSION}?refused=version-changed"
        c = await h.signed_in(ROOT)
        async with using(c):
            page = await c.get(stale.headers["location"])
        assert page.status_code == 200
        assert "changed since you opened it" in page.text
    else:
        assert stale.status_code == 409
        assert "changed since you opened it" in stale.text
    assert await status_of(h) == "pending"
    assert "agent_publish" not in [e.kind for e in h.audit.events]

    current = await publish_through(surface, h, now)

    assert current.status_code == (200 if surface == "api" else 303)
    stored = await h.agents.get_version(TENANT, "it-helper", 1)
    assert stored.status == "published" and stored.definition["max_steps"] == 4
    event = next(e for e in h.audit.events if e.kind == "agent_publish")
    assert event.input_sha256 == now


@pytest.mark.parametrize("surface", ["api", "console"])
async def test_a_publish_that_names_no_hash_publishes_nothing(h, surface):
    """Fails if a publish request without the hash of the version shown publishes whatever
    is pending when it lands."""
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)

    response = await publish_through(surface, h, None)

    if surface == "console":
        assert response.status_code == 303
    else:
        assert response.status_code in (409, 422)
    assert await status_of(h) == "pending"


async def test_the_version_page_and_record_carry_the_hash_the_publish_asks_for(h):
    """Fails if the console's publish form or the API's version record shows a hash other
    than the one a publish must name, so no honest admin could publish."""
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
    wanted = await hash_of(h)
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(VERSION)).text
    form = re.search(r'name="definition_sha256" value="([0-9a-f]{64})"', page)
    assert form and form.group(1) == wanted
    async with api_client(h) as api:
        record = (await api.get(f"{API}/agents/it-helper/versions/1")).json()
    assert record["definition_sha256"] == wanted


async def test_the_store_keeps_the_hash_of_what_was_submitted():
    """Fails if a stale or foreign hash publishes, if the hash moves on publish, or if a refused
    publish supersedes the version already live."""
    store = InMemoryAgentStore()
    await store.create_agent(TENANT, "it-helper", helper_block(), OWNER)
    draft = await store.get_version(TENANT, "it-helper", 1)
    assert draft.definition_sha256 is None
    first = await store.submit(TENANT, "it-helper", 1, OWNER)
    await store.withdraw(TENANT, "it-helper", 1)
    assert (await store.get_version(TENANT, "it-helper", 1)).definition_sha256 is None
    await store.replace_draft(TENANT, "it-helper", 1, helper_block() | {"max_steps": 3})
    second = await store.submit(TENANT, "it-helper", 1, OWNER)
    assert second.definition_sha256 != first.definition_sha256

    with pytest.raises(VersionChanged):
        await store.publish(TENANT, "it-helper", 1, ROOT, reviewed_sha256=first.definition_sha256)
    assert (await store.get_version(TENANT, "it-helper", 1)).status == "pending"
    published = await store.publish(
        TENANT, "it-helper", 1, ROOT, reviewed_sha256=second.definition_sha256
    )

    assert published.definition_sha256 == second.definition_sha256
    assert published.status == "published"


class ReplacesWhileChecking:
    """A grant checker during whose check the creator replaces the pending version."""

    def __init__(self, plane_for):
        self.plane_for = plane_for
        self.done = False

    async def check(self, tenant_id, agent):
        if not self.done:
            self.done = True
            await replace_pending(self.plane_for(), max_steps=2)
        return []


async def test_a_version_replaced_during_the_checks_is_not_published():
    """Fails if the version replaced while the publish's checks ran is published, though the
    checks (and the admin's reading) were of the old content."""
    agents, members, audit = InMemoryAgentStore(), InMemoryMemberStore(), InMemoryAuditStore()
    await members.add_member(TENANT, OWNER, "creator", ROOT)
    plane = ControlPlane(
        agents, members, audit, tenants(ROOT), ReplacesWhileChecking(lambda: plane)
    )
    await plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    await plane.submit_version(TENANT, OWNER, "it-helper", 1)

    with pytest.raises(VersionChanged):
        await plane.publish_version(TENANT, ROOT, "it-helper", 1)

    assert (await agents.get_version(TENANT, "it-helper", 1)).status == "pending"
    assert "agent_publish" not in [e.kind for e in audit.events]


# --- S4: a change and its audit event are one commit ------------------------------------


class World:
    """A control plane on in-memory stores, with the stores in hand to compare."""

    def __init__(self, admins=(ROOT,)):
        self.agents = InMemoryAgentStore()
        self.members = InMemoryMemberStore()
        self.audit = InMemoryAuditStore()
        self.budgets = InMemoryBudgetStore()
        self.switches = InMemorySwitchStore()
        self.pauses = InMemoryPauseStore()
        checker = NotionGrantChecker(lambda tenant_id: make_fake_notion())
        self.plane = ControlPlane(
            self.agents,
            self.members,
            self.audit,
            tenants(*admins),
            checker,
            budgets=self.budgets,
            switches=self.switches,
            pauses=self.pauses,
        )

    def state(self) -> list:
        stores = (self.agents, self.members, self.budgets, self.switches, self.pauses)
        return [store.snapshot() for store in stores]

    async def draft(self):
        await self.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())

    async def pending(self):
        await self.draft()
        await self.plane.submit_version(TENANT, OWNER, "it-helper", 1)

    async def published(self):
        await self.pending()
        await self.plane.publish_version(TENANT, ROOT, "it-helper", 1)

    async def published_and_paused(self):
        await self.published()
        await self.pauses.open_pause(
            TENANT, "it-helper", "C0CHAN", "guest_joined", datetime.now(UTC)
        )


async def nothing(world: World) -> None:
    """A change that needs no set-up."""


# case: (event kind, what to set up, the change)
CHANGES = {
    "add_agent": (
        "agent_create",
        nothing,
        lambda w: w.plane.add_agent(TENANT, OWNER, "it-helper", helper_block()),
    ),
    "new_draft": (
        "agent_draft",
        lambda w: w.published(),
        lambda w: w.plane.new_draft(TENANT, OWNER, "it-helper"),
    ),
    "edit_draft": (
        "agent_edit",
        lambda w: w.draft(),
        lambda w: w.plane.edit_draft(
            TENANT, OWNER, "it-helper", 1, helper_block() | {"actions": [], "max_steps": 3}
        ),
    ),
    "submit": (
        "agent_submit",
        lambda w: w.draft(),
        lambda w: w.plane.submit_version(TENANT, OWNER, "it-helper", 1),
    ),
    "withdraw": (
        "agent_withdraw",
        lambda w: w.pending(),
        lambda w: w.plane.withdraw_version(TENANT, OWNER, "it-helper", 1),
    ),
    "reject": (
        "agent_reject",
        lambda w: w.pending(),
        lambda w: w.plane.reject_version(TENANT, ROOT, "it-helper", 1),
    ),
    "publish": (
        "agent_publish",
        lambda w: w.pending(),
        lambda w: w.plane.publish_version(TENANT, ROOT, "it-helper", 1),
    ),
    "invite_member": (
        "member_add",
        nothing,
        lambda w: w.plane.invite_member(TENANT, ROOT, "U_NEW", "creator"),
    ),
    "change_role": (
        "member_role",
        lambda w: w.members.add_member(TENANT, "U_NEW", "creator", ROOT),
        lambda w: w.plane.change_role(TENANT, ROOT, "U_NEW", "admin"),
    ),
    "drop_member": (
        "member_remove",
        lambda w: w.members.add_member(TENANT, "U_NEW", "creator", ROOT),
        lambda w: w.plane.drop_member(TENANT, ROOT, "U_NEW"),
    ),
    "set_budget": (
        "budget_set",
        nothing,
        lambda w: w.plane.set_budget(TENANT, ROOT, None, "10.00"),
    ),
    "remove_budget": (
        "budget_set",
        lambda w: w.plane.set_budget(TENANT, ROOT, None, "10.00"),
        lambda w: w.plane.remove_budget(TENANT, ROOT, None),
    ),
    "turn_off": (
        "switch_off",
        nothing,
        lambda w: w.plane.turn_off(TENANT, ROOT, "writes"),
    ),
    "turn_on": (
        "switch_on",
        lambda w: w.plane.turn_off(TENANT, ROOT, "writes"),
        lambda w: w.plane.turn_on(TENANT, ROOT, "writes"),
    ),
    "resume_channel": (
        "audience_resume",
        lambda w: w.published_and_paused(),
        lambda w: w.plane.resume_channel(TENANT, OWNER, "it-helper", "C0CHAN"),
    ),
}


@pytest.mark.parametrize("change", sorted(CHANGES))
async def test_a_failed_audit_insert_rolls_the_change_back_and_the_retry_records_one_event(change):
    """Fails if a change stays committed when its audit event could not be written, so the
    retry is refused as already done and the change is never audited."""
    kind, setup, act = CHANGES[change]
    w = World()
    await w.members.add_member(TENANT, OWNER, "creator", ROOT)
    await setup(w)
    before, events = w.state(), len(w.audit.events)

    w.audit.fail_next_records(1)
    with pytest.raises(RuntimeError):
        await act(w)

    assert w.state() == before
    assert len(w.audit.events) == events

    await act(w)

    assert [e.kind for e in w.audit.events[events:] if e.kind != "widening_confirm"] == [kind]


async def test_the_operators_switch_command_is_one_commit_with_its_audit_event():
    """Fails if a switch the host operator's command turns off stays off when the audit event
    could not be written, so the retry is refused as already off and is never audited."""
    switches, audit, agents = InMemorySwitchStore(), InMemoryAuditStore(), InMemoryAgentStore()

    async def turn_off():
        return await set_switch(
            switches,
            audit,
            agents,
            TENANT,
            "writes",
            None,
            off=True,
            actor="operator:ops",
            identity="operator:ops",
        )

    audit.fail_next_records(1)
    with pytest.raises(RuntimeError):
        await turn_off()
    assert await switches.list(TENANT) == []

    assert await turn_off() is True
    assert [e.kind for e in audit.events] == ["switch_off"]


# --- the last admin ----------------------------------------------------------------------


class SlowCount(InMemoryMemberStore):
    """Lets the other call run between the count of admins and the change that follows."""

    async def list_members(self, tenant_id, conn=None):
        rows = await super().list_members(tenant_id)
        for _ in range(20):
            await asyncio.sleep(0)
        return rows


async def test_two_admins_demoting_each_other_leave_one():
    """Fails if two concurrent demotions each see the other admin still in place and leave the
    tenant with no admin."""
    members, audit = SlowCount(), InMemoryAuditStore()
    plane = ControlPlane(
        InMemoryAgentStore(),
        members,
        audit,
        tenants(),
        NotionGrantChecker(lambda tenant_id: make_fake_notion()),
    )
    await members.add_member(TENANT, ROOT, "admin", "setup")
    await members.add_member(TENANT, ADMIN2, "admin", "setup")

    results = await asyncio.gather(
        plane.change_role(TENANT, ROOT, ADMIN2, "creator"),
        plane.change_role(TENANT, ADMIN2, ROOT, "creator"),
        return_exceptions=True,
    )

    assert sum(isinstance(r, Forbidden) for r in results) == 1
    admins = [m.user_id for m in await members.list_members(TENANT) if m.role == "admin"]
    assert len(admins) == 1
    assert [e.kind for e in audit.events] == ["member_role"]


# --- a retried new draft -----------------------------------------------------------------


async def new_draft_twice(surface: str, h: ConsoleHarness) -> None:
    """The same request sent twice: the same plane request ID, API header or form token."""
    if surface == "plane":
        for _ in range(2):
            await h.plane.new_draft(TENANT, OWNER, "it-helper", None, "retry-1")
    elif surface == "api":
        async with api_client(h, OWNER) as c:
            for _ in range(2):
                sent = await c.post(
                    f"{API}/agents/it-helper/drafts", headers={"Idempotency-Key": "retry-1"}
                )
                assert sent.status_code == 201
    else:
        c = await h.signed_in(OWNER)
        async with using(c):
            token = await csrf_token(c)
            for _ in range(2):
                sent = await post(
                    c,
                    f"/ui/{TENANT}/agents/it-helper/drafts",
                    token,
                    request_id="0123456789abcdef0123456789abcdef",
                )
                assert sent.status_code == 303


@pytest.mark.parametrize("surface", ["plane", "api", "console"])
async def test_a_retried_new_draft_makes_one_draft_and_one_event(h, surface):
    """Fails if sending the same new-draft request twice creates a second draft or records a
    second event."""
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
    await h.plane.publish_version(TENANT, ROOT, "it-helper", 1)

    await new_draft_twice(surface, h)

    assert [r.version for r in await h.agents.versions(TENANT, "it-helper")] == [2, 1]
    assert [e.kind for e in h.audit.events].count("agent_draft") == 1


async def test_a_different_request_makes_a_different_draft(h):
    """Fails if distinct new-draft requests are collapsed into one draft."""
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
    await h.plane.publish_version(TENANT, ROOT, "it-helper", 1)

    await h.plane.new_draft(TENANT, OWNER, "it-helper", None, "one")
    await h.plane.new_draft(TENANT, OWNER, "it-helper", None, "two")

    assert len(await h.agents.versions(TENANT, "it-helper")) == 3


async def test_a_request_id_cannot_be_reused_for_another_agent(h):
    """Fails if a request ID already recorded for one agent's draft returns that draft to a
    request about another agent."""
    for agent in ("it-helper", "other"):
        await h.plane.add_agent(TENANT, OWNER, agent, helper_block())
        await h.plane.submit_version(TENANT, OWNER, agent, 1)
        await h.plane.publish_version(TENANT, ROOT, agent, 1)
    await h.plane.new_draft(TENANT, OWNER, "it-helper", None, "same")

    with pytest.raises(AgentStoreError):
        await h.plane.new_draft(TENANT, OWNER, "other", None, "same")
