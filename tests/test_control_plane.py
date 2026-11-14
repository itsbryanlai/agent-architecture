"""The control plane: roles, the rules table, the audit trail and the stale-publish guard."""

import ast
from pathlib import Path

import pytest

from agentos.agent_store import AgentStoreError, InMemoryAgentStore, PostgresAgentStore
from agentos.audit import InMemoryAuditStore, canonical_sha256
from agentos.config import TenantsConfig, parse_agents_config
from agentos.control import ControlPlane, Forbidden
from agentos.members import InMemoryMemberStore, MemberError
from tests.conftest import TENANT, agents_dict
from tests.test_agent_store import helper_block
from tests.test_row_level_security import AGENT_ROW, FakeConnection, FakePool

SRC = Path(__file__).resolve().parent.parent / "src" / "agentos"

ROOT = "U_ROOT"  # an admin from the tenant file
ADMIN2 = "U_ADMIN2"  # an admin added through the app
OWNER = "U_OWNER"  # a creator who creates the agent under test
OTHER = "U_OTHER"  # a creator who did not
OUTSIDER = "U_OUTSIDER"  # no role
PROMPT = "You help employees with IT requests."


def tenants(*admins: str) -> TenantsConfig:
    raw = agents_dict()
    raw["admins"] = list(admins)
    return TenantsConfig.of(parse_agents_config(raw))


async def make_plane(admins: tuple[str, ...] = (ROOT,), seed: bool = True):
    agents, members, audit = InMemoryAgentStore(), InMemoryMemberStore(), InMemoryAuditStore()
    if seed:
        await members.add_member(TENANT, ADMIN2, "admin", ROOT)
        await members.add_member(TENANT, OWNER, "creator", ROOT)
        await members.add_member(TENANT, OTHER, "creator", ROOT)
    return ControlPlane(agents, members, audit, tenants(*admins)), agents, members, audit


async def refused(coro):
    with pytest.raises(Forbidden):
        await coro


def kinds(audit: InMemoryAuditStore) -> list[str]:
    return [e.kind for e in audit.events]


async def pending_version(plane, actor=OWNER):
    await plane.add_agent(TENANT, actor, "it-helper", helper_block())
    await plane.submit_version(TENANT, actor, "it-helper", 1)


# --- roles -------------------------------------------------------------------------


async def test_effective_role_is_config_then_row_then_nothing():
    plane, _, members, _ = await make_plane()
    assert await plane.role_of(TENANT, ROOT) == "admin"
    assert await plane.role_of(TENANT, ADMIN2) == "admin"
    assert await plane.role_of(TENANT, OWNER) == "creator"
    assert await plane.role_of(TENANT, OUTSIDER) is None
    # A row for a config admin does not lower the role.
    await members.add_member(TENANT, ROOT, "creator", ADMIN2)
    assert await plane.role_of(TENANT, ROOT) == "admin"
    # Another tenant's rows and config give nothing here.
    assert await plane.role_of("globex", OWNER) is None


async def test_a_tenant_with_no_admins_can_publish_nothing():
    plane, _, members, _ = await make_plane(admins=(), seed=False)
    await members.add_member(TENANT, OWNER, "creator", "x")
    await pending_version(plane)
    await refused(plane.publish_version(TENANT, OWNER, "it-helper", 1))
    await refused(plane.publish_version(TENANT, OUTSIDER, "it-helper", 1))


# --- reads --------------------------------------------------------------------------


async def test_admins_and_creators_read_agents_and_versions_and_others_cannot():
    plane, *_ = await make_plane()
    await plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    for actor in (ROOT, ADMIN2, OWNER, OTHER):
        assert [a.agent_id for a in await plane.list_agents(TENANT, actor)] == ["it-helper"]
        assert [v.version for v in await plane.versions(TENANT, actor, "it-helper")] == [1]
        assert (await plane.get_version(TENANT, actor, "it-helper", 1)).status == "draft"
    await refused(plane.list_agents(TENANT, OUTSIDER))
    await refused(plane.versions(TENANT, OUTSIDER, "it-helper"))
    await refused(plane.get_version(TENANT, OUTSIDER, "it-helper", 1))


# --- agent lifecycle ----------------------------------------------------------------


async def test_anyone_with_a_role_creates_an_agent_and_is_its_creator():
    plane, agents, *_ = await make_plane()
    record = await plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    assert (record.version, record.status, record.created_by) == (1, "draft", OWNER)
    assert (await agents.get_agent(TENANT, "it-helper")).created_by == OWNER
    await plane.add_agent(TENANT, ADMIN2, "other-agent", helper_block())
    await refused(plane.add_agent(TENANT, OUTSIDER, "third", helper_block()))
    assert await agents.get_agent(TENANT, "third") is None


@pytest.mark.parametrize(
    ("actor", "allowed"),
    [(OWNER, True), (ROOT, True), (ADMIN2, True), (OTHER, False), (OUTSIDER, False)],
)
async def test_draft_edit_submit_and_withdraw_are_for_the_agents_creator_or_an_admin(
    actor, allowed
):
    plane, agents, *_ = await make_plane()
    await plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    changed = {**helper_block(), "name": "Renamed"}

    async def attempt():
        await plane.edit_draft(TENANT, actor, "it-helper", 1, changed)
        await plane.submit_version(TENANT, actor, "it-helper", 1)
        await plane.withdraw_version(TENANT, actor, "it-helper", 1)
        await plane.new_draft(TENANT, actor, "it-helper", from_version=1)

    if allowed:
        await attempt()
        assert [v.status for v in await agents.versions(TENANT, "it-helper")] == ["draft", "draft"]
    else:
        await refused(plane.edit_draft(TENANT, actor, "it-helper", 1, changed))
        await refused(plane.submit_version(TENANT, actor, "it-helper", 1))
        await refused(plane.new_draft(TENANT, actor, "it-helper", from_version=1))
        assert [v.status for v in await agents.versions(TENANT, "it-helper")] == ["draft"]


async def test_only_the_creator_or_an_admin_withdraws_a_pending_version():
    plane, agents, *_ = await make_plane()
    await pending_version(plane)
    await refused(plane.withdraw_version(TENANT, OTHER, "it-helper", 1))
    await refused(plane.withdraw_version(TENANT, OUTSIDER, "it-helper", 1))
    assert (await agents.get_version(TENANT, "it-helper", 1)).status == "pending"
    assert (await plane.withdraw_version(TENANT, OWNER, "it-helper", 1)).status == "draft"


async def test_reject_and_publish_are_admin_only():
    plane, agents, *_ = await make_plane()
    await pending_version(plane)
    for actor in (OWNER, OTHER, OUTSIDER):
        await refused(plane.reject_version(TENANT, actor, "it-helper", 1))
        await refused(plane.publish_version(TENANT, actor, "it-helper", 1))
    assert (await agents.get_version(TENANT, "it-helper", 1)).status == "pending"

    assert (await plane.reject_version(TENANT, ADMIN2, "it-helper", 1)).status == "draft"
    await plane.submit_version(TENANT, OWNER, "it-helper", 1)
    done = await plane.publish_version(TENANT, ROOT, "it-helper", 1)
    assert (done.status, done.approved_by) == ("published", ROOT)
    assert (await agents.published(TENANT, "it-helper")).version == 1


async def test_an_admin_approves_their_own_submission_and_the_event_shows_both_ids():
    plane, _, _, audit = await make_plane()
    await plane.add_agent(TENANT, ROOT, "it-helper", helper_block())
    await plane.submit_version(TENANT, ROOT, "it-helper", 1)
    await plane.publish_version(TENANT, ROOT, "it-helper", 1)
    event = audit.events[-1]
    assert event.kind == "agent_publish"
    assert (event.requester_user_id, event.approver_user_id) == (ROOT, ROOT)

    await pending_version_for(plane, "second", OWNER)
    await plane.publish_version(TENANT, ADMIN2, "second", 1)
    event = audit.events[-1]
    assert (event.requester_user_id, event.approver_user_id) == (OWNER, ADMIN2)
    assert event.identity == f"user:{ADMIN2}"


async def pending_version_for(plane, agent_id, actor):
    await plane.add_agent(TENANT, actor, agent_id, helper_block())
    await plane.submit_version(TENANT, actor, agent_id, 1)


# --- stale publish and rollback -------------------------------------------------------


async def three_versions(store, publish_last_first=True):
    """v1 published, v2 and v3 pending."""
    first = await store.create_agent(TENANT, "it-helper", helper_block(), OWNER)
    await store.submit(TENANT, "it-helper", first.version, OWNER)
    await store.publish(TENANT, "it-helper", first.version, ROOT)
    for _ in range(2):
        draft = await store.create_draft(TENANT, "it-helper", OWNER)
        await store.submit(TENANT, "it-helper", draft.version, OWNER)


async def test_in_memory_store_refuses_to_publish_a_version_older_than_the_published_one():
    store = InMemoryAgentStore()
    await three_versions(store)
    await store.publish(TENANT, "it-helper", 3, ROOT)
    with pytest.raises(AgentStoreError, match="older than the published version 3"):
        await store.publish(TENANT, "it-helper", 2, ROOT)
    assert [v.status for v in await store.versions(TENANT, "it-helper")] == [
        "published",
        "pending",
        "superseded",
    ]


class ScriptedConnection(FakeConnection):
    """Answers the publish queries: the target is version 2 and version 3 is published."""

    async def execute(self, sql, params=None):
        flat = " ".join(sql.split())
        await super().execute(sql, params)
        if "AND version = %s" in flat:
            self.row = (*AGENT_ROW[:2], 2, *AGENT_ROW[3:])
        elif "status = 'published'" in flat:
            self.row = (*AGENT_ROW[:2], 3, "published", *AGENT_ROW[4:])
        return await super().execute("SELECT 1")


class ScriptedPool(FakePool):
    def connection(self):
        pool = self

        class Ctx:
            async def __aenter__(self_inner):
                pool.taken += 1
                return ScriptedConnection(pool.events)

            async def __aexit__(self_inner, *exc):
                return False

        return Ctx()


async def test_postgres_store_refuses_to_publish_a_version_older_than_the_published_one():
    store = PostgresAgentStore(ScriptedPool())
    with pytest.raises(AgentStoreError, match="older than the published version 3"):
        await store.publish(TENANT, "it-helper", 2, ROOT)


async def test_rollback_is_a_new_draft_from_the_old_version_then_submit_and_publish():
    plane, agents, *_ = await make_plane()
    await plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    await plane.submit_version(TENANT, OWNER, "it-helper", 1)
    await plane.publish_version(TENANT, ROOT, "it-helper", 1)
    v2 = await plane.new_draft(TENANT, OWNER, "it-helper")
    await plane.edit_draft(TENANT, OWNER, "it-helper", v2.version, {**helper_block(), "name": "V2"})
    await plane.submit_version(TENANT, OWNER, "it-helper", 2)
    await plane.publish_version(TENANT, ROOT, "it-helper", 2)
    assert (await agents.published(TENANT, "it-helper")).name == "V2"

    back = await plane.new_draft(TENANT, OWNER, "it-helper", from_version=1)
    assert back.version == 3
    await plane.submit_version(TENANT, OWNER, "it-helper", 3)
    await plane.publish_version(TENANT, ROOT, "it-helper", 3)
    live = await agents.published(TENANT, "it-helper")
    assert (live.version, live.name) == (3, "IT helper")


# --- members --------------------------------------------------------------------------


async def test_only_an_admin_manages_members():
    plane, _, members, _ = await make_plane()
    for actor in (OWNER, OUTSIDER):
        await refused(plane.invite_member(TENANT, actor, "U_NEW", "creator"))
        await refused(plane.change_role(TENANT, actor, OTHER, "admin"))
        await refused(plane.drop_member(TENANT, actor, OTHER))
        await refused(plane.list_members(TENANT, actor))

    await plane.invite_member(TENANT, ADMIN2, "U_NEW", "creator")
    await plane.change_role(TENANT, ROOT, "U_NEW", "admin")
    assert (await plane.role_of(TENANT, "U_NEW")) == "admin"
    await plane.drop_member(TENANT, ROOT, "U_NEW")
    assert await members.get_member(TENANT, "U_NEW") is None
    assert {m.user_id for m in await plane.list_members(TENANT, ROOT)} == {ADMIN2, OWNER, OTHER}


async def test_a_config_admin_cannot_be_removed_or_demoted_or_added_as_a_row():
    plane, _, members, _ = await make_plane()
    await refused(plane.drop_member(TENANT, ADMIN2, ROOT))
    await refused(plane.change_role(TENANT, ADMIN2, ROOT, "creator"))
    with pytest.raises(MemberError):
        await plane.invite_member(TENANT, ADMIN2, ROOT, "creator")
    assert await members.get_member(TENANT, ROOT) is None
    assert await plane.role_of(TENANT, ROOT) == "admin"
    # Not even by the config admin themself.
    await refused(plane.drop_member(TENANT, ROOT, ROOT))


async def test_the_last_admin_cannot_remove_or_demote_themself():
    plane, _, members, audit = await make_plane(admins=(), seed=False)
    await members.add_member(TENANT, ADMIN2, "admin", "setup")
    await refused(plane.drop_member(TENANT, ADMIN2, ADMIN2))
    await refused(plane.change_role(TENANT, ADMIN2, ADMIN2, "creator"))
    assert (await members.get_member(TENANT, ADMIN2)).role == "admin"
    assert audit.events == []

    # With a second admin, one may leave.
    await members.add_member(TENANT, "U_SECOND", "admin", "setup")
    await plane.drop_member(TENANT, ADMIN2, ADMIN2)
    assert await members.get_member(TENANT, ADMIN2) is None


async def test_member_changes_that_name_nobody_or_repeat_are_errors_not_forbidden():
    plane, *_ = await make_plane()
    with pytest.raises(MemberError):
        await plane.invite_member(TENANT, ROOT, OWNER, "creator")
    with pytest.raises(MemberError):
        await plane.drop_member(TENANT, ROOT, "U_NOBODY")
    with pytest.raises(MemberError):
        await plane.change_role(TENANT, ROOT, OWNER, "creator")
    with pytest.raises(MemberError):
        await plane.invite_member(TENANT, ROOT, "U_NEW", "owner")


# --- audit ----------------------------------------------------------------------------


async def test_only_an_admin_reads_the_audit_log():
    plane, _, _, audit = await make_plane()
    await plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    for actor in (OWNER, OUTSIDER):
        await refused(plane.audit_log(TENANT, actor))
    events = await plane.audit_log(TENANT, ADMIN2)
    assert [e.kind for e in events] == ["agent_create"]
    assert events == await audit.list(TENANT)


async def test_each_action_writes_one_event_with_ids_and_a_hash_and_no_definition_text():
    plane, _, _, audit = await make_plane()
    definition = helper_block()
    steps = [
        ("agent_create", plane.add_agent(TENANT, OWNER, "it-helper", definition)),
        ("agent_submit", plane.submit_version(TENANT, OWNER, "it-helper", 1)),
        ("agent_withdraw", plane.withdraw_version(TENANT, OWNER, "it-helper", 1)),
        ("agent_draft", plane.edit_draft(TENANT, OWNER, "it-helper", 1, definition)),
        ("agent_submit", plane.submit_version(TENANT, OWNER, "it-helper", 1)),
        ("agent_reject", plane.reject_version(TENANT, ROOT, "it-helper", 1)),
        ("agent_submit", plane.submit_version(TENANT, OWNER, "it-helper", 1)),
        ("agent_publish", plane.publish_version(TENANT, ROOT, "it-helper", 1)),
        ("agent_draft", plane.new_draft(TENANT, OWNER, "it-helper")),
        ("member_add", plane.invite_member(TENANT, ROOT, "U_NEW", "creator")),
        ("member_role", plane.change_role(TENANT, ROOT, "U_NEW", "admin")),
        ("member_remove", plane.drop_member(TENANT, ROOT, "U_NEW")),
    ]
    for count, (kind, step) in enumerate(steps, start=1):
        await step
        assert len(audit.events) == count, kind
        assert audit.events[-1].kind == kind

    for event in audit.events:
        assert event.tenant_id == TENANT
        assert event.event_key.startswith(f"{TENANT}:audit:control:{event.kind}:")
        assert PROMPT not in repr(event)
        if event.kind.startswith("agent_"):
            assert (event.agent_id, event.input_sha256) == (
                "it-helper",
                canonical_sha256(definition),
            )
        else:
            assert (event.agent_id, event.agent_version, event.input_sha256) == (None, None, None)
    assert len({e.event_key for e in audit.events}) == len(audit.events)

    member_events = audit.events[-3:]
    assert [(e.target, e.tool) for e in member_events] == [
        ("U_NEW", "role:creator"),
        ("U_NEW", "role:admin"),
        ("U_NEW", None),
    ]
    assert audit.events[0].requester_user_id == OWNER
    assert audit.events[0].identity == f"user:{OWNER}"
    assert audit.events[-1].identity == f"user:{ROOT}"


async def test_a_refused_or_failed_action_writes_no_event():
    plane, _, _, audit = await make_plane()
    await refused(plane.add_agent(TENANT, OUTSIDER, "it-helper", helper_block()))
    with pytest.raises(AgentStoreError):
        await plane.submit_version(TENANT, ROOT, "nope", 1)
    assert audit.events == []


# --- every write goes through the control plane ------------------------------------------

STORE_WRITES = {
    "create_agent",
    "create_draft",
    "replace_draft",
    "submit",
    "withdraw",
    "publish",
    "import_agents",
    "add_member",
    "set_member_role",
    "remove_member",
}
MAY_CALL_WRITES = {"control.py", "agent_store.py", "members.py"}


def test_only_the_control_plane_and_the_stores_call_the_store_writes():
    offenders = []
    for path in sorted(SRC.rglob("*.py")):
        if path.name in MAY_CALL_WRITES:
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in STORE_WRITES
            ):
                offenders.append(f"{path.relative_to(SRC)}:{node.lineno} {node.func.attr}")
    assert offenders == []


def test_the_scan_finds_the_calls_it_is_looking_for():
    calls = {
        node.func.attr
        for node in ast.walk(ast.parse((SRC / "control.py").read_text()))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert STORE_WRITES - {"import_agents"} <= calls
