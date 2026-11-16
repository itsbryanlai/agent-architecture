"""The approval hash, the unit of work and the tenant lock, against the real schema."""

from __future__ import annotations

import asyncio

import pytest
from psycopg import errors

from agentos.agent_store import AgentStoreError, PostgresAgentStore, VersionChanged
from agentos.audit import PostgresAuditStore, canonical_sha256
from agentos.config import TenantsConfig, parse_agents_config
from agentos.control import ControlPlane, Forbidden
from agentos.grant_checker import NotionGrantChecker
from agentos.members import PostgresMemberStore
from agentos.models import AuditEvent
from agentos.schema_version import EXPECTED_VERSIONS
from agentos.stores import tenant_transaction
from agentos.switches import PostgresSwitchStore
from agentos.unit_of_work import PostgresUnitOfWork
from tests.conftest import agents_dict, make_fake_notion
from tests.helpers.agents import helper_block
from tests.helpers.pg_seed import seed_version
from tests.pg.conftest import MIGRATIONS, Databases, TestDatabase, load_migrate


def plane_for(pool, tenant: str, admins=("U_ROOT",), **parts) -> ControlPlane:
    raw = agents_dict()
    raw["tenant_id"], raw["admins"] = tenant, list(admins)
    return ControlPlane(
        PostgresAgentStore(pool),
        parts.pop("members", PostgresMemberStore(pool)),
        parts.pop("audit", PostgresAuditStore(pool)),
        TenantsConfig.of(parse_agents_config(raw)),
        NotionGrantChecker(lambda tenant_id: make_fake_notion()),
        switches=PostgresSwitchStore(pool),
        unit_of_work=parts.pop("uow", PostgresUnitOfWork(pool)),
    )


# --- items 2 to 6: the column, the CHECK and the trigger -----------------------------------


def test_a_draft_cannot_carry_a_hash(db: TestDatabase, tenant: str):
    """Fails if a draft row can be inserted with a hash, or a submitted one without."""
    with db.owner() as conn:
        seed_version(conn, tenant, "aa", "draft")
        with pytest.raises(errors.CheckViolation):
            conn.execute(
                "INSERT INTO agent_versions (tenant_id, agent_id, version, status, definition, "
                "created_by, definition_sha256) VALUES (%s, 'aa', 2, 'draft', '{}', 'u', 'h')",
                (tenant,),
            )
        with pytest.raises(errors.CheckViolation):
            conn.execute(
                "INSERT INTO agent_versions (tenant_id, agent_id, version, status, definition, "
                "created_by, submitted_by) VALUES (%s, 'aa', 3, 'pending', '{}', 'u', 'u')",
                (tenant,),
            )


HASH_CASES = [
    # (start, change, expected error or None)
    ("pending", "definition_sha256 = 'x'", errors.RaiseException),
    ("draft", "definition_sha256 = 'x'", errors.RaiseException),
    ("draft", "status = 'pending', definition_sha256 = 'x'", None),
    ("draft", "status = 'pending'", errors.CheckViolation),
    ("pending", "status = 'published', definition_sha256 = 'x'", errors.RaiseException),
    ("pending", "status = 'published'", None),
    ("pending", "status = 'draft', definition_sha256 = NULL", None),
    ("pending", "status = 'draft'", errors.CheckViolation),
    ("published", "status = 'superseded'", None),
    ("published", "status = 'superseded', definition_sha256 = 'x'", errors.RaiseException),
    ("superseded", "definition_sha256 = 'x'", errors.RaiseException),
]


@pytest.mark.parametrize(("start", "change", "error"), HASH_CASES)
def test_the_hash_changes_only_at_submit_and_withdraw(
    db: TestDatabase, tenant: str, start: str, change: str, error
):
    """Fails if the definition hash can move outside submit and withdraw, or on publish."""
    with db.owner() as conn:
        seed_version(conn, tenant, "aa", start)
        update = f"UPDATE agent_versions SET {change} WHERE tenant_id = %s"
        if error is None:
            conn.execute(update, (tenant,))
        else:
            with pytest.raises(error):
                conn.execute(update, (tenant,))


# --- the migration over rows ---------------------------------------------------------------


def database_at_016(databases: Databases) -> tuple[TestDatabase, dict]:
    migrate = load_migrate()
    database = databases.create()
    database.execute_file(
        MIGRATIONS.parent.parent / "tests" / "pg" / "fixtures" / "base_schema.sql"
    )
    files = migrate.migration_files()
    with database.owner() as conn:
        migrate.migrate(
            conn, {v: p for v, p in files.items() if v <= "016"}, log=lambda _line: None
        )
    return database, files


def test_migrate_applies_017_over_rows_and_backfills_them(databases: Databases):
    """Fails if 017 cannot run over existing versions, leaves one without a hash, hashes a
    draft, or leaves the immutability trigger off."""
    migrate = load_migrate()
    database, files = database_at_016(databases)
    with database.owner() as conn:
        conn.execute("INSERT INTO agents VALUES ('t', 'aa', 'u', now())")
        for version, status in enumerate(("draft", "pending", "published", "superseded"), 1):
            conn.execute(
                "INSERT INTO agent_versions (tenant_id, agent_id, version, status, definition, "
                "created_by, submitted_by, approved_by, published_at) "
                "VALUES ('t', 'aa', %s, %s, %s::jsonb, 'u', 'u', 'u', now())",
                (version, status, f'{{"n": {version}}}'),
            )
        applied = migrate.migrate(conn, files, log=lambda _line: None)
        assert applied == [v for v in EXPECTED_VERSIONS if v >= "017"]
        rows = conn.execute(
            "SELECT status, definition_sha256, "
            "encode(sha256(convert_to(definition::text, 'UTF8')), 'hex') "
            "FROM agent_versions ORDER BY version"
        ).fetchall()
        assert [(s, h is None) for s, h, _ in rows] == [
            ("draft", True),
            ("pending", False),
            ("published", False),
            ("superseded", False),
        ]
        assert all(h == expected for _, h, expected in rows if h)
        enabled = conn.execute(
            "SELECT tgenabled FROM pg_trigger WHERE tgname = 'agent_versions_guard'"
        ).fetchone()[0]
        assert enabled == "O"
        with pytest.raises(errors.RaiseException):
            conn.execute("UPDATE agent_versions SET definition = '{}' WHERE version = 3")


# --- item 7: the app role through the store -------------------------------------------------


async def test_publish_names_the_submitted_hash_through_the_app_role(pool, tenant: str):
    """Fails if a stale hash publishes, a refused publish supersedes the live version, a
    concurrent withdraw surfaces as anything but a typed error, or submit stores another hash."""
    store = PostgresAgentStore(pool)
    await store.create_agent(tenant, "aa", helper_block(), "U1")
    first = await store.submit(tenant, "aa", 1, "U1")
    assert first.definition_sha256 == canonical_sha256(helper_block())
    await store.publish(tenant, "aa", 1, "U2", reviewed_sha256=first.definition_sha256)

    await store.create_draft(tenant, "aa", "U1")
    seen = (await store.submit(tenant, "aa", 2, "U1")).definition_sha256
    await store.withdraw(tenant, "aa", 2)
    assert (await store.get_version(tenant, "aa", 2)).definition_sha256 is None
    await store.replace_draft(tenant, "aa", 2, helper_block() | {"max_steps": 3})
    now = (await store.submit(tenant, "aa", 2, "U1")).definition_sha256
    assert now != seen

    with pytest.raises(VersionChanged):
        await store.publish(tenant, "aa", 2, "U2", reviewed_sha256=seen)
    assert (await store.published(tenant, "aa")).version == 1

    await store.withdraw(tenant, "aa", 2)
    with pytest.raises(AgentStoreError) as refused:
        await store.publish(tenant, "aa", 2, "U2", reviewed_sha256=now)
    assert not isinstance(refused.value, TypeError)

    await store.submit(tenant, "aa", 2, "U1")
    done = await store.publish(tenant, "aa", 2, "U2", reviewed_sha256=now)
    assert done.definition_sha256 == now and done.status == "published"


async def test_import_stores_the_hash_of_what_it_inserts(pool, tenant: str):
    """Fails if an imported published version has no hash (the CHECK would refuse it) or a
    hash other than the canonical one."""
    config = parse_agents_config(agents_dict())
    store = PostgresAgentStore(pool)
    await store.import_agents(tenant, config.agents)
    record = await store.get_version(tenant, "it-helper", 1)
    assert record.definition_sha256 == canonical_sha256(record.definition)


# --- item 9 and 8: the audit store and the unit of work -------------------------------------


async def test_audit_get_reads_an_event_by_key_inside_a_transaction(pool, tenant: str):
    """Fails if an event cannot be found by its key, on its own or on a unit's connection."""
    audit = PostgresAuditStore(pool)
    event = AuditEvent(
        tenant_id=tenant,
        event_key=f"{tenant}:k",
        kind="agent_draft",
        agent_id="aa",
        agent_version=2,
        requester_user_id="U1",
        identity="user:U1",
    )
    assert await audit.get(tenant, event.event_key) is None
    async with PostgresUnitOfWork(pool).begin(tenant) as conn:
        await audit.record(event, conn=conn)
        assert (await audit.get(tenant, event.event_key, conn=conn)).agent_version == 2
    assert (await audit.get(tenant, event.event_key)).kind == "agent_draft"


class RecordThenFail(PostgresAuditStore):
    """Writes the event on the unit's connection, then fails as a lost connection would."""

    async def record(self, event, conn=None):
        await super().record(event, conn=conn)
        raise RuntimeError("audit failed after the insert")


CHANGES = {
    "add_agent": lambda p, t: p.add_agent(t, "U_ROOT", "it-helper", helper_block()),
    "invite_member": lambda p, t: p.invite_member(t, "U_ROOT", "U_NEW", "creator"),
    "turn_off": lambda p, t: p.turn_off(t, "U_ROOT", "writes"),
    "set_budget": lambda p, t: p.set_budget(t, "U_ROOT", None, "5.00"),
}


@pytest.mark.parametrize("change", sorted(CHANGES))
async def test_a_failed_audit_insert_rolls_the_change_back_in_postgres(pool, tenant: str, change):
    """Fails if a change commits although its audit insert failed, so the retry is refused or
    the change is never audited."""
    act = CHANGES[change]
    with pytest.raises(RuntimeError):
        await act(plane_for(pool, tenant, audit=RecordThenFail(pool)), tenant)
    async with tenant_transaction(pool, tenant) as conn:
        counts = [
            (await (await conn.execute(f"SELECT count(*) FROM {table}")).fetchone())[0]
            for table in ("agents", "members", "kill_switches", "budgets", "audit_events")
        ]
    assert counts == [0, 0, 0, 0, 0]

    await act(plane_for(pool, tenant), tenant)

    events = await PostgresAuditStore(pool).list(tenant)
    assert len(events) == 1


# --- item 8: the last admin, on two connections --------------------------------------------


class Gate(PostgresUnitOfWork):
    """Counts the units that have begun, so a test can hold one until both have."""

    def __init__(self, pool):
        super().__init__(pool)
        self.begun = 0

    def begin(self, tenant_id):
        self.begun += 1
        return super().begin(tenant_id)


class HoldsTheCount(PostgresMemberStore):
    """Waits after counting admins until the other call has begun and had time to run."""

    def __init__(self, pool, gate: Gate):
        super().__init__(pool)
        self.gate = gate

    async def list_members(self, tenant_id, conn=None):
        rows = await super().list_members(tenant_id, conn=conn)
        while self.gate.begun < 2:
            await asyncio.sleep(0)
        for _ in range(200):
            await asyncio.sleep(0)
        return rows


async def test_two_admins_demoting_each_other_leave_one_in_postgres(pool, tenant: str):
    """Fails if the tenant's advisory lock does not serialise two concurrent demotions, so each
    counts the other still in place and the tenant is left with no admin."""
    gate = Gate(pool)
    members = HoldsTheCount(pool, gate)
    plane = plane_for(pool, tenant, admins=(), members=members, uow=gate)
    await PostgresMemberStore(pool).add_member(tenant, "U_A", "admin", "setup")
    await PostgresMemberStore(pool).add_member(tenant, "U_B", "admin", "setup")

    results = await asyncio.gather(
        plane.change_role(tenant, "U_A", "U_B", "creator"),
        plane.change_role(tenant, "U_B", "U_A", "creator"),
        return_exceptions=True,
    )

    assert sum(isinstance(r, Forbidden) for r in results) == 1
    rows = await PostgresMemberStore(pool).list_members(tenant)
    assert [m.role for m in rows].count("admin") == 1
