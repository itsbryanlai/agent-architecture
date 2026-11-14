"""The control-plane API: endpoints, error mapping, the fixed actor and the local-only entry point.

Everything is in memory: the three stores, a fake Notion and a stand-in identity.
"""

import ast
from pathlib import Path

import httpx
import pytest

from agentos.agent_store import InMemoryAgentStore
from agentos.audit import InMemoryAuditStore
from agentos.config import TenantsConfig, parse_agents_config
from agentos.control import ControlPlane
from agentos.control_api import (
    ACTOR_VARIABLE,
    MAX_AUDIT_LIMIT,
    LocalOperator,
    check_bind_host,
    create_app,
    operator_from_environment,
)
from agentos.grant_checker import GrantCheckUnavailable, NotionGrantChecker
from agentos.members import InMemoryMemberStore
from tests.conftest import (
    DB_ID,
    GLOBEX,
    TENANT,
    default_schema,
    make_fake_notion,
    two_tenant_dicts,
)
from tests.test_agent_store import helper_block

ROOT = "U_ROOT"  # an admin from the tenant file
ADMIN2 = "U_ADMIN2"
OWNER = "U_OWNER"  # a creator
OTHER = "U_OTHER"  # a creator who owns nothing
OUTSIDER = "U_OUTSIDER"  # no role
OTHER_TENANT = GLOBEX

BASE = "http://127.0.0.1:8100"
JSON = {"Content-Type": "application/json"}

SRC = Path(__file__).resolve().parent.parent / "src" / "agentos"


class StandIn:
    """A test identity: whoever the test says is acting. Wave 5 replaces the real one."""

    def __init__(self, user: str):
        self.user = user

    async def actor(self, request) -> str:
        return self.user


class Harness:
    def __init__(self):
        self.agents = InMemoryAgentStore()
        self.members = InMemoryMemberStore()
        self.audit = InMemoryAuditStore()
        self.notion = make_fake_notion()
        self.identity = StandIn(ROOT)
        acme, globex = two_tenant_dicts()
        acme["admins"] = globex["admins"] = [ROOT]
        self.tenants = TenantsConfig.of(parse_agents_config(acme), parse_agents_config(globex))
        self.plane = ControlPlane(
            self.agents,
            self.members,
            self.audit,
            self.tenants,
            NotionGrantChecker(lambda tenant_id: self.notion),
        )
        self.app = create_app(self.plane, self.tenants, self.identity)

    def as_user(self, user: str) -> "Harness":
        self.identity.user = user
        return self

    def client(self) -> httpx.AsyncClient:
        transport = httpx.ASGITransport(app=self.app)
        return httpx.AsyncClient(transport=transport, base_url=BASE, headers=JSON)

    async def seed_members(self):
        await self.members.add_member(TENANT, ADMIN2, "admin", ROOT)
        await self.members.add_member(TENANT, OWNER, "creator", ROOT)
        await self.members.add_member(TENANT, OTHER, "creator", ROOT)


@pytest.fixture
async def h():
    harness = Harness()
    await harness.seed_members()
    return harness


def t(path: str, tenant: str = TENANT) -> str:
    return f"/tenants/{tenant}{path}"


def kinds(h: Harness) -> list[str]:
    return [e.kind for e in h.audit.events if e.tenant_id == TENANT]


async def create_pending(h: Harness, user=OWNER):
    async with h.client() as c:
        h.as_user(user)
        made = await c.post(
            t("/agents"), json={"agent_id": "it-helper", "definition": helper_block()}
        )
        assert made.status_code == 201
        sub = await c.post(t("/agents/it-helper/versions/1/submit"))
        assert sub.status_code == 200
    return sub.json()


# --- health and identity ------------------------------------------------------------


async def test_healthz_needs_no_identity_or_tenant(h):
    async with h.client() as c:
        response = await c.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


async def test_me_reports_the_actor_and_the_role(h):
    async with h.client() as c:
        for user, role in [
            (ROOT, "admin"),
            (ADMIN2, "admin"),
            (OWNER, "creator"),
            (OUTSIDER, None),
        ]:
            h.as_user(user)
            response = await c.get(t("/me"))
            assert response.status_code == 200
            assert response.json() == {"user_id": user, "role": role}


async def test_an_unknown_tenant_is_404(h):
    async with h.client() as c:
        for method, path in [
            ("GET", "/me"),
            ("GET", "/agents"),
            ("GET", "/members"),
            ("GET", "/audit"),
        ]:
            response = await c.request(method, t(path, "nope"))
            assert response.status_code == 404, path


# --- agents -------------------------------------------------------------------------


async def test_the_agent_lifecycle_through_the_api(h):
    async with h.client() as c:
        h.as_user(OWNER)
        made = await c.post(
            t("/agents"), json={"agent_id": "it-helper", "definition": helper_block()}
        )
        assert made.status_code == 201
        assert made.json()["status"] == "draft"
        assert made.json()["version"] == 1

        listed = await c.get(t("/agents"))
        assert [a["agent_id"] for a in listed.json()] == ["it-helper"]
        assert listed.json()[0]["created_by"] == OWNER

        edited = helper_block()
        edited["name"] = "IT helper v2"
        put = await c.put(t("/agents/it-helper/versions/1"), json={"definition": edited})
        assert put.status_code == 200
        assert put.json()["definition"]["name"] == "IT helper v2"

        submitted = await c.post(t("/agents/it-helper/versions/1/submit"))
        assert submitted.json()["status"] == "pending"
        withdrawn = await c.post(t("/agents/it-helper/versions/1/withdraw"))
        assert withdrawn.json()["status"] == "draft"
        await c.post(t("/agents/it-helper/versions/1/submit"))

        h.as_user(ADMIN2)
        rejected = await c.post(t("/agents/it-helper/versions/1/reject"))
        assert rejected.json()["status"] == "draft"
        h.as_user(OWNER)
        await c.post(t("/agents/it-helper/versions/1/submit"))

        h.as_user(ADMIN2)
        published = await c.post(t("/agents/it-helper/versions/1/publish"))
        assert published.status_code == 200
        assert published.json()["status"] == "published"
        assert published.json()["approved_by"] == ADMIN2

        h.as_user(OWNER)
        draft = await c.post(t("/agents/it-helper/drafts"), json={"from_version": 1})
        assert draft.status_code == 201
        assert draft.json()["version"] == 2
        again = await c.post(t("/agents/it-helper/drafts"))
        assert again.status_code == 201
        assert again.json()["version"] == 3

        versions = await c.get(t("/agents/it-helper/versions"))
        assert [v["version"] for v in versions.json()] == [3, 2, 1]
        one = await c.get(t("/agents/it-helper/versions/1"))
        assert one.json()["status"] == "published"

    assert kinds(h) == [
        "agent_create",
        "agent_draft",
        "agent_submit",
        "agent_withdraw",
        "agent_submit",
        "agent_reject",
        "agent_submit",
        "agent_publish",
        "agent_draft",
        "agent_draft",
    ]


# --- members ------------------------------------------------------------------------


async def test_members_through_the_api(h):
    async with h.client() as c:
        h.as_user(ROOT)
        added = await c.post(t("/members"), json={"user_id": "U_NEW", "role": "creator"})
        assert added.status_code == 201
        listed = await c.get(t("/members"))
        assert {m["user_id"]: m["role"] for m in listed.json()}["U_NEW"] == "creator"
        changed = await c.patch(t("/members/U_NEW"), json={"role": "admin"})
        assert changed.status_code == 200
        assert changed.json() == {"user_id": "U_NEW", "role": "admin"}
        removed = await c.delete(t("/members/U_NEW"))
        assert removed.status_code == 200
        listed = await c.get(t("/members"))
        assert "U_NEW" not in {m["user_id"] for m in listed.json()}
    assert kinds(h) == ["member_add", "member_role", "member_remove"]


# --- error mapping ------------------------------------------------------------------


async def test_a_refusal_by_role_is_403(h):
    async with h.client() as c:
        h.as_user(OUTSIDER)
        assert (await c.get(t("/agents"))).status_code == 403
        assert (await c.get(t("/me"))).status_code == 200
        h.as_user(OWNER)
        assert (await c.get(t("/members"))).status_code == 403
        assert (await c.get(t("/audit"))).status_code == 403
        assert (
            await c.post(t("/members"), json={"user_id": "U_X", "role": "admin"})
        ).status_code == 403
        await create_pending(h)
        assert (await c.post(t("/agents/it-helper/versions/1/publish"))).status_code == 403
        h.as_user(OTHER)
        assert (await c.post(t("/agents/it-helper/drafts"))).status_code == 403
        # The last admin cannot be demoted: the control plane's Forbidden, not a conflict.
        h.as_user(ROOT)
        await c.delete(t(f"/members/{ADMIN2}"))
        assert (await c.patch(t(f"/members/{ROOT}"), json={"role": "creator"})).status_code == 403


async def test_a_missing_thing_is_404(h):
    async with h.client() as c:
        h.as_user(ROOT)
        assert (await c.get(t("/agents/ghost/versions"))).status_code == 404
        assert (await c.get(t("/agents/ghost/versions/1"))).status_code == 404
        assert (await c.post(t("/agents/ghost/versions/1/submit"))).status_code == 404
        assert (await c.post(t("/agents/ghost/drafts"))).status_code == 404
        assert (await c.delete(t("/members/U_GHOST"))).status_code == 404
        assert (await c.patch(t("/members/U_GHOST"), json={"role": "admin"})).status_code == 404
        h.as_user(OWNER)
        assert (await c.post(t("/agents/ghost/drafts"))).status_code == 404
        await create_pending(h)
        assert (await c.get(t("/agents/it-helper/versions/9"))).status_code == 404
        assert (await c.post(t("/agents/it-helper/versions/9/submit"))).status_code == 404
        # Another tenant's agent is not visible here (ROOT is an admin of both tenants).
        h.as_user(ROOT)
        assert (await c.get(t("/agents/it-helper/versions", OTHER_TENANT))).status_code == 404


async def test_a_wrong_state_is_409(h):
    async with h.client() as c:
        h.as_user(OWNER)
        body = {"agent_id": "it-helper", "definition": helper_block()}
        assert (await c.post(t("/agents"), json=body)).status_code == 201
        assert (await c.post(t("/agents"), json=body)).status_code == 409
        h.as_user(ROOT)
        assert (await c.post(t("/agents/it-helper/versions/1/publish"))).status_code == 409
        assert (await c.post(t("/agents/it-helper/versions/1/withdraw"))).status_code == 409
        assert (await c.post(t("/agents/it-helper/drafts"))).status_code == 409  # nothing published
        h.as_user(OWNER)
        await c.post(t("/agents/it-helper/versions/1/submit"))
        assert (await c.post(t("/agents/it-helper/versions/1/submit"))).status_code == 409
        assert (
            await c.put(t("/agents/it-helper/versions/1"), json={"definition": helper_block()})
        ).status_code == 409
        h.as_user(ROOT)
        assert (
            await c.post(t("/members"), json={"user_id": OWNER, "role": "admin"})
        ).status_code == 409
        assert (await c.patch(t(f"/members/{OWNER}"), json={"role": "creator"})).status_code == 409


async def test_a_stale_publish_is_409(h):
    await create_pending(h)
    async with h.client() as c:
        h.as_user(ROOT)
        v1 = await c.post(t("/agents/it-helper/drafts"), json={"from_version": 1})
        assert v1.status_code == 201  # version 2
        await c.post(t("/agents/it-helper/versions/2/submit"))
        assert (await c.post(t("/agents/it-helper/versions/2/publish"))).status_code == 200
        # Version 1 is still pending but older than the published version 2.
        stale = await c.post(t("/agents/it-helper/versions/1/publish"))
        assert stale.status_code == 409


async def test_an_invalid_definition_or_request_is_422_without_echoing_the_input(h):
    async with h.client() as c:
        h.as_user(OWNER)
        bad = helper_block()
        bad["max_steps"] = "many"
        response = await c.post(t("/agents"), json={"agent_id": "it-helper", "definition": bad})
        assert response.status_code == 422
        assert "invalid" in response.json()["error"]
        missing = await c.post(t("/agents"), json={"agent_id": "it-helper"})
        assert missing.status_code == 422
        junk = await c.post(t("/agents"), json={"agent_id": "x", "definition": "SECRET-TEXT"})
        assert junk.status_code == 422
        assert "SECRET-TEXT" not in junk.text
        h.as_user(ROOT)
        assert (
            await c.post(t("/members"), json={"user_id": "U_X", "role": "owner"})
        ).status_code == 422
        assert (
            await c.post(t("/members"), json={"user_id": " ", "role": "admin"})
        ).status_code == 422
        assert (await c.patch(t(f"/members/{OWNER}"), json={"role": "root"})).status_code == 422
        assert (await c.get(t("/audit?limit=0"))).status_code == 422
        assert (await c.get(t("/audit?before=abc"))).status_code == 422
    assert kinds(h) == []


async def test_an_unexpected_error_is_a_plain_500_without_a_trace(h):
    class Broken(StandIn):
        async def actor(self, request):
            raise RuntimeError("secret detail at /some/path.py")

    app = create_app(h.plane, h.tenants, Broken(ROOT))
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url=BASE, headers=JSON) as c:
        response = await c.get(t("/me"))
    assert response.status_code == 500
    assert response.json() == {"error": "internal error"}


# --- the actor is the runtime's ----------------------------------------------------------


async def test_no_header_query_or_body_field_changes_the_actor(h):
    spoof_headers = {
        "X-Slack-User": ROOT,
        "X-Slack-User-Id": ROOT,
        "X-Forwarded-User": ROOT,
        "X-Actor": ROOT,
        "Authorization": f"Bearer {ROOT}",
        "X-Slack-Team": TENANT,
    }
    async with h.client() as c:
        h.as_user(OUTSIDER)
        me = await c.get(t(f"/me?actor={ROOT}&user_id={ROOT}&as={ROOT}"), headers=spoof_headers)
        assert me.json() == {"user_id": OUTSIDER, "role": None}
        denied = await c.get(t(f"/agents?actor={ROOT}"), headers=spoof_headers)
        assert denied.status_code == 403

        h.as_user(OWNER)
        await create_pending(h)
        # A lifecycle call with an admin's name in the body is still the owner's call.
        body = {"actor": ROOT, "user_id": ROOT, "approver": ROOT}
        publish = await c.post(
            t(f"/agents/it-helper/versions/1/publish?actor={ROOT}"),
            headers=spoof_headers,
            json=body,
        )
        assert publish.status_code == 403
        withdraw = await c.post(
            t("/agents/it-helper/versions/1/withdraw"), headers=spoof_headers, json=body
        )
        assert withdraw.status_code == 200

        # A body that names an actor on a route that reads a body is refused, not obeyed.
        refused = await c.post(
            t("/agents"),
            json={"agent_id": "other", "definition": helper_block(), "actor": ROOT},
        )
        assert refused.status_code == 422
        refused = await c.post(
            t("/members"), json={"user_id": "U_X", "role": "admin"}, headers=spoof_headers
        )
        assert refused.status_code == 403  # the owner's role decides, not the headers

    last = [e for e in h.audit.events if e.kind == "agent_withdraw"][-1]
    assert last.identity == f"user:{OWNER}"
    assert all(e.identity == f"user:{OWNER}" for e in h.audit.events)


def test_the_local_operator_is_one_fixed_slack_user():
    operator = LocalOperator("U0123ABCDEF")
    assert operator._user_id == "U0123ABCDEF"
    for bad in ["", "alice", "u0123", "U 123", "U0123\nX", "@U0123ABC"]:
        with pytest.raises(ValueError):
            LocalOperator(bad)


async def test_the_real_provider_ignores_the_request(h):
    app = create_app(h.plane, h.tenants, LocalOperator("UROOT123"))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url=BASE, headers=JSON) as c:
        response = await c.get(
            t("/me"), headers={"X-Slack-User": ROOT}, params={"actor": ROOT, "user_id": ROOT}
        )
    assert response.json() == {"user_id": "UROOT123", "role": None}


# --- the entry point ----------------------------------------------------------------


@pytest.mark.parametrize("host", ["127.0.0.1", "::1", "localhost"])
def test_loopback_hosts_are_accepted(host):
    check_bind_host(host)


@pytest.mark.parametrize(
    "host",
    [
        "0.0.0.0",
        "::",
        "",
        "192.168.1.20",
        "10.0.0.5",
        "example.com",
        "localhost.example.com",
        "127.0.0.2",
        "LOCALHOST",
    ],
)
def test_other_hosts_are_refused(host):
    with pytest.raises(SystemExit) as refused:
        check_bind_host(host)
    assert "refusing to listen" in str(refused.value)


def test_it_refuses_to_start_without_an_actor():
    for environ in [{}, {ACTOR_VARIABLE: ""}, {ACTOR_VARIABLE: "   "}]:
        with pytest.raises(SystemExit) as refused:
            operator_from_environment(environ)
        assert ACTOR_VARIABLE in str(refused.value)
    with pytest.raises(SystemExit):
        operator_from_environment({ACTOR_VARIABLE: "not-a-user"})
    assert operator_from_environment({ACTOR_VARIABLE: "U0123ABCDEF"})


def test_the_api_has_no_way_to_bind_publicly():
    source = (SRC / "control_api.py").read_text()
    assert "0.0.0.0" not in source
    compose = (SRC.parent.parent / "docker-compose.yml").read_text()
    assert "control_api" not in compose


def test_the_gateway_does_not_mount_the_control_api():
    source = (SRC / "gateway" / "app.py").read_text()
    assert "control_api" not in source


def test_the_api_calls_no_store_directly():
    tree = ast.parse((SRC / "control_api.py").read_text())
    called = {
        n.func.attr
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and ast.unparse(n.func.value) != "deps.plane"
    }
    store_methods = {
        "create_agent", "create_draft", "replace_draft", "submit", "withdraw", "publish",
        "add_member", "set_member_role", "remove_member", "get_member", "list_members",
        "get_agent", "get_version", "versions", "record",
    }  # fmt: skip
    # Every such call is made on the control plane, and on nothing else.
    assert called & store_methods == set()


# --- grants -------------------------------------------------------------------------


async def test_check_grants_reports_each_grant(h):
    await create_pending(h)
    async with h.client() as c:
        h.as_user(OWNER)
        clean = await c.post(t("/agents/it-helper/versions/1/check-grants"))
        assert clean.status_code == 200
        assert clean.json()["ok"] is True
        [report] = clean.json()["grants"]
        assert report["grant"] == "IT requests"
        assert report["database_id"] == DB_ID
        assert report["problems"] == []

        schema = default_schema()
        schema["Agent request ID"] = {"id": "reqid1", "type": "people"}
        h.notion.set_schema(DB_ID, schema)
        h.as_user(ADMIN2)
        broken = await c.post(t("/agents/it-helper/versions/1/check-grants"))
        assert broken.status_code == 200
        assert broken.json()["ok"] is False
        assert "request_id" in broken.json()["grants"][0]["problems"][0]


async def test_check_grants_is_for_editors_and_admins(h):
    await create_pending(h)
    async with h.client() as c:
        for user, status in [(OWNER, 200), (ROOT, 200), (OTHER, 403), (OUTSIDER, 403)]:
            h.as_user(user)
            response = await c.post(t("/agents/it-helper/versions/1/check-grants"))
            assert response.status_code == status, user
        h.as_user(ROOT)
        assert (await c.post(t("/agents/it-helper/versions/9/check-grants"))).status_code == 404
        assert (await c.post(t("/agents/ghost/versions/1/check-grants"))).status_code == 404
    # The check only reads.
    assert kinds(h) == ["agent_create", "agent_submit"]


async def test_publish_is_refused_while_a_grant_has_a_problem(h):
    await create_pending(h)
    schema = default_schema()
    schema["Agent request ID"] = {"id": "reqid1", "type": "people"}
    h.notion.set_schema(DB_ID, schema)
    async with h.client() as c:
        h.as_user(ROOT)
        refused = await c.post(t("/agents/it-helper/versions/1/publish"))
        assert refused.status_code == 409
        assert refused.json()["grants"][0]["problems"]
        still = await c.get(t("/agents/it-helper/versions/1"))
        assert still.json()["status"] == "pending"
        assert "agent_publish" not in kinds(h)

        h.notion.set_schema(DB_ID, default_schema())
        allowed = await c.post(t("/agents/it-helper/versions/1/publish"))
        assert allowed.status_code == 200
        assert allowed.json()["status"] == "published"


async def test_publish_of_a_draft_is_a_state_conflict_before_any_notion_call(h):
    class Exploding:
        async def check(self, tenant_id, agent):
            raise AssertionError("Notion must not be read for a draft")

    plane = ControlPlane(h.agents, h.members, h.audit, h.tenants, Exploding())
    app = create_app(plane, h.tenants, StandIn(ROOT))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=BASE, headers=JSON
    ) as c:
        await c.post(t("/agents"), json={"agent_id": "it-helper", "definition": helper_block()})
        assert (await c.post(t("/agents/it-helper/versions/1/publish"))).status_code == 409


async def test_notion_being_unreadable_is_502_and_nothing_is_published(h):
    await create_pending(h)

    class Down:
        async def retrieve_schema(self, database_id):
            raise TimeoutError("boom with a token inside")

    h.notion = Down()
    async with h.client() as c:
        h.as_user(ROOT)
        response = await c.post(t("/agents/it-helper/versions/1/publish"))
        assert response.status_code == 502
        assert "token" not in response.text
        status = await c.get(t("/agents/it-helper/versions/1"))
        assert status.json()["status"] == "pending"
    assert issubclass(GrantCheckUnavailable, Exception)


async def test_an_agent_without_database_grants_publishes_with_no_notion_call(h):
    definition = helper_block()
    definition["grants"] = {}

    class Exploding:
        async def retrieve_schema(self, database_id):
            raise AssertionError("no database is granted")

    h.notion = Exploding()
    async with h.client() as c:
        h.as_user(OWNER)
        await c.post(t("/agents"), json={"agent_id": "plain", "definition": definition})
        await c.post(t("/agents/plain/versions/1/submit"))
        h.as_user(ROOT)
        response = await c.post(t("/agents/plain/versions/1/publish"))
    assert response.status_code == 200


# --- retries record once ------------------------------------------------------------


async def test_a_retried_write_with_the_same_key_records_one_audit_event(h):
    async with h.client() as c:
        h.as_user(OWNER)
        key = {"Idempotency-Key": "req-1"}
        body = {"agent_id": "it-helper", "definition": helper_block()}
        first = await c.post(t("/agents"), json=body, headers=key)
        retry = await c.post(t("/agents"), json=body, headers=key)
        assert (first.status_code, retry.status_code) == (201, 409)
        assert kinds(h) == ["agent_create"]

        submit = await c.post(t("/agents/it-helper/versions/1/submit"), headers=key)
        again = await c.post(t("/agents/it-helper/versions/1/submit"), headers=key)
        assert (submit.status_code, again.status_code) == (200, 409)
        assert kinds(h) == ["agent_create", "agent_submit"]

        h.as_user(ROOT)
        added = await c.post(
            t("/members"), json={"user_id": "U_NEW", "role": "creator"}, headers=key
        )
        added_again = await c.post(
            t("/members"), json={"user_id": "U_NEW", "role": "creator"}, headers=key
        )
        assert (added.status_code, added_again.status_code) == (201, 409)
        assert kinds(h).count("member_add") == 1


async def test_a_key_does_not_hide_a_different_request(h):
    async with h.client() as c:
        h.as_user(ROOT)
        key = {"Idempotency-Key": "same-key"}
        for user in ["U_ONE", "U_TWO"]:
            await c.post(t("/members"), json={"user_id": user, "role": "creator"}, headers=key)
        assert kinds(h).count("member_add") == 2  # a different path or actor is a different request
        await c.delete(t("/members/U_ONE"), headers=key)
        await c.delete(t("/members/U_TWO"), headers=key)
        assert kinds(h).count("member_remove") == 2
        # The same key under another tenant is its own request too.
        await c.post(
            t("/members", OTHER_TENANT), json={"user_id": "U_ONE", "role": "creator"}, headers=key
        )
        assert [e.tenant_id for e in h.audit.events].count(OTHER_TENANT) == 1


async def test_without_a_key_each_write_is_its_own_event(h):
    async with h.client() as c:
        h.as_user(ROOT)
        await c.post(t("/members"), json={"user_id": "U_ONE", "role": "creator"})
        await c.delete(t("/members/U_ONE"))
        await c.post(t("/members"), json={"user_id": "U_ONE", "role": "creator"})
        await c.delete(t("/members/U_ONE"))
    assert kinds(h) == ["member_add", "member_remove", "member_add", "member_remove"]


# --- audit paging -------------------------------------------------------------------


async def add_members(h, count, tenant=TENANT):
    async with h.client() as c:
        h.as_user(ROOT)
        for i in range(count):
            response = await c.post(
                t("/members", tenant), json={"user_id": f"U_M{i:03d}", "role": "creator"}
            )
            assert response.status_code == 201


async def test_audit_is_paged_by_seq_newest_first(h):
    await add_members(h, 5)
    async with h.client() as c:
        h.as_user(ROOT)
        first = (await c.get(t("/audit?limit=2"))).json()
        assert [e["target"] for e in first["events"]] == ["U_M004", "U_M003"]
        assert first["next_before"] == first["events"][-1]["seq"]
        second = (await c.get(t(f"/audit?limit=2&before={first['next_before']}"))).json()
        assert [e["target"] for e in second["events"]] == ["U_M002", "U_M001"]
        last = (await c.get(t(f"/audit?limit=2&before={second['next_before']}"))).json()
        assert [e["target"] for e in last["events"]] == ["U_M000"]
        assert last["next_before"] is None

        # A new event between pages does not shift the next page.
        await c.post(t("/members"), json={"user_id": "U_LATE", "role": "creator"})
        again = (await c.get(t(f"/audit?limit=2&before={first['next_before']}"))).json()
        assert [e["target"] for e in again["events"]] == ["U_M002", "U_M001"]


async def test_audit_limit_is_capped(h):
    await add_members(h, 3)
    async with h.client() as c:
        h.as_user(ROOT)
        response = await c.get(t("/audit?limit=100000"))
        assert response.status_code == 200
        assert len(response.json()["events"]) == 3
    assert MAX_AUDIT_LIMIT == 200

    seen = {}

    class Spy(InMemoryAuditStore):
        async def list(self, tenant_id, limit=50, before=None, before_seq=None):
            seen["limit"] = limit
            return await super().list(tenant_id, limit, before, before_seq)

    spy = Spy()
    plane = ControlPlane(
        h.agents, h.members, spy, h.tenants, NotionGrantChecker(lambda t_: h.notion)
    )
    app = create_app(plane, h.tenants, StandIn(ROOT))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=BASE, headers=JSON
    ) as c:
        await c.get(t("/audit?limit=100000"))
    assert seen["limit"] == MAX_AUDIT_LIMIT


async def test_audit_is_scoped_to_the_tenant_in_the_path(h):
    await add_members(h, 2)
    await add_members(h, 1, OTHER_TENANT)
    async with h.client() as c:
        h.as_user(ROOT)
        acme = (await c.get(t("/audit"))).json()["events"]
        globex = (await c.get(t("/audit", OTHER_TENANT))).json()["events"]
    assert {e["tenant_id"] for e in acme} == {TENANT}
    assert {e["tenant_id"] for e in globex} == {OTHER_TENANT}
    assert len(acme) == 2 and len(globex) == 1
    # A seq from one tenant's page never exposes the other tenant's events.
    async with h.client() as c:
        high = max(e["seq"] for e in globex + acme) + 1
        everything = (await c.get(t(f"/audit?before={high}"))).json()["events"]
    assert {e["tenant_id"] for e in everything} == {TENANT}


async def test_audit_events_hold_ids_and_hashes_only(h):
    await create_pending(h)
    async with h.client() as c:
        h.as_user(ROOT)
        events = (await c.get(t("/audit"))).json()["events"]
    assert {e["kind"] for e in events} == {"agent_create", "agent_submit"}
    text = str(events)
    assert helper_block()["system_prompt"] not in text


# --- a web page in the operator's browser -------------------------------------------


async def test_a_no_cors_style_post_is_refused_and_changes_nothing(h):
    await create_pending(h)
    before = len(h.audit.events)
    async with h.client() as c:
        for content_type in [
            "text/plain",
            "application/x-www-form-urlencoded",
            "multipart/form-data",
            "",
        ]:
            response = await c.post(
                t("/agents/it-helper/versions/1/publish"),
                headers={"Content-Type": content_type},
                content=b"{}",
            )
            assert response.status_code == 415, content_type
        for method in ["PUT", "PATCH", "DELETE"]:
            response = await c.request(
                method, t("/members/U_X"), headers={"Content-Type": "text/plain"}, content=b"{}"
            )
            assert response.status_code == 415
        status = await c.get(t("/agents/it-helper/versions/1"))
    assert status.json()["status"] == "pending"
    assert len(h.audit.events) == before


async def test_json_writes_pass_with_a_charset_or_an_empty_body(h):
    await create_pending(h)
    h.as_user(ROOT)
    async with h.client() as c:
        ok = await c.post(
            t("/agents/it-helper/versions/1/publish"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            content=b"{}",
        )
        assert ok.status_code == 200


async def test_a_foreign_host_is_refused(h):
    async with h.client() as c:
        for host in [
            "evil.example:8100",
            "127.0.0.1:9999",
            "127.0.0.1",
            "localhost",
            "evil.example",
        ]:
            response = await c.get(t("/audit"), headers={"Host": host})
            assert response.status_code == 421, host
            assert host not in response.text
        for host in ["127.0.0.1:8100", "localhost:8100", "[::1]:8100"]:
            assert (await c.get(t("/audit"), headers={"Host": host})).status_code == 200, host


async def test_a_foreign_origin_or_cross_site_fetch_is_refused(h):
    async with h.client() as c:
        for origin in [
            "http://evil.example",
            "http://127.0.0.1:9999",
            "null",
            "https://127.0.0.1:8100",
        ]:
            response = await c.get(t("/audit"), headers={"Origin": origin})
            assert response.status_code == 403, origin
        for site in ["cross-site", "same-site"]:
            response = await c.get(t("/audit"), headers={"Sec-Fetch-Site": site})
            assert response.status_code == 403, site
        for site in ["same-origin", "none"]:
            assert (await c.get(t("/audit"), headers={"Sec-Fetch-Site": site})).status_code == 200
        ok = await c.get(t("/audit"), headers={"Origin": "http://localhost:8100"})
        assert ok.status_code == 200


async def test_healthz_is_exempt_from_the_browser_guard(h):
    async with h.client() as c:
        response = await c.get("/healthz", headers={"Host": "anything"})
    assert response.status_code == 200


async def test_the_allowed_port_is_a_parameter(h):
    app = create_app(h.plane, h.tenants, StandIn(ROOT), port=9001)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:9001", headers=JSON
    ) as c:
        assert (await c.get(t("/me"))).status_code == 200
        assert (await c.get(t("/me"), headers={"Host": "127.0.0.1:8100"})).status_code == 421
