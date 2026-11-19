"""The admin settings page: Connect Notion, default agent and routing, webhook steps."""

from __future__ import annotations

import json
import secrets as token_source
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from agentos import ids
from agentos.agent_health import AVAILABLE, UNAVAILABLE, AgentHealth
from agentos.audit import event_json
from agentos.config import parse_agents_config
from agentos.credentials import NOTION, InMemoryCredentialStore, TenantCredentials
from agentos.notion_picker import HttpNotionOAuth, Picker, PickerConfig
from agentos.notion_webhook import InMemoryWebhookTokenStore, WebhookTokens
from agentos.payload_codec import decrypt_for_tenant
from agentos.slack_client import FakeSlack
from agentos.tenant_store import InMemoryTenantStore
from tests.conftest import (
    ACME_WORKSPACE,
    GLOBEX_TEAM,
    GLOBEX_WORKSPACE,
    TEAM,
    TENANT,
    agents_dict,
    two_tenant_dicts,
)
from tests.helpers.console_harness import (
    ADMIN2,
    OWNER,
    ROOT,
    ConsoleHarness,
    csrf_token,
    fake_slack_for,
    post,
    using,
)
from tests.helpers.keys import RefreshableKeyStore

WORKSPACE_TOKEN = "ntn_workspace_bot_token_for_the_test"
WORKSPACE_ID = "ws-connected"
WORKSPACE_NAME = "Acme HQ"
CHANNEL = "C0REQUESTS"
SETTINGS = f"/ui/{TENANT}/settings"
CALLBACK = "/auth/notion/callback"


def connect_transport(
    seen: dict,
    *,
    token_status: int = 200,
    validation_status: int = 200,
    raise_on: str | None = None,
    workspace_id: str = WORKSPACE_ID,
):
    """Notion's OAuth token endpoint and the cheap validation call, over a mocked transport."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/oauth/token":
            code = json.loads(request.content)["code"]
            seen["code"] = code
            if raise_on == "token":
                raise httpx.ConnectError("connection refused")
            if token_status != 200:
                return httpx.Response(token_status, json={"error": "invalid_grant"})
            return httpx.Response(
                200,
                json={
                    # The token Notion returns is bound to the code that bought it.
                    "access_token": f"{WORKSPACE_TOKEN}:{code}",
                    "workspace_id": workspace_id,
                    "workspace_name": WORKSPACE_NAME,
                    "bot_id": "bot-1",
                },
            )
        if request.url.path == "/v1/users/me":
            if raise_on == "validation":
                raise httpx.ConnectError("connection refused")
            return httpx.Response(
                validation_status, json={"object": "user", "id": "bot-1", "type": "bot"}
            )
        if request.url.path == "/v1/search":
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "object": "page",
                            "id": "page-1",
                            "properties": {
                                "title": {"type": "title", "title": [{"plain_text": "A page"}]}
                            },
                        }
                    ],
                    "has_more": False,
                },
            )
        return httpx.Response(404)

    return httpx.MockTransport(handler)


async def build_harness(seen: dict, *, directory_ttl: float = 0, **transport_kw) -> ConsoleHarness:
    acme, globex = two_tenant_dicts()
    acme["admins"] = globex["admins"] = [ROOT]
    store = InMemoryTenantStore(parse_agents_config(acme), parse_agents_config(globex))
    keys = RefreshableKeyStore({(TENANT, 1): token_source.token_bytes(32)})
    await keys.refresh()
    credential_store = InMemoryCredentialStore()
    credentials = TenantCredentials(credential_store, keys)
    config = PickerConfig("notion-client", "notion-secret")
    picker = Picker(config, HttpNotionOAuth(config, connect_transport(seen, **transport_kw)))
    slack = FakeSlack(channels={CHANNEL: "it-requests", "C0OTHER": "other"}, team_id=TEAM)
    webhook_tokens = WebhookTokens(InMemoryWebhookTokenStore(), keys)
    harness = ConsoleHarness(
        picker=picker,
        tenant_store=store,
        credentials=credentials,
        webhook_tokens=webhook_tokens,
        slack_for=fake_slack_for(slack),
        directory_ttl=directory_ttl,
    )
    await harness.seed_members()
    harness.store = store
    harness.keys = keys
    harness.credentials = credentials
    harness.credential_store = credential_store
    harness.webhook_tokens = webhook_tokens
    return harness


async def start_state(c) -> str:
    response = await post(c, f"{SETTINGS}/notion/start", await csrf_token(c))
    assert response.status_code == 303
    location = response.headers["location"]
    assert location.startswith("https://api.notion.com/v1/oauth/authorize")
    query = parse_qs(urlsplit(location).query)
    assert query["owner"] == ["workspace"]
    return query["state"][0]


async def connect_flow(c, code: str = "abc"):
    state = await start_state(c)
    return await c.get(CALLBACK, params={"code": code, "state": state})


@pytest.mark.parametrize(
    "case",
    [
        "success",
        "reconnect replaces",
        "bad state",
        "reused state",
        "other session",
        "other tenant's session",
        "validation refused",
        "token refused",
        "transport failure",
        "workspace of another tenant",
        "non-admin start",
    ],
    ids=[
        "success",
        "reconnect",
        "bad-state",
        "reused-state",
        "other-session",
        "other-tenant",
        "validation-refused",
        "token-refused",
        "transport-failure",
        "workspace-conflict",
        "non-admin",
    ],
)
async def test_the_notion_connect_callback_stores_only_a_validated_workspace_token(case):
    """Fails if the callback stores a token it did not validate, accepts a state this session
    never began, one already spent or one begun in another tenant, writes anything on a
    refusal, or lets a non-admin start."""
    kw = {
        "validation refused": {"validation_status": 401},
        "token refused": {"token_status": 400},
        "transport failure": {"raise_on": "validation"},
        "workspace of another tenant": {"workspace_id": GLOBEX_WORKSPACE},
    }.get(case, {})
    seen: dict = {}
    h = await build_harness(seen, **kw)
    admin = await h.signed_in(ROOT)
    async with using(admin):
        if case == "non-admin start":
            creator = await h.signed_in(OWNER)
            async with using(creator):
                response = await post(
                    creator, f"{SETTINGS}/notion/start", await csrf_token(creator)
                )
            assert response.status_code == 403 and "location" not in response.headers
            assert "code" not in seen and h.audit.events == []
            return
        if case == "bad state":
            response = await admin.get(CALLBACK, params={"code": "c", "state": "forged"})
            assert response.status_code == 400
            assert "code" not in seen  # Notion was never asked
        elif case == "other session":
            state = await start_state(admin)
            other = await h.signed_in(ADMIN2)
            async with using(other):
                response = await other.get(CALLBACK, params={"code": "c", "state": state})
            assert response.status_code == 400
            assert "code" not in seen
        elif case == "other tenant's session":
            state = await start_state(admin)
            other = await h.signed_in(ROOT, GLOBEX_TEAM)
            async with using(other):
                response = await other.get(CALLBACK, params={"code": "c", "state": state})
            assert response.status_code == 400
            assert "code" not in seen
        elif case == "reused state":
            state = await start_state(admin)
            first = await admin.get(CALLBACK, params={"code": "abc", "state": state})
            assert first.status_code == 303
            response = await admin.get(CALLBACK, params={"code": "abc", "state": state})
            assert response.status_code == 400
        elif case == "reconnect replaces":
            assert (await connect_flow(admin, code="first")).status_code == 303
            response = await connect_flow(admin, code="second")
            assert response.status_code == 303
        else:
            response = await connect_flow(admin)
            expected = {
                "success": 303,
                "workspace of another tenant": 409,
            }.get(case, 503)
            assert response.status_code == expected, response.text

        stored = await h.credential_store.get(TENANT, NOTION)
        config = (await h.store.load(TENANT)).config
        if case in ("success", "reconnect replaces"):
            code = "second" if case == "reconnect replaces" else "abc"
            token = f"{WORKSPACE_TOKEN}:{code}"
            assert stored is not None
            assert decrypt_for_tenant(h.keys, TENANT, stored.ciphertext, stored.key_version) == (
                token.encode()
            )
            assert (config.notion_workspace_id, config.notion_workspace_name) == (
                WORKSPACE_ID,
                WORKSPACE_NAME,
            )
            assert await h.credentials.notion_token(config) == token
            connects = [e for e in h.audit.events if e.kind == "connector"]
            assert [(e.tool, e.target, e.identity) for e in connects] == [
                ("connect", WORKSPACE_ID, f"user:{ROOT}")
            ] * (2 if case == "reconnect replaces" else 1)
            assert all(WORKSPACE_TOKEN not in event_json(e) for e in h.audit.events)
            # Settle point: the flow is over; the page shows the connection, not the token.
            page = (await admin.get(SETTINGS)).text
            assert WORKSPACE_NAME in page and ROOT in page and "Connected by" in page
            assert token not in page
        elif case == "reused state":
            # The spent state changed nothing further: one connect, the first token.
            assert stored is not None
            assert decrypt_for_tenant(h.keys, TENANT, stored.ciphertext, stored.key_version) == (
                f"{WORKSPACE_TOKEN}:abc".encode()
            )
            assert len([e for e in h.audit.events if e.kind == "connector"]) == 1
        else:
            assert stored is None
            assert config.notion_workspace_id == ACME_WORKSPACE
            assert config.notion_workspace_name is None
            assert h.audit.events == []


async def test_disconnect_removes_the_credential_and_the_workspace_and_audits():
    """Fails if Disconnect keeps the credential or the workspace, does not audit, or a second
    Disconnect changes or records anything."""
    seen: dict = {}
    h = await build_harness(seen)
    admin = await h.signed_in(ROOT)
    async with using(admin):
        assert (await connect_flow(admin)).status_code == 303
        csrf = await csrf_token(admin)
        response = await post(admin, f"{SETTINGS}/notion/disconnect", csrf)
        assert response.status_code == 303
        assert response.headers["location"].endswith("settings?done=notion-disconnected")

        assert await h.credential_store.get(TENANT, NOTION) is None
        config = (await h.store.load(TENANT)).config
        assert (config.notion_workspace_id, config.notion_workspace_name) == (None, None)
        assert [(e.kind, e.tool) for e in h.audit.events] == [
            ("connector", "connect"),
            ("connector", "disconnect"),
        ]
        page = (await admin.get(SETTINGS)).text
        assert "Connect Notion" in page and WORKSPACE_NAME not in page

        again = await post(admin, f"{SETTINGS}/notion/disconnect", await csrf_token(admin))
        assert again.status_code == 409
        assert len(h.audit.events) == 2


def published_definition(**over) -> dict:
    definition = agents_dict()["agents"]["it-helper"]
    definition["audience"] = {"channels": [CHANNEL], "dm": "everyone"}
    definition.update(over)
    return definition


async def publish_helper(h, agent_id: str = "it-helper", **over) -> None:
    record = await h.plane.add_agent(TENANT, ROOT, agent_id, published_definition(**over))
    await h.plane.submit_version(TENANT, ROOT, agent_id, record.version)
    await h.plane.publish_version(
        TENANT,
        ROOT,
        agent_id,
        record.version,
        reviewed_sha256=record.definition_sha256,
        acknowledge_widening=True,
        reviewed_published="none",
    )


@pytest.mark.parametrize(
    ("case", "status"),
    [
        ("valid", 303),
        ("unpublished default", 422),
        ("unknown channel agent", 422),
        ("channel outside audience", 422),
        ("bad channel id", 422),
        ("non-admin", 403),
    ],
    ids=["valid", "unpublished", "unknown-agent", "outside-audience", "bad-channel", "non-admin"],
)
async def test_routing_is_validated_like_the_yaml_import_and_audited(case, status):
    """Fails if an unpublished agent or a channel outside its agent's audience can be saved,
    a valid change is not audited, or a non-admin can change routing."""
    seen: dict = {}
    h = await build_harness(seen)
    await publish_helper(h)
    await h.plane.add_agent(TENANT, ROOT, "draft-only", published_definition())
    await h.store.set_routing(TENANT, None, {})
    actor = OWNER if case == "non-admin" else ROOT
    c = await h.signed_in(actor)
    fields = {
        "valid": {"default_agent": "it-helper", "new_channel": CHANNEL, "new_agent": "it-helper"},
        "unpublished default": {"default_agent": "draft-only"},
        "unknown channel agent": {
            "default_agent": "it-helper",
            "new_channel": CHANNEL,
            "new_agent": "no-such-agent",
        },
        "channel outside audience": {
            "default_agent": "it-helper",
            "new_channel": "C0OTHER",
            "new_agent": "it-helper",
        },
        "bad channel id": {
            "default_agent": "it-helper",
            "new_channel": "not-a-channel",
            "new_agent": "it-helper",
        },
        "non-admin": {"default_agent": "it-helper"},
    }[case]
    async with using(c):
        response = await post(c, f"{SETTINGS}/routing", await csrf_token(c), **fields)
        assert response.status_code == status, response.text
        if case == "valid":
            page = (await c.get(SETTINGS)).text
            assert "it-requests" in page  # the mapped channel's name, from Slack

    config = (await h.store.load(TENANT)).config
    if case == "valid":
        assert (config.default_agent, config.channels) == ("it-helper", {CHANNEL: "it-helper"})
        events = [e for e in h.audit.events if e.kind == "tenant_settings"]
        assert [(e.tool, e.identity) for e in events] == [("routing", f"user:{ROOT}")]
    else:
        assert (config.default_agent, config.channels) == (None, {})
        assert [e for e in h.audit.events if e.kind == "tenant_settings"] == []


@pytest.mark.parametrize(
    ("case", "status_cell"),
    [
        ("available", "<td>available</td>"),
        ("unavailable", '<span class="error">unavailable</span>'),
        ("never checked", "<td>never checked</td>"),
    ],
    ids=["available", "unavailable", "never-checked"],
)
async def test_the_settings_page_shows_each_published_agents_health(case, status_cell):
    """Fails if the settings page hides a published agent's health status, the reason an
    unavailable agent is down, or when it was last checked."""
    seen: dict = {}
    h = await build_harness(seen)
    await publish_helper(h)
    reason = "the granted database is not shared with the integration"
    if case != "never checked":
        status = AVAILABLE if case == "available" else UNAVAILABLE
        await h.health.record(
            AgentHealth(
                TENANT, "it-helper", 1, status, reason if case == "unavailable" else None, h.clock()
            )
        )
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(SETTINGS)).text
    assert "Agent health" in page and status_cell in page
    if case == "unavailable":
        assert reason in page and "2026-11-14 12:00 UTC" in page
    else:
        assert reason not in page


@pytest.mark.parametrize("default_set", [False, True], ids=["not set up", "set up"])
async def test_the_settings_page_shows_the_setup_banner_until_a_default_agent_is_set(default_set):
    """Fails if a tenant with no default agent is not told what is left, or a set-up tenant
    still sees the banner."""
    seen: dict = {}
    h = await build_harness(seen)
    if default_set:
        await publish_helper(h)
        await h.store.set_routing(TENANT, "it-helper", {})
    else:
        await h.store.set_routing(TENANT, None, {})
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(SETTINGS)).text
    assert ("not set up yet" in page) is (not default_set)
    if not default_set:
        assert "Create an agent" in page and "publish" in page


async def test_reveal_shows_the_pending_webhook_token_once_to_an_admin():
    """Fails if the revealed token stays on the page after a reload, or the reveal is not audited."""
    seen: dict = {}
    h = await build_harness(seen)
    token = "secret_verification_token_abc123"
    assert await h.webhook_tokens.accept_handshake(TENANT, token) is True
    c = await h.signed_in(ROOT)
    async with using(c):
        response = await post(c, f"{SETTINGS}/webhook/reveal", await csrf_token(c))
        assert response.status_code == 200
        assert token in response.text and 'data-copy="webhook-token"' in response.text
        # Settle point: a fresh page load. The token was shown once, not kept.
        again = (await c.get(SETTINGS)).text
        assert token not in again and "pending" in again
    assert [(e.kind, e.tool, e.identity) for e in h.audit.events] == [
        ("webhook_token", "show", f"user:{ROOT}")
    ]
    assert all(token not in event_json(e) for e in h.audit.events)


async def test_the_page_shows_a_routing_change_at_once_and_an_unchanged_form_records_nothing():
    """Fails if the settings page serves the directory cache after a routing change, or an
    unchanged resubmit records a second event because the no-op comparison read the cache."""
    seen: dict = {}
    h = await build_harness(seen, directory_ttl=15)
    await publish_helper(h)
    await h.store.set_routing(TENANT, None, {})
    admin = await h.signed_in(ROOT)
    async with using(admin):
        await admin.get(SETTINGS)  # prime the directory cache
        saved = await post(
            admin,
            f"{SETTINGS}/routing",
            await csrf_token(admin),
            default_agent="it-helper",
            new_channel=CHANNEL,
            new_agent="it-helper",
        )
        assert saved.status_code == 303
        # Still inside the cache's TTL: the page must show the mapping just saved.
        page = (await admin.get(SETTINGS)).text
        assert f'name="route_{CHANNEL}"' in page
        again = await post(
            admin,
            f"{SETTINGS}/routing",
            await csrf_token(admin),
            default_agent="it-helper",
            **{f"route_{CHANNEL}": "it-helper"},
        )
        assert again.status_code == 303
    events = [e for e in h.audit.events if e.kind == "tenant_settings"]
    assert [(e.tool, e.target) for e in events] == [
        ("routing", f"default=it-helper; {CHANNEL}->it-helper")
    ]


@pytest.mark.parametrize("case", ["no token", "env only"], ids=["not-connected", "env-only"])
async def test_the_notion_section_says_what_is_in_use_when_nothing_is_connected(case, monkeypatch):
    """Fails if an env-only tenant is told "Not connected", or Disconnect with nothing stored
    changes or records anything instead of explaining there is nothing to remove."""
    seen: dict = {}
    h = await build_harness(seen)
    monkeypatch.delenv("NOTION_TOKEN_ACME", raising=False)
    if case == "env only":
        monkeypatch.setenv("NOTION_TOKEN_ACME", "ntn-env-fallback")
    admin = await h.signed_in(ROOT)
    async with using(admin):
        page = (await admin.get(SETTINGS)).text
        if case == "env only":
            assert "Using the" in page and "NOTION_TOKEN_ACME" in page
            assert "(not connected in the console)" in page
            assert "Not connected." not in page
            refused = await post(admin, f"{SETTINGS}/notion/disconnect", await csrf_token(admin))
            assert refused.status_code == 409
            assert "Nothing is stored to disconnect" in refused.text
            config = (await h.store.load(TENANT)).config
            assert await h.credentials.notion_token(config) == "ntn-env-fallback"
        else:
            assert "Not connected." in page
            assert "Using the" not in page
    assert await h.credential_store.get(TENANT, NOTION) is None
    assert [e for e in h.audit.events if e.kind == "connector"] == []


@pytest.mark.parametrize(
    "case",
    ["consent", "re-pick replaces", "revoke"],
    ids=["consent", "replaces", "revoke"],
)
async def test_the_pick_flow_keeps_the_sign_in_only_with_consent(case):
    """Fails if a consented pick does not store the creator's token encrypted (or shows it on
    a page), a second consented pick keeps the old token, or revoke keeps the token, records
    nothing, or a second revoke records again."""
    seen: dict = {}
    h = await build_harness(seen)
    await h.plane.add_agent(TENANT, ROOT, "it-helper", published_definition())
    admin = await h.signed_in(ROOT)
    kind = ids.notion_creator_kind(ROOT)
    version = f"/ui/{TENANT}/agents/it-helper/versions/1"

    async def pick(code: str):
        start = await post(admin, f"{version}/pick/start", await csrf_token(admin), consent="yes")
        assert start.status_code == 303
        query = parse_qs(urlsplit(start.headers["location"]).query)
        response = await admin.get(CALLBACK, params={"code": code, "state": query["state"][0]})
        assert response.status_code == 303
        # The consented flow still leaves a listing the creator can pick from.
        listing = await admin.get(response.headers["location"])
        assert listing.status_code == 200 and "A page" in listing.text

    async with using(admin):
        await pick("abc")
        if case == "re-pick replaces":
            await pick("second")
        if case == "revoke":
            response = await post(admin, f"{version}/pick/revoke", await csrf_token(admin))
            assert response.status_code == 303
            assert response.headers["location"].endswith("done=consent-revoked")

        stored = await h.credential_store.get(TENANT, kind)
        # Settle point: the flow is over; a fresh page load shows the state, never the token.
        page = (await admin.get(version)).text
        if case == "revoke":
            assert stored is None
            assert "Stop keeping it" not in page
            again = await post(admin, f"{version}/pick/revoke", await csrf_token(admin))
            assert again.status_code == 409
            assert [(e.kind, e.tool) for e in h.audit.events if e.kind == "connector"] == [
                ("connector", "creator_consent"),
                ("connector", "creator_consent_revoked"),
            ]
        else:
            code = "second" if case == "re-pick replaces" else "abc"
            token = f"{WORKSPACE_TOKEN}:{code}"
            assert stored is not None
            assert decrypt_for_tenant(h.keys, TENANT, stored.ciphertext, stored.key_version) == (
                token.encode()
            )
            assert token not in page
            assert 'name="consent" value="yes" checked' in page
            assert "Stop keeping it" in page
            connects = [e for e in h.audit.events if e.kind == "connector"]
            assert [(e.tool, e.target, e.identity) for e in connects] == [
                ("creator_consent", ROOT, f"user:{ROOT}")
            ] * (2 if case == "re-pick replaces" else 1)
            assert all(WORKSPACE_TOKEN not in event_json(e) for e in h.audit.events)


async def test_the_pick_purpose_flow_stores_no_credential():
    """Fails if a Choose-from-Notion flow stores a Notion credential or records a connector
    event: only the connect purpose stores the workspace token."""
    seen: dict = {}
    h = await build_harness(seen)
    await h.plane.add_agent(TENANT, ROOT, "it-helper", published_definition())
    admin = await h.signed_in(ROOT)
    async with using(admin):
        version = f"/ui/{TENANT}/agents/it-helper/versions/1"
        start = await post(admin, f"{version}/pick/start", await csrf_token(admin))
        assert start.status_code == 303
        query = parse_qs(urlsplit(start.headers["location"]).query)
        assert query["owner"] == ["user"]
        response = await admin.get(CALLBACK, params={"code": "abc", "state": query["state"][0]})
        assert response.status_code == 303
        assert response.headers["location"].endswith("/pick")
        assert seen["code"] == "abc"
        # Settle point: the flow is over; the settings page still shows Notion not connected.
        page = (await admin.get(SETTINGS)).text
        assert "Connect Notion" in page and "Connected to" not in page
    assert await h.credential_store.get(TENANT, NOTION) is None
    assert [e for e in h.audit.events if e.kind == "connector"] == []
