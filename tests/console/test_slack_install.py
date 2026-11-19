"""The Slack OAuth install: the page, the callback's checks, and the tenant it creates.

Slack is a fake, or the real client over a mocked transport; no request leaves the process.
"""

from __future__ import annotations

import html
import re
import secrets as token_source
from urllib.parse import parse_qs

import httpx
import pytest
import yaml

from agentos import ids
from agentos.agent_store import InMemoryAgentStore
from agentos.audit import InMemoryAuditStore
from agentos.config import AgentsConfig, ConfigError
from agentos.connectors.notion import FakeNotionAPI
from agentos.control import ControlPlane
from agentos.control_api import create_app
from agentos.credentials import SLACK_BOT, InMemoryCredentialStore, TenantCredentials
from agentos.grant_checker import NotionGrantChecker
from agentos.members import InMemoryMemberStore
from agentos.sessions import InMemorySessionStore
from agentos.slack_install import (
    AUTHORIZE_URL,
    INSTALL_CALLBACK_PATH,
    INSTALL_COOKIE,
    INSTALL_PATH,
    HttpSlackOAuth,
    InstallConfig,
    Installer,
    SlackInstallError,
    app_name_from_manifest,
    bot_scopes_from_manifest,
    grant_from_response,
    installed_tenant_secrets,
)
from agentos.slack_signin import Console, SignInConfig, SlackSession
from agentos.tenant_store import InMemoryTenantStore, TenantDirectory
from agentos.unit_of_work import InMemoryUnitOfWork
from tests.helpers.console_harness import CLIENT_ID, CONSOLE_URL, Clock
from tests.helpers.keys import RefreshableKeyStore

NEW_TEAM = "T0NEWTEAM"
INSTALLER = "U_INSTALLER"
BOT_TOKEN = "xoxb-made-up-bot-token"
REPLACEMENT_TOKEN = "xoxb-made-up-replacement"
SCOPES = ("chat:write", "im:history")


class FakeOAuth:
    """Stands in for Slack's OAuth v2 token endpoint; no network."""

    def __init__(self):
        self.body = {
            "ok": True,
            "token_type": "bot",
            "access_token": BOT_TOKEN,
            "team": {"id": NEW_TEAM, "name": "New Team"},
            "authed_user": {"id": INSTALLER},
        }
        self.calls: list[tuple[str, str]] = []

    async def exchange_code(self, code, redirect_uri):
        self.calls.append((code, redirect_uri))
        return self.body


class InstallHarness:
    """A console with the install flow on in-memory stores and a fake Slack."""

    def __init__(self, allowed=(NEW_TEAM,)):
        self.tenant_id = ids.tenant_for_slack_team(NEW_TEAM)
        self.key = token_source.token_bytes(32)
        self.keys = RefreshableKeyStore({(self.tenant_id, 1): self.key})
        self.tenants = InMemoryTenantStore()
        self.credential_store = InMemoryCredentialStore()
        self.credentials = TenantCredentials(self.credential_store, self.keys)
        self.audit = InMemoryAuditStore()
        self.oauth = FakeOAuth()
        self.key_calls: list[str] = []
        self.config = InstallConfig(
            client_id=CLIENT_ID,
            client_secret="made-up-client-secret",
            console_url=CONSOLE_URL,
            allowed_teams=frozenset(allowed),
            scopes=SCOPES,
        )
        uow = InMemoryUnitOfWork(self.tenants, self.credential_store, self.audit)

        async def ensure_key(tenant_id, conn):
            self.key_calls.append(tenant_id)
            return 1, self.key

        self.installer = Installer(
            self.config, self.oauth, self.tenants, self.credentials, self.audit, uow, ensure_key
        )
        directory = TenantDirectory(self.tenants)
        plane = ControlPlane(
            InMemoryAgentStore(),
            InMemoryMemberStore(),
            self.audit,
            directory,
            NotionGrantChecker(lambda t: FakeNotionAPI()),
        )
        sign_in = SignInConfig(CLIENT_ID, "made-up-client-secret", CONSOLE_URL)
        self.app = create_app(
            plane,
            directory,
            SlackSession(),
            sessions=InMemorySessionStore(),
            console=Console(sign_in, None, Clock()),
            install=self.installer,
        )

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url=CONSOLE_URL)


@pytest.fixture
async def h():
    harness = InstallHarness()
    await harness.keys.refresh()
    return harness


async def start_install(client) -> str:
    page = await client.get(INSTALL_PATH)
    assert page.status_code == 200
    return client.cookies.get(INSTALL_COOKIE)


async def callback(client, state, code="made-up-code"):
    return await client.get(INSTALL_CALLBACK_PATH, params={"code": code, "state": state})


# --- the page ---------------------------------------------------------------------


@pytest.mark.parametrize("enabled", [True, False], ids=["allowlist set", "allowlist unset"])
async def test_the_install_page_binds_state_and_offers_the_configured_scopes(enabled):
    """Fails if the button requests other scopes, the state is not bound to the browser cookie,
    or an unset allowlist still offers the button."""
    harness = InstallHarness(allowed=(NEW_TEAM,) if enabled else ())
    async with harness.client() as c:
        page = await c.get(INSTALL_PATH)
    assert page.status_code == 200
    if not enabled:
        assert "not enabled" in page.text
        assert AUTHORIZE_URL not in page.text
        assert INSTALL_COOKIE not in c.cookies
        return
    match = re.search(rf'href="{re.escape(AUTHORIZE_URL)}\?([^"]+)"', page.text)
    assert match, "the page carries no Add to Slack link"
    query = parse_qs(html.unescape(match.group(1)))
    assert query["scope"] == [",".join(SCOPES)]
    assert query["client_id"] == [CLIENT_ID]
    assert query["redirect_uri"] == [f"{CONSOLE_URL}{INSTALL_CALLBACK_PATH}"]
    state = query["state"][0]
    assert len(state) >= 40
    assert c.cookies.get(INSTALL_COOKIE) == state
    [cookie] = [
        v for v in page.headers.get_list("set-cookie") if v.startswith(f"{INSTALL_COOKIE}=")
    ]
    cookie = cookie.lower()
    assert "httponly" in cookie and "secure" in cookie and "samesite=lax" in cookie
    assert "max-age=600" in cookie and "path=/install/slack" in cookie


@pytest.mark.parametrize("valid", [True, False], ids=["bot scopes", "no bot scopes"])
def test_the_scopes_come_from_the_manifest(tmp_path, valid):
    """Fails if the install reads scopes from anywhere but the manifest's bot list."""
    path = tmp_path / "manifest.yaml"
    body = (
        {"oauth_config": {"scopes": {"bot": ["chat:write", "im:history"]}}}
        if valid
        else {"oauth_config": {"scopes": {}}}
    )
    path.write_text(yaml.safe_dump(body))
    if valid:
        assert bot_scopes_from_manifest(str(path)) == ("chat:write", "im:history")
    else:
        with pytest.raises(ConfigError, match="scopes.bot"):
            bot_scopes_from_manifest(str(path))


# --- the callback -----------------------------------------------------------------


def stored_tenant(tenant_id: str, team_id: str) -> AgentsConfig:
    """A tenant already in the table, as a YAML import would have left it."""
    return AgentsConfig(
        tenant_id=tenant_id,
        slack_team_id=team_id,
        agents={},
        default_agent=None,
        notion_workspace_id=None,
        secrets=installed_tenant_secrets(tenant_id),
        channels={},
        admins=("U_SEED_ADMIN",),
    )


@pytest.mark.parametrize(
    "case",
    [
        "allowlisted team",
        "team not allowlisted",
        "bad state",
        "reused state",
        "reinstall",
        "id collision",
        "same team different tenant",
        "error parameter",
        "no state cookie",
        "reinstall by another user",
    ],
    ids=[
        "success",
        "not-allowlisted",
        "bad-state",
        "reused-state",
        "reinstall",
        "id-collision",
        "same-team-other-tenant",
        "error-param",
        "no-cookie",
        "reinstall-other-user",
    ],
)
async def test_the_install_callback(h, case):
    """Fails if an allowed install does not create the tenant, key, token and first admin with
    one install audit, a refused or conflicting one writes anything, or a reinstall creates a
    second tenant or changes the admins."""
    if case == "team not allowlisted":
        h = InstallHarness(allowed=("T_OTHER",))
        await h.keys.refresh()
    if case == "id collision":
        await h.tenants.insert(stored_tenant(h.tenant_id, "T_OTHER_WS"))
    if case == "same team different tenant":
        await h.tenants.insert(stored_tenant("otherco", NEW_TEAM))
    async with h.client() as c:
        if case == "no state cookie":
            # Another browser: a well-formed state, but no cookie was ever set here.
            response = await callback(c, "a" * 40)
        else:
            state = await start_install(c)
            if case == "error parameter":
                response = await c.get(
                    INSTALL_CALLBACK_PATH,
                    params={"code": "made-up-code", "state": state, "error": "access_denied"},
                )
            else:
                response = await callback(c, state + "tampered" if case == "bad state" else state)
        if case == "reused state":
            assert response.status_code == 200
            response = await callback(c, state)
        if case in ("reinstall", "reinstall by another user"):
            assert response.status_code == 200
            h.oauth.body["access_token"] = REPLACEMENT_TOKEN
            if case == "reinstall by another user":
                h.oauth.body["authed_user"] = {"id": "U_SECOND"}
            response = await callback(c, await start_install(c))

    if case == "team not allowlisted":
        assert response.status_code == 403
        assert "allowlist" in response.text
        assert await h.tenants.list_refs() == []
        assert h.audit.events == []
        assert await h.credential_store.get(h.tenant_id, SLACK_BOT) is None
        assert h.key_calls == []
        return
    if case in ("bad state", "error parameter", "no state cookie"):
        assert response.status_code == 400
        assert await h.tenants.list_refs() == []
        assert h.audit.events == []
        assert await h.credential_store.get(h.tenant_id, SLACK_BOT) is None
        assert h.key_calls == []
        return
    if case in ("id collision", "same team different tenant"):
        assert response.status_code == 409
        assert "already" in response.text
        refs = await h.tenants.list_refs()
        assert [r.tenant_id for r in refs] == [h.tenant_id if case == "id collision" else "otherco"]
        kept = await h.tenants.load(refs[0].tenant_id)
        assert kept.config.admins == ("U_SEED_ADMIN",)
        assert kept.config.slack_team_id == ("T_OTHER_WS" if case == "id collision" else NEW_TEAM)
        assert h.audit.events == []
        assert await h.credential_store.get(h.tenant_id, SLACK_BOT) is None
        assert h.key_calls == []
        return
    if case == "reused state":
        assert response.status_code == 400
        assert len(await h.tenants.list_refs()) == 1
        assert len(h.audit.events) == 1
        assert len(h.oauth.calls) == 1
        return

    assert response.status_code == 200
    assert f"/ui/{h.tenant_id}/agents" in response.text
    refs = await h.tenants.list_refs()
    assert [r.tenant_id for r in refs] == [h.tenant_id]
    row = await h.tenants.load(h.tenant_id)
    assert row.enabled is True
    assert (row.config.slack_team_id, row.config.admins, row.config.default_agent) == (
        NEW_TEAM,
        (INSTALLER,),
        None,
    )
    if case in ("reinstall", "reinstall by another user"):
        assert await h.credentials.slack_bot_token(row.config) == REPLACEMENT_TOKEN
        assert [e.tool for e in h.audit.events] == ["install", "reinstall"]
        assert row.config.admins == (INSTALLER,)
        assert (await h.credential_store.get(h.tenant_id, SLACK_BOT)).key_version == 1
        if case == "reinstall by another user":
            assert h.audit.events[1].requester_user_id == "U_SECOND"
    else:
        assert h.key_calls == [h.tenant_id]
        assert await h.credentials.slack_bot_token(row.config) == BOT_TOKEN
        assert len(h.audit.events) == 1
        event = h.audit.events[0]
        assert (event.kind, event.tool, event.requester_user_id, event.identity, event.target) == (
            "install",
            "install",
            INSTALLER,
            f"user:{INSTALLER}",
            NEW_TEAM,
        )
        assert event.agent_id is None
        assert BOT_TOKEN not in repr(event)


@pytest.mark.parametrize(
    "failure", ["ok false", "connection lost"], ids=["ok-false", "connect-error"]
)
async def test_a_failed_exchange_writes_nothing_and_names_the_cause(h, failure):
    """Fails if a refused or lost code exchange still creates a tenant, or the page hides the
    cause or shows a token."""
    if failure == "ok false":
        transport = httpx.MockTransport(
            lambda request: httpx.Response(200, json={"ok": False, "error": "invalid_code"})
        )
    else:

        def down(request):
            raise httpx.ConnectError("connection refused")

        transport = httpx.MockTransport(down)
    h.installer.oauth = HttpSlackOAuth(CLIENT_ID, "made-up-client-secret", transport)
    async with h.client() as c:
        state = await start_install(c)
        response = await callback(c, state)

    assert response.status_code == 503
    if failure == "ok false":
        assert "invalid_code" in response.text
    else:
        assert "ConnectError" in response.text
    assert BOT_TOKEN not in response.text
    assert await h.tenants.list_refs() == []
    assert h.audit.events == []
    assert await h.credential_store.get(h.tenant_id, SLACK_BOT) is None
    assert h.key_calls == []


@pytest.mark.parametrize(
    ("body", "accepted"),
    [
        (
            {
                "token_type": "bot",
                "access_token": "t",
                "team": {"id": "T1"},
                "authed_user": {"id": "U1"},
            },
            True,
        ),
        (
            {
                "token_type": "user",
                "access_token": "t",
                "team": {"id": "T1"},
                "authed_user": {"id": "U1"},
            },
            False,
        ),
        ({"token_type": "bot", "team": {"id": "T1"}, "authed_user": {"id": "U1"}}, False),
        ({"token_type": "bot", "access_token": "t", "authed_user": {"id": "U1"}}, False),
        ({"token_type": "bot", "access_token": "t", "team": {"id": "T1"}}, False),
    ],
    ids=["bot grant", "user token", "no token", "no team", "no installer"],
)
def test_only_a_bot_grant_naming_a_workspace_and_an_installer_is_accepted(body, accepted):
    """Fails if a response without a bot token, a workspace or an installer could install."""
    if accepted:
        assert grant_from_response(body) == ("T1", "t", "U1")
    else:
        with pytest.raises(SlackInstallError):
            grant_from_response(body)


@pytest.mark.parametrize("team_known", [True, False], ids=["team known", "team unknown"])
async def test_an_install_refusal_logs_the_tenant_when_the_team_is_known(h, caplog, team_known):
    """Fails if a refusal after Slack named the workspace logs no tenant ID, or a refusal
    before that logs one."""
    if team_known:
        h = InstallHarness(allowed=("T_OTHER",))
        await h.keys.refresh()
    else:
        transport = httpx.MockTransport(
            lambda request: httpx.Response(200, json={"ok": False, "error": "invalid_code"})
        )
        h.installer.oauth = HttpSlackOAuth(CLIENT_ID, "made-up-client-secret", transport)
    async with h.client() as c:
        state = await start_install(c)
        response = await callback(c, state)

    assert response.status_code == (403 if team_known else 503)
    assert "install refused" in caplog.text
    if team_known:
        assert f"tenant_id={h.tenant_id}" in caplog.text
    else:
        assert "tenant_id=" not in caplog.text


@pytest.mark.parametrize(
    ("manifest", "expected"),
    [
        ({"display_information": {"name": "Acme Helper"}}, "Acme Helper"),
        ({"display_information": {}}, "This app"),
        (None, "This app"),
    ],
    ids=["name", "no name", "no file"],
)
def test_the_app_name_comes_from_the_manifest(tmp_path, manifest, expected):
    """Fails if the not-set-up reply's app name comes from anywhere but the manifest's
    display_information.name, or a missing name or file raises instead of falling back."""
    path = tmp_path / "manifest.yaml"
    if manifest is not None:
        path.write_text(yaml.safe_dump(manifest))

    assert app_name_from_manifest(str(path)) == expected
