"""Drive as the agent's own Google user (D-53): one sign-in by an admin, one stored token."""

from __future__ import annotations

import asyncio
import html
import secrets as token_source
from urllib.parse import parse_qs, urlsplit

import pytest
from psycopg.errors import UniqueViolation

from agentos import ids
from agentos.audit import event_json
from agentos.config import ConfigError
from agentos.connectors.google_iam import FakeGoogleAccounts, GoogleError
from agentos.control import Forbidden
from agentos.credentials import GOOGLE_USER, InMemoryCredentialStore, TenantCredentials
from agentos.drive_connections import (
    SERVICE_ACCOUNT,
    USER,
    InMemoryDriveConnectionStore,
)
from agentos.google_signin import (
    NOT_WORKSPACE,
    GoogleSignIn,
    GoogleSignInConfig,
    GoogleUserGrant,
    HttpGoogleOAuth,
)
from agentos.payload_codec import decrypt_for_tenant
from tests.conftest import GLOBEX, TENANT
from tests.helpers.console_harness import (
    OWNER,
    ROOT,
    ConsoleHarness,
    csrf_token,
    post,
    using,
)
from tests.helpers.google_oauth import (
    AGENT_EMAIL,
    CLIENT_ID,
    NOW,
    REFRESH_TOKEN,
    google_transport,
)
from tests.helpers.keys import RefreshableKeyStore

SETTINGS = f"/ui/{TENANT}/settings"
CALLBACK = "/auth/google/callback"


async def build(seen: dict | None = None, *, signin: bool = True, **claims):
    """A console with Drive, an encrypted credential store and, unless ``signin`` is False,
    the sign-in as the agent's Google account over a mocked token endpoint."""
    keys = RefreshableKeyStore({(TENANT, 1): token_source.token_bytes(32)})
    await keys.refresh()
    credential_store = InMemoryCredentialStore()
    credentials = TenantCredentials(credential_store, keys)
    drive = InMemoryDriveConnectionStore()
    google = FakeGoogleAccounts()
    config = GoogleSignInConfig(CLIENT_ID, "google-secret")
    oauth = HttpGoogleOAuth(
        config, google_transport({} if seen is None else seen, **claims), clock=lambda: NOW
    )
    h = ConsoleHarness(
        drive=drive,
        google=google,
        credentials=credentials,
        google_signin=GoogleSignIn(config, oauth) if signin else None,
    )
    await h.seed_members()
    h.keys, h.credentials, h.credential_store = keys, credentials, credential_store
    h.drive, h.google = drive, google
    return h


async def stored_token(h) -> str | None:
    row = await h.credential_store.get(TENANT, GOOGLE_USER)
    if row is None:
        return None
    return decrypt_for_tenant(h.keys, TENANT, row.ciphertext, row.key_version).decode()


async def start_state(c) -> str:
    response = await post(c, f"{SETTINGS}/drive/google/start", await csrf_token(c))
    assert response.status_code == 303
    location = response.headers["location"]
    assert location.startswith("https://accounts.google.com/o/oauth2/v2/auth")
    query = parse_qs(urlsplit(location).query)
    assert query["access_type"] == ["offline"] and query["prompt"] == ["consent"]
    assert query["scope"] == ["openid email https://www.googleapis.com/auth/drive"]
    return query["state"][0]


@pytest.mark.parametrize(
    "case",
    ["workspace account", "personal account", "wrong audience", "unverified email"]
    + ["bad state", "reused state", "creator starts"],
    ids=["workspace", "no-hd", "wrong-aud", "unverified", "bad-state", "reused-state", "creator"],
)
async def test_the_google_callback_connects_only_a_workspace_user_this_session_began(case):
    """Fails if the callback stores a sign-in from an account with no Workspace domain or a
    token meant for another client or an unverified address, accepts a state this session never
    began or one already spent, writes anything on a refusal or leaves the refused sign-in
    valid at Google, or lets a creator start the sign-in."""
    claims = {
        "personal account": {"hd": None},
        "wrong audience": {"aud": "another-client"},
        "unverified email": {"email_verified": False},
    }.get(case, {})
    seen: dict = {}
    h = await build(seen, **claims)
    admin = await h.signed_in(ROOT)
    async with using(admin):
        if case == "creator starts":
            creator = await h.signed_in(OWNER)
            async with using(creator):
                response = await post(
                    creator, f"{SETTINGS}/drive/google/start", await csrf_token(creator)
                )
            assert response.status_code == 403 and "location" not in response.headers
        elif case == "bad state":
            response = await admin.get(CALLBACK, params={"code": "c", "state": "forged"})
            assert response.status_code == 400
            assert "form" not in seen  # Google was never asked
        elif case == "reused state":
            state = await start_state(admin)
            first = await admin.get(CALLBACK, params={"code": "abc", "state": state})
            assert first.status_code == 303
            response = await admin.get(CALLBACK, params={"code": "abc", "state": state})
            assert response.status_code == 400
        else:
            state = await start_state(admin)
            response = await admin.get(CALLBACK, params={"code": "abc", "state": state})
            if case == "workspace account":
                assert response.status_code == 303
                assert response.headers["location"].endswith("?done=drive-user-connected")
            elif case == "personal account":
                assert response.status_code == 422
                assert NOT_WORKSPACE in html.unescape(response.text)
            else:
                assert response.status_code == 503

    row = await h.drive.get(TENANT)
    if case in ("workspace account", "reused state"):
        assert (row.kind, row.service_account_email, row.status) == (USER, AGENT_EMAIL, "connected")
        assert await stored_token(h) == f"{REFRESH_TOKEN}:abc"
        assert [(e.kind, e.tool, e.target, e.identity) for e in h.audit.events] == [
            ("connector", "drive_connect", AGENT_EMAIL, f"user:{ROOT}")
        ]
        assert all(REFRESH_TOKEN not in event_json(e) for e in h.audit.events)
    else:
        assert row is None and await stored_token(h) is None and h.audit.events == []
    refused_after_issue = case in ("personal account", "wrong audience", "unverified email")
    assert seen.get("revoked", []) == ([f"{REFRESH_TOKEN}:abc"] if refused_after_issue else [])


async def connect_as_user(h, email: str = AGENT_EMAIL, token: str = REFRESH_TOKEN) -> None:
    await h.plane.connect_drive_user(TENANT, ROOT, GoogleUserGrant(email, token))


@pytest.mark.parametrize(
    "case",
    ["service account to user", "user to service account", "disconnect a user connection"],
    ids=["to-user", "to-service-account", "disconnect"],
)
async def test_switching_the_drive_account_keeps_the_service_account_and_removes_the_token(case):
    """Fails if signing in as the user deletes the service account at Google, switching back
    leaves the user's token stored or unrevoked, or disconnecting a user connection keeps the
    token, skips the revoke, or deletes the user's address as if it were a service account."""
    h = await build()
    account = ids.drive_account_id(TENANT)
    if case != "user to service account":
        await h.plane.connect_drive(TENANT, ROOT)
    if case == "service account to user":
        await connect_as_user(h)
        row = await h.drive.get(TENANT)
        assert (row.kind, row.service_account_email) == (USER, AGENT_EMAIL)
        assert account in h.google.accounts and h.google.delete_calls == []
        assert await stored_token(h) == REFRESH_TOKEN
    elif case == "user to service account":
        await connect_as_user(h)
        await h.plane.connect_drive(TENANT, ROOT)
        row = await h.drive.get(TENANT)
        assert (row.kind, row.service_account_email) == (
            SERVICE_ACCOUNT,
            h.google.accounts[account],
        )
        assert await stored_token(h) is None
        assert h.google.revoke_calls == [(AGENT_EMAIL, REFRESH_TOKEN)]
    else:
        await connect_as_user(h)
        assert await h.plane.disconnect_drive(TENANT, ROOT) is True
        assert await h.drive.get(TENANT) is None and await stored_token(h) is None
        assert h.google.revoke_calls == [(AGENT_EMAIL, REFRESH_TOKEN)]
        assert h.google.accounts == {} and h.google.delete_calls == [
            h.google.account_email(account)
        ]
        assert h.audit.events[-1].tool == "drive_disconnect"
        assert h.audit.events[-1].target == AGENT_EMAIL


@pytest.mark.parametrize(
    "case",
    [
        "retry after lost response",
        "overlapping callbacks",
        "reconnect as the same address",
        "another organisation's account",
        "another organisation wins the race",
        "creator",
    ],
    ids=["retry", "overlap", "reconnect", "other-organisation", "race", "creator"],
)
async def test_signing_in_as_the_agent_user_stores_one_credential_and_records_each_change_once(
    case,
):
    """Fails if a retry after a lost revoke response or an overlapping second callback stores
    a second credential or audit event, a new sign-in for the same address is not recorded
    and its old token not revoked, an address another organisation uses (also one that won
    the race) is taken over, left valid at Google or a 500, or a creator connects."""
    h = await build()
    grant = GoogleUserGrant(AGENT_EMAIL, REFRESH_TOKEN)
    renewed = GoogleUserGrant(AGENT_EMAIL, "1//renewed")
    if case == "retry after lost response":
        await h.plane.connect_drive_user(TENANT, ROOT, grant)
        # The renewal's revoke of the old token takes effect, but its response is lost.
        h.google.fail_after_write("revoke")
        await h.plane.connect_drive_user(TENANT, ROOT, renewed)
        await h.plane.connect_drive_user(TENANT, ROOT, renewed)
    elif case == "overlapping callbacks":
        await asyncio.gather(
            h.plane.connect_drive_user(TENANT, ROOT, grant),
            h.plane.connect_drive_user(TENANT, ROOT, grant),
        )
    elif case == "reconnect as the same address":
        await h.plane.connect_drive_user(TENANT, ROOT, grant)
        await h.plane.connect_drive_user(TENANT, ROOT, renewed)
    elif case == "another organisation's account":
        await h.drive.connect(GLOBEX, AGENT_EMAIL, NOW, kind=USER)
        with pytest.raises(ConfigError):
            await h.plane.connect_drive_user(TENANT, ROOT, grant)
    elif case == "another organisation wins the race":
        # The directory read finds no owner; the write then meets the other tenant's row.
        async def first_to_commit(*args, **kwargs):
            raise UniqueViolation("address taken")

        h.drive.connect = first_to_commit
        with pytest.raises(ConfigError):
            await h.plane.connect_drive_user(TENANT, ROOT, grant)
    else:
        with pytest.raises(Forbidden):
            await h.plane.connect_drive_user(TENANT, OWNER, grant)
    if case in ("retry after lost response", "reconnect as the same address"):
        assert await stored_token(h) == "1//renewed"
        assert [(e.tool, e.target) for e in h.audit.events] == [("drive_connect", AGENT_EMAIL)] * 2
        assert h.google.revoke_calls == [(AGENT_EMAIL, REFRESH_TOKEN)]
    elif case == "overlapping callbacks":
        assert (await h.drive.get(TENANT)).service_account_email == AGENT_EMAIL
        assert await stored_token(h) == REFRESH_TOKEN
        assert [(e.tool, e.target) for e in h.audit.events] == [("drive_connect", AGENT_EMAIL)]
        assert h.google.revoke_calls == []
    else:
        assert await h.drive.get(TENANT) is None and await stored_token(h) is None
        assert h.audit.events == []
        refused = case.startswith("another")
        assert h.google.revoke_calls == ([(AGENT_EMAIL, REFRESH_TOKEN)] if refused else [])


@pytest.mark.parametrize(
    "case",
    ["no sign-in", "service account", "agent user", "reconnect needed"],
    ids=["not-configured", "service-account", "agent-user", "reconnect-needed"],
)
async def test_the_settings_page_shows_each_kind_of_drive_connection(case):
    """Fails if the sign-in button shows where the OAuth client is not set, the agent user's
    address or the switch back is missing for a user connection, or a lost sign-in shows as
    a raw code instead of the plain words and a button to sign in again."""
    h = await build(signin=case != "no sign-in")
    if case == "service account":
        await h.plane.connect_drive(TENANT, ROOT)
    if case in ("agent user", "reconnect needed"):
        await connect_as_user(h)
    if case == "reconnect needed":
        h.google.fail_next_list(GoogleError("reconnect_needed"))
    c = await h.signed_in(ROOT)
    async with using(c):
        page = html.unescape((await c.get(SETTINGS)).text)
    button = "Sign in as the agent's Google account"
    assert (button in page) == (case == "service account")
    if case in ("agent user", "reconnect needed"):
        assert AGENT_EMAIL in page and "Switch back to the built-in address" in page
    if case == "reconnect needed":
        assert "The agent's Google account needs to sign in again" in page
        assert "Sign in again as the agent's Google account" in page
        assert "reconnect_needed" not in page
