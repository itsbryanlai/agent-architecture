"""Sign in with Slack: the redirect, the callback's checks, the session and its cookie.

Slack is a fake: no request leaves the process.
"""

import hashlib
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from agentos.config import ConfigError, Settings
from agentos.control_api import build_process_app, check_bind_host
from agentos.slack_signin import (
    AUTHORIZE_URL,
    TOKEN_URL,
    HttpSlackOidc,
    SlackSignInError,
    read_session_cookie,
    sign_in_from_settings,
)
from tests.conftest import GLOBEX, GLOBEX_TEAM, TEAM, TENANT
from tests.console_harness import (
    CLIENT_ID,
    CONSOLE_URL,
    OUTSIDER,
    OWNER,
    ROOT,
    ConsoleHarness,
    csrf_token,
    id_token,
    post,
    using,
)


@pytest.fixture
async def h():
    harness = ConsoleHarness()
    await harness.seed_members()
    return harness


def authorize_query(response) -> dict[str, list[str]]:
    return parse_qs(urlsplit(response.headers["location"]).query)


def set_cookie(response, name: str) -> str:
    [header] = [v for v in response.headers.get_list("set-cookie") if v.startswith(f"{name}=")]
    return header


# --- the redirect to Slack ----------------------------------------------------------


async def test_login_redirects_to_slack_with_state_nonce_client_and_redirect_uri(h):
    async with h.client() as c:
        response = await c.get("/login")
    assert response.status_code == 303
    assert response.headers["location"].startswith(AUTHORIZE_URL + "?")
    query = authorize_query(response)
    assert query["response_type"] == ["code"]
    assert query["scope"] == ["openid profile"]
    assert query["client_id"] == [CLIENT_ID]
    assert query["redirect_uri"] == [f"{CONSOLE_URL}/auth/slack/callback"]
    assert len(query["state"][0]) >= 40 and len(query["nonce"][0]) >= 40
    assert query["state"] != query["nonce"]
    assert "team" not in query  # two tenants: Slack's picker decides


async def test_login_names_the_workspace_when_there_is_one_tenant():
    single = ConsoleHarness(single_tenant=True)
    async with single.client() as c:
        query = authorize_query(await c.get("/login"))
    assert query["team"] == [TEAM]


async def test_login_values_are_fresh_each_time_and_kept_in_a_short_lived_cookie(h):
    async with h.client() as c:
        first, second = await c.get("/login"), await c.get("/login")
    assert authorize_query(first)["state"] != authorize_query(second)["state"]
    cookie = set_cookie(first, "agentos_login").lower()
    assert "httponly" in cookie and "secure" in cookie and "samesite=lax" in cookie
    assert "max-age=600" in cookie and "path=/auth/slack" in cookie
    state = authorize_query(first)["state"][0]
    nonce = authorize_query(first)["nonce"][0]
    assert f"{state}.{nonce}" in set_cookie(first, "agentos_login")


# --- the callback's refusals --------------------------------------------------------


async def start_login(h, c):
    login = await c.get("/login")
    query = authorize_query(login)
    h.slack.nonce = query["nonce"][0]
    return query["state"][0]


async def assert_no_session(h, c, response, status):
    assert response.status_code == status, response.text
    assert "agentos_session" not in response.headers.get("set-cookie", "")
    assert h.sessions._rows == {}


async def test_callback_with_a_wrong_or_missing_state_is_refused(h):
    async with h.client() as c:
        state = await start_login(h, c)
        for params in [
            {"code": "x", "state": state + "tampered"},
            {"code": "x", "state": ""},
            {"code": "x"},
        ]:
            await assert_no_session(h, c, await c.get("/auth/slack/callback", params=params), 400)
    assert h.slack.calls == []


async def test_callback_without_the_login_cookie_is_refused(h):
    async with h.client() as c:
        state = await start_login(h, c)
        c.cookies.clear()
        response = await c.get("/auth/slack/callback", params={"code": "x", "state": state})
        await assert_no_session(h, c, response, 400)
    assert h.slack.calls == []


async def test_callback_after_slack_reports_an_error_is_refused(h):
    async with h.client() as c:
        state = await start_login(h, c)
        response = await c.get(
            "/auth/slack/callback", params={"error": "access_denied", "state": state}
        )
        await assert_no_session(h, c, response, 400)
    assert h.slack.calls == []


async def test_the_login_cookie_is_spent_by_the_callback(h):
    async with h.client() as c:
        state = await start_login(h, c)
        response = await c.get("/auth/slack/callback", params={"code": "x", "state": state})
        assert response.status_code == 303
        assert 'agentos_login=""' in set_cookie(response, "agentos_login") or (
            "Max-Age=0" in set_cookie(response, "agentos_login")
        )
        again = await c.get("/auth/slack/callback", params={"code": "x", "state": state})
        assert again.status_code == 400


async def test_a_nonce_that_does_not_match_is_refused(h):
    async with h.client() as c:
        state = await start_login(h, c)
        h.slack.nonce = "some-other-nonce-value-that-is-long-enough"
        response = await c.get("/auth/slack/callback", params={"code": "x", "state": state})
        await assert_no_session(h, c, response, 502)


async def test_a_token_endpoint_error_is_refused_without_naming_it(h):
    async with h.client() as c:
        state = await start_login(h, c)
        h.slack.error = "invalid_code with details"
        response = await c.get("/auth/slack/callback", params={"code": "x", "state": state})
        await assert_no_session(h, c, response, 502)
        assert "invalid_code" not in response.text


@pytest.mark.parametrize(
    "overrides",
    [
        {"iss": "https://evil.example"},
        {"aud": "someone-elses-client"},
        {"aud": ["a", "b"]},
        {"exp": 1},
        {"exp": "never"},
        {"https://slack.com/user_id": ""},
        {"https://slack.com/team_id": None},
    ],
)
async def test_an_id_token_that_fails_a_check_is_refused(h, overrides):
    async with h.client() as c:
        state = await start_login(h, c)
        h.slack.overrides = overrides
        response = await c.get("/auth/slack/callback", params={"code": "x", "state": state})
        await assert_no_session(h, c, response, 502)


async def test_an_audience_list_that_holds_the_client_is_accepted(h):
    async with h.client() as c:
        state = await start_login(h, c)
        h.slack.overrides = {"aud": ["other", CLIENT_ID]}
        response = await c.get("/auth/slack/callback", params={"code": "x", "state": state})
        assert response.status_code == 303


async def test_a_workspace_that_is_no_tenant_is_refused_without_naming_internals(h):
    async with h.client() as c:
        response = await h.sign_in_as(c, ROOT, team="T0STRANGER")
        await assert_no_session(h, c, response, 403)
    for internal in (TENANT, GLOBEX, "T0STRANGER", TEAM, GLOBEX_TEAM):
        assert internal not in response.text


async def test_a_user_with_no_role_sees_no_access_and_gets_no_session(h):
    async with h.client() as c:
        response = await h.sign_in_as(c, OUTSIDER)
        await assert_no_session(h, c, response, 403)
        assert "no role" in response.text
        assert (await c.get(f"/ui/{TENANT}/agents")).status_code == 303  # still signed out


async def test_the_workspace_decides_the_tenant(h):
    async with h.client() as c:
        response = await h.sign_in_as(c, ROOT, team=GLOBEX_TEAM)
    assert response.headers["location"] == f"/ui/{GLOBEX}/agents"
    assert response.headers["set-cookie"]
    assert read_session_cookie(c.cookies.get("agentos_session"))[0] == GLOBEX


# --- the session --------------------------------------------------------------------


async def test_success_sets_a_locked_down_cookie_and_stores_only_the_hash(h):
    async with h.client() as c:
        response = await h.sign_in_as(c, OWNER)
    assert response.status_code == 303
    assert response.headers["location"] == f"/ui/{TENANT}/agents"
    cookie = set_cookie(response, "agentos_session")
    lowered = cookie.lower()
    assert "httponly" in lowered and "secure" in lowered and "samesite=lax" in lowered
    assert "max-age=28800" in lowered and "path=/" in lowered
    value = cookie.split(";")[0].split("=", 1)[1]
    tenant, token = value.split(".")
    assert tenant == TENANT and len(token) >= 40  # 256 bits, url-safe
    [(key, record)] = h.sessions._rows.items()
    assert key == (TENANT, hashlib.sha256(token.encode()).hexdigest())
    stored = repr(h.sessions._rows)
    assert token not in stored
    assert record.user_id == OWNER and record.tenant_id == TENANT
    assert (record.expires_at - record.created_at).total_seconds() == 8 * 3600
    assert record.csrf_secret and record.csrf_secret != token
    assert h.slack.calls == [("made-up-code", f"{CONSOLE_URL}/auth/slack/callback")]


async def test_sessions_are_unique_per_sign_in(h):
    async with h.client() as one, h.client() as two:
        await h.sign_in_as(one, OWNER)
        await h.sign_in_as(two, OWNER)
    assert one.cookies.get("agentos_session") != two.cookies.get("agentos_session")
    assert len(h.sessions._rows) == 2


async def test_a_local_http_console_url_drops_the_secure_flag_and_only_then():
    local = ConsoleHarness("http://127.0.0.1:8100")
    async with local.client() as c:
        login = await c.get("/login")
        assert "secure" not in set_cookie(login, "agentos_login").lower()
        response = await local.sign_in_as(c, ROOT)
        cookie = set_cookie(response, "agentos_session").lower()
        assert "secure" not in cookie and "httponly" in cookie and "samesite=lax" in cookie
    assert local.sign_in.secure_cookies is False
    assert ConsoleHarness().sign_in.secure_cookies is True


async def test_an_expired_session_is_refused_and_a_live_one_is_not(h):
    c = await h.signed_in(OWNER)
    async with using(c):
        h.clock.advance(hours=7, minutes=59)
        assert (await c.get(f"/ui/{TENANT}/agents")).status_code == 200
        h.clock.advance(minutes=2)
        redirect = await c.get(f"/ui/{TENANT}/agents")
        assert redirect.status_code == 303 and redirect.headers["location"] == "/login"
        api = await c.get(f"/tenants/{TENANT}/me")
        assert api.status_code == 401


async def test_an_expired_session_is_cleaned_up_by_the_next_sign_in(h):
    first = await h.signed_in(OWNER)
    h.clock.advance(hours=9)
    second = await h.signed_in(ROOT)
    await first.aclose()
    await second.aclose()
    assert [r.user_id for r in h.sessions._rows.values()] == [ROOT]


async def test_logout_deletes_the_session_and_clears_the_cookie(h):
    c = await h.signed_in(OWNER)
    async with using(c):
        token = await csrf_token(c)
        response = await post(c, "/logout", token)
        assert response.status_code == 200
        assert "Max-Age=0" in set_cookie(response, "agentos_session")
        assert h.sessions._rows == {}
        assert (await c.get(f"/ui/{TENANT}/agents")).status_code == 303


async def test_logout_without_the_csrf_token_changes_nothing(h):
    c = await h.signed_in(OWNER)
    async with using(c):
        for token in (None, "wrong"):
            response = await post(c, "/logout", token)
            assert response.status_code == 403
        assert len(h.sessions._rows) == 1
        assert (await c.get(f"/ui/{TENANT}/agents")).status_code == 200


async def test_a_session_of_another_tenant_is_refused_for_pages_and_the_api(h):
    c = await h.signed_in(OWNER)
    async with using(c):
        page = await c.get(f"/ui/{GLOBEX}/agents")
        assert page.status_code == 403
        api = await c.get(f"/tenants/{GLOBEX}/me")
        assert api.status_code == 403
        assert (await c.get(f"/tenants/{TENANT}/me")).json() == {
            "user_id": OWNER,
            "role": "creator",
        }
        # A cookie that names the other tenant does not find this tenant's row.
        token = c.cookies.get("agentos_session").split(".", 1)[1]
        c.cookies.clear()
        c.cookies.set("agentos_session", f"{GLOBEX}.{token}", domain="console.example.com")
        assert (await c.get(f"/tenants/{GLOBEX}/me")).status_code == 401
        assert (await c.get(f"/ui/{GLOBEX}/agents")).status_code == 303


async def test_the_json_api_needs_a_session_and_acts_as_its_user(h):
    async with h.client() as anonymous:
        assert (await anonymous.get(f"/tenants/{TENANT}/me")).status_code == 401
        assert (await anonymous.get(f"/tenants/{TENANT}/agents")).status_code == 401
    c = await h.signed_in(OWNER)
    async with using(c):
        made = await c.post(
            f"/tenants/{TENANT}/agents",
            json={"agent_id": "from-api", "definition": SIMPLE},
            headers={"Origin": CONSOLE_URL},
        )
        assert made.status_code == 201
        assert [e.identity for e in h.audit.events] == [f"user:{OWNER}"]
        # A JSON write must still be JSON.
        refused = await c.post(
            f"/tenants/{TENANT}/agents", data={"agent_id": "x"}, headers={"Origin": CONSOLE_URL}
        )
        assert refused.status_code == 415


SIMPLE = {"name": "Simple", "system_prompt": "Help.", "max_steps": 3}


async def test_nothing_in_a_request_can_name_the_acting_user(h):
    c = await h.signed_in(OWNER)
    async with using(c):
        response = await c.get(
            f"/tenants/{TENANT}/me",
            headers={"X-Slack-User": ROOT, "X-Forwarded-User": ROOT},
            params={"actor": ROOT, "user_id": ROOT},
        )
    assert response.json()["user_id"] == OWNER


async def test_the_index_goes_to_the_users_tenant_or_to_sign_in(h):
    async with h.client() as c:
        assert (await c.get("/")).headers["location"] == "/login"
    c = await h.signed_in(OWNER)
    async with using(c):
        assert (await c.get("/")).headers["location"] == f"/ui/{TENANT}/agents"


# --- the Slack client ---------------------------------------------------------------


async def test_the_real_client_posts_the_code_to_slacks_token_endpoint():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"ok": True, "id_token": "a.b.c"})

    client = HttpSlackOidc("cid", "made-up-secret", httpx.MockTransport(handler))
    body = await client.exchange_code("the-code", "https://c.example/auth/slack/callback")
    assert body == {"ok": True, "id_token": "a.b.c"}
    [request] = seen
    assert str(request.url) == TOKEN_URL and request.method == "POST"
    form = parse_qs(request.content.decode())
    assert form == {
        "client_id": ["cid"],
        "client_secret": ["made-up-secret"],
        "code": ["the-code"],
        "redirect_uri": ["https://c.example/auth/slack/callback"],
        "grant_type": ["authorization_code"],
    }


@pytest.mark.parametrize(
    "reply",
    [
        httpx.Response(200, json={"ok": False, "error": "invalid_code"}),
        httpx.Response(500, json={"ok": True}),
        httpx.Response(200, text="not json"),
        httpx.Response(200, json=["not", "an", "object"]),
    ],
)
async def test_the_real_client_turns_a_bad_answer_into_a_sign_in_error(reply):
    client = HttpSlackOidc("cid", "s", httpx.MockTransport(lambda request: reply))
    with pytest.raises(SlackSignInError):
        await client.exchange_code("c", "https://c.example/auth/slack/callback")


async def test_the_real_client_turns_a_network_failure_into_a_sign_in_error():
    def down(request):
        raise httpx.ConnectError("secret-bearing detail")

    client = HttpSlackOidc("cid", "s", httpx.MockTransport(down))
    with pytest.raises(SlackSignInError) as err:
        await client.exchange_code("c", "https://c.example/auth/slack/callback")
    assert "secret-bearing" not in str(err.value)


def test_a_fake_id_token_is_three_parts():
    assert id_token({"a": 1}).count(".") == 2


# --- settings and the bind rules ----------------------------------------------------


def settings(**values) -> Settings:
    return Settings(**values)


def full(url: str = CONSOLE_URL) -> Settings:
    return settings(slack_client_id="cid", slack_client_secret="made-up", console_url=url)


def test_sign_in_is_off_when_none_of_its_settings_is_set():
    assert sign_in_from_settings(settings()) is None


def test_sign_in_is_configured_from_all_three_settings():
    config = sign_in_from_settings(full(CONSOLE_URL + "/"))
    assert config.console_url == CONSOLE_URL
    assert config.redirect_uri == f"{CONSOLE_URL}/auth/slack/callback"
    assert config.host == "console.example.com" and config.secure_cookies
    assert "made-up" not in repr(config) and "made-up" not in repr(full())


@pytest.mark.parametrize(
    "partial",
    [
        {"slack_client_id": "cid"},
        {"slack_client_secret": "s"},
        {"console_url": CONSOLE_URL},
        {"slack_client_id": "cid", "console_url": CONSOLE_URL},
    ],
)
def test_a_partial_configuration_is_an_error_naming_the_missing_variables(partial):
    with pytest.raises(ConfigError) as err:
        sign_in_from_settings(settings(**partial))
    assert "not set:" in str(err.value)
    assert "made-up" not in str(err.value)


@pytest.mark.parametrize(
    "url",
    [
        "http://console.example.com",
        "http://192.168.1.5:8100",
        "http://127.0.0.1",
        "ftp://console.example.com",
        "console.example.com",
        "https://console.example.com/app",
        "https://console.example.com?x=1",
        "https://user@console.example.com",
    ],
)
def test_a_console_url_that_is_not_https_or_local_http_is_refused(url):
    with pytest.raises(ConfigError):
        sign_in_from_settings(full(url))


@pytest.mark.parametrize("url", ["http://127.0.0.1:8100", "http://localhost:3000"])
def test_a_local_http_url_with_a_port_is_allowed_without_secure_cookies(url):
    config = sign_in_from_settings(full(url))
    assert config.secure_cookies is False and config.console_url == url


def test_a_public_bind_needs_sign_in_configured():
    for host in ("0.0.0.0", "192.168.1.20", "::"):
        with pytest.raises(SystemExit) as refused:
            check_bind_host(host)
        assert "refusing to listen" in str(refused.value)
        check_bind_host(host, sign_in_configured=True)


def test_without_sign_in_the_process_is_loopback_and_acts_as_the_operator():
    environ = {"AGENTOS_CONTROL_ACTOR": "U0123ABCDEF"}
    with pytest.raises(SystemExit):
        build_process_app("0.0.0.0", 8100, settings(), environ)
    app = build_process_app("127.0.0.1", 8100, settings(), environ)
    assert app.state.deps.console is None
    with pytest.raises(SystemExit):
        build_process_app("127.0.0.1", 8100, settings(), {})


async def test_with_sign_in_the_process_binds_publicly_and_the_fixed_operator_is_off(h):
    for environ in ({}, {"AGENTOS_CONTROL_ACTOR": ROOT}):
        app = build_process_app("0.0.0.0", 8100, full(), environ)
        assert app.state.deps.console.sign_in.console_url == CONSOLE_URL
        app.state.deps.tenants = h.tenants  # what the real start-up fills in
        app.state.deps.plane = h.plane
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url=CONSOLE_URL) as c:
            # Nothing acts without a session, whether or not the operator variable is set.
            assert (await c.get(f"/tenants/{TENANT}/me")).status_code == 401
            assert (await c.get("/login")).status_code == 303


def test_a_half_configured_process_refuses_to_start():
    with pytest.raises(SystemExit) as refused:
        build_process_app("127.0.0.1", 8100, settings(console_url=CONSOLE_URL), {})
    assert "SLACK_CLIENT_ID" in str(refused.value)


def test_the_settings_are_read_from_the_three_variables(monkeypatch):
    from agentos.config import load_settings

    monkeypatch.setenv("SLACK_CLIENT_ID", "made-up-id")
    monkeypatch.setenv("SLACK_CLIENT_SECRET", "made-up-secret")
    monkeypatch.setenv("AGENTOS_CONSOLE_URL", CONSOLE_URL)
    loaded = load_settings()
    assert (loaded.slack_client_id, loaded.console_url) == ("made-up-id", CONSOLE_URL)
    assert loaded.slack_client_secret == "made-up-secret"
    assert "made-up-secret" not in repr(loaded)
    for name in ("SLACK_CLIENT_ID", "SLACK_CLIENT_SECRET", "AGENTOS_CONSOLE_URL"):
        monkeypatch.delenv(name)
    assert sign_in_from_settings(load_settings()) is None
