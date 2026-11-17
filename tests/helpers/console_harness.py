"""A web console on fakes: in-memory stores, a fake Slack and a clock the test sets."""

import base64
import json
import re
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from agentos.agent_store import InMemoryAgentStore
from agentos.audience_pauses import InMemoryPauseStore
from agentos.audit import InMemoryAuditStore
from agentos.budgets import InMemoryBudgetStore
from agentos.config import TenantsConfig, parse_agents_config
from agentos.control import ControlPlane
from agentos.control_api import create_app
from agentos.grant_checker import NotionGrantChecker
from agentos.members import InMemoryMemberStore
from agentos.sessions import InMemorySessionStore
from agentos.slack_signin import (
    ISSUER,
    TEAM_CLAIM,
    USER_CLAIM,
    Console,
    SignInConfig,
    SlackSession,
    SlackSignInError,
)
from agentos.switches import InMemorySwitchStore
from agentos.usage import InMemoryUsageStore
from tests.conftest import (
    TEAM,
    TENANT,
    make_fake_notion,
    two_tenant_dicts,
)

CONSOLE_URL = "https://console.example.com"
CLIENT_ID = "1111.2222"
ROOT = "U_ROOT"  # an admin from the tenant file
ADMIN2 = "U_ADMIN2"
OWNER = "U_OWNER"  # a creator
OTHER = "U_OTHER"  # a creator who owns nothing
OUTSIDER = "U_OUTSIDER"  # no role


SIGNING_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
SIGNING_KID = "test-key-1"


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def id_token(claims: dict, key=SIGNING_KEY, kid: str = SIGNING_KID) -> str:
    """An RS256 ID token signed with ``key``; the harness's Slack publishes ``SIGNING_KEY``."""
    signing_input = (
        f"{_b64(json.dumps({'alg': 'RS256', 'kid': kid}).encode())}"
        f".{_b64(json.dumps(claims).encode())}"
    )
    signature = key.sign(signing_input.encode(), padding.PKCS1v15(), hashes.SHA256())
    return f"{signing_input}.{_b64(signature)}"


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **delta) -> None:
        self.now += timedelta(**delta)


class FakeSlack:
    """Stands in for Slack's token endpoint; no network."""

    def __init__(self, clock: Clock):
        self.clock = clock
        self.calls: list[tuple[str, str]] = []
        self.error: str | None = None
        self.user = ROOT
        self.team = TEAM
        self.nonce = ""
        self.overrides: dict = {}

    async def signing_key(self, kid: str):
        return SIGNING_KEY.public_key() if kid == SIGNING_KID else None

    async def exchange_code(self, code: str, redirect_uri: str) -> dict:
        self.calls.append((code, redirect_uri))
        if self.error:
            raise SlackSignInError(self.error)
        claims = {
            "iss": ISSUER,
            "aud": CLIENT_ID,
            "exp": int(self.clock().timestamp()) + 300,
            "nonce": self.nonce,
            USER_CLAIM: self.user,
            TEAM_CLAIM: self.team,
        }
        claims.update(self.overrides)
        return {"ok": True, "id_token": id_token(claims)}


class ConsoleHarness:
    def __init__(
        self,
        console_url: str = CONSOLE_URL,
        single_tenant: bool = False,
        audience=None,
        picker=None,
        require_grant_picker: bool = False,
    ):
        self.agents = InMemoryAgentStore()
        self.members = InMemoryMemberStore()
        self.audit = InMemoryAuditStore()
        self.usage = InMemoryUsageStore()
        self.budgets = InMemoryBudgetStore()
        self.pauses = InMemoryPauseStore()
        self.switches = InMemorySwitchStore()
        self.sessions = InMemorySessionStore()
        self.notion = make_fake_notion()
        self.clock = Clock()
        self.slack = FakeSlack(self.clock)
        acme, globex = two_tenant_dicts()
        acme["admins"] = globex["admins"] = [ROOT]
        configs = [parse_agents_config(acme)]
        if not single_tenant:
            configs.append(parse_agents_config(globex))
        self.tenants = TenantsConfig.of(*configs)
        self.plane = ControlPlane(
            self.agents,
            self.members,
            self.audit,
            self.tenants,
            NotionGrantChecker(lambda tenant_id: self.notion),
            audience,
            usage=self.usage,
            budgets=self.budgets,
            clock=self.clock,
            pauses=self.pauses,
            switches=self.switches,
            require_grant_picker=require_grant_picker,
        )
        self.sign_in = SignInConfig(CLIENT_ID, "made-up-client-secret", console_url)
        self.console = Console(self.sign_in, self.slack, self.clock)
        self.app = create_app(
            self.plane,
            self.tenants,
            SlackSession(),
            sessions=self.sessions,
            console=self.console,
            picker=picker,
        )
        self.base_url = console_url
        self._picker = picker

    def picker_state(self):
        """Everything the picker holds, for a test that looks for what must not be kept."""
        p = self._picker
        return None if p is None else (vars(p.sessions), vars(p.oauth), p.config)

    async def seed_members(self) -> None:
        await self.members.add_member(TENANT, ADMIN2, "admin", ROOT)
        await self.members.add_member(TENANT, OWNER, "creator", ROOT)
        await self.members.add_member(TENANT, OTHER, "creator", ROOT)

    def client(self) -> httpx.AsyncClient:
        transport = httpx.ASGITransport(app=self.app)
        return httpx.AsyncClient(transport=transport, base_url=self.base_url)

    async def sign_in_as(self, client: httpx.AsyncClient, user: str, team: str = TEAM):
        """Run the whole sign-in; the client keeps the session cookie. Returns the callback."""
        login = await client.get("/login")
        query = parse_qs(urlsplit(login.headers["location"]).query)
        self.slack.user, self.slack.team = user, team
        self.slack.nonce = query["nonce"][0]
        return await client.get(
            "/auth/slack/callback", params={"code": "made-up-code", "state": query["state"][0]}
        )

    async def signed_in(self, user: str, team: str = TEAM) -> httpx.AsyncClient:
        client = self.client()
        response = await self.sign_in_as(client, user, team)
        assert response.status_code == 303, response.text
        return client


@asynccontextmanager
async def using(client: httpx.AsyncClient):
    """Close a client that ``signed_in`` already used."""
    try:
        yield client
    finally:
        await client.aclose()


def csrf_in(html: str) -> str:
    match = re.search(r'name="csrf" value="([^"]+)"', html)
    assert match, "the page carries no CSRF token"
    return match.group(1)


async def csrf_token(client: httpx.AsyncClient, tenant: str = TENANT) -> str:
    return csrf_in((await client.get(f"/ui/{tenant}/agents")).text)


def form_headers(origin: str = CONSOLE_URL) -> dict[str, str]:
    return {"Origin": origin}


async def post(client: httpx.AsyncClient, path: str, token: str | None, **fields):
    """A form post from our own page."""
    data = dict(fields)
    if token is not None:
        data["csrf"] = token
    headers = {**form_headers(), "Content-Type": "application/x-www-form-urlencoded"}
    return await client.post(path, content=urlencode(data), headers=headers)
