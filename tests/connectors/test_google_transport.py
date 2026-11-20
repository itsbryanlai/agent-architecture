"""RealGoogleAccounts over a mocked transport, with google-auth's and requests' own
error types: one adapter answers the deployment credential refresh and the API calls."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2 import service_account

from agentos.connectors.google_iam import (
    CLOUD_PLATFORM,
    DRIVE_SCOPES,
    GoogleError,
    RealGoogleAccounts,
)
from tests.helpers.console_harness import Clock

PROJECT = "test-project"
ACCOUNT_ID = "ag-abcdefghijklmnopqrstuvwx"
EMAIL = f"{ACCOUNT_ID}@{PROJECT}.iam.gserviceaccount.com"
TOKEN_URI = "https://oauth2.googleapis.com/token"
CREATE_PATH = f"/v1/projects/{PROJECT}/serviceAccounts"
ACCOUNT_PATH = f"{CREATE_PATH}/{EMAIL}"
MINT_PATH = f"/v1/projects/-/serviceAccounts/{EMAIL}:generateAccessToken"


@pytest.fixture(scope="module")
def deployment_key() -> str:
    """A service-account JSON key with a test-generated RSA key; no real credential."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    return json.dumps(
        {
            "type": "service_account",
            "project_id": PROJECT,
            "private_key_id": "key-1",
            "private_key": pem,
            "client_email": f"deploy@{PROJECT}.iam.gserviceaccount.com",
            "token_uri": TOKEN_URI,
        }
    )


def reply(request: requests.PreparedRequest, status: int, body: dict) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body).encode()
    response.headers["Content-Type"] = "application/json"
    response.request = request
    response.url = request.url
    return response


class MockAdapter(requests.adapters.HTTPAdapter):
    """The session's only network: ``handler`` answers every request, and the requests
    sent are kept for the test to read."""

    def __init__(self, handler):
        super().__init__()
        self.handler = handler
        self.sent: list[requests.PreparedRequest] = []

    def send(self, request, **kwargs):
        self.sent.append(request)
        return self.handler(request)


def client_with(handler, deployment_key, clock=None, token_status: int = 200):
    def answered(request):
        if request.url.startswith(TOKEN_URI):
            if token_status != 200:
                return reply(request, token_status, {"error": "invalid_grant"})
            return reply(
                request,
                200,
                {"access_token": "deployment-token", "expires_in": 3600, "token_type": "Bearer"},
            )
        return handler(request)

    credentials = service_account.Credentials.from_service_account_info(
        json.loads(deployment_key), scopes=[CLOUD_PLATFORM]
    )
    adapter = MockAdapter(answered)
    # The credential refresh runs on its own session inside AuthorizedSession, so the
    # mocked adapter must be mounted there too (auth_request) — one transport for all.
    auth_session = requests.Session()
    auth_session.mount("https://", adapter)
    session = AuthorizedSession(credentials, auth_request=Request(auth_session))
    session.mount("https://", adapter)
    drive_session = requests.Session()
    drive_session.mount("https://", adapter)
    fixed = clock or (lambda: datetime(2026, 11, 20, 12, 0, tzinfo=UTC))
    return (
        RealGoogleAccounts(
            PROJECT, deployment_key, session=session, drive_session=drive_session, clock=fixed
        ),
        adapter.sent,
    )


@pytest.mark.parametrize(
    ("case", "code"),
    [
        ("created", None),
        ("already exists", None),
        ("refused", "permission_denied"),
        ("connection lost", "unavailable"),
        ("key refused", "invalid_key"),
    ],
    ids=["created", "already-exists", "refused", "connection-lost", "key-refused"],
)
async def test_create_account_sends_the_id_and_maps_googles_answers(case, code, deployment_key):
    """Fails if create does not POST the account ID to the project, a 409 does not find
    the existing account, or a refusal, a lost connection or a refused deployment key
    comes back as anything but a GoogleError with its code."""

    def handler(request):
        if request.method == "POST" and request.path_url == CREATE_PATH:
            if case == "created":
                return reply(request, 201, {"email": EMAIL, "accountId": ACCOUNT_ID})
            if case == "already exists":
                return reply(request, 409, {"error": {"status": "ALREADY_EXISTS"}})
            if case == "refused":
                return reply(request, 403, {"error": {"status": "PERMISSION_DENIED"}})
            raise requests.exceptions.ConnectionError("connection refused")
        if request.method == "GET" and request.path_url == ACCOUNT_PATH:
            return reply(request, 200, {"email": EMAIL})
        return reply(request, 404, {"error": {"status": "NOT_FOUND"}})

    client, sent = client_with(
        handler, deployment_key, token_status=400 if case == "key refused" else 200
    )
    if code is None:
        assert await client.ensure_account(ACCOUNT_ID) == EMAIL
    else:
        with pytest.raises(GoogleError) as err:
            await client.ensure_account(ACCOUNT_ID)
        assert err.value.code == code
    if case == "created":
        posts = [r for r in sent if r.method == "POST" and r.path_url == CREATE_PATH]
        assert len(posts) == 1
        assert json.loads(posts[0].body)["accountId"] == ACCOUNT_ID
    if case == "already exists":
        assert [r.method for r in sent if r.path_url in (CREATE_PATH, ACCOUNT_PATH)] == [
            "POST",
            "GET",
        ]


@pytest.mark.parametrize(
    ("status", "expected"),
    [(200, True), (404, False), (403, "permission_denied")],
    ids=["deleted", "already-gone", "refused"],
)
async def test_delete_account_maps_gone_to_false_and_a_refusal_to_its_code(
    status, expected, deployment_key
):
    """Fails if a deleted account does not read as deleted, a 404 raises instead of
    reading as already gone, or a refusal comes back as anything but its GoogleError
    code."""
    client, sent = client_with(lambda request: reply(request, status, {}), deployment_key)
    if isinstance(expected, bool):
        assert await client.delete_account(ACCOUNT_ID) is expected
    else:
        with pytest.raises(GoogleError) as err:
            await client.delete_account(ACCOUNT_ID)
        assert err.value.code == expected
    assert [r.method for r in sent if r.path_url == ACCOUNT_PATH] == ["DELETE"]


async def test_listing_mints_a_drive_token_once_and_reuses_it_until_it_expires(deployment_key):
    """Fails if every list mints a new token, the mint asks for scopes other than the
    Drive read scope, the list does not carry the minted token, or an expired token is
    reused."""
    clock = Clock()
    clock.now = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)

    def handler(request):
        if request.method == "POST" and request.path_url == MINT_PATH:
            return reply(
                request,
                200,
                {"accessToken": "tenant-token", "expireTime": "2026-11-20T13:00:00Z"},
            )
        if request.path_url.startswith("/drive/v3/files"):
            return reply(request, 200, {"files": []})
        if request.path_url.startswith("/drive/v3/drives"):
            return reply(request, 200, {"drives": []})
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key, clock=clock)
    await client.list_shared(EMAIL)
    clock.advance(minutes=58)  # 12:58, within a minute of the 13:00 expiry
    await client.list_shared(EMAIL)
    mints = [r for r in sent if r.path_url == MINT_PATH]
    assert len(mints) == 1
    assert json.loads(mints[0].body)["scope"] == list(DRIVE_SCOPES)
    files = [r for r in sent if r.path_url.startswith("/drive/v3/files")]
    assert files[0].headers["Authorization"] == "Bearer tenant-token"
    clock.advance(minutes=2)  # 13:00, expired
    await client.list_shared(EMAIL)
    assert len([r for r in sent if r.path_url == MINT_PATH]) == 2


async def test_listing_pages_files_and_shared_drives_into_names_ids_and_links(deployment_key):
    """Fails if listing stops at the first page of files, misses the shared drives the
    account is a member of, or returns anything but names, IDs and links."""

    def handler(request):
        if request.path_url.startswith("/drive/v3/files"):
            if "pageToken=p2" in request.path_url:
                return reply(
                    request,
                    200,
                    {
                        "files": [
                            {
                                "id": "f2",
                                "name": "Roster",
                                "webViewLink": "https://drive.google.com/file/d/f2",
                            }
                        ]
                    },
                )
            return reply(
                request,
                200,
                {
                    "files": [
                        {
                            "id": "f1",
                            "name": "Guide",
                            "webViewLink": "https://drive.google.com/file/d/f1",
                        }
                    ],
                    "nextPageToken": "p2",
                },
            )
        if request.path_url.startswith("/drive/v3/drives"):
            return reply(request, 200, {"drives": [{"id": "d1", "name": "Team drive"}]})
        if request.method == "POST":
            return reply(
                request,
                200,
                {"accessToken": "tenant-token", "expireTime": "2026-11-20T13:00:00Z"},
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    items = await client.list_shared(EMAIL)
    assert [(i.id, i.name, i.link) for i in items] == [
        ("f1", "Guide", "https://drive.google.com/file/d/f1"),
        ("f2", "Roster", "https://drive.google.com/file/d/f2"),
        ("d1", "Team drive", "https://drive.google.com/drive/folders/d1"),
    ]
    files = [r for r in sent if r.path_url.startswith("/drive/v3/files")]
    assert len(files) == 2
    assert "sharedWithMe" in files[0].path_url
    assert "includeItemsFromAllDrives=true" in files[0].path_url
