"""RealGoogleAccounts over a mocked transport, with google-auth's and requests' own
error types: one adapter answers the deployment credential refresh and the API calls."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2 import service_account

from agentos.config import DriveGrant
from agentos.connectors import drive_errors
from agentos.connectors.drive import DriveConnector, DriveReader
from agentos.connectors.google_iam import (
    CLOUD_PLATFORM,
    DRIVE_SCOPES,
    LIST_CAP,
    MIME_DOC,
    MIME_FOLDER,
    MIME_SHEET,
    MIME_SLIDES,
    DriveFile,
    DriveSection,
    GoogleError,
    RealGoogleAccounts,
    SheetMetadata,
    SheetTab,
)
from tests.helpers.console_harness import Clock

PROJECT = "test-project"
ACCOUNT_ID = "ag-abcdefghijklmnopqrstuvwx"
EMAIL = f"{ACCOUNT_ID}@{PROJECT}.iam.gserviceaccount.com"
# The row's stored email after a project move: the delete must target it, not the ID
# re-derived at the current project.
OTHER_EMAIL = f"{ACCOUNT_ID}@old-project.iam.gserviceaccount.com"
TOKEN_URI = "https://oauth2.googleapis.com/token"
CREATE_PATH = f"/v1/projects/{PROJECT}/serviceAccounts"
ACCOUNT_PATH = f"{CREATE_PATH}/{EMAIL}"
OTHER_ACCOUNT_PATH = f"{CREATE_PATH}/{OTHER_EMAIL}"
MINT_PATH = f"/v1/projects/-/serviceAccounts/{EMAIL}:generateAccessToken"
EXPIRES = "2026-11-20T13:00:00Z"


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


def reply(request: requests.PreparedRequest, status: int, body: dict | str) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    if isinstance(body, str):
        response._content = body.encode()
        response.headers["Content-Type"] = "text/html"
    else:
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
        ("recently deleted", "recently_deleted"),
        ("refused", "permission_denied"),
        ("bad request", "refused"),
        ("connection lost", "unavailable"),
        ("key refused", "invalid_key"),
    ],
    ids=[
        "created",
        "already-exists",
        "recently-deleted",
        "refused",
        "bad-request",
        "connection-lost",
        "key-refused",
    ],
)
async def test_create_account_sends_the_id_and_maps_googles_answers(case, code, deployment_key):
    """Fails if create does not POST the account ID to the project, a 409 does not find
    the existing account, a 409 whose get answers 404 does not read as recently deleted,
    or a refusal, a bad request, a lost connection or a refused deployment key comes
    back as anything but a GoogleError with its code."""

    def handler(request):
        if request.method == "POST" and request.path_url == CREATE_PATH:
            if case == "created":
                return reply(request, 201, {"email": EMAIL, "accountId": ACCOUNT_ID})
            if case in ("already exists", "recently deleted"):
                return reply(request, 409, {"error": {"status": "ALREADY_EXISTS"}})
            if case == "refused":
                return reply(request, 403, {"error": {"status": "PERMISSION_DENIED"}})
            if case == "bad request":
                return reply(request, 400, {"error": {"status": "INVALID_ARGUMENT"}})
            raise requests.exceptions.ConnectionError("connection refused")
        if request.method == "GET" and request.path_url == ACCOUNT_PATH:
            if case == "recently deleted":
                return reply(request, 404, {"error": {"status": "NOT_FOUND"}})
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
    if case in ("already exists", "recently deleted"):
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
    """Fails if a delete does not target the stored email it is given, a deleted account
    does not read as deleted, a 404 raises instead of reading as already gone, or a
    refusal comes back as anything but its GoogleError code."""
    client, sent = client_with(lambda request: reply(request, status, {}), deployment_key)
    if isinstance(expected, bool):
        assert await client.delete_account(OTHER_EMAIL) is expected
    else:
        with pytest.raises(GoogleError) as err:
            await client.delete_account(OTHER_EMAIL)
        assert err.value.code == expected
    assert [r.method for r in sent if r.path_url == OTHER_ACCOUNT_PATH] == ["DELETE"]


@pytest.mark.parametrize(
    "case",
    [
        "create without email",
        "create not json",
        "mint without token",
        "list not json",
        "file without name",
    ],
    ids=[
        "create-missing-email",
        "create-not-json",
        "mint-missing-token",
        "list-not-json",
        "file-missing-name",
    ],
)
async def test_a_2xx_body_missing_a_field_or_not_json_reads_as_unavailable(case, deployment_key):
    """Fails if a 2xx answer with a missing field or a non-JSON body escapes as anything
    but GoogleError('unavailable')."""

    def handler(request):
        if request.method == "POST" and request.path_url == CREATE_PATH:
            if case == "create without email":
                return reply(request, 201, {})
            return reply(request, 201, "<html>maintenance</html>")
        if request.path_url == MINT_PATH:
            if case == "mint without token":
                return reply(request, 200, {})
            return reply(request, 200, {"accessToken": "tenant-token", "expireTime": EXPIRES})
        if request.path_url.startswith("/drive/v3/files"):
            if case == "list not json":
                return reply(request, 200, "<html>maintenance</html>")
            return reply(request, 200, {"files": [{"id": "f1"}]})
        return reply(request, 200, {"drives": []})

    client, _sent = client_with(handler, deployment_key)
    with pytest.raises(GoogleError) as err:
        if case.startswith("create"):
            await client.ensure_account(ACCOUNT_ID)
        else:
            await client.list_shared(EMAIL)
    assert err.value.code == "unavailable"


@pytest.mark.parametrize(
    ("kind", "names", "not_names"),
    [("iam", "deployment", "sharing"), ("drive", "sharing", "deployment")],
    ids=["iam", "drive"],
)
async def test_a_refusal_words_the_deployment_role_on_iam_and_the_accounts_access_on_drive(
    kind, names, not_names, deployment_key
):
    """Fails if a 401/403 names the deployment account's role on a Drive call or the
    tenant account's refused access on an IAM call."""

    def handler(request):
        if kind == "iam":
            return reply(request, 403, {"error": {"status": "PERMISSION_DENIED"}})
        if request.path_url == MINT_PATH:
            return reply(request, 200, {"accessToken": "tenant-token", "expireTime": EXPIRES})
        return reply(request, 403, {"error": {"status": "PERMISSION_DENIED"}})

    client, _sent = client_with(handler, deployment_key)
    with pytest.raises(GoogleError) as err:
        if kind == "iam":
            await client.ensure_account(ACCOUNT_ID)
        else:
            await client.list_shared(EMAIL)
    assert err.value.code == "permission_denied"
    assert names in str(err.value)
    assert not_names not in str(err.value)


async def test_listing_mints_a_drive_token_once_and_reuses_it_until_it_expires(deployment_key):
    """Fails if every list mints a new token, the mint asks for scopes other than the
    Drive scope, the list does not carry the minted token, or an expired token is
    reused."""
    clock = Clock()
    clock.now = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)

    def handler(request):
        if request.method == "POST" and request.path_url == MINT_PATH:
            return reply(
                request,
                200,
                {"accessToken": "tenant-token", "expireTime": EXPIRES},
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


@pytest.mark.parametrize("is_drive_root", [True, False], ids=["shared-drive-root", "folder"])
async def test_listing_one_folder_reads_drives_get_first_and_lists_a_drive_root_drive_scoped(
    is_drive_root, deployment_key
):
    """Fails if a shared drive's root is listed without the drive-scoped query, a plain folder
    (drives.get 404, then files.get) is listed drive-scoped, the level is not ordered folders
    first, or a full page does not stop at the cap and say there is more."""
    children = [
        {"id": f"c{i}", "name": f"n{i}", "mimeType": MIME_DOC, "driveId": "node-1"}
        for i in range(LIST_CAP)
    ]

    def handler(request):
        if request.path_url == MINT_PATH:
            return reply(request, 200, {"accessToken": "tenant-token", "expireTime": EXPIRES})
        if request.path_url.startswith("/drive/v3/drives/node-1"):
            if is_drive_root:
                return reply(request, 200, {"id": "node-1", "name": "Team drive"})
            return reply(request, 404, {"error": {"code": 404}})
        if request.path_url.startswith("/drive/v3/files/node-1"):
            return reply(request, 200, {"id": "node-1"})
        return reply(request, 200, {"files": children, "nextPageToken": "more"})

    client, sent = client_with(handler, deployment_key)
    listing = await client.list_folder(EMAIL, "node-1")
    assert len(listing.items) == LIST_CAP and listing.more is True
    assert listing.items[0].drive_id == "node-1"
    reads = [p for p in (urlsplit(r.url).path for r in sent) if p.startswith("/drive/")]
    expected = ["/drive/v3/drives/node-1"]
    expected += [] if is_drive_root else ["/drive/v3/files/node-1"]
    assert reads == [*expected, "/drive/v3/files"]
    query = parse_qs(urlsplit(sent[-1].url).query)
    assert query["q"] == ["'node-1' in parents and trashed=false"]
    assert query["orderBy"] == ["folder,name"]
    assert query["pageSize"] == [str(LIST_CAP)]
    if is_drive_root:
        assert (query["corpora"], query["driveId"]) == (["drive"], ["node-1"])
    else:
        assert "corpora" not in query and "driveId" not in query


async def test_a_recreated_account_mints_a_fresh_token(deployment_key):
    """Fails if a list after a delete and a re-create reuses the deleted account's
    cached token."""
    mints = 0

    def handler(request):
        nonlocal mints
        if request.path_url == MINT_PATH:
            mints += 1
            return reply(
                request, 200, {"accessToken": f"tenant-token-{mints}", "expireTime": EXPIRES}
            )
        if request.method == "POST" and request.path_url == CREATE_PATH:
            return reply(request, 201, {"email": EMAIL})
        if request.method == "DELETE":
            return reply(request, 200, {})
        if request.path_url.startswith("/drive/v3/files"):
            return reply(request, 200, {"files": []})
        return reply(request, 200, {"drives": []})

    client, sent = client_with(handler, deployment_key)
    await client.list_shared(EMAIL)
    await client.delete_account(EMAIL)
    await client.ensure_account(ACCOUNT_ID)
    await client.list_shared(EMAIL)
    assert mints == 2
    files = [r for r in sent if r.path_url.startswith("/drive/v3/files")]
    assert files[-1].headers["Authorization"] == "Bearer tenant-token-2"


async def test_two_accounts_keep_separate_cached_tokens(deployment_key):
    """Fails if one account's list reuses another account's token or evicts it."""

    def handler(request):
        if ":generateAccessToken" in request.path_url:
            email = request.path_url.split("/serviceAccounts/")[1].split(":generateAccessToken")[0]
            return reply(request, 200, {"accessToken": f"token-{email}", "expireTime": EXPIRES})
        if request.path_url.startswith("/drive/v3/files"):
            return reply(request, 200, {"files": []})
        return reply(request, 200, {"drives": []})

    client, sent = client_with(handler, deployment_key)
    await client.list_shared(EMAIL)
    await client.list_shared(OTHER_EMAIL)
    await client.list_shared(EMAIL)
    mints = [r for r in sent if ":generateAccessToken" in r.path_url]
    assert len(mints) == 2
    files = [r for r in sent if r.path_url.startswith("/drive/v3/files")]
    assert [r.headers["Authorization"] for r in files] == [
        f"Bearer token-{EMAIL}",
        f"Bearer token-{OTHER_EMAIL}",
        f"Bearer token-{EMAIL}",
    ]


async def test_listing_stops_at_the_first_100_items_and_says_there_are_more(deployment_key):
    """Fails if a list with more than 100 items shared fetches a second page, returns
    more than the first 100 items, or does not say there are more."""

    def handler(request):
        if request.path_url == MINT_PATH:
            return reply(request, 200, {"accessToken": "tenant-token", "expireTime": EXPIRES})
        if request.path_url.startswith("/drive/v3/files"):
            return reply(
                request,
                200,
                {
                    "files": [{"id": f"f{i}", "name": f"File {i}"} for i in range(100)],
                    "nextPageToken": "p2",
                },
            )
        return reply(request, 200, {"drives": [{"id": "d1", "name": "Team drive"}]})

    client, sent = client_with(handler, deployment_key)
    listing = await client.list_shared(EMAIL)
    assert len(listing.items) == 100
    assert listing.more is True
    assert listing.items[0].id == "f0"
    assert listing.items[-1].id == "f99"
    pages = [r for r in sent if r.path_url.startswith("/drive/v3/")]
    assert len(pages) == 1


async def test_listing_pages_files_and_shared_drives_into_names_ids_and_links(deployment_key):
    """Fails if listing stops at the first page of files, misses the shared drives the
    account is a member of, drops the shared drive a file sits in, or returns anything but
    names, IDs, links and shared-drive IDs."""

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
                            "driveId": "d1",
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
                {"accessToken": "tenant-token", "expireTime": EXPIRES},
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    listing = await client.list_shared(EMAIL)
    assert [(i.id, i.name, i.link, i.kind, i.drive_id) for i in listing.items] == [
        ("f1", "Guide", "https://drive.google.com/file/d/f1", "file", "d1"),
        ("f2", "Roster", "https://drive.google.com/file/d/f2", "file", ""),
        ("d1", "Team drive", "https://drive.google.com/drive/folders/d1", "shared_drive", ""),
    ]
    assert listing.more is False
    files = [r for r in sent if r.path_url.startswith("/drive/v3/files")]
    assert len(files) == 2
    assert "sharedWithMe" in files[0].path_url
    assert "includeItemsFromAllDrives=true" in files[0].path_url
    assert "driveId" in files[0].path_url


# --- the Drive read surface (knowledge) ---------------------------------------------------


def _mint(request):
    if request.path_url == MINT_PATH:
        return reply(request, 200, {"accessToken": "tenant-token", "expireTime": EXPIRES})
    return None


async def test_list_children_pages_through_and_returns_metadata(deployment_key):
    """Fails if listing a folder's children stops at the first page, does not ask for the
    folder's non-trashed children, or drops the metadata the walk needs."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url.startswith("/drive/v3/files"):
            if "pageToken=p2" in request.path_url:
                return reply(
                    request,
                    200,
                    {
                        "files": [
                            {"id": "f2", "name": "Sheet", "mimeType": MIME_SHEET, "version": "3"}
                        ]
                    },
                )
            return reply(
                request,
                200,
                {
                    "files": [{"id": "f1", "name": "Doc", "mimeType": MIME_DOC, "version": "2"}],
                    "nextPageToken": "p2",
                },
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    children = await client.list_children(EMAIL, "folder-1")
    assert [(c.id, c.mime_type, c.version) for c in children] == [
        ("f1", MIME_DOC, "2"),
        ("f2", MIME_SHEET, "3"),
    ]
    lists = [r for r in sent if r.path_url.startswith("/drive/v3/files")]
    assert len(lists) == 2
    assert "folder-1" in lists[0].path_url and "trashed" in lists[0].path_url
    assert lists[0].headers["Authorization"] == "Bearer tenant-token"


@pytest.mark.parametrize(("status", "gone"), [(200, False), (404, True), (403, True)])
async def test_get_file_returns_metadata_or_maps_gone_and_refused(deployment_key, status, gone):
    """Fails if files.get does not return the metadata with its parents and the tenant's
    token, or a 404/403 is not a GoogleError carrying its status so the caller can classify
    the refusal."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if status == 200:
            return reply(
                request,
                200,
                {
                    "id": "f1",
                    "name": "Doc",
                    "mimeType": MIME_DOC,
                    "version": "7",
                    "webViewLink": "https://drive/f1",
                    "trashed": False,
                    "parents": ["folder-1"],
                },
            )
        return reply(request, status, {"error": {"status": "NOT_FOUND"}})

    client, sent = client_with(handler, deployment_key)
    if not gone:
        assert await client.get_file(EMAIL, "f1") == DriveFile(
            "f1",
            "Doc",
            MIME_DOC,
            version="7",
            web_view_link="https://drive/f1",
            parents=("folder-1",),
        )
    else:
        with pytest.raises(GoogleError) as err:
            await client.get_file(EMAIL, "f1")
        assert err.value.status == status
    gets = [r for r in sent if r.path_url.startswith("/drive/v3/files/f1")]
    assert gets[0].headers["Authorization"] == "Bearer tenant-token"
    assert "parents" in gets[0].path_url


@pytest.mark.parametrize(
    ("reason", "retryable", "gone"),
    [
        ("userRateLimitExceeded", True, False),
        ("rateLimitExceeded", True, False),
        ("quotaExceeded", True, False),
        ("SERVICE_DISABLED", False, False),
        ("accessNotConfigured", False, False),
        ("insufficientFilePermissions", False, True),
        ("forbidden", False, True),
        (None, False, True),
    ],
)
async def test_a_403_is_classified_by_its_reason(deployment_key, reason, retryable, gone):
    """Fails if a quota 403 reads as gone or final, a disabled-API 403 reads as gone, a
    sharing-loss 403 (or one with no reason) reads as retryable or kept, or a quota wait
    Google named is dropped."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        errors = [{"reason": reason}] if reason is not None else []
        response = reply(request, 403, {"error": {"errors": errors}})
        response.headers["Retry-After"] = "7"
        return response

    client, _sent = client_with(handler, deployment_key)
    with pytest.raises(GoogleError) as err:
        await client.get_file(EMAIL, "f1")
    assert err.value.status == 403
    assert drive_errors.entry_for(err.value).retryable is retryable
    assert drive_errors.is_gone(err.value) is gone
    delayed = drive_errors.retry_failure(err.value)
    if retryable:
        assert delayed is not None
        assert delayed.next_retry_delay == timedelta(seconds=7)
    else:
        assert delayed is None


async def test_the_shared_drive_surface_reads_drive_scoped_with_the_tenant_token(deployment_key):
    """Fails if a shared drive's root is not read with drives.get, its children are not
    listed with corpora=drive and the drive's ID, or either call carries another token."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url.startswith("/drive/v3/drives/d1"):
            return reply(request, 200, {"id": "d1", "name": "Team drive"})
        if request.path_url.startswith("/drive/v3/files"):
            return reply(
                request,
                200,
                {"files": [{"id": "f1", "name": "Doc", "mimeType": MIME_DOC, "version": "2"}]},
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    root = await client.get_drive(EMAIL, "d1")
    children = await client.list_drive_children(EMAIL, "d1")
    assert (root.id, root.name, root.mime_type) == ("d1", "Team drive", MIME_FOLDER)
    assert [(c.id, c.version) for c in children] == [("f1", "2")]
    gets = [r for r in sent if r.path_url.startswith("/drive/v3/drives/d1")]
    lists = [r for r in sent if r.path_url.startswith("/drive/v3/files")]
    assert len(gets) == 1 and len(lists) == 1
    assert gets[0].headers["Authorization"] == "Bearer tenant-token"
    assert lists[0].headers["Authorization"] == "Bearer tenant-token"
    assert "corpora=drive" in lists[0].path_url and "driveId=d1" in lists[0].path_url
    assert "includeItemsFromAllDrives=true" in lists[0].path_url
    assert "supportsAllDrives=true" in lists[0].path_url


async def test_resolving_a_shared_drive_file_never_gets_the_drive_root(deployment_key):
    """Fails if resolve_item sends files.get for a granted shared drive's root (not a
    file, 404 at Google) instead of matching the grant by ID."""
    file_id, drive_id = "shared-file-0001", "shared-drive-0001"

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url.startswith(f"/drive/v3/files/{file_id}"):
            return reply(
                request,
                200,
                {
                    "id": file_id,
                    "name": "Shared guide",
                    "mimeType": MIME_DOC,
                    "webViewLink": f"https://drive/{file_id}",
                    "trashed": False,
                    "parents": [drive_id],
                },
            )
        return reply(request, 404, {"error": {"status": "NOT_FOUND"}})

    client, sent = client_with(handler, deployment_key)
    conn = DriveConnector(
        DriveReader(client, EMAIL), [DriveGrant(drive_id, "Team drive", "shared_drive")]
    )

    item = await conn.resolve_item(file_id)

    assert (item.page_id, item.title) == (file_id, "Shared guide")
    gets = [r.path_url.split("?")[0] for r in sent if r.path_url.startswith("/drive/v3/files/")]
    assert gets == [f"/drive/v3/files/{file_id}"]


@pytest.mark.parametrize("mime", [MIME_DOC, MIME_SLIDES], ids=["doc", "slides"])
async def test_export_doc_and_slides_as_plain_text(deployment_key, mime):
    """Fails if a Doc or Slides export does not ask Drive for text/plain, or returns
    anything but the exported text."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url.startswith("/drive/v3/files/f1/export"):
            return reply(request, 200, "the exported text")
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    sections = await client.export_file(EMAIL, "f1", mime)
    assert sections == [DriveSection("", "the exported text")]
    exports = [r for r in sent if "/export" in r.path_url]
    assert len(exports) == 1 and "mimeType=text" in exports[0].path_url
    assert exports[0].headers["Authorization"] == "Bearer tenant-token"


async def test_export_sheet_reads_every_tab_as_csv_with_its_name(deployment_key):
    """Fails if a Sheet export uses Drive's first-tab-only CSV export, or drops a tab's
    name or rows."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url.startswith("/v4/spreadsheets/f1"):
            return reply(
                request,
                200,
                {
                    "sheets": [
                        {
                            "properties": {"title": "Q1"},
                            "data": [
                                {
                                    "rowData": [
                                        {
                                            "values": [
                                                {"formattedValue": "name"},
                                                {"formattedValue": "role"},
                                            ]
                                        },
                                        {
                                            "values": [
                                                {"formattedValue": "alice"},
                                                {"formattedValue": "eng"},
                                            ]
                                        },
                                    ]
                                }
                            ],
                        },
                        {
                            "properties": {"title": "Q2"},
                            "data": [{"rowData": [{"values": [{"formattedValue": "bob"}]}]}],
                        },
                    ]
                },
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    sections = await client.export_file(EMAIL, "f1", MIME_SHEET)
    assert [(s.heading, s.text) for s in sections] == [
        ("Q1", "name,role\nalice,eng\n"),
        ("Q2", "bob\n"),
    ]
    assert not any("/export" in r.path_url for r in sent)
    sheets = [r for r in sent if r.path_url.startswith("/v4/spreadsheets/f1")]
    assert sheets[0].headers["Authorization"] == "Bearer tenant-token"


# --- the comment write surface (D-47) ----------------------------------------------------


@pytest.mark.parametrize(
    "case",
    ["create", "list pages", "create 403", "create 404", "list 403"],
    ids=["create", "list-pages", "create-refused", "create-gone", "list-refused"],
)
async def test_comments_create_and_list_page_with_the_tenant_token_and_map_refusals(
    deployment_key, case
):
    """Fails if comments.create does not post the content with the tenant's token, the
    comment list stops at its first page or drops who wrote each comment, or a 403/404
    is not a GoogleError carrying its status so the error table reads it as gone."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url.startswith("/drive/v3/files/f1/comments"):
            if request.method == "POST":
                if case == "create 403":
                    return reply(
                        request,
                        403,
                        {"error": {"errors": [{"reason": "insufficientFilePermissions"}]}},
                    )
                if case == "create 404":
                    return reply(request, 404, {"error": {"status": "NOT_FOUND"}})
                return reply(request, 200, {"id": "c1"})
            if case == "list 403":
                return reply(request, 403, {"error": {"errors": [{"reason": "forbidden"}]}})
            if "pageToken=p2" in request.path_url:
                return reply(
                    request,
                    200,
                    {"comments": [{"id": "c2", "content": "second", "author": {"me": True}}]},
                )
            return reply(
                request,
                200,
                {
                    "comments": [{"id": "c1", "content": "first", "author": {"me": False}}],
                    "nextPageToken": "p2",
                },
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    if case == "create":
        assert await client.create_comment(EMAIL, "f1", "the text") == "c1"
        posts = [r for r in sent if r.method == "POST" and "/comments" in r.path_url]
        assert len(posts) == 1
        assert json.loads(posts[0].body) == {"content": "the text"}
        assert posts[0].headers["Authorization"] == "Bearer tenant-token"
        assert "supportsAllDrives=true" in posts[0].path_url
    elif case == "list pages":
        comments = await client.list_comments(EMAIL, "f1")
        assert [(c.id, c.content, c.mine) for c in comments] == [
            ("c1", "first", False),
            ("c2", "second", True),
        ]
        lists = [r for r in sent if r.method == "GET" and "/comments" in r.path_url]
        assert len(lists) == 2
        assert lists[0].headers["Authorization"] == "Bearer tenant-token"
    else:
        with pytest.raises(GoogleError) as err:
            if case.startswith("create"):
                await client.create_comment(EMAIL, "f1", "the text")
            else:
                await client.list_comments(EMAIL, "f1")
        assert err.value.status == int(case.split()[-1])
        assert drive_errors.is_gone(err.value)


# --- the Sheet append write surface (D-48) -----------------------------------------------


async def test_append_sends_one_atomic_batchupdate_with_the_row_and_its_metadata(deployment_key):
    """Fails if the append is not one batchUpdate carrying appendCells and
    createDeveloperMetadata on the appended row, with the tenant's token, or the created
    metadata's ID is not returned."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url == "/v4/spreadsheets/f1:batchUpdate":
            return reply(
                request,
                200,
                {
                    "replies": [
                        {},
                        {"createDeveloperMetadata": {"developerMetadata": {"metadataId": 7}}},
                    ]
                },
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    made = await client.append_sheet_row(
        EMAIL, "f1", 42, ["bob", "people"], 2, "agentos-write", "hash"
    )

    assert made == "7"
    posts = [
        r for r in sent if r.method == "POST" and r.path_url == "/v4/spreadsheets/f1:batchUpdate"
    ]
    assert len(posts) == 1
    assert posts[0].headers["Authorization"] == "Bearer tenant-token"
    requests = json.loads(posts[0].body)["requests"]
    assert requests[0] == {
        "appendCells": {
            "sheetId": 42,
            "rows": [
                {
                    "values": [
                        {"userEnteredValue": {"stringValue": "bob"}},
                        {"userEnteredValue": {"stringValue": "people"}},
                    ]
                }
            ],
            "fields": "userEnteredValue",
        }
    }
    assert requests[1] == {
        "createDeveloperMetadata": {
            "developerMetadata": {
                "metadataKey": "agentos-write",
                "metadataValue": "hash",
                "visibility": "DOCUMENT",
                "location": {
                    "dimensionRange": {
                        "sheetId": 42,
                        "dimension": "ROWS",
                        "startIndex": 2,
                        "endIndex": 3,
                    }
                },
            }
        }
    }


@pytest.mark.parametrize("found", [True, False], ids=["found", "not-found"])
async def test_metadata_search_posts_the_key_and_value_and_maps_matches(deployment_key, found):
    """Fails if the marker lookup does not search developer metadata by the key and value
    with the tenant's token, or returns anything but the matching entries (none when absent)."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url == "/v4/spreadsheets/f1/developerMetadata:search":
            if not found:
                return reply(request, 200, {"matchedDeveloperMetadata": []})
            return reply(
                request,
                200,
                {
                    "matchedDeveloperMetadata": [
                        {
                            "developerMetadata": {
                                "metadataId": 7,
                                "metadataKey": "agentos-write",
                                "metadataValue": "hash",
                                "location": {"dimensionRange": {"startIndex": 2}},
                            }
                        }
                    ]
                },
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    matches = await client.search_sheet_metadata(EMAIL, "f1", "agentos-write", "hash")

    assert matches == ([SheetMetadata(7, "agentos-write", "hash", 2)] if found else [])
    posts = [r for r in sent if r.path_url == "/v4/spreadsheets/f1/developerMetadata:search"]
    assert len(posts) == 1
    assert posts[0].headers["Authorization"] == "Bearer tenant-token"
    assert json.loads(posts[0].body) == {
        "dataFilters": [
            {"developerMetadataLookup": {"metadataKey": "agentos-write", "metadataValue": "hash"}}
        ]
    }


@pytest.mark.parametrize(
    "case", ["tab", "header only", "missing tab"], ids=["tab", "header-only", "missing-tab"]
)
async def test_read_sheet_returns_the_tabs_id_header_and_data_row_count(deployment_key, case):
    """Fails if the header read asks for every tab instead of the granted one (ranges), asks
    for more than row 1 on a header check, drops the tab's ID, header or data-row count, or a
    missing tab reads as anything but a refusal."""
    header_row = {"values": [{"formattedValue": "name"}, {"formattedValue": "role"}]}
    data_row = {"values": [{"formattedValue": "alice"}, {"formattedValue": "eng"}]}

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url.startswith("/v4/spreadsheets/f1"):
            query = parse_qs(urlsplit(request.path_url).query)
            # Google answers a ranges read with the asked rows alone.
            rows = [header_row] if query.get("ranges") == ["'Q1'!1:1"] else [header_row, data_row]
            return reply(
                request,
                200,
                {
                    "sheets": [
                        {
                            "properties": {"sheetId": 42, "title": "Q1"},
                            "data": [{"rowData": rows}],
                        }
                    ]
                },
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    if case == "tab":
        assert await client.read_sheet(EMAIL, "f1", "Q1") == SheetTab(42, ("name", "role"), 2)
    elif case == "header only":
        assert await client.read_sheet(EMAIL, "f1", "Q1", header_only=True) == SheetTab(
            42, ("name", "role"), 1
        )
    else:
        with pytest.raises(GoogleError) as err:
            await client.read_sheet(EMAIL, "f1", "Q9")
        assert err.value.status == 400
    gets = [r for r in sent if r.path_url.startswith("/v4/spreadsheets/f1")]
    assert gets[0].headers["Authorization"] == "Bearer tenant-token"
    assert "sheetId" in gets[0].path_url and "rowData" in gets[0].path_url
    query = parse_qs(urlsplit(gets[0].path_url).query)
    if case == "tab":
        assert query["ranges"] == ["'Q1'"]
    elif case == "header only":
        assert query["ranges"] == ["'Q1'!1:1"]
    else:
        assert query["ranges"] == ["'Q9'"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("=1+1", {"stringValue": "=1+1"}),
        (
            '=HYPERLINK("http://example.com","click")',
            {"stringValue": '=HYPERLINK("http://example.com","click")'},
        ),
        ("007", {"stringValue": "007"}),
        ("TRUE", {"stringValue": "TRUE"}),
        (42, {"numberValue": 42}),
    ],
    ids=["formula-text", "hyperlink-text", "leading-zero", "bool-text", "number"],
)
async def test_append_cells_send_text_literally_and_a_number_as_a_number(
    deployment_key, value, expected
):
    """Fails if a text cell is sent as anything but stringValue (a formula, a number or a
    boolean), or a JSON number as anything but numberValue: the cell must land as approved."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url == "/v4/spreadsheets/f1:batchUpdate":
            return reply(
                request,
                200,
                {
                    "replies": [
                        {},
                        {"createDeveloperMetadata": {"developerMetadata": {"metadataId": 7}}},
                    ]
                },
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    await client.append_sheet_row(EMAIL, "f1", 42, [value], 2, "agentos-write", "hash")

    [post] = [
        r for r in sent if r.method == "POST" and r.path_url == "/v4/spreadsheets/f1:batchUpdate"
    ]
    requests = json.loads(post.body)["requests"]
    assert requests[0]["appendCells"]["rows"][0]["values"] == [{"userEnteredValue": expected}]


async def test_list_sheet_tabs_reads_only_the_titles_with_the_tenant_token(deployment_key):
    """Fails if the picker's tab read asks for the tabs' content, uses the deployment's
    token instead of the tenant's, or drops a tab title."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url.startswith("/v4/spreadsheets/f1"):
            return reply(
                request,
                200,
                {"sheets": [{"properties": {"title": "Q1"}}, {"properties": {"title": "Q2"}}]},
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    assert await client.list_sheet_tabs(EMAIL, "f1") == ["Q1", "Q2"]

    [get] = [r for r in sent if r.path_url.startswith("/v4/spreadsheets/f1")]
    assert get.headers["Authorization"] == "Bearer tenant-token"
    query = parse_qs(urlsplit(get.path_url).query)
    assert query["fields"] == ["sheets.properties.title"]
    assert "rowData" not in get.path_url


# --- the create-from-template write surface (D-49) ---------------------------------------


async def test_copy_sends_the_title_folder_and_marker_appproperty_in_one_request(deployment_key):
    """Fails if the copy does not POST the title, the destination folder and the marker
    appProperty in one files.copy with supportsAllDrives and the tenant's token, or does not
    return the new Doc's metadata."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url.startswith("/drive/v3/files/tpl1/copy"):
            return reply(
                request,
                200,
                {
                    "id": "new1",
                    "name": "Offer for Bob",
                    "mimeType": MIME_DOC,
                    "webViewLink": "https://drive/new1",
                },
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    made = await client.copy_file(
        EMAIL, "tpl1", "folder1", "Offer for Bob", "agentos_write", "hash"
    )

    assert (made.id, made.name, made.web_view_link) == (
        "new1",
        "Offer for Bob",
        "https://drive/new1",
    )
    [post] = [
        r for r in sent if r.method == "POST" and r.path_url.startswith("/drive/v3/files/tpl1/copy")
    ]
    assert post.headers["Authorization"] == "Bearer tenant-token"
    assert "supportsAllDrives=true" in post.path_url
    assert json.loads(post.body) == {
        "name": "Offer for Bob",
        "parents": ["folder1"],
        "appProperties": {"agentos_write": "hash"},
    }


@pytest.mark.parametrize("found", [True, False], ids=["found", "not-found"])
async def test_app_property_search_lists_the_folder_drive_scoped(deployment_key, found):
    """Fails if the marker lookup does not search the destination folder for the appProperty
    key and value, drive-scoped with supportsAllDrives and includeItemsFromAllDrives and the
    tenant's token, or returns anything but the matching files (none when absent)."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url.startswith("/drive/v3/files"):
            if not found:
                return reply(request, 200, {"files": []})
            return reply(
                request,
                200,
                {
                    "files": [
                        {
                            "id": "new1",
                            "name": "Offer for Bob",
                            "mimeType": MIME_DOC,
                            "webViewLink": "https://drive/new1",
                        }
                    ]
                },
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    matches = await client.search_app_properties(
        EMAIL, "folder1", "drive1", "agentos_write", "hash"
    )

    assert [m.id for m in matches] == (["new1"] if found else [])
    [get] = [r for r in sent if r.path_url.startswith("/drive/v3/files")]
    assert get.headers["Authorization"] == "Bearer tenant-token"
    query = parse_qs(urlsplit(get.path_url).query)
    assert query["corpora"] == ["drive"]
    assert query["driveId"] == ["drive1"]
    assert query["supportsAllDrives"] == ["true"]
    assert query["includeItemsFromAllDrives"] == ["true"]
    assert "'folder1' in parents" in query["q"][0]
    assert "appProperties has { key='agentos_write' and value='hash' }" in query["q"][0]


async def test_fill_sends_one_batchupdate_of_ordered_literal_replacements(deployment_key):
    """Fails if the fill is not one Docs batchUpdate applying the ordered replacement pairs
    case-sensitively and literally, in order, with the tenant's token."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url == "/v1/documents/new1:batchUpdate":
            return reply(request, 200, {"replies": [{}, {}, {}, {}]})
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    await client.fill_doc(
        EMAIL,
        "new1",
        [
            ("{{name}}", "sentinel-name"),
            ("{{ role }}", "sentinel-role"),
            ("sentinel-name", "Bob"),
            ("sentinel-role", "engineer"),
        ],
    )

    [post] = [
        r for r in sent if r.method == "POST" and r.path_url == "/v1/documents/new1:batchUpdate"
    ]
    assert post.headers["Authorization"] == "Bearer tenant-token"
    assert json.loads(post.body) == {
        "requests": [
            {
                "replaceAllText": {
                    "containsText": {"text": "{{name}}", "matchCase": True},
                    "replaceText": "sentinel-name",
                }
            },
            {
                "replaceAllText": {
                    "containsText": {"text": "{{ role }}", "matchCase": True},
                    "replaceText": "sentinel-role",
                }
            },
            {
                "replaceAllText": {
                    "containsText": {"text": "sentinel-name", "matchCase": True},
                    "replaceText": "Bob",
                }
            },
            {
                "replaceAllText": {
                    "containsText": {"text": "sentinel-role", "matchCase": True},
                    "replaceText": "engineer",
                }
            },
        ]
    }


async def test_read_doc_returns_the_revision_title_and_paragraphs(deployment_key):
    """Fails if documents.get does not return the revisionId, title and body paragraphs (each
    with its text, named style and UTF-16 indices) with the tenant's token, asking for the
    revisionId (D-50)."""

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url.startswith("/v1/documents/f1"):
            return reply(
                request,
                200,
                {
                    "documentId": "f1",
                    "title": "Guide",
                    "revisionId": "rev-1",
                    "body": {
                        "content": [
                            {
                                "startIndex": 0,
                                "endIndex": 7,
                                "paragraph": {
                                    "elements": [{"textRun": {"content": "Status\n"}}],
                                    "paragraphStyle": {"namedStyleType": "HEADING_1"},
                                },
                            },
                            {
                                "startIndex": 7,
                                "endIndex": 16,
                                "paragraph": {
                                    "elements": [{"textRun": {"content": "old line\n"}}],
                                    "paragraphStyle": {"namedStyleType": "NORMAL_TEXT"},
                                },
                            },
                        ]
                    },
                },
            )
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    doc = await client.read_doc(EMAIL, "f1")

    assert (doc.revision_id, doc.title) == ("rev-1", "Guide")
    assert [(p.text, p.style, p.start_index, p.end_index) for p in doc.paragraphs] == [
        ("Status\n", "HEADING_1", 0, 7),
        ("old line\n", "NORMAL_TEXT", 7, 16),
    ]
    [get] = [r for r in sent if r.method == "GET" and r.path_url.startswith("/v1/documents/f1")]
    assert get.headers["Authorization"] == "Bearer tenant-token"
    assert "revisionId" in get.path_url


@pytest.mark.parametrize(
    "case",
    ["writes", "insert-only", "normalize", "status mismatch", "reason-only mismatch"],
    ids=["writes", "insert-only", "normalize", "status-mismatch", "reason-only"],
)
async def test_replace_sends_one_batchupdate_locked_to_the_read_revision(deployment_key, case):
    """Fails if the section replace is not one Docs batchUpdate carrying writeControl with the
    read revision and the tenant's token, inserting the new text first then deleting the
    shifted old range (insert-only when nothing is deleted, NORMAL_TEXT style and bullets
    cleared over the inserted range for an empty section), or a 400 failedPrecondition — by
    status or by reason alone — is not a revision_mismatch GoogleError (D-50)."""
    # insert_index, insert_text, delete_start, delete_end, style_start, style_end
    plans = {
        "writes": (7, "new line\n", 16, 25, 0, 0),
        "insert-only": (7, "new line\n", 7, 7, 0, 0),
        "normalize": (7, "new line\n", 0, 0, 7, 16),
        "status mismatch": (7, "new line\n", 16, 25, 0, 0),
        "reason-only mismatch": (7, "new line\n", 16, 25, 0, 0),
    }
    expected_requests = {
        "writes": [
            {"insertText": {"location": {"index": 7}, "text": "new line\n"}},
            {"deleteContentRange": {"range": {"startIndex": 16, "endIndex": 25}}},
        ],
        "insert-only": [{"insertText": {"location": {"index": 7}, "text": "new line\n"}}],
        "normalize": [
            {"insertText": {"location": {"index": 7}, "text": "new line\n"}},
            {
                "updateParagraphStyle": {
                    "range": {"startIndex": 7, "endIndex": 16},
                    "paragraphStyle": {"namedStyleType": "NORMAL_TEXT"},
                    "fields": "namedStyleType",
                }
            },
            {"deleteParagraphBullets": {"range": {"startIndex": 7, "endIndex": 16}}},
        ],
    }
    mismatch = case in ("status mismatch", "reason-only mismatch")

    def handler(request):
        minted = _mint(request)
        if minted is not None:
            return minted
        if request.path_url == "/v1/documents/f1:batchUpdate":
            if case == "status mismatch":
                return reply(
                    request,
                    400,
                    {
                        "error": {
                            "status": "FAILED_PRECONDITION",
                            "errors": [{"reason": "failedPrecondition"}],
                        }
                    },
                )
            if case == "reason-only mismatch":
                return reply(
                    request, 400, {"error": {"errors": [{"reason": "failedPrecondition"}]}}
                )
            return reply(request, 200, {"documentId": "f1", "replies": [{}, {}]})
        return reply(request, 404, {})

    client, sent = client_with(handler, deployment_key)
    if mismatch:
        with pytest.raises(GoogleError) as err:
            await client.replace_doc_section(EMAIL, "f1", "rev-1", *plans[case])
        assert err.value.code == "revision_mismatch"
    else:
        assert await client.replace_doc_section(EMAIL, "f1", "rev-1", *plans[case]) == "f1"

    [post] = [
        r for r in sent if r.method == "POST" and r.path_url == "/v1/documents/f1:batchUpdate"
    ]
    assert post.headers["Authorization"] == "Bearer tenant-token"
    body = json.loads(post.body)
    assert body["writeControl"] == {"requiredRevisionId": "rev-1"}
    if not mismatch:
        assert body["requests"] == expected_requests[case]
