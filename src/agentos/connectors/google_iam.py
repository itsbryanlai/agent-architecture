"""The deployment's Google service account and the tenant accounts it manages (D-45).

One deployment service account — its JSON key is a deployment secret in the settings,
never stored per tenant — creates each tenant's own service account in the project
(``projects.serviceAccounts.create``; a 409 finds the account a retried connect already
made), deletes it on disconnect or uninstall, mints short-lived access tokens for it
through the IAM Credentials API (``generateAccessToken``), and lists what it can see
(``files.list`` with ``sharedWithMe`` plus the shared drives it is a member of; names,
IDs and links only, never content). Tenant isolation is by identity: whatever the
tenant's account can see is the tenant's.

The real client speaks through google-auth's ``AuthorizedSession``: the deployment
credentials refresh themselves through the same session, so one mocked transport covers
the whole flow in a test. The session is synchronous, so every call runs in a worker
thread, and one lock keeps two calls from sharing it at once. ``session`` and ``clock``
let a test supply a mocked transport and a fixed time.

The fake keeps the accounts and what is shared with each, and carries the failure
injections in one place: ``fail_after_write`` (the response is lost after the effect),
``hang_after_write`` (a second attempt overlaps the first), ``fail_next_list`` and
``fail_next_delete``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

import requests
from google.auth.exceptions import RefreshError, TransportError
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

from agentos.connectors.notion import Hang
from agentos.usage import utcnow

IAM = "https://iam.googleapis.com/v1"
IAM_CREDENTIALS = "https://iamcredentials.googleapis.com/v1"
DRIVE = "https://www.googleapis.com/drive/v3"
CLOUD_PLATFORM = "https://www.googleapis.com/auth/cloud-platform"
# D-2 only lists what the account can see. The write tickets add their scopes here.
DRIVE_SCOPES = ("https://www.googleapis.com/auth/drive.readonly",)
TOKEN_LIFETIME = "3600s"
# A cached token is reused until this close to its expiry, then minted again.
TOKEN_MARGIN = timedelta(seconds=60)
REQUEST_TIMEOUT = 30
DISPLAY_NAME = "AgentOS agent account"

# What a failed Google call means in plain words, for the settings page and the
# console's error page. The store keeps the code; the words live here once.
PROBLEM_TEXT = {
    "invalid_key": "The deployment's Google service-account key is not accepted "
    "(GOOGLE_SERVICE_ACCOUNT_KEY).",
    "permission_denied": "Google refused the request: the deployment service account is "
    "missing a role on the project.",
    "not_found": "Google could not find the account or the project.",
    "rate_limited": "Google is rate-limiting the requests. Try again in a moment.",
    "unavailable": "Google could not be reached. Try again.",
}


def problem_text(code: str | None) -> str:
    """The plain words for a stored error code; an unknown code still reads as a problem."""
    return PROBLEM_TEXT.get(code or "", "Google Drive reported a problem.")


class GoogleError(Exception):
    """A Google call failed. ``code`` is the short class the row stores and the page
    words; the message is the plain words, never Google's body, a key or a token."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(problem_text(code))


@dataclass(frozen=True)
class DriveItem:
    """One file or shared drive the tenant's account can see: its ID, name and link."""

    id: str
    name: str
    link: str


class GoogleAccounts(Protocol):
    """The deployment's hold on the tenant accounts: create-or-find, delete, and list."""

    async def ensure_account(self, account_id: str) -> str:
        """Create the account, or find the one a retried connect already made (409).
        Returns its email."""
        ...

    async def delete_account(self, account_id: str) -> bool:
        """Delete the account. False when it is already gone at Google (404)."""
        ...

    async def list_shared(self, email: str) -> list[DriveItem]:
        """The files shared with the account and the shared drives it is a member of."""
        ...


def _code_for(status: int) -> str:
    if status in (401, 403):
        return "permission_denied"
    if status == 404:
        return "not_found"
    if status == 429:
        return "rate_limited"
    return "unavailable"


class RealGoogleAccounts:
    def __init__(
        self,
        project_id: str,
        service_account_key: str,
        session: AuthorizedSession | None = None,
        drive_session: requests.Session | None = None,
        clock: Callable[[], datetime] = utcnow,
    ):
        try:
            info = json.loads(service_account_key)
            credentials = service_account.Credentials.from_service_account_info(
                info, scopes=[CLOUD_PLATFORM]
            )
        except (ValueError, KeyError, TypeError) as err:
            raise GoogleError("invalid_key") from err
        self._project = project_id
        self._session = AuthorizedSession(credentials) if session is None else session
        # Drive calls carry the TENANT account's minted token, so they run on a plain
        # session: an AuthorizedSession would stamp the deployment token over it and
        # read as the deployment account, not the tenant's.
        self._drive_session = requests.Session() if drive_session is None else drive_session
        self._clock = clock
        self._tokens: dict[str, tuple[str, datetime]] = {}
        self._lock = asyncio.Lock()

    def account_email(self, account_id: str) -> str:
        return f"{account_id}@{self._project}.iam.gserviceaccount.com"

    def _send(
        self, session: requests.Session, method: str, url: str, **kwargs
    ) -> requests.Response:
        try:
            return session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
        except RefreshError as err:
            raise GoogleError("invalid_key") from err
        except (TransportError, requests.RequestException) as err:
            raise GoogleError("unavailable") from err

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        return self._send(self._session, method, url, **kwargs)

    def _check(self, response: requests.Response) -> None:
        if response.status_code >= 400:
            raise GoogleError(_code_for(response.status_code))

    async def ensure_account(self, account_id: str) -> str:
        email = self.account_email(account_id)

        def call() -> str:
            response = self._request(
                "POST",
                f"{IAM}/projects/{self._project}/serviceAccounts",
                json={"accountId": account_id, "serviceAccount": {"displayName": DISPLAY_NAME}},
            )
            if response.status_code == 409:
                response = self._request(
                    "GET", f"{IAM}/projects/{self._project}/serviceAccounts/{email}"
                )
            self._check(response)
            return response.json()["email"]

        async with self._lock:
            return await asyncio.to_thread(call)

    async def delete_account(self, account_id: str) -> bool:
        email = self.account_email(account_id)

        def call() -> bool:
            response = self._request(
                "DELETE", f"{IAM}/projects/{self._project}/serviceAccounts/{email}"
            )
            if response.status_code == 404:
                return False
            self._check(response)
            return True

        async with self._lock:
            return await asyncio.to_thread(call)

    def _token_for(self, email: str) -> str:
        now = self._clock()
        cached = self._tokens.get(email)
        if cached is not None and cached[1] - TOKEN_MARGIN > now:
            return cached[0]
        response = self._request(
            "POST",
            f"{IAM_CREDENTIALS}/projects/-/serviceAccounts/{email}:generateAccessToken",
            json={"scope": list(DRIVE_SCOPES), "lifetime": TOKEN_LIFETIME},
        )
        self._check(response)
        body = response.json()
        token = body["accessToken"]
        self._tokens[email] = (token, datetime.fromisoformat(body["expireTime"]))
        return token

    async def list_shared(self, email: str) -> list[DriveItem]:
        def call() -> list[DriveItem]:
            headers = {"Authorization": f"Bearer {self._token_for(email)}"}
            items: list[DriveItem] = []
            page_token = None
            while True:
                params = {
                    "q": "sharedWithMe and trashed=false",
                    "fields": "nextPageToken,files(id,name,webViewLink)",
                    "includeItemsFromAllDrives": "true",
                    "supportsAllDrives": "true",
                    "pageSize": "100",
                }
                if page_token:
                    params["pageToken"] = page_token
                response = self._send(
                    self._drive_session, "GET", f"{DRIVE}/files", headers=headers, params=params
                )
                self._check(response)
                body = response.json()
                for file in body.get("files", []):
                    link = (
                        file.get("webViewLink") or f"https://drive.google.com/file/d/{file['id']}"
                    )
                    items.append(DriveItem(file["id"], file["name"], link))
                page_token = body.get("nextPageToken")
                if not page_token:
                    break
            page_token = None
            while True:
                params = {"fields": "nextPageToken,drives(id,name)", "pageSize": "100"}
                if page_token:
                    params["pageToken"] = page_token
                response = self._send(
                    self._drive_session, "GET", f"{DRIVE}/drives", headers=headers, params=params
                )
                self._check(response)
                body = response.json()
                for drive in body.get("drives", []):
                    items.append(
                        DriveItem(
                            drive["id"],
                            drive["name"],
                            f"https://drive.google.com/drive/folders/{drive['id']}",
                        )
                    )
                page_token = body.get("nextPageToken")
                if not page_token:
                    break
            return items

        async with self._lock:
            return await asyncio.to_thread(call)


class FakeGoogleAccounts:
    """The same surface without Google: the accounts by ID, what is shared with each
    email, and the failure injections in one place."""

    def __init__(self, project_id: str = "test-project"):
        self.project_id = project_id
        self.accounts: dict[str, str] = {}
        self.shared: dict[str, list[DriveItem]] = {}
        self.created: list[str] = []
        self.create_calls: list[str] = []
        self.delete_calls: list[str] = []
        self._fail_after_write = {"create": 0, "delete": 0}
        self._hang_after_write: dict[str, list[Hang]] = {"create": [], "delete": []}
        self._list_error: Exception | None = None
        self._delete_error: Exception | None = None

    def fail_after_write(self, kind: str = "create", times: int = 1) -> None:
        """The next ``times`` writes of ``kind`` ("create" or "delete") take effect and
        then raise, as when the response is lost."""
        self._fail_after_write[kind] = times

    def hang_after_write(self, kind: str = "create") -> Hang:
        """The next write of ``kind`` takes effect, then waits until the returned ``Hang``
        is released. Another attempt meanwhile runs normally: this is how two overlap."""
        hang = Hang()
        self._hang_after_write[kind].append(hang)
        return hang

    def fail_next_list(self, error: Exception) -> None:
        """Make the next list_shared raise ``error``."""
        self._list_error = error

    def fail_next_delete(self, error: Exception) -> None:
        """Make the next delete_account raise ``error`` before it changes anything."""
        self._delete_error = error

    def account_email(self, account_id: str) -> str:
        return f"{account_id}@{self.project_id}.iam.gserviceaccount.com"

    async def ensure_account(self, account_id: str) -> str:
        self.create_calls.append(account_id)
        if account_id in self.accounts:
            return self.accounts[account_id]
        email = self.account_email(account_id)
        self.accounts[account_id] = email
        self.created.append(account_id)
        await self._after_write("create")
        return email

    async def delete_account(self, account_id: str) -> bool:
        self.delete_calls.append(account_id)
        if self._delete_error is not None:
            error, self._delete_error = self._delete_error, None
            raise error
        if account_id not in self.accounts:
            return False
        del self.accounts[account_id]
        await self._after_write("delete")
        return True

    async def _after_write(self, kind: str) -> None:
        if self._fail_after_write[kind]:
            self._fail_after_write[kind] -= 1
            raise GoogleError("unavailable")
        if self._hang_after_write[kind]:
            await self._hang_after_write[kind].pop(0).wait()

    async def list_shared(self, email: str) -> list[DriveItem]:
        if self._list_error is not None:
            error, self._list_error = self._list_error, None
            raise error
        return list(self.shared.get(email, []))


def accounts_from_settings(
    project_id: str | None, service_account_key: str | None
) -> RealGoogleAccounts | None:
    """The deployment's Google client, or None when either setting is unset: Drive is
    then not set up on this deployment. A malformed key raises ``GoogleError`` here,
    at start-up, never at the first click."""
    if not project_id or not service_account_key:
        return None
    return RealGoogleAccounts(project_id, service_account_key)
