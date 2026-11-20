"""The deployment's Google service account and the tenant accounts it manages (D-45).

One deployment service account — its JSON key is a deployment secret in the settings,
never stored per tenant — creates each tenant's own service account in the project
(``projects.serviceAccounts.create``; a 409 finds the account a retried connect already
made, and a 409 whose get answers 404 is a delete still propagating), deletes it by its
stored email on disconnect or uninstall, mints short-lived access tokens for it through
the IAM Credentials API (``generateAccessToken``), and lists the first 100 items it can
see (``files.list`` with ``sharedWithMe`` plus the shared drives it is a member of; names,
IDs and links only, never content). Tenant isolation is by identity: whatever the
tenant's account can see is the tenant's.

The real client speaks through google-auth's ``AuthorizedSession``: the deployment
credentials refresh themselves through the same session, so one mocked transport covers
the whole flow in a test. The session is synchronous, so every call runs in a worker
thread. One lock keeps the IAM calls (create, delete, token mint) from sharing the
deployment session at once; Drive lists page on a session of their own per call, so one
tenant's list never waits on another's. ``session`` and ``clock`` let a test supply a
mocked transport and a fixed time.

The fake keeps the accounts and what is shared with each, and carries the failure
injections in one place: ``fail_after_write`` (the response is lost after the effect),
``hang_after_write`` (a second attempt overlaps the first), ``fail_next_list`` and
``fail_next_delete``.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
from collections.abc import Callable
from dataclasses import dataclass, replace
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
SHEETS = "https://sheets.googleapis.com/v4"
CLOUD_PLATFORM = "https://www.googleapis.com/auth/cloud-platform"

# Google's native mime types. Only Docs, Sheets and Slides export to text; a folder is
# walked, and anything else is skipped and counted by the indexer.
MIME_FOLDER = "application/vnd.google-apps.folder"
MIME_DOC = "application/vnd.google-apps.document"
MIME_SHEET = "application/vnd.google-apps.spreadsheet"
MIME_SLIDES = "application/vnd.google-apps.presentation"
EXPORTABLE = (MIME_DOC, MIME_SHEET, MIME_SLIDES)
# Connect only lists what the account can see. A later write adds its scopes here.
DRIVE_SCOPES = ("https://www.googleapis.com/auth/drive.readonly",)
TOKEN_LIFETIME = "3600s"
# A cached token is reused until this close to its expiry, then minted again.
TOKEN_MARGIN = timedelta(seconds=60)
REQUEST_TIMEOUT = 30
# Create and delete run under the caller's unit-of-work lock, holding a pool connection:
# they get a short timeout of their own.
IAM_TIMEOUT = 10
# The settings page lists at most this many items, files and shared drives together.
LIST_CAP = 100
DISPLAY_NAME = "AgentOS agent account"

# What a failed Google call means in plain words, for the settings page and the
# console's error page. The store keeps the code; the words live here once.
PROBLEM_TEXT = {
    "invalid_key": "The deployment's Google service-account key is not accepted "
    "(GOOGLE_SERVICE_ACCOUNT_KEY).",
    "permission_denied": "Google refused the request: the deployment service account is "
    "missing a role on the project.",
    "refused": "Google refused the request.",
    "not_found": "Google could not find the account or the project.",
    "rate_limited": "Google is rate-limiting the requests. Try again in a moment.",
    "unavailable": "Google could not be reached. Try again.",
    "recently_deleted": "Google is still removing this workspace's previous Drive account; "
    "try Connect again in a few minutes.",
}
# A Drive refusal is the tenant account's access, not the deployment account's role.
DRIVE_REFUSED_TEXT = (
    "Google refused this account's access: sharing with its address was removed, "
    "or the Drive API is not enabled on the project."
)


def problem_text(code: str | None) -> str:
    """The plain words for a stored error code; an unknown code still reads as a problem."""
    return PROBLEM_TEXT.get(code or "", "Google Drive reported a problem.")


class GoogleError(Exception):
    """A Google call failed. ``code`` is the short class the row stores and the page
    words; the message is the plain words, never Google's body, a key or a token.
    ``words`` overrides the code's default text where the call class words it
    differently (a Drive refusal names the tenant account's access).

    ``status`` and ``retry_after`` are set only by the Drive read surface, so the
    connector error table (``drive_errors``) can classify a read for retry; the IAM
    calls leave them None and are handled by their own callers.
    """

    def __init__(
        self,
        code: str,
        words: str | None = None,
        status: int | None = None,
        retry_after: float | None = None,
    ):
        self.code = code
        self.status = status
        self.retry_after = retry_after
        super().__init__(words if words is not None else problem_text(code))


@dataclass(frozen=True)
class DriveItem:
    """One file or shared drive the tenant's account can see: its ID, name and link.

    ``kind`` is ``folder`` or ``file``, so the grant picker knows which grant to add.
    """

    id: str
    name: str
    link: str
    kind: str = "file"


@dataclass(frozen=True)
class DriveListing:
    """The first ``LIST_CAP`` items the account can see, and whether more are shared."""

    items: list[DriveItem]
    more: bool


@dataclass(frozen=True)
class DriveFile:
    """One Drive file or folder's metadata: references and small values, never content.

    ``version`` is Drive's monotonically increasing revision number, as a string; the
    indexer re-exports a file only when it changes. ``mime_type`` decides whether the
    node is a folder (walked), an exportable Doc/Sheet/Slides, or a skipped type.
    """

    id: str
    name: str
    mime_type: str
    version: str = ""
    web_view_link: str = ""
    trashed: bool = False


@dataclass(frozen=True)
class DriveSection:
    """One headed stretch of an exported file's text.

    A Doc or Slides export is one section with no heading; a Sheet gives one section per
    tab, its name the heading and its rows the text (CSV). The indexer turns these into
    the same heading/text blocks a Notion page gives, so chunking is shared.
    """

    heading: str
    text: str


class GoogleAccounts(Protocol):
    """The deployment's hold on the tenant accounts: create-or-find, delete, and list."""

    def account_email(self, account_id: str) -> str:
        """The address Google gives the account: its ID at the project's domain."""
        ...

    async def ensure_account(self, account_id: str) -> str:
        """Create the account, or find the one a retried connect already made (409).
        Returns its email."""
        ...

    async def delete_account(self, email: str) -> bool:
        """Delete the account by its email — the row's stored email, so a project move
        still deletes the old account. False when it is already gone at Google (404)."""
        ...

    async def list_shared(self, email: str) -> DriveListing:
        """The first 100 files shared with the account and shared drives it is a member
        of, and whether more are shared."""
        ...

    async def list_children(self, email: str, folder_id: str) -> list[DriveFile]:
        """Every non-trashed child of a folder (files and sub-folders), paging through
        Drive's list. Metadata only, never content."""
        ...

    async def get_file(self, email: str, file_id: str) -> DriveFile:
        """One file or folder's metadata. A 404 or 403 raises ``GoogleError`` with its
        status, so the caller can drop a gone or refused file."""
        ...

    async def export_file(self, email: str, file_id: str, mime_type: str) -> list[DriveSection]:
        """A Doc, Sheet or Slides as text sections. Docs and Slides export to plain text;
        a Sheet gives one CSV section per tab, its name the heading."""
        ...


def _code_for(status: int) -> str:
    if status == 400:
        return "refused"
    if status in (401, 403):
        return "permission_denied"
    if status == 404:
        return "not_found"
    if status == 429:
        return "rate_limited"
    return "unavailable"


# Longest wait taken from a Retry-After header, so one odd header cannot park an activity.
MAX_RETRY_AFTER_SECONDS = 300


def _retry_after_seconds(response: requests.Response) -> float | None:
    """Seconds from a ``Retry-After`` header in whole seconds, or None."""
    try:
        seconds = int(response.headers.get("retry-after"))
    except (TypeError, ValueError):
        return None
    return float(min(max(seconds, 0), MAX_RETRY_AFTER_SECONDS))


def _json_object(response: requests.Response) -> dict:
    """A 2xx body as a JSON object. A body that is not JSON, or not an object, is
    Google unavailable — never a decode error escaping to the caller."""
    try:
        body = response.json()
    except ValueError as err:
        raise GoogleError("unavailable") from err
    if not isinstance(body, dict):
        raise GoogleError("unavailable")
    return body


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
        # read as the deployment account, not the tenant's. Production makes one per
        # call; a test injects its mocked session here.
        self._drive_session = drive_session
        self._clock = clock
        self._tokens: dict[str, tuple[str, datetime]] = {}
        # The deployment session and the token cache are shared: the IAM calls (create,
        # delete, mint) serialize on this lock. Drive pages never take it.
        self._iam_lock = asyncio.Lock()

    def account_email(self, account_id: str) -> str:
        return f"{account_id}@{self._project}.iam.gserviceaccount.com"

    def _send(
        self,
        session: requests.Session,
        method: str,
        url: str,
        timeout: int = REQUEST_TIMEOUT,
        **kwargs,
    ) -> requests.Response:
        try:
            return session.request(method, url, timeout=timeout, **kwargs)
        except RefreshError as err:
            raise GoogleError("invalid_key") from err
        except (TransportError, requests.RequestException) as err:
            raise GoogleError("unavailable") from err

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        return self._send(self._session, method, url, **kwargs)

    def _check(self, response: requests.Response) -> None:
        if response.status_code >= 400:
            raise GoogleError(_code_for(response.status_code))

    def _check_drive(self, response: requests.Response) -> None:
        status = response.status_code
        if status < 400:
            return
        retry_after = _retry_after_seconds(response)
        if status in (401, 403):
            raise GoogleError(
                "permission_denied", DRIVE_REFUSED_TEXT, status=status, retry_after=retry_after
            )
        raise GoogleError(_code_for(status), status=status, retry_after=retry_after)

    async def ensure_account(self, account_id: str) -> str:
        email = self.account_email(account_id)

        def call() -> str:
            response = self._request(
                "POST",
                f"{IAM}/projects/{self._project}/serviceAccounts",
                json={"accountId": account_id, "serviceAccount": {"displayName": DISPLAY_NAME}},
                timeout=IAM_TIMEOUT,
            )
            created = response.status_code < 300
            if response.status_code == 409:
                response = self._request(
                    "GET",
                    f"{IAM}/projects/{self._project}/serviceAccounts/{email}",
                    timeout=IAM_TIMEOUT,
                )
                if response.status_code == 404:
                    # The IAM API is eventually consistent: a delete still propagating
                    # reads as "exists" to create and "gone" to get. Re-creating the ID
                    # is the documented path once the delete lands; undelete would
                    # restore the old identity, which an intentional disconnect rejects.
                    raise GoogleError("recently_deleted")
                created = False
            self._check(response)
            try:
                found = _json_object(response)["email"]
            except KeyError as err:
                raise GoogleError("unavailable") from err
            if created:
                # A re-created account is a new identity: the old token is stale.
                self._tokens.pop(email, None)
            return found

        async with self._iam_lock:
            return await asyncio.to_thread(call)

    async def delete_account(self, email: str) -> bool:
        def call() -> bool:
            response = self._request(
                "DELETE",
                f"{IAM}/projects/{self._project}/serviceAccounts/{email}",
                timeout=IAM_TIMEOUT,
            )
            if response.status_code == 404:
                return False
            self._check(response)
            return True

        async with self._iam_lock:
            gone = await asyncio.to_thread(call)
            # Deleted or already gone: a cached token for the account is stale either way.
            self._tokens.pop(email, None)
            return gone

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
        try:
            body = _json_object(response)
            token = body["accessToken"]
            expires = datetime.fromisoformat(body["expireTime"])
        except (KeyError, ValueError, TypeError) as err:
            raise GoogleError("unavailable") from err
        self._tokens[email] = (token, expires)
        return token

    async def list_shared(self, email: str) -> DriveListing:
        async with self._iam_lock:
            token = await asyncio.to_thread(self._token_for, email)

        def call() -> DriveListing:
            session = self._drive_session if self._drive_session is not None else requests.Session()
            try:
                headers = {"Authorization": f"Bearer {token}"}
                items: list[DriveItem] = []
                more = False
                page_token = None
                while True:
                    params = {
                        "q": "sharedWithMe and trashed=false",
                        "fields": "nextPageToken,files(id,name,mimeType,webViewLink)",
                        "includeItemsFromAllDrives": "true",
                        "supportsAllDrives": "true",
                        "pageSize": str(LIST_CAP),
                    }
                    if page_token:
                        params["pageToken"] = page_token
                    response = self._send(
                        session, "GET", f"{DRIVE}/files", headers=headers, params=params
                    )
                    self._check_drive(response)
                    body = _json_object(response)
                    try:
                        for file in body.get("files", []):
                            link = (
                                file.get("webViewLink")
                                or f"https://drive.google.com/file/d/{file['id']}"
                            )
                            kind = "folder" if file.get("mimeType") == MIME_FOLDER else "file"
                            items.append(DriveItem(file["id"], file["name"], link, kind))
                    except (KeyError, TypeError) as err:
                        raise GoogleError("unavailable") from err
                    page_token = body.get("nextPageToken")
                    if len(items) >= LIST_CAP:
                        more = bool(page_token) or len(items) > LIST_CAP
                        break
                    if not page_token:
                        break
                # At the cap with files exhausted, one drive still proves "more".
                if len(items) < LIST_CAP or not more:
                    page_token = None
                    while True:
                        params = {
                            "fields": "nextPageToken,drives(id,name)",
                            "pageSize": str(max(1, LIST_CAP - len(items))),
                        }
                        if page_token:
                            params["pageToken"] = page_token
                        response = self._send(
                            session, "GET", f"{DRIVE}/drives", headers=headers, params=params
                        )
                        self._check_drive(response)
                        body = _json_object(response)
                        try:
                            for drive in body.get("drives", []):
                                items.append(
                                    DriveItem(
                                        drive["id"],
                                        drive["name"],
                                        f"https://drive.google.com/drive/folders/{drive['id']}",
                                        "folder",
                                    )
                                )
                        except (KeyError, TypeError) as err:
                            raise GoogleError("unavailable") from err
                        page_token = body.get("nextPageToken")
                        if len(items) >= LIST_CAP:
                            more = more or bool(page_token) or len(items) > LIST_CAP
                            break
                        if not page_token:
                            break
                return DriveListing(items[:LIST_CAP], more)
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    # --- the Drive read surface (knowledge) -------------------------------------------
    # These carry the TENANT account's minted token, like list_shared, and run on a
    # session per call. Errors carry the HTTP status so ``drive_errors`` can classify
    # them for retry; a 404/403 is the tenant account's lost access to the file.

    async def _drive_token(self, email: str) -> str:
        async with self._iam_lock:
            return await asyncio.to_thread(self._token_for, email)

    def _drive_session_for_call(self) -> requests.Session:
        return self._drive_session if self._drive_session is not None else requests.Session()

    async def list_children(self, email: str, folder_id: str) -> list[DriveFile]:
        token = await self._drive_token(email)

        def call() -> list[DriveFile]:
            session = self._drive_session_for_call()
            try:
                headers = {"Authorization": f"Bearer {token}"}
                children: list[DriveFile] = []
                page_token = None
                while True:
                    params = {
                        "q": f"'{folder_id}' in parents and trashed=false",
                        "fields": "nextPageToken,files(id,name,mimeType,version,webViewLink,trashed)",
                        "includeItemsFromAllDrives": "true",
                        "supportsAllDrives": "true",
                        "pageSize": "1000",
                    }
                    if page_token:
                        params["pageToken"] = page_token
                    response = self._send(
                        session, "GET", f"{DRIVE}/files", headers=headers, params=params
                    )
                    self._check_drive(response)
                    body = _json_object(response)
                    try:
                        for file in body.get("files", []):
                            children.append(_drive_file(file))
                    except (KeyError, TypeError) as err:
                        raise GoogleError("unavailable") from err
                    page_token = body.get("nextPageToken")
                    if not page_token:
                        break
                return children
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    async def get_file(self, email: str, file_id: str) -> DriveFile:
        token = await self._drive_token(email)

        def call() -> DriveFile:
            session = self._drive_session_for_call()
            try:
                headers = {"Authorization": f"Bearer {token}"}
                params = {
                    "fields": "id,name,mimeType,version,webViewLink,trashed",
                    "supportsAllDrives": "true",
                }
                response = self._send(
                    session, "GET", f"{DRIVE}/files/{file_id}", headers=headers, params=params
                )
                self._check_drive(response)
                try:
                    return _drive_file(_json_object(response))
                except (KeyError, TypeError) as err:
                    raise GoogleError("unavailable") from err
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    async def export_file(self, email: str, file_id: str, mime_type: str) -> list[DriveSection]:
        token = await self._drive_token(email)

        def call() -> list[DriveSection]:
            session = self._drive_session_for_call()
            try:
                headers = {"Authorization": f"Bearer {token}"}
                if mime_type == MIME_SHEET:
                    return self._export_sheet(session, headers, file_id)
                response = self._send(
                    session,
                    "GET",
                    f"{DRIVE}/files/{file_id}/export",
                    headers=headers,
                    params={"mimeType": "text/plain"},
                )
                self._check_drive(response)
                return [DriveSection("", response.text)]
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    def _export_sheet(
        self, session: requests.Session, headers: dict, file_id: str
    ) -> list[DriveSection]:
        """Every tab of a Sheet as CSV, its name the heading, through the Sheets API.

        Drive's own ``text/csv`` export only covers the first tab and names none, so the
        tabs are read from the spreadsheet's grid data instead (the Drive read scope
        authorizes the Sheets API)."""
        params = {"fields": "sheets(properties.title,data.rowData.values.formattedValue)"}
        response = self._send(
            session, "GET", f"{SHEETS}/spreadsheets/{file_id}", headers=headers, params=params
        )
        self._check_drive(response)
        body = _json_object(response)
        sections: list[DriveSection] = []
        try:
            for sheet in body.get("sheets", []):
                title = sheet["properties"]["title"]
                buffer = io.StringIO()
                writer = csv.writer(buffer, lineterminator="\n")
                grid = (sheet.get("data") or [{}])[0]
                for row in grid.get("rowData", []):
                    writer.writerow(
                        [(cell or {}).get("formattedValue", "") for cell in row.get("values", [])]
                    )
                sections.append(DriveSection(title, buffer.getvalue()))
        except (KeyError, TypeError, IndexError) as err:
            raise GoogleError("unavailable") from err
        return sections


def _drive_file(file: dict) -> DriveFile:
    return DriveFile(
        id=file["id"],
        name=file["name"],
        mime_type=file["mimeType"],
        version=str(file.get("version", "")),
        web_view_link=file.get("webViewLink", ""),
        trashed=bool(file.get("trashed", False)),
    )


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
        # The Drive tree the read surface serves: metadata by ID, a folder's child IDs,
        # and each exportable file's text sections. ``export_calls`` and ``get_calls``
        # record what production asked for, so a test can see an unchanged file was not
        # re-exported. The failure injections live here, not monkeypatched per test.
        self.drive_files: dict[str, DriveFile] = {}
        self.drive_children: dict[str, list[str]] = {}
        self.drive_content: dict[str, list[DriveSection]] = {}
        self.drive_refused: set[str] = set()
        self.export_calls: list[str] = []
        self.get_calls: list[str] = []
        self._export_error: Exception | None = None
        self._get_error: Exception | None = None
        self._children_error: Exception | None = None
        # A file whose export always fails (a lasting refusal), for the retry-then-count path.
        self.drive_export_errors: dict[str, Exception] = {}

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

    async def delete_account(self, email: str) -> bool:
        self.delete_calls.append(email)
        if self._delete_error is not None:
            error, self._delete_error = self._delete_error, None
            raise error
        account_id = next((aid for aid, e in self.accounts.items() if e == email), None)
        if account_id is None:
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

    async def list_shared(self, email: str) -> DriveListing:
        if self._list_error is not None:
            error, self._list_error = self._list_error, None
            raise error
        shared = self.shared.get(email, [])
        return DriveListing(list(shared[:LIST_CAP]), len(shared) > LIST_CAP)

    # --- the Drive tree, for the read surface -----------------------------------------

    def add_drive_folder(self, folder_id: str, name: str, parent: str | None = None) -> None:
        self.drive_files[folder_id] = DriveFile(
            folder_id,
            name,
            MIME_FOLDER,
            web_view_link=f"https://drive.google.com/drive/folders/{folder_id}",
        )
        self.drive_children.setdefault(folder_id, [])
        if parent is not None:
            self.drive_children.setdefault(parent, []).append(folder_id)

    def add_drive_file(
        self,
        file_id: str,
        name: str,
        mime_type: str,
        parent: str,
        sections: list[DriveSection],
        version: str = "1",
    ) -> None:
        self.drive_files[file_id] = DriveFile(
            file_id,
            name,
            mime_type,
            version=version,
            web_view_link=f"https://drive.google.com/file/d/{file_id}",
        )
        self.drive_content[file_id] = list(sections)
        self.drive_children.setdefault(parent, []).append(file_id)

    def set_drive_version(self, file_id: str, version: str) -> None:
        self.drive_files[file_id] = replace(self.drive_files[file_id], version=version)

    def set_drive_content(self, file_id: str, sections: list[DriveSection]) -> None:
        self.drive_content[file_id] = list(sections)

    def trash_drive_file(self, file_id: str) -> None:
        self.drive_files[file_id] = replace(self.drive_files[file_id], trashed=True)

    def remove_drive_child(self, parent: str, child_id: str) -> None:
        self.drive_children[parent] = [c for c in self.drive_children[parent] if c != child_id]

    def fail_next_export(self, error: Exception) -> None:
        self._export_error = error

    def fail_export_for(self, file_id: str, error: Exception) -> None:
        """Make every export of ``file_id`` raise ``error`` (a lasting refusal)."""
        self.drive_export_errors[file_id] = error

    def fail_next_get(self, error: Exception) -> None:
        self._get_error = error

    def fail_next_children(self, error: Exception) -> None:
        self._children_error = error

    async def list_children(self, email: str, folder_id: str) -> list[DriveFile]:
        if self._children_error is not None:
            error, self._children_error = self._children_error, None
            raise error
        folder = self.drive_files.get(folder_id)
        if folder is None or folder.trashed:
            raise GoogleError("not_found", status=404)
        if folder_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        return [
            self.drive_files[c]
            for c in self.drive_children.get(folder_id, [])
            if c in self.drive_files and not self.drive_files[c].trashed
        ]

    async def get_file(self, email: str, file_id: str) -> DriveFile:
        self.get_calls.append(file_id)
        if self._get_error is not None:
            error, self._get_error = self._get_error, None
            raise error
        if file_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        file = self.drive_files.get(file_id)
        if file is None:
            raise GoogleError("not_found", status=404)
        return file

    async def export_file(self, email: str, file_id: str, mime_type: str) -> list[DriveSection]:
        self.export_calls.append(file_id)
        if self._export_error is not None:
            error, self._export_error = self._export_error, None
            raise error
        if file_id in self.drive_export_errors:
            raise self.drive_export_errors[file_id]
        if file_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        if file_id not in self.drive_files:
            raise GoogleError("not_found", status=404)
        return list(self.drive_content.get(file_id, []))


def accounts_from_settings(
    project_id: str | None, service_account_key: str | None
) -> RealGoogleAccounts | None:
    """The deployment's Google client, or None when either setting is unset: Drive is
    then not set up on this deployment. A malformed key raises ``GoogleError`` here,
    at start-up, never at the first click."""
    if not project_id or not service_account_key:
        return None
    return RealGoogleAccounts(project_id, service_account_key)
