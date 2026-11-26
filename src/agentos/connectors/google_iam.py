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

The knowledge read surface (D-46) lists a folder's children, gets a file's metadata
(parents included), exports a Doc/Sheet/Slides as text, and reads a shared drive's root
(``drives.get``) and its children (a drive-scoped ``files.list``). A 403 carries the
body's reason, so ``drive_errors`` can tell a quota wave from a disabled API from lost
sharing. The comment write surface (D-47) lists a file's comments (paged, each with
whether the tenant's account wrote it) and posts a comment as that account.

The section write surface (D-50) reads a Doc's structure (``documents.get``: its
revisionId, title and body paragraphs with their named styles and UTF-16 indices, plus the
index ranges of tables and inline images) and replaces a section's content in one
``documents.batchUpdate`` carrying ``writeControl.requiredRevisionId``: it inserts the new
text first at the section's first content paragraph (so the new paragraphs take its NORMAL
style), then deletes the shifted old range, normalising an empty section's inserted range to
NORMAL_TEXT; a Doc that moved since the read answers 400 ``revision_mismatch``.

The fake keeps the accounts and what is shared with each, and carries the failure
injections in one place: ``fail_after_write`` (the response is lost after the effect),
``hang_after_write`` (a second attempt overlaps the first), ``delay_visibility`` (a
comment or appended row stays out of the next searches), ``hang_next_sheet_read``,
``fail_next_list``, ``fail_next_delete``, and the Drive tree's ``fail_next_get``/
``fail_next_export``/``fail_next_children`` and the lasting ``fail_get_for``/
``fail_export_for``/``fail_children_for``.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Protocol
from urllib.parse import quote

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
DOCS = "https://docs.googleapis.com/v1"
CLOUD_PLATFORM = "https://www.googleapis.com/auth/cloud-platform"

# Google's native mime types. Only Docs, Sheets and Slides export to text; a folder is
# walked, and anything else is skipped and counted by the indexer.
MIME_FOLDER = "application/vnd.google-apps.folder"
MIME_DOC = "application/vnd.google-apps.document"
MIME_SHEET = "application/vnd.google-apps.spreadsheet"
MIME_SLIDES = "application/vnd.google-apps.presentation"
EXPORTABLE = (MIME_DOC, MIME_SHEET, MIME_SLIDES)
# The full Drive scope: the comment write (D-47) needs it, and it covers every read too
# (the Sheets tab export included). One constant, so a minted token serves reads and writes.
DRIVE_SCOPES = ("https://www.googleapis.com/auth/drive",)
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

# A 403's body reason says which refusal it is. Quota refusals pass on their own (retry
# like a 429); a disabled API is a lasting misconfiguration (failed, rows kept); any other
# reason (insufficientFilePermissions, forbidden, none) is lost sharing — gone.
DRIVE_QUOTA_REASONS = frozenset({"userRateLimitExceeded", "rateLimitExceeded", "quotaExceeded"})
DRIVE_DISABLED_REASONS = frozenset({"SERVICE_DISABLED", "accessNotConfigured"})


def problem_text(code: str | None) -> str:
    """The plain words for a stored error code; an unknown code still reads as a problem."""
    return PROBLEM_TEXT.get(code or "", "Google Drive reported a problem.")


class GoogleError(Exception):
    """A Google call failed. ``code`` is the short class the row stores and the page
    words; the message is the plain words, never Google's body, a key or a token.
    ``words`` overrides the code's default text where the call class words it
    differently (a Drive refusal names the tenant account's access).

    ``status``, ``retry_after`` and ``reason`` are set only by the Drive read surface,
    so the connector error table (``drive_errors``) can classify a read for retry; the
    IAM calls leave them None and are handled by their own callers. ``reason`` is the
    body's ``error.errors[].reason`` (a 403's quota, disabled-API or sharing class).
    """

    def __init__(
        self,
        code: str,
        words: str | None = None,
        status: int | None = None,
        retry_after: float | None = None,
        reason: str | None = None,
    ):
        self.code = code
        self.status = status
        self.retry_after = retry_after
        self.reason = reason
        super().__init__(words if words is not None else problem_text(code))


@dataclass(frozen=True)
class DriveItem:
    """One file or shared drive the tenant's account can see: its ID, name and link.

    ``kind`` is ``folder``, ``file``, ``shared_drive``, ``sheet``, ``template`` or
    ``section``, so the grant picker knows which grant to add. ``tab`` is empty on a
    listing; the picker's tab step fills it for a Sheet before the grant is added.
    ``mime_type`` is the file's Drive mime type (the template picker offers Docs only);
    ``folder_id`` is empty on a listing, the template picker's folder step fills it before
    the grant is added. ``drive_id`` is the shared drive the item sits in, empty for one in
    My Drive; the template picker offers only folders with one as destinations (D-49).
    ``heading`` is empty on a listing; a section pick fills it with the heading the creator
    names (D-50).
    """

    id: str
    name: str
    link: str
    kind: str = "file"
    tab: str = ""
    mime_type: str = ""
    folder_id: str = ""
    drive_id: str = ""
    heading: str = ""


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
    ``parents`` are the folder IDs the file sits in (``files.get`` only); the search
    re-check walks them up to the granted root. ``drive_id`` is the shared drive the file
    sits in (``files.get`` only), empty for a file in My Drive; the template write's checks
    refuse a destination folder whose ``drive_id`` is empty or not the recorded one (D-49).
    """

    id: str
    name: str
    mime_type: str
    version: str = ""
    web_view_link: str = ""
    trashed: bool = False
    parents: tuple[str, ...] = ()
    drive_id: str = ""


@dataclass(frozen=True)
class DriveSection:
    """One headed stretch of an exported file's text.

    A Doc or Slides export is one section with no heading; a Sheet gives one section per
    tab, its name the heading and its rows the text (CSV). The indexer turns these into
    the same heading/text blocks a Notion page gives, so chunking is shared.
    """

    heading: str
    text: str


@dataclass(frozen=True)
class DriveComment:
    """One comment on a file: its ID, its text, and whether the tenant's own account
    wrote it (Drive's ``author.me``). The marker lookup counts only the account's own
    comments, as Notion's counts only the integration's."""

    id: str
    content: str
    mine: bool


@dataclass(frozen=True)
class SheetTab:
    """One tab of a Sheet as the append write needs it: references and small values.

    ``sheet_id`` is the tab's numeric ID (the Sheets API addresses tabs by it, not by
    name); ``header`` is its first row's column names; ``data_rows`` is the number of rows
    that hold data, so the appended row lands at that zero-based index. The read these come
    from asks for the granted tab alone (``ranges``): its rows' formatted values for the
    write's header-and-count read, row 1 alone for a draft's or submit's header check. Only
    these three values are kept — the response's cell values never become the table.
    """

    sheet_id: int
    header: tuple[str, ...]
    data_rows: int


@dataclass(frozen=True)
class SheetMetadata:
    """One developer metadata entry on a Sheet: its ID, key, value and the zero-based row
    it marks. The append write's marker lookup searches by key and value (D-48)."""

    metadata_id: int
    key: str
    value: str
    row_index: int


@dataclass(frozen=True)
class DocParagraph:
    """One paragraph of a Doc as the section write needs it: its text (ending in a
    newline), its named style (``HEADING_1``..``HEADING_6``, ``TITLE``, ``SUBTITLE`` or
    ``NORMAL_TEXT``) and its ``startIndex``/``endIndex`` in the document body, as Google
    reports them (UTF-16 code-unit offsets). The section write locates a heading and the
    paragraphs under it by these (D-50)."""

    text: str
    style: str
    start_index: int
    end_index: int


@dataclass(frozen=True)
class DocRead:
    """One ``documents.get`` of a Doc: its ``revisionId`` (the section write's lock), its
    title and its body's paragraphs. References and small values plus the section text the
    draft freezes; the revisionId is what the write's ``writeControl`` names (D-50).

    ``foreign`` is the UTF-16 index ranges of the body's non-paragraph elements (tables,
    section breaks) and inline images: a section whose content range overlaps one is not
    plain paragraphs, so the replace refuses it (D-50)."""

    revision_id: str
    title: str
    paragraphs: tuple[DocParagraph, ...]
    foreign: tuple[tuple[int, int], ...] = ()


def utf16_len(text: str) -> int:
    """A string's length in UTF-16 code units, the offset unit Google Docs indexes by."""
    return len(text.encode("utf-16-le")) // 2


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

    async def list_folder(self, email: str, folder_id: str) -> DriveListing:
        """One level of a folder or a shared drive's root for the grant picker: its
        non-trashed children, folders first then by name, at most 100, and whether more
        are there. A folder the account cannot see raises ``GoogleError``."""
        ...

    async def list_children(self, email: str, folder_id: str) -> list[DriveFile]:
        """Every non-trashed child of a folder (files and sub-folders), paging through
        Drive's list. Metadata only, never content."""
        ...

    async def get_file(self, email: str, file_id: str) -> DriveFile:
        """One file or folder's metadata, its parents included. A 404 or 403 raises
        ``GoogleError`` with its status and reason, so the caller can classify the
        refusal (gone, retryable quota, or a lasting failure)."""
        ...

    async def export_file(self, email: str, file_id: str, mime_type: str) -> list[DriveSection]:
        """A Doc, Sheet or Slides as text sections. Docs and Slides export to plain text;
        a Sheet gives one CSV section per tab, its name the heading."""
        ...

    async def get_drive(self, email: str, drive_id: str) -> DriveFile:
        """A shared drive's root as a folder node (``drives.get``): its ID and name."""
        ...

    async def list_drive_children(self, email: str, drive_id: str) -> list[DriveFile]:
        """The shared drive root's non-trashed children, listed drive-scoped
        (``corpora=drive`` with the drive's ID). Metadata only, never content."""
        ...

    async def list_comments(self, email: str, file_id: str) -> list[DriveComment]:
        """Every comment on a file, paging through Drive's list, each with whether the
        tenant's own account wrote it. The marker lookup of a comment write (D-47)."""
        ...

    async def create_comment(self, email: str, file_id: str, content: str) -> str:
        """Post a comment on a file as the tenant's account; returns the comment's ID."""
        ...

    async def read_sheet(
        self, email: str, file_id: str, tab: str, header_only: bool = False
    ) -> SheetTab:
        """One tab's numeric sheet ID, header row and count of data rows, read from the
        spreadsheet's grid data (one ``spreadsheets.get`` asking ``ranges`` for the granted
        tab alone; ``header_only`` asks for row 1 only, so ``data_rows`` counts that row).
        A tab the Sheet does not have raises ``GoogleError`` (a 400), so the caller refuses
        rather than appends blind."""
        ...

    async def list_sheet_tabs(self, email: str, file_id: str) -> list[str]:
        """The Sheet's tab titles (one ``spreadsheets.get`` of ``sheets.properties.title``
        only), for the picker's tab step. Never the tabs' content."""
        ...

    async def search_sheet_metadata(
        self, email: str, file_id: str, key: str, value: str
    ) -> list[SheetMetadata]:
        """The developer metadata entries matching ``key`` and ``value``
        (``developerMetadata:search``): the marker lookup of an append write (D-48)."""
        ...

    async def append_sheet_row(
        self,
        email: str,
        file_id: str,
        sheet_id: int,
        row: list[str | int | float],
        row_index: int,
        key: str,
        value: str,
    ) -> str:
        """Append ``row`` and mark it with developer metadata in ONE atomic batchUpdate
        (``appendCells`` then ``createDeveloperMetadata`` on the appended row), so a lost
        response never leaves an unmarked row. A text cell is sent as ``stringValue`` and
        lands literally (never parsed into a formula or another type); a number as
        ``numberValue``. Returns the created metadata's ID."""
        ...

    async def copy_file(
        self, email: str, template_id: str, folder_id: str, title: str, key: str, value: str
    ) -> DriveFile:
        """Copy a template Doc into a folder as a new Doc titled ``title``, carrying the
        marker appProperty (``key``: ``value``) in the SAME ``files.copy`` request, so a lost
        response never leaves an unmarked copy a retry would make again. Returns the new
        Doc's metadata (D-49)."""
        ...

    async def search_app_properties(
        self, email: str, folder_id: str, drive_id: str, key: str, value: str
    ) -> list[DriveFile]:
        """The non-trashed files in ``folder_id`` carrying the appProperty ``key``: ``value``
        (``files.list`` with ``appProperties has``, drive-scoped): the marker lookup of a
        create-from-template write (D-49)."""
        ...

    async def fill_doc(self, email: str, file_id: str, replacements: list[tuple[str, str]]) -> None:
        """Apply the ordered ``(old, new)`` literal replacements in ONE Docs
        ``documents.batchUpdate`` (one ``replaceAllText`` per pair, ``matchCase``): the
        create write's two passes — every raw ``{{blank}}`` form to its sentinel, then each
        sentinel to its value — so a value containing another blank's placeholder lands
        literally (D-49). A string no longer present is a no-op, so a retry fills only what
        is left."""
        ...

    async def read_template_text(self, email: str, file_id: str) -> str:
        """The text of every part of a Doc that can hold a ``{{blank}}``: one
        ``documents.get`` of every tab's body, headers, footers and footnotes, joined with
        newlines. The plain-text export leaves headers and footers out, so the template
        blank read and the post-fill check use this instead (D-49)."""
        ...

    async def read_doc(self, email: str, file_id: str) -> DocRead:
        """One ``documents.get`` of a Doc: its revisionId, title and body paragraphs (each
        with its text, named style and UTF-16 start/end index). The section write reads the
        heading and the text under it from this, and locks the write to its revisionId
        (D-50). A Doc that is gone or unreadable raises ``GoogleError`` with its status."""
        ...

    async def replace_doc_section(
        self,
        email: str,
        file_id: str,
        required_revision_id: str,
        insert_index: int,
        insert_text: str,
        delete_start: int,
        delete_end: int,
        style_start: int = 0,
        style_end: int = 0,
    ) -> str:
        """Replace a section's content in ONE Docs ``documents.batchUpdate`` carrying
        ``writeControl.requiredRevisionId``: insert ``insert_text`` at ``insert_index`` first
        (the section's first content paragraph, so the new paragraphs take its NORMAL style),
        then delete the shifted old range ``[delete_start, delete_end)``. A non-zero
        ``[style_start, style_end)`` normalises that range to NORMAL_TEXT and clears its
        bullets (an empty section, which has no content paragraph to inherit from). The
        revision is the lock — a Doc that moved since the read answers 400
        (``revision_mismatch``) and nothing is written. Returns the Doc's ID (D-50)."""
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


def _drive_reason(response: requests.Response) -> str | None:
    """The first ``error.errors[].reason`` of an error body, or None when the body
    carries none (a refusal with no reason reads as lost sharing)."""
    try:
        errors = response.json()["error"]["errors"]
        reason = errors[0]["reason"]
    except (ValueError, KeyError, TypeError, IndexError):
        return None
    return reason if isinstance(reason, str) else None


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
            reason = _drive_reason(response) if status == 403 else None
            if reason in DRIVE_QUOTA_REASONS:
                # A quota wave, not lost sharing: retry like a 429, with Google's wait.
                raise GoogleError(
                    "rate_limited", status=status, retry_after=retry_after, reason=reason
                )
            raise GoogleError(
                "permission_denied",
                DRIVE_REFUSED_TEXT,
                status=status,
                retry_after=retry_after,
                reason=reason,
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
                        "fields": "nextPageToken,files(id,name,mimeType,webViewLink,driveId)",
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
                            items.append(_picker_item(file))
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
                                        "shared_drive",
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

    async def list_folder(self, email: str, folder_id: str) -> DriveListing:
        token = await self._drive_token(email)

        def call() -> DriveListing:
            session = self._drive_session_for_call()
            try:
                headers = {"Authorization": f"Bearer {token}"}
                # The same two reads the indexer makes: ``drives.get`` says the ID is a
                # shared drive's root (listed drive-scoped, as a member can only list it);
                # a 404 means a folder, read with ``files.get`` (which refuses an ID the
                # account cannot see) and listed unscoped.
                drive_id = ""
                response = self._send(
                    session,
                    "GET",
                    f"{DRIVE}/drives/{quote(folder_id, safe='')}",
                    headers=headers,
                    params={"fields": "id,name"},
                )
                if response.status_code == 404:
                    response = self._send(
                        session,
                        "GET",
                        f"{DRIVE}/files/{quote(folder_id, safe='')}",
                        headers=headers,
                        params={"fields": "id", "supportsAllDrives": "true"},
                    )
                else:
                    drive_id = folder_id
                self._check_drive(response)
                items: list[DriveItem] = []
                more = False
                page_token = None
                while True:
                    params = {
                        "q": f"'{folder_id}' in parents and trashed=false",
                        "fields": "nextPageToken,files(id,name,mimeType,webViewLink,driveId)",
                        "includeItemsFromAllDrives": "true",
                        "supportsAllDrives": "true",
                        "orderBy": "folder,name",
                        "pageSize": str(LIST_CAP),
                    }
                    if drive_id:
                        params["corpora"] = "drive"
                        params["driveId"] = drive_id
                    if page_token:
                        params["pageToken"] = page_token
                    response = self._send(
                        session, "GET", f"{DRIVE}/files", headers=headers, params=params
                    )
                    self._check_drive(response)
                    body = _json_object(response)
                    try:
                        for file in body.get("files", []):
                            items.append(_picker_item(file))
                    except (KeyError, TypeError) as err:
                        raise GoogleError("unavailable") from err
                    page_token = body.get("nextPageToken")
                    if len(items) >= LIST_CAP:
                        more = bool(page_token) or len(items) > LIST_CAP
                        break
                    if not page_token:
                        break
                return DriveListing(items[:LIST_CAP], more)
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

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
                    "fields": "id,name,mimeType,version,webViewLink,trashed,parents,driveId",
                    "supportsAllDrives": "true",
                }
                response = self._send(
                    session,
                    "GET",
                    f"{DRIVE}/files/{quote(file_id, safe='')}",
                    headers=headers,
                    params=params,
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
                    f"{DRIVE}/files/{quote(file_id, safe='')}/export",
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
            session,
            "GET",
            f"{SHEETS}/spreadsheets/{quote(file_id, safe='')}",
            headers=headers,
            params=params,
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

    async def get_drive(self, email: str, drive_id: str) -> DriveFile:
        token = await self._drive_token(email)

        def call() -> DriveFile:
            session = self._drive_session_for_call()
            try:
                headers = {"Authorization": f"Bearer {token}"}
                response = self._send(
                    session,
                    "GET",
                    f"{DRIVE}/drives/{quote(drive_id, safe='')}",
                    headers=headers,
                    params={"fields": "id,name"},
                )
                self._check_drive(response)
                body = _json_object(response)
                try:
                    return DriveFile(
                        body["id"],
                        body["name"],
                        MIME_FOLDER,
                        web_view_link=f"https://drive.google.com/drive/folders/{body['id']}",
                    )
                except (KeyError, TypeError) as err:
                    raise GoogleError("unavailable") from err
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    async def list_drive_children(self, email: str, drive_id: str) -> list[DriveFile]:
        token = await self._drive_token(email)

        def call() -> list[DriveFile]:
            session = self._drive_session_for_call()
            try:
                headers = {"Authorization": f"Bearer {token}"}
                children: list[DriveFile] = []
                page_token = None
                while True:
                    # The drive root's folder ID is the drive's own ID; the drive-scoped
                    # corpora is what lets a member list it at all.
                    params = {
                        "q": f"'{drive_id}' in parents and trashed=false",
                        "corpora": "drive",
                        "driveId": drive_id,
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

    # --- the comment write surface (D-47) ----------------------------------------------
    # These carry the TENANT account's minted token, like the reads, so a comment is
    # written as the account the files are shared with, never the deployment's.

    async def list_comments(self, email: str, file_id: str) -> list[DriveComment]:
        token = await self._drive_token(email)

        def call() -> list[DriveComment]:
            session = self._drive_session_for_call()
            try:
                headers = {"Authorization": f"Bearer {token}"}
                comments: list[DriveComment] = []
                page_token = None
                while True:
                    params = {
                        "fields": "nextPageToken,comments(id,content,author)",
                        "supportsAllDrives": "true",
                        "pageSize": "100",
                    }
                    if page_token:
                        params["pageToken"] = page_token
                    response = self._send(
                        session,
                        "GET",
                        f"{DRIVE}/files/{quote(file_id, safe='')}/comments",
                        headers=headers,
                        params=params,
                    )
                    self._check_drive(response)
                    body = _json_object(response)
                    try:
                        for comment in body.get("comments", []):
                            comments.append(
                                DriveComment(
                                    comment["id"],
                                    comment.get("content", ""),
                                    bool((comment.get("author") or {}).get("me", False)),
                                )
                            )
                    except (KeyError, TypeError) as err:
                        raise GoogleError("unavailable") from err
                    page_token = body.get("nextPageToken")
                    if not page_token:
                        break
                return comments
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    async def create_comment(self, email: str, file_id: str, content: str) -> str:
        token = await self._drive_token(email)

        def call() -> str:
            session = self._drive_session_for_call()
            try:
                response = self._send(
                    session,
                    "POST",
                    f"{DRIVE}/files/{quote(file_id, safe='')}/comments",
                    headers={"Authorization": f"Bearer {token}"},
                    params={"fields": "id", "supportsAllDrives": "true"},
                    json={"content": content},
                )
                self._check_drive(response)
                try:
                    return _json_object(response)["id"]
                except (KeyError, TypeError) as err:
                    raise GoogleError("unavailable") from err
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    # --- the Sheet append write surface (D-48) -----------------------------------------
    # These carry the TENANT account's minted token, like the reads and the comment, so a
    # row is appended as the account the Sheet is shared with, never the deployment's.

    async def read_sheet(
        self, email: str, file_id: str, tab: str, header_only: bool = False
    ) -> SheetTab:
        token = await self._drive_token(email)

        def call() -> SheetTab:
            session = self._drive_session_for_call()
            try:
                response = self._send(
                    session,
                    "GET",
                    f"{SHEETS}/spreadsheets/{quote(file_id, safe='')}",
                    headers={"Authorization": f"Bearer {token}"},
                    params={
                        "ranges": _tab_range(tab, header_only),
                        "fields": "sheets(properties(sheetId,title),data.rowData.values.formattedValue)",
                    },
                )
                self._check_drive(response)
                body = _json_object(response)
                try:
                    for sheet in body.get("sheets", []):
                        props = sheet["properties"]
                        if props["title"] != tab:
                            continue
                        rows = (sheet.get("data") or [{}])[0].get("rowData", [])
                        return _sheet_tab(int(props["sheetId"]), rows)
                except (KeyError, TypeError, IndexError, ValueError) as err:
                    raise GoogleError("unavailable") from err
                raise GoogleError("refused", f"The Sheet has no tab named '{tab}'.", status=400)
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    async def list_sheet_tabs(self, email: str, file_id: str) -> list[str]:
        token = await self._drive_token(email)

        def call() -> list[str]:
            session = self._drive_session_for_call()
            try:
                response = self._send(
                    session,
                    "GET",
                    f"{SHEETS}/spreadsheets/{quote(file_id, safe='')}",
                    headers={"Authorization": f"Bearer {token}"},
                    params={"fields": "sheets.properties.title"},
                )
                self._check_drive(response)
                body = _json_object(response)
                try:
                    return [sheet["properties"]["title"] for sheet in body.get("sheets", [])]
                except (KeyError, TypeError) as err:
                    raise GoogleError("unavailable") from err
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    async def search_sheet_metadata(
        self, email: str, file_id: str, key: str, value: str
    ) -> list[SheetMetadata]:
        token = await self._drive_token(email)

        def call() -> list[SheetMetadata]:
            session = self._drive_session_for_call()
            try:
                response = self._send(
                    session,
                    "POST",
                    f"{SHEETS}/spreadsheets/{quote(file_id, safe='')}/developerMetadata:search",
                    headers={"Authorization": f"Bearer {token}"},
                    json={
                        "dataFilters": [
                            {
                                "developerMetadataLookup": {
                                    "metadataKey": key,
                                    "metadataValue": value,
                                }
                            }
                        ]
                    },
                )
                self._check_drive(response)
                body = _json_object(response)
                try:
                    found = []
                    for match in body.get("matchedDeveloperMetadata", []):
                        meta = match["developerMetadata"]
                        dim = (meta.get("location") or {}).get("dimensionRange") or {}
                        found.append(
                            SheetMetadata(
                                int(meta["metadataId"]),
                                meta.get("metadataKey", ""),
                                meta.get("metadataValue", ""),
                                int(dim.get("startIndex", 0)),
                            )
                        )
                    return found
                except (KeyError, TypeError, ValueError) as err:
                    raise GoogleError("unavailable") from err
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    async def append_sheet_row(
        self,
        email: str,
        file_id: str,
        sheet_id: int,
        row: list[str | int | float],
        row_index: int,
        key: str,
        value: str,
    ) -> str:
        token = await self._drive_token(email)

        def call() -> str:
            session = self._drive_session_for_call()
            try:
                # One batchUpdate: Google applies its requests together atomically, so the
                # row and its marker land together or not at all — a lost response cannot
                # leave an unmarked row a retry would append again. Each cell's type is the
                # typed ExtendedValue union: stringValue lands literally (only formulaValue
                # becomes a formula), numberValue keeps a model's JSON number a number.
                response = self._send(
                    session,
                    "POST",
                    f"{SHEETS}/spreadsheets/{quote(file_id, safe='')}:batchUpdate",
                    headers={"Authorization": f"Bearer {token}"},
                    json={
                        "requests": [
                            {
                                "appendCells": {
                                    "sheetId": sheet_id,
                                    "rows": [
                                        {
                                            "values": [
                                                {"userEnteredValue": {"numberValue": v}}
                                                if isinstance(v, (int, float))
                                                else {"userEnteredValue": {"stringValue": v}}
                                                for v in row
                                            ]
                                        }
                                    ],
                                    "fields": "userEnteredValue",
                                }
                            },
                            {
                                "createDeveloperMetadata": {
                                    "developerMetadata": {
                                        "metadataKey": key,
                                        "metadataValue": value,
                                        "visibility": "DOCUMENT",
                                        "location": {
                                            "dimensionRange": {
                                                "sheetId": sheet_id,
                                                "dimension": "ROWS",
                                                "startIndex": row_index,
                                                "endIndex": row_index + 1,
                                            }
                                        },
                                    }
                                }
                            },
                        ]
                    },
                )
                self._check_drive(response)
                try:
                    replies = _json_object(response)["replies"]
                    created = replies[1]["createDeveloperMetadata"]["developerMetadata"]
                    return str(created["metadataId"])
                except (KeyError, TypeError, IndexError, ValueError) as err:
                    raise GoogleError("unavailable") from err
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    # --- the create-from-template write surface (D-49) ---------------------------------
    # These carry the TENANT account's minted token, like the reads and the other writes, so
    # a new Doc is created as the account the template and folder are shared with.

    async def copy_file(
        self, email: str, template_id: str, folder_id: str, title: str, key: str, value: str
    ) -> DriveFile:
        token = await self._drive_token(email)

        def call() -> DriveFile:
            session = self._drive_session_for_call()
            try:
                # The marker appProperty lands in the same request as the copy, so a lost
                # response never leaves an unmarked copy a retry would make again.
                response = self._send(
                    session,
                    "POST",
                    f"{DRIVE}/files/{quote(template_id, safe='')}/copy",
                    headers={"Authorization": f"Bearer {token}"},
                    params={"fields": "id,name,mimeType,webViewLink", "supportsAllDrives": "true"},
                    json={
                        "name": title,
                        "parents": [folder_id],
                        "appProperties": {key: value},
                    },
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

    async def search_app_properties(
        self, email: str, folder_id: str, drive_id: str, key: str, value: str
    ) -> list[DriveFile]:
        token = await self._drive_token(email)

        def call() -> list[DriveFile]:
            session = self._drive_session_for_call()
            try:
                headers = {"Authorization": f"Bearer {token}"}
                found: list[DriveFile] = []
                page_token = None
                while True:
                    params = {
                        "q": (
                            f"'{folder_id}' in parents and trashed=false and "
                            f"appProperties has {{ key='{key}' and value='{value}' }}"
                        ),
                        "corpora": "drive",
                        "driveId": drive_id,
                        "fields": "nextPageToken,files(id,name,mimeType,webViewLink)",
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
                            found.append(_drive_file(file))
                    except (KeyError, TypeError) as err:
                        raise GoogleError("unavailable") from err
                    page_token = body.get("nextPageToken")
                    if not page_token:
                        break
                return found
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    async def fill_doc(self, email: str, file_id: str, replacements: list[tuple[str, str]]) -> None:
        token = await self._drive_token(email)

        def call() -> None:
            session = self._drive_session_for_call()
            try:
                # One replaceAllText per replacement, in order, literal text (no markup
                # interpretation): the create write's two passes — blank forms to sentinels,
                # sentinels to values. A string no longer present is a no-op, so a retry
                # fills only what is left. ``replaceAllText`` takes no segment or header
                # setting: its reference says only "all instances of text matching a
                # criteria" and, without ``tabsCriteria``, every tab. Whether that reaches
                # headers and footers is not stated there, so the create write reads every
                # segment after the fill and refuses while a blank is left.
                response = self._send(
                    session,
                    "POST",
                    f"{DOCS}/documents/{quote(file_id, safe='')}:batchUpdate",
                    headers={"Authorization": f"Bearer {token}"},
                    json={
                        "requests": [
                            {
                                "replaceAllText": {
                                    "containsText": {"text": old, "matchCase": True},
                                    "replaceText": new,
                                }
                            }
                            for old, new in replacements
                        ]
                    },
                )
                self._check_drive(response)
                _json_object(response)
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    async def read_template_text(self, email: str, file_id: str) -> str:
        token = await self._drive_token(email)

        def call() -> str:
            session = self._drive_session_for_call()
            try:
                response = self._send(
                    session,
                    "GET",
                    f"{DOCS}/documents/{quote(file_id, safe='')}",
                    headers={"Authorization": f"Bearer {token}"},
                    params={"includeTabsContent": "true", "fields": "tabs"},
                )
                self._check_drive(response)
                body = _json_object(response)
                try:
                    return "\n".join(_doc_segment_texts(body.get("tabs", [])))
                except (KeyError, TypeError, AttributeError) as err:
                    raise GoogleError("unavailable") from err
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    async def read_doc(self, email: str, file_id: str) -> DocRead:
        token = await self._drive_token(email)

        def call() -> DocRead:
            session = self._drive_session_for_call()
            try:
                response = self._send(
                    session,
                    "GET",
                    f"{DOCS}/documents/{quote(file_id, safe='')}",
                    headers={"Authorization": f"Bearer {token}"},
                    params={"fields": "documentId,title,revisionId,body.content"},
                )
                self._check_drive(response)
                body = _json_object(response)
                try:
                    paragraphs, foreign = _doc_body(body.get("body", {}))
                    return DocRead(
                        revision_id=str(body["revisionId"]),
                        title=str(body.get("title", "")),
                        paragraphs=paragraphs,
                        foreign=foreign,
                    )
                except (KeyError, TypeError) as err:
                    raise GoogleError("unavailable") from err
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)

    async def replace_doc_section(
        self,
        email: str,
        file_id: str,
        required_revision_id: str,
        insert_index: int,
        insert_text: str,
        delete_start: int,
        delete_end: int,
        style_start: int = 0,
        style_end: int = 0,
    ) -> str:
        token = await self._drive_token(email)

        def call() -> str:
            session = self._drive_session_for_call()
            try:
                # Insert the new text first at the section's first content paragraph, so the
                # new paragraphs take that paragraph's NORMAL style, then delete the old range
                # at its shifted position (the caller passes the shifted indices). An empty
                # section has no content paragraph to inherit from, so the inserted range is
                # normalised to NORMAL_TEXT and its bullets cleared. The writeControl locks the
                # batchUpdate to the read revision: a Doc that moved answers 400
                # FAILED_PRECONDITION and nothing is written.
                requests: list[dict] = [
                    {"insertText": {"location": {"index": insert_index}, "text": insert_text}}
                ]
                if delete_end > delete_start:
                    requests.append(
                        {
                            "deleteContentRange": {
                                "range": {"startIndex": delete_start, "endIndex": delete_end}
                            }
                        }
                    )
                if style_end > style_start:
                    style_range = {"startIndex": style_start, "endIndex": style_end}
                    requests.append(
                        {
                            "updateParagraphStyle": {
                                "range": style_range,
                                "paragraphStyle": {"namedStyleType": "NORMAL_TEXT"},
                                "fields": "namedStyleType",
                            }
                        }
                    )
                    requests.append({"deleteParagraphBullets": {"range": style_range}})
                response = self._send(
                    session,
                    "POST",
                    f"{DOCS}/documents/{quote(file_id, safe='')}:batchUpdate",
                    headers={"Authorization": f"Bearer {token}"},
                    json={
                        "requests": requests,
                        "writeControl": {"requiredRevisionId": required_revision_id},
                    },
                )
                if response.status_code == 400 and _is_revision_mismatch(response):
                    raise GoogleError(
                        "revision_mismatch",
                        "The document changed since this write was drafted.",
                        status=400,
                    )
                self._check_drive(response)
                _json_object(response)
                return file_id
            finally:
                if self._drive_session is None:
                    session.close()

        return await asyncio.to_thread(call)


def _is_revision_mismatch(response: requests.Response) -> bool:
    """A Docs batchUpdate 400 whose body says the required revision no longer holds
    (``FAILED_PRECONDITION`` / ``failedPrecondition``), as Google answers a moved Doc."""
    try:
        if response.json()["error"]["status"] == "FAILED_PRECONDITION":
            return True
    except (ValueError, KeyError, TypeError):
        pass
    return _drive_reason(response) == "failedPrecondition"


def _structural_text(content: list) -> str:
    """The text of a list of structural elements: paragraph runs joined within a paragraph,
    table cells and tables of contents walked, one line per paragraph."""
    lines: list[str] = []
    for element in content:
        if "paragraph" in element:
            runs = element["paragraph"].get("elements", [])
            lines.append(
                "".join(r.get("textRun", {}).get("content", "") for r in runs).rstrip("\n")
            )
        elif "table" in element:
            for row in element["table"].get("tableRows", []):
                for cell in row.get("tableCells", []):
                    lines.append(_structural_text(cell.get("content", [])))
        elif "tableOfContents" in element:
            lines.append(_structural_text(element["tableOfContents"].get("content", [])))
    return "\n".join(lines)


def _doc_segment_texts(tabs: list) -> list[str]:
    """The text of the body, every header, every footer and every footnote of each tab
    (child tabs included), each as one string."""
    texts: list[str] = []
    for tab in tabs:
        doc_tab = tab.get("documentTab", {})
        texts.append(_structural_text(doc_tab.get("body", {}).get("content", [])))
        for key in ("headers", "footers", "footnotes"):
            for segment in doc_tab.get(key, {}).values():
                texts.append(_structural_text(segment.get("content", [])))
        texts.extend(_doc_segment_texts(tab.get("childTabs", [])))
    return texts


def _doc_body(body: dict) -> tuple[tuple[DocParagraph, ...], tuple[tuple[int, int], ...]]:
    """A Doc body's structural elements as paragraphs and foreign ranges: each paragraph's
    text (its text runs joined), its named style and its UTF-16 start/end index; a
    non-paragraph element (a table, a section break) contributes its index range to
    ``foreign``, and an inline image run contributes its range, so a section overlapping one
    is refused as not plain paragraphs (D-50). A non-text run contributes no text."""
    paragraphs: list[DocParagraph] = []
    foreign: list[tuple[int, int]] = []
    for element in body.get("content", []):
        paragraph = element.get("paragraph")
        if paragraph is None:
            foreign.append((int(element["startIndex"]), int(element["endIndex"])))
            continue
        text = ""
        for run in paragraph.get("elements", []):
            if "textRun" in run:
                text += run["textRun"].get("content", "")
            elif "inlineObjectElement" in run:
                foreign.append((int(run["startIndex"]), int(run["endIndex"])))
        style = paragraph.get("paragraphStyle", {}).get("namedStyleType", "NORMAL_TEXT")
        paragraphs.append(
            DocParagraph(
                text=text,
                style=style,
                start_index=int(element["startIndex"]),
                end_index=int(element["endIndex"]),
            )
        )
    return tuple(paragraphs), tuple(foreign)


def _split_doc_paragraphs(text: str) -> list[dict]:
    """Inserted text as the paragraphs a Docs insertText makes: one per line, each ending
    in a newline, normal text (the heading is kept separately). A trailing newline does not
    make an empty paragraph; an interior blank line does."""
    pieces = text.split("\n")
    if pieces and pieces[-1] == "":
        pieces.pop()
    return [{"text": piece + "\n", "style": "NORMAL_TEXT"} for piece in pieces]


def _u16_slice(text: str, start: int, end: int | None = None) -> str:
    """A substring by UTF-16 code-unit offset, the unit Google Docs indexes by."""
    units = text.encode("utf-16-le")
    if end is None:
        end = len(units) // 2
    return units[start * 2 : end * 2].decode("utf-16-le")


def _fake_doc_index(paragraphs: list[dict]):
    """The fake Doc body's index model: each entry's ``(start, end)`` bound, the start index
    of every table, the foreign ranges (tables and inline-image paragraphs) and the body's
    end. A table entry (``kind == "table"``) occupies ``size`` index units and is not a
    paragraph; an ``image`` paragraph carries an inline image (D-50)."""
    bounds: list[tuple[int, int]] = []
    table_starts: list[int] = []
    foreign: list[tuple[int, int]] = []
    idx = 0
    for p in paragraphs:
        if p.get("kind") == "table":
            size = int(p.get("size", 1))
            table_starts.append(idx)
            foreign.append((idx, idx + size))
            bounds.append((idx, idx + size))
            idx += size
        else:
            end = idx + utf16_len(p["text"])
            if p.get("image"):
                foreign.append((idx, end))
            bounds.append((idx, end))
            idx = end
    return bounds, table_starts, foreign, idx


def _fake_insert(paragraphs: list[dict], insert_index: int, insert_text: str) -> list[dict]:
    """Google's insertText: the inserted text takes the paragraph style at the insertion
    index (lists included); a newline starts a new paragraph with that style. The text before
    the first newline joins the paragraph at the index, the text after the last newline joins
    its remainder. Inserting at a table's start index is refused (D-50)."""
    bounds, table_starts, _, _ = _fake_doc_index(paragraphs)
    if insert_index in table_starts:
        raise GoogleError("refused", "Cannot insert text at the start of a table.", status=400)
    target = next((i for i, (s, e) in enumerate(bounds) if s <= insert_index < e), None)
    if target is None:
        raise GoogleError("refused", "Cannot insert text at that index.", status=400)
    p = paragraphs[target]
    start, _ = bounds[target]
    offset = insert_index - start
    head = _u16_slice(p["text"], 0, offset)
    tail = _u16_slice(p["text"], offset)
    style, bullet = p.get("style", "NORMAL_TEXT"), p.get("bullet", False)
    lines = insert_text.split("\n")
    if len(lines) == 1:
        pieces = [{"text": head + lines[0] + tail, "style": style, "bullet": bullet}]
    else:
        pieces = [{"text": head + lines[0] + "\n", "style": style, "bullet": bullet}]
        pieces += [{"text": line + "\n", "style": style, "bullet": bullet} for line in lines[1:-1]]
        pieces.append({"text": lines[-1] + tail, "style": style, "bullet": bullet})
    return paragraphs[:target] + pieces + paragraphs[target + 1 :]


def _fake_delete(paragraphs: list[dict], delete_start: int, delete_end: int) -> list[dict]:
    """Google's deleteContentRange: remove ``[delete_start, delete_end)`` from the body,
    merging paragraphs across the range. The body's final newline cannot be deleted, so a
    range reaching the body end is refused (D-50)."""
    _, _, _, body_end = _fake_doc_index(paragraphs)
    if delete_end >= body_end:
        raise GoogleError("refused", "Cannot delete the document's final newline.", status=400)
    text_paras = [p for p in paragraphs if p.get("kind") != "table"]
    units = "".join(p["text"] for p in text_paras).encode("utf-16-le")
    styles: list[str] = []
    for p in text_paras:
        styles.extend([p.get("style", "NORMAL_TEXT")] * utf16_len(p["text"]))
    new_units = units[: delete_start * 2] + units[delete_end * 2 :]
    new_styles = styles[:delete_start] + styles[delete_end:]
    result: list[dict] = []
    buf = bytearray()
    para_style = "NORMAL_TEXT"
    have_style = False
    for cu in range(len(new_units) // 2):
        pair = new_units[cu * 2 : cu * 2 + 2]
        if not have_style:
            para_style = new_styles[cu]
            have_style = True
        buf.extend(pair)
        if pair.decode("utf-16-le") == "\n":
            result.append({"text": bytes(buf).decode("utf-16-le"), "style": para_style})
            buf = bytearray()
            have_style = False
    if buf:
        result.append({"text": bytes(buf).decode("utf-16-le"), "style": para_style})
    return result


def _fake_restyle(
    paragraphs: list[dict], style_start: int, style_end: int, *, clear_bullets: bool
) -> list[dict]:
    """Google's updateParagraphStyle (NORMAL_TEXT) and deleteParagraphBullets over a range:
    every paragraph the range touches takes the named style, and its bullets are cleared."""
    bounds, _, _, _ = _fake_doc_index(paragraphs)
    result: list[dict] = []
    for p, (s, e) in zip(paragraphs, bounds):
        if p.get("kind") != "table" and s < style_end and e > style_start:
            p = {**p, "style": "NORMAL_TEXT"}
            if clear_bullets:
                p["bullet"] = False
        result.append(p)
    return result


def _trim_trailing_empty(values: list[str]) -> list[str]:
    end = len(values)
    while end and values[end - 1] == "":
        end -= 1
    return values[:end]


def _tab_range(tab: str, header_only: bool) -> str:
    """The A1 range of the granted tab — row 1 alone for a header check. The tab name is
    quoted (it may hold spaces) and its own single quotes doubled, as A1 notation asks."""
    quoted = "'" + tab.replace("'", "''") + "'"
    return f"{quoted}!1:1" if header_only else quoted


def _sheet_tab(sheet_id: int, rows: list[dict]) -> SheetTab:
    """The tab's header (first row, trailing empty cells trimmed) and its count of data
    rows: the index after the last row holding any value, where ``appendCells`` appends."""
    grid = [
        [(cell or {}).get("formattedValue", "") for cell in (row.get("values") or [])]
        for row in rows
    ]
    header = tuple(_trim_trailing_empty(grid[0])) if grid else ()
    data_rows = 0
    for i, row in enumerate(grid):
        if any(value != "" for value in row):
            data_rows = i + 1
    return SheetTab(sheet_id, header, data_rows)


def _picker_item(file: dict) -> DriveItem:
    """One Drive file as a picker row: the kind the grant picker acts on, its link and
    the shared drive it sits in. Both listings (the top level and one folder) map here."""
    link = file.get("webViewLink") or f"https://drive.google.com/file/d/{file['id']}"
    mime = file.get("mimeType")
    kind = "folder" if mime == MIME_FOLDER else "sheet" if mime == MIME_SHEET else "file"
    return DriveItem(
        file["id"],
        file["name"],
        link,
        kind,
        mime_type=mime or "",
        drive_id=file.get("driveId") or "",
    )


def _is_drive_root(file: DriveFile) -> bool:
    """A shared drive's root: its ``driveId`` is its own ID."""
    return bool(file.drive_id) and file.drive_id == file.id


def _drive_file(file: dict) -> DriveFile:
    return DriveFile(
        id=file["id"],
        name=file["name"],
        mime_type=file["mimeType"],
        version=str(file.get("version", "")),
        web_view_link=file.get("webViewLink", ""),
        trashed=bool(file.get("trashed", False)),
        parents=tuple(file.get("parents") or ()),
        drive_id=file.get("driveId", ""),
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
        self._fail_after_write = {
            "create": 0,
            "delete": 0,
            "comment": 0,
            "append": 0,
            "copy": 0,
            "fill": 0,
            "replace": 0,
        }
        self._hang_after_write: dict[str, list[Hang]] = {
            "create": [],
            "delete": [],
            "comment": [],
            "append": [],
            "copy": [],
            "replace": [],
        }
        self._visibility_delay = 0
        self._hidden_comments: dict[str, int] = {}
        self._hidden_metadata: dict[int, int] = {}
        self._hidden_copies: dict[str, int] = {}
        self._sheet_read_hangs: list[Hang] = []
        self._list_error: Exception | None = None
        self._delete_error: Exception | None = None
        # The Drive tree the read surface serves: metadata by ID, a folder's child IDs,
        # and each exportable file's text sections. ``get_calls``, ``export_calls`` and
        # ``list_calls`` record the (email, ID) production asked with, so a test can see
        # an unchanged file was not re-exported and whose account read. The failure
        # injections live here, not monkeypatched per test.
        self.drive_files: dict[str, DriveFile] = {}
        self.drive_children: dict[str, list[str]] = {}
        self.drive_content: dict[str, list[DriveSection]] = {}
        # The text of a Doc's headers and footers, which the plain-text export leaves out:
        # the structural read returns it, a copy carries it and the fill replaces in it.
        self.drive_segments: dict[str, list[str]] = {}
        # False models a fill that does not reach headers and footers, so the post-fill
        # check has something to catch.
        self.fill_reaches_segments = True
        self.drive_refused: set[str] = set()
        # The comments each file carries, as Google returns them (id, content, author).
        # ``comment_calls`` and ``comment_list_calls`` record the (email, file ID) of every
        # create and list, so a test can see whose account wrote and that a lookup ran.
        self.drive_comments: dict[str, list[dict]] = {}
        self.comment_calls: list[tuple[str, str]] = []
        self.comment_list_calls: list[tuple[str, str]] = []
        # The Sheet tabs each file carries: file ID -> tab name -> its numeric sheet ID and
        # its rows (row 0 the header). ``sheet_metadata`` holds the developer metadata as
        # Google returns it; ``append_calls``, ``sheet_read_calls`` and ``metadata_search_calls``
        # record the (email, file ID) of every call, so a test sees whose account wrote and
        # that a lookup ran. The append failure injections live in ``_after_write``.
        self.sheets: dict[str, dict[str, dict]] = {}
        self.sheet_metadata: dict[str, list[dict]] = {}
        self.append_calls: list[tuple[str, str]] = []
        self.sheet_read_calls: list[tuple[str, str]] = []
        self.metadata_search_calls: list[tuple[str, str]] = []
        self._next_sheet_id = 1
        self._next_metadata_id = 1
        # The create-from-template surface (D-49): each file's appProperties, and the
        # (email, ID) of every copy, appProperty search and fill, so a test sees whose account
        # wrote and that a lookup ran. A copied Doc's text lives in ``drive_content`` like any
        # Doc's; ``fill_doc`` applies the ordered literal replacements there. The copy and
        # fill failure injections live in ``_after_write``.
        self.drive_app_properties: dict[str, dict[str, str]] = {}
        self.copy_calls: list[tuple[str, str]] = []
        self.app_search_calls: list[tuple[str, str]] = []
        self.fill_calls: list[tuple[str, str]] = []
        # The section-replace surface (D-50): each Doc's structural model — its opaque
        # revisionId and its body entries (paragraphs of text and named style, a table entry
        # ``{kind: "table", size}``, an inline-image paragraph ``{image: True}``) — and the
        # (email, ID) of every documents.get and section batchUpdate, so a test sees whose
        # account read and wrote. The replace follows Google's structural rules (insert copies
        # the style at the index, the body's final newline cannot be deleted, a table's start
        # index refuses an insert). The replace failure injections live in ``_after_write``.
        self.docs: dict[str, dict] = {}
        self.doc_read_calls: list[tuple[str, str]] = []
        self.template_read_calls: list[tuple[str, str]] = []
        self.doc_replace_calls: list[tuple[str, str]] = []
        self.export_calls: list[tuple[str, str]] = []
        self.get_calls: list[tuple[str, str]] = []
        self.list_calls: list[tuple[str, str]] = []
        self._export_error: Exception | None = None
        self._get_error: Exception | None = None
        self._children_error: Exception | None = None
        # A file whose export always fails (a lasting refusal), for the retry-then-count path.
        self.drive_export_errors: dict[str, Exception] = {}
        # A folder whose listing always fails (a lasting refusal), for the incomplete-walk path.
        self.drive_children_errors: dict[str, Exception] = {}
        # A file whose get always fails (a lasting refusal), for the unreadable-root path.
        self.drive_get_errors: dict[str, Exception] = {}

    def fail_after_write(self, kind: str = "create", times: int = 1) -> None:
        """The next ``times`` writes of ``kind`` ("create", "delete", "comment", "append",
        "copy", "fill" or "replace") take effect and then raise, as when the response is
        lost."""
        self._fail_after_write[kind] = times

    def hang_after_write(self, kind: str = "create") -> Hang:
        """The next write of ``kind`` ("create", "delete", "comment", "append", "copy" or
        "replace") takes effect, then waits until the returned ``Hang`` is released. Another
        attempt meanwhile runs normally: this is how two overlap."""
        hang = Hang()
        self._hang_after_write[kind].append(hang)
        return hang

    def delay_visibility(self, calls: int) -> None:
        """A comment, appended row or copied Doc created from now on is left out of the next
        ``calls`` comment lists, metadata searches or appProperty searches, as when Google's
        search lags a write."""
        self._visibility_delay = calls

    def hang_next_sheet_read(self) -> Hang:
        """The next ``read_sheet`` waits until the returned ``Hang`` is released, as a slow
        Sheets read does. Other reads run normally."""
        hang = Hang()
        self._sheet_read_hangs.append(hang)
        return hang

    @staticmethod
    def _tick(hidden: dict[str | int, int]) -> None:
        for key in list(hidden):
            hidden[key] -= 1
            if hidden[key] <= 0:
                del hidden[key]

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
        hangs = self._hang_after_write.get(kind)
        if hangs:
            await hangs.pop(0).wait()

    async def list_shared(self, email: str) -> DriveListing:
        if self._list_error is not None:
            error, self._list_error = self._list_error, None
            raise error
        shared = self.shared.get(email, [])
        return DriveListing(list(shared[:LIST_CAP]), len(shared) > LIST_CAP)

    # --- the Drive tree, for the read surface -----------------------------------------

    def add_drive_folder(
        self, folder_id: str, name: str, parent: str | None = None, drive_id: str = ""
    ) -> None:
        # A node under a shared-drive root inherits its driveId, as Google reports it.
        if not drive_id and parent is not None and parent in self.drive_files:
            drive_id = self.drive_files[parent].drive_id
        self.drive_files[folder_id] = DriveFile(
            folder_id,
            name,
            MIME_FOLDER,
            web_view_link=f"https://drive.google.com/drive/folders/{folder_id}",
            parents=(parent,) if parent is not None else (),
            drive_id=drive_id,
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
        drive_id: str = "",
    ) -> None:
        if not drive_id and parent in self.drive_files:
            drive_id = self.drive_files[parent].drive_id
        self.drive_files[file_id] = DriveFile(
            file_id,
            name,
            mime_type,
            version=version,
            web_view_link=f"https://drive.google.com/file/d/{file_id}",
            parents=(parent,),
            drive_id=drive_id,
        )
        self.drive_content[file_id] = list(sections)
        self.drive_children.setdefault(parent, []).append(file_id)

    def set_drive_version(self, file_id: str, version: str) -> None:
        self.drive_files[file_id] = replace(self.drive_files[file_id], version=version)

    def set_drive_content(self, file_id: str, sections: list[DriveSection]) -> None:
        self.drive_content[file_id] = list(sections)

    def set_drive_segments(self, file_id: str, texts: list[str]) -> None:
        """A Doc's header and footer texts, as ``documents.get`` returns them."""
        self.drive_segments[file_id] = list(texts)

    def trash_drive_file(self, file_id: str) -> None:
        self.drive_files[file_id] = replace(self.drive_files[file_id], trashed=True)

    def remove_drive_child(self, parent: str, child_id: str) -> None:
        self.drive_children[parent] = [c for c in self.drive_children[parent] if c != child_id]
        child = self.drive_files.get(child_id)
        if child is not None:
            self.drive_files[child_id] = replace(
                child, parents=tuple(p for p in child.parents if p != parent)
            )

    def move_drive_child(self, from_parent: str, to_parent: str, child_id: str) -> None:
        """Move a child to another folder: sharing follows the file, its parents change,
        and its driveId becomes the new folder's, as Google reports it (empty in My Drive)."""
        self.remove_drive_child(from_parent, child_id)
        self.drive_children.setdefault(to_parent, []).append(child_id)
        child = self.drive_files[child_id]
        parent = self.drive_files.get(to_parent)
        self.drive_files[child_id] = replace(
            child, parents=(*child.parents, to_parent), drive_id=parent.drive_id if parent else ""
        )

    def add_drive_comment(self, file_id: str, content: str, mine: bool = True) -> str:
        """A comment already on a file, as Google would return it; its ID comes back.
        ``mine=False`` is another author's comment (the marker lookup must not count it)."""
        comment = {"id": str(uuid.uuid4()), "content": content, "author": {"me": mine}}
        self.drive_comments.setdefault(file_id, []).append(comment)
        return comment["id"]

    def add_sheet_tab(self, file_id: str, tab: str, header: list[str], rows=()) -> int:
        """A tab of a Sheet as Google would serve it: its header row and any data rows.
        Returns the tab's numeric sheet ID. The file itself is added with add_drive_file."""
        sheet_id = self._next_sheet_id
        self._next_sheet_id += 1
        self.sheets.setdefault(file_id, {})[tab] = {
            "sheet_id": sheet_id,
            "rows": [list(header), *[list(r) for r in rows]],
        }
        return sheet_id

    def set_sheet_header(self, file_id: str, tab: str, header: list[str]) -> None:
        """Replace a tab's header row, as a creator editing the Sheet's columns would."""
        self.sheets[file_id][tab]["rows"][0] = list(header)

    def add_drive_doc(
        self,
        file_id: str,
        name: str,
        paragraphs: list[dict],
        parent: str,
        revision_id: str = "1",
        drive_id: str = "",
    ) -> None:
        """A Google Doc as the section write reads it: a Drive file (indexed like any Doc)
        plus its structural model — a revisionId and body entries: paragraphs of
        ``{text, style}`` (each text ending in a newline), a table ``{kind: "table", size}``
        occupying ``size`` index units, or an inline-image paragraph ``{image: True}``. The
        export text is the paragraphs joined (a table contributes no text)."""
        text = "".join(p["text"] for p in paragraphs if p.get("kind") != "table")
        self.add_drive_file(
            file_id,
            name,
            MIME_DOC,
            parent,
            [DriveSection("", text)],
            version=revision_id,
            drive_id=drive_id,
        )
        self.docs[file_id] = {
            "revision_id": revision_id,
            "paragraphs": [dict(p) for p in paragraphs],
        }

    def set_doc_revision(self, file_id: str, revision_id: str) -> None:
        """Move a Doc to another revision, as a collaborator's edit would (the write's
        requiredRevisionId no longer holds)."""
        self.docs[file_id]["revision_id"] = revision_id
        self.set_drive_version(file_id, revision_id)

    def fail_next_export(self, error: Exception) -> None:
        self._export_error = error

    def fail_export_for(self, file_id: str, error: Exception) -> None:
        """Make every export of ``file_id`` raise ``error`` (a lasting refusal)."""
        self.drive_export_errors[file_id] = error

    def fail_children_for(self, folder_id: str, error: Exception) -> None:
        """Make every listing of ``folder_id`` raise ``error`` (a lasting refusal)."""
        self.drive_children_errors[folder_id] = error

    def fail_get_for(self, file_id: str, error: Exception) -> None:
        """Make every get of ``file_id`` raise ``error`` (a lasting refusal)."""
        self.drive_get_errors[file_id] = error

    def fail_next_get(self, error: Exception) -> None:
        self._get_error = error

    def fail_next_children(self, error: Exception) -> None:
        self._children_error = error

    async def list_children(self, email: str, folder_id: str) -> list[DriveFile]:
        self.list_calls.append((email, folder_id))
        if self._children_error is not None:
            error, self._children_error = self._children_error, None
            raise error
        if folder_id in self.drive_children_errors:
            raise self.drive_children_errors[folder_id]
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

    async def list_folder(self, email: str, folder_id: str) -> DriveListing:
        # The same two paths as the real client: a shared drive's root through
        # ``get_drive``, a folder through ``get_file``; either refuses an ID the account
        # cannot see.
        try:
            await self.get_drive(email, folder_id)
        except GoogleError as err:
            if err.status != 404:
                raise
            await self.get_file(email, folder_id)
        children = await self.list_children(email, folder_id)
        children.sort(key=lambda f: (f.mime_type != MIME_FOLDER, f.name))
        items = [
            DriveItem(
                f.id,
                f.name,
                f.web_view_link,
                "folder"
                if f.mime_type == MIME_FOLDER
                else "sheet"
                if f.mime_type == MIME_SHEET
                else "file",
                mime_type=f.mime_type,
                drive_id=f.drive_id,
            )
            for f in children
        ]
        return DriveListing(items[:LIST_CAP], len(items) > LIST_CAP)

    async def get_file(self, email: str, file_id: str) -> DriveFile:
        self.get_calls.append((email, file_id))
        if self._get_error is not None:
            error, self._get_error = self._get_error, None
            raise error
        if file_id in self.drive_get_errors:
            raise self.drive_get_errors[file_id]
        if file_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        file = self.drive_files.get(file_id)
        # Google answers ``files.get`` for a shared drive's ID with a 404: a drive is not a
        # file; ``drives.get`` is how it is read.
        if file is None or _is_drive_root(file):
            raise GoogleError("not_found", status=404)
        return file

    async def export_file(self, email: str, file_id: str, mime_type: str) -> list[DriveSection]:
        self.export_calls.append((email, file_id))
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

    async def get_drive(self, email: str, drive_id: str) -> DriveFile:
        self.get_calls.append((email, drive_id))
        if drive_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        drive = self.drive_files.get(drive_id)
        if drive is None or not _is_drive_root(drive):
            raise GoogleError("not_found", status=404)
        return DriveFile(
            drive.id,
            drive.name,
            MIME_FOLDER,
            web_view_link=f"https://drive.google.com/drive/folders/{drive.id}",
        )

    async def list_drive_children(self, email: str, drive_id: str) -> list[DriveFile]:
        return await self.list_children(email, drive_id)

    async def list_comments(self, email: str, file_id: str) -> list[DriveComment]:
        self.comment_list_calls.append((email, file_id))
        if file_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        if file_id not in self.drive_files:
            raise GoogleError("not_found", status=404)
        found = [
            DriveComment(c["id"], c["content"], bool((c.get("author") or {}).get("me", False)))
            for c in self.drive_comments.get(file_id, [])
            if c["id"] not in self._hidden_comments
        ]
        self._tick(self._hidden_comments)
        return found

    async def create_comment(self, email: str, file_id: str, content: str) -> str:
        self.comment_calls.append((email, file_id))
        if file_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        if file_id not in self.drive_files:
            raise GoogleError("not_found", status=404)
        comment_id = self.add_drive_comment(file_id, content)
        if self._visibility_delay:
            self._hidden_comments[comment_id] = self._visibility_delay
        await self._after_write("comment")
        return comment_id

    async def read_sheet(
        self, email: str, file_id: str, tab: str, header_only: bool = False
    ) -> SheetTab:
        self.sheet_read_calls.append((email, file_id))
        if self._sheet_read_hangs:
            await self._sheet_read_hangs.pop(0).wait()
        if file_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        if file_id not in self.drive_files:
            raise GoogleError("not_found", status=404)
        state = self.sheets.get(file_id, {}).get(tab)
        if state is None:
            raise GoogleError("refused", f"The Sheet has no tab named '{tab}'.", status=400)
        # Google answers a ranges read with the asked rows alone: row 1 for a header check.
        rows = state["rows"][:1] if header_only else state["rows"]
        header = tuple(_trim_trailing_empty(rows[0])) if rows else ()
        data_rows = 0
        for i, row in enumerate(rows):
            if any(value != "" for value in row):
                data_rows = i + 1
        return SheetTab(state["sheet_id"], header, data_rows)

    async def list_sheet_tabs(self, email: str, file_id: str) -> list[str]:
        if file_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        if file_id not in self.drive_files:
            raise GoogleError("not_found", status=404)
        return list(self.sheets.get(file_id, {}))

    async def search_sheet_metadata(
        self, email: str, file_id: str, key: str, value: str
    ) -> list[SheetMetadata]:
        self.metadata_search_calls.append((email, file_id))
        if file_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        if file_id not in self.drive_files:
            raise GoogleError("not_found", status=404)
        found = [
            SheetMetadata(
                m["metadataId"],
                m["metadataKey"],
                m["metadataValue"],
                m["location"]["dimensionRange"]["startIndex"],
            )
            for m in self.sheet_metadata.get(file_id, [])
            if m["metadataId"] not in self._hidden_metadata
            and m["metadataKey"] == key
            and m["metadataValue"] == value
        ]
        self._tick(self._hidden_metadata)
        return found

    async def append_sheet_row(
        self,
        email: str,
        file_id: str,
        sheet_id: int,
        row: list[str | int | float],
        row_index: int,
        key: str,
        value: str,
    ) -> str:
        self.append_calls.append((email, file_id))
        if file_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        if file_id not in self.drive_files:
            raise GoogleError("not_found", status=404)
        tab = next(
            (t for t, s in self.sheets.get(file_id, {}).items() if s["sheet_id"] == sheet_id),
            None,
        )
        if tab is None:
            raise GoogleError("refused", "The Sheet has no tab with that ID.", status=400)
        rows = self.sheets[file_id][tab]["rows"]
        # appendCells adds after the last row with data, inserting rows if necessary — it
        # never overwrites a data row, whatever index the read before the write computed.
        end = 0
        for i, existing in enumerate(rows):
            if any(value != "" for value in existing):
                end = i + 1
        while len(rows) < end:
            rows.append([])
        if len(rows) == end:
            rows.append(list(row))
        else:
            rows[end] = list(row)
        metadata_id = self._next_metadata_id
        self._next_metadata_id += 1
        self.sheet_metadata.setdefault(file_id, []).append(
            {
                "metadataId": metadata_id,
                "metadataKey": key,
                "metadataValue": value,
                "location": {
                    "dimensionRange": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": row_index,
                        "endIndex": row_index + 1,
                    }
                },
            }
        )
        if self._visibility_delay:
            self._hidden_metadata[metadata_id] = self._visibility_delay
        await self._after_write("append")
        return str(metadata_id)

    async def copy_file(
        self, email: str, template_id: str, folder_id: str, title: str, key: str, value: str
    ) -> DriveFile:
        self.copy_calls.append((email, template_id))
        if template_id in self.drive_refused or folder_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        if template_id not in self.drive_files or folder_id not in self.drive_files:
            raise GoogleError("not_found", status=404)
        folder = self.drive_files[folder_id]
        # The pilot's service account cannot own files: a copy into My Drive is refused,
        # as Google refuses it for an account that can only write inside shared drives.
        if not folder.drive_id:
            raise GoogleError(
                "invalid_destination",
                "New Docs can only be created in a shared drive folder",
                status=400,
            )
        new_id = f"new-doc-{uuid.uuid4().hex}"
        copied = DriveFile(
            new_id,
            title,
            self.drive_files[template_id].mime_type,
            version="1",
            web_view_link=f"https://drive.google.com/file/d/{new_id}",
            parents=(folder_id,),
            drive_id=folder.drive_id,
        )
        self.drive_files[new_id] = copied
        self.drive_content[new_id] = list(self.drive_content.get(template_id, []))
        self.drive_segments[new_id] = list(self.drive_segments.get(template_id, []))
        self.drive_children.setdefault(folder_id, []).append(new_id)
        # The marker lands with the copy, as the one files.copy request carries it.
        self.drive_app_properties.setdefault(new_id, {})[key] = value
        if self._visibility_delay:
            self._hidden_copies[new_id] = self._visibility_delay
        await self._after_write("copy")
        return copied

    async def search_app_properties(
        self, email: str, folder_id: str, drive_id: str, key: str, value: str
    ) -> list[DriveFile]:
        self.app_search_calls.append((email, folder_id))
        if folder_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        if folder_id not in self.drive_files:
            raise GoogleError("not_found", status=404)
        found = [
            self.drive_files[c]
            for c in self.drive_children.get(folder_id, [])
            if c in self.drive_files
            and not self.drive_files[c].trashed
            and c not in self._hidden_copies
            and self.drive_app_properties.get(c, {}).get(key) == value
        ]
        self._tick(self._hidden_copies)
        return found

    async def fill_doc(self, email: str, file_id: str, replacements: list[tuple[str, str]]) -> None:
        self.fill_calls.append((email, file_id))
        if file_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        if file_id not in self.drive_files:
            raise GoogleError("not_found", status=404)
        filled = []
        for section in self.drive_content.get(file_id, []):
            text = section.text
            for old, new in replacements:
                text = text.replace(old, new)
            filled.append(DriveSection(section.heading, text))
        self.drive_content[file_id] = filled
        segments = []
        for text in self.drive_segments.get(file_id, []):
            for old, new in replacements if self.fill_reaches_segments else []:
                text = text.replace(old, new)
            segments.append(text)
        self.drive_segments[file_id] = segments
        await self._after_write("fill")

    async def read_template_text(self, email: str, file_id: str) -> str:
        self.template_read_calls.append((email, file_id))
        if file_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        if file_id not in self.drive_files:
            raise GoogleError("not_found", status=404)
        body = [s.text for s in self.drive_content.get(file_id, [])]
        return "\n".join(body + self.drive_segments.get(file_id, []))

    async def read_doc(self, email: str, file_id: str) -> DocRead:
        self.doc_read_calls.append((email, file_id))
        if file_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        if file_id not in self.drive_files:
            raise GoogleError("not_found", status=404)
        doc = self.docs.get(file_id)
        if doc is None:
            # A Doc added without a structural model: its export text as normal paragraphs.
            text = "\n".join(s.text for s in self.drive_content.get(file_id, []))
            paragraphs = _split_doc_paragraphs(text + "\n")
            revision_id = self.drive_files[file_id].version or "1"
        else:
            paragraphs = doc["paragraphs"]
            revision_id = doc["revision_id"]
        bounds, _, foreign, _ = _fake_doc_index(paragraphs)
        indexed = [
            DocParagraph(p["text"], p.get("style", "NORMAL_TEXT"), s, e)
            for p, (s, e) in zip(paragraphs, bounds)
            if p.get("kind") != "table"
        ]
        return DocRead(revision_id, self.drive_files[file_id].name, tuple(indexed), tuple(foreign))

    async def replace_doc_section(
        self,
        email: str,
        file_id: str,
        required_revision_id: str,
        insert_index: int,
        insert_text: str,
        delete_start: int,
        delete_end: int,
        style_start: int = 0,
        style_end: int = 0,
    ) -> str:
        self.doc_replace_calls.append((email, file_id))
        if file_id in self.drive_refused:
            raise GoogleError("permission_denied", DRIVE_REFUSED_TEXT, status=403)
        if file_id not in self.drive_files:
            raise GoogleError("not_found", status=404)
        doc = self.docs.get(file_id)
        if doc is None:
            raise GoogleError("refused", "The Doc has no readable body.", status=400)
        # The revision is the lock: a Doc that moved since the read refuses, nothing written.
        if required_revision_id != doc["revision_id"]:
            raise GoogleError(
                "revision_mismatch",
                "The document changed since this write was drafted.",
                status=400,
            )
        # Google's structural rules, in request order: insert first (the new paragraphs take
        # the style at the insertion index), then delete the shifted old range (never the
        # body's final newline), then normalise an empty section's inserted range to
        # NORMAL_TEXT and clear its bullets. A table's start index refuses an insert.
        result = _fake_insert(doc["paragraphs"], insert_index, insert_text)
        if delete_end > delete_start:
            result = _fake_delete(result, delete_start, delete_end)
        if style_end > style_start:
            result = _fake_restyle(result, style_start, style_end, clear_bullets=True)
        doc["paragraphs"] = result
        # The revision is opaque, as Google's revisionId is: a write moves it to a fresh token,
        # never a number a caller could predict or compare arithmetically.
        doc["revision_id"] = uuid.uuid4().hex
        self.set_drive_version(file_id, doc["revision_id"])
        self.drive_content[file_id] = [
            DriveSection("", "".join(p["text"] for p in result if p.get("kind") != "table"))
        ]
        await self._after_write("replace")
        return file_id


def accounts_from_settings(
    project_id: str | None, service_account_key: str | None
) -> RealGoogleAccounts | None:
    """The deployment's Google client, or None when either setting is unset: Drive is
    then not set up on this deployment. A malformed key raises ``GoogleError`` here,
    at start-up, never at the first click."""
    if not project_id or not service_account_key:
        return None
    return RealGoogleAccounts(project_id, service_account_key)
