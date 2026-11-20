"""Connect Google Drive (D-45): one service account per tenant, created on connect."""

from __future__ import annotations

import asyncio
import re

import pytest

from agentos import ids
from agentos.config import ConfigError
from agentos.connectors.google_iam import DriveItem, FakeGoogleAccounts, GoogleError
from agentos.control import Forbidden
from agentos.drive_connections import CONNECTED, InMemoryDriveConnectionStore
from tests.conftest import TENANT
from tests.helpers.console_harness import OWNER, ROOT, ConsoleHarness, using

SETTINGS = f"/ui/{TENANT}/settings"


def drive_harness() -> tuple[ConsoleHarness, InMemoryDriveConnectionStore, FakeGoogleAccounts]:
    drive = InMemoryDriveConnectionStore()
    google = FakeGoogleAccounts()
    return ConsoleHarness(drive=drive, google=google), drive, google


def tenant_email() -> str:
    """The address Google gives the tenant's account: its ID at the project's domain."""
    return f"{ids.drive_account_id(TENANT)}@test-project.iam.gserviceaccount.com"


@pytest.mark.parametrize(
    "case",
    ["first connect", "second click", "lost response", "overlap", "non-admin", "not set up"],
    ids=["first", "second-click", "lost-response", "overlap", "non-admin", "not-set-up"],
)
async def test_connect_creates_the_tenant_account_once_and_a_retry_finds_it(case):
    """Fails if a second click, a retry after a lost create response or an overlapping
    attempt creates a second account, stores a second row or records a second event, or
    a non-admin or an unconfigured deployment connects."""
    if case == "not set up":
        h = ConsoleHarness()
        with pytest.raises(ConfigError):
            await h.plane.connect_drive(TENANT, ROOT)
        assert h.audit.events == []
        return
    h, drive, google = drive_harness()
    await h.seed_members()
    if case == "non-admin":
        with pytest.raises(Forbidden):
            await h.plane.connect_drive(TENANT, OWNER)
        assert google.accounts == {}
        assert await drive.get(TENANT) is None
        assert h.audit.events == []
        return
    if case == "first connect":
        await h.plane.connect_drive(TENANT, ROOT)
    elif case == "second click":
        await h.plane.connect_drive(TENANT, ROOT)
        await h.plane.connect_drive(TENANT, ROOT)
    elif case == "lost response":
        google.fail_after_write("create")
        with pytest.raises(GoogleError):
            await h.plane.connect_drive(TENANT, ROOT)
        assert await drive.get(TENANT) is None
        assert h.audit.events == []
        await h.plane.connect_drive(TENANT, ROOT)
    elif case == "overlap":
        hang = google.hang_after_write("create")
        first = asyncio.create_task(h.plane.connect_drive(TENANT, ROOT))
        await hang.reached.wait()
        await h.plane.connect_drive(TENANT, ROOT)
        hang.release()
        await first

    account_id = ids.drive_account_id(TENANT)
    email = tenant_email()
    assert re.fullmatch(r"[a-z][a-z0-9-]{5,29}", account_id)
    assert google.created == [account_id]
    assert google.accounts == {account_id: email}
    row = await drive.get(TENANT)
    assert (row.service_account_email, row.status, row.last_error) == (email, CONNECTED, None)
    assert [(e.kind, e.tool, e.target, e.identity) for e in h.audit.events] == [
        ("connector", "drive_connect", email, f"user:{ROOT}")
    ]


@pytest.mark.parametrize(
    "case",
    ["connected", "not connected", "lost delete response", "already gone", "overlap", "non-admin"],
    ids=[
        "connected",
        "not-connected",
        "lost-delete-response",
        "already-gone",
        "overlap",
        "non-admin",
    ],
)
async def test_disconnect_deletes_the_account_and_the_row_once(case):
    """Fails if disconnect leaves the account at Google or the row stored, records a
    second event, refuses when the account is already gone, or lets a non-admin
    disconnect."""
    h, drive, google = drive_harness()
    await h.seed_members()
    account_id = ids.drive_account_id(TENANT)
    email = tenant_email()
    if case != "not connected":
        await h.plane.connect_drive(TENANT, ROOT)
    if case == "non-admin":
        with pytest.raises(Forbidden):
            await h.plane.disconnect_drive(TENANT, OWNER)
        assert google.accounts == {account_id: email}
        assert await drive.get(TENANT) is not None
        assert [(e.kind, e.tool) for e in h.audit.events] == [("connector", "drive_connect")]
        return
    if case == "connected":
        assert await h.plane.disconnect_drive(TENANT, ROOT) is True
    elif case == "not connected":
        assert await h.plane.disconnect_drive(TENANT, ROOT) is False
        assert google.delete_calls == []
        assert h.audit.events == []
        return
    elif case == "lost delete response":
        google.fail_after_write("delete")
        with pytest.raises(GoogleError):
            await h.plane.disconnect_drive(TENANT, ROOT)
        assert await drive.get(TENANT) is not None
        assert await h.plane.disconnect_drive(TENANT, ROOT) is True
    elif case == "already gone":
        assert await google.delete_account(account_id) is True
        assert await h.plane.disconnect_drive(TENANT, ROOT) is True
    elif case == "overlap":
        hang = google.hang_after_write("delete")
        first = asyncio.create_task(h.plane.disconnect_drive(TENANT, ROOT))
        await hang.reached.wait()
        second = await h.plane.disconnect_drive(TENANT, ROOT)
        hang.release()
        assert (await first, second) == (False, True)

    assert google.accounts == {}
    assert await drive.get(TENANT) is None
    assert [(e.kind, e.tool, e.target, e.identity) for e in h.audit.events] == [
        ("connector", "drive_connect", email, f"user:{ROOT}"),
        ("connector", "drive_disconnect", email, f"user:{ROOT}"),
    ]


@pytest.mark.parametrize(
    "case",
    ["not set up", "not connected", "connected", "empty", "error"],
    ids=["not-set-up", "not-connected", "connected", "empty", "error"],
)
async def test_the_settings_page_shows_the_drive_section(case):
    """Fails if the page offers a connect button on a deployment without Drive, hides
    the address, the copy button or the shared list when connected, shows no empty
    state, or shows a raw error code instead of plain words."""
    email = tenant_email()
    if case == "not set up":
        h = ConsoleHarness()
    else:
        h, _drive, google = drive_harness()
        if case != "not connected":
            await h.plane.connect_drive(TENANT, ROOT)
        if case == "connected":
            google.shared[email] = [
                DriveItem("file-1", "IT guidebook", "https://drive.google.com/file/d/file-1")
            ]
        if case == "error":
            google.fail_next_list(GoogleError("permission_denied"))
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(SETTINGS)).text
    assert "Google Drive" in page
    if case == "not set up":
        assert "Google Drive is not set up on this deployment" in page
        assert "Connect Google Drive" not in page
    elif case == "not connected":
        assert "Connect Google Drive" in page
        assert "Copy address" not in page
    else:
        assert email in page
        assert "Copy address" in page
        assert "Disconnect" in page
        if case == "connected":
            assert "IT guidebook" in page
            assert "https://drive.google.com/file/d/file-1" in page
            assert "Status: connected" in page
        elif case == "empty":
            assert "Nothing is shared with this address yet" in page
        elif case == "error":
            assert "Status: error" in page
            assert "Google refused the request" in page
            assert "permission_denied" not in page
