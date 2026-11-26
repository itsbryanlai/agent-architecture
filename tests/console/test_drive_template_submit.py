"""Submitting a version with a Drive template grant reads the template's blanks and the
destination folder's shared drive once and stores them; publishing refuses a destination
folder outside a shared drive (D-49)."""

from dataclasses import replace

import pytest

from agentos import ids
from agentos.config import ConfigError
from agentos.connectors.google_iam import MIME_SHEET, DriveSection, FakeGoogleAccounts
from agentos.drive_connections import InMemoryDriveConnectionStore
from tests.conftest import TENANT
from tests.helpers.agents import helper_block
from tests.helpers.console_harness import OWNER, ROOT, ConsoleHarness
from tests.helpers.drive import (
    DRIVE_TEMPLATE,
    TEMPLATE_DRIVE,
    TEMPLATE_FOLDER,
    TEMPLATE_PLAIN_FOLDER,
    add_template_tree,
)


def template_harness():
    drive = InMemoryDriveConnectionStore()
    google = FakeGoogleAccounts()
    return ConsoleHarness(drive=drive, google=google), drive, google


def template_definition(folder_id: str) -> dict:
    """A draft naming the offer template and its folder, with no blanks: the creator names none."""
    definition = helper_block()
    definition["grants"]["drive"] = {
        "templates": [{"id": DRIVE_TEMPLATE, "name": "Offer template", "folder_id": folder_id}]
    }
    return definition


@pytest.mark.parametrize(
    "case",
    ["reads and stores", "duplicates collapse", "not connected"],
    ids=["stores", "duplicates", "not-connected"],
)
async def test_submit_reads_the_templates_blanks_once_and_stores_them(case):
    """Fails if submit does not read the template's blanks once and store them, keeps a
    duplicate blank name twice, or accepts an unconnected Drive."""
    h, _drive, google = template_harness()
    await h.seed_members()
    if case != "not connected":
        await h.plane.connect_drive(TENANT, ROOT)
    add_template_tree(google)
    if case == "duplicates collapse":
        google.set_drive_content(DRIVE_TEMPLATE, [DriveSection("", "{{name}} {{name}} {{role}}")])
    await h.plane.add_agent(TENANT, OWNER, "it-helper", template_definition(TEMPLATE_FOLDER))

    if case == "not connected":
        with pytest.raises(ConfigError, match="not connected"):
            await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
        record = await h.agents.get_version(TENANT, "it-helper", 1)
        assert record.status == "draft"
        assert record.agent().grants.drive[0].blanks == ()
        return

    await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    [template] = record.agent().grants.drive
    expected = ["name", "role"]
    assert template.blanks == tuple(expected)
    assert template.drive_id == TEMPLATE_DRIVE
    entry = record.definition["grants"]["drive"]["templates"][0]
    assert entry["blanks"] == expected
    assert entry["drive_id"] == TEMPLATE_DRIVE
    email = f"{ids.drive_account_id(TENANT)}@test-project.iam.gserviceaccount.com"
    assert google.template_read_calls == [(email, DRIVE_TEMPLATE)]


@pytest.mark.parametrize(
    ("body", "segments", "expected"),
    [
        ("Dear {{name}}", [], ["name"]),
        ("Dear all", ["Client: {{client}}"], ["client"]),
        ("Dear all", ["Page {{ref}}", "Ref {{ref}}"], ["ref"]),
        ("Dear {{name}}", ["Client: {{client}}", "Ref {{name}}"], ["name", "client"]),
    ],
    ids=["body", "header-or-footer-only", "header-and-footer-once", "header-and-body"],
)
async def test_submit_offers_a_blank_found_in_a_header_or_footer_once(body, segments, expected):
    """Fails if a blank written only in a header or footer is not stored with the template's
    blanks, or one written in several places is stored twice."""
    h, _drive, google = template_harness()
    await h.seed_members()
    await h.plane.connect_drive(TENANT, ROOT)
    add_template_tree(google)
    google.set_drive_content(DRIVE_TEMPLATE, [DriveSection("", body)])
    google.set_drive_segments(DRIVE_TEMPLATE, segments)
    await h.plane.add_agent(TENANT, OWNER, "it-helper", template_definition(TEMPLATE_FOLDER))

    await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)

    record = await h.agents.get_version(TENANT, "it-helper", 1)
    assert record.agent().grants.drive[0].blanks == tuple(expected)


@pytest.mark.parametrize(
    "case",
    ["shared-drive-folder", "plain-folder", "no-drive-recorded"],
)
async def test_publish_refuses_a_folder_outside_a_shared_drive(case):
    """Fails if a destination folder outside a shared drive, or one whose shared drive the
    grant does not record, still publishes, or a folder inside its recorded drive is refused."""
    h, _drive, google = template_harness()
    await h.seed_members()
    await h.plane.connect_drive(TENANT, ROOT)
    add_template_tree(google)
    folder_id = TEMPLATE_FOLDER if case == "shared-drive-folder" else TEMPLATE_PLAIN_FOLDER
    await h.plane.add_agent(TENANT, OWNER, "it-helper", template_definition(folder_id))
    await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
    if case == "no-drive-recorded":
        # The folder moved into a shared drive after the submit read it in My Drive.
        google.drive_files[TEMPLATE_PLAIN_FOLDER] = replace(
            google.drive_files[TEMPLATE_PLAIN_FOLDER], drive_id=TEMPLATE_DRIVE
        )

    if case == "shared-drive-folder":
        record = await h.plane.publish_version(TENANT, ROOT, "it-helper", 1)
        assert record.status == "published"
    else:
        with pytest.raises(ConfigError) as err:
            await h.plane.publish_version(TENANT, ROOT, "it-helper", 1)
        needle = (
            "New Docs can only be created in a shared drive folder"
            if case == "plain-folder"
            else "no shared drive recorded"
        )
        assert needle in str(err.value)
        assert (await h.agents.get_version(TENANT, "it-helper", 1)).status == "pending"


@pytest.mark.parametrize("case", ["a Sheet", "a PDF", "trashed"], ids=["sheet", "pdf", "trashed"])
async def test_submit_refuses_a_template_that_is_not_an_untrashed_google_doc(case):
    """Fails if a template that is not an untrashed Google Doc is submitted instead of refused."""
    h, _drive, google = template_harness()
    await h.seed_members()
    await h.plane.connect_drive(TENANT, ROOT)
    add_template_tree(google)
    if case == "trashed":
        google.trash_drive_file(DRIVE_TEMPLATE)
    else:
        mime = MIME_SHEET if case == "a Sheet" else "application/pdf"
        google.drive_files[DRIVE_TEMPLATE] = replace(
            google.drive_files[DRIVE_TEMPLATE], mime_type=mime
        )
    await h.plane.add_agent(TENANT, OWNER, "it-helper", template_definition(TEMPLATE_FOLDER))

    with pytest.raises(ConfigError, match="must be a Google Doc"):
        await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
    assert (await h.agents.get_version(TENANT, "it-helper", 1)).status == "draft"
