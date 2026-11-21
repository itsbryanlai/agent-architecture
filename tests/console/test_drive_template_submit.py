"""Submitting a version with a Drive template grant reads the template's blanks once and
stores them; publishing refuses a destination folder outside a shared drive (D-49)."""

import pytest

from agentos import ids
from agentos.config import ConfigError
from agentos.connectors.google_iam import DriveSection, FakeGoogleAccounts
from agentos.drive_connections import InMemoryDriveConnectionStore
from tests.conftest import TENANT
from tests.helpers.agents import helper_block
from tests.helpers.console_harness import OWNER, ROOT, ConsoleHarness
from tests.helpers.drive import (
    DRIVE_TEMPLATE,
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
    assert record.definition["grants"]["drive"]["templates"][0]["blanks"] == expected
    email = f"{ids.drive_account_id(TENANT)}@test-project.iam.gserviceaccount.com"
    assert google.export_calls == [(email, DRIVE_TEMPLATE)]


@pytest.mark.parametrize(
    ("folder_id", "published"),
    [(TEMPLATE_FOLDER, True), (TEMPLATE_PLAIN_FOLDER, False)],
    ids=["shared-drive-folder", "plain-folder"],
)
async def test_publish_refuses_a_folder_outside_a_shared_drive(folder_id, published):
    """Fails if a template whose destination folder is outside a shared drive still publishes,
    or a folder inside a shared drive is refused."""
    h, _drive, google = template_harness()
    await h.seed_members()
    await h.plane.connect_drive(TENANT, ROOT)
    add_template_tree(google)
    await h.plane.add_agent(TENANT, OWNER, "it-helper", template_definition(folder_id))
    await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)

    if published:
        record = await h.plane.publish_version(TENANT, ROOT, "it-helper", 1)
        assert record.status == "published"
    else:
        with pytest.raises(ConfigError) as err:
            await h.plane.publish_version(TENANT, ROOT, "it-helper", 1)
        assert "New Docs can only be created in a shared drive folder" in str(err.value)
        assert (await h.agents.get_version(TENANT, "it-helper", 1)).status == "pending"
