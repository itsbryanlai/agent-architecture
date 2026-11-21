"""Submitting a version with a Drive section grant reads the Doc once and checks the heading
exists exactly once; a missing or duplicated heading refuses in plain words (D-50)."""

import pytest

from agentos.config import ConfigError
from agentos.connectors.google_iam import FakeGoogleAccounts
from agentos.drive_connections import InMemoryDriveConnectionStore
from tests.conftest import TENANT
from tests.helpers.agents import helper_block
from tests.helpers.console_harness import OWNER, ROOT, ConsoleHarness
from tests.helpers.drive import DRIVE_SECTION, SECTION_HEADING, add_section_doc, section_grant_block


def section_harness():
    drive = InMemoryDriveConnectionStore()
    google = FakeGoogleAccounts()
    return ConsoleHarness(drive=drive, google=google), drive, google


def section_definition() -> dict:
    """A draft naming the VPN guide's Status section; the heading is the creator's, typed."""
    definition = helper_block()
    definition["grants"]["drive"] = {"sections": [section_grant_block()]}
    return definition


@pytest.mark.parametrize(
    "case",
    ["heading exists once", "missing heading", "duplicated heading", "not connected"],
    ids=["ok", "missing", "duplicated", "not-connected"],
)
async def test_submit_checks_the_section_heading_exists_exactly_once(case):
    """Fails if submit does not read the Doc once and accept a heading present exactly once,
    or accepts a heading the Doc lacks or has twice, or accepts an unconnected Drive."""
    h, _drive, google = section_harness()
    await h.seed_members()
    if case != "not connected":
        await h.plane.connect_drive(TENANT, ROOT)
    add_section_doc(google)
    if case == "missing heading":
        google.docs[DRIVE_SECTION]["paragraphs"][0]["text"] = "Overview\n"
    elif case == "duplicated heading":
        google.docs[DRIVE_SECTION]["paragraphs"].append(
            {"text": f"{SECTION_HEADING}\n", "style": "HEADING_1"}
        )
    await h.plane.add_agent(TENANT, OWNER, "it-helper", section_definition())

    if case == "not connected":
        with pytest.raises(ConfigError, match="not connected"):
            await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
        assert (await h.agents.get_version(TENANT, "it-helper", 1)).status == "draft"
        return

    if case == "heading exists once":
        await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
        record = await h.agents.get_version(TENANT, "it-helper", 1)
        [section] = record.agent().grants.drive
        assert section.heading == SECTION_HEADING
        assert section.doc_id == DRIVE_SECTION
        assert record.status == "pending"
        return

    with pytest.raises(ConfigError) as err:
        await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
    needle = "I can't find the heading" if case == "missing heading" else "appears more than once"
    assert needle in str(err.value)
    assert (await h.agents.get_version(TENANT, "it-helper", 1)).status == "draft"
