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
    [
        "heading exists once",
        "missing heading",
        "duplicated heading",
        "image under heading",
        "divider-like line under heading",
        "not connected",
    ],
    ids=["ok", "missing", "duplicated", "image", "divider-like", "not-connected"],
)
async def test_submit_checks_the_section_heading_exists_exactly_once(case):
    """Fails if submit does not read the Doc and accept a heading present exactly once in a
    plain-paragraph section, or accepts a heading the Doc lacks or has twice, a section holding
    an image or a line the model's --- and pipe-row view would misread, or an unconnected Drive."""
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
    elif case == "divider-like line under heading":
        google.docs[DRIVE_SECTION]["paragraphs"].insert(
            2, {"text": "---\n", "style": "NORMAL_TEXT"}
        )
    elif case == "image under heading":
        google.docs[DRIVE_SECTION]["paragraphs"].insert(
            2, {"text": "Caption.\n", "style": "NORMAL_TEXT", "image": True}
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
    needles = {
        "missing heading": "I can't find the heading",
        "duplicated heading": "appears more than once",
        "image under heading": "text, lines and tables",
        "divider-like line under heading": "reads like a divider or table row",
    }
    assert needles[case] in str(err.value)
    assert (await h.agents.get_version(TENANT, "it-helper", 1)).status == "draft"


@pytest.mark.parametrize(
    ("first_line", "recorded"),
    [
        ({"text": "Status\n", "style": "HEADING_1"}, "HEADING_1"),
        ({"text": "Status\n", "style": "NORMAL_TEXT", "bold": True}, "NORMAL_TEXT:bold"),
    ],
    ids=["heading", "bold line"],
)
async def test_submit_records_the_style_of_the_line_that_marks_the_section(first_line, recorded):
    """Fails if submit does not freeze the marker line's style signature into the definition, so
    a line later restyled would still mark the section, or a bold line is not accepted."""
    h, _drive, google = section_harness()
    await h.seed_members()
    await h.plane.connect_drive(TENANT, ROOT)
    add_section_doc(google)
    google.docs[DRIVE_SECTION]["paragraphs"][0] = first_line
    await h.plane.add_agent(TENANT, OWNER, "it-helper", section_definition())

    await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)

    record = await h.agents.get_version(TENANT, "it-helper", 1)
    [section] = record.agent().grants.drive
    assert section.marker == recorded
