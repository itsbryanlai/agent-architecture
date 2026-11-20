"""Drive knowledge search: the live re-check before citing, and the citation footer."""

from tests.conftest import TENANT, eventually
from tests.helpers.drive import DRIVE_DOC, DRIVE_SCOPE, DRIVE_SHEET, run_drive_index
from tests.helpers.knowledge import answer, call, converse, search, tool_turn


async def test_a_drive_hit_that_now_answers_403_is_dropped(dharness):
    """Fails if a Drive file that lost access since indexing still reaches the model."""
    h = dharness
    await run_drive_index(h)
    _, before = await search(h, "it-helper", "reset your vpn password")
    assert DRIVE_DOC in {c.page_id for c in before.citations}

    h.google.drive_refused.add(DRIVE_DOC)
    _, after = await search(h, "it-helper", "reset your vpn password")

    assert DRIVE_DOC not in {c.page_id for c in after.citations}


async def test_a_drive_citation_links_to_the_file_and_names_the_sheet(dharness):
    """Fails if a cited Drive answer does not link to the file's webViewLink, or the sheet
    passage the model reads does not name its tab."""
    h = dharness
    await run_drive_index(h)
    h.llm.queue(tool_turn(call("tu_1", "alice engineer")), answer("Alice is an engineer [1]."))

    await converse(h, "it-helper", "1700000000.000900", "who is alice?")
    await eventually(lambda: h.slack.updates())

    title, url = (await h.knowledge.page_meta(TENANT, [DRIVE_SCOPE], [DRIVE_SHEET]))[DRIVE_SHEET]
    assert url == f"https://drive.google.com/file/d/{DRIVE_SHEET}"
    assert (
        h.slack.updates()[0]["text"] == f"Alice is an engineer [1].\n\nSources: [1] <{url}|{title}>"
    )
    cited = h.llm.calls[1]["messages"][2]["content"][0]["content"]
    assert "Roster › Q1" in cited
