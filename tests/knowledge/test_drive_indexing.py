"""DriveIndexingWorkflow over a fake Drive tree, on the time-skipping server."""

import pytest

from agentos.connectors.google_iam import GoogleError
from tests.conftest import TENANT
from tests.helpers.drive import (
    DRIVE_DOC,
    DRIVE_FOLDER,
    DRIVE_NESTED_DOC,
    DRIVE_SCOPE,
    DRIVE_SHEET,
    DRIVE_SLIDES,
    run_drive_index,
)

EXPORTABLE = (DRIVE_DOC, DRIVE_SHEET, DRIVE_SLIDES, DRIVE_NESTED_DOC)


async def test_drive_indexing_indexes_docs_sheets_slides_and_skips_other_types(dharness):
    """Fails if a granted folder's Docs, Sheets and Slides are not indexed, a nested file is
    missed, or a non-exportable type is indexed instead of counted."""
    h = dharness

    summary = await run_drive_index(h)

    assert (summary.files, summary.changed, summary.skipped_type, summary.failed) == (5, 4, 1, 0)
    assert set(h.knowledge.pages) == {(TENANT, DRIVE_SCOPE, f) for f in EXPORTABLE}
    texts = [t for chunks in h.knowledge.chunks.values() for t, _ in chunks]
    assert any("Reset your VPN password" in t for t in texts)
    assert any(t.startswith("Roster › Q1\n") for t in texts)
    assert any(t.startswith("Roster › Q2\n") for t in texts)
    assert any("Laptops are replaced" in t for t in texts)


async def test_an_unchanged_file_is_not_exported_again(dharness):
    """Fails if a second run re-exports a file whose Drive version did not change."""
    h = dharness
    await run_drive_index(h)
    exports = len(h.google.export_calls)
    embedded = h.embedder.texts_embedded

    again = await run_drive_index(h, suffix=":2")

    assert again.changed == 0
    assert len(h.google.export_calls) == exports
    assert h.embedder.texts_embedded == embedded


@pytest.mark.parametrize("case", ["removed", "unshared", "trashed"])
async def test_a_file_no_longer_seen_or_readable_is_pruned(dharness, case):
    """Fails if a file removed from the folder, unshared from the address, or trashed stays
    in the index after the next complete walk."""
    h = dharness
    await run_drive_index(h)
    assert (TENANT, DRIVE_SCOPE, DRIVE_DOC) in h.knowledge.pages
    if case == "removed":
        h.google.remove_drive_child(DRIVE_FOLDER, DRIVE_DOC)
    elif case == "unshared":
        h.google.drive_refused.add(DRIVE_DOC)
    else:
        h.google.trash_drive_file(DRIVE_DOC)

    again = await run_drive_index(h, suffix=":2")

    assert (TENANT, DRIVE_SCOPE, DRIVE_DOC) not in h.knowledge.pages
    assert again.pruned == 1


@pytest.mark.parametrize("case", ["transient", "lasting"])
async def test_an_export_failure_is_retried_and_a_lasting_one_is_counted(dharness, case):
    """Fails if a transient export failure is not retried to success, or a lasting one fails
    the whole run instead of being counted while the other files index."""
    h = dharness
    if case == "transient":
        h.google.fail_next_export(GoogleError("rate_limited", status=429, retry_after=0))
    else:
        h.google.fail_export_for(DRIVE_DOC, GoogleError("unavailable", status=503))

    summary = await run_drive_index(h)

    if case == "transient":
        assert summary.failed == 0
        assert set(h.knowledge.pages) == {(TENANT, DRIVE_SCOPE, f) for f in EXPORTABLE}
        assert len(h.google.export_calls) > len(EXPORTABLE)  # one export was retried
    else:
        assert summary.failed == 1
        assert (TENANT, DRIVE_SCOPE, DRIVE_DOC) not in h.knowledge.pages
        assert (TENANT, DRIVE_SCOPE, DRIVE_SHEET) in h.knowledge.pages
