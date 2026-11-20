"""DriveIndexingWorkflow over a fake Drive tree, on the time-skipping server."""

import pytest
from temporalio.client import WorkflowFailureError

from agentos.connectors.google_iam import DRIVE_REFUSED_TEXT, DriveSection, GoogleError
from agentos.knowledge.indexing import GONE_RUNS_BEFORE_PRUNE
from agentos.knowledge.notion_text import TextBlock, chunk
from agentos.switches import CONNECTOR, DRIVE, connector_off_text
from tests.conftest import TENANT
from tests.helpers.agents import republish
from tests.helpers.budgets import NOV
from tests.helpers.drive import (
    DRIVE_DOC,
    DRIVE_FOLDER,
    DRIVE_NESTED_DOC,
    DRIVE_SCOPE,
    DRIVE_SHARED,
    DRIVE_SHARED_DOC,
    DRIVE_SHARED_NESTED,
    DRIVE_SHEET,
    DRIVE_SLIDES,
    DRIVE_SUBFOLDER,
    SHARED_DRIVE_SCOPE,
    add_bulk_children,
    add_shared_drive_tree,
    drive_account_email,
    run_drive_index,
)
from tests.helpers.knowledge import search
from tests.helpers.llm_errors import StatusError
from tests.helpers.replay_support import suggest_continue_as_new_after

EXPORTABLE = (DRIVE_DOC, DRIVE_SHEET, DRIVE_SLIDES, DRIVE_NESTED_DOC)
OWNER = "U_OWNER"


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


async def test_a_failed_folder_prunes_nothing_and_is_counted(dharness):
    """Fails if a folder whose listing keeps failing prunes its still-granted subtree,
    goes uncounted in the summary, or leaves the connection reading connected."""
    h = dharness
    await run_drive_index(h)
    stored = dict(h.knowledge.pages)
    h.google.fail_children_for(DRIVE_SUBFOLDER, GoogleError("unavailable", status=503))

    summary = await run_drive_index(h, suffix=":2")

    assert (summary.failed, summary.pruned) == (1, 0)
    assert h.knowledge.pages == stored
    row = await h.drive_connections.get(TENANT)
    assert (row.status, row.last_error) == ("error", "unavailable")


@pytest.mark.parametrize(
    ("case", "confirm", "code"),
    [
        ("trashed", False, "not_found"),
        ("unshared", False, "permission_denied"),
        ("disabled", False, "permission_denied"),
        ("trashed", True, "not_found"),
    ],
)
async def test_an_unreadable_root_waits_for_repeated_runs_and_tells_the_owner_once(
    dharness, case, confirm, code
):
    """Fails if one run that finds the root gone or unreadable prunes the scope, the owner
    is not told once with the root's label and ID, the connection stores a wrong code, or
    the scope is never pruned when the root stays unreadable."""
    h = dharness
    h.agents.admins = (OWNER,)
    await run_drive_index(h)
    stored = dict(h.knowledge.pages)
    if case == "trashed":
        h.google.trash_drive_file(DRIVE_FOLDER)
    elif case == "unshared":
        h.google.drive_refused.add(DRIVE_FOLDER)
    else:
        h.google.fail_get_for(
            DRIVE_FOLDER,
            GoogleError(
                "permission_denied", DRIVE_REFUSED_TEXT, status=403, reason="SERVICE_DISABLED"
            ),
        )

    if confirm:
        summary = await run_drive_index(h, suffix=":gone-0", confirm_gone=True)
        assert (summary.root_gone, summary.pruned) == (True, len(stored))
    else:
        for run in range(GONE_RUNS_BEFORE_PRUNE - 1):
            summary = await run_drive_index(h, suffix=f":gone-{run}")
            assert (summary.root_gone, summary.pruned) == (True, 0)
            assert h.knowledge.pages == stored
        summary = await run_drive_index(h, suffix=f":gone-{GONE_RUNS_BEFORE_PRUNE - 1}")
        assert summary.pruned == len(stored)

    assert h.knowledge.pages == {}
    assert h.knowledge.health == {}
    notices = [p for p in h.slack.posts() if p["channel"] == f"D-{OWNER}"]
    assert len(notices) == 1
    assert "Team folder" in notices[0]["text"] and DRIVE_FOLDER in notices[0]["text"]
    row = await h.drive_connections.get(TENANT)
    assert (row.status, row.last_error) == ("error", code)


@pytest.mark.parametrize("shape", ["wide folder", "three folders"])
async def test_a_capped_walk_prunes_nothing(dharness, shape):
    """Fails if a walk that hit the discovery cap or a truncated listing prunes files it
    never saw, or the summary does not say the walk was capped."""
    h = dharness
    if shape == "wide folder":
        # More children than one activity carries; the Doc is first, so the first walk
        # sees and stores it, and the reversed second walk cannot reach it.
        add_bulk_children(h.google, DRIVE_FOLDER, 2001, "drive-bulk")
    else:
        for i in range(3):
            folder = f"drive-bulk-folder-{i}"
            h.google.add_drive_folder(folder, f"Bulk {i}", parent=DRIVE_FOLDER)
            add_bulk_children(h.google, folder, 700, f"drive-bulk-{i}")

    with suggest_continue_as_new_after(200):
        first = await run_drive_index(h)
        assert (first.capped, first.pruned) == (True, 0)
        assert (TENANT, DRIVE_SCOPE, DRIVE_DOC) in h.knowledge.pages
        if shape == "wide folder":
            h.google.drive_children[DRIVE_FOLDER].reverse()
        else:
            h.google.move_drive_child("drive-bulk-folder-0", "drive-bulk-folder-2", DRIVE_DOC)
        again = await run_drive_index(h, suffix=":2")

    assert (again.capped, again.pruned) == (True, 0)
    assert (TENANT, DRIVE_SCOPE, DRIVE_DOC) in h.knowledge.pages


async def test_one_bad_file_never_fails_the_run(dharness):
    """Fails if an embedding refusal fails the whole run, drops what is stored for the
    file, or stops the files after it from syncing."""
    h = dharness
    await run_drive_index(h)
    stored_doc = h.knowledge.chunks[(TENANT, DRIVE_SCOPE, DRIVE_DOC)]
    new_text = "A new poison text."
    poison = chunk("Guide", [TextBlock("text", new_text)])[0]
    h.embedder.fail_embed_for(poison, StatusError(400))
    h.google.set_drive_version(DRIVE_DOC, "2")
    h.google.set_drive_content(DRIVE_DOC, [DriveSection("", new_text)])
    h.google.set_drive_version(DRIVE_NESTED_DOC, "2")
    h.google.set_drive_content(DRIVE_NESTED_DOC, [DriveSection("", "Laptops now last four years.")])

    summary = await run_drive_index(h, suffix=":2")

    assert summary.failed == 1
    assert h.knowledge.chunks[(TENANT, DRIVE_SCOPE, DRIVE_DOC)] == stored_doc
    nested = [t for t, _ in h.knowledge.chunks[(TENANT, DRIVE_SCOPE, DRIVE_NESTED_DOC)]]
    assert any("four years" in t for t in nested)


async def test_a_granted_shared_drive_is_indexed_from_its_root(dharness):
    """Fails if a granted shared drive's root is read as a plain file, its children are
    not listed drive-scoped with the tenant's address, or its nested files are missed."""
    h = dharness
    add_shared_drive_tree(h.google)
    await republish(
        h.agent_store,
        "it-helper",
        lambda d: d["grants"].update(
            drive={"shared_drives": [{"id": DRIVE_SHARED, "name": "Team drive"}]}
        ),
    )

    summary = await run_drive_index(h, scope=SHARED_DRIVE_SCOPE, root=DRIVE_SHARED)

    assert (summary.files, summary.changed, summary.failed) == (2, 2, 0)
    assert set(h.knowledge.pages) == {
        (TENANT, SHARED_DRIVE_SCOPE, f) for f in (DRIVE_SHARED_DOC, DRIVE_SHARED_NESTED)
    }
    email = drive_account_email(h.google)
    assert (email, DRIVE_SHARED) in h.google.get_calls
    assert (email, DRIVE_SHARED) in h.google.list_calls


async def test_the_walk_reads_with_the_tenants_own_address(dharness):
    """Fails if any get, export or list of the walk reads with another account than the
    tenant's own Drive address."""
    h = dharness
    await run_drive_index(h)
    email = drive_account_email(h.google)
    assert h.google.get_calls and all(e == email for e, _ in h.google.get_calls)
    assert h.google.export_calls and all(e == email for e, _ in h.google.export_calls)
    assert h.google.list_calls and all(e == email for e, _ in h.google.list_calls)


async def test_the_drive_switch_stops_indexing_without_pruning_and_refuses_drive_search(dharness):
    """Fails if Drive-off lets a sync or prune step run (pruning stored rows), or lets a
    Drive-only agent's search cite Drive passages."""
    h = dharness
    await run_drive_index(h)
    stored = dict(h.knowledge.pages)
    await h.switches.switch_off(TENANT, CONNECTOR, DRIVE, OWNER, NOV)

    with pytest.raises(WorkflowFailureError) as err:
        await run_drive_index(h, suffix=":off")
    cause = err.value.cause.cause
    assert cause.non_retryable and cause.type == "ConnectorOff"
    assert h.knowledge.pages == stored

    text, result = await search(h, "it-helper", "reset your vpn password")
    assert result.is_error and result.citations == []
    assert text == connector_off_text(DRIVE)
