"""Indexing: page text, chunks, and the workflow that walks a granted tree."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from agentos import ids, index_knowledge
from agentos.agent_store import seeded_agent_store
from agentos.config import TASK_QUEUE, Settings, TenantsConfig, parse_agents_config
from agentos.connectors.notion import FakeNotionAPI
from agentos.index_knowledge import apply_drive_schedules, granted_roots, start_indexing
from agentos.knowledge.notion_text import CHUNK_CHARS, TextBlock, chunk, page_text
from agentos.knowledge.store import InMemoryKnowledgeStore
from agentos.usage import UsageEvent
from agentos.workflows.indexing import IndexingWorkflow
from tests.conftest import (
    PAGE_A1,
    PAGE_A2,
    ROOT_A,
    TENANT,
    knowledge_agents_dict,
)
from tests.conftest import (
    text_block as tb,
)
from tests.helpers.drive import (
    DRIVE_DOC,
    DRIVE_FOLDER,
    DRIVE_SCOPE,
    drive_agents_dict,
    run_drive_index,
)
from tests.helpers.indexing_run import SCOPE_A, run_index
from tests.helpers.knowledge import FakeScheduleClient

ROOT_A_PLAIN = ROOT_A.replace("-", "")


async def test_page_text_extracts_blocks_and_children():
    api = FakeNotionAPI()
    child = "cccccccc-0000-0000-0000-000000000009"
    api.add_page_tree(
        ROOT_A,
        {
            "title": "Guide",
            "blocks": [
                tb("heading_2", "Setup"),
                tb("paragraph", "First paragraph."),
                tb("bulleted_list_item", "A bullet"),
                tb("to_do", "A task"),
                tb("code", "print('hi')"),
                tb("table_row", cells=["a", "b", "c"]),
                tb("image", ""),
                tb("toggle", "Toggle title", children=[tb("paragraph", "Inside the toggle")]),
            ],
            "children": [{"id": child, "title": "Sub", "blocks": [tb("paragraph", "Sub text")]}],
        },
    )

    content = await page_text(api, ROOT_A)

    assert content.title == "Guide"
    assert content.url.endswith(ROOT_A_PLAIN)
    assert [(b.kind, b.text) for b in content.blocks] == [
        ("heading", "Setup"),
        ("text", "First paragraph."),
        ("text", "A bullet"),
        ("text", "A task"),
        ("text", "print('hi')"),
        ("text", "a | b | c"),
        ("text", "Toggle title"),
        ("text", "Inside the toggle"),
    ]
    # The sub-page is returned by ID and its text is not inlined.
    assert content.child_page_ids == [child.replace("-", "")]
    assert all("Sub text" not in b.text for b in content.blocks)


def test_chunking_respects_limit_and_headings():
    long_block = " ".join(f"Sentence number {i} is here." for i in range(200))
    unbroken = "x" * 3000
    blocks = [
        TextBlock("text", "Intro before any heading."),
        TextBlock("heading", "Policy"),
        TextBlock("text", long_block),
        TextBlock("text", unbroken),
        TextBlock("heading", "Empty section"),
        TextBlock("heading", "Last"),
        TextBlock("text", "Closing words."),
    ]

    chunks = chunk("Handbook", blocks)

    assert chunks[0] == "Handbook\nIntro before any heading."
    policy = [c for c in chunks if c.startswith("Handbook › Policy\n")]
    assert len(policy) >= 4
    prefix = len("Handbook › Policy\n")
    assert all(len(c) <= CHUNK_CHARS + prefix for c in chunks)
    assert chunks[-1] == "Handbook › Last\nClosing words."
    assert not any("Empty section" in c for c in chunks)
    assert chunk("Empty", [TextBlock("heading", "Only a heading")]) == []


async def test_indexing_walks_tree_and_stores_chunks(kharness):
    h = kharness

    summary = await run_index(h)

    assert (summary.pages, summary.changed, summary.skipped, summary.pruned) == (3, 3, 0, 0)
    stored = {key for key in h.knowledge.pages}
    assert stored == {
        (TENANT, SCOPE_A, ROOT_A_PLAIN),
        (TENANT, SCOPE_A, PAGE_A1.replace("-", "")),
        (TENANT, SCOPE_A, PAGE_A2.replace("-", "")),
    }
    texts = [t for chunks in h.knowledge.chunks.values() for t, _ in chunks]
    assert any("Swollen batteries" in t for t in texts)
    assert any(t.startswith("IT wiki › VPN\n") for t in texts)


async def test_reindex_skips_unchanged_pages(kharness):
    h = kharness
    await run_index(h)
    embedded = h.embedder.texts_embedded
    assert embedded > 0

    # A new workflow ID per run, as the schedule gives each nightly run.
    again = await h.client.execute_workflow(
        IndexingWorkflow.run,
        args=[TENANT, SCOPE_A, ROOT_A_PLAIN],
        id=ids.index_workflow_id(TENANT, SCOPE_A) + ":2",
        task_queue=TASK_QUEUE,
    )

    assert h.embedder.texts_embedded == embedded
    assert (again.pages, again.changed, again.pruned) == (3, 0, 0)


async def test_reindex_updates_changed_and_prunes_removed(kharness):
    h = kharness
    await run_index(h)
    before = h.embedder.texts_embedded

    # set_blocks replaces the page's blocks, so the Batteries sub-page block is put back.
    h.notion.set_blocks(
        PAGE_A1,
        [
            tb("paragraph", "Laptops are now replaced every four years."),
            {
                "id": PAGE_A2,
                "type": "child_page",
                "has_children": False,
                "child_page": {"title": "Batteries"},
            },
        ],
    )
    summary_edit = await h.client.execute_workflow(
        IndexingWorkflow.run,
        args=[TENANT, SCOPE_A, ROOT_A_PLAIN],
        id=ids.index_workflow_id(TENANT, SCOPE_A) + ":2",
        task_queue=TASK_QUEUE,
    )
    assert (summary_edit.changed, summary_edit.pruned) == (1, 0)
    assert h.embedder.texts_embedded > before

    h.notion.remove_child_page(PAGE_A1, PAGE_A2)
    summary_gone = await h.client.execute_workflow(
        IndexingWorkflow.run,
        args=[TENANT, SCOPE_A, ROOT_A_PLAIN],
        id=ids.index_workflow_id(TENANT, SCOPE_A) + ":3",
        task_queue=TASK_QUEUE,
    )

    assert (summary_gone.pages, summary_gone.pruned) == (2, 1)
    assert (TENANT, SCOPE_A, PAGE_A2.replace("-", "")) not in h.knowledge.pages
    assert len(h.knowledge.pages) == 2


async def test_indexing_history_has_no_page_text(kharness):
    h = kharness
    await run_index(h)

    history = await h.client.get_workflow_handle(
        ids.index_workflow_id(TENANT, SCOPE_A)
    ).fetch_history()
    blob = b"".join(e.SerializeToString() for e in history.events)

    assert PAGE_A2.replace("-", "").encode() in blob  # IDs are in history
    for text in ("Swollen batteries", "Reset your VPN password", "Laptops", "Batteries"):
        assert text.encode() not in blob
    # The control: the text is in the store.
    assert any("Swollen batteries" in t for c in h.knowledge.chunks.values() for t, _ in c)


async def test_index_command_starts_one_workflow_per_scope(kharness):
    h = kharness
    raw = knowledge_agents_dict()
    # A second agent granted the same root page as it-helper.
    raw["agents"]["hr-helper"]["grants"]["notion"]["pages"] = [
        {"id": ROOT_A.upper(), "name": "Same wiki, other spelling"}
    ]
    cfg = parse_agents_config(raw)

    assert list(granted_roots(cfg)) == [SCOPE_A]
    started = await start_indexing(h.client, cfg)

    assert [workflow_id for workflow_id, _ in started] == [ids.index_workflow_id(TENANT, SCOPE_A)]
    summary = await started[0][1].result()
    assert summary.pages == 3


async def over_the_tenant_budget(h):
    await h.budgets.set_budget(TENANT, None, Decimal(5), "root")
    await h.usage_store.record(
        UsageEvent(
            tenant_id=TENANT,
            usage_key="spent",
            kind="llm",
            agent_id="it-helper",
            agent_version=1,
            model="m",
            cost=Decimal(5),
            at=datetime.now(UTC),
        )
    )


async def test_a_run_over_the_tenant_budget_embeds_nothing_and_prunes_nothing(kharness):
    h = kharness
    await run_index(h)
    stored = dict(h.knowledge.pages)
    embedded = h.embedder.texts_embedded
    h.notion.set_blocks(
        PAGE_A1,
        [
            tb("paragraph", "Laptops are now replaced every four years."),
            {
                "id": PAGE_A2,
                "type": "child_page",
                "has_children": False,
                "child_page": {"title": "Batteries"},
            },
        ],
    )
    await over_the_tenant_budget(h)

    summary = await h.client.execute_workflow(
        IndexingWorkflow.run,
        args=[TENANT, SCOPE_A, ROOT_A_PLAIN],
        id=ids.index_workflow_id(TENANT, SCOPE_A) + ":2",
        task_queue=TASK_QUEUE,
    )

    assert h.embedder.texts_embedded == embedded
    assert (summary.pages, summary.changed, summary.skipped, summary.pruned) == (3, 0, 0, 0)
    assert summary.deferred == 1
    assert h.knowledge.pages == stored


async def test_the_run_picks_up_again_when_the_budget_is_raised(kharness):
    h = kharness
    await over_the_tenant_budget(h)
    first = await run_index(h)
    assert (first.pages, first.changed, first.deferred, first.pruned) == (3, 0, 3, 0)
    assert h.knowledge.pages == {}

    await h.budgets.set_budget(TENANT, None, Decimal(500), "root")
    again = await h.client.execute_workflow(
        IndexingWorkflow.run,
        args=[TENANT, SCOPE_A, ROOT_A_PLAIN],
        id=ids.index_workflow_id(TENANT, SCOPE_A) + ":2",
        task_queue=TASK_QUEUE,
    )

    assert (again.changed, again.deferred) == (3, 0)
    assert len(h.knowledge.pages) == 3


# --- the operator path for Drive scopes ---------------------------------------------------

DRIVE_SCHEDULE = ids.drive_index_schedule_id(TENANT, DRIVE_SCOPE)
STALE_DRIVE_SCHEDULE = ids.drive_index_schedule_id(
    TENANT, ids.drive_scope("drive-gone-folder-0001")
)
OTHER_DRIVE_SCHEDULE = ids.drive_index_schedule_id("other", DRIVE_SCOPE)
NOTION_SCHEDULE = ids.index_schedule_id(TENANT, SCOPE_A)


@pytest.mark.parametrize(
    ("existing", "create", "update", "delete"),
    [
        ([], [DRIVE_SCHEDULE], [], []),
        ([DRIVE_SCHEDULE], [], [DRIVE_SCHEDULE], []),
        ([STALE_DRIVE_SCHEDULE], [DRIVE_SCHEDULE], [], [STALE_DRIVE_SCHEDULE]),
        ([OTHER_DRIVE_SCHEDULE, NOTION_SCHEDULE], [DRIVE_SCHEDULE], [], []),
    ],
)
async def test_drive_schedules_are_planned_per_tenant(existing, create, update, delete):
    """Fails if a granted Drive scope gets no nightly schedule running the Drive walk, an
    existing one is recreated instead of updated, a stale one of this tenant survives, or
    another tenant's (or a Notion) schedule is touched."""
    cfg = parse_agents_config(drive_agents_dict())
    client = FakeScheduleClient(existing)

    plan = await apply_drive_schedules(client, cfg)

    assert (plan.create, plan.update, plan.delete) == (create, update, delete)
    assert sorted(client.schedules) == sorted((set(existing) - set(delete)) | set(create))
    if create:
        action = client.schedules[create[0]].action
        assert action.workflow == "DriveIndexingWorkflow"
        assert action.args == [TENANT, DRIVE_SCOPE, DRIVE_FOLDER]


@pytest.mark.parametrize("mode", ["start", "schedule", "confirm-gone"])
async def test_the_index_command_runs_drive_scopes(dharness, monkeypatch, capsys, mode):
    """Fails if the command refuses a tenant whose only grants are Drive, starts no Drive
    run, leaves a Drive scope without a nightly schedule or purges its rows, or
    --confirm-gone cannot prune a gone Drive root at once."""
    h = dharness
    cfg = parse_agents_config(drive_agents_dict())
    store = InMemoryKnowledgeStore()
    client = FakeScheduleClient() if mode == "schedule" else h.client

    async def build_client(_settings, keys=None, pool=None):
        return client

    class OpenStore:
        def __init__(self, opened):
            self.opened = opened

        async def __aenter__(self):
            return self.opened

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(
        index_knowledge, "load_settings", lambda: Settings(database_url="postgresql://unused")
    )

    async def load_tenants(_database_url):
        return TenantsConfig.of(cfg)

    monkeypatch.setattr(index_knowledge, "load_stored_tenants", load_tenants)
    monkeypatch.setattr(index_knowledge, "build_temporal_client", build_client)
    monkeypatch.setattr(index_knowledge, "open_store", lambda _settings: OpenStore(store))
    monkeypatch.setattr(
        index_knowledge, "open_agent_store", lambda _settings: OpenStore(seeded_agent_store(cfg))
    )
    if mode == "confirm-gone":
        await run_drive_index(h)
        h.google.trash_drive_file(DRIVE_FOLDER)
    if mode == "schedule":
        await store.replace_page(
            TENANT, DRIVE_SCOPE, DRIVE_DOC, "Guide", "https://drive/x", "h", [("text", [1.0, 0.0])]
        )

    await index_knowledge.run(
        wait=mode != "schedule", schedule=mode == "schedule", confirm_gone=mode == "confirm-gone"
    )

    out = capsys.readouterr().out
    if mode == "schedule":
        assert DRIVE_SCHEDULE in client.schedules
        assert await store.scopes(TENANT) == [DRIVE_SCOPE]
        assert f"created schedule {DRIVE_SCHEDULE}" in out
    else:
        assert f"started {ids.drive_index_workflow_id(TENANT, DRIVE_SCOPE)}" in out
        if mode == "start":
            assert "files=5 changed=4" in out
        else:
            assert "pruned=4 root-gone" in out
            assert h.knowledge.pages == {}
