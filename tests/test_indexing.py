"""Indexing: page text, chunks, and the workflow that walks a granted tree."""

from agentos import ids
from agentos.config import TASK_QUEUE, parse_agents_config
from agentos.connectors.notion import FakeNotionAPI
from agentos.index_knowledge import granted_roots, start_indexing
from agentos.knowledge.notion_text import CHUNK_CHARS, TextBlock, chunk, page_text
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

SCOPE_A = ids.knowledge_scope(ROOT_A)
ROOT_A_PLAIN = ROOT_A.replace("-", "")


async def run_index(h, scope=SCOPE_A, root=ROOT_A):
    return await h.client.execute_workflow(
        IndexingWorkflow.run,
        args=[TENANT, scope, root.replace("-", "")],
        id=ids.index_workflow_id(TENANT, scope),
        task_queue=TASK_QUEUE,
    )


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
