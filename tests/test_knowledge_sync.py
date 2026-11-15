"""Knowledge sync and search: the three page states, scope health, ancestors, child databases,
unchanged pages, embedding models and the relevance cut-off."""

import copy
from datetime import UTC, datetime, timedelta

from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.agent_store import InMemoryAgentStore
from agentos.config import TASK_QUEUE
from agentos.connectors.notion import normalize_id
from agentos.knowledge.embedder import FakeEmbedder
from agentos.knowledge.indexing import GONE_RUNS_BEFORE_PRUNE, IndexActivities
from agentos.knowledge.search import MAX_CHUNKS_PER_PAGE, NO_MATCH
from agentos.knowledge.store import InMemoryKnowledgeStore, PageSync, PostgresKnowledgeStore
from agentos.tenants import TenantRegistry, runtime_for
from agentos.workflows.indexing import IndexingWorkflow
from tests.conftest import (
    PAGE_A1,
    PAGE_A2,
    ROOT_A,
    ROOT_B,
    TENANT,
    knowledge_agents_dict,
    make_fake_notion,
)
from tests.conftest import text_block as tb
from tests.test_knowledge_search import search

SCOPE_A = ids.knowledge_scope(ROOT_A)
ROOT = normalize_id(ROOT_A)
OWNER = "U_OWNER"
RETRIEVALS = ("retrieve_page", "retrieve_block")


class NotionStatusError(Exception):
    """What the Notion client raises for an HTTP error."""

    def __init__(self, status: int):
        super().__init__(f"notion answered {status}")
        self.status = status


def refuse(notion, method: str, statuses: dict[str, int]) -> None:
    """Make ``notion.<method>`` raise an HTTP error for the given page or block IDs."""
    real = getattr(notion, method)

    async def wrapped(notion_id, *args):
        status = statuses.get(normalize_id(notion_id))
        if status:
            raise NotionStatusError(status)
        return await real(notion_id, *args)

    setattr(notion, method, wrapped)


async def run_index(h, run: int, confirm_gone: bool = False):
    """One IndexingWorkflow run, with its own workflow ID as each nightly run has."""
    return await h.client.execute_workflow(
        IndexingWorkflow.run,
        args=[TENANT, SCOPE_A, ROOT, None, None, None, 0, 0, None, None, 0, confirm_gone],
        id=f"{ids.index_workflow_id(TENANT, SCOPE_A)}:run-{run}",
        task_queue=TASK_QUEUE,
    )


def notion_calls(h, *methods: str) -> int:
    return sum(h.notion.count(m) for m in methods)


def chain_tree(depth: int, text: str) -> tuple[dict, str]:
    """A page tree one page per level under ``ROOT_A``; the deepest page holds ``text``."""
    leaf_id = f"cccccccc-0000-0000-0000-{depth:012d}"
    node = {"id": leaf_id, "title": "Deepest", "blocks": [tb("paragraph", text)]}
    for level in range(depth - 1, 0, -1):
        node = {
            "id": f"cccccccc-0000-0000-0000-{level:012d}",
            "title": f"Level {level}",
            "blocks": [],
            "children": [node],
        }
    return {"title": "IT wiki", "blocks": [], "children": [node]}, leaf_id


# --- Three-state sync and scope health ---------------------------------------------------


async def test_root_gone_deletes_nothing_until_three_runs_and_tells_the_owner_once(kharness):
    """Fails if one run that finds the root page gone prunes the scope, the owner is not told,
    is told every run, or the scope is never pruned when the root stays gone."""
    h = kharness
    h.agents.admins = (OWNER,)
    await run_index(h, 1)
    stored = dict(h.knowledge.pages)
    assert len(stored) == 3
    h.notion.free_pages.pop(ROOT)

    for run in range(2, 2 + GONE_RUNS_BEFORE_PRUNE - 1):
        summary = await run_index(h, run)
        assert (summary.root_gone, summary.pruned) == (True, 0)
        assert h.knowledge.pages == stored
    notices = [p for p in h.slack.posts() if p["channel"] == f"D-{OWNER}"]
    assert len(notices) == 1
    assert ROOT in notices[0]["text"] and "IT helper" in notices[0]["text"]

    final = await run_index(h, 2 + GONE_RUNS_BEFORE_PRUNE)

    assert final.pruned == 3
    assert h.knowledge.pages == {}
    assert h.knowledge.health == {}
    assert len([p for p in h.slack.posts() if p["channel"] == f"D-{OWNER}"]) == 1


async def test_a_run_that_finds_the_root_again_restarts_the_count(kharness):
    """Fails if gone runs separated by a healthy run still add up to a prune."""
    h = kharness
    await run_index(h, 1)
    root_page = copy.deepcopy(h.notion.free_pages[ROOT])
    h.notion.free_pages.pop(ROOT)
    await run_index(h, 2)
    await run_index(h, 3)
    h.notion.free_pages[ROOT] = root_page
    await run_index(h, 4)
    h.notion.free_pages.pop(ROOT)

    await run_index(h, 5)
    summary = await run_index(h, 6)

    assert summary.pruned == 0
    assert len(h.knowledge.pages) == 3


async def test_an_admin_confirmation_prunes_a_gone_root_at_once(kharness):
    """Fails if the confirmation flag cannot delete the index of a gone root page."""
    h = kharness
    await run_index(h, 1)
    h.notion.free_pages.pop(ROOT)

    summary = await run_index(h, 2, confirm_gone=True)

    assert summary.pruned == 3
    assert h.knowledge.pages == {}


async def test_a_confirmation_does_nothing_while_the_root_is_there(kharness):
    """Fails if the confirmation flag prunes a scope whose root page is live."""
    h = kharness
    await run_index(h, 1)

    summary = await run_index(h, 2, confirm_gone=True)

    assert (summary.pruned, summary.root_gone) == (0, False)
    assert len(h.knowledge.pages) == 3


async def test_pages_beneath_a_forbidden_page_are_kept(kharness):
    """Fails if a 403 on a page, or on its sub-pages, prunes what was indexed beneath it."""
    h = kharness
    await run_index(h, 1)
    stored = dict(h.knowledge.pages)
    refuse(h.notion, "retrieve_page", {normalize_id(PAGE_A1): 403, normalize_id(PAGE_A2): 403})

    summary = await run_index(h, 2)

    assert (summary.pruned, summary.failed, summary.skipped) == (0, 2, 0)
    assert h.knowledge.pages == stored


async def test_a_forbidden_root_is_kept_not_treated_as_gone(kharness):
    """Fails if a 403 on the root page records the scope as gone or prunes it."""
    h = kharness
    h.agents.admins = (OWNER,)
    await run_index(h, 1)
    refuse(h.notion, "retrieve_page", {ROOT: 403})

    summary = await run_index(h, 2)

    assert (summary.pruned, summary.root_gone) == (0, False)
    assert len(h.knowledge.pages) == 3
    assert h.knowledge.health == {}
    assert h.slack.posts() == []


async def test_one_rejected_page_does_not_stop_the_run(kharness):
    """Fails if a page Notion rejects with a 400 fails the run, or stops the other pages."""
    h = kharness
    refuse(h.notion, "list_child_blocks", {normalize_id(PAGE_A2): 400})

    summary = await run_index(h, 1)

    assert (summary.failed, summary.pages) == (1, 3)
    assert {page for (_, _, page) in h.knowledge.pages} == {ROOT, normalize_id(PAGE_A1)}


async def test_a_trashed_page_leaves_the_index(kharness):
    """Fails if a page that is gone (as opposed to unreachable) is kept."""
    h = kharness
    await run_index(h, 1)
    h.notion.trash_page(PAGE_A2)

    summary = await run_index(h, 2)

    assert (summary.skipped, summary.pruned) == (1, 1)
    assert (TENANT, SCOPE_A, normalize_id(PAGE_A2)) not in h.knowledge.pages


async def test_a_page_stored_during_the_run_is_not_pruned():
    """Fails if prune deletes a page that a page event stored after the run started."""
    started = datetime(2026, 11, 16, 3, 0, tzinfo=UTC)
    clock = [started - timedelta(minutes=5)]
    store = InMemoryKnowledgeStore(clock=lambda: clock[0])
    cfg = _config()
    registry = TenantRegistry({TENANT: runtime_for(cfg, None, make_fake_notion())})
    acts = IndexActivities(registry, store, FakeEmbedder(), InMemoryAgentStore.seeded(cfg))
    await store.replace_page(TENANT, SCOPE_A, "old", "T", "u", "h", [])
    clock[0] = started + timedelta(seconds=30)
    await store.replace_page(TENANT, SCOPE_A, "new", "T", "u", "h", [])

    deleted = await ActivityEnvironment().run(
        acts.prune_scope, TENANT, SCOPE_A, [ROOT], started.isoformat()
    )

    assert deleted == 1
    assert set(await store.page_hashes(TENANT, SCOPE_A)) == {"new"}


def _config():
    from agentos.config import parse_agents_config

    return parse_agents_config(knowledge_agents_dict())


# --- Ancestors, depth and the search check ------------------------------------------


async def test_a_page_twelve_levels_deep_is_found_with_three_notion_calls(kharness):
    """Fails if a deep page is dropped by the search, or costs a call per level to confirm."""
    h = kharness
    tree, leaf = chain_tree(12, "Zircon calibration procedure for the lab.")
    h.notion.add_page_tree(ROOT_A, tree)
    await run_index(h, 1)
    stored = await h.knowledge.page_state(TENANT, SCOPE_A, normalize_id(leaf))
    assert len(stored.sync.ancestors) == 12  # the root and eleven levels
    before = notion_calls(h, *RETRIEVALS)

    text, result = await search(h, "it-helper", "zircon calibration procedure")

    assert "Zircon calibration" in text
    assert [c.page_id for c in result.citations] == [normalize_id(leaf)]
    assert notion_calls(h, *RETRIEVALS) - before <= 3


async def test_a_page_moved_out_of_its_parent_is_left_out(kharness):
    """Fails if a page moved elsewhere in Notion is still returned from the stored position."""
    h = kharness
    tree, leaf = chain_tree(4, "Zircon calibration procedure for the lab.")
    h.notion.add_page_tree(ROOT_A, tree)
    await run_index(h, 1)
    query = "zircon calibration procedure"
    assert "Zircon" in (await search(h, "it-helper", query))[0]

    h.notion.move_page(leaf, {"type": "page_id", "page_id": normalize_id(ROOT_B)})
    text, _ = await search(h, "it-helper", query)

    assert text == NO_MATCH


async def test_a_page_stored_without_its_ancestors_is_still_found(kharness):
    """Fails if a row without a stored chain (written before chains were kept) is never
    returned."""
    h = kharness
    await run_index(h, 1)
    key = normalize_id(PAGE_A2)
    state = await h.knowledge.page_state(TENANT, SCOPE_A, key)
    await h.knowledge.touch_page(
        TENANT, SCOPE_A, key, state.content_hash, PageSync(embed_model=h.embedder.model)
    )

    text, _ = await search(h, "it-helper", "swollen batteries must be reported")

    assert "Swollen batteries" in text


# --- Child databases -------------------------------------------------------------------------


async def test_rows_of_a_child_database_are_indexed_and_searchable(kharness):
    """Fails if rows of a database beneath a granted page are not indexed with their properties
    and body, or cannot be found by a search."""
    h = kharness
    database = "dddddddd-0000-0000-0000-000000000001"
    h.notion.add_database(database)
    row = h.notion.add_page(
        database,
        {
            "Name": {"title": [{"text": {"content": "Expense policy"}}]},
            "Rule": {"rich_text": [{"text": {"content": "Receipts within thirty days"}}]},
        },
    )
    h.notion.set_blocks(row["id"], [tb("paragraph", "Reimbursement is paid monthly.")])
    h.notion.set_blocks(
        ROOT_A,
        [
            tb("paragraph", "Reset your VPN password at the portal."),
            {
                "id": database,
                "type": "child_database",
                "has_children": False,
                "child_database": {"title": "Expenses"},
            },
            {
                "id": PAGE_A1,
                "type": "child_page",
                "has_children": False,
                "child_page": {"title": "Laptops"},
            },
        ],
    )

    summary = await run_index(h, 1)
    text, result = await search(h, "it-helper", "receipts thirty days reimbursement paid monthly")

    assert summary.pages == 4
    assert "Receipts within thirty days" in text and "Reimbursement is paid monthly" in text
    assert [c.page_id for c in result.citations] == [normalize_id(row["id"])]


# --- Unchanged pages ---------------------------------------------------------------------


async def test_unchanged_pages_are_not_read_again_and_edited_pages_are(kharness):
    """Fails if a page whose edit time has not moved has its blocks fetched again, or an
    edited page is not."""
    h = kharness
    await run_index(h, 1)
    first = h.notion.count("list_child_blocks")
    assert first == 3

    await run_index(h, 2)
    assert h.notion.count("list_child_blocks") == first

    h.notion.set_blocks(PAGE_A2, [tb("paragraph", "Batteries are swapped on Fridays.")])
    summary = await run_index(h, 3)

    assert h.notion.count("list_child_blocks") == first + 1
    assert (summary.changed, summary.pages) == (1, 3)


# --- Embedding model and relevance -----------------------------------------------------------


async def test_chunks_of_another_embedding_model_are_never_returned(kharness):
    """Fails if a search compares the question with chunks embedded by a different model, or
    a run after a model change leaves them in place."""
    h = kharness
    await run_index(h, 1)
    query = "swollen batteries must be reported"
    assert "Swollen" in (await search(h, "it-helper", query))[0]

    h.embedder.model = "other-embed"
    assert (await search(h, "it-helper", query))[0] == NO_MATCH

    embedded = h.embedder.texts_embedded
    await run_index(h, 2)

    assert h.embedder.texts_embedded > embedded
    assert "Swollen" in (await search(h, "it-helper", query))[0]


async def test_an_off_topic_question_gets_the_no_match_text(kharness):
    """Fails if a question unrelated to every page still returns passages."""
    h = kharness
    await run_index(h, 1)

    text, result = await search(h, "it-helper", "quantum astrophysics of zebra migration")

    assert text == NO_MATCH
    assert result.citations == []


async def test_one_page_gives_at_most_the_per_page_cap_of_passages(kharness):
    """Fails if one long page fills the answer with all its chunks."""
    h = kharness
    blocks = []
    for n in range(5):
        blocks += [tb("heading_2", f"Part {n}"), tb("paragraph", f"Invoice rules number {n}.")]
    h.notion.add_page_tree(ROOT_A, {"title": "Finance", "blocks": blocks})
    await run_index(h, 1)

    text, _ = await search(h, "it-helper", "invoice rules")

    assert text.count("<source ") == MAX_CHUNKS_PER_PAGE


# --- Query counts ---------------------------------------------------------------


async def test_sync_reads_one_page_row_per_page_and_the_bulk_read_only_to_prune(kharness):
    """Fails if a page's sync loads every page hash of the scope again."""
    h = kharness
    bulk = []
    real = h.knowledge.page_hashes

    async def counted(tenant, scope):
        bulk.append(scope)
        return await real(tenant, scope)

    h.knowledge.page_hashes = counted

    await run_index(h, 1)

    assert len(bulk) == 1
    # Each of the three pages is read once; its parent's row is read once per batch.
    assert 3 <= h.knowledge.page_state_reads <= 5


async def test_published_agents_are_loaded_once_per_batch_of_pages(kharness):
    """Fails if the published agents are loaded again for every page of a batch."""
    h = kharness
    children = [
        {"id": f"cccccccc-0000-0000-0000-{n:012d}", "title": f"Child {n}", "blocks": []}
        for n in range(1, 5)
    ]
    h.notion.add_page_tree(ROOT_A, {"title": "IT wiki", "blocks": [], "children": children})
    loads = []
    real = h.agent_store.published_all

    async def counted(tenant):
        loads.append(tenant)
        return await real(tenant)

    h.agent_store.published_all = counted

    summary = await run_index(h, 1)

    assert summary.pages == 5
    batches = 3  # the root, then two pairs of children
    assert len(loads) == batches + 1  # and the prune step


class RecordingConnection:
    def __init__(self):
        self.statements: list[str] = []

    async def execute(self, sql, params=None):
        self.statements.append(sql)
        return self

    def transaction(self):
        return _Passthrough(None)


class _Passthrough:
    def __init__(self, value):
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, *exc):
        return False


class RecordingPool:
    def __init__(self):
        self.conn = RecordingConnection()

    def connection(self):
        return _Passthrough(self.conn)


async def test_a_pages_chunks_are_inserted_in_one_statement():
    """Fails if storing a page sends one INSERT per chunk."""
    pool = RecordingPool()
    store = PostgresKnowledgeStore(pool)
    chunks = [(f"text {n}", [0.0, 1.0]) for n in range(6)]

    await store.replace_page(TENANT, SCOPE_A, "p", "T", "u", "h", chunks, PageSync())

    inserts = [s for s in pool.conn.statements if "INSERT INTO knowledge_chunks" in s]
    assert len(inserts) == 1
