"""IndexingWorkflow: index one granted root page and everything under it.

One workflow runs per (tenant, scope). Rules it keeps:

* History carries references only. The workflow passes page IDs and counts;
  page text is fetched, chunked, embedded and stored inside one activity per
  page and never reaches workflow history.
* Walking the tree is breadth first from the root, at most two pages read at a
  time (Notion rate limits) and at most ``MAX_PAGES_PER_SCOPE`` pages. When the
  cap is hit it stops discovering new pages but still prunes with what it saw.
* A page that was skipped (trashed or unreachable) is not kept: it is pruned
  from the store like a page that left the tree.
* Workflow code is deterministic: no I/O, no clocks, no randomness.
"""

import asyncio
from dataclasses import dataclass

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agentos.knowledge.indexing import IndexActivities, SyncResult
    from agentos.workflows import ACTIVITY_TIMEOUT, INDEX_TIMEOUT, RETRY

MAX_PAGES_PER_SCOPE = 2000
PARALLEL_PAGES = 2


@dataclass
class IndexSummary:
    pages: int
    changed: int
    skipped: int
    pruned: int


@workflow.defn(name="IndexingWorkflow")
class IndexingWorkflow:
    @workflow.run
    async def run(
        self,
        tenant_id: str,
        scope: str,
        root_page_id: str,
        seen: list[str] | None = None,
        queue: list[str] | None = None,
        skipped: list[str] | None = None,
        changed: int = 0,
    ) -> IndexSummary:
        # A fresh start begins at the root; a continue-as-new carries the walk.
        # ``root_page_id`` is the normalized 32-hex ID.
        if seen is None or queue is None:
            seen, queue = [root_page_id], [root_page_id]
        else:
            seen, queue = list(seen), list(queue)
        skipped = list(skipped or [])
        capped = False

        while queue:
            batch, queue = queue[:PARALLEL_PAGES], queue[PARALLEL_PAGES:]
            results: list[SyncResult] = await asyncio.gather(
                *[
                    workflow.execute_activity_method(
                        IndexActivities.sync_page,
                        args=[tenant_id, scope, page_id],
                        start_to_close_timeout=INDEX_TIMEOUT,
                        retry_policy=RETRY,
                    )
                    for page_id in batch
                ]
            )
            for page_id, result in zip(batch, results):
                if result.skipped:
                    skipped.append(page_id)
                    continue
                changed += int(result.changed)
                for child in result.child_page_ids:
                    if child in seen:
                        continue
                    if len(seen) >= MAX_PAGES_PER_SCOPE:
                        capped = True
                        continue
                    seen.append(child)
                    queue.append(child)
            if capped:
                workflow.logger.warning(
                    "page cap reached tenant_id=%s scope=%s cap=%d",
                    tenant_id,
                    scope,
                    MAX_PAGES_PER_SCOPE,
                )
                capped = False

            if queue and workflow.info().is_continue_as_new_suggested():
                workflow.continue_as_new(
                    args=[tenant_id, scope, root_page_id, seen, queue, skipped, changed]
                )

        kept = [page_id for page_id in seen if page_id not in set(skipped)]
        pruned: int = await workflow.execute_activity_method(
            IndexActivities.prune_scope,
            args=[tenant_id, scope, kept],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        return IndexSummary(pages=len(seen), changed=changed, skipped=len(skipped), pruned=pruned)
