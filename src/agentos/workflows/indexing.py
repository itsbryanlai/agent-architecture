"""IndexingWorkflow: index one granted root page and everything under it.

One workflow runs per (tenant, scope). Rules it keeps:

* History carries references only. The workflow passes page IDs and counts;
  page text is fetched, chunked, embedded and stored inside one activity per
  page and never reaches workflow history.
* Walking the tree is breadth first from the root, at most two pages read at a
  time (this bounds the work in flight; it is not rate limiting: the Notion client's
  per-tenant limiter in ``agentos.connectors.notion_limiter`` is, and it queues indexing
  behind interactive requests) and at most ``MAX_PAGES_PER_SCOPE`` pages. The two pages of
  a step are one activity, so the grants are checked once for them. When the cap is hit
  it stops discovering new pages but still prunes with what it saw.
* A page ends in one of three states (see ``agentos.knowledge.indexing``). A ``gone`` page
  is not kept: it is pruned from the store like a page that left the tree. A ``failed``
  page is kept, and so is everything stored beneath it, because its stored sub-pages go
  back with the result and the walk continues through them. One failed page never fails
  the run.
* A page the tenant budget stopped from being embedded is counted in ``deferred``. It is kept,
  not pruned, and a later run indexes it once the budget allows.
* A root page that is gone does not prune: the prune step records the scope as unhealthy and
  tells the owner, and deletes only after repeated runs or an admin's confirmation.
* The run's start time (``workflow.now()``) is passed to the prune step, which never deletes
  a page stored after it: a page event can index a page while the run is going.
* Workflow code is deterministic: no I/O, no clocks, no randomness.
"""

import asyncio
from dataclasses import dataclass

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agentos.knowledge.indexing import FAILED, IndexActivities, SyncResult
    from agentos.workflows import ACTIVITY_TIMEOUT, INDEX_TIMEOUT, RETRY

MAX_PAGES_PER_SCOPE = 2000
PARALLEL_PAGES = 2


@dataclass
class IndexSummary:
    pages: int
    changed: int
    skipped: int
    pruned: int
    deferred: int = 0
    failed: int = 0
    root_gone: bool = False


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
        deferred: int = 0,
        started: str | None = None,
        parents: dict[str, str] | None = None,
        failed: int = 0,
        confirm_gone: bool = False,
    ) -> IndexSummary:
        # Runs that began before these two changes replay the older commands.
        batched = workflow.patched("index-sync-batch")
        guarded = workflow.patched("index-prune-guard")
        if started is None:
            started = workflow.now().isoformat()
        # A fresh start begins at the root; a continue-as-new carries the walk.
        # ``root_page_id`` is the normalized 32-hex ID.
        if seen is None or queue is None:
            seen, queue = [root_page_id], [root_page_id]
        else:
            seen, queue = list(seen), list(queue)
        skipped = list(skipped or [])
        parents = dict(parents or {})
        capped = False

        while queue:
            batch, queue = queue[:PARALLEL_PAGES], queue[PARALLEL_PAGES:]
            results = await self._sync(tenant_id, scope, batch, parents, batched)
            for page_id, result in zip(batch, results):
                parents.pop(page_id, None)
                if result.gone:
                    skipped.append(page_id)
                    continue
                failed += int(result.status == FAILED)
                changed += int(result.changed)
                deferred += int(result.budget_stopped)
                for child in result.child_page_ids:
                    if child in seen:
                        continue
                    if len(seen) >= MAX_PAGES_PER_SCOPE:
                        capped = True
                        continue
                    seen.append(child)
                    queue.append(child)
                    parents[child] = page_id
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
                    args=[
                        tenant_id,
                        scope,
                        root_page_id,
                        seen,
                        queue,
                        skipped,
                        changed,
                        deferred,
                        started,
                        parents,
                        failed,
                        confirm_gone,
                    ]
                )

        gone = set(skipped)
        kept = [page_id for page_id in seen if page_id not in gone]
        root_gone = root_page_id in gone
        prune_args = [tenant_id, scope, kept]
        if guarded:
            prune_args += [started, root_gone, confirm_gone]
        pruned: int = await workflow.execute_activity_method(
            IndexActivities.prune_scope,
            args=prune_args,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        return IndexSummary(
            pages=len(seen),
            changed=changed,
            skipped=len(skipped),
            pruned=pruned,
            deferred=deferred,
            failed=failed,
            root_gone=root_gone,
        )

    async def _sync(
        self, tenant_id: str, scope: str, batch: list[str], parents: dict[str, str], batched: bool
    ) -> list[SyncResult]:
        if batched:
            return await workflow.execute_activity_method(
                IndexActivities.sync_pages,
                args=[tenant_id, scope, batch, [parents.get(page_id, "") for page_id in batch]],
                start_to_close_timeout=INDEX_TIMEOUT,
                retry_policy=RETRY,
            )
        return list(
            await asyncio.gather(
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
        )
