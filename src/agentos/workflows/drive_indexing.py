"""DriveIndexingWorkflow: index one granted Drive folder (recursively), file or shared drive.

One workflow runs per (tenant, Drive scope), its own name so it never shares a history
with Notion's ``IndexingWorkflow``. Rules it keeps:

* History carries references only: file IDs and counts. A file's text is exported,
  chunked, embedded and stored inside one activity and never reaches workflow history.
* The walk is breadth first from the granted root, at most two nodes read at a time and
  at most ``MAX_DRIVE_FILES_PER_SCOPE`` nodes. A folder node lists its children; a file
  node is exported when its Drive version changed.
* Only a complete walk prunes. When the discovery cap is hit, or a folder's listing is
  truncated, the walk stops discovering new nodes and prunes nothing that run: the files
  it could not see are still granted, and the summary says the walk was capped.
* A node that is trashed, gone or no longer readable is not kept: it is pruned like a
  file that left the folder. A node that fails for a lasting reason (a folder included)
  keeps what is stored for it, is counted in the summary, and the run goes on. One failed
  file never fails the run; a failed folder makes the walk incomplete.
* A file the tenant budget stopped from being embedded is counted ``deferred`` and kept.
* A root that is gone or unreadable — any failure, not only a gone one — does not prune
  at once: the prune step records the scope unhealthy and deletes only after repeated
  runs or an admin's confirmation (Notion's guard).
* The run's start time (``workflow.now()``) goes to the prune step, which never deletes a
  file stored after it.
* Workflow code is deterministic: no I/O, no clocks, no randomness.
"""

from dataclasses import dataclass

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agentos.knowledge.drive_indexing import DriveIndexActivities
    from agentos.knowledge.indexing import FAILED, GONE
    from agentos.workflows import ACTIVITY_TIMEOUT, INDEX_TIMEOUT, RETRY

MAX_DRIVE_FILES_PER_SCOPE = 2000
PARALLEL_NODES = 2


@dataclass
class DriveIndexSummary:
    files: int
    changed: int
    skipped_type: int
    pruned: int
    deferred: int = 0
    failed: int = 0
    root_gone: bool = False
    # The walk could not discover everything (the discovery cap or a truncated listing),
    # so it pruned nothing.
    capped: bool = False


@workflow.defn(name="DriveIndexingWorkflow")
class DriveIndexingWorkflow:
    @workflow.run
    async def run(
        self,
        tenant_id: str,
        scope: str,
        root_id: str,
        seen: list[str] | None = None,
        queue: list[str] | None = None,
        kept_files: list[str] | None = None,
        changed: int = 0,
        deferred: int = 0,
        failed: int = 0,
        skipped_type: int = 0,
        files: int = 0,
        started: str | None = None,
        root_gone: bool = False,
        walk_complete: bool = True,
        capped: bool = False,
        error_code: str | None = None,
        root_code: str | None = None,
        confirm_gone: bool = False,
    ) -> DriveIndexSummary:
        if started is None:
            started = workflow.now().isoformat()
        # A fresh start begins at the granted root; a continue-as-new carries the walk.
        if seen is None or queue is None:
            seen, queue = [root_id], [root_id]
        else:
            seen, queue = list(seen), list(queue)
        kept_files = list(kept_files or [])

        while queue:
            batch, queue = queue[:PARALLEL_NODES], queue[PARALLEL_NODES:]
            results = await workflow.execute_activity_method(
                DriveIndexActivities.sync_drive_nodes,
                args=[tenant_id, scope, batch],
                start_to_close_timeout=INDEX_TIMEOUT,
                retry_policy=RETRY,
            )
            for node_id, result in zip(batch, results):
                if result.status == GONE:
                    if node_id == root_id:
                        root_gone = True
                        root_code = result.error_code or "not_found"
                    continue
                if result.status == FAILED:
                    failed += 1
                    if error_code is None:
                        error_code = result.error_code
                    if node_id == root_id:
                        # An unreadable root — a folder or the one granted file — goes
                        # through the repeated-run guard, never a prune on one run.
                        root_gone = True
                        root_code = result.error_code or "not_found"
                    if not result.is_file:
                        # A folder the walk could not list: what is stored beneath it is
                        # unseen, not gone.
                        walk_complete = False
                if result.is_file:
                    files += 1
                    kept_files.append(node_id)
                    changed += int(result.changed)
                    deferred += int(result.budget_stopped)
                    skipped_type += int(result.skipped_type)
                if result.truncated:
                    walk_complete = False
                    capped = True
                for child in result.child_ids:
                    if child in seen:
                        continue
                    if len(seen) >= MAX_DRIVE_FILES_PER_SCOPE:
                        if not capped:
                            workflow.logger.warning(
                                "drive file cap reached tenant_id=%s scope=%s cap=%d",
                                tenant_id,
                                scope,
                                MAX_DRIVE_FILES_PER_SCOPE,
                            )
                        capped = True
                        walk_complete = False
                        continue
                    seen.append(child)
                    queue.append(child)

            if queue and workflow.info().is_continue_as_new_suggested():
                workflow.continue_as_new(
                    args=[
                        tenant_id,
                        scope,
                        root_id,
                        seen,
                        queue,
                        kept_files,
                        changed,
                        deferred,
                        failed,
                        skipped_type,
                        files,
                        started,
                        root_gone,
                        walk_complete,
                        capped,
                        error_code,
                        root_code,
                        confirm_gone,
                    ]
                )

        pruned: int = await workflow.execute_activity_method(
            DriveIndexActivities.prune_drive_scope,
            args=[
                tenant_id,
                scope,
                kept_files,
                started,
                root_gone,
                confirm_gone,
                walk_complete,
                root_code if root_gone else error_code,
            ],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY,
        )
        return DriveIndexSummary(
            files=files,
            changed=changed,
            skipped_type=skipped_type,
            pruned=pruned,
            deferred=deferred,
            failed=failed,
            root_gone=root_gone,
            capped=capped,
        )
