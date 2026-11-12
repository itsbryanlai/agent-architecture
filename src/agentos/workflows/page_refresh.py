"""PageRefreshWorkflow: bring one Notion page up to date in the knowledge index.

Started by a signed Notion page event with signal-with-start, one workflow per
page, so a burst of events for one page lands in one running workflow. Rules it
keeps:

* It waits until no event has arrived for ``QUIET``, then refreshes the page,
  then loops if another event arrived during the refresh.
* History carries page and scope IDs only; page text is read and embedded
  inside the activity and never reaches workflow history.
* Workflow code is deterministic: no I/O, no clocks, no randomness.
"""

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agentos.knowledge.indexing import IndexActivities
    from agentos.workflows import INDEX_TIMEOUT, RETRY

QUIET = timedelta(seconds=30)


@workflow.defn(name="PageRefreshWorkflow")
class PageRefreshWorkflow:
    def __init__(self) -> None:
        self._dirty = False

    @workflow.signal
    def changed(self) -> None:
        self._dirty = True

    @workflow.run
    async def run(self, tenant_id: str, page_id: str) -> int:
        refreshes = 0
        while True:
            # Wait for a quiet period: every new signal restarts the wait.
            while True:
                self._dirty = False
                try:
                    await workflow.wait_condition(lambda: self._dirty, timeout=QUIET)
                except TimeoutError:
                    break
            await workflow.execute_activity_method(
                IndexActivities.refresh_page,
                args=[tenant_id, page_id],
                start_to_close_timeout=INDEX_TIMEOUT,
                retry_policy=RETRY,
            )
            refreshes += 1
            if not self._dirty:
                return refreshes
