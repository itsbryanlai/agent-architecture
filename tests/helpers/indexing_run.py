"""A helper that runs the indexing workflow over a fake Notion tree."""

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.workflows.indexing import IndexingWorkflow
from tests.conftest import (
    ROOT_A,
    TENANT,
)

SCOPE_A = ids.knowledge_scope(ROOT_A)


async def run_index(h, scope=SCOPE_A, root=ROOT_A):
    return await h.client.execute_workflow(
        IndexingWorkflow.run,
        args=[TENANT, scope, root.replace("-", "")],
        id=ids.index_workflow_id(TENANT, scope),
        task_queue=TASK_QUEUE,
    )
