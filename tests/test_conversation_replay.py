"""A thread recorded before the front door still replays on the current workflow code.

The histories in ``tests/fixtures/replay`` were recorded with the conversation
workflow as it was before hand-offs existed, using fakes and the default data
converter. Replaying them fails with a non-determinism error if the turn loop
issues a different command for a thread that never hands off.
"""

from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import TaskWorkflow

FIXTURES = Path(__file__).parent / "fixtures" / "replay"


@pytest.mark.parametrize("name", ["plain_turn", "step_limit", "partial_failure"])
async def test_recorded_thread_without_hand_off_replays(name):
    history = WorkflowHistory.from_json(
        f"acme:conv:{name}", (FIXTURES / f"{name}.json").read_text()
    )

    await Replayer(workflows=[ConversationWorkflow, TaskWorkflow]).replay_workflow(history)
