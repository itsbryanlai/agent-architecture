"""Every recorded history still replays on the current workflow code.

The files in ``tests/fixtures/replay`` come from ``scripts/record_replay_fixtures.py`` (see the
README beside them). Replaying one fails with a non-determinism error when a workflow change
alters the commands an existing history would issue, which is a workflow that was running at
deploy time failing on its next step.
"""

import pytest

from tests.helpers.replay_support import FIXTURES, load_fixture, replay

NAMES = sorted(p.stem for p in FIXTURES.glob("*.json"))


def test_there_are_fixtures_for_every_workflow_type_and_both_shapes():
    """Fails if the fixture directory is emptied or loses a workflow type, so replay checks nothing."""
    started = {
        load_fixture(FIXTURES / f"{n}.json")[0]
        .events[0]
        .workflow_execution_started_event_attributes.workflow_type.name
        for n in NAMES
    }

    assert started == {
        "ConversationWorkflow",
        "TaskWorkflow",
        "CreatorAccessWorkflow",
        "TestRunWorkflow",
        "DriveIndexingWorkflow",
    }
    assert any("_inline" in n for n in NAMES) and any("_stored" in n for n in NAMES)


@pytest.mark.parametrize("name", NAMES)
async def test_a_recorded_history_replays(name, tmp_path):
    """Fails if the current workflow code issues other commands than the code that recorded this."""
    await replay(FIXTURES / f"{name}.json", tmp_path / "keys")
