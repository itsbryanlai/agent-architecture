"""The nightly retention schedule, and the bounds that fix how long a task can live."""

import copy

import pytest
from temporalio.client import ScheduleAlreadyRunningError

from agentos import ids
from agentos.config import MAX_APPROVAL_EXPIRES_HOURS, MAX_WATCH_DAYS, ConfigError, parse_agent
from agentos.retention import MIN_TASK_DAYS, apply_schedule
from tests.conftest import TENANT, agents_dict


class FakeScheduleClient:
    """Create fails when the schedule exists, as Temporal's does; update replaces it."""

    def __init__(self) -> None:
        self.schedules: dict = {}
        self.log: list[tuple[str, str]] = []

    async def create_schedule(self, schedule_id, schedule):
        if schedule_id in self.schedules:
            raise ScheduleAlreadyRunningError()
        self.schedules[schedule_id] = schedule
        self.log.append(("create", schedule_id))

    def get_schedule_handle(self, schedule_id):
        client = self

        class Handle:
            async def update(self, updater):
                client.schedules[schedule_id] = updater(None).schedule
                client.log.append(("update", schedule_id))

        return Handle()


async def test_scheduling_twice_leaves_one_schedule_per_tenant():
    """Fails if a second run duplicates the schedule, errors on it, or touches another tenant's."""
    client = FakeScheduleClient()

    first = await apply_schedule(client, TENANT)
    second = await apply_schedule(client, TENANT)
    await apply_schedule(client, "globex")

    assert (first, second) == ("created", "updated")
    assert sorted(client.schedules) == sorted(
        [ids.retention_schedule_id(TENANT), ids.retention_schedule_id("globex")]
    )
    assert client.log == [
        ("create", ids.retention_schedule_id(TENANT)),
        ("update", ids.retention_schedule_id(TENANT)),
        ("create", ids.retention_schedule_id("globex")),
    ]
    assert client.schedules[ids.retention_schedule_id(TENANT)].action.id == (
        ids.retention_workflow_id(TENANT)
    )


@pytest.mark.parametrize(
    ("block", "key", "value"),
    [
        ("approval", "expires_after_hours", MAX_APPROVAL_EXPIRES_HOURS + 1),
        ("approval", "expires_after_hours", 0),
        ("follow_up", "max_watch_days", MAX_WATCH_DAYS + 1),
        ("follow_up", "max_watch_days", "forever"),
    ],
)
def test_a_definition_cannot_outlive_the_retention_floor(block, key, value):
    """Fails if an agent may ask for an approval window or watch longer than the floor covers."""
    raw = copy.deepcopy(agents_dict()["agents"]["it-helper"])
    raw[block][key] = value

    with pytest.raises(ConfigError, match=key):
        parse_agent("it-helper", raw, 1)


def test_the_largest_allowed_definition_fits_inside_the_floor():
    """Fails if a definition at the validated maxima can run longer than the retention floor."""
    raw = copy.deepcopy(agents_dict()["agents"]["it-helper"])
    raw["approval"]["expires_after_hours"] = MAX_APPROVAL_EXPIRES_HOURS
    raw["follow_up"]["max_watch_days"] = MAX_WATCH_DAYS

    agent = parse_agent("it-helper", raw, 1)

    task_days = agent.approval.expires_after_hours / 24 + agent.follow_up.max_watch_days
    assert task_days < MIN_TASK_DAYS
