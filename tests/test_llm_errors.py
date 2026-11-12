"""call_llm error handling: a client error is final, anything else is retried."""

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from agentos.activities import AgentActivities, _is_client_error
from agentos.config import Settings
from agentos.connectors.notion import FakeNotionAPI
from agentos.llm import ScriptedLLMClient
from agentos.models import TurnContext
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for
from tests.conftest import ALICE, TENANT


class StatusError(Exception):
    """Stands in for an SDK APIStatusError, which carries ``status_code``."""

    def __init__(self, status_code: int):
        super().__init__(f"Error code: {status_code}")
        self.status_code = status_code


class NotionStatusError(Exception):
    def __init__(self, status: int):
        super().__init__(f"status {status}")
        self.status = status


def make_activities(agents, llm):
    return AgentActivities(
        settings=Settings(),
        registry=TenantRegistry(
            {agents.tenant_id: runtime_for(agents, FakeSlack(), FakeNotionAPI())}
        ),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=llm,
        tool_outputs=InMemoryToolOutputStore(),
    )


def turn_ctx() -> TurnContext:
    return TurnContext(
        tenant_id=TENANT,
        agent_id="it-helper",
        agent_version=1,
        requester_user_id=ALICE,
        channel="C1",
        thread_ts="1.1",
        channel_kind="channel",
    )


async def run_call_llm(agents, error):
    llm = ScriptedLLMClient()
    llm.queue(error)
    acts = make_activities(agents, llm)
    return await ActivityEnvironment().run(acts.call_llm, turn_ctx(), "conv-1")


async def test_call_llm_auth_error_is_non_retryable(agents):
    with pytest.raises(ApplicationError) as exc:
        await run_call_llm(agents, StatusError(401))
    assert exc.value.non_retryable is True
    assert exc.value.type == "StatusError"


async def test_call_llm_rate_limit_is_retried(agents):
    error = StatusError(429)
    with pytest.raises(StatusError) as exc:
        await run_call_llm(agents, error)
    assert exc.value is error


async def test_call_llm_server_error_is_retried(agents):
    error = StatusError(500)
    with pytest.raises(StatusError) as exc:
        await run_call_llm(agents, error)
    assert exc.value is error


def test_notion_client_error_rule_unchanged():
    assert _is_client_error(NotionStatusError(404)) is True
    assert _is_client_error(NotionStatusError(429)) is False
