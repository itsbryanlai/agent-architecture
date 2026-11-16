"""call_llm error handling: a client error is final, anything else is retried."""

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from agentos.activities import _is_client_error
from agentos.llm import ScriptedLLMClient
from agentos.models import TurnContext
from tests.conftest import ALICE, TENANT
from tests.helpers.llm_errors import StatusError, make_activities


class NotionStatusError(Exception):
    def __init__(self, status: int):
        super().__init__(f"status {status}")
        self.status = status


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


@pytest.mark.parametrize("status", [429, 500])
async def test_call_llm_rate_limit_and_server_error_are_retried(agents, status):
    """Fails if a rate limit or a server error is made final, so Temporal never retries the call."""
    error = StatusError(status)
    with pytest.raises(StatusError) as exc:
        await run_call_llm(agents, error)
    assert exc.value is error


def test_notion_client_error_rule_unchanged():
    assert _is_client_error(NotionStatusError(404)) is True
    assert _is_client_error(NotionStatusError(429)) is False
