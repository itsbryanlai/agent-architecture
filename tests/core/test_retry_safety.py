"""Retried history appends and Slack posts: no duplicate history, visible duplicate posts."""

import asyncio
import itertools
import logging

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.activities import WRITE_IN_PROGRESS
from agentos.connectors.notion import FakeNotionAPI
from agentos.llm import ScriptedLLMClient
from agentos.models import LLMResponse, ToolCall, TurnContext
from agentos.slack_client import FakeSlack, SlackHistoryUnavailable, post_metadata
from agentos.tenants import TenantRegistry, runtime_for
from agentos.workflows.conversation import TURN_FAILED, ConversationWorkflow
from tests.conftest import TENANT, eventually
from tests.helpers.conversation import (
    CHANNEL,
    THREAD,
    inbound,
    placeholders,
    start_conversation,
    stored_history,
)
from tests.helpers.llm_errors import make_activities
from tests.helpers.turns import assert_tool_pairs_valid


def text_of(message):
    return [b["text"] for b in message["content"] if b.get("type") == "text"]


async def test_append_history_rejects_empty_step_key(agents):
    acts = make_activities(agents, ScriptedLLMClient())
    with pytest.raises(ValueError):
        await ActivityEnvironment().run(
            acts.append_history, TENANT, "c1", "", "user", [{"type": "text", "text": "hi"}]
        )


async def test_a_tool_turn_keeps_all_four_messages_in_order(harness):
    """Fails if two steps of one turn collide and a stored message is dropped or reordered."""
    h = harness
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[ToolCall(id="tu_1", name="search_items", input={"query": "laptop"})],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Done", tool_calls=[], stop_reason="end_turn"),
    )

    await start_conversation(h, inbound("Ev1", "any laptop requests?"))
    await eventually(lambda: h.slack.updates())

    conv_id = ids.conversation_id(TENANT, CHANNEL, THREAD)

    async def reply_stored() -> bool:
        # The reply is stored after it was shown, so the update comes first.
        return len(await h.history.load(TENANT, conv_id)) == 4

    await eventually(reply_stored)
    stored = await h.history.load(TENANT, conv_id)
    assert [m["role"] for m in stored] == ["user", "assistant", "user", "assistant"]
    assert stored[1]["content"][0]["type"] == "tool_use"
    assert stored[2]["content"][0]["type"] == "tool_result"
    assert stored[3]["content"][0]["type"] == "text"


async def test_retried_append_does_not_duplicate_history(harness):
    h = harness
    h.llm.queue(LLMResponse(text="Hi there", tool_calls=[], stop_reason="end_turn"))
    h.history.fail_next_append_after_write()

    await start_conversation(h, inbound("Ev1", "hello"))
    await eventually(lambda: h.slack.updates())

    assert h.slack.updates()[0]["text"] == "Hi there"
    conv_id = ids.conversation_id(TENANT, CHANNEL, THREAD)

    async def reply_stored() -> bool:
        # The reply is stored after it was shown, so the update comes first.
        return len(await h.history.load(TENANT, conv_id)) >= 2

    await eventually(reply_stored)
    stored = await h.history.load(TENANT, conv_id)
    assert [m["role"] for m in stored] == ["user", "assistant"]
    assert [text_of(m) for m in stored if m["role"] == "user"] == [["hello"]]


async def test_two_messages_same_text_both_stored(harness):
    """Fails if a second message with the same text is dropped as a duplicate, or leaves the history invalid."""
    h = harness
    h.llm.queue(
        LLMResponse(text="one", tool_calls=[], stop_reason="end_turn"),
        LLMResponse(text="two", tool_calls=[], stop_reason="end_turn"),
    )

    handle = await start_conversation(h, inbound("Ev1", "same"))
    await eventually(lambda: len(h.slack.updates()) >= 1)

    await handle.signal(ConversationWorkflow.message, inbound("Ev2", "same"))
    await eventually(lambda: len(h.slack.updates()) >= 2)

    conv_id = ids.conversation_id(TENANT, CHANNEL, THREAD)
    stored = await h.history.load(TENANT, conv_id)
    users = [text_of(m) for m in stored if m["role"] == "user"]
    assert users == [["same"], ["same"]]
    assert_tool_pairs_valid(h.llm.calls[1]["messages"])


async def test_failed_finalize_stores_only_the_failure_notice_never_a_reply_nobody_saw(harness):
    """Fails if history keeps an answer Slack refused to show, or stores two assistant turns."""
    h = harness
    h.llm.queue(LLMResponse(text="The answer", tool_calls=[], stop_reason="end_turn"))
    h.slack.fail_next_update(ValueError("slack down"))

    await start_conversation(h, inbound("Ev1", "question"))
    await eventually(lambda: h.slack.updates())

    assert h.slack.updates()[0]["text"] == TURN_FAILED
    assert h.slack.updates()[0]["ts"] == placeholders(h)[0]["ts"]

    conv_id = ids.conversation_id(TENANT, CHANNEL, THREAD)
    stored = await stored_history(h, conv_id)
    roles = [m["role"] for m in stored]
    assert roles == ["user", "assistant"]
    assert all(a != b for a, b in itertools.pairwise(roles))
    assert text_of(stored[1]) == [TURN_FAILED]
    assert not any("The answer" in text_of(m) for m in stored)


class LostResponseSlack(FakeSlack):
    """Slack accepts the first post and the answer never arrives, as when a connection drops."""

    def __init__(self) -> None:
        super().__init__()
        self.lose_next_response = True

    async def post_message(self, channel, text, thread_ts=None, blocks=None, metadata=None) -> str:
        ts = await super().post_message(
            channel, text, thread_ts=thread_ts, blocks=blocks, metadata=metadata
        )
        if self.lose_next_response:
            self.lose_next_response = False
            raise ConnectionError("response lost")
        return ts


class SlowFirstPostSlack(FakeSlack):
    """The first post waits until the test lets it answer."""

    def __init__(self) -> None:
        super().__init__()
        self.first_started = asyncio.Event()
        self.answer_first = asyncio.Event()
        self._attempts = 0

    async def post_message(self, channel, text, thread_ts=None, blocks=None, metadata=None) -> str:
        self._attempts += 1
        first = self._attempts == 1
        if first:
            self.first_started.set()
            await self.answer_first.wait()
        return await super().post_message(
            channel, text, thread_ts=thread_ts, blocks=blocks, metadata=metadata
        )


def activities_with(agents, slack):
    acts = make_activities(agents, ScriptedLLMClient())
    acts.registry = TenantRegistry({agents.tenant_id: runtime_for(agents, slack, FakeNotionAPI())})
    acts.visibility_interval = 0  # the tests wait for no real seconds
    return acts


def turn_context(thread_ts: str = THREAD) -> TurnContext:
    return TurnContext(TENANT, "it-helper", 1, "U1", CHANNEL, thread_ts, "im")


async def post(acts, key: str = "k1", thread_ts: str = THREAD) -> str:
    """Run the post activity. An empty ``thread_ts`` posts at the top of the channel."""
    return await ActivityEnvironment().run(
        acts.post_in_thread, turn_context(thread_ts), "secret message text", key
    )


async def test_a_post_whose_result_was_lost_after_it_was_recorded_is_not_posted_again(agents):
    """Fails if a retried post activity posts a second message once the first one's ts was stored."""
    slack = FakeSlack()
    acts = activities_with(agents, slack)

    first = await post(acts)
    retried = await post(acts)

    assert retried == first
    assert len(slack.posts()) == 1


@pytest.mark.parametrize("thread_ts", [THREAD, ""], ids=["thread post", "top-level post"])
async def test_a_lost_slack_response_is_found_by_its_marker_and_not_posted_again(agents, thread_ts):
    """Fails if a post whose answer never arrived is posted a second time instead of being found in Slack."""
    slack = LostResponseSlack()
    acts = activities_with(agents, slack)
    with pytest.raises(ConnectionError):
        await post(acts, thread_ts=thread_ts)

    ts = await post(acts, thread_ts=thread_ts)

    # The retry returned, so any second post would be visible by now.
    assert [p["ts"] for p in slack.posts()] == [ts]
    assert "secret message text" not in str(slack.posts()[0]["metadata"])


async def test_overlapping_attempts_of_one_post_make_one_post(agents):
    """Fails if a second attempt posts while the first is still posting, or the two name different messages."""
    slack = SlowFirstPostSlack()
    acts = activities_with(agents, slack)
    first = asyncio.create_task(post(acts))
    await slack.first_started.wait()

    with pytest.raises(ApplicationError) as busy:
        await post(acts)
    slack.answer_first.set()
    ts = await first

    assert busy.value.type == WRITE_IN_PROGRESS
    assert await post(acts) == ts
    assert [p["ts"] for p in slack.posts()] == [ts]


async def test_a_post_whose_lookup_is_refused_is_posted_again_and_logged_without_its_text(
    agents, caplog
):
    """Fails if a refused history lookup blocks the post, goes unlogged, or logs the message text or repeats the scope notice."""
    slack = LostResponseSlack()
    slack.refuse_history(SlackHistoryUnavailable("missing_scope", "channels:history"))
    acts = activities_with(agents, slack)
    with pytest.raises(ConnectionError):
        await post(acts)

    with caplog.at_level(logging.WARNING, logger="agentos.activities"):
        ts = await post(acts)
        await post(acts, key="k2")  # a later refusal for the same tenant adds no second notice

    assert (await acts.idem.get(TENANT, "k1"))["ts"] == ts
    lines = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    duplicates = [line for line in lines if "possible duplicate Slack post" in line]
    assert len(duplicates) == 1 and TENANT in duplicates[0] and "k1" in duplicates[0]
    assert len([line for line in lines if "channels:history" in line]) == 1
    assert "secret message text" not in caplog.text


async def test_an_old_pending_marker_without_a_ts_is_looked_up_before_posting(agents):
    """Fails if a post an older version marked pending but never recorded is posted again without a lookup."""
    slack = FakeSlack()
    acts = activities_with(agents, slack)
    already = await slack.post_message(
        CHANNEL,
        "posted by the attempt that crashed",
        thread_ts=THREAD,
        metadata=post_metadata(ids.slack_post_marker(TENANT, "k1")),
    )
    await acts.idem.put_if_absent(TENANT, "k1:pending", {"channel": CHANNEL})

    assert await post(acts) == already
    assert [p["ts"] for p in slack.posts()] == [already]
