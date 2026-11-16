"""Conversation workflow helpers: the inbound message, the thread, and starting a conversation."""

from agentos import ids
from agentos.config import TASK_QUEUE
from agentos.models import ConversationRef, InboundMessage
from agentos.workflows.conversation import ConversationWorkflow
from tests.conftest import ALICE, TENANT

CHANNEL = "C_GENERAL"


THREAD = "1700000000.000100"


def inbound(event_id: str, text: str = "hello") -> InboundMessage:
    return InboundMessage(
        event_id=event_id,
        user_id=ALICE,
        text=text,
        channel=CHANNEL,
        thread_ts=THREAD,
        channel_kind="channel",
    )


async def start_conversation(h, msg: InboundMessage):
    return await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "it-helper", CHANNEL, THREAD),
        id=ids.conversation_id(TENANT, CHANNEL, THREAD),
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[msg],
    )


def placeholders(h):
    return [p for p in h.slack.posts() if p["text"] == "Thinking…"]
