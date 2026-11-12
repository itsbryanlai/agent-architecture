"""Data types shared by the gateway, the workflows and the activities.

Everything here is passed through Temporal, so each type is a plain dataclass
of primitives, lists, dicts and other dataclasses. Workflows carry only
references and small values: chat history lives in Postgres, never in these
objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class InboundMessage:
    event_id: str  # Slack event_id, used to drop duplicates
    user_id: str  # Slack user ID of the sender
    text: str
    channel: str
    thread_ts: str  # thread root ts (event.thread_ts or event.ts)
    channel_kind: str  # "im" or "channel"


@dataclass
class ConversationRef:
    tenant_id: str
    agent_id: str
    channel: str
    thread_ts: str


@dataclass
class ApprovalDecision:
    nonce: str
    user_id: str  # Slack user ID of whoever clicked
    approved: bool


@dataclass
class ExternalUpdate:
    page_id: str
    source: str  # "notion_webhook"


@dataclass
class TurnContext:
    """Who is asking, where, and which agent version answers.

    The requester's identity is set by the runtime from the Slack event. It is
    never taken from LLM output.
    """

    tenant_id: str
    agent_id: str
    agent_version: int
    requester_user_id: str
    channel: str
    thread_ts: str
    channel_kind: str


@dataclass
class ToolCall:
    id: str
    name: str
    input: dict


@dataclass
class Citation:
    """One numbered source the model may cite, like "[3]"."""

    marker: int  # the number the model writes
    scope: str  # e.g. "notion:page:<32-hex root id>"
    page_id: str  # normalized 32-hex Notion page id


@dataclass
class ToolResult:
    """What a tool call returned, without any document content.

    ``content`` holds inline text only for results the runtime itself made
    (drafting text, validation errors). Output from a tool that read data is
    kept in the tool output store and marked ``stored``, so workflow history
    carries a reference and never the text.
    """

    tool_use_id: str
    content: str | None = None
    is_error: bool = False
    stored: bool = False  # True: the text is in the tool output store
    citations: list[Citation] = field(default_factory=list)


@dataclass
class LLMResponse:
    text: str | None
    tool_calls: list[ToolCall]
    stop_reason: str
    # The assistant content blocks exactly as the model returned them
    # (including any thinking blocks), so they can be written back to history
    # unchanged. Empty when the client has no raw blocks (the scripted fake);
    # history is then rebuilt from ``text`` and ``tool_calls``.
    content: list[dict] = field(default_factory=list)


@dataclass
class TaskSpec:
    ctx: TurnContext
    tool_name: str
    # The frozen, validated item to create. What the requester approves is
    # exactly what runs; nothing changes this after the card is posted.
    payload: dict
    approval_nonce: str


@dataclass
class NotionItem:
    page_id: str
    url: str
    title: str
    status: str | None
