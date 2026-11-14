"""Data types shared by the gateway, the workflows and the activities.

Everything here is passed through Temporal, so each type is a plain dataclass
of primitives, lists, dicts and other dataclasses. Workflows carry only
references and small values: chat history lives in Postgres, never in these
objects.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime

from agentos.slack_format import escape_slack

# Texts the runtime itself posts through the reply path. A model answer is
# every other text, so the audit log tells them apart by these.
OUT_OF_STEPS = "I couldn't finish that. Try rephrasing."
TURN_FAILED = "Sorry, something went wrong and I couldn't answer that. Please try again."
HAND_OFF_ANNOUNCEMENT = "{agent} here, picking this up."


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
    database_id: str = ""  # the granted database the page sits in, when known


# Limits of the places a task's text goes: Notion takes 2000 characters per text
# item; the approval card shows a field in a section field (2000) or a section's
# text (3000) behind a bold label, with ``&``, ``<`` and ``>`` encoded.
NOTION_TEXT_MAX = 2000
# What a comment's signature ("— <agent name>, ref <8 hex>") may add.
COMMENT_SIGNATURE_MAX = 100
SIGNATURE_NAME_MAX = 60

# The fields a requester may edit on an approval card, per tool, with the
# longest value each may hold. Every path that accepts a value (the model's
# first draft, an edit) checks ``check_field``, so an accepted value always fits
# Notion and the card.
EDITABLE_FIELDS = {
    "create_item": {"title": 150, "details": NOTION_TEXT_MAX - 100},
    "add_comment": {"comment": NOTION_TEXT_MAX - COMMENT_SIGNATURE_MAX},
}

# Room on the card for the escaped value, after the bold label above it.
_CARD_ROOM = {
    "title": 2000 - len("*Title*\n"),
    "details": 2000 - len("*Details*\n"),
    "comment": 3000 - len("*Comment*\n"),
}


def check_field(tool_name: str, name: str, value: str) -> str | None:
    """Why ``value`` may not be this field of ``tool_name``, or None if it fits."""
    limit = EDITABLE_FIELDS[tool_name][name]
    if len(value) > limit:
        return (
            f"'{name}' is {len(value)} characters; the limit is {limit}. "
            "Shorten it and call the tool again."
        )
    if len(escape_slack(value)) > _CARD_ROOM[name]:
        return (
            f"'{name}' has too many '&', '<' or '>' characters: they are encoded on the "
            f"approval card and the result is over its {_CARD_ROOM[name]}-character room. "
            "Shorten it or use fewer of them, and call the tool again."
        )
    return None


@dataclass
class EditRequest:
    """A saved edit from the Edit form on an approval card."""

    nonce: str  # the nonce of the draft being edited
    user_id: str  # Slack user ID of whoever submitted the form
    fields: dict  # str -> str; keys are the tool's editable fields


@dataclass
class DraftView:
    """What the task's "draft" query returns: the current draft and its nonce."""

    tool_name: str
    fields: dict  # current editable values
    nonce: str
    requester_user_id: str
    editable: bool  # True only while awaiting approval and under the edit limit


AGENT_AUDIT_KINDS = (
    "agent_create",
    "agent_draft",
    "agent_submit",
    "agent_withdraw",
    "agent_reject",
    "agent_publish",
)
MEMBER_AUDIT_KINDS = ("member_add", "member_remove", "member_role")
AUDIT_KINDS = (
    ("answer", "tool_call", "hand_off", "approval", "write")
    + AGENT_AUDIT_KINDS
    + MEMBER_AUDIT_KINDS
)
AUDIT_OUTCOMES = ("ok", "error")
_SHA256_HEX = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class AuditEvent:
    """One thing an agent did, as references and hashes only.

    Nothing here holds chat text, document text or a tool's input or output:
    those are kept as SHA-256 hashes of their canonical JSON, and everything
    else is an ID. ``at`` is set by the store when the event is recorded.
    """

    tenant_id: str
    event_key: str  # unique per tenant: a retried activity records once
    kind: str
    agent_id: str | None  # None only for a membership event
    agent_version: int | None
    requester_user_id: str
    identity: str  # whose access was used, e.g. "agent:it-helper"
    outcome: str = "ok"
    conversation_id: str | None = None
    task_id: str | None = None
    tool: str | None = None
    target: str | None = None  # a Notion page or database ID, or a specialist ID
    input_sha256: str | None = None
    output_sha256: str | None = None
    approver_user_id: str | None = None
    at: datetime | None = None
    # The store's insertion order, set when the event is read back: it grows with
    # every event recorded and is what a page of the log continues from.
    seq: int | None = None

    def __post_init__(self) -> None:
        if not self.tenant_id or not self.event_key:
            raise ValueError("an audit event needs a tenant_id and an event_key")
        if self.kind not in AUDIT_KINDS:
            raise ValueError(f"invalid audit kind {self.kind!r}")
        if self.kind not in MEMBER_AUDIT_KINDS and (
            not self.agent_id or self.agent_version is None
        ):
            raise ValueError(f"audit kind {self.kind!r} needs an agent_id and an agent_version")
        if self.outcome not in AUDIT_OUTCOMES:
            raise ValueError(f"invalid audit outcome {self.outcome!r}")
        for digest in (self.input_sha256, self.output_sha256):
            if digest is not None and not _SHA256_HEX.fullmatch(digest):
                raise ValueError("audit hashes must be 64 lowercase hex characters")
