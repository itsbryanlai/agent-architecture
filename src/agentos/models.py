"""Data types shared by the gateway, the workflows and the activities.

Everything here is passed through Temporal, so each type is a plain dataclass
of primitives, lists, dicts and other dataclasses. A signal, a workflow argument
or an activity argument carries IDs and small values, never chat or document
content: the text a person wrote, the model's reply, a tool call's input and a
task's payload are kept in Postgres (``agentos.content_store``, chat history)
and read there by the activities that need them.

Two classes keep fields that held content before that change (``InboundMessage.text``,
``TaskSpec.payload``, ``EditRequest.fields``) because histories recorded then still
replay on them. A message, task or edit made now leaves them empty and sets the field
that names the stored content (``text_stored``, ``draft_ref``, ``edit_id``).
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
NO_ANSWER = "Sorry, I couldn't come up with an answer to that."
CUT_SHORT = "\n\n(reply cut short)"


@dataclass
class InboundMessage:
    event_id: str  # Slack event_id, used to drop duplicates
    user_id: str  # Slack user ID of the sender
    text: str  # empty when ``text_stored``: the text is in ``inbound_events`` under the event ID
    channel: str
    thread_ts: str  # thread root ts (event.thread_ts or event.ts)
    channel_kind: str  # "im" or "channel"
    # True when the gateway kept the text in ``inbound_events`` and the signal carries none.
    # A signal recorded before that has the text inline and is handled as it was.
    text_stored: bool = False


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
    source: str  # "notion_webhook" (the gateway) or "my_tasks" (the in-thread poke)


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


# ``ToolCall.kind`` of a tool that only reads. A write's kind is the task that carries it out,
# one of the keys of ``EDITABLE_FIELDS``.
READ_KIND = "read"


@dataclass
class ToolCall:
    id: str
    name: str
    input: dict
    # What the connector manifests say about this call, set by the ``call_llm`` activity and
    # never by the model: ``kind`` is "read", the kind of task a write becomes
    # ("create_item", "add_comment"), or "" for a tool no manifest offers; ``problem`` is why
    # the input does not fit the tool's schema, worded for the model. Both are empty on a call
    # recorded before they existed.
    kind: str = ""
    problem: str = ""


def assistant_blocks(resp: LLMResponse) -> list[dict]:
    """The assistant turn to store: the model's own blocks when available.

    A tool call the blocks lack is added, so every call has its ``tool_use`` block.
    """
    blocks = [dict(b) for b in resp.content]
    if not blocks and resp.text:
        blocks.append({"type": "text", "text": resp.text})
    listed = {b.get("id") for b in blocks if b.get("type") == "tool_use"}
    for call in resp.tool_calls:
        if call.id not in listed:
            blocks.append(
                {"type": "tool_use", "id": call.id, "name": call.name, "input": call.input}
            )
    return blocks


@dataclass
class Citation:
    """One numbered source the model may cite, like "[3]"."""

    marker: int  # the number the model writes
    scope: str  # e.g. "notion:page:<32-hex root id>"
    page_id: str  # normalized 32-hex Notion page id
    # The passage's distance from the question, so the reply can name the nearest pages it
    # searched. None on a citation recorded before distances were carried.
    distance: float | None = None


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
class Usage:
    """What one model call used, as the provider reported it.

    ``input_tokens`` excludes the tokens served from or written to the prompt
    cache, which are counted separately because they are priced differently.
    ``model`` is the name the provider reported; None when it reported none.
    """

    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    model: str | None = None


@dataclass
class ToolCallRef:
    """A tool call as the workflow sees it: what it is, never what the model put in it.

    The input is stored by tool-use ID (``tool_inputs``) and read by the activity that runs
    the call. ``input_sha256`` lets the workflow tell two identical calls apart without
    reading either; ``target`` is the specialist a ``hand_off`` names, when it is a
    well-formed agent ID, else empty.
    """

    id: str
    name: str
    kind: str = ""
    problem: str = ""
    input_sha256: str = ""
    target: str = ""


@dataclass
class StepResult:
    """What one model step decided, with the content left in Postgres.

    The assistant message is stored under the step key by the ``call_model`` activity. A
    final reply (no ``tool_calls``) waits in ``model_steps`` until the activity that shows it
    stores it in history.
    """

    stop_reason: str
    tool_calls: list[ToolCallRef] = field(default_factory=list)
    has_text: bool = False


@dataclass
class DraftOutcome:
    """What drafting a task stored: the revision, or why the model's input cannot be a task.

    ``error`` is worded for the model and holds no content (a missing field, a length).
    """

    error: str = ""
    revision: int = 0
    database_id: str = ""


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
    usage: Usage = field(default_factory=Usage)


@dataclass
class WatchState:
    """What a watching task carries into its next run: IDs, a status value, times and flags.

    Never a title, a comment or a link: those are read from Postgres when a line is posted.
    """

    page_id: str
    last_status: str | None
    last_change: datetime
    stale_sent: bool
    # When the watch ends, whatever the number of runs it takes.
    deadline: datetime
    # A poke arrived and has not been answered by a poll yet.
    poked: bool = False
    # Nudges this task has run; the live stale offer's nonce (empty when none); and the
    # nudge task in flight (empty when none). A watch recorded before nudges carries none.
    nudges_used: int = 0
    nudge_nonce: str = ""
    nudge_child: str = ""
    # A click or a report the watch loop has not applied yet; a hop must not drop it.
    nudge_requested: bool = False
    nudge_done: bool = False


@dataclass
class NudgeRequest:
    """A click on a stale notice's "Nudge the assignee" button.

    The nonce is the offer's, issued when the stale notice was posted; the user is the
    signed clicker. The workflow accepts it only from the requester, on the live offer.
    """

    nonce: str
    user_id: str


@dataclass
class NudgeDone:
    """A nudge task tells the watch it came from: the comment landed, the clock restarts.

    Sent only when the nudge ran; a declined, expired or failed nudge sends nothing.
    """

    child_id: str


@dataclass
class NudgePrepared:
    """What preparing a nudge found: the draft is stored, or there is nobody to nudge.

    ``refused`` is True when the assignee resolved to the requester themselves.
    ``dm`` says the assignee matched a Slack user, so the nudge includes a DM.
    """

    refused: bool = False
    dm: bool = False


@dataclass
class TaskSpec:
    ctx: TurnContext
    tool_name: str
    # Empty on a task made now: the payload is a revision of ``task_drafts`` (``draft_ref``,
    # ``revision``). A task recorded before that carries its frozen payload here.
    payload: dict
    approval_nonce: str
    # The task ID the draft revisions are stored under; empty on a task that carries its payload.
    draft_ref: str = ""
    # The revision the card shows and the approval names. An edit stores the next revision and
    # a new nonce, so an approval always binds to one (revision, nonce).
    revision: int = 0
    # The Notion database the task writes to (a reference). Empty until a comment's page is resolved.
    database_id: str = ""
    # Set on a continue-as-new: the task is already past approval and only watches.
    resume: WatchState | None = None
    # The watch that started this nudge task; it is told when the nudge ran. Empty otherwise.
    parent_id: str = ""
    # Whether the approved nudge includes a DM (the assignee matched a Slack user).
    # Set by the watch from the prepared draft; edits change only the text, not this.
    nudge_dm: bool = False
    # Set by the workflow when the tenant's policy lets this comment run without the
    # requester's approval (D-43): the write then records no approval row, no approver,
    # and outcome 'policy'. Never set from the model's input.
    under_policy: bool = False


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
# What a comment's signature ("— <agent name>, ref <64 hex>") may add: 4 characters
# before the name, 6 between name and ref, 64 for the ref, so 100 leaves 26 for the name.
COMMENT_SIGNATURE_MAX = 100
SIGNATURE_NAME_MAX = 26

# The assignee name a nudge's plain-name mention may carry, capped when the draft is
# frozen. A Notion name can be all "&", which escape to five characters each on the card.
NUDGE_NAME_MAX = 80

# The fields a requester may edit on an approval card, per tool, with the
# longest value each may hold. Every path that accepts a value (the model's
# first draft, an edit) checks ``check_field``, so an accepted value always fits
# Notion and the card.
EDITABLE_FIELDS = {
    "create_item": {"title": 150, "details": NOTION_TEXT_MAX - 100},
    "add_comment": {"comment": NOTION_TEXT_MAX - COMMENT_SIGNATURE_MAX},
    # A nudge's editable text goes into both the comment and the DM; the mention and the
    # DM's frame are the runtime's, rebuilt from the approved revision. The comment also
    # carries a mention: worst case a plain-name one ("@<name> ", the name capped) or a
    # real one's leading space, so the cap leaves its room under Notion's 2000 as well.
    "nudge": {"comment": NOTION_TEXT_MAX - COMMENT_SIGNATURE_MAX - (NUDGE_NAME_MAX + len("@ "))},
}

# Room on the card for the escaped value, after the bold label above it.
_CARD_ROOM = {
    "title": 2000 - len("*Title*\n"),
    "details": 2000 - len("*Details*\n"),
    "comment": 3000 - len("*Comment*\n"),
}
# A nudge's comment preview shares its section with the mention ("@<name> ") behind a
# longer label. The name is Notion-written, so its worst escaped size (five characters
# per "&") is reserved too, and the section fits Slack's 3000 even for a hostile name.
_NUDGE_COMMENT_ROOM = 3000 - len("*Comment on the item*\n") - (len("@ ") + 5 * NUDGE_NAME_MAX)


def _field_problem(tool_name: str, name: str, value: str) -> tuple[str, str] | None:
    """Why ``value`` does not fit, and what the model should do about it."""
    limit = EDITABLE_FIELDS[tool_name][name]
    if len(value) > limit:
        return (
            f"'{name}' is {len(value)} characters; the limit is {limit}.",
            "Shorten it and call the tool again.",
        )
    room = _NUDGE_COMMENT_ROOM if tool_name == "nudge" else _CARD_ROOM[name]
    if len(escape_slack(value)) > room:
        what = (
            f"'{name}' has too many '&', '<' or '>' characters: they are encoded on the "
            f"approval card and the result is over its {room}-character room."
        )
        return what, "Shorten it or use fewer of them, and call the tool again."
    return None


def check_field(tool_name: str, name: str, value: str) -> str | None:
    """Why ``value`` may not be this field of ``tool_name``, or None if it fits.

    The message is written for the model, which is told to call the tool again.
    """
    problem = _field_problem(tool_name, name, value)
    return " ".join(problem) if problem else None


def field_error(tool_name: str, name: str, value: str) -> str | None:
    """The same check as :func:`check_field`, worded for a person editing the field."""
    problem = _field_problem(tool_name, name, value)
    return problem[0] if problem else None


@dataclass
class EditRequest:
    """A saved edit from the Edit form on an approval card."""

    nonce: str  # the nonce of the draft being edited
    user_id: str  # Slack user ID of whoever submitted the form
    fields: dict  # str -> str; keys are the tool's editable fields. Empty when ``edit_id`` is set
    # The ``inbound_events`` row that holds the saved values; empty on an edit recorded with them inline.
    edit_id: str = ""


@dataclass
class DraftView:
    """What the task's "draft" query returns: which draft is current, and its nonce.

    A task made now returns no field values: ``revision`` names the row of ``task_drafts``
    that holds them. ``fields`` is filled only for a task that carries its payload.
    """

    tool_name: str
    fields: dict  # current editable values; empty when ``revision`` is set
    nonce: str
    requester_user_id: str
    editable: bool  # True only while awaiting approval and under the edit limit
    revision: int = 0


AGENT_AUDIT_KINDS = (
    "agent_create",
    "agent_draft",
    "agent_edit",
    "agent_import",
    "agent_submit",
    "agent_withdraw",
    "agent_reject",
    "agent_publish",
)
MEMBER_AUDIT_KINDS = ("member_add", "member_remove", "member_role")
AUDIENCE_AUDIT_KINDS = (
    "audience_refused",
    "audience_confirm",
    "audience_pause",
    "audience_resume",
)
# A resume may close a pause after the agent's published version is gone: the agent, no version.
AGENT_NO_VERSION_AUDIT_KINDS = ("audience_resume",)
# A budget belongs to an agent or to the whole tenant, not to a version of an agent.
BUDGET_AUDIT_KINDS = ("budget_set",)
# A kill switch belongs to an agent or to the whole tenant, not to a version of an agent.
SWITCH_AUDIT_KINDS = ("switch_off", "switch_on")
# A reprice of a month's unpriced usage belongs to the tenant, run by an owner from the command line.
USAGE_AUDIT_KINDS = ("usage_repriced",)
# A tenant's retention limits, set by an owner from the command line, belong to the tenant.
RETENTION_AUDIT_KINDS = ("retention_set",)
# Choosing grants from Notion, and an approver accepting that a version widens access.
ACCESS_AUDIT_KINDS = ("grant_pick", "widening_confirm")
# A tenant's Notion webhook token, shown, confirmed or cleared by a host command.
WEBHOOK_AUDIT_KINDS = ("webhook_token",)
# A tenant's payload key, created or rotated in the database by a host command.
PAYLOAD_KEY_AUDIT_KINDS = ("payload_key",)
# A tenant created by the Slack OAuth install, its bot token replaced by a reinstall,
# or its app uninstalled (the tenant disabled and its Slack credential deleted).
INSTALL_AUDIT_KINDS = ("install", "uninstall")
# An agent's grant check flipping its health: the agent and the version checked.
HEALTH_AUDIT_KINDS = ("agent_health",)
# A tenant's Notion workspace connected or disconnected in the console; never the token.
CONNECTOR_AUDIT_KINDS = ("connector",)
# A tenant's default agent and channel routing, set by an admin in the console.
TENANT_SETTINGS_AUDIT_KINDS = ("tenant_settings",)
# A tenant's approval policy for a low-risk action, changed by an admin in the console.
POLICY_AUDIT_KINDS = ("policy_change",)
# A person's 👍/👎 on an answer: the agent and version that answered, never the reason text.
FEEDBACK_AUDIT_KINDS = ("feedback",)
# A creator's test cases on a version, and a test run of them: references and hashes only,
# never a question's text or a case's result detail.
TEST_AUDIT_KINDS = ("test_case", "test_run")
AUDIT_KINDS = (
    ("answer", "notice", "tool_call", "hand_off", "approval", "approval_closed", "write")
    + AGENT_AUDIT_KINDS
    + MEMBER_AUDIT_KINDS
    + AUDIENCE_AUDIT_KINDS
    + BUDGET_AUDIT_KINDS
    + SWITCH_AUDIT_KINDS
    + USAGE_AUDIT_KINDS
    + RETENTION_AUDIT_KINDS
    + ACCESS_AUDIT_KINDS
    + WEBHOOK_AUDIT_KINDS
    + PAYLOAD_KEY_AUDIT_KINDS
    + INSTALL_AUDIT_KINDS
    + HEALTH_AUDIT_KINDS
    + CONNECTOR_AUDIT_KINDS
    + TENANT_SETTINGS_AUDIT_KINDS
    + POLICY_AUDIT_KINDS
    + FEEDBACK_AUDIT_KINDS
    + TEST_AUDIT_KINDS
)
# 'policy' marks a write that ran under an admin's no-approval policy (D-43): it
# succeeded, and no approval row precedes it.
AUDIT_OUTCOMES = ("ok", "error", "policy")
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
    agent_id: (
        str | None
    )  # None only for a tenant-wide event: membership, budget, switch, usage, retention, webhook token, payload key, install, uninstall, connector, tenant settings, approval policy
    agent_version: int | None
    requester_user_id: str
    identity: str  # whose access was used, e.g. "agent:it-helper"
    outcome: str = "ok"
    conversation_id: str | None = None
    task_id: str | None = None
    tool: str | None = None
    target: str | None = None  # a Notion page or database ID, a specialist ID, or a feedback vote
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
        tenant_wide = (
            MEMBER_AUDIT_KINDS
            + BUDGET_AUDIT_KINDS
            + SWITCH_AUDIT_KINDS
            + USAGE_AUDIT_KINDS
            + RETENTION_AUDIT_KINDS
            + WEBHOOK_AUDIT_KINDS
            + PAYLOAD_KEY_AUDIT_KINDS
            + INSTALL_AUDIT_KINDS
            + CONNECTOR_AUDIT_KINDS
            + TENANT_SETTINGS_AUDIT_KINDS
            + POLICY_AUDIT_KINDS
        )
        if self.kind in AGENT_NO_VERSION_AUDIT_KINDS:
            if not self.agent_id:
                raise ValueError(f"audit kind {self.kind!r} needs an agent_id")
        elif self.kind not in tenant_wide and (not self.agent_id or self.agent_version is None):
            raise ValueError(f"audit kind {self.kind!r} needs an agent_id and an agent_version")
        if self.outcome not in AUDIT_OUTCOMES:
            raise ValueError(f"invalid audit outcome {self.outcome!r}")
        for digest in (self.input_sha256, self.output_sha256):
            if digest is not None and not _SHA256_HEX.fullmatch(digest):
                raise ValueError("audit hashes must be 64 lowercase hex characters")


@dataclass
class AudienceEvent:
    """A channel being shared with another organisation. Travels through Temporal: IDs only."""

    tenant_id: str
    event_id: str
    channel: str
