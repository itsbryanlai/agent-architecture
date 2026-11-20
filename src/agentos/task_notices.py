"""The lines a task posts in its conversation's thread, written in one place.

A line names what the task is about (the title the requester approved, a Notion item's title),
so it holds content. A task made now keeps that content out of Temporal history: the workflow
names the kind of line and the ``post_task_notice`` activity writes it from the stored draft.
A task recorded before that wrote the line in the workflow; it calls ``notice_text`` too, so
the two say the same words.
"""

from __future__ import annotations

from agentos.slack_format import escape_slack

# Notice kinds. The ones with a stored item (a filed item the watch follows) read it from the
# write's result; the others read the draft.
CARRY_ON_FAILED = "carry_on_failed"
REMINDER = "reminder"
FAILED_CREATE = "failed_create"
FILED = "filed"
STOPPED_WATCH = "stopped_watch"
STOPPED_UNREADABLE = "stopped_unreadable"
DONE = "done"
STALE = "stale"
FAILED_COMMENT = "failed_comment"
NOT_CONFIRMED_COMMENT = "not_confirmed_comment"
COMMENTED = "commented"
COMMENTED_NO_APPROVAL = "commented_no_approval"
NUDGED = "nudged"
NUDGE_COMMENTED = "nudge_commented"
NUDGE_NO_DM = "nudge_no_dm"
NUDGE_SELF = "nudge_self"
NUDGE_PREPARE_FAILED = "nudge_prepare_failed"

# Kinds about the item a create made, not about the draft.
ITEM_NOTICES = (FILED, STOPPED_WATCH, STOPPED_UNREADABLE, DONE, STALE)


def subject(tool_name: str, payload: dict) -> str:
    """The short name of what a task is about, for card and thread lines."""
    if tool_name in ("add_comment", "nudge"):
        return payload.get("item_title") or payload.get("item") or ""
    return payload.get("title", "")


def notice_text(
    kind: str,
    *,
    subject: str = "",
    reason: str = "",
    url: str = "",
    status: str = "",
    requester: str = "",
    days: int = 0,
) -> str:
    """The thread line for ``kind``. ``subject`` is the draft's or item's title, unescaped."""
    name = escape_slack(subject)
    if kind == CARRY_ON_FAILED:
        return (
            f"I couldn't carry on with '{name}': "
            "something went wrong on my side. Please ask me again."
        )
    if kind == REMINDER:
        return (
            f"Reminder: <@{requester}>, the request "
            f"'{name}' is waiting for your approval in your DMs."
        )
    if kind == FAILED_CREATE:
        return f"I couldn't file '{name}': {escape_slack(reason)}"
    if kind == FILED:
        link = f" ({url})" if url else ""
        return f"Filed: {name}{link}. I'll tell you here when it's done."
    if kind == STOPPED_WATCH:
        return f"I've stopped watching '{name}'."
    if kind == STOPPED_UNREADABLE:
        return (
            f"I've stopped watching '{name}': I can no longer "
            "read it. It may have been deleted or moved out of the databases "
            "I can access."
        )
    if kind == DONE:
        return f"Your request '{name}' was marked {escape_slack(status)}."
    if kind == STALE:
        return f"No movement on '{name}' in {days} days."
    if kind == FAILED_COMMENT:
        return f"I couldn't comment on '{name}': {escape_slack(reason)}"
    if kind == NOT_CONFIRMED_COMMENT:
        return f"I couldn't confirm whether the comment was posted; please check {url}"
    if kind == COMMENTED:
        return f"Commented on {name} ({url})."
    if kind == COMMENTED_NO_APPROVAL:
        return f"Commented on '{name}' (no approval needed: your admin's setting)."
    if kind == NUDGED:
        return f"Nudged the assignee about '{name}': commented on the item and sent a DM."
    if kind == NUDGE_COMMENTED:
        return f"Commented on '{name}' as a nudge."
    if kind == NUDGE_NO_DM:
        return f"Commented on '{name}', but I couldn't DM the assignee."
    if kind == NUDGE_SELF:
        return f"The assignee of '{name}' is you, so there's nobody to nudge."
    if kind == NUDGE_PREPARE_FAILED:
        return f"I couldn't prepare the nudge for '{name}'."
    raise ValueError(f"unknown notice kind {kind!r}")
