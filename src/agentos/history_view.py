"""The view of a thread's stored history that is sent to the model.

Stored history stays complete. Every model call goes through
``prepare_for_model``, which first repairs the messages and then keeps only
the most recent turns that fit the budget. Both steps are pure.
"""

from __future__ import annotations

import json

# Rough size, in characters of JSON, of the history sent to the model: about
# 30k tokens, which leaves room for the system prompt, tools and the reply in
# the smaller context windows we run against.
HISTORY_BUDGET_CHARS = 120_000

TOOL_RESULT_MISSING = "The tool call did not complete."


def prepare_for_model(messages: list[dict]) -> list[dict]:
    return window(close_open_tool_uses(messages))


def _blocks(message: dict, kind: str) -> list[dict]:
    content = message["content"]
    if not isinstance(content, list):
        return []
    return [b for b in content if isinstance(b, dict) and b.get("type") == kind]


def close_open_tool_uses(messages: list[dict]) -> list[dict]:
    """Give every tool_use a tool_result in the message right after it.

    A missing result is added as an error result: to the user message that
    follows when there is one, otherwise as a new user message. A thread that
    holds a tool_use with no tool_result is rejected by the model for good,
    whatever the cause of the gap.
    """
    fixed: list[dict] = []
    for i, message in enumerate(messages):
        if message["role"] == "user" and i > 0 and _blocks(messages[i - 1], "tool_use"):
            # Handled together with the assistant message before it.
            continue
        fixed.append(message)
        if message["role"] != "assistant":
            continue
        uses = [b["id"] for b in _blocks(message, "tool_use")]
        if not uses:
            continue
        following = messages[i + 1] if i + 1 < len(messages) else None
        answered: set[str] = set()
        if following is not None and following["role"] == "user":
            answered = {b["tool_use_id"] for b in _blocks(following, "tool_result")}
        missing = [
            {
                "type": "tool_result",
                "tool_use_id": use,
                "content": TOOL_RESULT_MISSING,
                "is_error": True,
            }
            for use in uses
            if use not in answered
        ]
        if following is not None and following["role"] == "user":
            if missing:
                content = following["content"]
                content = content if isinstance(content, list) else []
                results = [b for b in content if b.get("type") == "tool_result"]
                rest = [b for b in content if b.get("type") != "tool_result"]
                following = {**following, "content": missing + results + rest}
            fixed.append(following)
        elif missing:
            fixed.append({"role": "user", "content": missing})
    return fixed


def _starts_a_turn(message: dict) -> bool:
    return message["role"] == "user" and bool(_blocks(message, "text"))


def window(messages: list[dict], budget: int = HISTORY_BUDGET_CHARS) -> list[dict]:
    """Keep the most recent whole turns that fit ``budget``.

    The cut is made only in front of a user message with text, so a tool_use
    is never separated from its tool_result. The newest such message is always
    kept, even when it alone is over budget.
    """
    sizes = [len(json.dumps(m)) for m in messages]
    if sum(sizes) <= budget:
        return messages
    starts = [i for i, m in enumerate(messages) if _starts_a_turn(m)]
    if not starts:
        return messages
    kept_from = starts[-1]
    for start in starts:
        if sum(sizes[start:]) <= budget:
            kept_from = start
            break
    return messages[kept_from:]
