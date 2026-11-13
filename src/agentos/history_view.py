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
    """Make every tool_use answered by the message right after it, and only so.

    A missing result is added as an error result: to the user message that
    follows when there is one, otherwise as a new user message. A tool_result
    that answers no tool_use of the message before it is dropped. The model
    rejects a thread with either gap, for good, whatever caused it.
    """
    fixed: list[dict] = []
    for i, message in enumerate(messages):
        previous = messages[i - 1] if i > 0 else None
        if message["role"] == "user" and previous is not None and _blocks(previous, "tool_use"):
            continue  # handled together with the assistant message before it
        if message["role"] == "user":
            message = _without_results(message)
            if not message["content"]:
                continue
        fixed.append(message)
        uses = [b["id"] for b in _blocks(message, "tool_use")]
        if message["role"] != "assistant" or not uses:
            continue
        following = messages[i + 1] if i + 1 < len(messages) else None
        if following is not None and following["role"] == "user":
            results = [b for b in _blocks(following, "tool_result") if b["tool_use_id"] in uses]
            rest = _without_results(following)["content"]
        else:
            results, rest = [], []
        answered = {b["tool_use_id"] for b in results}
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
        fixed.append({"role": "user", "content": missing + results + rest})
    return fixed


def _without_results(message: dict) -> dict:
    content = message["content"]
    if isinstance(content, str):
        content = [{"type": "text", "text": content}]
    kept = [b for b in content if not (isinstance(b, dict) and b.get("type") == "tool_result")]
    return {**message, "content": kept}


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
