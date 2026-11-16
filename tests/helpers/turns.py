"""Helpers that read tool-use and tool-result ids out of a stored conversation."""


def tool_use_ids(messages):
    return [
        b["id"]
        for m in messages
        for b in m["content"]
        if isinstance(b, dict) and b.get("type") == "tool_use"
    ]


def tool_result_ids(messages):
    return [
        b["tool_use_id"]
        for m in messages
        for b in m["content"]
        if isinstance(b, dict) and b.get("type") == "tool_result"
    ]


def assert_tool_pairs_valid(messages: list[dict]) -> None:
    """Every tool_use is answered by the very next message, and no result is orphaned."""
    for i, m in enumerate(messages):
        uses = [
            b["id"] for b in m["content"] if isinstance(b, dict) and b.get("type") == "tool_use"
        ]
        if uses:
            assert i + 1 < len(messages), "tool_use is the last message"
            nxt = messages[i + 1]
            assert nxt["role"] == "user"
            answered = [
                b["tool_use_id"]
                for b in nxt["content"]
                if isinstance(b, dict) and b.get("type") == "tool_result"
            ]
            assert sorted(answered) == sorted(uses)
        results = [
            b["tool_use_id"]
            for b in m["content"]
            if isinstance(b, dict) and b.get("type") == "tool_result"
        ]
        if results:
            assert i > 0
            prev_uses = [
                b["id"]
                for b in messages[i - 1]["content"]
                if isinstance(b, dict) and b.get("type") == "tool_use"
            ]
            assert sorted(results) == sorted(prev_uses)
