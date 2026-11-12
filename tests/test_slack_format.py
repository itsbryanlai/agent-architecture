"""Markdown from the model becomes Slack mrkdwn in the posted reply only."""

import pytest

from agentos import ids
from agentos.slack_format import to_slack_mrkdwn
from tests.conftest import TENANT, eventually
from tests.test_knowledge_search import (
    CHANNEL,
    ROOT_A,
    SCOPE_A,
    answer,
    call,
    converse,
    index_both,
    plain,
    tool_turn,
)

FENCE = "```\n**x**\n# y\n* z\n```"

TABLE = [
    (1, "**Report it** now", "*Report it* now"),
    (2, "__Report it__ now", "*Report it* now"),
    (3, "an *important* step", "an _important_ step"),
    (4, "an _important_ step", "an _important_ step"),
    (5, "~~old~~ new", "~old~ new"),
    (6, "## Steps", "*Steps*"),
    (7, "# Phishing: what to do", "*Phishing: what to do*"),
    (8, "* first item", "• first item"),
    (9, "- first item", "• first item"),
    (10, "  * nested item", "  • nested item"),
    (11, "1. **Instruct them** not to click", "1. *Instruct them* not to click"),
    (12, "***both***", "*both*"),
    (13, "use `**not bold**` here", "use `**not bold**` here"),
    (14, FENCE, FENCE),
    (15, "see [the guide](https://example.com)", "see [the guide](https://example.com)"),
    (16, "cite [1][2] and [3]", "cite [1][2] and [3]"),
    (17, "2 * 3 * 4", "2 * 3 * 4"),
    (18, "a**b", "a**b"),
    (19, "Line one\n\n**Bold** line two", "Line one\n\n*Bold* line two"),
    (20, "", ""),
]


@pytest.mark.parametrize(
    "source,expected", [(s, e) for _, s, e in TABLE], ids=[f"row{n}" for n, _, _ in TABLE]
)
def test_conversion_table(source, expected):
    assert to_slack_mrkdwn(source) == expected


async def test_finalize_reply_posts_converted_text_and_footer(kharness):
    h = kharness
    await index_both(h)
    h.llm.queue(
        tool_turn(call("tu_1", "reset your vpn password")),
        answer("**Report it** [1]"),
    )

    await converse(h, "it-helper", "1700000000.000300", "how do I reset my VPN password?")
    await eventually(lambda: h.slack.updates())

    title, url = (await h.knowledge.page_meta(TENANT, [SCOPE_A], [plain(ROOT_A)]))[plain(ROOT_A)]
    update = h.slack.updates()[0]["text"]
    assert update == f"*Report it* [1]\n\nSources: [1] <{url}|{title}>"
    stored = await h.history.load(TENANT, ids.conversation_id(TENANT, CHANNEL, "1700000000.000300"))
    assert stored[-1]["content"][0]["text"] == "**Report it** [1]"
