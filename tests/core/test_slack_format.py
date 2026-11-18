"""Markdown from the model becomes Slack mrkdwn in the posted reply only."""

from dataclasses import replace

import pytest
from temporalio.testing import ActivityEnvironment

from agentos import ids
from agentos.slack_client import SlackLookupError, SlackUser
from agentos.slack_format import to_slack_mrkdwn
from tests.conftest import ALICE, TENANT, eventually
from tests.helpers.conversation import stored_history
from tests.helpers.knowledge import (
    CHANNEL,
    ROOT_A,
    SCOPE_A,
    answer,
    call,
    converse,
    index_both,
    plain,
    tool_turn,
    turn_ctx,
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
    (21, "\ue0017\ue002 `c` **b** \uf8ff", "\ue0017\ue002 `c` *b* \uf8ff"),
    (22, "```code **x**```\n**y**", "```code **x**```\n*y*"),
    (23, "```py\n**x**\n\n**y**\n```\n**z**", "```py\n**x**\n\n**y**\n```\n*z*"),
    (24, "use `a*b_c` and ``x`y`` *i*", "use `a*b_c` and ``x`y`` _i_"),
    (25, "**run `x` now**", "*run `x` now*"),
    (26, "see [a *b*](http://x/_y_) **z**", "see [a *b*](http://x/_y_) *z*"),
    (27, "## Using `foo` **now**", "*Using `foo` now*"),
    (28, "> * a & <b>", "> • a &amp; &lt;b&gt;"),
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
    stored = await stored_history(h, ids.conversation_id(TENANT, CHANNEL, "1700000000.000300"))
    assert stored[-1]["content"][0]["text"] == "**Report it** [1]"


MENTION_TABLE = [
    (1, f"thanks <@{ALICE}>", f"thanks <@{ALICE}>"),
    (2, "ask <@U0BOB>", "ask Bob &lt;b&gt;"),
    (3, "ask <@W0BOB>", "ask Bob &lt;b&gt;"),
    (4, "ping <!here> now", "ping here now"),
    (5, "ping <!channel> now", "ping channel now"),
    (6, "ping <!everyone> now", "ping everyone now"),
    (7, "cc <!subteam^S123|design>", "cc design"),
    (8, "cc <!subteam^S123>", "cc a group"),
    (
        9,
        "run `<@U0BOB>` in\n```\n<@U0BOB> <!here>\n```",
        "run `&lt;@U0BOB&gt;` in\n```\n&lt;@U0BOB&gt; &lt;!here&gt;\n```",
    ),
]


@pytest.mark.parametrize(
    "source,expected",
    [(s, e) for _, s, e in MENTION_TABLE],
    ids=[f"row{n}" for n, _, _ in MENTION_TABLE],
)
async def test_model_mentions_ping_only_the_requester(kharness, source, expected):
    """Fails if a model-written mention pings anyone but the requester or renders literally."""
    h = kharness
    h.slack.users["U0BOB"] = SlackUser("U0BOB", h.slack.team_id, display_name="Bob <b>")
    h.slack.users["W0BOB"] = SlackUser("W0BOB", h.slack.team_id, display_name="Bob <b>")
    h.llm.queue(answer(source))

    await converse(h, "it-helper", "1700000000.000400")
    await eventually(lambda: h.slack.updates())

    assert h.slack.updates()[0]["text"] == expected


async def test_failed_mention_lookup_renders_someone(kharness):
    """Fails if a mention whose directory lookup failed renders as a ping or a literal token."""
    h = kharness
    h.slack.fail_lookups(SlackLookupError("user_not_found"))

    await ActivityEnvironment().run(
        h.acts.finalize_reply, turn_ctx("it-helper"), "1700000000.000500", "ask <@U0BOB>"
    )

    assert h.slack.updates()[0]["text"] == "ask someone"


async def test_enterprise_requester_mention_pings_only_them(kharness):
    """Fails if an enterprise-grid requester's own mention does not ping, or another's does."""
    h = kharness
    h.slack.users["W0OTHER"] = SlackUser("W0OTHER", h.slack.team_id, display_name="Wendy")
    ctx = replace(turn_ctx("it-helper"), requester_user_id="W0ME")

    await ActivityEnvironment().run(
        h.acts.finalize_reply, ctx, "1700000000.000600", "<@W0ME> meet <@W0OTHER>"
    )

    assert h.slack.updates()[0]["text"] == "<@W0ME> meet Wendy"


async def test_footer_links_survive_mention_rendering(kharness):
    """Fails if rendering mentions rewrites the Sources footer's Slack links."""
    h = kharness
    h.slack.users["U0BOB"] = SlackUser("U0BOB", h.slack.team_id, display_name="Bob <b>")
    await index_both(h)
    h.llm.queue(
        tool_turn(call("tu_1", "reset your vpn password")),
        answer("Thanks <@U0BOB> [1]"),
    )

    await converse(h, "it-helper", "1700000000.000700")
    await eventually(lambda: h.slack.updates())

    title, url = (await h.knowledge.page_meta(TENANT, [SCOPE_A], [plain(ROOT_A)]))[plain(ROOT_A)]
    assert (
        h.slack.updates()[0]["text"] == f"Thanks Bob &lt;b&gt; [1]\n\nSources: [1] <{url}|{title}>"
    )
