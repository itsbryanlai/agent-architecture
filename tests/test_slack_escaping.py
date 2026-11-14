"""Text from the model, the requester or Notion reaches Slack escaped.

Slack reads ``<...>`` as control syntax (``<!channel>``, ``<@U1>``,
``<url|label>``), so ``&``, ``<`` and ``>`` are encoded wherever such text
enters a message. Links the runtime builds itself stay live.
"""

import uuid

from agentos import ids
from agentos.activities import approval_card_blocks
from agentos.config import TASK_QUEUE
from agentos.knowledge.search import sources_footer
from agentos.models import ApprovalDecision, Citation, TaskSpec, TurnContext
from agentos.slack_format import escape_slack, from_slack_text, to_slack_mrkdwn
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, DB_ID, TENANT, eventually

HOSTILE = "<!channel> <@U1> <https://a|b> & done"
ESCAPED = "&lt;!channel&gt; &lt;@U1&gt; &lt;https://a|b&gt; &amp; done"
NONCE = "n-1"


def test_escape_slack_encodes_ampersand_first():
    assert escape_slack(HOSTILE) == ESCAPED
    assert escape_slack("&lt;") == "&amp;lt;"


def test_from_slack_text_shows_links_as_the_user_wrote_them():
    assert from_slack_text("see <http://example.com|example.com>") == "see example.com"
    assert from_slack_text("<mailto:a@b.co|a@b.co>") == "a@b.co"
    assert from_slack_text("<https://example.com>") == "https://example.com"
    assert from_slack_text("<https://a.io/x|the docs>") == "the docs (https://a.io/x)"
    assert from_slack_text("<https://a?b=1&amp;c=2>") == "https://a?b=1&c=2"


def test_from_slack_text_decodes_only_slack_entities_once():
    assert from_slack_text("&lt;!here&gt; &amp; done") == "<!here> & done"
    assert from_slack_text("&amp;lt;") == "&lt;"
    assert from_slack_text("&quot;&#39;&nbsp;") == "&quot;&#39;&nbsp;"


def test_from_slack_text_leaves_mentions_alone():
    text = "<@U1> <#C1|general> <!here> <!subteam^S1|@ops>"
    assert from_slack_text(text) == text


def test_reply_is_escaped_outside_code_and_in_code():
    assert to_slack_mrkdwn(HOSTILE) == ESCAPED
    assert to_slack_mrkdwn("run `a<b` now") == "run `a&lt;b` now"
    assert to_slack_mrkdwn("```\n<!here>\n```") == "```\n&lt;!here&gt;\n```"
    assert to_slack_mrkdwn("**<!here>** hi") == "*&lt;!here&gt;* hi"
    assert to_slack_mrkdwn("[x](https://a?b=1&c=2)") == "[x](https://a?b=1&amp;c=2)"


def test_leading_quote_marker_still_quotes():
    assert to_slack_mrkdwn("> quoted <!here>") == "> quoted &lt;!here&gt;"


def test_footer_link_stays_live_and_title_is_escaped():
    cited = [Citation(1, "notion:page:" + "a" * 32, "a" * 32)]
    footer = sources_footer(cited, {"a" * 32: ("R&D <!here>", "https://n.so/p?a=1")})
    assert footer == "\n\nSources: [1] <https://n.so/p?a=1|R&amp;D &lt;!here&gt;>"


def _spec(tool: str, payload: dict) -> TaskSpec:
    ctx = TurnContext(TENANT, "it-helper", 1, ALICE, "C_GENERAL", "1700000000.000100", "channel")
    return TaskSpec(ctx, tool, payload, NONCE)


def _card_text(blocks: list) -> str:
    out = []
    for b in blocks:
        if isinstance(b.get("text"), dict):
            out.append(b["text"]["text"])
        for f in b.get("fields", []):
            out.append(f["text"])
    return "\n".join(out)


def test_create_card_escapes_fields():
    spec = _spec("create_item", {"title": HOSTILE, "details": HOSTILE, "database_id": DB_ID})
    text = _card_text(approval_card_blocks(spec, "t1", "IT helper", "Requests"))
    assert "<!channel>" not in text and "<@U1>" not in text and "<https" not in text
    assert text.count(ESCAPED) == 2


def test_comment_card_escapes_fields():
    payload = {"item_title": HOSTILE, "comment": HOSTILE, "item_url": "u"}
    text = _card_text(approval_card_blocks(_spec("add_comment", payload), "t1", "A", "D"))
    assert "<!channel>" not in text and text.count(ESCAPED) == 2


async def test_thread_and_card_lines_are_escaped(harness):
    h = harness
    spec = _spec("create_item", {"title": HOSTILE, "details": "d", "database_id": DB_ID})
    handle = await h.client.start_workflow(
        TaskWorkflow.run, spec, id=ids.task_id(TENANT, str(uuid.uuid4())), task_queue=TASK_QUEUE
    )

    async def awaiting():
        return await handle.query(TaskWorkflow.state) == "awaiting_approval"

    await eventually(awaiting, timeout=20)
    await handle.signal(TaskWorkflow.decision, ApprovalDecision(NONCE, ALICE, True))

    async def watching():
        return await handle.query(TaskWorkflow.state) == "watching"

    await eventually(watching, timeout=20)

    filed = [p["text"] for p in h.slack.posts() if p["text"].startswith("Filed:")]
    assert len(filed) == 1 and ESCAPED in filed[0] and "<!channel>" not in filed[0]
    submitted = [u["text"] for u in h.slack.updates() if u["text"].startswith("Submitted")]
    assert submitted == [f"Submitted: {ESCAPED}"]
