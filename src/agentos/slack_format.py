"""Turn the model's Markdown into Slack's mrkdwn before a reply is posted.

Slack's message text is not Markdown. Bold is ``*x*`` (Markdown: ``**x**``),
italic is ``_x_`` (Markdown: ``*x*``), strike is ``~x~``, and headings and
``*`` bullets do not exist. The model writes Markdown, and telling it to write
Slack syntax instead does not hold: it drifts back. So the runtime converts
what it posts and leaves what it stores alone.

Fenced code blocks and inline code are never changed. Markdown links are left
as written: the runtime does not turn model-written links into Slack links.
Numbered lists and citation markers such as ``[1]`` pass through unchanged.

The conversion is NOT idempotent, and must not be. Markdown ``*x*`` means
italic while Slack ``*x*`` means bold, so a second pass over converted text
would turn bold into italic. Apply it exactly once, to model output, and never
to text that is already mrkdwn. There is deliberately no "is this already
Slack text" guess.
"""

from __future__ import annotations

import re

_QUOTE_PREFIX = re.compile(r"^(\s*(?:>\s?)+)")
_HEADING = re.compile(r"^#{1,6} +(.*?)\s*$")
_BULLET = re.compile(r"^(\s*)[*-] +")
_CODE_SPAN = re.compile(r"`[^`\n]*`")
_LINK = re.compile(r"\[[^\]\n]*\]\([^)\n]*\)")
_BOLD3 = re.compile(r"\*\*\*(?=\S)(.+?)(?<=\S)\*\*\*")
_BOLD_STAR = re.compile(r"\*\*(?=\S)(.+?)(?<=\S)\*\*")
_BOLD_UNDER = re.compile(r"(?<!\w)__(?=\S)(.+?)(?<=\S)__(?!\w)")
_STRIKE = re.compile(r"~~(?=\S)(.+?)(?<=\S)~~")
_ITALIC_STAR = re.compile(r"(?<![*\w])\*(?=[^\s*])(.+?)(?<=[^\s*])\*(?![*\w])")

# Private-use marks: a converted bold marker, and a protected span's slot.
_BOLD = ""
_SLOT = re.compile("(\\d+)")


def escape_slack(text: str) -> str:
    """Encode ``&``, ``<`` and ``>`` so Slack shows text instead of reading control syntax.

    Slack treats ``<!channel>``, ``<@U1>`` and ``<url|label>`` as pings and
    links. Apply this to every string the runtime did not write itself (the
    model's, the requester's, Notion's) before it goes into message text or a
    mrkdwn block. It is not idempotent: apply it once, at the point of use.
    """
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def to_slack_mrkdwn(text: str) -> str:
    """Convert Markdown emphasis, headings and bullets to Slack mrkdwn, escaped."""
    out: list[str] = []
    in_fence = False
    for line in text.split("\n"):
        if line.startswith("```"):
            in_fence = not in_fence
            out.append(escape_slack(line))
        elif in_fence:
            out.append(escape_slack(line))
        else:
            out.append(_convert_line(line))
    return "\n".join(out)


def _convert_line(line: str) -> str:
    kept: list[str] = []

    def protect(m: re.Match[str]) -> str:
        kept.append(escape_slack(m.group(0)))
        return f"{len(kept) - 1}"

    line = _LINK.sub(protect, _CODE_SPAN.sub(protect, line))
    # A leading ">" is Slack's quote marker; it stays, the rest is escaped.
    quote = _QUOTE_PREFIX.match(line)
    prefix = quote.group(1) if quote else ""
    line = prefix + escape_slack(line[len(prefix) :])

    heading = _HEADING.match(line)
    if heading:
        # A heading is one bold line; bold inside it would nest, so drop it.
        inner = _BOLD_STAR.sub(r"\1", heading.group(1))
        inner = _BOLD_UNDER.sub(r"\1", inner)
        line = f"{_BOLD}{_inline(inner)}{_BOLD}"
    else:
        line = _BULLET.sub(r"\1• ", line)
        line = _inline(line)

    line = line.replace(_BOLD, "*")
    return _SLOT.sub(lambda m: kept[int(m.group(1))], line)


def _inline(s: str) -> str:
    s = _BOLD3.sub(_BOLD + r"\1" + _BOLD, s)
    s = _BOLD_STAR.sub(_BOLD + r"\1" + _BOLD, s)
    s = _BOLD_UNDER.sub(_BOLD + r"\1" + _BOLD, s)
    s = _STRIKE.sub(r"~\1~", s)
    return _ITALIC_STAR.sub(r"_\1_", s)
