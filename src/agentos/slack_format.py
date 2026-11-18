"""Convert text at the Slack boundary, in both directions.

Outbound, the model's Markdown becomes Slack's mrkdwn and is escaped before a
reply is posted. Inbound, :func:`from_slack_text` decodes Slack's encoding of
what the user typed.

Slack's message text is not Markdown. Bold is ``*x*`` (Markdown: ``**x**``),
italic is ``_x_`` (Markdown: ``*x*``), strike is ``~x~``, and headings and
``*`` bullets do not exist. The model writes Markdown, and telling it to write
Slack syntax instead does not hold: it drifts back. So the runtime converts
what it posts and leaves what it stores alone.

Each line is cut once into segments: inline code, Markdown link, or plain text.
Only plain text is converted. Fenced code blocks (several lines, or a whole
fence on one line) and the code and link segments are escaped but otherwise
left as written; the runtime does not turn model-written links into Slack
links. Emphasis may wrap a code or link segment (``**run `x` now**``) but a
marker inside one is never read. Numbered lists and citation markers such as
``[1]`` pass through unchanged. Input may hold any character, including the
private-use ones: nothing is used as an in-band marker.

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
# A run of backticks closed by a run of the same length; or a Markdown link.
_PROTECTED = re.compile(
    r"(?P<code>(?<!`)(?P<ticks>`+)(?!`)[^\n]*?(?<!`)(?P=ticks)(?!`))"
    r"|(?P<link>\[[^\]\n]*\]\([^)\n]*\))"
)
_BOLD3 = re.compile(r"\*\*\*(?=\S)(.+?)(?<=\S)\*\*\*")
_BOLD_STAR = re.compile(r"\*\*(?=\S)(.+?)(?<=\S)\*\*")
_BOLD_UNDER = re.compile(r"(?<!\w)__(?=\S)(.+?)(?<=\S)__(?!\w)")
_STRIKE = re.compile(r"~~(?=\S)(.+?)(?<=\S)~~")
_ITALIC_STAR = re.compile(r"(?<![*\w])\*(?=[^\s*])(.+?)(?<=[^\s*])\*(?![*\w])")

# Slack markup ``<target|label>`` or one of the three entities Slack encodes.
_SLACK_TOKEN = re.compile(r"<([^<>|]*)(?:\|([^<>]*))?>|&(amp|lt|gt);")
_ENTITY = re.compile(r"&(amp|lt|gt);")
_ENTITIES = {"amp": "&", "lt": "<", "gt": ">"}
_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:(?://)?")

_FILL = "\0"  # stands in the mask for characters that emphasis rules must not touch


def escape_slack(text: str) -> str:
    """Encode ``&``, ``<`` and ``>`` so Slack shows text instead of reading control syntax.

    Slack treats ``<!channel>``, ``<@U1>`` and ``<url|label>`` as pings and
    links. Apply this to every string the runtime did not write itself (the
    model's, the requester's, Notion's) before it goes into message text or a
    mrkdwn block. It is not idempotent: apply it once, at the point of use.
    """
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def from_slack_text(text: str) -> str:
    """Decode the text of an inbound Slack event back to what the user typed.

    The inverse direction of :func:`escape_slack`. User-typed ``&``, ``<`` and
    ``>`` arrive as entities, so any real ``<...>`` is Slack markup. Links
    become their label, or ``label (url)`` when the user gave one; mentions
    and channel refs stay as markup. One pass, so ``&amp;lt;`` becomes ``&lt;``.
    """

    def decode(m: re.Match[str]) -> str:
        if m.group(3):
            return _ENTITIES[m.group(3)]
        target = m.group(1)
        if target[:1] in ("@", "#", "!"):
            return m.group(0)
        url = _decode_entities(target)
        if m.group(2) is None:
            return url
        label = _decode_entities(m.group(2))
        return label if label == _SCHEME.sub("", url) else f"{label} ({url})"

    return _SLACK_TOKEN.sub(decode, text)


def _decode_entities(s: str) -> str:
    return _ENTITY.sub(lambda m: _ENTITIES[m.group(1)], s)


def to_slack_mrkdwn(text: str) -> str:
    """Convert Markdown emphasis, headings and bullets to Slack mrkdwn, escaped."""
    out: list[str] = []
    in_fence = False
    for line in text.split("\n"):
        if in_fence:
            in_fence = not line.startswith("```")
            out.append(escape_slack(line))
        elif line.startswith("```"):
            # A fence that closes on its own line is a whole block already.
            in_fence = "```" not in line[3:]
            out.append(escape_slack(line))
        else:
            out.append(_convert_line(line))
    return "\n".join(out)


_MENTION = re.compile(r"&lt;([@!])([^<>]*?)&gt;")
_USER_ID = re.compile(r"[UW][A-Z0-9]+")


def _mention_pieces(text: str) -> list[tuple[str, str]]:
    """Cut converted mrkdwn into ``(kind, text)`` pieces; fences and code are kind ``code``."""
    pieces: list[tuple[str, str]] = []
    in_fence = False
    lines = text.split("\n")
    for index, line in enumerate(lines):
        if in_fence:
            in_fence = not line.startswith("```")
            pieces.append(("code", line))
        elif line.startswith("```"):
            in_fence = "```" not in line[3:]
            pieces.append(("code", line))
        else:
            pieces.extend(_segments(line))
        if index < len(lines) - 1:
            pieces.append(("plain", "\n"))
    return pieces


def mentioned_user_ids(text: str) -> list[str]:
    """The Slack user IDs mentioned outside code in converted mrkdwn, first-seen order."""
    found: list[str] = []
    for kind, piece in _mention_pieces(text):
        if kind != "plain":
            continue
        for match in _MENTION.finditer(piece):
            user_id = match.group(2)
            if match.group(1) == "@" and _USER_ID.fullmatch(user_id) and user_id not in found:
                found.append(user_id)
    return found


def render_mentions(text: str, requester_id: str, names: dict[str, str]) -> str:
    """Rewrite model-written mentions in converted mrkdwn so only the requester is pinged.

    The requester's own ``<@ID>`` becomes a real mention; any other user ID becomes
    that user's escaped display name from ``names`` ("someone" when absent or empty).
    ``here``, ``channel`` and ``everyone`` become the plain word; a user group becomes
    its already-escaped label ("a group" without one). Fenced blocks, inline code and
    Markdown links are left as written, and every other token stays escaped, so no
    model-written token can turn into a ping.
    """

    def render(match: re.Match[str]) -> str:
        body = match.group(2)
        if match.group(1) == "@":
            if body == requester_id:
                return f"<@{body}>"
            if _USER_ID.fullmatch(body):
                return escape_slack(names.get(body) or "someone")
            return match.group(0)
        if body in ("here", "channel", "everyone"):
            return body
        if body.startswith("subteam^"):
            _, _, label = body.partition("|")
            return label or "a group"
        return match.group(0)

    return "".join(
        _MENTION.sub(render, piece) if kind == "plain" else piece
        for kind, piece in _mention_pieces(text)
    )


class _Text:
    """Escaped text and a same-length mask that hides what a rule must not touch.

    The mask is a separate string, never mixed into the output: it holds the
    text of the plain segments, and a filler wherever the text is code, a
    link, or a marker already written. Rules match on the mask and edit both.
    """

    def __init__(self, text: str, mask: str) -> None:
        self.text, self.mask = text, mask

    def sub(self, pattern: re.Pattern[str], parts: tuple[int | str, ...]) -> _Text:
        """Replace each match by ``parts``: an int is that group, a str is a new marker."""
        text: list[str] = []
        mask: list[str] = []
        pos = 0
        for m in pattern.finditer(self.mask):
            text.append(self.text[pos : m.start()])
            mask.append(self.mask[pos : m.start()])
            for part in parts:
                if isinstance(part, int):
                    text.append(self.text[m.start(part) : m.end(part)])
                    mask.append(self.mask[m.start(part) : m.end(part)])
                else:
                    text.append(part)
                    mask.append(_FILL * len(part))
            pos = m.end()
        text.append(self.text[pos:])
        mask.append(self.mask[pos:])
        return _Text("".join(text), "".join(mask))

    def slice(self, start: int, end: int) -> _Text:
        return _Text(self.text[start:end], self.mask[start:end])


def _segments(line: str) -> list[tuple[str, str]]:
    """Cut a line into ``(kind, text)`` pieces; kind is plain, code or link."""
    pieces: list[tuple[str, str]] = []
    pos = 0
    for m in _PROTECTED.finditer(line):
        if m.start() > pos:
            pieces.append(("plain", line[pos : m.start()]))
        pieces.append(("code" if m.group("code") else "link", m.group(0)))
        pos = m.end()
    if pos < len(line):
        pieces.append(("plain", line[pos:]))
    return pieces


def _convert_line(line: str) -> str:
    # A leading ">" is Slack's quote marker; it stays, the rest is converted.
    quote = _QUOTE_PREFIX.match(line)
    prefix = quote.group(1) if quote else ""
    text = _Text("", "")
    for kind, piece in _segments(line[len(prefix) :]):
        escaped = escape_slack(piece)
        text.text += escaped
        text.mask += escaped if kind == "plain" else _FILL * len(escaped)

    heading = _HEADING.match(text.mask)
    if heading:
        # A heading is one bold line; bold inside it would nest, so drop it.
        inner = text.slice(heading.start(1), heading.end(1))
        inner = inner.sub(_BOLD_STAR, (1,)).sub(_BOLD_UNDER, (1,))
        text = _inline(inner)
        return f"{prefix}*{text.text}*"
    return prefix + _inline(text.sub(_BULLET, (1, "• "))).text


def _inline(t: _Text) -> _Text:
    return (
        t.sub(_BOLD3, ("*", 1, "*"))
        .sub(_BOLD_STAR, ("*", 1, "*"))
        .sub(_BOLD_UNDER, ("*", 1, "*"))
        .sub(_STRIKE, ("~", 1, "~"))
        .sub(_ITALIC_STAR, ("_", 1, "_"))
    )
