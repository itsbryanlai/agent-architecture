"""Turn a Notion page into plain text and then into chunks for embedding.

Page text is read here and goes straight into the knowledge store. It never
passes through Temporal workflow history: only page IDs and counts do.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from agentos.connectors.notion import NotionAPI, normalize_id

CHUNK_CHARS = 1200
MAX_BLOCK_DEPTH = 5

TEXT_BLOCKS = (
    "paragraph",
    "bulleted_list_item",
    "numbered_list_item",
    "to_do",
    "toggle",
    "quote",
    "callout",
    "code",
)
HEADING_BLOCKS = ("heading_1", "heading_2", "heading_3")


class PageUnavailable(Exception):
    """The page is in the trash or archived, so it is not knowledge any more."""


@dataclass
class TextBlock:
    kind: str  # "heading" or "text"
    text: str


@dataclass
class PageContent:
    title: str
    url: str
    blocks: list[TextBlock] = field(default_factory=list)
    child_page_ids: list[str] = field(default_factory=list)


def _plain(rich: list[dict] | None) -> str:
    return "".join(
        t.get("plain_text") or (t.get("text") or {}).get("content", "") for t in rich or []
    )


def page_title(page: dict) -> str:
    for prop in (page.get("properties") or {}).values():
        if isinstance(prop, dict) and prop.get("type") == "title":
            return _plain(prop.get("title")).strip()
    return ""


def _block_text(block: dict) -> TextBlock | None:
    kind = block.get("type", "")
    body = block.get(kind) or {}
    if kind in HEADING_BLOCKS:
        text = _plain(body.get("rich_text")).strip()
        return TextBlock("heading", text) if text else None
    if kind in TEXT_BLOCKS:
        text = _plain(body.get("rich_text")).strip()
        return TextBlock("text", text) if text else None
    if kind == "table_row":
        cells = [_plain(cell).strip() for cell in body.get("cells", [])]
        text = " | ".join(cells)
        return TextBlock("text", text) if text.strip(" |") else None
    return None


async def page_text(api: NotionAPI, page_id: str) -> PageContent:
    """Read a page's title, link, text blocks and sub-page IDs.

    Blocks are read depth first, following ``has_children`` up to depth 5.
    A ``child_page`` block is returned as a sub-page ID and not read here; any
    other block type contributes no text. Raises ``PageUnavailable`` for a
    trashed or archived page.
    """
    page = await api.retrieve_page(page_id)
    if page.get("in_trash") or page.get("archived"):
        raise PageUnavailable(normalize_id(page_id))
    content = PageContent(title=page_title(page), url=page.get("url", ""))
    await _read_children(api, page_id, 1, content)
    return content


async def _read_children(api: NotionAPI, block_id: str, depth: int, out: PageContent) -> None:
    for block in await api.list_child_blocks(block_id):
        if block.get("type") == "child_page":
            out.child_page_ids.append(normalize_id(block["id"]))
            continue
        text = _block_text(block)
        if text is not None:
            out.blocks.append(text)
        if block.get("has_children") and depth < MAX_BLOCK_DEPTH:
            await _read_children(api, block["id"], depth + 1, out)


_SENTENCE_END = re.compile(r"(?<=[.!?])\s+")


def _split_long(text: str, limit: int) -> list[str]:
    """Split one long block on sentence ends, hard-splitting what is still too long."""
    if len(text) <= limit:
        return [text]
    pieces: list[str] = []
    current = ""
    for sentence in _SENTENCE_END.split(text):
        while len(sentence) > limit:
            if current:
                pieces.append(current)
                current = ""
            pieces.append(sentence[:limit])
            sentence = sentence[limit:]
        if current and len(current) + 1 + len(sentence) > limit:
            pieces.append(current)
            current = sentence
        else:
            current = f"{current} {sentence}" if current else sentence
    if current:
        pieces.append(current)
    return pieces


def chunk(title: str, blocks: list[TextBlock]) -> list[str]:
    """Group blocks under their latest heading into chunks of at most ``CHUNK_CHARS``.

    Each chunk starts with ``"<title> › <heading>\\n"``, or ``"<title>\\n"``
    when no heading came before it. The limit applies to the body, not the
    prefix. A page with no body text gives no chunks.
    """
    chunks: list[str] = []
    heading: str | None = None
    body: list[str] = []
    size = 0

    def flush() -> None:
        nonlocal body, size
        if body:
            prefix = f"{title} › {heading}\n" if heading else f"{title}\n"
            chunks.append(prefix + "\n".join(body))
        body, size = [], 0

    for block in blocks:
        if block.kind == "heading":
            flush()
            heading = block.text
            continue
        for piece in _split_long(block.text, CHUNK_CHARS):
            added = len(piece) + (1 if body else 0)
            if body and size + added > CHUNK_CHARS:
                flush()
                added = len(piece)
            body.append(piece)
            size += added
    flush()
    return chunks
