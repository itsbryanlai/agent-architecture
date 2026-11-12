"""Activities that index a granted page tree into the knowledge store.

Rules kept here:

* Page text is fetched, chunked, embedded and stored inside one activity per
  page. Temporal workflow history sees only page IDs and counts, never text.
* The agent uses its own access: pages are read with the same Notion token the
  agents use, and only under a root page an agent was granted.
* Log lines carry the tenant, scope and page ID only.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field

from temporalio import activity

from agentos.activities import _is_client_error, _non_retryable
from agentos.config import EMBED_DIMENSIONS
from agentos.connectors.notion import NotionAPI, is_unreachable, normalize_id
from agentos.knowledge.embedder import Embedder
from agentos.knowledge.notion_text import PageUnavailable, chunk, page_text
from agentos.knowledge.store import KnowledgeStore

log = logging.getLogger("agentos.indexing")


@dataclass
class SyncResult:
    child_page_ids: list[str] = field(default_factory=list)
    changed: bool = False
    chunks: int = 0
    skipped: bool = False


def content_hash(model: str, title: str, chunks: list[str]) -> str:
    """Hash of everything that decides what is stored for a page.

    The embedding model and its dimensions are part of it, so changing the
    model re-indexes every page.
    """
    h = hashlib.sha256()
    for part in (model, str(EMBED_DIMENSIONS), title, "\x00".join(chunks)):
        h.update(part.encode())
        h.update(b"\x01")
    return h.hexdigest()


class IndexActivities:
    def __init__(self, notion_api: NotionAPI, store: KnowledgeStore, embedder: Embedder):
        self.api = notion_api
        self.store = store
        self.embedder = embedder

    def all(self) -> list:
        return [self.sync_page, self.prune_scope]

    @activity.defn(name="sync_page")
    async def sync_page(self, tenant_id: str, scope: str, page_id: str) -> SyncResult:
        """Read one page and, when its content changed, embed and store it.

        A trashed or unreachable page (Notion 404 or 403) is skipped, with no
        sub-pages. Otherwise the sub-page IDs are returned so the workflow can
        go on down the tree. The page is re-embedded only when its content
        hash differs from the stored one.
        """
        page_id = normalize_id(page_id)
        try:
            content = await page_text(self.api, page_id)
        except PageUnavailable:
            log.info("page skipped tenant_id=%s scope=%s page_id=%s", tenant_id, scope, page_id)
            return SyncResult(skipped=True)
        except Exception as err:
            if not is_unreachable(err):
                if _is_client_error(err):
                    log.warning(
                        "page rejected tenant_id=%s scope=%s page_id=%s error_type=%s",
                        tenant_id,
                        scope,
                        page_id,
                        type(err).__name__,
                    )
                    raise _non_retryable(err) from err
                raise
            log.info("page skipped tenant_id=%s scope=%s page_id=%s", tenant_id, scope, page_id)
            return SyncResult(skipped=True)

        try:
            return await self._index(tenant_id, scope, page_id, content)
        except Exception as err:
            if _is_client_error(err):
                # The embedder or the store rejected the request for good:
                # retrying sends the same request and fails the same way.
                log.warning(
                    "page rejected tenant_id=%s scope=%s page_id=%s error_type=%s",
                    tenant_id,
                    scope,
                    page_id,
                    type(err).__name__,
                )
                raise _non_retryable(err) from err
            raise

    async def _index(self, tenant_id: str, scope: str, page_id: str, content) -> SyncResult:
        chunks = chunk(content.title, content.blocks)
        digest = content_hash(self.embedder.model, content.title, chunks)
        stored = await self.store.page_hashes(tenant_id, scope)
        if stored.get(page_id) == digest:
            return SyncResult(content.child_page_ids, changed=False, chunks=len(chunks))
        vectors = await self.embedder.embed(chunks) if chunks else []
        await self.store.replace_page(
            tenant_id,
            scope,
            page_id,
            content.title,
            content.url,
            digest,
            list(zip(chunks, vectors)),
        )
        log.info(
            "page indexed tenant_id=%s scope=%s page_id=%s chunks=%d",
            tenant_id,
            scope,
            page_id,
            len(chunks),
        )
        return SyncResult(content.child_page_ids, changed=True, chunks=len(chunks))

    @activity.defn(name="prune_scope")
    async def prune_scope(self, tenant_id: str, scope: str, seen_page_ids: list[str]) -> int:
        """Delete stored pages of the scope that are not in ``seen_page_ids``."""
        seen = set(seen_page_ids)
        stored = await self.store.page_hashes(tenant_id, scope)
        gone = [page_id for page_id in stored if page_id not in seen]
        deleted = await self.store.delete_pages(tenant_id, scope, gone)
        log.info("scope pruned tenant_id=%s scope=%s pages=%d", tenant_id, scope, deleted)
        return deleted
