"""Activities that index a granted page tree into the knowledge store.

Rules kept here:

* Page text is fetched, chunked, embedded and stored inside one activity per
  page. Temporal workflow history sees only page IDs and counts, never text.
* Every activity works for exactly one tenant, named by its ``tenant_id``
  argument. It reads pages with that tenant's own Notion client and only under
  the root pages that tenant granted. An unknown tenant is a non-retryable
  failure, never a fallback to another tenant.
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
from agentos.agent_store import AgentStore, with_published_agents
from agentos.config import EMBED_DIMENSIONS
from agentos.connectors.notion import is_unreachable, normalize_id
from agentos.knowledge.embedder import Embedder
from agentos.knowledge.notion_text import PageUnavailable, chunk, page_text
from agentos.knowledge.search import PageChecker
from agentos.knowledge.store import KnowledgeStore
from agentos.tenants import TenantRegistry, TenantRuntime, UnknownTenant, granted_roots

log = logging.getLogger("agentos.indexing")


class ScopeNotGranted(Exception):
    """No published agent of the tenant has the scope granted any more."""


@dataclass
class SyncResult:
    child_page_ids: list[str] = field(default_factory=list)
    changed: bool = False
    chunks: int = 0
    skipped: bool = False


@dataclass
class RefreshResult:
    """Scopes only, never page text: indexed scopes and scopes the page left."""

    indexed: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)


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
    def __init__(
        self,
        registry: TenantRegistry,
        store: KnowledgeStore,
        embedder: Embedder,
        agents: AgentStore,
    ):
        self.registry = registry
        self.agents = agents
        self.store = store
        self.embedder = embedder

    def _runtime(self, tenant_id: str) -> TenantRuntime:
        """The config and Notion client of the tenant an activity works for."""
        try:
            return self.registry.get(tenant_id)
        except UnknownTenant as err:
            raise _non_retryable(err) from err

    async def _granted(self, runtime: TenantRuntime) -> dict[str, str]:
        """Scope to root page for the pages granted by the tenant's published agents."""
        return granted_roots(await with_published_agents(self.agents, runtime.config))

    async def _require_granted(self, tenant_id: str, scope: str) -> TenantRuntime:
        """The tenant's runtime, only while some published agent of it has the scope granted.

        A scope is derived from its root page ID, so a scope still granted
        always has the root it was started with. A schedule left over from a
        removed grant fails here, before any Notion or embedding call.
        """
        runtime = self._runtime(tenant_id)
        if scope not in await self._granted(runtime):
            log.warning("scope not granted tenant_id=%s scope=%s", tenant_id, scope)
            raise _non_retryable(ScopeNotGranted(f"scope {scope} is no longer granted"))
        return runtime

    def all(self) -> list:
        return [self.sync_page, self.prune_scope, self.refresh_page]

    @activity.defn(name="sync_page")
    async def sync_page(self, tenant_id: str, scope: str, page_id: str) -> SyncResult:
        """Read one page and, when its content changed, embed and store it.

        A trashed or unreachable page (Notion 404 or 403) is skipped, with no
        sub-pages. Otherwise the sub-page IDs are returned so the workflow can
        go on down the tree. The page is re-embedded only when its content
        hash differs from the stored one.
        """
        page_id = normalize_id(page_id)
        runtime = await self._require_granted(tenant_id, scope)
        try:
            content = await page_text(runtime.notion_api, page_id)
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

    @activity.defn(name="refresh_page")
    async def refresh_page(self, tenant_id: str, page_id: str) -> RefreshResult:
        """Bring one page up to date in every granted scope, after a Notion event.

        For each granted root: a page that is live and still under the root is
        re-read and re-embedded if its content changed (this also indexes a
        newly created page, or one moved in from outside); a page that is no
        longer under the root, or is trashed, is removed from that scope.
        Only this one page is synced: sub-pages of a newly created or moved-in
        page wait for the next full indexing run. Notion lookups are cached
        for the call by one page checker. The result and the logs hold scope
        and page IDs only, never page text.
        """
        page_id = normalize_id(page_id)
        runtime = self._runtime(tenant_id)
        checker = PageChecker(runtime.notion_api)
        result = RefreshResult()
        for scope, root in (await self._granted(runtime)).items():
            if await checker.under_root(page_id, root):
                synced = await self.sync_page(tenant_id, scope, page_id)
                if not synced.skipped:
                    result.indexed.append(scope)
            elif page_id in await self.store.page_hashes(tenant_id, scope):
                await self.store.delete_pages(tenant_id, scope, [page_id])
                result.removed.append(scope)
                log.info("page removed tenant_id=%s scope=%s page_id=%s", tenant_id, scope, page_id)
        return result

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
        await self._require_granted(tenant_id, scope)
        seen = set(seen_page_ids)
        stored = await self.store.page_hashes(tenant_id, scope)
        gone = [page_id for page_id in stored if page_id not in seen]
        deleted = await self.store.delete_pages(tenant_id, scope, gone)
        log.info("scope pruned tenant_id=%s scope=%s pages=%d", tenant_id, scope, deleted)
        return deleted
