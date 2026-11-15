"""The store of indexed knowledge: one row per page and one per chunk.

Rules kept here:

* The agent uses its own access. A search is limited to the scopes the caller
  passes (the roots granted to that agent), and the tenant and scope filter is
  part of the SQL query itself, never applied to results afterwards.
* Every row and every query carries the tenant ID.
* The database enforces tenant separation with row-level security: every call
  runs in a transaction that first sets the tenant. The explicit tenant
  conditions in the SQL are a second line of defense.
* Page text lives only in this store, never in Temporal workflow history.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.stores import tenant_transaction


@dataclass
class ChunkHit:
    scope: str
    page_id: str
    chunk_no: int
    text: str
    title: str
    url: str
    distance: float
    # Where the page sat when it was indexed (see ``PageSync``); empty on a row from before
    # that was recorded.
    parent_ref: str = ""
    ancestors: list[str] = field(default_factory=list)


@dataclass
class PageSync:
    """What a sync records about a page besides its text.

    ``ancestors`` are page IDs from the scope's root page down to the page's parent (empty
    for the root itself). ``parent_ref`` is the ID of the parent Notion reported, which may
    be a block or a database: a search compares it with Notion's answer to notice a move.
    ``row_page_ids`` are the rows of the page's child databases at the last read.
    """

    embed_model: str = ""
    last_edited_time: str = ""
    parent_ref: str = ""
    ancestors: list[str] = field(default_factory=list)
    child_page_ids: list[str] = field(default_factory=list)
    child_database_ids: list[str] = field(default_factory=list)
    row_page_ids: list[str] = field(default_factory=list)


@dataclass
class PageState:
    """One stored page row, without its text."""

    content_hash: str
    indexed_at: datetime
    sync: PageSync


@dataclass
class ScopeHealth:
    """A scope whose root page is gone: how many runs in a row saw it, and whether the
    owner has been told."""

    consecutive_gone: int
    first_seen: datetime
    notified_at: datetime | None


class KnowledgeStore(Protocol):
    async def page_hashes(self, tenant_id: str, scope: str) -> dict[str, str]:
        """Page ID to content hash, for every page stored under the scope (a bulk read, for
        pruning)."""
        ...

    async def page_state(self, tenant_id: str, scope: str, page_id: str) -> PageState | None:
        """One stored page row, or None."""
        ...

    async def replace_page(
        self,
        tenant_id: str,
        scope: str,
        page_id: str,
        title: str,
        url: str,
        content_hash: str,
        chunks: list[tuple[str, list[float]]],
        sync: PageSync | None = None,
    ) -> None:
        """Upsert the page row and replace all its chunks, in one transaction."""
        ...

    async def touch_page(
        self, tenant_id: str, scope: str, page_id: str, content_hash: str, sync: PageSync
    ) -> None:
        """Record a new sync state and hash for a stored page whose chunks stay as they are.
        Does nothing for a page that is not stored."""
        ...

    async def delete_pages(
        self,
        tenant_id: str,
        scope: str,
        page_ids: list[str],
        indexed_before: datetime | None = None,
    ) -> int:
        """Delete pages (and their chunks); return how many pages were deleted.

        With ``indexed_before``, a page indexed at or after that time is kept.
        """
        ...

    async def scopes(self, tenant_id: str) -> list[str]:
        """Every scope that has stored pages for the tenant."""
        ...

    async def delete_scope(self, tenant_id: str, scope: str) -> int:
        """Delete every page (and chunk) of one scope; return how many pages."""
        ...

    async def search(
        self,
        tenant_id: str,
        scopes: list[str],
        query_embedding: list[float],
        limit: int,
        embed_model: str | None = None,
    ) -> list[ChunkHit]:
        """Nearest chunks within the tenant's given scopes. No scopes: no results.

        With ``embed_model``, only pages embedded with that model count, so vectors of
        another model are never compared with the query.
        """
        ...

    async def page_meta(
        self, tenant_id: str, scopes: list[str], page_ids: list[str]
    ) -> dict[str, tuple[str, str]]:
        """Page ID to (title, url), for pages within those scopes only."""
        ...

    async def record_scope_gone(self, tenant_id: str, scope: str, run: str) -> ScopeHealth:
        """Count one more run that found the scope's root page gone. ``run`` names the run
        (its start time): recording the same run twice counts once."""
        ...

    async def mark_scope_notified(self, tenant_id: str, scope: str) -> None: ...

    async def clear_scope_health(self, tenant_id: str, scope: str) -> None:
        """The root page is back, or the scope was pruned: forget the unhealthy record."""
        ...


def _vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(repr(float(v)) for v in vector) + "]"


_SYNC_COLUMNS = (
    "embed_model, last_edited_time, parent_ref, ancestors, child_page_ids,"
    " child_database_ids, row_page_ids"
)


def _sync_params(sync: PageSync) -> tuple:
    return (
        sync.embed_model,
        sync.last_edited_time,
        sync.parent_ref,
        list(sync.ancestors),
        list(sync.child_page_ids),
        list(sync.child_database_ids),
        list(sync.row_page_ids),
    )


class PostgresKnowledgeStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def page_hashes(self, tenant_id: str, scope: str) -> dict[str, str]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                "SELECT page_id, content_hash FROM knowledge_pages"
                " WHERE tenant_id = %s AND scope = %s",
                (tenant_id, scope),
            )
            return {page_id: h for page_id, h in await cur.fetchall()}

    async def page_state(self, tenant_id: str, scope: str, page_id: str) -> PageState | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT content_hash, indexed_at, {_SYNC_COLUMNS} FROM knowledge_pages"
                " WHERE tenant_id = %s AND scope = %s AND page_id = %s",
                (tenant_id, scope, page_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        content_hash, indexed_at, *rest = row
        return PageState(content_hash, indexed_at, PageSync(*rest))

    async def replace_page(
        self,
        tenant_id: str,
        scope: str,
        page_id: str,
        title: str,
        url: str,
        content_hash: str,
        chunks: list[tuple[str, list[float]]],
        sync: PageSync | None = None,
    ) -> None:
        sync = sync or PageSync()
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                f"""
                    INSERT INTO knowledge_pages
                      (tenant_id, scope, page_id, title, url, content_hash, indexed_at,
                       {_SYNC_COLUMNS})
                    VALUES (%s, %s, %s, %s, %s, %s, now(), %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tenant_id, scope, page_id) DO UPDATE
                      SET title = EXCLUDED.title, url = EXCLUDED.url,
                          content_hash = EXCLUDED.content_hash, indexed_at = now(),
                          embed_model = EXCLUDED.embed_model,
                          last_edited_time = EXCLUDED.last_edited_time,
                          parent_ref = EXCLUDED.parent_ref, ancestors = EXCLUDED.ancestors,
                          child_page_ids = EXCLUDED.child_page_ids,
                          child_database_ids = EXCLUDED.child_database_ids,
                          row_page_ids = EXCLUDED.row_page_ids
                    """,
                (tenant_id, scope, page_id, title, url, content_hash, *_sync_params(sync)),
            )
            await conn.execute(
                "DELETE FROM knowledge_chunks WHERE tenant_id = %s AND scope = %s AND page_id = %s",
                (tenant_id, scope, page_id),
            )
            if chunks:
                # One statement for all the chunks of the page.
                rows = ", ".join(["(%s, %s, %s, %s, %s, %s::vector)"] * len(chunks))
                params: list = []
                for chunk_no, (text, embedding) in enumerate(chunks):
                    params += [
                        tenant_id,
                        scope,
                        page_id,
                        chunk_no,
                        text,
                        _vector_literal(embedding),
                    ]
                await conn.execute(
                    "INSERT INTO knowledge_chunks"
                    " (tenant_id, scope, page_id, chunk_no, text, embedding)"
                    f" VALUES {rows}",
                    params,
                )

    async def touch_page(
        self, tenant_id: str, scope: str, page_id: str, content_hash: str, sync: PageSync
    ) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                """
                UPDATE knowledge_pages
                SET content_hash = %s, indexed_at = now(), embed_model = %s,
                    last_edited_time = %s, parent_ref = %s, ancestors = %s,
                    child_page_ids = %s, child_database_ids = %s, row_page_ids = %s
                WHERE tenant_id = %s AND scope = %s AND page_id = %s
                """,
                (content_hash, *_sync_params(sync), tenant_id, scope, page_id),
            )

    async def delete_pages(
        self,
        tenant_id: str,
        scope: str,
        page_ids: list[str],
        indexed_before: datetime | None = None,
    ) -> int:
        if not page_ids:
            return 0
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                "DELETE FROM knowledge_pages"
                " WHERE tenant_id = %s AND scope = %s AND page_id = ANY(%s)"
                " AND (%s::timestamptz IS NULL OR indexed_at < %s::timestamptz)",
                (tenant_id, scope, list(page_ids), indexed_before, indexed_before),
            )
            return cur.rowcount

    async def scopes(self, tenant_id: str) -> list[str]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                "SELECT DISTINCT scope FROM knowledge_pages WHERE tenant_id = %s",
                (tenant_id,),
            )
            return [scope for (scope,) in await cur.fetchall()]

    async def delete_scope(self, tenant_id: str, scope: str) -> int:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                "DELETE FROM knowledge_pages WHERE tenant_id = %s AND scope = %s",
                (tenant_id, scope),
            )
            return cur.rowcount

    async def search(
        self,
        tenant_id: str,
        scopes: list[str],
        query_embedding: list[float],
        limit: int,
        embed_model: str | None = None,
    ) -> list[ChunkHit]:
        if not scopes:
            return []
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute("SET LOCAL hnsw.iterative_scan = strict_order")
            cur = await conn.execute(
                """
                    SELECT c.scope, c.page_id, c.chunk_no, c.text, p.title, p.url,
                           c.embedding <=> %(q)s::vector AS distance,
                           p.parent_ref, p.ancestors
                    FROM knowledge_chunks c
                    JOIN knowledge_pages p USING (tenant_id, scope, page_id)
                    WHERE c.tenant_id = %(tenant)s AND c.scope = ANY(%(scopes)s)
                      AND (%(model)s::text IS NULL OR p.embed_model = %(model)s::text)
                    ORDER BY c.embedding <=> %(q)s::vector
                    LIMIT %(limit)s
                    """,
                {
                    "q": _vector_literal(query_embedding),
                    "tenant": tenant_id,
                    "scopes": list(scopes),
                    "model": embed_model,
                    "limit": limit,
                },
            )
            rows = await cur.fetchall()
        return [ChunkHit(*row) for row in rows]

    async def page_meta(
        self, tenant_id: str, scopes: list[str], page_ids: list[str]
    ) -> dict[str, tuple[str, str]]:
        if not scopes or not page_ids:
            return {}
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                SELECT page_id, title, url FROM knowledge_pages
                WHERE tenant_id = %s AND scope = ANY(%s) AND page_id = ANY(%s)
                """,
                (tenant_id, list(scopes), list(page_ids)),
            )
            return {page_id: (title, url) for page_id, title, url in await cur.fetchall()}

    async def record_scope_gone(self, tenant_id: str, scope: str, run: str) -> ScopeHealth:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                INSERT INTO knowledge_scope_health (tenant_id, scope, consecutive_gone, last_run)
                VALUES (%s, %s, 1, %s)
                ON CONFLICT (tenant_id, scope) DO UPDATE
                  SET consecutive_gone = CASE
                        WHEN knowledge_scope_health.last_run = EXCLUDED.last_run
                        THEN knowledge_scope_health.consecutive_gone
                        ELSE knowledge_scope_health.consecutive_gone + 1 END,
                      last_run = EXCLUDED.last_run
                RETURNING consecutive_gone, first_seen, notified_at
                """,
                (tenant_id, scope, run),
            )
            row = await cur.fetchone()
        return ScopeHealth(*row)

    async def mark_scope_notified(self, tenant_id: str, scope: str) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                "UPDATE knowledge_scope_health SET notified_at = now()"
                " WHERE tenant_id = %s AND scope = %s AND notified_at IS NULL",
                (tenant_id, scope),
            )

    async def clear_scope_health(self, tenant_id: str, scope: str) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                "DELETE FROM knowledge_scope_health WHERE tenant_id = %s AND scope = %s",
                (tenant_id, scope),
            )


class InMemoryKnowledgeStore:
    """Test store. Search applies the same tenant and scope filter as the SQL.

    ``clock`` stamps ``indexed_at``; a test passes its own to place a write before or after a
    run started.
    """

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or (lambda: datetime.now(UTC))
        # (tenant, scope, page_id) -> (title, url, content_hash)
        self.pages: dict[tuple[str, str, str], tuple[str, str, str]] = {}
        # (tenant, scope, page_id) -> [(text, embedding)]
        self.chunks: dict[tuple[str, str, str], list[tuple[str, list[float]]]] = {}
        # (tenant, scope, page_id) -> (indexed_at, sync)
        self.states: dict[tuple[str, str, str], tuple[datetime, PageSync]] = {}
        # (tenant, scope) -> [consecutive_gone, last_run, first_seen, notified_at]
        self.health: dict[tuple[str, str], list] = {}
        self.page_state_reads = 0

    async def page_hashes(self, tenant_id: str, scope: str) -> dict[str, str]:
        return {
            page_id: meta[2]
            for (tenant, sc, page_id), meta in self.pages.items()
            if tenant == tenant_id and sc == scope
        }

    async def page_state(self, tenant_id: str, scope: str, page_id: str) -> PageState | None:
        self.page_state_reads += 1
        key = (tenant_id, scope, page_id)
        if key not in self.pages:
            return None
        indexed_at, sync = self.states[key]
        return PageState(self.pages[key][2], indexed_at, _copy_sync(sync))

    async def replace_page(
        self,
        tenant_id: str,
        scope: str,
        page_id: str,
        title: str,
        url: str,
        content_hash: str,
        chunks: list[tuple[str, list[float]]],
        sync: PageSync | None = None,
    ) -> None:
        key = (tenant_id, scope, page_id)
        self.pages[key] = (title, url, content_hash)
        self.chunks[key] = [(text, list(vec)) for text, vec in chunks]
        self.states[key] = (self._clock(), _copy_sync(sync or PageSync()))

    async def touch_page(
        self, tenant_id: str, scope: str, page_id: str, content_hash: str, sync: PageSync
    ) -> None:
        key = (tenant_id, scope, page_id)
        if key not in self.pages:
            return
        title, url, _ = self.pages[key]
        self.pages[key] = (title, url, content_hash)
        self.states[key] = (self._clock(), _copy_sync(sync))

    async def delete_pages(
        self,
        tenant_id: str,
        scope: str,
        page_ids: list[str],
        indexed_before: datetime | None = None,
    ) -> int:
        deleted = 0
        for page_id in page_ids:
            key = (tenant_id, scope, page_id)
            if key not in self.pages:
                continue
            if indexed_before is not None and self.states[key][0] >= indexed_before:
                continue
            del self.pages[key]
            self.chunks.pop(key, None)
            self.states.pop(key, None)
            deleted += 1
        return deleted

    async def scopes(self, tenant_id: str) -> list[str]:
        return sorted({sc for (tenant, sc, _) in self.pages if tenant == tenant_id})

    async def delete_scope(self, tenant_id: str, scope: str) -> int:
        return await self.delete_pages(
            tenant_id,
            scope,
            [p for (t, sc, p) in list(self.pages) if (t, sc) == (tenant_id, scope)],
        )

    async def search(
        self,
        tenant_id: str,
        scopes: list[str],
        query_embedding: list[float],
        limit: int,
        embed_model: str | None = None,
    ) -> list[ChunkHit]:
        if not scopes:
            return []
        hits: list[ChunkHit] = []
        for (tenant, scope, page_id), chunks in self.chunks.items():
            if tenant != tenant_id or scope not in scopes:
                continue
            title, url, _ = self.pages[(tenant, scope, page_id)]
            sync = self.states[(tenant, scope, page_id)][1]
            if embed_model is not None and sync.embed_model != embed_model:
                continue
            for chunk_no, (text, vector) in enumerate(chunks):
                hits.append(
                    ChunkHit(
                        scope,
                        page_id,
                        chunk_no,
                        text,
                        title,
                        url,
                        _cosine_distance(query_embedding, vector),
                        sync.parent_ref,
                        list(sync.ancestors),
                    )
                )
        hits.sort(key=lambda h: h.distance)
        return hits[:limit]

    async def page_meta(
        self, tenant_id: str, scopes: list[str], page_ids: list[str]
    ) -> dict[str, tuple[str, str]]:
        found: dict[str, tuple[str, str]] = {}
        for (tenant, scope, page_id), (title, url, _) in self.pages.items():
            if tenant == tenant_id and scope in scopes and page_id in page_ids:
                found.setdefault(page_id, (title, url))
        return found

    async def record_scope_gone(self, tenant_id: str, scope: str, run: str) -> ScopeHealth:
        key = (tenant_id, scope)
        row = self.health.get(key)
        if row is None:
            row = self.health[key] = [1, run, self._clock(), None]
        elif row[1] != run:
            row[0] += 1
            row[1] = run
        return ScopeHealth(row[0], row[2], row[3])

    async def mark_scope_notified(self, tenant_id: str, scope: str) -> None:
        row = self.health.get((tenant_id, scope))
        if row is not None and row[3] is None:
            row[3] = self._clock()

    async def clear_scope_health(self, tenant_id: str, scope: str) -> None:
        self.health.pop((tenant_id, scope), None)


def _copy_sync(sync: PageSync) -> PageSync:
    return PageSync(
        sync.embed_model,
        sync.last_edited_time,
        sync.parent_ref,
        list(sync.ancestors),
        list(sync.child_page_ids),
        list(sync.child_database_ids),
        list(sync.row_page_ids),
    )


def _cosine_distance(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm = math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b))
    return 1.0 - dot / norm if norm else 1.0
