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
from dataclasses import dataclass
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


class KnowledgeStore(Protocol):
    async def page_hashes(self, tenant_id: str, scope: str) -> dict[str, str]:
        """Page ID to content hash, for every page stored under the scope."""
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
    ) -> None:
        """Upsert the page row and replace all its chunks, in one transaction."""
        ...

    async def delete_pages(self, tenant_id: str, scope: str, page_ids: list[str]) -> int:
        """Delete pages (and their chunks); return how many pages were deleted."""
        ...

    async def scopes(self, tenant_id: str) -> list[str]:
        """Every scope that has stored pages for the tenant."""
        ...

    async def delete_scope(self, tenant_id: str, scope: str) -> int:
        """Delete every page (and chunk) of one scope; return how many pages."""
        ...

    async def search(
        self, tenant_id: str, scopes: list[str], query_embedding: list[float], limit: int
    ) -> list[ChunkHit]:
        """Nearest chunks within the tenant's given scopes. No scopes: no results."""
        ...

    async def page_meta(
        self, tenant_id: str, scopes: list[str], page_ids: list[str]
    ) -> dict[str, tuple[str, str]]:
        """Page ID to (title, url), for pages within those scopes only."""
        ...


def _vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(repr(float(v)) for v in vector) + "]"


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

    async def replace_page(
        self,
        tenant_id: str,
        scope: str,
        page_id: str,
        title: str,
        url: str,
        content_hash: str,
        chunks: list[tuple[str, list[float]]],
    ) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                """
                    INSERT INTO knowledge_pages
                      (tenant_id, scope, page_id, title, url, content_hash, indexed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (tenant_id, scope, page_id) DO UPDATE
                      SET title = EXCLUDED.title, url = EXCLUDED.url,
                          content_hash = EXCLUDED.content_hash, indexed_at = now()
                    """,
                (tenant_id, scope, page_id, title, url, content_hash),
            )
            await conn.execute(
                "DELETE FROM knowledge_chunks WHERE tenant_id = %s AND scope = %s AND page_id = %s",
                (tenant_id, scope, page_id),
            )
            for chunk_no, (text, embedding) in enumerate(chunks):
                await conn.execute(
                    """
                        INSERT INTO knowledge_chunks
                          (tenant_id, scope, page_id, chunk_no, text, embedding)
                        VALUES (%s, %s, %s, %s, %s, %s::vector)
                        """,
                    (tenant_id, scope, page_id, chunk_no, text, _vector_literal(embedding)),
                )

    async def delete_pages(self, tenant_id: str, scope: str, page_ids: list[str]) -> int:
        if not page_ids:
            return 0
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                "DELETE FROM knowledge_pages"
                " WHERE tenant_id = %s AND scope = %s AND page_id = ANY(%s)",
                (tenant_id, scope, list(page_ids)),
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
        self, tenant_id: str, scopes: list[str], query_embedding: list[float], limit: int
    ) -> list[ChunkHit]:
        if not scopes:
            return []
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute("SET LOCAL hnsw.iterative_scan = strict_order")
            cur = await conn.execute(
                """
                    SELECT c.scope, c.page_id, c.chunk_no, c.text, p.title, p.url,
                           c.embedding <=> %(q)s::vector AS distance
                    FROM knowledge_chunks c
                    JOIN knowledge_pages p USING (tenant_id, scope, page_id)
                    WHERE c.tenant_id = %(tenant)s AND c.scope = ANY(%(scopes)s)
                    ORDER BY c.embedding <=> %(q)s::vector
                    LIMIT %(limit)s
                    """,
                {
                    "q": _vector_literal(query_embedding),
                    "tenant": tenant_id,
                    "scopes": list(scopes),
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


class InMemoryKnowledgeStore:
    """Test store. Search applies the same tenant and scope filter as the SQL."""

    def __init__(self) -> None:
        # (tenant, scope, page_id) -> (title, url, content_hash)
        self.pages: dict[tuple[str, str, str], tuple[str, str, str]] = {}
        # (tenant, scope, page_id) -> [(text, embedding)]
        self.chunks: dict[tuple[str, str, str], list[tuple[str, list[float]]]] = {}

    async def page_hashes(self, tenant_id: str, scope: str) -> dict[str, str]:
        return {
            page_id: meta[2]
            for (tenant, sc, page_id), meta in self.pages.items()
            if tenant == tenant_id and sc == scope
        }

    async def replace_page(
        self,
        tenant_id: str,
        scope: str,
        page_id: str,
        title: str,
        url: str,
        content_hash: str,
        chunks: list[tuple[str, list[float]]],
    ) -> None:
        key = (tenant_id, scope, page_id)
        self.pages[key] = (title, url, content_hash)
        self.chunks[key] = [(text, list(vec)) for text, vec in chunks]

    async def delete_pages(self, tenant_id: str, scope: str, page_ids: list[str]) -> int:
        deleted = 0
        for page_id in page_ids:
            key = (tenant_id, scope, page_id)
            if self.pages.pop(key, None) is not None:
                deleted += 1
            self.chunks.pop(key, None)
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
        self, tenant_id: str, scopes: list[str], query_embedding: list[float], limit: int
    ) -> list[ChunkHit]:
        if not scopes:
            return []
        hits: list[ChunkHit] = []
        for (tenant, scope, page_id), chunks in self.chunks.items():
            if tenant != tenant_id or scope not in scopes:
                continue
            title, url, _ = self.pages[(tenant, scope, page_id)]
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


def _cosine_distance(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm = math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b))
    return 1.0 - dot / norm if norm else 1.0
