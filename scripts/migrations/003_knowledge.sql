-- Indexed knowledge: one row per page and one per chunk, partitioned by tenant
-- and by scope (the granted root a page was found under).
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS knowledge_pages (
  tenant_id     text        NOT NULL,
  scope         text        NOT NULL,
  page_id       text        NOT NULL,
  title         text        NOT NULL,
  url           text        NOT NULL,
  content_hash  text        NOT NULL,
  indexed_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, scope, page_id)
);

CREATE TABLE IF NOT EXISTS knowledge_chunks (
  tenant_id  text         NOT NULL,
  scope      text         NOT NULL,
  page_id    text         NOT NULL,
  chunk_no   integer      NOT NULL,
  text       text         NOT NULL,
  embedding  vector(1024) NOT NULL,
  PRIMARY KEY (tenant_id, scope, page_id, chunk_no),
  FOREIGN KEY (tenant_id, scope, page_id)
    REFERENCES knowledge_pages (tenant_id, scope, page_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS knowledge_chunks_embedding
  ON knowledge_chunks USING hnsw (embedding vector_cosine_ops);
