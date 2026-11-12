-- Every table starts its primary key with tenant_id: the skeleton runs one
-- tenant by configuration, but the data is already shaped for many.

CREATE TABLE conversation_messages (
  tenant_id       text        NOT NULL,
  conversation_id text        NOT NULL,
  seq             integer     NOT NULL,
  step_key        text,                   -- names the append; repeats are ignored
  role            text        NOT NULL,   -- 'user' | 'assistant'
  content         jsonb       NOT NULL,   -- Anthropic content blocks
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, conversation_id, seq),
  UNIQUE (tenant_id, conversation_id, step_key)
);

CREATE TABLE idempotency_records (
  tenant_id  text        NOT NULL,
  idem_key   text        NOT NULL,
  value      jsonb       NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, idem_key)
);

CREATE TABLE task_links (
  tenant_id   text NOT NULL,
  page_id     text NOT NULL,
  task_id     text NOT NULL,
  PRIMARY KEY (tenant_id, page_id)
);

-- Tool output text lives here, not in workflow history.
CREATE TABLE IF NOT EXISTS tool_outputs (
  tenant_id       text        NOT NULL,
  conversation_id text        NOT NULL,
  tool_use_id     text        NOT NULL,
  content         text        NOT NULL,
  is_error        boolean     NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, conversation_id, tool_use_id)
);

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
