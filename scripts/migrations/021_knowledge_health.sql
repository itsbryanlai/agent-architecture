-- Knowledge indexing: per-page sync state and a per-scope health row.
--
-- knowledge_pages gains what a sync needs to skip an unchanged page and what a search needs
-- to check a page with two or three Notion calls:
--   embed_model          the embedding model of the page's chunks; a search only returns
--                        chunks of the model in use, so vector spaces never mix
--   last_edited_time     Notion's value when the page was read; empty on rows from before
--                        this migration, which makes the next run read them again
--   parent_ref           the ID of the parent Notion reported (a page, a block or a
--                        database), to notice a move
--   ancestors            page IDs from the scope's root page down to the parent
--   child_page_ids       sub-pages found in the page's blocks
--   child_database_ids   child databases found in the page's blocks
--   row_page_ids         the rows of those databases at the last read
-- knowledge_scope_health has one row per scope whose root page is gone or unshared. A run
-- that finds the root gone deletes nothing; the row counts consecutive runs and records when
-- the owner was told. It holds IDs, counts and times, never page text.
-- Run as the owner role, after 015. Safe to run more than once.

ALTER TABLE knowledge_pages
  ADD COLUMN IF NOT EXISTS embed_model        text   NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS last_edited_time   text   NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS parent_ref         text   NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS ancestors          text[] NOT NULL DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS child_page_ids     text[] NOT NULL DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS child_database_ids text[] NOT NULL DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS row_page_ids       text[] NOT NULL DEFAULT '{}';

CREATE TABLE IF NOT EXISTS knowledge_scope_health (
  tenant_id       text        NOT NULL,
  scope           text        NOT NULL,
  consecutive_gone integer    NOT NULL,
  last_run        text        NOT NULL,   -- start time of the last run counted
  first_seen      timestamptz NOT NULL DEFAULT now(),
  notified_at     timestamptz,
  PRIMARY KEY (tenant_id, scope)
);

GRANT USAGE ON SCHEMA public TO agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON knowledge_scope_health TO agentos_app;

ALTER TABLE knowledge_scope_health ENABLE ROW LEVEL SECURITY;
ALTER TABLE knowledge_scope_health FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON knowledge_scope_health;
CREATE POLICY tenant_isolation ON knowledge_scope_health
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
