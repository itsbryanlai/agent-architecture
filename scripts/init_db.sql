-- Every table that holds a tenant's rows starts its primary key with tenant_id: the
-- skeleton runs one tenant by configuration, but the data is already shaped for many.
-- The two tables with no tenant (schema_migrations, audit_kinds) are shared lookups.
-- This file equals migrations 000 to 043 applied in order; a test in tests/pg checks it.

CREATE TABLE conversation_messages (
  tenant_id       text        NOT NULL,
  conversation_id text        NOT NULL,
  seq             integer     NOT NULL,
  step_key        text,                   -- names the append; repeats are ignored
  role            text        NOT NULL,   -- 'user' | 'assistant'
  content         jsonb       NOT NULL,   -- Anthropic content blocks
  char_len        integer     NOT NULL DEFAULT 0,   -- size of the message as JSON, set on append
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, conversation_id, seq)
);

-- A repeated append of the same step finds this index and stores nothing. Same as migration 001.
CREATE UNIQUE INDEX IF NOT EXISTS conversation_messages_step_key
  ON conversation_messages (tenant_id, conversation_id, step_key);

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
  -- Sync state and search checks; see migration 021.
  embed_model        text   NOT NULL DEFAULT '',
  last_edited_time   text   NOT NULL DEFAULT '',
  parent_ref         text   NOT NULL DEFAULT '',
  ancestors          text[] NOT NULL DEFAULT '{}',
  child_page_ids     text[] NOT NULL DEFAULT '{}',
  child_database_ids text[] NOT NULL DEFAULT '{}',
  row_page_ids       text[] NOT NULL DEFAULT '{}',
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

-- Row-level security: every tenant table only shows rows of the tenant set for
-- the current transaction (app.tenant_id). The app connects as agentos_app,
-- which owns nothing and cannot bypass these policies. A transaction that has
-- not set app.tenant_id sees no rows and can write none.
-- Run as the owner role. Safe to run more than once.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON
  conversation_messages, idempotency_records, task_links, tool_outputs,
  knowledge_pages, knowledge_chunks
TO agentos_app;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['conversation_messages', 'idempotency_records', 'task_links',
                           'tool_outputs', 'knowledge_pages', 'knowledge_chunks'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', t);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I '
      'USING (tenant_id = current_setting(''app.tenant_id'', true)) '
      'WITH CHECK (tenant_id = current_setting(''app.tenant_id'', true))', t);
  END LOOP;
END
$$;

-- One row per scope whose root page is gone or unshared (migration 021).
CREATE TABLE IF NOT EXISTS knowledge_scope_health (
  tenant_id        text        NOT NULL,
  scope            text        NOT NULL,
  consecutive_gone integer     NOT NULL,
  last_run         text        NOT NULL,
  first_seen       timestamptz NOT NULL DEFAULT now(),
  notified_at      timestamptz,
  PRIMARY KEY (tenant_id, scope)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON knowledge_scope_health TO agentos_app;

ALTER TABLE knowledge_scope_health ENABLE ROW LEVEL SECURITY;
ALTER TABLE knowledge_scope_health FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON knowledge_scope_health;
CREATE POLICY tenant_isolation ON knowledge_scope_health
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- The audit event kinds, one row each. A migration that adds a kind adds a row. Same as migration 016.
CREATE TABLE IF NOT EXISTS audit_kinds (
  kind text PRIMARY KEY
);

INSERT INTO audit_kinds (kind) VALUES
  ('answer'), ('notice'), ('tool_call'), ('hand_off'), ('approval'), ('approval_closed'), ('write'),
  ('agent_create'), ('agent_draft'), ('agent_edit'), ('agent_import'), ('agent_submit'),
  ('agent_withdraw'), ('agent_reject'), ('agent_publish'),
  ('member_add'), ('member_remove'), ('member_role'),
  ('audience_refused'), ('audience_confirm'), ('budget_set'),
  ('audience_pause'), ('audience_resume'),
  ('switch_off'), ('switch_on'), ('usage_repriced'), ('retention_set'),
  ('grant_pick'), ('widening_confirm'), ('webhook_token'), ('payload_key'), ('install'),
  ('uninstall'), ('agent_health'), ('connector'), ('tenant_settings'), ('feedback'),
  ('policy_change')
ON CONFLICT DO NOTHING;

-- Audit log: append-only, one row per thing an agent did. Holds IDs and hashes,
-- never content. The app role can INSERT and SELECT only. Same as migration 005.
CREATE TABLE IF NOT EXISTS audit_events (
  seq                bigint      GENERATED ALWAYS AS IDENTITY,
  tenant_id          text        NOT NULL,
  event_key          text        NOT NULL,   -- names the event; repeats are ignored
  at                 timestamptz NOT NULL DEFAULT now(),
  kind               text        NOT NULL CONSTRAINT audit_events_kind_fkey
    REFERENCES audit_kinds (kind),
  agent_id           text,                   -- null only for a membership event, a tenant-wide budget or switch
  agent_version      integer,
  requester_user_id  text        NOT NULL,   -- Slack user who asked
  identity           text        NOT NULL,   -- whose access was used, e.g. agent:it-helper
  conversation_id    text,
  task_id            text,
  tool               text,
  target             text,                   -- a Notion page or database ID, or a specialist ID
  input_sha256       text        CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
  output_sha256      text        CHECK (output_sha256 ~ '^[0-9a-f]{64}$'),
  approver_user_id   text,
  outcome            text        NOT NULL CONSTRAINT audit_events_outcome_check
    CHECK (outcome IN ('ok', 'error', 'policy')),
  PRIMARY KEY (tenant_id, event_key),
  UNIQUE (seq),
  CONSTRAINT audit_events_agent_required CHECK (
    kind IN ('member_add', 'member_remove', 'member_role', 'budget_set',
             'switch_off', 'switch_on', 'usage_repriced', 'retention_set', 'webhook_token',
             'payload_key', 'install', 'uninstall', 'connector', 'tenant_settings',
             'policy_change')
    OR (kind = 'audience_resume' AND agent_id IS NOT NULL)
    OR (agent_id IS NOT NULL AND agent_version IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS audit_events_by_time ON audit_events (tenant_id, at DESC, seq DESC);

-- The feedback page reads one agent's rows in a time window. Same as migration 039.
CREATE INDEX IF NOT EXISTS audit_events_by_agent
  ON audit_events (tenant_id, agent_id, at DESC, seq DESC);

CREATE OR REPLACE FUNCTION audit_events_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'audit_events is append-only: % is not allowed', TG_OP;
END
$$;

DROP TRIGGER IF EXISTS audit_events_no_change ON audit_events;
CREATE TRIGGER audit_events_no_change
  BEFORE UPDATE OR DELETE ON audit_events
  FOR EACH ROW EXECUTE FUNCTION audit_events_append_only();

DROP TRIGGER IF EXISTS audit_events_no_truncate ON audit_events;
CREATE TRIGGER audit_events_no_truncate
  BEFORE TRUNCATE ON audit_events
  FOR EACH STATEMENT EXECUTE FUNCTION audit_events_append_only();

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON audit_events, audit_kinds FROM agentos_app;
GRANT INSERT, SELECT ON audit_events TO agentos_app;

ALTER TABLE audit_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON audit_events;
CREATE POLICY tenant_isolation ON audit_events
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Agents in the database: numbered versions with a draft, pending, published and
-- superseded lifecycle, enforced by a trigger. Same as migration 006.
CREATE TABLE IF NOT EXISTS agents (
  tenant_id   text        NOT NULL,
  agent_id    text        NOT NULL,
  created_by  text        NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, agent_id)
);

CREATE TABLE IF NOT EXISTS agent_versions (
  tenant_id     text        NOT NULL,
  agent_id      text        NOT NULL,
  version       integer     NOT NULL CHECK (version >= 1),   -- assigned by the store: max + 1
  status        text        NOT NULL
    CHECK (status IN ('draft', 'pending', 'published', 'superseded')),
  definition    jsonb       NOT NULL,   -- one agent block, as in the YAML file, without version
  created_by    text        NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  submitted_by  text,
  submitted_at  timestamptz,
  approved_by   text,
  published_at  timestamptz,
  PRIMARY KEY (tenant_id, agent_id, version),
  FOREIGN KEY (tenant_id, agent_id) REFERENCES agents (tenant_id, agent_id),
  CHECK (status <> 'pending' OR submitted_by IS NOT NULL),
  CHECK (status NOT IN ('published', 'superseded')
         OR (approved_by IS NOT NULL AND published_at IS NOT NULL))
);

-- At most one published version per agent.
CREATE UNIQUE INDEX IF NOT EXISTS agent_versions_one_published
  ON agent_versions (tenant_id, agent_id) WHERE status = 'published';

CREATE OR REPLACE FUNCTION agent_versions_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    IF OLD.status <> 'draft' THEN
      RAISE EXCEPTION 'agent_versions: only a draft can be deleted (status is %)', OLD.status;
    END IF;
    RETURN OLD;
  END IF;

  IF (NEW.tenant_id, NEW.agent_id, NEW.version, NEW.created_by, NEW.created_at)
     IS DISTINCT FROM (OLD.tenant_id, OLD.agent_id, OLD.version, OLD.created_by, OLD.created_at)
  THEN
    RAISE EXCEPTION 'agent_versions: the identity of a version cannot change';
  END IF;

  IF OLD.status IN ('published', 'superseded') THEN
    -- Frozen. The one move is published -> superseded, and only the status changes.
    IF NOT (OLD.status = 'published' AND NEW.status = 'superseded'
            AND (NEW.definition, NEW.submitted_by, NEW.submitted_at, NEW.approved_by,
                 NEW.published_at)
                IS NOT DISTINCT FROM
                (OLD.definition, OLD.submitted_by, OLD.submitted_at, OLD.approved_by,
                 OLD.published_at))
    THEN
      RAISE EXCEPTION 'agent_versions: a % version is immutable', OLD.status;
    END IF;
    RETURN NEW;
  END IF;

  IF NEW.definition IS DISTINCT FROM OLD.definition AND OLD.status <> 'draft' THEN
    RAISE EXCEPTION 'agent_versions: the definition changes only while a version is a draft';
  END IF;

  IF NEW.status <> OLD.status AND NOT (
       (OLD.status = 'draft'   AND NEW.status = 'pending')
    OR (OLD.status = 'pending' AND NEW.status IN ('draft', 'published'))
  ) THEN
    RAISE EXCEPTION 'agent_versions: % -> % is not allowed', OLD.status, NEW.status;
  END IF;
  RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS agent_versions_guard ON agent_versions;
CREATE TRIGGER agent_versions_guard
  BEFORE UPDATE OR DELETE ON agent_versions
  FOR EACH ROW EXECUTE FUNCTION agent_versions_guard();

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON agents, agent_versions FROM agentos_app;
GRANT SELECT, INSERT ON agents TO agentos_app;
GRANT SELECT, INSERT, UPDATE ON agent_versions TO agentos_app;

ALTER TABLE agents ENABLE ROW LEVEL SECURITY;
ALTER TABLE agents FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON agents;
CREATE POLICY tenant_isolation ON agents
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE agent_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_versions FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON agent_versions;
CREATE POLICY tenant_isolation ON agent_versions
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Members and roles: the admins and creators added through the app. Same as migration 007.
CREATE TABLE IF NOT EXISTS members (
  tenant_id  text        NOT NULL,
  user_id    text        NOT NULL,   -- Slack user ID
  role       text        NOT NULL CHECK (role IN ('admin', 'creator')),
  added_by   text        NOT NULL,
  added_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, user_id)
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON members FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON members TO agentos_app;

ALTER TABLE members ENABLE ROW LEVEL SECURITY;
ALTER TABLE members FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON members;
CREATE POLICY tenant_isolation ON members
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Web console sessions: one row per signed-in browser, session ID stored as a hash. Same as migration 008.
CREATE TABLE IF NOT EXISTS sessions (
  tenant_id    text        NOT NULL,
  session_hash text        NOT NULL,   -- SHA-256 (hex) of the session ID
  user_id      text        NOT NULL,   -- Slack user ID
  csrf_secret  text        NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  expires_at   timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, session_hash)
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON sessions FROM agentos_app;
GRANT SELECT, INSERT, DELETE ON sessions TO agentos_app;

ALTER TABLE sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON sessions;
CREATE POLICY tenant_isolation ON sessions
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Usage and cost: one row per model call or embedded page, never the requester. Same as migration 011.
CREATE TABLE IF NOT EXISTS usage_events (
  tenant_id          text        NOT NULL,
  usage_key          text        NOT NULL,   -- names the activity attempt that made the call
  kind               text        NOT NULL CHECK (kind IN ('llm', 'embedding')),
  agent_id           text,
  agent_version      integer,
  model              text        NOT NULL,
  input_tokens       bigint      CHECK (input_tokens >= 0),
  output_tokens      bigint      CHECK (output_tokens >= 0),
  cache_read_tokens  bigint      CHECK (cache_read_tokens >= 0),
  cache_write_tokens bigint      CHECK (cache_write_tokens >= 0),
  cost_usd           numeric(20, 8) CHECK (cost_usd >= 0),
  conversation_id    text,
  at                 timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, usage_key),
  CONSTRAINT usage_events_agent_required CHECK (
    kind <> 'llm' OR (agent_id IS NOT NULL AND agent_version IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS usage_events_by_month ON usage_events (tenant_id, at);

-- The retention job finds expired rows by age, per tenant. Same as migration 024.
CREATE INDEX IF NOT EXISTS idempotency_records_created ON idempotency_records (tenant_id, created_at);
CREATE INDEX IF NOT EXISTS tool_outputs_created ON tool_outputs (tenant_id, created_at);
CREATE INDEX IF NOT EXISTS sessions_expires ON sessions (tenant_id, expires_at);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON usage_events FROM agentos_app;
GRANT SELECT, INSERT ON usage_events TO agentos_app;

ALTER TABLE usage_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON usage_events;
CREATE POLICY tenant_isolation ON usage_events
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- The cost a usage row got after it was recorded, written only by the owner-run reprice command. Same as migration 019.
CREATE TABLE IF NOT EXISTS usage_adjustments (
  tenant_id            text           NOT NULL,
  usage_key            text           NOT NULL,
  cost_usd             numeric(20, 8) NOT NULL CHECK (cost_usd >= 0),
  price_model          text           NOT NULL,
  price_effective_from date           NOT NULL,
  repriced_at          timestamptz    NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, usage_key),
  FOREIGN KEY (tenant_id, usage_key) REFERENCES usage_events (tenant_id, usage_key)
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON usage_adjustments FROM agentos_app;
GRANT SELECT ON usage_adjustments TO agentos_app;

ALTER TABLE usage_adjustments ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_adjustments FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON usage_adjustments;
CREATE POLICY tenant_isolation ON usage_adjustments
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Monthly usage rollup, written with each usage row and each adjustment. Same as migration 026.
CREATE TABLE IF NOT EXISTS usage_month_totals (
  tenant_id       text           NOT NULL,
  month           date           NOT NULL CHECK (month = date_trunc('month', month)::date),
  agent_id        text           NOT NULL DEFAULT '',
  kind            text           NOT NULL CHECK (kind IN ('llm', 'embedding')),
  calls           bigint         NOT NULL DEFAULT 0 CHECK (calls >= 0),
  cost_usd        numeric(20, 8) NOT NULL DEFAULT 0 CHECK (cost_usd >= 0),
  unpriced_calls  bigint         NOT NULL DEFAULT 0 CHECK (unpriced_calls >= 0),
  PRIMARY KEY (tenant_id, month, agent_id, kind),
  CONSTRAINT usage_month_totals_agent_id_pattern
    CHECK (agent_id = '' OR agent_id ~ '^[a-z][a-z0-9-]{1,39}$')
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON usage_month_totals FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON usage_month_totals TO agentos_app;

ALTER TABLE usage_month_totals ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_month_totals FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON usage_month_totals;
CREATE POLICY tenant_isolation ON usage_month_totals
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Monthly budgets, per tenant and per agent. Same as migration 012.
CREATE TABLE IF NOT EXISTS budgets (
  tenant_id   text          NOT NULL,
  agent_id    text,                          -- null is the tenant-wide budget
  amount_usd  numeric(12, 2) NOT NULL CHECK (amount_usd > 0),
  updated_by  text          NOT NULL,
  updated_at  timestamptz   NOT NULL DEFAULT now(),
  scope       text          GENERATED ALWAYS AS (COALESCE(agent_id, '')) STORED,
  CONSTRAINT budgets_pkey PRIMARY KEY (tenant_id, scope)
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON budgets FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON budgets TO agentos_app;

ALTER TABLE budgets ENABLE ROW LEVEL SECURITY;
ALTER TABLE budgets FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON budgets;
CREATE POLICY tenant_isolation ON budgets
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Audience changes are watched: one row per pause of an agent in a channel. Same as migration 013.
CREATE TABLE IF NOT EXISTS audience_pauses (
  tenant_id   text        NOT NULL,
  agent_id    text        NOT NULL,
  channel     text        NOT NULL,
  reason      text        NOT NULL CHECK (reason IN ('channel_shared')),
  paused_at   timestamptz NOT NULL DEFAULT now(),
  resumed_by  text,
  resumed_at  timestamptz,
  PRIMARY KEY (tenant_id, agent_id, channel, paused_at),
  CONSTRAINT audience_pauses_resume_together CHECK (
    (resumed_by IS NULL) = (resumed_at IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS audience_pauses_one_active
  ON audience_pauses (tenant_id, agent_id, channel) WHERE resumed_at IS NULL;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON audience_pauses FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON audience_pauses TO agentos_app;

ALTER TABLE audience_pauses ENABLE ROW LEVEL SECURITY;
ALTER TABLE audience_pauses FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON audience_pauses;
CREATE POLICY tenant_isolation ON audience_pauses
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Kill switches: one row per switch that is off. Same as migration 014.
CREATE TABLE IF NOT EXISTS kill_switches (
  tenant_id      text        NOT NULL,
  scope          text        NOT NULL CHECK (scope IN ('agent', 'all_agents', 'connector', 'writes')),
  target         text        NOT NULL DEFAULT '',   -- agent ID or connector name; empty otherwise
  turned_off_by  text        NOT NULL,
  turned_off_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, scope, target),
  CONSTRAINT kill_switches_target_matches_scope CHECK (
    (scope IN ('agent', 'connector')) = (target <> ''))
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON kill_switches FROM agentos_app;
GRANT SELECT, INSERT, DELETE ON kill_switches TO agentos_app;

ALTER TABLE kill_switches ENABLE ROW LEVEL SECURITY;
ALTER TABLE kill_switches FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON kill_switches;
CREATE POLICY tenant_isolation ON kill_switches
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- The hash of the definition an admin approves: set by submit, checked by publish. Same as migration 017.

ALTER TABLE agent_versions ADD COLUMN IF NOT EXISTS definition_sha256 text;

-- The guard forbids changes to published rows; the backfill is the one change it must allow.
ALTER TABLE agent_versions DISABLE TRIGGER agent_versions_guard;
UPDATE agent_versions
SET definition_sha256 = encode(sha256(convert_to(definition::text, 'UTF8')), 'hex')
WHERE status IN ('pending', 'published', 'superseded') AND definition_sha256 IS NULL;
ALTER TABLE agent_versions ENABLE TRIGGER agent_versions_guard;

ALTER TABLE agent_versions DROP CONSTRAINT IF EXISTS agent_versions_hash_matches_status;
ALTER TABLE agent_versions ADD CONSTRAINT agent_versions_hash_matches_status CHECK (
  (status = 'draft') = (definition_sha256 IS NULL));

CREATE OR REPLACE FUNCTION agent_versions_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    IF OLD.status <> 'draft' THEN
      RAISE EXCEPTION 'agent_versions: only a draft can be deleted (status is %)', OLD.status;
    END IF;
    RETURN OLD;
  END IF;

  IF (NEW.tenant_id, NEW.agent_id, NEW.version, NEW.created_by, NEW.created_at)
     IS DISTINCT FROM (OLD.tenant_id, OLD.agent_id, OLD.version, OLD.created_by, OLD.created_at)
  THEN
    RAISE EXCEPTION 'agent_versions: the identity of a version cannot change';
  END IF;

  IF OLD.status IN ('published', 'superseded') THEN
    -- Frozen. The one move is published -> superseded, and only the status changes.
    IF NOT (OLD.status = 'published' AND NEW.status = 'superseded'
            AND (NEW.definition, NEW.definition_sha256, NEW.submitted_by, NEW.submitted_at,
                 NEW.approved_by, NEW.published_at)
                IS NOT DISTINCT FROM
                (OLD.definition, OLD.definition_sha256, OLD.submitted_by, OLD.submitted_at,
                 OLD.approved_by, OLD.published_at))
    THEN
      RAISE EXCEPTION 'agent_versions: a % version is immutable', OLD.status;
    END IF;
    RETURN NEW;
  END IF;

  IF NEW.definition IS DISTINCT FROM OLD.definition AND OLD.status <> 'draft' THEN
    RAISE EXCEPTION 'agent_versions: the definition changes only while a version is a draft';
  END IF;

  IF NEW.definition_sha256 IS DISTINCT FROM OLD.definition_sha256 AND NOT (
       (OLD.status = 'draft'   AND NEW.status = 'pending')
    OR (OLD.status = 'pending' AND NEW.status = 'draft')
  ) THEN
    RAISE EXCEPTION 'agent_versions: the definition hash changes only when a version is submitted or withdrawn';
  END IF;

  IF NEW.status <> OLD.status AND NOT (
       (OLD.status = 'draft'   AND NEW.status = 'pending')
    OR (OLD.status = 'pending' AND NEW.status IN ('draft', 'published'))
  ) THEN
    RAISE EXCEPTION 'agent_versions: % -> % is not allowed', OLD.status, NEW.status;
  END IF;
  RETURN NEW;
END
$$;


-- Tenants: one row per tenant, read across tenants only through two narrow functions. Same as migration 015.
CREATE TABLE IF NOT EXISTS tenants (
  tenant_id           text        PRIMARY KEY,
  slack_team_id       text        NOT NULL UNIQUE,
  enabled             boolean     NOT NULL DEFAULT true,
  -- NULL for a tenant installed from Slack with no agent yet (migration 032).
  default_agent       text,
  notion_workspace_id text        UNIQUE,
  -- The connected Notion workspace's name, for display (migration 034).
  notion_workspace_name text,
  channels            jsonb       NOT NULL DEFAULT '{}'::jsonb,   -- channel ID -> agent ID
  admins              jsonb       NOT NULL DEFAULT '[]'::jsonb,   -- Slack user IDs
  secret_env          jsonb       NOT NULL DEFAULT '{}'::jsonb,   -- field -> env var NAME
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT tenants_id_format CHECK (tenant_id ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
  CONSTRAINT tenants_channels_object CHECK (jsonb_typeof(channels) = 'object'),
  CONSTRAINT tenants_admins_array CHECK (jsonb_typeof(admins) = 'array'),
  CONSTRAINT tenants_secret_env_object CHECK (jsonb_typeof(secret_env) = 'object')
);

-- Retention settings: whole days per kind of data; empty means the defaults. Same as migration 024.
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS retention jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE tenants DROP CONSTRAINT IF EXISTS tenants_retention_object;
ALTER TABLE tenants ADD CONSTRAINT tenants_retention_object CHECK (jsonb_typeof(retention) = 'object');

-- Every process notices a change by updated_at, so an UPDATE always moves it, including
-- one an operator types by hand.
CREATE OR REPLACE FUNCTION tenants_touch() RETURNS trigger
LANGUAGE plpgsql SET search_path = public, pg_temp AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END
$$;
DROP TRIGGER IF EXISTS tenants_touch ON tenants;
CREATE TRIGGER tenants_touch BEFORE UPDATE ON tenants
  FOR EACH ROW EXECUTE FUNCTION tenants_touch();

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_directory') THEN
    CREATE ROLE agentos_directory NOLOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

-- Lets this script hand function ownership to agentos_directory.
GRANT agentos_directory TO CURRENT_USER;

GRANT USAGE ON SCHEMA public TO agentos_app, agentos_directory;
REVOKE ALL ON tenants FROM agentos_app, agentos_directory;
GRANT SELECT, INSERT, UPDATE ON tenants TO agentos_app;
GRANT SELECT ON tenants TO agentos_directory;

ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenants FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON tenants;
CREATE POLICY tenant_isolation ON tenants
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
DROP POLICY IF EXISTS tenant_directory ON tenants;
CREATE POLICY tenant_directory ON tenants FOR SELECT TO agentos_directory USING (true);

-- The tenant of a Slack workspace: its ID and whether it is enabled, nothing else.
CREATE OR REPLACE FUNCTION tenant_for_slack_team(team text)
RETURNS TABLE (tenant_id text, enabled boolean, updated_at timestamptz)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT t.tenant_id, t.enabled, t.updated_at FROM tenants t WHERE t.slack_team_id = team
$$;

-- Every tenant: its ID, whether it is enabled and when it last changed, nothing else.
CREATE OR REPLACE FUNCTION list_tenants()
RETURNS TABLE (tenant_id text, enabled boolean, updated_at timestamptz)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT t.tenant_id, t.enabled, t.updated_at FROM tenants t ORDER BY t.tenant_id
$$;

ALTER FUNCTION tenant_for_slack_team(text) OWNER TO agentos_directory;
ALTER FUNCTION list_tenants() OWNER TO agentos_directory;
REVOKE ALL ON FUNCTION tenant_for_slack_team(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION list_tenants() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION tenant_for_slack_team(text) TO agentos_app;
GRANT EXECUTE ON FUNCTION list_tenants() TO agentos_app;

-- External writes: which attempt may write to an outside system for one key, and what it returned. Same as migration 018.
CREATE TABLE IF NOT EXISTS external_writes (
  tenant_id   text        NOT NULL,
  idem_key    text        NOT NULL,
  state       text        NOT NULL CHECK (state IN ('claimed', 'issued', 'done')),
  result_ref  text,
  lease_until timestamptz NOT NULL,
  lease_token text        NOT NULL,   -- whoever holds the lease; every later step must present it
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, idem_key),
  CONSTRAINT external_writes_result_when_done CHECK ((state = 'done') = (result_ref IS NOT NULL))
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON external_writes FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON external_writes TO agentos_app;

ALTER TABLE external_writes ENABLE ROW LEVEL SECURITY;
ALTER TABLE external_writes FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON external_writes;
CREATE POLICY tenant_isolation ON external_writes
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Grants picked from Notion by the draft's owner. Same as migration 020.
CREATE TABLE IF NOT EXISTS grant_picks (
  tenant_id  text        NOT NULL,
  agent_id   text        NOT NULL,
  notion_id  text        NOT NULL,   -- normalised: lower case, no hyphens
  kind       text        NOT NULL CHECK (kind IN ('database', 'page')),
  picked_by  text        NOT NULL,
  picked_at  timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, agent_id, notion_id)
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON grant_picks FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON grant_picks TO agentos_app;

ALTER TABLE grant_picks ENABLE ROW LEVEL SECURITY;
ALTER TABLE grant_picks FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON grant_picks;
CREATE POLICY tenant_isolation ON grant_picks
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Agent IDs follow one pattern (migration 023): lower-case ASCII letters, digits and hyphens,
-- 2 to 40 characters, starting with a letter.
ALTER TABLE agents DROP CONSTRAINT IF EXISTS agents_agent_id_pattern;
ALTER TABLE agents ADD CONSTRAINT agents_agent_id_pattern
  CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE agent_versions DROP CONSTRAINT IF EXISTS agent_versions_agent_id_pattern;
ALTER TABLE agent_versions ADD CONSTRAINT agent_versions_agent_id_pattern
  CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_id_pattern;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_id_pattern
  CHECK (agent_id IS NULL OR agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE usage_events DROP CONSTRAINT IF EXISTS usage_events_agent_id_pattern;
ALTER TABLE usage_events ADD CONSTRAINT usage_events_agent_id_pattern
  CHECK (agent_id IS NULL OR agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE budgets DROP CONSTRAINT IF EXISTS budgets_agent_id_pattern;
ALTER TABLE budgets ADD CONSTRAINT budgets_agent_id_pattern
  CHECK (agent_id IS NULL OR agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE audience_pauses DROP CONSTRAINT IF EXISTS audience_pauses_agent_id_pattern;
ALTER TABLE audience_pauses ADD CONSTRAINT audience_pauses_agent_id_pattern
  CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE grant_picks DROP CONSTRAINT IF EXISTS grant_picks_agent_id_pattern;
ALTER TABLE grant_picks ADD CONSTRAINT grant_picks_agent_id_pattern
  CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE kill_switches DROP CONSTRAINT IF EXISTS kill_switches_agent_target_pattern;
ALTER TABLE kill_switches ADD CONSTRAINT kill_switches_agent_target_pattern
  CHECK (scope <> 'agent' OR target ~ '^[a-z][a-z0-9-]{1,39}$');

-- Thread agents and owner mentions. Same as migration 025.
CREATE TABLE IF NOT EXISTS threads (
  tenant_id   text        NOT NULL,
  thread_key  text        NOT NULL,
  agent_id    text        NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, thread_key)
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON threads FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON threads TO agentos_app;

ALTER TABLE threads ENABLE ROW LEVEL SECURITY;
ALTER TABLE threads FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON threads;
CREATE POLICY tenant_isolation ON threads
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

CREATE TABLE IF NOT EXISTS owner_pings (
  tenant_id      text NOT NULL,
  agent_id       text NOT NULL,
  channel        text NOT NULL,
  asker_user_id  text NOT NULL,
  notified_on    date NOT NULL,   -- UTC day of the last mention
  PRIMARY KEY (tenant_id, agent_id, channel, asker_user_id)
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON owner_pings FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON owner_pings TO agentos_app;

ALTER TABLE owner_pings ENABLE ROW LEVEL SECURITY;
ALTER TABLE owner_pings FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON owner_pings;
CREATE POLICY tenant_isolation ON owner_pings
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Agent IDs in the thread tables follow the pattern of migration 023.
ALTER TABLE threads DROP CONSTRAINT IF EXISTS threads_agent_id_pattern;
ALTER TABLE threads ADD CONSTRAINT threads_agent_id_pattern
  CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE owner_pings DROP CONSTRAINT IF EXISTS owner_pings_agent_id_pattern;
ALTER TABLE owner_pings ADD CONSTRAINT owner_pings_agent_id_pattern
  CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

-- Content references: text kept out of Temporal history. Same as migration 022.
CREATE TABLE IF NOT EXISTS inbound_events (
  tenant_id       text        NOT NULL,
  event_id        text        NOT NULL,
  payload         jsonb       NOT NULL,
  received_at     timestamptz NOT NULL DEFAULT now(),
  delivered_at    timestamptz,
  consumed_at     timestamptz,   -- when the content was taken and cleared from payload
  attempts        integer     NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, event_id)
);
CREATE INDEX IF NOT EXISTS inbound_events_pending
  ON inbound_events (tenant_id, received_at) WHERE delivered_at IS NULL;
CREATE INDEX IF NOT EXISTS inbound_events_delivered
  ON inbound_events (tenant_id, delivered_at) WHERE delivered_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS model_steps (
  tenant_id       text        NOT NULL,
  conversation_id text        NOT NULL,
  step_key        text        NOT NULL,
  stop_reason     text        NOT NULL,
  content         jsonb       NOT NULL,   -- the assistant content blocks, as the model returned them
  reply_text      text,                   -- the text a final reply shows; null when the step called tools
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, conversation_id, step_key)
);
CREATE INDEX IF NOT EXISTS model_steps_created ON model_steps (tenant_id, created_at);

CREATE TABLE IF NOT EXISTS tool_inputs (
  tenant_id       text        NOT NULL,
  conversation_id text        NOT NULL,
  tool_use_id     text        NOT NULL,
  input           jsonb       NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, conversation_id, tool_use_id)
);
CREATE INDEX IF NOT EXISTS tool_inputs_created ON tool_inputs (tenant_id, created_at);

CREATE TABLE IF NOT EXISTS task_drafts (
  tenant_id  text        NOT NULL,
  task_id    text        NOT NULL,
  revision   integer     NOT NULL CHECK (revision >= 1),
  payload    jsonb       NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, task_id, revision)
);
CREATE INDEX IF NOT EXISTS task_drafts_created ON task_drafts (tenant_id, created_at);

GRANT USAGE ON SCHEMA public TO agentos_app;

REVOKE ALL ON inbound_events FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON inbound_events TO agentos_app;
REVOKE ALL ON model_steps FROM agentos_app;
GRANT SELECT, INSERT, DELETE ON model_steps TO agentos_app;
REVOKE ALL ON tool_inputs FROM agentos_app;
GRANT SELECT, INSERT, DELETE ON tool_inputs TO agentos_app;
REVOKE ALL ON task_drafts FROM agentos_app;
GRANT SELECT, INSERT, DELETE ON task_drafts TO agentos_app;

ALTER TABLE inbound_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE inbound_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON inbound_events;
CREATE POLICY tenant_isolation ON inbound_events
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE model_steps ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_steps FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON model_steps;
CREATE POLICY tenant_isolation ON model_steps
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE tool_inputs ENABLE ROW LEVEL SECURITY;
ALTER TABLE tool_inputs FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON tool_inputs;
CREATE POLICY tenant_isolation ON tool_inputs
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE task_drafts ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_drafts FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON task_drafts;
CREATE POLICY tenant_isolation ON task_drafts
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- The Notion webhook verification token, encrypted per tenant. Same as migration 030.
CREATE TABLE IF NOT EXISTS notion_webhook_tokens (
  tenant_id    text        NOT NULL PRIMARY KEY,
  state        text        NOT NULL CHECK (state IN ('pending', 'active')),
  ciphertext   bytea       NOT NULL,
  key_version  integer     NOT NULL CHECK (key_version >= 1),
  created_at   timestamptz NOT NULL DEFAULT now(),
  confirmed_at timestamptz
);

REVOKE ALL ON notion_webhook_tokens FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON notion_webhook_tokens TO agentos_app;

ALTER TABLE notion_webhook_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE notion_webhook_tokens FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON notion_webhook_tokens;
CREATE POLICY tenant_isolation ON notion_webhook_tokens
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Tenant payload keys, wrapped by master keys from host variables. Same as migration 031.
-- The app role reads every row of this one table (the codec decrypts for whichever tenant a
-- payload belongs to) and inserts only within its own tenant; stored versions are never
-- updated or deleted.
CREATE TABLE IF NOT EXISTS tenant_payload_keys (
  tenant_id      text        NOT NULL,
  version        integer     NOT NULL CHECK (version >= 1),
  wrapped_key    bytea       NOT NULL,
  master_version integer     NOT NULL CHECK (master_version >= 1),
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, version)
);

REVOKE ALL ON tenant_payload_keys FROM agentos_app;
GRANT SELECT, INSERT ON tenant_payload_keys TO agentos_app;

ALTER TABLE tenant_payload_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_payload_keys FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS key_read ON tenant_payload_keys;
CREATE POLICY key_read ON tenant_payload_keys FOR SELECT USING (true);
DROP POLICY IF EXISTS key_insert ON tenant_payload_keys;
CREATE POLICY key_insert ON tenant_payload_keys FOR INSERT
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Per-tenant connector credentials, encrypted with the tenant's payload key. Same as
-- migrations 032, 034, 038 and 043: the kinds are 'slack_bot', 'notion', the agent's
-- Google user ('google_user') and a creator's kept Notion sign-in
-- ('notion_creator:<Slack user ID>'), and Disconnect or a consent revoke removes a row,
-- so the app role may DELETE too.
CREATE TABLE IF NOT EXISTS tenant_credentials (
  tenant_id   text        NOT NULL,
  kind        text        NOT NULL CHECK (
    kind IN ('slack_bot', 'notion', 'google_user')
    OR kind ~ '^notion_creator:[A-Z0-9_]{1,64}$'),
  ciphertext  bytea       NOT NULL,
  key_version integer     NOT NULL CHECK (key_version >= 1),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, kind)
);

REVOKE ALL ON tenant_credentials FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON tenant_credentials TO agentos_app;

ALTER TABLE tenant_credentials ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_credentials FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON tenant_credentials;
CREATE POLICY tenant_isolation ON tenant_credentials
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE tenants ALTER COLUMN default_agent DROP NOT NULL;

-- Agent health: the worker's grant check, one row per tenant and agent. Same as migration 033.
CREATE TABLE IF NOT EXISTS agent_health (
  tenant_id     text        NOT NULL,
  agent_id      text        NOT NULL,
  agent_version integer     NOT NULL CHECK (agent_version >= 1),
  status        text        NOT NULL CHECK (status IN ('available', 'unavailable')),
  reason        text,
  checked_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, agent_id),
  CONSTRAINT agent_health_agent_id_pattern CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$'),
  CONSTRAINT agent_health_reason_matches_status CHECK (
    (status = 'unavailable') = (reason IS NOT NULL))
);

REVOKE ALL ON agent_health FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON agent_health TO agentos_app;

ALTER TABLE agent_health ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_health FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON agent_health;
CREATE POLICY tenant_isolation ON agent_health
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- The nightly verdict on a creator's access to one granted object, written by the
-- creator-access pass and read by the health check and the console. Same as migration 038.
CREATE TABLE IF NOT EXISTS creator_access (
  tenant_id  text        NOT NULL,
  agent_id   text        NOT NULL,
  notion_id  text        NOT NULL,
  status     text        NOT NULL CHECK (status IN ('verified', 'lost', 'cannot_verify')),
  checked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, agent_id, notion_id),
  CONSTRAINT creator_access_agent_id_pattern CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$')
);

REVOKE ALL ON creator_access FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON creator_access TO agentos_app;

ALTER TABLE creator_access ENABLE ROW LEVEL SECURITY;
ALTER TABLE creator_access FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON creator_access;
CREATE POLICY tenant_isolation ON creator_access
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- The platform's record of each task, written by the task workflow and read by the
-- my_tasks tool for the requester named on the row. Same as migration 035.
CREATE TABLE IF NOT EXISTS tasks (
  tenant_id         text        NOT NULL,
  task_id           text        NOT NULL,
  requester_user_id text        NOT NULL,
  agent_id          text        NOT NULL,
  title             text        NOT NULL DEFAULT '',
  url               text        NOT NULL DEFAULT '',
  page_id           text        NOT NULL DEFAULT '',
  state             text        NOT NULL CHECK (
    state IN ('awaiting_approval', 'filed', 'watching', 'done', 'failed', 'cancelled')),
  last_status       text        NOT NULL DEFAULT '',
  updated_at        timestamptz NOT NULL DEFAULT now(),
  channel           text        NOT NULL,
  thread_ts         text        NOT NULL,
  PRIMARY KEY (tenant_id, task_id),
  CONSTRAINT tasks_agent_id_pattern CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$')
);

CREATE INDEX IF NOT EXISTS tasks_requester
  ON tasks (tenant_id, requester_user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS tasks_thread
  ON tasks (tenant_id, channel, thread_ts);

REVOKE ALL ON tasks FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON tasks TO agentos_app;

ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE tasks FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON tasks;
CREATE POLICY tenant_isolation ON tasks
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Answer feedback: one 👍/👎 per person per answer, changeable; the optional reason is the
-- only free text and is purged with the row by retention. Same as migration 036.
CREATE TABLE IF NOT EXISTS answer_feedback (
  tenant_id       text        NOT NULL,
  conversation_id text        NOT NULL,
  answer_ts       text        NOT NULL,
  voter           text        NOT NULL,
  agent_id        text        NOT NULL,
  agent_version   integer     NOT NULL CHECK (agent_version >= 1),
  vote            text        NOT NULL CHECK (vote IN ('up', 'down')),
  reason          text        CHECK (reason IS NULL OR char_length(reason) <= 300),
  at              timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, conversation_id, answer_ts, voter),
  CONSTRAINT answer_feedback_agent_id_pattern CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$')
);

-- The feedback page reads one agent's votes in a time window. Same as migration 039.
CREATE INDEX IF NOT EXISTS answer_feedback_by_agent
  ON answer_feedback (tenant_id, agent_id, at DESC);

REVOKE ALL ON answer_feedback FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON answer_feedback TO agentos_app;

ALTER TABLE answer_feedback ENABLE ROW LEVEL SECURITY;
ALTER TABLE answer_feedback FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON answer_feedback;
CREATE POLICY tenant_isolation ON answer_feedback
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Approval policy per low-risk action (D-43): requester_approval (the default, also
-- for an action with no row) or none, one row per tenant and action. Same as
-- migration 040.
CREATE TABLE IF NOT EXISTS approval_policies (
  tenant_id  text        NOT NULL,
  action     text        NOT NULL,
  policy     text        NOT NULL CHECK (policy IN ('requester_approval', 'none')),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, action)
);

REVOKE ALL ON approval_policies FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON approval_policies TO agentos_app;

ALTER TABLE approval_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE approval_policies FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON approval_policies;
CREATE POLICY tenant_isolation ON approval_policies
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Creator test sets: cases on an agent version, and runs of them against a draft.
-- The question is the only free text; a run's results are references and fixed reason
-- codes. Same as migration 041.
CREATE TABLE IF NOT EXISTS test_cases (
  tenant_id   text        NOT NULL,
  agent_id    text        NOT NULL,
  version     integer     NOT NULL CHECK (version >= 1),
  case_id     text        NOT NULL,
  question    text        NOT NULL CHECK (char_length(question) BETWEEN 1 AND 2000),
  expect      text        NOT NULL DEFAULT ''
    CHECK (expect IN ('', 'cite', 'write', 'refuse', 'hand_off')),
  target      text        NOT NULL DEFAULT '',
  seq         integer     NOT NULL CHECK (seq >= 1),
  created_by  text        NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, agent_id, version, case_id),
  FOREIGN KEY (tenant_id, agent_id, version)
    REFERENCES agent_versions (tenant_id, agent_id, version) ON DELETE CASCADE,
  CONSTRAINT test_cases_agent_id_pattern CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$'),
  CONSTRAINT test_cases_target_set CHECK (expect IN ('', 'refuse') OR target <> '')
);

CREATE TABLE IF NOT EXISTS test_runs (
  tenant_id         text        NOT NULL,
  run_id            text        NOT NULL,
  agent_id          text        NOT NULL,
  version           integer     NOT NULL CHECK (version >= 1),
  status            text        NOT NULL CHECK (status IN ('running', 'done', 'failed')),
  started_by        text        NOT NULL,
  definition_sha256 text        NOT NULL CHECK (definition_sha256 ~ '^[0-9a-f]{64}$'),
  cases_sha256      text        NOT NULL CHECK (cases_sha256 ~ '^[0-9a-f]{64}$'),
  started_at        timestamptz NOT NULL DEFAULT now(),
  finished_at       timestamptz,
  spend_usd         numeric(20, 8) NOT NULL DEFAULT 0 CHECK (spend_usd >= 0),
  results           jsonb       NOT NULL DEFAULT '[]',
  PRIMARY KEY (tenant_id, run_id),
  CONSTRAINT test_runs_agent_id_pattern CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$')
);

CREATE INDEX IF NOT EXISTS test_runs_by_version
  ON test_runs (tenant_id, agent_id, version, started_at DESC);

REVOKE ALL ON test_cases, test_runs FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON test_cases TO agentos_app;
GRANT SELECT, INSERT, UPDATE ON test_runs TO agentos_app;

ALTER TABLE test_cases ENABLE ROW LEVEL SECURITY;
ALTER TABLE test_cases FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON test_cases;
CREATE POLICY tenant_isolation ON test_cases
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE test_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE test_runs FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON test_runs;
CREATE POLICY tenant_isolation ON test_runs
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

INSERT INTO audit_kinds (kind) VALUES ('test_case'), ('test_run') ON CONFLICT DO NOTHING;

-- Google Drive connections (D-45): one service account per tenant; the row is the
-- connection's identity and last check, not a credential. Same as migration 042.
CREATE TABLE IF NOT EXISTS drive_connections (
  tenant_id             text        NOT NULL PRIMARY KEY,
  service_account_email text        NOT NULL,
  status                text        NOT NULL CHECK (status IN ('connected', 'error')),
  last_error            text,
  connected_at          timestamptz NOT NULL DEFAULT now(),
  checked_at            timestamptz,
  kind                  text        NOT NULL DEFAULT 'service_account',
  CONSTRAINT drive_connections_error_matches_status CHECK (
    (status = 'error') = (last_error IS NOT NULL)),
  CONSTRAINT drive_connections_kind_check CHECK (kind IN ('service_account', 'user'))
);

REVOKE ALL ON drive_connections FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON drive_connections TO agentos_app;

ALTER TABLE drive_connections ENABLE ROW LEVEL SECURITY;
ALTER TABLE drive_connections FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON drive_connections;
CREATE POLICY tenant_isolation ON drive_connections
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- The agent's own Google user as the Drive account (D-53). Same as migration 043: a user
-- address belongs to one tenant, and a directory function maps it to its tenant.
CREATE UNIQUE INDEX IF NOT EXISTS drive_connections_user_email
  ON drive_connections (lower(service_account_email)) WHERE kind = 'user';

REVOKE ALL ON drive_connections FROM agentos_directory;
GRANT SELECT ON drive_connections TO agentos_directory;
DROP POLICY IF EXISTS tenant_directory ON drive_connections;
CREATE POLICY tenant_directory ON drive_connections FOR SELECT TO agentos_directory
  USING (kind = 'user');

CREATE OR REPLACE FUNCTION drive_user_tenant(account text)
RETURNS text
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT d.tenant_id FROM drive_connections d
  WHERE d.kind = 'user' AND lower(d.service_account_email) = lower(account)
$$;

ALTER FUNCTION drive_user_tenant(text) OWNER TO agentos_directory;
REVOKE ALL ON FUNCTION drive_user_tenant(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION drive_user_tenant(text) TO agentos_app;

-- Which migrations this database has had. Same as migration 000. A new install has them all.
CREATE TABLE IF NOT EXISTS schema_migrations (
  version     text        PRIMARY KEY,   -- the number in the migration's file name
  applied_at  timestamptz NOT NULL DEFAULT now()
);

REVOKE ALL ON schema_migrations FROM agentos_app;
GRANT SELECT ON schema_migrations TO agentos_app;

INSERT INTO schema_migrations (version) VALUES
  ('000'), ('001'), ('002'), ('003'), ('004'), ('005'), ('006'), ('007'), ('008'),
  ('009'), ('010'), ('011'), ('012'), ('013'), ('014'), ('015'), ('016'), ('017'), ('018'), ('019'), ('020'), ('021'), ('022'), ('023'), ('024'), ('025'), ('026'), ('027'), ('028'), ('029'), ('030'), ('031'), ('032'), ('033'), ('034'), ('035'), ('036'), ('037'), ('038'), ('039'), ('040'), ('041'), ('042'), ('043')
ON CONFLICT DO NOTHING;
