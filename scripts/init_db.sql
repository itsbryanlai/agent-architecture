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

-- Row-level security: every tenant table only shows rows of the tenant set for
-- the current transaction (app.tenant_id). The app connects as agentos_app,
-- which owns nothing and cannot bypass these policies. A transaction that has
-- not set app.tenant_id sees no rows and can write none.
-- Run as the owner role. Safe to run more than once.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
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

-- Audit log: append-only, one row per thing an agent did. Holds IDs and hashes,
-- never content. The app role can INSERT and SELECT only. Same as migration 005.
CREATE TABLE IF NOT EXISTS audit_events (
  seq                bigint      GENERATED ALWAYS AS IDENTITY,
  tenant_id          text        NOT NULL,
  event_key          text        NOT NULL,   -- names the event; repeats are ignored
  at                 timestamptz NOT NULL DEFAULT now(),
  kind               text        NOT NULL
    CHECK (kind IN (
      'answer', 'tool_call', 'hand_off', 'approval', 'write',
      'agent_create', 'agent_draft', 'agent_edit', 'agent_import', 'agent_submit',
      'agent_withdraw', 'agent_reject', 'agent_publish',
      'member_add', 'member_remove', 'member_role')),
  agent_id           text,                   -- null only for a membership event
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
  outcome            text        NOT NULL CHECK (outcome IN ('ok', 'error')),
  PRIMARY KEY (tenant_id, event_key),
  UNIQUE (seq),
  CONSTRAINT audit_events_agent_required CHECK (
    kind IN ('member_add', 'member_remove', 'member_role')
    OR (agent_id IS NOT NULL AND agent_version IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS audit_events_by_time ON audit_events (tenant_id, at DESC, seq DESC);

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
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON audit_events FROM agentos_app;
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
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
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
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
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
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
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
