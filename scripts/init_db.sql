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
      'member_add', 'member_remove', 'member_role',
      'audience_refused', 'audience_confirm', 'budget_set',
      'audience_pause', 'audience_resume',
      'switch_off', 'switch_on')),
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
  outcome            text        NOT NULL CHECK (outcome IN ('ok', 'error')),
  PRIMARY KEY (tenant_id, event_key),
  UNIQUE (seq),
  CONSTRAINT audit_events_agent_required CHECK (
    kind IN ('member_add', 'member_remove', 'member_role', 'budget_set',
             'switch_off', 'switch_on')
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

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
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

-- Monthly budgets, per tenant and per agent. Same as migration 012.
CREATE TABLE IF NOT EXISTS budgets (
  tenant_id   text          NOT NULL,
  agent_id    text,                          -- null is the tenant-wide budget
  amount_usd  numeric(12, 2) NOT NULL CHECK (amount_usd > 0),
  updated_by  text          NOT NULL,
  updated_at  timestamptz   NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS budgets_one_per_scope
  ON budgets (tenant_id, (COALESCE(agent_id, '')));

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
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
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
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
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
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

-- Tenants: one row per tenant, read across tenants only through two narrow functions. Same as migration 015.
CREATE TABLE IF NOT EXISTS tenants (
  tenant_id           text        PRIMARY KEY,
  slack_team_id       text        NOT NULL UNIQUE,
  enabled             boolean     NOT NULL DEFAULT true,
  default_agent       text        NOT NULL,
  notion_workspace_id text        UNIQUE,
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
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
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
