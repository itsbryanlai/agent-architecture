-- Agents in the database. An agent has numbered versions; a published version
-- is immutable, an edit makes a new draft, and every publish records the
-- approver. Workers load the published version on each turn. Tenant settings
-- (workspace, default agent, channels, secrets) stay in the YAML file.
-- A trigger enforces the lifecycle for every role: the definition changes only
-- while a version is a draft, and a version only moves draft -> pending,
-- pending -> draft (withdrawn or rejected), pending -> published and
-- published -> superseded. The app role can INSERT and SELECT agents, and
-- INSERT, SELECT and UPDATE versions; it has no DELETE.
-- Run as the owner role, after 005. Safe to run more than once.

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
