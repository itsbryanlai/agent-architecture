-- Creator test sets: cases on an agent version, and runs of them against a draft.
-- A case is a question (the only free text, kept with the version, never in Temporal
-- history) and an expected behaviour: cite a granted page, draft a write to a granted
-- database, refuse, or hand off to a listed specialist. '' is an expectation not chosen
-- yet; a run refuses unset cases. Cases copy forward when a new draft is made, and go
-- with a deleted draft (the foreign key). A run records its results as references and
-- fixed reason codes, its spend, and the definition and case-set hashes it ran against,
-- so the console can warn when the draft or its cases changed since. test_case and
-- test_run are the audit kinds; both name the agent and version. Run as the owner role,
-- after 040. Safe to run more than once.

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

-- The version page reads a version's newest run.
CREATE INDEX IF NOT EXISTS test_runs_by_version
  ON test_runs (tenant_id, agent_id, version, started_at DESC);

GRANT USAGE ON SCHEMA public TO agentos_app;
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
