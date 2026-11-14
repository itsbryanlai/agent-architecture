-- Audit log: one append-only row per thing an agent did, per tenant. Rows hold
-- IDs and SHA-256 hashes only, never chat text, document text or tool
-- input/output. The app role can INSERT and SELECT; a trigger rejects UPDATE,
-- DELETE and TRUNCATE for every role. Retention is not enforced yet.
-- Run as the owner role, after 004. Safe to run more than once.

CREATE TABLE IF NOT EXISTS audit_events (
  seq                bigint      GENERATED ALWAYS AS IDENTITY,
  tenant_id          text        NOT NULL,
  event_key          text        NOT NULL,   -- names the event; repeats are ignored
  at                 timestamptz NOT NULL DEFAULT now(),
  kind               text        NOT NULL
    CHECK (kind IN ('answer', 'tool_call', 'hand_off', 'approval', 'write')),
  agent_id           text        NOT NULL,
  agent_version      integer     NOT NULL,
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
  UNIQUE (seq)
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
