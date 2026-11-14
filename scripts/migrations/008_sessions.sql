-- Web console sessions. A row is one signed-in browser: the Slack user who signed in
-- and the tenant their Slack workspace maps to. The browser holds a random 256-bit
-- session ID; only its SHA-256 is stored, so a copy of this table cannot be used to
-- sign in. csrf_secret is the token every form of that session must carry.
-- The app role can SELECT, INSERT and DELETE sessions (sign-out, and the cleanup of
-- expired rows); it cannot UPDATE one, so a session's user and tenant never change.
-- Run as the owner role. Safe to run more than once.

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
