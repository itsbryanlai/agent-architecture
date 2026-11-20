-- Creator consent and the nightly re-check of a creator's access to what they granted.
-- 1. tenant_credentials gains per-creator Notion sign-ins: kind 'notion_creator:<Slack user
--    ID>', stored encrypted like every credential here when the creator consents at the
--    picker. A consented pick replaces the row; revoking consent deletes it (the app role
--    already has DELETE, migration 034).
-- 2. creator_access holds the nightly verdict per granted object: 'verified' (the creator's
--    sign-in can still read it), 'lost' (Notion refused the creator: gone or unshared) or
--    'cannot_verify' (no consent, no token). checked_at is when the pass that wrote the row
--    ran. The worker's health check reads it: a lost object suspends the agent; the console
--    shows the verdict per grant. The nightly pass replaces an agent's rows wholesale, so
--    the app role can DELETE too; row-level security keeps every tenant's rows apart.
-- Run as the owner role, after 037. Safe to run more than once.

ALTER TABLE tenant_credentials DROP CONSTRAINT IF EXISTS tenant_credentials_kind_check;
ALTER TABLE tenant_credentials ADD CONSTRAINT tenant_credentials_kind_check
  CHECK (kind IN ('slack_bot', 'notion') OR kind ~ '^notion_creator:[A-Z0-9_]{1,64}$');

CREATE TABLE IF NOT EXISTS creator_access (
  tenant_id  text        NOT NULL,
  agent_id   text        NOT NULL,
  notion_id  text        NOT NULL,
  status     text        NOT NULL CHECK (status IN ('verified', 'lost', 'cannot_verify')),
  checked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, agent_id, notion_id),
  CONSTRAINT creator_access_agent_id_pattern CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$')
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON creator_access FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON creator_access TO agentos_app;

ALTER TABLE creator_access ENABLE ROW LEVEL SECURITY;
ALTER TABLE creator_access FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON creator_access;
CREATE POLICY tenant_isolation ON creator_access
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
