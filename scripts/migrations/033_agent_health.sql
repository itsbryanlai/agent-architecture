-- Agent health: one row per tenant and agent, written by the worker's grant check.
-- An agent whose grants fail the check is 'unavailable' with the reason (the grant and
-- the problem, never document content); a passing check stores 'available' with no reason.
-- checked_at is when the last check ran: it names the transition's audit event and notices,
-- and bounds the next re-check. The app role can SELECT, INSERT and UPDATE; nothing deletes
-- a health row here. agent_health is the audit kind for a transition; it names the agent and
-- the version checked, so it joins the kinds that need both.
-- Run as the owner role, after 032. Safe to run more than once.

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

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON agent_health FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON agent_health TO agentos_app;

ALTER TABLE agent_health ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_health FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON agent_health;
CREATE POLICY tenant_isolation ON agent_health
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

INSERT INTO audit_kinds (kind) VALUES ('agent_health') ON CONFLICT DO NOTHING;
