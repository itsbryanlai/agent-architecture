-- Per-tenant connector credentials, encrypted with the tenant's payload key. One row per
-- tenant and kind ('slack_bot' now; a later ticket adds 'notion'). ciphertext is nonce ||
-- AES-256-GCM under the tenant's key version key_version, with the tenant ID as associated
-- data, so a value stored for one tenant does not decrypt for another. The credential itself
-- is never stored in the clear and never logged. A reinstall replaces the row (UPDATE), so
-- the app role can SELECT, INSERT and UPDATE; nothing deletes a credential here yet.
-- install is the audit kind for the Slack OAuth install (a new tenant, or a reinstall that
-- replaces its bot token); it belongs to the tenant, so it joins the kinds that may leave
-- agent_id and agent_version null.
-- A tenant installed from Slack has no agent yet: default_agent becomes nullable, NULL meaning
-- "no default agent" (the gateway answers such a tenant with a fixed not-set-up reply).
-- Run as the owner role, after 031. Safe to run more than once.

CREATE TABLE IF NOT EXISTS tenant_credentials (
  tenant_id   text        NOT NULL,
  kind        text        NOT NULL CHECK (kind IN ('slack_bot')),
  ciphertext  bytea       NOT NULL,
  key_version integer     NOT NULL CHECK (key_version >= 1),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, kind)
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON tenant_credentials FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON tenant_credentials TO agentos_app;

ALTER TABLE tenant_credentials ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_credentials FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON tenant_credentials;
CREATE POLICY tenant_isolation ON tenant_credentials
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

INSERT INTO audit_kinds (kind) VALUES ('install') ON CONFLICT DO NOTHING;

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_required;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_required CHECK (
  kind IN ('member_add', 'member_remove', 'member_role', 'budget_set',
           'switch_off', 'switch_on', 'usage_repriced', 'retention_set', 'webhook_token',
           'payload_key', 'install')
  OR (kind = 'audience_resume' AND agent_id IS NOT NULL)
  OR (agent_id IS NOT NULL AND agent_version IS NOT NULL));

ALTER TABLE tenants ALTER COLUMN default_agent DROP NOT NULL;
