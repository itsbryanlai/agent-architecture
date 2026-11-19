-- The Notion webhook verification token, encrypted per tenant with the tenant's payload key.
-- One row per tenant: the handshake stores it pending (INSERT ... ON CONFLICT DO NOTHING, so a
-- second handshake cannot replace a pending or active token), the host command
-- agentos.notion_webhook confirms it (the only state that accepts events), and clear removes it
-- so a new handshake is accepted. ciphertext is nonce || AES-256-GCM under the tenant's key
-- version key_version, with the tenant ID as associated data; the token itself is never stored
-- in the clear and never logged. The app role can SELECT, INSERT, UPDATE and DELETE.
-- webhook_token is the audit kind for the host command (show, confirm, clear in tool); it
-- belongs to the tenant, so it joins the kinds that may leave agent_id and agent_version null.
-- Run as the owner role, after 029. Safe to run more than once.

CREATE TABLE IF NOT EXISTS notion_webhook_tokens (
  tenant_id    text        NOT NULL PRIMARY KEY,
  state        text        NOT NULL CHECK (state IN ('pending', 'active')),
  ciphertext   bytea       NOT NULL,
  key_version  integer     NOT NULL CHECK (key_version >= 1),
  created_at   timestamptz NOT NULL DEFAULT now(),
  confirmed_at timestamptz
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON notion_webhook_tokens FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON notion_webhook_tokens TO agentos_app;

ALTER TABLE notion_webhook_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE notion_webhook_tokens FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON notion_webhook_tokens;
CREATE POLICY tenant_isolation ON notion_webhook_tokens
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

INSERT INTO audit_kinds (kind) VALUES ('webhook_token') ON CONFLICT DO NOTHING;

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_required;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_required CHECK (
  kind IN ('member_add', 'member_remove', 'member_role', 'budget_set',
           'switch_off', 'switch_on', 'usage_repriced', 'retention_set', 'webhook_token')
  OR (kind = 'audience_resume' AND agent_id IS NOT NULL)
  OR (agent_id IS NOT NULL AND agent_version IS NOT NULL));
