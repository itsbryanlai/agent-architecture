-- Tenant settings in the console. Connect Notion stores the workspace bot token as a
-- credential of kind 'notion', so the kind CHECK widens; the workspace's name joins its
-- ID on the tenants row for display. Disconnect removes the credential row, so the app
-- role gains DELETE on tenant_credentials (until now it could only SELECT, INSERT and
-- UPDATE, since nothing deleted a credential). Two audit kinds join the tenant-wide
-- ones: 'connector' (Notion connected or disconnected; the row never holds the token)
-- and 'tenant_settings' (the default agent and channel routing an admin sets).
-- Run as the owner role, after 033. Safe to run more than once.

ALTER TABLE tenant_credentials DROP CONSTRAINT IF EXISTS tenant_credentials_kind_check;
ALTER TABLE tenant_credentials ADD CONSTRAINT tenant_credentials_kind_check
  CHECK (kind IN ('slack_bot', 'notion'));

GRANT DELETE ON tenant_credentials TO agentos_app;

ALTER TABLE tenants ADD COLUMN IF NOT EXISTS notion_workspace_name text;

INSERT INTO audit_kinds (kind) VALUES ('connector'), ('tenant_settings')
ON CONFLICT DO NOTHING;

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_required;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_required CHECK (
  kind IN ('member_add', 'member_remove', 'member_role', 'budget_set',
           'switch_off', 'switch_on', 'usage_repriced', 'retention_set', 'webhook_token',
           'payload_key', 'install', 'connector', 'tenant_settings')
  OR (kind = 'audience_resume' AND agent_id IS NOT NULL)
  OR (agent_id IS NOT NULL AND agent_version IS NOT NULL));
