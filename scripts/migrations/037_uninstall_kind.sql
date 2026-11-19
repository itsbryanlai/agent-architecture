-- The uninstall audit kind: the gateway's Slack event handler (app_uninstalled, or
-- tokens_revoked naming bot tokens) disables the tenant and deletes its slack_bot
-- credential in one unit of work, and records one uninstall event (the event type in
-- tool, the team ID in target, never a token). It belongs to the tenant, so it joins
-- the kinds that may leave agent_id and agent_version null. The Notion credential is
-- kept: a reinstall re-enables the tenant and resumes its data.
-- Run as the owner role, after 035. Safe to run more than once.

INSERT INTO audit_kinds (kind) VALUES ('uninstall') ON CONFLICT DO NOTHING;

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_required;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_required CHECK (
  kind IN ('member_add', 'member_remove', 'member_role', 'budget_set',
           'switch_off', 'switch_on', 'usage_repriced', 'retention_set', 'webhook_token',
           'payload_key', 'install', 'uninstall', 'connector', 'tenant_settings')
  OR (kind = 'audience_resume' AND agent_id IS NOT NULL)
  OR (agent_id IS NOT NULL AND agent_version IS NOT NULL));
