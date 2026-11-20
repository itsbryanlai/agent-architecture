-- Approval policy per low-risk action (D-43): one row per tenant and action,
-- requester_approval (the default, also for an action with no row) or none. The app
-- role can SELECT, INSERT and UPDATE; setting the default back is an update, nothing
-- deletes a row. policy_change is the audit kind for an admin's change on the settings
-- page; it belongs to the tenant, so it joins the kinds that need no agent. A write
-- that ran under the policy records outcome 'policy' (no approval row), so the outcome
-- CHECK widens.
-- Run as the owner role, after 038. Safe to run more than once.

CREATE TABLE IF NOT EXISTS approval_policies (
  tenant_id  text        NOT NULL,
  action     text        NOT NULL,
  policy     text        NOT NULL CHECK (policy IN ('requester_approval', 'none')),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, action)
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON approval_policies FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON approval_policies TO agentos_app;

ALTER TABLE approval_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE approval_policies FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON approval_policies;
CREATE POLICY tenant_isolation ON approval_policies
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

INSERT INTO audit_kinds (kind) VALUES ('policy_change') ON CONFLICT DO NOTHING;

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_outcome_check;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_outcome_check
  CHECK (outcome IN ('ok', 'error', 'policy'));

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_required;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_required CHECK (
  kind IN ('member_add', 'member_remove', 'member_role', 'budget_set',
           'switch_off', 'switch_on', 'usage_repriced', 'retention_set', 'webhook_token',
           'payload_key', 'install', 'uninstall', 'connector', 'tenant_settings',
           'policy_change')
  OR (kind = 'audience_resume' AND agent_id IS NOT NULL)
  OR (agent_id IS NOT NULL AND agent_version IS NOT NULL));
