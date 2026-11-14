-- Kill switches: an admin turns an agent, every agent, a connector or all writes off.
-- One row is one switch that is off; turning it on deletes the row, and the audit log keeps
-- the history. scope is agent, all_agents, connector or writes. target is the agent ID or the
-- connector name, and empty for the two tenant-wide scopes, so the primary key is one switch
-- per tenant, scope and target. The app role can SELECT, INSERT and DELETE, never UPDATE.
-- A row holds IDs and names, never chat or document text.
-- switch_off and switch_on are the audit kinds for turning a switch off and on. They name the
-- agent for an agent switch (never a version) and none for a tenant-wide one, so they join the
-- kinds that may leave agent_id and agent_version null. Every kind from 013 stays.
-- Run as the owner role, after 013. Safe to run more than once.

CREATE TABLE IF NOT EXISTS kill_switches (
  tenant_id      text        NOT NULL,
  scope          text        NOT NULL CHECK (scope IN ('agent', 'all_agents', 'connector', 'writes')),
  target         text        NOT NULL DEFAULT '',   -- agent ID or connector name; empty otherwise
  turned_off_by  text        NOT NULL,
  turned_off_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, scope, target),
  CONSTRAINT kill_switches_target_matches_scope CHECK (
    (scope IN ('agent', 'connector')) = (target <> ''))
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON kill_switches FROM agentos_app;
GRANT SELECT, INSERT, DELETE ON kill_switches TO agentos_app;

ALTER TABLE kill_switches ENABLE ROW LEVEL SECURITY;
ALTER TABLE kill_switches FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON kill_switches;
CREATE POLICY tenant_isolation ON kill_switches
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_kind_check;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_kind_check CHECK (kind IN (
  'answer', 'tool_call', 'hand_off', 'approval', 'write',
  'agent_create', 'agent_draft', 'agent_edit', 'agent_import', 'agent_submit',
  'agent_withdraw', 'agent_reject', 'agent_publish',
  'member_add', 'member_remove', 'member_role',
  'audience_refused', 'audience_confirm', 'budget_set',
  'audience_pause', 'audience_resume',
  'switch_off', 'switch_on'));
ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_required;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_required CHECK (
  kind IN ('member_add', 'member_remove', 'member_role', 'budget_set',
           'switch_off', 'switch_on')
  OR (agent_id IS NOT NULL AND agent_version IS NOT NULL));
