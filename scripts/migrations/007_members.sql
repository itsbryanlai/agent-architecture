-- Members and roles. A tenant has two roles, admin and creator. The first admins
-- come from the tenant YAML file (`admins:`) and are never rows here; the rows
-- are the admins and creators added through the app. The app role can SELECT,
-- INSERT, UPDATE and DELETE members (a member can be removed; the audit log
-- keeps the record).
-- The audit log also records agent lifecycle and membership events. A membership
-- event names no agent, so agent_id and agent_version may be null for those kinds.
-- Run as the owner role, after 006. Safe to run more than once.

CREATE TABLE IF NOT EXISTS members (
  tenant_id  text        NOT NULL,
  user_id    text        NOT NULL,   -- Slack user ID
  role       text        NOT NULL CHECK (role IN ('admin', 'creator')),
  added_by   text        NOT NULL,
  added_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, user_id)
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON members FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON members TO agentos_app;

ALTER TABLE members ENABLE ROW LEVEL SECURITY;
ALTER TABLE members FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON members;
CREATE POLICY tenant_isolation ON members
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_kind_check;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_kind_check CHECK (kind IN (
  'answer', 'tool_call', 'hand_off', 'approval', 'write',
  'agent_create', 'agent_draft', 'agent_submit', 'agent_withdraw', 'agent_reject',
  'agent_publish', 'member_add', 'member_remove', 'member_role'));
ALTER TABLE audit_events ALTER COLUMN agent_id DROP NOT NULL;
ALTER TABLE audit_events ALTER COLUMN agent_version DROP NOT NULL;
ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_required;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_required CHECK (
  kind IN ('member_add', 'member_remove', 'member_role')
  OR (agent_id IS NOT NULL AND agent_version IS NOT NULL));
