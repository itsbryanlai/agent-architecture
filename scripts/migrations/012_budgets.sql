-- Monthly budgets in US dollars, per tenant (agent_id null) and per agent, for the UTC
-- calendar month. At most one row per tenant-wide scope and one per agent. No row means no
-- limit; removing a budget deletes its row, and the audit log keeps the record. The app role
-- can SELECT, INSERT, UPDATE and DELETE budgets.
-- budget_set is the audit kind for setting, raising or removing a budget. It names the agent
-- when the budget is an agent's (never a version) and none when it is tenant-wide, with the
-- new amount in target; so it joins the kinds that may leave agent_id and agent_version null.
-- Run as the owner role, after 011. Safe to run more than once.

CREATE TABLE IF NOT EXISTS budgets (
  tenant_id   text          NOT NULL,
  agent_id    text,                          -- null is the tenant-wide budget
  amount_usd  numeric(12, 2) NOT NULL CHECK (amount_usd > 0),
  updated_by  text          NOT NULL,
  updated_at  timestamptz   NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS budgets_one_per_scope
  ON budgets (tenant_id, (COALESCE(agent_id, '')));

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON budgets FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON budgets TO agentos_app;

ALTER TABLE budgets ENABLE ROW LEVEL SECURITY;
ALTER TABLE budgets FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON budgets;
CREATE POLICY tenant_isolation ON budgets
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_kind_check;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_kind_check CHECK (kind IN (
  'answer', 'tool_call', 'hand_off', 'approval', 'write',
  'agent_create', 'agent_draft', 'agent_edit', 'agent_import', 'agent_submit',
  'agent_withdraw', 'agent_reject', 'agent_publish',
  'member_add', 'member_remove', 'member_role',
  'audience_refused', 'audience_confirm',
  'budget_set'));
ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_required;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_required CHECK (
  kind IN ('member_add', 'member_remove', 'member_role', 'budget_set')
  OR (agent_id IS NOT NULL AND agent_version IS NOT NULL));
