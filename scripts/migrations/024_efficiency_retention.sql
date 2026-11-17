-- Two changes for running longer: the size of each history message, and per-tenant retention.
--
-- conversation_messages.char_len is the length of the message as JSON ({"role", "content"}),
-- set when the message is appended. A model call selects its window of history from these
-- sizes in SQL instead of loading the whole thread and measuring every message. Rows from
-- before this migration are filled here, from the stored JSON.
--
-- tenants.retention holds one tenant's retention settings, in whole days per kind of data
-- (messages_days, tool_outputs_days, idempotency_days, sessions_days). An empty object means
-- the defaults in src/agentos/retention.py. The nightly retention job reads it; it is changed
-- by the owner role or by an UPDATE through the app role's tenant policy.
--
-- No new table and no new privilege: the app role already has DELETE on the tables the job
-- purges, and none on audit_events or usage_events, which are kept.
-- Run as the owner role. Safe to run more than once.

ALTER TABLE conversation_messages ADD COLUMN IF NOT EXISTS char_len integer NOT NULL DEFAULT 0;

UPDATE conversation_messages
SET char_len = length(jsonb_build_object('role', role, 'content', content)::text)
WHERE char_len = 0;

ALTER TABLE tenants ADD COLUMN IF NOT EXISTS retention jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE tenants DROP CONSTRAINT IF EXISTS tenants_retention_object;
ALTER TABLE tenants ADD CONSTRAINT tenants_retention_object CHECK (jsonb_typeof(retention) = 'object');

-- The retention job finds expired rows by age, per tenant.
CREATE INDEX IF NOT EXISTS idempotency_records_created ON idempotency_records (tenant_id, created_at);
CREATE INDEX IF NOT EXISTS tool_outputs_created ON tool_outputs (tenant_id, created_at);
CREATE INDEX IF NOT EXISTS sessions_expires ON sessions (tenant_id, expires_at);

-- The audit kind for a tenant's retention limits being set, written by
-- `python -m agentos.retention set` for the tenant as a whole.
INSERT INTO audit_kinds (kind) VALUES ('retention_set') ON CONFLICT DO NOTHING;

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_required;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_required CHECK (
  kind IN ('member_add', 'member_remove', 'member_role', 'budget_set',
           'switch_off', 'switch_on', 'usage_repriced', 'retention_set')
  OR (agent_id IS NOT NULL AND agent_version IS NOT NULL));
