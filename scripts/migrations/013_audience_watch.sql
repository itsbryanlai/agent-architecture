-- Audience changes are watched. audience_pauses holds one row per pause of an agent in a
-- channel: why (a fixed reason code), when, and who resumed it and when. A resumed row is
-- kept as history; at most one pause per agent and channel is active (resumed_at is null).
-- A row holds IDs and codes, never chat or document text. The app role can SELECT, INSERT
-- and UPDATE (to resume) but not DELETE. audience_pause and audience_resume join the audit
-- kinds; both name an agent and a version, so the rule that an agent event names its agent
-- already covers them. Every kind from 010 stays, and budget_set (migration 012) too.
-- Run as the owner role, after 011 (and 012). Safe to run more than once.

CREATE TABLE IF NOT EXISTS audience_pauses (
  tenant_id   text        NOT NULL,
  agent_id    text        NOT NULL,
  channel     text        NOT NULL,
  reason      text        NOT NULL CHECK (reason IN ('channel_shared')),
  paused_at   timestamptz NOT NULL DEFAULT now(),
  resumed_by  text,
  resumed_at  timestamptz,
  PRIMARY KEY (tenant_id, agent_id, channel, paused_at),
  CONSTRAINT audience_pauses_resume_together CHECK (
    (resumed_by IS NULL) = (resumed_at IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS audience_pauses_one_active
  ON audience_pauses (tenant_id, agent_id, channel) WHERE resumed_at IS NULL;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON audience_pauses FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON audience_pauses TO agentos_app;

ALTER TABLE audience_pauses ENABLE ROW LEVEL SECURITY;
ALTER TABLE audience_pauses FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON audience_pauses;
CREATE POLICY tenant_isolation ON audience_pauses
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_kind_check;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_kind_check CHECK (kind IN (
  'answer', 'tool_call', 'hand_off', 'approval', 'write',
  'agent_create', 'agent_draft', 'agent_edit', 'agent_import', 'agent_submit',
  'agent_withdraw', 'agent_reject', 'agent_publish',
  'member_add', 'member_remove', 'member_role',
  'audience_refused', 'audience_confirm', 'budget_set',
  'audience_pause', 'audience_resume'));
