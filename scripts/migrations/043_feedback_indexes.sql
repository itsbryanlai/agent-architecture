-- The creator feedback page reads one agent's audit rows and answer votes in a time
-- window, newest first, with a limit per section. These indexes serve that read per
-- agent, beside the tenant-wide time index (005) and the votes' primary key (036):
-- a tenant with many agents does not scan another agent's rows for one agent's page.
-- No new table, no new privilege, no new kind.
-- Run as the owner role, after 038. Safe to run more than once.

CREATE INDEX IF NOT EXISTS audit_events_by_agent
  ON audit_events (tenant_id, agent_id, at DESC, seq DESC);

CREATE INDEX IF NOT EXISTS answer_feedback_by_agent
  ON answer_feedback (tenant_id, agent_id, at DESC);
