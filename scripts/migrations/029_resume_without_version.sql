-- The audience_resume audit kind may name an agent with no published version: a resume can
-- close a pause after the agent's published version is gone. The agent is still required, and
-- every other agent kind still needs both an agent and a version.
-- Run as the owner role, after 028. Safe to run more than once.

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_required;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_required CHECK (
  kind IN ('member_add', 'member_remove', 'member_role', 'budget_set',
           'switch_off', 'switch_on', 'usage_repriced', 'retention_set')
  OR (kind = 'audience_resume' AND agent_id IS NOT NULL)
  OR (agent_id IS NOT NULL AND agent_version IS NOT NULL));
