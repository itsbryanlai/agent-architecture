-- Agent audiences. audience_refused records a turn or a hand-off refused because the asker is
-- outside the agent's audience (agent, version, requester and a fixed reason code; never
-- content). audience_confirm records the approver's confirmation at publish, with the people
-- counts they saw. Both name an agent and a version, so the rule that an agent event names
-- its agent already covers them.
-- Run as the owner role, after 009. Safe to run more than once.

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_kind_check;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_kind_check CHECK (kind IN (
  'answer', 'tool_call', 'hand_off', 'approval', 'write',
  'agent_create', 'agent_draft', 'agent_edit', 'agent_import', 'agent_submit',
  'agent_withdraw', 'agent_reject', 'agent_publish',
  'member_add', 'member_remove', 'member_role',
  'audience_refused', 'audience_confirm'));
