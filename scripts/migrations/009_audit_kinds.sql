-- Two more audit event kinds. agent_edit records replacing a draft's definition (it was
-- recorded as agent_draft, the kind for creating a draft). agent_import records an agent
-- the one-time YAML import inserted. Both name an agent and a version, so the rule
-- that an agent event names its agent already covers them.
-- Run as the owner role, after 007. Safe to run more than once.

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_kind_check;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_kind_check CHECK (kind IN (
  'answer', 'tool_call', 'hand_off', 'approval', 'write',
  'agent_create', 'agent_draft', 'agent_edit', 'agent_import', 'agent_submit',
  'agent_withdraw', 'agent_reject', 'agent_publish',
  'member_add', 'member_remove', 'member_role'));
