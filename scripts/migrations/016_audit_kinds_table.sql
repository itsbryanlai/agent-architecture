-- The audit event kinds move from one CHECK constraint, rewritten by every migration that
-- added a kind, to a table the column references. A migration that adds a kind now adds
-- one row:  INSERT INTO audit_kinds (kind) VALUES ('new_kind') ON CONFLICT DO NOTHING;
-- The kinds in code are agentos.models.AUDIT_KINDS; a test keeps the two equal.
-- The app role does not read or write audit_kinds: the foreign key check runs as the owner.
--
-- budgets gets the primary key it never had. scope is the generated key of the
-- tenant-wide budget (the empty string) or of an agent's, so the primary key
-- (tenant_id, scope) replaces the unique index on the same expression and the store
-- names that column in ON CONFLICT.
-- Run as the owner role, after 015. Safe to run more than once.

CREATE TABLE IF NOT EXISTS audit_kinds (
  kind text PRIMARY KEY
);

INSERT INTO audit_kinds (kind) VALUES
  ('answer'), ('tool_call'), ('hand_off'), ('approval'), ('write'),
  ('agent_create'), ('agent_draft'), ('agent_edit'), ('agent_import'), ('agent_submit'),
  ('agent_withdraw'), ('agent_reject'), ('agent_publish'),
  ('member_add'), ('member_remove'), ('member_role'),
  ('audience_refused'), ('audience_confirm'), ('budget_set'),
  ('audience_pause'), ('audience_resume'),
  ('switch_off'), ('switch_on')
ON CONFLICT DO NOTHING;

REVOKE ALL ON audit_kinds FROM agentos_app;

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_kind_check;
ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_kind_fkey;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_kind_fkey
  FOREIGN KEY (kind) REFERENCES audit_kinds (kind);

ALTER TABLE budgets ADD COLUMN IF NOT EXISTS scope text
  GENERATED ALWAYS AS (COALESCE(agent_id, '')) STORED;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conrelid = 'budgets'::regclass AND contype = 'p'
  ) THEN
    ALTER TABLE budgets ADD CONSTRAINT budgets_pkey PRIMARY KEY (tenant_id, scope);
  END IF;
END
$$;

DROP INDEX IF EXISTS budgets_one_per_scope;
