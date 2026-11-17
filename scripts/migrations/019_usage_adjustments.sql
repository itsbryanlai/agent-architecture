-- Usage adjustments: the cost a usage row got after it was recorded. A row recorded for a
-- model with no price keeps a null cost for good (the app role cannot update usage_events),
-- and a budget cannot count it. `python -m agentos.usage reprice` appends one adjustment per
-- row it can now price, and totals count a row as priced once it has one. One adjustment
-- per row (the primary key), never changed, never removed. price_model and
-- price_effective_from name the price entry used, so a figure can be traced to the file.
-- The app role can SELECT only: the command connects as the owner role.
-- Also adds the audit kind usage_repriced, written by that command for the tenant as a whole
-- (no agent), so audit_events_agent_required lists it with the other tenant-wide kinds.
-- Run as the owner role, after 018. Safe to run more than once.

CREATE TABLE IF NOT EXISTS usage_adjustments (
  tenant_id            text           NOT NULL,
  usage_key            text           NOT NULL,
  cost_usd             numeric(20, 8) NOT NULL CHECK (cost_usd >= 0),
  price_model          text           NOT NULL,
  price_effective_from date           NOT NULL,
  repriced_at          timestamptz    NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, usage_key),
  FOREIGN KEY (tenant_id, usage_key) REFERENCES usage_events (tenant_id, usage_key)
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON usage_adjustments FROM agentos_app;
GRANT SELECT ON usage_adjustments TO agentos_app;

ALTER TABLE usage_adjustments ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_adjustments FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON usage_adjustments;
CREATE POLICY tenant_isolation ON usage_adjustments
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

INSERT INTO audit_kinds (kind) VALUES ('usage_repriced') ON CONFLICT DO NOTHING;

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_required;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_required CHECK (
  kind IN ('member_add', 'member_remove', 'member_role', 'budget_set',
           'switch_off', 'switch_on', 'usage_repriced')
  OR (agent_id IS NOT NULL AND agent_version IS NOT NULL));
