-- Monthly usage rollup: one row per tenant, UTC month, agent and cost type, with the number
-- of calls, the cost of the priced ones (an adjustment counts as a price) and the number
-- still unpriced. The budget check reads these few rows instead of grouping the month's
-- whole usage table. The store writes a row's rollup in the same transaction as the row
-- itself (usage_events) and as the adjustment that prices it (usage_adjustments), so a
-- repeat that stores nothing changes nothing here. agent_id '' is a call with no agent
-- (an embedding), so that the key has no null in it.
-- The backfill below builds the rows from what is already stored; a row that exists is left
-- as it is, so running it again changes nothing. It reads every tenant's rows, so run it as
-- the owner role that bypasses row-level security.
-- Run as the owner role, after 025. Safe to run more than once.

CREATE TABLE IF NOT EXISTS usage_month_totals (
  tenant_id       text           NOT NULL,
  month           date           NOT NULL CHECK (month = date_trunc('month', month)::date),
  agent_id        text           NOT NULL DEFAULT '',
  kind            text           NOT NULL CHECK (kind IN ('llm', 'embedding')),
  calls           bigint         NOT NULL DEFAULT 0 CHECK (calls >= 0),
  cost_usd        numeric(20, 8) NOT NULL DEFAULT 0 CHECK (cost_usd >= 0),
  unpriced_calls  bigint         NOT NULL DEFAULT 0 CHECK (unpriced_calls >= 0),
  PRIMARY KEY (tenant_id, month, agent_id, kind),
  CONSTRAINT usage_month_totals_agent_id_pattern
    CHECK (agent_id = '' OR agent_id ~ '^[a-z][a-z0-9-]{1,39}$')
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON usage_month_totals FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON usage_month_totals TO agentos_app;

ALTER TABLE usage_month_totals ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_month_totals FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON usage_month_totals;
CREATE POLICY tenant_isolation ON usage_month_totals
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

INSERT INTO usage_month_totals (tenant_id, month, agent_id, kind, calls, cost_usd, unpriced_calls)
SELECT e.tenant_id,
       date_trunc('month', e.at AT TIME ZONE 'UTC')::date,
       COALESCE(e.agent_id, ''),
       e.kind,
       count(*),
       COALESCE(sum(COALESCE(e.cost_usd, 0) + COALESCE(a.cost_usd, 0)), 0),
       count(*) FILTER (WHERE e.cost_usd IS NULL AND a.usage_key IS NULL)
FROM usage_events e
LEFT JOIN usage_adjustments a
  ON a.tenant_id = e.tenant_id AND a.usage_key = e.usage_key
GROUP BY 1, 2, 3, 4
ON CONFLICT (tenant_id, month, agent_id, kind) DO NOTHING;
