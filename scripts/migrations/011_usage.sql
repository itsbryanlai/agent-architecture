-- Usage and cost. One row per model call or embedded page: which tenant, which agent
-- and agent version (none for an embedding), the model the provider reported, the
-- token counts, the cost in US dollars, the conversation (for tracing) and the time.
-- A row never names the person who asked and never holds chat or document text.
-- The cost is worked out once, from the price in effect when the row is written, and
-- a later price change never rewrites it. A null cost means the model had no price,
-- and a null count means the provider did not report it.
-- The app role can SELECT and INSERT rows; it cannot UPDATE or DELETE one. A repeat of
-- the same usage_key (the same activity attempt recorded twice) is ignored by the insert;
-- each attempt that the provider answered has its own key and its own row.
-- Run as the owner role. Safe to run more than once.

CREATE TABLE IF NOT EXISTS usage_events (
  tenant_id          text        NOT NULL,
  usage_key          text        NOT NULL,   -- names the activity attempt that made the call
  kind               text        NOT NULL CHECK (kind IN ('llm', 'embedding')),
  agent_id           text,
  agent_version      integer,
  model              text        NOT NULL,
  input_tokens       bigint      CHECK (input_tokens >= 0),
  output_tokens      bigint      CHECK (output_tokens >= 0),
  cache_read_tokens  bigint      CHECK (cache_read_tokens >= 0),
  cache_write_tokens bigint      CHECK (cache_write_tokens >= 0),
  cost_usd           numeric(20, 8) CHECK (cost_usd >= 0),
  conversation_id    text,
  at                 timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, usage_key),
  CONSTRAINT usage_events_agent_required CHECK (
    kind <> 'llm' OR (agent_id IS NOT NULL AND agent_version IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS usage_events_by_month ON usage_events (tenant_id, at);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON usage_events FROM agentos_app;
GRANT SELECT, INSERT ON usage_events TO agentos_app;

ALTER TABLE usage_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON usage_events;
CREATE POLICY tenant_isolation ON usage_events
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
