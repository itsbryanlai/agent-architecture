-- Which agent a Slack thread belongs to, and when an agent's owner was last mentioned about a refusal.
--
-- threads has one row per thread that was handed to a specialist: the thread (an ID built
-- by the code, starting with the tenant ID) and the agent that now owns it. A conversation
-- workflow that closes after a week of silence and starts again reads the row, so the thread
-- keeps its specialist. A row holds IDs, never message text. The app role can SELECT, INSERT
-- and UPDATE, never DELETE.
-- owner_pings has one row per (agent, channel, asker): the UTC day the owner was last
-- mentioned in the reply that told that person the agent is not open to them. The reply
-- mentions the owner once a day for each such person and place, so a guest who keeps asking
-- cannot page the owner. It holds IDs and a date. The app role can SELECT, INSERT and UPDATE.
-- Run as the owner role. Safe to run more than once.

CREATE TABLE IF NOT EXISTS threads (
  tenant_id   text        NOT NULL,
  thread_key  text        NOT NULL,
  agent_id    text        NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, thread_key)
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON threads FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON threads TO agentos_app;

ALTER TABLE threads ENABLE ROW LEVEL SECURITY;
ALTER TABLE threads FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON threads;
CREATE POLICY tenant_isolation ON threads
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

CREATE TABLE IF NOT EXISTS owner_pings (
  tenant_id      text NOT NULL,
  agent_id       text NOT NULL,
  channel        text NOT NULL,
  asker_user_id  text NOT NULL,
  notified_on    date NOT NULL,   -- UTC day of the last mention
  PRIMARY KEY (tenant_id, agent_id, channel, asker_user_id)
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON owner_pings FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON owner_pings TO agentos_app;

ALTER TABLE owner_pings ENABLE ROW LEVEL SECURITY;
ALTER TABLE owner_pings FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON owner_pings;
CREATE POLICY tenant_isolation ON owner_pings
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Agent IDs in the thread tables follow the pattern of migration 023.
ALTER TABLE threads DROP CONSTRAINT IF EXISTS threads_agent_id_pattern;
ALTER TABLE threads ADD CONSTRAINT threads_agent_id_pattern
  CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE owner_pings DROP CONSTRAINT IF EXISTS owner_pings_agent_id_pattern;
ALTER TABLE owner_pings ADD CONSTRAINT owner_pings_agent_id_pattern
  CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$');
