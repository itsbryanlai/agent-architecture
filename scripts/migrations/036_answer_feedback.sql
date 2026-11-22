-- Answer feedback: one 👍/👎 per person per answer, changeable. The voter is the Slack
-- user of the signed click; the row names the answer (conversation ID and message ts) and
-- the agent and version that answered. The reason is optional, at most 300 characters, and
-- the only free text: it is purged with the row by the retention job (tool_outputs_days).
-- The app role can SELECT, INSERT, UPDATE and DELETE (the retention purge). feedback is the
-- audit kind for a vote; it names the agent and version, so it joins the kinds that need both.
-- Run as the owner role, after 035. Safe to run more than once.

CREATE TABLE IF NOT EXISTS answer_feedback (
  tenant_id       text        NOT NULL,
  conversation_id text        NOT NULL,
  answer_ts       text        NOT NULL,
  voter           text        NOT NULL,
  agent_id        text        NOT NULL,
  agent_version   integer     NOT NULL CHECK (agent_version >= 1),
  vote            text        NOT NULL CHECK (vote IN ('up', 'down')),
  reason          text        CHECK (reason IS NULL OR char_length(reason) <= 300),
  at              timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, conversation_id, answer_ts, voter),
  CONSTRAINT answer_feedback_agent_id_pattern CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$')
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON answer_feedback FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON answer_feedback TO agentos_app;

ALTER TABLE answer_feedback ENABLE ROW LEVEL SECURITY;
ALTER TABLE answer_feedback FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON answer_feedback;
CREATE POLICY tenant_isolation ON answer_feedback
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

INSERT INTO audit_kinds (kind) VALUES ('feedback') ON CONFLICT DO NOTHING;
