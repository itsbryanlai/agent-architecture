-- Chat and document content out of Temporal history. A workflow, a signal or an activity
-- argument carries IDs and small values; the text lives in these tables, keyed by those IDs,
-- so deleting a thread's rows deletes what was said in it.
--
-- inbound_events: what Slack sent, kept until the workflow has taken it. The gateway stores
-- the event here and answers Slack at once; a relay loop signals the conversation workflow
-- with the event ID and marks the row delivered (delivered_at). The workflow's first step for
-- the message reads the text here, appends it to the thread's history and clears it from the
-- row (consumed_at); the row then keeps only IDs, for deduplication. attempts and
-- next_attempt_at back off a delivery that failed. An Edit form's values are kept here too
-- (event_id 'edit:<id>', inserted already delivered) for the task workflow to read.
-- model_steps: what the model returned for one step of a turn, written before anything else
-- is done with it, so a retried model call finds it instead of asking the model again. A
-- final reply stays here until it has been shown; the activity that shows it then stores it
-- in history.
-- tool_inputs: the input of each tool call the model made, by tool-use ID. Activities read
-- it; the workflow never does.
-- task_drafts: the frozen payload of a task, one row per revision. A revision is never
-- changed; an edit adds the next one, and the approval names the revision it approved.
-- None of these is kept forever: the retention job purges them (see src/agentos/retention.py).
-- The app role can SELECT, INSERT and DELETE on the three that are never changed in place,
-- and also UPDATE inbound_events (delivery state). It can also DELETE threads and owner_pings
-- now, which the retention job purges.
-- Run as the owner role. Safe to run more than once.

CREATE TABLE IF NOT EXISTS inbound_events (
  tenant_id       text        NOT NULL,
  event_id        text        NOT NULL,
  payload         jsonb       NOT NULL,
  received_at     timestamptz NOT NULL DEFAULT now(),
  delivered_at    timestamptz,
  consumed_at     timestamptz,   -- when the content was taken and cleared from payload
  attempts        integer     NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, event_id)
);
CREATE INDEX IF NOT EXISTS inbound_events_pending
  ON inbound_events (tenant_id, received_at) WHERE delivered_at IS NULL;
CREATE INDEX IF NOT EXISTS inbound_events_delivered
  ON inbound_events (tenant_id, delivered_at) WHERE delivered_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS model_steps (
  tenant_id       text        NOT NULL,
  conversation_id text        NOT NULL,
  step_key        text        NOT NULL,
  stop_reason     text        NOT NULL,
  content         jsonb       NOT NULL,   -- the assistant content blocks, as the model returned them
  reply_text      text,                   -- the text a final reply shows; null when the step called tools
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, conversation_id, step_key)
);
CREATE INDEX IF NOT EXISTS model_steps_created ON model_steps (tenant_id, created_at);

CREATE TABLE IF NOT EXISTS tool_inputs (
  tenant_id       text        NOT NULL,
  conversation_id text        NOT NULL,
  tool_use_id     text        NOT NULL,
  input           jsonb       NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, conversation_id, tool_use_id)
);
CREATE INDEX IF NOT EXISTS tool_inputs_created ON tool_inputs (tenant_id, created_at);

CREATE TABLE IF NOT EXISTS task_drafts (
  tenant_id  text        NOT NULL,
  task_id    text        NOT NULL,
  revision   integer     NOT NULL CHECK (revision >= 1),
  payload    jsonb       NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, task_id, revision)
);
CREATE INDEX IF NOT EXISTS task_drafts_created ON task_drafts (tenant_id, created_at);

GRANT USAGE ON SCHEMA public TO agentos_app;

REVOKE ALL ON inbound_events FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON inbound_events TO agentos_app;
REVOKE ALL ON model_steps FROM agentos_app;
GRANT SELECT, INSERT, DELETE ON model_steps TO agentos_app;
REVOKE ALL ON tool_inputs FROM agentos_app;
GRANT SELECT, INSERT, DELETE ON tool_inputs TO agentos_app;
REVOKE ALL ON task_drafts FROM agentos_app;
GRANT SELECT, INSERT, DELETE ON task_drafts TO agentos_app;

ALTER TABLE inbound_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE inbound_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON inbound_events;
CREATE POLICY tenant_isolation ON inbound_events
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE model_steps ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_steps FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON model_steps;
CREATE POLICY tenant_isolation ON model_steps
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE tool_inputs ENABLE ROW LEVEL SECURITY;
ALTER TABLE tool_inputs FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON tool_inputs;
CREATE POLICY tenant_isolation ON tool_inputs
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE task_drafts ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_drafts FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON task_drafts;
CREATE POLICY tenant_isolation ON task_drafts
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- The retention job deletes idle threads and old owner mentions. Those two tables are made by
-- migration 025, which grants DELETE itself on a new database; a database that already ran 025
-- before this change gets it here.
DO $$
BEGIN
  IF to_regclass('public.threads') IS NOT NULL THEN
    GRANT DELETE ON threads TO agentos_app;
  END IF;
  IF to_regclass('public.owner_pings') IS NOT NULL THEN
    GRANT DELETE ON owner_pings TO agentos_app;
  END IF;
END
$$;
