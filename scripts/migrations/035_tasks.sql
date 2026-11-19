-- The platform's record of each task: one row per task, written by the task workflow
-- through record_task_state (an upsert by task ID) and read by the my_tasks tool for
-- the requester named on the row. A row holds IDs and short metadata: the item's title
-- and link, its last known status, the page it watches (a reference) and the thread it
-- lives in. Never conversation text or a comment.
-- state is one of the six listed states; updated_at is when the task last reported
-- (a state change, a status change, or a poke-answered poll). The requester index serves
-- the listing (open first, newest first); the thread index serves the in-thread poke.
-- The app role can SELECT, INSERT and UPDATE; nothing deletes a task row here.
-- Run as the owner role, after 033. Safe to run more than once.

CREATE TABLE IF NOT EXISTS tasks (
  tenant_id         text        NOT NULL,
  task_id           text        NOT NULL,
  requester_user_id text        NOT NULL,
  agent_id          text        NOT NULL,
  title             text        NOT NULL DEFAULT '',
  url               text        NOT NULL DEFAULT '',
  page_id           text        NOT NULL DEFAULT '',
  state             text        NOT NULL CHECK (
    state IN ('awaiting_approval', 'filed', 'watching', 'done', 'failed', 'cancelled')),
  last_status       text        NOT NULL DEFAULT '',
  updated_at        timestamptz NOT NULL DEFAULT now(),
  channel           text        NOT NULL,
  thread_ts         text        NOT NULL,
  PRIMARY KEY (tenant_id, task_id),
  CONSTRAINT tasks_agent_id_pattern CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$')
);

CREATE INDEX IF NOT EXISTS tasks_requester
  ON tasks (tenant_id, requester_user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS tasks_thread
  ON tasks (tenant_id, channel, thread_ts);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON tasks FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON tasks TO agentos_app;

ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE tasks FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON tasks;
CREATE POLICY tenant_isolation ON tasks
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
