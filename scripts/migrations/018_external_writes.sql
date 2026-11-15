-- External writes: who may write to an outside system for one key, and what the write
-- returned. The task workflow's Notion writes (a new item, a comment) have no idempotency
-- key on Notion's side, and Notion's search can lag a write, so Postgres is the one place
-- that decides which attempt writes. An attempt claims the key with a lease, marks it
-- issued just before it sends the write, and stores the result as soon as the write
-- returns. A second attempt that finds a live lease writes nothing; one that finds a stored
-- result returns it without calling Notion. state is claimed, issued or done. result_ref is
-- a small reference to what the write made (a page's ID, title and link; a comment's ID),
-- never chat or document text. Each claim gets a fresh lease_token; mark_issued, complete and
-- release act only for the holder of the current token, so an attempt whose lease was taken
-- over cannot end the new holder's lease or store a result.
-- The app role can SELECT, INSERT and UPDATE, never DELETE.
-- Run as the owner role. Safe to run more than once.

CREATE TABLE IF NOT EXISTS external_writes (
  tenant_id   text        NOT NULL,
  idem_key    text        NOT NULL,
  state       text        NOT NULL CHECK (state IN ('claimed', 'issued', 'done')),
  result_ref  text,
  lease_until timestamptz NOT NULL,
  lease_token text        NOT NULL,   -- whoever holds the lease; every later step must present it
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, idem_key),
  CONSTRAINT external_writes_result_when_done CHECK ((state = 'done') = (result_ref IS NOT NULL))
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON external_writes FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON external_writes TO agentos_app;

ALTER TABLE external_writes ENABLE ROW LEVEL SECURITY;
ALTER TABLE external_writes FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON external_writes;
CREATE POLICY tenant_isolation ON external_writes
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
