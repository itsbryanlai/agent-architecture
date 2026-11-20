-- Google Drive connections (D-45): one service account per tenant, created in the
-- deployment's Google project when an admin clicks Connect Google Drive. The row is
-- the connection's identity and last check, not a credential: the account email and
-- status are not secret, and no key is stored per tenant. status is connected or error
-- (the settings page's last check of what the account can see); last_error is a short
-- code exactly when status is error. connected_at is the first connect (a reconnect
-- keeps it); checked_at is the last check. The app role can SELECT, INSERT, UPDATE and
-- DELETE: disconnect and uninstall remove the row. The connector audit kind already
-- exists; drive_connect and drive_disconnect are its tools, so no kinds change.
-- Run as the owner role, after 041. Safe to run more than once.

CREATE TABLE IF NOT EXISTS drive_connections (
  tenant_id             text        NOT NULL PRIMARY KEY,
  service_account_email text        NOT NULL,
  status                text        NOT NULL CHECK (status IN ('connected', 'error')),
  last_error            text,
  connected_at          timestamptz NOT NULL DEFAULT now(),
  checked_at            timestamptz,
  CONSTRAINT drive_connections_error_matches_status CHECK (
    (status = 'error') = (last_error IS NOT NULL))
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON drive_connections FROM agentos_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON drive_connections TO agentos_app;

ALTER TABLE drive_connections ENABLE ROW LEVEL SECURITY;
ALTER TABLE drive_connections FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON drive_connections;
CREATE POLICY tenant_isolation ON drive_connections
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
